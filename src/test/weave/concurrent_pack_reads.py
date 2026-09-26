#!/usr/bin/env python3
"""Concurrent reads during packing on an isolated BlueStore 4+2 EC cluster."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import threading
import time

import rados
from durable_recovery import IsolatedCluster, wait_for


class ReaderLoad:
    def __init__(self, case, workers=4):
        self.case = case
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.counts = [0] * workers
        self.longest = [0.0] * workers
        self.executor = ThreadPoolExecutor(max_workers=workers)
        self.tasks = []

    def start(self):
        self.tasks = [self.executor.submit(self.read, index) for index in range(len(self.counts))]
        return self

    def read(self, index):
        io = self.case.cluster.client.open_ioctx(self.case.pool)
        try:
            while not self.stop.is_set():
                for key in self.case.data:
                    begin = time.monotonic()
                    self.case.check(io, key)
                    elapsed = time.monotonic() - begin
                    with self.lock:
                        self.counts[index] += 1
                        self.longest[index] = max(self.longest[index], elapsed)
                    if self.stop.is_set():
                        break
        finally:
            io.close()

    def sample(self):
        for task in self.tasks:
            if task.done():
                task.result()  # Surface data errors immediately.
        with self.lock:
            return list(self.counts)

    def progress(self, label, samples=8):
        before = self.sample()
        wait_for(label, lambda: all(n - old >= samples for n, old in zip(self.sample(), before)), 30)
        return [n - old for n, old in zip(self.sample(), before)]

    def close(self):
        self.stop.set()
        try:
            for task in self.tasks:
                task.result(timeout=120)
        finally:
            self.executor.shutdown(wait=True)


class PackingReadCase:
    def __init__(self, cluster, direct):
        self.cluster = cluster
        self.direct = direct
        self.pool = 'weave-pack-reads-' + ('direct' if direct else 'primary')
        self.data = {f'member-{i}': bytes([65 + i]) * (2 << 20) for i in range(4)}
        self.saved = {}
        # The class appends its whole char[33], including the terminating NUL.
        self.digests = {key: hashlib.md5(value).hexdigest().encode() + b'\0'
                        for key, value in self.data.items()}

    @staticmethod
    def stat(io, key):
        size, mtime = io.stat(key)
        return size, repr(mtime), io.get_last_version()

    def check(self, io, key):
        assert io.read(key, len(self.data[key]) + 1) == self.data[key], (key, 'bytes')
        assert self.stat(io, key) == self.saved[key], (key, 'stat/version/mtime')
        assert io.get_xattr(key, 'tag') == key.encode(), (key, 'xattr')
        result, digest = io.execute(key, 'openssl_md5', 'compute', b'')
        assert result == len(digest) and digest == self.digests[key], (key, 'data-class')

    def run(self):
        c = self.cluster
        c.configure('osd_weave_background_enabled', 'false')
        c.configure('osd_weave_redirect_reads', str(self.direct).lower())
        c.configure('osd_weave_debug_source_remove_error', 'true')
        c.create_pool(self.pool)
        io = c.client.open_ioctx(self.pool)
        load = None
        paused = None
        writer_executor = ThreadPoolExecutor(max_workers=1)
        writer = None
        try:
            for key, data in self.data.items():
                io.write_full(key, data)
                io.set_xattr(key, 'tag', key.encode())
                self.saved[key] = self.stat(io, key)
            io.create_snap('before')
            snapshot = io.lookup_snap('before').snap_id
            mapping = c.mapping(self.pool)
            primary = mapping['acting_primary']
            parity = next(osd for osd in mapping['acting'][4:] if osd != primary)
            primary_log = c.directory / f'osd.{primary}.log'
            log_offset = primary_log.stat().st_size
            client_log = c.directory / 'client.log'
            client_offset = client_log.stat().st_size
            load = ReaderLoad(self).start()
            load.progress('initial native reads')

            # Stopping a parity shard prevents durable write completion while
            # ordinary EC reads can still use all four data shards.
            paused = c.processes[f'osd.{parity}']
            os.kill(paused.pid, signal.SIGSTOP)
            c.admin(primary, 'config', 'set', 'osd_weave_background_enabled', 'true')

            def volume_write_pending():
                ops = json.loads(c.admin(primary, 'dump_ops_in_flight'))['ops']
                return any('volume_' in op['description'] and 'writefull' in op['description'] for op in ops)

            wait_for('Volume write awaiting parity', volume_write_pending, 30)
            native_samples = load.progress('native reads during pending Volume write')
            assert volume_write_pending(), 'Volume write completed while parity was stopped'
            print('VERIFIED: native reads while Volume commit is waiting', native_samples, flush=True)

            def mutate():
                writer_io = c.client.open_ioctx(self.pool)
                try:
                    writer_io.write_full('member-0', b'acknowledged new version')
                finally:
                    writer_io.close()

            writer = writer_executor.submit(mutate)
            load.progress('reads continue with a waiting foreground write', 4)
            assert not writer.done(), 'foreground mutation bypassed packing reservation'
            os.kill(paused.pid, signal.SIGCONT)
            paused = None
            marker = 'Weave source retirement injected EIO for '
            wait_for('published Volume and failed source cleanup',
                     lambda: marker in primary_log.read_text(errors='replace')[log_offset:])
            packed_samples = load.progress('Volume reads during repeated source deletion failures')
            errors = primary_log.read_text(errors='replace')[log_offset:].count(marker)
            assert errors >= 2, 'did not observe repeated retirement failures'
            assert not writer.done(), 'mutation escaped reservation during source cleanup'
            print('VERIFIED: Volume reads while source deletion fails', packed_samples, flush=True)
            c.configure('osd_weave_background_enabled', 'false')
            load.close()
            counts, longest = load.sample(), list(load.longest)
            load = None
            # Check direct routing without concurrent readers forcing normal
            # primary fallback. Retirement remains in its injected error loop.
            for key in self.data:
                self.check(io, key)
            redirected = 'accepted=1' in client_log.read_text(errors='replace')[client_offset:]
            if self.direct:
                assert redirected, 'no successful data-shard redirect observed'
            else:
                assert not redirected, 'unexpected redirect with direct reads disabled'
            c.configure('osd_weave_debug_source_remove_error', 'false')
            writer.result(timeout=120)
            assert io.read('member-0', 100) == b'acknowledged new version'
            assert io.get_last_version() > self.saved['member-0'][2]
            io.remove_object('member-1')
            io.write_full('member-1', b'new generation')
            io.set_xattr('member-1', 'tag', b'member-1')
            changed = {key: self.stat(io, key) for key in ('member-0', 'member-1')}
            c.restart_osds()
            wait_for('restart with acknowledged mutations', lambda: c.clean(self.pool))
            for key, expected in (('member-0', b'acknowledged new version'), ('member-1', b'new generation')):
                assert io.read(key, 100) == expected
                assert self.stat(io, key) == changed[key]
            for key in ('member-2', 'member-3'):
                self.check(io, key)
            io.set_read(snapshot)
            try:
                for key in self.data:
                    self.check(io, key)
            finally:
                io.set_read(rados.LIBRADOS_SNAP_HEAD)
            return {'mode': 'direct' if self.direct else 'primary', 'result': 'PASS',
                    'primary': primary, 'paused_parity': parity,
                    'native_samples_per_reader': native_samples,
                    'packed_samples_per_reader': packed_samples,
                    'total_samples_per_reader': counts, 'max_sample_seconds': longest,
                    'source_retirement_errors': errors, 'redirect_observed': redirected,
                    'acknowledged_versions': changed}
        finally:
            if paused:
                os.kill(paused.pid, signal.SIGCONT)
            try:
                c.configure('osd_weave_debug_source_remove_error', 'false')
                c.configure('osd_weave_background_enabled', 'false')
                if load:
                    load.close()
            finally:
                writer_executor.shutdown(wait=True)
                io.close()
                c.ceph('osd', 'pool', 'delete', self.pool, self.pool, '--yes-i-really-really-mean-it')


class ReadFailoverCase(PackingReadCase):
    def __init__(self, cluster):
        super().__init__(cluster, True)
        self.pool = 'weave-pack-reads-failover'

    def run(self):
        c = self.cluster
        c.configure('osd_weave_background_enabled', 'false')
        c.configure('osd_weave_redirect_reads', 'true')
        c.configure('osd_weave_debug_source_remove_error', 'true')
        c.create_pool(self.pool)
        io = c.client.open_ioctx(self.pool)
        load = None
        try:
            for key, data in self.data.items():
                io.write_full(key, data)
                io.set_xattr(key, 'tag', key.encode())
                self.saved[key] = self.stat(io, key)
            primary = c.mapping(self.pool)['acting_primary']
            log = c.directory / f'osd.{primary}.log'
            offset = log.stat().st_size
            load = ReaderLoad(self).start()
            load.progress('native readers before failover packing')
            c.configure('osd_weave_background_enabled', 'true')
            wait_for('published before failover', lambda:
                     'Weave source retirement injected EIO for ' in log.read_text(errors='replace')[offset:])
            load.progress('packed readers before primary failure')
            c.configure('osd_weave_background_enabled', 'false')
            before = load.sample()
            failed = c.processes[f'osd.{primary}']
            failed.kill()
            failed.wait(timeout=10)
            c.ceph('osd', 'primary-affinity', primary, '0')
            c.start_daemon(f'osd.{primary}')
            wait_for('different primary and recovered Volume', lambda: c.clean(self.pool))
            successor = c.mapping(self.pool)['acting_primary']
            assert successor != primary, 'primary did not change'
            load.progress('reads after primary failure')
            load.close()
            counts = load.sample()
            longest = list(load.longest)
            load = None
            c.configure('osd_weave_debug_source_remove_error', 'false')
            io.write_full('member-0', b'new version after failover')
            changed = self.stat(io, 'member-0')
            c.restart_osds()
            wait_for('restart after concurrent-read failover', lambda: c.clean(self.pool))
            assert io.read('member-0', 100) == b'new version after failover'
            assert self.stat(io, 'member-0') == changed
            for key in ('member-1', 'member-2', 'member-3'):
                self.check(io, key)
            c.ceph('osd', 'primary-affinity', primary, '1')
            return {'mode': 'failover', 'result': 'PASS', 'old_primary': primary,
                    'new_primary': successor, 'killed_exit_code': failed.returncode,
                    'samples_before_failure': before, 'total_samples_per_reader': counts,
                    'max_sample_seconds': longest, 'acknowledged_version': changed}
        finally:
            try:
                c.configure('osd_weave_debug_source_remove_error', 'false')
                c.configure('osd_weave_background_enabled', 'false')
                if load:
                    load.close()
            finally:
                io.close()
                c.ceph('osd', 'pool', 'delete', self.pool, self.pool, '--yes-i-really-really-mean-it')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--case', action='append', choices=['primary', 'direct', 'failover'])
    args = parser.parse_args()
    cluster = IsolatedCluster(args.build_dir, args.work_dir)
    results = []
    try:
        cluster.start()
        for name in args.case or ['primary', 'direct', 'failover']:
            print('RUN:', name, flush=True)
            case = ReadFailoverCase(cluster) if name == 'failover' else PackingReadCase(cluster, name == 'direct')
            results.append(case.run())
            (cluster.directory / 'results.json').write_text(json.dumps(results, indent=2))
            print('PASS:', results[-1], flush=True)
    finally:
        cluster.close()


if __name__ == '__main__':
    main()
