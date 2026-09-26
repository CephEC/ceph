#!/usr/bin/env python3
"""D1 crash matrix on a disposable six-OSD BlueStore 4+2 EC cluster.

Run with the build's lib directory in LD_LIBRARY_PATH and its
lib/cython_modules/lib.3 plus src/pybind and src/python-common in PYTHONPATH.
Only processes and pools created by this harness are changed. Daemons are
stopped on exit; the supplied empty work directory retains data and evidence.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import resource
import socket
import subprocess
import time
import uuid

import rados


def wait_for(label, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.25)
    raise AssertionError('timed out: ' + label)


class IsolatedCluster:
    def __init__(self, build, directory):
        self.build = build.resolve()
        self.directory = directory.resolve()
        self.conf = self.directory / 'ceph.conf'
        self.processes = {}
        self.client = None

    def run(self, binary, *args, timeout=60):
        command = [str(self.build / 'bin' / binary), *map(str, args)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        with (self.directory / 'commands.log').open('a') as log:
            log.write(json.dumps(command) + '\n' + result.stdout + result.stderr)
        if result.returncode:
            raise RuntimeError(f'{command}: {result.returncode}\n{result.stdout}\n{result.stderr}')
        return result.stdout

    def ceph(self, *args):
        return self.run('ceph', '-c', self.conf, *args)

    def admin(self, osd, *args):
        result = self.ceph('daemon', self.directory / f'osd.{osd}.asok', *args)
        if result.startswith('ERROR:'):
            raise RuntimeError(result)
        return result

    def configure(self, key, value):
        self.ceph('config', 'set', 'osd', key, value)
        # Wait for the exact setting on every currently live daemon.
        for osd in range(6):
            process = self.processes.get(f'osd.{osd}')
            if process and process.poll() is None:
                self.admin(osd, 'config', 'set', key, value)

    def start_daemon(self, name):
        kind, identity = name.split('.')
        with (self.directory / f'{name}.stderr').open('a') as log:
            self.processes[name] = subprocess.Popen(
                [str(self.build / 'bin' / ('ceph-' + kind)), '-c', str(self.conf),
                 '-i', identity, '-f'], stdout=log, stderr=subprocess.STDOUT)

    def stop_daemon(self, name):
        process = self.processes.get(name)
        if process and process.poll() is None:
            process.terminate()

    def join_daemon(self, name):
        process = self.processes.get(name)
        if process:
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)

    def restart_osds(self):
        for osd in range(6):
            self.stop_daemon(f'osd.{osd}')
        for osd in range(6):
            self.join_daemon(f'osd.{osd}')
        for osd in range(6):
            self.start_daemon(f'osd.{osd}')

    def start(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        assert not self.conf.exists(), 'work directory must be unused'
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        listeners = [socket.socket(), socket.socket()]
        for listener in listeners:
            listener.bind(('127.0.0.1', 0))
        ports = [listener.getsockname()[1] for listener in listeners]
        for listener in listeners:
            listener.close()
        fsid = str(uuid.uuid4())
        address = f'[v2:127.0.0.1:{ports[0]},v1:127.0.0.1:{ports[1]}]'
        self.conf.write_text(f'''[global]
fsid = {fsid}
mon initial members = a
mon host = {address}
auth cluster required = none
auth service required = none
auth client required = none
public network = 127.0.0.0/8
cluster network = 127.0.0.0/8
log to file = true
log to stderr = false
log file = {self.directory}/$name.log
admin socket = {self.directory}/$name.asok
pid file = {self.directory}/$name.pid
crash dir = {self.directory}/crash
erasure code dir = {self.build}/lib
osd class dir = {self.build}/lib
plugin dir = {self.build}/lib
mon allow pool delete = true
osd crush chooseleaf type = 0
osd pool default pg autoscale mode = off
enable experimental unrecoverable data corrupting features = *
[mon.a]
mon data = {self.directory}/mon.a
[osd]
osd data = {self.directory}/osd.$id
osd objectstore = bluestore
bluestore block size = 1073741824
bdev aio max queue depth = 128
osd memory target = 536870912
osd op num shards = 1
osd op num threads per shard = 2
osd weave background enabled = false
osd weave min object size = 1
osd weave quiet period = 0.5
osd weave scan interval = 0.2
osd weave max padding percent = 100
osd weave cleanup live percent = 100
debug osd = 10/10
''')
        (self.directory / 'mon.a').mkdir()
        monmap = self.directory / 'monmap'
        self.run('monmaptool', '--create', '--fsid', fsid, '--addv', 'a', address, monmap)
        self.run('ceph-mon', '-c', self.conf, '-i', 'a', '--mkfs', '--monmap', monmap)
        self.start_daemon('mon.a')
        self.ceph('status')
        self.ceph('osd', 'set', 'noout')
        for osd in range(6):
            osd_uuid = str(uuid.uuid4())
            self.ceph('osd', 'new', osd_uuid, osd)
            (self.directory / f'osd.{osd}').mkdir()
            self.run('ceph-osd', '-c', self.conf, '-i', osd, '--mkfs',
                     '--osd-uuid', osd_uuid)
            self.start_daemon(f'osd.{osd}')
        self.ceph('osd', 'erasure-code-profile', 'set', 'weave-d1', 'k=4', 'm=2',
                  'plugin=jerasure', 'technique=reed_sol_van', 'crush-failure-domain=osd')
        self.client = rados.Rados(conffile=str(self.conf), conf={
            'rados_osd_op_timeout': '120', 'admin_socket': '',
            'debug_objecter': '10/10', 'log_to_file': 'true',
            'log_file': str(self.directory / 'client.log')})
        self.client.connect()
        print('READY: isolated BlueStore 4+2 cluster', flush=True)

    def mapping(self, pool):
        return json.loads(self.ceph('osd', 'map', pool, 'member-0', '-f', 'json'))

    def clean(self, pool):
        mapping = self.mapping(pool)
        if mapping['acting_primary'] < 0:
            return False
        query = json.loads(self.ceph('pg', mapping['pgid'], 'query', '-f', 'json'))
        return 'active' in query['state'] and 'clean' in query['state']

    def create_pool(self, pool):
        self.ceph('osd', 'pool', 'create', pool, 1, 1, 'erasure', 'weave-d1')
        self.ceph('osd', 'pool', 'set', pool, 'allow_ec_overwrites', 'true')
        self.ceph('osd', 'pool', 'application', 'enable', pool, 'rados')
        wait_for(pool + ' clean', lambda: self.clean(pool))

    def close(self):
        if self.client:
            self.client.shutdown()
        for name in self.processes:
            self.stop_daemon(name)
        for name in self.processes:
            self.join_daemon(name)
        (self.directory / 'stopped.json').write_text(json.dumps(
            {name: process.poll() for name, process in self.processes.items()}, indent=2))


class CrashCase:
    def __init__(self, cluster, index, point, self_managed=False):
        self.cluster = cluster
        self.pool = f'weave-d1-{index}'
        self.point = point
        self.self_managed = self_managed
        self.snap_ids = []
        self.io = None
        self.saved = {}
        self.original = {f'member-{i}': bytes([65 + i]) * 16384 for i in range(4)}

    def snapshot(self, name):
        if self.self_managed:
            sid = self.io.create_self_managed_snap()
            self.snap_ids.insert(0, sid)
            self.io.set_self_managed_snap_write(self.snap_ids)
            return sid
        self.io.create_snap(name)
        return self.io.lookup_snap(name).snap_id

    def stat(self, key):
        size, mtime = self.io.stat(key)
        return size, repr(mtime), self.io.get_last_version()

    def check(self, key, expected, saved=None):
        assert self.io.read(key, 20000) == expected, (self.point, key, 'data')
        if saved:
            assert self.stat(key) == saved, (self.point, key, 'stat/version/mtime')
        assert self.io.get_xattr(key, 'tag') == key.encode(), (self.point, key, 'xattr')

    def snapshots(self):
        try:
            self.io.set_read(self.before)
            for key, data in self.original.items():
                self.check(key, data, self.saved[key])
            self.io.set_read(self.absent)
            for key in self.original:
                try:
                    self.io.read(key, 1)
                except rados.ObjectNotFound:
                    continue
                raise AssertionError((self.point, key, 'existed before creation'))
        finally:
            self.io.set_read(rados.LIBRADOS_SNAP_HEAD)

    def run(self):
        c = self.cluster
        c.create_pool(self.pool)
        self.io = c.client.open_ioctx(self.pool)
        try:
            self.absent = self.snapshot('absent')
            for key, data in self.original.items():
                self.io.write_full(key, data)
                self.io.set_xattr(key, 'tag', key.encode())
                self.saved[key] = self.stat(key)
            self.before = self.snapshot('before')
            primary = c.mapping(self.pool)['acting_primary']
            unpack = self.point.startswith(('member_', 'volume_'))
            log = c.directory / 'client.log'
            start = log.stat().st_size if log.exists() else 0
            if unpack:
                c.configure('osd_weave_background_enabled', 'true')

                def packed():
                    for key, data in self.original.items():
                        self.check(key, data, self.saved[key])
                    return 'accepted=1' in log.read_text(errors='replace')[start:]

                wait_for('packed before ' + self.point, packed)
                c.configure('osd_weave_background_enabled', 'false')
            crash_log = c.directory / f'osd.{primary}.stderr'
            crash_start = crash_log.stat().st_size
            c.admin(primary, 'config', 'set', 'osd_weave_debug_crash_point', self.point)
            with ThreadPoolExecutor(max_workers=1) as executor:
                pending = None
                if unpack:
                    def trigger():
                        writer = c.client.open_ioctx(self.pool)
                        try:
                            if self.self_managed:
                                writer.set_self_managed_snap_write(self.snap_ids)
                            writer.write_full('member-0', b'trigger')
                        finally:
                            writer.close()
                    pending = executor.submit(trigger)
                else:
                    c.configure('osd_weave_background_enabled', 'true')
                wait_for('crash at ' + self.point,
                         lambda: c.processes[f'osd.{primary}'].poll() is not None)
                stderr = crash_log.read_text(errors='replace')[crash_start:]
                assert 'Weave conversion crash at ' + self.point in stderr, self.point
                c.configure('osd_weave_background_enabled', 'false')
                # Force a different primary as well as reconstructing the dead OSD.
                c.ceph('osd', 'primary-affinity', primary, '0')
                c.start_daemon(f'osd.{primary}')
                wait_for('recovery after ' + self.point, lambda: c.clean(self.pool))
                successor = c.mapping(self.pool)['acting_primary']
                assert successor != primary, (self.point, 'primary did not change')
                if pending:
                    pending.result(timeout=120)
            for key, data in self.original.items():
                if not unpack or key != 'member-0':
                    self.check(key, data, self.saved[key])
            self.io.write_full('member-0', b'confirmed new version')
            self.io.remove_object('member-1')
            self.io.remove_object('member-2')
            self.io.write_full('member-2', b'confirmed new generation')
            self.io.set_xattr('member-2', 'tag', b'member-2')
            after = {key: self.stat(key) for key in ('member-0', 'member-2', 'member-3')}
            self.snapshots()
            # Every case reconstructs all controllers after acknowledged changes.
            c.restart_osds()
            wait_for('full restart after ' + self.point, lambda: c.clean(self.pool))
            self.check('member-0', b'confirmed new version', after['member-0'])
            self.check('member-2', b'confirmed new generation', after['member-2'])
            self.check('member-3', self.original['member-3'], after['member-3'])
            try:
                self.io.read('member-1', 1)
            except rados.ObjectNotFound:
                pass
            else:
                raise AssertionError((self.point, 'deleted object resurrected'))
            self.snapshots()
            for osd in range(6):
                c.admin(osd, 'weave', 'cleanup')
            self.check('member-2', b'confirmed new generation', after['member-2'])
            c.ceph('osd', 'primary-affinity', primary, '1')
            return {'checkpoint': self.point, 'old_primary': primary, 'new_primary': successor,
                    'snapshot_mode': 'self-managed' if self.self_managed else 'pool',
                    'versions': after, 'result': 'PASS'}
        finally:
            self.io.close()
            c.ceph('osd', 'pool', 'delete', self.pool, self.pool, '--yes-i-really-really-mean-it')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--point', action='append', help='Run only these checkpoint:index values')
    parser.add_argument('--self-managed', action='store_true', help='Use client-managed snapshots')
    args = parser.parse_args()
    points = ['pack_before_write:0', 'pack_committed:0', 'pack_published:0']
    for stage in ('source_before_remove', 'source_removed', 'member_before_write', 'member_written'):
        points.extend(f'{stage}:{i}' for i in range(4))
    points += ['volume_before_remove:0', 'volume_removed:0', 'volume_detached:0']
    cluster = IsolatedCluster(args.build_dir, args.work_dir)
    results = []
    try:
        cluster.start()
        for index, point in enumerate(args.point or points):
            print('RUN:', point, flush=True)
            results.append(CrashCase(cluster, index, point, args.self_managed).run())
            (cluster.directory / 'results.json').write_text(json.dumps(results, indent=2))
            print('PASS:', point, 'crash, primary change, mutations, snapshots, full restart', flush=True)
    finally:
        cluster.close()


if __name__ == '__main__':
    main()
