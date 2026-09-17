#!/usr/bin/env python3
"""Exercise Weave on a disposable, three-OSD vstart cluster.

Run with the build's rados Python extension and lib directory in PYTHONPATH and
LD_LIBRARY_PATH. --stop-target requires the same PID namespace as vstart.
The cluster and pool are retained for inspection; only the selected OSD is
stopped and restarted during the optional degraded-read test.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

import rados


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', required=True, type=Path)
    parser.add_argument('--cluster-dir', required=True, type=Path)
    parser.add_argument('--stop-target', action='store_true')
    args = parser.parse_args()
    build = args.build_dir.resolve()
    cluster_dir = args.cluster_dir.resolve()
    conf = cluster_dir / 'ceph.conf'
    client_log = cluster_dir / 'out/weave-client.log'
    report_path = cluster_dir / 'weave-direct-report.json'
    pool = 'weave-direct-read'
    checks = []

    def ceph(*cmd):
        result = subprocess.run([str(build / 'bin/ceph'), '-c', str(conf), *cmd],
                                check=True, capture_output=True, text=True, timeout=45)
        return result.stdout

    def config(key, value):
        ceph('config', 'set', 'osd', key, str(value))

    def wait_for(label, predicate, seconds=90):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if predicate():
                print('PASS:', label, flush=True)
                return
            time.sleep(1)
        raise AssertionError('timed out: ' + label)

    def logs():
        return client_log.read_text(errors='replace') if client_log.exists() else ''

    def routes():
        return re.findall(r'Weave redirect (\S+) to (\d+)\((\d+)\) accepted=1', logs())

    def passed(name):
        checks.append(name)
        print('PASS:', name, flush=True)
        report_path.write_text(json.dumps({'checks': checks, 'routes': routes()}, indent=2))

    def check_md5(key, data):
        _, digest = io.execute(key, 'openssl_md5', 'compute', b'')
        # The class appends its entire char[33], including the terminating NUL.
        expected = hashlib.md5(data).hexdigest().encode() + b'\0'
        assert digest == expected, (key, digest, expected)

    config('osd_aggregate_background_enabled', 'false')
    config('osd_aggregate_min_object_size', 1)
    config('osd_aggregate_quiet_period', 1)
    config('osd_aggregate_scan_interval', 1)
    config('osd_aggregate_max_padding_percent', 100)
    config('osd_aggregate_redirect_reads', 'true')
    config('debug_osd', '10/10')
    ceph('osd', 'erasure-code-profile', 'set', 'weave-direct', 'k=2', 'm=1',
         'plugin=jerasure', 'technique=reed_sol_van', 'crush-failure-domain=osd')
    ceph('osd', 'pool', 'create', pool, '1', '1', 'erasure', 'weave-direct')
    ceph('osd', 'pool', 'set', pool, 'pg_autoscale_mode', 'off')
    ceph('osd', 'pool', 'set', pool, 'min_size', '2')
    ceph('osd', 'pool', 'set', pool, 'allow_ec_overwrites', 'true')
    ceph('osd', 'pool', 'application', 'enable', pool, 'rados')
    client = rados.Rados(conffile=str(conf), conf={
        'debug_objecter': '10/10', 'log_to_file': 'true',
        'log_file': str(client_log), 'rados_osd_op_timeout': '30'})
    client.connect()
    io = client.open_ioctx(pool)
    try:
        content = {f'member-{i}': bytes((j * 17 + i) % 251 for j in range(17003 + i * 7))
                   for i in range(8)}
        versions = {}
        for key, data in content.items():
            io.write_full(key, data)
            io.set_xattr(key, 'weave-test', ('value-' + key).encode())
            io.stat(key)
            versions[key] = io.get_last_version()
        config('osd_aggregate_background_enabled', 'true')

        def observe_redirect():
            for key, data in content.items():
                assert io.read(key, len(data), 0) == data
            return bool(routes())

        wait_for('background packing and client redirection', observe_redirect, 120)
        # Stop candidate selection while exercising deterministic read behavior.
        config('osd_aggregate_background_enabled', 'false')
        for key, data in content.items():
            assert io.read(key, len(data), 0) == data
            for offset, length in [(3, 701), (4091, 5000), (len(data) - 9, 100),
                                   (len(data), 10), (len(data) + 31, 10)]:
                assert io.read(key, length, offset) == data[offset:offset + length]
            assert io.stat(key)[0] == len(data)
            assert io.get_last_version() == versions[key]
            assert io.get_xattr(key, 'weave-test') == ('value-' + key).encode()
            assert dict(io.get_xattrs(key)) == {'weave-test': ('value-' + key).encode()}
            check_md5(key, data)
        passed('full, unaligned, cross-stripe and EOF reads; logical STAT/version/xattrs; MD5')

        key, target, shard = routes()[-1]
        assert key in content, (key, routes())
        target_log = cluster_dir / f'out/osd.{target}.log'
        wait_for('replica performed local reads and data-class calls', lambda:
                 target_log.exists() and 'Weave local member read' in target_log.read_text(errors='replace')
                 and 'Weave local data-class call' in target_log.read_text(errors='replace'))
        passed('data and class results returned through the direct replica path')

        asok = str(cluster_dir / f'out/osd.{target}.asok')
        ceph('daemon', asok, 'config', 'set', 'osd_aggregate_redirect_reads', 'false')
        before = len(logs())
        assert io.read(key, len(content[key]), 0) == content[key]
        check_md5(key, content[key])
        wait_for('rejected direct read returned to primary', lambda:
                 'Weave direct read fallback' in logs()[before:])
        ceph('daemon', asok, 'config', 'set', 'osd_aggregate_redirect_reads', 'true')
        passed('receiver rejection falls back once for reads and data-class calls')

        if args.stop_target:
            ceph('osd', 'set', 'noout')
            pid = int((cluster_dir / f'out/osd.{target}.pid').read_text())
            # Require an actual OSD before touching the PID from this cluster.
            assert 'ceph-osd' in Path(f'/proc/{pid}/comm').read_text()
            os.kill(pid, signal.SIGTERM)
            try:
                ceph('osd', 'down', target)
                wait_for('target OSD is down', lambda: next(
                    x for x in json.loads(ceph('osd', 'dump', '-f', 'json'))['osds']
                    if str(x['osd']) == target)['up'] == 0)
                assert io.read(key, len(content[key]), 0) == content[key]
                check_md5(key, content[key])
                passed('missing member shard reconstructs reads and MD5 at primary')
            finally:
                subprocess.run([str(build / 'bin/ceph-osd'), '-c', str(conf), '-i', target],
                               check=True, timeout=45)
                ceph('osd', 'unset', 'noout')
                wait_for('target OSD restarted', lambda: next(
                    x for x in json.loads(ceph('osd', 'dump', '-f', 'json'))['osds']
                    if str(x['osd']) == target)['up'] == 1)

        # A foreground write materializes the group; no old route may return
        # the previous bytes or expose the internal Volume.
        replacement = b'new-value-after-materialization' * 500
        io.write_full(key, replacement)
        assert io.read(key, len(replacement), 0) == replacement
        check_md5(key, replacement)
        passed('foreground materialization preserves subsequent read and class semantics')
    finally:
        io.close()
        client.shutdown()
    print('Report:', report_path, flush=True)


if __name__ == '__main__':
    main()
