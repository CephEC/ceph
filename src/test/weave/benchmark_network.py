#!/usr/bin/env python3
"""Launch seven isolated network nodes inside an already private outer network."""
import argparse
import gzip
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import time


class NetworkLab:
    def __init__(self, directory):
        parent = os.environ.get('WEAVE_PARENT_NETNS')
        if not parent or parent == os.readlink('/proc/self/ns/net'):
            raise RuntimeError('launch through benefit_benchmark.sh in a private network namespace')
        if directory.exists():
            raise FileExistsError('work directory must be unused: ' + str(directory))
        directory.mkdir(parents=True)
        if shutil.disk_usage(directory).free < 8 * 2**30:
            raise RuntimeError('benchmark requires at least 8 GiB free disk space')
        self.directory = directory
        self.keepers = []
        self.child = None
        self.topology = {'controller_pid': os.getpid(), 'nodes': {}}

    def run(self, *args):
        result = subprocess.run(args, capture_output=True, text=True, timeout=15)
        with (self.directory / 'network-setup.log').open('a') as log:
            log.write(json.dumps(args) + '\n' + result.stdout + result.stderr)
        result.check_returncode()

    def setup(self):
        self.run('ip', 'link', 'set', 'lo', 'up')
        self.run('ip', 'link', 'add', 'weavebr', 'type', 'bridge')
        self.run('ip', 'link', 'set', 'weavebr', 'up')
        for index in range(7):
            name = f'osd.{index}' if index < 6 else 'client'
            address = f'10.77.0.{11 + index}' if index < 6 else '10.77.0.100'
            process = subprocess.Popen(['unshare', '--net', '--', 'sleep', 'infinity'])
            self.keepers.append(process)
            namespace = f'/proc/{process.pid}/ns/net'
            deadline = time.monotonic() + 5
            while os.readlink(namespace) == os.readlink('/proc/self/ns/net'):
                assert time.monotonic() < deadline
                time.sleep(.01)
            host, peer = f'node{index}', f'peer{index}'
            self.run('ip', 'link', 'add', host, 'type', 'veth', 'peer', 'name', peer)
            self.run('ip', 'link', 'set', host, 'master', 'weavebr')
            self.run('ip', 'link', 'set', host, 'up')
            self.run('ip', 'link', 'set', peer, 'netns', str(process.pid))
            prefix = ['nsenter', '--net=' + namespace, '--']
            self.run(*prefix, 'ip', 'link', 'set', 'lo', 'up')
            self.run(*prefix, 'ip', 'addr', 'add', address + '/24', 'dev', peer)
            self.run(*prefix, 'ip', 'link', 'set', peer, 'up')
            self.topology['nodes'][name] = {'address': address, 'pid': process.pid,
                                           'interface': host, 'namespace': namespace}
        (self.directory / 'topology.json').write_text(json.dumps(self.topology, indent=2))

    def execute(self, arguments):
        environment = os.environ | {'WEAVE_NET_TOPOLOGY': str(self.directory / 'topology.json')}
        namespace = self.topology['nodes']['client']['namespace']
        script = Path(__file__).with_name('benefit_benchmark.py')
        self.child = subprocess.Popen(['nsenter', '--net=' + namespace, '--',
                                       sys.executable, '-u', str(script), *arguments], env=environment)
        return self.child.wait()

    def close(self):
        try:
            if self.child and self.child.poll() is None:
                self.child.send_signal(signal.SIGINT)
                self.child.wait(timeout=60)
        finally:
            for process in self.keepers:
                if process.poll() is None:
                    process.terminate()
            for process in self.keepers:
                process.wait(timeout=5)
        self.remove_cluster_data()

    def remove_cluster_data(self):
        """Keep measurements, but remove only this run's stopped daemon stores."""
        stopped_path = self.directory / 'stopped.json'
        if not stopped_path.exists():
            print('No daemon shutdown record; retaining data for manual inspection.', file=sys.stderr)
            return
        stopped = json.loads(stopped_path.read_text())
        if any(code is None for code in stopped.values()):
            raise RuntimeError('refusing to remove stores while daemons are running')
        removed = []
        for name in ['mon.a', *(f'osd.{index}' for index in range(6))]:
            path = self.directory / name
            if path.exists():
                shutil.rmtree(path)
                removed.append(name)
        for path in self.directory.iterdir():
            if path.suffix in ('.log', '.stderr') and path.is_file():
                with path.open('rb') as source, gzip.open(str(path) + '.gz', 'wb') as target:
                    shutil.copyfileobj(source, target)
                path.unlink()
            elif path.suffix in ('.asok', '.pid'):
                path.unlink()
        (self.directory / 'cleanup.json').write_text(json.dumps({
            'removed_stores': removed, 'daemon_exit_codes': stopped,
            'namespace_keeper_exit_codes': [p.returncode for p in self.keepers],
            'logs_compressed': True}, indent=2))


def interrupted(*_):
    raise KeyboardInterrupt()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--work-dir', required=True, type=Path)
    args, _ = parser.parse_known_args()
    signal.signal(signal.SIGTERM, interrupted)
    lab = NetworkLab(args.work_dir.resolve())
    try:
        lab.setup()
        status = lab.execute(sys.argv[1:])
    finally:
        lab.close()
    sys.exit(status)
