"""Disposable Ceph cluster and per-destination network emulation for benchmarks."""
from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import time

from ceph_daemon import admin_socket
import rados

from durable_recovery import IsolatedCluster, wait_for


@dataclass(frozen=True)
class NetworkProfile:
    name: str
    rtt_ms: float
    rate_mbit: int


class Network:
    """Shape each node's incoming veth; outgoing sockets have distinct source IPs."""
    addresses = [f'10.77.0.{11 + n}' for n in range(6)] + ['10.77.0.100']

    def __init__(self, directory):
        self.directory = directory
        topology = Path(os.environ['WEAVE_NET_TOPOLOGY'])
        assert topology.parent == directory
        self.topology = json.loads(topology.read_text())
        controller = self.topology['controller_pid']
        self.controller = ['nsenter', f'--net=/proc/{controller}/ns/net', '--']
        assert os.readlink(f'/proc/{controller}/ns/net') != os.environ['WEAVE_PARENT_NETNS']
        self.applied = False

    def command(self, *args):
        result = subprocess.run(args, capture_output=True, text=True, timeout=15)
        with (self.directory / 'network.log').open('a') as log:
            log.write(json.dumps(args) + '\n' + result.stdout + result.stderr)
        result.check_returncode()
        return result.stdout

    def apply(self, profile):
        for node in self.topology['nodes'].values():
            # netem change/replace can retain an omitted previous rate. Recreate
            # between idle phases and verify the actual kernel configuration.
            if self.applied:
                self.command(*self.controller, 'tc', 'qdisc', 'del', 'dev', node['interface'], 'root')
            args = [*self.controller, 'tc', 'qdisc', 'add', 'dev', node['interface'],
                    'root', 'netem', 'limit', '20000',
                    'delay', f'{profile.rtt_ms / 2}ms']
            if profile.rate_mbit:
                args += ['rate', f'{profile.rate_mbit}mbit']
            self.command(*args)
        self.applied = True
        actual = {}
        for node in self.topology['nodes'].values():
            queues = json.loads(self.command(*self.controller, 'tc', '-j', '-s', 'qdisc',
                                            'show', 'dev', node['interface']))
            options = next(q['options'] for q in queues if q['kind'] == 'netem')
            rate = options.get('rate', {}).get('rate', 0)
            delay = options.get('delay', {}).get('delay', 0)
            assert rate == profile.rate_mbit * 1000000 / 8, (profile, options)
            assert abs(delay - profile.rtt_ms / 2000) < 1e-7, (profile, options)
            actual[node['address']] = options
        # Requests and replies each traverse one destination queue.
        calibration = self.command('ping', '-n', '-c', '5', '-i', '0.05', self.addresses[0])
        return {'profile': profile.__dict__, 'actual_qdiscs': actual, 'ping': calibration}

    def counters(self):
        result = {}
        for node in self.topology['nodes'].values():
            queues = json.loads(self.command(*self.controller, 'tc', '-j', '-s', 'qdisc',
                                            'show', 'dev', node['interface']))
            queue = next(q for q in queues if q['kind'] == 'netem')
            result[node['address']] = {key: queue.get(key, 0) for key in ('bytes', 'packets', 'drops')}
        assert len(result) == 7
        return result


class BenchmarkCluster(IsolatedCluster):
    def __init__(self, build, directory, pgs):
        super().__init__(build, directory)
        self.pgs = pgs
        self.configured = False
        self.placements = {}
        self.topology = json.loads(Path(os.environ['WEAVE_NET_TOPOLOGY']).read_text())

    def run(self, binary, *args, timeout=60):
        if binary == 'monmaptool' and not self.configured:
            text = self.conf.read_text().replace('debug osd = 10/10', 'debug osd = 0/0')
            text = text.replace('127.0.0.1', Network.addresses[-1]).replace('127.0.0.0/8', '10.77.0.0/24')
            text = text.replace('bluestore block size = 1073741824',
                                'bluestore block size = 4294967296')
            text += '\nosd scrub during recovery = false\n'
            for osd in range(6):
                address = Network.addresses[osd]
                text += f'\n[osd.{osd}]\npublic addr = {address}\ncluster addr = {address}\n'
            self.conf.write_text(text)
            self.configured = True
        if binary == 'monmaptool':
            args = tuple(str(arg).replace('127.0.0.1', Network.addresses[-1]) for arg in args)
        return super().run(binary, *args, timeout=timeout)

    def start_daemon(self, name):
        if name.startswith('mon.'):
            return super().start_daemon(name)
        namespace = self.topology['nodes'][name]['namespace']
        with (self.directory / f'{name}.stderr').open('a') as log:
            self.processes[name] = subprocess.Popen([
                'nsenter', '--net=' + namespace, '--', str(self.build / 'bin/ceph-osd'),
                '-c', str(self.conf), '-i', name.split('.')[1], '-f'],
                stdout=log, stderr=subprocess.STDOUT)

    def start(self):
        super().start()
        self.client.shutdown()
        self.client = self.connect()
        self.ceph('osd', 'set', 'noscrub')
        self.ceph('osd', 'set', 'nodeep-scrub')
        self.configure('osd_aggregate_quiet_period', '0')
        self.configure('osd_aggregate_scan_interval', '0.05')

    def connect(self, trace=False):
        client = rados.Rados(conffile=str(self.conf), conf={
            'rados_osd_op_timeout': '45', 'admin_socket': '',
            'debug_objecter': '10/10' if trace else '0/0',
            'log_to_file': 'true', 'log_file': str(self.directory / 'routes.log')})
        client.connect()
        return client

    def admin(self, osd, *args):
        result = admin_socket(str(self.directory / f'osd.{osd}.asok'), list(map(str, args)))
        text = result.decode()
        if text.startswith('ERROR:'):
            raise RuntimeError(text)
        return text

    def configure(self, key, value):
        for osd in range(6):
            self.admin(osd, 'config', 'set', key, str(value))

    def query(self, pgid):
        mapping = json.loads(self.ceph('pg', 'map', pgid, '-f', 'json'))
        return json.loads(self.admin(mapping['acting'][0], 'pg', pgid, 'query'))

    def clean(self, pool):
        pool_id = self.mapping(pool)['pool_id']
        try:
            return all('active' in (state := self.query(f'{pool_id}.{n:x}')['state'])
                       and 'clean' in state for n in range(self.pgs))
        except (RuntimeError, FileNotFoundError):
            return False

    def create_pool(self, pool):
        self.ceph('osd', 'pool', 'create', pool, self.pgs, self.pgs, 'erasure', 'weave-d1')
        self.ceph('osd', 'pool', 'set', pool, 'allow_ec_overwrites', 'true')
        self.ceph('osd', 'pool', 'application', 'enable', pool, 'rados')
        wait_for(pool + ' clean', lambda: self.clean(pool))
        pool_id = self.mapping(pool)['pool_id']
        self.placements[pool] = {
            f'{pool_id}.{n:x}': json.loads(self.ceph('pg', 'map', f'{pool_id}.{n:x}',
                                                  '-f', 'json'))['acting'][0]
            for n in range(self.pgs)}

    def physical_stats(self, pool):
        stats = {}
        for pgid, primary in self.placements[pool].items():
            query = json.loads(self.admin(primary, 'pg', pgid, 'query'))
            stats[pgid] = query['info']['stats']['stat_sum']
        return stats


class ProcessMeter:
    def __init__(self, cluster):
        self.pids = {name: process.pid for name, process in cluster.processes.items()
                     if name.startswith('osd.')}
        self.pids['client'] = os.getpid()

    def snapshot(self):
        result = {}
        for name, pid in self.pids.items():
            path = Path('/proc') / str(pid)
            fields = (path / 'stat').read_text().split(') ', 1)[1].split()
            io = dict(line.split(': ') for line in (path / 'io').read_text().splitlines())
            result[name] = {'cpu_seconds': (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK'),
                            'read_bytes': int(io['read_bytes']),
                            'write_bytes': int(io['write_bytes'])}
        return result


def difference(after, before):
    return {name: {key: value - before[name][key] for key, value in counters.items()}
            for name, counters in after.items()}
