#!/usr/bin/env python3
"""Paired native EC / Weave benchmarks with real Parquet queries and netem."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import signal
import struct
import threading
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from benchmark_cluster import BenchmarkCluster, Network, NetworkProfile, ProcessMeter, difference
from durable_recovery import wait_for


PROFILES = {'local': NetworkProfile('local', 0, 0),
            'rtt0': NetworkProfile('rtt0', 0, 1000),
            'lan': NetworkProfile('lan', 0.2, 1000),
            'rtt2': NetworkProfile('rtt2', 2, 1000),
            'slow': NetworkProfile('slow', 2, 100)}


class Dataset:
    """A sealed log partition: immutable, similarly sized Parquet objects."""
    def __init__(self, count, rows, seed):
        rng = np.random.default_rng(seed)
        self.data, self.tables, self.expected = {}, {}, {}
        for index in range(count):
            key = f'events-{index:06d}'
            payload = rng.bytes(rows * 32)
            table = pa.table({
                'event_id': np.arange(index * rows, (index + 1) * rows, dtype=np.int64),
                'status': rng.integers(0, 100, rows, dtype=np.int32),
                'bytes': rng.integers(100, 100000, rows, dtype=np.int64),
                'tenant': rng.integers(0, 10000, rows, dtype=np.int32),
                'payload': [payload[offset:offset + 32].hex()
                            for offset in range(0, len(payload), 32)]})
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink, compression='snappy', use_dictionary=False,
                           row_group_size=8192, write_statistics=True)
            self.data[key] = sink.getvalue().to_pybytes()
            self.tables[key] = table
        self.keys = list(self.data)

    @staticmethod
    def filter(table, percent):
        return table.filter(pc.less(table['status'], percent)).select(['event_id', 'bytes'])

    def answer(self, key, percent):
        identity = (key, percent)
        if identity not in self.expected:
            self.expected[identity] = self.filter(self.tables[key], percent)
        return self.expected[identity]

    def manifest(self):
        return {key: {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                      'rows': self.tables[key].num_rows} for key, data in self.data.items()}


def decode_scan(data):
    version, compatible, size = struct.unpack_from('<BBI', data)
    assert version == compatible == 1 and size + 6 == len(data)
    stats_size, = struct.unpack_from('<I', data, 6)
    stats = json.loads(data[10:10 + stats_size])
    offset = 10 + stats_size
    ipc_size, = struct.unpack_from('<I', data, offset)
    assert offset + 4 + ipc_size == len(data)
    table = pa.ipc.open_stream(data[offset + 4:]).read_all()
    assert table.num_rows == stats['rows_returned']
    return table


class Operation:
    def __init__(self, dataset, workload, client_filter):
        self.dataset = dataset
        self.workload = workload
        self.client_filter = client_filter
        self.percent = int(workload.split('-')[1]) if workload.startswith('scan-') else None
        self.request = json.dumps({'version': 1, 'projection': ['event_id', 'bytes'],
            'predicate': {'op': 'lt', 'column': 'status', 'value': self.percent}}).encode()

    def execute(self, io, key, validate=False):
        if self.workload == 'read' or self.client_filter:
            data = io.read(key, len(self.dataset.data[key]) + 1)
            assert len(data) == len(self.dataset.data[key]), (key, 'length')
            if validate:
                assert data == self.dataset.data[key], (key, 'content')
            if self.workload == 'read':
                return len(data)
            table = pq.read_table(pa.BufferReader(data), columns=['event_id', 'status', 'bytes'],
                                  use_threads=False)
            table = self.dataset.filter(table, self.percent)
        else:
            result, data = io.execute(key, 'parquet_scan', 'scan', self.request)
            assert result == len(data), (result, len(data))
            table = decode_scan(data)
        expected = self.dataset.answer(key, self.percent)
        assert table.num_rows == expected.num_rows, (key, 'row count')
        if validate:
            assert table.equals(expected, check_metadata=False), (key, 'query result')
        return len(data)  # application reply bytes, excluding protocol overhead


class Benchmark:
    native = 'weave-benefit-native'
    packed = 'weave-benefit-packed'

    def __init__(self, args, cluster, network, dataset):
        self.args, self.cluster, self.network, self.dataset = args, cluster, network, dataset
        self.meter = ProcessMeter(cluster)
        self.results = {'parameters': vars(args) | {'build_dir': str(args.build_dir),
                        'work_dir': str(args.work_dir)}, 'runs': [], 'calibrations': []}
        self.results['environment'] = {'uname': list(os.uname()), 'cpus': os.cpu_count(),
            'cpu_affinity': sorted(os.sched_getaffinity(0)), 'pyarrow': pa.__version__,
            'numpy': np.__version__, 'topology': network.topology,
            'osd_worker_shards': 1, 'osd_threads_per_shard': 2,
            'osd_memory_target_bytes': 536870912}
        self.routes = {}

    def save(self):
        path = self.cluster.directory / 'results.json'
        temporary = path.with_suffix('.tmp')
        temporary.write_text(json.dumps(self.results, indent=2))
        temporary.replace(path)

    def seed(self, pool):
        self.cluster.create_pool(pool)
        with self.cluster.client.open_ioctx(pool) as io:
            for key, data in self.dataset.data.items():
                io.write_full(key, data)

    def setup(self):
        c = self.cluster
        self.network.apply(PROFILES['local'])
        self.seed(self.packed)
        buckets = Counter()
        for key in self.dataset.keys:
            mapping = json.loads(c.ceph('osd', 'map', self.packed, key, '-f', 'json'))
            buckets[mapping['pgid']] += 1
        expected_objects = sum(n // 4 + n % 4 for n in buckets.values())
        self.results['expected_coverage'] = sum(n // 4 * 4 for n in buckets.values()) / len(self.dataset.keys)
        self.results['before_pack'] = c.physical_stats(self.packed)
        self.results['calibrations'].append(self.network.apply(PROFILES['rtt2']))
        before_net, before_cpu = self.network.counters(), self.meter.snapshot()
        begin = time.perf_counter()
        c.configure('osd_weave_background_enabled', 'true')
        wait_for('physical sources retired', lambda: sum(
            v['num_objects'] for v in c.physical_stats(self.packed).values()) == expected_objects, 180)
        self.results['packing'] = {'seconds': time.perf_counter() - begin,
            'profile': 'rtt2', 'process': difference(self.meter.snapshot(), before_cpu),
            'network': difference(self.network.counters(), before_net)}
        c.configure('osd_weave_background_enabled', 'false')
        self.results['after_pack'] = c.physical_stats(self.packed)
        self.network.apply(PROFILES['local'])
        # Populate the control pool only after optional packing is stopped.
        self.seed(self.native)
        self.results['native_physical'] = c.physical_stats(self.native)
        c.configure('osd_weave_redirect_reads', 'true')
        trace = c.connect(trace=True)
        log = c.directory / 'routes.log'
        try:
            with trace.open_ioctx(self.packed) as io:
                for key in self.dataset.keys:
                    Operation(self.dataset, 'read', False).execute(io, key, True)
        finally:
            trace.shutdown()
        # Logging is asynchronous. Join request/reply IDs after shutdown flushes
        # the trace instead of attributing newly visible lines to the last read.
        text = log.read_text(errors='replace')
        requests = {tid: key for key, tid in re.findall(
            r'_op_submit oid (events-\d+).*? tid (\d+) osd', text)}
        self.routes = {requests[tid]: volume for tid, volume in re.findall(
            r'osd_op_reply\((\d+) (volume_[^ ]+)', text)}
        self.results['direct_route_observed'] = 'accepted=1' in text
        self.results['packed_members'] = self.routes
        expected_members = sum(n // 4 * 4 for n in buckets.values())
        assert len(self.routes) == expected_members, (len(self.routes), expected_members)
        assert self.results['direct_route_observed']
        self.save()
        print('SETUP:', len(self.routes), '/', len(self.dataset.keys), 'packed members;',
              'packing seconds', round(self.results['packing']['seconds'], 3), flush=True)

    def run_case(self, profile, workload, mode, concurrency, repeat):
        c, dataset = self.cluster, self.dataset
        pool = self.native if mode.startswith('native') else self.packed
        c.configure('osd_weave_redirect_reads', str(mode == 'weave-direct').lower())
        op = Operation(dataset, workload, mode == 'native-client')
        client = c.connect()
        barrier = threading.Barrier(concurrency + 1)
        deadline = [0.0]
        per_worker = math.ceil(self.args.min_ops / concurrency)

        def worker(index):
            rng = random.Random(self.args.seed + repeat * 1000 + index)
            latencies, reply_bytes, input_bytes = [], 0, 0
            with client.open_ioctx(pool) as io:
                barrier.wait(timeout=30)
                while time.perf_counter() < deadline[0] or len(latencies) < per_worker:
                    key = rng.choice(dataset.keys)
                    begin = time.perf_counter()
                    reply_bytes += op.execute(io, key)
                    latencies.append(time.perf_counter() - begin)
                    input_bytes += len(dataset.data[key])
            return latencies, reply_bytes, input_bytes

        try:
            # Warm each object's exact access path and verify complete results.
            with client.open_ioctx(pool) as io:
                for key in dataset.keys:
                    op.execute(io, key, True)
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(worker, index) for index in range(concurrency)]
                before_net, before_cpu = self.network.counters(), self.meter.snapshot()
                begin = time.perf_counter()
                deadline[0] = begin + self.args.duration
                barrier.wait(timeout=30)
                values = [future.result(timeout=120) for future in futures]
                elapsed = time.perf_counter() - begin
                process = difference(self.meter.snapshot(), before_cpu)
                network = difference(self.network.counters(), before_net)
            assert sum(v['drops'] for v in network.values()) == 0, 'netem queue dropped packets'
            latencies = [value for row in values for value in row[0]]
            result = {'profile': profile, 'workload': workload, 'mode': mode,
                'concurrency': concurrency, 'repeat': repeat, 'seconds': elapsed,
                'operations': len(latencies), 'ops_per_second': len(latencies) / elapsed,
                'logical_input_bytes': sum(row[2] for row in values),
                'application_reply_bytes': sum(row[1] for row in values),
                'latency_ms': {f'p{q}': float(np.percentile(latencies, q) * 1000)
                               for q in (50, 95, 99)},
                'samples_seconds': latencies, 'process': process, 'network': network}
            self.results['runs'].append(result)
            self.save()
            print('RUN:', profile, workload, mode, 'qd', concurrency, 'repeat', repeat,
                  'ops/s', round(result['ops_per_second'], 1), flush=True)
        finally:
            client.shutdown()

    def measure(self):
        for name in self.args.profiles:
            self.results['calibrations'].append(self.network.apply(PROFILES[name]))
            for repeat in range(self.args.repetitions):
                cases = []
                for workload in self.args.workloads:
                    modes = ['native-client', 'weave-primary', 'weave-direct']
                    if workload != 'read':
                        modes.insert(1, 'native-pushdown')
                    cases += [(workload, mode, qd) for mode in modes for qd in self.args.qd]
                random.Random(self.args.seed + repeat).shuffle(cases)
                for workload, mode, qd in cases:
                    self.run_case(name, workload, mode, qd, repeat)

    def overwrite_cost(self):
        self.network.apply(PROFILES['rtt2'])
        representatives = {}
        for key, volume in self.routes.items():
            representatives.setdefault(volume, key)
        keys = list(representatives.values())[:4]
        result = {}
        for pool in (self.native, self.packed):
            before_net, before_cpu = self.network.counters(), self.meter.snapshot()
            latencies = []
            with self.cluster.client.open_ioctx(pool) as io:
                for key in keys:
                    begin = time.perf_counter()
                    io.write(key, b'X', 16)
                    latencies.append(time.perf_counter() - begin)
                for key, original in self.dataset.data.items():
                    expected = original[:16] + b'X' + original[17:] if key in keys else original
                    assert io.read(key, len(expected) + 1) == expected
            result[pool] = {'keys': keys, 'latencies_seconds': latencies,
                'process_including_verification': difference(self.meter.snapshot(), before_cpu),
                'network_including_verification': difference(self.network.counters(), before_net)}
        self.results['overwrite'] = result
        self.save()


def interrupted(*_):
    raise KeyboardInterrupt()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', required=True, type=Path)
    parser.add_argument('--work-dir', required=True, type=Path)
    parser.add_argument('--objects', type=int, default=32)
    parser.add_argument('--rows', type=int, default=16384)
    parser.add_argument('--pgs', type=int, default=1)
    parser.add_argument('--seed', type=int, default=20260916)
    parser.add_argument('--profiles', nargs='+', choices=PROFILES, default=['rtt0', 'rtt2'])
    parser.add_argument('--workloads', nargs='+', choices=['read', 'scan-1', 'scan-50', 'scan-100'],
                        default=['read', 'scan-1'])
    parser.add_argument('--qd', nargs='+', type=int, default=[1, 8])
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--duration', type=float, default=2)
    parser.add_argument('--min-ops', type=int, default=64)
    args = parser.parse_args()
    assert args.objects >= 4 and args.rows > 0 and args.pgs > 0
    assert args.repetitions > 0 and args.duration > 0 and args.min_ops > 0 and min(args.qd) > 0
    args.work_dir = args.work_dir.resolve()
    assert Path(os.environ['WEAVE_NET_TOPOLOGY']).parent == args.work_dir
    assert not (args.work_dir / 'ceph.conf').exists(), 'cluster directory must be unused'
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    dataset = Dataset(args.objects, args.rows, args.seed)
    (args.work_dir / 'dataset.json').write_text(json.dumps(dataset.manifest(), indent=2))
    signal.signal(signal.SIGTERM, interrupted)
    network = Network(args.work_dir)
    network.apply(PROFILES['local'])
    cluster = BenchmarkCluster(args.build_dir, args.work_dir, args.pgs)
    try:
        cluster.start()
        benchmark = Benchmark(args, cluster, network, dataset)
        benchmark.setup()
        benchmark.measure()
        benchmark.overwrite_cost()
    finally:
        cluster.close()


if __name__ == '__main__':
    main()
