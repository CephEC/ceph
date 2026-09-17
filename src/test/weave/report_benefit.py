#!/usr/bin/env python3
"""Summarize measurement rounds without treating requests as independent repeats."""
import argparse
from collections import defaultdict
import csv
import json
import math
import os
from pathlib import Path
import statistics


def summarize(result, profile_aliases=None):
    profile_aliases = profile_aliases or {}
    groups = defaultdict(list)
    for run in result['runs']:
        profile = profile_aliases.get(run['profile'], run['profile'])
        groups[(profile, run['workload'], run['concurrency'], run['mode'])].append(run)
    rows = []
    for (profile, workload, qd, mode), runs in sorted(groups.items()):
        rates = [run['ops_per_second'] for run in runs]
        network = [sum(v['bytes'] for v in run['network'].values()) / run['operations']
                   for run in runs]
        osd_cpu = [sum(v['cpu_seconds'] for k, v in run['process'].items() if k != 'client')
                   / run['operations'] for run in runs]
        client_network = [run['network']['10.77.0.100']['bytes'] / run['operations'] for run in runs]
        osd_network = [sum(v['bytes'] for k, v in run['network'].items() if k != '10.77.0.100')
                       / run['operations'] for run in runs]
        rows.append({'profile': profile, 'workload': workload, 'qd': qd, 'mode': mode,
            'repeats': len(runs), 'ops_s_median': statistics.median(rates),
            'ops_s_min': min(rates), 'ops_s_max': max(rates),
            'p50_ms_median': statistics.median(run['latency_ms']['p50'] for run in runs),
            'p95_ms_median': statistics.median(run['latency_ms']['p95'] for run in runs),
            'p99_ms_median': statistics.median(run['latency_ms']['p99'] for run in runs),
            'network_bytes_per_op': statistics.median(network),
            'client_ingress_bytes_per_op': statistics.median(client_network),
            'osd_ingress_bytes_per_op': statistics.median(osd_network),
            'osd_cpu_ms_per_op': statistics.median(osd_cpu) * 1000,
            'min_samples_per_repeat': min(run['operations'] for run in runs)})
    return rows


def report(directory):
    result = json.loads((directory / 'results.json').read_text())
    dataset = json.loads((directory / 'dataset.json').read_text())
    audit_path = directory / 'profile-audit.json'
    audit = json.loads(audit_path.read_text()) if audit_path.exists() else {}
    rows = summarize(result, audit.get('aliases'))
    with (directory / 'summary.csv').open('w') as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    total = sum(item['bytes'] for item in dataset.values())
    lines = ['# Weave benefit pilot', '',
        f"PGs per pool: {result['parameters']['pgs']}; objects: {len(dataset)}; "
        f"Parquet payload: {total / 2**20:.2f} MiB; packed members: {len(result['packed_members'])}.", '',
        'Numbers are medians across repeated rounds in the same cluster; min/max show round variability. '
        'Single-machine, warm-cache measurements under simulated network conditions.', '',
        '| Network | Workload | QD | Mode | ops/s median [min, max] | p50 ms | p95 ms | Network KiB/op |',
        '|---|---|---:|---|---:|---:|---:|---:|']
    for row in rows:
        lines.append(f"| {row['profile']} | {row['workload']} | {row['qd']} | {row['mode']} | "
            f"{row['ops_s_median']:.1f} [{row['ops_s_min']:.1f}, {row['ops_s_max']:.1f}] | "
            f"{row['p50_ms_median']:.2f} | {row['p95_ms_median']:.2f} | "
            f"{row['network_bytes_per_op'] / 1024:.1f} |")
    before = sum(v['num_objects'] for v in result['before_pack'].values())
    after = sum(v['num_objects'] for v in result['after_pack'].values())
    padding = sum(v['num_bytes'] for v in result['after_pack'].values()) / total - 1
    lines += ['', '## Conversion and unfavorable cases', '',
        f"Packing under rtt2: {result['packing']['seconds']:.3f} seconds; "
        f'physical RADOS objects {before} -> {after}; Volume payload padding {padding:.2%}. '
        'This object count is not a measured disk-capacity saving.', '']
    if audit:
        lines += ['Profile audit: ' + audit['note'], '']
    for pool, value in result.get('overwrite', {}).items():
        times = [v * 1000 for v in value['latencies_seconds']]
        lines.append(f"- One-byte overwrite, {pool}: n={len(times)}, median={statistics.median(times):.2f} ms, "
                     f'range=[{min(times):.2f}, {max(times):.2f}] ms. One member per distinct original Volume.')
    lines += ['', '## Serial wall-clock amortization estimate', '',
        'Estimated complete-partition scan times use QD=1 throughput. Packing and reads '
        'use the same rtt2 profile. This excludes later reclamation, CPU opportunity cost '
        'and production interference; it is not a TCO claim.', '']
    for workload in sorted({row['workload'] for row in rows}):
        selected = {row['mode']: row for row in rows if row['profile'] == 'rtt2'
                    and row['qd'] == 1 and row['workload'] == workload}
        if 'weave-direct' not in selected:
            continue
        for base in ['native-client', 'native-pushdown']:
            if base not in selected:
                continue
            saved = len(dataset) * (1 / selected[base]['ops_s_median']
                                   - 1 / selected['weave-direct']['ops_s_median'])
            payoff = math.ceil(result['packing']['seconds'] / saved) if saved > 0 else None
            lines.append(f'- {workload}, {base} -> weave-direct: estimated saving '
                         f'{saved:.3f} s/partition; amortization scans: {payoff if payoff else "no positive saving"}.')
    lines += ['', '## Limits', '',
        '- Each OSD shares the same host CPU, RAM, and disk. Only network conditions are emulated.',
        '- No production tail-latency claim: request samples may be small and the driver is closed-loop.',
        '- The native-client control fetches full objects; no mature Parquet range-read engine is included.',
        '- qdisc bytes include protocol/ACK/control traffic; /proc I/O is process accounting.',
        '- Both pools use this branch and identical 4+2 EC; no upstream-Ceph comparison or redundancy reduction.',
        '- Query results and unaffected objects after overwrite were checked; this is not a fault-injection run.', '']
    (directory / 'report.md').write_text('\n'.join(lines))
    plot(directory, rows)


def plot(directory, rows):
    os.environ.setdefault('MPLCONFIGDIR', '/tmp/weave-benchmark-matplotlib')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    profiles = sorted({row['profile'] for row in rows})
    workloads = sorted({row['workload'] for row in rows})
    qds = sorted({row['qd'] for row in rows})
    modes = ['native-client', 'native-pushdown', 'weave-primary', 'weave-direct']
    colors = ['#64748b', '#94a3b8', '#0d9488', '#2563eb']
    fig, axes = plt.subplots(len(workloads), len(qds), figsize=(6 * len(qds), 3.7 * len(workloads)), squeeze=False)
    for wi, workload in enumerate(workloads):
        for qi, qd in enumerate(qds):
            ax = axes[wi, qi]
            selected = {(row['profile'], row['mode']): row for row in rows
                        if row['workload'] == workload and row['qd'] == qd}
            active = [mode for mode in modes if any((p, mode) in selected for p in profiles)]
            x, width = np.arange(len(profiles)), .8 / len(active)
            for mi, mode in enumerate(active):
                values = [selected[p, mode] for p in profiles]
                medians = np.array([v['ops_s_median'] for v in values])
                errors = np.array([[v['ops_s_median'] - v['ops_s_min'] for v in values],
                                   [v['ops_s_max'] - v['ops_s_median'] for v in values]])
                ax.bar(x - .4 + width * (mi + .5), medians, width, label=mode,
                       color=colors[modes.index(mode)], yerr=errors, capsize=3)
            ax.set_xticks(x, profiles)
            ax.set_title(f'{workload} | concurrency {qd}')
            ax.set_ylabel('Completed objects / second')
            ax.grid(axis='y', alpha=.2)
            ax.set_axisbelow(True)
    handles, labels = axes[-1, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(.5, .92), ncol=4, fontsize=9)
    fig.suptitle('Weave pilot: median throughput, error bars = repeat min/max\nSingle host, warm cache, simulated network', fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, .87))
    fig.savefig(directory / 'throughput.png', dpi=160)
    plt.close(fig)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    report(parser.parse_args().directory)
