#!/usr/bin/env python3
"""Snapshot integration phases for an isolated EC pool.

Seed with aggregation disabled, let the harness pack the objects, then run
mutate and verify (also after restarting every OSD). No cluster configuration
or process management is performed by this client.
"""
import argparse
import json
from pathlib import Path
import rados

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--conf', required=True)
p.add_argument('--pool', required=True)
p.add_argument('--state', required=True, type=Path)
p.add_argument('--self-managed', action='store_true')
p.add_argument('phase', choices=['seed', 'mutate', 'verify'])
a = p.parse_args()
cluster = rados.Rados(conffile=a.conf, conf={'rados_osd_op_timeout': '45', 'admin_socket': ''})
cluster.connect()
io = cluster.open_ioctx(a.pool)

def snapshot(name):
    if a.self_managed:
        sid = io.create_self_managed_snap()
        state['snaps'][name] = sid
        io.set_self_managed_snap_write(sorted(state['snaps'].values(), reverse=True))
    else:
        io.create_snap(name)
        state['snaps'][name] = io.lookup_snap(name).snap_id
    return state['snaps'][name]

def original(i):
    return bytes([65 + i]) * 16384

def check_snapshot(name):
    io.set_read(state['snaps'][name])
    for i in range(16):
        key = 'snap-member-' + str(i)
        assert io.read(key, 20000) == original(i), (name, key, 'data')
        size, mtime = io.stat(key)
        saved = state['objects'][key]
        assert (size, repr(mtime), io.get_last_version()) == tuple(saved), (name, key, 'stat')
        assert io.get_xattr(key, 'snapshot-tag') == key.encode(), (name, key, 'xattr')
    io.set_read(rados.LIBRADOS_SNAP_HEAD)

try:
    if a.phase == 'seed':
        state = {'snaps': {}, 'objects': {}}
        snapshot('weave-absent')
        for i in range(16):
            key = 'snap-member-' + str(i)
            io.write_full(key, original(i))
            io.set_xattr(key, 'snapshot-tag', key.encode())
            size, mtime = io.stat(key)
            state['objects'][key] = [size, repr(mtime), io.get_last_version()]
        snapshot('weave-before')
        check_snapshot('weave-before')
    else:
        state = json.loads(a.state.read_text())
        if a.self_managed:
            io.set_self_managed_snap_write(sorted(state['snaps'].values(), reverse=True))
        if a.phase == 'mutate':
            snapshot('weave-after')
            # Mutate before snapshot reads can materialize the other groups.
            io.write_full('snap-member-0', b'new head contents')
            io.remove_object('snap-member-1')
            io.write('snap-member-3', b'patch', 5)
            io.remove_object('snap-member-4')
            io.write_full('snap-member-4', b'new generation')
        check_snapshot('weave-before')
        check_snapshot('weave-after')
        assert io.read('snap-member-0', 100) == b'new head contents'
        assert io.read('snap-member-3', 20000) == original(3)[:5] + b'patch' + original(3)[10:]
        assert io.read('snap-member-4', 100) == b'new generation'
        try:
            io.read('snap-member-1', 1)
            raise AssertionError('deleted head reappeared')
        except rados.ObjectNotFound:
            pass
    io.set_read(state['snaps']['weave-absent'])
    for key in state['objects']:
        try:
            io.read(key, 1)
            raise AssertionError(('object existed before creation', key))
        except rados.ObjectNotFound:
            pass
    a.state.write_text(json.dumps(state, indent=2))
    print('PASS: snapshot', a.phase, 'self-managed' if a.self_managed else 'pool', flush=True)
finally:
    io.close()
    cluster.shutdown()
