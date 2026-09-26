#!/usr/bin/env bash
# Start, exercise and stop an isolated Weave vstart cluster; retain its files.
set -euo pipefail
weave_source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
weave_build_dir=$(realpath "${1:-$weave_source_dir/build}")
weave_cluster_dir=$(mktemp -d "$weave_build_dir/weave-vstart.XXXXXX")
printf 'Weave test cluster: %s\n' "$weave_cluster_dir"
ln -s "$weave_build_dir/bin" "$weave_cluster_dir/bin"
ln -s "$weave_build_dir/lib" "$weave_cluster_dir/lib"
ln -s "$weave_build_dir/CMakeCache.txt" "$weave_cluster_dir/CMakeCache.txt"
cd "$weave_cluster_dir"

export VSTART_DEST="$weave_cluster_dir"
export MON=1 OSD=3 MGR=1 MDS=0 RGW=0 FS=0
export CEPH_PORT="${CEPH_PORT:-6871}"
export PYTHONPATH="$weave_build_dir/lib/cython_modules/lib.3:$weave_source_dir/src/pybind:$weave_source_dir/src/python-common"
export LD_LIBRARY_PATH="$weave_build_dir/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

cleanup() {
  python3 - "$weave_cluster_dir" <<'PY'
import os
from pathlib import Path
import signal
import sys

for path in (Path(sys.argv[1]) / 'out').glob('*.pid'):
    try:
        pid = int(path.read_text())
        if Path(f'/proc/{pid}/comm').read_text().strip().startswith('ceph-'):
            os.kill(pid, signal.SIGTERM)
    except (OSError, ValueError):
        pass
PY
}
trap cleanup EXIT

"$weave_source_dir/src/vstart.sh" -n -l --nolockdep --without-dashboard \
  -o 'bluestore block size = 1073741824' \
  -o 'osd memory target = 536870912' \
  -o 'osd weave quiet period = 1' \
  -o 'osd weave scan interval = 1' \
  -o 'osd weave min object size = 1'
python3 "$weave_source_dir/src/test/weave/vstart_direct_read.py" \
  --build-dir "$weave_build_dir" --cluster-dir "$weave_cluster_dir" --stop-target
