#!/usr/bin/env bash
set -euo pipefail
weave_source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
weave_build_dir=$(realpath "${1:?usage: benefit_benchmark.sh BUILD UNUSED_WORK_DIR [options]}")
weave_work_dir=${2:?unused work directory required}
shift 2
export LD_LIBRARY_PATH="$weave_build_dir/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PYTHONPATH="$weave_build_dir/lib/cython_modules/lib.3:$weave_source_dir/src/pybind:$weave_source_dir/src/python-common${PYTHONPATH:+:$PYTHONPATH}"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
unset PYTHONOPTIMIZE
export WEAVE_PARENT_NETNS=$(readlink /proc/self/ns/net)
exec unshare --net -- /usr/bin/python3.10 -u "$weave_source_dir/src/test/weave/benchmark_network.py" \
  --build-dir "$weave_build_dir" --work-dir "$weave_work_dir" "$@"
