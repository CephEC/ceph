#!/usr/bin/env bash
set -euo pipefail
weave_source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.." && pwd)
weave_build_dir=$(realpath "${1:?usage: concurrent_pack_reads.sh BUILD WORK_DIR}")
weave_work_dir=${2:?work directory is required}
shift 2
export LD_LIBRARY_PATH="$weave_build_dir/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export PYTHONPATH="$weave_build_dir/lib/cython_modules/lib.3:$weave_source_dir/src/pybind:$weave_source_dir/src/python-common${PYTHONPATH:+:$PYTHONPATH}"
exec /usr/bin/python3.10 "$weave_source_dir/src/test/weave/concurrent_pack_reads.py" \
  --build-dir "$weave_build_dir" --work-dir "$weave_work_dir" "$@"
