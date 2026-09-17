#!/usr/bin/env python3
"""Keep native OSD code on Weave facades and policy off native PG internals."""
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
module = root / 'src/osd/weave'
errors = []
for path in (root / 'src/osd').rglob('*'):
    if path.suffix not in ('.cc', '.h') or module in path.parents:
        continue
    text = path.read_text()
    for include in re.findall(r'#include "([^\"]+)"', text):
        if 'weave/detail/' in include:
            # OpRequest owns allocation/destruction, not policy or catalog state.
            if path.name != 'OpRequest.cc' or not include.endswith('/WeaveRequestContext.h'):
                errors.append(f'{path}: internal include {include}')
    if re.search(r'friend\s+class\s+ceph::weave::', text):
        errors.append(f'{path}: native friendship granted to Weave')
for path in module.glob('*.h'):
    if re.search(r'#include ".*detail/', path.read_text()):
        errors.append(f'{path}: public header includes implementation detail')
for name in ('WeaveConversionJob.cc', 'WeavePGControllerImpl.cc', 'WeaveReadRouter.cc'):
    path = module / 'detail' / name
    if re.search(r'#include "(?:osd/(?:PG|PrimaryLogPG|OSD)\.h|osdc/Objecter\.h)"', path.read_text()):
        errors.append(f'{path}: core depends on native PG/OSD/Objecter')
if errors:
    sys.exit('\n'.join(errors))
print('Weave public and native-host boundaries verified')
