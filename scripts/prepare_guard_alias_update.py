"""Offline preparation only: explicit snapshot in, proposed state out. No live defaults."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import hermes_bootstrap  # noqa
from agent.backend_inventory_update import add_verified_alias

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--snapshot', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
p.add_argument('--expected-sha256', required=True)
a = p.parse_args()
raw = a.snapshot.read_bytes()
if hashlib.sha256(raw).hexdigest() != a.expected_sha256:
    raise SystemExit('Snapshot changed; stop and re-review')
value = add_verified_alias(json.loads(raw), owner='spark-pair',
                           canonical='http://spark-1.hef.run:8000', alias='http://spark-1:8000')
with a.output.open('x') as f:
    json.dump(value, f, indent=2, sort_keys=True)
    f.write('\n')
print('Proposed inventory written; not applied')
