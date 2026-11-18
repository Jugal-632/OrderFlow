"""Package existing machine-generated test reports; never manufacture test results."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json

root = Path(__file__).resolve().parent.parent
runs = []
for source in sorted((root / 'docs/evidence').glob('phase*-test-summary.json')):
    raw = source.read_bytes()
    suites = json.loads(raw.decode('utf-8-sig'))
    if not isinstance(suites, list) or not suites:
        raise ValueError(f'Invalid test summary: {source.name}')
    for suite in suites:
        for key in ('tests', 'failures', 'errors', 'skipped'):
            if not isinstance(suite.get(key), int) or suite[key] < 0:
                raise ValueError(f'Invalid test count: {source.name}')
        if suite['failures'] + suite['errors'] + suite['skipped'] > suite['tests']:
            raise ValueError('Inconsistent test counts')
    runs.append({'source': source.name, 'sha256': hashlib.sha256(raw).hexdigest(), 'suites': suites})
target = root / 'order-service/src/main/resources/evidence/test-runs.json'
target.parent.mkdir(parents=True, exist_ok=True)
document={'generatedAt': datetime.now(timezone.utc).isoformat(), 'runs': runs}
matrix=root/'docs/evidence/phase9-matrix.json'
performance=root/'docs/evidence/phase9-system/performance.json'
if matrix.exists() and performance.exists():
    document['verification']={'matrix':json.loads(matrix.read_text()),'performance':json.loads(performance.read_text())}
target.write_text(json.dumps(document, indent=2), encoding='utf-8')
print(f'Packaged {len(runs)} verified source files')
