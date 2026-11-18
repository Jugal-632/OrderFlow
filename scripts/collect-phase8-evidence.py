"""Collect actual completed Phase 8 verification reports, then package them."""
from pathlib import Path
import json
import subprocess
import sys
import xml.etree.ElementTree as ET

root = Path(__file__).resolve().parent.parent
log = root / 'docs/evidence/phase8-verify.log'
if '[INFO] BUILD SUCCESS' not in log.read_text(encoding='utf-8-sig'):
    raise SystemExit('Complete the affected-service Maven verify run first.')
reports = []
for module in ('event-contracts', 'security-common', 'api-gateway', 'notification-service', 'inventory-service', 'order-service'):
    for directory in ('surefire-reports', 'failsafe-reports'):
        reports.extend(sorted((root / module / 'target' / directory).glob('TEST-*.xml')))
reports.append(root / 'docs/evidence/phase8-frontend-unit.xml')
suites = []
for report in reports:
    document = ET.parse(report).getroot()
    entries = [document] if document.tag == 'testsuite' else document.findall('testsuite')
    if not entries and document.findall('testcase'):
        cases = document.findall('testcase')
        if any(case.find(tag) is not None for case in cases for tag in ('failure', 'error', 'skipped')):
            raise SystemExit(f'Unverified frontend tests: {report}')
        suites.append({'suite': 'frontend.ClientPolling', 'tests': len(cases), 'failures': 0, 'errors': 0, 'skipped': 0})
        continue
    if not entries:
        raise SystemExit(f'No tests in report: {report}')
    for suite in entries:
        counts = {key: int(suite.get(key, '0')) for key in ('tests', 'failures', 'errors', 'skipped')}
        if counts['failures'] or counts['errors'] or counts['skipped']:
            raise SystemExit(f'Unverified suite: {report}')
        suites.append({'suite': suite.get('name', report.stem), **counts})
browser = json.loads((root / 'docs/evidence/phase8-browser-results.json').read_text(encoding='utf-8'))
stats = browser['stats']
if stats['unexpected'] or stats['flaky'] or stats['skipped'] or stats['expected'] != 3:
    raise SystemExit('Browser checks must all pass without retries or skips.')
suites.append({'suite': 'frontend.ChromeDashboardContracts', 'tests': stats['expected'], 'failures': 0, 'errors': 0, 'skipped': 0})
if not any(s['suite'] == 'com.orderflow.order.Phase8IT' and s['tests'] >= 7 for s in suites):
    raise SystemExit('Phase 8 integration results are missing.')
target = root / 'docs/evidence/phase8-test-summary.json'
target.write_text(json.dumps(suites, indent=2), encoding='utf-8')
subprocess.run([sys.executable, str(root / 'scripts/package-evidence.py')], check=True)
print(f"Recorded {sum(s['tests'] for s in suites)} passing tests across {len(suites)} suites")
