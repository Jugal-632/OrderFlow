"""Summarize generated evidence without converting scan failures into passes."""
from pathlib import Path
from collections import Counter
from datetime import datetime,timezone
import json,hashlib,subprocess
root=Path(__file__).resolve().parent.parent;out=root/'docs/evidence/phase10'
scans=[]
for service in ['auth-service','order-service','inventory-service','notification-service','api-gateway','frontend']:
    path=out/f'{service}-scan.json';document=json.loads(path.read_text(encoding='utf-8-sig'))
    vulnerabilities=[v for result in document.get('Results',[]) for v in result.get('Vulnerabilities',[])]
    secrets=[v for result in document.get('Results',[]) for v in result.get('Secrets',[])]
    counts=Counter(v['Severity'] for v in vulnerabilities)
    scans.append({'service':service,'imageId':document['Metadata']['ImageID'],'severityCounts':dict(counts),'secretFindings':len(secrets),'result':'FAIL' if vulnerabilities or secrets else 'PASS','report':path.name,'criticalPackages':sorted({v['PkgName'] for v in vulnerabilities if v['Severity']=='CRITICAL'})})
checks=json.loads((out/'compose-checks.json').read_text())
assert len(checks['results'])==6 and all(item['result']=='PASS' for item in checks['results'])
runtime=json.loads((out/'runtime-artifacts.json').read_text());assert len(runtime)==5
for artifact in runtime:
    scan=next(s for s in scans if s['service']==artifact['service'])
    assert scan['imageId']==artifact['imageId'], 'Scans and runtime inspection must describe the same image'
    assert checks['results'][0]['observed']['imageIds'][artifact['service']]==artifact['imageId'], 'Live checks must use the scanned image'
regression_path=out/'regression-summary.json'
regression=json.loads(regression_path.read_text())['result'] if regression_path.exists() else 'PENDING_FINAL_BENCHMARK'
report={'regressionVerification':regression,'recordedAt':datetime.now(timezone.utc).isoformat(),'localVerification':'PASS','releaseSecurityGate':'FAIL' if any(s['result']=='FAIL' for s in scans) else 'PASS','hostedCi':'NOT_RUN_NO_REMOTE_CONFIGURED','composeProject':checks['composeProject'],'ports':{'gateway':18080,'frontend':15173,'postgres':25432,'redis':26379,'kafka':19092,'kafkaUi':18090},'checks':checks['results'],'scans':scans,'runtimeArtifacts':runtime}
(out/'summary.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
print(json.dumps({'localVerification':report['localVerification'],'securityGate':report['releaseSecurityGate'],'scans':[{k:v for k,v in s.items() if k in ('service','severityCounts','secretFindings')} for s in scans]},indent=2))
