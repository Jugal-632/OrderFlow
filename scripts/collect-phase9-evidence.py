"""Fail-closed evidence assembly from the completed Phase 9 reactor and full-duration benchmark."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
import hashlib, json, shutil, subprocess, sys, zipfile, re
import xml.etree.ElementTree as ET

root=Path(__file__).resolve().parent.parent
evidence=root/'docs/evidence'
full_log=(evidence/'phase9-verify.log').read_text(encoding='utf-8-sig')
verification_logs=['phase9-verify.log']
if '[INFO] BUILD SUCCESS' not in full_log:
    # Keep the failed full run visible. Only its explicitly identified interrupted
    # crash test can be superseded, and the complete rerun suite must pass below.
    # Filter out Maven retry-hint lines (e.g. 'mvn <args> -rf :module') – only actual test failure lines count.
    failed=[f for f in re.findall(r'^\[ERROR\]   ([^\r\n]+)',full_log,re.MULTILINE) if not f.strip().startswith('mvn ')]
    if len(failed)!=1 or not failed[0].startswith('Phase5IT.realProcessCrashAfterCommitRecoversSavedResponse:'):
        raise SystemExit('The full run has unresolved failures.')
    recheck=evidence/'phase9-recheck.log'
    if not recheck.exists() or '[INFO] BUILD SUCCESS' not in recheck.read_text(encoding='utf-8-sig'):
        raise SystemExit('The interrupted crash suite has not passed its rerun.')
    old=ET.parse(evidence/'phase9-crash-timeout.xml').getroot()
    current=ET.parse(root/'order-service/target/failsafe-reports/TEST-com.orderflow.order.Phase5IT.xml').getroot()
    # Guard against None timestamps; treat missing timestamp as empty string so comparison succeeds when file is fresher.
    if (current.get('timestamp') or '') <= (old.get('timestamp') or '') and current.get('time','0') == old.get('time','0'):
        raise SystemExit('The crash suite report was not regenerated.')
    verification_logs.append('phase9-recheck.log')
destination=evidence/'phase9-reports';destination.mkdir(exist_ok=True)
cases={};suites=[]
for module in ('event-contracts','security-common','ops-common','api-gateway','auth-service','inventory-service','notification-service','order-service'):
    for kind in ('surefire-reports','failsafe-reports'):
        for source in sorted((root/module/'target'/kind).glob('TEST-*.xml')):
            suite=ET.parse(source).getroot();counts={k:int(suite.get(k,'0')) for k in ('tests','failures','errors','skipped')}
            if any(counts[k] for k in ('failures','errors','skipped')):raise SystemExit(f'Incomplete suite: {source}')
            artifact=destination/(module+'-'+source.name);shutil.copy2(source,artifact)
            suites.append({'suite':suite.get('name'),**counts})
            for case in suite.findall('testcase'):
                ts_raw=suite.get('timestamp');start=datetime.fromisoformat(ts_raw).replace(tzinfo=timezone.utc) if ts_raw else datetime.fromtimestamp(source.stat().st_mtime,tz=timezone.utc)
                end=start+timedelta(seconds=float(suite.get('time','0')))
                cases[case.get('classname')+'#'+case.get('name')]={'startedAt':start.isoformat(),'endedAt':end.isoformat(),'durationSeconds':float(case.get('time','0')),'artifact':str(artifact.relative_to(evidence)).replace('\\','/'),'result':'PASS'}
system=json.loads((evidence/'phase9-system/scenarios.json').read_text())
observed={item['scenarioId']:item for item in system}
if observed.get('IDEMPOTENCY-CONTENTION',{}).get('result')!='PASS':raise SystemExit('The separate real HTTP contention run has not passed.')
perf=json.loads((evidence/'phase9-system/performance.json').read_text())
if perf['result']!='PASS' or perf['warmupSeconds']!=60 or perf['measuredSeconds']!=300 or perf['users']!=20 or perf['measuredRequests']!=3000:
    raise SystemExit('The required full-duration performance gate has not passed.')
commit=subprocess.check_output(['git','-c',f'safe.directory={root.as_posix()}','rev-parse','HEAD'],cwd=root,text=True).strip()
if perf['commit']!=commit:raise SystemExit('Performance evidence was generated for a different commit.')
# The workspace contains uncommitted implementation work; HEAD alone does not identify it.
sources={}
for module in ('event-contracts','security-common','ops-common','api-gateway','auth-service','inventory-service','notification-service','order-service'):
    for source in sorted((root/module/'src').rglob('*')):
        if source.is_file() and 'resources/evidence/' not in source.as_posix():
            sources[source.relative_to(root).as_posix()]=hashlib.sha256(source.read_bytes()).hexdigest()
    source=root/module/'pom.xml'
    sources[source.relative_to(root).as_posix()]=hashlib.sha256(source.read_bytes()).hexdigest()
sources['pom.xml']=hashlib.sha256((root/'pom.xml').read_bytes()).hexdigest()
fingerprint=hashlib.sha256(json.dumps(sources,sort_keys=True).encode()).hexdigest()
(evidence/'phase9-source-manifest.json').write_text(json.dumps({'baseCommit':commit,'workspaceFingerprint':fingerprint,'files':sources},indent=2),encoding='utf-8')
mapping={
 'T02':('order.Phase4CIT','insufficientStockAndMissingProductRemainFailed','Distinct stock/missing-product rejection without stock change'),
 'T03':('inventory.InventoryProcessingIT','reservationAndTenReplaysDeductOnlyOnce','Duplicate reservation deducts once'),
 'T04':('inventory.InventoryProcessingIT','rejectionStaysRejectedAfterRestocking','Original rejection remains immutable after replenishment'),
 'T05':('inventory.InventoryProcessingIT','hundredConcurrentOrdersCannotOversell','100 orders / 50 stock yields 50 reservations and no oversell'),
 'T06':('order.Phase5IT','twentyConcurrentHttpRequestsCreateOneOrderAndOneEvent','20 identical requests create one order and one event'),
 'T07':('order.Phase5IT','changedProductOrQuantityConflictsAndCustomersAreIndependent','Changed request conflicts; customer scopes remain independent'),
 'T08':('order.Phase5IT','realProcessCrashBeforeCommitLeavesNoRows','Pre-commit process halt leaves no order, outbox or request claim'),
 'T10':('inventory.InventoryProcessingIT','processTerminationAfterBrokerAckRepublishesWithoutStockChange','Post-ACK process halt republishes original event without duplicate stock effect'),
 'T14':('order.Phase8IT','replayIsAtomicAndDuplicateDeliveryHasNoBusinessEffects','Replay is audited, atomic and idempotent'),
 'T15':('order.Phase7IT','fullLifecycleTenDuplicatesAndLateReservationsRestoreOnlyOnce','Cancellation restores stock once despite duplicate release'),
 'T16':('order.Phase7IT','kafkaLifecycleWaitsForInventoryRecoveryAndNotifiesOnlyAfterRelease','CANCEL_PENDING persists through Inventory unavailability then completes'),
 'T17':('order.Phase4CIT','contradictoryUnknownAndMismatchedOutcomesAreNotAcknowledged','Contradictory outcome preserves terminal state and remains recoverable'),
 'T18':('order.Phase4CIT','cancelledNotificationCanArriveBeforeEarlierLifecycle','Out-of-order lifecycle notifications retain unique versioned history'),
 'T19':('order.Phase6IT','ownerAndAdminCanReadButOtherCustomerGets404ForOrderAndTimeline','Foreign customer receives 404; admin scope enforced'),
 'T20':('gateway.GatewaySecurityIT','rejectsMissingInvalidExpiredRevokedAndCustomerAdminAccess','Expired and revoked tokens rejected'),
 'T21':('inventory.InventoryProcessingIT','allServiceMigrationsWorkFreshAndOnExplicitV1Baseline','Fresh and populated baseline upgrades preserve application rows')}
extra={
 'T06':[('order.Phase9SystemIT','separateRealHttpIdempotencyContention')],
 'T14':[('order.Phase8IT','concurrentRequestsQueueOnceAndRejectChangedPayload')],
 'T19':[('order.Phase6IT','directServiceAccessRequiresJwtAndAdminEndpointsRejectCustomer')],
 'T20':[('auth.AuthSecurityIT','loginMeLogoutAndRemainingTtl'),('auth.AuthSecurityIT','logoutCannotSucceedDuringRedisOutageAndRecovers'),('gateway.GatewaySecurityIT','redisOutageFailsClosedAndRecoversWithoutRestart')]}
now=datetime.now(timezone.utc).isoformat();matrix=[]
for number in range(1,23):
    scenario=f'T{number:02d}'
    if scenario in mapping:
        suite,method,expect=mapping[scenario];result=cases['com.orderflow.'+suite+'#'+method]
        additional=[cases['com.orderflow.'+s+'#'+m] for s,m in extra.get(scenario,[])]
        matrix.append({'scenarioId':scenario,'commit':commit,'environment':perf['environment'],'startedAt':result['startedAt'],'endedAt':result['endedAt'],'expectedInvariant':expect,'observed':{'assertionResult':'PASS','testDurationSeconds':result['durationSeconds'],'verificationWindow':'containing JUnit suite'},'result':'PASS','artifacts':[result['artifact']]+[r['artifact'] for r in additional]})
    else:
        result=observed[scenario]
        if result['result']!='PASS' or result['commit']!=commit:raise SystemExit(f'Invalid live evidence for {scenario}')
        matrix.append(result)
report={'generatedAt':now,'commit':commit,'workspaceFingerprint':fingerprint,'sourceManifest':'phase9-source-manifest.json','verificationLogs':verification_logs,'result':'PASS','scenarios':matrix,'performanceArtifact':'phase9-system/performance.json'}
(evidence/'phase9-matrix.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
(evidence/'phase9-test-summary.json').write_text(json.dumps(suites,indent=2),encoding='utf-8')
subprocess.run([sys.executable,str(root/'scripts/package-evidence.py')],check=True)
artifacts=[evidence/name for name in ('phase9-matrix.json','phase9-test-summary.json','phase9-source-manifest.json','phase9-verify.log','phase9-recheck.log','phase9-crash-timeout.xml','phase9-verify-contention-failure.log','phase9-contention-failure.xml','phase9-browser-results.json','phase9-evidence.png')]
for directory in (destination,evidence/'phase9-system',root/'docs/runbooks'):
    artifacts.extend(path for path in directory.rglob('*') if path.is_file())
with zipfile.ZipFile(evidence/'phase9-evidence.zip','w',compression=zipfile.ZIP_DEFLATED) as archive:
    for artifact in artifacts:
        if artifact.exists():archive.write(artifact,artifact.relative_to(root).as_posix())
print(f"T01–T22 PASS; {sum(s['tests'] for s in suites)} passing tests. Measured HTTP p95={perf['httpMilliseconds']['p95']:.2f} ms; confirmation p95={perf['confirmationMilliseconds']['p95']:.2f} ms")
