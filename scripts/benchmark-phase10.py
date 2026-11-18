"""Measure the scanned Compose runtime: 60s warm-up, 300s measurement, 20 users.

Tokens stay in memory. Fixtures belong only to the isolated Phase 10 database.
No thresholds are relaxed relative to the Java system benchmark.
"""
import concurrent.futures
import ctypes
import json
import math
import os
import platform
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
import secrets
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / os.getenv('PHASE10_PERFORMANCE_DIR', 'docs/evidence/phase10/compose-performance-tuned')
assert OUT.resolve().is_relative_to((ROOT / 'docs/evidence/phase10').resolve())
PROJECT = os.getenv('PHASE10_COMPOSE_PROJECT', 'orderflow-phase10-final')
assert PROJECT.startswith('orderflow-phase10'), 'Use an isolated verification project'
COMPOSE = ['docker', 'compose', '--env-file', '.env', '--profile', 'compose', '-p', PROJECT]
BASE = 'http://127.0.0.1:' + os.getenv('GATEWAY_PORT', '18080')
SERVICES = ['auth-service', 'order-service', 'inventory-service', 'notification-service', 'api-gateway']


def run(args, text=None):
    return subprocess.run(args, cwd=ROOT, input=text, text=True, capture_output=True, check=True, timeout=120).stdout.strip()


def sql(query):
    return run(COMPOSE + ['exec', '-T', 'postgres', 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                          '-U', 'orderflow', '-d', 'orderflow_db', '-At'], query)


def wait(check, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(.5)
    raise AssertionError('Business reconciliation deadline expired')


def percentiles(values):
    values = sorted(values)
    assert values, 'No latency samples'
    return {name: values[math.ceil(len(values) * q) - 1]
            for name, q in [('p50', .50), ('p95', .95), ('p99', .99)]}


def host_memory():
    if os.name == 'nt':
        class Memory(ctypes.Structure):
            _fields_ = [('length', ctypes.c_ulong), ('load', ctypes.c_ulong)] + [
                (name, ctypes.c_ulonglong) for name in
                ['total', 'available', 'pageTotal', 'pageAvailable', 'virtualTotal', 'virtualAvailable', 'extended']]
        memory = Memory()
        memory.length = ctypes.sizeof(memory)
        assert ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(memory))
        return memory.total
    return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    assert not (OUT / 'performance.json').exists(), 'Preserve the prior report before another run'
    report = {'result': 'FAIL', 'startedAt': datetime.now(timezone.utc).isoformat(),
              'warmupSeconds': 60, 'measuredSeconds': 300, 'users': 20,
              'scheduledRequestsPerSecond': 10, 'composeProject': PROJECT}
    try:
        containers = []
        for service in SERVICES:
            cid = run(COMPOSE + ['ps', '-q', service])
            item = json.loads(run(['docker', 'inspect', cid]))[0]
            assert item['State']['Health']['Status'] == 'healthy', service
            scan = json.loads((OUT.parent / f'{service}-scan.json').read_text(encoding='utf-8-sig'))
            assert item['Image'] == scan['Metadata']['ImageID'], 'Runtime must match scanned image'
            containers.append({'service': service, 'imageId': item['Image'],
                               'memoryBytes': item['HostConfig']['Memory'],
                               'nanoCpus': item['HostConfig']['NanoCpus'],
                               'javaOptions': next(e for e in item['Config']['Env'] if e.startswith('JAVA_TOOL_OPTIONS='))})
        report['postgresSettings'] = json.loads(sql("SELECT json_object_agg(name,setting) FROM pg_settings WHERE name IN ('fsync','synchronous_commit','checkpoint_timeout','checkpoint_flush_after','bgwriter_flush_after')"))
        assert report['postgresSettings']['fsync']=='on' and report['postgresSettings']['synchronous_commit']=='on'
        report['environment'] = {'runtime': 'Docker Compose application images', 'hostOs': platform.platform(),
                                 'hostProcessors': os.cpu_count(), 'hostMemoryBytes': host_memory(),
                                 'dockerMemoryBytes': int(run(['docker', 'info', '--format', '{{.MemTotal}}'])),
                                 'loadDriver': platform.python_version(), 'containers': containers}
        product = str(uuid.uuid4())
        run_id = uuid.uuid4().hex
        users = [{'id': str(uuid.uuid4()), 'email': f'phase10-{run_id}-{i}@benchmark.test'} for i in range(20)]
        password = secrets.token_hex(32)
        statements = ['CREATE EXTENSION IF NOT EXISTS pgcrypto;']
        for user in users:
            statements += [f"INSERT INTO auth_schema.users(id,email,password_hash) VALUES ('{user['id']}','{user['email']}',crypt('{password}',gen_salt('bf',10)));",
                           f"INSERT INTO auth_schema.user_roles(user_id,role) VALUES ('{user['id']}','CUSTOMER');"]
        statements.append(f"INSERT INTO inventory_schema.inventory(product_id,product_name,quantity_available) VALUES ('{product}','Isolated Phase 10 benchmark',10000);")
        sql('\n'.join(statements))
        (OUT / 'fixture.json').write_text(json.dumps({'productId': product, 'customers': users, 'initialStock': 10000}, indent=2))
        # Login through Auth on the internal network, as in the Java benchmark.
        # The frontend image already provides Node/fetch; no extra image or public Auth port is needed.
        login_script = 'const users=' + json.dumps(users) + ';const password=' + json.dumps(password) + ';' + """
        const tokens=[];
        for (const user of users) {
          const r=await fetch('http://auth-service:8081/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:user.email,password})});
          if(r.status!==200) throw new Error('Fixture login returned '+r.status);
          const body=await r.json(); if(!body.accessToken) throw new Error('Missing access token');
          tokens.push(body.accessToken);
        }
        process.stdout.write(JSON.stringify(tokens));
        """
        tokens = json.loads(run(COMPOSE + ['exec', '-T', 'frontend', 'node', '--input-type=module', '-'], login_script))
        assert len(tokens) == 20
        samples, errors = [], []
        lock = threading.Lock()
        payload = json.dumps({'productId': product, 'quantity': 1}).encode()
        start = time.monotonic()
        report['loadStartedAt'] = datetime.now(timezone.utc).isoformat()
        checkpoint = {}
        def checkpoint_under_load():
            before = time.monotonic()
            checkpoint['startedSeconds'] = before - start
            try:
                sql('CHECKPOINT;')
                checkpoint['result'] = 'PASS'
            except Exception as error:
                checkpoint.update(result='FAIL', errorType=type(error).__name__)
            checkpoint['durationMs'] = (time.monotonic() - before) * 1000
        # Reproduce the observed I/O pressure inside the measured window, rather
        # than accidentally avoiding it by restarting PostgreSQL's checkpoint timer.
        checkpoint_timer = threading.Timer(120, checkpoint_under_load)
        checkpoint_timer.start()
        with (OUT / 'load-samples.jsonl').open('w', encoding='utf-8', buffering=1) as evidence:
            def user_work(index):
                sequence = 0
                while True:
                    scheduled = start + index * .1 + sequence * 2
                    if scheduled >= start + 360:
                        return
                    time.sleep(max(0, scheduled - time.monotonic()))
                    before = time.monotonic()
                    if before >= start + 360:
                        return
                    request = urllib.request.Request(BASE + '/api/orders', data=payload, method='POST', headers={
                        'Content-Type': 'application/json', 'Authorization': 'Bearer ' + tokens[index],
                        'Idempotency-Key': str(uuid.uuid4()), 'X-Correlation-Id': str(uuid.uuid4())})
                    try:
                        with urllib.request.urlopen(request, timeout=15) as response:
                            status, body = response.status, json.load(response)
                        elapsed = (time.monotonic() - before) * 1000
                        order_id = str(uuid.UUID(body['id']))
                        sample = {'user': index, 'measured': before >= start + 60, 'httpMs': elapsed,
                                  'status': status, 'orderId': order_id, 'startedSeconds': before - start,
                                  'scheduleDelayMs': (before - scheduled) * 1000}
                        with lock:
                            samples.append(sample)
                            evidence.write(json.dumps(sample) + '\n')
                            if status != 201:
                                errors.append('HTTP_' + str(status))
                    except Exception as error:
                        detail = {'errorType': type(error).__name__}
                        if isinstance(error, urllib.error.HTTPError):
                            detail['status'] = error.code
                            try:
                                detail['code'] = json.load(error).get('code')
                            except (ValueError, OSError):
                                pass
                        with lock:
                            errors.append(type(error).__name__)
                            evidence.write(json.dumps({'user': index, **detail, 'startedSeconds': before - start}) + '\n')
                    sequence += 1
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
                list(pool.map(user_work, range(20)))
        checkpoint_timer.join(timeout=120)
        report['checkpointUnderLoad'] = checkpoint
        measured = [s for s in samples if s['measured']]
        report.update({'measuredRequests': len(measured), 'totalRequests': len(samples), 'errors': errors,
                       'throughputPerSecond': len(measured) / 300,
                       'httpMilliseconds': percentiles([s['httpMs'] for s in measured]),
                       'scheduleDelayMilliseconds': percentiles([s['scheduleDelayMs'] for s in measured])})
        wait(lambda: sql(f"SELECT count(*) FROM order_schema.orders WHERE product_id='{product}' AND status='PENDING'") == '0')
        rows = json.loads(sql(f"SELECT COALESCE(json_agg(json_build_object('orderId',o.id,'confirmationMs',EXTRACT(EPOCH FROM h.recorded_at-o.created_at)*1000)),'[]'::json) FROM order_schema.orders o JOIN order_schema.order_history h ON h.order_id=o.id AND h.new_status='CONFIRMED' WHERE o.product_id='{product}'"))
        confirmations = {r['orderId']: float(r['confirmationMs']) for r in rows}
        report['confirmationMilliseconds'] = percentiles([confirmations[s['orderId']] for s in measured])
        orders = int(sql(f"SELECT count(*) FROM order_schema.orders WHERE product_id='{product}'"))
        notification_query = f"SELECT count(*) FROM notification_schema.notifications n JOIN order_schema.orders o ON o.id=n.order_id WHERE o.product_id='{product}'"
        wait(lambda: int(sql(notification_query)) == 2 * orders)
        invariant = {
            'orders': orders, 'confirmed': len(rows), 'initialStock': 10000,
            'reservations': int(sql(f"SELECT count(*) FROM inventory_schema.inventory_reservations WHERE product_id='{product}'")),
            'finalStock': int(sql(f"SELECT quantity_available FROM inventory_schema.inventory WHERE product_id='{product}'")),
            'notifications': int(sql(notification_query)),
            'publishedOrderEvents': int(sql(f"SELECT count(*) FROM order_schema.outbox_events e JOIN order_schema.orders o ON o.id=e.aggregate_id WHERE o.product_id='{product}' AND e.status='PUBLISHED'")),
            'duplicateBusinessEvents': int(sql(f"SELECT count(*) FROM (SELECT aggregate_id,event_type FROM order_schema.outbox_events WHERE aggregate_id IN (SELECT id FROM order_schema.orders WHERE product_id='{product}') GROUP BY aggregate_id,event_type HAVING count(*)<>1) d"))}
        report['invariants'] = invariant
        passed = (checkpoint.get('result') == 'PASS' and not errors and len(measured) == 3000 and len(samples) == 3600
                  and report['httpMilliseconds']['p95'] < 500 and report['confirmationMilliseconds']['p95'] < 5000
                  and orders == len(samples) == len(confirmations) == len(rows) == invariant['reservations']
                  and len({s['orderId'] for s in samples}) == orders
                  and invariant['finalStock'] == 10000 - orders and invariant['duplicateBusinessEvents'] == 0
                  and invariant['notifications'] == invariant['publishedOrderEvents'] == 2 * orders)
        report['result'] = 'PASS' if passed else 'FAIL'
        (OUT / 'load-samples.json').write_text(json.dumps(samples), encoding='utf-8')
    except Exception as error:
        report['failure'] = {'type': type(error).__name__, 'message': str(error)}
    finally:
        report['endedAt'] = datetime.now(timezone.utc).isoformat()
        (OUT / 'performance.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps({k: report.get(k) for k in ['result', 'measuredRequests', 'httpMilliseconds', 'confirmationMilliseconds', 'invariants', 'failure']}, indent=2))
    return 0 if report['result'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
