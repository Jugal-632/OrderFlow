"""Real Compose smoke/chaos checks. Run only against the isolated orderflow-phase10 project."""
import concurrent.futures, json, os, subprocess, time, urllib.request, urllib.error, uuid
from pathlib import Path
from local_env import credentials
CONFIG=credentials()

ROOT=Path(__file__).resolve().parent.parent
OUT=ROOT/'docs/evidence/phase10';OUT.mkdir(parents=True,exist_ok=True)
PROJECT=os.getenv('PHASE10_COMPOSE_PROJECT','orderflow-phase10')
assert PROJECT.startswith('orderflow-phase10'), 'Use an isolated Phase 10 project'
COMPOSE=['docker','compose','--env-file','.env','--profile','compose','-p',PROJECT]
SERVICES=['auth-service','order-service','inventory-service','notification-service','api-gateway']
BASE='http://127.0.0.1:'+os.getenv('GATEWAY_PORT','8080')
results=[]
def run(args,input=None):
    return subprocess.run(args,cwd=ROOT,input=input,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True).stdout.strip()
def compose(*args):return run(COMPOSE+list(args))
def sql(query):return run(COMPOSE+['exec','-T','postgres','psql','-X','-v','ON_ERROR_STOP=1','-U','orderflow','-d','orderflow_db','-At'],query)
def wait(check,seconds=240):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        if check():return
        time.sleep(.5) # Poll a condition; never assume readiness after a fixed delay.
    raise AssertionError('Bounded condition did not become true')
def healthy(service):
    cid=compose('ps','-q',service)
    return bool(cid) and run(['docker','inspect','--format','{{.State.Health.Status}}',cid])=='healthy'
def record(name,observed):
    results.append({'scenario':name,'result':'PASS','observed':observed})
    (OUT/'compose-checks.json').write_text(json.dumps({'composeProject':PROJECT,'results':results},indent=2))
def request(method,path,body=None,key=None,token=None):
    headers={'Content-Type':'application/json'}
    if token:headers['Authorization']='Bearer '+token
    if key:headers['Idempotency-Key']=key
    req=urllib.request.Request(BASE+path,data=None if body is None else json.dumps(body).encode(),headers=headers,method=method)
    try:
        with urllib.request.urlopen(req,timeout=25) as response:return response.status,json.load(response),dict(response.headers)
    except urllib.error.HTTPError as error:return error.code,json.load(error),dict(error.headers)
def state(order):return sql(f"SELECT status FROM order_schema.orders WHERE id='{order}'")
def pending(schema,order):return sql(f"SELECT count(*) FROM {schema}.outbox_events WHERE aggregate_id='{order}' AND status='PENDING'")=='1'

try:
    image_ids={}
    for service in SERVICES:
        wait(lambda:healthy(service))
        image_ids[service]=run(["docker","inspect","--format","{{.Image}}",compose("ps","-q",service)])
        assert compose('exec','-T',service,'id','-u')=='10001'
        assert compose('exec','-T',service,'id','-g')=='10001'
        compose('exec','-T',service,'sh','-c','test -w /tmp && test -w /app/logs')
    record('non-root-health',{'services':SERVICES,'uid':10001,'gid':10001,'imageIds':image_ids})
    code,login,_=request('POST','/auth/login',{'email':CONFIG['BOOTSTRAP_ADMIN_EMAIL'],'password':CONFIG['BOOTSTRAP_ADMIN_PASSWORD']})
    assert code==200,login
    token=login['accessToken'];product=str(uuid.uuid4())
    sql(f"INSERT INTO inventory_schema.inventory(product_id,product_name,quantity_available) VALUES ('{product}','Phase10 isolated test',50)")
    body={'productId':product,'quantity':2}
    code,created,_=request('POST','/api/orders',body,str(uuid.uuid4()),token);assert code==201,created
    order=created['id'];wait(lambda:state(order)=='CONFIRMED')
    assert sql(f"SELECT quantity_available FROM inventory_schema.inventory WHERE product_id='{product}'")=='48'
    code,cancelled,_=request('POST',f'/api/orders/{order}/cancel',None,str(uuid.uuid4()),token);assert code==202,cancelled
    wait(lambda:state(order)=='CANCELLED')
    assert sql(f"SELECT quantity_available FROM inventory_schema.inventory WHERE product_id='{product}'")=='50'
    assert request('GET','/api/orders/'+order)[0]==401
    code,customer,_=request('POST','/auth/login',{'email':CONFIG['BOOTSTRAP_CUSTOMER_EMAIL'],'password':CONFIG['BOOTSTRAP_CUSTOMER_PASSWORD']});assert code==200
    assert request('GET','/api/orders/'+order,token=customer['accessToken'])[0]==404
    assert request('GET','/api/dashboard/overview/stats',token=customer['accessToken'])[0]==403
    record('smoke-authorization-cancellation',{'orderId':order,'status':'CANCELLED','stock':50})

    # An isolated DB trigger holds the actual create transaction at the write boundary.
    sql("CREATE FUNCTION order_schema.phase10_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(20); RETURN NEW; END $$; CREATE TRIGGER phase10_hold BEFORE INSERT ON order_schema.orders FOR EACH ROW EXECUTE FUNCTION order_schema.phase10_hold();")
    key='phase10-midwrite-'+str(uuid.uuid4())
    try:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            future=pool.submit(request,'POST','/api/orders',body,key,token)
            wait(lambda:sql("SELECT count(*) FROM pg_stat_activity WHERE usename='order_runtime' AND wait_event='PgSleep'")=='1',20)
            compose('kill','-s','SIGKILL','order-service')
            try:future.result(timeout=30)
            except Exception:pass # Client failure is expected; database invariants decide the result.
        assert sql(f"SELECT count(*) FROM order_schema.request_idempotency WHERE idempotency_key='{key}'")=='0'
    finally:
        sql('DROP TRIGGER IF EXISTS phase10_hold ON order_schema.orders; DROP FUNCTION IF EXISTS order_schema.phase10_hold();')
        compose('start','order-service');wait(lambda:healthy('order-service'))
    code,retried,_=request('POST','/api/orders',body,key,token);assert code==201
    wait(lambda:state(retried['id'])=='CONFIRMED')
    record('order-midwrite-kill',{'rolledBack':True,'retryOrder':retried['id']})

    # Stop Inventory first so its outcome cannot race the outage setup.
    compose('stop','inventory-service')
    code,created,_=request('POST','/api/orders',body,str(uuid.uuid4()),token);assert code==201,created
    order=created['id'];assert state(order)=='PENDING'
    compose('start','inventory-service');wait(lambda:healthy('inventory-service'));wait(lambda:state(order)=='CONFIRMED')
    record('inventory-restart',{'orderId':order,'status':'CONFIRMED'})

    # Hold the real relay's session lock while Inventory commits its decision.
    # This changes only the isolated test session, never application code.
    hold=subprocess.Popen(COMPOSE+['exec','-T','-e','PGAPPNAME=phase10-relay-hold','postgres','psql','-X','-v','ON_ERROR_STOP=1','-U','orderflow','-d','orderflow_db'],cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,text=True)
    try:
        hold.stdin.write("SET lock_timeout='20s'; SELECT pg_advisory_lock(5282254311686160975);\n");hold.stdin.flush()
        wait(lambda:sql("SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.application_name='phase10-relay-hold' AND l.locktype='advisory' AND l.granted")=='1',30)
        code,created,_=request('POST','/api/orders',body,str(uuid.uuid4()),token);assert code==201,created
        order=created['id'];wait(lambda:pending('inventory_schema',order),30)
        compose('stop','kafka')
        assert state(order)=='PENDING'
        compose('kill','-s','SIGKILL','inventory-service')
        assert pending('inventory_schema',order)
    finally:
        hold.stdin.close();hold.wait(timeout=30)
        compose('start','kafka');wait(lambda:healthy('kafka'))
        compose('start','inventory-service');wait(lambda:healthy('inventory-service'))
    wait(lambda:state(order)=='CONFIRMED')
    assert sql(f"SELECT count(*) FROM inventory_schema.inventory_reservations WHERE order_id='{order}'")=='1'
    assert sql(f"SELECT quantity_available FROM inventory_schema.inventory WHERE product_id='{product}'")=='44'
    wait(lambda:sql(f"SELECT count(*) FROM notification_schema.notifications n JOIN order_schema.orders o ON o.id=n.order_id WHERE o.product_id='{product}'")=='9')
    record('broker-outage-and-pending-inventory-outbox',{'orderId':order,'status':'CONFIRMED','reservations':1,'finalStock':44,'notifications':9})
    compose('down')
    compose('up','-d','--quiet-pull','--wait','--wait-timeout','600')
    assert state(order)=='CONFIRMED'
    assert sql(f"SELECT quantity_available FROM inventory_schema.inventory WHERE product_id='{product}'")=='44'
    record('compose-recreation-preserves-data',{'status':'CONFIRMED','finalStock':44,'kafkaHealthy':healthy('kafka')})
except Exception as error:
    (OUT/'compose-failure.json').write_text(json.dumps({'error':str(error),'completed':results},indent=2))
    raise
