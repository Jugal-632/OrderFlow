import {test,expect} from '@playwright/test';
import {readFile} from 'node:fs/promises';
const id='11111111-1111-4111-8111-111111111111',product='22222222-2222-4222-8222-222222222222',letter='33333333-3333-4333-8333-333333333333';
const now='2026-10-27T12:00:00Z';
const component={status:'AVAILABLE',lastSuccessfulAt:now,data:{readiness:'UP',dependencies:{db:'UP'},outbox:{pending:0,blocked:0,oldest_pending_seconds:0},unresolvedDeadLetters:0}};
async function setup(page,{customer=false}={}){
  const seen=[],keys=[];let replayCalls=0,status='CONFIRMED',outage=false;
  const evidence=JSON.parse(await readFile('../order-service/src/main/resources/evidence/test-runs.json','utf8'));
  await page.route('**/*',async route=>{
    const req=route.request(),url=new URL(req.url()),p=url.pathname;if(!p.startsWith('/api/')&&!p.startsWith('/auth/'))return route.continue();
    seen.push(p);let data;let code=200;
    if(p==='/auth/login')data={accessToken:'test-memory-token',roles:[customer?'CUSTOMER':'ADMIN'],userId:id};
    else if(p==='/auth/logout')data={};
    else if(p==='/api/dashboard/overview/stats'){if(outage){code=503;data={code:'DEPENDENCY_UNAVAILABLE'};}else data={counts:{PENDING:1,CONFIRMED:1,FAILED:0,CANCEL_PENDING:0,CANCELLED:0},confirmationLatency:{mean_seconds:0.25},order:component.data,services:{inventory:component,notification:component}};}
    else if(p==='/api/orders')data=[{id,status,quantity:2,version:2,createdAt:now}];
    else if(p===`/api/orders/${id}/cancel`){status='CANCEL_PENDING';code=202;data={status};}
    else if(p===`/api/orders/${id}`)data={id,customerId:id,status,version:status==='CANCEL_PENDING'?3:2,timeline:[{version:1,status:'PENDING',recordedAt:now,correlationId:id},{version:2,previousStatus:'PENDING',status:'CONFIRMED',recordedAt:now,correlationId:id},...(status==='CANCEL_PENDING'?[{version:3,previousStatus:'CONFIRMED',status,recordedAt:now,correlationId:id}]:[])]};
    else if(p==='/api/notifications')data=[{event_type:'ORDER_CONFIRMED',order_version:2,recorded_at:now,message:'Order confirmed'}];
    else if(p==='/api/inventory')data=[{product_id:product,product_name:'Test product',quantity_available:48,updated_at:now}];
    else if(p===`/api/inventory/${product}/reservations`)data=[{order_id:id,quantity:2,status:'RESERVED',reserved_at:now}];
    else if(p==='/api/dashboard/services/health')data={order:component,inventory:{...component,status:'STALE'},notification:{status:'UNAVAILABLE',data:null,lastSuccessfulAt:null}};
    else if(p==='/api/dashboard/kafka/topics')data={status:'AVAILABLE',data:[{topic:'inventory.reserved',partition:0,group:'order-service',logEndOffset:3,committedOffset:null,lag:null,status:'UNINITIALIZED',leader:1}]};
    else if(p===`/api/ops/order/dead-letters/${letter}/replay`){keys.push(req.headers()['idempotency-key']);replayCalls++;if(replayCalls===1){code=503;data={code:'RETRY_LATER'};}else{code=202;data={status:'QUEUED'};}}
    else if(p===`/api/ops/order/dead-letters/${letter}`)data={deadLetter:{id:letter,status:replayCalls>1?'REPLAY_QUEUED':'OPEN',source_topic:'inventory.reserved',source_partition:1,source_offset:37,dlt_topic:'inventory.reserved.order-service.DLT',dlt_partition:1,dlt_offset:0,raw_payload:'{"eventId":"immutable"}',sanitized_error:'Dependency unavailable'},replays:replayCalls>1?[{id,requested_by:id,reason:'Dependency repaired',requested_at:now,result:'QUEUED'}]:[]};
    else if(p==='/api/ops/order/dead-letters')data=[{id:letter,status:'OPEN',source_topic:'inventory.reserved',source_partition:1,source_offset:37,replay_attempts:0,last_seen_at:now}];
    else if(p==='/api/dashboard/test-runs')data=evidence;
    else throw new Error('Unexpected endpoint '+p);
    await route.fulfill({status:code,contentType:'application/json',body:JSON.stringify(data)});
  });
  await page.goto('/');await page.getByLabel('Email').fill('admin@example.test');await page.getByLabel('Password',{exact:true}).fill('test-password');await page.getByRole('button',{name:'Sign in'}).click();
  return {seen,keys,outage:()=>{outage=true;}};
}
test('six pages consume API contracts; persisted timeline, evidence and outage states render',async({page})=>{
  const {seen,outage}=await setup(page);await expect(page.getByText('Total orders',{exact:true})).toBeVisible();await page.screenshot({path:'../docs/evidence/phase8-overview.png',fullPage:true});
  outage();await page.getByRole('button',{name:'Refresh data'}).click();await expect(page.getByRole('alert')).toContainText('Stale data');await expect(page.getByText('Total orders',{exact:true})).toBeVisible();
  await page.locator('[data-page="1"]').click();await page.locator('[data-order]').click();await expect(page.getByText('Persisted lifecycle')).toBeVisible();await expect(page.getByText('ORDER_CONFIRMED',{exact:true})).toBeVisible();
  await page.getByRole('button',{name:'Request cancellation'}).click();await expect(page.locator('.badge').filter({hasText:/^CANCEL_PENDING$/}).first()).toBeVisible();await expect(page.locator('.badge').filter({hasText:/^CANCELLED$/})).toHaveCount(0);
  await page.locator('[data-page="2"]').click();await page.locator('[data-product]').click();await expect(page.getByText('RESERVED',{exact:true})).toBeVisible();
  await page.locator('[data-page="3"]').click();await expect(page.getByText('STALE',{exact:true})).toBeVisible();await expect(page.getByText('UNAVAILABLE',{exact:true})).toBeVisible();
  await page.locator('[data-page="4"]').click();await expect(page.getByText('UNINITIALIZED',{exact:true})).toBeVisible();
  await page.locator('[data-page="5"]').click();await expect(page.getByText(/phase7-test-summary.json/)).toBeVisible();
  for(const endpoint of ['/api/orders','/api/inventory','/api/dashboard/overview/stats','/api/dashboard/services/health','/api/dashboard/kafka/topics','/api/dashboard/test-runs'])expect(seen).toContain(endpoint);
  expect(await page.evaluate(()=>localStorage.length+sessionStorage.length)).toBe(0);await page.reload();await expect(page.getByRole('button',{name:'Sign in'})).toBeVisible();
});
test('replay retains action key after failure and mobile layout avoids page overflow',async({page})=>{
  const {keys}=await setup(page);await page.locator('[data-page="4"]').click();await page.locator('[data-letter]').click();await page.getByLabel('Reason for replay').fill('Dependency repaired');
  await page.getByRole('button',{name:'Queue audited replay'}).click();await expect(page.getByRole('alert')).toContainText('RETRY_LATER');
  await page.getByRole('button',{name:'Queue audited replay'}).click();await expect(page.locator('.badge').filter({hasText:/^REPLAY_QUEUED$/})).toBeVisible();expect(keys).toHaveLength(2);expect(keys[0]).toBe(keys[1]);
  await page.setViewportSize({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);await page.screenshot({path:'../docs/evidence/phase8-mobile.png',fullPage:true});
});
test('customer login cannot open operational pages',async({page})=>{await setup(page,{customer:true});await expect(page.getByRole('alert')).toContainText('requires an ADMIN');await expect(page.locator('nav')).toHaveCount(0);});

