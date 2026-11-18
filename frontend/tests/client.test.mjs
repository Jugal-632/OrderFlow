import {test} from 'node:test';
import assert from 'node:assert/strict';
import {api,setToken,clearToken,poll} from '../client.js';
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
test('memory token, correlation and retained mutation key; 401 clears credentials',async()=>{
  const original=global.fetch;const calls=[];
  global.fetch=async(path,options)=>{calls.push(options);return new Response(JSON.stringify(calls.length===2?{code:'EXPIRED'}:{ok:true}),{status:calls.length===2?401:200});};
  try{setToken('ephemeral');await api('/api/ops/order/dead-letters/id/replay',{method:'POST',body:{reason:'Repaired'},key:'retry-key'});
    assert.equal(calls[0].headers.Authorization,'Bearer ephemeral');assert.equal(calls[0].headers['Idempotency-Key'],'retry-key');assert.match(calls[0].headers['X-Correlation-Id'],/^[a-f0-9-]{36}$/);
    await assert.rejects(api('/api/orders'),e=>e.status===401);await api('/api/orders');assert.equal(calls[2].headers.Authorization,undefined);
  }finally{clearToken();global.fetch=original;}
});
test('polls never overlap, hide aborts, return refreshes, stop prevents further work',async()=>{
  const doc=new EventTarget();doc.hidden=false;let active=0,max=0,calls=0,aborted=0;
  const polling=poll(signal=>new Promise(resolve=>{calls++;active++;max=Math.max(max,active);let done=false;const finish=()=>{if(!done){done=true;active--;resolve();}};signal.addEventListener('abort',()=>{aborted++;finish();});setTimeout(finish,35);}),{documentRef:doc,delay:20});
  await polling.refresh();await pause(5);assert.equal(calls,1);
  doc.hidden=true;doc.dispatchEvent(new Event('visibilitychange'));await pause(60);assert.equal(calls,1);assert.equal(aborted,1);
  doc.hidden=false;doc.dispatchEvent(new Event('visibilitychange'));await pause(5);assert.equal(calls,2);
  polling.stop();const stopped=calls;await pause(90);assert.equal(calls,stopped);assert.equal(max,1);
});
test('a response error arriving after navigation cannot replace the current page',async()=>{
  const doc=new EventTarget();doc.hidden=false;let reject,errors=0;
  const polling=poll(()=>new Promise((resolve,failure)=>{reject=failure;}),{documentRef:doc,onError:()=>errors++});
  polling.stop();reject(new Error('Old request failed'));await pause(0);assert.equal(errors,0);
});
