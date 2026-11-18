import {api,setToken,clearToken,poll} from './client.js';
const root=document.querySelector('#app');
const pages=['Overview','Orders','Inventory','Services','Events & Recovery','Test Evidence'];
const descriptions=['Every order. Every outcome.','Follow the complete order lifecycle.','Available stock and its reservation history.','Dependency health, with the last known state.','Inspect failures. Recover with a recorded reason.','Read-only results from automated verification.'];
const state={page:0,index:0,size:20,status:'',query:'',service:'order',letterStatus:'OPEN',selected:null,detail:null,reason:'',data:null,error:null,updated:null};
let user=null,poller=null,actionPending=false;
const actionKeys=new Map();
const escape=value=>String(value??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const date=value=>value?new Date(value).toLocaleString():'Never';
const badge=value=>`<span class="badge ${['FAIL','FAILED','UNAVAILABLE','OPEN','FORBIDDEN','BLOCKED'].includes(value)?'danger':['STALE','PENDING','CANCEL_PENDING','REPLAY_QUEUED','UNINITIALIZED'].includes(value)?'warning':'good'}">${escape(value)}</span>`;
const table=(headers,rows)=>rows.length?`<div class="table-scroll"><table><thead><tr>${headers.map(h=>`<th>${escape(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>`<tr>${row.map(cell=>`<td>${cell}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`:'<div class="empty">No records match this view.</div>';
const panel=(title,content,extra='')=>`<section class="panel"><div class="panel-heading"><h2>${escape(title)}</h2>${extra}</div>${content}</section>`;
const button=(label,attribute,value)=>`<button class="text-button" ${attribute}="${escape(value)}">${escape(label)}</button>`;
function pagination(){return `<div class="pagination"><button id="previous" ${state.index===0?'disabled':''}>← Previous</button><span>Page ${state.index+1} · ${state.size} per page</span><button id="next" ${Array.isArray(state.data?.items)&&state.data.items.length<state.size?'disabled':''}>Next →</button></div>`;}
function login(message=''){
  poller?.stop();user=null;clearToken();state.data=null;actionKeys.clear();
  root.innerHTML=`<main class="login"><section class="login-story"><div class="brand"><span class="brand-mark">O</span> OrderFlow</div><div><span class="eyebrow">OPERATIONS WORKSPACE</span><h1>Every order,<br>accounted for.</h1><p>Trace the journey. Understand the failure.<br>Recover with confidence.</p></div><small>Order lifecycle & inventory operations</small></section><section class="login-form"><div><span class="eyebrow">ADMIN ACCESS</span><h2>Welcome back</h2><p class="muted">Sign in to your operations workspace.</p><form id="login"><label>Email<input id="email" name="email" type="email" autocomplete="username" required></label><label>Password<input id="password" name="password" type="password" autocomplete="current-password" required></label><p class="error" role="alert">${escape(message)}</p><button class="primary" type="submit">Sign in →</button></form><p class="muted small">Your session ends on reload or token expiry.</p></div></section></main>`;
  root.querySelector('#login').onsubmit=async event=>{event.preventDefault();const submit=event.target.querySelector('button');submit.disabled=true;
    try{const result=await api('/auth/login',{method:'POST',body:{email:root.querySelector('#email').value,password:root.querySelector('#password').value}});
      if(!result.roles.includes('ADMIN')){login('Forbidden: this workspace requires an ADMIN account.');return;}
      setToken(result.accessToken);user=result;state.page=0;state.index=0;state.selected=null;start();
    }catch(error){root.querySelector('.error').textContent=error.message;submit.disabled=false;}
  };
}
function shell(content){
  const focused=document.activeElement?.id,selection=document.activeElement?.selectionStart;
  root.innerHTML=`<div class="workspace"><aside><div class="brand"><span class="brand-mark">O</span> OrderFlow</div><div class="nav-label">WORKSPACE</div><nav>${pages.map((name,i)=>`<button data-page="${i}" class="${state.page===i?'active':''}"><span class="nav-icon">${['◫','≡','▦','◉','↻','✓'][i]}</span>${name}</button>`).join('')}</nav><div class="sidebar-foot"><span class="live-dot"></span> Operational workspace<p>Persisted state. Audited actions.</p></div></aside><main><header><span>Workspace <span class="slash">/</span> ${pages[state.page]}</span><div><span class="role">ADMIN</span><button id="logout" class="text-button">Sign out</button></div></header><div class="content"><div class="page-title"><div><span class="eyebrow">ORDERFLOW / OPERATIONS</span><h1>${pages[state.page]}</h1><p class="muted">${descriptions[state.page]}</p></div><div class="refresh"><span class="live-dot"></span>${state.updated?'Updated '+date(state.updated):'Loading live data…'}<button id="refresh" aria-label="Refresh data">↻</button></div></div>${state.error?`<div class="notice" role="alert"><strong>${state.error.status===403?'Forbidden':state.data?'Stale data':'Unavailable'}</strong> · ${escape(state.error.message)}${state.error.correlationId?` · Reference ${escape(state.error.correlationId)}`:''}</div>`:''}${content}<footer>Updates every 5 seconds while visible · Times shown in your local timezone</footer></div></main></div>`;
  root.querySelectorAll('[data-page]').forEach(b=>b.onclick=()=>{state.page=Number(b.dataset.page);state.index=0;state.selected=null;state.detail=null;state.data=null;state.error=null;state.reason='';start();});
  root.querySelector('#logout').onclick=async()=>{try{await api('/auth/logout',{method:'POST'});login();}catch(error){state.error=error;render();}};
  root.querySelector('#refresh').onclick=()=>poller.refresh();
  root.querySelector('#previous')?.addEventListener('click',()=>{state.index--;start();});root.querySelector('#next')?.addEventListener('click',()=>{state.index++;start();});
  if(focused){const element=document.getElementById(focused);element?.focus();if(typeof selection==='number'&&element?.setSelectionRange)try{element.setSelectionRange(selection,selection);}catch{}}
}
function overview(data){
  const total=Object.values(data.counts).reduce((a,b)=>a+Number(b),0);
  const cards=[['Total orders',total,'Persisted across all states'],['Confirmed',data.counts.CONFIRMED,'Stock reserved'],['Awaiting outcome',data.counts.PENDING+data.counts.CANCEL_PENDING,'Pending or cancelling'],['Open dead letters',data.order.unresolvedDeadLetters,'Order service']];
  let html=`<div class="metrics">${cards.map(([label,value,description])=>`<article><span>${label}</span><strong>${escape(value)}</strong><small>${description}</small></article>`).join('')}</div><div class="two-columns">`;
  html+=panel('Order states',table(['State','Orders'],Object.entries(data.counts).map(([s,n])=>[badge(s),escape(n)])));
  html+=panel('Delivery & confirmation',`<dl><dt>Mean confirmation latency</dt><dd>${data.confirmationLatency.mean_seconds===null?'No confirmed samples':Number(data.confirmationLatency.mean_seconds).toFixed(2)+' seconds'}</dd><dt>Pending outbox events</dt><dd>${escape(data.order.outbox.pending)}</dd><dt>Oldest pending event</dt><dd>${Number(data.order.outbox.oldest_pending_seconds).toFixed(1)} seconds</dd><dt>Blocked publications</dt><dd>${escape(data.order.outbox.blocked)}</dd></dl>`)+ '</div>';
  html+=panel('Connected services',serviceRows(data.services));return html;
}
function serviceRows(services){return table(['Service','Observation','Readiness','Pending outbox','Unresolved DLT','Last successful observation'],Object.entries(services).map(([name,item])=>[escape(name),badge(item.status),escape(item.data?.readiness||'—'),escape(item.data?.outbox?.pending),escape(item.data?.unresolvedDeadLetters),escape(date(item.lastSuccessfulAt))]));}
function orders(data){
  const filters=`<form id="order-filter" class="filters"><label>Order ID<input id="query" placeholder="Exact order UUID" value="${escape(state.query)}"></label><label>Status<select id="order-status">${['','PENDING','CONFIRMED','FAILED','CANCEL_PENDING','CANCELLED'].map(s=>`<option value="${s}" ${state.status===s?'selected':''}>${s||'All states'}</option>`).join('')}</select></label><button>Apply filters</button></form>`;
  let html=panel('Orders',filters+table(['Order','Status','Quantity','Version','Created'],data.items.map(o=>[button(o.id,'data-order',o.id),badge(o.status),escape(o.quantity),escape(o.version),escape(date(o.createdAt))]))+pagination());
  if(data.detail){const d=data.detail;html+=panel('Order detail',`<dl><dt>Order</dt><dd class="mono">${escape(d.id)}</dd><dt>Customer</dt><dd class="mono">${escape(d.customerId)}</dd><dt>Current state</dt><dd>${badge(d.status)}</dd><dt>Failure reason</dt><dd>${escape(d.failureReason||'None')}</dd></dl>${d.customerId===user.userId&&d.status==='CONFIRMED'?'<button id="cancel" class="danger-button">Request cancellation</button>':''}`);
    html+=panel('Persisted lifecycle',table(['Version','Transition','Recorded','Correlation ID','Reason'],d.timeline.map(t=>[escape(t.version),`${escape(t.previousStatus||'NEW')} → ${badge(t.status)}`,escape(date(t.recordedAt)),`<span class="mono">${escape(t.correlationId)}</span>`,escape(t.reason)])));
    html+=panel('Linked notifications',data.notificationError?'<div class="notice">Notification service unavailable.</div>':table(['Event','Version','Recorded','Message'],data.notifications.map(n=>[badge(n.event_type),escape(n.order_version),escape(date(n.recorded_at)),escape(n.message)])));
  }return html;
}
function inventory(data){let html=panel('Available stock',table(['Product','Name','Available','Updated'],data.items.map(p=>[button(p.product_id,'data-product',p.product_id),escape(p.product_name),escape(p.quantity_available),escape(date(p.updated_at))]))+pagination());
  if(data.reservations)html+=panel('Reservation & release history',table(['Order','Quantity','Status','Reserved','Released'],data.reservations.map(r=>[button(r.order_id,'data-order-link',r.order_id),escape(r.quantity),badge(r.status),escape(date(r.reserved_at)),escape(date(r.released_at))])));return html;}
function events(data){
  let html=panel('Kafka consumer progress',`<p class="muted inset">Lag counts records awaiting consumer progress, not business failures. ${badge(data.kafka.status)}</p>`+table(['Topic','Partition','Consumer group','End offset','Committed','Lag / status','Leader'],(data.kafka.data||[]).map(p=>[escape(p.topic),escape(p.partition),escape(p.group),escape(p.logEndOffset),escape(p.committedOffset),p.status==='UNINITIALIZED'?badge(p.status):escape(p.lag),escape(p.leader)])));
  html+=panel('Dead letters',`<div class="filters"><label>Service<select id="service">${['order','inventory','notification'].map(s=>`<option ${state.service===s?'selected':''}>${s}</option>`).join('')}</select></label><label>Status<select id="letter-status">${['OPEN','REPLAY_QUEUED','RESOLVED','LINKED','ALL'].map(s=>`<option ${state.letterStatus===s?'selected':''}>${s}</option>`).join('')}</select></label></div>`+table(['Record','Source','Status','Attempts','Last seen'],data.items.map(d=>[button(d.id,'data-letter',d.id),`${escape(d.source_topic)} / ${d.source_partition} / ${d.source_offset}`,badge(d.status),escape(d.replay_attempts),escape(date(d.last_seen_at))]))+pagination());
  if(data.detail){const d=data.detail.deadLetter;html+=panel('Inspect & recover',`<p>${escape(d.sanitized_error)}</p><dl><dt>Exact source</dt><dd>${escape(d.source_topic)} · partition ${escape(d.source_partition)} · offset ${escape(d.source_offset)}</dd><dt>DLT source</dt><dd>${escape(d.dlt_topic)} · partition ${escape(d.dlt_partition)} · offset ${escape(d.dlt_offset)}</dd><dt>Status</dt><dd>${badge(d.status)}</dd></dl><details><summary>Original payload (read-only)</summary><pre>${escape(d.raw_payload)}</pre></details>${d.status==='OPEN'?`<form id="replay"><label>Reason for replay<textarea id="reason" minlength="5" maxlength="500" required placeholder="Explain what was corrected and why replay is appropriate.">${escape(state.reason)}</textarea></label><button class="primary" ${actionPending?'disabled':''}>Queue audited replay</button></form>`:'<p class="muted">Replay is available only for OPEN records.</p>'}`);
    html+=panel('Replay audit history',table(['Request','Actor','Reason','Requested','Result'],data.detail.replays.map(r=>[escape(r.id),escape(r.requested_by),escape(r.reason),escape(date(r.requested_at)),badge(r.result)])));}return html;
}
function evidence(data){
  let html=`<p class="muted">Packaged ${escape(date(data.generatedAt))}. Reports are read-only evidence, not current production health.</p>`;
  if(data.verification){const p=data.verification.performance;
    html+=panel('Measured performance',`<p class="inset">${badge(p.result)} · ${escape(p.users)} users · ${escape(p.warmupSeconds)}s warm-up + ${escape(p.measuredSeconds)}s measurement</p>`+table(['Measurement','Observed','Acceptance target'],[
      ['Create latency, p95',escape(Number(p.httpMilliseconds.p95).toFixed(2))+' ms','< 500 ms'],
      ['Confirmation latency, p95',escape(Number(p.confirmationMilliseconds.p95).toFixed(2))+' ms','< 5,000 ms'],
      ['Throughput',escape(Number(p.throughputPerSecond).toFixed(2))+' requests/s','10 scheduled requests/s'],
      ['Duplicate business events',escape(p.invariants.duplicateBusinessEvents),'0']
    ]));
    html+=panel('Recovery verification matrix',table(['Scenario','Result','Verified invariant'],data.verification.matrix.scenarios.map(s=>[escape(s.scenarioId),badge(s.result),escape(s.expectedInvariant)])));
  }
  return html+data.runs.map(run=>panel(run.source,`<p class="mono small inset">SHA-256 ${escape(run.sha256)}</p>`+table(['Suite','Tests','Failures','Errors','Skipped'],run.suites.map(s=>[escape(s.suite),escape(s.tests),escape(s.failures),escape(s.errors),escape(s.skipped)])))).join('');
}
function render(){
  let content='<div class="empty">Loading live data…</div>';
  if(state.data)content=[overview,orders,inventory,d=>serviceRows(d)+Object.entries(d).map(([name,item])=>panel(name+' dependencies',table(['Dependency','Status'],Object.entries(item.data?.dependencies||{}).map(([k,v])=>[escape(k),badge(v)])))).join(''),events,evidence][state.page](state.data);
  else if(state.error)content='<div class="empty">Data is unavailable. Use Refresh to retry.</div>';
  shell(content);bind();
}
function bind(){
  root.querySelector('#order-filter')?.addEventListener('submit',e=>{e.preventDefault();state.query=root.querySelector('#query').value.trim();state.status=root.querySelector('#order-status').value;state.index=0;start();});
  for(const [attribute,kind] of [['data-order','order'],['data-product','product'],['data-letter','letter']])root.querySelectorAll('['+attribute+']').forEach(b=>b.onclick=()=>{state.selected=b.getAttribute(attribute);state.reason='';start();});
  root.querySelectorAll('[data-order-link]').forEach(b=>b.onclick=()=>{state.page=1;state.selected=b.dataset.orderLink;state.query=b.dataset.orderLink;state.status='';state.data=null;start();});
  for(const [id,property] of [['service','service'],['letter-status','letterStatus']])root.querySelector('#'+id)?.addEventListener('change',e=>{state[property]=e.target.value;state.index=0;state.selected=null;start();});
  root.querySelector('#reason')?.addEventListener('input',e=>{state.reason=e.target.value;});
  root.querySelector('#replay')?.addEventListener('submit',async e=>{e.preventDefault();await mutate('replay:'+state.service+':'+state.selected+':'+state.reason,`/api/ops/${state.service}/dead-letters/${state.selected}/replay`,{reason:state.reason});});
  root.querySelector('#cancel')?.addEventListener('click',async()=>{await mutate('cancel:'+state.selected,`/api/orders/${state.selected}/cancel`);});
}
async function mutate(action,path,body){
  if(actionPending)return;actionPending=true;
  if(!actionKeys.has(action))actionKeys.set(action,crypto.randomUUID());render();
  try{await api(path,{method:'POST',body,key:actionKeys.get(action)});actionKeys.delete(action);state.error=null;await poller.refresh();}
  catch(error){handleError(error);}finally{actionPending=false;if(user)render();}
}
async function load(signal){
  const page=state.page;const params=new URLSearchParams({page:state.index,size:state.size});let data;
  if(page===0)data=await api('/api/dashboard/overview/stats',{signal});
  if(page===1){if(state.status)params.set('status',state.status);if(state.query)params.set('id',state.query);data={items:await api('/api/orders?'+params,{signal})};if(state.selected){data.detail=await api('/api/orders/'+state.selected,{signal});try{data.notifications=await api('/api/notifications?orderId='+state.selected,{signal});}catch(error){if(error.status===401||error.name==='AbortError')throw error;data.notificationError=true;}}}
  if(page===2){data={items:await api('/api/inventory?'+params,{signal})};if(state.selected)data.reservations=await api('/api/inventory/'+state.selected+'/reservations',{signal});}
  if(page===3)data=await api('/api/dashboard/services/health',{signal});
  if(page===4){params.set('status',state.letterStatus);const [kafka,items]=await Promise.all([api('/api/dashboard/kafka/topics',{signal}),api('/api/ops/'+state.service+'/dead-letters?'+params,{signal})]);data={kafka,items};if(state.selected)data.detail=await api('/api/ops/'+state.service+'/dead-letters/'+state.selected,{signal});}
  if(page===5)data=await api('/api/dashboard/test-runs',{signal});
  if(signal.aborted)return;state.data=data;state.error=null;state.updated=Date.now();render();
}
function handleError(error){if(error.status===401){login('Your session expired. Please sign in again.');return;}state.error=error;render();}
function start(){poller?.stop();state.error=null;render();poller=poll(load,{onError:handleError});}
window.addEventListener('pagehide',()=>poller?.stop());login();

