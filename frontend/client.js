let token=null;
export function setToken(value){token=value;}
export function clearToken(){token=null;}
export async function api(path,{signal,method='GET',body,key}={}){
  const headers={'X-Correlation-Id':crypto.randomUUID()};
  if(token)headers.Authorization='Bearer '+token;
  if(body!==undefined)headers['Content-Type']='application/json';
  if(key)headers['Idempotency-Key']=key;
  const response=await fetch(path,{method,headers,signal,body:body===undefined?undefined:JSON.stringify(body)});
  const payload=response.status===204?null:await response.json().catch(()=>({message:'Unexpected server response'}));
  if(!response.ok){if(response.status===401)clearToken();const error=new Error(payload?.message||payload?.code||'Request failed');error.status=response.status;error.code=payload?.code;error.correlationId=payload?.correlationId;error.retryAfter=response.headers.get('Retry-After');throw error;}
  return payload;
}
export function poll(task,{documentRef=document,delay=5000,onError=()=>{}}={}){
  let active=true,timer=null,inFlight=false,controller=null,refreshPending=false;
  async function run(){
    if(!active||documentRef.hidden||inFlight)return;
    clearTimeout(timer);inFlight=true;controller=new AbortController();
    try{await task(controller.signal);}catch(error){if(active&&!controller.signal.aborted&&error.name!=='AbortError')onError(error);}
    finally{inFlight=false;if(active&&!documentRef.hidden){if(refreshPending){refreshPending=false;void run();}else timer=setTimeout(run,delay);}}
  }
  function visibility(){clearTimeout(timer);if(documentRef.hidden)controller?.abort();else if(inFlight)refreshPending=true;else run();}
  documentRef.addEventListener('visibilitychange',visibility);run();
  return {refresh:run,stop(){active=false;clearTimeout(timer);controller?.abort();documentRef.removeEventListener('visibilitychange',visibility);}};
}
