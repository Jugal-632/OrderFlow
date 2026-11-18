import http from 'node:http';
import https from 'node:https';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
const gateway=new URL(process.env.GATEWAY_URL||'http://127.0.0.1:8080');
const assets=new Map([['/',['index.html','text/html']],['/app.js',['app.js','text/javascript']],['/client.js',['client.js','text/javascript']],['/style.css',['style.css','text/css']]]);
const server=http.createServer(async(req,res)=>{
  if(req.url.startsWith('/api/')||req.url.startsWith('/auth/')) {
    const transport=gateway.protocol==='https:'?https:http;
    const upstream=transport.request(new URL(req.url,gateway),{method:req.method,headers:{...req.headers,host:gateway.host},timeout:12000},response=>{
      res.writeHead(response.statusCode,response.headers);response.pipe(res);
    });
    upstream.on('timeout',()=>upstream.destroy());
    upstream.on('error',()=>{if(!res.headersSent)res.writeHead(503,{'Content-Type':'application/json'});res.end(JSON.stringify({code:'GATEWAY_UNAVAILABLE',message:'Gateway unavailable'}));});
    req.on('aborted',()=>upstream.destroy());req.pipe(upstream);return;
  }
  const asset=assets.get(req.url.split('?')[0]);
  if(!asset){res.writeHead(404);res.end('Not found');return;}
  try{const body=await readFile(fileURLToPath(new URL(asset[0],import.meta.url)));res.writeHead(200,{'Content-Type':asset[1], 'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'});res.end(body);}
  catch{res.writeHead(500);res.end('Asset unavailable');}
});
server.listen(Number(process.env.PORT||5173),process.env.HOST||'127.0.0.1',()=>console.log('OrderFlow frontend: http://127.0.0.1:'+(process.env.PORT||5173)));
