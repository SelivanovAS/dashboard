import {request} from 'node:http';

// Logical HTTPS address is validated by the shared gateway module. For the
// closed pilot only, its body is fetched from our separate loopback service.
export function pilotGatewayFetch(url,options={}){
  const parsed=new URL(url);
  if(parsed.origin!=='https://api2-hmao.delosud.ru'||!/^\/_gateway-upload\/[0-9a-f-]{36}$/.test(parsed.pathname)
    ||parsed.search||parsed.hash||options.method!=='GET'||options.redirect!=='manual')throw Error('Шлюз вне закрытого пилота');
  return new Promise((resolve,reject)=>{
    const req=request({hostname:'127.0.0.1',port:18082,path:parsed.pathname,method:'GET',
      headers:{...options.headers,Host:'api2-hmao.delosud.ru'},signal:options.signal,timeout:20000},res=>{
      const chunks=[];let size=0;
      res.on('data',b=>{size+=b.length;if(size>10*1024*1024){req.destroy(Error('Слишком большая загрузка'));return;}chunks.push(b);});
      res.on('error',reject);
      res.on('end',()=>resolve(new Response(Buffer.concat(chunks),{status:res.statusCode,headers:res.headers})));
    });
    req.on('timeout',()=>req.destroy(Error('Шлюз не ответил')));req.on('error',reject);req.end();
  });
}
