import {test} from 'node:test';
import assert from 'node:assert/strict';
import {spawn,execFileSync} from 'node:child_process';
import {request} from 'node:http';
import {fileURLToPath} from 'node:url';
import {once} from 'node:events';
import {fixture} from './helpers.mjs';
import {serve} from '../server.mjs';
import {pilotGatewayFetch} from '../gateway-transport.mjs';
import {PILOT_ROOT,validMarker} from '../pilot-guard.mjs';
import {validateConfig} from '../runtime.mjs';
const python=process.env.TEST_PYTHON||'python3';
const source=fileURLToPath(new URL('../../',import.meta.url));

test('closed pilot is confined to one separate HMAO root',async()=>{
  const f=await fixture();
  assert.equal(validMarker({region:'hmao',mode:'closed-pilot'},PILOT_ROOT,'hmao'),true);
  for(const root of ['/srv/projects/sbersud/court-monitor','/opt/court-monitor',PILOT_ROOT+'/..']){
    assert.equal(validMarker({region:'hmao',mode:'closed-pilot'},root,'hmao'),false);
  }
  assert.throws(()=>validateConfig({...f.config,mode:'closed-pilot'}));
  assert.throws(()=>pilotGatewayFetch('https://api2-tyumen.delosud.ru/_gateway-upload/'+crypto.randomUUID(),{method:'GET',redirect:'manual'}));
});
test('court transport rejects non-court/private targets, pins public IP and bounds requests',()=>{
  const result=execFileSync(python,['-c',String.raw`
import sys
sys.path.insert(0,'server-api')
from unittest.mock import patch
from court_transport import CourtTransport,validate_url,public_addresses
import requests,socket
host='surggor--hmao.sudrf.ru'
url='https://'+host+'/modules.php?name=sud_delo&name_op=case&case_id=1'
for bad in ['http://'+host+'/modules.php?name=sud_delo&name_op=case','https://api.telegram.org/bot/sendMessage',
            'https://'+host+'/other','https://user:secret@'+host+'/modules.php?name=sud_delo&name_op=case']:
    try:validate_url(bad,{host});raise AssertionError('invalid URL accepted')
    except ValueError:pass
with patch('socket.getaddrinfo',return_value=[(socket.AF_INET,socket.SOCK_STREAM,6,'',('127.0.0.1',443))]):
    try:public_addresses(host);raise AssertionError('private address accepted')
    except ValueError:pass
seen=[]
class Response:
    status=200
    def read(self,size):return b'<html>test</html>'
    def getheaders(self):return [('Content-Type','text/html')]
class Connection:
    def __init__(self,host,ip,context):seen.append((host,ip,context.check_hostname,context.verify_mode))
    def request(self,*args,**kwargs):pass
    def getresponse(self):return Response()
    def close(self):pass
transport=CourtTransport({host},1)
assert 'surggor.hmao.sudrf.ru' in transport.allowed
assert 'oblsud.hmao.sudrf.ru' not in transport.allowed
validate_url(url.replace(host,'surggor.hmao.sudrf.ru'),transport.allowed)
with patch('court_transport.public_addresses',return_value=['93.184.216.34']),patch('court_transport.PinnedHTTPS',Connection):
    assert transport.request(requests.Session(),'GET',url).status_code==200
    try:transport.request(requests.Session(),'GET',url);raise AssertionError('budget bypass')
    except requests.ConnectionError:pass
assert len(seen)==1 and seen[0][1]=='93.184.216.34' and seen[0][2] is True and seen[0][3]==2
print('transport checks passed')
`],{cwd:source,encoding:'utf8'});
  assert.match(result,/transport checks passed/);
});
test('separate real upload relay roundtrip reaches only the local API and verifies body hash',async t=>{
  const f=await fixture();f.config.port=18081;f.config.vars.IMPORT_GATEWAY_ORIGIN='https://api2-hmao.delosud.ru';
  const instance=await serve(f.config,{gatewayFetch:pilotGatewayFetch});t.after(()=>instance.close());
  const gateway=spawn(python,['server-api/pilot_gateway.py'],{cwd:source,stdio:['ignore','pipe','pipe']});
  t.after(async()=>{if(gateway.exitCode===null){const stopped=once(gateway,'exit');gateway.kill('SIGTERM');await stopped;}});
  await new Promise((resolve,reject)=>{gateway.stdout.once('data',resolve);gateway.once('exit',()=>reject(Error('gateway exited')));});
  const post=(body,secret)=>new Promise((resolve,reject)=>{
    const bytes=Buffer.from(JSON.stringify(body));
    const req=request({hostname:'127.0.0.1',port:18082,path:'/admin/import-dump?secret='+encodeURIComponent(secret),method:'POST',
      headers:{'content-type':'application/json','content-length':bytes.length}},res=>{
      let body='';res.on('data',b=>body+=b);res.on('end',()=>resolve({status:res.statusCode,body:JSON.parse(body),headers:res.headers}));
    });req.on('error',reject);req.end(bytes);
  });
  assert.equal((await post({html:'x'},'invalid')).status,401);
  const body={court_domain:'surggor--hmao.sudrf.ru',section:'first_instance',delo_id:'1540005',html:'<html>'+('synthetic '.repeat(150))+'</html>'};
  const response=await post(body,f.config.secrets.OPERATOR_SECRET);
  assert.equal(response.status,200,JSON.stringify(response.body));
  assert.match(response.headers['x-import-gateway-sha256'],/^[a-f0-9]{64}$/);
  assert.equal(instance.runtime.store.db.prepare('SELECT COUNT(*) AS n FROM jobs').get().n,1);
  const key=response.body.key;
  await instance.runtime.store.transaction(async()=>assert.equal(await instance.runtime.store.kv.get('import:dump:'+key),body.html));
});
