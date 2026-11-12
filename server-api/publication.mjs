import {readFile,realpath,stat} from 'node:fs/promises';
import {join} from 'node:path';
import {hash} from './store.mjs';
import {validMarker} from './pilot-guard.mjs';

const sorted=value=>Array.isArray(value)?value.map(sorted):value&&typeof value==='object'
  ?Object.fromEntries(Object.keys(value).sort().map(k=>[k,sorted(value[k])])):value;
export const manifestHash=value=>hash(JSON.stringify(sorted(value)));
const cache=new Map();let cacheBytes=0;
async function verifiedBytes(file,expected){
  if(await realpath(file)!==file)throw Error('Неверный файл');
  const info=await stat(file),stamp=[info.dev,info.ino,info.size,info.mtimeMs,info.ctimeMs,expected].join(':');
  const old=cache.get(file);
  if(old?.stamp===stamp)return old.bytes;
  if(old){cacheBytes-=old.bytes.length;cache.delete(file);}
  const bytes=await readFile(file);
  if(hash(bytes)!==expected)throw Error('Повреждён файл публикации');
  if(bytes.length<=4*1024*1024){
    while(cacheBytes+bytes.length>8*1024*1024&&cache.size){const key=cache.keys().next().value;cacheBytes-=cache.get(key).bytes.length;cache.delete(key);}
    cache.set(file,{stamp,bytes});cacheBytes+=bytes.length;
  }
  return bytes;
}

export async function publishedSnapshot(root,region) {
  if(await realpath(root)!==root)throw Error('Ссылка вместо публикации');
  const marker=JSON.parse(await readFile(join(root,'.sbersud-isolated.json'),'utf8'));
  if(!validMarker(marker,root,region))throw Error('Другая территория или неподходящий каталог');
  const current=JSON.parse(await readFile(join(root,'current.json'),'utf8'));
  if(current.version!=='initial'&&!/^[a-f0-9-]{36}-[1-9][0-9]*$/.test(current.version))throw Error('Нет опубликованного снимка');
  const directory=join(root,'snapshots',current.version);
  if(await realpath(directory)!==directory)throw Error('Неверный каталог');
  const receipt=JSON.parse(await readFile(join(directory,'receipt.json'),'utf8'));
  const payload={files:receipt.files,index_sha256:receipt.index_sha256,...(receipt.gzip_files?{gzip_files:receipt.gzip_files}:{})};
  if(receipt.region!==region||receipt.version!==current.version||receipt.outcome==='failed'
    ||receipt.sha256!==current.sha256||manifestHash(payload)!==receipt.sha256)throw Error('Повреждён паспорт публикации');
  return {directory,receipt,version:current.version};
}
export async function publicationResponse(root,region,path,requestHeaders=new Headers()) {
  // Only files already intended for public case viewing; input jobs, logs,
  // summaries, hidden state, credentials and receipts are never public routes.
  const name=path.slice('/data/'.length);
  if(!/^(?:cases(?:_bank)?(?:_archive(?:_[0-9]{4})?)?(?:_events)?\.json|sberbank_cases(?:_archive)?\.csv)$/.test(name))return new Response('Not Found',{status:404});
  try {
    const {directory,receipt,version}=await publishedSnapshot(root,region);
    if(!receipt.files[name])return new Response('Not Found',{status:404});
    const gzip=(requestHeaders.get('accept-encoding')||'').split(',').some(part=>{
      const [coding,...parameters]=part.trim().toLowerCase().split(';');
      const quality=parameters.find(p=>p.trim().startsWith('q='));
      return coding==='gzip'&&(!quality||Number(quality.trim().slice(2))>0);
    })&&receipt.gzip_files?.[name];
    const file=gzip?join(directory,'encoded',name+'.gz'):join(directory,'data',name);
    const expected=gzip?receipt.gzip_files[name]:receipt.files[name];
    const bytes=await verifiedBytes(file,expected);
    const headers={'content-type':name.endsWith('.json')?'application/json; charset=utf-8':'text/csv; charset=utf-8',
      'cache-control':'private, no-cache',etag:`"${expected}"`,'x-data-version':version,'vary':'Accept-Encoding',
      ...(gzip?{'content-encoding':'gzip'}:{})};
    if((requestHeaders.get('if-none-match')||'').split(',').map(v=>v.trim()).includes(headers.etag))return new Response(null,{status:304,headers});
    return new Response(bytes,{headers});
  }catch{return new Response('Публикация недоступна',{status:503});}
}
