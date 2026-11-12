import {readFile,realpath} from 'node:fs/promises';
import {join} from 'node:path';
import {hash} from './store.mjs';

const sorted=value=>Array.isArray(value)?value.map(sorted):value&&typeof value==='object'
  ?Object.fromEntries(Object.keys(value).sort().map(k=>[k,sorted(value[k])])):value;
export const manifestHash=value=>hash(JSON.stringify(sorted(value)));

export async function publishedSnapshot(root,region) {
  if(await realpath(root)!==root)throw Error('Ссылка вместо публикации');
  const marker=JSON.parse(await readFile(join(root,'.sbersud-isolated.json'),'utf8'));
  if(marker.region!==region||marker.synthetic!==true)throw Error('Другая территория');
  const current=JSON.parse(await readFile(join(root,'current.json'),'utf8'));
  if(current.version!=='initial'&&!/^[a-f0-9-]{36}-[1-9][0-9]*$/.test(current.version))throw Error('Нет опубликованного снимка');
  const directory=join(root,'snapshots',current.version);
  if(await realpath(directory)!==directory)throw Error('Неверный каталог');
  const receipt=JSON.parse(await readFile(join(directory,'receipt.json'),'utf8'));
  if(receipt.region!==region||receipt.version!==current.version||receipt.outcome==='failed'
    ||receipt.sha256!==current.sha256||manifestHash({files:receipt.files,index_sha256:receipt.index_sha256})!==receipt.sha256)throw Error('Повреждён паспорт публикации');
  return {directory,receipt,version:current.version};
}
export async function publicationResponse(root,region,path) {
  // Only files already intended for public case viewing; input jobs, logs,
  // summaries, hidden state, credentials and receipts are never public routes.
  const name=path.slice('/data/'.length);
  if(!/^(?:cases(?:_bank)?(?:_archive(?:_[0-9]{4})?)?(?:_events)?\.json|sberbank_cases(?:_archive)?\.csv)$/.test(name))return new Response('Not Found',{status:404});
  try {
    const {directory,receipt,version}=await publishedSnapshot(root,region);
    if(!receipt.files[name])return new Response('Not Found',{status:404});
    const file=join(directory,'data',name);
    if(await realpath(file)!==file)throw Error('Неверный файл');
    const bytes=await readFile(file);
    if(hash(bytes)!==receipt.files[name])throw Error('Повреждён файл публикации');
    return new Response(bytes,{headers:{'content-type':name.endsWith('.json')?'application/json; charset=utf-8':'text/csv; charset=utf-8',
      'cache-control':'no-store',etag:`"${receipt.files[name]}"`,'x-data-version':version}});
  }catch{return new Response('Публикация недоступна',{status:503});}
}
