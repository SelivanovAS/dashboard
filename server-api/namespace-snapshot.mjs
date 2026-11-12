import { hash,REGIONS } from './store.mjs';

// Read adapter only. No credentials or Cloudflare client in this module.
// The caller must first fence ALL writers, including calendar GET and cleanup.
export async function collectNamespace(kv,region,clock=()=>Date.now()) {
  if(!REGIONS.includes(region))throw Error('Неизвестная территория');
  const entries=[];const seen=new Set();const cursors=new Set();let cursor='';let expired=0;
  do {
    const page=await kv.list({limit:1000,...(cursor?{cursor}:{})});
    if(!Array.isArray(page.keys)||typeof page.list_complete!=='boolean')throw Error('Неполная страница');
    for(const key of page.keys){
      if(seen.has(key.name))throw Error('Повтор ключа в обходе');seen.add(key.name);
      const record=await kv.getWithMetadata(key.name,'arrayBuffer');
      if(record.value===null){
        if(key.expiration && key.expiration<=Math.floor(clock()/1000)){expired++;continue;}
        throw Error('Непросроченное значение отсутствует; снимок неустойчив');
      }
      if(JSON.stringify(record.metadata??null)!==JSON.stringify(key.metadata??null))throw Error('Метаданные изменились');
      entries.push({key:key.name,value_base64:Buffer.from(record.value).toString('base64'),
        metadata:record.metadata??null,expiration:key.expiration??null});
    }
    if(page.list_complete)break;
    if(!page.cursor||cursors.has(page.cursor))throw Error('Пагинация остановилась');
    cursor=page.cursor;cursors.add(cursor);
  }while(true);
  entries.sort((a,b)=>Buffer.compare(Buffer.from(a.key),Buffer.from(b.key)));
  const payload={schema:1,region,entries,jobs:[],requests:[]};
  return {snapshot:{...payload,sha256:hash(JSON.stringify(payload))},observed_expired:expired};
}
export async function stableNamespaceSnapshot(kv,region,clock) {
  const first=await collectNamespace(kv,region,clock);
  const second=await collectNamespace(kv,region,clock);
  if(first.snapshot.sha256!==second.snapshot.sha256)throw Error('Полные проходы различаются: экспорт не принят');
  return second;
}
