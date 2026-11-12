// Executor boundary: apply and publish must provide independently verified receipts.
// Tests inject transports. The included writ adapter only accepts an isolated root.
export async function runOne(runtime, adapter) {
  if(adapter.publicationRoot&&adapter.publicationRoot!==runtime.publicationRoot)throw Error('API читает другую публикацию');
  const job = await runtime.jobs.claim({ workflows: adapter.workflows });
  if (!job) return null;
  return resumeJob(runtime, adapter, job);
}
export async function resumeJob(runtime, adapter, job) {
  if(adapter.publicationRoot&&adapter.publicationRoot!==runtime.publicationRoot)throw Error('API читает другую публикацию');
  let ownershipError;
  const heartbeat=setInterval(()=>runtime.jobs.renew(job.id,job.generation).catch(e=>{ownershipError=e;}),20000);
  heartbeat.unref();
  try {
  let receipt;
  if (['leased','uncertain'].includes(job.state)) {
    receipt = job.state === 'uncertain' ? await adapter.inspect(job) : await adapter.apply(job);
    if (!receipt && job.state === 'uncertain' && adapter.recover) receipt = await adapter.recover(job);
    if (!receipt) throw new Error('После обрыва результат применения ещё не подтверждён');
    if(ownershipError)throw ownershipError;
    if(receipt.outcome==='failed')return {id:job.id,...await runtime.reject(job,receipt.summary)};
    await runtime.jobs.applied(job.id,job.generation,receipt);
  } else if (['applied','published'].includes(job.state)) receipt = JSON.parse(job.receipt);
  else throw new Error('Задание не допускает продолжение');
  await runtime.jobs.renew(job.id,job.generation);
  const published = await adapter.publish(receipt);
  await runtime.jobs.published(job.id,job.generation,published);
  const result=await runtime.finish(job.id,job.generation,receipt.summary || {},{retry:receipt.outcome==='retry'});
  return { id:job.id, state:result.state || 'done', sha256:receipt.sha256 };
  }catch(error){
    await runtime.jobs.interrupted(job.id,job.generation).catch(()=>{});
    throw error;
  }finally{clearInterval(heartbeat);}
}
