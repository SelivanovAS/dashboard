// Executor boundary: apply and publish must provide independently verified receipts.
// Tests inject transports. The included writ adapter only accepts an isolated root.
export async function runOne(runtime, adapter) {
  const job = await runtime.jobs.claim({ workflows: adapter.workflows });
  if (!job) return null;
  return resumeJob(runtime, adapter, job);
}
export async function resumeJob(runtime, adapter, job) {
  let receipt;
  if (['leased','uncertain'].includes(job.state)) {
    receipt = job.state === 'uncertain' ? await adapter.inspect(job) : await adapter.apply(job);
    if (!receipt && job.state === 'uncertain' && adapter.recover) receipt = await adapter.recover(job);
    if (!receipt) throw new Error('После обрыва результат применения ещё не подтверждён');
    await runtime.jobs.applied(job.id,job.generation,receipt);
  } else if (['applied','published'].includes(job.state)) receipt = JSON.parse(job.receipt);
  else throw new Error('Задание не допускает продолжение');
  const published = await adapter.publish(receipt);
  await runtime.jobs.published(job.id,job.generation,published);
  await runtime.finish(job.id,job.generation,receipt.summary || {});
  return { id:job.id, state:'done', sha256:receipt.sha256 };
}
