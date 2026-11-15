// На общем домене у каждой картотеки своё хранилище и свой путь к профилям.
{
  const script = new URL(document.currentScript.src);
  const territory = script.pathname.split('/')[1];
  Object.assign(window.REGION_FRONT, {
    PUSH_WORKER_URL: script.origin + '/_api/' + territory,
    PUSH_WORKER_FALLBACKS: [],
    STORAGE_NS: 'delosud-' + territory,
  });
}
