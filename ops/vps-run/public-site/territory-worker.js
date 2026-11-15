// Несколько установленных картотек могут быть открыты на одном домене.
self.addEventListener('notificationclick', (event) => {
  event.stopImmediatePropagation();
  event.notification.close();
  const scope = self.registration.scope;
  const target = new URL(event.notification.data?.url || './?digest=open', scope);
  event.waitUntil((async () => {
    const windows = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    const existing = windows.find(client => client.url.startsWith(scope));
    if (existing) {
      existing.postMessage({ type: 'open-digest' });
      return existing.focus();
    }
    return self.clients.openWindow(target.href.startsWith(scope) ? target.href : scope);
  })());
});
self.addEventListener('fetch', event => {
  const { request } = event;
  if (!request.url.startsWith(self.registration.scope)) {
    event.stopImmediatePropagation();
    return;
  }
  const url = new URL(request.url);
  if (request.method === 'GET' && isDataRequest(url) && !isLastDigestRequest(url)) {
    event.stopImmediatePropagation();
    event.respondWith(readTerritoryData(event));
  }
});

async function readTerritoryData(event) {
  const cache = await caches.open(DATA_CACHE);
  if (await cache.match(event.request)) {
    return staleWhileRevalidate(event.request, DATA_CACHE, event);
  }
  // При первом открытии большой картотеки сразу передаём браузеру заголовки
  // и поток ответа. Заполнение офлайн-кэша может длиться дольше тайм-аута страницы.
  const response = await fetch(event.request);
  if (response.ok) event.waitUntil(cachePutSafe(cache, event.request, response.clone()));
  return response;
}
