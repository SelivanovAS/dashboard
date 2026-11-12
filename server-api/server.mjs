import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import {realpathSync} from 'node:fs';
import { pathToFileURL } from 'node:url';
import { createRuntime } from './runtime.mjs';
import {pilotGatewayFetch} from './gateway-transport.mjs';

export async function loadConfig(path) {
  if (!path?.startsWith('/')) throw new Error('Нужен абсолютный путь закрытой конфигурации');
  const info = await stat(path);
  if (!info.isFile() || info.mode & 0o077) throw new Error('Конфигурация должна иметь права 0600');
  return JSON.parse(await readFile(path, 'utf8'));
}
export async function serve(config,adapters={}) {
  const runtime = createRuntime(config,config.mode==='closed-pilot'?{gatewayFetch:pilotGatewayFetch}:adapters);
  const server = createServer(async (req, res) => {
    try {
      if (!req.url?.startsWith('/') || req.url.startsWith('//')) {
        res.writeHead(400); res.end(); return;
      }
      let size = 0;
      const parts = [];
      for await (const part of req) {
        size += part.length;
        if (size > 16 * 1024 * 1024) { res.writeHead(413); res.end(); return; }
        parts.push(part);
      }
      const headers = new Headers();
      for (let i=0; i<req.rawHeaders.length; i+=2) headers.append(req.rawHeaders[i],req.rawHeaders[i+1]);
      const request = new Request(config.publicOrigin + req.url, { method:req.method, headers,
        ...(!['GET','HEAD'].includes(req.method) ? { body:Buffer.concat(parts) } : {}) });
      const response = await runtime.fetch(request);
      if (!response.headers.has('cache-control')) response.headers.set('cache-control','no-store');
      response.headers.set('referrer-policy','no-referrer');
      response.headers.set('x-content-type-options','nosniff');
      res.writeHead(response.status, Object.fromEntries(response.headers));
      res.end(Buffer.from(await response.arrayBuffer()));
    } catch {
      // Ни URL с secret/feed_token, ни тела и ключи не попадают в access/error log.
      if (!res.headersSent) res.writeHead(500, { 'content-type':'application/json' });
      res.end('{"ok":false,"error":"request_failed"}');
    }
  });
  server.requestTimeout = 15000;
  server.headersTimeout = 10000;
  server.keepAliveTimeout = 5000;
  server.maxConnections = 64;
  await new Promise((resolve,reject) => {
    server.once('error',reject);
    server.listen(config.port ?? 0,'127.0.0.1',resolve);
  });
  return { runtime, server, close: () => new Promise(resolve => server.close(() => { runtime.close(); resolve(); })) };
}
if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  // Общий Worker имеет исторические журналы с идентификаторами. В серверном
  // процессе подавляем их; наружу идёт только служебный статус без реквизитов.
  console.log = () => {}; console.warn = () => {}; console.error = () => {};
  globalThis.fetch = async () => { throw new Error('External fetch disabled in isolated mode'); };
  try {
    const instance = await serve(await loadConfig(process.argv[2]));
    process.stdout.write(JSON.stringify({ ready:true, port:instance.server.address().port })+'\n');
    process.on('SIGTERM',async () => { await instance.close(); process.exit(0); });
    process.on('SIGINT',async () => { await instance.close(); process.exit(0); });
  } catch { process.stderr.write('Серверный прототип не запущен: проверьте конфигурацию и права.\n'); process.exitCode=1; }
}
