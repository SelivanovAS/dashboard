import worker from '../cloudflare-worker/worker.js';
import { cleanupProfiles } from '../cloudflare-worker/profile_lifecycle.js';
import { Store, hash, REGIONS } from './store.mjs';
import { Jobs } from './jobs.mjs';
import { DataIndex } from './data-index.mjs';
import { readGatewayImportBody } from '../cloudflare-worker/import_gateway.js';
import {publicationResponse} from './publication.mjs';
import {PILOT_ROOT} from './pilot-guard.mjs';

const names = { hmao: 'hmao', sverdlovsk_yanao: 'ural', bashkortostan: 'bashkortostan', tyumen: 'tyumen' };
const createPaths = new Set(['/admin/import-dump','/admin/add-case','/admin/writ-waiver','/admin/dispatch']);
let instanceCreated = false;

export function validateConfig(c) {
  if (!REGIONS.includes(c.region) || !['isolated','closed-pilot'].includes(c.mode)) throw new Error('Нужен явно изолированный режим или закрытый пилот');
  if(c.mode==='closed-pilot'&&(c.region!=='hmao'||c.publicationRoot!==PILOT_ROOT
    ||c.database!=='/srv/projects/sbersud/pilot/hmao/state/state.sqlite'||c.port!==18081))throw Error('Неверные границы закрытого пилота');
  const canonical = `https://api-${names[c.region]}.delosud.ru`;
  if (c.publicOrigin !== canonical) throw new Error('Адрес API не соответствует территории');
  if (!Array.isArray(c.allowedOrigins) || !c.allowedOrigins.length || c.allowedOrigins.some(v => {
    try { return new URL(v).origin !== v || new URL(v).protocol !== 'https:'; } catch { return true; }
  })) throw new Error('Нужен явный список HTTPS-origin');
  for (const key of ['database','dataIndex']) if (typeof c[key] !== 'string' || !c[key].startsWith('/')) throw new Error(`Нет пути ${key}`);
  if(c.publicationRoot!==undefined&&(typeof c.publicationRoot!=='string'||!c.publicationRoot.startsWith('/')))throw Error('Неверный каталог публикации');
  if (!Array.isArray(c.allowedCourts) || !c.allowedCourts.length || c.allowedCourts.some(h => !/^[a-z0-9.-]+\.sudrf\.ru$/.test(h))) throw new Error('Нет реестра судов территории');
  if (!c.vars?.CASES_DATA_URL || !c.vars?.GH_REPO || !c.vars?.VAPID_PUBLIC_KEY) throw new Error('Нет региональных настроек');
  if (c.vars.CRON_UTC !== '' || c.vars.IMPORT_EXECUTOR !== 'vps') throw new Error('Недопустимый исполнитель');
  for (const name of ['OWNER_SECRET','OPERATOR_SECRET','PUSH_SECRET','PROGRESS_SECRET']) {
    if (typeof c.secrets?.[name] !== 'string' || c.secrets[name].length < 16) throw new Error(`Отсутствует ${name}`);
  }
  if (new Set(Object.values(c.secrets)).size !== Object.values(c.secrets).length) throw new Error('Секреты ролей должны различаться');
  return c;
}

export function createRuntime(config, { gatewayFetch } = {}) {
  config = structuredClone(config);
  validateConfig(config);
  Object.freeze(config.allowedCourts);
  Object.freeze(config);
  if (instanceCreated) throw new Error('Один региональный API на процесс; создайте отдельный процесс');
  instanceCreated = true;
  const store = new Store(config.database, config.region);
  const jobs = new Jobs(store);
  const data = new DataIndex(config.dataIndex, config.region, config.vars.CASES_DATA_URL, worker.buildAliasMap,config.publicationRoot);
  const env = Object.freeze({ ...config.vars, ...config.secrets, PUSH_SUBSCRIPTIONS: store.kv,
    ALLOWED_ORIGINS: Object.freeze([...config.allowedOrigins]), API_DATA: data, LOCAL_JOBS: jobs,
    CRON_UTC: '', IMPORT_EXECUTOR: 'vps',
    IMPORT_GATEWAY_ORIGIN: gatewayFetch ? config.vars.IMPORT_GATEWAY_ORIGIN : '', GATEWAY_FETCH: gatewayFetch,
  });
  const publicHost = new URL(config.publicOrigin).host;
  const respond = (error, status) => Response.json({ ok: false, error }, { status });
  async function fetch(request) {
    const url = new URL(request.url);
    if (url.origin !== config.publicOrigin || (request.headers.get('host') && request.headers.get('host') !== publicHost)) {
      return respond('wrong_host', 421);
    }
    // Никогда не доверяем forwarded-заголовкам из произвольного запроса.
    if (['forwarded','x-forwarded-host','x-forwarded-proto','x-original-url'].some(h => request.headers.has(h))) {
      return respond('untrusted_forwarded_headers', 400);
    }
    if (url.pathname === '/admin/test-push') return respond('external_sending_disabled', 503);
    if (url.pathname.startsWith('/server/')) return respond('not_found', 404);
    let raw = request.method === 'GET' || request.method === 'HEAD' ? null : await request.arrayBuffer();
    let gatewaySha = '';
    if (raw && raw.byteLength > 16 * 1024 * 1024) return respond('body_too_large', 413);
    const copy = () => new Request(request.url, { method: request.method, headers: request.headers, ...(raw ? { body: raw } : {}) });
    return store.transaction(async () => {
      if (await store.kv.get('server:maintenance') === '1') return new Response('Service Unavailable',{
        status:503,headers:{'Retry-After':'60','Cache-Control':'no-store'}});
      if(config.publicationRoot&&url.pathname.startsWith('/data/')&&request.method==='GET')return publicationResponse(config.publicationRoot,config.region,url.pathname);
      await data.pin();
      if (raw && createPaths.has(url.pathname)) {
        const secret = url.searchParams.get('secret') || (request.headers.get('authorization') || '').replace(/^Bearer /, '');
        if (![env.OWNER_SECRET,env.OPERATOR_SECRET].includes(secret)) return respond('unauthorized',401);
        let parsed;
        try { parsed = JSON.parse(Buffer.from(raw).toString()); } catch { return respond('bad_json',400); }
        if (!parsed || typeof parsed !== 'object') return respond('bad_json',400);
        if (parsed.__gateway_upload) {
          if (!gatewayFetch || url.pathname !== '/admin/import-dump') return respond('gateway_adapter_not_enabled',503);
          try {
            const loaded = await readGatewayImportBody(parsed, env);
            parsed = loaded.body; gatewaySha = loaded.sha256;
            raw = new TextEncoder().encode(JSON.stringify(parsed)).buffer;
          } catch(e) { return respond('gateway_validation_failed', e.status || 502); }
        }
        const courtHosts = [parsed.court_domain,
          ...(Array.isArray(parsed.items) ? parsed.items.flatMap(item => {
            if (item && typeof item === 'object') return [item.court_domain];
            if (typeof item === 'string' && /^https?:\/\//.test(item)) {
              try { return [new URL(item.replace(/&amp;/g,'&')).hostname]; } catch { return ['invalid']; }
            }
            return [];
          }) : [])].filter(Boolean).map(h => String(h).toLowerCase().replace(/^www\./,''));
        if (courtHosts.some(h => !config.allowedCourts.includes(h))) return respond('court_outside_region',400);
        if (url.pathname === '/admin/dispatch') {
          if (['update_cases.yml','test_digest.yml'].includes(parsed.workflow)) {
            if (secret !== env.OWNER_SECRET) return respond('forbidden',403);
            return respond('local_executor_not_ready',503);
          }
        }
      }
      // Старый отчёт не может самостоятельно объявить новое задание выполненным.
      if (url.pathname === '/import-result' && raw) {
        if (![env.PUSH_SECRET,env.OWNER_SECRET].some(s => request.headers.get('authorization') === `Bearer ${s}`)) return respond('unauthorized',401);
        let body;
        try { body = JSON.parse(Buffer.from(raw).toString()); } catch { return respond('bad_json',400); }
        if (body.status === 'done') return respond('local_publication_receipt_required',409);
        // Состоянием принятой местной очереди владеет местный исполнитель.
        if (store.db.prepare('SELECT id FROM jobs WHERE body_key=?').get(body.job_key || body.dump_key || '')) {
          return respond('local_job_owner_required',409);
        }
      }
      const idem = request.headers.get('idempotency-key');
      if (idem && !/^[A-Za-z0-9:._-]{8,120}$/.test(idem)) return respond('invalid_idempotency_key',400);
      const digest = hash(Buffer.from(raw || new ArrayBuffer(0)));
      const authScope = hash((url.searchParams.get('secret') || '') + '|' + (request.headers.get('authorization') || ''));
      const key = createPaths.has(url.pathname) && request.method === 'POST'
        ? hash(`${config.region}|${url.pathname}|${authScope}|${idem || digest}`) : null;
      if (key) {
        const old = store.db.prepare('SELECT * FROM requests WHERE key=? AND expires_at>?').get(key, Date.now());
        if (old) {
          if (old.digest !== digest) return respond('idempotency_conflict',409);
          const saved = JSON.parse(old.response);
          return new Response(saved.body, { status: saved.status, headers: saved.headers });
        }
      }
      let response;
      try { response = await worker.fetch(copy(), env); } catch { return respond('request_failed',500); }
      if (gatewaySha) response.headers.set('X-Import-Gateway-SHA256',gatewaySha);
      if (key && response.ok) {
        const saved = { body: await response.clone().text(), status: response.status, headers: [...response.headers] };
        store.db.prepare('INSERT OR REPLACE INTO requests VALUES (?,?,?,?)')
          .run(key,digest,JSON.stringify(saved),Date.now()+72*3600*1000);
      }
      return response;
    });
  }
  return { store, jobs, env, fetch, publicationRoot:config.publicationRoot,
    cleanup: () => store.transaction(() => cleanupProfiles(env)),
    finish: (id, generation, summary, options) => jobs.finish(id, generation, row => worker.fetch(new Request(
      `${config.publicOrigin}/import-result`, { method:'POST', headers: { authorization:`Bearer ${env.PUSH_SECRET}`, 'content-type':'application/json' },
        body:JSON.stringify({ ...summary, [row.workflow === 'import_cases.yml' ? 'dump_key' : 'job_key']: row.body_key,
          source:'vps',status:'done',attempt_id:`local:${id}:${JSON.parse(row.inputs).attempt || generation}` }) }),env),options),
    reject: (job,summary) => jobs.reject(job.id,job.generation,()=>worker.fetch(new Request(
      `${config.publicOrigin}/import-result`,{method:'POST',headers:{authorization:`Bearer ${env.PUSH_SECRET}`,'content-type':'application/json'},
        body:JSON.stringify({...summary,[job.workflow==='import_cases.yml'?'dump_key':'job_key']:job.body_key,
          source:'vps',status:'failed',attempt_id:`local:${job.id}:${JSON.parse(job.inputs).attempt || job.generation}`})}),env)),
    close: () => store.close(),
  };
}
