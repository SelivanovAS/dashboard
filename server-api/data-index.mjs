import { readFile, open, rename, mkdir, stat } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { hash, REGIONS } from './store.mjs';
import {pathToFileURL} from 'node:url';
import {publishedSnapshot} from './publication.mjs';

const caseFields = ['id','current_stage','bank_role','plaintiff','defendant','category'];
const stageFields = ['case_number','material_number','cassation_number','court_domain','court',
  'hearing_date','hearing_time','timezone','link','delo_id','srv_num','new','judge','judge_reporter','last_event'];
function pick(object, fields) {
  return Object.fromEntries(fields.filter(k => object[k] !== undefined).map(k => [k, object[k]]));
}
export function compact(document) {
  if (!document || !Array.isArray(document.cases) || !document.region) throw new Error('Неверная картотека');
  return { region: document.region, cases: document.cases.map(c => ({ ...pick(c, caseFields),
    ...Object.fromEntries(['first_instance','appeal','cassation'].map(stage => {
      const block = c[stage] || {};
      return [stage, { ...pick(block, stageFields), events: (block.events || [])
        .filter(e => e.date === block.hearing_date && e.place).map(e => pick(e, ['date','place'])) }];
    })) })) };
}
export async function atomicJson(path, value) {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const temp = `${path}.${crypto.randomUUID()}.tmp`;
  const file = await open(temp, 'wx', 0o600);
  try { await file.writeFile(JSON.stringify(value)); await file.sync(); } finally { await file.close(); }
  await rename(temp, path);
  const dir = await open(dirname(path), 'r');
  try { await dir.sync(); } finally { await dir.close(); }
}
export async function buildIndex({ dataDir, output, region }) {
  if (!REGIONS.includes(region)) throw new Error('Неверная территория');
  const files = ['cases.json', 'cases_bank.json'];
  const bytes = await Promise.all(files.map(f => readFile(join(dataDir, f))));
  const documents = bytes.map(b => JSON.parse(b));
  if (documents[0].region?.code !== region || documents[1].region && documents[1].region.code !== region) throw new Error('Картотека другой территории');
  // Bank JSON historically has no region block; it shares the pinned main snapshot.
  documents[1].region = documents[0].region;
  // Снимок источника должен оставаться закреплённым при построении индекса.
  const after = await Promise.all(files.map(f => readFile(join(dataDir, f))));
  if (bytes.some((b,i) => !b.equals(after[i]))) throw new Error('Картотека изменилась при чтении');
  const payload = { schema: 1, region, source_hashes: Object.fromEntries(files.map((f,i) => [f, hash(bytes[i])])),
    main: compact(documents[0]), bank: compact(documents[1]) };
  const index = { ...payload, sha256: hash(JSON.stringify(payload)) };
  await atomicJson(output, index);
  return { sha256: index.sha256, cases: documents.map(d => d.cases.length),
    input_bytes: bytes.reduce((n,b) => n+b.length,0), index_bytes: Buffer.byteLength(JSON.stringify(index)) };
}
export class DataIndex {
  constructor(path, region, casesUrl, buildAliases, publicationRoot) {
    this.path = path; this.region = region; this.casesUrl = casesUrl;
    this.bankUrl = casesUrl.replace(/cases\.json$/, 'cases_bank.json');
    this.buildAliases = buildAliases;
    this.publicationRoot=publicationRoot;
  }
  // Pin one version for the complete request: calendar and aliases never mix releases.
  async load() {
    try {
      const snapshot=this.publicationRoot?await publishedSnapshot(this.publicationRoot,this.region):null;
      const path=snapshot?join(snapshot.directory,'api-index.json'):this.path;
      const info = await stat(path);
      const stamp = `${path}:${snapshot?.receipt.index_sha256 || ''}:${info.dev}:${info.ino}:${info.size}:${info.mtimeMs}:${info.ctimeMs}`;
      if (stamp === this.cachedStamp) return this.cached;
      const raw = await readFile(path, 'utf8');
      const digest = hash(raw);
      if(snapshot&&digest!==snapshot.receipt.index_sha256)throw Error('Индекс отличается от публикации');
      if (digest === this.cachedDigest) return this.cached;
      const { sha256, ...payload } = JSON.parse(raw);
      if (payload.schema !== 1 || payload.region !== this.region || hash(JSON.stringify(payload)) !== sha256
          || payload.main.region.code !== this.region || payload.bank.region.code !== this.region) throw new Error('Неверный индекс');
      this.cached = { ...payload, aliases: this.buildAliases(payload.main.cases) };
      this.cachedDigest = digest;
      this.cachedStamp = stamp;
      return this.cached;
    } catch { return null; } // Ошибка даёт 503 календарю, не старый «свежий» индекс.
  }
  async pin() { this.pinned = await this.load(); }
  async read(url) {
    if (url === this.casesUrl) return this.pinned?.main || null;
    if (url === this.bankUrl) return this.pinned?.bank || null;
    return null;
  }
  async aliases() { return this.pinned?.aliases || null; }
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  const [dataDir,output,region]=process.argv.slice(2);
  process.stdout.write(JSON.stringify(await buildIndex({dataDir,output,region}))+'\n');
}
