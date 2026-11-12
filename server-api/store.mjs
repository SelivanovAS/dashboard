import { DatabaseSync, backup } from 'node:sqlite';
import { AsyncLocalStorage } from 'node:async_hooks';
import { chmodSync, existsSync, lstatSync, mkdirSync, openSync, closeSync } from 'node:fs';
import { dirname, isAbsolute } from 'node:path';
import { createHash } from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';

export const REGIONS = ['hmao', 'sverdlovsk_yanao', 'bashkortostan', 'tyumen'];
export const hash = value => createHash('sha256').update(value).digest('hex');

// Один файл закреплён за территорией навсегда. Все операции API проходят через
// transaction(), включая читающие GET, которые учитывают активность профиля.
export class Store {
  #tail = Promise.resolve();
  #scope = new AsyncLocalStorage();
  constructor(path, region, { clock = () => Date.now() } = {}) {
    if (!REGIONS.includes(region)) throw new Error('Территория должна быть задана явно');
    if (path !== ':memory:') {
      if (!isAbsolute(path)) throw new Error('Нужен абсолютный путь SQLite');
      mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
      if (existsSync(path) && (!lstatSync(path).isFile() || lstatSync(path).nlink !== 1)) {
        throw new Error('SQLite должна быть обычным файлом без ссылок');
      }
    }
    this.region = region;
    this.clock = clock;
    this.db = new DatabaseSync(path, { timeout: 1000 });
    if (path !== ':memory:') chmodSync(path, 0o600);
    const tables = this.db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();
    if (tables.length && !tables.some(t => t.name === 'identity')) {
      this.db.close(); throw new Error('Неизвестная SQLite: автоматическое присвоение территории запрещено');
    }
    if (tables.some(t => t.name === 'identity')) {
      const rows = this.db.prepare('SELECT * FROM identity').all();
      if (rows.length !== 1 || rows[0].region !== region || rows[0].version !== 1) {
        this.db.close(); throw new Error('База принадлежит другой территории или версии');
      }
    }
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
      PRAGMA foreign_keys=ON; PRAGMA busy_timeout=1000;
      CREATE TABLE IF NOT EXISTS identity(region TEXT NOT NULL, version INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY COLLATE BINARY, value BLOB NOT NULL,
        metadata TEXT, expires_at INTEGER);
      CREATE INDEX IF NOT EXISTS kv_expiry ON kv(expires_at);
      CREATE TABLE IF NOT EXISTS requests(key TEXT PRIMARY KEY, digest TEXT NOT NULL,
        response TEXT NOT NULL, expires_at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, workflow TEXT NOT NULL,
        inputs TEXT NOT NULL, body_key TEXT NOT NULL, log_key TEXT NOT NULL,
        state TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0,
        lease_until INTEGER, receipt TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
    `);
    if(!tables.some(t=>t.name==='identity')){
      this.db.exec('BEGIN IMMEDIATE');
      try {
      const identity = this.db.prepare('SELECT * FROM identity').all();
      if (!identity.length) this.db.prepare('INSERT INTO identity VALUES (?,1)').run(region);
      else if (identity.length !== 1 || identity[0].region !== region || identity[0].version !== 1) {
        throw new Error('База принадлежит другой территории или версии');
      }
      this.db.exec('COMMIT');
      } catch (e) { this.db.exec('ROLLBACK'); this.db.close(); throw e; }
    }
    // Existing identity was checked above. Opening a runner must not compete
    // for a write lock just to re-confirm the unchanged region.
    this.db.exec('PRAGMA busy_timeout=0');
    this.kv = Object.freeze({
      get: (...args) => this.#guard(() => this.#get(...args)),
      getWithMetadata: (...args) => this.#guard(() => this.#getWithMetadata(...args)),
      put: (...args) => this.#guard(() => this.#put(...args)),
      delete: key => this.#guard(() => { this.db.prepare('DELETE FROM kv WHERE key=?').run(key); }),
      list: (...args) => this.#guard(() => this.#list(...args)),
    });
  }
  async #guard(fn) {
    const context = this.#scope.getStore();
    if (!context) throw new Error('Операция KV вне транзакции');
    try { return await fn(); } catch (e) { context.failed = true; throw e; }
  }
  async transaction(fn) {
    if (this.#scope.getStore()) throw new Error('Вложенная транзакция запрещена');
    const before = this.#tail;
    let release;
    this.#tail = new Promise(resolve => { release = resolve; });
    await before;
    try {
      // A second process (API/runner) can own the writer. Waiting synchronously
      // freezes this process's HTTP and heartbeats, so yield between attempts.
      const deadline=Date.now()+10000;
      for(;;){
        try{this.db.exec('BEGIN IMMEDIATE');break;}
        catch(e){
          if(![5,6].includes(e.errcode)||Date.now()>=deadline)throw e;
          await delay(15+Math.floor(Math.random()*20));
        }
      }
      const context = { failed: false };
      return await this.#scope.run(context, async () => {
        try {
          const result = await fn();
          if (context.failed) throw new Error('Ошибка хранилища: операция отменена');
          // Обработчики Worker иногда перехватывают исключение и возвращают 500.
          this.db.exec(result instanceof Response && !result.ok ? 'ROLLBACK' : 'COMMIT');
          return result;
        } catch (e) { this.db.exec('ROLLBACK'); throw e; }
      });
    } finally {
      // Let another process acquire the writer before draining this process's
      // HTTP queue. Immediate re-acquisition can starve runner heartbeats under
      // sustained load even though every individual transaction is short.
      await delay(10);
      release();
    }
  }
  assertTransaction() {
    if (!this.#scope.getStore()) throw new Error('Операция вне транзакции');
  }
  #row(key) {
    return this.db.prepare('SELECT * FROM kv WHERE key=? AND (expires_at IS NULL OR expires_at>?)')
      .get(key, Math.floor(this.clock() / 1000));
  }
  #decode(row, options) {
    if (!row) return null;
    const type = typeof options === 'string' ? options : options?.type || 'text';
    const bytes = Buffer.from(row.value);
    if (type === 'json') return JSON.parse(bytes.toString('utf8'));
    if (type === 'arrayBuffer') return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    if (type === 'stream') return new ReadableStream({ start(c) { c.enqueue(bytes); c.close(); } });
    if (type !== 'text') throw new Error('Неизвестный тип KV');
    return bytes.toString('utf8');
  }
  #get(key, options) { return this.#decode(this.#row(key), options); }
  #getWithMetadata(key, options) {
    const row = this.#row(key);
    return { value: this.#decode(row, options), metadata: row?.metadata ? JSON.parse(row.metadata) : null };
  }
  async #put(key, value, options = {}) {
    if (typeof key !== 'string' || !key || ['.', '..'].includes(key) || Buffer.byteLength(key) > 512) {
      throw new Error('Неверный ключ KV');
    }
    if (value instanceof ReadableStream) value = await new Response(value).arrayBuffer();
    const bytes = typeof value === 'string' ? Buffer.from(value) : Buffer.from(value);
    if (bytes.length > 25 * 1024 * 1024) throw new Error('Значение KV слишком большое');
    const metadata = options.metadata == null ? null : JSON.stringify(options.metadata);
    if (metadata && Buffer.byteLength(metadata) > 1024) throw new Error('Метаданные KV слишком большие');
    let expires = options.expiration ?? null;
    if (options.expirationTtl !== undefined) {
      if (!Number.isInteger(options.expirationTtl) || options.expirationTtl < 60) throw new Error('Неверный TTL');
      expires = Math.floor(this.clock() / 1000) + options.expirationTtl;
    }
    if (expires !== null && (!Number.isInteger(expires) || expires <= 0)) throw new Error('Неверный срок KV');
    this.db.prepare(`INSERT INTO kv VALUES (?,?,?,?) ON CONFLICT(key) DO UPDATE SET
      value=excluded.value, metadata=excluded.metadata, expires_at=excluded.expires_at`)
      .run(key, bytes, metadata, expires);
  }
  #list({ prefix = '', cursor = '', limit = 1000 } = {}) {
    if (!Number.isInteger(limit) || limit < 1 || limit > 1000) throw new Error('Неверный размер страницы');
    let after = '';
    if (cursor) {
      const decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString());
      if (decoded.region !== this.region || decoded.prefix !== prefix || typeof decoded.after !== 'string') {
        throw new Error('Курсор другой территории или префикса');
      }
      after = decoded.after;
    }
    const rows = this.db.prepare(`SELECT key,metadata,expires_at FROM kv
      WHERE key>? AND substr(key,1,length(?))=? AND (expires_at IS NULL OR expires_at>?)
      ORDER BY key COLLATE BINARY LIMIT ?`).all(after, prefix, prefix, Math.floor(this.clock() / 1000), limit + 1);
    const complete = rows.length <= limit;
    const selected = rows.slice(0, limit);
    return {
      keys: selected.map(r => ({ name: r.key,
        ...(r.metadata !== null ? { metadata: JSON.parse(r.metadata) } : {}),
        ...(r.expires_at !== null ? { expiration: r.expires_at } : {}) })),
      list_complete: complete,
      cursor: complete ? '' : Buffer.from(JSON.stringify({ region: this.region, prefix, after: selected.at(-1).key })).toString('base64url'),
    };
  }
  async snapshot() {
    return this.transaction(() => {
      const rows = this.db.prepare('SELECT * FROM kv ORDER BY key COLLATE BINARY').all();
      const entries = rows.map(r => ({ key: r.key, value_base64: Buffer.from(r.value).toString('base64'),
        metadata: r.metadata === null ? null : JSON.parse(r.metadata), expiration: r.expires_at }));
      const jobs = this.db.prepare('SELECT * FROM jobs ORDER BY id').all();
      const requests = this.db.prepare('SELECT * FROM requests ORDER BY key').all();
      const payload = { schema: 1, region: this.region, entries, jobs, requests };
      return { ...payload, sha256: hash(JSON.stringify(payload)) };
    });
  }
  async restore(snapshot) {
    const { sha256, ...payload } = snapshot;
    if (payload.schema !== 1 || payload.region !== this.region || hash(JSON.stringify(payload)) !== sha256) {
      throw new Error('Регион, версия или контрольная сумма снимка не совпали');
    }
    return this.transaction(async () => {
      if (['kv', 'jobs', 'requests'].some(t => this.db.prepare(`SELECT count(*) AS n FROM ${t}`).get().n)) {
        throw new Error('Восстановление разрешено только в пустую базу');
      }
      let expired = 0;
      const seen = new Set();
      for (const entry of payload.entries) {
        if (seen.has(entry.key)) throw new Error('Повтор ключа в снимке');
        seen.add(entry.key);
        const bytes = Buffer.from(entry.value_base64, 'base64');
        if (bytes.toString('base64') !== entry.value_base64) throw new Error('Повреждено значение');
        if (entry.expiration !== null && entry.expiration <= Math.floor(this.clock() / 1000)) expired++;
        // Абсолютный срок сохраняется, в том числе для уже истёкшей записи.
        await this.kv.put(entry.key, bytes, { metadata: entry.metadata, expiration: entry.expiration });
      }
      for (const row of payload.jobs || []) {
        this.db.prepare('INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(
          row.id, row.workflow, row.inputs, row.body_key, row.log_key, row.state,
          row.generation, row.lease_until, row.receipt, row.created_at, row.updated_at);
      }
      for (const row of payload.requests || []) this.db.prepare('INSERT INTO requests VALUES (?,?,?,?)')
        .run(row.key, row.digest, row.response, row.expires_at);
      return { total: seen.size, expired };
    });
  }
  async backup(path) {
    if (!isAbsolute(path) || existsSync(path)) throw new Error('Нужен новый абсолютный путь резервной копии');
    const before = this.#tail;
    let release;
    this.#tail = new Promise(resolve => { release = resolve; });
    await before;
    try {
      // Online backup cannot run inside a transaction on the source connection.
      // Hold the request mutex, while SQLite itself provides the consistent copy.
      closeSync(openSync(path, 'wx', 0o600));
      await backup(this.db, path);
      chmodSync(path, 0o600);
    } finally { release(); }
    const check = new DatabaseSync(path, { readOnly: true });
    try {
      if (check.prepare('PRAGMA integrity_check').get().integrity_check !== 'ok') throw new Error('Повреждён backup');
    } finally { check.close(); }
  }
  close() { this.db.close(); }
}
