const IMPORTS = { dump: ['import_cases.yml', 'dump'], case: ['add_cases.yml', 'case'], writ_waiver: ['mark_writ.yml', 'writ'] };
const TERMINAL = new Set(['done', 'cancelled']);

export class Jobs {
  constructor(store) { this.store = store; }
  async enqueueImport(kind, uuid) {
    this.store.assertTransaction();
    const spec = IMPORTS[kind];
    if (!spec || !/^[0-9a-f-]{36}$/.test(uuid)) throw new Error('Неверное задание');
    const bodyKey = `import:${spec[1]}:${uuid}`;
    if (await this.store.kv.get(bodyKey) === null) throw new Error('Тело задания отсутствует');
    const logs = this.store.db.prepare("SELECT key FROM kv WHERE key LIKE 'import:log:%' AND key LIKE ?")
      .all(`%|${uuid}`);
    if (logs.length !== 1) throw new Error('Нужна ровно одна запись журнала');
    const now = this.store.clock();
    this.store.db.prepare(`INSERT INTO jobs
      (id,workflow,inputs,body_key,log_key,state,created_at,updated_at) VALUES (?,?,?,?,?,'queued',?,?)
      ON CONFLICT(id) DO NOTHING`).run(uuid, spec[0], '{}', bodyKey, logs[0].key, now, now);
    return { ok: true, key: uuid, executor: 'vps' };
  }
  async dispatch(workflow, inputs) {
    this.store.assertTransaction();
    const entry = Object.entries(IMPORTS).find(([, spec]) => spec[0] === workflow);
    if (!entry) return { ok: false, error: 'local_executor_not_ready',
      detail: 'Подготовка выпусков и рассылка ещё не перенесены; задание не запускалось.' };
    const key = workflow === 'import_cases.yml' ? inputs.dump_key : inputs.job_key;
    const prefix = `import:${entry[1][1]}:`;
    if (typeof key !== 'string' || !key.startsWith(prefix)) return { ok: false, error: 'invalid_job_key' };
    return this.enqueueImport(entry[0], key.slice(prefix.length));
  }
  async reconcile() {
    // После защищённого импорта KV: перенос ВСЕХ незавершённых UUID, включая ИЛ.
    return this.store.transaction(async () => {
      let cursor = '', count = 0;
      do {
        const page = await this.store.kv.list({ prefix: 'import:log:', cursor });
        for (const key of page.keys) {
          const record = await this.store.kv.get(key.name, 'json');
          const kind = record.kind || 'dump';
          if (!IMPORTS[kind] || TERMINAL.has(record.status) && ![
            record.fetch_error, record.fetch_fail, record.card_failed].some(n => n > 0)) continue;
          if (record.status === 'failed' && String(record.error).includes('повтор не поможет')) continue;
          const body = await this.store.kv.get(`import:${IMPORTS[kind][1]}:${record.uuid}`);
          if (body === null) continue; // Не оживляем истёкшее тело; журнал остаётся.
          await this.enqueueImport(kind, record.uuid); count++;
        }
        cursor = page.cursor;
      } while (cursor);
      return count;
    });
  }
  async claim({ workflows = ['mark_writ.yml'], leaseMs = 60000 } = {}) {
    if (!Number.isInteger(leaseMs) || leaseMs < 1000 || leaseMs > 900000) throw new Error('Неверный срок аренды');
    if (!workflows.length || workflows.some(w => !Object.values(IMPORTS).some(x => x[0] === w))) throw new Error('Неверный исполнитель');
    return this.store.transaction(async () => {
      const now = this.store.clock();
      // По таймауту нельзя утверждать, что применение не состоялось.
      const uncertain = this.store.db.prepare("SELECT * FROM jobs WHERE state='leased' AND lease_until<=?").all(now);
      for (const previous of uncertain) {
        this.store.db.prepare("UPDATE jobs SET state='uncertain',updated_at=? WHERE id=?").run(now,previous.id);
        await this.#log(previous,'failed','Результат прерванного местного задания не определён; требуется сверка сохранённого результата.');
      }
      const row = this.store.db.prepare(`SELECT * FROM jobs WHERE state='queued'
        AND workflow IN (${workflows.map(() => '?').join(',')}) ORDER BY created_at,id LIMIT 1`).get(...workflows);
      if (!row) return null;
      const body = await this.store.kv.get(row.body_key);
      if (body === null) {
        this.store.db.prepare("UPDATE jobs SET state='expired',updated_at=? WHERE id=?").run(now, row.id);
        await this.#log(row,'failed','Тело задания истекло: повтор не поможет — вставьте выдачу заново.');
        return null;
      }
      const generation = row.generation + 1;
      this.store.db.prepare("UPDATE jobs SET state='leased',generation=?,lease_until=?,updated_at=? WHERE id=?")
        .run(generation, now + leaseMs, now, row.id);
      await this.#log(row, 'started');
      return { ...row, generation, state: 'leased', body };
    });
  }
  #owned(id, generation, states) {
    const row = this.store.db.prepare('SELECT * FROM jobs WHERE id=?').get(id);
    if (!row || row.generation !== generation || !states.includes(row.state)) throw new Error('Устаревший исполнитель');
    return row;
  }
  async applied(id, generation, receipt) {
    return this.store.transaction(() => {
      this.#owned(id, generation, ['leased', 'uncertain']);
      if (receipt?.region !== this.store.region || receipt?.job_id !== id || !/^[a-f0-9]{64}$/.test(receipt?.sha256)) {
        throw new Error('Нет проверенного результата применения');
      }
      this.store.db.prepare("UPDATE jobs SET state='applied',receipt=?,updated_at=? WHERE id=?")
        .run(JSON.stringify(receipt), this.store.clock(), id);
    });
  }
  async published(id, generation, receipt) {
    return this.store.transaction(() => {
      const row = this.#owned(id, generation, ['applied', 'published']);
      const applied = JSON.parse(row.receipt);
      if (receipt?.sha256 !== applied.sha256 || receipt?.job_id !== id || receipt?.region !== this.store.region
          || receipt?.verified !== true) throw new Error('Публикация не подтверждена');
      this.store.db.prepare("UPDATE jobs SET state='published',updated_at=? WHERE id=?").run(this.store.clock(), id);
    });
  }
  async finish(id, generation, report) {
    return this.store.transaction(async () => {
      const row = this.#owned(id, generation, ['published', 'done']);
      if (row.state === 'done') return { duplicate: true };
      // report — вызов общего обработчика Worker с прежним форматом итогов.
      const response = await report(row);
      if (!response.ok) throw new Error('Журнал результата не принял подтверждение');
      this.store.db.prepare("UPDATE jobs SET state='done',lease_until=NULL,updated_at=? WHERE id=?")
        .run(this.store.clock(), id);
      return { done: true };
    });
  }
  async #log(row, status, error = '') {
    const record = await this.store.kv.get(row.log_key, 'json');
    if (!record) throw new Error('Журнал задания потерян');
    record.status = status; record.executor = 'vps'; record.updated_at = new Date(this.store.clock()).toISOString();
    if (error) record.error = error;
    await this.store.kv.put(row.log_key, JSON.stringify(record), {
      expirationTtl: 90 * 86400, metadata: { queue_pending: status !== 'done' },
    });
  }
  async runs() {
    this.store.assertTransaction();
    const runs = this.store.db.prepare('SELECT * FROM jobs ORDER BY created_at DESC,id LIMIT 20').all().map(row => ({
      name: row.workflow, path: row.workflow, status: ['done','expired','cancelled'].includes(row.state) ? 'completed' : row.state === 'queued' ? 'queued' : 'in_progress',
      conclusion: row.state === 'done' ? 'success' : row.state === 'expired' ? 'failure' : '',
      run_started_at: new Date(row.created_at).toISOString(), updated_at: new Date(row.updated_at).toISOString(),
      html_url: '', run_number: row.created_at, event: 'local', source: 'vps', local_state: row.state,
    }));
    return { runs, main_run: null, last_run: runs[0] || null, next_cron_at: null,
      today_non_working: [0, 6].includes(new Date(this.store.clock()).getUTCDay()), executor: 'vps' };
  }
}
