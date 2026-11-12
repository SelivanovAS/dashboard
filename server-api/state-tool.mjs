import { readFile, writeFile, open, chmod } from 'node:fs/promises';
import { Store } from './store.mjs';

// Только локальные защищённые файлы. Здесь нет Cloudflare API или авторизации.
const [command,region,database,path] = process.argv.slice(2);
if (!['export','restore','backup','check'].includes(command) || !path?.startsWith('/')) {
  throw new Error('state-tool.mjs export|restore|backup|check REGION /abs/state.sqlite /abs/snapshot');
}
process.umask(0o077);
const store = new Store(database,region);
try {
  if (command === 'export') {
    const snapshot = await store.snapshot();
    const file = await open(path,'wx',0o600);
    try { await file.writeFile(JSON.stringify(snapshot)); await file.sync(); } finally { await file.close(); }
    process.stdout.write(JSON.stringify({ keys:snapshot.entries.length, sha256:snapshot.sha256 })+'\n');
  } else if (command === 'restore') {
    const result = await store.restore(JSON.parse(await readFile(path,'utf8')));
    process.stdout.write(JSON.stringify(result)+'\n');
  } else if (command === 'backup') {
    await store.backup(path); process.stdout.write('backup: ok\n');
  } else {
    const snapshot = await store.snapshot();
    const expected = JSON.parse(await readFile(path,'utf8'));
    if (snapshot.sha256 !== expected.sha256) throw new Error('Снимки отличаются');
    process.stdout.write('snapshot: ok\n');
  }
} finally { store.close(); }
