// ── Файл территории (единственный фронтовый файл, который правит форк) ──────
// Пер-инстансные значения фронта: URL Cloudflare Worker территории и её
// VAPID-публичный ключ (парный приватному в секретах Worker'а и GitHub).
// app.js читает window.REGION_FRONT и остаётся общим для всех территорий —
// merge из эталона этот файл не трогает (merge-driver ours в форке).
// Подключается в sberbank_dashboard.html ПЕРЕД app.js.
window.REGION_FRONT = {
  PUSH_WORKER_URL: 'https://court-monitor-trigger.7selivanov-a.workers.dev',
  VAPID_PUBLIC_KEY: 'BOQM36gf407_Ebe_r-eDOJ8pjrlhhFlNefhwzmZMRdpgj6DPogIkmcWWxzoeDSlK9fzdNanoMYBLEQfKHg9cHNU',
};
