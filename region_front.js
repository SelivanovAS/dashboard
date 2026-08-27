// ── Файл территории (единственный фронтовый файл, который правит форк) ──────
// Пер-инстансные значения фронта: URL Cloudflare Worker территории и её
// VAPID-публичный ключ (парный приватному в секретах Worker'а и GitHub).
// app.js читает window.REGION_FRONT и остаётся общим для всех территорий —
// merge из эталона этот файл не трогает (merge-driver ours в форке).
// Подключается в sberbank_dashboard.html ПЕРЕД app.js.
window.REGION_FRONT = {
  // Основной адрес — свой домен (27.08.2026): часть операторов связи режет
  // *.workers.dev по имени (SNI), с их сетей не работали синк и админка.
  PUSH_WORKER_URL: 'https://api-hmao.delosud.ru',
  // Фолбэк-адреса ТОГО ЖЕ Worker'а (перебор в app.js/workerFetch при
  // недоступности основного). Пустой PUSH_WORKER_URL по-прежнему значит
  // «синк выключен» — фолбэки при нём не используются. Совпадающий с
  // основным адрес отфильтровывается сам (дедуп в app.js).
  PUSH_WORKER_FALLBACKS: ['https://court-monitor-trigger.7selivanov-a.workers.dev'],
  VAPID_PUBLIC_KEY: 'BOQM36gf407_Ebe_r-eDOJ8pjrlhhFlNefhwzmZMRdpgj6DPogIkmcWWxzoeDSlK9fzdNanoMYBLEQfKHg9cHNU',
  // Подпись региона в шапке до загрузки данных (данные перекрывают её
  // значением name_short из блока region).
  REGION_LABEL: 'ХМАО-Югра',
  // STORAGE_NS — неймспейс localStorage территории (фронты живут на одном
  // origin github.io, хранилище общее). Эталон ХМАО NS НЕ задаёт: его ключи
  // исторические, без префикса. Форк территории ОБЯЗАН задать свой короткий
  // идентификатор (например, 'ural') — иначе его звёзды/заметки перемешаются
  // с ХМАО в общем браузере. Обрабатывается в app.js (lsKey + одноразовая
  // миграция-копия bare-ключей в неймспейс).
  // STORAGE_NS: 'ural',
};
