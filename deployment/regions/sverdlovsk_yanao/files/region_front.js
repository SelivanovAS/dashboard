// ── Файл территории «Урал» (Свердловская обл. + ЯНАО) ───────────────────────
// Пер-инстансные значения фронта. Push ВКЛЮЧЁН 16.07.2026: Worker
// court-monitor-ural задеплоен, VAPID-пара своя (public здесь и в [vars]
// wrangler.toml форка; private — в GH-секрете VAPID_PRIVATE_KEY форка и
// секрете Worker'а). app.js читает window.REGION_FRONT и остаётся общим.
window.REGION_FRONT = {
  // Основной адрес — свой домен (27.08.2026): часть операторов связи режет
  // *.workers.dev по имени (SNI), с их сетей не работали синк и админка.
  PUSH_WORKER_URL: 'https://api-ural.delosud.ru',
  // Фолбэк-адреса ТОГО ЖЕ Worker'а (перебор в app.js/workerFetch при
  // недоступности основного). Пустой PUSH_WORKER_URL по-прежнему значит
  // «синк выключен» — фолбэки при нём не используются.
  // ⚠️ Шлюз api2-ural.delosud.ru здесь НЕ держим (был в v185, убран
  // 27.08.2026): тот же молодой delosud.ru, SNI-фильтр МТС режет наравне с
  // основным; канал VPS→CF душит большие POST (дампы → 502). Сам VPS —
  // инфраструктура на будущее, во фронте от него лишний таймаут.
  PUSH_WORKER_FALLBACKS: ['https://court-monitor-ural.7selivanov-a.workers.dev'],
  VAPID_PUBLIC_KEY: 'BHjYv0QmRYDkdwqvERpsbWi8wWkmwqMkn78Q-TN9gK7awAVjeQ7u2LebeUKFyiT_BTlJOVD3YB6E3MLUKe43d7k',
  // Подпись региона в шапке до загрузки данных (данные перекрывают её
  // значением name_short из блока region).
  REGION_LABEL: 'ЕКБ + ЯНАО',
  // Неймспейс localStorage территории: фронты живут на одном origin
  // github.io, без префикса звёзды/заметки/owner-секрет перемешивались бы
  // с ХМАО в общем браузере. app.js (lsKey) добавит 'ural:' ко всем ключам
  // и один раз скопирует исторические bare-значения в неймспейс.
  STORAGE_NS: 'ural',
};
