# 09. Cloudflare Worker

## Что это и зачем

Cloudflare Worker — это маленький серверный скрипт, который делает две вещи,
которые иначе делать негде:

1. **Запускает обновление точно по расписанию** (каждое утро в будни) — потому
   что встроенный cron GitHub Actions на бесплатном плане опаздывает на часы.
2. **Хранит push-подписки и watchlist** пользователей PWA и отдаёт **админку**
   подписчиков — потому что у дашборда (статика на GitHub Pages) нет своего
   бэкенда, а где-то хранить подписки нужно.

Код — [`cloudflare-worker/worker.js`](../../cloudflare-worker/worker.js),
конфигурация — [`cloudflare-worker/wrangler.toml`](../../cloudflare-worker/wrangler.toml).
Деплой: `cd cloudflare-worker && wrangler deploy`.

> ⚠️ **Автозапуск — только через этот Worker.** cron-job.org и аналоги не
> добавлять. Любые правки расписания — в `wrangler.toml`, затем `wrangler deploy`.

## Автозапуск (cron)

`scheduled(event, env)` ([worker.js:1006](../../cloudflare-worker/worker.js#L1006)):

1. Вычисляет текущую дату по МСК (UTC+3).
2. `isHoliday(now)` ([19](../../cloudflare-worker/worker.js#L19)) — **второй щит**:
   режет субботу/воскресенье (`getDay()`) и праздники РФ (`HOLIDAYS_2026`). Если
   праздник — прогон пропускается.
3. Иначе — `POST` на GitHub API
   `…/actions/workflows/update_cases.yml/dispatches` с `ref: "main"` и входом
   `inputs: { smart_skip: "true" }`. Авторизация — `Bearer ${env.GITHUB_PAT}`.

Расписание в `wrangler.toml`: `crons = ["45 3 * * mon-fri"]` = **06:45 МСК, пн-пт**.

> ⚠️ Cloudflare Cron Triggers нумерует дни недели **1=Sun..7=Sat** (не как POSIX).
> Цифровое `1-5` эмпирически срабатывало в т.ч. в воскресенье, поэтому
> используется буквенный `mon-fri`. `isHoliday()` — дополнительная страховка.

Cron всегда передаёт `smart_skip=true` (парсер пропускает нерабочие дни и дела с
известной будущей датой — экономит запросы к ГАС «Правосудие»). Ручной запуск из
UI идёт без этого input и парсит всё.

## HTTP API (управление подписками)

Маршрутизатор — `fetch(request, env)` ([1048](../../cloudflare-worker/worker.js#L1048)).
Хранилище — KV-namespace `PUSH_SUBSCRIPTIONS` (биндинг в `wrangler.toml`).
Ключ записи — хвост endpoint браузерного push-сервиса (`endpointToKey`,
[47](../../cloudflare-worker/worker.js#L47)), префикс `sub:`.

| Маршрут | Метод | Обработчик | Авторизация | Назначение |
|---------|-------|-----------|-------------|------------|
| `/subscribe` | POST | `handleSubscribe` ([126](../../cloudflare-worker/worker.js#L126)) | — | Создать/обновить подписку. Пишет `created_at`, `last_seen_at`, `user_agent`. |
| `/watchlist` | POST | `handleSetWatchlist` ([178](../../cloudflare-worker/worker.js#L178)) | — | Обновить watchlist подписки. Канонизирует алиасы → FI-ID, возвращает `canonical`. |
| `/unsubscribe` | POST | `handleUnsubscribe` ([234](../../cloudflare-worker/worker.js#L234)) | `PUSH_SECRET` | Удалить подписку (вызывается автоочисткой из Python). |
| `/subscriptions` | GET | `handleListSubscriptions` ([262](../../cloudflare-worker/worker.js#L262)) | `PUSH_SECRET` | Список подписок для рассылки (`?role=owner` — только владельцы). |
| `/mark-owner` | POST | `handleMarkOwner` ([292](../../cloudflare-worker/worker.js#L292)) | `OWNER_SECRET` | Пометить устройство владельческим (для owner-only push). |
| `/admin` | GET | `handleAdmin` ([383](../../cloudflare-worker/worker.js#L383)) | `OWNER_SECRET` (в URL) | HTML-админка подписчиков. |
| `/admin/data` | GET | `handleAdminData` ([342](../../cloudflare-worker/worker.js#L342)) | `OWNER_SECRET` | JSON-данные для админки. |
| `/admin/label` | POST | `handleAdminLabel` ([436](../../cloudflare-worker/worker.js#L436)) | `OWNER_SECRET` | Задать имя подписке. |
| `/admin/watchlist` | POST | `handleAdminWatchlist` ([461](../../cloudflare-worker/worker.js#L461)) | `OWNER_SECRET` | Перезаписать чужой watchlist. |
| `/admin/unsubscribe` | POST | `handleAdminUnsubscribe` ([450](../../cloudflare-worker/worker.js#L450)) | `OWNER_SECRET` | Принудительно удалить подписку. |
| `/admin/test-push` | POST | `handleAdminTestPush` ([546](../../cloudflare-worker/worker.js#L546)) | `OWNER_SECRET` | Тестовый push (**отложено** — нужен `VAPID_PRIVATE_KEY` в secret). |

CORS разрешён только для `ALLOWED_ORIGIN` и `localhost:8081` (`corsHeaders`,
[34](../../cloudflare-worker/worker.js#L34)).

## Метаданные подписки в KV

Каждая запись хранит: `created_at` (один раз), `last_seen_at` (на каждом
`/subscribe`), `last_watchlist_update_at` (на `/watchlist`), `user_agent`,
`label`, `is_owner`, `watchlist`. Канонизация watchlist использует ту же логику,
что и бэкенд (`wnBuildAliasToCanonical`, [67](../../cloudflare-worker/worker.js#L67),
с кэшем `getAliasMapCached`, [94](../../cloudflare-worker/worker.js#L94), читающим
`cases.json` с GitHub Pages).

## Админка подписчиков

URL: `https://court-monitor-trigger.7selivanov-a.workers.dev/admin?secret=<OWNER_SECRET>`.
`handleAdmin` ([383](../../cloudflare-worker/worker.js#L383)) рендерит HTML
(`renderAdminHtml`, [595](../../cloudflare-worker/worker.js#L595)), внутри JS
тянет `/admin/data` и `cases.json`. По каждой подписке показывает: имя,
устройство (`detectDevice`, [709](../../cloudflare-worker/worker.js#L709)), флаг
owner, даты создания/входа/обновления watchlist, размер и раскрываемый список дел
со сторонами, а также **журнал последнего push** (из `last_personal_pushes.json`,
`renderLastPush`, [810](../../cloudflare-worker/worker.js#L810)). Действия: ✏ имя,
📋 редактировать watchlist, 🗑 удалить.

## Секреты Worker'а

Задаются через `wrangler secret put <NAME>`:

- `GITHUB_PAT` — токен для `workflow_dispatch` (запуск GitHub Actions).
- `PUSH_SECRET` — авторизация служебных эндпоинтов рассылки (`/subscriptions`,
  `/unsubscribe`), общий с бэкендом.
- `OWNER_SECRET` — авторизация `/mark-owner` и админки.
- `VAPID_PRIVATE_KEY` — нужен только для test-push из админки (сейчас не
  положен, фича отложена).

Как Worker встроен в общий поток (cron → GitHub Actions → парсер) — см.
[01. Обзор](01-обзор-и-архитектура.md) и [10. CI/CD и эксплуатация](10-ci-cd-и-эксплуатация.md).
