# 09. Cloudflare Worker

## Что это и зачем

Cloudflare Worker — это маленький серверный скрипт, который:

1. **Хранит push-подписки и watchlist** пользователей PWA и отдаёт **админку**
   подписчиков — потому что у дашборда (статика на GitHub Pages) нет своего
   бэкенда, а где-то хранить подписки нужно.
2. **Показывает живой лог прогона** (блок лога в админке): облачный прогон
   GitHub Actions шлёт весь свой stdout через
   [`scripts/gh_progress_pusher.py`](../../scripts/gh_progress_pusher.py)
   (`source:"github"` + ссылка на run), Mac-резерв — вехи через
   `ops/mac-local-run/progress_pusher.py` (без `source`); оба — батчами на
   `POST /run-progress`. Админка автообновляется — ход парсинга видно из
   браузера и с телефона, лог хранится 14 дней (текущий + предыдущий прогон).
3. **Запускает обновление по расписанию** — cron возвращён в облако
   **05.07.2026** (суды снова пускают иностранные IP; история раскола D2 — в
   [01. Обзор](01-обзор-и-архитектура.md)). LaunchAgent на Mac усыплён и
   оставлен спящим резервом (см.
   [`ops/mac-local-run/README.md`](../../ops/mac-local-run/README.md)).

Код — [`cloudflare-worker/worker.js`](../../cloudflare-worker/worker.js),
конфигурация — [`cloudflare-worker/wrangler.toml`](../../cloudflare-worker/wrangler.toml).
Деплой: `cd cloudflare-worker && wrangler deploy`.

> ⚠️ cron-job.org и аналоги не добавлять по-прежнему. Расписание — только
> Worker-cron (`crons` в `wrangler.toml` + деплой).

## Автозапуск (cron)

`scheduled(event, env)` ([worker.js:1006](../../cloudflare-worker/worker.js#L1006)):

1. Вычисляет текущую дату по МСК (UTC+3).
2. `isHoliday(now)` ([19](../../cloudflare-worker/worker.js#L19)) — **второй щит**:
   режет субботу/воскресенье (`getDay()`) и праздники РФ (`HOLIDAYS_2026`). Если
   праздник — прогон пропускается.
3. Иначе — `POST` на GitHub API
   `…/actions/workflows/update_cases.yml/dispatches` с `ref: "main"` и входом
   `inputs: { smart_skip: "true" }`. Авторизация — `Bearer ${env.GITHUB_PAT}`.

Расписание в `wrangler.toml`: `crons = ["45 3 * * mon-fri"]` = **06:45 МСК,
пн-пт** (применяется только после `wrangler deploy`). Отключить (флип на
Mac-резерв) — вернуть `crons = []` и задеплоить.

> ⚠️ Cloudflare Cron Triggers нумерует дни недели **1=Sun..7=Sat** (не как POSIX).
> Цифровое `1-5` эмпирически срабатывало в т.ч. в воскресенье, поэтому
> используется буквенный `mon-fri`. `isHoliday()` — дополнительная страховка.

Cron всегда передаёт `smart_skip=true` (парсер пропускает нерабочие дни и дела с
известной будущей датой — экономит запросы к ГАС «Правосудие»). Ручной запуск —
из GitHub UI (галка) или из админки: кнопка «Полный прогон» шлёт
`smart_skip:"false"` (парсит всё), «Стандартный прогон» — `smart_skip:"true"`
(как крон).

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
| `/run-progress` | POST | `handleRunProgress` ([345](../../cloudflare-worker/worker.js#L345)) | `PROGRESS_SECRET` или `PUSH_SECRET` (Bearer) | Принять батч строк лога прогона: GitHub Actions (`scripts/gh_progress_pusher.py`, поля `source:"github"` + `link` на run) или Mac (`progress_pusher.py`, без `source`). KV `progress:current`/`progress:prev`, cap 1000 строк, TTL 14 дн. |
| `/admin/run-progress` | GET | `handleAdminRunProgress` ([385](../../cloudflare-worker/worker.js#L385)) | `OWNER_SECRET` | JSON текущего и предыдущего прогона для блока живого лога. |
| `/admin` | GET | `handleAdmin` ([455](../../cloudflare-worker/worker.js#L455)) | `OWNER_SECRET` (в URL) | HTML-админка подписчиков. |
| `/admin/data` | GET | `handleAdminData` ([414](../../cloudflare-worker/worker.js#L414)) | `OWNER_SECRET` | JSON-данные для админки. |
| `/admin/label` | POST | `handleAdminLabel` ([508](../../cloudflare-worker/worker.js#L508)) | `OWNER_SECRET` | Задать имя подписке. |
| `/admin/watchlist` | POST | `handleAdminWatchlist` ([533](../../cloudflare-worker/worker.js#L533)) | `OWNER_SECRET` | Перезаписать чужой watchlist. |
| `/admin/unsubscribe` | POST | `handleAdminUnsubscribe` ([522](../../cloudflare-worker/worker.js#L522)) | `OWNER_SECRET` | Принудительно удалить подписку. |
| `/admin/test-push` | POST | `handleAdminTestPush` ([618](../../cloudflare-worker/worker.js#L618)) | `OWNER_SECRET` | Тестовый push (**отложено** — нужен `VAPID_PRIVATE_KEY` в secret). |

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
`handleAdmin` ([455](../../cloudflare-worker/worker.js#L455)) рендерит HTML
(`renderAdminHtml`, [667](../../cloudflare-worker/worker.js#L667)), внутри JS
тянет `/admin/data` и `cases.json`. По каждой подписке показывает: имя,
устройство, флаг owner, даты создания/входа/обновления watchlist, размер и
раскрываемый список дел со сторонами, а также **журнал последнего push** (из
`last_personal_pushes.json`). Действия: ✏ имя, 📋 редактировать watchlist,
🗑 удалить.

В карточке «Прогоны GitHub Actions» — блок **живого лога прогона**
(`loadProgress` в `admin_page.js`): статус текущего прогона (идёт / завершён +
давность), заголовок по источнику — «Прогон (GitHub Actions)» со ссылкой на run
или «Парсинг на Mac (резерв)», автообновление каждые 15 с, пока прогон не
завершён (батчи пушера уходят раз в ~60 с — экономия KV-writes free-tier,
инцидент «50% лимита» 17.07.2026); предыдущий прогон — под спойлером, завершённый старше суток —
свёрнутый details. Лог сворачивается по фазам «— [N/9] …» (`renderLogGroups`):
у фаз счётчик строк и бейджи ⚠/✖, вручную открытые фазы переживают ререндер,
финальная «Сводка прогона» видна без разворачивания. Источник данных —
`GET /admin/run-progress`; лог заливает облачный workflow
([`scripts/gh_progress_pusher.py`](../../scripts/gh_progress_pusher.py), весь
stdout) или Mac-резерв
([`ops/mac-local-run/progress_pusher.py`](../../ops/mac-local-run/progress_pusher.py),
только вехи).

## Секреты Worker'а

Задаются через `wrangler secret put <NAME>`:

- `GITHUB_PAT` — токен GitHub API: `workflow_dispatch` (cron и
  `/admin/dispatch`) и чтение прогонов (`/admin/gh-runs`).
- `PUSH_SECRET` — авторизация служебных эндпоинтов рассылки (`/subscriptions`,
  `/unsubscribe`), общий с бэкендом; принимается и на `POST /run-progress`
  (им пушит лог облачный workflow, пока в GitHub secrets нет отдельного
  `PROGRESS_SECRET`).
- `OWNER_SECRET` — авторизация `/mark-owner` и админки.
- `PROGRESS_SECRET` — авторизация `POST /run-progress` (живой лог прогона).
  Низкопривилегированный: умеет только дописывать строки прогресса. То же
  значение лежит на Mac в `~/.config/court-monitor/progress_token` (chmod 600,
  вне публичного репозитория).
- `VAPID_PRIVATE_KEY` — нужен только для test-push из админки (сейчас не
  положен, фича отложена).

Как Worker встроен в общий поток (Mac-парсинг → push → replay на GitHub) — см.
[01. Обзор](01-обзор-и-архитектура.md) и [10. CI/CD и эксплуатация](10-ci-cd-и-эксплуатация.md).
