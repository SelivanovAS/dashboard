# 09. Cloudflare Worker

## Что это и зачем

Cloudflare Worker — это маленький серверный скрипт, который:

1. **Хранит push-подписки и watchlist** пользователей PWA и отдаёт **админку**
   подписчиков — потому что у дашборда (статика на GitHub Pages) нет своего
   бэкенда, а где-то хранить подписки нужно.
2. **Принимает лог прогона** (`POST /run-progress`): облачный прогон
   GitHub Actions шлёт весь свой stdout через
   [`scripts/gh_progress_pusher.py`](../../scripts/gh_progress_pusher.py)
   (`source:"github"` + ссылка на run), Mac-резерв — вехи через
   `ops/mac-local-run/progress_pusher.py` (без `source`); оба — батчами,
   лог хранится в KV 14 дней (текущий + предыдущий прогон). ⚠️ Блок живого
   лога из админки удалён 29.07.2026 (см. раздел «Админка» ниже) — канал
   пишется без UI-читателя, логи смотрятся на вкладке Actions GitHub.
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

`scheduled(event, env)` ([worker.js:1632](../../cloudflare-worker/worker.js#L1632)):

1. Вычисляет текущую дату по МСК (UTC+3).
2. `isHoliday(now)` ([32](../../cloudflare-worker/worker.js#L32)) — **второй щит**:
   режет субботу/воскресенье (`getDay()`) и праздники РФ (`HOLIDAYS_2026`). Если
   праздник — прогон пропускается.
3. Иначе — `POST` на GitHub API
   `…/actions/workflows/update_cases.yml/dispatches` с `ref: "main"` и входом
   `inputs: { smart_skip: "true" }`. Авторизация — `Bearer ${env.GITHUB_PAT}`.

Расписание в `wrangler.toml`: `crons = ["30 3 * * mon-fri"]` = **06:30 МСК,
08:30 ХМАО, пн-пт** (применяется только после `wrangler deploy`). Отключить (флип на
Mac-резерв) — вернуть `crons = []` и задеплоить.

> ⚠️ Cloudflare Cron Triggers нумерует дни недели **1=Sun..7=Sat** (не как POSIX).
> Цифровое `1-5` эмпирически срабатывало в т.ч. в воскресенье, поэтому
> используется буквенный `mon-fri`. `isHoliday()` — дополнительная страховка.

Cron всегда передаёт `smart_skip=true` (парсер пропускает нерабочие дни и дела с
известной будущей датой — экономит запросы к ГАС «Правосудие»). Ручной запуск —
из GitHub UI (галка) или из админки: единственная кнопка «Запустить прогон»
шлёт `smart_skip:"true"` (ровно как крон).

> **02.08.2026, решение юриста:** кнопка «Полный прогон» (`smart_skip:"false"`)
> из админки УДАЛЕНА — тяжёлый обход всех активных дел нужен редко и
> запускается из GitHub Actions (Run workflow → снять галку `smart_skip`).
> `DISPATCH_WORKFLOWS` по-прежнему разрешает оба входа: белый список Worker'а
> правки не требовал.

В нерабочий день «Запустить прогон» завершился бы за секунды («нерабочий
день РФ, парсинг пропущен»), поэтому с 02.08.2026 кнопка сперва спрашивает
«прогнать всё равно?» и при согласии добавляет `ignore_calendar:"true"` —
календарный гейт снят, пер-кейсовый smart-skip сохранён. Выходной ли сегодня,
страница не считает сама: `GET /admin/gh-runs` отдаёт поле `today_non_working`
(`todayNonWorking()` → `isHoliday()` по МСК, тем же календарём, что у крона).
Третьей копии производственного календаря в проекте быть не должно — их и так
две (`worker.js` и `textutil.py`).

## HTTP API (управление подписками)

Маршрутизатор — `fetch(request, env)` ([1675](../../cloudflare-worker/worker.js#L1675)).
Хранилище — KV-namespace `PUSH_SUBSCRIPTIONS` (биндинг в `wrangler.toml`).
Ключ записи — хвост endpoint браузерного push-сервиса (`endpointToKey`,
[60](../../cloudflare-worker/worker.js#L60)), префикс `sub:`.

| Маршрут | Метод | Обработчик | Авторизация | Назначение |
|---------|-------|-----------|-------------|------------|
| `/subscribe` | POST | `handleSubscribe` ([171](../../cloudflare-worker/worker.js#L171)) | — | Создать/обновить подписку. Пишет `created_at`, `last_seen_at`, `user_agent`. |
| `/watchlist` | POST | `handleSetWatchlist` ([239](../../cloudflare-worker/worker.js#L239)) | — | Обновить watchlist подписки. Канонизирует алиасы → FI-ID, возвращает `canonical`. |
| `/unsubscribe` | POST | `handleUnsubscribe` ([295](../../cloudflare-worker/worker.js#L295)) | `PUSH_SECRET` | Удалить подписку (вызывается автоочисткой из Python). |
| `/subscriptions` | GET | `handleListSubscriptions` ([323](../../cloudflare-worker/worker.js#L323)) | `PUSH_SECRET` | Список подписок для рассылки (`?role=owner` — только владельцы). |
| `/mark-owner` | POST | `handleMarkOwner` ([353](../../cloudflare-worker/worker.js#L353)) | `OWNER_SECRET` | Пометить устройство владельческим (для owner-only push). |
| `/run-progress` | POST | `handleRunProgress` ([410](../../cloudflare-worker/worker.js#L410)) | `PROGRESS_SECRET` или `PUSH_SECRET` (Bearer) | Принять батч строк лога прогона: GitHub Actions (`scripts/gh_progress_pusher.py`, поля `source:"github"` + `link` на run) или Mac (`progress_pusher.py`, без `source`). KV `progress:current`/`progress:prev`, cap 1000 строк, TTL 14 дн. |
| `/admin/run-progress` | GET | `handleAdminRunProgress` ([464](../../cloudflare-worker/worker.js#L464)) | `OWNER_SECRET` | JSON текущего и предыдущего прогона. С 29.07.2026 админка его не зовёт (блок живого лога удалён) — эндпоинт оставлен для ручной отладки. |
| `/admin` | GET | `handleAdmin` ([556](../../cloudflare-worker/worker.js#L556)) | `OWNER_SECRET` (в URL) | HTML-админка подписчиков. |
| `/admin/data` | GET | `handleAdminData` ([516](../../cloudflare-worker/worker.js#L516)) | `OWNER_SECRET` | JSON-данные для админки. |
| `/admin/label` | POST | `handleAdminLabel` ([606](../../cloudflare-worker/worker.js#L606)) | `OWNER_SECRET` | Задать имя подписке. |
| `/admin/watchlist` | POST | `handleAdminWatchlist` ([631](../../cloudflare-worker/worker.js#L631)) | `OWNER_SECRET` | Перезаписать чужой watchlist. |
| `/admin/unsubscribe` | POST | `handleAdminUnsubscribe` ([620](../../cloudflare-worker/worker.js#L620)) | `OWNER_SECRET` | Принудительно удалить подписку. |
| `/admin/test-push` | POST | `handleAdminTestPush` ([719](../../cloudflare-worker/worker.js#L719)) | `OWNER_SECRET` | Тестовый push (**отложено** — нужен `VAPID_PRIVATE_KEY` в secret). |

CORS разрешён только для `ALLOWED_ORIGIN` и `localhost:8081` (`corsHeaders`,
[47](../../cloudflare-worker/worker.js#L47)).

## Метаданные подписки в KV

Каждая запись хранит: `created_at` (один раз), `last_seen_at` (на каждом
`/subscribe`), `last_watchlist_update_at` (на `/watchlist`), `user_agent`,
`label`, `is_owner`, `watchlist`. Канонизация watchlist использует ту же логику,
что и бэкенд (`wnBuildAliasToCanonical`, [104](../../cloudflare-worker/worker.js#L104),
с кэшем `getAliasMapCached`, [139](../../cloudflare-worker/worker.js#L139), читающим
`cases.json` с GitHub Pages).

## Админка подписчиков

URL: `https://court-monitor-trigger.7selivanov-a.workers.dev/admin?secret=<OWNER_SECRET>`.
`handleAdmin` ([556](../../cloudflare-worker/worker.js#L556)) рендерит HTML
(`renderAdminHtml`, [34](../../cloudflare-worker/admin_page.js#L34)), внутри JS
тянет `/admin/data` и `cases.json`. По каждой подписке показывает: имя,
устройство, флаг owner, даты создания/входа/обновления watchlist, размер и
раскрываемый список дел со сторонами, а также **журнал последнего push** (из
`last_personal_pushes.json`). Действия: ✏ имя, 📋 редактировать watchlist,
🗑 удалить.

⚠️ **Каркас страницы — вкладки (02.08.2026).** Чипы шапки переключают панели,
показана ровно одна секция; пульт плиток остаётся сверху вне вкладок.
Состояние — в hash формата `#tab-<id>` (голый id секции вызывал у Chrome
отложенный «прыжок к фрагменту» после `replaceState`), `history.replaceState`.
Оператор открывается на вкладке «Импорт» и больше не видит ни карточку
«Парсинг исков банка», ни плитки «Дайджест»/«Автозапуск» — `loadStaticData`
ему эти файлы вообще не запрашивает. **С 23.08.2026 операторская доведена
разбором** (см. «Админка подписчиков» в [CLAUDE.md](../../CLAUDE.md)): порядок
карточек вкладки «Импорт» задаёт роль (оператору дампы первыми, точечное
добавление — свёрткой ниже), имя оператора — одно поле в шапке секции (вне
`.imp-form`, иначе на территории без капчёвых судов оно пряталось бы вместе с
ней), вместо плитки «Парсеры» у него плитка «Карточки судов» (parse_health
описывает только `courts_for_search`, то есть суды БЕЗ капчи — не его), а
светофор свежести умеет набор «мои суды» (localStorage `admin_my_courts`,
пустой = все). Карточки подписчиков свёрнуты по
умолчанию (`<details>`), состояние раскрытия пишется по клику на строку, а не
по событию `toggle`. Подробности и ловушки — в разделе «Админка подписчиков»
[CLAUDE.md](../../CLAUDE.md).

⚠️ **С 29.07.2026 карточка «Прогоны GitHub Actions» урезана до «Запуск
прогона»** (решение юриста): список последних 8 runs и блок живого лога из
админки удалены — статусы и логи смотрятся на вкладке Actions GitHub.
Остались кнопки запуска, метка следующего крона и плитки пульта «Последний
прогон»/«Автозапуск» (их питает прежний `GET /admin/gh-runs`). Канал лога
при этом жив целиком: `POST /run-progress` (заливает облачный workflow —
[`scripts/gh_progress_pusher.py`](../../scripts/gh_progress_pusher.py), весь
stdout — или Mac-резерв
[`ops/mac-local-run/progress_pusher.py`](../../ops/mac-local-run/progress_pusher.py),
только вехи) и `GET /admin/run-progress` работают, лог лежит в KV 14 дней —
UI-читателя у него просто нет.

Рядом — карточка **«Парсинг исков банка»** (с 29.07.2026; с 02.08.2026 —
только owner: оператору это не его трек, у него карточка скрыта ролью и файл
не запрашивается):
пер-кейсовый отчёт последнего прогона по bank-треку из
`data/bank_parse_report.json` (пишет `BankParseReport`, фаза 7c `main_json`;
URL — `bankParseUrl` из `adminPageConfig()`). Группы по исходам: ошибки
загрузки (капча/блок/HTTP/пустая шелуха), «без карточки», «вне очереди» —
раскрыты; «спарсено» и пропуски (недельный ритм ИЛ, будущее заседание,
прочее) — свёрнуты. Внутри группы строки рендерятся порциями по 30
(`BP_CHUNK`, кнопка «Показать ещё») — на Урале дел будут тысячи. Русские
причины приходят готовыми из Python (`skip_reason_ru`, `_OUTCOME_RU`).
Файла нет (404, трек выключен) → карточка скрыта.

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
