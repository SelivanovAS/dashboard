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

`scheduled(event, env)` ([worker.js:2485](../../cloudflare-worker/worker.js#L2485)):

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

Маршрутизатор — `fetch(request, env)` ([2528](../../cloudflare-worker/worker.js#L2528)).
Хранилище — KV-namespace `PUSH_SUBSCRIPTIONS` (биндинг в `wrangler.toml`).
Ключ записи — хвост endpoint браузерного push-сервиса (`endpointToKey`,
[60](../../cloudflare-worker/worker.js#L60)), префикс `sub:`.

| Маршрут | Метод | Обработчик | Авторизация | Назначение |
|---------|-------|-----------|-------------|------------|
| `/subscribe` | POST | `handleSubscribe` ([555](../../cloudflare-worker/worker.js#L555)) | — | Создать/обновить подписку. Пишет `created_at`, `last_seen_at`, `user_agent`. |
| `/watchlist` | POST | `handleSetWatchlist` ([632](../../cloudflare-worker/worker.js#L632)) | — | Обновить watchlist подписки. Канонизирует алиасы → FI-ID, возвращает `canonical`. |
| `/unsubscribe` | POST | `handleUnsubscribe` ([1083](../../cloudflare-worker/worker.js#L1083)) | `PUSH_SECRET` | Удалить подписку (вызывается автоочисткой из Python). |
| `/subscriptions` | GET | `handleListSubscriptions` ([1111](../../cloudflare-worker/worker.js#L1111)) | `PUSH_SECRET` | Список подписок для рассылки (`?role=owner` — только владельцы). |
| `/mark-owner` | POST | `handleMarkOwner` ([1144](../../cloudflare-worker/worker.js#L1144)) | `OWNER_SECRET` | Пометить устройство владельческим (для owner-only push). |
| `/run-progress` | POST | `handleRunProgress` ([1201](../../cloudflare-worker/worker.js#L1201)) | `PROGRESS_SECRET` или `PUSH_SECRET` (Bearer) | Принять батч строк лога прогона: GitHub Actions (`scripts/gh_progress_pusher.py`, поля `source:"github"` + `link` на run) или Mac (`progress_pusher.py`, без `source`). KV `progress:current`/`progress:prev`, cap 1000 строк, TTL 14 дн. |
| `/admin/run-progress` | GET | `handleAdminRunProgress` ([1255](../../cloudflare-worker/worker.js#L1255)) | `OWNER_SECRET` | JSON текущего и предыдущего прогона. С 29.07.2026 админка его не зовёт (блок живого лога удалён) — эндпоинт оставлен для ручной отладки. |
| `/admin` | GET | `handleAdmin` ([1371](../../cloudflare-worker/worker.js#L1371)) | `OWNER_SECRET` (в URL) | HTML-админка подписчиков. |
| `/admin/data` | GET | `handleAdminData` ([1307](../../cloudflare-worker/worker.js#L1307)) | `OWNER_SECRET` | JSON-данные для админки. |
| `/admin/label` | POST | `handleAdminLabel` ([1421](../../cloudflare-worker/worker.js#L1421)) | `OWNER_SECRET` | Задать имя подписке. |
| `/admin/watchlist` | POST | `handleAdminWatchlist` ([1446](../../cloudflare-worker/worker.js#L1446)) | `OWNER_SECRET` | Перезаписать чужой watchlist. |
| `/admin/unsubscribe` | POST | `handleAdminUnsubscribe` ([1435](../../cloudflare-worker/worker.js#L1435)) | `OWNER_SECRET` | Принудительно удалить подписку. |
| `/admin/test-push` | POST | `handleAdminTestPush` ([1572](../../cloudflare-worker/worker.js#L1572)) | `OWNER_SECRET` | Тестовый push (**отложено** — нужен `VAPID_PRIVATE_KEY` в secret). |
| `/profile/link-code` | POST | `handleProfileLinkCode` ([720](../../cloudflare-worker/worker.js#L720)) | знание uuid | Код связывания устройств (профиля нет → создаёт из набора устройства). |
| `/profile/link` | POST | `handleProfileLink` ([784](../../cloudflare-worker/worker.js#L784)) | код | Обмен кода на profile_id; union наборов; код сжигается. |
| `/profile/get` | POST | `handleProfileGet` ([834](../../cloudflare-worker/worker.js#L834)) | знание uuid | Чтение профильного watchlist (старт страницы). POST — uuid не светится в URL. |
| `/profile/watchlist` | POST | `handleProfileSetWatchlist` ([860](../../cloudflare-worker/worker.js#L860)) | знание uuid | Запись набора с LWW: устаревший `base_ts` → 409 + серверный набор. |
| `/profile/unlink` | POST | `handleProfileUnlink` ([907](../../cloudflare-worker/worker.js#L907)) | знание endpoint | Отвязать устройство; набор уносится в `sub.watchlist`; профиль живёт. |
| `/profile/calendar-token` | POST | `handleProfileCalendarToken` | знание uuid | Выдача/перевыпуск токена календарного фида (без uuid — создаёт профиль из набора, зеркало link-code). Идемпотентен; `regenerate:true` — перевыпуск. |
| `/calendar/<token>.ics` | GET | `handleCalendarFeed` | знание токена | Персональный iCalendar-фид «Мои заседания» по watchlist профиля; поллится календарём клиента. |

CORS разрешён только для `ALLOWED_ORIGIN` и `localhost:8081` (`corsHeaders`,
[47](../../cloudflare-worker/worker.js#L47)).

**Календарный фид (webcal, 29.08.2026):** `profile_id` в URL не светится —
у фида производный read-only `feed_token` (поле профиля + индекс
`calfeed:<token>` → `{profile_id}`, оба без TTL; отзыв = перевыпуск, токен
пишется через `putProfile` и НЕ трогает LWW-штамп `updated_at`). Фид: 2 KV
reads на поллинг, 0 writes/lists; пустой watchlist → валидный пустой
VCALENDAR (200); недоступный cases.json → 503 + Retry-After (пустой ответ
стёр бы события у подписчика); заголовки `text/calendar; charset=utf-8` +
`Cache-Control: private, max-age=900`. UID событий =
`<canon>--<stage>@<host>` — стабилен при переносах заседаний (событие
обновляется, а не дублируется). ICS-строки свёрнуты по 75 октетов
(`icsFold`, кириллица не режется посреди code point), склейка CRLF. TZ и имя
календаря — `CAL_TZID`/`CAL_TZ_OFFSET_MIN`/`CAL_FEED_NAME` в `[vars]`
(дефолты — Asia/Yekaterinburg, «Мои заседания»). Полная модель — CLAUDE.md,
раздел «Календарный фид»; стражи — `scripts/tests/test_calendar_feed.py`.

## Метаданные подписки в KV

Каждая запись хранит: `created_at` (один раз), `last_seen_at` (на каждом
`/subscribe`), `last_watchlist_update_at` (на `/watchlist`), `user_agent`,
`label`, `is_owner`, `watchlist`, с 26.08.2026 — опциональный `profile_id`
(связка с профилем синхронизации). Канонизация watchlist использует ту же логику,
что и бэкенд (`wnBuildAliasToCanonical`, [234](../../cloudflare-worker/worker.js#L234),
с кэшем `getAliasMapCached`, [284](../../cloudflare-worker/worker.js#L284), читающим
`cases.json` с GitHub Pages).

**Профили синхронизации (26.08.2026, пока только ХМАО):** `profile:<uuid>` =
общий watchlist устройств юриста (`updated_at` в мс — LWW-штамп набора, ставит
только Worker; записи БЕЗ TTL), `paircode:<6 цифр>` — одноразовый код
связывания (TTL 600 с). Привязанным подпискам выдачи `/subscriptions` и
`/admin/data` подставляют ПРОФИЛЬНЫЙ watchlist (`resolveProfilesInto`,
[165](../../cloudflare-worker/worker.js#L165)) — поэтому delivery.py о
профилях не знает вовсе; `/admin/data` отвечает `{subs, profiles}` (ключ
`subs` — контракт `scripts/audit_watchlists.py`). Legacy `/watchlist` и
`/admin/watchlist` при `sub.profile_id` пишут в профиль. Подробная модель —
CLAUDE.md, раздел «Синхронизация подписок между устройствами».

## Адреса Worker'а (custom domain, 27.08.2026)

Часть операторов связи режет `*.workers.dev` по имени (SNI) — с их сетей не
работали синк подписок, push и админка. Worker привязан к своему домену:
`api-hmao.delosud.ru` (форк Урала — `api-ural.delosud.ru`); workers.dev-адрес
ЖИВ и остаётся фолбэком (в wrangler.toml — явный `workers_dev = true`: при
появлении `routes` wrangler иначе гасит его молча). Зона `delosud.ru` живёт в
том же аккаунте Cloudflare (Free), NS у reg.ru; ⚠️ у зоны выключен ECH
(RU DPI режет ECH-хендшейки; ручка только в API —
`PATCH /zones/<id>/settings/ech`, `{"value":"off"}`) — не включать обратно.
Фронт перебирает адреса сам: `workerFetch` в app.js (таймаут на адрес +
фолбэки из `PUSH_WORKER_FALLBACKS` region_front.js), стражи —
[scripts/tests/test_worker_fallback.py](../../scripts/tests/test_worker_fallback.py).
CORS не менялся: `ALLOWED_ORIGIN` — это origin ФРОНТА (github.io), от адреса
Worker'а он не зависит.

**Шлюз api2-\*.delosud.ru (27.08.2026) и настоящий диагноз МТС/Мегафона.**
Первичная гипотеза «режут адреса Cloudflare целиком» НЕ подтвердилась:
пакетный разбор (tcpdump на шлюзе) показал, что МТС фильтрует **TLS по SNI
МОЛОДОГО домена delosud.ru на ЛЮБОМ IP** — соединение устанавливается, после
ClientHello поток уходит в одностороннюю «чёрную дыру» (у пользователя
«висит», у сервера бесконечные FIN-ретрансмиты); plain http и https по голому
IP проходят. Это операторский фильтр свежерегистрированных доменов — имя
«созревает» и выпадает из него само (обычно 1–2 недели от регистрации
27.08.2026); блок workers.dev — отдельный и по имени же. Пер-SIM услуги,
которую можно выключить, у МТС не нашлось.
⚠️ **Шлюз `api2-*.delosud.ru` фильтр МТС НЕ обходит** (осознано после
замеров, исправляет более ранний ошибочный вывод): шлюз стоит на ПОДДОМЕНЕ
того же молодого `delosud.ru`, и DPI режет его по SNI ровно так же — с
телефона МТС https-запросы к шлюзу до nginx не доходили (в access.log только
http и заход по голому IP). Против фильтра молодого домена помогает лишь
СОЗРЕВАНИЕ имени (проверять `https://api-hmao.delosud.ru` с МТС раз в
несколько дней; «Not Found» = отпустило, дальше всё работает само).
Отдельная особенность канала VPS→Cloudflare Workers: большой POST (сотни КБ,
загрузка дампов) виснет ~45 с и рвётся, а тот же объём на github/sudrf/
CF-IP летит мегабайтами — на синк подписок (тела < 1 КБ) не влияет, на
загрузку дампов ЧЕРЕЗ шлюз — да; корень не локализован (малый POST проходит).
Сам VPS ОСТАВЛЕН как инфраструктура на будущее, НЕ как лекарство от МТС:
Cloud.ru (Free Tier до ~27.11.2026, дальше ~513 ₽/мес — решить; IP
195.19.66.234, Ubuntu 24.04, nginx + certbot), `api2-hmao.delosud.ru` →
`api-hmao.delosud.ru`, `api2-ural.delosud.ru` → `api-ural.delosud.ru`
(DNS `proxied:false`). Ценность: (1) готовая ступень на случай НАСТОЯЩЕГО
блока адресов Cloudflare по IP — тогда шлюз перевесить на ПОСТОРОННЕЕ зрелое
имя (не delosud); (2) проверенная площадка под перенос парсинга — sudrf
пускает IP Cloud.ru (нормальная страница поиска). Шлюз пока ПЕРВЫЙ фолбэк
`PUSH_WORKER_FALLBACKS` обеих территорий (пользы для МТС нет — оба адреса
delosud мертвы вместе; на нормальных операторах основной адрес и так
открывается — кандидат на откат до созревания). Конфиг nginx —
`/etc/nginx/sites-available/delosud-gw` (`resolver ... ipv6=off` —
у Cloudflare-хостнеймов есть AAAA, а IPv6-маршрута с VPS нет:
`Network is unreachable`; `proxy_http_version 1.1`), сертификаты обновляет
`certbot.timer`, доступ — только SSH-ключ (`~/.ssh/delosud_gw` на Mac).

## Админка подписчиков

URL: `https://api-hmao.delosud.ru/admin?secret=<OWNER_SECRET>`
(фолбэк — прежний `https://court-monitor-trigger.7selivanov-a.workers.dev/admin?…`).
`handleAdmin` ([1371](../../cloudflare-worker/worker.js#L1371)) рендерит HTML
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
