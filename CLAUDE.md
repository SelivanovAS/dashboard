# CLAUDE.md

Карта проекта для новых сессий — чтобы не тратить токены на разведку.

## Что это

Дашборд юриста ПАО Сбербанк: мониторинг гражданских дел в 20 судах ХМАО-Югры (первая инстанция) + апелляция (Суд ХМАО-Югры) + кассация (7-й кассационный суд общей юрисдикции, фильтр по 1-й инст. ХМАО). AI-дайджесты в Telegram, автозапуск через Cloudflare Worker cron → GitHub Actions. Пользователь — юрист банка, общение на русском.

## Главные файлы

- [scripts/update_cases.py](scripts/update_cases.py) — **монолит** (~231 KB): парсеры судов, LLM-дайджесты, Telegram, CLI.
- [scripts/add_cases_manually.py](scripts/add_cases_manually.py) — ручное добавление дел 1-й инстанции.
- [scripts/migrate_csv_to_json.py](scripts/migrate_csv_to_json.py) — одноразовая миграция CSV→JSON (выполнена).
- [data/cases.json](data/cases.json) — активные дела (UTF-8, `version: 1`, `updated_at` ISO).
- [data/cases_archive.json](data/cases_archive.json) — архив.
- `data/.digested_acts` — дедуп уже обработанных судебных актов (скрытый файл).
- `data/.cassation_acts` — дедуп уже обработанных кассационных определений (планируется при включении LLM-разбора кассации).
- [data/last_digest_context.json](data/last_digest_context.json) — снимок контекста для `--replay-last`.
- [data/last_personal_pushes.json](data/last_personal_pushes.json) — журнал последней push-рассылки (что получила каждая подписка): variant, title, body, click_url. Перезаписывается на каждом прогоне `send_web_push`. Читается админкой подписчиков.
- [data/sberbank_cases.csv](data/sberbank_cases.csv) + архив — legacy CSV (UTF-8 с BOM), всё ещё коммитится для совместимости.
- [app.js](app.js) + [sberbank_dashboard.html](sberbank_dashboard.html) + [styles.css](styles.css) — SPA-фронт (GitHub Pages).
- [cloudflare-worker/wrangler.toml](cloudflare-worker/wrangler.toml) + [cloudflare-worker/worker.js](cloudflare-worker/worker.js) — автозапуск.
- [.github/workflows/update_cases.yml](.github/workflows/update_cases.yml) — основной workflow (парсинг + дайджест + commit).
- [.github/workflows/test_digest.yml](.github/workflows/test_digest.yml) — единый ручной тест: replay последнего дайджеста, Telegram (личный/группа по галке), PWA push (владельцу/всем по галке), коммит свежего `data/last_digest.json`.
- [.github/workflows/digest_only_gigachat.yml](.github/workflows/digest_only_gigachat.yml) — ручной дайджест через GigaChat (альтернативный LLM).
- [README.md](README.md) — подробная документация на русском (дублирует часть этого файла).

## Ключевые точки в update_cases.py

| Что | Где |
|-----|-----|
| `APPEAL_COURT` (конфиг апелляции) | [scripts/update_cases.py:119](scripts/update_cases.py:119) |
| `FIRST_INSTANCE_COURTS` (массив 20 `CourtConfig`) | [scripts/update_cases.py:127](scripts/update_cases.py:127) |
| `CASSATION_COURT` (7kas.sudrf.ru, гражданская кассация) | [scripts/update_cases.py:154](scripts/update_cases.py:154) |
| `match_hmao_first_instance` (длинная форма → CourtConfig) | [scripts/update_cases.py:162](scripts/update_cases.py:162) |
| `DIGESTED_ACTS_PATH` | [scripts/update_cases.py:216](scripts/update_cases.py:216) |
| Константы state-machine (`FI_ARCHIVE_DAYS`, `CASSATION_*`) | [scripts/update_cases.py:246](scripts/update_cases.py:246) |
| `advance_case_stage` / `is_case_archived` / `migrate_stages` | [scripts/update_cases.py:514](scripts/update_cases.py:514) |
| `class TableExtractor(HTMLParser)` — парсер карточек дела | [scripts/update_cases.py:1110](scripts/update_cases.py:1110) |
| `parse_cassation_search_page` — поиск 7kas (HMAO-фильтр) | [scripts/update_cases.py:1486](scripts/update_cases.py:1486) |
| `classify_cassation_outcome` — детерм. enum исхода | [scripts/update_cases.py:2036](scripts/update_cases.py:2036) |
| `parse_cassation_card` + `_extract_cassation_act_text` (`cont_doc1`) | [scripts/update_cases.py:2124](scripts/update_cases.py:2124) |
| `relink_awaiting_relink_first_instance` (re-link после remanded) | [scripts/update_cases.py:2654](scripts/update_cases.py:2654) |
| `link_cassation_cases` (link + discovery + remanded) | [scripts/update_cases.py:2756](scripts/update_cases.py:2756) |
| `GIGACHAT_SYSTEM_PROMPT` | [scripts/update_cases.py:2049](scripts/update_cases.py:2049) |
| `def generate_digest` — Claude-дайджест | [scripts/update_cases.py:2330](scripts/update_cases.py:2330) |
| Claude model: `claude-haiku-4-5-20251001` | [scripts/update_cases.py:2694](scripts/update_cases.py:2694) |
| `def generate_template_digest` — fallback без LLM | [scripts/update_cases.py:2820](scripts/update_cases.py:2820) |

## Схема cases.json

```json
{
  "version": 1,
  "updated_at": "ISO-8601",
  "cases": [
    {
      "id": "номер дела",
      "current_stage": "first_instance" | "awaiting_appeal" | "appeal" | "cassation_watch" | "cassation_pending" | "cassation" | "awaiting_relink",
      "round": 1,                  // ≥2 после cassation_remanded (см. history)
      "history": [...],            // снимки прошлых раундов после remanded
      "discovered_via_cassation": false,  // true если дело создано discovery'ем
      "plaintiff": "...", "defendant": "...",
      "bank_role": "Истец|Ответчик|Третье лицо",
      "category": "...", "notes": "...",
      "first_instance": {
         "court", "judge", "status", "events": [], "resolved_emitted": bool,
         "hearing_date",           // дата резолютивки, якорь 45-дневного окна
         "act_date",               // дата публикации мотивировки (когда есть)
         "appeal_filed", "appeal_filed_date",        // апел. жалоба в карточке 1-й инст.
         "cassation_filed", "cassation_filed_date",  // касс. жалоба (идёт через 1-ю инст.)
         "sent_to_cassation", "sent_to_cassation_date"
      },
      "appeal":         { "court", "status", "result", "events": [], "act_published", "hearing_date", "act_date", ... },
      "cassation":      { "case_number", "cassation_number", "court", "judge",
                          "filing_date", "decision_date", "act_date",
                          "result_text", "result_for_appeal", "review_result",
                          "outcome", "remanded_to", "act_published", "act_text",
                          "appellant", "appellant_is_bank", "appellant_status",
                          "events", "link", "last_checked_at",
                          "discovered_via_cassation" },
      "cassation_pending_since": "YYYY-MM-DD"  // если перешли в cassation_pending
    }
  ]
}
```

## Автозапуск

- Cron `"45 3 * * mon-fri"` = **6:45 МСК пн-пт** в [cloudflare-worker/wrangler.toml:6](cloudflare-worker/wrangler.toml:6).
- ⚠️ Cloudflare Cron Triggers нумерует дни недели 1=Sun..7=Sat (не как POSIX). Цифровое `1-5` эмпирически срабатывало в т.ч. в воскресенье — поэтому используем буквенный `mon-fri`. Дополнительный щит — `isHoliday()` в `worker.js` режет сб/вс через `getDay()`.
- Worker вызывает `workflow_dispatch` для `update_cases.yml` через GitHub API (нужен `GITHUB_PAT`).
- **Автозапуск = Cloudflare Worker, НЕ cron-job.org.** Любые правки расписания — в `wrangler.toml`, потом `wrangler deploy`.

## Жизненный цикл дела (state machine)

Семь рабочих стадий в `current_stage` + архив. Переходы — в
`advance_case_stage()`, архивация — в `is_case_archived()`.

| Стадия | Что парсим | Что запускает переход |
|---|---|---|
| `first_instance` | карточка 1-й инст. | подана апел. жалоба → `awaiting_appeal` · 60 дней от hearing_date без жалобы → архив (с возможностью реактивации при появлении жалобы) |
| `awaiting_appeal` | ничего (жалоба подана, ждём карточку в апел. суде) | link_cases находит апел. карточку → `appeal` · бессрочно, не архивируется |
| `appeal` | карточка апел. суда | опубликован акт ИЛИ 30 дней от апел. заседания без акта → `cassation_watch` · не архивируется по времени |
| `cassation_watch` | карточка 1-й инст. (ищем касс. жалобу) | касс. жалоба или направление в кассац. суд → `cassation_pending` · 120 дней от апел. заседания → архив |
| `cassation_pending` | ничего (ждём появления карточки на 7kas) | link_cassation_cases находит карточку → `cassation` · не архивируется |
| `cassation` | карточка 7kas (гражданская кассация) | `outcome=cassation_remanded` → `awaiting_relink` (re-link при появлении новой карточки в нижестоящей) · `act_published` + 30 дней / `decision_date` + 45 дней без акта → архив (для финальных исходов, кроме remanded) |
| `awaiting_relink` | ничего (ждём карточку в нижестоящей инст.) | парсер 1-й инст. находит дело → `first_instance` (round +1, прошлые блоки в `history`) ИЛИ парсер апел. → `appeal` · бессрочно, не архивируется |

Константы в [scripts/update_cases.py:201](scripts/update_cases.py:201):
`FI_ARCHIVE_DAYS=60`, `APPEAL_NO_ACT_GRACE_DAYS=30`,
`CASSATION_WATCH_DAYS=120`, `CASSATION_ACT_ARCHIVE_DAYS=30`,
`CASSATION_NO_ACT_PUBLISH_DAYS=45`.

⚠ Фронт ([app.js:11](app.js:11)) держит свою константу `ARCHIVE_DAYS` —
синхронизировать вручную при правке `FI_ARCHIVE_DAYS`, иначе фронт
будет прятать дела раньше, чем парсер их архивирует.

`migrate_stages()` идемпотентно подтягивает старые записи (до появления
state-machine) под новую модель при каждом запуске.

**Реактивация из архива:** функция `reactivate_archived_first_instance`
(рядом с `relink_awaiting_relink_first_instance`) возвращает дело из
[data/cases_archive.json](data/cases_archive.json) обратно в активные,
если парсер 1-й инст. снова увидел карточку с признаком подачи апел./
касс. жалобы (`appeal_filed*`, `cassation_filed*`, `sent_to_cassation*`).
Прочие изменения карточки реактивацию не триггерят. Отдельного события
в дайджесте нет: сработает обычное `fi_appeal_filed`.

**7kas.sudrf.ru — параметры запросов** (эмпирически найдены):
- `delo_id=2800001` (гражданская кассация, не уголовка/админка),
- `delo_table=g33_case`, `name_field=G33_PARTS__NAMESS`,
- `new=2800001` (НЕ `0` и НЕ `5` — отдельная ветка для КСОЮ).

Любые правки этих параметров — только после ручной проверки на 7kas, иначе
поиск молча вернёт «Данных по запросу не обнаружено».

## Команды

```bash
# Полный прогон локально (парсинг + дайджест + Telegram)
python3 scripts/update_cases.py --json

# Переиграть последний дайджест (из data/last_digest_context.json)
python3 scripts/update_cases.py --replay-last

# Добавить дело 1-й инстанции вручную
python3 scripts/add_cases_manually.py

# Зависимости
pip install -r scripts/requirements.txt

# Деплой Worker
cd cloudflare-worker && wrangler deploy
```

GitHub Actions workflows запускаются из UI репозитория (Run workflow) или автоматически cron'ом Worker'а.

## Переменные окружения

- `ANTHROPIC_API_KEY` — Claude.
- `GIGACHAT_CREDENTIALS` — GigaChat (альтернативный LLM).
- `TELEGRAM_BOT_TOKEN` — токен бота.
- `TELEGRAM_CHAT_ID` — корпоративная группа (используется только при `to_group=true`).
- `TELEGRAM_CHAT_ID_TEST` — личный чат, дефолтный получатель дайджеста.
- `PUSH_WORKER_URL`, `PUSH_SECRET`, `VAPID_PRIVATE_KEY` — Web Push для PWA.
- `OWNER_SECRET` — секрет Worker'а для `POST /mark-owner` (пометка устройства владельцем).
- `GITHUB_PAT` — в secrets Worker'а, для `workflow_dispatch`.
- `DIGESTED_ACTS_PATH` — опционально переопределить путь к `.digested_acts`.

## Куда уходит дайджест

- **Telegram:** все workflow'и шлют в личный чат (`TELEGRAM_CHAT_ID_TEST`) по умолчанию. Чтобы продублировать в корпоративную группу — поставить галку `to_group` в UI Run workflow. Текст дайджеста в Telegram **общий**, не персонализированный.
- **PWA push:** `update_cases.yml` (крон) шлёт всем подписчикам PWA. Тестовые workflow'и (`test_digest.yml`, `digest_only_gigachat.yml`) шлют push **только устройствам-владельцам** по умолчанию, чтобы не спамить коллегам прототипами. У `test_digest.yml` есть галка «push_all» — отправит на все устройства. Чтобы пометить своё устройство владельцем — открыть PWA по URL `https://selivanovas.github.io/dashboard/sberbank_dashboard.html?owner=<OWNER_SECRET>` (один раз).
- **Персонализация push по watchlist (`_per_sub` callback):** push-payload собирается под каждого подписчика отдельно через фабрику `_make_per_sub_callback` ([scripts/update_cases.py:4128](scripts/update_cases.py:4128)). Новые дела (`fi_new_cases`, `appeal_new_cases_csv`) — общесистемный сигнал, шлются всем; изменения и переходы стадий — только если дело в watchlist подписчика. Click_url для подписчиков с watchlist — `?digest=open&mine=1`. Используется в основном кроне (`main_json`), `--replay-last`, `--push-last-digest`.

## Админка подписчиков

URL: `https://court-monitor-trigger.7selivanov-a.workers.dev/admin?secret=<OWNER_SECRET>`. Открывается в браузере (мобильно тоже). Endpoint реализован в [cloudflare-worker/worker.js](cloudflare-worker/worker.js): `handleAdmin` рендерит HTML, JS внутри тянет `/admin/data?secret=...` (защищён OWNER_SECRET) и `cases.json` с GitHub Pages.

Что показывает по каждой push-подписке: имя (если задано), устройство (парсится из user_agent), флаг owner, дата создания, последний вход в PWA, дата последнего обновления watchlist, размер watchlist и раскрываемый список дел со сторонами (Истец vs Ответчик · Суд) — стороны подтягиваются из `cases.json` по номеру.

Действия по каждой подписке (3 кнопки):
- **✏ Имя** → POST `/admin/label` `{endpoint, label}`. Сохраняет произвольное имя («Иван», «iPhone Дани»).
- **📋 Ред. watchlist** → POST `/admin/watchlist` `{endpoint, watchlist}`. Перезаписывает watchlist чужой подписки (когда коллега не разобралась со звёздочками).
- **🗑 Удалить** → POST `/admin/unsubscribe` `{endpoint}`. Принудительно убирает подписку из KV.

Все админ-эндпоинты авторизуются через `?secret=<OWNER_SECRET>` в URL (для удобства открытия из браузера).

Метаданные в KV: `created_at` (один раз), `last_seen_at` (на каждом `/subscribe`), `last_watchlist_update_at` (на `/watchlist`), `user_agent`, `label`. Старые подписки заполняют поля при следующем `/subscribe`.

**Тестовый push отложен** — эндпоинт `/admin/test-push` и VAPID-utility в `worker.js` готовы, но кнопка из UI убрана: для активации нужен `VAPID_PRIVATE_KEY` в Worker secret (`wrangler secret put VAPID_PRIVATE_KEY`), которого сейчас нет. Чтобы вернуть фичу — положить PEM и добавить кнопку обратно в `renderCard`.

**Журнал последней push-рассылки** — на каждой карточке раскрываемый блок «🪞 Последний push для этой подписки». Показывает variant (personal/general/skip/broadcast), title, body, click_url, timestamp. Источник — [data/last_personal_pushes.json](data/last_personal_pushes.json), перезаписывается каждый прогон `send_web_push` в `update_cases.py`. Если по подписке уход push'а в этом прогоне был skipped (нет событий по watchlist) — блок показывает «Push не отправлен — нет событий по watchlist».

## Подписки на дела (watchlist) на фронте

- Звёздочка ★/☆ в карточке/строке/drawer → `localStorage['watchlist_v1']` → POST `/watchlist` на Worker (KV).
- **Фильтр «Мои дела»** в chip-bar (`★ Мои`) — виден только при непустом watchlist. Показывает отслеживаемые ★ + новые дела за день. Состояние в `localStorage['filter_mine_v1']`. Включается автоматически при «первом открытии» с непустым watchlist (первая звезда или гидратация с Worker). После явного выключения юристом — НЕ возвращается даже при добавлении новых звёзд.
- **`?mine=1` в URL** (выставляется click_url'ом персонального push) → фронт читает `data/last_digest_context.json`, фильтрует через клон `_filter_events_by_watchlist` (новые дела целиком) и подменяет содержимое блока «Последний дайджест» на mine-версию. При пустом watchlist или отсутствии своих событий — оставляет общий дайджест + плашка-заметка.

## Соглашения

- **Язык:** весь код, переменные, комментарии, промпты — **на русском**.
- **Коммиты:** `EMOJI описание на русском`. Примеры:
  - `📊 Обновление данных 23.04.2026 03:52` — автоматический от workflow.
  - `Дайджест: ...`, `Карточка: ...`, `GigaChat: ...` — правки скрипта.
- **Telegram HTML:** только `<b>`, `<i>`, `<a href>`. Лимит 4096 символов на сообщение, дайджест режется автоматически (целевой объём ~7600).
- **JSON:** UTF-8 без BOM, `version: 1`, `updated_at` ISO.
- **CSV:** UTF-8 с BOM, legacy-формат, по-прежнему коммитится.
- **Дедупликация актов:** через `.digested_acts` — не обрабатывать акт дважды.
- **Bust фронта/PWA:** при любых правках [app.js](app.js) или [styles.css](styles.css) **обязательно**:
  - инкрементить `?v=N` в [sberbank_dashboard.html](sberbank_dashboard.html) (строка `<script src="app.js?v=N">` и/или `<link href="styles.css?v=N">`),
  - инкрементить `CACHE_VERSION` в [service-worker.js](service-worker.js).
  Без этого у юриста на устройстве PWA будет показывать старую версию из cache-first (см. инцидент `0b70826` — реактивация архива не была видна, потому что забыли bust).

## Чего НЕ делать

- Не коммитить секреты (`.env`, ключи API, `GITHUB_PAT`).
- Не переименовывать поля в `cases.json` без миграции — завязан фронт (`app.js`) и архив.
- Не добавлять cron-job.org / аналоги — автозапуск только через Cloudflare Worker.
- Не ломать структуру промптов в `generate_digest` / `GIGACHAT_SYSTEM_PROMPT` без предупреждения: пользователь долго их настраивал (см. `git log` по этим функциям).
- Не менять `delo_table=g33_case` и `new=2800001` для 7kas без проверки — эти константы эмпирически подобраны к API КСОЮ; неверные значения дают «Данных по запросу не обнаружено» без явной ошибки.
- Не амендить опубликованные коммиты — создавать новые.

## Когда всё-таки нужна разведка

Если задача касается:
- Конкретного парсера одного суда — читать `CourtConfig` в `FIRST_INSTANCE_COURTS`.
- Логики парсинга таблиц → `TableExtractor` ([scripts/update_cases.py:599](scripts/update_cases.py:599)).
- Фронтенда (фильтры, рендер) → [app.js](app.js).
- Конкретного workflow → соответствующий `.github/workflows/*.yml`.

Иначе — этой карты достаточно, не нужно запускать Grep/Glob с нуля.
