# CLAUDE.md

Карта проекта для новых сессий — чтобы не тратить токены на разведку.

> 📚 **Полная техническая документация** — [docs/technical/README.md](docs/technical/README.md).
> Этот файл — быстрая карта (где что в коде); `docs/technical/` — глубокий
> справочник «как всё работает» (архитектура, модель данных, жизненный цикл,
> парсеры, конвейер, дайджест, доставка, фронтенд, worker, эксплуатация).

## Что это

Дашборд юриста ПАО Сбербанк: мониторинг гражданских дел в 20 судах ХМАО-Югры (первая инстанция) + апелляция (Суд ХМАО-Югры) + кассация (7-й кассационный суд общей юрисдикции, фильтр по 1-й инст. ХМАО). AI-дайджесты в Telegram, автозапуск через Cloudflare Worker cron → GitHub Actions. Пользователь — юрист банка, общение на русском.

## Главные файлы

- [scripts/update_cases.py](scripts/update_cases.py) — **тонкий фасад CLI** (~220 строк): разбор argv + ре-экспорт прежних имён. Весь код — в пакете `scripts/court_monitor/` (распил монолита, см. [docs/Распил_монолита_контекст.md](docs/Распил_монолита_контекст.md)).
- `scripts/court_monitor/` — **пакет модулей** (читать только нужный):
  - [config.py](scripts/court_monitor/config.py) — env-константы, пути данных, окна state-machine, `log`, `METRICS`. Патчабельные константы код читает ТОЛЬКО как `config.X` — тесты патчат `monkeypatch.setattr(config, ...)`.
  - [textutil.py](scripts/court_monitor/textutil.py) — даты, HTML-очистка, экранирование, сокращение имён сторон/судов, производственный календарь.
  - [netutil.py](scripts/court_monitor/netutil.py) — `session`, `fetch_page` (ретраи, win-1251), `polite_delay`.
  - [courts.py](scripts/court_monitor/courts.py) — `CourtConfig`, реестры судов (апелляция, 20 судов 1-й инст., 7kas), матчер ХМАО, URL карточек.
  - [storage.py](scripts/court_monitor/storage.py) — cases.json/CSV, `.digested_acts`, `.cassation_acts`, кэш пересказов.
  - [health.py](scripts/court_monitor/health.py) — журнал здоровья парсеров + детектор молчаливой поломки.
  - [lifecycle.py](scripts/court_monitor/lifecycle.py) — классификация событий карточки, state machine стадий, дедуп, архив.
  - [parsing/](scripts/court_monitor/parsing/__init__.py) — `tables.py` (TableExtractor), `search.py` (поисковая выдача), `cards.py` (карточки дел), `cassation.py` (7kas).
  - [linking.py](scripts/court_monitor/linking.py) — связка FI ↔ апелляция ↔ кассация, discovery, реактивация, ротация архива.
  - [digest/](scripts/court_monitor/digest/__init__.py) — `llm.py` (Claude/GigaChat, промпты — патч-цели тестов живут тут), `postprocess.py` (валидация/чистка HTML), `template.py` (программный рендер), `core.py` (диспетчер `generate_digest`).
  - [delivery.py](scripts/court_monitor/delivery.py) — Telegram, Web Push с watchlist-персонализацией, алерты.
  - [runs.py](scripts/court_monitor/runs.py) — `main_json` и остальные режимы прогона, `update_active_cases`.
- [scripts/add_cases_manually.py](scripts/add_cases_manually.py) — ручное добавление дел 1-й инстанции.
- `scripts/tests/` + `tests/` — pytest-набор (230+ тестов: парсеры, state machine, линковка, архив, детектор здоровья, рендер дайджеста). Запуск одним прогоном: `python3 -m pytest` из корня (конфиг — [pytest.ini](pytest.ini)); CI гоняет на каждый push ([.github/workflows/tests.yml](.github/workflows/tests.yml)).
- [data/cases.json](data/cases.json) — активные дела (UTF-8, `version: 1`, `updated_at` ISO).
- [data/cases_archive.json](data/cases_archive.json) — «горячий» архив: дела, заархивированные за последние 12 мес. (`COLD_ARCHIVE_DAYS`). Грузится фронтом.
- `data/cases_archive_YYYY.json` — «холодные» годовые архивы: дела старше года, вынесенные ротацией (`rotate_cold_archive`). **Фронт их не грузит** (чтобы вес не рос безгранично), но скрипт читает их в индекс дедупликации. Холодные дела «заморожены»: не реактивируются автоматически.
- `data/.digested_acts` — дедуп уже обработанных судебных актов (скрытый файл).
- `data/.cassation_acts` — дедуп кассационных определений: ключи «8Г-номер|дата акта», чьи `new_act` уже уходили в дайджест. Гасит повторный `new_act` при «мигании» `act_published` (сбойный парс 7kas). Ведётся в `link_cassation_cases`.
- `data/parse_health.json` — журнал здоровья парсеров: пер-источник история количества результатов поиска (20 судов 1-й инст., апелляция, 7kas до/после HMAO-фильтра). Детектор «молчаливой поломки» (`update_parse_health`, блок 4e в `main_json`) шлёт сервисный 🩺-алерт в Telegram: суд с медианой ≥1 вернул 0 (на 1-м и 3-м нулевом прогоне + сообщение о восстановлении), HTTP-фейл 3 прогона подряд, все источники разом по нулям, ≥5 карточек-«огрызков» за прогон.
- [data/last_digest_context.json](data/last_digest_context.json) — снимок контекста для `--replay-last`.
- [data/last_personal_pushes.json](data/last_personal_pushes.json) — журнал последней push-рассылки (что получила каждая подписка): variant, title, body, click_url. Перезаписывается на каждом прогоне `send_web_push`. Читается админкой подписчиков.
- [data/sberbank_cases.csv](data/sberbank_cases.csv) + архив — legacy CSV (UTF-8 с BOM), всё ещё коммитится для совместимости.
- [app.js](app.js) + [sberbank_dashboard.html](sberbank_dashboard.html) + [styles.css](styles.css) — SPA-фронт (GitHub Pages).
- [cloudflare-worker/wrangler.toml](cloudflare-worker/wrangler.toml) + [cloudflare-worker/worker.js](cloudflare-worker/worker.js) — автозапуск.
- [.github/workflows/update_cases.yml](.github/workflows/update_cases.yml) — основной workflow (парсинг + дайджест + commit). При падении любого шага шлёт 🚨-алерт в личный Telegram (шаг `if: failure()`, curl без Python).
- [.github/workflows/tests.yml](.github/workflows/tests.yml) — pytest на каждый push (кроме правок только .md/docs).
- [.github/workflows/test_digest.yml](.github/workflows/test_digest.yml) — единый ручной тест: replay последнего дайджеста, Telegram (личный/группа по галке), PWA push (владельцу/всем по галке), коммит свежего `data/last_digest.json`.
- [.github/workflows/digest_only_gigachat.yml](.github/workflows/digest_only_gigachat.yml) — ручной дайджест через GigaChat (альтернативный LLM).
- [README.md](README.md) — подробная документация на русском (дублирует часть этого файла).

## Ключевые точки в пакете court_monitor

| Что | Где |
|-----|-----|
| dataclass конфига суда: `CourtConfig` | [scripts/court_monitor/courts.py:29](scripts/court_monitor/courts.py:29) |
| `APPEAL_COURT` (конфиг апелляции) | [scripts/court_monitor/courts.py:88](scripts/court_monitor/courts.py:88) |
| массив 20 судов: `FIRST_INSTANCE_COURTS` | [scripts/court_monitor/courts.py:96](scripts/court_monitor/courts.py:96) |
| `CASSATION_COURT` (7kas.sudrf.ru, гражданская кассация) | [scripts/court_monitor/courts.py:123](scripts/court_monitor/courts.py:123) |
| `match_hmao_first_instance` (длинная форма → CourtConfig) | [scripts/court_monitor/courts.py:139](scripts/court_monitor/courts.py:139) |
| `DIGESTED_ACTS_PATH` / `CASSATION_ACTS_PATH` / `PARSE_HEALTH_PATH` | [scripts/court_monitor/config.py:87](scripts/court_monitor/config.py:87) |
| Константы state-machine (`FI_ARCHIVE_DAYS`, `CASSATION_*`) | [scripts/court_monitor/config.py:99](scripts/court_monitor/config.py:99) |
| `update_parse_health` — детектор молчаливой поломки парсеров | [scripts/court_monitor/health.py:42](scripts/court_monitor/health.py:42) |
| `advance_case_stage` / `is_case_archived` / `migrate_stages` | [scripts/court_monitor/lifecycle.py:410](scripts/court_monitor/lifecycle.py:410) |
| `reactivate_archived_first_instance` (возврат из архива) | [scripts/court_monitor/linking.py:273](scripts/court_monitor/linking.py:273) |
| `rotate_cold_archive` (горячий → холодный архив) | [scripts/court_monitor/linking.py:835](scripts/court_monitor/linking.py:835) |
| `class TableExtractor(HTMLParser)` — парсер карточек дела | [scripts/court_monitor/parsing/tables.py:13](scripts/court_monitor/parsing/tables.py:13) |
| `parse_case_card` — карточка 1-й инст./апелляции | [scripts/court_monitor/parsing/cards.py:113](scripts/court_monitor/parsing/cards.py:113) |
| `parse_cassation_search_page` — поиск 7kas (HMAO-фильтр) | [scripts/court_monitor/parsing/cassation.py:50](scripts/court_monitor/parsing/cassation.py:50) |
| `classify_cassation_outcome` — детерм. enum исхода | [scripts/court_monitor/parsing/cassation.py:180](scripts/court_monitor/parsing/cassation.py:180) |
| `parse_cassation_card` + `_extract_cassation_act_text` (`cont_doc1`) | [scripts/court_monitor/parsing/cassation.py:361](scripts/court_monitor/parsing/cassation.py:361) |
| `relink_awaiting_relink_first_instance` (re-link после remanded) | [scripts/court_monitor/linking.py:205](scripts/court_monitor/linking.py:205) |
| `link_cases` (FI ↔ апелляция) | [scripts/court_monitor/linking.py:48](scripts/court_monitor/linking.py:48) |
| `link_cassation_cases` (link + discovery + remanded + архив + дедуп актов) | [scripts/court_monitor/linking.py:419](scripts/court_monitor/linking.py:419) |
| `update_active_cases` (обход карточек активных дел) | [scripts/court_monitor/runs.py:84](scripts/court_monitor/runs.py:84) |
| `main_json` (оркестрация полного прогона) | [scripts/court_monitor/runs.py:873](scripts/court_monitor/runs.py:873) |
| `GIGACHAT_SYSTEM_PROMPT` | [scripts/court_monitor/digest/llm.py:73](scripts/court_monitor/digest/llm.py:73) |
| `def generate_digest` — диспетчер дайджеста | [scripts/court_monitor/digest/core.py:333](scripts/court_monitor/digest/core.py:333) |
| `summarize_act_motivation` — LLM-пересказ акта | [scripts/court_monitor/digest/llm.py:491](scripts/court_monitor/digest/llm.py:491) |
| `polish_digest_html` — LLM-полировщик (опц.) | [scripts/court_monitor/digest/llm.py:693](scripts/court_monitor/digest/llm.py:693) |
| Пост-обработка HTML (`_ensure_*`/`_validate_*`/`_drop_*`/`_normalize_*`) | весь [scripts/court_monitor/digest/postprocess.py](scripts/court_monitor/digest/postprocess.py) |
| Claude model: `claude-haiku-4-5-20251001` (`_current_digest_model_name`) | [scripts/court_monitor/digest/llm.py:832](scripts/court_monitor/digest/llm.py:832) |
| `def generate_template_digest` — программный рендер | [scripts/court_monitor/digest/template.py:322](scripts/court_monitor/digest/template.py:322) |
| доставка: `send_telegram` | [scripts/court_monitor/delivery.py:615](scripts/court_monitor/delivery.py:615) |
| PWA push: `send_web_push` | [scripts/court_monitor/delivery.py:430](scripts/court_monitor/delivery.py:430) |
| персонализация push: `_make_per_sub_callback` | [scripts/court_monitor/delivery.py:305](scripts/court_monitor/delivery.py:305) |
| фильтр по watchlist: `_filter_events_by_watchlist` | [scripts/court_monitor/delivery.py:111](scripts/court_monitor/delivery.py:111) |

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

## Автозапуск (вариант D2 — с 03.07.2026, ⏳ ВРЕМЕННОЕ решение)

> **Временно до переезда на сервер.** План: перенести парсинг на российский VPS
> (точечный прокси `COURT_PROXY_URL` → `netutil.session.proxies` — Claude и
> Telegram ходят мимо общей session, проверено; либо полный перенос прогона).
> После переезда Mac-звено демонтируется, расписание вернётся в инфраструктуру.

⚠️ **Суды режут иностранные IP.** `*.sudrf.ru` молча дропает TLS с не-российских
адресов (TCP проходит, хендшейк — нет). GitHub Actions ходит из США → парсинг
судов оттуда невозможен. Одновременно `api.anthropic.com` недоступен из РФ. Отсюда
разделение конвейера по географии (детали — [ops/mac-local-run/README.md](ops/mac-local-run/README.md)).

- **Парсинг судов — на Mac юриста** (физически в сети Сбера, egress РФ). Планировщик
  — LaunchAgent `com.court-monitor.parse` (будни, local-время +05), запускает
  [ops/mac-local-run/parse_and_push.sh](ops/mac-local-run/parse_and_push.sh):
  preflight «в сети Сбера?» → пересоздание маршрута судов мимо VPN через шлюз
  `10.217.111.250` → `ops/mac-local-run/run_parse.py` (это `main_json` с
  заглушённой `validate_environment` — иначе `exit(2)` без секретов; доставка
  сама скипается, контекст сохраняется) → `git commit && push`. Установка/откат
  — [ops/mac-local-run/README.md](ops/mac-local-run/README.md).
- **Дайджест Claude + доставка — на GitHub** (там Claude доступен). Workflow
  [.github/workflows/replay_on_push.yml](.github/workflows/replay_on_push.yml)
  ловит `push` с изменённым `data/last_digest_context.json` → `--replay-last
  --push-all` → Claude-дайджест в личный Telegram + Web Push всем подписчикам →
  коммитит `last_digest.json`. Анти-петля: replay не трогает контекст +
  GITHUB_TOKEN-пуши не триггерят workflow.
- **Cloudflare Worker cron ОТКЛЮЧЁН** (`crons = []` в
  [cloudflare-worker/wrangler.toml](cloudflare-worker/wrangler.toml), нужен
  `wrangler deploy`). `worker.js` и админка подписчиков живы — выключено только
  расписание. Вернуть прежний автозапуск — раскомментировать `crons` и задеплоить
  (но тогда снова упрётся в блокировку судов).
- **НЕ cron-job.org.** Планировщик теперь — LaunchAgent на Mac; расписание правится
  в `.plist`, не в чужих крон-сервисах.
- **Живой просмотр парсинга:** ярлык `ops/mac-local-run/Парсинг судов.command`
  (двойной клик — текущий прогон с начала + live) и блок «🛰 Парсинг» в админке
  Worker (`progress_pusher.py` шлёт вехи на `POST /run-progress`, auth —
  Worker-секрет `PROGRESS_SECRET`, токен на Mac в
  `~/.config/court-monitor/progress_token` вне репо).

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

Константы в [scripts/court_monitor/config.py:78](scripts/court_monitor/config.py:78):
`FI_ARCHIVE_DAYS=60`, `APPEAL_NO_ACT_GRACE_DAYS=30`,
`CASSATION_WATCH_DAYS=120`, `CASSATION_ACT_ARCHIVE_DAYS=30`,
`CASSATION_NO_ACT_PUBLISH_DAYS=45`, `COLD_ARCHIVE_DAYS=365`.

**Ротация архива (`rotate_cold_archive`):** при каждом полном прогоне дела,
заархивированные более `COLD_ARCHIVE_DAYS` назад (по полю `archived_at`),
выносятся из горячего [data/cases_archive.json](data/cases_archive.json) в
холодные годовые `data/cases_archive_YYYY.json`. Якорь `archived_at` ставится
при переносе в архив; старым делам без штампа он бэкфиллится из дат стадий.
Фронт холодные файлы не грузит — их id подмешиваются только в индекс
дедупликации (`existing_ids`), чтобы старое дело не всплыло как «новое».
Холодные дела не сканируются `reactivate_archived_first_instance` (возврат —
вручную через [scripts/add_cases_manually.py](scripts/add_cases_manually.py)).

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
Второй канал восстановления — `link_cassation_cases`: если карточка 7kas
сматчилась с делом из горячего архива (ушло из `cassation_watch` по
120-дневному окну до регистрации жалобы на 7kas), дело возвращается в
активные со всей историей вместо создания discovery-дубля; карточки
прошлых кругов (их 8Г-номер уже в `history`) ничего не воскрешают.

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

# Тесты (оба каталога одним прогоном, см. pytest.ini)
python3 -m pytest

# После правок модулей court_monitor: обновить якоря строк в docs/technical и CLAUDE.md
python3 scripts/refresh_doc_anchors.py --write

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
- **Персонализация push по watchlist (`_per_sub` callback):** push-payload собирается под каждого подписчика отдельно через фабрику `_make_per_sub_callback` ([scripts/court_monitor/delivery.py:305](scripts/court_monitor/delivery.py:305)). Новые дела (`fi_new_cases`, `appeal_new_cases_csv`) — общесистемный сигнал, шлются всем; изменения и переходы стадий — только если дело в watchlist подписчика. Click_url для подписчиков с watchlist — `?digest=open&mine=1`. Используется в основном кроне (`main_json`), `--replay-last`, `--push-last-digest`.

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

**Журнал последней push-рассылки** — на каждой карточке раскрываемый блок «🪞 Последний push для этой подписки». Показывает variant (personal/general/skip/broadcast), title, body, click_url, timestamp. Источник — [data/last_personal_pushes.json](data/last_personal_pushes.json), перезаписывается каждый прогон `send_web_push` в `court_monitor/delivery.py`. Если по подписке уход push'а в этом прогоне был skipped (нет событий по watchlist) — блок показывает «Push не отправлен — нет событий по watchlist».

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
- Логики парсинга таблиц → `TableExtractor` ([scripts/court_monitor/parsing/tables.py:13](scripts/court_monitor/parsing/tables.py:13)).
- Фронтенда (фильтры, рендер) → [app.js](app.js).
- Конкретного workflow → соответствующий `.github/workflows/*.yml`.

Иначе — этой карты достаточно, не нужно запускать Grep/Glob с нуля.
