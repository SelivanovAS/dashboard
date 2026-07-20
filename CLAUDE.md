# CLAUDE.md

Карта проекта для новых сессий — чтобы не тратить токены на разведку.

> 📚 **Полная техническая документация** — [docs/technical/README.md](docs/technical/README.md).
> Этот файл — быстрая карта (где что в коде); `docs/technical/` — глубокий
> справочник «как всё работает» (архитектура, модель данных, жизненный цикл,
> парсеры, конвейер, дайджест, доставка, фронтенд, worker, эксплуатация).

## Что это

Дашборд юриста ПАО Сбербанк: мониторинг гражданских дел в 20 судах ХМАО-Югры (первая инстанция) + апелляция (Суд ХМАО-Югры) + кассация (7-й кассационный суд общей юрисдикции, фильтр по 1-й инст. ХМАО). AI-дайджесты в Telegram, автозапуск через Cloudflare Worker cron → GitHub Actions. Пользователь — юрист банка, общение на русском.

**С 15.07.2026 система регионализована** (тиражирование на территории Уральского банка, этап 1 — Свердловская обл.+ЯНАО): регион = конфиг в `scripts/court_monitor/regions/` (выбор — env `REGION`, дефолт hmao), территория = форк, отличающийся только Variables/секретами и тремя «файлами территории» (`region_front.js` — обязательный `STORAGE_NS` (неймспейс localStorage: фронты на одном origin github.io, без него звёзды/заметки перемешиваются между территориями; эталон NS не задаёт), `manifest.json`, `wrangler.toml`). Апелляций в регионе может быть НЕСКОЛЬКО (`APPEAL_COURTS`, `appeal.court_domain` в JSON, составной ключ связки). Подробно — [docs/Тиражирование_регионы.md](docs/Тиражирование_регионы.md).

## Главные файлы

- [scripts/update_cases.py](scripts/update_cases.py) — **тонкий фасад CLI** (~220 строк): разбор argv + ре-экспорт прежних имён. Весь код — в пакете `scripts/court_monitor/` (распил монолита, см. [docs/Распил_монолита_контекст.md](docs/Распил_монолита_контекст.md)).
- `scripts/court_monitor/` — **пакет модулей** (читать только нужный):
  - [config.py](scripts/court_monitor/config.py) — env-константы, пути данных, окна state-machine, `log` (пишет в **stdout**), `METRICS`. Патчабельные константы код читает ТОЛЬКО как `config.X` — тесты патчат `monkeypatch.setattr(config, ...)`.
  - [ghlog.py](scripts/court_monitor/ghlog.py) — GitHub Actions: сворачиваемые группы фаз (`::group::`) и аннотации `::warning::`/`::error::`. Включается только env `LOG_GH_ANNOTATIONS=1` (ставят боевые workflow; pytest в CI не должен плодить аннотации), без него всё no-op.
  - [textutil.py](scripts/court_monitor/textutil.py) — даты, HTML-очистка, экранирование, сокращение имён сторон/судов, производственный календарь.
  - [netutil.py](scripts/court_monitor/netutil.py) — `session`, `fetch_page` (ретраи, win-1251; `context=` — номер дела/суд в WARNING/ERROR), `fetch_card_checked` (карточки/тексты актов: детект проверочного кода → WARNING + `METRICS["cards_captcha"]` + пропуск; карточный детектор строже поискового — фразы из СМС-цитат актов о мошенничестве не матчит; с 20.07.2026 — детект заглушки/блока `looks_like_non_card_page` (аутейдж sudrf «Информация временно недоступна» отдавал HTTP 200 без таблиц и молча засчитывался успешной проверкой) → `METRICS["cards_blocked"]` + 🩺-алерт + пропуск; второй рубеж в FI-цикле — `card_is_empty_shell`: 0 таблиц не бумпает `last_checked_at`), `polite_delay`.
  - [regions/](scripts/court_monitor/regions/__init__.py) — **регионы-конфиги**: `base.py` (типы `CourtConfig`/`RegionConfig`), `hmao.py` (реестры ХМАО), `get_region()` (env `REGION` → `config.REGION`, ленивый importlib). Новая территория = новый модуль здесь, форк задаёт только `REGION`.
  - [courts.py](scripts/court_monitor/courts.py) — **фасад активного региона**: ре-экспорт `APPEAL_COURTS`/`APPEAL_COURT`/`FIRST_INSTANCE_COURTS`/`CASSATION_COURT`, матчер `match_region_first_instance` (`match_hmao_first_instance` — legacy-обёртка), `appeal_court_by_domain`, URL карточек.
  - [storage.py](scripts/court_monitor/storage.py) — cases.json/CSV, `.digested_acts`, `.cassation_acts`, кэш пересказов.
  - [health.py](scripts/court_monitor/health.py) — журнал здоровья парсеров + детектор молчаливой поломки.
  - [lifecycle.py](scripts/court_monitor/lifecycle.py) — классификация событий карточки, state machine стадий, дедуп, архив.
  - [parsing/](scripts/court_monitor/parsing/__init__.py) — `tables.py` (TableExtractor), `search.py` (поисковая выдача), `cards.py` (карточки дел), `cassation.py` (7kas).
  - [linking.py](scripts/court_monitor/linking.py) — связка FI ↔ апелляция ↔ кассация, discovery, реактивация, ротация архива.
  - [digest/](scripts/court_monitor/digest/__init__.py) — `llm.py` (Claude/GigaChat/OpenRouter — выбор через `LLM_PROVIDER`; промпты — патч-цели тестов живут тут), `postprocess.py` (валидация/чистка HTML), `template.py` (программный рендер — **боевой путь с 03.07.2026**, компакт-вёрстка без отступов), `core.py` (диспетчер `generate_digest`), `lint.py` (программный линтер готового HTML после отправки: полнота номеров, счётчики (N), теги, футер → 🩺-алерт; `DIGEST_LINT=0` — выключатель). Прод — гибрид: события рендерит код, LLM только пересказывает мотивировки актов; `DIGEST_FULL_LLM=1` — откат на полный LLM-дайджест.
  - [delivery.py](scripts/court_monitor/delivery.py) — Telegram, Web Push с watchlist-персонализацией, алерты.
  - [runs.py](scripts/court_monitor/runs.py) — `main_json` и остальные режимы прогона, `update_active_cases`.
- [scripts/add_cases_manually.py](scripts/add_cases_manually.py) — ручное добавление дел 1-й инстанции.
- [scripts/import_search_dump.py](scripts/import_search_dump.py) — **офлайн-импортёр дампов выдачи капчёвых судов** (Свердловская обл.: 54 записи реестра со `search_gated=True` — автопоиск выключен, карточки мониторятся). Оператор решает капчу → вставляет дамп в секцию «Импорт дел» админки → Worker кладёт в KV + диспатчит [import_cases.yml](.github/workflows/import_cases.yml) → импортёр (utf-8→win-1251, нормализация pretty-print, дедуп `collect_existing_ids`, `srv_num` из href; **только «банк-ответчик»**, как в автопоиске — истец/третье лицо идут в отчёт как `[SKIPPED ROLE]`, решение юриста 16.07.2026 после первого живого импорта; промоушен М→2 при комбо-номере — `[PROMOTED]`, зеркало main_json) → коммит cases.json → итог назад в админку (`/import-result`, журнал `import:log:*`). **Защита «дамп ↔ выбранный суд» (17.07.2026):** хосты абсолютных href карточек (`name=sud_delo`) + маркер Chrome «saved from url» сверяются с судом импорта на трёх уровнях — автоопределение суда в админке (`impDetectDomains`/`impRunDetect`, подставляет суд сам, ручной выбор не перебивает), 400 Worker'а (`detectDumpSudrfHosts`), `EXIT_WRONG_COURT=5` импортёра (`detect_dump_hosts`); `delo_id` из href карточек ловит выдачу не того раздела (суды 1-й инст. он не различает — у всех 1540005). Относительные href (файл Firefox) хостов не несут — проверки молчат. Дела получают служебный блок `"import": {operator, at, source, announced}`; ближайший прогон объявляет их «новыми исками» в дайджесте/пуше один раз (`announce_imported_cases`, runs.py). Подробно — [docs/Тиражирование_регионы.md](docs/Тиражирование_регионы.md).
- [scripts/build_region_registry.py](scripts/build_region_registry.py) + [.github/workflows/probe_region_registry.yml](.github/workflows/probe_region_registry.yml) — проба реестра территории с GitHub-раннера (delo_id + классификация капчи; вход `ops/region_probe/courts_probe.csv`, отчёт коммитится в `ops/region_probe/report.txt`).
- `scripts/tests/` + `tests/` — pytest-набор (320+ тестов: парсеры, state machine, линковка, архив, детектор здоровья, рендер дайджеста — матрица всех 29 типов событий в [tests/test_digest_template_events.py](tests/test_digest_template_events.py), линтер). Запуск одним прогоном: `python3 -m pytest` из корня (конфиг — [pytest.ini](pytest.ini)); CI гоняет на каждый push ([.github/workflows/tests.yml](.github/workflows/tests.yml)).
- [data/cases.json](data/cases.json) — активные дела (UTF-8, `version: 1`, `updated_at` ISO).
- [data/cases_archive.json](data/cases_archive.json) — «горячий» архив: дела, заархивированные за последние 12 мес. (`COLD_ARCHIVE_DAYS`). Грузится фронтом.
- `data/cases_archive_YYYY.json` — «холодные» годовые архивы: дела старше года, вынесенные ротацией (`rotate_cold_archive`). **Фронт их не грузит** (чтобы вес не рос безгранично), но скрипт читает их в индекс дедупликации. Холодные дела «заморожены»: не реактивируются автоматически.
- `data/.digested_acts` — дедуп уже обработанных судебных актов (скрытый файл).
- `data/.cassation_acts` — дедуп кассационных определений: ключи «8Г-номер|дата акта», чьи `new_act` уже уходили в дайджест. Гасит повторный `new_act` при «мигании» `act_published` (сбойный парс 7kas). Ведётся в `link_cassation_cases`.
- `data/.act_summaries.json` — кэш LLM-пересказов мотивировок актов (ключ `sha1(act_text+"|v3-detailed")[:16]`; маркер стиля бампается только при смене формата пересказа — с 14.07.2026 это 2-3 предложения ≤450 симв.). Пополняется на GitHub-replay (на Mac ключа Anthropic нет), коммитится workflow'ами — без коммита каждый replay заново оплачивал бы пересказ тех же актов.
- `data/parse_health.json` — журнал здоровья парсеров: пер-источник история количества результатов поиска (20 судов 1-й инст., апелляция, 7kas до/после HMAO-фильтра). Для судов 1-й инст. счётчик — сберовские строки ДО фильтра «банк-ответчик» (`stats["sber_rows"]` из `parse_first_instance_search`): вал исков самого банка вытесняет ответчик-дела со стр. 1 и обнулял бы метрику без поломки (ложный алерт по Октябрьскому р/с 14–15.07.2026). Детектор «молчаливой поломки» (`update_parse_health`, блок 4e в `main_json`) шлёт сервисный 🩺-алерт в Telegram: суд с медианой ≥1 вернул 0 (на 1-м и 3-м нулевом прогоне + сообщение о восстановлении), HTTP-фейл 3 прогона подряд, все источники разом по нулям, ≥5 карточек-«огрызков» за прогон.
- [data/last_digest_context.json](data/last_digest_context.json) — снимок контекста для `--replay-last`.
- [data/last_personal_pushes.json](data/last_personal_pushes.json) — журнал последней push-рассылки (что получила каждая подписка): variant, title, body, click_url. Перезаписывается на каждом прогоне `send_web_push`. Читается админкой подписчиков.
- [data/sberbank_cases.csv](data/sberbank_cases.csv) + архив — legacy CSV (UTF-8 с BOM), всё ещё коммитится для совместимости.
- [app.js](app.js) + [sberbank_dashboard.html](sberbank_dashboard.html) + [styles.css](styles.css) — SPA-фронт (GitHub Pages).
- [cloudflare-worker/wrangler.toml](cloudflare-worker/wrangler.toml) + [cloudflare-worker/worker.js](cloudflare-worker/worker.js) — автозапуск, push-подписки, админ-эндпоинты; [cloudflare-worker/admin_page.js](cloudflare-worker/admin_page.js) — HTML/JS страницы админки (см. «Админка подписчиков»).
- [.github/workflows/update_cases.yml](.github/workflows/update_cases.yml) — основной workflow (парсинг + дайджест + commit). При падении любого шага шлёт 🚨-алерт в личный Telegram (шаг `if: failure()`, curl без Python).
- [.github/workflows/tests.yml](.github/workflows/tests.yml) — pytest на каждый push (кроме правок только .md/docs).
- [.github/workflows/test_digest.yml](.github/workflows/test_digest.yml) — единый ручной тест: replay последнего дайджеста, Telegram (личный/группа по галке), PWA push (владельцу/всем по галке), выбор LLM-провайдера (`llm_provider`: claude/gigachat/openrouter) и модели: списки `gigachat_model` (GigaChat-2-Pro/2/2-Max) и `openrouter_model` (место в рейтинге shir-man: «модель дня (топ-1)»…«топ-5», id резолвится на прогоне — список не протухает), текстовое `llm_model` перебивает оба. Публикация результатов (`last_digest.json`, `cases.json`, кэш пересказов) и PWA push — только по галке `commit_results` (по умолчанию ВЫКЛ: тестовый дайджест не попадает на дашборд, пуш не уходит — он вёл бы на старый дайджест; без галки прогон шлёт только Telegram).
- [README.md](README.md) — подробная документация на русском (дублирует часть этого файла).

## Ключевые точки в пакете court_monitor

| Что | Где |
|-----|-----|
| dataclass конфига суда: `CourtConfig` | [scripts/court_monitor/regions/base.py:32](scripts/court_monitor/regions/base.py:32) |
| `APPEAL_COURT` (конфиг апелляции) | [scripts/court_monitor/courts.py:36](scripts/court_monitor/courts.py:36) |
| массив 20 судов: `FIRST_INSTANCE_COURTS` | [scripts/court_monitor/courts.py:38](scripts/court_monitor/courts.py:38) |
| `CASSATION_COURT` (7kas.sudrf.ru, гражданская кассация) | [scripts/court_monitor/courts.py:40](scripts/court_monitor/courts.py:40) |
| `match_hmao_first_instance` (длинная форма → CourtConfig) | [scripts/court_monitor/courts.py:106](scripts/court_monitor/courts.py:106) |
| `RegionConfig` (регион-конфиг: суды, маркеры, public_info) | [scripts/court_monitor/regions/base.py:170](scripts/court_monitor/regions/base.py:170) |
| `CourtConfig.search_gated` (капча: поиск выкл., карточки мониторятся) | [scripts/court_monitor/regions/base.py:39](scripts/court_monitor/regions/base.py:39) |
| `courts_for_search` (суды автопоиска: enabled и не gated) | [scripts/court_monitor/courts.py:43](scripts/court_monitor/courts.py:43) |
| `collect_existing_ids` (общий дедуп-индекс main_json/импортёра) | [scripts/court_monitor/linking.py:1009](scripts/court_monitor/linking.py:1009) |
| `get_region` (env REGION → RegionConfig, ленивый лоадер) | [scripts/court_monitor/regions/__init__.py:20](scripts/court_monitor/regions/__init__.py:20) |
| `match_region_first_instance` (обобщённый матчер по региону) | [scripts/court_monitor/courts.py:58](scripts/court_monitor/courts.py:58) |
| `appeal_court_by_domain` (апел-суд по appeal.court_domain) | [scripts/court_monitor/courts.py:132](scripts/court_monitor/courts.py:132) |
| `appeal_court_for_fi_domain` (апел-суд по домену суда 1-й инст.) | [scripts/court_monitor/courts.py:159](scripts/court_monitor/courts.py:159) |
| `CourtConfig.search_by_fi_number_url` (целевой поиск апелляции по номеру 1-й инст., G2_CASE__CASE_NUMBER_ISS) | [scripts/court_monitor/regions/base.py:114](scripts/court_monitor/regions/base.py:114) |
| `relink_awaiting_appeal` (дослинк awaiting_appeal, не попавших на стр. 1 поиска апелляции) | [scripts/court_monitor/runs.py:150](scripts/court_monitor/runs.py:150) |
| `migrate_appeal_court_fields` (бэкфилл суда в блоках appeal) | [scripts/court_monitor/lifecycle.py:613](scripts/court_monitor/lifecycle.py:613) |
| `fetch_card_checked` (карточный fetch с детектом кода) | [scripts/court_monitor/netutil.py:79](scripts/court_monitor/netutil.py:79) |
| `DIGESTED_ACTS_PATH` / `CASSATION_ACTS_PATH` / `PARSE_HEALTH_PATH` | [scripts/court_monitor/config.py:109](scripts/court_monitor/config.py:109) |
| Константы state-machine (`FI_ARCHIVE_DAYS`, `CASSATION_*`) | [scripts/court_monitor/config.py:99](scripts/court_monitor/config.py:99) |
| `update_parse_health` — детектор молчаливой поломки парсеров | [scripts/court_monitor/health.py:42](scripts/court_monitor/health.py:42) |
| `advance_case_stage` / `is_case_archived` / `migrate_stages` | [scripts/court_monitor/lifecycle.py:643](scripts/court_monitor/lifecycle.py:643) |
| `reactivate_archived_first_instance` (возврат из архива) | [scripts/court_monitor/linking.py:375](scripts/court_monitor/linking.py:375) |
| `backfill_fi_links` (достройка `fi.link` у дел «с апелляции» — без неё cassation_watch слеп) | [scripts/court_monitor/linking.py:275](scripts/court_monitor/linking.py:275) |
| `rotate_cold_archive` (горячий → холодный архив) | [scripts/court_monitor/linking.py:941](scripts/court_monitor/linking.py:941) |
| `class TableExtractor(HTMLParser)` — парсер карточек дела | [scripts/court_monitor/parsing/tables.py:13](scripts/court_monitor/parsing/tables.py:13) |
| `parse_case_card` — карточка 1-й инст./апелляции | [scripts/court_monitor/parsing/cards.py:206](scripts/court_monitor/parsing/cards.py:206) |
| `parse_cassation_search_page` — поиск 7kas (HMAO-фильтр) | [scripts/court_monitor/parsing/cassation.py:50](scripts/court_monitor/parsing/cassation.py:50) |
| `classify_cassation_outcome` — детерм. enum исхода | [scripts/court_monitor/parsing/cassation.py:180](scripts/court_monitor/parsing/cassation.py:180) |
| `parse_cassation_card` + `_extract_cassation_act_text` (`cont_doc1`) | [scripts/court_monitor/parsing/cassation.py:361](scripts/court_monitor/parsing/cassation.py:361) |
| `relink_awaiting_relink_first_instance` (re-link после remanded) | [scripts/court_monitor/linking.py:232](scripts/court_monitor/linking.py:232) |
| `link_cases` (FI ↔ апелляция) | [scripts/court_monitor/linking.py:52](scripts/court_monitor/linking.py:52) |
| `link_cassation_cases` (link + discovery + remanded + архив + дедуп актов) | [scripts/court_monitor/linking.py:525](scripts/court_monitor/linking.py:525) |
| `update_active_cases` (обход карточек активных дел) | [scripts/court_monitor/runs.py:298](scripts/court_monitor/runs.py:298) |
| `main_json` (оркестрация полного прогона) | [scripts/court_monitor/runs.py:1415](scripts/court_monitor/runs.py:1415) |
| `GIGACHAT_SYSTEM_PROMPT` | [scripts/court_monitor/digest/llm.py:76](scripts/court_monitor/digest/llm.py:76) |
| `def generate_digest` — диспетчер дайджеста | [scripts/court_monitor/digest/core.py:333](scripts/court_monitor/digest/core.py:333) |
| `summarize_act_motivation` — LLM-пересказ акта | [scripts/court_monitor/digest/llm.py:871](scripts/court_monitor/digest/llm.py:871) |
| `polish_digest_html` — LLM-полировщик (опц.) | [scripts/court_monitor/digest/llm.py:1114](scripts/court_monitor/digest/llm.py:1114) |
| Пост-обработка HTML (`_ensure_*`/`_validate_*`/`_drop_*`/`_normalize_*`) | весь [scripts/court_monitor/digest/postprocess.py](scripts/court_monitor/digest/postprocess.py) |
| Claude model: `claude-haiku-4-5-20251001` (`_current_digest_model_name`) | [scripts/court_monitor/digest/llm.py:1255](scripts/court_monitor/digest/llm.py:1255) |
| `def generate_template_digest` — программный рендер | [scripts/court_monitor/digest/template.py:322](scripts/court_monitor/digest/template.py:322) |
| доставка: `send_telegram` | [scripts/court_monitor/delivery.py:617](scripts/court_monitor/delivery.py:617) |
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
         // events[] — событие «Движения дела». Базовые поля {date, time, text}
         // + колонки карточки, если шапка распозналась: name, place,
         // result_event, ground, note, posted_at (имена зеркалят парсер
         // кассации — фронт рендерит все три инстанции одним кодом).
         // ⚠ text — склейка всех ячеек через ". " — НЕ МЕНЯТЬ: по паре
         // (date, text) дедуплицирует _events_newly_match, смена формата
         // объявит всю историю дел новой (дайджест-паводок).
         "court", "judge", "status", "events": [], "resolved_emitted": bool,
         "hearing_date",           // дата резолютивки, якорь 45-дневного окна
         "act_date",               // дата публикации мотивировки (когда есть)
         "appeal_filed", "appeal_filed_date",        // апел. жалоба в карточке 1-й инст.
         "cassation_filed", "cassation_filed_date",  // касс. жалоба (идёт через 1-ю инст.)
         "sent_to_cassation", "sent_to_cassation_date"
      },
      "appeal":         { "court", "court_domain", "delo_id",   // суд апелляции дела (в регионе их может быть >1; миграция migrate_appeal_court_fields)
                          "status", "result", "events": [], "act_published", "hearing_date", "act_date", ... },
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

## Автозапуск (с 05.07.2026 — облако; D2/Mac — спящий резерв)

> **История.** 02.07.2026 суды начали резать иностранные IP (`*.sudrf.ru` молча
> дропал TLS с не-российских адресов: TCP проходит, хендшейк — нет) → парсинг с
> GitHub (США) встал, и за день собрали раскол D2: парсинг на Mac юриста +
> дайджест на GitHub. 05.07.2026 проверка [.github/workflows/probe_courts.yml](.github/workflows/probe_courts.yml)
> с раннера GitHub показала: **блок сняли и для США** — US-IP снова получает 200
> + данные по всем судам (побайтово те же страницы, что из РФ). Полный прогон
> снова умещается в один раннер → автозапуск возвращён в облако, Mac — в резерв.
> ⚠️ Блок появился и ушёл за 3 дня — может вернуться (см. «Процедура флипа»).

### Основной путь — облако (бесплатно, без включённой машины)

- **Полный прогон на GitHub Actions:** [.github/workflows/update_cases.yml](.github/workflows/update_cases.yml)
  — Cloudflare Worker по крону дёргает его через `workflow_dispatch` (03:45 UTC =
  08:45 ХМАО, пн-пт) — гоняет `python scripts/update_cases.py --json` целиком: парсинг 20 судов +
  апелляция + 7kas → гибридный дайджест (программный рендер + Claude только на
  пересказ мотивировок; откат — `DIGEST_FULL_LLM: "1"` в env) → Telegram (личный
  чат `TELEGRAM_CHAT_ID_TEST`) + Web Push всем подписчикам → коммит данных.
  Плановый прогон идёт со `smart_skip=true` (пропуск нерабочих дней РФ и дел с
  известной будущей датой); ручной — по галке. Падение шага → 🚨-алерт в личный
  Telegram (шаг `if: failure()`, curl без Python).
- **Живой лог прогона (с 13.07.2026):** stdout прогона идёт через pass-through-пушер
  [scripts/gh_progress_pusher.py](scripts/gh_progress_pusher.py) (`… --json 2>&1 |
  python -u …`, `set -o pipefail`) → батчи на `POST /run-progress` Worker'а →
  блок «Прогон (GitHub Actions)» в админке (свёртка по фазам «— [N/9]»), лог
  хранится в KV 14 дней (current + prev, cap 1000 строк). Токен —
  `secrets.PUSH_SECRET || secrets.PROGRESS_SECRET` (Worker принимает оба;
  PUSH_SECRET первым — им же ходит пуш-доставка, он проверяемо совпадает);
  без секретов пушер — чистый cat, прогон не страдает, но объявляет об этом
  одной строкой в логе рана, а первый сбой POST печатает одну ⚠️-строку с
  HTTP-кодом. ⚠️ Пушер обязан слать свой `User-Agent` (константа
  `USER_AGENT`): дефолтный `Python-urllib/…` Cloudflare режет на workers.dev
  (ошибка 1010 → 403 до Worker'а) — так канал молчал 13–16.07.2026.
- **Секреты** уже в repo secrets (`ANTHROPIC_API_KEY`, `TELEGRAM_*`, `PUSH_*`) —
  новых не нужно.
- **Планировщик — Cloudflare Worker cron** (`crons = ["45 3 * * mon-fri"]` в
  [cloudflare-worker/wrangler.toml](cloudflare-worker/wrangler.toml); применяется
  только после `wrangler deploy`). Worker дёргает `workflow_dispatch`
  `update_cases.yml` с `smart_skip=true` (`worker.js:1139` — `scheduled`).
  ⚠️ `mon-fri` буквами, не `1-5`: Cloudflare нумерует дни 1=Sun..7=Sat. push-
  подписки и админка Worker'а — там же.

### Спящий резерв — D2 (Mac-парсинг), НЕ демонтирован

На случай, если суды снова закроют иностранные IP. Ничего в коде/репо не удалено:

- **Парсинг на Mac юриста** (физически в сети Сбера, egress РФ): LaunchAgent
  `com.court-monitor.parse` → [ops/mac-local-run/parse_and_push.sh](ops/mac-local-run/parse_and_push.sh)
  (preflight «в сети Сбера?» → маршрут судов мимо VPN через шлюз `10.217.111.250`
  → `ops/mac-local-run/run_parse.py` = `main_json` с заглушённой
  `validate_environment` → `git commit && push`). Сейчас **усыплён**:
  `launchctl unload ~/Library/LaunchAgents/com.court-monitor.parse.plist` (плист
  на месте, реактивация — `launchctl load …`).
- **Дайджест-на-push:** [.github/workflows/replay_on_push.yml](.github/workflows/replay_on_push.yml)
  ловил `push` с изменённым `data/last_digest_context.json` → `--replay-last
  --push-all`. Сейчас **заглушён** (`if: false` на job'е) — страховка от второго
  дайджеста, если Mac ещё раз отработает до выгрузки LaunchAgent'а. Разбудить —
  вернуть `if: github.actor != 'github-actions[bot]'`. (Push облачного крона и
  так идёт через GITHUB_TOKEN и его бы не триггерил.)
- **Живой просмотр парсинга (для резерва):** ярлык `ops/mac-local-run/Парсинг судов.command`
  + блок живого лога в админке Worker (`progress_pusher.py` → `POST /run-progress`,
  auth — Worker-секрет `PROGRESS_SECRET`, токен на Mac в
  `~/.config/court-monitor/progress_token` вне репо). Канал `/run-progress`
  общий с облачным пушером (`scripts/gh_progress_pusher.py`, `source:"github"`);
  записи Mac-пушера без `source` админка подписывает «Парсинг на Mac (резерв)».

### Процедура флипа обратно на Mac (если блок вернётся)

1. Сигнал: 🩺-алерт «все источники по нулям» / 🚨-падение прогона; при сомнении —
   запустить `probe_courts.yml` вручную (Actions → Run workflow).
2. Отключить облако: вернуть `crons = []` в `wrangler.toml` + `wrangler deploy`
   (Worker перестанет дёргать прогон).
3. Включить дайджест-на-push: в `replay_on_push.yml` вернуть
   `if: github.actor != 'github-actions[bot]'` вместо `if: false` (коммит).
4. Разбудить Mac: `launchctl load ~/Library/LaunchAgents/com.court-monitor.parse.plist`.

Детали установки/отката Mac-звена — [ops/mac-local-run/README.md](ops/mac-local-run/README.md).

## Жизненный цикл дела (state machine)

Семь рабочих стадий в `current_stage` + архив. Переходы — в
`advance_case_stage()`, архивация — в `is_case_archived()`.

| Стадия | Что парсим | Что запускает переход |
|---|---|---|
| `first_instance` | карточка 1-й инст. | подана апел. жалоба → `awaiting_appeal` · 60 дней от hearing_date без жалобы → архив (с возможностью реактивации при появлении жалобы) |
| `awaiting_appeal` | карточка 1-й инст. — ПОКА не `sent_to_appeal`; после `sent_to_appeal` — целевой дослинк `relink_awaiting_appeal` (запрос к апел-суду по номеру 1-й инст., G2_CASE__CASE_NUMBER_ISS: дела не со стр. 1 поиска по «Сбербанк» — например, заведённые импортёром после регистрации апелляции) | link_cases находит апел. карточку → `appeal` · бессрочно, не архивируется |
| `appeal` | карточка апел. суда | опубликован акт ИЛИ 30 дней от апел. заседания без акта → `cassation_watch` · не архивируется по времени |
| `cassation_watch` | карточка 1-й инст. (ищем касс. жалобу) — КРОМЕ дел, где банк «Третье лицо»: их не парсим, ждём дело на 7kas | касс. жалоба или направление в кассац. суд → `cassation_pending` · 120 дней от апел. заседания → архив |
| `cassation_pending` | карточка 1-й инст. — ПОКА не `sent_to_cassation` (потом ничего, ждём карточку на 7kas) | link_cassation_cases находит карточку → `cassation` · не архивируется |
| `cassation` | карточка 7kas (гражданская кассация) | `outcome=cassation_remanded` → `awaiting_relink` (re-link при появлении новой карточки в нижестоящей) · `act_published` + 30 дней / `decision_date` + 45 дней без акта → архив (для финальных исходов, кроме remanded) |
| `awaiting_relink` | ничего (ждём карточку в нижестоящей инст.) | парсер 1-й инст. находит дело → `first_instance` (round +1, прошлые блоки в `history`) ИЛИ парсер апел. → `appeal` · бессрочно, не архивируется |

**Что парсим на прогоне (`should_parse_fi_card`, [lifecycle.py](scripts/court_monitor/lifecycle.py)):**
карточку 1-й инст. парсим в `first_instance`, `cassation_watch`, а также в
`awaiting_appeal`/`cassation_pending` — но только ПОКА дело не направлено в
вышестоящий суд (`sent_to_appeal` / `sent_to_cassation`). Смысл: после подачи
жалобы продолжаем следить за карточкой 1-й инст. (ловим «направлено в
кассацию/апелляцию» и промежуточные события), а как дело ушло наверх — ждём
только появления карточки в вышестоящем суде (`link_cases`/`link_cassation_cases`).
Этот же предикат гейтит `backfill_fi_links`. Исключение (13.07.2026): в
`cassation_watch` дела с ролью банка «Третье лицо» НЕ парсим — кассацию по ним
обнаружит поиск 7kas по имени банка (`link_cassation_cases` догонит активное
дело или воскресит архивное); теряется только раннее `fi_cassation_filed`.
**Эхо-фильтр дайджеста (`suppress_fi_echo_events`, lifecycle.py):** если
вышестоящая карточка уже связана (`appeal_card_linked`/`cassation_card_linked`),
«догоняющие» события FI-карточки НЕ идут в дайджест: жалобы (`fi_appeal_filed`,
`fi_cassation_filed`, `fi_sent_to_cassation`) + весь catch-up класс
(`fi_resolved`, `fi_act_published`, `fi_act_text_published`,
`fi_motivirovka_emitted`, `fi_final_event`, `fi_status_change`) — юрист всё
это знает из апел./касс. карточки (паводок 07.07: 60 первых парсов дали
272 события и 48 КБ). Флаги в JSON ставятся как обычно — state machine,
бейджи и drawer не затронуты. Там же схлопывается дубль «решение
изготовлено»+«текст опубликован» об одном акте в одном прогоне.
Рядом — **стародатный фильтр** (`suppress_stale_fi_events`): анонс заседания
с датой в прошлом и жалобы старше `DIGEST_STALE_EVENT_DAYS` (45 дн.) в
дайджест не идут, и **дедуп** (`dedupe_fi_changes`): одно FI-дело в двух
записях (апелляция по существу + частная жалоба) не двоится. Replay-режимы
(`--replay-last`/`--push-last-digest`) прогоняют сохранённый контекст через
все три фильтра (`_filter_ctx_fi_changes_echo` в runs.py).

Константы в [scripts/court_monitor/runs.py:1192](scripts/court_monitor/runs.py:1192):
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
- `CLAUDE_MODEL` — модель Claude (алиасы haiku/sonnet/opus или точный id; пусто = боевой эталон haiku 4.5). Ставит только test_digest.yml; боевой крон переменную не задаёт.
- `CLAUDE_EFFORT` — уровень усилий (`low`/`medium`/`high`/`xhigh`/`max`) для Sonnet 5/Opus 4.8 → `output_config.effort`; пусто/`default` = не отправлять (у API дефолт high). Для haiku игнорируется — модель эффорт не поддерживает.
- `GIGACHAT_AUTH_KEY` (+ `GIGACHAT_SCOPE`, `GIGACHAT_MODEL`) — GigaChat, альтернативный LLM; включается `LLM_PROVIDER=gigachat` (отдельный workflow удалён 09.07.2026, теперь выбор провайдера — input `llm_provider` в test_digest.yml).
- `OPENROUTER_API_KEY` (+ `OPENROUTER_MODEL`) — OpenRouter, третий LLM (тестовый контур); включается `LLM_PROVIDER=openrouter`. Пустая модель = «модель дня» с `shir-man.com/api/free-llm/top-models`, fallback `openrouter/free`. Кэш пересказов для gigachat/openrouter неймспейсится по `провайдер:модель` (`_act_cache_key`), Claude-ключи прежние.
- `TELEGRAM_BOT_TOKEN` — токен бота.
- `TELEGRAM_CHAT_ID` — корпоративная группа (используется только при `to_group=true`).
- `TELEGRAM_CHAT_ID_TEST` — личный чат, дефолтный получатель дайджеста.
- `TELEGRAM_CHAT_ID_PERSONAL` — личный чат юриста (workflow'ы передают тот же `TELEGRAM_CHAT_ID_TEST`): при совпадении с `TELEGRAM_CHAT_ID` к Telegram-версии дайджеста добавляется сервисная приписка «🤖 LLM: модель (режим)» — в группу и на дашборд не попадает (`_telegram_digest_text` в runs.py).
- `PUSH_WORKER_URL`, `PUSH_SECRET`, `VAPID_PRIVATE_KEY` — Web Push для PWA.
- `OWNER_SECRET` — секрет Worker'а для `POST /mark-owner` (пометка устройства владельцем).
- `GITHUB_PAT` — в secrets Worker'а, для `workflow_dispatch`.
- `DIGESTED_ACTS_PATH` — опционально переопределить путь к `.digested_acts`.
- `LOG_LEVEL` — уровень логов прогона (`DEBUG`/`INFO`/`WARNING`/`ERROR`, дефолт `INFO`); `DEBUG` показывает пер-кейсовые skip/«без изменений» и прочую диагностику.

## Куда уходит дайджест

- **Telegram:** все workflow'и шлют в личный чат (`TELEGRAM_CHAT_ID_TEST`) по умолчанию. Чтобы продублировать в корпоративную группу — поставить галку `to_group` в UI Run workflow. Текст дайджеста в Telegram **общий**, не персонализированный.
- **PWA push:** `update_cases.yml` (крон) шлёт всем подписчикам PWA. Тестовый workflow `test_digest.yml` шлёт push **только устройствам-владельцам** по умолчанию, чтобы не спамить коллегам прототипами. У `test_digest.yml` есть галка «push_all» — отправит на все устройства. Чтобы пометить своё устройство владельцем — открыть PWA по URL `https://selivanovas.github.io/dashboard/sberbank_dashboard.html?owner=<OWNER_SECRET>` (один раз).
- **Персонализация push по watchlist (`_per_sub` callback):** push-payload собирается под каждого подписчика отдельно через фабрику `_make_per_sub_callback` ([scripts/court_monitor/delivery.py:305](scripts/court_monitor/delivery.py:305)). Новые дела (`fi_new_cases`, `appeal_new_cases_csv`) — общесистемный сигнал, шлются всем; изменения и переходы стадий — только если дело в watchlist подписчика. Click_url для подписчиков с watchlist — `?digest=open&mine=1`. Используется в основном кроне (`main_json`), `--replay-last`, `--push-last-digest`.

## Админка подписчиков

URL: `https://court-monitor-trigger.7selivanov-a.workers.dev/admin?secret=<OWNER_SECRET>`. Открывается в браузере (мобильно тоже). HTML-страница вынесена в [cloudflare-worker/admin_page.js](cloudflare-worker/admin_page.js) (`renderAdminHtml(secret, role, cfg)`, wrangler бандлит импорт сам); серверные эндпоинты — в [cloudflare-worker/worker.js](cloudflare-worker/worker.js). ⚠️ Вся страница — один template literal: внутренний JS пишется без backtick'ов и `${`, backslash удваивается. Naive-таймстампы из data/*.json (Python на UTC-раннере пишет без «Z») страница парсит как UTC (`parseIso`).

**Роли (с 16.07.2026):** owner (`OWNER_SECRET`) — всё; operator (`OPERATOR_SECRET`, один общий на сопровождающих капчёвых судов; не задан — роль неактивна, у ХМАО так) — статус+здоровье+живой лог+секция «Импорт дел». Гейт — `resolveAdminRole`/`requireAdminRole` (worker.js): чужой секрет → 401, оператор на owner-эндпоинте → 403 (реальный запрет на сервере, скрытие в UI — `data-owner-only` + `html[data-role]`). `DISPATCH_WORKFLOWS` — `{inputs, roles}`: update_cases/test_digest — owner, import_cases — обе роли. Секция «Импорт дел» (`#import`): dropdown gated-судов из `region.fi_courts` cases.json (у ХМАО их нет — секция скрыта), вставка rich-paste/файл, поллинг журнала (`/admin/import-log`), история импортов, **светофор свежести по судам** (когда каждый суд импортировался в последний раз: зелёный ≤7 дн, жёлтый ≤14, красный дольше/ни разу; данные — вечные ключи `import:last:<домен>`, пишутся на done в `/import-result`). **С 17.07.2026 — операторский UX (394cd14):** оператору секция идёт ПЕРВОЙ (перестановка серверная — константы `IMPORT_SECTION`/`IMPORT_CHIP` в `renderAdminHtml` вставляются по роли), светофор раскрыт и кликабелен (клик по суду = выбрать его в форме, слушатели — делегированием), 5-я плитка пульта «Импорты» (N просрочено; обеим ролям, без gated-судов скрыта — ХМАО не видит, `.pult.has-import` в мобильном медиа-блоке обязателен), drag-n-drop файла в поле вставки + индикатор «что уйдёт» (файл побеждает вставку) + предпроверка «есть ли ссылки» до отправки, статус ожидания с таймером, сбой cases.json → алерт с «Повторить» (owner'у — тихо, как раньше). **Автоопределение суда по вставке (17.07.2026):** хост из абсолютных ссылок карточек подставляет суд в dropdown сам (пока оператор не выбирал вручную — флаг `impCourtTouched`), конфликт — заметка «⚠ ссылки ведут в …» с кнопкой «выбрать этот суд», отправка чужого дампа блокируется на клиенте, Worker'е (400) и в импортёре (`EXIT_WRONG_COURT`). ВСЕ URL данных страницы выводятся из `CASES_DATA_URL` Worker'а (`adminPageConfig()`) — хардкод сломал бы админку территорий.

**Дизайн v2 (13.07.2026)** — визуальный язык дашборда: токены цветов/шрифтов скопированы из [styles.css](styles.css) (IBM Plex с Google Fonts, сберовский зелёный, бейджи-пилюли, цвета стадий teal/indigo/violet — карта `stageBadge` зеркалит `stageBadgeHtml` из app.js), 3-режимная тема авто/свет/тьма (localStorage `admin_theme`, инлайн-скрипт в head), статусы — цветные точки/пилюли вместо эмодзи, иконки — inline-SVG. При смене палитры дашборда токены админки синхронизировать вручную.

Компоновка: липкая glass-шапка (лого · чипы-якоря «Система/LLM/Подписчики» с подсветкой активной секции через IntersectionObserver · сводка · тоггл темы · Обновить) → **пульт из 4 кликабельных stat-плиток** (Последний прогон ok/сбой/идёт из gh-runs · Дайджест N изменений · Парсеры «все 22 ok»/«N ⚠» · Автозапуск + push-агрегат) → секции:
- **#system** (грид 2 колонки на десктопе): карточка «Прогоны GitHub Actions» — последние 8 runs (точки-статусы, живой пульсирует и автообновляется каждые 15 с, ссылки на GitHub) через GET `/admin/gh-runs` (Worker проксирует GitHub API, PAT на сервере; отдаёт и `next_cron_at` с учётом праздников), кнопки «▶ Полный прогон» (`smart_skip:"false"`) и «Стандартный прогон» (`smart_skip:"true"`, как ежедневный крон; в выходной сразу завершится «нерабочий день») → POST `/admin/dispatch`; внутри же — блок живого лога прогона (данные `GET /admin/run-progress`, поллинг 15 с пока идёт — батчи пушера раз в ~60 с; заголовок по `source`: «Прогон (GitHub Actions)» с ссылкой на run / «Парсинг на Mac (резерв)»; лог сворачивается по фазам «— [N/9]» — `renderLogGroups`, вручную открытые фазы переживают ререндер, у фаз бейджи ⚠/✖, финальная «Сводка прогона» видна без разворачивания; завершённый старше суток — свёрнутый details, предыдущий прогон — вложенный details) | карточка «Здоровье парсеров» из [data/parse_health.json](data/parse_health.json): светофор-точки (красный fail_streak≥3/alerted_zero; жёлтый fail_streak≥1 или ноль при медиане≥1), спарклайны, проблемные вверху, первые 8 + свёрток; имена судов — карта `COURT_NAMES` (синхронизировать при правке `FIRST_INSTANCE_COURTS`).
- **#llm**: топ-5 рейтинга shir-man (браузером напрямую, CORS `*`) + мини-форма запуска `test_digest.yml` через POST `/admin/dispatch`: провайдер, модель (подписи «топ-N» обогащаются рейтингом), галки to_group/push_all/full_llm/commit_results (по умолчанию ВЫКЛ — безопасный прогон в личку; при опасных галках — confirm). У claude — выбор модели (haiku эталон / Sonnet 5 / Opus 4.8, `CLAUDE_MODEL` через input `claude_model`; кэш пересказов не-haiku неймспейсится по модели) и уровня усилий (`claude_effort` → env `CLAUDE_EFFORT` → `output_config.effort`; селектор виден только для sonnet/opus — haiku эффорт не поддерживает). ⚠ Модели нового поколения (Opus 4.7+/Sonnet 5) не принимают `temperature` (400) — пейлоад собирает `llm._claude_payload`: adaptive-мышление + effort вместо температуры, расширенный max_tokens и таймаут; боевой haiku-путь байт-в-байт прежний.
- **#subs**: счётчик + **поиск по подпискам** (имя/устройство/номера и стороны дел watchlist) + карточки: имя, пилюля устройства, бейджи owner/«⏳ истекает» (нет входа 45+ дней — KV-TTL 60), kv-строка дат, свёртки «Последний push» (бейдж варианта; из [data/last_personal_pushes.json](data/last_personal_pushes.json); skip = «нет событий по watchlist») и «Дела» с бейджами стадий, сторонами и судом. Карта дел строится из cases.json **и cases_archive.json** (с 13.07): звезда на завершённом деле — бейдж «в архиве» (в модалке Watchlist такая строка видна с галкой, снять можно; при реактивации дела звезда оживает), номер-сирота (нет ни в активных, ни в архиве — дело удалено вручную или переименовано до Этапа 3) — бейдж «нигде не найдено» + крестик-удаление прямо в строке; счётчик «⚠ N нигде не найдено» — в сводке шапки. Периодический read-only аудит — [scripts/audit_watchlists.py](scripts/audit_watchlists.py). Данные плитки «Дайджест» — из [data/last_digest.json](data/last_digest.json).

Действия по каждой подписке (5 кнопок):
- **✏ Имя** → POST `/admin/label` `{endpoint, label}`. Сохраняет произвольное имя («Иван», «iPhone Дани»).
- **Watchlist** → модалка с чекбоксами по активным делам из `cases.json` (поиск по номеру/сторонам/суду, бейджи стадий, ручное добавление номеров не из списка) → POST `/admin/watchlist` `{endpoint, watchlist}` (сервер канонизирует алиасы).
- **Тест push** → POST `/admin/test-push` `{endpoint}`. Требует Worker-секрет `VAPID_PRIVATE_KEY` (`wrangler secret put VAPID_PRIVATE_KEY`, тот же PEM, что в GitHub secret) — без него кнопка отдаёт понятную ошибку 503. Мёртвый endpoint (404/410) заодно вычищается из KV.
- **⧉ (иконка)** — копировать полный endpoint в буфер (в карточке он больше не светится).
- **Удалить** → POST `/admin/unsubscribe` `{endpoint}`. Принудительно убирает подписку из KV.

Все админ-эндпоинты авторизуются через `?secret=<OWNER_SECRET>` в URL (для удобства открытия из браузера); `<meta name="referrer" content="no-referrer">` — чтобы секрет не утекал по внешним ссылкам. POST `/admin/dispatch` принимает только workflow из белого списка `DISPATCH_WORKFLOWS` (worker.js) и только разрешённые inputs-строки.

Метаданные в KV: `created_at` (один раз), `last_seen_at` (на каждом `/subscribe`), `last_watchlist_update_at` (на `/watchlist`), `user_agent`, `label`. Старые подписки заполняют поля при следующем `/subscribe`.

Локальная отладка админки: `wrangler dev --config cloudflare-worker/wrangler.toml --port 8787` + `cloudflare-worker/.dev.vars` (gitignored) с `OWNER_SECRET=localtest123` → `http://localhost:8787/admin?secret=localtest123`. KV локальный пустой, GitHub API отвечает 401 (нет PAT) — это ожидаемо.

## Подписки на дела (watchlist) на фронте

- Звёздочка ★/☆ в карточке/строке/drawer → `localStorage['watchlist_v1']` → POST `/watchlist` на Worker (KV). **С v98 watchlist хранит только канонические bare-id** (`bare(id)` из cases.json — та же форма, что канонизирует Worker): любой видимый номер (скобочный двойник, апелляционный `33-…`, кассационный `8Г-…`, материал `М-…`) сводится к канону через `canonCaseNumber`/`buildWatchCanonMap` (app.js, зеркало `wnBuildAliasToCanonical` из worker.js). Звезда переживает смену номера при переходе стадии; отписка удаляет именно ту запись, по которой идёт push. Legacy-формы из старого localStorage мигрируются на загрузке.
- **Фильтр «Мои дела»** в chip-bar (`★ Мои`) — виден только при непустом watchlist. Показывает отслеживаемые ★ + новые дела за день. **Единый источник истины — `localStorage['filter_mine_v1']`** (`filterMineActive`): чип, таблица, дайджест и «Ближайшие заседания» согласованы, `_digestViewMode` — производное (ключ `digest_view_v1` упразднён в v98, явный «Мой» из него мигрируется). Дефолт свежего устройства — ВЫКЛ; автовключения нет (юрист включает чипом, выбор помнится); `?mine=1` из push форсит включение.
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
