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
  - [netutil.py](scripts/court_monitor/netutil.py) — `session`, `fetch_page` (win-1251; **одна попытка по умолчанию** с 26.07.2026 — пропуск безопасен, всё перечитывается следующим прогоном; env `FETCH_MAX_RETRIES` возвращает ретраи ручным пробам/импортам в их workflow; `context=` — номер дела/суд в WARNING/ERROR), `fetch_card_checked` (карточки/тексты актов: детект проверочного кода → WARNING + `METRICS["cards_captcha"]` + пропуск; карточный детектор строже поискового — фразы из СМС-цитат актов о мошенничестве не матчит; с 20.07.2026 — детект заглушки/блока `looks_like_non_card_page` (аутейдж sudrf «Информация временно недоступна» отдавал HTTP 200 без таблиц и молча засчитывался успешной проверкой) → `METRICS["cards_blocked"]` + 🩺-алерт + пропуск; второй рубеж в FI-цикле — `card_is_empty_shell`: 0 таблиц не бумпает `last_checked_at`; с 29.07.2026 — **пер-суд предохранитель** (аутейдж Сургутского: заглушка на каждой карточке, прогон впустую молотил весь суд): `CARD_BREAKER_THRESHOLD`=5 не прочитанных карточек ПОДРЯД одного хоста (заглушка/код/сеть) → суд снят с обхода до конца прогона — гейт `card_breaker_allows` пропускает без HTTP (пре-чеки в FI-цикле и `update_active_cases` стоят ДО `polite_delay`, fetch после них — с `breaker_gate=False`, иначе двойной гейт ломает каденс проб), канарейка `card_breaker_preopen` пре-открывает по заглушке на странице ПОИСКА (`looks_like_outage_page`; капча НЕ пре-открывает — штатный режим `search_gated`), half-open проба каждые `CARD_BREAKER_PROBE_EVERY`=25 пропущенных возвращает ожившего в обход; состояние `config.CARD_BREAKER` живёт один прогон (сброс в `_metrics_reset`), 🩺-алерт по судам в 4e (`_card_breaker_alert_lines`), исход `court_breaker` в отчёте bank-трека + группа в админке), `polite_delay`.
  - [regions/](scripts/court_monitor/regions/__init__.py) — **регионы-конфиги**: `base.py` (типы `CourtConfig`/`RegionConfig`), `hmao.py` (реестры ХМАО), `get_region()` (env `REGION` → `config.REGION`, ленивый importlib). Новая территория = новый модуль здесь, форк задаёт только `REGION`.
  - [courts.py](scripts/court_monitor/courts.py) — **фасад активного региона**: ре-экспорт `APPEAL_COURTS`/`APPEAL_COURT`/`FIRST_INSTANCE_COURTS`/`CASSATION_COURT`, матчер `match_region_first_instance` (`match_hmao_first_instance` — legacy-обёртка), `appeal_court_by_domain`, URL карточек.
  - [storage.py](scripts/court_monitor/storage.py) — cases.json/CSV, `.digested_acts`, `.cassation_acts`, кэш пересказов; split-хранение bank-трека (`load_bank_json`/`save_bank_json`, ключ `bank_events_key` «домен|номер»).
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
- `data/bank_parse_report.json` — пер-кейсовый отчёт парсинга трека «Иски банка» за последний прогон: какое дело парсили / пропустили и почему (пишет `BankParseReport` из [scripts/court_monitor/bank_report.py](scripts/court_monitor/bank_report.py) в фазе 7c `main_json`; перезаписывается каждым прогоном, история — в git). Читает карточка «Парсинг исков банка» в админке; нет файла (трек выключен) — карточка скрыта.
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
| `_FI_CASE_NUM_RE` (номер дела 1-й инст.; средний сегмент — постоянное присутствие: Покачи «2-2-279/2026», без него суд невидим целиком) | [scripts/court_monitor/textutil.py:43](scripts/court_monitor/textutil.py:43) |
| `fi_health_key` (ключ журнала здоровья; `#2` у второго сервера домена — иначе Покачи затирал наблюдение районного) | [scripts/court_monitor/runs.py:120](scripts/court_monitor/runs.py:120) |
| `collect_existing_ids` (общий дедуп-индекс main_json/импортёра) | [scripts/court_monitor/linking.py:1071](scripts/court_monitor/linking.py:1071) |
| `load_bank_json` / `save_bank_json` (split-хранение bank-трека: список + events) | [scripts/court_monitor/storage.py:174](scripts/court_monitor/storage.py:174) |
| `bank_writ_expected` (ждём ли ИЛ: отказ/присоединение → нет) | [scripts/court_monitor/lifecycle.py:842](scripts/court_monitor/lifecycle.py:842) |
| `_FI_MERGED_RX` (присоединение к делу; ТОЛЬКО поле «Результат») | [scripts/court_monitor/lifecycle.py:171](scripts/court_monitor/lifecycle.py:171) |
| `repair_cancelled_merges` (объединение отменили → снять флаги) | [scripts/court_monitor/lifecycle.py:440](scripts/court_monitor/lifecycle.py:440) |
| `resolve_bank_merged_targets` (подбор дела-приёмника по ФИО ответчика) | [scripts/court_monitor/linking.py:1251](scripts/court_monitor/linking.py:1251) |
| `bank_cold_archive_path` / `is_bank_cold_archive_file` (холодные bank-архивы) | [scripts/court_monitor/config.py:107](scripts/court_monitor/config.py:107) |
| `case_court_key` / `dedupe_new_archive_entries` (ключ (домен, id) — номера не уникальны между судами) | [scripts/court_monitor/linking.py:1186](scripts/court_monitor/linking.py:1186) |
| `get_region` (env REGION → RegionConfig, ленивый лоадер) | [scripts/court_monitor/regions/__init__.py:20](scripts/court_monitor/regions/__init__.py:20) |
| `match_region_first_instance` (обобщённый матчер по региону) | [scripts/court_monitor/courts.py:58](scripts/court_monitor/courts.py:58) |
| `appeal_court_by_domain` (апел-суд по appeal.court_domain) | [scripts/court_monitor/courts.py:132](scripts/court_monitor/courts.py:132) |
| `appeal_court_for_fi_domain` (апел-суд по домену суда 1-й инст.) | [scripts/court_monitor/courts.py:159](scripts/court_monitor/courts.py:159) |
| `CourtConfig.search_by_fi_number_url` (целевой поиск апелляции по номеру 1-й инст., G2_CASE__CASE_NUMBER_ISS) | [scripts/court_monitor/regions/base.py:114](scripts/court_monitor/regions/base.py:114) |
| `relink_awaiting_appeal` (дослинк awaiting_appeal, не попавших на стр. 1 поиска апелляции) | [scripts/court_monitor/runs.py:150](scripts/court_monitor/runs.py:150) |
| `backfill_appeal_appellants` (тихий бэкфилл апеллянта в стадии appeal: апел. карточка подателя жалобы не публикует — разовый заход в карточку 1-й инст. ТОЛЬКО за «Заявителем жалобы», без событий/дайджеста; штамп `fi.appeal_appellant_checked_at`; капчёвые суды (search_gated) без fi.link пропускаются без HTTP и кэпа — иначе на Урале они вечно съедали весь max_per_run) | [scripts/court_monitor/runs.py:316](scripts/court_monitor/runs.py:316) |
| `reclassify_roleword_appellants` (пересчёт сохранённых слов-ролей подателя жалобы без HTTP: составные «ИСТЕЦ, ПРЕДСТАВИТЕЛЬ» старый классификатор писал «Иное лицо»/is_bank=False — бейдж вставал на противника банка, кейс 33-5089/2026; голый «ПРЕДСТАВИТЕЛЬ» → is_bank=null, бейдж спрятан) | [scripts/court_monitor/runs.py:1603](scripts/court_monitor/runs.py:1603) |
| `appellant_role_words` (разбор «Заявителя» жалобы на слова-роли, в т.ч. составные; None = настоящее имя) | [scripts/court_monitor/textutil.py:471](scripts/court_monitor/textutil.py:471) |
| `migrate_appeal_court_fields` (бэкфилл суда в блоках appeal) | [scripts/court_monitor/lifecycle.py:1330](scripts/court_monitor/lifecycle.py:1330) |
| `fetch_card_checked` (карточный fetch с детектом кода) | [scripts/court_monitor/netutil.py:182](scripts/court_monitor/netutil.py:182) |
| `card_breaker_allows` (пер-суд предохранитель карточек: гейт пропуск/проба) | [scripts/court_monitor/netutil.py:100](scripts/court_monitor/netutil.py:100) |
| `looks_like_outage_page` (URL-независимый детект заглушки — канарейка) | [scripts/court_monitor/parsing/search.py:432](scripts/court_monitor/parsing/search.py:432) |
| `DIGESTED_ACTS_PATH` / `CASSATION_ACTS_PATH` / `PARSE_HEALTH_PATH` | [scripts/court_monitor/config.py:165](scripts/court_monitor/config.py:165) |
| Константы state-machine (`FI_ARCHIVE_DAYS`, `CASSATION_*`) | [scripts/court_monitor/config.py:99](scripts/court_monitor/config.py:99) |
| `update_parse_health` — детектор молчаливой поломки парсеров | [scripts/court_monitor/health.py:42](scripts/court_monitor/health.py:42) |
| `advance_case_stage` / `is_case_archived` / `migrate_stages` | [scripts/court_monitor/lifecycle.py:1360](scripts/court_monitor/lifecycle.py:1360) |
| `reactivate_archived_first_instance` (возврат из архива) | [scripts/court_monitor/linking.py:375](scripts/court_monitor/linking.py:375) |
| `backfill_fi_links` (достройка `fi.link` у дел «с апелляции» — без неё cassation_watch слеп) | [scripts/court_monitor/linking.py:275](scripts/court_monitor/linking.py:275) |
| `rotate_cold_archive` (горячий → холодный архив) | [scripts/court_monitor/linking.py:981](scripts/court_monitor/linking.py:981) |
| `class TableExtractor(HTMLParser)` — парсер карточек дела | [scripts/court_monitor/parsing/tables.py:13](scripts/court_monitor/parsing/tables.py:13) |
| `parse_case_card` — карточка 1-й инст./апелляции | [scripts/court_monitor/parsing/cards.py:265](scripts/court_monitor/parsing/cards.py:265) |
| `parse_cassation_search_page` — поиск 7kas (HMAO-фильтр) | [scripts/court_monitor/parsing/cassation.py:50](scripts/court_monitor/parsing/cassation.py:50) |
| `classify_cassation_outcome` — детерм. enum исхода | [scripts/court_monitor/parsing/cassation.py:180](scripts/court_monitor/parsing/cassation.py:180) |
| `_extract_cassation_act_text` (секция `cont_doc1`) + `parse_cassation_card` | [scripts/court_monitor/parsing/cassation.py:361](scripts/court_monitor/parsing/cassation.py:361) |
| `relink_awaiting_relink_first_instance` (re-link после remanded) | [scripts/court_monitor/linking.py:232](scripts/court_monitor/linking.py:232) |
| `link_cases` (FI ↔ апелляция) | [scripts/court_monitor/linking.py:52](scripts/court_monitor/linking.py:52) |
| `link_cassation_cases` (link + discovery + remanded + архив + дедуп актов + бэкфилл сторон из УЧАСТНИКОВ 7kas; ⚠ признак «карточки ещё не было» для `new_cassation` — ОТСУТСТВИЕ `cassation.case_number`, а не пустота блока: `_apply_fi_cassator` кладёт туда заглушку с одним заявителем, и прежнее `if not old_cass` глушило объявление поступления в кассацию — 9 дел молча, 09–31.07.2026) | [scripts/court_monitor/linking.py:529](scripts/court_monitor/linking.py:529) |
| `parties_from_participants` (УЧАСТНИКИ → истец/ответчик; кроме ИСТЕЦ/ОТВЕТЧИК понимает ЗАЯВИТЕЛЬ/ВЗЫСКАТЕЛЬ и ЗАИНТЕРЕСОВАННОЕ ЛИЦО/ДОЛЖНИК — иначе у «прочих» категорий стороны пусты и касс. запись дайджеста вырождается в голый 8Г-номер) | [scripts/court_monitor/parsing/search.py:142](scripts/court_monitor/parsing/search.py:142) |
| `update_active_cases` (обход карточек активных дел) | [scripts/court_monitor/runs.py:528](scripts/court_monitor/runs.py:528) |
| `main_json` (оркестрация полного прогона) | [scripts/court_monitor/runs.py:1837](scripts/court_monitor/runs.py:1837) |
| `GIGACHAT_SYSTEM_PROMPT` | [scripts/court_monitor/digest/llm.py:76](scripts/court_monitor/digest/llm.py:76) |
| `def generate_digest` — диспетчер дайджеста | [scripts/court_monitor/digest/core.py:333](scripts/court_monitor/digest/core.py:333) |
| `summarize_act_motivation` — LLM-пересказ акта | [scripts/court_monitor/digest/llm.py:871](scripts/court_monitor/digest/llm.py:871) |
| `polish_digest_html` — LLM-полировщик (опц.) | [scripts/court_monitor/digest/llm.py:1114](scripts/court_monitor/digest/llm.py:1114) |
| Пост-обработка HTML (`_ensure_*`/`_validate_*`/`_drop_*`/`_normalize_*`) | весь [scripts/court_monitor/digest/postprocess.py](scripts/court_monitor/digest/postprocess.py) |
| Claude model: `claude-haiku-4-5-20251001` (`_current_digest_model_name`) | [scripts/court_monitor/digest/llm.py:1255](scripts/court_monitor/digest/llm.py:1255) |
| `def generate_template_digest` — программный рендер | [scripts/court_monitor/digest/template.py:322](scripts/court_monitor/digest/template.py:322) |
| доставка: `send_telegram` | [scripts/court_monitor/delivery.py:646](scripts/court_monitor/delivery.py:646) |
| PWA push: `send_web_push` | [scripts/court_monitor/delivery.py:459](scripts/court_monitor/delivery.py:459) |
| персонализация push: `_make_per_sub_callback` | [scripts/court_monitor/delivery.py:325](scripts/court_monitor/delivery.py:325) |
| фильтр по watchlist: `_filter_events_by_watchlist` | [scripts/court_monitor/delivery.py:120](scripts/court_monitor/delivery.py:120) |

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
         "termination_emitted",    // возврат иска / отказ в принятии / передача
                                   // по подсудности уже объявлены в дайджесте
                                   // (идемпотентность + анти-паводок; ставится
                                   // вместе с resolved_emitted, сбрасывается
                                   // только при смене роли банка)
         "hearing_date",           // ПОСЛЕДНЕЕ session-событие карточки; у решённого
                                   // дела обычно = дата резолютивки, якорь 45-дн. окна.
                                   // ⚠️ ДРЕЙФУЕТ: перечитывается каждым прогоном, и
                                   // пост-решенческое заседание (судебные расходы,
                                   // индексация, разъяснение) уводит его вперёд
         "decision_date",          // ЗАМОРОЖЕННАЯ дата решения — якорь classify_writ_kind
                                   // и bank_legal_force_est. Пишется на эмите fi_resolved,
                                   // старым делам бэкфиллится в migrate_stages
         "act_date",               // дата публикации мотивировки (когда есть)
         // Поля трека «Иски банка» (штампует split_bank_track из events на
         // каждом прогоне — фронт bank-картотеки events не грузит):
         "legal_force_est",        // ISO; ПЕРВЫЙ день решения в силе (bank_legal_force_est)
         "default_judgment",       // заочное решение (ст. 233 ГПК); тип определяет
                                   // ПОСЛЕДНЕЕ решение-событие (отмена заочного → обычное снимает)
         "motivirovka_date",       // дата события «Изготовлено мотивированное решение»
         "default_copy_served_date", // «Копия заочного решения … вручена» (ст. 237: срок от вручения)
         "default_copy_returned",  // «возвратилась невручённой» → формула ВС
         "appeal_filed", "appeal_filed_date",        // апел. жалоба в карточке 1-й инст.
         "appeal_appellant", "appeal_appellant_is_bank", "appeal_appellant_status",
         "appeal_appellant_checked_at",  // штамп тихого бэкфилла апеллянта (backfill_appeal_appellants)
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
  python -u …`, `set -o pipefail`) → батчи на `POST /run-progress` Worker'а; лог
  хранится в KV 14 дней (current + prev, cap 1000 строк). ⚠️ С 29.07.2026 блок
  живого лога из админки УДАЛЁН (решение юриста, вместе со списком последних
  прогонов) — канал и эндпоинты `/run-progress`/`/admin/run-progress` живы, но
  UI-читателя нет; смотреть логи — на вкладке Actions в GitHub. Токен —
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
| `first_instance` | карточка 1-й инст. | подана апел. жалоба → `awaiting_appeal` · 60 дней от hearing_date (пуст → от `event_date`: иски, возвращённые на стадии принятия, заседания не имеют — кейс 9-1012/2026) без жалобы → архив (с возможностью реактивации при появлении жалобы) |
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

**Процессуальное завершение 1-й инст. (`classify_fi_termination` /
`fi_termination_details`, lifecycle.py; с 29.07.2026):** возврат иска, отказ
в принятии и передача по подсудности — НЕ решения по существу и живут ОДНОЙ
строкой в 3.2 «Изменения» (`🔚 иск возвращён: причина (для банка: …)`,
`🔚 отказано в принятии иска: …`, `➡️ дело передано по подсудности: куда`),
в 3.5 «Вынесенные решения» они не выводятся. Эмит — отдельным блоком в
FI-цикле `main_json` (фаза 4b; НЕ в `update_active_cases` — та обходит
апелляционные карточки) ДО hearing-блока: когда суд заполнил «Результат»,
карточка ставит статус «Решено» (`resolved_keywords` в parsing/cards.py) →
`case_decided` глушит hearing-блок, а прежний `fi_returned` жил только внутри
него, в ветке «фантомной даты заседания» — поэтому возврат уезжал в 3.5 как
«Итог: возвращено» И параллельно печатался сырым текстом события в 3.2
(инцидент 9-336/2026, Урал, 29.07.2026). Эмит ставит `fi["termination_emitted"]`
и `fi["resolved_emitted"]` — второй закрывает канал 3.5 навсегда, оба вместе
гейтят повтор; **гейт по `resolved_emitted` — ещё и анти-паводок**: на первом
прогоне после деплоя дела, чей исход юрист уже получал, повторно возвратами не
объявляются (на 29.07.2026 это все 6 терминальных дел `cases.json`, новых
событий 0). **Гейт по статусу** (ревью 29.07.2026): эмит только с терминальной
карточки («Решено» с термин-«Результатом» / «Возвращено»); у живого дела
«В производстве» отменённый возврат в истории движения давал бы ложное «иск
возвращён» и навсегда закрытый 3.5, а непустой «Результат» по существу
(«Иск удовлетворён…») — единственный арбитр, история не сканируется. События
про ВСТРЕЧНЫЙ иск завершением не считаются. Залипание флагов лечат: расширенный
`repair_spurious_fi_resolutions`/`spurious_resolution` (статус «Возвращено» +
будущее заседание = возврат отменён → сброс статуса и обоих флагов) и промоушен
М→2 (принятие после возврата → сброс). `fi_returned` — в `FI_ECHO_CATCHUP_TYPES`
(он теперь несёт исход: у дела со связанной апелляцией первый парс FI-карточки
объявлял бы полугодовой возврат новостью). ⚠️ Порядок блоков в FI-цикле
`main_json` load-bearing: завершение → hearing → status_change → `fi_resolved` →
`fi_final_event`; каждый следующий смотрит на предыдущие (стережёт
`TestFiTerminationWiring`). Причину берём из ТЕКСТА СОБЫТИЯ (там она отделена
точкой), из поля «Результат» — только после срезки шапки
(`_extract_termination_reason_from_result`): sudrf клеит её без пробела
(«Заявление ВОЗВРАЩЕНО заявителюДЕЛО НЕ ПОДСУДНО…»), и `_TERMINAL_FI_EVENT_RX`
такую строку не матчит вовсе.

**Предохранитель от сырого дубля (`_strip_echoed_terminal_events`,
digest/template.py):** если у дела в прогоне есть исход (`fi_resolved` → 3.5
или `fi_returned` → 3.2), сырая строка события карточки (`fi_final_event`)
его пересказывает — гасим на входе, ДО сводки и тела (иначе счётчик
«🏁 N финальных событий» разойдётся с содержимым секции). Исключение —
«Изготовлено мотивированное решение…»: рендер нормализует её в отдельный
полезный факт. Применяется в обоих путях (гибрид и `DIGEST_FULL_LLM=1`).

Константы в [scripts/court_monitor/runs.py:1368](scripts/court_monitor/runs.py:1368):
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

## Трек «Иски банка» (банк — истец, с 25.07.2026)

Лёгкий трек для исков самого банка (~1000 дел по ХМАО, на Урале 2500–3500;
пилот — Сургутский городской). На 30.07.2026 в треке **345 дел из 8 судов**:
Сургутский городской 163, Нижневартовский городской 103, Сургутский районный 37,
Радужнинский 11, Мегионский 10, Нижневартовский районный 10, Лангепасский 8,
Покачи 3 (глубина сбора пер-судовая: 10 / 10 / 3 страницы, остальные по одной —
решения юриста 26–30.07.2026). Дела живут в **отдельных файлах** — с 26.07.2026
**split-хранение** ([storage.py](scripts/court_monitor/storage.py):
`load_bank_json`/`save_bank_json`): [data/cases_bank.json](data/cases_bank.json) —
лёгкий список записей **без `events`** (схема та же + маркер
`track: "plaintiff_light"`), `data/cases_bank_events.json` — мапа
`«домен|номер» → events[]` (events = 64% веса записи, фронту нужны только в
drawer; номера не уникальны между судами — потому ключ композитный);
симметрично `cases_bank_archive.json` + `cases_bank_archive_events.json`
(горячий архив ≤365 дн). Холодные годовые `cases_bank_archive_YYYY.json` —
полные записи с inline events (write-only; ротация — тот же
`rotate_cold_archive` с `path_builder=config.bank_cold_archive_path`; glob
`bank_cold_archive_glob()` цепляет и events-файл — фильтровать
`is_bank_cold_archive_file`). Пайплайн работает со СКЛЕЕННЫМИ записями
(split только на границе load/save; содержимое events не меняется ни на
байт — инвариант дедупа `(date, text)`); ⚠️ перед `save_bank_json` базу
обязательно грузить `load_bank_json` — events-файл перезаписывается целиком.
Старый монолит читается прозрачно, первый же прогон мигрирует формат.
Основной cases.json не растёт. Главная ценность — **исполнительные листы**: вкладка
«ИСПОЛНИТЕЛЬНЫЕ ЛИСТЫ» карточки 1-й инст. подтверждена пробой
([ops/writ_probe/report.txt](ops/writ_probe/report.txt), workflow
[probe_writ_section.yml](.github/workflows/probe_writ_section.yml)) и
парсится `_исполнительные_листы` (parsing/cards.py, `_writs` →
`fi.writs`: issue_date/blank_number/electronic_id/status/recipient; статусы
Выдан/Отозван/Возвращен, листов может быть несколько, вкладки нет — пустой
список, таблица ищется по заголовку, не по индексу cont*).

- **Ввод пула**: [scripts/import_bank_registry.py](scripts/import_bank_registry.py)
  + workflow [import_bank_registry.yml](.github/workflows/import_bank_registry.yml) —
  реестр `ops/bank_registry/registry.csv` («домен;номер»), целевой поиск по
  номеру (общие функции — [scripts/court_monitor/target_search.py](scripts/court_monitor/target_search.py),
  вынесены из add_cases_manually), только роль «Истец», порционно `--limit`,
  идемпотентно; `import.announced=true` сразу и уже решённые получают
  `resolved_emitted=True` — **иски банка в дайджесте не анонсируются**, и
  старые решения задним числом не льются. **Второй канал — разовый сборщик
  выдачи** [scripts/collect_bank_claims.py](scripts/collect_bank_claims.py)
  + workflow [collect_bank_claims.yml](.github/workflows/collect_bank_claims.yml)
  (галка dry_run, отчёт → `ops/bank_registry/collect_report.txt`): обходит
  первые N страниц выдачи поиска по «Сбербанк» одного суда — **единственное
  место с пагинацией** (`discover_page_urls` находит ссылки пейджера в HTML,
  фолбэк `&page=N`, стоп-защиты от пустой/повторившейся страницы); строка
  выдачи уже несёт ссылку карточки → 1 HTTP на дело; исключаются итоги
  «без рассмотрения»/«по подсудности»/«возвращено»/«прекращено»
  (`_EXCLUDED_RESULT_RX`, решение юриста 26.07.2026; «отказано» вносится —
  возможна апелляция банка), а с 30.07.2026 (сбор по Нижневартовскому
  городскому) — также дела с карточным признаком апелляции/кассации
  (`_fi_appeal_filed`/`_fi_sent_to_appeal`/`_fi_cassation_filed`/
  `_fi_sent_to_cassation` — дело покинуло бы трек первым прогоном) и дела с
  уже выданным ИЛ на исполнение решения (`classify_writ_kind == "enforcement"`
  с якорем «Дата заседания» карточки; обеспечительные листы не считаются,
  статус листа не важен). Общая сборка записи — `make_bank_entry`
  (import_bank_registry.py). Суд резолвится ПАРОЙ (домен, `--srv`, вход
  `srv_num` в workflow; `resolve_court`): на одном домене может жить два суда —
  Нижневартовский районный и его постоянное присутствие в Покачи
  (vartovray--hmao.sudrf.ru, srv 1 и 2), — и прежний резолв по домену всегда
  отдавал первый.
- **Прогон**: main_json подмешивает bank-дела в общий FI-цикл (фаза 1) и
  раскладывает обратно перед сохранением (`split_bank_track`, фаза 7c).
  **Переезд**: подана апел. жалоба / стадия ушла выше → дело остаётся в
  основном cases.json навсегда (`bank_case_left_track`, маркер → `track_origin`),
  дальше живёт стандартным треком, как 57 истцовых дел «с апелляции».
- **Ритм опроса** (`should_skip_case`): до решения — обычный smart-skip; после
  решения — раз в `BANK_WRIT_CHECK_DAYS=7` дней (`writ_weekly`: до расчётного
  вступления в силу ловит раннюю апел. жалобу, после — ИЛ). Расчётная дата —
  `bank_legal_force_est` (по ГПК, с 28.07.2026; см. «Ожидание ИЛ» ниже).
  Присоединённые к другим делам — тот же недельный ритм, но своя причина
  `merged_weekly` (ждём только отмены объединения): статус карточки у них
  «В производстве», и без явной ветки они парсились бы КАЖДЫМ прогоном.
- **Архив** (`_is_bank_track_archived` — обычные 60 дн убивали бы ожидание
  ИЛ): ИЛ выдан +14 дн → архив; без ИЛ — потолок 180 дн от вступления в силу;
  возврат/прекращено +30 дн. Признак жалобы всегда держит в активных.
- **Листа не будет** (`bank_writ_expected`, с 31.07.2026 — находки юриста в
  прогоне 30.07): два класса дел ИЛ не породят, и держать их в очереди
  ожидания бессмысленно. **(1) В иске ОТКАЗАНО** — ждём только апел. жалобу
  банка: архив через `BANK_DENIED_ARCHIVE_DAYS=30` от **мотивировки** (ветка
  стоит ДО поиска листов, месячный срок ст. 321 ГПК течёт от неё; «отказано в
  принятии» сюда НЕ относится — это возврат на стадии принятия со своим видом
  завершения). **(2) Дело присоединено к другому** (ст. 151 ГПК,
  `FI_TERMINATION_MERGED`) — новый вид процессуального завершения:
  архив через `BANK_MERGED_ARCHIVE_DAYS=30` от определения (окно на отмену
  объединения). Предикат штампуется в запись как `first_instance.writ_expected
  = False` (`split_bank_track`, только False) — фронт читает готовый штамп,
  своей копии правила в JS нет; `legal_force_est` при этом не пишется вовсе
  (иначе drawer показывал бы «Вступило в силу (расч.)» там, где исполнять
  нечего). Дела с частичным удовлетворением лист получают — их не трогаем.
- **Присоединение к другому делу** — три особенности против остальных видов
  завершения: (1) карточка суда статус НЕ флипает (остаётся «В производстве»),
  поэтому гейт статуса в `fi_termination_details` для merged ослаблен;
  (2) merged читается **только из поля «Результат»** (`_FI_MERGED_RX`), НЕ из
  истории движения — гейта статуса нет, а событие отменённого объединения
  остаётся в списке навсегда и дало бы ложный merged; ⚠️ по той же причине
  паттерн НЕЛЬЗЯ добавлять в `_TERMINAL_FI_EVENT_RX` — по нему `cards.py:890`
  выставляет статус «Возвращено»; (3) отмену объединения чинит отдельный
  `repair_cancelled_merges` (`repair_spurious_fi_resolutions` гейтится по
  терминальному статусу и merged не покроет никогда) — снимает флаги вместе с
  `decision_date`, иначе будущий `fi_resolved` не перезаписал бы её
  (`setdefault`) и `classify_writ_kind` считал бы тип листа от чужого якоря.
  **Номер дела-приёмника суд не публикует** (проверено на всех 9 делах): его
  подбирает `resolve_bank_merged_targets` ([linking.py](scripts/court_monitor/linking.py))
  по совпадению полного ФИО ответчика в том же суде (юрлица в ключ не берём —
  МТУ Росимущества стоит соответчиком почти в каждом наследственном иске),
  при ничье номер НЕ ставим. Результат ВСЕГДА помечен `merged_into_guess` и
  словом «предположительно» в дайджесте/drawer. Подбор идёт после FI-цикла
  (нужен весь список дел), поэтому он же дописывает номер в уже собранное
  `change["details"]` — эмит завершения одноразовый. Звезда переезжает на
  приёмника алиасом во фронте (`buildWatchCanonMap`, merged-алиасы
  регистрируются ПЕРВЫМИ — записи карты защищены гардом `!map.has`, и общий
  цикл bank-дел иначе занял бы ключ); `canonicalizeWatchlistSet` шлёт
  однократный sync в KV. `worker.js`/`delivery.py` не трогаем: их alias-карты
  строятся из cases.json, где bank-дел нет.
- **Дайджест**: секция «🏦 ИСКИ БАНКА (N)» — компакт, одна строка на дело,
  ПОСЛЕДНЕЙ (при обрезке Telegram страдает первой); события `fi_writ_issued`/
  `fi_writ_status_changed` (НЕ в эхо/stale-фильтрах); маркер `change["track"]`
  едет в данных fi_changes — сигнатуры/replay не тронуты. Рутина отключается
  `BANK_DIGEST_ROUTINE=0` (`filter_bank_routine_events`; дефолт 1 — пилот
  шлёт всё).
- **Выключатель `BANK_TRACK`** — Actions Variable территории (прокидывается в
  [update_cases.yml](.github/workflows/update_cases.yml), фолбэк `'1'` = как
  сейчас). ⚠️ Гасит только ПРОГОН: сегмент «🏦 Иски банка» на дашборде
  прячется по отсутствию `data/cases_bank.json` (HEAD-проба `probeBankFile`),
  про флаг фронт не знает — файл территории всё равно надо удалять, флаг его
  не заменяет. Ручные `import_bank_registry`/`collect_bank_claims` флаг тоже
  не спрашивают. До 26.07.2026 переменная не работала вовсе: код её читал, а
  workflow не передавал — проводку стережёт `TestBankTrackWiring`.
- **Push** (с 26.07.2026): общесистемный агрегат (подписчики без watchlist)
  track-события НЕ считает; персональные push по watchlist работают —
  `_filter_events_by_watchlist` матчит bank-изменения по composite
  `details.court_domain|bare(case)` и по голому номеру (фолбэк ручного ввода).
- **Watchlist** (с 26.07.2026, v119): звёзды работают и в картотеке банка —
  запись хранится composite-формой `«домен|номер»` (основной трек — прежний
  bare-канон, миграций нет). Worker пропускает строки с `|` без канонизации,
  а alias-карты (worker.js `wnBuildAliasToCanonical`, delivery.py, app.js
  `buildWatchCanonMap`) регистрируют composite-алиасы основных дел — при
  переезде bank-дела в cases.json звезда «оживает» на переехавшем. «★ Мои» —
  **надкартотечный** объединённый список звёзд обеих картотек (bank-дела с
  бейджем 🏦, переключатель картотек в mine-режиме скрыт, bank-список
  подгружается сам при composite-звёздах); mine-дайджест и «Ближайшие
  заседания» — по тому же объединённому набору. Админка подписчиков грузит
  cases_bank*.json в карту дел (bank-звезда — не «нигде не найдено»).
- **Фронт** (v119): сегмент «Основные | 🏦 Иски банка» (`#dataset-switch`;
  виден при существующем файле — HEAD-проба + персист `bank_exists_v1` для
  офлайна) → **трёхступенчатая ленивая загрузка**: вход в картотеку — только
  список; первый клик чипа «Архив» — `ensureBankArchive`; первое открытие
  drawer — `ensureBankEvents` (events всем делам разом, спиннер в хронологии;
  inline events старого монолита из SW-кэша не перетираются). Тяжёлые
  bank-файлы качаются с таймаутом 30 с (`FETCH_TIMEOUT_HEAVY_MS`).
  **Пагинация рендера** (обе картотеки): первые `RENDER_CHUNK=120` строк +
  «Показать ещё»/IntersectionObserver — фильтры и поиск работают по всему
  датасету, ограничен только DOM. В bank-режиме: сегменты роль/инстанция
  скрыты и игнорируются (значения не сбрасываются), категории пересобираются
  (`populateFilterOptions` по активному датасету), чип «🧾 ИЛ» и bank-KPI
  «В производстве / Решено / С ИЛ / Ждут ИЛ» (`renderBankStats`; «Ждут ИЛ» =
  решено без enforcement-листа), «Ближайшие заседания» по искам банка;
  архивность — track-осведомлённый `caseArchived` (только `_bankArchived`).
  Номера из секции «🏦 ИСКИ БАНКА» дайджеста кликабельны — при незагруженном
  датасете bank-список подтягивается фоном (`enhanceDigestCaseLinks`).
  PWA-shortcut «🏦 Иски банка» → `?bank=1`. Кнопка «Обновить» перезагружает
  и bank-датасет до достигнутого уровня цепочки.
- **Секция «Исполнительные листы» в drawer** (`buildWritsSectionHtml`, только
  вкладка 1-й инстанции — листы живут в `fi.writs`): герой карточки —
  **НОМЕР листа**, им юрист оперирует (передача приставам, отзыв,
  отслеживание ИП). Отсюда решения, которые нельзя откатывать «для
  компактности»: электронный ИД и бумажный бланк выводятся ОБА значениями
  (это разные реквизиты одного листа, а не фолбэк друг для друга; текстовые
  подписи «Электронный ИД»/«Бланк» убраны 28.07.2026 решением юриста —
  форматы самоописательны: «ФС № …» против «…#…#…»); строка типа листа
  («🛡 Обеспечительные меры»/«🧾 На исполнение решения») — только в
  смешанной секции, в однородной её дословно говорит заголовок; номер —
  крупный mono, `user-select:all`, кнопка копирования (`copyBtnHtml`),
  перенос только по «#» (`writNumHtml`, без `word-break:break-all`);
  «Лист N из M» при нескольких листах — одна дата/ОСП/статус на двух строках
  различаются только суффиксом `#N`. Получатель сокращается `shortBailiff`
  (полное имя — в `title`). Дата свежайшего enforcement-листа продублирована
  строкой «🧾 ИЛ выдан» в «Ключевых датах». **Мобильная карточка
  перекомпонована 28.07.2026** (решение юриста): в шапке `mc-badges` только
  🛡-иконка обеспечительного листа перед бейджем стадии (`writShieldIconHtml`),
  а «🧾 ИЛ ДД.ММ»/«⏳ ждёт ИЛ N дн.» — взаимоисключающая текстовая строка
  СРАЗУ ПОД ДАТОЙ, внутри правой колонки `.mc-hearing` (`mcTrackLineHtml` →
  `.mc-track`, без своей черты). ⚠️ Не выносить её отдельным рядом карточки:
  так строку отбивала вниз высота левой колонки (бейдж результата + «Акт
  опубликован»), связь с датой терялась, а вторая линия делала карточку
  полосатой. Нижний ряд выровнен по baseline
  (текст бейджа-результата на одной линии с датой), прочерк «—» пустой даты
  в compact-режиме `buildHearingHtml` не рендерится (только в десктоп-таблице);
  относительная метка даты заседания («ср»/«завтра») в compact убрана, дата
  укрупнена до fs-md — наравне с бейджем «Назначено». Пилюли
  `writBadgeHtml`/`awaitingWritBadgeHtml` остаются в таблице десктопа и hero
  drawer'а. Эмодзи 🏦 из сегмента «Иски банка» (#dataset-switch) убран.
  Мобильные размеры секции — в блоке `@media (max-width:768px)`; держать их
  наравне с соседями, на `--fs-2xs` (11px) не опускать. Заголовок называет
  содержимое и несёт эмодзи бейджей (`🛡 Обеспечительные листы (4)`, если
  листов на исполнение нет — иначе юрист читает его как «ИЛ есть»;
  `🧾 Исполнительные листы (N)`), отозванные/возвращённые листы
  приглушены (`.writ-row.is-inactive`), а выдача листа подмешивается в
  хронологию drawer (`buildTimeline` → `веха`, листы одной даты схлопываются
  со счётчиком «(N шт.)»).
- **Ожидание ИЛ** (`legal_force_est`): `split_bank_track` штампует в
  `first_instance.legal_force_est` расчётную дату вступления решения в силу
  (`bank_legal_force_est`) — фронту её не посчитать, производственного
  календаря в JS нет. **С 28.07.2026 расчёт по ГПК** (решение юриста,
  утверждено 4 вопросами AskUserQuestion): сроки в днях — РАБОЧИЕ (ст. 107,
  `add_working_days`), месяц — календарный (ст. 108 + п. 16 ПП ВС №16,
  `month_term_last_day`: 31.07→31.08, нет числа → последний день месяца,
  конец-нерабочий → следующий рабочий), поле = ПЕРВЫЙ день в силе (последний
  день срока + 1 календ., в силу вступает и в выходной). Обычное решение:
  мотивировка (`act_date` → событие «Изготовлено мотивированное решение» →
  фолбэк `decision_date` + 10 раб. дн, ст. 199) + месяц (ст. 321). Заочное
  (`default_judgment`, детект `bank_default_judgment_info` по events,
  тип решает ПОСЛЕДНЕЕ решение-событие): копия вручена
  (`default_copy_served_date`) → вручение + 7 раб. дн + месяц (ст. 237);
  сведений нет / `default_copy_returned` → формула ВС (Обзор №2 (2015), в. 14):
  решение + 3 раб. дн + 7 раб. дн + месяц. Константы —
  `BANK_MOTIVATION_TERM_WORKDAYS`/`BANK_DEFAULT_COPY_SEND_WORKDAYS`/
  `BANK_DEFAULT_CANCEL_WORKDAYS` (config.py). Отсюда
  `awaitingWritDays`/`awaitingWritBadgeHtml`: бейдж «⏳ ждёт ИЛ N дн.» в
  строке/карточке/hero, бейдж «🌙 Заочное» (`defaultJudgmentBadgeHtml`),
  строки «Вступило в силу (расч.)» и «🌙 Копия ответчику»
  (`defaultCopyKvHtml`) в «Ключевых датах» и сортировка чипа «Ждут ИЛ» по
  убыванию ожидания (очередь работы, а не алфавит). Пороги (30/60 дн)
  привязаны к реальности выдачи (+40..55 дн от решения) и
  `BANK_WRIT_WAIT_MAX_DAYS`.
- **Сокращение ОСП** — две реализации по необходимости (`shortBailiff` в
  app.js для фронта, `shorten_bailiff_name` в textutil.py для дайджеста);
  правила держать согласованными, общие фикстуры — в test_frontend_writs.py.
- ⚠️ **Якорь типа листа — `fi.decision_date`, НЕ `hearing_date`.**
  `classify_writ_kind` (и зеркало `classifyWritKind` в app.js) сравнивают дату
  выдачи с замороженной датой решения. `hearing_date` перечитывается каждым
  прогоном из последнего session-события карточки и уезжает вперёд, назначь
  суд по решённому делу заседание (судебные расходы, индексация, разъяснение,
  правопреемство, дубликат ИЛ) — лист на исполнение молча стал бы
  обеспечительным, вместе с бейджем, KPI «С ИЛ», заголовком секции, бейджем
  «⏳ ждёт ИЛ» и окном архива (дело зависло бы на потолке 180 дн и
  опрашивалось бы еженедельно). Дайджест бы при этом промолчал: `kind` не
  хранится в `fi.writs`, диффа нет, а гард `case_decided` глушит
  hearing-события. На симуляции дрейфа по пилоту переворачивалось 6 листов из
  6. Фолбэк на `hearing_date` оставлен для архивных записей.
- **Отчёт парсинга (с 29.07.2026)**: аккумулятор `BankParseReport`
  ([scripts/court_monitor/bank_report.py](scripts/court_monitor/bank_report.py))
  собирает в FI-цикле пер-кейсовый исход каждого bank-дела (parsed /
  skip+причина из `skip_reason_ru` / fetch_captcha·blocked·http·empty по
  **дельте METRICS вокруг единственного HTTP-запроса итерации** /
  empty_shell / no_link·bad_link·court_disabled / not_in_queue) + флаги
  degraded/force_parsed/left_track/archived и типы событий дайджеста →
  `data/bank_parse_report.json` в фазе 7c (обёртка `save_bank_parse_report`
  глушит ошибки записи — сервисный канал не роняет прогон), коммитится
  workflow'ом → карточка «Парсинг исков банка» в админке. Методы
  аккумулятора сами игнорируют не-track дела; ключ — идентичность dict
  (промоушен М→2 не рвёт запись). В сводке прогона — ключ `Bank parse`
  (X/Y). Попутно `writ_weekly` выделен отдельным слагаемым в плане очереди
  и итоге FI (раньше сливался в «без движения»/«заседание в будущем»).
- Тесты: [scripts/tests/test_bank_track.py](scripts/tests/test_bank_track.py),
  [scripts/tests/test_import_bank_registry.py](scripts/tests/test_import_bank_registry.py),
  [scripts/tests/test_bank_storage_split.py](scripts/tests/test_bank_storage_split.py),
  [scripts/tests/test_frontend_writs.py](scripts/tests/test_frontend_writs.py),
  [scripts/tests/test_bank_report.py](scripts/tests/test_bank_report.py)
  (split-хранение, ротация bank-архива, composite-матчинг push, отчёт
  парсинга и его проводка).

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

# После правок кода (court_monitor, app.js, worker.js): обновить якоря строк
# в docs/technical и CLAUDE.md. Протухание стережёт test_doc_anchors.py.
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
- `CARD_BREAKER_THRESHOLD` — пер-суд предохранитель карточек: столько не прочитанных карточек подряд (заглушка/код/сеть) снимают суд с обхода до конца прогона (дефолт 5; `0` — выключить, например для ручной пробы лежащего суда).
- `CARD_BREAKER_PROBE_EVERY` — half-open: каждая K-я пропущенная карточка отключённого суда идёт пробой, успех возвращает суд в обход (дефолт 25; `0` — без проб).
- `LOG_LEVEL` — уровень логов прогона (`DEBUG`/`INFO`/`WARNING`/`ERROR`, дефолт `INFO`); `DEBUG` показывает пер-кейсовые skip/«без изменений» и прочую диагностику.

## Куда уходит дайджест

- **Telegram:** все workflow'и шлют в личный чат (`TELEGRAM_CHAT_ID_TEST`) по умолчанию. Чтобы продублировать в корпоративную группу — поставить галку `to_group` в UI Run workflow. Текст дайджеста в Telegram **общий**, не персонализированный.
- **PWA push:** `update_cases.yml` (крон) шлёт всем подписчикам PWA. Тестовый workflow `test_digest.yml` шлёт push **только устройствам-владельцам** по умолчанию, чтобы не спамить коллегам прототипами. У `test_digest.yml` есть галка «push_all» — отправит на все устройства. Чтобы пометить своё устройство владельцем — открыть PWA по URL `https://selivanovas.github.io/dashboard/sberbank_dashboard.html?owner=<OWNER_SECRET>` (один раз).
- **Персонализация push по watchlist (`_per_sub` callback):** push-payload собирается под каждого подписчика отдельно через фабрику `_make_per_sub_callback` ([scripts/court_monitor/delivery.py:325](scripts/court_monitor/delivery.py:325)). Новые дела (`fi_new_cases`, `appeal_new_cases_csv`) — общесистемный сигнал, шлются всем; изменения и переходы стадий — только если дело в watchlist подписчика. Click_url для подписчиков с watchlist — `?digest=open&mine=1`. Используется в основном кроне (`main_json`), `--replay-last`, `--push-last-digest`.

## Админка подписчиков

URL: `https://court-monitor-trigger.7selivanov-a.workers.dev/admin?secret=<OWNER_SECRET>`. Открывается в браузере (мобильно тоже). HTML-страница вынесена в [cloudflare-worker/admin_page.js](cloudflare-worker/admin_page.js) (`renderAdminHtml(secret, role, cfg)`, wrangler бандлит импорт сам); серверные эндпоинты — в [cloudflare-worker/worker.js](cloudflare-worker/worker.js). ⚠️ Вся страница — один template literal: внутренний JS пишется без backtick'ов и `${`, backslash удваивается. Naive-таймстампы из data/*.json (Python на UTC-раннере пишет без «Z») страница парсит как UTC (`parseIso`).

**Роли (с 16.07.2026):** owner (`OWNER_SECRET`) — всё; operator (`OPERATOR_SECRET`, один общий на сопровождающих капчёвых судов; не задан — роль неактивна, у ХМАО так) — статус+здоровье+живой лог+секция «Импорт дел». Гейт — `resolveAdminRole`/`requireAdminRole` (worker.js): чужой секрет → 401, оператор на owner-эндпоинте → 403 (реальный запрет на сервере, скрытие в UI — `data-owner-only` + `html[data-role]`). `DISPATCH_WORKFLOWS` — `{inputs, roles}`: update_cases/test_digest — owner, import_cases — обе роли. Секция «Импорт дел» (`#import`): dropdown gated-судов из `region.fi_courts` cases.json (у ХМАО их нет — секция скрыта), вставка rich-paste/файл, поллинг журнала (`/admin/import-log`), история импортов, **светофор свежести по судам** (когда каждый суд импортировался в последний раз: зелёный ≤7 дн, жёлтый ≤14, красный дольше/ни разу; данные — вечные ключи `import:last:<домен>`, пишутся на done в `/import-result`). **С 17.07.2026 — операторский UX (394cd14):** оператору секция идёт ПЕРВОЙ (перестановка серверная — константы `IMPORT_SECTION`/`IMPORT_CHIP` в `renderAdminHtml` вставляются по роли), светофор раскрыт и кликабелен (клик по суду = выбрать его в форме, слушатели — делегированием), 5-я плитка пульта «Импорты» (N просрочено; обеим ролям, без gated-судов скрыта — ХМАО не видит, `.pult.has-import` в мобильном медиа-блоке обязателен), drag-n-drop файла в поле вставки + индикатор «что уйдёт» (файл побеждает вставку) + предпроверка «есть ли ссылки» до отправки, статус ожидания с таймером, сбой cases.json → алерт с «Повторить» (owner'у — тихо, как раньше). **Автоопределение суда по вставке (17.07.2026):** хост из абсолютных ссылок карточек подставляет суд в dropdown сам (пока оператор не выбирал вручную — флаг `impCourtTouched`), конфликт — заметка «⚠ ссылки ведут в …» с кнопкой «выбрать этот суд», отправка чужого дампа блокируется на клиенте, Worker'е (400) и в импортёре (`EXIT_WRONG_COURT`). ВСЕ URL данных страницы выводятся из `CASES_DATA_URL` Worker'а (`adminPageConfig()`) — хардкод сломал бы админку территорий.

**Дизайн v2 (13.07.2026)** — визуальный язык дашборда: токены цветов/шрифтов скопированы из [styles.css](styles.css) (IBM Plex с Google Fonts, сберовский зелёный, бейджи-пилюли, цвета стадий teal/indigo/violet — карта `stageBadge` зеркалит `stageBadgeHtml` из app.js), 3-режимная тема авто/свет/тьма (localStorage `admin_theme`, инлайн-скрипт в head), статусы — цветные точки/пилюли вместо эмодзи, иконки — inline-SVG. При смене палитры дашборда токены админки синхронизировать вручную.

Компоновка: липкая glass-шапка (лого · чипы-якоря «Система/LLM/Подписчики» с подсветкой активной секции через IntersectionObserver · сводка · тоггл темы · Обновить) → **пульт из 4 кликабельных stat-плиток** (Последний прогон ok/сбой/идёт из gh-runs · Дайджест N изменений · Парсеры «все 22 ok»/«N ⚠» · Автозапуск + push-агрегат) → секции:
- **#system** (грид 2 колонки на десктопе): карточка «Запуск прогона» — кнопки «▶ Полный прогон» (`smart_skip:"false"`) и «Стандартный прогон» (`smart_skip:"true"`, как ежедневный крон; в выходной сразу завершится «нерабочий день») → POST `/admin/dispatch`, рядом метка следующего автозапуска. **Список последних 8 runs и блок живого лога УДАЛЕНЫ 29.07.2026** (решение юриста) — статусы/логи смотрятся на вкладке Actions GitHub; GET `/admin/gh-runs` (Worker проксирует GitHub API, PAT на сервере; отдаёт и `next_cron_at` с учётом праздников) остался — им питаются плитки пульта «Последний прогон» (автообновление каждые 15 с пока прогон идёт) и «Автозапуск» | карточка «Здоровье парсеров» из [data/parse_health.json](data/parse_health.json): светофор-точки (красный fail_streak≥3/alerted_zero; жёлтый fail_streak≥1 или ноль при медиане≥1), спарклайны, проблемные вверху, первые 8 + свёрток; имена судов — карта `COURT_NAMES` (синхронизировать при правке `FIRST_INSTANCE_COURTS`) | карточка «Парсинг исков банка» (с 29.07.2026, обе роли) из `data/bank_parse_report.json`: пер-кейсовый итог последнего прогона по bank-треку — группы по исходам (ошибки загрузки/без карточки/вне очереди раскрыты; спарсено и пропуски по ритму ИЛ / будущим заседаниям свёрнуты), внутри группы порции по 30 строк (`BP_CHUNK`, «Показать ещё» — на Урале дел тысячи), русские причины считает Python (`skip_reason_ru`/`_OUTCOME_RU` в bank_report.py), 404 файла → карточка скрыта (территория без трека).
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
