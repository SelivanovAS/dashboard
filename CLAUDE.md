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
  - [textutil.py](scripts/court_monitor/textutil.py) — даты, HTML-очистка, экранирование, сокращение имён сторон/судов, производственный календарь. **Сокращатель сторон с 09.08.2026** (разбор дайджеста 07.08): МТУ Росимущества заменяется ПОДСТАНОВКОЙ `_MTU_FULL_RE.sub` до сплита по запятым (прежний ранний `return` молча съедал ВСЕХ соответчиков — 33-5577/2026, 2-2630/2026; региональный хвост с внутренними запятыми матчится белым списком регион-токенов, покрыты именительный «Росимуществ**о**» и «МТУ ФА по управлению…»); ФИО-слоты имени/отчества — с заглавной + русские окончания отчества (гард: «Уральский банк ПАО Сбербанк» сворачивался в «Уральский Б.С.»), КАПС-ФИО нормализуются (`_decapitalize_fio`: «ЗАЛАН ЖАННА ГЕННАДЬЕВНА» → «Залан Ж.Г.»); наследственные формулировки всех видов → «насл. имущество Фамилия И.О. (дата смерти …)» (`_shorten_heritage`); пустые скобки после срезанной ОПФ чистятся («Банк ВТБ ()»); точные повторы сторон печатаются один раз (решение юриста — суд в объединённых делах перечисляет стороны по каждому иску; однофамильцы разведены `_resolve_initial_collisions` и не схлопываются).
  - [netutil.py](scripts/court_monitor/netutil.py) — `session`, `fetch_page` (win-1251; **одна попытка по умолчанию** с 26.07.2026 — пропуск безопасен, всё перечитывается следующим прогоном; env `FETCH_MAX_RETRIES` возвращает ретраи ручным пробам/импортам в их workflow; `context=` — номер дела/суд в WARNING/ERROR), `fetch_card_checked` (карточки/тексты актов: детект проверочного кода → WARNING + `METRICS["cards_captcha"]` + пропуск; карточный детектор строже поискового — фразы из СМС-цитат актов о мошенничестве не матчит; с 20.07.2026 — детект заглушки/блока `looks_like_non_card_page` (аутейдж sudrf «Информация временно недоступна» отдавал HTTP 200 без таблиц и молча засчитывался успешной проверкой) → `METRICS["cards_blocked"]` + 🩺-алерт + пропуск; второй рубеж в FI-цикле — `card_is_empty_shell`: 0 таблиц не бумпает `last_checked_at`; с 29.07.2026 — **пер-суд предохранитель** (аутейдж Сургутского: заглушка на каждой карточке, прогон впустую молотил весь суд): `CARD_BREAKER_THRESHOLD`=3 не прочитанных карточек ПОДРЯД одного хоста (заглушка/код/сеть) → суд снят с обхода до конца прогона — гейт `card_breaker_allows` пропускает без HTTP (пре-чеки в FI-цикле и `update_active_cases` стоят ДО `polite_delay`, fetch после них — с `breaker_gate=False`, иначе двойной гейт ломает каденс проб), канарейка `card_breaker_preopen` пре-открывает по заглушке на странице ПОИСКА (`looks_like_outage_page`; капча НЕ пре-открывает — штатный режим `search_gated`), half-open проба каждые `CARD_BREAKER_PROBE_EVERY`=30 пропущенных возвращает ожившего в обход; состояние `config.CARD_BREAKER` живёт один прогон (сброс в `_metrics_reset`), 🩺-алерт по судам в 4e (`_card_breaker_alert_lines`), исход `court_breaker` в отчёте bank-трека + группа в админке), `polite_delay`.
  - [regions/](scripts/court_monitor/regions/__init__.py) — **регионы-конфиги**: `base.py` (типы `CourtConfig`/`RegionConfig`), `hmao.py` (реестры ХМАО), `get_region()` (env `REGION` → `config.REGION`, ленивый importlib). Новая территория = новый модуль здесь, форк задаёт только `REGION`.
  - [courts.py](scripts/court_monitor/courts.py) — **фасад активного региона**: ре-экспорт `APPEAL_COURTS`/`APPEAL_COURT`/`FIRST_INSTANCE_COURTS`/`CASSATION_COURT`, матчер `match_region_first_instance` (`match_hmao_first_instance` — legacy-обёртка), `appeal_court_by_domain`, URL карточек.
  - [storage.py](scripts/court_monitor/storage.py) — cases.json/CSV, `.digested_acts`, `.cassation_acts`, кэш пересказов; split-хранение bank-трека (`load_bank_json`/`save_bank_json`, ключ `bank_events_key` «домен|номер»).
  - [health.py](scripts/court_monitor/health.py) — журнал здоровья парсеров + детектор молчаливой поломки.
  - [lifecycle.py](scripts/court_monitor/lifecycle.py) — классификация событий карточки, state machine стадий, дедуп, архив.
  - [parsing/](scripts/court_monitor/parsing/__init__.py) — `tables.py` (TableExtractor), `search.py` (поисковая выдача), `cards.py` (карточки дел), `cassation.py` (7kas).
  - [linking.py](scripts/court_monitor/linking.py) — связка FI ↔ апелляция ↔ кассация, discovery, реактивация, ротация архива.
  - [digest/](scripts/court_monitor/digest/__init__.py) — `llm.py` (Claude/GigaChat/OpenRouter — выбор через `LLM_PROVIDER`; промпты — патч-цели тестов живут тут), `postprocess.py` (валидация/чистка HTML), `template.py` (программный рендер — **боевой путь с 03.07.2026**, компакт-вёрстка без отступов), `core.py` (диспетчер `generate_digest`), `lint.py` (программный линтер готового HTML после отправки: полнота номеров, счётчики (N), теги, футер → 🩺-алерт; `DIGEST_LINT=0` — выключатель). Рядом с линтером (и ТОЛЬКО там — блок 4e идёт ДО генерации дайджеста, счётчик в нём всегда 0) с 02.08.2026 стоит `_alert_llm_summary_failures`: при `METRICS["llm_summary_failed"] > 0` шлёт 🩺-алерт «пересказы актов» — иначе отказ провайдера (429 free-пула OpenRouter, 17.07.2026: в дайджест уходила сырая мотивировка) был виден только в логе прогона. Зовётся с боевого пути `main_json`, не с replay — `test_digest.yml` гоняет его для экспериментов. Прод — гибрид: события рендерит код, LLM только пересказывает мотивировки актов; `DIGEST_FULL_LLM=1` — откат на полный LLM-дайджест.
  - [delivery.py](scripts/court_monitor/delivery.py) — Telegram, Web Push с watchlist-персонализацией, алерты.
  - [runs.py](scripts/court_monitor/runs.py) — `main_json` и остальные режимы прогона, `update_active_cases`. **С 12.08.2026 порядок инстанций в прогоне: кассация → апелляция → 1-я инстанция** (решение юриста: важные инстанции первыми — при падении прогона посередине они уже проверены). Исторические баннеры блоков (`2.`/`3.`/`3b`/`4a`–`4e`) сохранены, видимую нумерацию задаёт `log_phase(N/9)`; таблица «баннер → фаза» — [docs/technical/05-конвейер-обновления.md](docs/technical/05-конвейер-обновления.md). Инварианты: поиск инстанции раньше её карточек (канарейка предохранителя); `dedupe_cassation_by_uid` — после карточек апелляции (УИД дозаполняется с апел. карточки); discovery-id кассации дозаписываются в `existing_ids`/`fi_dedup_*` до поиска 1-й инст.
- [scripts/add_cases_manually.py](scripts/add_cases_manually.py) — ручное добавление дел 1-й инстанции.
- [scripts/import_search_dump.py](scripts/import_search_dump.py) — **офлайн-импортёр дампов выдачи капчёвых судов** (Свердловская обл.: 54 записи реестра со `search_gated=True` — автопоиск выключен, карточки мониторятся). Оператор решает капчу → вставляет дамп в секцию «Импорт дел» админки → Worker кладёт в KV + диспатчит [import_cases.yml](.github/workflows/import_cases.yml) → импортёр (utf-8→win-1251, нормализация pretty-print, дедуп `collect_fi_dedup_index` по ВСЕМ картотекам, `srv_num` из href; **«банк-ответчик» → cases.json**, как в автопоиске, третье лицо — `[SKIPPED ROLE]`; **с 13.08.2026 «банк-истец» → трек «Иски банка»** (разгон Урала: правила общие с авто-подхватом — `row_passes`/`card_rejects(skip_appeal=False)`/`make_bank_entry(source="dump")`/`entry_is_spent` + негативный кэш, но no_link в кэш НЕ пишется: ссылку теряет вставка «как текст», а не выдача; по ссылке дампа качается карточка — единственный онлайн-шаг, кэп `MAX_BANK_CARDS_PER_IMPORT`=100, маркеры `[ADDED BANK]`/`[SPENT]`/`[SEEN]`/`[BANK CAPPED]`, в дайджест не анонсируется; при `BANK_TRACK=0` — прежний `[SKIPPED ROLE]`, решение 16.07.2026 принималось до появления трека); промоушен М→2 при комбо-номере — `[PROMOTED]`, зеркало main_json) → коммит cases.json → итог назад в админку (`/import-result`, журнал `import:log:*`). **Защита «дамп ↔ выбранный суд» (17.07.2026):** хосты абсолютных href карточек (`name=sud_delo`) + маркер Chrome «saved from url» сверяются с судом импорта на трёх уровнях — автоопределение суда в админке (`impDetectDomains`/`impRunDetect`, подставляет суд сам, ручной выбор не перебивает), 400 Worker'а (`detectDumpSudrfHosts`), `EXIT_WRONG_COURT=5` импортёра (`detect_dump_hosts`); `delo_id` из href карточек ловит выдачу не того раздела (суды 1-й инст. он не различает — у всех 1540005). Относительные href (файл Firefox) хостов не несут — проверки молчат. Дела получают служебный блок `"import": {operator, at, source, announced}`; ближайший прогон объявляет их «новыми исками» в дайджесте/пуше один раз (`announce_imported_cases`, runs.py). Подробно — [docs/Тиражирование_регионы.md](docs/Тиражирование_регионы.md).
- [scripts/add_cases_targeted.py](scripts/add_cases_targeted.py) + [scripts/court_monitor/targeted_add.py](scripts/court_monitor/targeted_add.py) + [.github/workflows/add_cases.yml](.github/workflows/add_cases.yml) — **точечное добавление дел из админки** (с 10.08.2026, блок «Добавить дела» на вкладке «Импорт», обе роли, вкладка теперь видна и на ХМАО): до 20 строк за отправку — номер дела ИЛИ ссылка на карточку sudrf (для капчёвых судов ссылка — единственный путь: код закрывает поиск, карточки открыты). Worker кладёт пачку в KV `import:case:<uuid>` (строки не касаются shell и не упираются в лимит 100 символов у inputs) → `add_cases.yml` (та же concurrency-группа `cases-data-write`; таймаут 45 мин и `FETCH_MAX_RETRIES=1` — до 20 номеров × все открытые суды, а лежащий суд с ретраями взрывал бы худший случай за пределы таймаута; ⚠️ у GitHub-группы ЖИВЁТ ОДИН pending: пачка, вставшая в очередь второй, молча отменяется до первого шага (журнал навсегда «отправлено») — повтор пачки безопасен, уже добавленное отсеет дедуп; `done` в журнале — только при успешном push (упавший коммит перекрашивает итог в failed с подсказкой повторить)) → пер-строчно: номер → целевой поиск по всем `courts_for_search` (0 совпадений → отказ с подсказкой про ссылку; >1 суда → «выберите суд в форме», селект действует на всю пачку); ссылка → резолв по (домен, srv_num) через `fi_court_by_domain` с точными отказами (апелляция/кассация/чужой регион/чужой delo_id — клиентское зеркало в админке валидирует ДО отправки по `region.*` cases.json, у `fi_courts` для этого появился `delo_id`). Дальше: промоушен М→2 (`promote_material_record`, общий с дамповым импортёром) → дедуп по ВСЕМ картотекам (активные+горячие+холодные архивы обеих, ключ (домен, номер); активные проверяются раньше архивов — анти-клон) → находка в архиве = **реактивация с полной историей** (`reactivate_from_archive`: архив-источник обязательно пересохраняется, bank-пары — только через `load_bank_json`, `archived_count` пересчитывается, `import.announced=True` — не «новый иск») → свободное дело: роль по УЧАСТНИКАМ карточки (`bank_role_from_participants`; Сбер не найден/дочка → отказ со сторонами), Ответчик/Третье лицо → cases.json (`_fi_search_to_json_case`, `import` без `announced` → объявится «новым иском» ближайшим прогоном), Истец → bank-трек (`make_bank_entry`, тихо; гейты `card_rejects(skip_appeal=False)`+`entry_is_spent`). Отказ строки НЕ валит пачку; сохранение файлов один раз; отчёт → `/import-result` с `job_key` → общий журнал импортов (`kind:"case"`; светофор свежести дампов НЕ бумпается). Коды выхода: 0 — пачка обработана (даже все отказы), 4 — тотальный сетевой сбой, 5 — job нечитаем. Тесты — [scripts/tests/test_add_cases_targeted.py](scripts/tests/test_add_cases_targeted.py) (включая `TestWiring` по workflow/worker/админке).
- [scripts/build_region_registry.py](scripts/build_region_registry.py) + [.github/workflows/probe_region_registry.yml](.github/workflows/probe_region_registry.yml) — проба реестра территории с GitHub-раннера (delo_id + классификация капчи; вход `ops/region_probe/courts_probe.csv`, отчёт коммитится в `ops/region_probe/report.txt`). **С 13.08.2026 — второй режим `--scan-servers`** (галка scan_servers в workflow, env REGION): разведка судебных присутствий — 1 GET страницы sud_delo на каждый домен 1-й инст. региона, разбор селектора площадок (`parse_server_options`: ссылки с srv_num=, фолбэк union), сверка с конфигом (`compare_servers`: «⚠ НОВАЯ ПЛОЩАДКА» + готовая строка CourtConfig, search_gated наследуется от домена), отчёт → `ops/region_probe/servers_report.txt`; обычная CSV-проба площадок не видит — ходит только на сервер 1. Тесты — [scripts/tests/test_region_probe_servers.py](scripts/tests/test_region_probe_servers.py).
- `scripts/tests/` + `tests/` — pytest-набор (320+ тестов: парсеры, state machine, линковка, архив, детектор здоровья, рендер дайджеста — матрица всех 30 типов событий в [tests/test_digest_template_events.py](tests/test_digest_template_events.py), линтер). Запуск одним прогоном: `python3 -m pytest` из корня (конфиг — [pytest.ini](pytest.ini)); CI гоняет на каждый push ([.github/workflows/tests.yml](.github/workflows/tests.yml)).
- [data/cases.json](data/cases.json) — активные дела (UTF-8, `version: 1`, `updated_at` ISO).
- [data/cases_archive.json](data/cases_archive.json) — «горячий» архив: дела, заархивированные за последние 12 мес. (`COLD_ARCHIVE_DAYS`). Грузится фронтом.
- `data/cases_archive_YYYY.json` — «холодные» годовые архивы: дела старше года, вынесенные ротацией (`rotate_cold_archive`). **Фронт их не грузит** (чтобы вес не рос безгранично), но скрипт читает их в индекс дедупликации. Холодные дела «заморожены»: не реактивируются автоматически.
- `data/.digested_acts` — дедуп уже обработанных судебных актов (скрытый файл). ⚠️ С 13.08.2026 номер попадает в файл ТОЛЬКО при реально взятом тексте (motive >100 симв.): суд часто поднимает флаг «Акт опубликован» раньше выкладки текста, а безусловный add после первого `new_act` навсегда закрывал ветку добора B (её гейт — этот же файл) — доехавший позже текст не попадал в дайджест никогда. Теперь первый `new_act` без текста даёт «Итог», второй (добор) — «Почему».
- `data/.cassation_acts` — дедуп кассационных определений: ключи «8Г-номер|дата акта», чьи `new_act` уже уходили в дайджест. Гасит повторный `new_act` при «мигании» `act_published` (сбойный парс 7kas). Ведётся в `link_cassation_cases`. С 13.08.2026 мигание закрыто и на уровне блока: pre-merge бэкфилл «исход не отзывают» (linking.py) не даёт деградировавшему парсу затирать терминальные поля (outcome/review_result/result_*/decision_date/act_*) — повторных `outcome_change` больше нет; hearing/suspended затираются легитимно. Бэкфилл только для ТОЙ ЖЕ жалобы (гард по 8Г-номеру — в awaiting_relink бывает замещение другой жалобой).
- `data/.act_summaries.json` — кэш LLM-пересказов мотивировок актов (ключ `sha1(act_text+"|v3-detailed")[:16]`; маркер стиля бампается только при смене формата пересказа — с 14.07.2026 это 2-3 предложения ≤450 симв.). Пополняется на GitHub-replay (на Mac ключа Anthropic нет), коммитится workflow'ами — без коммита каждый replay заново оплачивал бы пересказ тех же актов.
- `data/parse_health.json` — журнал здоровья парсеров: пер-источник история количества результатов поиска (20 судов 1-й инст., апелляция, 7kas до/после HMAO-фильтра). Для судов 1-й инст. счётчик — сберовские строки ДО фильтра ролей (`stats["sber_rows"]` из `parse_first_instance_search`; с 31.07.2026 прогон зовёт парсер с `keep_all_roles=True` и делит строки по роли сам — метрика от этого не изменилась): вал исков самого банка вытесняет ответчик-дела со стр. 1 и обнулял бы метрику без поломки (ложный алерт по Октябрьскому р/с 14–15.07.2026). Детектор «молчаливой поломки» (`update_parse_health`, блок 4e в `main_json`) шлёт сервисный 🩺-алерт в Telegram: суд с медианой ≥1 вернул 0 (на 1-м и 3-м нулевом прогоне + сообщение о восстановлении), HTTP-фейл 3 прогона подряд, все источники разом по нулям, ≥5 карточек-«огрызков» за прогон.
- `data/.bank_intake_seen.json` — негативный кэш авто-подхвата исков банка: строки выдачи, отвергнутые по КАРТОЧКЕ (итог из списка исключений, уже выданный ИЛ) или без ссылки. В дедуп-индекс такие дела не попадают, и без памяти прогон качал бы их карточки каждый день (оценка по свипу 31.07 — 20–60 лишних HTTP в сутки). Пишутся только ВЕЧНЫЕ причины (сетевой сбой ретраится), прунинг по `BANK_INTAKE_SEEN_TTL_DAYS`=60 от последнего появления в выдаче.
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
| `fi_court_by_domain` (суд 1-й инст. по (домен, srv_num); None = чужой регион) | [scripts/court_monitor/courts.py:219](scripts/court_monitor/courts.py:219) |
| `promote_material_record` (общее тело промоушена М→2: дамповый импорт + точечное добавление) | [scripts/court_monitor/linking.py:1417](scripts/court_monitor/linking.py:1417) |
| `process_item` (пер-строчная оркестрация точечного добавления) | [scripts/court_monitor/targeted_add.py:524](scripts/court_monitor/targeted_add.py:524) |
| `collect_existing_ids` (общий дедуп-индекс main_json/импортёра) | [scripts/court_monitor/linking.py:1281](scripts/court_monitor/linking.py:1281) |
| `load_bank_json` / `save_bank_json` (split-хранение bank-трека: список + events) | [scripts/court_monitor/storage.py:174](scripts/court_monitor/storage.py:174) |
| `bank_writ_expected` (ждём ли ИЛ: отказ/присоединение → нет) | [scripts/court_monitor/lifecycle.py:1260](scripts/court_monitor/lifecycle.py:1260) |
| `default_cancellation_state` (особый порядок отмены заочного: подано/отменено/отказано; матч по тексту события, белый список исходов) | [scripts/court_monitor/lifecycle.py:809](scripts/court_monitor/lifecycle.py:809) |
| `default_judgment_vacated` (решение отменено, а запись держит его действующим) | [scripts/court_monitor/lifecycle.py:1092](scripts/court_monitor/lifecycle.py:1092) |
| `default_cancellation_blocks_appeal` (гейт: апел. хода ещё нет, ст. 237 ч. 2) | [scripts/court_monitor/lifecycle.py:1119](scripts/court_monitor/lifecycle.py:1119) |
| `repair_vacated_default_judgments` (ремонт: откат решения + возврат в трек) | [scripts/court_monitor/lifecycle.py:1876](scripts/court_monitor/lifecycle.py:1876) |
| `intake_bank_rows` (блок 3b: приём исков банка с выдачи в прогоне) | [scripts/court_monitor/runs.py:1777](scripts/court_monitor/runs.py:1777) |
| `card_rejects` (карточные правила приёма; флаг skip_appeal — ручные каналы vs прогон) | [scripts/court_monitor/bank_intake.py:57](scripts/court_monitor/bank_intake.py:57) |
| `row_passes` (правила приёма по строке выдачи) | [scripts/court_monitor/bank_intake.py:49](scripts/court_monitor/bank_intake.py:49) |
| `make_bank_entry` (сборка записи трека: маркеры, ИЛ, флаги жалобы, delo_id/srv_num) | [scripts/court_monitor/bank_intake.py:193](scripts/court_monitor/bank_intake.py:193) |
| `_stamp_appeal_flags` (флаги жалобы + ДВИЖЕНИЕ жалобы + апеллянт из карточки в запись) | [scripts/court_monitor/bank_intake.py:280](scripts/court_monitor/bank_intake.py:280) |
| `appeal_objections_deadline` / `stamp_objections_deadline` (срок возражений из движения жалобы) | [scripts/court_monitor/lifecycle.py:1070](scripts/court_monitor/lifecycle.py:1070) |
| `apply_fi_appellant` / `appellant_is_bank` (апеллянт из карточки 1-й инст.; ре-экспорт `_apply_fi_appellant`/`_appellant_is_bank` в runs.py; **именной податель — «банк» ТОЛЬКО для самого ПАО Сбербанк**: дочки (страхование/НПФ/лизинг) отсеиваются `config.name_is_real_sberbank` с 09.08.2026 — 🏦 в кассации вставал на жалобу ООО «Сбербанк страхование жизни», кейс 8Г-11469/2026; та же проверка в `_cassation_card_to_block` linking.py; сохранённые True у дочек понижает тихая миграция `reclassify_named_appellants_is_bank`) | [scripts/court_monitor/runs.py:1720](scripts/court_monitor/runs.py:1720) |
| `bank_track_pending` (гейт раскладки 7c — по данным, не по счётчику загрузки) | [scripts/court_monitor/runs.py:1886](scripts/court_monitor/runs.py:1886) |
| `_FI_MERGED_RX` (присоединение к делу; ТОЛЬКО поле «Результат») | [scripts/court_monitor/lifecycle.py:174](scripts/court_monitor/lifecycle.py:174) |
| `repair_cancelled_merges` (объединение отменили → снять флаги) | [scripts/court_monitor/lifecycle.py:443](scripts/court_monitor/lifecycle.py:443) |
| `resolve_bank_merged_targets` (подбор дела-приёмника по ФИО ответчика) | [scripts/court_monitor/linking.py:1461](scripts/court_monitor/linking.py:1461) |
| `bank_cold_archive_path` / `is_bank_cold_archive_file` (холодные bank-архивы) | [scripts/court_monitor/config.py:108](scripts/court_monitor/config.py:108) |
| `case_court_key` / `dedupe_new_archive_entries` (ключ (домен, id) — номера не уникальны между судами) | [scripts/court_monitor/linking.py:1396](scripts/court_monitor/linking.py:1396) |
| `get_region` (env REGION → RegionConfig, ленивый лоадер) | [scripts/court_monitor/regions/__init__.py:20](scripts/court_monitor/regions/__init__.py:20) |
| `match_region_first_instance` (обобщённый матчер по региону) | [scripts/court_monitor/courts.py:58](scripts/court_monitor/courts.py:58) |
| `appeal_court_by_domain` (апел-суд по appeal.court_domain) | [scripts/court_monitor/courts.py:132](scripts/court_monitor/courts.py:132) |
| `appeal_court_for_fi_domain` (апел-суд по домену суда 1-й инст.) | [scripts/court_monitor/courts.py:159](scripts/court_monitor/courts.py:159) |
| `CourtConfig.search_by_fi_number_url` (целевой поиск апелляции по номеру 1-й инст., G2_CASE__CASE_NUMBER_ISS) | [scripts/court_monitor/regions/base.py:114](scripts/court_monitor/regions/base.py:114) |
| `relink_awaiting_appeal` (дослинк awaiting_appeal, не попавших на стр. 1 поиска апелляции) | [scripts/court_monitor/runs.py:150](scripts/court_monitor/runs.py:150) |
| `backfill_appeal_appellants` (тихий бэкфилл апеллянта в стадии appeal: апел. карточка подателя жалобы не публикует — разовый заход в карточку 1-й инст. ТОЛЬКО за «Заявителем жалобы», без событий/дайджеста; штамп `fi.appeal_appellant_checked_at`; капчёвые суды (search_gated) без fi.link пропускаются без HTTP и кэпа — иначе на Урале они вечно съедали весь max_per_run) | [scripts/court_monitor/runs.py:316](scripts/court_monitor/runs.py:316) |
| `reclassify_roleword_appellants` (пересчёт сохранённых слов-ролей подателя жалобы без HTTP: составные «ИСТЕЦ, ПРЕДСТАВИТЕЛЬ» старый классификатор писал «Иное лицо»/is_bank=False — бейдж вставал на противника банка, кейс 33-5089/2026; голый «ПРЕДСТАВИТЕЛЬ» → is_bank=null, бейдж спрятан) | [scripts/court_monitor/runs.py:1603](scripts/court_monitor/runs.py:1603) |
| `appellant_role_words` (разбор «Заявителя» жалобы на слова-роли, в т.ч. составные; None = настоящее имя) | [scripts/court_monitor/textutil.py:471](scripts/court_monitor/textutil.py:471) |
| `migrate_appeal_court_fields` (бэкфилл суда в блоках appeal) | [scripts/court_monitor/lifecycle.py:1846](scripts/court_monitor/lifecycle.py:1846) |
| `fetch_card_checked` (карточный fetch с детектом кода) | [scripts/court_monitor/netutil.py:182](scripts/court_monitor/netutil.py:182) |
| `card_breaker_allows` (пер-суд предохранитель карточек: гейт пропуск/проба) | [scripts/court_monitor/netutil.py:100](scripts/court_monitor/netutil.py:100) |
| `looks_like_outage_page` (URL-независимый детект заглушки — канарейка) | [scripts/court_monitor/parsing/search.py:422](scripts/court_monitor/parsing/search.py:422) |
| `DIGESTED_ACTS_PATH` / `CASSATION_ACTS_PATH` / `PARSE_HEALTH_PATH` | [scripts/court_monitor/config.py:174](scripts/court_monitor/config.py:174) |
| Константы state-machine (`FI_ARCHIVE_DAYS`, `CASSATION_*`) | [scripts/court_monitor/config.py:99](scripts/court_monitor/config.py:99) |
| `update_parse_health` — детектор молчаливой поломки парсеров | [scripts/court_monitor/health.py:42](scripts/court_monitor/health.py:42) |
| `advance_case_stage` / `is_case_archived` / `migrate_stages` | [scripts/court_monitor/lifecycle.py:1937](scripts/court_monitor/lifecycle.py:1937) |
| `reactivate_archived_first_instance` (возврат из архива) | [scripts/court_monitor/linking.py:441](scripts/court_monitor/linking.py:441) |
| `reactivate_bank_archived` (возврат из bank-архива; гейт «уже в активных» по case_court_key + мутация архива на месте — счётчик обязан пересохранить архив, иначе клоны) | [scripts/court_monitor/linking.py:451](scripts/court_monitor/linking.py:451) |
| `backfill_fi_links` (достройка `fi.link` у дел «с апелляции» — без неё cassation_watch слеп) | [scripts/court_monitor/linking.py:275](scripts/court_monitor/linking.py:275) |
| `rotate_cold_archive` (горячий → холодный архив) | [scripts/court_monitor/linking.py:1191](scripts/court_monitor/linking.py:1191) |
| `class TableExtractor(HTMLParser)` — парсер карточек дела | [scripts/court_monitor/parsing/tables.py:13](scripts/court_monitor/parsing/tables.py:13) |
| `parse_case_card` — карточка 1-й инст./апелляции | [scripts/court_monitor/parsing/cards.py:269](scripts/court_monitor/parsing/cards.py:269) |
| `parse_cassation_search_page` — поиск 7kas (HMAO-фильтр) | [scripts/court_monitor/parsing/cassation.py:50](scripts/court_monitor/parsing/cassation.py:50) |
| `classify_cassation_outcome` — детерм. enum исхода | [scripts/court_monitor/parsing/cassation.py:180](scripts/court_monitor/parsing/cassation.py:180) |
| `_extract_cassation_act_text` (секция `cont_doc1`) + `parse_cassation_card` | [scripts/court_monitor/parsing/cassation.py:361](scripts/court_monitor/parsing/cassation.py:361) |
| `relink_awaiting_relink_first_instance` (re-link после remanded) | [scripts/court_monitor/linking.py:234](scripts/court_monitor/linking.py:234) |
| `link_cases` (FI ↔ апелляция) | [scripts/court_monitor/linking.py:54](scripts/court_monitor/linking.py:54) |
| `link_cassation_cases` (link + discovery + remanded + архив + дедуп актов + бэкфилл сторон из УЧАСТНИКОВ 7kas; ⚠ признак «карточки ещё не было» для `new_cassation` — ОТСУТСТВИЕ `cassation.case_number`, а не пустота блока: `_apply_fi_cassator` кладёт туда заглушку с одним заявителем, и прежнее `if not old_cass` глушило объявление поступления в кассацию — 9 дел молча, 09–31.07.2026) | [scripts/court_monitor/linking.py:529](scripts/court_monitor/linking.py:529) |
| `parties_from_participants` (УЧАСТНИКИ → истец/ответчик; кроме ИСТЕЦ/ОТВЕТЧИК понимает ЗАЯВИТЕЛЬ/ВЗЫСКАТЕЛЬ и ЗАИНТЕРЕСОВАННОЕ ЛИЦО/ДОЛЖНИК — иначе у «прочих» категорий стороны пусты и касс. запись дайджеста вырождается в голый 8Г-номер) | [scripts/court_monitor/parsing/search.py:142](scripts/court_monitor/parsing/search.py:142) |
| `update_active_cases` (обход карточек активных дел) | [scripts/court_monitor/runs.py:543](scripts/court_monitor/runs.py:543) |
| `main_json` (оркестрация полного прогона) | [scripts/court_monitor/runs.py:2108](scripts/court_monitor/runs.py:2108) |
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
         "objections_due",         // ISO; срок для представления возражений на
                                   // апел. жалобу (ст. 325 ГПК) — штамп из
                                   // appeal_events, фронт читает готовым
         "objections_set_at",      // ISO; когда суд срок установил
         "objections_emitted",     // объявленный срок (идемпотентность ЗНАЧЕНИЕМ)
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
  — Cloudflare Worker по крону дёргает его через `workflow_dispatch` (03:30 UTC =
  08:30 ХМАО, пн-пт) — гоняет `python scripts/update_cases.py --json` целиком: парсинг 20 судов +
  апелляция + 7kas → гибридный дайджест (программный рендер + Claude только на
  пересказ мотивировок; откат — `DIGEST_FULL_LLM: "1"` в env) → Telegram (личный
  чат `TELEGRAM_CHAT_ID_TEST`) + Web Push всем подписчикам → коммит данных.
  Плановый прогон идёт со `smart_skip=true` (пропуск нерабочих дней РФ и дел с
  известной будущей датой); ручной — по галке. **С 02.08.2026 календарный гейт
  отделён от пер-кейсового skip'а**: галка `ignore_calendar` (env
  `IGNORE_NON_WORKING_DAY`) даёт «режим крона» в выходной — пропуск карточек с
  известной будущей датой сохраняется, а проверка производственного календаря
  не делается. Раньше оба механизма сидели на одном флаге, и прогнать в
  выходной «как крон» было нельзя вовсе: оставался только полный обход всех
  активных дел. Предикат — `skip_non_working_day` (runs.py); крон галку не
  передаёт, плановые прогоны в праздники по-прежнему завершаются сразу. Падение шага → 🚨-алерт в личный
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
- **Планировщик — Cloudflare Worker cron** (`crons = ["30 3 * * mon-fri"]` в
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
С 03.08.2026 у стародатного фильтра третье правило: «догоняющие» события об
акте/решении (`_FI_CATCHUP_DATED_TYPES`: `fi_resolved`, `fi_act_published`,
`fi_act_text_published`, `fi_motivirovka_emitted`, `fi_final_event`) старше
того же порога режутся, но ТОЛЬКО на первом парсе заведённого дела
(`first_parse=`, флаг `first_card_parse` снимается в FI-цикле ДО бампа
`fi["last_checked_at"]` — после него первый парс неотличим от рутинного;
стережёт `TestFirstParseFlagWiring`). Оба условия обязательны: только по
возрасту нельзя — суд штатно публикует текст акта через недели после решения,
и для дела на мониторинге это новость; только по «первому парсу» нельзя —
свежий иск с решением на той же неделе объявить надо. Вместе они описывают
раскопки истории только что заведённой карточки (2-592/2025: решение
06.10.2025, заведено 31.07.2026, объявлено 03.08.2026 и тем же прогоном ушло
в архив). ⚠️ `fi_writ_issued`/`fi_writ_status_changed` в правило НЕ входят —
ради листов трек и существует. Тяжёлый `details["act_text"]` уезжает вместе
с подавленным событием (иначе остался бы в снимке контекста и в оплаченном
LLM-пересказе ради строки, которую никто не увидит).

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

**Дайджест основного прогона — доработки 13.08.2026** (разбор «что уходит в
дайджест» юристом, согласован весь набор): (1) `fi_post_decision_hearing`
теперь ОБА трека (track-гейт снят: в основной картотеке это судебные
расходы/индексация ПРОТИВ банка; ветка рендера в 3.2, счётчик — в общий
«📅 заседаний в 1-й инст.»); (2) два новых кассационных типа в
`link_cassation_cases` — **`cass_hearing_scheduled`** (заседание 7kas:
раньше события не было ВООБЩЕ, дата печаталась только «прицепом»; эмит на
изменение даты, только будущая, только без исхода, не при `new_cassation`;
рендер — штатная типо-независимая строка, счётчик «📅 N заседаний
кассации») и **`cass_suspended`** («без движения»: срок устранения
недостатков жил только в skip-логике; эмит на новое/продлённое будущее
значение, строка «⏸ … срок устранения недостатков до даты»);
(3) детали в строках: секция «Вынесенные акты» апелляции печатает
нормализованный «Итог: {verdict_label}. Для банка: {bank_outcome}» вместо
сырого поля «Результат» (replay-фолбэк на прежнюю форму), тексты решений
3.6 несут дату решения («Итог: … (решение от …)»), новые апелляции — номер
дела 1-й инст. ХВОСТОМ СТРОКИ 1 (⚠️ линтер считает дела по строкам с
номерами — отдельная строка удвоила бы счётчик; проводка —
`_enrich_appeal_row_from_card` кладёт «Номер дела 1 инстанции» в строку,
CSV не растёт: `extrasaction="ignore"`), исход remanded — «→ в суд
апелляционной/первой инстанции» (`_REMANDED_TO_RU`, details обеих веток),
тип заседания 3.2 — скобками и только не-родовой («(беседа)»,
`_hearing_type_paren` — «назначено {тип}» ломал бы род), отказ в отмене
заочного в основной цепочке — нейтральный «⚖️ … открыт месячный срок»
(«✅…пошёл месяц» писался с позиции истца, банк-секция хранит свою);
(4) `fi_default_copy_served` получил ветку в основной 3.2 (эмит общий для
треков, а рендер был только банковский — выходила голая строка);
(5) новые апелляции ВТОРОГО КРУГА (после cassation_remanded) помечаются
строкой «🔁 повторное рассмотрение после кассации» — матч по свежему
cases.json (`case_by_appeal_num` в template.py: (домен, bare апел. номера)
→ дело с round≥2 и history-снимком `cassation_remanded*`; cases.json
грузится ОДИН раз на рендер, общий с parent-lookup кассации; несматч/нет
файла — тихо без пометки).

**Пересказы актов — доработки 13.08.2026** (разбор «какие акты
пересказываются» юристом): (1) секция «🏦 ИСКИ БАНКА» получила «Почему»
(см. блок трека ниже); (2) discovery-путь кассации унифицирован: рендер
«Новых касс. дел» берёт текст из details discovery-change'а (обрезан 1800 и
загейчен `.cassation_acts` в linking.py), прямое чтение
`cassation.act_text` — только фолбэк legacy-replay с той же обрезкой
(раньше полный акт до ~10 КБ уходил в LLM целиком и мимо дедупа);
(3) **`appeal.act_text` персистится** (`ap_json["act_text"]=act_text[:8000]`
в update_active_cases, один раз, только новые акты — бэкфилл требовал бы
перекачку карточек; потребителей у поля раньше не было) — drawer апелляции
показывает «Текст определения (полный)» (v140, страж
test_frontend_appeal_act.py).

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

- **Правила приёма в трек** (роль, исключаемые итоги, карточные фильтры, сборка
  записи) — общий модуль [scripts/court_monitor/bank_intake.py](scripts/court_monitor/bank_intake.py)
  для всех трёх каналов ввода: `row_passes` (строка выдачи), `card_rejects`
  (карточка; флаг `skip_appeal` — см. ниже), `make_bank_entry`, `entry_is_spent`,
  негативный кэш подхвата. Ручные скрипты зовут его ре-экспортом — `runs.py` из
  пакета не может импортировать `scripts/*.py` (зависимости односторонние).
- **Гейт «дело уже отработало» (`entry_is_spent`, с 03.08.2026)** — последний
  рубеж ВСЕХ трёх каналов, после `make_bank_entry`: собранная запись проверяется
  БОЕВЫМ `is_case_archived` (запись несёт `current_stage`+`track`, проверка сама
  уходит в `_is_bank_track_archived` — своей копии правил нет). Дело, уже
  подпадающее под архивное окно, первый же прогон архивирует, но перед этим
  качает карточку и пишет в дайджест решение полугодовой давности: 2-592/2025
  (решение 06.10.2025, отказ, суд сдал в архив 12.11.2025) заведено 31.07.2026,
  объявлено «текст решения опубликован» 03.08.2026 и тем же прогоном ушло в
  архив трека; **26 из 27** записей bank-архива прожили в треке ≤3 дней. Отказ
  вечный (`already_spent` в `PERMANENT_REJECTIONS`) — иначе авто-подхват качал
  бы карточку каждый прогон. Дела с признаком жалобы гейт не трогает (первая
  ветка `_is_bank_track_archived`). Там же `make_bank_entry` **замораживает
  `decision_date`** из событий карточки: он ставит решённым делам
  `resolved_emitted=True`, а эмит `fi_resolved` — единственное место, где дата
  замерзает, и для импортированного дела он уже не выстрелит; без штампа якорем
  `classify_writ_kind`/`bank_legal_force_est`/архивного окна осталась бы
  дрейфующая `hearing_date`.
- **Ввод пула — канал 1 (авто, с 31.07.2026)**: **блок 3b фазы 3 прогона**
  (`intake_bank_rows`, runs.py) — истцовые строки той же страницы выдачи, которую
  прогон уже качает ради ответчик-дел, заводятся в трек сами (`import.source =
  "auto_search"`). До этого трек пополнялся только вручную, и новый иск вставал
  на мониторинг лишь после того, как юрист вспомнит запустить сбор — ранние ИЛ
  терялись. Отличие от ручных каналов: дела с признаком апелляции/кассации
  **берутся** (`card_rejects(skip_appeal=False)`, решение юриста 31.07.2026) —
  `make_bank_entry` переносит флаги жалобы из карточки в запись, и `split_bank_track`
  тем же прогоном уводит дело в основной cases.json на полный мониторинг апелляции
  (иначе апелляция по иску банка вне охвата: автопоиск 1-й инст. истцовые дела не
  заводит). Глубина — **только страница 1** (решение юриста); если новой оказалась
  и последняя строка выдачи, прогон предупреждает, что нужен добор ручным
  `collect_bank_claims.yml`. Предохранители: негативный кэш отказников
  `data/.bank_intake_seen.json` (в дедуп-индекс отвергнутые дела не попадают —
  без кэша их карточки качались бы каждый прогон заново; пишутся только вечные
  причины, сетевые сбои ретраятся), `BANK_INTAKE_MAX_PER_RUN`=30,
  `BANK_INTAKE_MAX_CARDS_PER_COURT`=10 (поиск 1-й инст. идёт РАНЬШЕ FI-цикла карточек — пачка
  нечитаемых карточек открыла бы пер-судовый предохранитель и сняла суд с обхода
  на весь прогон), пре-чек `card_breaker_allows` до `polite_delay`, 🩺-алерт при
  `bank_intake_added > BANK_INTAKE_ALERT_ADDED`. Рубильники (Actions Variables,
  прокинуты в update_cases.yml): `BANK_AUTO_INTAKE=0` — трек живёт, авто-приём
  выключен; `BANK_INTAKE_DRY_RUN=1` — холостой прогон (кандидаты считаются,
  карточки не качаются, записи не создаются). В дайджесте — строка
  `fi_bank_claim_registered` в секции «🏦 ИСКИ БАНКА»; в админке — группа
  «Заведено авто-подхватом» отчёта парсинга (исход `intake_new`).
- **Ввод пула — канал 2 (реестр)**: [scripts/import_bank_registry.py](scripts/import_bank_registry.py)
  + workflow [import_bank_registry.yml](.github/workflows/import_bank_registry.yml) —
  реестр `ops/bank_registry/registry.csv` («домен;номер»), целевой поиск по
  номеру (общие функции — [scripts/court_monitor/target_search.py](scripts/court_monitor/target_search.py),
  вынесены из add_cases_manually), только роль «Истец», порционно `--limit`,
  идемпотентно; `import.announced=true` сразу и уже решённые получают
  `resolved_emitted=True` — **старые решения задним числом не льются**, и как
  «новые иски» основной картотеки track-дела не анонсируются (своя секция —
  выше). **Канал 3 — разовый сборщик
  выдачи** [scripts/collect_bank_claims.py](scripts/collect_bank_claims.py)
  + workflow [collect_bank_claims.yml](.github/workflows/collect_bank_claims.yml)
  (галка dry_run, отчёт → `ops/bank_registry/collect_report.txt`; **с
  13.08.2026 безопасен в форке территории**: push-запуски запинены на эталон
  `github.repository == 'SelivanovAS/dashboard'` — инцидент 26.07.2026, когда
  merge синхронизации триггерил сбор судов ХМАО из форка; `REGION` из vars;
  `court_domain` обязателен, без ХМАО-дефолта; стережёт `TestWorkflowWiring`
  в test_collect_bank_claims.py): обходит
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
  (bank_intake.py). Суд резолвится ПАРОЙ (домен, `--srv`, вход
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
  ИЛ): ИЛ выдан +14 дн → архив (**заочное решение +90 дн**,
  `BANK_DEFAULT_WRIT_ARCHIVE_DAYS`); без ИЛ — потолок 180 дн от вступления в
  силу; возврат/прекращено +30 дн. Признак жалобы всегда держит в активных.
  ⚠️ Заочность в этой ветке ПЕРЕСЧИТЫВАЕТСЯ по событиям, а не читается штампом
  `fi["default_judgment"]`: у архивных записей штампа нет вовсе — именно
  поэтому три заочных дела Сургутского гор. уехали в архив по 14-дневному окну
  27.07.2026. Возврат таких дел — `reactivate_bank_archived` (linking.py, при
  загрузке архива трека в `main_json`; штатного канала реактивации у
  bank-архива нет: `reactivate_archived_first_instance` работает по основному
  архиву). ⚠️ **Инцидент 04–07.08.2026**: первая версия возврата (inline-блок
  8dba1a8) не имела гейта «уже в активных» и не пересохраняла архивный файл
  (условие сохранения считало `bank_hot_before` УЖЕ после изъятия) — три
  заочных Сургутского клонировались из архива КАЖДЫМ прогоном (+1 копия в
  день, копии отличались только `last_checked_at`; события дубли маскировали —
  у копий один ключ `bank_events_key`). С 09.08.2026 хелпер гейтит по
  `case_court_key` (совпадение с активным оставляет запись в архиве), мутирует
  архивный список на месте и возвращает счётчик, который ОБЯЗАН входить в
  условие пересохранения архива в фазе 7c (стережёт
  `TestBankArchiveReactivation.test_run_wiring_saves_archive_after_reactivation`);
  данные отремонтированы разово (502→493 активных, 27→24 архивных). Тогда же
  в корень cases_bank.json добавлен **`archived_count`** — размер горячего
  bank-архива для фронта (архив грузится лениво, и до клика по чипу «Архив»
  счётчик «N в архиве» взять больше неоткуда).
- **Особый порядок отмены заочного решения (ст. 237-243 ГПК, с 03.08.2026)**:
  ответчик подаёт заявление об отмене **в тот же суд 1-й инстанции** (7 дн со
  дня вручения копии), суд рассматривает его за 10 дн (ст. 240) и выносит
  определение об ОТКАЗЕ либо об ОТМЕНЕ решения с возобновлением рассмотрения
  (ст. 241, 243). Это **не апелляция**: апелляционный ход у ответчика
  открывается только со дня определения об отказе (ст. 237 ч. 2), поэтому
  зарегистрированная судом апел. жалоба до этого момента **не уводит дело из
  трека** (`default_cancellation_blocks_appeal` — гейт в `bank_case_left_track`
  ТОЛЬКО на ветке признаков жалобы и в `advance_case_stage` ТОЛЬКО для дел
  трека: код общий, и банк-ОТВЕТЧИК с заочным решением обязан дойти до
  `awaiting_appeal`, иначе `relink_awaiting_appeal` его не увидит).
  Состояние читает `default_cancellation_state` — по `ev["text"]`, а НЕ по
  колонке `ev["name"]`: в основной картотеке 43% событий 1-й инст. идут без
  колонок (legacy-склейки), а кейс 2-616/2026 живёт именно там. Исходы —
  **белый список** («заочное решение отменено» / «в удовлетворении заявления
  отказано»); любой другой результат заседания — `pending`, потому что в
  колонке реально встречаются «Заседание отложено» (124 раза по корпусу),
  «Объявлен перерыв», «Производство приостановлено». Потолок ожидания —
  `BANK_DEFAULT_CANCEL_PENDING_MAX_DAYS`=90 (без него дело с незаполненным
  результатом висело бы активным вечно). Ритм опроса — скип до даты заседания
  по заявлению (`default_cancel_hearing`): событие «Рассмотрение заявления об
  отмене…» не матчится ни `_SESSION_START_RX`, ни `_HEARING_MARKERS_RX`, и без
  своей ветки дело парсилось бы каждым прогоном.
  **Отмена возвращает дело в работу** (`default_judgment_vacated`): понижение
  статуса делает FI-цикл ТАМ, ГДЕ СТАТУС ПИШЕТСЯ (исключение `vacated_default`
  рядом со `spurious_resolution`) — правка только в `cards.py` бесполезна, Гард
  2 вернул бы «Решено» тем же прогоном, а существующая калитка
  `fi_resolution_contradicted_by_future_hearing` завершается `return not
  has_decision` и у заочного дела всегда False. Транзакция ОДНА: статус +
  `result=""` + сброс `resolved_emitted`/`motivirovka_emitted` (сброс флага
  отдельно от статуса дал бы ложный «Иск удовлетворён» КАЖДЫМ прогоном) +
  `decision_date` → `decision_date_vacated` (не `pop`: `classify_writ_kind`
  фолбэчится на дрейфующую `hearing_date`, и уже выданный лист на исполнение
  молча стал бы обеспечительным). Предикат сравнивает дату отмены с
  **замороженной** `decision_date`, а не «нет ли более позднего решения»: при
  недельном ритме отмена и новое решение по ст. 243 попадают в одно окно
  парса. Разовый ремонт существующих данных — `repair_vacated_default_judgments`
  в НАЧАЛЕ `migrate_stages` (позже бэкфилл `decision_date` вернул бы снятую
  дату, а цикл `advance_case_stage` — стадию). В дайджесте — четыре типа
  (`fi_default_cancellation_filed`/`_hearing`/`fi_default_judgment_vacated`/
  `fi_default_cancellation_refused`), идемпотентные ЗНАЧЕНИЯМИ в флагах
  `default_cancel_*_emitted`; подписи — в трёх местах (`_BANK_TYPE_LABELS`,
  цепочка секции 3.2, `digest/core.py`), иначе неизвестный тип даёт голую
  строку дела и всё равно считается в счётчике «Изменения (N)». Фронт читает
  готовый штамп `first_instance.default_cancellation` (самоисцеляющийся блок в
  `split_bank_track`) — своей копии правил в JS нет.
- **Срок для возражений на апел. жалобу (ст. 325 ГПК, с 03.08.2026)**: строка
  «Установлен срок для предоставления возражений · Срок до ДД.ММ.ГГГГ» живёт НЕ
  в «Движении дела», а во вкладке карточки «Обжалование решений, определений» →
  вложенная таблица «Движение жалобы», и приезжает в `first_instance.appeal_events`
  (парсер это умел всегда, 17 строк в корпусе — но датой-дедлайном срок нигде не
  был, только сырым текстом в ленте drawer'а). Извлекает `appeal_objections_deadline`
  (lifecycle.py) — матч по `name`, при его отсутствии по `text` (43% строк —
  legacy-склейки), при нескольких строках побеждает МАКСИМАЛЬНЫЙ срок (суд
  продлевает). ⚠️ Слово «возражени» в паттерне обязательно: рядом в той же
  таблице живёт «Оставление жалобы без движения · Срок для устранения
  недостатков до …» — срок ОБРАТНОЙ полярности (недостатки устраняет податель
  жалобы). Штамп `objections_due`/`objections_set_at` (ISO) ставится в ДВУХ
  местах: `migrate_stages` (идемпотентно, обе картотеки — bank-дела подмешаны в
  `cases` до вызова) и эмит-блок FI-цикла (миграция отработала на загрузке, ДО
  вливания свежих `appeal_events` — без второго вызова срок доезжал бы до
  drawer'а следующим прогоном). Событие `fi_objections_deadline_set`,
  идемпотентность ЗНАЧЕНИЕМ в `objections_emitted`. **Анти-паводок:** эмит
  гейтится «срок ещё не истёк», а `migrate_stages` засевает маркер ТОЛЬКО
  истёкшим сроком — безусловный посев закрыл бы дедлайн навсегда у дела,
  заведённого авто-подхватом (эмит-блок к тому моменту ещё не отрабатывал).
  Stale-якорь — САМ срок (`objections_due` в `_FI_DATED_COMPLAINT_TYPES`): он в
  будущем, штатный дедлайн фильтр не тронет. В эхо-класс тип НЕ входит —
  апелляционная карточка срок не публикует. Фронт: строка «⏳ Возражения до» в
  «Ключевых датах», пилюля `objectionsBadgeHtml` в строке/hero и в слоте
  `.mc-track` мобильной карточки (не отдельным рядом!). **Полярность срочности
  задаёт апеллянт**: `appeal_appellant_is_bank===false` → срок наш, красим по
  остатку (>7 дн / ≤7 / ≤2); `true` → жалоба банка, пилюли нет; `null`
  («неопределимо» при соответчиках) → строка есть, срочности нет. Читать
  `c._fi` напрямую — VM коэрсит `!!` и теряет разницу false/null.
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
  `fi_writ_status_changed` (НЕ в эхо/stale-фильтрах) и `fi_bank_claim_registered`
  (дело заведено авто-подхватом; НЕ рутина — переживает `BANK_DIGEST_ROUTINE=0`);
  маркер `change["track"]`
  едет в данных fi_changes — сигнатуры/replay не тронуты. Рутина отключается
  `BANK_DIGEST_ROUTINE=0` (`filter_bank_routine_events`; дефолт 1 — пилот
  шлёт всё). **С 09.08.2026 (разбор дайджеста 07.08 юристом)**: секция
  сгруппирована «по важности» (`_BANK_GROUP_ORDER`/`_bank_change_group`,
  template.py: решения/акты → ИЛ → завершения → новые иски → заседания по
  дате, ближайшие сверху; пустая строка между группами, БЕЗ подзаголовков —
  их пришлось бы синхронизировать с линтером счётчиков); строки решений и
  публикаций несут дату решения и пометку «(🌙 заочное)»
  (`details["default_judgment"]`, только True — replay-safe);
  `fi_final_event` показывает суть события («📄 мотивировка изготовлена
  (дата)» / короткая цитата) вместо генерика «движение по делу»;
  передачи/возвраты — с датой события (`details["termination_date"]`,
  ставит `fi_termination_details`, печатают обе секции и full-LLM);
  сводка считает передачи по подсудности ОБОИХ треков; новое событие
  **`fi_default_copy_returned`** («🌙 копия заочного решения возвратилась
  невручённой», факт формулы ВС) — эмит в FI-цикле идемпотентен ЗНАЧЕНИЕМ
  (`fi["default_copy_returned_emitted"]` = дата), анти-паводок: посев в
  `migrate_stages` + `_FI_CATCHUP_DATED_TYPES`; футер получает приписку
  «· 🏦 иски банка: N в производстве» (`total_active_bank` считается при
  `split_bank_track`, едет опциональным kwarg до шаблона/контекста —
  старые контексты replay живут; в сумму «всего» НЕ входит), а счётчик
  «апел.» считается из cases.json (стадия appeal, не «Решено») вместо
  CSV-снимка с мёртвыми делами. Дубль «решение + мотивировка» одного дела
  склеивается в одну запись 3.5 (`_merge_motiv_into_resolved`, кейс Урала
  2-484/2026; банк-трек функция пропускает — там дело и так одна строка).
  **С 13.08.2026 (разбор «что уходит в дайджест» юристом)**: (1) голые
  подписи обогащены датами/временем в `_bank_event_phrases` — срок
  возражений («до ДД.ММ.ГГГГ»), четыре события отмены заочного, жалобы,
  мотивировка/акт, время заседаний (00:00-заглушка ГАС скрыта,
  `_bank_hearing_time`), перерыв — с датой продолжения, роль банка —
  «X → Y»; всё replay-safe (нет ключа → прежняя подпись). (2) Четыре
  НОВЫХ события: **`fi_legal_force_reached`** («✅ решение вступило в силу
  (расч.) — ожидаем ИЛ») и **`fi_writ_overdue`** («⚠️ ИЛ не выдан N дн.»,
  порог `BANK_WRIT_OVERDUE_ALERT_DAYS`=30 — синхронен фронтовому бейджу) —
  **календарный проход `collect_bank_calendar_events`** (runs.py, после
  врезки intake / ДО фильтра рутины и вливания `bank_new_cases`; решённые
  дела живут в недельном ритме и в FI-цикле change не собирают, а эти
  события наступают датой); гейты: «Решено» (est умеет посчитаться и у
  нерешённого от дрейфующей hearing_date), не покинуло трек,
  `bank_writ_expected`, не архивируется, нет enforcement-листа;
  идемпотентность ЗНАЧЕНИЕМ est (сдвиг даты переобъявляет — осознанно);
  типы календарных дописываются в существующую запись дела (одна строка).
  ⚠️ Анти-паводок — НЕ посев в migrate_stages (расчётная дата пересекает
  «сегодня» без изменения данных, посев на загрузке глушил бы и будущие
  события), а **эпоха `BANK_CALENDAR_EVENTS_SINCE`**=13.08.2026: сила
  раньше эпохи / порог просрочки, пересечённый до эпохи, — тихая пометка
  (бэклог «64 в силе / 14 просрочено» юрист велел не объявлять);
  `fi_legal_force_reached` вдобавок в `_FI_DATED_COMPLAINT_TYPES`
  (массовый импорт давно решённых дел), `fi_writ_overdue` — нет намеренно.
  **`fi_post_decision_hearing`** («📅 заседание по решённому делу», ветка
  в FI-цикле ПОСЛЕ hearing-блока: гард `case_decided` глушил индексацию /
  суд. расходы / отсрочку должника целиком) — только трек, только будущая
  дата, обязательное session-событие (фантомная «Дата заседания» решённых
  дел — фолбэк по определениям), не дублирует заседание по отмене
  заочного; тема из `ev.ground` → `details["hearing_topic"]`; тип в
  `_FI_HEARING_ANNOUNCE_TYPES`. **`fi_default_copy_served`** («🌙 копия
  заочного вручена ответчику», парное к возвратившейся: запускает 7-дн
  срок отмены и пересчёт est) — эмит рядом с copy_returned, посев в
  `migrate_stages` + `_FI_CATCHUP_DATED_TYPES`. Группы: календарные — с
  ИЛ, пост-решенческое — с заседаниями. Сводка теперь пишет «N дел с
  событиями по искам банка» (считала ДЕЛА, а подписывала «событий»).
  **С 13.08.2026 — LLM-пересказ «Почему»** у `fi_act_text_published` с
  исходом ПРОТИВ банка (гейт `bank_act_why_eligible`, template.py:
  `bank_outcome ∉ {"", "в пользу банка"}` — отказ/частичное/прекращение с
  учётом роли; полные удовлетворения НЕ пересказываются — решение юриста):
  строка «Почему» СРАЗУ за строкой дела (абзац — контракт
  attach_act_analyses), БЕЗ номера дела (линтер считает дела по строкам с
  номерами) и БЕЗ excerpt-фолбэка (при отказе LLM — прежняя одна строка).
  Тем же днём — второй вызов `attach_act_analyses` для банк-дел в runs.py
  (после основного: банк-дела к моменту attach уже разложены в
  `bank_active`, и первый вызов их не находил): гейт тот же, цели
  фильтруются по `court_domain` details (номера не уникальны между судами),
  `require_explained=True` (без «Почему»-абзаца — raw_act-фолбэк, а не
  строка события под видом анализа), при обновлениях `cases_bank.json`
  пересохраняется; drawer банк-дела показывает «AI анализ» без правок
  фронта.
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
  бейджем 🏦 — УДАЛЁН 11.08.2026 решением юриста: роль видна из сторон,
  страж test_bank_track_badge_stays_removed; переключатель картотек в
  mine-режиме скрыт, bank-список
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
  и bank-датасет до достигнутого уровня цепочки. **С v130 (09.08.2026)**:
  счётчик под таблицей и сегмент считают знаменатель по АКТИВНЫМ делам
  (после ленивой догрузки архива `bankCases` прирастает архивными и число
  прыгало бы 493→517) + приписка «· N в архиве» из `archived_count` корня
  cases_bank.json (`bankArchivedMeta` в app.js; после загрузки архива — по
  факту); чип «Архив» до загрузки показывает мету, а не «0» (нет меты у
  старого снимка — «…»); свёрнутость «Ближайших заседаний» персистится
  (`UPCOMING_COLLAPSED_KEY` через `lsKey`, классы подставляются прямо в
  разметку `renderAnalytics` — `#analytics-row` пересобирается целиком на
  каждом `applyFilters`, и без персиста блок разворачивался при каждом
  переключении картотек/фильтров). **С v132 (10.08.2026) — кросс-поиск между
  картотеками и честные счётчики** (юрист предлагал слить картотеки под
  ролевые фильтры — отвергнуто: граница не по роли, а по жизненному циклу,
  58 из 59 «истцовых» основной картотеки — переехавшие из трека обжалования):
  кросс-поиск при нулевой выдаче
  (`renderSearchCrossHint`/`countSearchMatches`, `#search-cross-hint`;
  единственная фоновая догрузка bank-списка вне штатных триггеров — тройной
  гард + `_crossHintLoadFailed`, архив/events не трогает); поисковый блоб
  ЕДИНЫЙ — `caseSearchBlob` (вторая склейка разъедется молча); знаменатели
  счётчиков везде = АКТИВНЫЕ («Основные» считались с архивом — асимметрия);
  в «★ Мои» KPI (`mainKpiCounts`) и сегменты стадий — по mine-набору с тем же
  предикатом `isWatchedCase(c)||isNewCase(c)`, что mine-ветка `applyFilters`
  (держать синхронно); «Сбросить» сбрасывает и категорию; счётчик кнопки
  «Фильтры» не считает роль/инстанцию в bank-режиме; `bankArchivedMeta` —
  только из `cases_bank.json` (`isBankListUrl`). ⚠️ Баннер-мостик при фильтре
  «Истец» построен и УДАЛЁН тем же днём решением юриста (дублировал
  #dataset-switch прямо над собой, дёргал раскладку) — не возвращать, страж
  `test_bridge_banner_stays_removed`. Стражи —
  [scripts/tests/test_frontend_bridges.py](scripts/tests/test_frontend_bridges.py).
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
- `BANK_AUTO_INTAKE` — авто-подхват исков банка в прогоне (дефолт 1; `0` — трек работает, но новые иски снова заводятся только вручную).
- `BANK_INTAKE_DRY_RUN` — холостой прогон подхвата: кандидаты считаются и логируются, карточки не качаются, записи не создаются (дефолт 0). Для первого прогона после деплоя.
- `BANK_INTAKE_MAX_PER_RUN` / `BANK_INTAKE_MAX_CARDS_PER_COURT` / `BANK_INTAKE_ALERT_ADDED` — потолки подхвата за прогон (30) и карточек на один суд (10: поиск 1-й инст. идёт раньше FI-цикла карточек, пачка нечитаемых карточек открыла бы предохранитель суда) + порог 🩺-алерта «паводок» (50). С 13.08.2026 прокинуты в update_cases.yml из Actions Variables (рычаг темпа ввода территории: разгон Урала — 200/25/200 без правки кода; стережёт `TestBankIntakeCapsWiring`).
- `BANK_INTAKE_SEEN_TTL_DAYS` — сколько помним отказников негативного кэша (60).
- `CARD_BREAKER_THRESHOLD` — пер-суд предохранитель карточек: столько не прочитанных карточек подряд (заглушка/код/сеть) снимают суд с обхода до конца прогона (дефолт 3; `0` — выключить, например для ручной пробы лежащего суда).
- `CARD_BREAKER_PROBE_EVERY` — half-open: каждая K-я пропущенная карточка отключённого суда идёт пробой, успех возвращает суд в обход (дефолт 30; `0` — без проб).
- `LOG_LEVEL` — уровень логов прогона (`DEBUG`/`INFO`/`WARNING`/`ERROR`, дефолт `INFO`); `DEBUG` показывает пер-кейсовые skip/«без изменений» и прочую диагностику.

## Куда уходит дайджест

- **Telegram:** все workflow'и шлют в личный чат (`TELEGRAM_CHAT_ID_TEST`) по умолчанию. Чтобы продублировать в корпоративную группу — поставить галку `to_group` в UI Run workflow. Текст дайджеста в Telegram **общий**, не персонализированный.
- **PWA push:** `update_cases.yml` (крон) шлёт всем подписчикам PWA. Тестовый workflow `test_digest.yml` шлёт push **только устройствам-владельцам** по умолчанию, чтобы не спамить коллегам прототипами. У `test_digest.yml` есть галка «push_all» — отправит на все устройства. Чтобы пометить своё устройство владельцем — открыть PWA по URL `https://selivanovas.github.io/dashboard/sberbank_dashboard.html?owner=<OWNER_SECRET>` (один раз).
- **Персонализация push по watchlist (`_per_sub` callback):** push-payload собирается под каждого подписчика отдельно через фабрику `_make_per_sub_callback` ([scripts/court_monitor/delivery.py:325](scripts/court_monitor/delivery.py:325)). Новые дела (`fi_new_cases`, `appeal_new_cases_csv`) — общесистемный сигнал, шлются всем; изменения и переходы стадий — только если дело в watchlist подписчика. Click_url для подписчиков с watchlist — `?digest=open&mine=1`. Используется в основном кроне (`main_json`), `--replay-last`, `--push-last-digest`.

## Админка подписчиков

URL: `https://court-monitor-trigger.7selivanov-a.workers.dev/admin?secret=<OWNER_SECRET>`. Открывается в браузере (мобильно тоже). HTML-страница вынесена в [cloudflare-worker/admin_page.js](cloudflare-worker/admin_page.js) (`renderAdminHtml(secret, role, cfg)`, wrangler бандлит импорт сам); серверные эндпоинты — в [cloudflare-worker/worker.js](cloudflare-worker/worker.js). ⚠️ Вся страница — один template literal: внутренний JS пишется без backtick'ов и `${`, backslash удваивается. Naive-таймстампы из data/*.json (Python на UTC-раннере пишет без «Z») страница парсит как UTC (`parseIso`).

**Роли (с 16.07.2026):** owner (`OWNER_SECRET`) — всё; operator (`OPERATOR_SECRET`, один общий на сопровождающих капчёвых судов; не задан — роль неактивна, у ХМАО так) — статус+здоровье+живой лог+секция «Импорт дел». Гейт — `resolveAdminRole`/`requireAdminRole` (worker.js): чужой секрет → 401, оператор на owner-эндпоинте → 403 (реальный запрет на сервере, скрытие в UI — `data-owner-only` + `html[data-role]`). `DISPATCH_WORKFLOWS` — `{inputs, roles}`: update_cases/test_digest — owner, import_cases/add_cases — обе роли. Секция «Импорт дел» (`#import`): **с 10.08.2026 вкладка видна ОБЕИМ ролям на ЛЮБОЙ территории** — первой карточкой блок «Добавить дела» (точечное добавление до 20 строк: номер/ссылка, построчная валидация против `region.*` cases.json, POST `/admin/add-case`, поллинг того же журнала тиком 60 с с потолком 40 мин — пачка номеров идёт дольше дампа, а каждый тик стоит KV-list; имя оператора общее с дамповой формой через `admin_operator_name`); дамповая часть ниже — dropdown gated-судов из `region.fi_courts` (у ХМАО их нет — прячется только она + светофор, `.imp-grid` схлопывается в одну колонку, журнал грузится `logonly=1`), вставка rich-paste/файл, поллинг журнала (`/admin/import-log`), история импортов (записи `kind:"case"` рендерятся «📌 точечно · N стр.»), **светофор свежести по судам** (когда каждый суд импортировался в последний раз: зелёный ≤7 дн, жёлтый ≤14, красный дольше/ни разу; данные — вечные ключи `import:last:<домен>`, пишутся на done в `/import-result`). **С 17.07.2026 — операторский UX (394cd14):** оператору секция открыта по умолчанию (с 02.08.2026 — вкладка по умолчанию, `TAB_DEFAULT`; серверная перестановка `IMPORT_SECTION` убрана), светофор раскрыт и кликабелен (клик по суду = выбрать его в форме, слушатели — делегированием), 5-я плитка пульта «Импорты» (N просрочено; обеим ролям, без gated-судов скрыта — ХМАО не видит, `.pult.has-import` в мобильном медиа-блоке обязателен), drag-n-drop файла в поле вставки + индикатор «что уйдёт» (файл побеждает вставку) + предпроверка «есть ли ссылки» до отправки, статус ожидания с таймером, сбой cases.json → алерт с «Повторить» (owner'у — тихо, как раньше). **Автоопределение суда по вставке (17.07.2026):** хост из абсолютных ссылок карточек подставляет суд в dropdown сам (пока оператор не выбирал вручную — флаг `impCourtTouched`), конфликт — заметка «⚠ ссылки ведут в …» с кнопкой «выбрать этот суд», отправка чужого дампа блокируется на клиенте, Worker'е (400) и в импортёре (`EXIT_WRONG_COURT`). ВСЕ URL данных страницы выводятся из `CASES_DATA_URL` Worker'а (`adminPageConfig()`) — хардкод сломал бы админку территорий. **Операторский путь дочищен 02.08.2026:** после успешного импорта форма ОЧИЩАЕТСЯ (поле + файл + `impRunDetect`) и светофор перерисовывается из кэша `impLastFreshMap` без похода в KV — раньше оператор шёл очередью судов, и следующий Ctrl+V клеился в конец прошлого дампа («⚠ ссылки нескольких судов», отправка заблокирована, кнопки «очистить» нет); при `failed` вставка сохраняется для повтора. Ссылка на суд несёт `srv_num` выбранной площадки (+`delo_id=1540005&name_op=sf`) — голая вела на первую площадку домена, а часть судов заведена ТОЛЬКО как `srv_num=2` (серверные предохранители этого не ловят: хост и delo_id совпадают). Первый рендер светофора подставляет самый просроченный суд (`impFreshAutoPicked`; `impCourtTouched` при этом НЕ ставится — автоопределение по вставке должно сохранить право переключить). Индикатор и предпроверка считают `name_op=case` («дел на странице: N»), а не все `a[href]` — прежний счётчик показывал 137 при десятке дел, а тест на голый `<a>` пропускал вставку без единого дела. Светофор показывает «+7 из 24» (`rows` в вечном ключе `import:last:*`, с 02.08.2026 — у старых записей его нет, фолбэк «+7»). Мобильная форма — своим медиа-блоком ПОСЛЕ правил `.imp-row` (общий блок «Мобильная раскладка» стоит в файле раньше и при равной специфичности проигрывал), поля 16px — iOS зумит всё мельче. **С 02.08.2026 вкладка на ≥1200px в две колонки** (`.imp-grid`: форма слева, свежесть+история справа) — иначе на 1440px операторская была одной узкой колонкой и рабочая очередь не читалась одновременно с формой; обеим колонкам `min-width:0` (зона вставки с таблицей суда распирала бы трек), ≤1200 — обратно в одну.

**Дизайн v2 (13.07.2026)** — визуальный язык дашборда: токены цветов/шрифтов скопированы из [styles.css](styles.css) (IBM Plex с Google Fonts, сберовский зелёный, бейджи-пилюли, цвета стадий teal/indigo/violet — карта `stageBadge` зеркалит `stageBadgeHtml` из app.js), 3-режимная тема авто/свет/тьма (localStorage `admin_theme`, инлайн-скрипт в head), статусы — цветные точки/пилюли вместо эмодзи, иконки — inline-SVG. При смене палитры дашборда токены админки синхронизировать вручную.

**Каркас — ВКЛАДКИ (с 02.08.2026).** Чипы шапки (`role="tablist"`) переключают панели: показана ровно одна секция, `.section{display:none}` / `.section.is-tab-active{display:block}`. До этого страница была лентой на 3,6 экрана у владельца (76% — «Подписчики») и 5,7 на телефоне, а у оператора пульт с плиткой «Импорты» начинался на 897px — ниже первого экрана. Теперь любая вкладка умещается в один экран (@1440: 813px), и серверная перестановка `IMPORT_SECTION` по роли убрана — порядок в DOM ничего не решает, решает активный чип.
- **Состояние — hash, без localStorage** (`showTab`/`tabAllowed`/`tabFromHash`, дефолт: оператор → `import`, владелец → `system`). ⚠️ В hash пишется **`#tab-<id>`, а не голый id секции**: иначе Chrome после `replaceState` находит в документе элемент с таким id и выполняет отложенный «прыжок к фрагменту» уже после load — страница уезжала вниз, липкая шапка уходила за верхний край. Старый формат читается тоже. `history.replaceState`, не `pushState` (иначе «назад» листает вкладки и плодит копии URL с секретом); присваивать `location.hash` нельзя.
- ⚠️ **Делегирование кликов строго на `#nav`**: класс `.chip-btn` носит ещё и ссылка «Открыть поиск по суду» внутри формы импорта.
- ⚠️ **`initTabs()` вызывается в стартовой секции внизу**, а не на месте определения: `onTabShown` читает `lastStaticLoadAt` (`let` ниже по файлу) — вызов раньше объявления упал бы в TDZ.
- Недоступная вкладка из hash откатывается на дефолт (`tabAllowed`: роль → `data-owner-only`; конфиг → инлайновый `display:none` у `#import`). Дип-линк `#tab-import` у владельца доводится в `loadImportCourts` после загрузки cases.json — на старте чип ещё скрыт.
- Скроллспай `IntersectionObserver` и `scroll-margin-top` удалены (дрались бы с `showTab` за класс `.active`).

Компоновка: липкая glass-шапка (лого · чипы-вкладки «Система/Импорт/LLM/Подписчики» · сводка · тоггл темы · Обновить) → **пульт кликабельных stat-плиток** (Последний прогон ok/сбой/идёт из gh-runs · Дайджест N изменений · Парсеры «все 22 ok»/«N ⚠» · Автозапуск + push-агрегат) — вне вкладок, всегда сверху. **У оператора плиток три:** «Дайджест» и «Автозапуск» — `data-owner-only` (дайджест — продукт юриста, а импорт диспатчит свой workflow сразу и крона не ждёт); колонки — правилом `html[data-role="operator"] .pult` строго внутри `@media (min-width:769px)`, иначе оно перебило бы двухколоночный телефон. Плитки ведут: «Последний прогон» → лог run в GitHub, «Дайджест» → дашборд, «Автозапуск» → страница workflow (там же «Run workflow» для полного обхода), «Парсеры»/«Импорты» → своя вкладка. Дальше — вкладки:
- **#system**: **полоса «Запуск прогона» во всю ширину** (`.run-bar`, вся `data-owner-only`) над сеткой карточек — после удаления «Полного прогона» карточкой в сетке она читалась обрубком (122px рядом с 367px «Здоровья»). Ниже — `.system-grid` на `repeat(auto-fit, minmax(320px,1fr))`: число видимых карточек переменное (у оператора одна, у владельца одна-две — «Иски банка» скрыта на 404), и фиксированные колонки при любом выборе давали дыру; `.system-grid > .card { min-width:0; max-width:700px }` — потолок ОБЩИЙ, не операторский (`:only-child` не сработает: скрытые соседи остаются в DOM). Правило `.system-grid{1fr}` из вилки 769–1024 удалено — именно оно делало страницу на 1000px длиннее, чем на 1280. Полоса содержит ОДНУ кнопку «▶ Запустить прогон» (`smart_skip:"true"`, ровно как ежедневный крон; в нерабочий день спрашивает «прогнать всё равно?» и добавляет `ignore_calendar:"true"` — признак дня приходит с сервера полем `today_non_working` в `/admin/gh-runs`, своей копии календаря у страницы нет) → POST `/admin/dispatch`, рядом метка следующего автозапуска. **Кнопка «Полный прогон» (`smart_skip:"false"`) удалена 02.08.2026** (решение юриста): тяжёлый обход всех дел запускается из GitHub Actions (Run workflow → снять галку `smart_skip`); белый список `DISPATCH_WORKFLOWS` не менялся. **Список последних 8 runs и блок живого лога УДАЛЕНЫ 29.07.2026** (решение юриста) — статусы/логи смотрятся на вкладке Actions GitHub; GET `/admin/gh-runs` (Worker проксирует GitHub API, PAT на сервере; отдаёт `next_cron_at` с учётом праздников и `today_non_working`) остался — им питаются плитки пульта «Последний прогон» (автообновление каждые 15 с пока прогон идёт) и «Автозапуск» | карточка «Здоровье парсеров» из [data/parse_health.json](data/parse_health.json): светофор-точки (красный fail_streak≥3/alerted_zero; жёлтый fail_streak≥1 или ноль при медиане≥1), спарклайны, проблемные вверху, первые 8 + свёрток; имена судов — карта `COURT_NAMES` (синхронизировать при правке `FIRST_INSTANCE_COURTS`) | карточка «Парсинг исков банка» (с 29.07.2026, обе роли) из `data/bank_parse_report.json`: пер-кейсовый итог последнего прогона по bank-треку — свёртка «По судам» первой (`bpCourtsFoldHtml`: строка на суд, проблемные сверху, раскрыта только при сбоях — иначе лежащий суд тонул бы среди сотен рутинных строк), дальше группы по исходам (ошибки загрузки/без карточки/вне очереди раскрыты; спарсено и пропуски по ритму ИЛ / будущим заседаниям свёрнуты), внутри группы порции по 30 строк (`BP_CHUNK`, «Показать ещё» — на Урале дел тысячи), `case_status` пилюлей только в группе «Спарсено» (в пропусках это данные прошлого прогона), русские причины считает Python (`skip_reason_ru`/`_OUTCOME_RU` в bank_report.py), **404 → карточка скрыта (территория без трека), прочие ошибки → блок «не загрузилось» с «Повторить»** (до 02.08.2026 пряталась при любом `!r.ok`, и 502 от Pages выглядел как отсутствие трека).
- **#llm**: топ-5 рейтинга shir-man (браузером напрямую, CORS `*`; с 02.08.2026 в свёртке с **ленивой** загрузкой — по раскрытию или при выборе провайдера openrouter: раньше внешний запрос уходил на каждый заход и каждое «Обновить») + мини-форма запуска `test_digest.yml` через POST `/admin/dispatch`: провайдер, модель (подписи «топ-N» обогащаются рейтингом), галки to_group/push_all/full_llm/commit_results (по умолчанию ВЫКЛ — безопасный прогон в личку; при опасных галках — confirm). У claude — выбор модели (haiku эталон / Sonnet 5 / Opus 4.8, `CLAUDE_MODEL` через input `claude_model`; кэш пересказов не-haiku неймспейсится по модели) и уровня усилий (`claude_effort` → env `CLAUDE_EFFORT` → `output_config.effort`; селектор виден только для sonnet/opus — haiku эффорт не поддерживает). ⚠ Модели нового поколения (Opus 4.7+/Sonnet 5) не принимают `temperature` (400) — пейлоад собирает `llm._claude_payload`: adaptive-мышление + effort вместо температуры, расширенный max_tokens и таймаут; боевой haiku-путь байт-в-байт прежний.
- **#subs**: счётчик + **поиск по подпискам** (имя/устройство/номера и стороны дел watchlist) + карточки. **С 02.08.2026 карточка — `<details>`, свёрнута по умолчанию** (8 подписок: ~430px вместо 1983px = 76% страницы). Свёрнутая строка: имя, устройство, ★ owner, «⏳ истекает», «⚠ N» сирот ПО ЭТОЙ подписке (`subOrphanCount`), справа «N дел» и бейдж варианта push. ⚠️ Класс `.sub-card` остаётся на самом `<details>`, `data-endpoint` не переезжает — на них завязаны `btn.closest(".sub-card")` и `flash()`. ⚠️ **В `<summary>` не должно быть кнопок** (клик по кнопке переключал бы свёртку, а `<button>` внутри `<summary>` — вложенный интерактив): пять действий переехали в тело. Состояние раскрытия — `subsOpen` вне DOM (`#root` перерисовывается на каждое нажатие в поиске и после `render(true)`); ⚠️ пишется по **клику на `summary`**, а НЕ по событию `toggle`: Chrome шлёт toggle и при парсинге `<details open>`, гард по таймеру ненадёжен (эти задачи дренируются позже `setTimeout(0)`), и авто-раскрытые поиском карточки записывались как «раскрытые вручную». Поиск раскрывает найденное только при выдаче ≤3 — иначе буква «а» развернула бы всех. В развёрнутом теле — kv-строка дат, свёртки «Последний push» (бейдж варианта; из [data/last_personal_pushes.json](data/last_personal_pushes.json); skip = «нет событий по watchlist») и «Дела» с бейджами стадий, сторонами и судом. Карта дел строится из cases.json **и cases_archive.json** (с 13.07): звезда на завершённом деле — бейдж «в архиве» (в модалке Watchlist такая строка видна с галкой, снять можно; при реактивации дела звезда оживает), номер-сирота (нет ни в активных, ни в архиве — дело удалено вручную или переименовано до Этапа 3) — бейдж «нигде не найдено» + крестик-удаление прямо в строке; счётчик «⚠ N нигде не найдено» — в сводке шапки И бейджем в заголовке секции (сводка шапки скрыта на ≤768px — с телефона сироты иначе не видны). Периодический read-only аудит — [scripts/audit_watchlists.py](scripts/audit_watchlists.py). Данные плитки «Дайджест» — из [data/last_digest.json](data/last_digest.json).

**Достоверность и состояния (02.08.2026).** Плитка «Дайджест» разбирает сводку по ИМЕНОВАННЫМ частям (`digestSummaryParts`: «Новых»/«Изменений»/«Переходов») и показывает оба числа — прежний `match(/\d+/)` брал ПЕРВОЕ число строки «🆕 Новых: 4 · 📋 Изменений: 6» и подписывал его словом «изменений», то есть каждое утро печатал число новых дел под чужой подписью. Клик по плиткам «Дайджест» и «Последний прогон» ведёт наружу (`data-href` + `DASHBOARD_URL`/`html_url` прогона), а не скроллит в `#system`, где о них нет ни строки; ссылка внутри `<button>` не используется — вложенный интерактив ловил бы клик дважды. Сбой загрузки везде отличим от «данных нет»: общий `loadErrorHtml` (человеческий текст + «Повторить», исключение — в `title`/`console`), плитка при сбое — серый «?» (не янтарь: он занят под «N парсеров ⚠»); `loadImportLog` при ошибке рисует её вместо вечного «Загрузка…». Вспышки-ошибки (`setFlash`) НЕ гаснут по таймеру — закрываются крестиком, иначе код сбоя («× endpoint мёртв (410)») исчезал через 5 с. Возврат на вкладку (`visibilitychange`) освежает статику Pages, если с последней загрузки прошло >10 мин (`loadStaticData` — health/bank/digest; `/admin/data` и `/admin/import-log` НЕ трогаем, KV); «Обновить» блокируется с `aria-busy` до `Promise.allSettled`. Журнал импортов с 10.08.2026 запрашивается на ВСЕХ территориях (история точечных добавлений нужна и без капчёвых судов — один `?logonly=1`-list на загрузку вкладки; до этого ХМАО не запрашивал его вовсе).

Действия по каждой подписке (5 кнопок):
- **✏ Имя** → POST `/admin/label` `{endpoint, label}`. Сохраняет произвольное имя («Иван», «iPhone Дани»).
- **Watchlist** → модалка с чекбоксами по активным делам из `cases.json` (поиск по номеру/сторонам/суду, бейджи стадий, ручное добавление номеров не из списка) → POST `/admin/watchlist` `{endpoint, watchlist}` (сервер канонизирует алиасы).
- **Тест push** → POST `/admin/test-push` `{endpoint}`. Требует Worker-секрет `VAPID_PRIVATE_KEY` (`wrangler secret put VAPID_PRIVATE_KEY`, тот же PEM, что в GitHub secret) — без него кнопка отдаёт понятную ошибку 503. Мёртвый endpoint (404/410) заодно вычищается из KV.
- **⧉ (иконка)** — копировать полный endpoint в буфер (в карточке он больше не светится).
- **Удалить** → POST `/admin/unsubscribe` `{endpoint}`. Принудительно убирает подписку из KV.

Все админ-эндпоинты авторизуются через `?secret=<OWNER_SECRET>` в URL (для удобства открытия из браузера); `<meta name="referrer" content="no-referrer">` — чтобы секрет не утекал по внешним ссылкам. POST `/admin/dispatch` принимает только workflow из белого списка `DISPATCH_WORKFLOWS` (worker.js) и только разрешённые inputs-строки.

Метаданные в KV: `created_at` (один раз), `last_seen_at` (на каждом `/subscribe`), `last_watchlist_update_at` (на `/watchlist`), `user_agent`, `label`. Старые подписки заполняют поля при следующем `/subscribe`.

Локальная отладка админки: `wrangler dev --config cloudflare-worker/wrangler.toml --port 8787` + `cloudflare-worker/.dev.vars` (gitignored) с `OWNER_SECRET=localtest123` → `http://localhost:8787/admin?secret=localtest123`. KV локальный пустой, GitHub API отвечает 401 (нет PAT) — это ожидаемо.

## Свежесть данных на фронте (SW, v127)

`data/*.json` идут в service worker'е по **stale-while-revalidate** (кэш
мгновенно, сеть в фоне) — иначе первый экран ждал бы 2 МБ `cases.json` и
1.4 МБ `cases_bank.json`. Цена: страница показывала снимок ПРЕДЫДУЩЕГО
прогона, а свежий появлялся лишь со следующего открытия. `fetch(url,
{cache:'no-cache'})` в app.js от этого не спасает — это директива HTTP-кэша,
SW её не касается. Инцидент 03.08.2026: дело 2-592/2025 висело в активных,
хотя утренний прогон увёл его в архив трека, при этом блок дайджеста рядом
был сегодняшним (`last_digest.json` — network-first), а шапка писала
«Обновлено: <время рендера>» — отличить вчерашний снимок было нечем.

Как сейчас: `staleWhileRevalidate` сравнивает `ETag` (фолбэк `Last-Modified`)
кэша и сети и при расхождении шлёт окнам `postMessage({type:'data-updated'})`
(первая загрузка — молча, там и так свежее). В app.js `onDataUpdated` →
`dataFileKind` (main/bank) → дебаунс 400 мс → `applyPendingDataRefresh`:
`loadFromSheet(url,{quiet:true})` и/или `reloadBankDataset()`, повторный fetch
попадает в уже свежий кэш (мгновенно, без сети), в конце — тост. Пока открыт
drawer/шторка фильтров/beacon (`uiBusyForRefresh`) — откладываем, набор не
теряется, `closeDrawer`/`closeFiltersSheet`/`closeDigestBeacon` зовут проход
снова. Шапка показывает «Данные от: …» — `updated_at` файла; `parseIsoUtc`
дочитывает «Z» к naive-ISO (Python на UTC-раннере пишет без него, иначе ХМАО
увидел бы прогон на 5 часов раньше). Стражи —
[scripts/tests/test_frontend_freshness.py](scripts/tests/test_frontend_freshness.py).

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
