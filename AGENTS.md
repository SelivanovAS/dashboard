# AGENTS.md

Карта проекта для новых сессий — чтобы не тратить токены на разведку.

> 📚 **Полная техническая документация** — [docs/technical/README.md](docs/technical/README.md).
> Этот файл — быстрая карта (где что в коде); `docs/technical/` — глубокий
> справочник «как всё работает» (архитектура, модель данных, жизненный цикл,
> парсеры, конвейер, дайджест, доставка, фронтенд, worker, эксплуатация).

## Что это

Дашборд юриста ПАО Сбербанк: мониторинг гражданских дел в 20 судах ХМАО-Югры (первая инстанция) + апелляция (Суд ХМАО-Югры) + кассация (7-й кассационный суд общей юрисдикции, фильтр по 1-й инст. ХМАО). AI-дайджесты в Telegram, автозапуск через Cloudflare Worker cron → GitHub Actions. Пользователь — юрист банка, общение на русском.

**С 15.07.2026 система регионализована** (тиражирование на территории Уральского банка, этап 1 — Свердловская обл.+ЯНАО): регион = конфиг в `scripts/court_monitor/regions/` (выбор — env `REGION`, дефолт hmao), территория = форк, отличающийся только Variables/секретами и тремя «файлами территории» (`region_front.js` — обязательный `STORAGE_NS` (неймспейс localStorage: фронты на одном origin github.io, без него звёзды/заметки перемешиваются между территориями; эталон NS не задаёт), `manifest.json`, `wrangler.toml`). Апелляций в регионе может быть НЕСКОЛЬКО (`APPEAL_COURTS`, `appeal.court_domain` в JSON, составной ключ связки). **Иск, не принятый к производству, не заводится (14.08.2026):** строка «банк-ответчик» с итогом возврата / отказа в принятии / передачи по подсудности отсеивается гейтом `fi_not_accepted_kind` (lifecycle.py) ДО проверки ссылки и до `_fetch_main_card` — маркер `[NOT ACCEPTED]`, счётчик `not_accepted` (до оператора едет теми же тремя звеньями). Фильтр итогов был только у трека исков БАНКА (`_EXCLUDED_RESULT_RX`), и возврат любой давности заводился активным, объявлялся «новым иском» и 60 дней занимал картотеку, каждый прогон качая карточку (19 дел Урала, 6 ХМАО — разбор юриста). Класс узкий: прекращение и «оставлено без рассмотрения» заводятся по-прежнему (производство было, по частной жалобе дело оживает под ТЕМ ЖЕ номером), присоединение — тоже (дело живёт под номером приёмника). Зеркало гейта — блок 3 `main_json` (отсев ДО `_discovered_already_resolved_old`, стережёт `TestNotAcceptedIntakeWiring`); точечное добавление из админки гейта НЕ имеет осознанно — номер туда вводит юрист вручную. Уже заведённые 25 дел убраны разово. Подробно — [docs/Тиражирование_регионы.md](docs/Тиражирование_регионы.md).

## Главные файлы

- [scripts/update_cases.py](scripts/update_cases.py) — **тонкий фасад CLI** (~220 строк): разбор argv + ре-экспорт прежних имён. Весь код — в пакете `scripts/court_monitor/` (распил монолита, см. [docs/Распил_монолита_контекст.md](docs/Распил_монолита_контекст.md)).
- `scripts/court_monitor/` — **пакет модулей** (читать только нужный):
  - [config.py](scripts/court_monitor/config.py) — env-константы, пути данных, окна state-machine, `log` (пишет в **stdout**), `METRICS`. Патчабельные константы код читает ТОЛЬКО как `config.X` — тесты патчат `monkeypatch.setattr(config, ...)`.
  - [ghlog.py](scripts/court_monitor/ghlog.py) — GitHub Actions: сворачиваемые группы фаз (`::group::`) и аннотации `::warning::`/`::error::`. Включается только env `LOG_GH_ANNOTATIONS=1` (ставят боевые workflow; pytest в CI не должен плодить аннотации), без него всё no-op.
  - [textutil.py](scripts/court_monitor/textutil.py) — даты, HTML-очистка, экранирование, сокращение имён сторон/судов, производственный календарь. **Сокращатель сторон с 09.08.2026** (разбор дайджеста 07.08): МТУ Росимущества заменяется ПОДСТАНОВКОЙ `_MTU_FULL_RE.sub` до сплита по запятым (прежний ранний `return` молча съедал ВСЕХ соответчиков — 33-5577/2026, 2-2630/2026; региональный хвост с внутренними запятыми матчится белым списком регион-токенов, покрыты именительный «Росимуществ**о**» и «МТУ ФА по управлению…»); ФИО-слоты имени/отчества — с заглавной + русские окончания отчества (гард: «Уральский банк ПАО Сбербанк» сворачивался в «Уральский Б.С.»), КАПС-ФИО нормализуются (`_decapitalize_fio`: «ЗАЛАН ЖАННА ГЕННАДЬЕВНА» → «Залан Ж.Г.»); наследственные формулировки всех видов → «насл. имущество Фамилия И.О. (дата смерти …)» (`_shorten_heritage`); пустые скобки после срезанной ОПФ чистятся («Банк ВТБ ()»); точные повторы сторон печатаются один раз (решение юриста — суд в объединённых делах перечисляет стороны по каждому иску; однофамильцы разведены `_resolve_initial_collisions` и не схлопываются).
  - [netutil.py](scripts/court_monitor/netutil.py) — `session`, `fetch_page` (win-1251; **одна попытка по умолчанию** с 26.07.2026 — пропуск безопасен, всё перечитывается следующим прогоном; env `FETCH_MAX_RETRIES` возвращает ретраи ручным пробам/импортам в их workflow; `context=` — номер дела/суд в WARNING/ERROR), `fetch_card_checked` (карточки/тексты актов: детект проверочного кода → WARNING + `METRICS["cards_captcha"]` + пропуск; карточный детектор строже поискового — фразы из СМС-цитат актов о мошенничестве не матчит; с 20.07.2026 — детект заглушки/блока `looks_like_non_card_page` (аутейдж sudrf «Информация временно недоступна» отдавал HTTP 200 без таблиц и молча засчитывался успешной проверкой) → `METRICS["cards_blocked"]` + 🩺-алерт + пропуск; второй рубеж в FI-цикле — `card_is_empty_shell`: 0 таблиц не бумпает `last_checked_at`; с 29.07.2026 — **пер-суд предохранитель** (аутейдж Сургутского: заглушка на каждой карточке, прогон впустую молотил весь суд): `CARD_BREAKER_THRESHOLD`=3 не прочитанных карточек ПОДРЯД одного хоста (заглушка/код/сеть) → суд снят с обхода до конца прогона — гейт `card_breaker_allows` пропускает без HTTP (пре-чеки в FI-цикле и `update_active_cases` стоят ДО `polite_delay`, fetch после них — с `breaker_gate=False`, иначе двойной гейт ломает каденс проб), канарейка `card_breaker_preopen` пре-открывает по заглушке на странице ПОИСКА (`looks_like_outage_page`; капча НЕ пре-открывает — штатный режим `search_gated`), half-open проба каждые `CARD_BREAKER_PROBE_EVERY`=30 пропущенных возвращает ожившего в обход; состояние `config.CARD_BREAKER` живёт один прогон (сброс в `_metrics_reset`), 🩺-алерт по судам в 4e (`_card_breaker_alert_lines`), исход `court_breaker` в отчёте bank-трека + группа в админке), `polite_delay`.
  - **Уточнение breaker с 25.08.2026 (заменяет count-описание выше для полного прогона):** точные классы формируют `transport_fail_kind` и `classify_outage_page`/`classify_non_card_page`; policy не классифицирует заново. `CARD_BREAKER_MODE=time` явно включён в `update_cases.yml` и Mac `parse_and_push.sh`: fast 3/60с, portal outage 2/180с, slow 2/300с, WAF/403/CAPTCHA-card 2/600с; parser-quality breaker не открывает, разные families не складываются. `DeferredCardQueue` откладывает хост, продолжает другие суды, после срока даёт ровно одну half-open пробу и без `sleep` дочитывает хвост при успехе; первая неудачная проба исчерпывает бюджет хоста в этой фазе, поэтому новый cooldown не запускает круг повторных проб. Новая фаза получает отдельный бюджет. Импорты явно сохраняют `count` 5/3. `FETCH_MAX_RETRIES` не повышен. Checkpoint/`last_run.breaker` несут kind/cooldown/probes/deferred; `cards_breaker_unrequested` — только финально оставшиеся без HTTP, а не все временные срабатывания гейта.
  - [regions/](scripts/court_monitor/regions/__init__.py) — **регионы-конфиги**: `base.py` (типы `CourtConfig`/`RegionConfig`), `hmao.py` (реестры ХМАО), `get_region()` (env `REGION` → `config.REGION`, ленивый importlib). Новая территория = новый модуль здесь, форк задаёт только `REGION`.
  - [courts.py](scripts/court_monitor/courts.py) — **фасад активного региона**: ре-экспорт `APPEAL_COURTS`/`APPEAL_COURT`/`FIRST_INSTANCE_COURTS`/`CASSATION_COURT`, матчер `match_region_first_instance` (`match_hmao_first_instance` — legacy-обёртка), `appeal_court_by_domain`, URL карточек.
  - [storage.py](scripts/court_monitor/storage.py) — cases.json/CSV, `.digested_acts`, `.cassation_acts`, кэш пересказов; split-хранение bank-трека (`load_bank_json`/`save_bank_json`, ключ `bank_events_key` «домен|номер»).
  - [health.py](scripts/court_monitor/health.py) — журнал здоровья парсеров + детектор молчаливой поломки.
  - [telemetry.py](scripts/court_monitor/telemetry.py) — атомарный дневной checkpoint Mac: все попытки текущей даты, planned/read множества стабильных `домен|номер` по инстанциям, HTTP/semantic агрегаты по хостам, breaker recovery и сетевые отпечатки. Новый день очищает историю; лимита «три запуска» нет.
  - [lifecycle.py](scripts/court_monitor/lifecycle.py) — классификация событий карточки, state machine стадий, дедуп, архив.
  - [parsing/](scripts/court_monitor/parsing/__init__.py) — `tables.py` (TableExtractor), `search.py` (поисковая выдача), `cards.py` (карточки дел), `cassation.py` (7kas).
  - [linking.py](scripts/court_monitor/linking.py) — связка FI ↔ апелляция ↔ кассация, discovery, реактивация, ротация архива.
  - [digest/](scripts/court_monitor/digest/__init__.py) — `llm.py` (Codex/GigaChat/OpenRouter — выбор через `LLM_PROVIDER`; промпты — патч-цели тестов живут тут), `postprocess.py` (валидация/чистка HTML), `template.py` (программный рендер — **боевой путь с 03.07.2026**, компакт-вёрстка без отступов), `core.py` (диспетчер `generate_digest`), `lint.py` (программный линтер готового HTML после отправки: полнота номеров, счётчики (N), теги, футер → 🩺-алерт; `DIGEST_LINT=0` — выключатель). Рядом с линтером (и ТОЛЬКО там — блок 4e идёт ДО генерации дайджеста, счётчик в нём всегда 0) с 02.08.2026 стоит `_alert_llm_summary_failures`: при `METRICS["llm_summary_failed"] > 0` шлёт 🩺-алерт «пересказы актов» — иначе отказ провайдера (429 free-пула OpenRouter, 17.07.2026: в дайджест уходила сырая мотивировка) был виден только в логе прогона. Зовётся с боевого пути `main_json`, не с replay — `test_digest.yml` гоняет его для экспериментов. Прод — гибрид: события рендерит код, LLM только пересказывает мотивировки актов; `DIGEST_FULL_LLM=1` — откат на полный LLM-дайджест.
  - [delivery.py](scripts/court_monitor/delivery.py) — Telegram, Web Push с watchlist-персонализацией, алерты.
  - [runs.py](scripts/court_monitor/runs.py) — `main_json` и остальные режимы прогона, `update_active_cases`. **С 12.08.2026 порядок инстанций в прогоне: кассация → апелляция → 1-я инстанция** (решение юриста: важные инстанции первыми — при падении прогона посередине они уже проверены). Исторические баннеры блоков (`2.`/`3.`/`3b`/`4a`–`4e`) сохранены, видимую нумерацию задаёт `log_phase(N/9)`; таблица «баннер → фаза» — [docs/technical/05-конвейер-обновления.md](docs/technical/05-конвейер-обновления.md). Инварианты: поиск инстанции раньше её карточек (канарейка предохранителя); `dedupe_cassation_by_uid` — после карточек апелляции (УИД дозаполняется с апел. карточки); discovery-id кассации дозаписываются в `existing_ids`/`fi_dedup_*` до поиска 1-й инст.
- [scripts/add_cases_manually.py](scripts/add_cases_manually.py) — ручное добавление дел 1-й инстанции.
- [scripts/import_search_dump.py](scripts/import_search_dump.py) — **офлайн-импортёр дампов выдачи капчёвых судов** (Свердловская обл.: 54 записи реестра со `search_gated=True` — автопоиск выключен, карточки мониторятся). Оператор решает капчу → вставляет дамп в секцию «Импорт дел» админки → Worker кладёт в KV + диспатчит [import_cases.yml](.github/workflows/import_cases.yml) → импортёр (utf-8→win-1251, нормализация pretty-print, дедуп `collect_fi_dedup_index` по ВСЕМ картотекам, `srv_num` из href; **«банк-ответчик» → cases.json**, как в автопоиске, третье лицо — `[SKIPPED ROLE]`; **с 13.08.2026 «банк-истец» → трек «Иски банка»** (разгон Урала: правила общие с авто-подхватом — `row_passes`/`card_rejects(skip_appeal=False)`/`make_bank_entry(source="dump")`/`entry_is_spent` + негативный кэш, но no_link в кэш НЕ пишется: ссылку теряет вставка «как текст», а не выдача; по ссылке дампа качается карточка — единственный онлайн-шаг, кэп `MAX_BANK_CARDS_PER_IMPORT`=100, маркеры `[ADDED BANK]`/`[SPENT]`/`[SEEN]`/`[BANK CAPPED]`, в дайджест не анонсируется; при `BANK_TRACK=0` — прежний `[SKIPPED ROLE]`, решение 16.07.2026 принималось до появления трека); промоушен М→2 при комбо-номере — `[PROMOTED]`, зеркало main_json) → коммит cases.json → итог назад в админку (`/import-result`, журнал `import:log:*`). **Защита «дамп ↔ выбранный суд» (17.07.2026):** хосты абсолютных href карточек (`name=sud_delo`) + маркер Chrome «saved from url» сверяются с судом импорта на трёх уровнях — автоопределение суда в админке (`impDetectDomains`/`impRunDetect`, подставляет суд сам, ручной выбор не перебивает), 400 Worker'а (`detectDumpSudrfHosts`), `EXIT_WRONG_COURT=5` импортёра (`detect_dump_hosts`); `delo_id` из href карточек ловит выдачу не того раздела (суды 1-й инст. он не различает — у всех 1540005). Относительные href (файл Firefox) хостов не несут — проверки молчат. Дела получают служебный блок `"import": {operator, at, source, announced}`; ближайший прогон объявляет их «новыми исками» в дайджесте/пуше один раз (`announce_imported_cases`, runs.py). **С 14.08.2026 импортёр перестал быть офлайновым и для «банк-ответчик»**: карточка читается и по этим строкам (`_fetch_main_card`, наложение `build_json_entry` поверх card-blind записи — `delo_id`/`srv_num` из href и `act_text` уцелевают), запись получает штамп `last_checked_at` + маркер `intake_card_parse` (`_stamp_intake_checked`, зеркало `make_bank_entry`; пара неразделима — один штамп навсегда выключил бы `first_card_parse`). Кэп карточек ОБЩИЙ с истцовой веткой, dry-run офлайновый, а сбой загрузки строку НЕ роняет (в отличие от `[FETCH FAIL]` истцовой ветки): иск ПРОТИВ банка заводится card-blind с пометкой «карточка недоступна» и дозаполняется прогоном. ⚠️ Побочный эффект, закрытый в рендере: у заполненной записи прогону не с чем сравнивать диффом, и «заседание назначено»/«решение вынесено» отдельными событиями уже не объявятся — поэтому секция «Новые иски» печатает дату заседания и итог прямо в строке дела (`generate_template_digest`, стражи в tests/test_digest_template_events.py). **Сводка импорта считает ОБА трека (14.08.2026):** Python считал все 14 счётчиков, а до оператора доходили 6 — импорт, заведший 4 иска банка и отсеявший 5, показывал «+1 добавлено». Потери были в трёх независимых местах: jq-пейлоад `import_cases.yml`, числовой whitelist `handleImportResult` в worker.js и `impResultText` в admin_page.js (образец правильной сборки — `acResultText` рядом, он суммировал оба трека с самого начала). Светофор свежести «+N из M» тоже считает оба (`added_bank` в вечном ключе `import:last:<домен>`). Сквозную проводку стережёт `test_bank_counters_reach_operator`. **Провал чтения карточек больше не молчит (16.08.2026):** портал Ленинского р/с ЕКБ два импорта подряд отдавал «Этот запрос заблокирован по соображениям безопасности» с HTTP 200 (ловится `looks_like_non_card_page` по нулю таблиц), 5 дел завелись пустышками — а сводка админки писала «+4 в картотеку», признак жил только строкой в свёртке «Отчёт построчно». `_fetch_main_card` возвращает теперь ПАРУ (карточка, причина): «не пробовали» (dry-run) отделено от «пробовали и не вышло», иначе счётчик не из чего строить. Счётчик `card_failed` («failed» — один факт «карточки нет» на причины С ЗАПРОСОМ: отказ, заглушка, открытый предохранитель суда; кэп — отдельный исход `capped`, в счётчик не входит) идёт до оператора теми же тремя звеньями, что и счётчики трека, + WARNING отдельной строкой в логе прогона. Второе следствие инцидента: починить было нечем — повторная вставка того же дампа отвечала `[ALREADY]` и карточку не читала, а крон ходит пн-пт (дамп выходного дня стоял пустым до понедельника). Теперь ветка `[ALREADY]` дочитывает **card-blind** запись (`_card_blind_case`: ни `last_checked_at`, ни `intake_card_parse`, пустые `events`) общим телом `_apply_main_card` → `[REFILLED]`, счётчик `refilled`. ⚠️ Запись ищется ТОЧНЫМ ключом (домен, номер) в активных: `is_fi_number_tracked` матчит и архивы, и wildcard комбо-номеров; гейт стадии `first_instance` — у дела, уехавшего в апелляцию, наложение перетёрло бы статус строкой дампа; `refilled_any` ОБЯЗАН входить в условие сохранения cases.json, иначе дочитанная карточка живёт только в памяти процесса. Проводку стережёт `test_card_counters_reach_operator`, поведение — `TestCardBlindRefill`. **Отчёт называет ПРИЧИНУ отказа (16.08.2026, тот же день):** 403, страница защиты ГАС «Правосудие» с HTTP 200, проверочный код, заглушка портала и таймаут давали одну строку «карточка не прочиталась», и «нас блокируют по адресу» было неотличимо от «портал лёг». Класс ответа пишет `config.FETCH_DIAG` (заполняют `fetch_page` — единственное место, где вообще виден HTTP-код: наружу `raise_for_status` отдаёт только исключение, — и `fetch_card_checked`: `captcha`/`blocked`/`breaker`), формулировку даёт `netutil.fetch_fail_reason_ru`, а со страницы защиты `block_page_marks` снимает НАШ АДРЕС и букву правила («… (G) : ip: 43.245.226.66 … Australia») — он и объясняет, почему тот же URL с машины юриста открывается. Причина едет построчно и одной строкой `card_fail_reason` в сводке (строка, а не счётчик: числовой whitelist Worker'а её срезал бы — проводку стережёт `test_card_fail_reason_reaches_operator`). ⚠️ Диагноз перезаписывается КАЖДЫМ запросом — читать сразу после отказа; исход `capped` (кэп карточек) отделён от `failed` намеренно: запроса не было, и `FETCH_DIAG` держит диагноз ЧУЖОЙ карточки. ⚠️ У иска банка отказ означает потерю дела ЦЕЛИКОМ (правила приёма в трек решаются только по карточке) — строка `[FETCH FAIL]` обязана это говорить: 16.08 так молча пропало 11 исков Верх-Исетского. **Предохранитель не перебивает причину (16.08.2026, тот же день):** сводка берёт самую частую причину, а пер-суд предохранитель после трёх отказов подряд пропускает остальные карточки БЕЗ запроса — на дампе из 10 истцовых строк это 7 «суд снят с обхода» против 3 настоящих, и оператор читал наше следствие вместо «нас блокируют по адресу». `_note_card_failure` копит ПАРЫ (класс, текст), `_top_card_fail_reason` считает большинство только по причинам С ЗАПРОСОМ, а пропуски дописывает хвостом «ещё N карточек не запрашивали»; одни пропуски (канарейка открыла предохранитель до первой карточки) остаются всем ответом. Стражи — `TestCardBlindRefill.test_breaker_does_not_mask_the_cause` и `…_breaker_alone_stays_the_answer`. **Разбор боевой сводки 16.08.2026** (дамп Верх-Исетского: +0 в картотеку, 12 исков банка потеряно) — три правки. (1) Потеря исков банка называется ПОТЕРЕЙ: «⛔ N исков банка НЕ заведено (карточка не открылась) — повторите дамп, когда суд отвечает» вместо прежнего «N карточка не открылась», звучавшего технической мелочью; правила приёма в трек решаются только по карточке, и вернуть строку может лишь повторный дамп. (2) **Светофор свежести не бумпается**, если карточки не читались (`cardsUnread = fetch_fail + card_failed` в `handleImportResult`): импорт, заведший НОЛЬ, красил суд зелёным «импортирован сегодня», и через неделю оператор к нему не вернулся бы, а 12 исков остались бы ненайденными. (3) **Предохранитель настроен под размер ДАМПА** — `CARD_BREAKER_THRESHOLD=5` + `CARD_BREAKER_PROBE_EVERY=3` в import_cases.yml: дефолты (3 и 30) считаны на боевой обход сотен карточек по десяткам судов, и «проба каждые 30 пропущенных» в дампе на 25 строк означает «никогда» — восстановиться внутри одного импорта нельзя в принципе. При ТОТАЛЬНОМ блоке разницы нет (дела не завелись бы всё равно), но блок бывает мигающим — замер с машины юриста: один запрос отбит, следующие восемь прошли. Стражи — `test_lost_bank_claims_are_named_a_loss`, `test_freshness_not_bumped_when_cards_unread`, `test_breaker_tuned_for_dump_size`. **Выравнивание веток (18.08.2026, разбор юриста «истцовая ветка лучше»):** у «банк-ответчик» появились карточные фильтры приёма и общая память об отказниках — зеркало истцовой ветки; card-blind заведение при недоступной карточке СОХРАНЕНО (решение юриста: иск против банка терять нельзя, дочитка тремя каналами — повтор дампа, очередь Mac, ближайший прогон). (1) **Второй рубеж not_accepted — по карточке** (`fi_not_accepted_kind(card_info["Результат"])` после `_fetch_main_card`): выдача отстаёт от карточки, и возврат бывает виден только в ней; отказ пишется в ОБЩИЙ негативный кэш `.bank_intake_seen.json` (причина `not_accepted` добавлена в `PERMANENT_REJECTIONS`), повтор дампа отвечает `[SEEN]` без HTTP. ⚠️ У ответчик-`[SEEN]` `last_seen` НЕ бампается осознанно — TTL-прунинг (60 дн) отпускает строку перечитать карточку: возврат, отменённый БЕЗ следа в выдаче, иначе глушился бы вечно (истцовый `[SEEN]` теперь бампает, как авто-подхват); самоочистка — непустой НЕтерминальный итог в строке выдачи снимает запись из кэша сразу (дело ожило и дошло до решения). (2) **Давно решённые — тихо сразу в архив, а не отказ** (`discovered_already_resolved_old` вынесен из runs.py в lifecycle.py — правило ОДНО с блоком 3 main_json): строчный гейт заводит БЕЗ карточки (кэп бережём; прогон дочитает раз и заархивирует с полной историей) с якорем `hearing_date`=result_date и `announced=True`, карточное зеркало `_card_resolved_old` — боевой `is_case_archived`, но флаги жалобы смотрит в МАРКЕРАХ карточки (`build_json_entry` их не переносит, дело с жалобой обязано заводиться живым); маркер `[ADDED OLD]`, счётчик `resolved_old` (в `added` НЕ входит, проводка теми же тремя звеньями, страж `test_resolved_old_reaches_operator`). (3) **«Суд не в реестре» = честный `no_court`** (раньше маппился в `capped` и обещал дочитку): считается в `card_failed`, причина своя МИМО FETCH_DIAG (`_note_no_court`) — дыра в реестре обязана мозолить глаза (побочка: ненулевой `card_failed` заставит очередь Mac ретраить дамп — осознанно). (4) **Кросс-трековый `[REFILLED]` сохраняет правильный файл:** `case_owner` (индекс с 18.08 несёт обе картотеки) решает cases.json vs cases_bank.json — флаг `refilled_bank_any` ОБЯЗАН входить в условие сохранения трека, хвост строки «(иски банка)»; дочитанное давно решённое дело получает `announced=True` тем же заходом. Корзина «уже в треке» сводки админки разведена на «отработавших (иски банка)» и «из кэша отказов». Стражи — `TestDefendantCardGates`, `test_bank_record_refill_saves_bank_file`; даты фикстуры not_accepted пересажены на `days_ago` (статичные через 60 дн уехали бы в `[ADDED OLD]` и уронили `test_class_border_still_imported`). Подробно — [docs/Тиражирование_регионы.md](docs/Тиражирование_регионы.md).
- [scripts/add_cases_targeted.py](scripts/add_cases_targeted.py) + [scripts/court_monitor/targeted_add.py](scripts/court_monitor/targeted_add.py) + [.github/workflows/add_cases.yml](.github/workflows/add_cases.yml) — **точечное добавление дел из админки** (с 10.08.2026, блок «Добавить дела» на вкладке «Импорт», обе роли, вкладка теперь видна и на ХМАО): до 20 строк за отправку — номер дела ИЛИ ссылка на карточку sudrf (для капчёвых судов ссылка — единственный путь: код закрывает поиск, карточки открыты). Worker кладёт пачку в KV `import:case:<uuid>` (строки не касаются shell и не упираются в лимит 100 символов у inputs) → `add_cases.yml` (та же concurrency-группа `cases-data-write`; таймаут 45 мин и `FETCH_MAX_RETRIES=1` — до 20 номеров × все открытые суды, а лежащий суд с ретраями взрывал бы худший случай за пределы таймаута; ⚠️ у GitHub-группы ЖИВЁТ ОДИН pending: пачка, вставшая в очередь второй, молча отменяется до первого шага (журнал навсегда «отправлено») — повтор пачки безопасен, уже добавленное отсеет дедуп; `done` в журнале — только при успешном push (упавший коммит перекрашивает итог в failed с подсказкой повторить)) → пер-строчно: номер → целевой поиск по всем `courts_for_search` (0 совпадений → отказ с подсказкой про ссылку; >1 суда → «выберите суд в форме», селект действует на всю пачку); ссылка → резолв по (домен, srv_num) через `fi_court_by_domain` с точными отказами (апелляция/кассация/чужой регион/чужой delo_id — клиентское зеркало в админке валидирует ДО отправки по `region.*` cases.json, у `fi_courts` для этого появился `delo_id`). Дальше: промоушен М→2 (`promote_material_record`, общий с дамповым импортёром) → дедуп по ВСЕМ картотекам (активные+горячие+холодные архивы обеих, ключ (домен, номер); активные проверяются раньше архивов — анти-клон) → находка в архиве = **реактивация с полной историей** (`reactivate_from_archive`: архив-источник обязательно пересохраняется, bank-пары — только через `load_bank_json`, `archived_count` пересчитывается, `import.announced=True` — не «новый иск») → свободное дело: роль по УЧАСТНИКАМ карточки (`bank_role_from_participants`; Сбер не найден/дочка → отказ со сторонами), Ответчик/Третье лицо → cases.json (`_fi_search_to_json_case`, `import` без `announced` → объявится «новым иском» ближайшим прогоном), Истец → bank-трек (`make_bank_entry`, тихо; гейты `card_rejects(skip_appeal=False)`+`entry_is_spent`). Отказ строки НЕ валит пачку; сохранение файлов один раз; отчёт → `/import-result` с `job_key` → общий журнал импортов (`kind:"case"`; светофор свежести дампов НЕ бумпается). Коды выхода: 0 — пачка обработана (даже все отказы), 4 — тотальный сетевой сбой, 5 — job нечитаем. Тесты — [scripts/tests/test_add_cases_targeted.py](scripts/tests/test_add_cases_targeted.py) (включая `TestWiring` по workflow/worker/админке).
- [scripts/build_region_registry.py](scripts/build_region_registry.py) + [.github/workflows/probe_region_registry.yml](.github/workflows/probe_region_registry.yml) — проба реестра территории с GitHub-раннера (delo_id + классификация капчи; вход `ops/region_probe/courts_probe.csv`, отчёт коммитится в `ops/region_probe/report.txt`). **С 13.08.2026 — второй режим `--scan-servers`** (галка scan_servers в workflow, env REGION): разведка судебных присутствий — 1 GET страницы sud_delo на каждый домен 1-й инст. региона, разбор селектора площадок (`parse_server_options`: ссылки с srv_num=, фолбэк union), сверка с конфигом (`compare_servers`: «⚠ НОВАЯ ПЛОЩАДКА» + готовая строка CourtConfig, search_gated наследуется от домена), отчёт → `ops/region_probe/servers_report.txt`; обычная CSV-проба площадок не видит — ходит только на сервер 1. **С 14.08.2026 — классификация площадок** (`classify_server_label` → `SRV_CIVIL`/`SRV_OTHER`/`SRV_UNKNOWN`, второй слой `classify_server` по разделам страницы площадки через `survey_delo_ids(domain, srv_num)`): первый прогон на Урале нашёл 4 вторые площадки, и ВСЕ оказались картотеками уголовного судопроизводства (юрист их отверг) — уголовные больше не предлагаются в конфиг, у неопознанной подписи строка выдаётся с пометкой «проверить глазами». ⚠️ Классификация по ПОДПИСИ, а не по номеру: у Железнодорожного р/с ЕКБ гражданская картотека живёт на srv 2, уголовная на srv 1. Подписи печатаются для ВСЕХ площадок, включая сконфигурированные, + флаг «⚠ В КОНФИГЕ НЕ ГРАЖДАНСКАЯ ПЛОЩАДКА» — иначе уголовная картотека, заведённая в реестр вслепую, невидима. Путь самого workflow убран из `push.paths` (merge правки в форк запускал пробу на территории). Тесты — [scripts/tests/test_region_probe_servers.py](scripts/tests/test_region_probe_servers.py).
- **Полнота реестра судов территории** проверяется ВРУЧНУЮ (решение юриста 14.08.2026 — автоматику не строим): перечень судов субъекта берётся с портала ГАС «Правосудие» `https://sudrf.ru/index.php?id=300&act=go_search&searchtype=fs&court_subj=<код>&court_type=RS` (коды: 66 Свердловская обл., 89 ЯНАО, 86 ХМАО) и сверяется с `get_region().first_instance_courts` ПО ДОМЕНУ, не по названию. `--scan-servers` этого не заменяет — она ходит только по доменам конфига и целиком пропущенный суд не увидит. Так 14.08.2026 нашёлся незаведённый Приуральский районный суд ЯНАО (13-й суд округа).
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
- [.github/workflows/test_digest.yml](.github/workflows/test_digest.yml) — единый ручной тест: replay последнего дайджеста, Telegram (личный/группа по галке), PWA push (владельцу/всем по галке), выбор LLM-провайдера (`llm_provider`: Codex/gigachat/openrouter) и модели: списки `gigachat_model` (GigaChat-2-Pro/2/2-Max) и `openrouter_model` (место в рейтинге shir-man: «модель дня (топ-1)»…«топ-5», id резолвится на прогоне — список не протухает), текстовое `llm_model` перебивает оба. Публикация результатов (`last_digest.json`, `cases.json`, кэш пересказов) и PWA push — только по галке `commit_results` (по умолчанию ВЫКЛ: тестовый дайджест не попадает на дашборд, пуш не уходит — он вёл бы на старый дайджест; без галки прогон шлёт только Telegram).
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
| `repair_vacated_default_judgments` (ремонт: откат решения + возврат в трек) | [scripts/court_monitor/lifecycle.py:1898](scripts/court_monitor/lifecycle.py:1898) |
| `intake_bank_rows` (блок 3b: приём исков банка с выдачи в прогоне) | [scripts/court_monitor/runs.py:1777](scripts/court_monitor/runs.py:1777) |
| `card_rejects` (карточные правила приёма; флаг skip_appeal — ручные каналы vs прогон) | [scripts/court_monitor/bank_intake.py:57](scripts/court_monitor/bank_intake.py:57) |
| `row_passes` (правила приёма по строке выдачи) | [scripts/court_monitor/bank_intake.py:49](scripts/court_monitor/bank_intake.py:49) |
| `make_bank_entry` (сборка записи трека: маркеры, ИЛ, флаги жалобы, delo_id/srv_num) | [scripts/court_monitor/bank_intake.py:193](scripts/court_monitor/bank_intake.py:193) |
| `_stamp_appeal_flags` (флаги жалобы + ДВИЖЕНИЕ жалобы + апеллянт из карточки в запись) | [scripts/court_monitor/bank_intake.py:280](scripts/court_monitor/bank_intake.py:280) |
| `appeal_objections_deadline` / `stamp_objections_deadline` (срок возражений из движения жалобы) | [scripts/court_monitor/lifecycle.py:1070](scripts/court_monitor/lifecycle.py:1070) |
| `apply_fi_appellant` / `appellant_is_bank` (апеллянт из карточки 1-й инст.; ре-экспорт `_apply_fi_appellant`/`_appellant_is_bank` в runs.py; **именной податель — «банк» ТОЛЬКО для самого ПАО Сбербанк**: дочки (страхование/НПФ/лизинг) отсеиваются `config.name_is_real_sberbank` с 09.08.2026 — 🏦 в кассации вставал на жалобу ООО «Сбербанк страхование жизни», кейс 8Г-11469/2026; та же проверка в `_cassation_card_to_block` linking.py; сохранённые True у дочек понижает тихая миграция `reclassify_named_appellants_is_bank`) | [scripts/court_monitor/runs.py:1720](scripts/court_monitor/runs.py:1720) |
| `bank_track_pending` (гейт раскладки 7c — по данным, не по счётчику загрузки) | [scripts/court_monitor/runs.py:1886](scripts/court_monitor/runs.py:1886) |
| `fi_not_accepted_kind` (иск к производству не принят: возврат / отказ в принятии / передача по подсудности — каналы приёма такое дело не заводят) | [scripts/court_monitor/lifecycle.py:441](scripts/court_monitor/lifecycle.py:441) |
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
| `migrate_appeal_court_fields` (бэкфилл суда в блоках appeal) | [scripts/court_monitor/lifecycle.py:1868](scripts/court_monitor/lifecycle.py:1868) |
| `FETCH_DIAG` (точный класс последнего ответа: transport + captcha_card/waf_block/portal_placeholder/non_card_page/breaker) | [scripts/court_monitor/config.py](scripts/court_monitor/config.py) |
| `fetch_fail_reason_ru` (причина отказа по-русски, одно место на все каналы) | [scripts/court_monitor/netutil.py:84](scripts/court_monitor/netutil.py:84) |
| `fetch_card_checked` (карточный fetch с детектом кода) | [scripts/court_monitor/netutil.py:182](scripts/court_monitor/netutil.py:182) |
| `card_breaker_allows` / `card_breaker_policy` (time/count гейт и единая policy по готовому kind) | [scripts/court_monitor/netutil.py](scripts/court_monitor/netutil.py) |
| `DeferredCardQueue` (no-sleep очередь отложенных карточек и возврат хвоста после half-open) | [scripts/court_monitor/netutil.py](scripts/court_monitor/netutil.py) |
| `classify_outage_page` / `classify_non_card_page` (точные WAF/portal/non-card semantic-классы) | [scripts/court_monitor/parsing/search.py](scripts/court_monitor/parsing/search.py) |
| `DIGESTED_ACTS_PATH` / `CASSATION_ACTS_PATH` / `PARSE_HEALTH_PATH` | [scripts/court_monitor/config.py:174](scripts/court_monitor/config.py:174) |
| Константы state-machine (`FI_ARCHIVE_DAYS`, `CASSATION_*`) | [scripts/court_monitor/config.py:99](scripts/court_monitor/config.py:99) |
| `update_parse_health` — детектор молчаливой поломки парсеров | [scripts/court_monitor/health.py:42](scripts/court_monitor/health.py:42) |
| `advance_case_stage` / `is_case_archived` / `migrate_stages` | [scripts/court_monitor/lifecycle.py:1959](scripts/court_monitor/lifecycle.py:1959) |
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
| `polish_digest_html` — LLM-полировщик (опц.) | [scripts/court_monitor/digest/llm.py:1124](scripts/court_monitor/digest/llm.py:1124) |
| Пост-обработка HTML (`_ensure_*`/`_validate_*`/`_drop_*`/`_normalize_*`) | весь [scripts/court_monitor/digest/postprocess.py](scripts/court_monitor/digest/postprocess.py) |
| Codex model: `Codex-haiku-4-5-20251001` (`_current_digest_model_name`) | [scripts/court_monitor/digest/llm.py:1265](scripts/court_monitor/digest/llm.py:1265) |
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

> ⏸ **С 19.08.2026 крон облака ВЫКЛЮЧЕН на обеих территориях** (лотерея
> адресов раннеров; `crons = []` + `CRON_UTC = ""` в обоих wrangler.toml),
> боевой путь — Mac-агент. **С 20.08.2026 — «один дайджест в день»** (решение
> юриста): слоты 08:00–10:00 каждые 30 мин; НЕПОЛНАЯ попытка (поиски слепые
> ИЛИ карточек прочитано <85% плана — `cloud_run_ok.py --run-complete` по
> блоку `last_run` журнала здоровья, пишет блок 4e main_json) коммитит данные
> ЧЕРНОВЫМ сообщением без подстроки «Mac-парсинг» (replay молчит — его гард
> contains() по сообщению), а её новости копятся в last_digest_context.json
> (`save_digest_context` мержит дельты дня, пока нет `delivered_at`; дельты
> попыток дизъюнктны — события уже влиты, флаги поставлены); юристу уходит
> алерт-прогресс «прочитано X из Y карточек». Удачная попытка, слот 10:00
> (дедлайн `DELIVERY_DEADLINE_MIN`) или --force — доставка:
> `--mark-delivered` + маркерный коммит «(Mac-парсинг)» → replay шлёт ОДИН
> дайджест со всем накопленным; при мёртвых канарейках на дедлайне
> накопленное отправляется доставочным коммитом без парсинга. Гейт слота —
> «дайджест дня уже отправлен» (`delivered_at`; облачный ручной прогон ставит
> его сам через `will_deliver`). last_digest.json на дашборде ведёт выпуски
> дня (issues, ключ — `issue_key` контекста); Mac-черновики его НЕ пишут —
> только доставляющий процесс и replay. Описание облака ниже — историческое
> и рецепт отката.

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
  апелляция + 7kas → гибридный дайджест (программный рендер + Codex только на
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
  ловит `push` с изменённым `data/last_digest_context.json` → `--replay-last
  --push-all`. С 16.08.2026 **РАЗБУЖЕН** (проба подтвердила блок раннера):
  условие `github.actor != 'github-actions[bot]' && contains(head_commit.message,
  'Mac-парсинг')`. Первая половина отсекает облачный крон (GITHUB_TOKEN), вторая —
  гард ревью Fable: без неё любой человеческий push, задевший контекст (ручная
  починка данных, merge, тестовый прогон резерва), повторно разослал бы вчерашний
  дайджест всем при ещё живом кроне. «(Mac-парсинг)» — фиксированный хвост
  сообщения коммита `parse_and_push.sh`; менять их только парой.
- **Живой просмотр парсинга (для резерва):** ярлык `ops/mac-local-run/Парсинг судов.command`
  + блок живого лога в админке Worker (`progress_pusher.py` → `POST /run-progress`,
  auth — Worker-секрет `PROGRESS_SECRET`, токен на Mac в
  `~/.config/court-monitor/progress_token` вне репо). Канал `/run-progress`
  общий с облачным пушером (`scripts/gh_progress_pusher.py`, `source:"github"`);
  записи Mac-пушера без `source` админка подписывает «Парсинг на Mac (резерв)».

### Процедура флипа обратно на Mac (если блок вернётся)

1. Сигнал: 🩺-алерт «все источники по нулям» / 🚨-падение прогона; при сомнении —
   запустить `probe_courts.yml` вручную (Actions → Run workflow). С 16.08.2026 проба спрашивает не только
   ПОИСК, но и **карточки своего региона** ([scripts/probe_court_access.py](scripts/probe_court_access.py),
   env `REGION`): у капчёвых судов Свердловской обл. поиск закрыт по проекту, весь канал мониторинга —
   карточки, и молчат именно они. Цели берутся из живых `data/cases.json` (по одному делу на суд,
   капчёвые первыми), классификация — боевыми `detect_captcha_challenge_card`/`looks_like_non_card_page`/
   `card_is_empty_shell`, вердикт итога `OK`/`BLOCKED`/`CAPTCHA`/`OUTAGE`/`MIXED`. **Отчёт коммитится**
   в `ops/court_probe/report.txt` — логи ранов требуют admin-прав, и разбирать по ним провал нельзя.
2. Отключить облако: вернуть `crons = []` в `wrangler.toml` + `wrangler deploy`
   **ОБОИХ** Worker'ов (`court-monitor-trigger` и `court-monitor-ural`) — крон
   у каждой территории свой, выключенного эталона мало.
3. Дайджест-на-push уже разбужен (16.08.2026): `replay_on_push.yml` стоит с
   `if: github.actor != 'github-actions[bot]'`. Пока крон жив, условие
   безвредно — он пушит под актором `github-actions[bot]`.
4. Mac уже разбужен (18.08.2026): агенты `com.court-monitor.parse` (будни
   06:00–08:30 каждые 30 минут + 08:45) и `com.court-monitor.import` (будни 10:30–18:30 каждые
   2 часа) загружены. **Гейт подстраховки** (`ops/mac-local-run/cloud_run_ok.py`,
   зовёт parse_and_push ПОСЛЕ git pull и ДО пробы судов): зрячий облачный
   прогон сегодня уже был → Mac тихо выходит (иначе двойной дайджест);
   слепой (все источники журнала здоровья по нулям — адрес раннера
   заблокирован) или прогона не было → Mac парсит сам. `--force` (пульт)
   обходит гейт, `--anywhere` — работа вне сети Сбера (честная проба судов
   решает). Пульт юриста — `ops/mac-local-run/СберСуд-пульт.command`
   (парсинг/дампы руками + живой лог). Стражи —
   [scripts/tests/test_mac_launchers.py](scripts/tests/test_mac_launchers.py).

⚠️ **Резерв обслуживает ОБЕ территории с 16.08.2026** (`ops/mac-local-run/parse_all.sh`
→ два поклоновых `parse_and_push.sh`; с 24.08.2026 Урал стартует сразу, ХМАО через 10 минут,
а импорты — только после `wait` обоих; список клонов — `~/.config/court-monitor/territories`
вне репозитория, регион клон определяет сам по файлу `REGION` в корне). Общие host-route
готовит родитель до детей; откат — `CM_PARALLEL_TERRITORIES=0`, а `--check` всегда последовательный. До этого
он был однотерриториальным, и при блоке Урал просто стоял. Тем же заходом
закрыты три молчаливые поломки, любая из которых сделала бы флип холостым:
(1) **список коммитимых файлов** вёлся руками и здесь, и в `update_cases.yml`, и
разъехался — резерв не коммитил семь файлов трека «Иски банка» (появился
25.07.2026, уже после усыпления резерва), то есть трек парсился и выбрасывался;
теперь список не существует вовсе — [ops/stage_data_files.sh](ops/stage_data_files.sh)
спрашивает пути у `court_monitor.config`, страж `test_data_files_staged.py`;
(2) **маршруты судов мимо VPN** строились регекспом по `courts.py`, а после
регионализации домены уехали в `regions/*.py` — находилось ШЕСТЬ строк из
комментариев вместо 21 домена ХМАО (у Урала их 67), и суды шли через VPN мимо
egress РФ; теперь домены из `get_region()`, пустой список фатален;
(3) **`git push origin main` падал** (origin по https, учётных данных нет,
SSH:22 закрыт) — адрес выводится из origin в `ssh://git@ssh.github.com:443/…`.
Проверка из офиса, ничего не публикующая: `bash ops/mac-local-run/parse_all.sh --check`
(в логе «Доменов судов region-реестра отрезолвлено: 21/21» у ХМАО, «67/67» у Урала;
уникальных IP может быть хоть ОДИН — суды ГАС за общим балансировщиком, это норма). Настройки машины —
`~/.config/court-monitor/{territories,env.<регион>,telegram,progress_token,worker.<регион>}`,
см. [ops/mac-local-run/README.md](ops/mac-local-run/README.md).

⚠️ **Импорт дампов капчёвых судов резерв тоже подхватывает (16.08.2026)** —
[ops/mac-local-run/import_dumps.sh](ops/mac-local-run/import_dumps.sh), зовётся из
`parse_all.sh` после завершения парсеров обеих территорий (отказ импорта прогон НЕ валит).
Второй операторский канал умирает вместе с облаком по той же причине: правила
приёма исков банка решаются только по карточке, и при блоке импорт заводит
НОЛЬ, теряя строку выдачи целиком (Урал 16.08: «+0 в картотеку · 10 карточка не
открылась»). Cloudflare и KV при этом живы — на sudrf они не ходят, — поэтому
Worker менять не пришлось: скрипт ходит теми же эндпоинтами, что и workflow
(`GET /admin/import-log` → `GET /import-dump?key=` → импортёр → commit/push →
`POST /import-result`), и оператор видит в журнале ту же запись с настоящими
числами. Правила выборки — `ops/mac-local-run/import_queue.jq` (дамп моложе
суток = TTL KV, не точечное добавление, и облако его либо не довело, либо
довело с `fetch_fail`/`card_failed`; запись «идёт» моложе 15 мин не трогаем —
облачный джоб ещё жив, и два отчёта затёрли бы друг друга). Локальной памяти
«уже сделано» нет намеренно: успешный отчёт обнуляет оба счётчика, и следующий
заход запись не выберет. Три вещи держать общими с облаком, иначе разъедутся
молча (этим проект болел дважды): пейлоад отчёта —
[ops/import_result_body.jq](ops/import_result_body.jq), список коммитимых
файлов — `ops/stage_data_files.sh`, преflight сети/маршруты/ssh-адрес —
[ops/mac-local-run/lib_sber_net.sh](ops/mac-local-run/lib_sber_net.sh) (общий с
`parse_and_push.sh`). Лок у обоих скриптов ОДИН (`.run.lock`): импорт и парсинг
одного клона пишут в один индекс git. Секреты Worker'а лежат в
`~/.config/court-monitor/worker.<регион>` и читаются `awk`, а НЕ `source`: в
окружении прогона `PUSH_SECRET`+`PUSH_WORKER_URL` включили бы вторую доставку
push с Mac. Для дампа старше суток (в KV его уже нет) — режим
`--file <html> --court <домен>` мимо Worker'а. Стражи —
[scripts/tests/test_mac_import_dumps.py](scripts/tests/test_mac_import_dumps.py),
[scripts/tests/test_mac_reserve.py](scripts/tests/test_mac_reserve.py).

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

**Горизонт доверия к дате (`KNOWN_DATE_TRUST_DAYS`=180, 14.08.2026).** Известная будущая дата сильнее 21-дневной страховки force-parse (решение юриста): правило вынесено в `known_future_date_skip` и зовётся из ДВУХ мест — ветки force-parse и штатной проверки ниже. ⚠️ Горизонт передаётся ТОЛЬКО в ветку force-parse, и эта асимметрия — суть механизма: дата приходит из карточки суда как есть, и 2-1725/2026 приехало с заседанием 20.08.2029 при соседних событиях от 29.07.2026 (опечатка в годе) — безусловное доверие похоронило бы дело на три года. Ограничить горизонт ВНУТРИ хелпера нельзя: тогда такое дело перестанет скипаться вовсе и будет читаться каждый прогон (366 раз в год вместо 17); с асимметрией оно возвращается ровно к прежнему ритму «раз в 21 день» и само подхватит исправленную судом дату. Горизонт проверяется у КАЖДОГО кандидата с падением на следующий источник — абсурдный `hearing_date` кассации не глушит законный `suspended_until`. Порог с двукратным запасом: максимум законной дистанции по обеим территориям — 90 дней. Наблюдаемость: `fi_distrusted_date` в сводке прогона + WARNING + `mark_distrusted_date` в отчёте парсинга (иначе класс «опечатка суда» неотличим от рутинного форс-парса — находку добыли только симуляцией). ⚠️ Цена снятия страховки шире переноса заседания: до дня заседания невидимы ЛЮБЫЕ досудебные движения карточки (объединение, частные жалобы, отказ от иска, мировое) — при законной дате в 90 дней новость опаздывает на 90 дней вместо 21. Стражи — `TestKnownFutureDateBeatsForceParse`.

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
(`first_parse=`, флаг `first_card_parse` вычисляется в FI-цикле ДО бампа
`fi["last_checked_at"]` — после него первый парс неотличим от рутинного;
стережёт `TestFirstParseFlagWiring`). ⚠️ С 14.08.2026 одного отсутствия
штампа мало: трек «Иски банка» ставит `last_checked_at` ПРИ ЗАВЕДЕНИИ
(`make_bank_entry`, карточку читает сам приём), и признак первого парса несёт
маркер `fi["intake_card_parse"]` — FI-цикл снимает его `pop`'ом ОТДЕЛЬНОЙ
СТРОКОЙ перед вычислением флага. Оба условия обязательны: только по
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
  ⚠️ **С 14.08.2026 `make_bank_entry` ставит `first_instance.last_checked_at`
  ПРИ ЗАВЕДЕНИИ** (дата из `now_iso`, СРЕЗАННАЯ до `YYYY-MM-DD`: полный
  таймстамп `date.fromisoformat` не разбирает, и правка вышла бы холостой) —
  карточку читает сам импорт, а ветка force-parse в `should_skip_case`
  ([lifecycle.py:2755](scripts/court_monitor/lifecycle.py:2755)) стоит ПЕРВОЙ и
  без штампа перебивает всё остальное: и будущее заседание, и оба недельных
  ритма. Разгон Урала 14.08.2026 это и вскрыл — 265 карточек трека в очереди
  при 154 делах с заседанием впереди (после правки 37, пропуски только
  `future_hearing` 153 и `writ_weekly` 108). Записи, заведённые раньше,
  штампует идемпотентная `migrate_intake_checked_stamp` (в начале
  `migrate_stages`) по дате `import.at`, и только при непустых `events` —
  единственном доказательстве, что карточка читалась (в основной картотеке
  дела заводятся со СТРОКИ выдачи, там штамповать нечего). Признак «первый
  парс прогоном» переехал с «нет штампа» на маркер `intake_card_parse`:
  `first_card_parse` в FI-цикле снимает его `pop`'ом ОТДЕЛЬНОЙ СТРОКОЙ (внутри
  булева выражения он не выполнился бы на короткой схеме, и маркер объявлял бы
  первым каждый прогон), иначе стародатный фильтр «догоняющих» событий об
  акте/решении молча выключился бы — см. `TestFirstParseFlagWiring`.
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
  своей ветки дело парсилось бы каждым прогоном. **Пока даты нет (или она
  прошла без результата) — грейс + недельный ритм (с 20.08.2026)**: первые
  `BANK_DEFAULT_CANCEL_DAILY_GRACE_DAYS`=14 дн от якоря (дата заседания, иначе
  дата подачи) карточка читается ежедневно (ст. 240 даёт суду 10 дн), дальше —
  раз в `BANK_WRIT_CHECK_DAYS` с причиной `default_cancel_weekly`; прежний
  ранний выход `return False, ""` читал зависшее дело каждым прогоном все
  90 дн потолка (2-3005/2026 Орджоникидзевского: заявление 21.07, месяц
  ежедневных чтений). Причина проведена в оба кортежа недельного ритма runs.py
  (план очереди + классификация скипов) — иначе утекла бы в «без движения»;
  в админке осознанно остаётся в группе «Пропуск: прочее», как `merged_weekly`
  и `default_cancel_hearing`. Стражи — `TestDefaultCancelWeeklyRhythm`.
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
  прогоне 30.07): такие дела ИЛ не породят, и держать их в очереди ожидания
  бессмысленно. **(1) В иске ОТКАЗАНО** — ждём только апел. жалобу банка:
  архив через `BANK_DENIED_ARCHIVE_DAYS=30` от **мотивировки** (ветка стоит ДО
  поиска листов, месячный срок ст. 321 ГПК течёт от неё). **(2) ЛЮБОЕ
  процессуальное завершение** — присоединение к другому делу (ст. 151 ГПК),
  возврат, отказ в ПРИНЯТИИ, передача по подсудности: вид решает
  `classify_fi_termination` (у merged своя ветка окна — `BANK_MERGED_ARCHIVE_DAYS=30`
  от определения, окно на отмену объединения; остальные идут той же веткой
  «листа не будет», 30 дн = окно на частную жалобу, якорь доходит фолбэком до
  `event_date`). ⚠️ **С 14.08.2026 (разгон Урала) строковый матч заменён на
  классификатор**: прежняя версия ЯВНО исключала «отказано в принятии» в
  расчёте на статус карточки «Возвращено» и свою ветку завершения, но 9-125/2026
  (Пуровский) пришло со статусом **«Решено»** — дело ушло в ветку «Решено без
  ИЛ» и 180 дней числилось в очереди на лист; тем же дефектом были задеты 5 дел
  ХМАО (9-1424, 9-31, 9-121 — возвраты; 2-1588, 2-8088 — передачи). Классификатор
  зовётся **ТОЛЬКО по полю «Результат»** (`classify_fi_termination(result, "", [])`):
  при пустом результате он сканирует историю движения, а там у живого дела лежит
  отменённый возврат прошлого круга — дело молча перестало бы ждать лист (страж —
  `test_empty_result_does_not_read_movement_history`). Предикат штампуется в
  запись как `first_instance.writ_expected = False` (`split_bank_track`, только
  False) — фронт читает готовый штамп, своей копии правила в JS нет;
  `legal_force_est` при этом не пишется вовсе (иначе drawer показывал бы
  «Вступило в силу (расч.)» там, где исполнять нечего). Дела с частичным
  удовлетворением лист получают — их не трогаем.
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
  ПОСЛЕДНЕЙ (при обрезке Telegram страдает первой). **С 14.08.2026 —
  свёртка массовых заведений** (`split_bank_intake_fold`, template.py; порог
  `BANK_INTAKE_DIGEST_FOLD`=25 из Actions Variables, `0` — выключить):
  первый боевой прогон авто-подхвата на Урале завёл 116 исков разом, и секция
  стала стеной одинаковых строк «взят на мониторинг» (HTML 60 КБ) — решения,
  ИЛ и заседания в ней утонули. Больше порога → одна строка
  «🆕 заведено N новых исков банка в M судах — список на дашборде» на месте
  группы «новые иски»; сворачиваются ТОЛЬКО дела, где `fi_bank_claim_registered`
  единственный тип и нет `details["left_track"]` (дело с решением/ИЛ или
  уехавшее в общий трек печатается подробно). Счётчик заголовка = число
  ПОДРОБНЫХ дел, при нуле подробных заголовок вовсе без `(N)` (секцию без
  счётчика `_check_section_counters` штатно пропускает). Сводка получает
  отдельную часть «🆕 N новых исков банка заведено» (вложить второй
  разделитель внутрь части нельзя — части склеены через «·»); без свёртки
  строка прежняя посимвольно. ⚠️ **Хелпер общий с линтером**:
  `_expected_number_alternatives` (lint.py) и `llm._collect_case_numbers`
  перебирают ВЕСЬ `fi_changes` и требуют каждый номер в HTML — без гейта
  дайджест-паводок переехал бы в 🩺-алерт на 116 строк «потерян номер дела»
  (а при `DIGEST_POLISH=1` полировка отвергалась бы всегда); два независимых
  расчёта порога разъехались бы молча. Порог прокинут и в replay-пути
  (`test_digest.yml`, `replay_on_push.yml`) — иначе тестовый дайджест
  разойдётся с боевым. В mine-версии свёрнутая строка выпадает сама (параграф
  без номера дела) — это верно, звёзд у только что заведённых дел нет.
  Полный LLM-путь (`DIGEST_FULL_LLM=1`) свёртки не имеет осознанно: банк-записи
  там не отделяются, а путь — аварийный откат и в workflow не включён.
  События `fi_writ_issued`/
  `fi_writ_status_changed` (НЕ в эхо/stale-фильтрах) и `fi_bank_claim_registered`
  (дело заведено авто-подхватом; НЕ рутина — переживает `BANK_DIGEST_ROUTINE=0`);
  маркер `change["track"]`
  едет в данных fi_changes — сигнатуры/replay не тронуты. Рутина отключается
  `BANK_DIGEST_ROUTINE=0` (`filter_bank_routine_events`; дефолт 1 — пилот
  шлёт всё). **С 09.08.2026 (разбор дайджеста 07.08 юристом)**: секция
  сгруппирована «по важности» (`_BANK_GROUP_ORDER`/`_bank_change_group`,
  template.py; **порядок с 17.08.2026, решение юриста**: ИЛ (вместе с
  календарными «вступило в силу»/«лист завис») → решения/акты → иные (сюда
  же завершения: возврат, отказ в принятии, передача по подсудности,
  присоединение) → заседания по дате, ближайшие сверху → новые иски
  последними, там же строка-свёртка массового подхвата; рабочая очередь
  юриста начинается с листов, заведение дел — фон. Пустая строка между
  группами, БЕЗ подзаголовков — их пришлось бы синхронизировать с линтером
  счётчиков. ⚠️ «Иные» — ДЕФОЛТ `_bank_change_group`, и сидит он в СЕРЕДИНЕ
  порядка: `_BANK_GROUP_ORDER` поэтому словарь `{индекс: типы}` с
  именованными константами, а функция сначала собирает совпавшие группы и
  только потом берёт старшую — прежний `min(best, i)` с дефолтом-`len()`
  склеил бы заседания и новые иски с «иными». Гард мотивировочного
  `fi_final_event` целится в группу РЕШЕНИЙ, а не в нулевую: нулевая теперь
  листы, и дело с листом и мотивировкой в одном прогоне обязано остаться в
  листах); строки решений и
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
  **Разбор карточки 14.08.2026** (три правки одним заходом): (1) статусные
  бейджи без даты заседания получили **дату события в колонку дат** —
  «Передано судье», «Поступило в суд», «Приостановлено», «Без движения»
  (`vm.statusDate` в `prepareCaseViewModel` → `.status-date` в
  `buildHearingHtml`, приглушённо: это дата случившегося, не назначенного).
  Данные — ТОЛЬКО скалярные `fi.event_date`/`fi.filing_date`: в bank-картотеке
  события грузятся лениво и в списке их нет. Гейт «есть ли дата заседания» —
  общий предикат `hasHearingDate` (голого `nextDate` мало: он бывает заполнен
  при метке, которую колонка не печатает); «Поступило в суд» вдобавок гейтится
  1-й инстанцией (`filing_date` — дата поступления ИСКА, на карточке апелляции
  она лгала бы) и отсутствием даты в колонке (у дел с ПРОШЕДШИМ заседанием
  статус тоже `awaiting`, и дата рядом читалась бы как дата поступления).
  Тем же заходом починен мёртвый регексп передачи судье: суд пишет «Передача
  **материалов** судье», код искал «передача дела судье» — лейбл не
  рендерился ни разу. (2) Бейдж «Обжалуется» переехал из `.mc-badges` в свой
  ряд `.mc-pending` под шапкой карточки, по правому краю (в одной строке пара
  бейджей отжимала номер дела в многоточие); на десктопе он остался в строке
  номера. Ряд отдельный, а НЕ перенос внутри `.mc-badges`: у `.mc-top`
  `align-items:center`, двухстрочная группа увела бы номер в вертикальный
  центр, да и страж «одна группа `.mc-badges`» сломался бы. (3) Цветовая
  гамма разведена на **три оси**: холодное — стадия (teal→indigo→violet),
  тёплое — состояние и срочность, **графит — роль стороны в жалобе**
  (`--role-badge-*`, бейджи «Апеллянт»/«Кассатор»; вместе они не встречаются,
  различает их слово). До этого «Апеллянт» сидел на violet — том же цвете,
  что стадия «Кассация», а токены `--rose-*` «Кассатора» не имели тёмного
  варианта и светились розовым кирпичом на тёмной теме; пара удалена.
  Тогда же (14.08.2026) выровнены **типы заседаний в обеих картотеках**:
  `nextHearingType` считался ТОЛЬКО точным матчем по дате в `events[]`, а в
  треке «Иски банка» события грузятся лениво (`ensureBankEvents`) и в списке
  их нет — все заседания трека показывались общим «Назначено» вместо
  «Беседа»/«Предв-ое СЗ»/«Основное СЗ». Добавлен ОДНОСТОРОННИЙ фолбэк: при
  пустом `events[]` тип берётся из текста `fi.last_event` (`classifyEvent`) —
  не узнал заголовок, оставил прежнее «Назначено». Сверка с настоящими
  events по 478 делам трека: 355 попаданий, 0 ошибок типа, а среди дел с
  БУДУЩИМ заседанием (только там бейдж и виден) — 264 из 264; все «молчания»
  пришлись на прошедшие заседания, где `last_event` уже пост-решенческий.
  ⚠️ Порядок веток load-bearing: где события ЕСТЬ, матч по дате точнее текста
  последнего события. Заодно `classifyEvent` научился «Беседа. …» (бэковый
  `classify_hearing_type` знал её всегда, фронт — нет), а иконка у всех
  назначенных заседаний (`scheduled`/`prep`/`prelim`/`main`) стала ОДНА —
  календарь (`CALENDAR_ICON`): тип различает подпись, а прежние весы у
  «Основного СЗ» совпадали с иконкой «Рассмотрено».
  ⚠️ Тогда же из бейджа убран эмодзи 🔄 «рассмотрение начато с начала»: он
  ВЫТЕСНЯЛ иконку статуса (два одинаковых «Отложено» читались как разные
  состояния) и, как всякий эмодзи, рисовался системным цветным шрифтом мимо
  палитры бейджа — на тёмной теме выпадал пятном. Признак остался в
  хронологии карточки дела и в `title` бейджа с датой. Эмодзи в бейджи не
  возвращать: значок — только inline-SVG на `currentColor`.
  **Тёмная тема, разбор 14.08.2026.** Вскрылся целый класс дефектов: цвет,
  зашитый ЛИТЕРАЛОМ в правило бейджа, тёмного варианта не получает никогда.
  Так светили на тёмном фоне «Без движения» (`#ffedd5`/`#c2410c`), рамка
  «Передано в 1-ю инст.» (`#fecaca`) и — раньше — «Кассатор» (`--rose-*`);
  а `var(--amber-800,#92400e)` у «Отмена заочного» ставил тёмно-коричневый
  текст на тёмный фон, потому что токена `--amber-800` в проекте НЕТ и
  срабатывал литеральный фолбэк (он и маскировал опечатку — в светлой теме
  цвет совпадал). Все переведены на токены, объявленные в обоих блоках темы;
  страж `test_badges_have_no_hardcoded_colours` запрещает в правилах
  `.badge-*` литеральные hex (кроме `#fff`) и фолбэки вида `var(--x,#hex)`.
  Тогда же приглушён **активный сегмент картотек** в тёмной теме
  (`--seg-active-*`: `#1e7a34` + светлый текст вместо бренд-зелёного с белым)
  — он оставался единственной насыщенной заливкой тёмного экрана; светлая
  тема не изменилась побайтово.
  Стражи — [test_frontend_status_badges.py](scripts/tests/test_frontend_status_badges.py).
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
  строке/карточке/hero, признак заочности (`defaultJudgmentBadgeHtml`),
  строки «Вступило в силу (расч.)» и «🌙 Копия ответчику»
  (`defaultCopyKvHtml`) в «Ключевых датах» и сортировка чипа «Ждут ИЛ» по
  убыванию ожидания (очередь работы, а не алфавит). Пороги (30/60 дн)
  привязаны к реальности выдачи (+40..55 дн от решения) и
  `BANK_WRIT_WAIT_MAX_DAYS`.
- **Ряд метаданных под результатом (13.08.2026, разбор карточки юристом)**:
  плашками в нём остались только СОБЫТИЯ — синяя «Акт 12.08» и цветные
  состояния отмены заочного; факты-отсутствия печатаются тихим серым текстом
  `.state-quiet` одной строкой «🌙 заочное · акт не опубликован» (разделитель —
  псевдоэлемент `.state-quiet + .state-quiet::before`, не разметка: иначе «·»
  вылезал бы перед первой меткой и рядом с плашкой). Раньше ряд был вторым
  набором пилюль и на каждой пятой карточке трека (100 заочных из 509) спорил
  с бейджем результата. Заочность у решённого дела даёт `defaultJudgmentQuietHtml`,
  пилюля «🌙 Заочное» осталась там, где бейджа результата нет: hero drawer'а и
  дела, у которых результат снят ремонтом (`defaultJudgmentBadgeHtml` без opts;
  в `buildStateHtml` нейтральную ветку гасит `{skipNeutral:vm.resultPresent}`).
  ⚠️ В МОБИЛЬНОЙ карточке нейтральной заочности нет вовсе: `renderMobileCards`
  зовёт `buildStateHtml(c,vm,{compact:true})` (симметрично `buildHearingHtml`),
  и гейт `opts.compact` гасит метку — карточка плотная, а признак виден в
  десктопной строке и в hero drawer'а, который на телефоне и открывают
  (решение юриста). Особые состояния отмены гейт НЕ трогает — в карточке они
  остаются.
  ⚠️ Особые состояния отмены (`pending`/`cancelled`) тихой меткой НЕ заменяются
  никогда — цвет там и есть сигнал; отказ в отмене (`refused`) — наоборот,
  снова нейтральный, метка обязана появиться (иначе признак пропадёт из строки
  совсем). Класса `.badge-act-no` больше нет. Пилюля результата сокращает
  ТОЛЬКО частичное удовлетворение (`FI_RESULT_LABELS_SHORT` = один ключ
  `partial`: «Удовл-но частично»; полное «Удовлетворено» юрист велел писать
  словом — оно влезает в колонку и на 320px), полная форма — в тултипе пилюли
  (`vm.resultLabelFull`) и в «Результате» drawer'а. Тогда же закрыт давний
  Знак исхода (`RESULT_ICONS`) виден ТОЛЬКО при нейтральном для банка исходе —
  при favorable/unfavorable `buildFavorIcon` рисует свои ✓/✕ цветом, поэтому
  набор обязан говорить без опоры на цвет: геометрическая семья ⊘ (не
  рассмотрено) · ⊖ (снято) · ⊕ (присоединено) · ⊠ (прекращено) · ◐ (частично)
  + ↩ (возвращено). 13.08.2026 заменены «—» у Прекращено (читался дефисом
  перед словом), «⇥» у Присоединено (табуляция) и снят дубль ⊘ у Снято/
  Оставлено без рассмотрения; тогда же `.badge-icon` перестал прибивать кегль
  к 11px (`font-size:inherit`) — вдвое меньший знак рядом с 15px текстом
  карточки читался грязью, а ✓/✕ идут мимо класса и разнобой был виден.
  Тогда же закрыт давний
  дефект узкой карточки: `.cell-state > .badge` получил перенос (на 320px
  колонка ≈153px, и длинный исход наезжал на дату заседания), а начинка пилюли
  обёрнута в `.res-body` — без обёртки перенос отрывал галочку и «✓ / Удовл-но /
  частично» вставало тремя строками. Стражи —
  [test_frontend_writs.py](scripts/tests/test_frontend_writs.py).
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
# в docs/technical и AGENTS.md. Протухание стережёт test_doc_anchors.py.
python3 scripts/refresh_doc_anchors.py --write

# Зависимости
pip install -r scripts/requirements.txt

# Деплой Worker
cd cloudflare-worker && wrangler deploy
```

GitHub Actions workflows запускаются из UI репозитория (Run workflow) или автоматически cron'ом Worker'а.

## Переменные окружения

- `ANTHROPIC_API_KEY` — Codex.
- `CLAUDE_MODEL` — модель Codex (алиасы haiku/sonnet/opus или точный id; пусто = боевой эталон haiku 4.5). Ставит только test_digest.yml; боевой крон переменную не задаёт.
- `CLAUDE_EFFORT` — уровень усилий (`low`/`medium`/`high`/`xhigh`/`max`) для Sonnet 5/Opus 4.8 → `output_config.effort`; пусто/`default` = не отправлять (у API дефолт high). Для haiku игнорируется — модель эффорт не поддерживает.
- `GIGACHAT_AUTH_KEY` (+ `GIGACHAT_SCOPE`, `GIGACHAT_MODEL`) — GigaChat, альтернативный LLM; включается `LLM_PROVIDER=gigachat` (отдельный workflow удалён 09.07.2026, теперь выбор провайдера — input `llm_provider` в test_digest.yml).
- `OPENROUTER_API_KEY` (+ `OPENROUTER_MODEL`) — OpenRouter, третий LLM (тестовый контур); включается `LLM_PROVIDER=openrouter`. Пустая модель = «модель дня» с `shir-man.com/api/free-llm/top-models`, fallback `openrouter/free`. Кэш пересказов для gigachat/openrouter неймспейсится по `провайдер:модель` (`_act_cache_key`), Codex-ключи прежние.
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
- `BANK_INTAKE_DIGEST_FOLD` — порог свёртки массовых заведений в дайджесте (25; `0` — печатать подельно). Прокинут в update_cases.yml, test_digest.yml и replay_on_push.yml — replay обязан воспроизводить крон.
- `BANK_INTAKE_SEEN_TTL_DAYS` — сколько помним отказников негативного кэша (60).
- `CARD_BREAKER_MODE` — `time` для полного прогона (явно в cloud/Mac launcher), `count` для batch-утилит и импортов; неизвестное значение безопасно откатывается к `count`.
- `CARD_BREAKER_FAST_THRESHOLD` / `..._COOLDOWN_SECONDS` — reset/response/5xx: 3 подряд, 60 с.
- `CARD_BREAKER_OUTAGE_THRESHOLD` / `..._COOLDOWN_SECONDS` — заглушка/служебная страница портала: 2 подряд, 180 с (известная заглушка поиска открывает сразу).
- `CARD_BREAKER_SLOW_THRESHOLD` / `..._COOLDOWN_SECONDS` — timeout/connection/DNS/TLS/proxy: 2 подряд, 300 с.
- `CARD_BREAKER_BLOCK_THRESHOLD` / `..._COOLDOWN_SECONDS` — WAF/403/CAPTCHA карточки: 2 подряд, 600 с (WAF поиска открывает сразу). `CARD_BREAKER_THRESHOLD=0` остаётся master-off; `CARD_BREAKER_THRESHOLD`/`CARD_BREAKER_PROBE_EVERY` управляют старым count-профилем импортов 5/3.
- `LOG_LEVEL` — уровень логов прогона (`DEBUG`/`INFO`/`WARNING`/`ERROR`, дефолт `INFO`); `DEBUG` показывает пер-кейсовые skip/«без изменений» и прочую диагностику.

## Куда уходит дайджест

- **Telegram:** все workflow'и шлют в личный чат (`TELEGRAM_CHAT_ID_TEST`) по умолчанию. Чтобы продублировать в корпоративную группу — поставить галку `to_group` в UI Run workflow. Текст дайджеста в Telegram **общий**, не персонализированный.
- **PWA push:** `update_cases.yml` (крон) шлёт всем подписчикам PWA. Тестовый workflow `test_digest.yml` шлёт push **только устройствам-владельцам** по умолчанию, чтобы не спамить коллегам прототипами. У `test_digest.yml` есть галка «push_all» — отправит на все устройства. Чтобы пометить своё устройство владельцем — открыть PWA по URL `https://selivanovas.github.io/dashboard/sberbank_dashboard.html?owner=<OWNER_SECRET>` (один раз).
- **Персонализация push по watchlist (`_per_sub` callback):** push-payload собирается под каждого подписчика отдельно через фабрику `_make_per_sub_callback` ([scripts/court_monitor/delivery.py:325](scripts/court_monitor/delivery.py:325)). Новые дела (`fi_new_cases`, `appeal_new_cases_csv`) — общесистемный сигнал, шлются всем; изменения и переходы стадий — только если дело в watchlist подписчика. Click_url для подписчиков с watchlist — `?digest=open&mine=1`. Используется в основном кроне (`main_json`), `--replay-last`, `--push-last-digest`.

## Админка подписчиков

URL: `https://court-monitor-trigger.7selivanov-a.workers.dev/admin?secret=<OWNER_SECRET>`. Открывается в браузере (мобильно тоже). HTML-страница вынесена в [cloudflare-worker/admin_page.js](cloudflare-worker/admin_page.js) (`renderAdminHtml(secret, role, cfg)`, wrangler бандлит импорт сам); серверные эндпоинты — в [cloudflare-worker/worker.js](cloudflare-worker/worker.js). ⚠️ Вся страница — один template literal: внутренний JS пишется без backtick'ов и `${`, backslash удваивается. Naive-таймстампы из data/*.json (Python на UTC-раннере пишет без «Z») страница парсит как UTC (`parseIso`).

**Роли (с 16.07.2026):** owner (`OWNER_SECRET`) — всё; operator (`OPERATOR_SECRET`, один общий на сопровождающих капчёвых судов; не задан — роль неактивна, у ХМАО так) — статус+здоровье+живой лог+секция «Импорт дел». Гейт — `resolveAdminRole`/`requireAdminRole` (worker.js): чужой секрет → 401, оператор на owner-эндпоинте → 403 (реальный запрет на сервере, скрытие в UI — `data-owner-only` + `html[data-role]`). `DISPATCH_WORKFLOWS` — `{inputs, roles}`: update_cases/test_digest — owner, import_cases/add_cases — обе роли. Секция «Импорт дел» (`#import`): **с 10.08.2026 вкладка видна ОБЕИМ ролям на ЛЮБОЙ территории** — первой карточкой блок «Добавить дела» (точечное добавление до 20 строк: номер/ссылка, построчная валидация против `region.*` cases.json, POST `/admin/add-case`, поллинг того же журнала тиком 60 с с потолком 40 мин — пачка номеров идёт дольше дампа, а каждый тик стоит KV-list; имя оператора общее с дамповой формой через `admin_operator_name`); дамповая часть ниже — dropdown gated-судов из `region.fi_courts` (у ХМАО их нет — прячется только она + светофор, `.imp-grid` схлопывается в одну колонку, журнал грузится `logonly=1`), вставка rich-paste/файл, поллинг журнала (`/admin/import-log`), история импортов (записи `kind:"case"` рендерятся «📌 точечно · N стр.»), **светофор свежести по судам** (когда каждый суд импортировался в последний раз: зелёный ≤7 дн, жёлтый ≤14, красный дольше/ни разу; данные — вечные ключи `import:last:<домен>`, пишутся на done в `/import-result`). **С 17.07.2026 — операторский UX (394cd14):** оператору секция открыта по умолчанию (с 02.08.2026 — вкладка по умолчанию, `TAB_DEFAULT`; серверная перестановка `IMPORT_SECTION` убрана), светофор раскрыт и кликабелен (клик по суду = выбрать его в форме, слушатели — делегированием), 5-я плитка пульта «Импорты» (N просрочено; обеим ролям, без gated-судов скрыта — ХМАО не видит, `.pult.has-import` в мобильном медиа-блоке обязателен), drag-n-drop файла в поле вставки + индикатор «что уйдёт» (файл побеждает вставку) + предпроверка «есть ли ссылки» до отправки, статус ожидания с таймером, сбой cases.json → алерт с «Повторить» (owner'у — тихо, как раньше). **Автоопределение суда по вставке (17.07.2026):** хост из абсолютных ссылок карточек подставляет суд в dropdown сам (пока оператор не выбирал вручную — флаг `impCourtTouched`), конфликт — заметка «⚠ ссылки ведут в …» с кнопкой «выбрать этот суд», отправка чужого дампа блокируется на клиенте, Worker'е (400) и в импортёре (`EXIT_WRONG_COURT`). ВСЕ URL данных страницы выводятся из `CASES_DATA_URL` Worker'а (`adminPageConfig()`) — хардкод сломал бы админку территорий. **Операторский путь дочищен 02.08.2026:** после успешного импорта форма ОЧИЩАЕТСЯ (поле + файл + `impRunDetect`) и светофор перерисовывается из кэша `impLastFreshMap` без похода в KV — раньше оператор шёл очередью судов, и следующий Ctrl+V клеился в конец прошлого дампа («⚠ ссылки нескольких судов», отправка заблокирована, кнопки «очистить» нет); при `failed` вставка сохраняется для повтора. Ссылка на суд несёт `srv_num` выбранной площадки (+`delo_id=1540005&name_op=sf`) — голая вела на первую площадку домена, а часть судов заведена ТОЛЬКО как `srv_num=2` (серверные предохранители этого не ловят: хост и delo_id совпадают). Первый рендер светофора подставляет самый просроченный суд (`impFreshAutoPicked`; `impCourtTouched` при этом НЕ ставится — автоопределение по вставке должно сохранить право переключить). Индикатор и предпроверка считают `name_op=case` («дел на странице: N»), а не все `a[href]` — прежний счётчик показывал 137 при десятке дел, а тест на голый `<a>` пропускал вставку без единого дела. Светофор показывает «+7 из 24» (`rows` в вечном ключе `import:last:*`, с 02.08.2026 — у старых записей его нет, фолбэк «+7»). **С 14.08.2026 в списке видны ПОСТОЯННЫЕ СУДЕБНЫЕ ПРИСУТСТВИЯ**: прежний дедуп `impCourts` по домену выкидывал вторую площадку сайта (Пышма у Камышловского, Ачит у Красноуфимского — в реестре с 16.07.2026), и её дела не импортировал никто. Ключ строки — `impCourtKey` = «домен|srv_num» (`impDomainOf` отрезает домен там, где нужен он: POST `/admin/import-dump`, сверка с хостами дампа, автоопределение — у площадок одного суда хост ОБЩИЙ, сравнивать надо домены, иначе выбранное присутствие молча перевыбирается на первую площадку). Свежесть остаётся по домену (вечный ключ `import:last:*` серверный, площадок не различает) — у суда и присутствия дата общая, что для регламента честно: оператор берёт обе выдачи за один заход. Стражи — [test_admin_pult.py](scripts/tests/test_admin_pult.py). Мобильная форма — своим медиа-блоком ПОСЛЕ правил `.imp-row` (общий блок «Мобильная раскладка» стоит в файле раньше и при равной специфичности проигрывал), поля 16px — iOS зумит всё мельче. **С 02.08.2026 вкладка на ≥1200px в две колонки** (`.imp-grid`: форма слева, свежесть+история справа) — иначе на 1440px операторская была одной узкой колонкой и рабочая очередь не читалась одновременно с формой; обеим колонкам `min-width:0` (зона вставки с таблицей суда распирала бы трек), ≤1200 — обратно в одну.

**Дизайн v2 (13.07.2026)** — визуальный язык дашборда: токены цветов/шрифтов скопированы из [styles.css](styles.css) (IBM Plex с Google Fonts, сберовский зелёный, бейджи-пилюли, цвета стадий teal/indigo/violet — карта `stageBadge` зеркалит `stageBadgeHtml` из app.js), 3-режимная тема авто/свет/тьма (localStorage `admin_theme`, инлайн-скрипт в head), статусы — цветные точки/пилюли вместо эмодзи, иконки — inline-SVG. При смене палитры дашборда токены админки синхронизировать вручную.

**Каркас — ВКЛАДКИ (с 02.08.2026).** Чипы шапки (`role="tablist"`) переключают панели: показана ровно одна секция, `.section{display:none}` / `.section.is-tab-active{display:block}`. До этого страница была лентой на 3,6 экрана у владельца (76% — «Подписчики») и 5,7 на телефоне, а у оператора пульт с плиткой «Импорты» начинался на 897px — ниже первого экрана. Теперь любая вкладка умещается в один экран (@1440: 813px), и серверная перестановка `IMPORT_SECTION` по роли убрана — порядок в DOM ничего не решает, решает активный чип.
- **Состояние — hash, без localStorage** (`showTab`/`tabAllowed`/`tabFromHash`, дефолт: оператор → `import`, владелец → `system`). ⚠️ В hash пишется **`#tab-<id>`, а не голый id секции**: иначе Chrome после `replaceState` находит в документе элемент с таким id и выполняет отложенный «прыжок к фрагменту» уже после load — страница уезжала вниз, липкая шапка уходила за верхний край. Старый формат читается тоже. `history.replaceState`, не `pushState` (иначе «назад» листает вкладки и плодит копии URL с секретом); присваивать `location.hash` нельзя.
- ⚠️ **Делегирование кликов строго на `#nav`**: класс `.chip-btn` носит ещё и ссылка «Открыть поиск по суду» внутри формы импорта.
- ⚠️ **`initTabs()` вызывается в стартовой секции внизу**, а не на месте определения: `onTabShown` читает `lastStaticLoadAt` (`let` ниже по файлу) — вызов раньше объявления упал бы в TDZ.
- Недоступная вкладка из hash откатывается на дефолт (`tabAllowed`: роль → `data-owner-only`; конфиг → инлайновый `display:none` у `#import`). Дип-линк `#tab-import` у владельца доводится в `loadImportCourts` после загрузки cases.json — на старте чип ещё скрыт.
- Скроллспай `IntersectionObserver` и `scroll-margin-top` удалены (дрались бы с `showTab` за класс `.active`).

Компоновка: липкая glass-шапка (лого · чипы-вкладки «Система/Импорт/LLM/Подписчики» · сводка · тоггл темы · Обновить) → **пульт кликабельных stat-плиток** (Последний прогон ok/сбой/идёт из gh-runs · Дайджест N изменений · Парсеры «все 22 ok»/«N ⚠» · Автозапуск + push-агрегат) — вне вкладок, всегда сверху. **У оператора плиток три:** «Дайджест» и «Автозапуск» — `data-owner-only` (дайджест — продукт юриста, а импорт диспатчит свой workflow сразу и крона не ждёт); колонки — правилом `html[data-role="operator"] .pult` строго внутри `@media (min-width:769px)`, иначе оно перебило бы двухколоночный телефон. Плитки ведут: «Последний прогон» → лог run в GitHub, «Дайджест» → дашборд, «Автозапуск» → страница workflow (там же «Run workflow» для полного обхода), «Парсеры»/«Импорты» → своя вкладка. **С 13.08.2026 у оператора внешних ссылок на плитках нет** (решение юриста — на GitHub его не пустят): «Последний прогон» рендерится без `data-href` (гейт по `isOperator` прямо в разметке — без атрибута плитка выпадает из делегирования кликов) и с `disabled`, стрелку ↗ в подписи гасит `IS_OWNER` в `ghRunSub`, а рука и ховер-тень переехали с голого `.stat-card` на `.stat-card[data-href], .stat-card[data-goto]` — иначе неинтерактивная плитка притворяется ссылкой. Внутренние переходы «Парсеры»/«Импорты» на свою вкладку у оператора остаются. Дальше — вкладки:
- **#system**: **полоса «Запуск прогона» во всю ширину** (`.run-bar`, вся `data-owner-only`) над сеткой карточек — после удаления «Полного прогона» карточкой в сетке она читалась обрубком (122px рядом с 367px «Здоровья»). Ниже — `.system-grid` на `repeat(auto-fit, minmax(320px,1fr))`: число видимых карточек переменное (у оператора одна, у владельца одна-две — «Иски банка» скрыта на 404), и фиксированные колонки при любом выборе давали дыру; `.system-grid > .card { min-width:0; max-width:700px }` — потолок ОБЩИЙ, не операторский (`:only-child` не сработает: скрытые соседи остаются в DOM). Правило `.system-grid{1fr}` из вилки 769–1024 удалено — именно оно делало страницу на 1000px длиннее, чем на 1280. Полоса содержит ОДНУ кнопку «▶ Запустить прогон» (`smart_skip:"true"`, ровно как ежедневный крон; в нерабочий день спрашивает «прогнать всё равно?» и добавляет `ignore_calendar:"true"` — признак дня приходит с сервера полем `today_non_working` в `/admin/gh-runs`, своей копии календаря у страницы нет) → POST `/admin/dispatch`, рядом метка следующего автозапуска. **Кнопка «Полный прогон» (`smart_skip:"false"`) удалена 02.08.2026** (решение юриста): тяжёлый обход всех дел запускается из GitHub Actions (Run workflow → снять галку `smart_skip`); белый список `DISPATCH_WORKFLOWS` не менялся. **Список последних 8 runs и блок живого лога УДАЛЕНЫ 29.07.2026** (решение юриста) — статусы/логи смотрятся на вкладке Actions GitHub; GET `/admin/gh-runs` (Worker проксирует GitHub API, PAT на сервере; отдаёт `next_cron_at` с учётом праздников и `today_non_working`) остался — им питаются плитки пульта «Последний прогон» (автообновление каждые 15 с пока прогон идёт) и «Автозапуск» | карточка «Здоровье парсеров» из [data/parse_health.json](data/parse_health.json): светофор-точки (красный fail_streak≥3/alerted_zero; жёлтый fail_streak≥1 или ноль при медиане≥1), спарклайны, проблемные вверху, первые 8 + свёрток; имена судов — карта `COURT_NAMES` (синхронизировать при правке `FIRST_INSTANCE_COURTS`) | карточка «Парсинг исков банка» (с 29.07.2026, обе роли) из `data/bank_parse_report.json`: пер-кейсовый итог последнего прогона по bank-треку — свёртка «По судам» первой (`bpCourtsFoldHtml`: строка на суд, проблемные сверху, раскрыта только при сбоях — иначе лежащий суд тонул бы среди сотен рутинных строк), дальше группы по исходам (ошибки загрузки/без карточки/вне очереди раскрыты; спарсено и пропуски по ритму ИЛ / будущим заседаниям свёрнуты), внутри группы порции по 30 строк (`BP_CHUNK`, «Показать ещё» — на Урале дел тысячи), `case_status` пилюлей только в группе «Спарсено» (в пропусках это данные прошлого прогона), русские причины считает Python (`skip_reason_ru`/`_OUTCOME_RU` в bank_report.py), **404 → карточка скрыта (территория без трека), прочие ошибки → блок «не загрузилось» с «Повторить»** (до 02.08.2026 пряталась при любом `!r.ok`, и 502 от Pages выглядел как отсутствие трека).
- **#llm**: топ-5 рейтинга shir-man (браузером напрямую, CORS `*`; с 02.08.2026 в свёртке с **ленивой** загрузкой — по раскрытию или при выборе провайдера openrouter: раньше внешний запрос уходил на каждый заход и каждое «Обновить») + мини-форма запуска `test_digest.yml` через POST `/admin/dispatch`: провайдер, модель (подписи «топ-N» обогащаются рейтингом), галки to_group/push_all/full_llm/commit_results (по умолчанию ВЫКЛ — безопасный прогон в личку; при опасных галках — confirm). У Codex — выбор модели (haiku эталон / Sonnet 5 / Opus 4.8, `CLAUDE_MODEL` через input `claude_model`; кэш пересказов не-haiku неймспейсится по модели) и уровня усилий (`claude_effort` → env `CLAUDE_EFFORT` → `output_config.effort`; селектор виден только для sonnet/opus — haiku эффорт не поддерживает). ⚠ Модели нового поколения (Opus 4.7+/Sonnet 5) не принимают `temperature` (400) — пейлоад собирает `llm._claude_payload`: adaptive-мышление + effort вместо температуры, расширенный max_tokens и таймаут; боевой haiku-путь байт-в-байт прежний.
- **#subs**: счётчик + **поиск по подпискам** (имя/устройство/номера и стороны дел watchlist) + карточки. **С 02.08.2026 карточка — `<details>`, свёрнута по умолчанию** (8 подписок: ~430px вместо 1983px = 76% страницы). Свёрнутая строка: имя, устройство, ★ owner, «⏳ истекает», «⚠ N» сирот ПО ЭТОЙ подписке (`subOrphanCount`), справа «N дел» и бейдж варианта push. ⚠️ Класс `.sub-card` остаётся на самом `<details>`, `data-endpoint` не переезжает — на них завязаны `btn.closest(".sub-card")` и `flash()`. ⚠️ **В `<summary>` не должно быть кнопок** (клик по кнопке переключал бы свёртку, а `<button>` внутри `<summary>` — вложенный интерактив): пять действий переехали в тело. Состояние раскрытия — `subsOpen` вне DOM (`#root` перерисовывается на каждое нажатие в поиске и после `render(true)`); ⚠️ пишется по **клику на `summary`**, а НЕ по событию `toggle`: Chrome шлёт toggle и при парсинге `<details open>`, гард по таймеру ненадёжен (эти задачи дренируются позже `setTimeout(0)`), и авто-раскрытые поиском карточки записывались как «раскрытые вручную». Поиск раскрывает найденное только при выдаче ≤3 — иначе буква «а» развернула бы всех. В развёрнутом теле — kv-строка дат, свёртки «Последний push» (бейдж варианта; из [data/last_personal_pushes.json](data/last_personal_pushes.json); skip = «нет событий по watchlist») и «Дела» с бейджами стадий, сторонами и судом. Карта дел строится из cases.json **и cases_archive.json** (с 13.07): звезда на завершённом деле — бейдж «в архиве» (в модалке Watchlist такая строка видна с галкой, снять можно; при реактивации дела звезда оживает), номер-сирота (нет ни в активных, ни в архиве — дело удалено вручную или переименовано до Этапа 3) — бейдж «нигде не найдено» + крестик-удаление прямо в строке; счётчик «⚠ N нигде не найдено» — в сводке шапки И бейджем в заголовке секции (сводка шапки скрыта на ≤768px — с телефона сироты иначе не видны). Периодический read-only аудит — [scripts/audit_watchlists.py](scripts/audit_watchlists.py). Данные плитки «Дайджест» — из [data/last_digest.json](data/last_digest.json).

**Достоверность и состояния (02.08.2026).** Плитка «Дайджест» разбирает сводку по ИМЕНОВАННЫМ частям (`digestSummaryParts`: «Новых»/«Изменений»/«Переходов») и показывает оба числа — прежний `match(/\d+/)` брал ПЕРВОЕ число строки «🆕 Новых: 4 · 📋 Изменений: 6» и подписывал его словом «изменений», то есть каждое утро печатал число новых дел под чужой подписью. **С 13.08.2026 значение плитки не может раздуть ряд пульта:** разбор находит части только в КРОНОВОЙ сводке (`push_summary`, runs.py), а replay (`test_digest.yml` с галкой публикации) пишет в `summary` полную сводку дайджеста на девять частей — фолбэк печатал её целиком, и grid тянулся по самой высокой плитке (инцидент 13.08.2026, 9 строк). Теперь текстовый фолбэк — отдельный `.tile-text` с клампом в две строки и полной строкой в `title`, плюс потолок `max-height:3.4em` у самого `.stat-value`. ⚠️ Потолок считать НЕ по `line-height:1.15`: `.tile-part` — inline-flex с baseline-выравниванием подписи, его строчный бокс ≈1.55em, и на телефоне три числа (они переносятся в два ряда) подрезались бы. Формат replay-сводки не трогаем — та же строка идёт телом push'а, там подробность к месту. Клик по плиткам «Дайджест» и «Последний прогон» ведёт наружу (`data-href` + `DASHBOARD_URL`/`html_url` прогона), а не скроллит в `#system`, где о них нет ни строки; ссылка внутри `<button>` не используется — вложенный интерактив ловил бы клик дважды. Сбой загрузки везде отличим от «данных нет»: общий `loadErrorHtml` (человеческий текст + «Повторить», исключение — в `title`/`console`), плитка при сбое — серый «?» (не янтарь: он занят под «N парсеров ⚠»); `loadImportLog` при ошибке рисует её вместо вечного «Загрузка…». Вспышки-ошибки (`setFlash`) НЕ гаснут по таймеру — закрываются крестиком, иначе код сбоя («× endpoint мёртв (410)») исчезал через 5 с. Возврат на вкладку (`visibilitychange`) освежает статику Pages, если с последней загрузки прошло >10 мин (`loadStaticData` — health/bank/digest; `/admin/data` и `/admin/import-log` НЕ трогаем, KV); «Обновить» блокируется с `aria-busy` до `Promise.allSettled`. Журнал импортов с 10.08.2026 запрашивается на ВСЕХ территориях (история точечных добавлений нужна и без капчёвых судов — один `?logonly=1`-list на загрузку вкладки; до этого ХМАО не запрашивал его вовсе).

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
- ⚠️ **Mine-версия фильтрует ГОТОВЫЙ HTML** (`filterGeneralHtmlByMine`, app.js), а не контекст: контекст читается только ради списка новых дел (`collectNewCaseNumbers`). Отсюда два инварианта, оба нарушались до 13.08.2026 (секция «🏦 ИСКИ БАНКА» показывалась целиком, мимо звёзд). **(1) Наборы эмодзи `SECTION_HEADER_RE`/`SECTION_FILTERED_RE`/`SECTION_GROUPING_RE` держать наравне с `_DIGEST_HEADER_RE`** ([postprocess.py](scripts/court_monitor/digest/postprocess.py)): незнакомый заголовок не сбрасывает состояние машины, и секция наследует режим предыдущей — так 🏦, 📑 «Касс. события» и «⚖️🔬 КАССАЦИЯ» наследовали `'new'` от «📥 Новые дела» апелляции. Альтернатива `⚖️🔬` обязана стоять ПЕРЕД одиночным `⚖` (после него идёт 🔬, а не `<b>`); 📌📊📋 — только в HEADER: футер «📌 В производстве…» обязан закрывать секцию банка, иначе выпадет как «параграф без номера». **(2) Матчинг зеркалит `_fi_change_matches`** (delivery.py): `mineRefMatches` проверяет bare-номер И composite «домен|номер», домен берётся из href той же ссылки (`caseRefsInFragment`) — звезда иска банка хранится ТОЛЬКО composite-формой, и одна починка регекспов выкинула бы все звёздные bank-дела. Тем же предикатом считается `found` в `buildMineHtml` (иначе bank-only совпадение даёт ложный фолбэк «показан общий дайджест»). Новые иски банка (`fi_bank_claim_registered`) проходят как «новые дела» без звезды — **только на дашборде**: в push они остаются по watchlist (решение юриста — на Урале авто-подхват заводит десятки исков за прогон). Счётчик заголовка пересчитывается по факту (`retitleSectionHeader`). Стражи — [scripts/tests/test_frontend_mine_digest.py](scripts/tests/test_frontend_mine_digest.py).
- **Пилюля «★ Мои» в шапках дайджеста и «Ближайших заседаний»** (v171, решение юриста 17.08.2026): выбранный раздел был виден только по чипу вверху, а обе карточки молчали, чей это список. Общий хелпер `mineScopePillHtml` (класс `.mine-scope-pill`, значок — `scopeMineIcon()`: эмодзи и «★» в бейдж не ставить, системный цветной шрифт выпадает из палитры тёмной темы; цвет — токеном `--amber-700`, объявленным в ОБЕИХ темах). ⚠️ Шапку дайджеста рисует `renderDigestTitle` и зовётся она из ДВУХ мест — `loadLastDigest` (дата) и `setDigestView` (режим): заголовок строится при загрузке, а раздел переключается позже, и без второго вызова пилюля появлялась бы только после перезагрузки страницы. Пилюля идёт по РЕЖИМУ, а не по числу найденных дел: при пустом watchlist mine-версия честно откатывается на общий дайджест с плашкой, но раздел выбран — гейт по находкам выглядел бы поломкой (у юриста ноль звёзд). Стражи — [scripts/tests/test_frontend_mine_pills.py](scripts/tests/test_frontend_mine_pills.py).

## Календарный фид «Мои заседания» (webcal, 29.08.2026)

Персональная подписка календаря телефона/Outlook на заседания дел из
watchlist: `GET /calendar/<token>.ics` на Worker'е, клиент поллит ссылку сам
(дублей не бывает по построению — события синхронизируются по стабильным
UID, переносы обновляют, отпавшие исчезают). Кнопки — в модалке синка 🔗
(`calFeedBlockHtml` в app.js, обе ветки: связанное устройство и нет).

- **Токен**: `profile_id` — bearer-секрет, в URL ему не место → у фида СВОЙ
  read-only `feed_token` (второй `crypto.randomUUID()` в объекте профиля) +
  индекс KV `calfeed:<token>` → `{profile_id}` **без TTL**. Выдача —
  `POST /profile/calendar-token` (auth `profile_id` в body; без него создаёт
  профиль из `body.watchlist`, зеркало link-code → фронт зовёт с
  `ensureWorkerHost` + `failover:false`); идемпотентен, `regenerate:true` —
  перевыпуск (старый индекс удаляется, старая ссылка умирает). ⚠️ Токен
  пишется через `putProfile`, НЕ `writeProfileWatchlist` — `updated_at`
  принадлежит watchlist'у (LWW).
- **Фид** (`handleCalendarFeed`): 2 KV reads на поллинг, 0 writes/lists —
  free-tier не задевается. Пустой watchlist → валидный ПУСТОЙ VCALENDAR
  (200, подписка не битая); недоступный cases.json → **503 + Retry-After**
  (пустой ответ стёр бы события у подписчика). Bank-трек тянется вторым
  fetch'ем только при композитных канонах «домен|номер» в наборе. Заголовки:
  `text/calendar; charset=utf-8`, `Cache-Control: private, max-age=900`.
- **ICS**: UID = `<canon>--<stage>@<host>` (canon = bare id — стабилен при
  переезде дела между стадиями; stage разводит FI/апелляцию/кассацию; host —
  территории), DTSTAMP стабильный (производный от DTSTART, не `Date.now()`),
  `DTSTART;TZID` + DTEND +1 ч (all-day без времени), LOCATION = суд +
  `events[].place` (кабинет/зал; у bank-трека events ленивые — без кабинета),
  DESCRIPTION с судьёй/сторонами/ссылкой на карточку суда
  (`calBuildCourtLink` — мини-порт buildCourtLink). Отбор — упрощённое
  зеркало фронта (`calCaseIncluded`): дата ≥ сегодня в TZ территории,
  «приостановлено»/«без движения» — мимо. Свёртка строк по **75 октетов**
  (`icsFold`, кириллица 2 байта — code point не режется), склейка CRLF.
- **TZ/имя** — `CAL_TZID`/`CAL_TZ_OFFSET_MIN`/`CAL_FEED_NAME` через `cfgVar`
  (дефолты Asia/Yekaterinburg +300/«Мои заседания» — обе территории +05:00;
  форк с другим поясом меняет только wrangler.toml).
- Фронт — «один тап» (29.08.2026): в модалке 🔗 ОДНА умная кнопка
  `subscribeCalendar` — сама добывает токен (идемпотентный
  `/profile/calendar-token`) и сразу открывает календарь с диалогом подписки:
  Apple (`calIsApplePlatform`) → `webcal:`-переход, остальные →
  `calFeedGoogleUrl` (calendar.google.com/calendar/render?cid=…,
  `window.open` + фолбэк `location.href` — попап-блокер после await);
  сервисный ряд (копия/Outlook/перевыпуск/показ ссылки в `<details>`) —
  только при выданном токене; кнопка «Outlook» (`addCalToOutlook`) открывает
  веб-Outlook личных ящиков Microsoft с заполненной формой подписки
  (`outlook.live.com/calendar/0/addfromweb` — корпоративный Exchange в
  закрытом контуре до фида не достучится). `CAL_FEED_TOKEN_KEY` (`lsKey('cal_feed_token')`) — только
  кэш, источник истины — профиль; ссылка строится от **PUSH_WORKER_URL** (не
  от sticky-фолбэка — живёт в календаре месяцами); `clearProfileLink` чистит
  и токен, а `setProfileLink` сбрасывает кэш при смене profile_id (связка
  кодом переводит устройство на чужой профиль — иначе модалка показывала бы
  рабочую ссылку покинутого профиля со старым watchlist). Стражи —
  [scripts/tests/test_calendar_feed.py](scripts/tests/test_calendar_feed.py).

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
  ⚠️ **Бамп сносит ТОЛЬКО код.** С 15.08.2026 (v170) `data/*.json` живут в
  неверсионированном `DATA_CACHE` и деплой переживают — не приписывать ему
  `${CACHE_VERSION}` «для единообразия»: именно версионирование данных давало
  белый экран и 4 демо-дела офлайн (правка чаще раза в день = обнуление
  офлайн-датасета). Имя обязано не оканчиваться на `-v<цифры>` — иначе его
  съест `ownVersion` в `activate`. Подробно — [docs/technical/08-фронтенд.md](docs/technical/08-фронтенд.md),
  раздел «Офлайн: разведённые жизненные циклы кэшей».

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
