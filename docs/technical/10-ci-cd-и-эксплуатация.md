# 10. CI/CD и эксплуатация

## Что это и зачем

Этот документ — для того, кто **запускает, обслуживает и чинит** систему: какие
есть режимы запуска, какие переменные окружения нужны, как устроены GitHub
Actions, какие есть вспомогательные скрипты и тесты, и что делать, когда
что-то сломалось (рантбук).

## Режимы запуска (CLI)

`update_cases.py` выбирает режим по флагу в `sys.argv`
([__main__, 13611](../../scripts/update_cases.py#L177)). Любое необработанное
исключение оборачивается в `send_crash_alert` → уходит в Telegram.

| Команда | Функция | Что делает |
|---------|---------|-----------|
| `--json` | `main_json` ([1837](../../scripts/court_monitor/runs.py#L1837)) | **Основной прогон**: парсинг + JSON + дайджест + рассылка + коммит. Запускается кроном. `--smart-skip` (env `SKIP_NON_WORKING_DAYS`) пропускает нерабочие дни и дела с известной будущей датой. |
| _(без флага)_ | `main` ([1041](../../scripts/court_monitor/runs.py#L1041)) | Legacy CSV-прогон (апелляция). |
| `--digest-only` | `main_digest_only` ([4631](../../scripts/court_monitor/runs.py#L4631)) | Только дайджест по текущим данным, без парсинга. |
| `--replay-last [--push-all]` | `main_replay_last` ([4315](../../scripts/court_monitor/runs.py#L4315)) | Переиграть последний дайджест из `last_digest_context.json` с актуальным промптом. Push — владельцу (или всем при `--push-all`). |
| `--push-last-digest [--owner-only]` | `main_push_last_digest` ([4492](../../scripts/court_monitor/runs.py#L4492)) | Повторно разослать уже сохранённый дайджест. |
| `--backfill-appeal-anchors` | `main_backfill_appeal_anchors` ([1300](../../scripts/court_monitor/runs.py#L1300)) | Разовый бэкфилл якорей УИД/номеров из апел. карточек. |

```bash
# Полный боевой прогон локально
python3 scripts/update_cases.py --json

# Переиграть последний дайджест
python3 scripts/update_cases.py --replay-last

# Зависимости
pip install -r scripts/requirements.txt   # requests, pywebpush
```

## Переменные окружения

| Переменная | Назначение |
|------------|-----------|
| `ANTHROPIC_API_KEY` | Claude (генерация/пересказ). |
| `GIGACHAT_AUTH_KEY` / `GIGACHAT_*` | GigaChat (альтернативный LLM, `LLM_PROVIDER=gigachat`). |
| `OPENROUTER_API_KEY` / `OPENROUTER_MODEL` | OpenRouter (`LLM_PROVIDER=openrouter`); пустая модель = «модель дня» с shir-man.com, fallback `openrouter/free`. |
| `TELEGRAM_BOT_TOKEN` | Токен бота. |
| `TELEGRAM_CHAT_ID` | Корпоративная группа (только при `to_group=true`). |
| `TELEGRAM_CHAT_ID_TEST` | Личный чат — дефолтный получатель. |
| `TELEGRAM_CHAT_ID_PERSONAL` | Личный чат юриста (workflow'ы передают тот же `TELEGRAM_CHAT_ID_TEST`): при совпадении с `TELEGRAM_CHAT_ID` к Telegram-дайджесту добавляется приписка «🤖 LLM: …» — какая модель делала дайджест. В группу приписка не уходит. |
| `PUSH_WORKER_URL`, `PUSH_SECRET`, `VAPID_PRIVATE_KEY` | Web Push для PWA. |
| `OWNER_SECRET` | Секрет Worker'а для `/mark-owner` и админки. |
| `GITHUB_PAT` | В secrets Worker'а — для `workflow_dispatch`. |
| `LLM_PROVIDER` | `claude` (по умолч.) / `gigachat`. |
| `DIGEST_FULL_LLM`, `DIGEST_POLISH` | Переключатели режима дайджеста (см. [06](06-дайджесты-и-llm.md)). |
| `SKIP_NON_WORKING_DAYS` | `1` → smart-skip (передаёт крон). |
| `LOG_LEVEL` | Уровень логов (`DEBUG`/`INFO`/`WARNING`/`ERROR`, по умолч. `INFO`). `DEBUG` включает пер-кейсовые skip-строки, «без изменений», полные списки не-HMAO судов и диагностику нераспарсенных дат. |
| `JSON_PATH`, `CSV_PATH`, `DIGESTED_ACTS_PATH`, `CASSATION_ACTS_PATH`, `PARSE_HEALTH_PATH`, … | Переопределение путей к файлам данных. |

В GitHub Actions задаются через **Settings → Secrets and variables → Actions**.

`validate_environment` ([996](../../scripts/court_monitor/runs.py#L996)) проверяет
наличие ключей на старте; `check_court_available` ([1028](../../scripts/court_monitor/runs.py#L1028))
— доступность сайта суда.

## Ежедневный прогон (временная схема D2, с 03.07.2026)

> ⚠️ **Временное решение.** Суды `*.sudrf.ru` дропают TLS с иностранных IP →
> GitHub Actions больше не может их парсить; Claude, наоборот, недоступен из РФ.
> Поэтому парсинг выполняет **Mac юриста** (LaunchAgent
> `com.court-monitor.parse`, будни ~08:00 местного, сеть Сбера), а дайджест и
> доставку — GitHub по факту push'а. **В будущем парсинг переедет на сервер
> (RU VPS)**, и эта секция будет переписана. Установка/логи/откат Mac-звена —
> [`ops/mac-local-run/README.md`](../../ops/mac-local-run/README.md).

Цепочка: `parse_and_push.sh` (Mac: preflight сети → маршрут судов мимо VPN →
`run_parse.py` = `main_json` без секретов → коммит `📊 Обновление данных …
(Mac-парсинг)` → push) → `replay_on_push.yml` (GitHub: Claude-дайджест +
Telegram + Web Push). Ход парсинга виден в ярлыке «Парсинг судов.command» на
Mac и в блоке «🛰 Парсинг» админки Worker.

## GitHub Actions

Пять workflow в [`.github/workflows/`](../../.github/workflows).

### `replay_on_push.yml` — дайджест по факту Mac-парсинга (прод)
[Файл](../../.github/workflows/replay_on_push.yml). Триггер — `push` в `main`,
задевший `data/last_digest_context.json` (его коммитит Mac-обёртка). Шаги:
checkout → Python 3.12 → `python scripts/update_cases.py --replay-last
--push-all` со всеми секретами (гибридный дайджест в личный Telegram + Web
Push всем подписчикам) → коммит `last_digest.json`, `last_personal_pushes.json`,
`cases.json` (act_analysis из replay) и `.act_summaries.json` (кэш пересказов),
с `git pull --rebase` от гонки с Mac-пушем (`📰 Дайджест собран…`). Анти-петля:
replay не меняет сам контекст, а пуши через `GITHUB_TOKEN` не триггерят
workflow.

**С 03.07.2026 дайджест здесь — гибрид** (дефолт кода, флаг не выставлен):
программный рендер `generate_template_digest` + Claude только на пересказ
мотивировок актов; после отправки — программный линтер с 🩺-алертом. Откат
на старый полный LLM-дайджест — вернуть `DIGEST_FULL_LLM: "1"` в env шага.

### `update_cases.yml` — основной (cron Worker'а, с 05.07.2026 снова в облаке)
[Файл](../../.github/workflows/update_cases.yml). Триггер — `workflow_dispatch`:
его дёргает cron Cloudflare Worker'а (пн-пт 06:45 МСК, `smart_skip=true`),
вручную — GitHub UI или админка (кнопки «Полный прогон» / «Стандартный
прогон»). Шаги: checkout → Python 3.12 → установка зависимостей →
`python scripts/update_cases.py --json 2>&1 | python -u
scripts/gh_progress_pusher.py` (pass-through-пушер лога в KV Worker'а —
блок живого лога из админки удалён 29.07.2026, канал остался без
UI-читателя: батчи раз в ~60 с на `POST /run-progress` — каждый POST = 1 KV-write,
free-tier 1000/день на аккаунт; `set -o pipefail`, чтобы падение
парсера не маскировалось пайпом; env `PROGRESS_URL` =
`secrets.PUSH_WORKER_URL + "/run-progress"`, `PROGRESS_TOKEN` =
`secrets.PUSH_SECRET || secrets.PROGRESS_SECRET` — без секретов пушер
работает как cat, но с 16.07.2026 объявляет об этом одной строкой в логе
рана; первый сбой POST тоже печатает одну ⚠️-строку с HTTP-кодом. Пушер шлёт
собственный `User-Agent` — дефолтный `Python-urllib/…` Cloudflare банит на
workers.dev (ошибка 1010 → 403 до Worker'а), из-за чего канал молчал
13–16.07.2026) → коммит данных → (при падении любого шага) 🚨-алерт в
личный Telegram.

Входы: `to_group` (слать в корпоративную группу; иначе личный чат через
`TELEGRAM_CHAT_ID_TEST`), `smart_skip` (cron передаёт `true`: пропуск
нерабочих дней и дел с известной будущей датой; `false` = полный прогон).

С 03.07.2026 дайджест и здесь гибридный (флаг `DIGEST_FULL_LLM` снят, дефолт
кода); откат — вернуть `DIGEST_FULL_LLM: "1"` в env шага.

Коммит-шаг добавляет: `cases.json`, `cases_archive.json`, `cases_archive_*.json`
(холодные), `last_digest_context.json`, `last_digest.json`,
`last_personal_pushes.json`, legacy CSV, `.digested_acts`, `.cassation_acts`,
`parse_health.json`, `.act_summaries.json` (кэш пересказов).
Сообщение коммита — `📊 Обновление данных ДД.ММ.ГГГГ ЧЧ:ММ`.

Алерт о падении сделан через `curl` (не Python) — сработает, даже если упала
установка зависимостей; текст содержит ссылку на лог упавшего run'а.

### `tests.yml` — тесты на каждый push
[Файл](../../.github/workflows/tests.yml). Триггер — любой push (кроме правок
только `.md`/`docs/`) + ручной запуск. Ставит зависимости + pytest и гоняет
весь набор (`python -m pytest`). Прогоняется и на автокоммитах данных — это
осознанно: baseline-тесты дайджеста рендерят свежий
`data/last_digest_context.json`, так что регрессия рендера на реальных данных
всплывёт на следующее утро.

### `test_digest.yml` — ручной тест
[Файл](../../.github/workflows/test_digest.yml). Не парсит — переигрывает
последний дайджест (`--replay-last`). Входы: `to_group`, `push_all` (push всем,
иначе только владельцу), `full_llm` (`DIGEST_FULL_LLM=1` — старый полный
LLM-вариант вместо гибрида), `llm_provider` (выпадающий список
claude/gigachat/openrouter), `gigachat_model` (GigaChat-2-Pro (дефолт) /
GigaChat-2 / GigaChat-2-Max), `openrouter_model` (место в рейтинге бесплатных
моделей shir-man.com: «модель дня (топ-1)» (дефолт) … «топ-5» — конкретный id
резолвится на прогоне из свежего рейтинга, статичный список в YAML не
протухает) и `llm_model` (произвольный id текстом — перебивает оба списка).
Модельная переменная уходит в `GIGACHAT_MODEL`/`OPENROUTER_MODEL` — читает
только активный провайдер. Публикация результатов (`last_digest.json`, `cases.json`, кэш
пересказов) и PWA push — только по галке `commit_results` (по умолчанию
выключена: тестовый прогон не публикует ничего на дашборд и не шлёт пуш —
пуш вёл бы на неопубликованный дайджест; остаётся только Telegram).
Технически пуш гасится пустыми `PUSH_*`-переменными в env шага —
`send_web_push` при них тихо пропускает отправку.
(Workflow `digest_only_gigachat.yml` удалён 09.07.2026 — его роль теперь
выполняет выбор провайдера здесь.)

### Деплой Cloudflare Worker
Не через Actions, а вручную: `cd cloudflare-worker && wrangler deploy`. См.
[09. Cloudflare Worker](09-cloudflare-worker.md).

## Вспомогательные скрипты

| Скрипт | Назначение |
|--------|-----------|
| [`add_cases_manually.py`](../../scripts/add_cases_manually.py) | Добавить дела 1-й инстанции в `cases.json` по списку `(court_domain, case_number)` — для дел, не попавших в авто-выборку (старые / банк-истец / возврат из холодного архива). |
| [`audit_watchlists.py`](../../scripts/audit_watchlists.py) | Аудит подписок: находит в watchlist'ах номера дел, которых нет в активном `cases.json`. Пишет отчёт, **ничего не меняет**. Запуск: `OWNER_SECRET=… python3 scripts/audit_watchlists.py`. |
| [`find_cassation_orphans.py`](../../scripts/find_cassation_orphans.py) | Находит discovery-дубли кассации (эвристика: тот же суд/судья/ответчик). Печатает отчёт, не пишет в JSON. |
| [`generate_icon.py`](../../scripts/generate_icon.py) | Генерация иконок PWA (squircle Sber green + «§»). Требует Pillow. |
| [`refresh_doc_anchors.py`](../../scripts/refresh_doc_anchors.py) | Переанкеровка ссылок на строки кода в docs/technical и CLAUDE.md после правок модулей `court_monitor`: `symbol` рядом со ссылкой → актуальные файл и строка `def`/`class` (символ ищется по всем модулям пакета — переезд функции между модулями чинится автоматически). Dry-run по умолчанию, `--write` — применить. Главу 05 не трогает (она якорит места вызовов внутри `runs.main_json`, а не def). |

## Тесты

`pytest`, 228 тестов (июль 2026). Оба каталога собираются одним прогоном —
конфиг [`pytest.ini`](../../pytest.ini) (для этого у `scripts/` есть
`__init__.py`: пакеты `scripts.tests` и `tests` не конфликтуют именами).

- [`scripts/tests/test_parsing.py`](../../scripts/tests/test_parsing.py)
  + [`scripts/tests/fixtures/`](../../scripts/tests/fixtures) — парсеры на
  зафиксированных HTML-снимках карточек; state machine; линковка
  (`link_cases`, `link_cassation_cases`, `relink`), реактивация и ротация
  архива, детектор здоровья парсеров, дедуп кассационных определений.
  Главный страховочный слой для хрупких парсеров: добавляя обработку нового
  кейса суда, кладите фикстуру и тест.
- [`tests/test_digest_render.py`](../../tests/test_digest_render.py) —
  программный рендер и пост-обработка дайджеста; baseline-тесты гоняются на
  реальном `data/last_digest_context.json`.
- [`scripts/tests/test_versions.py`](../../scripts/tests/test_versions.py) —
  синхронность версий cache-bust (`?v=N` ↔ `CACHE_VERSION`).

```bash
python3 -m pytest
```

CI (`tests.yml`) гоняет тот же набор на каждый push.

## Наблюдаемость

- `log_run_summary` ([785](../../scripts/court_monitor/delivery.py#L785)) — итоговая
  сводка прогона (тайминги, счётчики `METRICS`: запросы, Telegram, Web Push,
  LLM-пересказы актов (вызовы/из кэша), карточки-«огрызки»; нулевые строки
  опускаются) + markdown-таблица в `$GITHUB_STEP_SUMMARY`.
- **Группы и аннотации GitHub Actions** (`court_monitor/ghlog.py`): при env
  `LOG_GH_ANNOTATIONS=1` (ставят `update_cases.yml` / `test_digest.yml` /
  `replay_on_push.yml`) фазы прогона сворачиваются в `::group::`, а
  WARNING/ERROR дублируются аннотациями `::warning::`/`::error::` — видны в
  панели Annotations. Гейт именно env-флагом, а не `GITHUB_ACTIONS`: pytest в
  `tests.yml` не должен плодить аннотации. Логи пишутся в stdout (не stderr) —
  workflow-команды GitHub читаются из stdout.
- **Контекст в сетевых ошибках:** `fetch_page(url, context=...)` — ретрай-WARNING
  и финальный ERROR содержат номер дела/имя суда («Попытка 2/3: host (2-716/2025,
  Сургутский горсуд) — Timeout…»). **Пер-судовые тайминги:** фаза 3 пишет время
  каждого суда в пер-судовую строку, фаза 5 — строку «1 инст: медленные суды — …»
  (топ-3 по времени обхода карточек, включая ретраи).
- `send_crash_alert` ([895](../../scripts/court_monitor/delivery.py#L895)) — падение
  прогона уходит в Telegram, чтобы не потеряться в логах Actions. Дублируется
  шагом `if: failure()` в самом workflow (ловит и падения до старта Python).
- **Детектор молчаливой поломки парсеров** (шаг 4e `main_json`, история в
  `data/parse_health.json`) — 🩺-алерт в Telegram, когда суд, стабильно
  дававший результаты, вернул 0; когда страница поиска не грузится 3 прогона
  подряд; когда все источники разом по нулям; когда за прогон ≥5
  карточек-«огрызков». См. [05](05-конвейер-обновления.md).
- Логи прогона — во вкладке Actions соответствующего workflow. Пушер
  `scripts/gh_progress_pusher.py` → `POST /run-progress` по-прежнему кладёт
  лог основного прогона в KV Worker'а (14 дней, текущий + предыдущий), но
  **блок живого лога из админки удалён 29.07.2026** — читателя у канала нет,
  оставлен на случай возврата (данные — `GET /admin/run-progress` руками).
- **Mac-парсинг (спящий резерв):** лог `ops/mac-local-run/parse_and_push.log`
  (ротация автоматическая) — живой просмотр двойным кликом по ярлыку
  «Парсинг судов.command» (рабочий стол юриста); вехи Mac-пушера тоже уходят
  в KV `/run-progress` (в админке больше не видны). Уведомления macOS:
  старт/готово/ошибка/пропуск.

## Рантбук (типичные инциденты)

| Симптом | Вероятная причина и что делать |
|---------|-------------------------------|
| **Дайджест не пришёл в Telegram** | Проверить `ANTHROPIC_API_KEY`, `TELEGRAM_BOT_TOKEN`/`*_CHAT_ID` в secrets; смотреть лог Actions и crash-alert. |
| **7kas: «Данных по запросу не обнаружено»** | Изменились параметры запроса. Проверить вручную на 7kas; не менять `delo_id=2800001`/`delo_table=g33_case`/`new=2800001` без проверки (см. [04](04-сбор-данных-и-парсеры.md)). |
| **Парсер суда вернул мало/0 дел** | Суд сменил вёрстку или временно недоступен. С июля 2026 об этом сам сообщит 🩺-алерт детектора (история в `parse_health.json`). Сравнить карточку на сайте с ожиданиями парсера; обновить фикстуру и тест. |
| **Push не приходят** | На локали push выключен (нет `VAPID_PRIVATE_KEY`). В проде: проверить secrets Worker'а, что устройство в подписках (`/subscriptions`), watchlist. |
| **Дашборд показывает старую версию** | Забыт cache-bust. Инкрементить `?v=N` в HTML и `CACHE_VERSION` в `service-worker.js` синхронно (см. [08](08-фронтенд.md)). |
| **Появились дубли дел** | Сработает один из `dedupe_*` щитов на следующем прогоне (см. [05](05-конвейер-обновления.md)); если нет — `find_cassation_orphans.py` + ручной мердж. |
| **Дело пропало из дашборда** | Ушло в архив по тайм-ауту (см. [03](03-жизненный-цикл-дела.md)). При поздней жалобе реактивируется автоматически (≤180 дн); старше года — вернуть через `add_cases_manually.py`. |
| **Watchlist «звёзды» на чужих/несуществующих делах** | Запустить `audit_watchlists.py`, почистить через админку (см. [09](09-cloudflare-worker.md)). |
| **Утром нет дайджеста (нет и 🚨)** | Проверить, был ли run `update_cases.yml` в Actions (и в плитке «Последний прогон» админки). Не было — смотреть cron Worker'а (`wrangler.toml`, логи Worker'а, `isHoliday`); был, но упал до алерта — лог run'а. Если активен Mac-резерв: Mac спал/не в сети Сбера — LaunchAgent догонит при входе, либо `launchctl start com.court-monitor.parse`. |
| **С Mac суды недоступны (таймауты)** | Маршрут мимо VPN слетел/битый после смены IP — обёртка пересоздаёт его сама; если руками: `sudo route -n delete -host 84.42.111.139; sudo route -n add -host 84.42.111.139 10.217.111.250`. Проверить, что сеть — Сбера (`netstat -rn`, шлюз `10.217.111.250`). |
| **Канал `/run-progress` молчит** (блока в админке больше нет — проверяется `GET /admin/run-progress` руками) | Первым делом — лог рана в Actions: с 16.07.2026 пушер сам печатает одну строку `⚠️ Живой лог админки: POST … (HTTP код)` при первом сбое или `🛰 …выключен` при пустых секретах. 403 = Cloudflare-бан User-Agent (ошибка 1010, лечится `USER_AGENT` пушера — так канал молчал 13–16.07.2026), 401 = секреты (в GitHub нет ни `PUSH_SECRET`, ни `PROGRESS_SECRET`, или не совпадают с Worker'ом), 404 = кривой `PUSH_WORKER_URL`. Для Mac-резерва: нет/пуст токен `~/.config/court-monitor/progress_token`, либо `PROGRESS_SECRET` Worker'а не совпадает. Некритично: прогон работает и без лога. |
| **Прогон был, а дайджест не пришёл** | Смотреть Actions → «💤 Резерв D2: дайджест на push» (`replay_on_push.yml`; стартует только если push задел `last_digest_context.json`). Дальше — как в первой строке таблицы. |
| **Автозапуск через Worker (если вернули cron)** | Проверить Cloudflare Worker (cron, `GITHUB_PAT`), `isHoliday`, логи Worker'а. Расписание — `wrangler.toml` + `wrangler deploy`. |

## Чего НЕ делать

- Не коммитить секреты (`.env`, ключи, `GITHUB_PAT`, `progress_token`).
- Не амендить опубликованные коммиты — создавать новые.
- Не переименовывать поля `cases.json` без миграции (завязан фронт и архив).
- Не добавлять сторонние планировщики (cron-job.org и т.п.). Расписание —
  cron Cloudflare Worker'а; спящий резерв — LaunchAgent на Mac.
- Не редактировать `data/last_digest_context.json` руками в `main` — push,
  задевший этот файл, запускает боевую рассылку (`replay_on_push.yml`).
