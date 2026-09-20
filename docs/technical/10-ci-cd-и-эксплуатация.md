# 10. CI/CD и эксплуатация

## Что это и зачем

Этот документ — для того, кто **запускает, обслуживает и чинит** систему: какие
есть режимы запуска, какие переменные окружения нужны, как устроены GitHub
Actions, какие есть вспомогательные скрипты и тесты, и что делать, когда
что-то сломалось (рантбук).

Схема сверена с рабочим деревом 13.09.2026. Это проверка кода и конфигураций
репозитория; состояние установленных systemd-служб, LaunchAgents, Secrets,
Actions Variables и опубликованных Workers проверяется отдельно.

## Режимы запуска (CLI)

`update_cases.py` выбирает режим по флагу в `sys.argv`
([блок `__main__` в update_cases.py](../../scripts/update_cases.py)). Любое необработанное
исключение оборачивается в `send_crash_alert` → уходит в Telegram.

| Команда | Функция | Что делает |
|---------|---------|-----------|
| `--json` | `main_json` ([runs.py](../../scripts/court_monitor/runs.py)) | **Полный прогон**: парсинг + JSON + дайджест + рассылка при настроенном транспорте. Публикацию Git выполняет launcher/workflow. `--smart-skip` (env `SKIP_NON_WORKING_DAYS`) пропускает нерабочие дни и дела с известной будущей датой. |
| _(без флага)_ | `main` ([runs.py](../../scripts/court_monitor/runs.py)) | Legacy CSV-прогон (апелляция). |
| `--digest-only` | `main_digest_only` ([runs.py](../../scripts/court_monitor/runs.py)) | Только дайджест по текущим данным, без парсинга. |
| `--replay-last [--push-all]` | `main_replay_last` ([runs.py](../../scripts/court_monitor/runs.py)) | Переиграть последний дайджест из `last_digest_context.json` с актуальным промптом. Push — владельцу (или всем при `--push-all`). |
| `--push-last-digest [--owner-only]` | `main_push_last_digest` ([runs.py](../../scripts/court_monitor/runs.py)) | Повторно разослать уже сохранённый дайджест. |
| `--push-web-only [--push-all]` | `main_push_web_only` ([runs.py](../../scripts/court_monitor/runs.py)) | Только web push по сохранённому контексту, без перегенерации дайджеста и Telegram. Вторая половина `replay_on_push.yml` — после публикации на Pages. |
| `--backfill-appeal-anchors` | `main_backfill_appeal_anchors` ([runs.py](../../scripts/court_monitor/runs.py)) | Разовый бэкфилл якорей УИД/номеров из апел. карточек. |

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
| `LLM_PROVIDER` | `claude` / `gigachat` / `openrouter`. Python по умолчанию использует `claude`; `update_cases.yml` и `replay_on_push.yml` — Actions Variable либо `openrouter`. |
| `DIGEST_FULL_LLM`, `DIGEST_POLISH` | Переключатели режима дайджеста (см. [06](06-дайджесты-и-llm.md)). |
| `SKIP_NON_WORKING_DAYS` | `1` → smart-skip; утренний launcher задаёт его явно, ручной workflow — по входу `smart_skip`. |
| `FETCH_MAX_RETRIES` | Потолок попыток одного логического запроса. Дефолт 1; launcher VPS/Mac ставит 3, но повтор разрешает только точная fast-policy (`connection_reset`/ошибка ответа/5xx до 5 с). |
| `FETCH_TIMEOUT_CONNECT`, `FETCH_TIMEOUT_READ` | Таймаут соединения/чтения, дефолты 10/65 с. |
| `CARD_BREAKER_MODE` | `time` для полного утреннего прогона; `count` для коротких batch-импортов. Не меняет `FETCH_MAX_RETRIES`. |
| `CARD_BREAKER_*_THRESHOLD`, `CARD_BREAKER_*_COOLDOWN_SECONDS` | Порог и cooldown по семантической семье: fast 3/60 с, outage 2/180 с, slow 2/300 с, block 2/600 с. Подробно — [04](04-сбор-данных-и-парсеры.md). |
| `LOG_LEVEL` | Уровень логов (`DEBUG`/`INFO`/`WARNING`/`ERROR`, по умолч. `INFO`). `DEBUG` включает пер-кейсовые skip-строки, «без изменений», полные списки не-HMAO судов и диагностику нераспарсенных дат. |
| `JSON_PATH`, `CSV_PATH`, `DIGESTED_ACTS_PATH`, `CASSATION_ACTS_PATH`, `PARSE_HEALTH_PATH`, … | Переопределение путей к файлам данных. |

В GitHub Actions задаются через **Settings → Secrets and variables → Actions**.

`validate_environment` ([runs.py](../../scripts/court_monitor/runs.py)) проверяет
наличие ключей на старте; `check_court_available` ([runs.py](../../scripts/court_monitor/runs.py))
— доступность сайта суда.

## Ежедневный прогон: VPS и Mac-резерв

Основной исполнитель — VPS; Mac использует общие скрипты как резерв. Клоны
выбираются из `~/.config/court-monitor/territories` (либо `CM_TERRITORIES_FILE`).
Код поддерживает ХМАО, Урал и Башкортостан. Подключение третьего VPS-клона и
ограничения Mac-резерва зафиксированы в [отчёте внедрения Башкортостана](../regions/Башкортостан_внедрение_2026-09-11.md);
это датированный результат, а не проверка текущей доступности судов или таймеров.

`parse_all.sh` готовит общие маршруты на Mac и запускает отдельный
`parse_and_push.sh` для каждого клона. Урал идёт первым, остальные — в порядке
списка территорий с интервалом 600 секунд по умолчанию. На VPS с 20.09.2026
интервал по умолчанию переопределён на 300 секунд, а `CM_PARALLEL_START_DELAYS`
задаёт Урал → ХМАО → Башкортостан и Тюмень как 0/5/10 минут.
Первые старты — 06:00/06:05/06:10/06:10 по Екатеринбургу (UTC+5);
настройка — в [VPS README](../../ops/vps-run/README.md).
Каждый клон имеет свои Git, данные, журнал
и `.run.lock`; ошибка соседней территории не отменяет остальные попытки.

С 13.09.2026 VPS разделяет три операции. `court-parse.timer` запускает парсинг
по будням с 06:00 до 08:30 каждые полчаса и в 08:45. После завершения службы,
в том числе с ошибкой, systemd отдельно запускает `court-import.service`:
`CM_IMPORTS_AFTER_PARSE=0` исключает ожидание импортов внутри `parse_all.sh`.
С 20.09.2026 дампы и одиночные добавления обрабатываются ежедневно:
новые отметки проверяются круглосуточно каждые пять минут, страховочные
слоты — 12:00/14:00/16:00/18:00/20:00, включая выходные и праздники.
`court-delivery.timer` независимо проверяет готовые выпуски с 08:45 до конца
дня. Точное расписание, связи служб, установка и восстановление — в
[VPS README](../../ops/vps-run/README.md).

На Mac отдельный таймер доставки не устанавливается имеющимися LaunchAgents.
Драйвер ждёт все парсеры, выполняет финальную доставку, затем последовательно
обрабатывает импорты. Если первая проверка окна была раньше 08:45, после
импортов он проверяет время ещё раз. Оба исполнителя используют
`parse_and_push.sh --deliver-pending`: без запросов к судам и повторного парсинга,
с восстановлением транзакций, pull и проверкой `cloud_run_ok.py --can-deliver`.
Готов сегодняшний ещё не закрытый выпуск завершённого прогона, в том числе
без новых событий. Процент прочитанных карточек влияет на отчёт, но не
запрещает доставку. Занятый `.run.lock` сериализует парсинг, импорт и доставку
внутри клона; два хоста с копиями одного региона этот lock не синхронизирует.

Публикация состоит из двух транзакций: сначала данные и контекст, затем
`delivered_at` и отдельный marker-коммит с `(Mac-парсинг)` — также на VPS.
Журнал доставки сверяет marker SHA с remote: подтверждённый коммит не
повторяется, отсутствующий допускает условный rollback, неопределённый исход
сохраняется для восстановления. Принятый Git-коммит запускает GitHub replay;
сам по себе он не доказывает успешную рассылку или обновление Pages.

Preflight и `network_fingerprint.py` проверяют реальные страницы поиска и
карточки. HTTP 200 с защитой или заглушкой не считается чтением дела.
Территориальный парсер имеет бюджет 3300 секунд: после его истечения новые
HTTP-запросы не начинаются, прочитанные данные сохраняются, попытка завершается
как `deadline_reached`. Логи, telemetry, переключатели параллельного режима и
диагностические команды — в [README общих скриптов / Mac](../../ops/mac-local-run/README.md).

## GitHub Actions

Ниже — основные workflow из [`.github/workflows/`](../../.github/workflows).
Запуск парсинга, replay и `test_digest.yml` имеет внешние последствия;
`test_digest.yml` отправляет Telegram даже при `commit_results=false`.

### `replay_on_push.yml` — дайджест по факту публикации VPS/Mac
[Файл](../../.github/workflows/replay_on_push.yml). Триггер — `push` в `main`,
задевший `data/last_digest_context.json`, с маркером `Mac-парсинг` в сообщении
последнего коммита и автором не `github-actions[bot]`. Шаги:
checkout → Python 3.12 → `python scripts/update_cases.py --replay-last
--push-all` со всеми секретами и `DEFER_WEB_PUSH=1` (гибридный дайджест в
личный Telegram; **web push здесь НЕ уходит**) → коммит `last_digest.json`,
`cases.json`, `cases_bank.json` и `cases_bank_events.json` при наличии
(анализ актов обоих треков), `.act_summaries.json` (кэш пересказов),
с `git pull --rebase` от гонки с публикацией исполнителя (`📰 Дайджест собран…`) → **ожидание
публикации на Pages** (до ~7 мин поллинга публичного URL, критерий — sha256
отданных байт `data/last_digest.json` равен закоммиченным; таймаут = warning и
отправка без подтверждения; при неудавшемся rebase шаг пропускается,
`pushed=0`) → **web push отдельным шагом** `--push-web-only --push-all`
(`main_push_web_only`: тот же контекст + эхо-фильтр + персонализация, дайджест
не перегенерируется) → докоммит `last_personal_pushes.json` (журнал пишется
отправкой, то есть после основного коммита). Порядок введён 26.08.2026:
очередь Pages из трёх пушей доставочного слота публиковала дайджест на ~5 мин
позже пуша, и клик по уведомлению открывал вчерашний выпуск. Откат — снимать
`DEFER_WEB_PUSH` и оба новых шага ПАРОЙ (иначе двойной пуш); стражи —
[`test_replay_push_after_pages.py`](../../scripts/tests/test_replay_push_after_pages.py).
Анти-петля: replay не меняет сам контекст, а пуши через `GITHUB_TOKEN` не
триггерят workflow.

**По умолчанию дайджест гибридный:** программный рендер
`generate_template_digest` + LLM-пересказ мотивировок актов, с программным
линтером и алертами. Провайдер в workflow — `vars.LLM_PROVIDER` либо
`openrouter`; фактические Variables территории проверяются отдельно. Полный
LLM-дайджест включается через `DIGEST_FULL_LLM=1`; подробности — в [06](06-дайджесты-и-llm.md).

### `update_cases.yml` — ручной и аварийный полный прогон
[Файл](../../.github/workflows/update_cases.yml). Триггер — `workflow_dispatch`
из GitHub UI либо Worker. В эталонном `wrangler.toml` сейчас `CRON_UTC = ""`:
регулярный парсинг задают VPS-таймеры. Выпуск Worker 15.09.2026 содержит
единственный служебный cron `17 21 * * *` для очистки профилей; его ветка
завершается до GitHub dispatch, а `PROFILE_CLEANUP_ENABLED` включает только
обслуживание профилей. Код и настройки трёх Workers проверены после
[выкладки 15.09.2026](../Выпуск_очистки_профилей_2026-09-15.md). Первый плановый
проход ещё не выполнялся на момент проверки. Правила и восстановление — в
[главе Worker](09-cloudflare-worker.md#очистка-неактивных-профилей-15092026).

Шаги: checkout → Python 3.12 → зависимости →
`python scripts/update_cases.py --json 2>&1 | python -u scripts/gh_progress_pusher.py`
→ публикация данных → при падении 🚨-алерт в Telegram. `set -o pipefail`
сохраняет ошибку парсера. Пушер передаёт вехи на `POST /run-progress` примерно
раз в 60 секунд; адрес берётся из `secrets.PUSH_WORKER_URL`, токен —
`secrets.PUSH_SECRET || secrets.PROGRESS_SECRET`. Без настроек он работает
как pass-through и сообщает о выключенном канале в логе; первый сбой POST
также выводится в лог. Канал хранится в KV; карточка «Ход последнего
прогона» удалена из админки 01.09.2026, вехи доступны через
`GET /admin/run-progress` с авторизацией владельца или оператора.

Входы: `to_group` (слать в корпоративную группу; иначе личный чат через
`TELEGRAM_CHAT_ID_TEST`), `smart_skip` (`true`: пропуск
нерабочих дней и дел с известной будущей датой; `false` = полный прогон),
`ignore_calendar` (прогнать в выходной/праздник, сохранив пер-кейсовый
smart-skip → env `IGNORE_NON_WORKING_DAY`).

С 03.07.2026 дайджест и здесь гибридный (флаг `DIGEST_FULL_LLM` снят, дефолт
кода); откат — вернуть `DIGEST_FULL_LLM: "1"` в env шага.

Коммит-шаг использует общий [`ops/stage_data_files.sh`](../../ops/stage_data_files.sh):
он получает пути из регионального `config`, включая данные, архивы и события
обоих треков, контекст и дайджест, историю push, кэши и журнал здоровья.
Не заменять его отдельным неполным списком файлов. Сообщение коммита —
`📊 Обновление данных ДД.ММ.ГГГГ ЧЧ:ММ`.

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

Оба каталога pytest собираются одним прогоном —
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

CI (`tests.yml`) гоняет тот же набор на push, кроме изменений только Markdown/`docs/**`,
и по ручному запуску. Число тестов и пропусков фиксируют по результату конкретной
проверки; `skipped` не подтверждает поведение. Для правки только документации
достаточно сверки фактов, ссылок и `git diff --check`.

## Наблюдаемость

- `log_run_summary` ([delivery.py](../../scripts/court_monitor/delivery.py)) — итоговая
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
  Сургутский горсуд) — Timeout…»). **Пер-судовые тайминги:** фаза поиска 1-й
  инст. (5/9) пишет время каждого суда в пер-судовую строку, фаза карточек
  1-й инст. (6/9) — строку «1 инст: медленные суды — …»
  (топ-3 по времени обхода карточек, включая ретраи).
- **Атомарный checkpoint сети и breaker:** launcher VPS/Mac пишет
  `ops/mac-local-run/.runtime/parse_telemetry.json` на старте/фазах/HTTP-
  попытках и переходах breaker. В `current.breaker` видны точные классы,
  cooldown, half-open пробы, сколько отложено/дочитано/осталось; завершённый
  итог дублируется в `data/parse_health.json → last_run.breaker`. Судебного
  HTML и полных URL карточек в checkpoint нет. История не ограничена тремя
  запусками: в файле остаются все попытки текущего дня, а `daily.network`
  агрегирует исходы по хостам, `daily.recovery` — открытия/пробы/восстановления.
  Дневное покрытие строится как union стабильных ID
  `planned_case_ids_today` / `read_case_ids_today` по 1-й инстанции,
  апелляции и кассации, поэтому планы дочиток можно сравнивать напрямую.
- `send_crash_alert` ([delivery.py](../../scripts/court_monitor/delivery.py)) — падение
  прогона уходит в Telegram, чтобы не потеряться в логах Actions. Дублируется
  шагом `if: failure()` в самом workflow (ловит и падения до старта Python).
- **Детектор молчаливой поломки парсеров** (шаг 4e `main_json`, история в
  `data/parse_health.json`) — 🩺-алерт в Telegram, когда суд, стабильно
  дававший результаты, вернул 0; когда страница поиска не грузится 3 прогона
  подряд; когда все источники разом по нулям; когда за прогон ≥5
  карточек-«огрызков»; когда поиск непомеченного суда закрылся проверочным
  кодом (🔐 с рецептом, напоминание раз в день, ✅ при снятии — 04.09.2026).
  На VPS/Mac Python без токена — строки детектора едут через
  `last_run.alerts` и shell-канал `parse_and_push.sh`. См. [05](05-конвейер-обновления.md).
- Логи VPS — `journalctl` служб и `ops/mac-local-run/parse_and_push.log`
  каждого клона; логи GitHub — во вкладке Actions соответствующего workflow.
  `scripts/gh_progress_pusher.py` и `ops/mac-local-run/progress_pusher.py`
  передают вехи в KV своего Worker. В текущей админке карточки живого лога
  нет; статус даёт плитка «Последний прогон», а вехи доступны через
  `GET /admin/run-progress`.
- На Mac лог открывается через пульт «СберСуд-пульт.command» или совместимый
  ярлык «Парсинг судов.command». Пушер использует `worker.<регион>` и
  `progress_token.<регион>`; общий `progress_token` поддержан только для
  ХМАО/Урала. Новая территория без своего адреса и токена не отправляет вехи
  в ХМАО. Подробности — в [настройках Mac/VPS](../../ops/mac-local-run/README.md#настройки-машины-вне-репозитория).

## Рантбук (типичные инциденты)

| Симптом | Вероятная причина и что делать |
|---------|-------------------------------|
| **Дайджест не пришёл в Telegram** | Проверить завершение доставки VPS, наличие marker-коммита и результат `replay_on_push.yml`, затем `TELEGRAM_BOT_TOKEN`/`*_CHAT_ID` и ошибки транспорта в логе. Для отказа анализа актов отдельно проверить выбранного LLM-провайдера и его ключ. |
| **7kas: «Данных по запросу не обнаружено»** | Изменились параметры запроса. Проверить вручную на 7kas; не менять `delo_id=2800001`/`delo_table=g33_case`/`new=2800001` без проверки (см. [04](04-сбор-данных-и-парсеры.md)). |
| **Парсер суда вернул мало/0 дел** | Суд сменил вёрстку или временно недоступен. С июля 2026 об этом сам сообщит 🩺-алерт детектора (история в `parse_health.json`). Сравнить карточку на сайте с ожиданиями парсера; обновить фикстуру и тест. |
| **Push не приходят** | На локали push выключен (нет `VAPID_PRIVATE_KEY`). В проде: проверить secrets Worker'а, что устройство в подписках (`/subscriptions`), watchlist. |
| **Дашборд показывает старую версию** | Забыт cache-bust. Инкрементить `?v=N` в HTML и `CACHE_VERSION` в `service-worker.js` синхронно (см. [08](08-фронтенд.md)). |
| **Появились дубли дел** | Сработает один из `dedupe_*` щитов на следующем прогоне (см. [05](05-конвейер-обновления.md)); если нет — `find_cassation_orphans.py` + ручной мердж. |
| **Дело пропало из дашборда** | Ушло в архив по тайм-ауту (см. [03](03-жизненный-цикл-дела.md)). При поздней жалобе реактивируется автоматически (≤180 дн); старше года — вернуть через `add_cases_manually.py`. |
| **Watchlist «звёзды» на чужих/несуществующих делах** | Запустить `audit_watchlists.py`, почистить через админку (см. [09](09-cloudflare-worker.md)). |
| **Утром нет дайджеста (нет и 🚨)** | На VPS проверить `court-parse.timer`, `court-delivery.timer`, журналы служб и `.run.lock` нужного клона; готовность выпуска и незавершённые транзакции — по [VPS README](../../ops/vps-run/README.md). Затем проверить marker-коммит и replay в Actions. При работе резерва проверить LaunchAgent, сон и сеть Mac. |
| **С Mac суды недоступны (таймауты)** | Маршрут мимо VPN слетел/битый после смены IP — обёртка пересоздаёт его сама; если руками: `sudo route -n delete -host 84.42.111.139; sudo route -n add -host 84.42.111.139 10.217.111.250`. Проверить, что сеть — Сбера (`netstat -rn`, шлюз `10.217.111.250`). |
| **Параллельный Mac-слот ведёт себя неожиданно** | Смотреть `launchd.out.log` драйвера и `ops/mac-local-run/parse_and_push.log` в **каждом** клоне. Импорты не начнутся, пока жив хоть один парсер. Для быстрого отката следующих слотов: `launchctl setenv CM_PARALLEL_TERRITORIES 0`; вернуть штатно — `launchctl unsetenv CM_PARALLEL_TERRITORIES`. |
| **Канал `/run-progress` молчит** | Проверить в логе включение пушера и Worker своей территории. VPS/Mac: `worker.<регион>` и `progress_token.<регион>`; GitHub: `PUSH_WORKER_URL` и `PUSH_SECRET`/`PROGRESS_SECRET`. HTTP 401 — авторизация, 403 может быть защитой до Worker, 404 — адрес. Парсинг не зависит от успешной передачи вех. |
| **Прогон был, а дайджест не пришёл** | Проверить, был ли доставочный marker `(Mac-парсинг)` после 08:45: черновой push данных его не содержит. Если marker принят, смотреть `replay_on_push.yml`, его условия запуска, шаги Telegram и Web Push. `delivered_at` сам по себе не доказывает доставку адресату. |
| **Автозапуск через Worker (если вернули cron)** | Проверить Cloudflare Worker (cron, `GITHUB_PAT`), `isHoliday`, логи Worker'а. Расписание — `wrangler.toml` + `wrangler deploy`. |

## Чего НЕ делать

- Не коммитить секреты (`.env`, ключи, `GITHUB_PAT`, `progress_token`).
- Не амендить опубликованные коммиты — создавать новые.
- Не переименовывать поля `cases.json` без миграции (завязан фронт и архив).
- Не добавлять сторонние планировщики (cron-job.org и т.п.). Расписание —
  systemd на VPS; резерв — LaunchAgent на Mac. Worker-cron включается только
  при согласованной смене исполнителя вместе с `CRON_UTC` и проверкой deploy.
- Не редактировать `data/last_digest_context.json` руками в `main` — push,
  задевший этот файл с доставочным маркером, запускает боевую рассылку
  (`replay_on_push.yml`).
