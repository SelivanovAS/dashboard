# CLAUDE.md

Карта проекта для новых сессий — чтобы не тратить токены на разведку.

## Что это

Дашборд юриста ПАО Сбербанк: мониторинг гражданских дел в 20 судах ХМАО-Югры (первая инстанция) + апелляция, AI-дайджесты в Telegram, автозапуск через Cloudflare Worker cron → GitHub Actions. Пользователь — юрист банка, общение на русском.

## Главные файлы

- [scripts/update_cases.py](scripts/update_cases.py) — **монолит** (~231 KB): парсеры судов, LLM-дайджесты, Telegram, CLI.
- [scripts/add_cases_manually.py](scripts/add_cases_manually.py) — ручное добавление дел 1-й инстанции.
- [scripts/migrate_csv_to_json.py](scripts/migrate_csv_to_json.py) — одноразовая миграция CSV→JSON (выполнена).
- [data/cases.json](data/cases.json) — активные дела (UTF-8, `version: 1`, `updated_at` ISO).
- [data/cases_archive.json](data/cases_archive.json) — архив.
- `data/.digested_acts` — дедуп уже обработанных судебных актов (скрытый файл).
- [data/last_digest_context.json](data/last_digest_context.json) — снимок контекста для `--replay-last`.
- [data/sberbank_cases.csv](data/sberbank_cases.csv) + архив — legacy CSV (UTF-8 с BOM), всё ещё коммитится для совместимости.
- [app.js](app.js) + [sberbank_dashboard.html](sberbank_dashboard.html) + [styles.css](styles.css) — SPA-фронт (GitHub Pages).
- [cloudflare-worker/wrangler.toml](cloudflare-worker/wrangler.toml) + [cloudflare-worker/worker.js](cloudflare-worker/worker.js) — автозапуск.
- [.github/workflows/update_cases.yml](.github/workflows/update_cases.yml) — основной workflow (парсинг + дайджест + commit).
- [.github/workflows/digest_only.yml](.github/workflows/digest_only.yml), [digest_only_gigachat.yml](.github/workflows/digest_only_gigachat.yml), [force_postponement_digest.yml](.github/workflows/force_postponement_digest.yml) — ручные дайджесты.
- [README.md](README.md) — подробная документация на русском (дублирует часть этого файла).

## Ключевые точки в update_cases.py

| Что | Где |
|-----|-----|
| `APPEAL_COURT` (конфиг апелляции) | [scripts/update_cases.py:106](scripts/update_cases.py:106) |
| `FIRST_INSTANCE_COURTS` (массив 20 `CourtConfig`) | [scripts/update_cases.py:114](scripts/update_cases.py:114) |
| `DIGESTED_ACTS_PATH` | [scripts/update_cases.py:155](scripts/update_cases.py:155) |
| Константы state-machine (`FI_ARCHIVE_DAYS` и т.д.) | [scripts/update_cases.py:171](scripts/update_cases.py:171) |
| `advance_case_stage` / `is_case_archived` / `migrate_stages` | [scripts/update_cases.py:421](scripts/update_cases.py:421) |
| `class TableExtractor(HTMLParser)` — парсер карточек дела | [scripts/update_cases.py:599](scripts/update_cases.py:599) |
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
      "current_stage": "first_instance" | "awaiting_appeal" | "appeal" | "cassation_watch" | "cassation_pending",
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
      "cassation_pending_since": "YYYY-MM-DD"  // если перешли в cassation_pending
    }
  ]
}
```

## Автозапуск

- Cron `"45 3 * * 1-5"` = **6:45 МСК пн-пт** в [cloudflare-worker/wrangler.toml:6](cloudflare-worker/wrangler.toml:6).
- Worker вызывает `workflow_dispatch` для `update_cases.yml` через GitHub API (нужен `GITHUB_PAT`).
- **Автозапуск = Cloudflare Worker, НЕ cron-job.org.** Любые правки расписания — в `wrangler.toml`, потом `wrangler deploy`.

## Жизненный цикл дела (state machine)

Пять рабочих стадий в `current_stage` + архив. Переходы — в
`advance_case_stage()`, архивация — в `is_case_archived()`.

| Стадия | Что парсим | Что запускает переход |
|---|---|---|
| `first_instance` | карточка 1-й инст. | подана апел. жалоба → `awaiting_appeal` · 45 дней от hearing_date без жалобы → архив |
| `awaiting_appeal` | ничего (жалоба подана, ждём карточку в апел. суде) | link_cases находит апел. карточку → `appeal` · бессрочно, не архивируется |
| `appeal` | карточка апел. суда | опубликован акт ИЛИ 30 дней от апел. заседания без акта → `cassation_watch` · не архивируется по времени |
| `cassation_watch` | карточка 1-й инст. (ищем касс. жалобу) | касс. жалоба или направление в кассац. суд → `cassation_pending` · 120 дней от апел. заседания → архив |
| `cassation_pending` | ничего (будет парсер кассации) | не архивируется по времени |

Константы в [scripts/update_cases.py:171](scripts/update_cases.py:171):
`FI_ARCHIVE_DAYS=45`, `APPEAL_NO_ACT_GRACE_DAYS=30`,
`CASSATION_WATCH_DAYS=120`.

`migrate_stages()` идемпотентно подтягивает старые записи (до появления
state-machine) под новую модель при каждом запуске.

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

- **Telegram:** все workflow'и шлют в личный чат (`TELEGRAM_CHAT_ID_TEST`) по умолчанию. Чтобы продублировать в корпоративную группу — поставить галку `to_group` в UI Run workflow.
- **PWA push:** `update_cases.yml` (крон) шлёт всем подписчикам PWA. Тестовые workflow'и (`digest_only.yml`, `digest_only_gigachat.yml`, `force_postponement_digest.yml`) шлют push **только устройствам-владельцам**, чтобы не спамить коллегам прототипами. Чтобы пометить своё устройство владельцем — открыть PWA по URL `https://selivanovas.github.io/dashboard/sberbank_dashboard.html?owner=<OWNER_SECRET>` (один раз).

## Соглашения

- **Язык:** весь код, переменные, комментарии, промпты — **на русском**.
- **Коммиты:** `EMOJI описание на русском`. Примеры:
  - `📊 Обновление данных 23.04.2026 03:52` — автоматический от workflow.
  - `Дайджест: ...`, `Карточка: ...`, `GigaChat: ...` — правки скрипта.
- **Telegram HTML:** только `<b>`, `<i>`, `<a href>`. Лимит 4096 символов на сообщение, дайджест режется автоматически (целевой объём ~7600).
- **JSON:** UTF-8 без BOM, `version: 1`, `updated_at` ISO.
- **CSV:** UTF-8 с BOM, legacy-формат, по-прежнему коммитится.
- **Дедупликация актов:** через `.digested_acts` — не обрабатывать акт дважды.

## Чего НЕ делать

- Не коммитить секреты (`.env`, ключи API, `GITHUB_PAT`).
- Не переименовывать поля в `cases.json` без миграции — завязан фронт (`app.js`) и архив.
- Не добавлять cron-job.org / аналоги — автозапуск только через Cloudflare Worker.
- Не ломать структуру промптов в `generate_digest` / `GIGACHAT_SYSTEM_PROMPT` без предупреждения: пользователь долго их настраивал (см. `git log` по этим функциям).
- Не амендить опубликованные коммиты — создавать новые.

## Когда всё-таки нужна разведка

Если задача касается:
- Конкретного парсера одного суда — читать `CourtConfig` в `FIRST_INSTANCE_COURTS`.
- Логики парсинга таблиц → `TableExtractor` ([scripts/update_cases.py:599](scripts/update_cases.py:599)).
- Фронтенда (фильтры, рендер) → [app.js](app.js).
- Конкретного workflow → соответствующий `.github/workflows/*.yml`.

Иначе — этой карты достаточно, не нужно запускать Grep/Glob с нуля.
