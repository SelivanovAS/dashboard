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
      "current_stage": "first_instance" | "appeal",
      "plaintiff": "...", "defendant": "...",
      "bank_role": "Истец|Ответчик|Третье лицо",
      "category": "...", "notes": "...",
      "first_instance": { "court", "judge", "status", "events": [], "resolved_emitted": bool, ... },
      "appeal":         { "court", "status", "result", "events": [], "act_published", ... }
    }
  ]
}
```

## Автозапуск

- Cron `"45 3 * * 1-5"` = **6:45 МСК пн-пт** в [cloudflare-worker/wrangler.toml:6](cloudflare-worker/wrangler.toml:6).
- Worker вызывает `workflow_dispatch` для `update_cases.yml` через GitHub API (нужен `GITHUB_PAT`).
- **Автозапуск = Cloudflare Worker, НЕ cron-job.org.** Любые правки расписания — в `wrangler.toml`, потом `wrangler deploy`.

## Окно архивирования

- Апелляция: 30 дней после решения → архив.
- Первая инстанция: 45 дней (мотивировка может прийти позже).

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
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — отправка дайджеста.
- `GITHUB_PAT` — в secrets Worker'а, для `workflow_dispatch`.
- `DIGESTED_ACTS_PATH` — опционально переопределить путь к `.digested_acts`.

## Соглашения

- **Язык:** весь код, переменные, комментарии, промпты — **на русском**.
- **Коммиты:** `EMOJI описание на русском`. Примеры:
  - `📊 Обновление данных 23.04.2026 03:52` — автоматический от workflow.
  - `🧪 TEST MODE: ...` — тестовый прогон (личный чат вместо группы).
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
