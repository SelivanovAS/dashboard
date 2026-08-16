# Автозапуск парсинга на Mac (вариант D2) — ⏳ ВРЕМЕННОЕ решение

> **Это временная схема.** В будущем парсинг переедет на сервер (российский
> VPS: точечный прокси для судов либо полный перенос прогона), после чего всё
> из этого каталога демонтируется по разделу «Откат» ниже, а расписание
> вернётся в инфраструктуру.

> 💤 **СТАТУС на 05.07.2026 — спящий резерв.** Суды снова пускают US-IP GitHub
> (блок 02.07 сняли, проверено `.github/workflows/probe_courts.yml`), автозапуск
> вернулся в облако (Cloudflare Worker cron → `update_cases.yml`). Эта Mac-схема **усыплена**, но
> не демонтирована — держим как запасной путь на случай возврата блока. Команды
> усыпить/разбудить — в разделе [«Резерв: усыпить/разбудить»](#резерв-усыпитьразбудить).
> Ниже описание для случая, когда схема активна.

Суды (`*.sudrf.ru`) режут TLS с иностранных IP, поэтому GitHub Actions (США)
больше не может парсить суды. Этот Mac физически в сети Сбера (выход в интернет
российский) → **парсинг делаем здесь**, а тяжёлый Claude-дайджест и рассылку
оставляем на GitHub (там Claude доступен).

```
┌─ Mac (по будням) ──────────────┐        ┌─ GitHub Actions ───────────────┐
│ parse_and_push.sh:             │  push  │ replay_on_push.yml:            │
│  • маршрут судов мимо VPN       │ ─────► │  • видит новый                 │
│  • update_cases.py --json      │  git   │    last_digest_context.json    │
│  • git commit + push           │        │  • --replay-last --push-all    │
│    (БЕЗ секретов)               │        │  • Claude-дайджест → Telegram  │
└────────────────────────────────┘        │    + Web Push всем подписчикам │
                                           └────────────────────────────────┘
```

Секретов на Mac нет. Доставка при локальном прогоне сама пропускается, дайджест
получается шаблонным (его выбрасывают) — важен только сохранённый **контекст**,
из которого GitHub соберёт настоящий дайджест.

## Что уже готово в репозитории

- `parse_all.sh` — драйвер территорий: гоняет обёртку по каждому клону
  **подряд**. Его и зовёт LaunchAgent.
- `parse_and_push.sh` — обёртка одного клона (preflight, маршрут, парсинг,
  commit/push). Путь к клону — первый аргумент или env `CM_REPO`.
- `run_parse.py` — запуск `main_json` без секретов (глушит `validate_environment`,
  иначе `exit(2)`); обёртка зовёт его.
- `com.court-monitor.parse.plist` — LaunchAgent (будни 08:00 местного, +05).
- `court-monitor-route.sudoers` — правило sudo для команды `route`.
- `../stage_data_files.sh` — какие файлы данных коммитить. Список ОБЩИЙ с
  облаком: он спрашивает пути у `court_monitor.config`. Прежде список вёлся
  руками и здесь, и в `update_cases.yml`, и они молча разъехались — резерв не
  коммитил семь файлов трека «Иски банка» (трек появился 25.07.2026, уже после
  того, как резерв усыпили).

## Настройки машины (вне репозитория)

Каталог `~/.config/court-monitor/`. Всё необязательное: нет файла — прежнее
поведение.

| Файл | Зачем |
|---|---|
| `territories` | Пути к клонам, по строке; `#` — комментарий. Нет файла → только `~/dashboard`. |
| `env.<регион>` | Переменные территории (`BANK_INTAKE_MAX_PER_RUN=200` и т.п.). В облаке это Actions Variables, на Mac их взять неоткуда — без файла подхват идёт с дефолтами кода. |
| `telegram` | `token=…` и `chat_id=…` — 🚨-алерт при падении прогона. Без него только уведомление на экране, а прогон идёт в 08:00. |
| `progress_token` | Токен живого лога прогона в админку Worker'а. |

Пример `territories`:

```
/Users/aleksandrselivanov/dashboard
/Users/aleksandrselivanov/dashboard-ural
```

Клон второй территории ставится как обычный клон форка, плюс обязательно
`git config merge.ours.driver true` (без него merge из эталона затрёт файлы
территории). Регион клон определяет сам — по файлу `REGION` в его корне.

## Установка (делается один раз)

Предполагается репозиторий в `/Users/aleksandrselivanov/dashboard`. Зависимости
(`requests`, `pywebpush`) уже стоят в пользовательском Python 3.9.

### 1. Разрешить добавление маршрута без пароля

Маршрут судов мимо VPN требует root. Чтобы launchd не завис на вводе пароля —
кладём узкое правило sudoers:

```bash
cd /Users/aleksandrselivanov/dashboard
sudo cp ops/mac-local-run/court-monitor-route.sudoers /etc/sudoers.d/court-monitor-route
sudo chmod 440 /etc/sudoers.d/court-monitor-route
sudo visudo -c          # должно вывести «parsed OK» по всем файлам
```

### 2. Поставить LaunchAgent

```bash
cp ops/mac-local-run/com.court-monitor.parse.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.court-monitor.parse.plist
launchctl list | grep court-monitor        # убедиться, что загрузился
```

Время запуска — **08:00 по местному** (Ханты-Мансийск, +05). Поменять — в plist
(`Hour`/`Minute`), затем `launchctl unload … && launchctl load …`.

### 3. Отключить старый автозапуск через Cloudflare Worker

Иначе GitHub каждое утро будет пытаться (и не мочь) спарсить суды и слать 🚨.
В `cloudflare-worker/wrangler.toml` уже проставлено `crons = []`; осталось
задеплоить:

```bash
cd /Users/aleksandrselivanov/dashboard/cloudflare-worker
wrangler deploy
```

### 4. Проверить GitHub-секреты

Workflow `replay_on_push.yml` использует те же секреты, что и `update_cases.yml`
(`ANTHROPIC_API_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID_TEST`,
`PUSH_WORKER_URL`, `PUSH_SECRET`, `VAPID_PRIVATE_KEY`) — они уже есть, ничего
добавлять не нужно.

## Проверка

```bash
# Диагностика без публикации: сеть, маршруты, доступность судов.
# Парсинг и push не запускаются, рабочее дерево не трогается.
bash ops/mac-local-run/parse_all.sh --check
```

```bash
# Полный ручной прогон всех территорий (как это сделает launchd):
bash ops/mac-local-run/parse_all.sh
tail -f ops/mac-local-run/parse_and_push.log
```

`--check` — первое, что стоит запускать из офиса: он показывает, сколько
судебных IP получилось замаршрутизировать. Должно быть **21 у ХМАО и 67 у
Урала**; если единицы — маршруты строятся не по реестру (так и было до
16.08.2026, когда домены искались регекспом по `courts.py` и находились шесть
строк из комментариев).

Ожидаемо: `Сеть Сбера подтверждена` → маршруты → `Парсинг завершён` → коммит →
`Запушено`. На GitHub появится коммит «📊 Обновление данных …», следом
отработает `replay_on_push` и в личный Telegram придёт Claude-дайджест.

Прогон из-под самого launchd (без ожидания 08:00):

```bash
launchctl start com.court-monitor.parse
```

## Как это себя ведёт

- **Best-effort по времени.** LaunchAgent срабатывает в залогиненной сессии.
  Если в 08:00 Mac спал/выключен — прогон случится при пробуждении/входе. День,
  когда Mac не открывали в сети Сбера, останется без дайджеста.
- **Не в сети Сбера** (из дома, другой Wi-Fi) → обёртка покажет уведомление
  «Пропуск: не в сети Сбера» и ничего не сделает. VPN при этом можно не трогать
  — для парсинга он не нужен (а если поднят, маршрут судов его обходит).
- **Праздники РФ** пропускаются автоматически (`SKIP_NON_WORKING_DAYS=1`).

## Живой просмотр парсинга

**На Mac — ярлык «Парсинг судов.command»** (копия лежит на рабочем столе,
оригинал здесь в каталоге). Двойной клик → открывается окно: текущий (или
последний) прогон **с самого начала** + живое продолжение. Прокрутка вверх —
прошлые прогоны. Закрыть окно — ⌘W, на парсинг не влияет.

**Из браузера/с телефона — блок «🛰 Парсинг» в админке подписчиков**
(`https://court-monitor-trigger.7selivanov-a.workers.dev/admin?secret=<OWNER_SECRET>`).
Обёртка запускает `progress_pusher.py`: он читает лог, фильтрует вехи
(апелляция, каждый суд, кассация, ошибки, финал) и раз в ~5 с шлёт их на
`POST /run-progress` Worker'а. Блок в админке автообновляется, пока прогон
не завершён; хранится также предыдущий прогон.

Токен для отправки вех: `~/.config/court-monitor/progress_token` (chmod 600,
**вне репозитория** — репо публичный). То же значение должно лежать в секрете
Worker'а: `wrangler secret put PROGRESS_SECRET`. Нет токена → вехи просто не
шлются, парсинг работает как обычно.

## Логи

- `parse_and_push.log` — основной лог обёртки (парсинг, маршруты, git).
  Ротация автоматическая: при >4000 строк остаются последние 2000.
- `launchd.out.log` / `launchd.err.log` — что видит launchd.

## Резерв: усыпить/разбудить

С 05.07.2026 основной автозапуск — в облаке (крон в `update_cases.yml`), а эта
схема держится как **спящий резерв** (файлы и плист на месте, LaunchAgent
выгружен). Это НЕ откат — ничего не удаляем.

**Усыпить** (перевести в резерв — плановый Mac-прогон больше не стартует):

```bash
launchctl unload ~/Library/LaunchAgents/com.court-monitor.parse.plist
launchctl list | grep court-monitor   # должно быть пусто
```

**Разбудить** (если суды снова закрыли иностранные IP — см. «Процедура флипа» в
CLAUDE.md). ⚠️ Сначала со стороны облака: **отключи Worker-крон** (верни
`crons = []` в `cloudflare-worker/wrangler.toml` и `wrangler deploy`) и **включи
дайджест-на-push** (в `.github/workflows/replay_on_push.yml` верни
`if: github.actor != 'github-actions[bot]'` вместо `if: false`, коммит). Затем на Mac:

```bash
launchctl load ~/Library/LaunchAgents/com.court-monitor.parse.plist
launchctl list | grep court-monitor   # должен появиться
```

Проверить, что блок судов действительно вернулся, можно заранее: Actions → workflow
`🔬 Проба доступа к судам с GitHub (US-egress)` → Run workflow.

## Откат (полный демонтаж — не путать с усыплением)

```bash
launchctl unload ~/Library/LaunchAgents/com.court-monitor.parse.plist
rm ~/Library/LaunchAgents/com.court-monitor.parse.plist
sudo rm /etc/sudoers.d/court-monitor-route
```

Вернуть автозапуск через Worker — раскомментировать `crons` в `wrangler.toml`
и `wrangler deploy`.
