# Автозапуск парсинга на Mac (вариант D2)

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

- `parse_and_push.sh` — обёртка (preflight, маршрут, парсинг, commit/push).
- `run_parse.py` — запуск `main_json` без секретов (глушит `validate_environment`,
  иначе `exit(2)`); обёртка зовёт его.
- `com.court-monitor.parse.plist` — LaunchAgent (будни 08:00 местного, +05).
- `court-monitor-route.sudoers` — правило sudo для команды `route`.

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
# Ручной прогон обёртки (как это сделает launchd):
bash ops/mac-local-run/parse_and_push.sh
tail -f ops/mac-local-run/parse_and_push.log
```

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

## Логи

- `parse_and_push.log` — основной лог обёртки (парсинг, маршруты, git).
- `launchd.out.log` / `launchd.err.log` — что видит launchd.

## Откат

```bash
launchctl unload ~/Library/LaunchAgents/com.court-monitor.parse.plist
rm ~/Library/LaunchAgents/com.court-monitor.parse.plist
sudo rm /etc/sudoers.d/court-monitor-route
```

Вернуть автозапуск через Worker — раскомментировать `crons` в `wrangler.toml`
и `wrangler deploy`.
