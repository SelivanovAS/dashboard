# VPS-звено (боевой парсинг с 28.08.2026)

Парсинг судов и очередь операторских импортов работают на VPS Cloud.ru
(195.19.66.234, Ubuntu 24.04, egress РФ — sudrf пускает; проба 27.08.2026:
обе территории зелёные, RAM ≤81 МБ из 960). Mac-звено НЕ демонтировано —
это ручной резерв (см. «Откат» ниже).

**С 08.09.2026 очередь импортов — ОСНОВНОЙ исполнитель дампов и точечных
пачек** (решение юриста): Worker с `IMPORT_EXECUTOR = "vps"` в GitHub не
диспатчит, записи ждут `court-import.timer` в статусе `queued` (будни
12:00/14:00/16:00/18:00/20:00 + утренние импорты после парсинга).
**С 09.09.2026 — немедленная попытка**: третий таймер
`court-import-poll.timer` (будни 08:00–20:55 каждые 5 мин) гоняет
`import_poll.sh` — один GET `/import-pending` на территорию (KV get, не list),
при новой отметке сразу `import_dumps.sh <клон> --anywhere`; занятый
`.run.lock` (утренний парсинг/слот) = пропуск тика без отметки. Слоты 12–20 —
страховка для провалов. Слоты
продублированы в `IMPORT_SLOTS_LOCAL` wrangler.toml — админка обещает
оператору именно эти часы; менять только парой (страж
`test_slots_var_mirrors_timer`). `vps_env.sh` экспортирует
`CM_IMPORT_SOURCE=vps` — им подписаны отчёт в журнал («🖥 обработано
сервером»), коммиты «(VPS)» и тексты ошибок.

**Здесь НЕТ своей логики.** `parse_all.sh`/`import_all.sh` — тонкие шимы:
готовят Linux-окружение (`vps_env.sh`) и exec'ают боевые
`ops/mac-local-run/parse_all.sh` / `import_all.sh` с `--anywhere`. Вся
механика (слоты, транзакции доставки, гейт «один дайджест в день», окно
08:45, sweep, очередь импортов) — одна с Mac-резервом, дрейфа нет по
построению. macOS-специфика закрыта снаружи: заглушка `shims/netstat`
(«не в сети Сбера»), `CM_COURT_ROUTES_READY=1` (маршруты Сбера не нужны —
egress уже РФ), notify/osascript безопасен сам (`|| true`).

## Установка (сделано 28.08.2026, повторяемо)

1. `timedatectl set-timezone Asia/Yekaterinburg` — окно 08:45 и календарь
   выходных живут по местному времени (`vps_env.sh` жёстко проверяет +0500).
2. `apt install -y python3-requests jq` (боевые скрипты зовут
   `/usr/bin/python3` жёстко — venv не поможет; net-tools НЕ ставить).
3. Клоны: `/opt/court-monitor/dashboard` (эталон) и
   `/opt/court-monitor/dashboard-ural` (форк; внутри
   `git config merge.ours.driver true`).
4. Deploy-ключи (write) на оба репо; `~/.ssh/config` выбирает ключ по cwd
   git-процесса (оба репо ходят на один хост ssh.github.com:443 — жёсткая
   строка `GIT_SSH_COMMAND` боевого скрипта, host-алиасы невозможны):
   ```
   Match host ssh.github.com exec "test $(pwd) = /opt/court-monitor/dashboard"
     IdentityFile ~/.ssh/deploy_hmao
     IdentitiesOnly yes
   Match host ssh.github.com exec "test $(pwd) = /opt/court-monitor/dashboard-ural"
     IdentityFile ~/.ssh/deploy_ural
     IdentitiesOnly yes
   ```
   known_hosts засеять: `ssh-keyscan -p 443 ssh.github.com`.
5. `~/.config/court-monitor/`: `territories` (пути обоих клонов, эталон
   ПОСЛЕДНИМ — parse_all сам ставит Урал первым), `env.<регион>` (кэпы
   территории), `telegram`, `progress_token`, `worker.<регион>`
   (chmod 600; скопированы с Mac). ⚠️ `PUSH_SECRET` в `env.*` не класть —
   включит вторую доставку push (worker.* читается awk'ом, не source).
6. `cp ops/vps-run/systemd/court-*.{service,timer} /etc/systemd/system/`
   → `systemctl daemon-reload` → `systemctl enable --now court-parse.timer
   court-import.timer court-import-poll.timer`.

**Обновление слотов** (таймер поменялся в репо): `git pull` в
`/opt/court-monitor/dashboard` → повторить `cp` → `systemctl daemon-reload`
→ `systemctl restart court-import.timer` (без restart таймер держит старый
NextElapse) → `systemctl list-timers court-import.timer` показывает новый
ближайший слот.

## Наблюдение

- `journalctl -u court-parse -u court-import -u court-import-poll --since today`
  (поллер при пустом флаге молчит — 156 тиков в день без единой строки)
- логи прогона: `<клон>/ops/mac-local-run/parse_and_push*.log`,
  `import_dumps*.log` (ротация по дням, как на Mac)
- вехи в админке (блок «🛰 Парсинг») — через progress_token
- Telegram-алерты подписаны «Mac-парсинг (<клон>)» — префикс жёсткий в
  боевом скрипте, менять только вместе с ним (не дублировать).

## Откат на Mac (если VPS лёг)

1. VPS: `systemctl disable --now court-parse.timer court-import.timer
   court-import-poll.timer`
   (или просто выключить сервер).
2. Mac: `launchctl load ~/Library/LaunchAgents/com.court-monitor.parse.plist`
   и `... com.court-monitor.import.plist` (plist на месте, агенты были
   выгружены 28.08.2026 при флипе).
Разовый ручной прогон с Mac в любой момент: пульт «СберСуд-пульт.command»
(--force идёт мимо гейта «дайджест уже отправлен» — при живом VPS даст
второй дайджест, использовать осознанно).

## Острые углы

- Одновременно VPS и Mac работать НЕ должны: гейт «один дайджест в день»
  смотрит только `delivered_at` (после git pull) и от гонки двух хостов в
  одном окне не защищает; черновые пуши двух писателей конфликтуют.
- Free Tier VPS до ~27.11.2026, дальше ~513 ₽/мес (решение о продлении за
  юристом). На VPS также живёт nginx-шлюз api2-*.delosud.ru — не трогать.
- ssh-канал Mac→VPS иногда рвётся («closed by remote host») — на прогоны
  не влияет (systemd), только на ручное наблюдение.
