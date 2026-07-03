#!/bin/bash
# =============================================================================
# Court Monitor — локальный парсинг судов на Mac (вариант D2).
#
# ЗАЧЕМ. Сайты sudrf.ru молча дропают TLS с иностранных IP, поэтому GitHub
# Actions (США) больше не достаёт суды. Этот Mac физически в сети Сбера
# (egress РФ) → с него суды парсятся. Скрипт парсит суды и пушит результат в
# git; дайджест Claude'ом и доставку делает уже GitHub (workflow
# replay_on_push.yml по факту push'а data/last_digest_context.json).
#
# Секреты здесь НЕ нужны: доставка (Telegram/push) сама пропускается, дайджест
# получается шаблонным (его выкинут), но контекст сохраняется — replay на
# GitHub соберёт настоящий Claude-дайджест.
#
# Запускается LaunchAgent'ом (com.court-monitor.parse) по будням; можно и
# вручную: bash ops/mac-local-run/parse_and_push.sh
# =============================================================================

# ── Параметры (правь тут при переезде/смене сети) ────────────────────────────
REPO="/Users/aleksandrselivanov/dashboard"
SBER_GATEWAY="10.217.111.250"          # шлюз сети Сбера (egress РФ). Маршрут
                                       # судов заворачиваем через него, мимо VPN.
PROBE_HOST="oblsud--hmao.sudrf.ru"     # по нему проверяем доступность судов
PYTHON="/usr/bin/python3"
LOG_DIR="$REPO/ops/mac-local-run"
LOG="$LOG_DIR/parse_and_push.log"
LOCK="$LOG_DIR/.run.lock"

# ── Утилиты ──────────────────────────────────────────────────────────────────
ts()  { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "$(ts) $*" >>"$LOG"; }
notify() {  # $1 = текст уведомления macOS
  /usr/bin/osascript -e "display notification \"$1\" with title \"Court Monitor\"" >/dev/null 2>&1 || true
}
die() {  # $1 = текст → в лог, уведомление, выход 1
  log "ERROR: $1"; notify "Ошибка: $1"
  finish_pusher   # дать pusher'у дослать «ERROR:» в админку (он выйдет сам)
  rmdir "$LOCK" 2>/dev/null; exit 1
}

# ── Онлайн-вехи в админку Worker (блок «🛰 Парсинг»; некритичная функция) ─────
PUSHER_PID=""
start_pusher() {
  # Токен вне репо (репо публичный). Нет токена — прогресс просто выключен.
  if [ -f "$HOME/.config/court-monitor/progress_token" ]; then
    "$PYTHON" "$REPO/ops/mac-local-run/progress_pusher.py" "run-$(date '+%Y%m%d-%H%M%S')" &
    PUSHER_PID=$!
    log "progress: онлайн-вехи включены (pid $PUSHER_PID)"
  else
    log "progress: токена нет (~/.config/court-monitor/progress_token) — пропуск"
  fi
}
finish_pusher() {
  # Pusher выходит сам, увидев финальную строку лога; ждём до ~12с, потом kill.
  [ -n "$PUSHER_PID" ] || return 0
  for _ in 1 2 3 4 5 6; do
    kill -0 "$PUSHER_PID" 2>/dev/null || { PUSHER_PID=""; return 0; }
    sleep 2
  done
  kill "$PUSHER_PID" 2>/dev/null; PUSHER_PID=""
}

# ── Один экземпляр за раз ─────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
if ! mkdir "$LOCK" 2>/dev/null; then
  log "Другой прогон уже идёт ($LOCK) — выход"
  exit 0
fi
trap 'rmdir "$LOCK" 2>/dev/null' EXIT

# ── Ротация лога: держим историю нескольких прогонов, но не даём расти вечно ──
if [ -f "$LOG" ] && [ "$(wc -l < "$LOG")" -gt 4000 ]; then
  tail -n 2000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
fi

log "=================================================================="
log "Старт parse_and_push (pid $$)"

cd "$REPO" || die "нет каталога $REPO"

# ── Preflight: мы в сети Сбера? ──────────────────────────────────────────────
# Признак — шлюз Сбера присутствует среди default-маршрутов (в т.ч. когда VPN
# поднят и добавляет свой второй default). Если нет — мы не в офисной сети,
# заворачивать суды некуда, тихо выходим (не ошибка).
if ! netstat -rn -f inet | awk '$1=="default"{print $2}' | grep -qx "$SBER_GATEWAY"; then
  log "Пропуск: шлюз $SBER_GATEWAY не найден среди default-маршрутов (не в сети Сбера)"
  notify "Пропуск: не в сети Сбера — дайджест не собран"
  exit 0
fi
log "Сеть Сбера подтверждена (шлюз $SBER_GATEWAY)"

start_pusher

# ── Подтянуть вчерашние replay-коммиты GitHub (иначе push отклонят) ───────────
if ! git pull --rebase --autostash origin main >>"$LOG" 2>&1; then
  die "git pull --rebase не удался (см. лог)"
fi

# ── Маршрут судов мимо VPN через en0 ─────────────────────────────────────────
# Домены берём прямо из courts.py (не дублируем список), резолвим, дедупим IP,
# на каждый ставим host-маршрут через шлюз Сбера. Идемпотентно.
UNIQ_IPS=$("$PYTHON" - <<'PY'
import re, socket
try:
    txt = open("scripts/court_monitor/courts.py", encoding="utf-8").read()
except OSError:
    raise SystemExit(0)
ips = set()
for h in sorted(set(re.findall(r'[a-z0-9.-]+\.sudrf\.ru', txt))):
    try:
        ips.add(socket.gethostbyname(h))
    except OSError:
        pass
print("\n".join(sorted(ips)))
PY
)
if [ -z "$UNIQ_IPS" ]; then
  log "WARN: не удалось резолвить домены судов — продолжаю (вдруг маршрут уже есть)"
fi
for ip in $UNIQ_IPS; do
  # Пересоздаём маршрут заново каждый прогон: старый мог остаться в таблице,
  # но битым после смены IP en0 (route висит, а connect даёт EADDRNOTAVAIL).
  # delete (без ошибки, если нет) + add. Идемпотентно и самозалечивается.
  sudo -n /sbin/route -n delete -host "$ip" >/dev/null 2>&1
  if sudo -n /sbin/route -n add -host "$ip" "$SBER_GATEWAY" >>"$LOG" 2>&1; then
    log "  маршрут $ip → $SBER_GATEWAY (пересоздан)"
  else
    log "  WARN: не смог поставить маршрут $ip (sudoers не настроен? см. README)"
  fi
done

# ── Проверка доступности судов ───────────────────────────────────────────────
if curl -sS -o /dev/null --connect-timeout 15 --max-time 45 "https://$PROBE_HOST/" >>"$LOG" 2>&1; then
  log "Суд $PROBE_HOST доступен"
else
  die "суд $PROBE_HOST недоступен даже с маршрутом — парсинг пропущен"
fi

# ── Парсинг (без секретов; доставка скипается, контекст сохраняется) ──────────
# run_parse.py = main_json с заглушённой validate_environment (иначе exit(2)
# без секретов). Доставка (Telegram/push) сама пропускается без токенов.
log "Парсинг судов: run_parse.py (main_json без секретов) ..."
SKIP_NON_WORKING_DAYS=1 "$PYTHON" ops/mac-local-run/run_parse.py >>"$LOG" 2>&1
RC=$?
if [ "$RC" -ne 0 ]; then
  die "парсинг завершился с кодом $RC (см. лог)"
fi
log "Парсинг завершён"

# ── Коммит и пуш (список файлов = как в update_cases.yml) ─────────────────────
git add data/sberbank_cases.csv data/.digested_acts data/cases.json 2>/dev/null
[ -f data/.cassation_acts ]            && git add data/.cassation_acts
[ -f data/sberbank_cases_archive.csv ] && git add data/sberbank_cases_archive.csv
[ -f data/cases_archive.json ]         && git add data/cases_archive.json
git add data/cases_archive_*.json 2>/dev/null
[ -f data/last_digest_context.json ]   && git add data/last_digest_context.json
[ -f data/last_digest.json ]           && git add data/last_digest.json
[ -f data/last_personal_pushes.json ]  && git add data/last_personal_pushes.json
[ -f data/parse_health.json ]          && git add data/parse_health.json
# Кэш LLM-пересказов: на Mac не пополняется (нет ANTHROPIC-ключа), но список
# файлов держим идентичным workflow'ам — защита на будущее.
[ -f data/.act_summaries.json ]        && git add data/.act_summaries.json

if git diff --cached --quiet; then
  log "Изменений нет — коммит не нужен (нерабочий день или без движения)"
  notify "Прогон завершён — изменений нет"
  finish_pusher
  exit 0
fi

git -c user.name="Court Monitor (Mac)" -c user.email="bot@court-monitor.local" \
    commit -m "📊 Обновление данных $(date +'%d.%m.%Y %H:%M') (Mac-парсинг)" >>"$LOG" 2>&1 \
    || die "git commit не удался"

if git push origin main >>"$LOG" 2>&1; then
  log "Запушено — GitHub соберёт дайджест Claude'ом и разошлёт"
  notify "Готово: данные обновлены, дайджест собирается"
else
  die "git push не удался (см. лог)"
fi

log "Готово"
finish_pusher
