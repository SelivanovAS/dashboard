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
# ТЕРРИТОРИИ. Скрипт работает с ОДНИМ клоном, путь к нему — первый аргумент
# (или env CM_REPO, дефолт ~/dashboard). Регион клон определяет сам: у форка в
# корне лежит файл REGION, его читает config._region_from_file(). Обе
# территории подряд гоняет ops/mac-local-run/parse_all.sh — его и зовёт
# LaunchAgent.
#
# Запускается LaunchAgent'ом (com.court-monitor.parse) по будням; можно и
# вручную:
#   bash ops/mac-local-run/parse_and_push.sh [путь-к-клону]
#   bash ops/mac-local-run/parse_and_push.sh --check   # только диагностика
# =============================================================================

# ── Аргументы ────────────────────────────────────────────────────────────────
CHECK_ONLY=0
REPO_ARG=""
for arg in "$@"; do
  case "$arg" in
    --check) CHECK_ONLY=1 ;;
    -*)      echo "неизвестный ключ: $arg" >&2; exit 2 ;;
    *)       REPO_ARG="$arg" ;;
  esac
done

# ── Параметры (правь тут при переезде/смене сети) ────────────────────────────
REPO="${REPO_ARG:-${CM_REPO:-/Users/aleksandrselivanov/dashboard}}"
SBER_GATEWAY="10.217.111.250"          # шлюз сети Сбера (egress РФ). Маршрут
                                       # судов заворачиваем через него, мимо VPN.
PYTHON="/usr/bin/python3"
LOG_DIR="$REPO/ops/mac-local-run"
LOG="$LOG_DIR/parse_and_push.log"
LOCK="$LOG_DIR/.run.lock"
CONF_DIR="$HOME/.config/court-monitor"

# ── Утилиты ──────────────────────────────────────────────────────────────────
ts()  { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "$(ts) $*" >>"$LOG"; }
notify() {  # $1 = текст уведомления macOS (+ Telegram, если настроен)
  /usr/bin/osascript -e "display notification \"$1\" with title \"Court Monitor\"" >/dev/null 2>&1 || true
}
# Алерт о СБОЕ в Telegram. В облаке это делает шаг `if: failure()` в
# update_cases.yml; на Mac уведомление на экране видно только если юрист за
# машиной — прогон идёт в 08:00. Секреты вне репозитория: файл
# ~/.config/court-monitor/telegram с двумя строками token=… и chat_id=…
# Нет файла — молча, как раньше.
alert_telegram() {  # $1 = текст
  local f="$CONF_DIR/telegram" token chat
  [ -f "$f" ] || return 0
  token=$(awk -F= '/^token=/{print $2}' "$f" | tr -d '[:space:]')
  chat=$(awk -F= '/^chat_id=/{print $2}' "$f" | tr -d '[:space:]')
  [ -n "$token" ] && [ -n "$chat" ] || return 0
  curl -sS -m 20 -o /dev/null \
    "https://api.telegram.org/bot$token/sendMessage" \
    --data-urlencode "chat_id=$chat" \
    --data-urlencode "text=🚨 Mac-парсинг ($(basename "$REPO")): $1" >/dev/null 2>&1 || true
}
die() {  # $1 = текст → в лог, уведомление, Telegram, выход 1
  log "ERROR: $1"; notify "Ошибка: $1"; alert_telegram "$1"
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

# ── Адрес для git по ssh:443 ─────────────────────────────────────────────────
# origin у клона — https, учётных данных на машине нет («could not read
# Username»), а SSH:22 к github.com в этой сети закрыт. Единственный рабочий
# путь — ssh.github.com:443; URL выводим из origin, чтобы форк и эталон
# обслуживались одним кодом.
GIT_URL=$(git remote get-url origin 2>/dev/null \
  | sed -E 's#^https://github\.com/#ssh://git@ssh.github.com:443/#; s#^git@github\.com:#ssh://git@ssh.github.com:443/#')
case "$GIT_URL" in
  ssh://git@ssh.github.com:443/*) : ;;
  *) die "не смог вывести ssh-адрес из origin ($GIT_URL)" ;;
esac
export GIT_SSH_COMMAND="ssh -p 443 -o HostName=ssh.github.com"
log "git через $GIT_URL"

# ── Подтянуть вчерашние replay-коммиты GitHub (иначе push отклонят) ───────────
# При --check репозиторий не трогаем вовсе: диагностика не должна двигать
# рабочее дерево (rebase с autostash — уже изменение).
if [ "$CHECK_ONLY" != "1" ] && ! git pull --rebase --autostash "$GIT_URL" main >>"$LOG" 2>&1; then
  die "git pull --rebase не удался (см. лог)"
fi

# ── Маршрут судов мимо VPN через en0 ─────────────────────────────
# Домены берём из РЕЕСТРА АКТИВНОГО РЕГИОНА (get_region), резолвим, дедупим IP,
# на каждый ставим host-маршрут через шлюз Сбера. Идемпотентно.
# ⚠️ Раньше домены искались регекспом по scripts/court_monitor/courts.py.
# После регионализации (16.07.2026) реестры уехали в regions/*.py, и регексп
# находил ШЕСТЬ строк из комментариев и докстрингов вместо 20 доменов ХМАО:
# маршруты строились не туда, суды шли через VPN мимо egress РФ, а WARNING
# ниже это молча проглатывал. Поэтому пустой список теперь фатален.
UNIQ_IPS=$("$PYTHON" - <<'PYROUTE'
import socket, sys
sys.path.insert(0, "scripts")
try:
    from court_monitor.regions import get_region
    region = get_region()
except Exception:
    raise SystemExit(0)
hosts = {c.domain for c in region.first_instance_courts if c.enabled}
hosts |= {c.domain for c in region.appeal_courts}
hosts.add(region.cassation_court.domain)
ips = set()
for h in sorted(hosts):
    try:
        ips.add(socket.gethostbyname(h))
    except OSError:
        pass
print("\n".join(sorted(ips)))
PYROUTE
)
if [ -z "$UNIQ_IPS" ]; then
  die "не удалось получить домены судов из реестра региона — маршруты не построить"
fi
log "Судебных IP для маршрутизации: $(echo "$UNIQ_IPS" | wc -l | tr -d ' ')"
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
# Хост берём из реестра региона (апелляция территории), а не хардкодом ХМАО:
# на форке проба стучалась бы в чужой суд.
PROBE_HOST=$("$PYTHON" -c 'import sys; sys.path.insert(0, "scripts");
from court_monitor.regions import get_region; print(get_region().appeal_courts[0].domain)' 2>/dev/null)
[ -n "$PROBE_HOST" ] || die "не смог определить суд для пробы доступности"
if curl -sS -o /dev/null --connect-timeout 15 --max-time 45 "https://$PROBE_HOST/" >>"$LOG" 2>&1; then
  log "Суд $PROBE_HOST доступен"
else
  die "суд $PROBE_HOST недоступен даже с маршрутом — парсинг пропущен"
fi

# ── --check: дальше не идём ──────────────────────────────────────────────────
# Диагностика для проверки резерва из офиса: preflight, маршруты и доступность
# суда проверены, а парсинг и push не запускаются — ничего не публикуется.
if [ "$CHECK_ONLY" = "1" ]; then
  log "--check: сеть, маршруты и доступ к судам в порядке; парсинг пропущен"
  notify "Проверка резерва пройдена ($(basename "$REPO"))"
  finish_pusher
  exit 0
fi

# ── Парсинг (без секретов; доставка скипается, контекст сохраняется) ──────────
# run_parse.py = main_json с заглушённой validate_environment (иначе exit(2)
# без секретов). Доставка (Telegram/push) сама пропускается без токенов.
# Настройки территории (кэпы авто-подхвата, DASHBOARD_URL) в облаке живут
# Actions Variables — на Mac их взять неоткуда, и подхват шёл бы с дефолтами
# кода (30/10/50 вместо 200/25/200 у Урала). Файл вне репозитория: это
# свойство машины, а не кода. Нет файла — дефолты, как раньше.
REGION_CODE=$("$PYTHON" -c 'import sys; sys.path.insert(0, "scripts");
from court_monitor import config; print(config.REGION)' 2>/dev/null)
if [ -n "$REGION_CODE" ] && [ -f "$CONF_DIR/env.$REGION_CODE" ]; then
  log "Переменные территории: $CONF_DIR/env.$REGION_CODE"
  set -a; . "$CONF_DIR/env.$REGION_CODE"; set +a
fi

log "Парсинг судов ($REGION_CODE): run_parse.py (main_json без секретов) ..."
SKIP_NON_WORKING_DAYS=1 "$PYTHON" ops/mac-local-run/run_parse.py >>"$LOG" 2>&1
RC=$?
if [ "$RC" -ne 0 ]; then
  die "парсинг завершился с кодом $RC (см. лог)"
fi
log "Парсинг завершён"

# ── Коммит и пуш ──────────────────────────────────────────────
# Список файлов ОДИН с облаком: ops/stage_data_files.sh спрашивает пути у
# court_monitor.config. Прежний ручной список здесь разъехался с workflow —
# не коммитились семь файлов трека «Иски банка» (он появился 25.07.2026,
# уже после того, как резерв усыпили), и флип выбросил бы весь трек.
bash ops/stage_data_files.sh >>"$LOG" 2>&1 || die "не удалось собрать файлы данных"

if git diff --cached --quiet; then
  log "Изменений нет — коммит не нужен (нерабочий день или без движения)"
  notify "Прогон завершён — изменений нет"
  finish_pusher
  exit 0
fi

git -c user.name="Court Monitor (Mac)" -c user.email="bot@court-monitor.local" \
    commit -m "📊 Обновление данных $(date +'%d.%m.%Y %H:%M') (Mac-парсинг)" >>"$LOG" 2>&1 \
    || die "git commit не удался"

if git push "$GIT_URL" HEAD:main >>"$LOG" 2>&1; then
  log "Запушено — GitHub соберёт дайджест Claude'ом и разошлёт"
  notify "Готово: данные обновлены, дайджест собирается"
else
  die "git push не удался (см. лог)"
fi

log "Готово"
finish_pusher
