# =============================================================================
# Общий слой «Mac в сети Сбера» для скриптов резерва. Подключается точкой:
#   . "$(dirname "${BASH_SOURCE[0]}")/lib_sber_net.sh"
#
# ЗАЧЕМ ОТДЕЛЬНЫМ ФАЙЛОМ. Преflight, маршруты судов мимо VPN, проба
# доступности и вывод ssh-адреса нужны и парсингу (parse_and_push.sh), и
# импорту дампов (import_dumps.sh). Копия во втором скрипте — та самая
# поломка, которой резерв уже дважды болел молча: список файлов данных
# разъехался с облаком (не коммитились семь путей трека «Иски банка»), а
# домены судов искались регекспом по файлу, из которого они уехали
# (шесть строк из комментариев вместо 21 домена — суды шли через VPN).
#
# ДОГОВОР. Функции НЕ логируют сами и НЕ завершают процесс: возвращают код и
# печатают данные в stdout. Логирование и `die` остаются в вызывающем скрипте
# (у парсинга свой pusher вех, у импорта — свой отчёт оператору). Где нужен
# построчный лог, имя лог-функции передаётся аргументом.
# =============================================================================

# Шлюз сети Сбера (egress РФ). Маршруты судов заворачиваем через него, мимо VPN.
CM_SBER_GATEWAY="${CM_SBER_GATEWAY:-10.217.111.250}"

# ── Мы в сети Сбера? ─────────────────────────────────────────────────────────
# Признак — шлюз Сбера среди default-маршрутов (в т.ч. когда VPN поднят и
# добавляет свой второй default). Нет → заворачивать суды некуда.
cm_in_sber_network() {
  netstat -rn -f inet | awk '$1=="default"{print $2}' \
    | grep -qx "$CM_SBER_GATEWAY"
}

# ── Адрес origin по ssh:443 ──────────────────────────────────────────────────
# origin у клона — https, учётных данных на машине нет («could not read
# Username»), а SSH:22 к github.com в этой сети закрыт. Единственный рабочий
# путь — ssh.github.com:443; URL выводим из origin, чтобы форк и эталон
# обслуживались одним кодом. Печатает URL, код 1 — вывести не удалось.
cm_git_ssh_url() {
  local url
  url=$(git remote get-url origin 2>/dev/null \
    | sed -E 's#^https://github\.com/#ssh://git@ssh.github.com:443/#; s#^git@github\.com:#ssh://git@ssh.github.com:443/#')
  case "$url" in
    ssh://git@ssh.github.com:443/*) printf '%s\n' "$url" ;;
    *) printf '%s\n' "$url"; return 1 ;;
  esac
}

# Переменная окружения для git: тот же ssh через 443.
cm_git_ssh_command() { printf '%s\n' "ssh -p 443 -o HostName=ssh.github.com"; }

# ── Маршруты судов мимо VPN ──────────────────────────────────────────────────
# $1 — python, $2 — имя лог-функции вызывающего скрипта. Домены берём из
# РЕЕСТРА АКТИВНОГО РЕГИОНА (get_region), резолвим, дедупим IP, на каждый
# ставим host-маршрут через шлюз Сбера. Идемпотентно.
# Код 1 — реестр не отдал доменов (это фатально для вызывающего: без
# маршрутов суды пойдут через VPN мимо egress РФ, и прогон промолчит).
cm_court_ips() {  # $1 = python; строка «отрезолвлено/всего», дальше IP построчно
  "$1" - <<'PYROUTE'
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
resolved = 0
for h in sorted(hosts):
    try:
        ips.add(socket.gethostbyname(h))
        resolved += 1
    except OSError:
        pass
# Первая строка — счётчики для лога, дальше IP построчно. Оба числа важны:
# доменов должно быть 21 (ХМАО) / 67 (Урал), а УНИКАЛЬНЫЙ IP может выйти
# ОДИН — суды ГАС сидят за общим балансировщиком, и это норма, не поломка.
print(f"{resolved}/{len(hosts)}")
print("\n".join(sorted(ips)))
PYROUTE
}

cm_setup_court_routes() {
  local python="$1" logfn="${2:-:}" out stat ips ip err
  out=$(cm_court_ips "$python")
  stat=$(echo "$out" | head -1)
  ips=$(echo "$out" | tail -n +2)
  [ -n "$ips" ] || return 1
  "$logfn" "Доменов судов region-реестра отрезолвлено: $stat → уникальных IP: $(echo "$ips" | wc -l | tr -d ' ') (IP может быть один — общий балансировщик ГАС)"
  for ip in $ips; do
    # Пересоздаём маршрут заново каждый прогон: старый мог остаться в таблице,
    # но битым после смены IP en0 (route висит, а connect даёт EADDRNOTAVAIL).
    # delete (без ошибки, если нет) + add. Идемпотентно и самозалечивается.
    sudo -n /sbin/route -n delete -host "$ip" >/dev/null 2>&1
    if err=$(sudo -n /sbin/route -n add -host "$ip" "$CM_SBER_GATEWAY" 2>&1); then
      "$logfn" "  маршрут $ip → $CM_SBER_GATEWAY (пересоздан)"
    else
      # Причину печатаем в самой строке: раньше вывод route уходил в лог
      # отдельно от WARNING, и «sudoers не настроен» приходилось искать глазами.
      "$logfn" "  WARN: не смог поставить маршрут $ip: ${err:-без вывода} (sudoers не настроен? см. README)"
    fi
  done
}

# ── Проба доступности суда ───────────────────────────────────────────────────
# Хост берём из реестра региона (апелляция территории), а не хардкодом ХМАО:
# на форке проба стучалась бы в чужой суд.
cm_probe_court_host() {
  local host
  host=$("$1" -c 'import sys; sys.path.insert(0, "scripts");
from court_monitor.regions import get_region; print(get_region().appeal_courts[0].domain)' 2>/dev/null)
  [ -n "$host" ] || return 1
  printf '%s\n' "$host"
}

# ⚠️ Мало «ответил ли сервер»: страница защиты ГАС «Правосудие» приходит с
# HTTP 200 и телом ~1 КБ («Этот запрос заблокирован по соображениям
# безопасности (G) : ip: …»). Прежняя проба считала её успехом, и прогон шёл
# читать карточки, которых нет, — ровно то, что случилось с облаком 16.08.2026.
# Судим по РАЗМЕРУ, а не по тексту: страницы судов в win-1251, и русский
# маркер в UTF-8-скрипте не совпал бы. Настоящая главная страница суда —
# десятки КБ, заглушка и блок-страница — около одного.
CM_COURT_MIN_BYTES="${CM_COURT_MIN_BYTES:-4096}"

# ⚠️ Подпись клиента (User-Agent) решает: WAF судов отдаёт `curl/…` и
# `python-requests/…` ровно тот же 403, что и заблокированному адресу.
# Замер 16.08.2026 с домашнего интернета юриста: голый curl → 403 (1330 б),
# браузерная подпись → 200 (197 КБ той же страницы). Проба без подписи
# объявляла бы блоком ЛЮБОЙ прогон, в том числе из офиса. Берём подпись у
# самого парсера — одно место правды, а не вторая копия строки.
cm_court_ua() {
  "$1" -c 'import sys; sys.path.insert(0, "scripts");
from court_monitor.netutil import session; print(session.headers.get("User-Agent", ""))' 2>/dev/null
}

cm_court_reachable() {  # $1 = хост, $2 = python; печатает диагностику, 0 = живой
  local out code size ua
  ua=$(cm_court_ua "${2:-python3}")
  [ -n "$ua" ] || ua="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  out=$(curl -sS -o /dev/null -w '%{http_code} %{size_download}' -A "$ua" \
        --connect-timeout 15 --max-time 45 "https://$1/" 2>&1) || {
    printf '%s' "$out"; return 1; }
  code=${out%% *}
  size=${out##* }
  if [ "$code" != "200" ]; then
    printf 'суд ответил HTTP %s' "$code"; return 1
  fi
  if [ "${size:-0}" -lt "$CM_COURT_MIN_BYTES" ]; then
    printf 'ответ всего %s байт — это не страница суда, а заглушка или страница защиты ГАС (нас блокируют по адресу)' "$size"
    return 1
  fi
  return 0
}

# ── Снять host-маршруты судов ────────────────────────────────────────────────
# Нужно вне сети Сбера: маршруты, поставленные в офисе, остаются в таблице и
# ведут в недоступный шлюз — суды перестают открываться вообще, а причина
# выглядит как «нас блокируют». sudoers разрешает delete без пароля.
cm_clear_court_routes() {
  local python="$1" logfn="${2:-:}" ips ip n=0
  ips=$(cm_court_ips "$python" | tail -n +2)
  [ -n "$ips" ] || return 0
  for ip in $ips; do
    if netstat -rn -f inet | awk -v i="$ip" '$1==i{f=1} END{exit !f}'; then
      sudo -n /sbin/route -n delete -host "$ip" >/dev/null 2>&1 && n=$((n + 1))
    fi
  done
  [ "$n" -gt 0 ] && "$logfn" "Сняты маршруты судов через шлюз Сбера: $n (мы не в офисной сети)"
  return 0
}

# ── Алерт о сбое в Telegram ──────────────────────────────────────────────────
# В облаке это делает шаг `if: failure()` в update_cases.yml; на Mac
# уведомление на экране видно только если юрист за машиной — прогон идёт в
# 08:00. Секреты вне репозитория: файл <conf>/telegram с двумя строками
# token=… и chat_id=… Нет файла — молча, как раньше.
# $1 — каталог конфигов, $2 — префикс («Mac-парсинг (клон)»), $3 — текст.
cm_alert_telegram() {
  local f="$1/telegram" prefix="$2" text="$3" token chat
  [ -f "$f" ] || return 0
  token=$(awk -F= '/^token=/{print $2}' "$f" | tr -d '[:space:]')
  chat=$(awk -F= '/^chat_id=/{print $2}' "$f" | tr -d '[:space:]')
  [ -n "$token" ] && [ -n "$chat" ] || return 0
  curl -sS -m 20 -o /dev/null \
    "https://api.telegram.org/bot$token/sendMessage" \
    --data-urlencode "chat_id=$chat" \
    --data-urlencode "text=🚨 $prefix: $text" >/dev/null 2>&1 || true
}

# ── Регион клона и переменные территории ─────────────────────────────────────
# Регион клон определяет сам: у форка в корне лежит файл REGION.
cm_region_code() {
  "$1" -c 'import sys; sys.path.insert(0, "scripts");
from court_monitor import config; print(config.REGION)' 2>/dev/null
}

# Настройки территории (кэпы авто-подхвата, BANK_TRACK, DASHBOARD_URL) в облаке
# живут Actions Variables — на Mac их взять неоткуда, и работа шла бы с
# дефолтами кода. Файл вне репозитория: это свойство машины, а не кода.
# $1 — python, $2 — каталог конфигов, $3 — имя лог-функции. Нет файла — тихо.
cm_load_territory_env() {
  local python="$1" conf_dir="$2" logfn="${3:-:}" region
  region=$(cm_region_code "$python")
  [ -n "$region" ] && [ -f "$conf_dir/env.$region" ] || return 0
  "$logfn" "Переменные территории: $conf_dir/env.$region"
  set -a
  . "$conf_dir/env.$region"
  set +a
}
