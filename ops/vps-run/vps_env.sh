# =============================================================================
# VPS-пролог (Ubuntu, Cloud.ru): готовит окружение и передаёт управление
# БОЕВЫМ скриптам ops/mac-local-run/*. Своей копии логики у VPS НЕТ намеренно:
# копия транзакций/гейта/фаз доставки — тот класс молчаливой поломки, которым
# резерв уже дважды болел (списки файлов данных, домены судов, jq-пейлоады).
#
# Вся macOS-специфика Mac-звена закрывается снаружи, без правок его файлов:
#  - `netstat` (BSD-флаги в cm_in_sber_network) — заглушка в shims/ ПЕРВОЙ в
#    PATH: пустой вывод = «не в сети Сбера» → ветка --anywhere, доступ судов
#    решает честная HTTP-проба (cm_any_court_reachable);
#  - маршруты судов через шлюз Сбера (sudo /sbin/route) — CM_COURT_ROUTES_READY=1
#    выключает их установку и в parse_all (prepare_shared_routes), и в детях:
#    на VPS egress уже РФ, заворачивать нечего;
#  - notify через /usr/bin/osascript уже безопасен (`|| true` + абс. путь);
#  - `date -r файл` / `find -mtime -delete` / `ps -o lstart=` — GNU-совместимы.
#
# Подключается точкой из ops/vps-run/{parse_all.sh,import_all.sh}.
# =============================================================================

VPS_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Окно доставки 08:45 и производственный календарь живут по местному времени
# территории (Asia/Yekaterinburg, +05 — обе территории в одном поясе).
# Сервер с иной таймзоной сдвинул бы дайджест и календарь выходных на часы —
# лучше громкий отказ, чем тихий сдвиг.
if [ "$(date +%z)" != "+0500" ]; then
  echo "ОШИБКА: таймзона сервера $(date +%z), нужна +0500 (Asia/Yekaterinburg):" >&2
  echo "  timedatectl set-timezone Asia/Yekaterinburg" >&2
  exit 1
fi

# Боевые скрипты зовут /usr/bin/python3 жёстко — requests обязан быть
# СИСТЕМНЫМ (apt install python3-requests), venv их не спасёт.
if ! /usr/bin/python3 -c 'import requests' 2>/dev/null; then
  echo "ОШИБКА: у /usr/bin/python3 нет requests: apt install -y python3-requests" >&2
  exit 1
fi

export PATH="$VPS_HERE/shims:$PATH"
export CM_COURT_ROUTES_READY=1
