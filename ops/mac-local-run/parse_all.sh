#!/bin/bash
# =============================================================================
# Резерв D2: безопасный параллельный прогон всех территорий.
#
# Последовательная схема «ХМАО → Урал» обычно занимала около часа, а при
# медленном первом регионе второй мог не стартовать до конца утреннего окна.
# С 24.08.2026 более длинный Урал стартует первым, ХМАО — через 10 минут.
# Общая длительность стремится к максимуму двух прогонов, а не к их сумме.
#
# Домены регионов могут резолвиться в один IP. Поэтому parse_all готовит
# маршруты последовательно ДО детей и передаёт CM_COURT_ROUTES_READY=1.
# После wait обоих парсеров родитель, если уже 08:45, делает delivery-sweep по
# контекстам БЕЗ повторного парсинга. Только затем начинаются импорты.
#
# Мгновенный откат:
#   CM_PARALLEL_TERRITORIES=0 bash ops/mac-local-run/parse_all.sh ...
# Настройки:
#   CM_PARALLEL_STAGGER_SECONDS=600
#   CM_PARALLEL_FIRST_REGION=sverdlovsk_yanao
#
# Список клонов: ~/.config/court-monitor/territories, по пути на строку.
# --check намеренно последовательный и без десятиминутной задержки.
# =============================================================================
set -u

# launchd запускает календарный слот, но сам по себе не держит Mac бодрствующим.
# Assertion живёт ровно пока жив этот родительский драйвер: экран может погаснуть,
# а idle sleep не прервёт парсеры, их десятиминутный stagger и последующие импорты.
# На Linux/другой системе без штатного macOS utility поведение не меняется.
if [ -x /usr/bin/caffeinate ]; then
  /usr/bin/caffeinate -i -w "$$" >/dev/null 2>&1 &
  echo "$(date '+%Y-%m-%d %H:%M:%S') parse_all: idle sleep заблокирован до конца прогона"
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKER="${CM_WORKER:-$HERE/parse_and_push.sh}"
IMPORTER="${CM_IMPORTER:-$HERE/import_dumps.sh}"
PYTHON="${CM_PYTHON:-/usr/bin/python3}"
. "$HERE/lib_sber_net.sh"

PARALLEL="${CM_PARALLEL_TERRITORIES:-1}"
STAGGER_SECONDS="${CM_PARALLEL_STAGGER_SECONDS:-600}"
FIRST_REGION="${CM_PARALLEL_FIRST_REGION:-sverdlovsk_yanao}"
CHECK_ONLY=0
ANYWHERE=0
for arg in "$@"; do
  case "$arg" in
    --check) CHECK_ONLY=1 ;;
    --anywhere) ANYWHERE=1 ;;
  esac
done
case "$STAGGER_SECONDS" in
  ""|*[!0-9]*)
    echo "$(date '+%Y-%m-%d %H:%M:%S') parse_all: некорректный stagger '$STAGGER_SECONDS' — последовательный fallback"
    PARALLEL=0
    STAGGER_SECONDS=600
    ;;
esac

repos=()
while IFS= read -r line; do
  repos+=("$line")
done < <(cm_territories)
echo "$(date '+%Y-%m-%d %H:%M:%S') parse_all: территорий ${#repos[@]}"

rc=0
valid_repos=()
for repo in "${repos[@]}"; do
  if [ ! -d "$repo/.git" ]; then
    echo "  ПРОПУСК: $repo — не похоже на клон репозитория"
    rc=1
    continue
  fi
  valid_repos+=("$repo")
done

routes_ready="${CM_COURT_ROUTES_READY:-0}"
parser_repos=()
parser_pids=()
parser_pid_repos=()
shared_court_ips=""
delivery_sweep_ran=0

driver_log() { echo "  маршруты: $*"; }

collect_shared_court_ips() {
  local repo region out stat ips ip
  local all_ips=()
  for repo in "${valid_repos[@]}"; do
    out=$(cd "$repo" && cm_court_ips "$PYTHON")
    stat=$(echo "$out" | head -1)
    ips=$(echo "$out" | tail -n +2)
    [ -n "$ips" ] || return 1
    region=$(cd "$repo" && cm_region_code "$PYTHON")
    driver_log "${region:-$(basename "$repo")}: доменов $stat → IP $(echo "$ips" | wc -l | tr -d ' ')"
    for ip in $ips; do
      all_ips+=("$ip")
    done
  done
  shared_court_ips=$(printf '%s\n' "${all_ips[@]}" | sort -u)
  [ -n "$shared_court_ips" ] || return 1
  driver_log "объединение территорий → уникальных IP $(echo "$shared_court_ips" | wc -l | tr -d ' ')"
}

prepare_shared_routes() {
  if [ "$routes_ready" = "1" ]; then
    echo "  маршруты: уже подготовлены вызывающим процессом"
    return 0
  fi
  collect_shared_court_ips || return 1
  if cm_in_sber_network; then
    echo "  маршруты: единая установка перед параллельным запуском"
    cm_install_court_routes driver_log "$shared_court_ips" || return 1
    return 0
  fi
  if [ "$ANYWHERE" = "1" ]; then
    echo "  маршруты: вне сети Сбера — единая очистка залипших host-route"
    cm_remove_court_routes driver_log "$shared_court_ips" || return 1
    return 0
  fi
  # Без офисной сети и --anywhere прежние воркеры тихо выходили сами.
  # CM_COURT_ROUTES_READY здесь ставить нельзя: маршрута в реальности нет.
  return 1
}

order_parser_repos() {
  local repo region
  local first=() rest=()
  for repo in "${valid_repos[@]}"; do
    region=$(cd "$repo" && cm_region_code "$PYTHON")
    if [ "$region" = "$FIRST_REGION" ]; then
      first+=("$repo")
    else
      rest+=("$repo")
    fi
  done
  parser_repos=("${first[@]}" "${rest[@]}")
}

region_for_repo() { (cd "$1" && cm_region_code "$PYTHON"); }

run_worker() {
  local repo="$1"
  shift
  if [ "$routes_ready" = "1" ]; then
    CM_COURT_ROUTES_READY=1 bash "$WORKER" "$repo" "$@"
  else
    bash "$WORKER" "$repo" "$@"
  fi
}

run_importer() {
  local repo="$1"
  shift
  if [ "$routes_ready" = "1" ]; then
    CM_COURT_ROUTES_READY=1 bash "$IMPORTER" "$repo" "$@"
  else
    bash "$IMPORTER" "$repo" "$@"
  fi
}

stop_parallel_children() {
  local exit_code="${1:-1}" pid
  trap - HUP INT TERM
  echo "$(date '+%Y-%m-%d %H:%M:%S') parse_all: остановка дочерних парсеров"
  for pid in "${parser_pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      # Прямые дети воркера — run_parse.py и progress_pusher.py.
      /usr/bin/pkill -TERM -P "$pid" 2>/dev/null || true
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  for pid in "${parser_pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  exit "$exit_code"
}

run_parallel_parsers() {
  local i=0 repo region delay status order=""
  parser_pids=()
  parser_pid_repos=()
  for repo in "${parser_repos[@]}"; do
    region=$(region_for_repo "$repo")
    order="${order}${order:+ → }${region:-$(basename "$repo")}"
  done
  echo "  порядок парсеров: $order"

  trap 'stop_parallel_children 129' HUP
  trap 'stop_parallel_children 130' INT
  trap 'stop_parallel_children 143' TERM
  for repo in "${parser_repos[@]}"; do
    region=$(region_for_repo "$repo")
    delay=$((i * STAGGER_SECONDS))
    echo "  → $repo (парсер ${region:-?}, старт через ${delay}с)"
    (
      [ "$delay" -eq 0 ] || sleep "$delay"
      if [ "$routes_ready" = "1" ]; then
        export CM_COURT_ROUTES_READY=1
      fi
      exec bash "$WORKER" "$repo" "$@"
    ) &
    parser_pids+=("$!")
    parser_pid_repos+=("$repo")
    i=$((i + 1))
  done

  # Лок у parse_and_push поклоновый: занятый/упавший сосед не
  # отменяет другую территорию. Ждём каждого ребёнка даже после
  # ненулевого exit, чтобы импорт не пересёкся с живым парсером.
  for i in "${!parser_pids[@]}"; do
    if wait "${parser_pids[$i]}"; then
      status=0
    else
      status=$?
    fi
    if [ "$status" -ne 0 ]; then
      echo "  ОШИБКА: парсер ${parser_pid_repos[$i]} завершился с кодом $status"
      rc=1
    fi
  done
  trap - HUP INT TERM
}

run_imports() {
  local repo
  for repo in "${valid_repos[@]}"; do
    echo "  → $repo (дампы, после всех парсеров)"
    run_importer "$repo" "$@" \
      || echo "  ПРЕДУПРЕЖДЕНИЕ: импорт дампов не доработал ($repo)"
  done
}

run_sequential_parsers() {
  local repo
  for repo in "${valid_repos[@]}"; do
    echo "  → $repo (последовательный парсер)"
    run_worker "$repo" "$@" || rc=1
  done
}

run_delivery_sweep() {
  local repo
  [ "$CHECK_ONLY" = "1" ] && return 0
  [ "$delivery_sweep_ran" = "1" ] && return 0
  if ! cm_delivery_window_open; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') parse_all: delivery-sweep пока не нужен — окно 08:45 не открыто"
    return 0
  fi

  # Отмечаем попытку ДО обхода: второй вызов после импортов нужен только когда
  # первый был раньше окна, а не как немедленный retry неясного git-исхода.
  delivery_sweep_ran=1
  echo "$(date '+%Y-%m-%d %H:%M:%S') parse_all: финальный delivery-sweep после всех парсеров"
  for repo in "${valid_repos[@]}"; do
    echo "  → $repo (pending-контекст, без парсинга)"
    if ! run_worker "$repo" --deliver-pending; then
      echo "  ОШИБКА: delivery-sweep не завершён ($repo)"
      rc=1
    fi
  done
}

if [ "${#valid_repos[@]}" -lt 2 ] \
  || [ "$PARALLEL" != "1" ] \
  || [ "$CHECK_ONLY" = "1" ]; then
  run_sequential_parsers "$@"
else
  order_parser_repos
  if prepare_shared_routes; then
    routes_ready=1
    run_parallel_parsers "$@"
  else
    echo "  ПРЕДУПРЕЖДЕНИЕ: общие маршруты не подготовлены — последовательный fallback"
    run_sequential_parsers "$@"
  fi
fi

# Главный sweep стоит сразу после barrier двух территорий, чтобы доставка не
# зависела от очередей импорта. Повтор после импортов сработает только если
# первый check был до 08:45, а за время импортов окно успело открыться.
run_delivery_sweep
run_imports "$@"
run_delivery_sweep

echo "$(date '+%Y-%m-%d %H:%M:%S') parse_all: готово (код $rc)"
exit "$rc"
