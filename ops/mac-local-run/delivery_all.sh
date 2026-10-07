#!/bin/bash
# Отдельная утренняя доставка по всем территориям, без обращения к судам.
# Вся транзакция и общий с парсингом/импортом .run.lock остаются в
# parse_and_push.sh --deliver-pending. Занятый клон пропустит эту попытку.
# --check читает только локальную готовность и ничего не отправляет.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/lib_sber_net.sh"
PYTHON="${CM_PYTHON:-/usr/bin/python3}"
CHECK_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --check) CHECK_ONLY=1 ;;
    --anywhere) ;; # Совместимость с VPS-шимом: пробы судов здесь нет.
    *) echo "delivery_all: неизвестный ключ: $arg" >&2; exit 2 ;;
  esac
done

if ! cm_delivery_window_open; then
  echo "delivery_all: окно 08:45 ещё не открыто"
  exit 0
fi

pids=()
rc=0
stop_children() {
  local code="$1" pid
  trap - HUP INT TERM
  for pid in "${pids[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  exit "$code"
}
trap 'stop_children 129' HUP
trap 'stop_children 130' INT
trap 'stop_children 143' TERM

deliver_repo() {
  local repo="$1" calendar_rc WORKER
  WORKER="${CM_WORKER:-$repo/ops/mac-local-run/parse_and_push.sh}"
  cd "$repo" || return 1
  "$PYTHON" ops/mac-local-run/cloud_run_ok.py --is-working-day
  calendar_rc=$?
  if [ "$calendar_rc" -eq 1 ]; then
    echo "delivery_all: $repo — нерабочий день"
    return 0
  elif [ "$calendar_rc" -ne 0 ]; then
    echo "delivery_all: $repo — не удалось проверить календарь" >&2
    return 1
  fi
  if [ "$CHECK_ONLY" = "1" ]; then
    if "$PYTHON" ops/mac-local-run/cloud_run_ok.py --can-deliver; then
      echo "delivery_all: $repo — локальный выпуск готов, отправка не запускалась"
    else
      echo "delivery_all: $repo — локального готового выпуска нет"
    fi
    return 0
  fi
  # Независимый от запуска parse-процесса контроль: после 10:00 сообщает,
  # если сегодня нет завершённого прогона и живого писателя в общем lock.
  # Проверяем и закрытый день: delivered_at сам по себе не доказывает обход.
  # Собственный lock отчёта не мешает штатной доставочной транзакции.
  if [ -f scripts/technical_report.py ]; then
    "$PYTHON" scripts/technical_report.py ops --event watchdog --repo "$repo" \
      --telegram-config "$HOME/.config/court-monitor/telegram" \
      || echo "delivery_all: $repo — технический контроль не завершён" >&2
  fi
  # Подтверждённый закрытый день не требует Git-запросов. При любом
  # незавершённом journal этот быстрый выход запрещён: worker обязан
  # восстановить транзакцию прежде, чем доверять локальному delivered_at.
  if "$PYTHON" ops/mac-local-run/cloud_run_ok.py --report >/dev/null 2>&1 \
    && [ ! -e ops/mac-local-run/.runtime/delivery_txn.json ] \
    && [ ! -e ops/mac-local-run/.runtime/parse_txn.json ]; then
    return 0
  fi
  exec bash "$WORKER" "$repo" --deliver-pending
}

while IFS= read -r repo; do
  if [ ! -d "$repo/.git" ]; then
    echo "delivery_all: нет клона $repo" >&2
    rc=1
    continue
  fi
  # Без stagger: медленный git одной территории не задерживает готовую
  # соседнюю. Здесь нет тяжёлого парсинга или обработки очереди импортов.
  deliver_repo "$repo" &
  pids+=("$!")
done < <(cm_territories)

for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
trap - HUP INT TERM
exit "$rc"
