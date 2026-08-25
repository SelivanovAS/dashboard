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
#   bash ops/mac-local-run/parse_and_push.sh [клон] --deliver-pending
#       # доставить свежий pending-контекст без пробы судов и без парсинга
# =============================================================================

# ── Аргументы ────────────────────────────────────────────────────────────────
CHECK_ONLY=0
FORCE=0
ANYWHERE=0
IGNORE_CALENDAR=0
DELIVER_PENDING_ONLY=0
REPO_ARG=""
for arg in "$@"; do
  case "$arg" in
    --check) CHECK_ONLY=1 ;;
    # Мимо гейта «облако уже отработало»: пульт и ручной запуск юриста. Агент
    # флага НЕ получает — при зрячем облачном прогоне он обязан тихо выйти,
    # иначе две машины делают одну работу и дайджест уходит дважды.
    --force) FORCE=1 ;;
    # Запуск вне сети Сбера (дом, выключенный корпоративный VPN): маршруты не
    # строим, суды спрашиваем напрямую — честная проба решает, есть ли доступ.
    --anywhere) ANYWHERE=1 ;;
    # Прогнать и в нерабочий день (пульт спрашивает юриста «всё равно
    # прогнать?») — зеркало галки ignore_calendar облачной админки. Календарь
    # решает Python: здесь только проводка env для run_parse.py.
    --ignore-calendar) IGNORE_CALENDAR=1 ;;
    # Служебная фаза родительского parse_all.sh после wait обеих территорий:
    # ни маршрутов, ни канареек, ни run_parse.py — только pending-контекст через
    # ту же exact-once доставочную транзакцию, что и обычный финиш.
    --deliver-pending) DELIVER_PENDING_ONLY=1 ;;
    -*)      echo "неизвестный ключ: $arg" >&2; exit 2 ;;
    # ПЕРВЫЙ позиционный побеждает: parse_all.sh передаёт путь клона первым
    # аргументом и добавляет свои «$@» следом — если бы побеждал последний,
    # случайный путь в аргументах драйвера перекрыл бы клон КАЖДОЙ итерации
    # (один репозиторий прогнался бы дважды, остальные молча пропали).
    *)       [ -n "$REPO_ARG" ] || REPO_ARG="$arg" ;;
  esac
done
if [ "$DELIVER_PENDING_ONLY" = "1" ] \
  && { [ "$CHECK_ONLY" = "1" ] || [ "$FORCE" = "1" ]; }; then
  echo "--deliver-pending нельзя совмещать с --check или --force" >&2
  exit 2
fi

# ── Общий слой сети Сбера (маршруты, преflight, ssh-адрес) ───────────────────
# Тот же файл подключает import_dumps.sh: копия преflight'а во втором скрипте
# разъехалась бы так же, как разъезжались списки файлов данных и домены судов.
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_sber_net.sh"

# ── Параметры (правь тут при переезде/смене сети) ────────────────────────────
REPO="${REPO_ARG:-${CM_REPO:-/Users/aleksandrselivanov/dashboard}}"
SBER_GATEWAY="$CM_SBER_GATEWAY"        # шлюз сети Сбера (egress РФ). Маршрут
                                       # судов заворачиваем через него, мимо VPN.
PYTHON="/usr/bin/python3"
LOG_DIR="$REPO/ops/mac-local-run"
LOG="$LOG_DIR/parse_and_push.log"
DELIVERY_TXN_TOOL="$REPO/ops/mac-local-run/delivery_txn.py"
DELIVERY_TXN_JOURNAL="$LOG_DIR/.runtime/delivery_txn.json"
PARSE_TXN_TOOL="$REPO/ops/mac-local-run/parse_txn.py"
PARSE_TXN_JOURNAL="$LOG_DIR/.runtime/parse_txn.json"
PARSE_TXN_ACK_FILE="$LOG_DIR/.runtime/parse_txn.ack.json"
RUN_LOCK_TOOL="$REPO/ops/mac-local-run/run_lock.py"
DELIVERY_CONTEXT_PATH="data/last_digest_context.json"
# ── Ротация лога по дням ─────────────────────────────────────────────────────
# Лог писался ОДНИМ файлом с 3 июля: разбор «что было сегодня» каждый раз
# требовал скрипта по маркеру «Старт», а файл рос без предела. Дата файла не
# сегодняшняя → уносим в <имя>-ГГГГММДД.log, старше CM_LOG_KEEP_DAYS удаляем.
CM_LOG_KEEP_DAYS="${CM_LOG_KEEP_DAYS:-14}"
cm_rotate_log() {  # $1 = путь к логу
  local log="$1" day
  [ -f "$log" ] || return 0
  day=$(date -r "$log" +%Y%m%d 2>/dev/null) || return 0
  [ "$day" = "$(date +%Y%m%d)" ] && return 0
  mv "$log" "${log%.log}-$day.log" 2>/dev/null || return 0
  find "$(dirname "$log")" -name "$(basename "${log%.log}")-*.log" \
       -mtime "+$CM_LOG_KEEP_DAYS" -delete 2>/dev/null || true
}
cm_rotate_log "$LOG"
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
alert_telegram() {  # $1 = текст (тело — cm_alert_telegram, общее с импортом)
  cm_alert_telegram "$CONF_DIR" "Mac-парсинг ($(basename "$REPO"))" "$1"
}
release_run_lock() {
  "$PYTHON" "$RUN_LOCK_TOOL" release "$LOCK" "$$" >/dev/null 2>&1 || true
}
die() {  # $1 = текст → в лог, уведомление, Telegram, выход 1
  log "ERROR: $1"; notify "Ошибка: $1"; alert_telegram "$1"
  finish_pusher   # дать pusher'у дослать «ERROR:» в админку (он выйдет сам)
  exit 1
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

# ── Транзакция доставочного marker-коммита ───────────────────────────────────
# delivered_at обязан попасть в ТОТ ЖЕ коммит, который запускает replay. Между
# штампом и подтверждённым push есть несколько crash-boundary (stage, commit,
# SIGKILL, потерянный ответ git push). Локальный journal создаётся ДО штампа и
# не даёт следующему слоту принять «локально закрыто» за «принято GitHub».
#
# rollback всегда условный по delivery_id: запоздавшая транзакция не вправе
# снять штамп уже другого выпуска. Ошибки отката не глотаем — в такой ситуации
# безопаснее остановиться с journal, чем соврать «день снова открыт».
rollback_delivery_transaction() {  # $1 = delivery_id, $2 = причина
  local delivery_id="$1" reason="$2" diff_rc
  log "Delivery rollback ($delivery_id): $reason"
  if ! "$PYTHON" ops/mac-local-run/cloud_run_ok.py \
      --unmark-delivered --delivery-id "$delivery_id" >>"$LOG" 2>&1; then
    log "Delivery rollback НЕ завершён: условный unmark не удался"
    return 1
  fi
  if ! git add -- "$DELIVERY_CONTEXT_PATH" >>"$LOG" 2>&1; then
    log "Delivery rollback НЕ завершён: контекст после unmark не добавлен в index"
    return 1
  fi
  git diff --cached --quiet -- "$DELIVERY_CONTEXT_PATH"
  diff_rc=$?
  if [ "$diff_rc" -eq 1 ]; then
    if ! git -c user.name="Court Monitor (Mac)" -c user.email="bot@court-monitor.local" \
        commit --only -m "↩️ Откат штампа доставки $(date +'%d.%m.%Y %H:%M') (Mac, push не подтверждён)" \
        -- "$DELIVERY_CONTEXT_PATH" \
        >>"$LOG" 2>&1; then
      log "Delivery rollback НЕ завершён: компенсирующий commit не создан"
      return 1
    fi
  elif [ "$diff_rc" -ne 0 ]; then
    log "Delivery rollback НЕ завершён: git diff вернул $diff_rc"
    return 1
  fi
  if ! "$PYTHON" "$DELIVERY_TXN_TOOL" clear \
      "$DELIVERY_TXN_JOURNAL" "$delivery_id" >>"$LOG" 2>&1; then
    log "Delivery rollback НЕ завершён: journal не очищен"
    return 1
  fi
  log "Delivery rollback завершён: штамп снят, день снова открыт"
  return 0
}

rollback_delivery_and_die() {  # $1 = delivery_id, $2 = причина
  local delivery_id="$1" reason="$2"
  if rollback_delivery_transaction "$delivery_id" "$reason"; then
    die "$reason — штамп снят, день остался открытым (см. лог)"
  fi
  die "$reason — ОТКАТ НЕ ПОДТВЕРЖДЁН, journal сохранён; новый marker запрещён (см. лог)"
}

clear_accepted_delivery_or_die() {  # $1 = delivery_id
  local delivery_id="$1"
  if ! "$PYTHON" "$DELIVERY_TXN_TOOL" clear \
      "$DELIVERY_TXN_JOURNAL" "$delivery_id" >>"$LOG" 2>&1; then
    # Remote уже принял marker. Ни при каких обстоятельствах здесь не unmark:
    # replay мог уже начаться, а повтор создал бы второй дневной дайджест.
    die "marker $delivery_id принят GitHub, но journal не очищен; штамп НЕ снимаю"
  fi
}

confirm_ambiguous_delivery() {  # $1 = delivery_id, $2 = marker SHA, $3 = причина
  local delivery_id="$1" marker_sha="$2" reason="$3" remote_state remote_rc
  remote_state=$("$PYTHON" "$DELIVERY_TXN_TOOL" remote-state \
      "$REPO" "$GIT_URL" "$marker_sha" 2>>"$LOG")
  remote_rc=$?
  log "Delivery remote-state ($marker_sha): ${remote_state:-unknown}, rc=$remote_rc"
  case "$remote_rc" in
    0)
      # git push мог вернуть ошибку уже ПОСЛЕ приёма commit. Marker либо сам
      # main, либо его предок (workflow успел дописать replay-коммит).
      clear_accepted_delivery_or_die "$delivery_id"
      log "Неоднозначный push подтверждён по remote: marker принят"
      return 0
      ;;
    1)
      rollback_delivery_and_die "$delivery_id" \
        "$reason; remote подтверждает отсутствие marker-коммита"
      ;;
    *)
      # Не знаем, принял ли GitHub commit. At-least-once rollback здесь опасен:
      # принятый marker уже мог запустить replay. Оставляем stamp+journal; новый
      # слот сначала повторит эту проверку, и только потом дойдёт до daily gate.
      die "$reason — исход push НЕИЗВЕСТЕН; штамп не снимаю, journal сохранён"
      ;;
  esac
}

reconcile_delivery_transaction() {
  local line read_rc status delivery_id marker_sha pre_sha remote_state remote_rc
  local head_sha work_diff_rc index_diff_rc
  line=$("$PYTHON" "$DELIVERY_TXN_TOOL" read "$DELIVERY_TXN_JOURNAL" 2>>"$LOG")
  read_rc=$?
  if [ "$read_rc" -eq 1 ]; then
    return 0  # journal нет — штатный старт
  fi
  if [ "$read_rc" -ne 0 ]; then
    die "delivery journal не читается — автоматическое продолжение опасно"
  fi
  IFS='|' read -r status delivery_id marker_sha pre_sha <<< "$line"
  if [ -z "$delivery_id" ]; then
    die "delivery journal не содержит delivery_id"
  fi
  log "Найдена незавершённая delivery-транзакция: status=$status id=$delivery_id marker=${marker_sha:-—}"
  case "$status" in
    prepared)
      # Скрипт ещё не дошёл до push: crash случился после journal/mark/stage
      # либо сразу после локального commit, но ДО записи marker SHA. Поэтому
      # безопасно условно снять stamp и, если commit успел появиться, создать
      # поверх него компенсирующий commit.
      #
      # Особая граница — SIGKILL МЕЖДУ prepare и mark: delivery_id в контексте
      # ещё нет, поэтому conditional unmark закономерно отказал бы mismatch.
      # Доказываем именно это состояние тройкой: HEAD всё ещё pre_sha, а путь
      # контекста чист и в working tree, и в index. Тогда снимать нечего —
      # достаточно условно удалить journal. Любое отличие идёт через rollback.
      head_sha=$(git rev-parse HEAD 2>>"$LOG") \
        || die "prepared recovery: не удалось определить HEAD"
      git diff --quiet -- "$DELIVERY_CONTEXT_PATH"
      work_diff_rc=$?
      git diff --cached --quiet -- "$DELIVERY_CONTEXT_PATH"
      index_diff_rc=$?
      if [ -n "$pre_sha" ] && [ "$head_sha" = "$pre_sha" ] \
          && [ "$work_diff_rc" -eq 0 ] && [ "$index_diff_rc" -eq 0 ]; then
        if ! "$PYTHON" "$DELIVERY_TXN_TOOL" clear \
            "$DELIVERY_TXN_JOURNAL" "$delivery_id" >>"$LOG" 2>&1; then
          die "prepared recovery до mark: чистый journal не удалось удалить"
        fi
        log "Startup recovery: crash был до mark, штампа нет — journal очищен"
        return 0
      fi
      if ! rollback_delivery_transaction "$delivery_id" \
          "startup recovery подготовленной транзакции (pre_sha=${pre_sha:-—})"; then
        die "prepared delivery-транзакцию не удалось откатить; journal сохранён"
      fi
      ;;
    committed)
      if [ -z "$marker_sha" ]; then
        die "committed delivery journal не содержит marker SHA"
      fi
      remote_state=$("$PYTHON" "$DELIVERY_TXN_TOOL" remote-state \
          "$REPO" "$GIT_URL" "$marker_sha" 2>>"$LOG")
      remote_rc=$?
      log "Startup delivery remote-state ($marker_sha): ${remote_state:-unknown}, rc=$remote_rc"
      case "$remote_rc" in
        0)
          clear_accepted_delivery_or_die "$delivery_id"
          log "Startup recovery: GitHub уже принял marker, локальный штамп сохранён"
          ;;
        1)
          if ! rollback_delivery_transaction "$delivery_id" \
              "startup recovery: remote не содержит marker $marker_sha"; then
            die "отсутствующий на remote marker не удалось откатить; journal сохранён"
          fi
          ;;
        *)
          die "не удалось установить судьбу marker $marker_sha; штамп не снимаю, journal сохранён"
          ;;
      esac
      ;;
    *)
      die "неизвестный status=$status в delivery journal"
      ;;
  esac
}

reconcile_parse_transaction() {
  local result rc
  result=$("$PYTHON" "$PARSE_TXN_TOOL" recover \
      "$PARSE_TXN_JOURNAL" "$PARSE_TXN_ACK_FILE" 2>>"$LOG")
  rc=$?
  if [ "$rc" -eq 1 ]; then
    return 0  # snapshot нет — штатный старт
  fi
  if [ "$rc" -ne 0 ]; then
    die "parse snapshot не удалось восстановить; pull/парсинг запрещены"
  fi
  log "Startup parse-txn recovery: ${result:-завершён}"
}

# ЕДИНСТВЕННАЯ воронка трёх доставочных веток: обычный финиш парсинга,
# доставка накопленного при мёртвых канарейках и финальный sweep родителя.
deliver_and_push() {  # $1 = что доставляем (для лога)
  local purpose="$1" delivery_id pre_sha mark_output mark_rc diff_rc marker_sha

  delivery_id=$("$PYTHON" ops/mac-local-run/cloud_run_ok.py --delivery-id 2>>"$LOG")
  if [ $? -ne 0 ] || [ -z "$delivery_id" ]; then
    die "не удалось получить delivery_id текущего контекста"
  fi
  pre_sha=$(git rev-parse HEAD 2>>"$LOG") || die "не удалось определить SHA до доставки"

  # Crash-boundary №1: journal обязан существовать раньше delivered_at.
  "$PYTHON" "$DELIVERY_TXN_TOOL" prepare \
      "$DELIVERY_TXN_JOURNAL" "$delivery_id" "$pre_sha" >>"$LOG" 2>&1 \
    || die "не удалось подготовить delivery journal для $delivery_id"

  mark_output=$("$PYTHON" ops/mac-local-run/cloud_run_ok.py --mark-delivered 2>>"$LOG")
  mark_rc=$?
  [ -n "$mark_output" ] && log "Delivery mark: $mark_output"
  if [ "$mark_rc" -ne 0 ]; then
    # Даже при rc!=0 os.replace внутри cloud_run_ok мог уже опубликовать stamp,
    # а поздний fsync/другая ошибка — вернуть failure. Journal НЕ удаляем:
    # startup recovery отличит чистое состояние до mark от состоявшегося stamp
    # и условно откатит именно этот delivery_id.
    die "не удалось подтвердить delivered_at; prepared journal сохранён для recovery"
  fi
  if [ "$mark_output" != "$delivery_id" ]; then
    rollback_delivery_and_die "$delivery_id" \
      "mark-delivered вернул чужой delivery_id: ${mark_output:-—}"
  fi

  git add -- "$DELIVERY_CONTEXT_PATH" >>"$LOG" 2>&1 \
    || rollback_delivery_and_die "$delivery_id" "не удалось добавить контекст доставки в index"
  git diff --cached --quiet -- "$DELIVERY_CONTEXT_PATH"
  diff_rc=$?
  if [ "$diff_rc" -eq 0 ]; then
    # Нормальный гейт не допускает сюда уже опубликованный stamp. Но если это
    # recovery/manual force, доказываем судьбу HEAD, а не объявляем успех по
    # одному локальному delivered_at.
    marker_sha=$(git rev-parse HEAD 2>>"$LOG") \
      || rollback_delivery_and_die "$delivery_id" "не удалось определить существующий marker SHA"
    if ! git log -1 --format=%B "$marker_sha" 2>>"$LOG" | grep -q '(Mac-парсинг)'; then
      rollback_delivery_and_die "$delivery_id" \
        "штамп уже стоял, но HEAD не является доставочным marker-коммитом"
    fi
    "$PYTHON" "$DELIVERY_TXN_TOOL" committed \
        "$DELIVERY_TXN_JOURNAL" "$delivery_id" "$marker_sha" >>"$LOG" 2>&1 \
      || rollback_delivery_and_die "$delivery_id" "не удалось записать существующий marker SHA"
    confirm_ambiguous_delivery "$delivery_id" "$marker_sha" \
      "существующий marker требует проверки remote"
    return 0
  elif [ "$diff_rc" -ne 1 ]; then
    rollback_delivery_and_die "$delivery_id" "git diff доставки вернул $diff_rc"
  fi

  git -c user.name="Court Monitor (Mac)" -c user.email="bot@court-monitor.local" \
      commit --only -m "📊 Обновление данных $(date +'%d.%m.%Y %H:%M') (Mac-парсинг)" \
      -- "$DELIVERY_CONTEXT_PATH" \
      >>"$LOG" 2>&1 \
    || rollback_delivery_and_die "$delivery_id" "git commit доставки не удался"
  marker_sha=$(git rev-parse HEAD 2>>"$LOG") \
    || rollback_delivery_and_die "$delivery_id" "не удалось определить marker SHA после commit"
  "$PYTHON" "$DELIVERY_TXN_TOOL" committed \
      "$DELIVERY_TXN_JOURNAL" "$delivery_id" "$marker_sha" >>"$LOG" 2>&1 \
    || rollback_delivery_and_die "$delivery_id" "не удалось записать marker SHA в journal"

  if git push "$GIT_URL" HEAD:main >>"$LOG" 2>&1; then
    clear_accepted_delivery_or_die "$delivery_id"
  else
    confirm_ambiguous_delivery "$delivery_id" "$marker_sha" \
      "git push доставки вернул ошибку"
  fi
  log "Доставка ($purpose): marker $marker_sha принят — GitHub соберёт дайджест"
  return 0
}

prepare_git_transport() {
  # origin у клона — https, учётных данных на машине нет («could not read
  # Username»), а SSH:22 к github.com в этой сети закрыт. Единственный рабочий
  # путь — ssh.github.com:443; URL выводим из origin, чтобы форк и эталон
  # обслуживались одним кодом.
  GIT_URL=$(cm_git_ssh_url) || die "не смог вывести ssh-адрес из origin ($GIT_URL)"
  export GIT_SSH_COMMAND="$(cm_git_ssh_command)"
  log "git через $GIT_URL"
}

sync_git_and_delivery_state() {
  prepare_git_transport

  # Незавершённую доставку доводим ДО pull и дневного гейта. Иначе локальный
  # delivered_at после SIGKILL заставил бы gate молча пропустить день, хотя
  # marker не дошёл до GitHub. --check остаётся строго read-only.
  if [ "$CHECK_ONLY" != "1" ]; then
    reconcile_delivery_transaction
  fi

  # Подтягиваем replay-коммиты GitHub. При --check репозиторий не трогаем:
  # rebase с autostash — уже изменение рабочего дерева.
  if [ "$CHECK_ONLY" != "1" ] && ! git pull --rebase --autostash "$GIT_URL" main >>"$LOG" 2>&1; then
    die "git pull --rebase не удался (см. лог)"
  fi
}

# ── Один экземпляр за раз ─────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
# Обычный mkdir-lock после SIGKILL/потери питания остаётся навсегда и
# не пускает startup recovery. Owner хранит PID + OS start-time:
# живой владелец даёт тихий skip, мёртвый/reused PID атомарно вытесняется.
"$PYTHON" "$RUN_LOCK_TOOL" acquire "$LOCK" "$$" >>"$LOG" 2>&1
LOCK_RC=$?
if [ "$LOCK_RC" -eq 1 ]; then
  log "Другой живой прогон уже идёт ($LOCK) — выход"
  exit 0
elif [ "$LOCK_RC" -ne 0 ]; then
  log "ERROR: не удалось захватить/recover lock ($LOCK), rc=$LOCK_RC"
  notify "Ошибка lock Mac-парсинга"
  alert_telegram "не удалось захватить/recover lock: rc=$LOCK_RC"
  exit 1
fi
trap 'release_run_lock' EXIT

# ── Ротация лога: держим историю нескольких прогонов, но не даём расти вечно ──
if [ -f "$LOG" ] && [ "$(wc -l < "$LOG")" -gt 4000 ]; then
  tail -n 2000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
fi

log "=================================================================="
log "Старт parse_and_push (pid $$)"

cd "$REPO" || die "нет каталога $REPO"

# Обрыв прошлого Python-процесса мог оставить one-shot флаги в data
# раньше дневного контекста. Восстанавливаем/подтверждаем снимок ДО
# сетевого preflight, pull и дневного гейта. --check строго read-only.
if [ "$CHECK_ONLY" != "1" ]; then
  reconcile_parse_transaction
fi

# ── Финальная доставка родителя: БЕЗ судов и БЕЗ повторного парсинга ──────────
# parse_all.sh зовёт этот режим только после wait обеих территорий. Сегодняшний
# инцидент: Урал закончил до 08:45 и оставил черновик, ХМАО держал единственный
# LaunchAgent занятым после 08:45, поэтому отдельный календарный слот не встал в
# очередь. Здесь мы подтягиваем remote, проверяем свежий pending-контекст и
# пропускаем его через СУЩЕСТВУЮЩИЙ deliver_and_push. Вторая реализация
# delivered_at/marker/push запрещена.
if [ "$DELIVER_PENDING_ONLY" = "1" ]; then
  if ! cm_delivery_window_open; then
    log "Delivery-sweep: окно 08:45 ещё не открыто — без действий"
    exit 0
  fi

  sync_git_and_delivery_state
  if CLOUD_STATUS=$("$PYTHON" ops/mac-local-run/cloud_run_ok.py --report 2>/dev/null); then
    log "Delivery-sweep: дайджест уже отправлен ($CLOUD_STATUS) — без действий"
    exit 0
  fi
  if ! "$PYTHON" ops/mac-local-run/cloud_run_ok.py --has-pending >/dev/null 2>&1; then
    log "Delivery-sweep: свежего pending-контекста нет (${CLOUD_STATUS:-статус не прочитался})"
    exit 0
  fi

  # Pending обязан уже входить в черновой commit данных. Локально изменённый
  # контекст означал бы, что фаза 1 не завершилась; marker без её данных мог бы
  # запустить дайджест по состоянию, которого ещё нет на GitHub. Локальный
  # ЧИСТЫЙ, но ещё не запушенный commit допустим: marker-push доставит оба.
  CONTEXT_STATE=$(git status --porcelain -- "$DELIVERY_CONTEXT_PATH") \
    || die "delivery-sweep не смог проверить состояние контекста"
  [ -z "$CONTEXT_STATE" ] \
    || die "delivery-sweep отказался от локально незафиксированного контекста"

  log "Delivery-sweep: найден свежий pending-контекст — отправляем без парсинга"
  deliver_and_push "финальный sweep после завершения территорий"
  notify "Дайджест отправляется ($(basename "$REPO"))"
  log "Готово"
  exit 0
fi

# ── Preflight: мы в сети Сбера? ──────────────────────────────────────────────
# Признак — шлюз Сбера присутствует среди default-маршрутов (в т.ч. когда VPN
# поднят и добавляет свой второй default). Если нет — мы не в офисной сети,
# заворачивать суды некуда, тихо выходим (не ошибка). --anywhere (пульт, дом):
# маршруты не строим, снимаем залипшие офисные, честная проба судов решит ниже.
IN_SBER=0
if cm_in_sber_network; then
  IN_SBER=1
  log "Сеть Сбера подтверждена (шлюз $SBER_GATEWAY)"
elif [ "$ANYWHERE" = "1" ]; then
  log "Не в сети Сбера, но задан --anywhere: маршруты не строим, спрашиваем суды напрямую"
  if [ "${CM_COURT_ROUTES_READY:-0}" = "1" ]; then
    log "Маршруты территорий уже подготовлены общим драйвером"
  else
    cm_clear_court_routes "$PYTHON" log
  fi
else
  log "Пропуск: шлюз $SBER_GATEWAY не найден среди default-маршрутов (не в сети Сбера)"
  notify "Пропуск: не в сети Сбера — дайджест не собран"
  exit 0
fi

start_pusher
sync_git_and_delivery_state

# ── Гейт «один дайджест в день»: дайджест уже отправлен? ─────────────────────
# Решение юриста 20.08.2026 + окно доставки 21.08.2026: слоты 06:00–08:45
# копят новости молча (черновые коммиты без маркера), отправляет первый слот,
# ЗАВЕРШИВШИЙСЯ после 08:45 (штатно — доставочный 08:45, он же последний).
# Гейт пропускает слот ТОЛЬКО когда дайджест дня уже отправлен (delivered_at
# в контексте; облачный ручной прогон ставит его сам через will_deliver) —
# иначе повторный маркер-коммит разослал бы дайджест дважды. Всё остальное —
# работать: журнал здоровья и контекст свежие (git pull строкой выше).
# СТОИТ ДО маршрутов и пробы судов: при отправленном дайджесте агент выходит
# за секунды. --force (пульт, юрист у экрана) гейт пропускает; --check ниже
# печатает статус информационно.
if [ "$CHECK_ONLY" != "1" ] && [ "$FORCE" != "1" ]; then
  if CLOUD_STATUS=$("$PYTHON" ops/mac-local-run/cloud_run_ok.py --report 2>/dev/null); then
    log "Дайджест дня уже отправлен ($CLOUD_STATUS) — пропуск, Mac не нужен"
    finish_pusher
    exit 0
  fi
  log "Работаем: ${CLOUD_STATUS:-статус гейта не прочитался}"
fi

# ── Маршрут судов мимо VPN через en0 (только в сети Сбера) ───────────────────
# Домены берём из РЕЕСТРА АКТИВНОГО РЕГИОНА (get_region) — тело в
# lib_sber_net.sh, общее с импортом дампов.
# ⚠️ Раньше домены искались регекспом по scripts/court_monitor/courts.py.
# После регионализации (16.07.2026) реестры уехали в regions/*.py, и регексп
# находил ШЕСТЬ строк из комментариев и докстрингов вместо 20 доменов ХМАО:
# маршруты строились не туда, суды шли через VPN мимо egress РФ, а WARNING
# внутри это молча проглатывал. Поэтому пустой список фатален.
if [ "$IN_SBER" = "1" ]; then
  if [ "${CM_COURT_ROUTES_READY:-0}" = "1" ]; then
    log "Маршруты территорий уже подготовлены общим драйвером"
  else
    cm_setup_court_routes "$PYTHON" log \
      || die "не удалось получить домены судов из реестра региона — маршруты не построить"
  fi
fi

# ── Проверка доступности судов ───────────────────────────────────────────────
# Канарейки — из реестра региона (апелляция + два суда 1-й инст.), а не
# хардкодом ХМАО: на форке проба стучалась бы в чужие суды. Мульти-хост с
# 20.08.2026 (cm_any_court_reachable): sudrf «мигает» пер-хостово, и одиночная
# канарейка давала ложный отказ на всю территорию.
#
# Неудача пробы у АГЕНТА — не повод кричать: слоты идут каждые полчаса
# (06:00–08:30 + доставочный 08:45), и сорвавшуюся пробу добивает следующий
# (20.08.2026 Урал не прошёл в 08:19 и спарсился в 08:30), а алерт на каждую
# неудачу дал 5 одинаковых сообщений за утро. Тихий выход до окна доставки; в
# окне (слот 08:45 — последний, либо Mac проснулся после сна): накоплено
# что-то за утро → доставка накопленного БЕЗ парсинга, иначе — ОДИН алерт в
# день «утро потеряно» (маркер-файл .alerted-parse-ДАТА, живёт рядом с
# логами, в git не попадает). Ручной запуск (--force) кричит сразу — юрист
# смотрит на экран.
#
# ⚠️ ОКНО ДОСТАВКИ, а не дедлайн (решение юриста 21.08.2026 «парсинг с 06:00,
# дайджест не раньше 08:45»): до 08:45 доставки НЕ БЫВАЕТ, даже когда попытка
# удачна, — иначе слот 06:30 разослал бы дайджест в 07:00. Первый слот,
# ЗАВЕРШИВШИЙСЯ после 08:45, отправляет всё накопленное с 06:00.
probe_failed() {  # $1 = диагностика канареек; наружу не возвращается
  local diag="$1" marker
  log "Канарейки не ответили: $diag"
  if [ "$CHECK_ONLY" = "1" ]; then
    # Диагностика: юрист смотрит в лог/пульт — тихий exit 0 замаскировал бы
    # проблему, а Telegram-алерт при ручной проверке лишний.
    log "✗ --check: суды не отвечают — это и есть блок/не та сеть"
    notify "Проверка: суды не отвечают"
    finish_pusher
    exit 1
  fi
  if [ "$FORCE" = "1" ]; then
    die "суды не отвечают ($diag) — парсинг пропущен"
  fi
  if cm_delivery_window_open; then
    if "$PYTHON" ops/mac-local-run/cloud_run_ok.py --has-pending >/dev/null 2>&1; then
      # Утро что-то накопило черновыми попытками — в окне доставки отправляем
      # это без парсинга: доставочный коммит несёт один штамп delivered_at.
      alert_telegram "суды к окну доставки так и не ответили — отправляю дайджест с накопленным: $("$PYTHON" ops/mac-local-run/cloud_run_ok.py --progress 2>/dev/null || echo 'без сводки')"
      deliver_and_push "накопленное утро, суды молчат"
      notify "Дайджест отправляется"
      log "Готово"
      finish_pusher
      exit 0
    fi
    marker="$LOG_DIR/.alerted-parse-$(date +%Y%m%d)"
    if [ ! -f "$marker" ]; then
      rm -f "$LOG_DIR"/.alerted-parse-* 2>/dev/null
      : > "$marker"
      # Статус гейта называет, ЧТО именно не удалось: «поиски слепые» ≠
      # «карточки срезаны» ≠ «прогона не было» — без него алерт пугал при
      # частично доехавших данных (20.08.2026, Урал).
      alert_telegram "утро потеряно: дайджеста не будет (${CLOUD_STATUS:-статус гейта не прочитался}); суды не отвечают ($diag)"
      # «ERROR:» в начале — финальная строка для progress_pusher (END_RE).
      log "ERROR: утро потеряно — суды не отвечают, накопить нечего (алерт отправлен, один в день)"
      finish_pusher
      exit 1
    fi
    log "Тихий выход: алерт «утро потеряно» за сегодня уже уходил"
    finish_pusher
    exit 0
  fi
  notify "Суды не отвечают — попробую следующим слотом"
  log "Тихий пропуск: следующий слот попробует снова"
  finish_pusher
  exit 0
}
if PROBE_HOST=$(cm_any_court_reachable "$PYTHON"); then
  log "Суд $PROBE_HOST доступен (канарейка)"
else
  probe_failed "$PROBE_HOST"
fi

# ── --check: дальше не идём ──────────────────────────────────────────────────
# Диагностика для проверки резерва из офиса: preflight, маршруты и доступность
# суда проверены, а парсинг и push не запускаются — ничего не публикуется.
if [ "$CHECK_ONLY" = "1" ]; then
  # Статус облака — информационно (репозиторий в --check не подтягивался,
  # журнал может быть вчерашним; боевой гейт ниже читает его ПОСЛЕ git pull).
  log "Облако: $("$PYTHON" ops/mac-local-run/cloud_run_ok.py --report 2>/dev/null || echo 'статус не прочитался')"
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
REGION_CODE=$(cm_region_code "$PYTHON")
cm_load_territory_env "$PYTHON" "$CONF_DIR" log

# Crash-consistency «data ↔ дневной контекст». Список тот же, что у
# staging ниже; сам last_digest_context из snapshot исключён как WAL.
# Manifest публикуется только после durable-копий всех файлов.
DATA_FILE_LIST=$(bash ops/stage_data_files.sh --list 2>>"$LOG") \
  || die "не удалось получить список файлов данных"
PARSE_TXN_ID=$("$PYTHON" "$PARSE_TXN_TOOL" prepare \
    "$PARSE_TXN_JOURNAL" "$PARSE_TXN_ACK_FILE" "$REPO" \
    "$DELIVERY_CONTEXT_PATH" 2>>"$LOG" <<< "$DATA_FILE_LIST") \
  || die "не удалось подготовить parse snapshot"
[ -n "$PARSE_TXN_ID" ] || die "parse snapshot не вернул txn_id"

log "Парсинг судов ($REGION_CODE): run_parse.py (main_json без секретов) ..."
# SKIP_CHECKED_TODAY — дочитка слотов: карточки со штампом «сегодня»
# пропускаются, повторная попытка тратит запросы только на недочитанное
# (21.08.2026: второй слот Урала сжёг ~105 из 119 удачных чтений на повторы
# утренних карточек). --force гасит: юрист у пульта хочет полный свежий прогон.
#
# ⚠️ РЕТРАИ И ПРЕДОХРАНИТЕЛЬ: предохранитель остаётся на дефолтах кода
# (порог 3, проба каждые 30), а максимум попыток здесь снова 3 — но теперь
# это только ПОТОЛОК для точной политики netutil.should_retry_fetch. У sudrf два
# ПРОТИВОПОЛОЖНЫХ режима отказа, и лекарство от одного — яд для другого.
#   21.08.2026, МИГАЮЩИЙ блок: таймаутов ноль, все 257 отказов дня —
#   мгновенный Connection reset by peer лотереей по ~70 хостам (тот же суд
#   сбрасывает в 08:16 и отвечает в 08:17). Повтор стоил копейки и спасал
#   карточку, а предохранитель, считанный под «суд лёг» (3 отказа подряд,
#   проба каждые 30 = «никогда» для суда с 5–10 делами), срезал 215 карточек
#   из 273 при 15 реальных отказах. Тогда и поставили 3 / 5 / 10.
#   24.08.2026, режим ОБРАТНЫЙ: reset ноль, все отказы — ReadTimeout по 30 с.
#   Суды отвечают (замер: 26–58 с на sud_delo при 0,2 с на корень сайта),
#   просто медленнее нашего таймаута. Повтор после таймаута — ещё 30 с против
#   того же распределения, то есть заведомо мимо: 40 промахов × 105 с сожгли
#   70 минут из 100, прогон прочитал 20 карточек из 287.
# Какой сегодня режим — константой не угадываем: код видит точный класс и
# elapsed. Быстрый reset/5xx можно повторить; ReadTimeout, connect-timeout,
# CAPTCHA, 4xx и HTTP-200-заглушка завершаются после одной попытки. Поэтому
# сегодняшнее медленное утро не станет длиннее, а мигающий reset получит ещё
# две дешёвые возможности вернуть карточку.
PARSE_TELEMETRY_FILE="$REPO/ops/mac-local-run/.runtime/parse_telemetry.json" \
DIGEST_CONTEXT_REQUIRED=1 \
PARSE_TXN_ID="$PARSE_TXN_ID" \
PARSE_TXN_ACK_FILE="$PARSE_TXN_ACK_FILE" \
FETCH_MAX_RETRIES="${FETCH_MAX_RETRIES:-3}" \
SKIP_NON_WORKING_DAYS=$([ "$IGNORE_CALENDAR" = "1" ] && echo 0 || echo 1) \
SKIP_CHECKED_TODAY=$([ "$FORCE" = "1" ] && echo 0 || echo 1) \
  "$PYTHON" ops/mac-local-run/run_parse.py >>"$LOG" 2>&1
RC=$?
if [ "$RC" -ne 0 ]; then
  PARSE_RECOVERY=$("$PYTHON" "$PARSE_TXN_TOOL" recover \
      "$PARSE_TXN_JOURNAL" "$PARSE_TXN_ACK_FILE" 2>>"$LOG")
  PARSE_RECOVERY_RC=$?
  if [ "$PARSE_RECOVERY_RC" -ne 0 ]; then
    die "парсинг rc=$RC; parse snapshot не восстановлен — см. лог"
  fi
  log "Парсинг rc=$RC; parse-txn: ${PARSE_RECOVERY:-завершён}"
  die "парсинг завершился с кодом $RC (см. лог)"
fi
PARSE_FINISH=$("$PYTHON" "$PARSE_TXN_TOOL" finish \
    "$PARSE_TXN_JOURNAL" "$PARSE_TXN_ACK_FILE" "$PARSE_TXN_ID" 2>>"$LOG")
PARSE_FINISH_RC=$?
if [ "$PARSE_FINISH_RC" -eq 3 ]; then
  die "парсер вернул 0, но изменил data без WAL; snapshot откачен"
elif [ "$PARSE_FINISH_RC" -ne 0 ]; then
  die "не удалось закрыть parse snapshot; следующий старт выполнит recovery"
fi
log "Parse-txn: ${PARSE_FINISH:-завершён}"
log "Парсинг завершён"

# ── Коммит и пуш ──────────────────────────────────────────────
# Список файлов ОДИН с облаком: ops/stage_data_files.sh спрашивает пути у
# court_monitor.config. Прежний ручной список здесь разъехался с workflow —
# не коммитились семь файлов трека «Иски банка» (он появился 25.07.2026,
# уже после того, как резерв усыпили), и флип выбросил бы весь трек.
DATA_FILES=()
while IFS= read -r data_path; do
  [ -n "$data_path" ] || continue
  case "$data_path" in
    *\**)
      # Те же глобы холодных архивов, что раскрывает stage_data_files.sh.
      for data_file in $data_path; do
        [ -e "$data_file" ] && DATA_FILES+=("$data_file")
      done
      ;;
    *)
      [ -e "$data_path" ] && DATA_FILES+=("$data_path")
      ;;
  esac
done <<< "$DATA_FILE_LIST"
[ "${#DATA_FILES[@]}" -gt 0 ] || die "список существующих файлов данных пуст"
bash ops/stage_data_files.sh >>"$LOG" 2>&1 || die "не удалось собрать файлы данных"

# ── «Один дайджест в день»: слать или копить? ────────────────────────────────
# Доставка ⇔ ОКНО ОТКРЫТО (сейчас ≥08:45) либо --force: штамп delivered_at +
# маркер «(Mac-парсинг)» → replay шлёт ОДИН дайджест со ВСЕМ накопленным
# контекстом. Иначе — черновой коммит: данные и накопление публикуются
# (дашборд свежий), replay молчит, алерт-прогресс говорит юристу, сколько
# дочиталось (решение юриста 20.08.2026 — после каждой попытки).
# ⚠️ RUN_OK (вердикт --run-complete: поиски зрячие И карточки ≥85% плана) на
# РЕШЕНИЕ о доставке больше НЕ влияет — только на формулировки алертов.
# Решение юриста 21.08.2026 «дайджест не раньше 08:45»: со слотами от 06:00
# прежняя ветка «удачная попытка → доставка» разослала бы дайджест в 06:30.
# ⚠️ В черновом сообщении НЕ ДОЛЖНО быть подстроки «Mac-парсинг»: гард
# replay_on_push — contains() по сообщению коммита, совпадение подстроки
# разослало бы недособранное утро.
RUN_OK=0
RUN_WHY=$("$PYTHON" ops/mac-local-run/cloud_run_ok.py --run-complete 2>/dev/null) && RUN_OK=1
DELIVER=0
[ "$FORCE" = "1" ] && DELIVER=1
cm_delivery_window_open && DELIVER=1

# ── Фаза 1: данные ───────────────────────────────────────────────────────────
# Сначала публикуем ДАННЫЕ и только потом, отдельным коммитом, штамп доставки.
# Прежний порядок (штамп → коммит → пуш) 24.08.2026 чуть не сжёг день: без сети
# пуш падает, а delivered_at уже лежит в контексте — все следующие слоты выходят
# по гейту, и дайджест не уходит НИКОГДА. Теперь упавший пуш фазы 1 оставляет
# день открытым: данные лежат локально, штампа нет, следующий слот доработает.
# ⚠️ Пустой дифф здесь НЕ повод выйти из скрипта: ранние слоты уже опубликовали
# данные, а доставить накопленный контекст всё равно надо — поэтому пропускаем
# только коммит и отдаём управление фазе 2.
# Scope обязателен и у черновика: пользователь мог заранее оставить в index
# свою работу. Обычный `git commit` включал её в data-push; marker/rollback уже
# защищены `--only`, теперь тот же инвариант действует на обеих фазах.
if git diff --cached --quiet -- "${DATA_FILES[@]}"; then
  log "Данных к публикации нет (нерабочий день или без движения)"
else
  git -c user.name="Court Monitor (Mac)" -c user.email="bot@court-monitor.local" \
      commit --only -m "📊 Данные обновлены $(date +'%d.%m.%Y %H:%M') (Mac, копим дайджест)" \
      -- "${DATA_FILES[@]}" \
      >>"$LOG" 2>&1 || die "git commit данных не удался"
  git push "$GIT_URL" HEAD:main >>"$LOG" 2>&1 || die "git push данных не удался (см. лог)"
fi

# ── Фаза 2: доставка ─────────────────────────────────────────────────────────
# ⚠️ RUN_OK (вердикт --run-complete: поиски зрячие И карточки ≥85% плана) на
# РЕШЕНИЕ о доставке НЕ влияет — только на формулировки алертов. Решение юриста
# 21.08.2026 «дайджест не раньше 08:45»: со слотами от 06:00 прежняя ветка
# «удачная попытка → доставка» разослала бы дайджест в 06:30.
# ⚠️ Маркер «(Mac-парсинг)» обязан стоять на коммите, который НЕСЁТ штамп: гард
# replay_on_push — contains() по сообщению head_commit, и сообщение фазы 1 этой
# подстроки содержать не должно, иначе разошлётся недособранное утро.
if [ "$DELIVER" != "1" ]; then
  log "Черновик запушен: ${RUN_WHY:-вердикт не прочитался}; дайджест копится до 08:45"
  notify "Копим — дайджест уйдёт после 08:45"
  # Алерт-прогресс после КАЖДОЙ неполной попытки — решение юриста 20.08.2026.
  # Удачная попытка до окна молчит: копить дальше нечего, а «всё прочитано»
  # шесть раз за утро — тот же спам, от которого уходили 20.08.
  if [ "$RUN_OK" != "1" ]; then
    alert_telegram "попытка неполная: $("$PYTHON" ops/mac-local-run/cloud_run_ok.py --progress 2>/dev/null || echo 'без сводки') — пробую ещё (слоты каждые полчаса, дайджест после 08:45)"
  fi
  log "Готово"
  finish_pusher
  exit 0
fi

deliver_and_push "обычный финиш; ${RUN_WHY:-вердикт не прочитался}"
notify "Готово: данные обновлены, дайджест собирается"
if [ "$RUN_OK" != "1" ] && [ "$FORCE" != "1" ]; then
  # Окно доставки с неполной попыткой: юрист должен знать, что дайджест
  # неполный (это последний слот утра — дочитывать больше некогда).
  alert_telegram "дайджест отправлен с тем, что дочиталось за утро — $("$PYTHON" ops/mac-local-run/cloud_run_ok.py --progress 2>/dev/null || echo 'без сводки')"
fi

log "Готово"
finish_pusher
