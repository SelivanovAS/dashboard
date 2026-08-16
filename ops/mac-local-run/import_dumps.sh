#!/bin/bash
# =============================================================================
# Court Monitor — импорт дампов капчёвых судов НА MAC (резерв канала оператора).
#
# ЗАЧЕМ. Оператор решает проверочный код, вставляет выдачу суда в админку →
# Worker кладёт дамп в KV и диспатчит import_cases.yml. Пока суды режут адреса
# облачных раннеров (16.08.2026: страница защиты ГАС с HTTP 200, 0 карточек из
# 10), облачный импорт заводит НОЛЬ: правила приёма исков банка решаются только
# по карточке, и строка теряется целиком. Cloudflare и его KV при этом живы —
# они на sudrf не ходят. Значит, ту же работу может сделать этот Mac из сети
# Сбера: тем же импортёром, по тем же эндпоинтам Worker'а, с тем же отчётом в
# журнал админки (оператор ничего нового не делает).
#
# ПОТОК (зеркало .github/workflows/import_cases.yml):
#   GET /admin/import-log  → какие дампы облако не смогло обработать
#   GET /import-dump?key=  → сам дамп из KV (Bearer PUSH_SECRET)
#   scripts/import_search_dump.py → ops/stage_data_files.sh → commit + push
#   POST /import-result    → сводка оператору (пейлоад — ops/import_result_body.jq,
#                            ОДИН файл с облаком: копия разъехалась бы молча)
#
# ЗАПУСК (обычно — из parse_all.sh следом за парсингом территории):
#   bash ops/mac-local-run/import_dumps.sh [путь-к-клону]
#   bash ops/mac-local-run/import_dumps.sh [клон] --dry-run   # ничего не пишем
#   bash ops/mac-local-run/import_dumps.sh [клон] --check     # только диагностика
#   bash ops/mac-local-run/import_dumps.sh [клон] --file дамп.html --court домен
#
# Последняя форма — для дампа, которого в KV уже нет (TTL 24 ч) или который
# прислали файлом: Worker не участвует вовсе.
#
# НАСТРОЙКА МАШИНЫ: ~/.config/court-monitor/worker.<регион> (chmod 600), три
# строки — url=, owner_secret=, push_secret=. Нет файла → работает только
# --file: очередь без адреса Worker'а не забрать.
# =============================================================================
set -u

# ── Общий слой сети Сбера (преflight, маршруты, ssh-адрес, Telegram) ─────────
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_sber_net.sh"

# ── Аргументы ────────────────────────────────────────────────────────────────
CHECK_ONLY=0
DRY_RUN=0
ANYWHERE=0
FILE_ARG=""
COURT_ARG=""
REPO_ARG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --check)   CHECK_ONLY=1 ;;
    --dry-run) DRY_RUN=1 ;;
    # Проверка вне офиса: не требовать шлюз Сбера, но суды всё равно обязаны
    # ответить по-настоящему. Домашний провайдер РФ суды пускает, корпоративный
    # VPN — нет; проба решает. Ежедневный запуск ключа не получает: из офиса
    # ходим через шлюз, а тихий пропуск дома остаётся тихим.
    --anywhere) ANYWHERE=1 ;;
    --file)    shift; FILE_ARG="${1:-}" ;;
    --court)   shift; COURT_ARG="${1:-}" ;;
    -*)        echo "неизвестный ключ: $1" >&2; exit 2 ;;
    # ПЕРВЫЙ позиционный побеждает: parse_all.sh передаёт путь клона первым
    # аргументом и добавляет свои «$@» следом (см. тот же приём в
    # parse_and_push.sh — иначе случайный путь перекрыл бы клон итерации).
    *)         [ -n "$REPO_ARG" ] || REPO_ARG="$1" ;;
  esac
  shift
done

REPO="${REPO_ARG:-${CM_REPO:-/Users/aleksandrselivanov/dashboard}}"
PYTHON="/usr/bin/python3"
LOG_DIR="$REPO/ops/mac-local-run"
LOG="$LOG_DIR/import_dumps.log"
# Лок ОБЩИЙ с parse_and_push.sh: импорт и парсинг одного клона пишут в один
# индекс git — параллельный запуск дал бы «index.lock» и потерянный коммит.
LOCK="$LOG_DIR/.run.lock"
CONF_DIR="$HOME/.config/court-monitor"
# workers.dev режет дефолтный UA некоторых клиентов (ошибка 1010 → 403 до
# Worker'а; инцидент живого лога 13–16.07.2026) — представляемся явно.
UA="court-monitor-import-mac/1.0"
DUMP_TTL=86400          # столько живёт дамп в KV — старше не забрать
STARTED_GRACE=900       # запись «идёт» моложе 15 мин — облачный джоб ещё жив

# ── Утилиты ──────────────────────────────────────────────────────────────────
ts()  { date '+%Y-%m-%d %H:%M:%S'; }
# Запуск руками (терминал) — дублируем строку на экран: юрист смотрит на неё,
# а не в лог-файл. Из-под launchd stdout не терминал, и лог остаётся тихим.
log() {
  echo "$(ts) $*" >>"$LOG"
  if [ -t 1 ]; then echo "$*"; fi
  return 0
}
notify() {
  /usr/bin/osascript -e "display notification \"$1\" with title \"Импорт дампов\"" >/dev/null 2>&1 || true
}
alert_telegram() { cm_alert_telegram "$CONF_DIR" "Импорт дампов ($(basename "$REPO"))" "$1"; }
die() {
  log "ERROR: $1"; notify "Ошибка: $1"; alert_telegram "$1"
  exit 1
}

mkdir -p "$LOG_DIR"
if ! mkdir "$LOCK" 2>/dev/null; then
  log "Занято другим прогоном ($LOCK) — выход"
  exit 0
fi
TMP_DIR=$(mktemp -d) || { rmdir "$LOCK" 2>/dev/null; exit 1; }
# mktemp -d даёт каталог с правами 700 — секретов в argv нет, и конфиги curl
# внутри него недоступны другим пользователям машины.
trap 'rm -rf "$TMP_DIR"; rmdir "$LOCK" 2>/dev/null' EXIT

if [ -f "$LOG" ] && [ "$(wc -l < "$LOG")" -gt 4000 ]; then
  tail -n 2000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
fi

log "=================================================================="
log "Старт import_dumps (pid $$)$([ "$DRY_RUN" = "1" ] && echo " · DRY-RUN")"

cd "$REPO" || die "нет каталога $REPO"
command -v jq >/dev/null 2>&1 || die "нужен jq (brew install jq) — им разбираются журнал и отчёт"

# ── Территория: есть ли тут вообще капчёвые суды ─────────────────────────────
# Дампы существуют только там, где поиск закрыт проверочным кодом (Свердловская
# обл.). На ХМАО импортировать нечего — молча выходим, чтобы ежедневный
# parse_all.sh не пугал юриста «нет настроек Worker'а».
REGION_CODE=$(cm_region_code "$PYTHON")
[ -n "$REGION_CODE" ] || die "не смог определить регион клона $REPO"
GATED=$("$PYTHON" -c 'import sys; sys.path.insert(0, "scripts");
from court_monitor.regions import get_region
print(sum(1 for c in get_region().first_instance_courts if c.search_gated))' 2>/dev/null)
if [ "${GATED:-0}" = "0" ]; then
  log "Территория $REGION_CODE: капчёвых судов нет — дампы не импортируются, выход"
  exit 0
fi

# ── Worker: адрес и секреты ──────────────────────────────────────────────────
# Читаем awk-ом, а НЕ `source`: файл env.<регион> уходит в окружение прогона,
# и PUSH_SECRET+PUSH_WORKER_URL там включили бы вторую доставку push с Mac.
WORKER_CONF="$CONF_DIR/worker.$REGION_CODE"
WORKER_URL=""; OWNER_SECRET=""; PUSH_SECRET=""
if [ -f "$WORKER_CONF" ]; then
  WORKER_URL=$(awk -F= '/^url=/{print $2}' "$WORKER_CONF" | tr -d '[:space:]')
  OWNER_SECRET=$(awk -F= '/^owner_secret=/{print $2}' "$WORKER_CONF" | tr -d '[:space:]')
  PUSH_SECRET=$(awk -F= '/^push_secret=/{print $2}' "$WORKER_CONF" | tr -d '[:space:]')
  WORKER_URL="${WORKER_URL%/}"
fi

# Секреты — через конфиг curl (`-K файл`), а не аргументами командной строки:
# в argv их видит любой `ps` на машине.
CURL_CFG="$TMP_DIR/curl.cfg"
worker_cfg() {  # $1 = путь+query — пишет конфиг curl (адрес + авторизация)
  {
    printf 'header = "Authorization: Bearer %s"\n' "$PUSH_SECRET"
    printf 'url = "%s%s"\n' "$WORKER_URL" "$1"
  } > "$CURL_CFG"
}
# Журнал импортов: секрет в query — как у самой админки, поэтому URL уходит
# через конфиг curl, а не argv.
journal_cfg() {
  printf 'url = "%s/admin/import-log?secret=%s&logonly=1"\n' \
    "$WORKER_URL" "$OWNER_SECRET" > "$CURL_CFG"
}

# ── Преflight: сеть Сбера, маршруты, живой суд ───────────────────────────────
# Импортёр ходит в суд за карточками (без них иск банка не заводится вовсе),
# поэтому проверки те же, что у парсинга.
sber_preflight() {  # 0 — можно идти в суды
  if cm_in_sber_network; then
    log "Сеть Сбера подтверждена (шлюз $CM_SBER_GATEWAY)"
    cm_setup_court_routes "$PYTHON" log \
      || die "не удалось получить домены судов из реестра региона — маршруты не построить"
  elif [ "$ANYWHERE" = "1" ]; then
    # Маршруты офиса, оставшиеся в таблице, вне сети Сбера ведут в никуда —
    # суды перестали бы открываться вообще, а выглядело бы это как блок.
    log "Не в сети Сбера, но задан --anywhere: маршруты не строим, спрашиваем суд напрямую"
    cm_clear_court_routes "$PYTHON" log
  else
    return 1
  fi
  # Коды: 0 — идём, 1 — мы не там (тихий пропуск), 2 — суды не отвечают.
  # Три состояния, а не два: «мы дома» — это не ошибка и алерта не стоит, а
  # «мы в офисе, но суд молчит» — ошибка, о которой надо кричать. Причина
  # остаётся в PREFLIGHT_ERR: её печатает вызыватель (в --check — строкой
  # отчёта, в боевом пути — текстом ошибки).
  PROBE_HOST=$(cm_probe_court_host "$PYTHON") || {
    PREFLIGHT_ERR="не смог определить суд для пробы доступности"; return 2; }
  if PREFLIGHT_ERR=$(cm_court_reachable "$PROBE_HOST"); then
    log "Суд $PROBE_HOST доступен"
    return 0
  fi
  return 2
}

# ── --check: отчёт по пунктам, ничего не меняем ──────────────────────────────
# Каждый пункт проверяется ОТДЕЛЬНО и не отменяет остальные: настройки Worker'а
# юрист заводит дома, а суды доступны только из офиса — требовать всё сразу
# значило бы «проверить нельзя никогда».
if [ "$CHECK_ONLY" = "1" ]; then
  log "Проверка: клон $REPO · территория $REGION_CODE · капчёвых судов $GATED"
  PREFLIGHT_ERR=""
  sber_preflight; PRE_RC=$?
  case "$PRE_RC" in
    0) log "✓ суды отвечают — карточки читать можно" ;;
    1) log "— сеть Сбера: НЕТ (шлюз $CM_SBER_GATEWAY среди маршрутов не найден)"
       log "  проверяйте из офиса; либо выключите VPN и добавьте ключ --anywhere" ;;
    *) log "✗ суды не отвечают: ${PREFLIGHT_ERR:-без диагноза}"
       log "  это и есть блок — карточки не прочитаются, дела не заведутся" ;;
  esac
  if [ ! -f "$WORKER_CONF" ]; then
    log "✗ настройки Worker'а: нет файла $WORKER_CONF (см. README)"
  elif [ -z "$WORKER_URL" ] || [ -z "$OWNER_SECRET" ] || [ -z "$PUSH_SECRET" ] \
       || case "$OWNER_SECRET$PUSH_SECRET" in *…*) true ;; *) false ;; esac; then
    log "✗ настройки Worker'а: в $WORKER_CONF пусто или остались «…» —"
    log "  впишите настоящие url / owner_secret / push_secret"
  else
    journal_cfg
    code=$(curl -s -o "$TMP_DIR/log.json" -w '%{http_code}' -m 30 -A "$UA" \
      -K "$CURL_CFG" 2>/dev/null)
    case "$code" in
      200) log "✓ журнал импортов читается (owner_secret подходит)" ;;
      401) log "✗ owner_secret не подходит — Worker ответил 401" ;;
      *)   log "✗ журнал импортов: Worker ответил $code" ;;
    esac
    # Пустой uuid: авторизация проверяется до поиска ключа, поэтому 404 —
    # это «секрет подходит, просто такого дампа нет».
    worker_cfg "/import-dump?key=import:dump:00000000-0000-0000-0000-000000000000"
    code=$(curl -s -o /dev/null -w '%{http_code}' -m 30 -A "$UA" -K "$CURL_CFG" 2>/dev/null)
    case "$code" in
      404|400) log "✓ push_secret подходит (Worker пустил, дампа с таким ключом нет)" ;;
      401)     log "✗ push_secret не подходит — Worker ответил 401" ;;
      *)       log "✗ доступ к дампам: Worker ответил $code" ;;
    esac
    if [ "$code" != "401" ] && [ -s "$TMP_DIR/log.json" ]; then
      n=$(jq -r --argjson now "$(date +%s)" --argjson ttl "$DUMP_TTL" \
             --argjson grace "$STARTED_GRACE" \
             -f "$REPO/ops/mac-local-run/import_queue.jq" "$TMP_DIR/log.json" \
             2>/dev/null | grep -c . || true)
      log "  дампов, которые резерв забрал бы прямо сейчас: ${n:-0}"
    fi
  fi
  log "Проверка закончена: ничего не менялось"
  exit 0
fi

# ── Боевой путь ──────────────────────────────────────────────────────────────
PREFLIGHT_ERR=""
sber_preflight; PRE_RC=$?
if [ "$PRE_RC" = "1" ]; then
  log "Пропуск: шлюз $CM_SBER_GATEWAY не найден среди default-маршрутов (не в сети Сбера)"
  exit 0
elif [ "$PRE_RC" != "0" ]; then
  # Мы там, где суды обязаны отвечать, а они не отвечают — это не рутина,
  # об этом надо кричать (в облаке ту же роль играет шаг `if: failure()`).
  die "суд $PROBE_HOST не отвечает: ${PREFLIGHT_ERR:-без диагноза} — импорт пропущен"
fi

# BANK_TRACK и кэпы в облаке живут Actions Variables — без файла территории
# импорт пошёл бы с дефолтами кода, то есть иначе, чем в облаке.
cm_load_territory_env "$PYTHON" "$CONF_DIR" log
# Операторский импорт: ретраи полезны — запросов мало, повтор дороже
# (боевой дефолт FETCH_MAX_RETRIES=1). Зеркало import_cases.yml.
export FETCH_MAX_RETRIES="${FETCH_MAX_RETRIES:-3}"
# Предохранитель под размер ДАМПА, а не боевого прогона — те же значения, что
# у облака (env в import_cases.yml, страж test_breaker_settings_match_cloud):
# дефолты кода (3 отказа, проба каждые 30 пропущенных) считаны на обход сотен
# карточек, а в дампе на 25 строк «каждые 30» означает «никогда», и мигающий
# суд не восстановится внутри импорта. 16.08.2026 это стоило дампа
# Верх-Исетского: три отказа подряд сняли суд с обхода, 12 исков банка не
# запрашивались вовсе.
export CARD_BREAKER_THRESHOLD="${CARD_BREAKER_THRESHOLD:-5}"
export CARD_BREAKER_PROBE_EVERY="${CARD_BREAKER_PROBE_EVERY:-3}"

GIT_URL=$(cm_git_ssh_url) || die "не смог вывести ssh-адрес из origin ($GIT_URL)"
export GIT_SSH_COMMAND="$(cm_git_ssh_command)"

if ! git pull --rebase --autostash "$GIT_URL" main >>"$LOG" 2>&1; then
  die "git pull --rebase не удался (см. лог)"
fi

post_body() {  # $1 = файл с JSON-телом (само тело не секрет — идёт аргументом)
  worker_cfg "/import-result"
  curl -s -m 20 -A "$UA" -X POST -K "$CURL_CFG" \
    -H "Content-Type: application/json" --data @"$1" -o /dev/null || true
}
post_status() {  # $1 = ключ дампа, $2 = статус
  # DRY-RUN не трогает журнал оператора вовсе: иначе холостая проверка
  # перевела бы запись в «идёт…» и оставила её такой навсегда.
  [ -n "$WORKER_URL" ] && [ "$DRY_RUN" != "1" ] || return 0
  jq -n --arg dk "$1" --arg st "$2" '{dump_key:$dk, status:$st}' \
    > "$TMP_DIR/body.json" && post_body "$TMP_DIR/body.json"
}
post_error() {  # $1 = ключ дампа, $2 = текст
  [ -n "$WORKER_URL" ] || return 0
  jq -n --arg dk "$1" --arg er "$2" '{dump_key:$dk, status:"failed", error:$er}' \
    > "$TMP_DIR/body.json" && post_body "$TMP_DIR/body.json"
}
post_summary() {  # $1 = ключ дампа, $2 = статус, $3 = summary импортёра
  [ -n "$WORKER_URL" ] || return 0
  # Пейлоад ОДИН с облаком: ops/import_result_body.jq. Своя сборка здесь
  # означала бы молча разъехавшиеся счётчики — этим проект уже болел дважды.
  jq -c --arg dk "$1" --arg st "$2" --arg ru "" \
     -f "$REPO/ops/import_result_body.jq" "$3" > "$TMP_DIR/body.json" \
    && post_body "$TMP_DIR/body.json"
}

# ── Коммит и пуш результата одного импорта ───────────────────────────────────
commit_and_push() {  # $1 = имя суда, $2 = added, $3 = added_bank
  local suffix=""
  # Список файлов ОДИН с облаком: пути спрашиваются у court_monitor.config.
  bash ops/stage_data_files.sh >>"$LOG" 2>&1 || return 1
  if git diff --cached --quiet; then
    log "  изменений в данных нет — коммит не нужен"
    return 0
  fi
  [ "$3" != "0" ] && suffix=" · 🏦 +$3 в трек"
  git -c user.name="Court Monitor (Mac)" -c user.email="bot@court-monitor.local" \
      commit -m "📥 Импорт (Mac): $1 +$2$suffix" >>"$LOG" 2>&1 || return 1
  # Ретраи: облачный джоб или парсинг могли запушить между pull и push.
  local i
  for i in 1 2 3; do
    git push "$GIT_URL" HEAD:main >>"$LOG" 2>&1 && return 0
    log "  push отклонён — подтягиваю чужие коммиты и повторяю ($i/3)"
    git pull --rebase --autostash "$GIT_URL" main >>"$LOG" 2>&1
    sleep 3
  done
  git push "$GIT_URL" HEAD:main >>"$LOG" 2>&1
}

# ── Один импорт: дамп-файл → cases.json → отчёт ──────────────────────────────
run_import() {  # $1 = файл дампа, $2 = домен суда, $3 = оператор, $4 = ключ|""
  local dump="$1" domain="$2" operator="$3" key="$4"
  local summary="$TMP_DIR/summary.json" rc=0 added added_bank court status
  local dry=""
  rm -f "$summary"
  # Флаг строкой, а не массивом: /bin/bash на macOS — 3.2, и раскрытие пустого
  # массива "${a[@]}" под `set -u` там падает «unbound variable».
  [ "$DRY_RUN" = "1" ] && dry="--dry-run"
  IMPORT_SUMMARY_PATH="$summary" "$PYTHON" scripts/import_search_dump.py "$dump" \
    --court-domain "$domain" --operator "$operator" $dry >>"$LOG" 2>&1 || rc=$?
  if [ ! -s "$summary" ]; then
    log "  ERROR: импортёр упал до разбора дампа (код $rc)"
    [ -n "$key" ] && post_error "$key" \
      "резерв на Mac: импортёр упал до разбора дампа (код $rc)"
    return 1
  fi
  added=$(jq -r '.added // 0' "$summary")
  added_bank=$(jq -r '.added_bank // 0' "$summary")
  court=$(jq -r '.court // .court_domain // "?"' "$summary")
  log "  итог: +$added в картотеку · +$added_bank в иски банка (код $rc)"

  status=done
  if [ "$rc" -ne 0 ]; then
    status=failed
  elif [ "$DRY_RUN" = "1" ]; then
    log "  DRY-RUN: коммит и отчёт пропущены"
    return 0
  elif ! commit_and_push "$court" "$added" "$added_bank"; then
    # Зеркало облака: «done» только когда И импорт отработал, И данные уехали.
    status=failed
    jq '.error = "резерв на Mac: дамп обработан, но коммит не запушился — повторите импорт, уже добавленное отсеет дедуп"' \
      "$summary" > "$summary.tmp" && mv "$summary.tmp" "$summary"
    log "  ERROR: коммит/push не удался"
  fi
  [ -n "$key" ] && post_summary "$key" "$status" "$summary"
  [ "$status" = "done" ]
}

# ── Режим 1: локальный файл (Worker не участвует) ────────────────────────────
if [ -n "$FILE_ARG" ]; then
  [ -f "$FILE_ARG" ] || die "нет файла дампа: $FILE_ARG"
  [ -n "$COURT_ARG" ] || die "с --file обязателен --court <домен суда>"
  log "Локальный дамп: $FILE_ARG · суд $COURT_ARG"
  if run_import "$FILE_ARG" "$COURT_ARG" "${USER:-оператор} (Mac)" ""; then
    notify "Дамп импортирован ($COURT_ARG)"
    log "Готово"
    exit 0
  fi
  die "импорт локального дампа не удался (см. лог)"
fi

# ── Режим 2: очередь Worker'а ────────────────────────────────────────────────
if [ -z "$WORKER_URL" ] || [ -z "$OWNER_SECRET" ] || [ -z "$PUSH_SECRET" ]; then
  log "Нет $WORKER_CONF (url/owner_secret/push_secret) — очередь не забрать, выход"
  exit 0
fi

journal_cfg
if ! curl -f -s -m 30 -A "$UA" -K "$CURL_CFG" -o "$TMP_DIR/log.json"; then
  die "журнал импортов не читается ($WORKER_URL/admin/import-log)"
fi

# Кого берём — правила в ops/mac-local-run/import_queue.jq (отдельным файлом,
# чтобы их проверял тест, а не только глаза).
NOW=$(date +%s)
jq -r --argjson now "$NOW" --argjson ttl "$DUMP_TTL" --argjson grace "$STARTED_GRACE" \
   -f "$REPO/ops/mac-local-run/import_queue.jq" "$TMP_DIR/log.json" \
   > "$TMP_DIR/queue.tsv" \
  || die "не разобрал журнал импортов (jq)"

QUEUE=$(wc -l < "$TMP_DIR/queue.tsv" | tr -d ' ')
if [ "$QUEUE" = "0" ]; then
  log "Очередь пуста — необработанных дампов за сутки нет"
  exit 0
fi
log "К обработке дампов: $QUEUE"

rc=0
done_n=0
# Читаем очередь с ОТДЕЛЬНОГО дескриптора: git/ssh внутри цикла иначе могли бы
# съесть остаток файла со stdin, и часть дампов молча не обработалась бы.
while IFS=$'\t' read -r uuid domain operator prev <&3; do
  [ -n "$uuid" ] || continue
  key="import:dump:$uuid"
  log "→ $domain · оператор ${operator:-—} · в журнале «$prev» · $uuid"
  dump="$TMP_DIR/dump.html"
  post_status "$key" started
  worker_cfg "/import-dump?key=$key"
  if ! curl -f -s -m 60 -A "$UA" -K "$CURL_CFG" -o "$dump"; then
    log "  ERROR: дамп не скачался — истёк TTL 24 ч или сеть"
    post_error "$key" "резерв на Mac: дамп не скачался из KV (истёк TTL 24 ч?) — вставьте выдачу заново"
    rc=1
    continue
  fi
  size=$(wc -c < "$dump" | tr -d ' ')
  if [ "$size" -lt 512 ]; then
    log "  ERROR: дамп подозрительно мал ($size байт)"
    post_error "$key" "резерв на Mac: дамп подозрительно мал ($size байт) — вставьте выдачу заново"
    rc=1
    continue
  fi
  log "  дамп: $size байт"
  if run_import "$dump" "$domain" "$operator" "$key"; then
    done_n=$((done_n + 1))
  else
    rc=1
  fi
done 3< "$TMP_DIR/queue.tsv"

log "Готово: обработано $done_n из $QUEUE (код $rc)"
if [ "$rc" -ne 0 ]; then
  alert_telegram "часть дампов не обработана ($done_n из $QUEUE) — см. лог резерва"
  notify "Импорт: обработано $done_n из $QUEUE"
else
  notify "Импорт дампов: $done_n из $QUEUE"
fi
exit "$rc"
