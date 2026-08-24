#!/bin/bash
# =============================================================================
# Court Monitor — разбор очереди ОПЕРАТОРСКИХ ИМПОРТОВ НА MAC (резерв).
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
# КАНАЛОВ ДВА (с 23.08.2026). Второй — точечные пачки «Добавить дела»: они
# умирают от блока ровно так же, а в ССЫЛОЧНОМ режиме (единственный путь для
# капчёвых судов) даже хуже — роль банка решается только по карточке, и без
# неё строка выбрасывается ЦЕЛИКОМ, card-blind записи там не выходит.
# Пачки есть на ЛЮБОЙ территории, поэтому этот скрипт работает и там, где
# капчёвых судов нет вовсе.
#
# ПОТОК (зеркало import_cases.yml и add_cases.yml):
#   GET /admin/import-log  → что облако не смогло обработать (оба канала)
#   GET /import-dump?key=  → сам дамп из KV        (канал дампов)
#   GET /add-case-job?key= → задание пачки из KV   (канал пачек)
#   scripts/import_search_dump.py | scripts/add_cases_targeted.py
#       → ops/stage_data_files.sh → commit + push
#   POST /import-result    → сводка оператору. Пейлоады — ops/import_result_body.jq
#                            и ops/add_case_result_body.jq, ОДНИ файлы с облаком:
#                            копия счётчиков разъехалась бы молча.
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
# Лок ОБЩИЙ с parse_and_push.sh: импорт и парсинг одного клона пишут в один
# индекс git — параллельный запуск дал бы «index.lock» и потерянный коммит.
LOCK="$LOG_DIR/.run.lock"
CONF_DIR="$HOME/.config/court-monitor"
# workers.dev режет дефолтный UA некоторых клиентов (ошибка 1010 → 403 до
# Worker'а; инцидент живого лога 13–16.07.2026) — представляемся явно.
UA="court-monitor-import-mac/1.0"
DUMP_TTL=86400          # столько живёт дамп/задание в KV — старше не забрать
STARTED_GRACE=900       # ДАМП «идёт» моложе 15 мин — облачный джоб ещё жив
# У ПАЧЕК грейс свой и вчетверо больше: add_cases.yml стоит на
# timeout-minutes: 45 (до 20 номеров × все открытые суды с вежливой паузой),
# и дамповые 15 мин пустили бы Mac писать в те же файлы посреди живого
# облачного джоба. Плюс запись может простоять в "dispatched" всё время
# очереди cases-data-write — у группы GitHub живёт один pending.
CASE_STARTED_GRACE=3000

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
# обл.) — на ХМАО их не бывает.
# ⚠️ Но выходить из-за этого НЕЛЬЗЯ (23.08.2026): в очереди теперь второй канал
# — точечные пачки из админки, а их вкладка открыта ОБЕИМ ролям на ЛЮБОЙ
# территории. Прежний ранний выход оставил бы ХМАО вовсе без локальной дочитки.
# Тишину, ради которой он и стоял («не пугать юриста нехваткой настроек»),
# держит гейт Worker-конфига ниже: нет worker.<регион> — тихий выход.
REGION_CODE=$(cm_region_code "$PYTHON")
[ -n "$REGION_CODE" ] || die "не смог определить регион клона $REPO"
GATED=$("$PYTHON" -c 'import sys; sys.path.insert(0, "scripts");
from court_monitor.regions import get_region
print(sum(1 for c in get_region().first_instance_courts if c.search_gated))' 2>/dev/null)
if [ "${GATED:-0}" = "0" ]; then
  log "Территория $REGION_CODE: капчёвых судов нет — дампов не ждём, только пачки"
fi

# ── Worker: адрес и секреты (общий парсер cm_worker_conf — как у пульта) ─────
WORKER_CONF="$CONF_DIR/worker.$REGION_CODE"
WORKER_URL=""; OWNER_SECRET=""; PUSH_SECRET=""
if CONF_LINES=$(cm_worker_conf "$CONF_DIR" "$REGION_CODE"); then
  WORKER_URL=$(echo "$CONF_LINES" | sed -n 1p)
  OWNER_SECRET=$(echo "$CONF_LINES" | sed -n 2p)
  PUSH_SECRET=$(echo "$CONF_LINES" | sed -n 3p)
  # push_secret НЕОБЯЗАТЕЛЕН: Worker принимает и владельческий секрет
  # (importChannelAuthOk). PUSH_SECRET на машине юриста взять неоткуда — он
  # write-only и в Cloudflare, и в GitHub secrets, — а owner_secret у юриста
  # есть всегда: им он открывает админку.
  case "$PUSH_SECRET" in ""|*…*) PUSH_SECRET="$OWNER_SECRET" ;; esac
fi

# Секреты — через конфиг curl (`-K файл`), а не аргументами командной строки:
# в argv их видит любой `ps` на машине.
#
# ⚠️ КАЖДЫЙ запрос идёт с `--compressed`. Без него Worker отдаёт ответ
# ОБРЕЗАННЫМ: замер 16.08.2026 — журнал импортов пришёл 17 757 байт вместо
# 194 710 и оборвался посреди строки. jq на таком молчал (ошибка уходила в
# /dev/null), очередь выглядела пустой при пяти необработанных дампах, а на
# самом дампе это дало бы ЧАСТИЧНЫЙ импорт — половину дел из выдачи.
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

# Каким секретом ходить в канал импорта. Пустой uuid: авторизация проверяется
# ДО поиска ключа, поэтому 404 значит «секрет подошёл, дампа с таким ключом
# нет», а 401 — «не подошёл». Один дешёвый запрос за прогон.
# ⚠️ Настоящий PUSH_SECRET на машине юриста взять неоткуда (write-only и в
# Cloudflare, и в GitHub), и в файле у него легко окажется чужой токен — так и
# случилось 16.08.2026 с progress_token. Поэтому не требуем его, а проверяем:
# не подошёл — молча переходим на владельческий, который Worker тоже принимает.
AUTH_KIND=""
resolve_worker_auth() {
  local code
  worker_cfg "/import-dump?key=import:dump:00000000-0000-0000-0000-000000000000"
  code=$(curl -s --compressed -o /dev/null -w "%{http_code}" -m 30 -A "$UA" -K "$CURL_CFG" 2>/dev/null)
  if [ "$code" != "401" ]; then AUTH_KIND="push_secret"; return 0; fi
  if [ -n "$OWNER_SECRET" ] && [ "$PUSH_SECRET" != "$OWNER_SECRET" ]; then
    PUSH_SECRET="$OWNER_SECRET"
    worker_cfg "/import-dump?key=import:dump:00000000-0000-0000-0000-000000000000"
    code=$(curl -s --compressed -o /dev/null -w "%{http_code}" -m 30 -A "$UA" -K "$CURL_CFG" 2>/dev/null)
    if [ "$code" != "401" ]; then AUTH_KIND="owner_secret"; return 0; fi
  fi
  AUTH_KIND=""
  return 1
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
  # «мы в офисе, но суды молчат» — проблема, о которой решает вызыватель.
  # Причина остаётся в PREFLIGHT_ERR (в --check — строка отчёта, в боевом
  # пути — текст алерта). Канарейка мульти-хост (20.08.2026,
  # cm_any_court_reachable): sudrf «мигает» пер-хостово, и одиночный хост
  # давал ложный отказ на весь импорт.
  if PROBE_HOST=$(cm_any_court_reachable "$PYTHON"); then
    log "Суд $PROBE_HOST доступен (канарейка)"
    return 0
  fi
  PREFLIGHT_ERR="$PROBE_HOST"
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
  elif [ -z "$WORKER_URL" ] || [ -z "$OWNER_SECRET" ] \
       || case "$OWNER_SECRET" in *…*) true ;; *) false ;; esac; then
    log "✗ настройки Worker'а: в $WORKER_CONF пусто или остались «…» —"
    log "  впишите настоящие url и owner_secret (push_secret не обязателен)"
  else
    journal_cfg
    code=$(curl -s --compressed -o "$TMP_DIR/log.json" -w '%{http_code}' -m 30 -A "$UA" \
      -K "$CURL_CFG" 2>/dev/null)
    case "$code" in
      200) log "✓ журнал импортов читается (owner_secret подходит)" ;;
      401) log "✗ owner_secret не подходит — Worker ответил 401" ;;
      *)   log "✗ журнал импортов: Worker ответил $code" ;;
    esac
    if resolve_worker_auth; then
      if [ "$AUTH_KIND" = "push_secret" ]; then
        log "✓ доступ к заданиям: подходит push_secret"
      else
        log "✓ доступ к заданиям: идём владельческим секретом"
        log "  (push_secret в файле не подошёл — это нормально, он не нужен)"
      fi
    else
      log "✗ доступ к заданиям: Worker не принял ни один секрет (401)"
    fi
    if [ -n "$AUTH_KIND" ] && [ -s "$TMP_DIR/log.json" ]; then
      # Разбор отделён от подсчёта: битый ответ обязан быть виден. Раньше
      # ошибка jq уходила в /dev/null, и обрезанный журнал показывал «0».
      if queue=$(jq -r --argjson now "$(date +%s)" --argjson ttl "$DUMP_TTL" \
                    --argjson grace "$STARTED_GRACE" \
                    --argjson cgrace "$CASE_STARTED_GRACE" \
                    -f "$REPO/ops/mac-local-run/import_queue.jq" \
                    "$TMP_DIR/log.json" 2>/dev/null); then
        # Каналы называем по отдельности: «5 в очереди» не отвечает на вопрос
        # оператора «мой дамп подхватят?» — у пачек своя цена и свой грейс.
        n_all=$(printf '%s' "$queue" | grep -c . || true)
        n_case=$(printf '%s' "$queue" | grep -c '^case' || true)
        log "  резерв забрал бы прямо сейчас: $n_all (из них точечных пачек: $n_case)"
      else
        log "  ✗ журнал пришёл битым — импорт бы не пошёл (ответ Worker'а не разобрался)"
      fi
    fi
  fi
  log "Проверка закончена: ничего не менялось"
  exit 0
fi

# ── Боевой путь ──────────────────────────────────────────────────────────────
# Порядок с 20.08.2026: СНАЧАЛА очередь, ПОТОМ суды. Cloudflare доступен из
# любой сети (на sudrf он не ходит), а канарейка судов при пустой очереди
# только шумела: 20.08 четыре слота подряд алертили «oblsud--svd не отвечает»,
# хотя импортировать было нечего. Пустая очередь = тихий выход без единого
# запроса к судам. KV-бюджет: +1-2 ЧТЕНИЯ на слот при недоступных судах
# (лимит чтений 100k/день; инцидент 17.07.2026 был про ЗАПИСИ — их не
# добавилось).

# Сеть/маршруты/канарейка + окружение территории + свежий git — общий хвост
# обоих режимов, зовётся когда работа ТОЧНО есть. $1 = "manual" (--file: юрист
# смотрит на экран — кричим сразу) или "queued", $2 = размер очереди (агент:
# слоты дампов идут до 18:30 и добьют сами — лог + уведомление, алерт не чаще
# раза в день, маркер .alerted-dumps-ДАТА рядом с логами).
courts_gate() {
  local mode="$1" queued="${2:-?}" marker
  PREFLIGHT_ERR=""
  sber_preflight; PRE_RC=$?
  if [ "$PRE_RC" = "1" ]; then
    log "Пропуск: шлюз $CM_SBER_GATEWAY не найден среди default-маршрутов (не в сети Сбера)"
    exit 0
  elif [ "$PRE_RC" != "0" ]; then
    if [ "$mode" = "manual" ]; then
      # Ручной запуск: юрист смотрит — кричим сразу, как раньше.
      die "суды не отвечают: ${PREFLIGHT_ERR:-без диагноза} — импорт пропущен"
    fi
    log "Суды не отвечают: ${PREFLIGHT_ERR:-без диагноза} — очередь ($queued) подождёт следующего слота"
    notify "Дампы ($queued) ждут: суды не отвечают"
    marker="$LOG_DIR/.alerted-dumps-$(date +%Y%m%d)"
    if [ ! -f "$marker" ]; then
      rm -f "$LOG_DIR"/.alerted-dumps-* 2>/dev/null
      : > "$marker"
      alert_telegram "дампы ($queued шт.) ждут, а суды не отвечают — резерв повторит следующим слотом (алерт один в день)"
    fi
    exit 1
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
}

post_body() {  # $1 = файл с JSON-телом (само тело не секрет — идёт аргументом)
  worker_cfg "/import-result"
  curl -s --compressed -m 20 -A "$UA" -X POST -K "$CURL_CFG" \
    -H "Content-Type: application/json" --data @"$1" -o /dev/null || true
}
# Имя ключа в теле отчёта решает КАНАЛ: у дампов dump_key, у точечных пачек
# job_key. Worker принимает оба и по нему же различает канал
# (handleImportResult, worker.js) — путать нельзя, иначе запись журнала не
# найдётся и отчёт молча пропадёт (Worker ответит 404, а мы игнорируем код).
key_field() {  # $1 = ключ (import:dump:… | import:case:…)
  case "$1" in
    import:case:*) echo "job_key" ;;
    *)             echo "dump_key" ;;
  esac
}
post_status() {  # $1 = ключ, $2 = статус
  # DRY-RUN не трогает журнал оператора вовсе: иначе холостая проверка
  # перевела бы запись в «идёт…» и оставила её такой навсегда.
  [ -n "$WORKER_URL" ] && [ "$DRY_RUN" != "1" ] || return 0
  # source:"mac" — чтобы застрявшее «выполняется» называло держателя записи.
  jq -n --arg kf "$(key_field "$1")" --arg k "$1" --arg st "$2" \
     '{($kf): $k, status:$st, source:"mac"}' \
    > "$TMP_DIR/body.json" && post_body "$TMP_DIR/body.json"
}
post_error() {  # $1 = ключ, $2 = текст
  [ -n "$WORKER_URL" ] || return 0
  jq -n --arg kf "$(key_field "$1")" --arg k "$1" --arg er "$2" \
     '{($kf): $k, status:"failed", error:$er, source:"mac"}' \
    > "$TMP_DIR/body.json" && post_body "$TMP_DIR/body.json"
}
post_summary() {  # $1 = ключ дампа, $2 = статус, $3 = summary импортёра
  [ -n "$WORKER_URL" ] || return 0
  # Пейлоад ОДИН с облаком: ops/import_result_body.jq. Своя сборка здесь
  # означала бы молча разъехавшиеся счётчики — этим проект уже болел дважды.
  jq -c --arg dk "$1" --arg st "$2" --arg ru "" --arg src mac \
     -f "$REPO/ops/import_result_body.jq" "$3" > "$TMP_DIR/body.json" \
    && post_body "$TMP_DIR/body.json"
}
post_case_summary() {  # $1 = ключ задания, $2 = статус, $3 = summary скрипта
  [ -n "$WORKER_URL" ] || return 0
  # Свой общий файл — у канала пачек другой ключ и другие счётчики.
  jq -c --arg jk "$1" --arg st "$2" --arg ru "" --arg src mac \
     -f "$REPO/ops/add_case_result_body.jq" "$3" > "$TMP_DIR/body.json" \
    && post_body "$TMP_DIR/body.json"
}

# ── Коммит и пуш результата одной обработанной записи ────────────────────────
# Общее тело обоих каналов: список файлов и ретраи push'а обязаны быть ОДНИ,
# иначе разъедутся молча (этим проект уже болел — см. ops/stage_data_files.sh).
commit_data() {  # $1 = сообщение коммита
  # Список файлов ОДИН с облаком: пути спрашиваются у court_monitor.config.
  bash ops/stage_data_files.sh >>"$LOG" 2>&1 || return 1
  if git diff --cached --quiet; then
    log "  изменений в данных нет — коммит не нужен"
    return 0
  fi
  git -c user.name="Court Monitor (Mac)" -c user.email="bot@court-monitor.local" \
      commit -m "$1" >>"$LOG" 2>&1 || return 1
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
commit_and_push() {  # $1 = имя суда, $2 = added, $3 = added_bank
  local suffix=""
  [ "$3" != "0" ] && suffix=" · 🏦 +$3 в трек"
  commit_data "📥 Импорт (Mac): $1 +$2$suffix"
}
commit_and_push_case() {  # $1 = сколько дел добавлено пачкой
  commit_data "📌 Точечное добавление (Mac): +$1"
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

# ── Одна точечная пачка: job-JSON → cases.json → отчёт ───────────────────────
# Зеркало run_import(), но канал другой: строки пачки (номера и ссылки на
# карточки) уже лежат в KV, скрипт ходит к судам сам.
#
# ⚠️ Повторяем пачку ЦЕЛИКОМ, а не одни упавшие строки. Во-первых, уже
# добавленное отсеет дедуп (стоит до карточки — лишнего HTTP нет). Во-вторых,
# отчёт ПЕРЕЗАПИСЫВАЕТ счётчики записи журнала: повтор двух строк из двадцати
# оставил бы оператору сводку «+2 добавлено» и спрятал исходные 18.
run_add_cases() {  # $1 = файл задания, $2 = ключ|""
  local job="$1" key="$2"
  local summary="$TMP_DIR/case_summary.json" rc=0 added lost items status
  local dry=""
  rm -f "$summary"
  [ "$DRY_RUN" = "1" ] && dry="--dry-run"
  IMPORT_SUMMARY_PATH="$summary" "$PYTHON" scripts/add_cases_targeted.py \
    --job "$job" $dry >>"$LOG" 2>&1 || rc=$?
  if [ ! -s "$summary" ]; then
    log "  ERROR: скрипт упал до разбора задания (код $rc)"
    [ -n "$key" ] && post_error "$key" \
      "резерв на Mac: скрипт упал до разбора задания (код $rc)"
    return 1
  fi
  added=$(jq -r '(.added_main // 0) + (.added_bank // 0) + (.reactivated // 0) + (.promoted // 0)' "$summary")
  lost=$(jq -r '.fetch_error // 0' "$summary")
  items=$(jq -r '.items // 0' "$summary")
  log "  итог: +$added из $items строк · $lost без карточки (код $rc)"

  status=done
  if [ "$rc" -ne 0 ]; then
    status=failed
  elif [ "$DRY_RUN" = "1" ]; then
    log "  DRY-RUN: коммит и отчёт пропущены"
    return 0
  elif ! commit_and_push_case "$added"; then
    # Зеркало облака: «done» только когда И скрипт отработал, И данные уехали.
    status=failed
    jq '.error = "резерв на Mac: пачка обработана, но коммит не запушился — повторите пачку, уже добавленное отсеет дедуп"' \
      "$summary" > "$summary.tmp" && mv "$summary.tmp" "$summary"
    log "  ERROR: коммит/push не удался"
  fi
  [ -n "$key" ] && post_case_summary "$key" "$status" "$summary"
  [ "$status" = "done" ]
}

# ── Режим 1: локальный файл (Worker не участвует) ────────────────────────────
if [ -n "$FILE_ARG" ]; then
  [ -f "$FILE_ARG" ] || die "нет файла дампа: $FILE_ARG"
  [ -n "$COURT_ARG" ] || die "с --file обязателен --court <домен суда>"
  courts_gate manual
  log "Локальный дамп: $FILE_ARG · суд $COURT_ARG"
  if run_import "$FILE_ARG" "$COURT_ARG" "${USER:-оператор} (Mac)" ""; then
    notify "Дамп импортирован ($COURT_ARG)"
    log "Готово"
    exit 0
  fi
  die "импорт локального дампа не удался (см. лог)"
fi

# ── Режим 2: очередь Worker'а ────────────────────────────────────────────────
if [ -z "$WORKER_URL" ] || [ -z "$OWNER_SECRET" ]; then
  log "Нет $WORKER_CONF (url/owner_secret) — очередь не забрать, выход"
  exit 0
fi
# Каким секретом ходим (push_secret или владельческий) — решает Worker, а не
# содержимое файла: чужой токен в push_secret иначе ронял бы весь импорт.
resolve_worker_auth \
  || die "Worker не принял ни один секрет (401) — проверьте owner_secret в $WORKER_CONF"
log "Канал импорта: авторизуемся ключом $AUTH_KIND"

journal_cfg
if ! curl -f -s --compressed -m 30 -A "$UA" -K "$CURL_CFG" -o "$TMP_DIR/log.json"; then
  die "журнал импортов не читается ($WORKER_URL/admin/import-log)"
fi

# Кого берём — правила в ops/mac-local-run/import_queue.jq (отдельным файлом,
# чтобы их проверял тест, а не только глаза).
NOW=$(date +%s)
jq -r --argjson now "$NOW" --argjson ttl "$DUMP_TTL" --argjson grace "$STARTED_GRACE" \
   --argjson cgrace "$CASE_STARTED_GRACE" \
   -f "$REPO/ops/mac-local-run/import_queue.jq" "$TMP_DIR/log.json" \
   > "$TMP_DIR/queue.tsv" \
  || die "не разобрал журнал импортов (jq)"

QUEUE=$(wc -l < "$TMP_DIR/queue.tsv" | tr -d ' ')
if [ "$QUEUE" = "0" ]; then
  log "Очередь пуста — необработанных записей за сутки нет (к судам не ходили)"
  exit 0
fi
log "К обработке записей: $QUEUE"
# Работа точно есть — теперь можно идти к судам (маршруты, канарейка, git).
courts_gate queued "$QUEUE"

rc=0
done_n=0
# Читаем очередь с ОТДЕЛЬНОГО дескриптора: git/ssh внутри цикла иначе могли бы
# съесть остаток файла со stdin, и часть дампов молча не обработалась бы.
while IFS=$'\t' read -r f1 f2 f3 f4 f5 <&3; do
  # ⚠️ Разбор ТЕРПИМ к старому формату очереди намеренно. LaunchAgent гоняет
  # ЭТОТ скрипт (из клона-эталона) по ВСЕМ территориям, а import_queue.jq
  # берётся из клона территории — между деплоем эталона и merge в форк они
  # неизбежно разной версии. Прежняя очередь отдавала 4 поля без канала, и
  # жёсткий разбор на 5 сдвинул бы их все: домен уехал бы в uuid, и работающий
  # канал дампов территории сломался бы молча на весь период раскатки.
  if [ "$f1" = "dump" ] || [ "$f1" = "case" ]; then
    kind="$f1"; uuid="$f2"; domain="$f3"; operator="$f4"; prev="$f5"
  else
    kind="dump"; uuid="$f1"; domain="$f2"; operator="$f3"; prev="$f4"
  fi
  # Прочерк — способ передать пустое значение через TSV (см. dash в
  # import_queue.jq): без него пустой домен пачки или оператор без имени
  # схлопнули бы табы и сдвинули оставшиеся поля.
  [ "$domain" = "-" ] && domain=""
  [ "$operator" = "-" ] && operator=""
  [ -n "$uuid" ] || continue
  # ⚠️ Фигурные скобки обязательны: `«$prev»` bash 3.2 в локали Терминала
  # разбирает как имя «prev»» (первый байт ёлочки — 0xC2 — приклеивается к
  # имени), и при `set -u` прогон падает на первой же записи. В локали C та же
  # строка работает — поэтому дефект пережил и bash -n, и репетицию.
  if [ "$kind" = "case" ]; then
    # Точечная пачка из админки: строки (номера/ссылки) лежат в KV заданием,
    # суд у каждой строки может быть свой — домена у записи нет.
    key="import:case:$uuid"
    log "→ точечная пачка · оператор ${operator:-—} · в журнале «${prev:-?}» · $uuid"
    job="$TMP_DIR/case_job.json"
    post_status "$key" started
    worker_cfg "/add-case-job?key=$key"
    if ! curl -f -s --compressed -m 60 -A "$UA" -K "$CURL_CFG" -o "$job"; then
      log "  ERROR: задание не скачалось — истёк TTL 24 ч или сеть"
      post_error "$key" "резерв на Mac: задание не скачалось из KV (истёк TTL 24 ч?) — отправьте пачку заново"
      rc=1
      continue
    fi
    size=$(wc -c < "$job" | tr -d ' ')
    if [ "$size" -lt 10 ]; then
      log "  ERROR: задание подозрительно мало ($size байт)"
      post_error "$key" "резерв на Mac: задание подозрительно мало ($size байт) — отправьте пачку заново"
      rc=1
      continue
    fi
    log "  задание: $size байт"
    if run_add_cases "$job" "$key"; then
      done_n=$((done_n + 1))
    else
      rc=1
    fi
    continue
  fi

  key="import:dump:$uuid"
  log "→ $domain · оператор ${operator:-—} · в журнале «${prev:-?}» · $uuid"
  dump="$TMP_DIR/dump.html"
  post_status "$key" started
  worker_cfg "/import-dump?key=$key"
  if ! curl -f -s --compressed -m 60 -A "$UA" -K "$CURL_CFG" -o "$dump"; then
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
  alert_telegram "часть записей не обработана ($done_n из $QUEUE) — см. лог резерва"
  notify "Импорт: обработано $done_n из $QUEUE"
else
  notify "Очередь импортов: $done_n из $QUEUE"
fi
exit "$rc"
