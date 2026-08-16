# =============================================================================
# Тело отчёта об импорте дампа для POST /import-result — ОДИН фильтр на все
# каналы запуска импортёра.
#
# ЗАЧЕМ. Фильтр жил инлайном в .github/workflows/import_cases.yml. Когда у
# импорта появился второй канал (резерв на Mac: ops/mac-local-run/
# import_dumps.sh — суды режут адреса облачных раннеров), копия пейлоада
# означала бы ровно ту поломку, которой этот проект уже дважды болел: списки,
# ведённые руками в двух местах, молча разъезжаются (файлы данных резерва,
# домены судов для маршрутов). Счётчик, забытый в одной копии, пропал бы из
# сводки оператора без единого сообщения.
#
# ⚠️ Что не перечислено ЗДЕСЬ — до оператора не доедет. Второе звено —
# числовой whitelist handleImportResult в cloudflare-worker/worker.js, третье —
# impResultText в cloudflare-worker/admin_page.js. Стерегут сквозную проводку
# test_bank_counters_reach_operator / test_card_counters_reach_operator /
# test_card_fail_reason_reaches_operator (scripts/tests/test_import_search_dump.py).
#
# Вход  — summary-JSON импортёра (env IMPORT_SUMMARY_PATH).
# Аргументы — --arg dk <ключ дампа> --arg st <started|done|failed>
#             --arg ru <URL прогона; пусто у Mac — Worker берёт только https://>
#
# Пример:
#   jq -c --arg dk "$DUMP_KEY" --arg st "$STATUS" --arg ru "$RUN_URL" \
#      -f ops/import_result_body.jq "$IMPORT_SUMMARY_PATH"
# =============================================================================
{
  dump_key: $dk,
  status: $st,
  run_url: $ru,

  # Основная картотека (дела ПРОТИВ банка)
  added: (.added // 0),
  promoted: (.promoted // 0),
  already: (.already // 0),
  skipped_role: (.skipped_role // 0),
  not_accepted: (.not_accepted // 0),
  no_link: (.no_link // 0),
  subsidiary: (.subsidiary // 0),

  # Карточки основной картотеки (16.08.2026): суд может отдать блок-страницу
  # вместо карточки, и дело заводится пустышкой — сводка обязана это сказать.
  card_failed: (.card_failed // 0),
  refilled: (.refilled // 0),

  # Трек «Иски банка» (истцовые строки дампа, с 13.08.2026)
  added_bank: (.added_bank // 0),
  excluded_result: (.excluded_result // 0),
  excluded_writ: (.excluded_writ // 0),
  already_spent: (.already_spent // 0),
  seen_cached: (.seen_cached // 0),
  bank_capped: (.bank_capped // 0),
  fetch_fail: (.fetch_fail // 0),

  rows: (.rows // 0),

  # Причина отказа карточек — СТРОКА, а не счётчик (числовой whitelist Worker'а
  # её срезал бы). Шлём ВСЕГДА, в том числе пустой: повтор того же импорта
  # после снятия блока обязан очистить прежнее предупреждение в журнале.
  card_fail_reason: (.card_fail_reason // ""),

  lines: (.lines // [])
}
+ (if .error then {error: .error} else {} end)
