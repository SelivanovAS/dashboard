# -*- coding: utf-8 -*-
"""Диспетчер дайджеста (generate_digest: программный шаблон и пересказы
актов, полировщик за DIGEST_POLISH=1), снимки контекста
(save_digest_context → --replay-last) и последнего дайджеста
(save_last_digest → фронт), привязка разборов актов к делам
(attach_act_analyses).
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime


from court_monitor import config
from court_monitor.config import log
from court_monitor.digest import llm
from court_monitor.digest.postprocess import _DIGEST_HEADER_RE
from court_monitor.digest.template import (
    generate_template_digest, _strip_echoed_terminal_events,
    _merge_motiv_into_resolved,
)
from court_monitor.storage import load_json, save_json
from court_monitor.textutil import _bare_case_number

# ── Контекст и генерация дайджеста ─────────────────────────────────────────

# Дельта-списки контекста: копятся при накоплении дня. «cases» сюда НЕ входит
# осознанно — это СНИМОК картотеки для рендера (lookups), а не дельта: при
# merge берётся свежий.
_CTX_DELTA_KEYS = (
    "new_cases", "changes", "fi_new_cases", "stage_transitions",
    "fi_changes", "cass_changes", "cass_discovered",
)


def _ack_parse_context_wal(issue_key: str) -> None:
    """Durably acknowledge the Mac parse snapshot after context is safe."""
    txn_id = config.PARSE_TXN_ID
    ack_file = config.PARSE_TXN_ACK_FILE
    if not txn_id and not ack_file:
        return
    if not txn_id or not ack_file:
        raise RuntimeError("parse transaction ACK configured incompletely")
    try:
        save_json(
            {
                "version": 1,
                "txn_id": txn_id,
                "issue_key": issue_key,
                "saved_at": datetime.now().isoformat(timespec="seconds"),
            },
            ack_file,
        )
    except Exception as exc:
        raise RuntimeError(
            "не удалось подтвердить WAL контекста дайджеста"
        ) from exc
    log.info(f"Контекст дайджеста подтверждён parse-txn: {txn_id}")


def _load_prev_context() -> dict | None:
    if not os.path.exists(config.LAST_DIGEST_CONTEXT_PATH):
        return None
    try:
        with open(config.LAST_DIGEST_CONTEXT_PATH, "r", encoding="utf-8") as f:
            prev = json.load(f)
    except (OSError, ValueError):
        return None
    return prev if isinstance(prev, dict) else None


def _merge_day_context(prev: dict, payload: dict) -> dict:
    """Накопление дня + свежая дельта. Дельты попыток дизъюнктны по построению
    (события уже влиты в данные, флаги «объявлено» поставлены — повторный парс
    той же карточки изменений не даёт); дедуп по json-идентичности — пояс."""
    merged = dict(payload)  # свежие totals и снимок cases
    for key in _CTX_DELTA_KEYS:
        seen = {
            json.dumps(x, ensure_ascii=False, sort_keys=True)
            for x in (prev.get(key) or [])
        }
        combined = list(prev.get(key) or [])
        for x in (payload.get(key) or []):
            sig = json.dumps(x, ensure_ascii=False, sort_keys=True)
            if sig not in seen:
                combined.append(x)
                seen.add(sig)
        merged[key] = combined
    return merged


def save_digest_context(
    new_cases: list[dict],
    changes: list[dict],
    *,
    cases: list[dict] | None = None,
    fi_new_cases: list[dict] | None = None,
    stage_transitions: list[dict] | None = None,
    fi_changes: list[dict] | None = None,
    total_active_appeal: int = 0,
    total_active_fi: int = 0,
    total_active_cassation: int = 0,
    total_active_bank: int = 0,
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
    will_deliver: bool = False,
    defer_data: dict | None = None,
) -> str:
    """Сохранить входные данные дайджеста в LAST_DIGEST_CONTEXT_PATH.

    Нужен для --replay-last (переиграть дайджест без повторного парсинга) и —
    с 20.08.2026 — как ДНЕВНОЙ НАКОПИТЕЛЬ (решение юриста «один дайджест в
    день»): неотправленный контекст того же дня не перезаписывается, а
    ПОПОЛНЯЕТСЯ дельтой попытки. Отправку решает Mac-обёртка выбором сообщения
    коммита (replay_on_push стреляет только по маркеру «(Mac-парсинг)»), а
    факт отправки фиксирует `delivered_at`: его ставит либо `will_deliver=True`
    (облачный прогон — доставляет сам, у него есть TELEGRAM_BOT_TOKEN), либо
    `cloud_run_ok.py --mark-delivered` перед доставочным коммитом на Mac.
    После delivered_at контекст дня закрыт — следующий прогон начинает свежий
    (ручной дневной прогон даст отдельный выпуск, не переотправит утро).

    `issue_key` — стабильный ключ ВЫПУСКА для save_last_digest: живёт с первой
    попытки накопления, пере-рендер того же контекста замещает свой выпуск на
    дашборде, а не дописывает второй.

    ⚠️ Пустая дельта НЕ трогает накопление (и файл байт-в-байт): холостой
    перезапис бампал бы saved_at и плодил коммиты каждые полчаса.
    """
    saved_at = datetime.now().isoformat(timespec="seconds")
    payload = {
        "saved_at": saved_at,
        "new_cases": new_cases or [],
        "changes": changes or [],
        "cases": cases or [],
        "fi_new_cases": fi_new_cases or [],
        "stage_transitions": stage_transitions or [],
        "fi_changes": fi_changes or [],
        "total_active_appeal": total_active_appeal,
        "total_active_fi": total_active_fi,
        "total_active_cassation": total_active_cassation,
        "total_active_bank": total_active_bank,
        "cass_changes": cass_changes or [],
        "cass_discovered": cass_discovered or [],
    }
    if defer_data is not None:
        # Дневная дочитка не переоткрывает выпуск и не меняет delivered_at.
        # Очередь живёт в уже публикуемом cases.json; ACK выдаём после записи.
        pending = _merge_day_context(defer_data.get('pending_retry_context') or {}, payload)
        pending['issue_key'] = 'retry:' + saved_at
        disk = load_json(config.JSON_PATH)
        disk['pending_retry_context'] = pending
        save_json(disk, config.JSON_PATH)
        defer_data['pending_retry_context'] = pending
        _ack_parse_context_wal(pending['issue_key'])
        return pending['issue_key']
    issue_key = saved_at
    today = {
        datetime.now().date().isoformat(),
        datetime.utcnow().date().isoformat(),
    }
    prev = _load_prev_context()
    if (
        prev
        and str(prev.get("saved_at", ""))[:10] in today
        and not prev.get("delivered_at")
    ):
        if not any(payload[k] for k in _CTX_DELTA_KEYS):
            log.info("Контекст дайджеста: дельта пуста — накопление дня не тронуто")
            issue_key = str(
                prev.get("issue_key") or prev.get("saved_at") or saved_at
            )
            _ack_parse_context_wal(issue_key)
            return issue_key
        payload = _merge_day_context(prev, payload)
        payload["saved_at"] = saved_at
        issue_key = str(prev.get("issue_key") or prev.get("saved_at") or saved_at)
        merged_n = sum(len(payload[k]) for k in _CTX_DELTA_KEYS)
        log.info(f"Контекст дайджеста: дельта влита в накопление дня ({merged_n} записей)")
    payload["issue_key"] = issue_key
    if will_deliver:
        payload["delivered_at"] = saved_at
    try:
        save_json(payload, config.LAST_DIGEST_CONTEXT_PATH)
        log.info(f"Контекст дайджеста сохранён: {config.LAST_DIGEST_CONTEXT_PATH}")
    except Exception as exc:
        # Для облачного legacy-пути контекст остаётся best-effort. Для Mac
        # это write-ahead условие доставки: если свежая дельта не записалась,
        # старый сегодняшний файл прошёл бы freshness-гейт и replay отправил
        # неполный выпуск, а события уже были бы влиты в cases навсегда.
        log.warning(f"Не удалось сохранить контекст дайджеста: {exc}")
        if config.DIGEST_CONTEXT_REQUIRED:
            raise RuntimeError(
                "обязательный контекст дайджеста не сохранён"
            ) from exc
    else:
        _ack_parse_context_wal(issue_key)
    return issue_key


def acknowledge_retry_context(data: dict, issue_key: str, expected: dict) -> None:
    """Убрать дневную дельту лишь после доказанного включения в новый выпуск."""
    pending = data.get('pending_retry_context')
    if not pending:
        return
    confirmed = _load_prev_context() or {}
    signature = lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False)
    if confirmed.get('issue_key') != issue_key or any(
        not {signature(x) for x in expected.get(key, [])}.issubset(
            {signature(x) for x in confirmed.get(key, [])})
        for key in _CTX_DELTA_KEYS
    ):
        raise RuntimeError('дневная дочитка не подтверждена контекстом следующего выпуска')
    data.pop('pending_retry_context')
    try:
        save_json(data, config.JSON_PATH)
    except Exception:
        data['pending_retry_context'] = pending
        raise


def _load_prev_issues() -> list[dict]:
    """Выпуски из существующего last_digest.json (легаси-файл без issues —
    один выпуск с ключом legacy:*). Ошибка чтения = пустой список: накопитель
    не должен ронять сохранение дайджеста."""
    if not os.path.exists(config.LAST_DIGEST_PATH):
        return []
    try:
        with open(config.LAST_DIGEST_PATH, "r", encoding="utf-8") as f:
            prev = json.load(f)
    except (OSError, ValueError):
        return []
    if not isinstance(prev, dict):
        return []
    issues = prev.get("issues")
    if isinstance(issues, list):
        return [i for i in issues if isinstance(i, dict) and i.get("html")]
    if prev.get("html"):
        return [{
            "key": "legacy:" + str(prev.get("generated_at") or ""),
            "at": str(prev.get("generated_at") or ""),
            "summary": prev.get("summary") or "",
            "is_empty": bool(prev.get("is_empty")),
            "html": prev.get("html"),
        }]
    return []


def save_last_digest(
    html: str, summary: str = "", *, is_empty: bool = False, issue_key: str = ""
) -> None:
    """Сохранить готовый HTML дайджеста в LAST_DIGEST_PATH.

    Фронт читает этот файл, чтобы показать блок «Последний дайджест»
    в дашборде. Вызывается после успешной отправки в Telegram.

    **Дневной накопитель (20.08.2026, решение юриста).** Выпуски одного дня
    складываются, а не затирают друг друга: с гейтом-дочиткой утро может дать
    два прогона (частичный + дочитку), и второй выпуск стирал первый с
    дашборда — утренние новости оставались только в Telegram. `issue_key` —
    `saved_at` контекста дайджеста: пере-рендер ТОГО ЖЕ контекста
    (Mac-черновик → полированный replay через минуту; повторный replay)
    замещает свой выпуск на месте, НОВЫЙ контекст того же дня дописывается,
    другой день начинает файл заново. Верхнеуровневый `html` — склейка
    выпусков хронологически (перед 2-м и далее — строка «➕ Дополнение»),
    фронт и mine-фильтр читают его как раньше. Telegram/push этим файлом не
    пользуются — туда уходит только дельта прогона, дублей нет.

    ⚠️ День сравнивается с допуском UTC/локаль (приём cloud_run_ok): файл
    пишут два автора — раннер GitHub (UTC) и Mac (+05), оба naive-ISO.

    `is_empty=True` — дайджест-заглушка (изменений не было). Используется
    `load_last_meaningful_digest()`, чтобы не цитировать «пустой» дайджест
    в качестве «предыдущего» в следующий тихий день. У файла с выпусками
    is_empty = «все выпуски пустые».
    """
    if not html:
        return
    now = datetime.now().isoformat(timespec="seconds")
    today = {
        datetime.now().date().isoformat(),
        datetime.utcnow().date().isoformat(),
    }
    issue = {
        "key": issue_key or ("solo:" + now),
        "at": now,
        "summary": summary or "",
        "is_empty": bool(is_empty),
        "html": html,
    }
    issues = [i for i in _load_prev_issues() if str(i.get("at", ""))[:10] in today]
    for idx, it in enumerate(issues):
        if it.get("key") == issue["key"]:
            issues[idx] = issue  # пере-рендер того же контекста — на месте
            break
    else:
        issues.append(issue)
    parts: list[str] = []
    for idx, it in enumerate(issues):
        if idx:
            hhmm = str(it.get("at", ""))[11:16]
            parts.append(f"\n\n➕ <b>Дополнение ({hhmm})</b>\n\n")
        parts.append(it["html"])
    payload = {
        "version": 1,
        "generated_at": issues[-1]["at"],
        "summary": issues[-1]["summary"],
        "html": "".join(parts),
        "is_empty": all(i.get("is_empty") for i in issues),
        "issues": issues,
    }
    try:
        save_json(payload, config.LAST_DIGEST_PATH)
        if len(issues) > 1:
            log.info(
                f"Дайджест сохранён для фронта: {config.LAST_DIGEST_PATH} "
                f"(выпусков за день: {len(issues)})"
            )
        else:
            log.info(f"Дайджест сохранён для фронта: {config.LAST_DIGEST_PATH}")
    except Exception as exc:
        log.warning(f"Не удалось сохранить дайджест для фронта: {exc}")


# ── Привязка LLM-разбора опубликованного акта к конкретному делу ──────
# Дайджест Claude уже содержит осмысленный анализ каждого опубликованного
# акта (мотивировка, итог, роль банка), но текст монолитный и живёт ровно
# до следующего дайджеста. Чтобы юрист видел разбор прямо в drawer
# карточки дела (и чтобы он не пропадал на следующий день), вырезаем
# относящиеся к делу абзацы из готового HTML и кладём в cases.json под
# `<stage>.act_analysis`. Парсер опирается на тот же контракт
# `<a><b>НОМЕР</b></a>`, который сейчас использует фронт в mine-режиме.

def _extract_case_paragraphs_from_digest(html: str, case_id: str, *,
                                         require_explained: bool = False) -> str:
    """Из HTML дайджеста вернуть «разборный» абзац — тот, в котором есть
    маркер `<b>Почему:</b>` и первый `<a><b>НОМЕР</b></a>` соответствует
    `case_id`. Маркер «Почему:» уникален для раздела «Опубликованные
    тексты актов» (5.5) — он отличает мотивировочный разбор от
    одностроковых упоминаний дела в других разделах дайджеста
    («Вынесенные акты» 5.4, «Новые дела», «Заседания»), которые иначе
    склеиваются в одно поле `act_analysis.html`. Если разборных абзацев
    нет (старый шаблонный дайджест без LLM-мотивировки) — возвращаем
    все найденные абзацы как раньше, чтобы не сломать исторический
    fallback. Пустую строку — если ничего не нашлось.

    `require_explained=True` — без «Почему»-абзаца вернуть пустоту (не
    фолбэчиться на любые абзацы с номером): банк-секция печатает дело
    ОДНОЙ строкой с номером, и фолбэк выдал бы её за «AI анализ»."""
    if not html or not case_id:
        return ""
    target = _bare_case_number(case_id)
    if not target:
        return ""
    case_re = re.compile(r"<a[^>]*><b>([^<]+)</b></a>")
    out: list[str] = []
    for para in re.split(r"\n{2,}", html):
        m = case_re.search(para)
        if not m:
            continue
        if _bare_case_number(m.group(1)) == target:
            # У первого дела секции в абзац прилипает строка-заголовок
            # («📄 <b>Опубликованные тексты актов (N):</b>» идёт без пустой
            # строки перед делом) — в drawer'е карточки это шум, срезаем.
            lines = [
                ln for ln in para.split("\n")
                if not _DIGEST_HEADER_RE.match(ln)
            ]
            stripped = "\n".join(lines).strip()
            if stripped:
                out.append(stripped)
    if not out:
        return ""
    explained = [p for p in out if "<b>Почему:</b>" in p]
    if require_explained:
        return "\n\n".join(explained)
    return "\n\n".join(explained or out)


def attach_act_analyses(
    cases: list[dict],
    digest_html: str,
    *,
    all_changes: list[dict] | None = None,
    cass_changes: list[dict] | None = None,
    is_empty: bool = False,
    require_explained: bool = False,
) -> int:
    """Записать LLM-разбор опубликованного акта в `cases.json`.

    Триггеры по типу change'а:
    - `new_act` в `all_changes` → `appeal.act_analysis` (апел. акт);
    - `fi_act_text_published` в `all_changes` → `first_instance.act_analysis`
      (мотивировка решения 1-й инст.);
    - `new_act` в `cass_changes` → `cassation.act_analysis` (текст касс.
      определения; `cass_changes` лежат отдельным списком, потому что у
      них тот же тип `new_act`, что и у апелляции, и стадию нужно
      назначить по источнику).

    Для каждого триггера сохраняет относящийся к делу абзац «Почему».
    Без готового пересказа поле не пишется: сырой акт не является AI-анализом.
    Событийный путь сохраняет прежние разборы; отложенные результаты независимо
    дописывает summary_queue с проверкой полного источника и фактическим автором.
    require_explained сохранён для совместимости; маркер обязателен всегда.

    Возвращает кол-во дел, у которых поле реально изменилось.
    """
    if is_empty or not digest_html or (not all_changes and not cass_changes):
        return 0

    # Индекс «bare-номер дела → объект case»: матчим как по верхнему
    # `id`, так и по case_number в каждой стадии. `change["case"]` для
    # апелляции = апел. номер, для 1-й инст. = номер 1-й инст., для
    # кассации = обычно номер 1-й инст. (см. cass_changes append'ы) —
    # все три должны находить нужное дело.
    by_id: dict[str, list[dict]] = {}
    for c in cases:
        for raw in (
            c.get("id"),
            (c.get("first_instance") or {}).get("case_number"),
            (c.get("appeal") or {}).get("case_number"),
            (c.get("cassation") or {}).get("case_number"),
        ):
            bare = _bare_case_number(raw or "")
            if bare:
                bucket = by_id.setdefault(bare, [])
                if not any(item is c for item in bucket):
                    bucket.append(c)

    # Собираем (stage, change) — один цикл вместо ветвлений в середине.
    # У апеллированного `new_act` и кассационного `new_act` тип совпадает,
    # поэтому списки разные.
    queued: list[tuple[str, dict]] = []
    for ch in all_changes or []:
        types = set(ch.get("type") or [])
        if "new_act" in types:
            queued.append(("appeal", ch))
        elif "fi_act_text_published" in types:
            queued.append(("first_instance", ch))
    for ch in cass_changes or []:
        types = set(ch.get("type") or [])
        if "new_act" in types:
            queued.append(("cassation", ch))

    model_name = llm._current_digest_model_name()
    now_iso = datetime.now().isoformat(timespec="seconds")
    updated = 0

    for stage, ch in queued:
        case_num = ch.get("case", "")
        bare = _bare_case_number(case_num)
        if not bare:
            continue
        details = ch.get("details") or {}
        candidates = by_id.get(bare, [])
        domain = details.get('court_domain') or ch.get('court_domain')
        if domain:
            candidates = [c for c in candidates if (c.get(stage) or {}).get('court_domain') == domain]
        case = candidates[0] if len(candidates) == 1 else None
        if not case:
            # DEBUG, а не INFO: штатная ситуация каждого прогона — общий
            # вызов идёт по основной картотеке, а очередь несёт и банк-дела
            # (их split_bank_track вынул фазой раньше), их подберёт вызов по
            # треку. Текст без «в cases.json»: функция не знает, какой список
            # ей передали, и во втором вызове ищут по cases_bank.json.
            log.debug(
                f"act_analysis: дело {case_num} ({stage}) вне переданной "
                "картотеки — пропуск"
            )
            continue

        details = ch.get("details") or {}
        act_date = details.get("act_date") or ""

        html_fragment = _extract_case_paragraphs_from_digest(
            digest_html, bare, require_explained=True
        )
        if not html_fragment and stage == "cassation":
            # Шаблонный рендер кассации оборачивает КАССАЦИОННЫЙ номер
            # (8Г-…), а не номер 1-й инст. из change["case"] — пробуем его.
            alt = _bare_case_number(ch.get("cassation_internal_number") or "")
            if alt:
                html_fragment = _extract_case_paragraphs_from_digest(
                    digest_html, alt, require_explained=True
                )
        if not html_fragment:
            # Сбой/отказ остаётся в очереди. Исходник не является AI-анализом.
            continue
        source = "digest"

        stage_obj = case.setdefault(stage, {})
        existing = stage_obj.get("act_analysis") or {}
        if (
            existing.get("html") == html_fragment
            and existing.get("source") == source
            and existing.get("act_date") == act_date
            and existing.get("model") == model_name
        ):
            # Идемпотентность: содержимое не поменялось — не трогаем
            # generated_at, иначе git diff пухнет на каждом replay.
            continue

        stage_obj["act_analysis"] = {
            "html": html_fragment,
            "source": source,
            "act_date": act_date,
            "generated_at": now_iso,
            "model": model_name,
        }
        updated += 1

    if updated:
        log.info(f"act_analysis: записан/обновлён для {updated} дел.")
    return updated


def _dedupe_existing_act_analyses(cases: list[dict]) -> int:
    """Идемпотентная чистка ранее сохранённых `act_analysis.html` от
    «склейки» абзацев. До правки `_extract_case_paragraphs_from_digest`
    функция могла отдать сразу несколько абзацев дайджеста с одним
    номером дела (например, одностроковое упоминание из «Вынесенных
    актов» + полноценный мотивировочный разбор из «Опубликованных
    текстов»). Дальше эти абзацы навсегда залипали в `cases.json`,
    потому что change[new_act] для уже опубликованного акта больше не
    приходит, и `attach_act_analyses` не пересчитывает поле.

    Здесь проходим по всем стадиям всех дел и применяем тот же приоритет
    «разборного» абзаца: если в html есть несколько абзацев и хотя бы
    один содержит маркер `<b>Почему:</b>` — оставляем только такие.
    Не трогаем `source="raw_act"` (там html собран вручную через `<p>` и
    делить его на абзацы по `\\n{2,}` неправильно). После прогона на
    почищенных данных функция отрабатывает no-op."""
    updated = 0
    for c in cases:
        for stage_key in ("first_instance", "appeal", "cassation"):
            stage = c.get(stage_key) or {}
            aa = stage.get("act_analysis") or {}
            if not aa or aa.get("source") != "digest":
                continue
            html = aa.get("html") or ""
            if not html:
                continue
            parts = [p.strip() for p in re.split(r"\n{2,}", html) if p.strip()]
            if len(parts) <= 1:
                continue
            explained = [p for p in parts if "<b>Почему:</b>" in p]
            if not explained or len(explained) == len(parts):
                continue
            aa["html"] = "\n\n".join(explained)
            updated += 1
    if updated:
        log.info(
            f"act_analysis: дедуп старых склеек применён к {updated} делам."
        )
    return updated


def generate_digest(new_cases: list[dict], changes: list[dict], *,
                    cases: list[dict] | None = None,
                    fi_new_cases: list[dict] | None = None,
                    stage_transitions: list[dict] | None = None,
                    fi_changes: list[dict] | None = None,
                    total_active_appeal: int = 0,
                    total_active_fi: int = 0,
                    total_active_cassation: int = 0,
                    total_active_bank: int = 0,
                    cass_changes: list[dict] | None = None,
                    cass_discovered: list[dict] | None = None) -> str:
    """Собрать дайджест по шаблону с пересказами актов и необязательной полировкой.

    total_active_appeal/total_active_fi/total_active_cassation передаются раздельно:
    шаблон выводит фактическое число дел каждой инстанции,
    а LLM используется для пересказов актов и включённой полировки HTML.
    """

    if cases is None:
        cases = []
    if fi_new_cases is None:
        fi_new_cases = []
    if stage_transitions is None:
        stage_transitions = []
    if fi_changes is None:
        fi_changes = []
    if cass_changes is None:
        cass_changes = []
    if cass_discovered is None:
        cass_discovered = []

    # Чистим спайк-кейсы status_change «Решено → В производстве» из
    # уже сформированного контекста (например, при --replay-last, когда
    # парсер заново не вызывается и его spike-фильтр не сработает).
    # См. парный гард в парсере апелляции.
    cleaned_changes: list[dict] = []
    for ch in changes:
        types = ch.get("type") or []
        d = ch.get("details") or {}
        is_spike = (
            "status_change" in types
            and d.get("old_status") == "Решено"
            and d.get("new_status") == "В производстве"
        )
        if not is_spike:
            cleaned_changes.append(ch)
            continue
        remaining = [t for t in types if t != "status_change"]
        if remaining:
            cleaned_changes.append({**ch, "type": remaining})
        # Если status_change был единственным типом — change целиком уходит.
    changes = cleaned_changes

    # Гасим сырое событие карточки, пересказывающее уже показанный исход
    # (инцидент 9-336/2026: возврат иска пришёл и строкой события
    # в 3.2, и как «Итог: возвращено» в 3.5). Гибридный путь применит фильтр
    # ещё раз внутри generate_template_digest — он идемпотентен.
    fi_changes = _strip_echoed_terminal_events(fi_changes)
    # Склейка «решение + мотивировка» одного дела (кейс Урала 2-484/2026) —
    # банк-трек функция пропускает сама, повторное
    # применение в generate_template_digest идемпотентно.
    fi_changes = _merge_motiv_into_resolved(fi_changes)

    log.info(
        "LLM: гибрид (программный рендер + микро-LLM на пересказы актов"
        + (", + полировщик HTML" if config.DIGEST_POLISH else "")
        + ")"
    )
    from court_monitor.digest.summary_queue import summarize_tracked
    draft = generate_template_digest(
        new_cases, changes, cases=cases,
        fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
        fi_changes=fi_changes,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
        total_active_cassation=total_active_cassation,
        total_active_bank=total_active_bank,
        cass_changes=cass_changes,
        cass_discovered=cass_discovered,
        act_summarizer=summarize_tracked,
    )
    if config.DIGEST_POLISH:
        expected_nums = llm._collect_case_numbers(
            new_cases=new_cases, changes=changes,
            fi_new_cases=fi_new_cases, fi_changes=fi_changes,
            cass_changes=cass_changes, cass_discovered=cass_discovered,
        )
        return llm.polish_digest_html(
            draft, expected_case_numbers=expected_nums
        )
    return draft
