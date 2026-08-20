# -*- coding: utf-8 -*-
"""Диспетчер дайджеста (generate_digest: гибрид по умолчанию, full-LLM за
флагом DIGEST_FULL_LLM=1, полировщик за DIGEST_POLISH=1), снимки контекста
(save_digest_context → --replay-last) и последнего дайджеста
(save_last_digest → фронт), привязка разборов актов к делам
(attach_act_analyses).

⚠ Полный LLM-промпт внутри generate_digest юрист настраивал долго —
не менять ни на символ.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from html import escape as html_escape

import requests

from court_monitor import config
from court_monitor.config import log
from court_monitor.courts import CASSATION_COURT, case_card_url, fi_card_url
from court_monitor.regions import get_region
from court_monitor.digest import llm
from court_monitor.digest.postprocess import (
    _ensure_appeal_new_case_full_layout, _validate_digest_new_sections,
    _ensure_footer, _normalize_section_spacing, _drop_zero_count_sections,
    _replace_summary_block, _renumber_section_headers,
    _warn_misplaced_appeal_cases, _shorten_categories_in_html,
    _strip_section_numbering, _purge_3_6_without_act_text,
    _close_open_tags, _strip_orphan_close_tags,
    _wrap_all_bare_case_numbers, _DIGEST_HEADER_RE,
)
from court_monitor.digest.template import (
    generate_template_digest, render_no_changes_digest, build_summary_line,
    short_category_chain, category_short,
    _strip_echoed_terminal_events, _merge_motiv_into_resolved,
    _FI_TERMINATION_LABELS,
)
from court_monitor.lifecycle import _fi_return_reason_for_render
from court_monitor.parsing import (
    CASSATION_OUTCOME_RU, cassation_review_label, cassation_terminated_label,
)
from court_monitor.storage import load_json, save_json
from court_monitor.textutil import (
    escape_html, shorten_party_name, shorten_court_name, _bare_case_number,
    case_id_uid, classify_appellant_role,
)

# ── Claude API — генерация дайджеста ─────────────────────────────────────────

# Дельта-списки контекста: копятся при накоплении дня. «cases» сюда НЕ входит
# осознанно — это СНИМОК картотеки для рендера (lookups), а не дельта: при
# merge берётся свежий.
_CTX_DELTA_KEYS = (
    "new_cases", "changes", "fi_new_cases", "stage_transitions",
    "fi_changes", "cass_changes", "cass_discovered",
)


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
            return str(prev.get("issue_key") or prev.get("saved_at") or saved_at)
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
        # Сохранение контекста — вспомогательная операция, не должна ронять
        # основной прогон. Ошибку залогируем и поедем дальше.
        log.warning(f"Не удалось сохранить контекст дайджеста: {exc}")
    return issue_key


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

    Для каждого триггера вырезает из `digest_html` относящийся к делу
    абзац с маркером `<b>Почему:</b>` (мотивировочный разбор LLM) и
    кладёт в `case[<stage>]["act_analysis"] = {html, source, act_date,
    generated_at, model}`. Если разборного абзаца в дайджесте нет
    (шаблонный fallback или нет мотивировки) — fallback на HTML-обёрнутую
    `change["details"]["act_text"]` с пометкой `source: "raw_act"`. Если
    и `act_text` пуст — поле просто не пишем.

    Поле перезаписывается ТОЛЬКО для дел с новым событием в этом прогоне;
    у остальных дел `act_analysis` сохраняется с прошлых прогонов и
    переживает любое количество последующих дайджестов. Идемпотентно:
    при повторном прогоне на тех же данных `generated_at` не обновляется.

    `require_explained=True` — абзац из дайджеста берётся только с маркером
    «Почему:», иначе сразу raw_act-фолбэк (банк-вызов в runs.py: дело там
    печатается одной строкой с номером, и обычный фолбэк выдал бы её за
    «AI анализ»).

    Возвращает кол-во дел, у которых поле реально изменилось.
    """
    if is_empty or not digest_html or (not all_changes and not cass_changes):
        return 0

    # Индекс «bare-номер дела → объект case»: матчим как по верхнему
    # `id`, так и по case_number в каждой стадии. `change["case"]` для
    # апелляции = апел. номер, для 1-й инст. = номер 1-й инст., для
    # кассации = обычно номер 1-й инст. (см. cass_changes append'ы) —
    # все три должны находить нужное дело.
    by_id: dict[str, dict] = {}
    for c in cases:
        for raw in (
            c.get("id"),
            (c.get("first_instance") or {}).get("case_number"),
            (c.get("appeal") or {}).get("case_number"),
            (c.get("cassation") or {}).get("case_number"),
        ):
            bare = _bare_case_number(raw or "")
            if bare:
                by_id.setdefault(bare, c)

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
        case = by_id.get(bare)
        if not case:
            log.info(
                f"act_analysis: дело {case_num} ({stage}) не нашлось "
                "в cases.json — пропуск"
            )
            continue

        details = ch.get("details") or {}
        act_date = details.get("act_date") or ""

        html_fragment = _extract_case_paragraphs_from_digest(
            digest_html, bare, require_explained=require_explained
        )
        if not html_fragment and stage == "cassation":
            # Шаблонный рендер кассации оборачивает КАССАЦИОННЫЙ номер
            # (8Г-…), а не номер 1-й инст. из change["case"] — пробуем его.
            alt = _bare_case_number(ch.get("cassation_internal_number") or "")
            if alt:
                html_fragment = _extract_case_paragraphs_from_digest(
                    digest_html, alt, require_explained=require_explained
                )
        if html_fragment:
            source = "digest"
        else:
            raw_act = (details.get("act_text") or "").strip()
            if not raw_act:
                continue
            # Сырая мотивировка: оборачиваем в <p>, экранируем угловые
            # скобки, переводы строк превращаем в <br> / новые абзацы.
            escaped = html_escape(raw_act).replace("\r\n", "\n")
            paragraphs = [p.strip() for p in escaped.split("\n\n") if p.strip()]
            html_fragment = "".join(
                "<p>" + p.replace("\n", "<br>") + "</p>" for p in paragraphs
            )
            source = "raw_act"

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
    """Сгенерировать дайджест через Claude API.

    total_active_appeal/total_active_fi/total_active_cassation передаются раздельно —
    раньше передавалась только сумма, и Claude выдумывал разбивку
    (типа «1 инст.: 2» при реальных 9).
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
    # парсер заново не вызывается и его spike-фильтр на ~3599 не сработает).
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

    # Сырое событие карточки, пересказывающее уже показанный исход, гасим для
    # ОБОИХ путей (инцидент 9-336/2026: возврат иска пришёл и строкой события
    # в 3.2, и как «Итог: возвращено» в 3.5). Гибридный путь применит фильтр
    # ещё раз внутри generate_template_digest — он идемпотентен.
    fi_changes = _strip_echoed_terminal_events(fi_changes)
    # Склейка «решение + мотивировка» одного дела (кейс Урала 2-484/2026) —
    # для обоих путей; банк-трек функция пропускает сама, повторное
    # применение в generate_template_digest идемпотентно.
    fi_changes = _merge_motiv_into_resolved(fi_changes)

    total_active = total_active_appeal + total_active_fi + total_active_cassation

    # ── Гибридный путь (по умолчанию) ────────────────────────────────────
    # Программный рендер (generate_template_digest) + LLM-микро-вызов
    # только на пересказ мотивировок (summarize_act_motivation).
    # При DIGEST_POLISH=1 готовый HTML дополнительно проходит через
    # polish_digest_html (косметика + валидатор контракта).
    # Старый полный LLM-вызов остаётся за DIGEST_FULL_LLM=1 для отката.
    if not config.DIGEST_FULL_LLM:
        log.info(
            "LLM: гибрид (программный рендер + микро-LLM на пересказы актов"
            + (", + полировщик HTML" if config.DIGEST_POLISH else "")
            + ")"
        )
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
            act_summarizer=llm.summarize_act_motivation,
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

    # ── Старая ветка: полный LLM-вызов (за флагом DIGEST_FULL_LLM=1) ─────
    if config.LLM_PROVIDER == "gigachat":
        if not config.GIGACHAT_AUTH_KEY:
            log.warning("GIGACHAT_AUTH_KEY не задан, дайджест будет шаблонным")
            return generate_template_digest(
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
                total_active_cassation=total_active_cassation,
                total_active_bank=total_active_bank,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            )
    elif config.LLM_PROVIDER == "openrouter":
        if not config.OPENROUTER_API_KEY:
            log.warning("OPENROUTER_API_KEY не задан, дайджест будет шаблонным")
            return generate_template_digest(
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
                total_active_cassation=total_active_cassation,
                total_active_bank=total_active_bank,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            )
    elif not config.ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY не задан, дайджест будет шаблонным")
        return generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
            total_active_bank=total_active_bank,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )

    today = datetime.now().strftime("%d.%m.%Y")
    summary = build_summary_line(
        new_cases, changes, fi_new_cases, stage_transitions, fi_changes,
        cass_changes=cass_changes, cass_discovered=cass_discovered,
    )

    # ── Короткое сообщение если изменений нет ──
    # stage_transitions намеренно НЕ учитываем: они дублируют 5.1 и в
    # дайджест не выводятся, так что прогон с одними переходами = пустой.
    if (not new_cases and not changes and not fi_new_cases
            and not fi_changes
            and not cass_changes and not cass_discovered):
        return render_no_changes_digest(
            today, f"В производстве: {total_active}"
        )

    # ── Формируем контекст для Claude ──
    # Порядок блоков в данных задаёт порядок больших блоков в дайджесте:
    # сначала 1-я инстанция (новые иски + изменения + решения + тексты),
    # потом апелляция (новые дела + изменения), потом кассация. Юрист
    # просил, чтобы первая инстанция была первой; LLM при равной
    # инструкции в промпте склонна следовать порядку контекста, поэтому
    # держим оба в синхроне (промпт + порядок данных).
    context_parts = [f"СВОДКА: {summary}"]

    def _appellant_fmt(d: dict) -> str:
        """Строка «роль + имя» для промпта. Если новых полей нет —
        откат к старому бинарному ярлыку (легаси-пэйлоад, --force-postpone).
        Если есть _appellant_raw но ролей нет (старый replay-last пэйлоад
        после правки) — переклассифицируем на лету из plaintiff/defendant.
        """
        role = d.get("appellant_role", "")
        name = d.get("appellant_name", "")
        if not role and not name and d.get("_appellant_raw"):
            role, name = classify_appellant_role(
                d["_appellant_raw"],
                d.get("plaintiff", ""),
                d.get("defendant", ""),
            )
        if role and name:
            return f"{role} {name}"
        if role:
            return role
        if name:
            return name
        binary = d.get("appellant", "")
        if binary:
            return shorten_party_name(binary)
        return ""

    # Апелляционные блоки (new_cases + changes) — собираем сейчас, добавим
    # в context_parts ПОСЛЕ всех fi-блоков (см. ниже, перед кассацией).
    _appeal_context_parts: list[str] = []

    if new_cases:
        _appeal_context_parts.append("\nНОВЫЕ ДЕЛА:")
        for c in new_cases:
            url = case_card_url(c)
            pl = shorten_party_name(c['Истец'], keep_fio_full=True)
            df = shorten_party_name(c['Ответчик'], keep_fio_full=True)
            line = (
                f"- {c['Номер дела']} (URL: {url}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"категория: {short_category_chain(c['Категория'])}, "
                f"роль банка: {c['Роль банка']}, "
                f"суд 1 инст.: {shorten_court_name(c['Суд 1 инстанции'])}"
            )
            fi_no_ap = (c.get("Номер дела 1 инстанции") or "").strip()
            if fi_no_ap:
                line += f", дело 1-й инст.: {fi_no_ap}"
            # Дату поступления выносим отдельным полем — в дайджесте она
            # уходит на самостоятельную строку «<b>дата</b> — 📥 поступило
            # в апел. суд» (см. пункт 5.1 промпта).
            filing = c.get('Дата поступления', '')
            if filing:
                line += f"\n  Дата поступления в апел. суд: {filing}"
            _appeal_context_parts.append(line)

    if changes:
        _appeal_context_parts.append("\nИЗМЕНЕНИЯ ПО ДЕЛАМ:")
        for ch in changes:
            d = ch["details"]
            url = d.get("case_url", "")
            line = f"- Дело {ch['case']} (URL: {url})"
            pl = shorten_party_name(d.get('plaintiff', ''))
            df = shorten_party_name(d.get('defendant', ''))
            line += f"\n  Стороны: {pl} (истец) vs {df} (ответчик)"
            line += f", роль банка: {d.get('role', '')}"
            app_str = _appellant_fmt(d)
            if app_str:
                line += f", апеллянт: {app_str}"

            has_new_act = "new_act" in ch["type"]
            for t in ch["type"]:
                if t == "new_event":
                    line += f"\n  Новое событие: {d.get('event', '')}"
                    if d.get("event_date"):
                        line += f" ({d['event_date']})"
                    if d.get("hearing_date"):
                        ht = d.get("hearing_time", "")
                        line += (f"\n  Дата заседания: {d['hearing_date']}"
                                 + (f" {ht}" if ht else ""))
                if t == "new_result":
                    # Дедуп: если в этом же change есть и new_act —
                    # выводим всё в блоке 5.5 (см. ниже), а 5.4 пропускаем.
                    if has_new_act:
                        continue
                    hearing_dt = d.get("hearing_date", "")
                    line += f"\n  ИТОГ: {d.get('verdict_label', '')}"
                    if d.get("bank_outcome"):
                        line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
                    line += (
                        f"\n  Категория спора: "
                        f"{short_category_chain(d.get('category', ''))}"
                    )
                    line += f"\n  Роль банка: {d.get('role', '')}"
                    app_str = _appellant_fmt(d)
                    if app_str:
                        line += f"\n  Апеллянт: {app_str}"
                    if hearing_dt:
                        line += f"\n  Дата апелляционного определения: {hearing_dt}"
                    if d.get("hearing_long_ago"):
                        line += "\n  Заседание состоялось давно — не пиши «сегодня»."
                    if d.get("last_event"):
                        line += f"\n  Последнее событие: {d['last_event']}"
                    if d.get("act_excerpt"):
                        line += f"\n  Цитата из мотивировки: {d['act_excerpt']}"
                    line += f"\n  Сырое поле «Результат»: {d.get('result', '')}"
                if t == "new_act":
                    line += "\n  Опубликован судебный акт"
                    if d.get("hearing_date"):
                        line += f"\n  Дата апелляционного определения: {d['hearing_date']}"
                    if d.get("act_date"):
                        line += f"\n  Дата публикации акта: {d['act_date']}"
                    if d.get("act_verdict_label"):
                        line += f"\n  ИТОГ (из карточки): {d['act_verdict_label']}"
                    if d.get("act_verdict_raw"):
                        line += f"\n  Сырое поле «Результат»: {d['act_verdict_raw']}"
                    if d.get("bank_outcome"):
                        line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
                    app_str = _appellant_fmt(d)
                    if app_str:
                        line += f"\n  Апеллянт: {app_str}"
                    if d.get("act_text"):
                        line += f"\n  МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА: {d['act_text']}"
                if t == "status_change":
                    line += (f"\n  Статус: {d.get('old_status', '')} "
                             f"→ {d.get('new_status', '')}")
                if t == "hearing_postponed":
                    new_dt = d.get("new_hearing_date", "")
                    new_tm = d.get("new_hearing_time", "")
                    new_part = f"{new_dt}" + (f" {new_tm}" if new_tm else "")
                    # В выходном тексте показываем только новую дату.
                    # Старая ('old_hearing_*') в d остаётся — на случай если
                    # промпт когда-нибудь снова попросит её цитировать.
                    line += f"\n  ОТЛОЖЕНО: заседание отложено на {new_part}"
                if t == "hearing_new":
                    new_dt = d.get("new_hearing_date", "")
                    new_tm = d.get("new_hearing_time", "")
                    new_part = f"{new_dt}" + (f" {new_tm}" if new_tm else "")
                    line += f"\n  НАЗНАЧЕНО: первое заседание {new_part}"
                if t == "appeal_to_fi_rules":
                    tr_dt = d.get("transition_date", "")
                    tr_ev = d.get("transition_event", "")
                    line += (
                        "\n  ПЕРЕХОД К ПРАВИЛАМ 1-Й ИНСТ.: апелляция перешла "
                        "к рассмотрению дела по правилам производства в суде первой инстанции"
                        + (f" ({tr_dt})" if tr_dt else "")
                    )
                    if tr_ev:
                        line += f"\n  Исходное событие: {tr_ev}"

            _appeal_context_parts.append(line)

    if fi_new_cases:
        context_parts.append("\nНОВЫЕ ДЕЛА ПЕРВОЙ ИНСТАНЦИИ:")
        for c in fi_new_cases:
            fi = c.get("first_instance", {})
            court = shorten_court_name(fi.get("court", ""))
            url = fi_card_url(fi)
            pl = shorten_party_name(c.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(c.get("defendant", ""), keep_fio_full=True)
            line = (
                f"- {c['id']} (URL: {url}) (суд: {court}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"категория: {short_category_chain(c.get('category', ''))}, "
                f"роль банка: {c.get('bank_role', '')}"
            )
            # Дату подачи иска выносим отдельным полем — в дайджесте она
            # уходит на самостоятельную строку «<b>дата</b> — 📥 иск
            # зарегистрирован в суде» (см. пункт 3.1 промпта).
            if fi.get("filing_date"):
                line += f"\n  Дата подачи иска: {fi['filing_date']}"
            context_parts.append(line)

    # Секция «ПЕРЕШЛИ В АПЕЛЛЯЦИЮ» убрана из контекста: state-machine-мостик
    # юристу не нужен, дело и так появляется в 5.1 «Новые дела апелляции».
    # stage_transitions по-прежнему собирается выше по пайплайну для
    # watchlist-фильтра и push-сводки.

    if fi_changes:
        # Буфер — чтобы не печатать заголовок «ИЗМЕНЕНИЯ» над пустотой, когда
        # все события дела ушли в секцию 3.5 «Вынесены решения».
        fi_changes_buf: list[str] = []
        for ch in fi_changes:
            d = ch["details"]
            url = fi_card_url(d)
            pl = shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(ch.get("defendant", ""), keep_fio_full=True)
            # Дедуп: если дело «Решено», и fi_resolved, и fi_status_change
            # информационно тождественны — первый уходит в 3.5, второй
            # в 3.2 не нужен. Оставляем в 3.2 только побочные события
            # (заседание, отложение, final_event и т.п.).
            # Аналогично для fi_act_text_published — всегда в 3.6; если у
            # того же дела есть fi_act_published (флаг), тоже подавляем
            # его в 3.2 (текст уже сказал больше, чем флаг).
            has_resolved = "fi_resolved" in ch["type"]
            has_act_text = "fi_act_text_published" in ch["type"]
            effective_types = [
                t for t in ch["type"]
                if not (has_resolved and t in ("fi_resolved", "fi_status_change"))
                and t != "fi_act_text_published"
                and not (has_act_text and t == "fi_act_published")
            ]
            if not effective_types:
                continue
            line = (
                f"- {ch['case']} (URL: {url}) ({shorten_court_name(ch.get('court', ''))}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"роль банка: {ch.get('bank_role', '')}"
            )
            for t in effective_types:
                if t == "fi_hearing_new":
                    if d.get("hearing_date_unpublished"):
                        # Дата заседания на карточке = артефакт парсинга
                        # (нет реального session-события на эту дату).
                        # Юрист хочет видеть пометку, чтобы не гадать.
                        line += (
                            "\n  Назначено первое заседание "
                            "(дата и время не опубликованы)"
                        )
                    else:
                        hd = d.get("hearing_date", "")
                        ht = d.get("hearing_time", "")
                        htype = d.get("hearing_type", "заседание")
                        # «Первое» — потому что fi_hearing_new срабатывает
                        # только если раньше session-событий не было
                        # (см. место создания события). Без уточнения LLM
                        # принимает такое дело за новое исковое.
                        line += (f"\n  Назначено первое {htype}: {hd}"
                                 + (f" {ht}" if ht else ""))
                elif t == "fi_hearing_next":
                    # Переход «подготовка/собеседование → заседание»: было
                    # что-то досудебное, теперь назначено заседание. Не
                    # «первое», не «отложение» — отдельный сценарий.
                    new_d = d.get("hearing_date", "")
                    new_t = d.get("hearing_time", "")
                    htype = d.get("hearing_type", "заседание")
                    new_p = f"{new_d}" + (f" {new_t}" if new_t else "")
                    line += f"\n  НАЗНАЧЕНО ({htype}): заседание назначено на {new_p}"
                    if ch.get("category"):
                        line += (
                            f"\n  Категория спора: "
                            f"{short_category_chain(ch['category'])}"
                        )
                elif t == "fi_hearing_postponed":
                    new_d = d.get("hearing_date", "")
                    new_t = d.get("hearing_time", "")
                    htype = d.get("hearing_type", "заседание")
                    new_p = f"{new_d}" + (f" {new_t}" if new_t else "")
                    # Старую дату НЕ передаём в текст контекста: юрист просит
                    # видеть только новую дату, без «⏪ старая → ⏩ новая».
                    line += f"\n  ОТЛОЖЕНО ({htype}): заседание отложено на {new_p}"
                    if ch.get("category"):
                        line += (
                            f"\n  Категория спора: "
                            f"{short_category_chain(ch['category'])}"
                        )
                elif t == "fi_hearing_recess":
                    # Перерыв (ст. 157 ГПК) — то же заседание продолжено, НЕ
                    # отложение. Решение может быть вынесено в тот же день.
                    new_d = d.get("hearing_date", "")
                    new_t = d.get("hearing_time", "")
                    new_p = f"{new_d}" + (f" {new_t}" if new_t else "")
                    line += (
                        f"\n  ПЕРЕРЫВ (заседание): в заседании объявлен "
                        f"перерыв до {new_p}"
                    )
                    if ch.get("category"):
                        line += (
                            f"\n  Категория спора: "
                            f"{short_category_chain(ch['category'])}"
                        )
                elif t == "fi_status_change":
                    line += (f"\n  Статус: {d.get('old_status', '')} "
                             f"→ {d.get('new_status', '')}")
                elif t == "fi_objections_deadline_set":
                    # Срок для возражений на апел. жалобу (ст. 325 ГПК) —
                    # дедлайн работы юриста, подаётся в суд 1-й инстанции.
                    line += ("\n  Срок для возражений на жалобу: до "
                             f"{d.get('objections_due', '')}")
                elif t == "fi_default_cancellation_filed":
                    # Особый порядок отмены заочного решения (ст. 237 ГПК):
                    # заявление подаётся в тот же суд 1-й инстанции, это не
                    # апелляция.
                    line += ("\n  ЗАОЧНОЕ: подано заявление об отмене "
                             f"({d.get('cancel_filed_date', '')})")
                elif t == "fi_default_cancellation_hearing":
                    line += ("\n  ЗАОЧНОЕ: заседание по заявлению об отмене "
                             f"{d.get('cancel_hearing_date', '')}")
                elif t == "fi_default_judgment_vacated":
                    line += ("\n  ЗАОЧНОЕ РЕШЕНИЕ ОТМЕНЕНО "
                             f"({d.get('cancel_outcome_date', '')}) — дело "
                             "рассматривается заново")
                elif t == "fi_default_cancellation_refused":
                    line += ("\n  ЗАОЧНОЕ: в отмене отказано "
                             f"({d.get('cancel_outcome_date', '')}) — открыт "
                             "месячный срок на апелляцию")
                elif t == "fi_default_copy_returned":
                    # Возврат копии запускает формулу ВС для срока
                    # вступления заочного решения в силу (09.08.2026).
                    line += ("\n  ЗАОЧНОЕ: копия решения возвратилась "
                             "невручённой"
                             + (f" ({d.get('copy_returned_date', '')})"
                                if d.get('copy_returned_date') else ""))
                elif t == "fi_returned":
                    # Процессуальное завершение: возврат иска / отказ в
                    # принятии / передача по подсудности. Эмитим короткую
                    # фразу с причиной — она пойдёт в 3.2. В 3.5 такое дело
                    # не дублируется, поэтому причину берём с fallback на
                    # event_text (см. хелпер), а знак для банка несём здесь.
                    kind = (d.get("termination_kind") or "returned").strip()
                    label = _FI_TERMINATION_LABELS.get(
                        kind, _FI_TERMINATION_LABELS["returned"]
                    )
                    reason = (d.get("return_reason") or "").strip()
                    if not reason and kind != "transfer":
                        reason = _fi_return_reason_for_render(d)
                    line += f"\n  {label.upper()}"
                    if reason:
                        line += f": {reason}"
                    bank_out = (d.get("bank_outcome") or "").strip()
                    if bank_out:
                        line += f" (для банка: {bank_out})"
                    # Дата события-завершения — суд заполняет «Результат»
                    # с лагом в недели (09.08.2026); ключ опционален.
                    td = (d.get("termination_date") or "").strip()
                    if td:
                        line += f" ({td})"
                elif t == "fi_act_published":
                    # Срабатывает, когда в карточке появилась дата публикации
                    # резолютивки, но полного текста (act_text) ещё нет.
                    # Юристу важно увидеть это как «изготовлено, но не опубл.»,
                    # а не как «опубликован акт» (последнее путает с 3.6).
                    ad = d.get("act_date", "")
                    line += (
                        "\n  Мотивированное решение изготовлено"
                        + (f" {ad}" if ad else "")
                        + ", полный текст пока не опубликован"
                    )
                elif t == "fi_final_event":
                    ev = d.get('event', '') or ''
                    ev_low = ev.lower()
                    # Спец-обработка фразы «Изготовлено мотивированное решение
                    # в окончательной форме» — это эквивалент fi_act_published
                    # (карточка получила дату резолютивки, текста ещё нет).
                    # Нормализуем под единый формат, чтобы LLM не путался.
                    if ('изготовлено' in ev_low
                            and 'мотивированное решение' in ev_low):
                        m = re.search(r'(\d{2}\.\d{2}\.\d{4})', ev)
                        ad = m.group(1) if m else (d.get('event_date') or '')
                        line += (
                            "\n  Мотивированное решение изготовлено"
                            + (f" {ad}" if ad else "")
                            + ", полный текст пока не опубликован"
                        )
                    else:
                        line += f"\n  Событие: {ev}"
                        if d.get("event_date"):
                            line += f" ({d['event_date']})"
                        # Запланированная дата ближайшего заседания (для
                        # «подготовка дела»/«беседа»/«предварительное заседание»
                        # это дата самого мероприятия). Уходит в строку
                        # «📅 Заседание назначено на …» — юрист сразу видит,
                        # к какому числу готовиться.
                        sh_d = d.get("scheduled_hearing_date", "")
                        sh_t = d.get("scheduled_hearing_time", "")
                        if sh_d:
                            sh_p = sh_d + (f" {sh_t}" if sh_t else "")
                            line += (
                                f"\n  НАЗНАЧЕНО: заседание назначено на {sh_p}"
                            )
                elif t == "fi_motivirovka_emitted":
                    md = d.get('motivirovka_date', '')
                    line += (
                        "\n  Мотивированное решение изготовлено"
                        + (f" {md}" if md else "")
                        + ", полный текст пока не опубликован"
                    )
                elif t == "fi_appeal_filed":
                    role = d.get("appellant_role", "")
                    name = d.get("appellant_name", "")
                    dt = d.get("appeal_filed_date", "")
                    app_str = f"{role} {name}".strip()
                    line += "\n  Подана апелляционная жалоба"
                    if dt:
                        line += f" ({dt})"
                    if app_str:
                        line += f", апеллянт: {app_str}"
                elif t == "fi_cassation_filed":
                    dt = d.get("cassation_filed_date", "")
                    line += "\n  Подана кассационная жалоба"
                    if dt:
                        line += f" ({dt})"
                elif t == "fi_sent_to_cassation":
                    dt = d.get("sent_to_cassation_date", "")
                    line += "\n  Дело направлено в кассационный суд"
                    if dt:
                        line += f" ({dt})"
                elif t == "fi_hearing_restart":
                    rd = d.get("restart_date", "")
                    rev = d.get("restart_event", "")
                    nhd = d.get("next_hearing_date", "")
                    nht = d.get("next_hearing_time", "")
                    line += (
                        "\n  РАССМОТРЕНИЕ НАЧАТО С НАЧАЛА"
                        + (f" ({rd})" if rd else "")
                    )
                    if rev:
                        line += f"\n  Исходное событие: {rev}"
                    if nhd:
                        nxt = nhd + (f" {nht}" if nht else "")
                        line += f"\n  Следующее заседание: {nxt}"
                elif t == "fi_bank_role_changed":
                    old_r = d.get("old_role", "")
                    new_r = d.get("new_role", "")
                    hint = d.get("reason_hint", "") or ""
                    line += (
                        f"\n  ИЗМЕНЕНИЕ РОЛИ БАНКА: {old_r} → {new_r}"
                    )
                    if hint:
                        line += f" ({hint})"
                    line += (
                        ". Согласно карточке банк не является стороной."
                        " Все исходы по этому делу — НЕЙТРАЛЬНО для банка."
                    )
                elif t == "fi_accepted_no_hearing":
                    mat = d.get("material_number", "")
                    line += (
                        "\n  ПРИНЯТО К ПРОИЗВОДСТВУ, ЗАСЕДАНИЕ НЕ НАЗНАЧЕНО"
                    )
                    if mat:
                        line += f" (ранее материал {mat})"
                elif t == "fi_default_copy_served":
                    # Парное к возврату копии: вручение запускает 7-дневный
                    # срок на заявление об отмене (ст. 237 ГПК, 13.08.2026).
                    line += ("\n  ЗАОЧНОЕ: копия решения вручена ответчику"
                             + (f" ({d.get('copy_served_date', '')})"
                                if d.get('copy_served_date') else "")
                             + " — 7 раб. дн. на заявление об отмене")
                elif t == "fi_legal_force_reached":
                    line += ("\n  РЕШЕНИЕ ВСТУПИЛО В СИЛУ (расчётно"
                             + (f" {d.get('legal_force_date', '')}"
                                if d.get('legal_force_date') else "")
                             + ") — ожидаем исполнительный лист")
                elif t == "fi_writ_overdue":
                    days = d.get("overdue_days", "")
                    line += (
                        "\n  ИСПОЛНИТЕЛЬНЫЙ ЛИСТ НЕ ВЫДАН"
                        + (f" {days} дн. после вступления в силу" if days
                           else " после вступления в силу")
                        + (f" (в силе с {d.get('legal_force_date', '')})"
                           if d.get('legal_force_date') else "")
                    )
                elif t == "fi_post_decision_hearing":
                    hd = d.get("hearing_date", "")
                    ht = d.get("hearing_time", "")
                    topic = d.get("hearing_topic", "")
                    line += ("\n  ЗАСЕДАНИЕ ПО РЕШЁННОМУ ДЕЛУ: "
                             + hd + (f" {ht}" if ht else ""))
                    if topic:
                        line += f" ({topic})"
            fi_changes_buf.append(line)
        if fi_changes_buf:
            context_parts.append("\nИЗМЕНЕНИЯ ПО ДЕЛАМ ПЕРВОЙ ИНСТАНЦИИ:")
            context_parts.extend(fi_changes_buf)

    # Отдельный блок «Вынесены решения 1 инст.» — источник для раздела 3.5
    # промпта. Дела с fi_resolved приходят из fi_changes и физически
    # остаются в нём, но их статус+итог рендерятся именно здесь.
    # Дедуп: если в этом же change есть и fi_act_text_published — выводим
    # ТОЛЬКО в 3.6 (там и ИТОГ из карточки, и мотивировка). В 3.5 не
    # повторяем, иначе пользователь видит дело в обоих разделах.
    fi_resolved_changes = [
        ch for ch in fi_changes
        if "fi_resolved" in ch["type"]
        and "fi_act_text_published" not in ch["type"]
        # Возврат материала/заявления — процессуальный возврат, не решение
        # по существу. Он уже выведен в 3.2 «Изменения» (🔚 иск возвращён: …),
        # в 3.5 «Вынесенные решения» не дублируем.
        and "fi_returned" not in ch["type"]
    ]
    if fi_resolved_changes:
        context_parts.append("\nВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.:")
        for ch in fi_resolved_changes:
            d = ch["details"]
            url = fi_card_url(d)
            pl = shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(ch.get("defendant", ""), keep_fio_full=True)
            line = (
                f"- {ch['case']} (URL: {url}) ({shorten_court_name(ch.get('court', ''))}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"роль банка: {ch.get('bank_role', '')}"
                f"\n  ИТОГ: {d.get('verdict_label', '')}"
                f"\n  Сырое поле «Результат»: {d.get('raw_result', '')}"
            )
            if d.get("decision_date"):
                line += f"\n  Дата решения: {d['decision_date']}"
            if d.get("category"):
                line += (
                    f"\n  Категория спора: "
                    f"{short_category_chain(d['category'])}"
                )
            if d.get("bank_outcome"):
                line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
            if "fi_bank_role_changed" in ch["type"]:
                line += (
                    f"\n  Смена роли банка: {d.get('old_role', '')} → "
                    f"{d.get('new_role', '')}"
                    f" (банк не является стороной согласно карточке;"
                    f" для банка — нейтрально)"
                )
            if "motiv_merged_date" in d:
                # Мотивировка того же прогона, приклеенная
                # _merge_motiv_into_resolved (кейс Урала 2-484/2026).
                _md = (d.get("motiv_merged_date") or "").strip()
                line += (
                    "\n  Мотивировка изготовлена"
                    + (f" {_md}" if _md else "")
                    + ", полный текст не опубликован"
                )
            if d.get("last_event"):
                line += f"\n  Последнее событие: {d['last_event']}"
            context_parts.append(line)

    # Отдельный блок «Опубликованы тексты решений 1 инст.» — источник для 3.6.
    # Зеркало 5.5 апелляции: дело может появиться и в 3.5, и в 3.6 (ИТОГ и
    # мотивировка — разные события во времени).
    fi_act_text_changes = [
        ch for ch in fi_changes if "fi_act_text_published" in ch["type"]
    ]
    if fi_act_text_changes:
        context_parts.append("\nОПУБЛИКОВАНЫ ТЕКСТЫ РЕШЕНИЙ 1 ИНСТ.:")
        for ch in fi_act_text_changes:
            d = ch["details"]
            url = fi_card_url(d)
            pl = shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(ch.get("defendant", ""), keep_fio_full=True)
            line = (
                f"- {ch['case']} (URL: {url}) ({shorten_court_name(ch.get('court', ''))}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"роль банка: {ch.get('bank_role', '')}"
            )
            if d.get("decision_date"):
                line += f"\n  Дата решения: {d['decision_date']}"
            if d.get("act_date"):
                line += f"\n  Дата публикации акта: {d['act_date']}"
            if d.get("verdict_label"):
                line += f"\n  ИТОГ (из карточки): {d['verdict_label']}"
            if d.get("raw_result"):
                line += f"\n  Сырое поле «Результат»: {d['raw_result']}"
            if d.get("bank_outcome"):
                line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
            if "fi_bank_role_changed" in ch["type"]:
                line += (
                    f"\n  Смена роли банка: {d.get('old_role', '')} → "
                    f"{d.get('new_role', '')}"
                    f" (банк не является стороной согласно карточке;"
                    f" для банка — нейтрально)"
                )
            if d.get("category"):
                line += (
                    f"\n  Категория спора: "
                    f"{short_category_chain(d['category'])}"
                )
            if d.get("last_event"):
                line += f"\n  Последнее событие: {d['last_event']}"
            if d.get("act_text"):
                line += f"\n  МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ: {d['act_text']}"
            context_parts.append(line)

    # Апелляционные блоки идут ПОСЛЕ всех fi-блоков и ПЕРЕД кассацией —
    # чтобы в дайджесте порядок больших блоков был
    # 🏛 ПЕРВАЯ ИНСТАНЦИЯ → ⚖️ АПЕЛЛЯЦИЯ → ⚖️🔬 КАССАЦИЯ.
    if _appeal_context_parts:
        context_parts.extend(_appeal_context_parts)

    # ── Кассация (7kas.sudrf.ru) ──
    # Discovery: дела, которые впервые появились в БД через 7kas (не было
    # 1-й инст./апел. в нашей истории). Идут отдельным блоком как «новые».
    if cass_discovered:
        context_parts.append("\nНОВЫЕ ДЕЛА КАССАЦИИ (открыты через 7kas):")
        for c in cass_discovered:
            cass = c.get("cassation") or {}
            fi = c.get("first_instance") or {}
            url_card = ""
            if cass.get("link"):
                cid_, cuid_ = case_id_uid(cass["link"])
                if cid_ and cuid_:
                    url_card = CASSATION_COURT.card_url(cid_, cuid_)
            # Заголовок строки = касс. внутренний номер (8Г-…/YYYY).
            # Юрист ориентируется по нему, не по номеру 1-й инст.
            line = f"- касс. № {cass.get('case_number', '')}"
            if cass.get("cassation_number"):
                line += f" [{cass['cassation_number']}]"
            line += f" (URL: {url_card or '—'}): "
            # Стороны бывают неизвестны (дело заведено discovery'ем с 7kas, в
            # карточке роли участников не свелись к истцу/ответчику). Пустой
            # фрагмент «  (истец) vs  (ответчик)» LLM только путал — в этом
            # случае дело опознаётся по заявителю (поле ниже).
            _pl_llm = shorten_party_name(c.get('plaintiff', ''), keep_fio_full=True)
            _df_llm = shorten_party_name(c.get('defendant', ''), keep_fio_full=True)
            if _pl_llm or _df_llm:
                line += f"{_pl_llm} (истец) vs {_df_llm} (ответчик), "
            line += f"роль банка: {c.get('bank_role', '?')}, "
            line += f"1-я инст. №: {c.get('id', '')}, "
            line += f"суд 1 инст.: {shorten_court_name(fi.get('court', '') or '?')}, "
            _cat_for_llm = (
                cass.get('category', '') or c.get('category', '') or '—'
            )
            if _cat_for_llm != '—':
                _cat_for_llm = short_category_chain(_cat_for_llm) or '—'
            line += f"категория: {_cat_for_llm}, "
            line += f"касс. судья: {cass.get('judge', '')}, "
            line += f"заявитель: {cass.get('appellant', '')} ({cass.get('appellant_status', '')})"
            # Дату поступления вынесли отдельным полем — LLM выводит её
            # самостоятельной строкой «<b>дата</b> — 📥 поступила касс.
            # жалоба от {заявитель}», см. пункт 6.1 промпта.
            if cass.get("filing_date"):
                line += f"\n  Дата поступления касс. жалобы: {cass['filing_date']}"
            if cass.get("review_result"):
                line += f"\n  Изучение жалобы: {cass['review_result']}"
            if cass.get("outcome"):
                line += f"\n  ИСХОД: {cass['outcome']}"
            if cass.get("result_text"):
                line += f"\n  Результат рассмотрения: {cass['result_text']}"
            if cass.get("result_for_appeal"):
                line += f"\n  В отношении апел. инст.: {cass['result_for_appeal']}"
            context_parts.append(line)

    # Кассационные события по уже известным делам (cassation_pending → cassation,
    # выход определения, новые слушания и т.п.). Текст определения — в act_text.
    # Стороны / категория / банк-роль / суд 1 инст. подтягиваются из
    # родительского case (в самом cass_changes.details их нет, иначе LLM
    # получает плейсхолдеры «{не указаны}»). URL карточки 7kas собираем из
    # details.link (case_id|case_uid). Готовые русские метки исхода/стадии
    # подаём отдельными полями — Python их формирует, чтобы LLM не переводила
    # длинные 7kas-формулировки самостоятельно.
    if cass_changes:
        # cass_changes ссылаются на FI-номер (например, «2-621/2025»), а в
        # ctx["cases"] / переданном `cases` могут быть только апел. дела
        # (33-XXXX) — особенно при `--replay-last` с legacy-CSV-контекста.
        # Поэтому подгружаем актуальный cases.json (JSON-формат, с FI-делами)
        # как основной источник родительских данных. Передан­ный `cases`
        # используем как fallback, чтобы тесты с моками тоже работали.
        try:
            full_cases_for_cass = load_json(config.JSON_PATH).get("cases", []) or []
        except (OSError, json.JSONDecodeError):
            full_cases_for_cass = []
        merge_cases = full_cases_for_cass or cases or []
        cases_by_id_for_cass: dict[str, dict] = {}
        for c in merge_cases:
            for k in (
                c.get("id") or "",
                (c.get("first_instance") or {}).get("case_number") or "",
                c.get("Номер дела") or "",
            ):
                if k:
                    cases_by_id_for_cass.setdefault(k, c)

        def _g(parent: dict, eng: str, ru: str) -> str:
            return (parent.get(eng) or parent.get(ru) or "").strip() if parent else ""

        context_parts.append("\nКАССАЦИОННЫЕ СОБЫТИЯ (7kas):")
        for ch in cass_changes:
            d = ch.get("details") or {}
            if "discovered_in_cassation" in ch.get("type", []):
                continue  # уже в блоке «НОВЫЕ ДЕЛА КАССАЦИИ» выше
            parent = cases_by_id_for_cass.get(ch.get("case", "")) or {}
            fi_p = parent.get("first_instance") or {}
            url_card = ""
            if d.get("link"):
                cid_, cuid_ = case_id_uid(d["link"])
                if cid_ and cuid_:
                    url_card = CASSATION_COURT.card_url(cid_, cuid_)
            line = (
                f"- 1-я инст. № {ch.get('case', '')} → касс. № "
                f"{ch.get('cassation_internal_number', '')}"
                f" (URL карточки 7kas: {url_card or '—'})"
            )
            # «стадия prev → now» оставляем ТОЛЬКО если она реально менялась.
            # Для review_result_change / outcome_change / new_act prev==now
            # (повторное событие в стадии cassation) — это не переход.
            sp = d.get("stage_prev", "")
            sn = d.get("stage_now", "")
            if sp and sn and sp != sn:
                line += f", переход стадии: {sp} → {sn}"
            pl = _g(parent, "plaintiff", "Истец")
            df = _g(parent, "defendant", "Ответчик")
            if pl or df:
                line += (
                    f"\n  Стороны: {shorten_party_name(pl, keep_fio_full=True)}"
                    f" (истец) vs "
                    f"{shorten_party_name(df, keep_fio_full=True)} (ответчик)"
                )
            role = _g(parent, "bank_role", "Роль банка")
            if role:
                line += f"\n  Роль банка: {role}"
            cat = short_category_chain(_g(parent, "category", "Категория"))
            if cat:
                line += f"\n  Категория: {cat}"
            fi_court = (fi_p.get("court") or "") or _g(parent, "court", "Суд 1 инстанции")
            if fi_court:
                line += f"\n  Суд 1 инст.: {shorten_court_name(fi_court)}"
            if d.get("appellant"):
                ap_status = d.get("appellant_status", "") or ""
                # Сокращаем имя заявителя на стороне Python: иначе в строке
                # Итог LLM напишет «; подана Ответчиком МТУ Росимущества в
                # Тюменской области, ХМАО-Югре, ЯНАО» вместо короткого
                # «МТУ Росимущество».
                appellant_short = shorten_party_name(
                    d["appellant"], keep_fio_full=True
                )
                line += (
                    f"\n  Заявитель: {appellant_short}"
                    f" ({ap_status or '—'}, банк_заявитель="
                    f"{d.get('appellant_is_bank', False)})"
                )
            # Готовые русские подписи: review_label_ru — для ранней стадии
            # (когда outcome пуст, но review_result есть); outcome_label_ru —
            # финальный исход. LLM подставляет их в строку «Итог:» как есть.
            review_label_ru = cassation_review_label(
                d.get("review_result", ""), d.get("outcome", "")
            )
            # Для cassation_terminated собираем конкретику (возврат /
            # прекращение / отзыв) + причину из review_result/result_text.
            # Для остальных исходов берём готовую подпись из CASSATION_OUTCOME_RU.
            outcome_enum = d.get("outcome", "")
            outcome_reason_ru = ""
            if outcome_enum == "cassation_terminated":
                outcome_label_ru, outcome_reason_ru = cassation_terminated_label(
                    d.get("review_result", ""), d.get("result_text", "")
                )
            else:
                outcome_label_ru = CASSATION_OUTCOME_RU.get(outcome_enum, "")
            if review_label_ru:
                line += f"\n  Метка стадии (готовая для «Итог»): {review_label_ru}"
            if outcome_label_ru:
                line += f"\n  Метка исхода (готовая для «Итог»): {outcome_label_ru}"
            if outcome_reason_ru:
                line += f"\n  Причина (для «Итог»): {outcome_reason_ru}"
            # Дата поступления — ТОЛЬКО при первой линковке карточки (тот же
            # гейт, что в template.py): в details она лежит у ВСЕХ типов, и без
            # него LLM печатала бы «поступила жалоба» на каждом следующем
            # событии дела — итоге, акте.
            if d.get("filing_date") and "new_cassation" in (ch.get("type") or []):
                line += f"\n  Дата поступления касс. жалобы: {d['filing_date']}"
            if d.get("review_result"):
                line += f"\n  Изучение жалобы (raw): {d['review_result']}"
            if d.get("outcome"):
                line += f"\n  ИСХОД (raw enum): {d['outcome']}"
            if d.get("result_text"):
                line += f"\n  Результат рассмотрения: {d['result_text']}"
            if d.get("result_for_appeal"):
                line += f"\n  В отношении апел. инст.: {d['result_for_appeal']}"
            if d.get("decision_date"):
                line += f"\n  Дата вынесения опред.: {d['decision_date']}"
            if d.get("hearing_date"):
                hd = d['hearing_date']
                ht = d.get("hearing_time", "") or ""
                line += f"\n  Дата заседания: {hd}{(' ' + ht) if ht else ''}"
            if d.get("suspended_until"):
                line += ("\n  БЕЗ ДВИЖЕНИЯ: срок устранения недостатков до "
                         f"{d['suspended_until']}")
            if d.get("remanded_to"):
                _rem = {"appeal": "в суд апелляционной инстанции",
                        "first_instance": "в суд первой инстанции"}.get(
                    d["remanded_to"], d["remanded_to"])
                line += f"\n  Куда направлено: {_rem}"
            if d.get("act_date"):
                line += f"\n  Дата публикации акта: {d['act_date']}"
            if d.get("act_text"):
                line += f"\n  МОТИВИРОВОЧНАЯ ЧАСТЬ ОПРЕДЕЛЕНИЯ: {d['act_text']}"
            context_parts.append(line)

    # Карта «номер дела → URL карточки» для пост-процессора
    # `_wrap_all_bare_case_numbers`: глобально оборачивает голые номера
    # дел в <a href>, если LLM забыл (особенно в 5.3/5.4/3.5 — там
    # `_validate_digest_new_sections` не работает, страховки не было).
    url_by_num: dict[str, str] = {}

    def _remember(num: str, url: str) -> None:
        if not num or not url:
            return
        url_by_num[num] = url
        url_by_num[_bare_case_number(num)] = url

    for c in fi_new_cases:
        fi = c.get("first_instance") or {}
        _remember(c.get("id", ""), fi_card_url(fi))
        _remember(fi.get("case_number", ""), fi_card_url(fi))
    for ch in fi_changes:
        _remember(ch.get("case", ""), fi_card_url(ch.get("details") or {}))
    for c in new_cases:
        _remember(c.get("Номер дела", ""), case_card_url(c))
    for ch in changes:
        _remember(ch.get("case", ""), (ch.get("details") or {}).get("case_url", ""))
    for c in cases:
        # Активные апел. дела: URL карточки в `link`, для построения через
        # case_card_url нужен «csv-shape» dict — собираем минимальный.
        ap = c.get("appeal") or {}
        n = (ap.get("case_number") or "").strip()
        link = (ap.get("link") or "").strip()
        if n and link:
            _remember(n, link)
        fi = c.get("first_instance") or {}
        n_fi = (fi.get("case_number") or c.get("id") or "").strip()
        url_fi = fi_card_url(fi)
        if n_fi and url_fi:
            _remember(n_fi, url_fi)

    # Региональные подстановки промпта. Для ХМАО все строки собираются
    # байт-в-байт прежними (name_gen="ХМАО-Югры", extra["appeal_prep"]="в Суде
    # ХМАО-Югры") — структура промпта охраняемая, меняются только названия.
    _region = get_region()
    _rn_gen = _region.name_gen or _region.name
    if len(_region.appeal_courts) == 1:
        _ap_prep = _region.extra.get("appeal_prep") or (
            f"в апелляционном суде региона ({_region.appeal_courts[0].name})"
        )
        _appeal_court_rule = (
            f"Для апелляционных дел (номер на `33-`) суд в скобках не пиши — "
            f"все апелляции рассматриваются {_ap_prep}, подсвечивать это не нужно."
        )
        _appeal_line1_rule = (
            f"БЕЗ суда в скобках (для апелляции суд всегда "
            f"«{_region.appeal_courts[0].name}», скрываем по правилу "
            f"«Суд в скобках» в шапке)"
        )
    else:
        # Мульти-апелляционный регион (напр. Свердловская обл. + ЯНАО):
        # суд у апелляций информативен — просим брать его из записи.
        _appeal_court_rule = (
            "Для апелляционных дел (номер на `33-`) суд в скобках бери "
            "ДОСЛОВНО из поля записи — в регионе несколько апелляционных судов."
        )
        _appeal_line1_rule = (
            "суд в скобках — из поля записи (в регионе несколько "
            "апелляционных судов)"
        )

    prompt = f"""Ты — помощник юриста ПАО Сбербанк. Сформируй дайджест изменений по судебным делам судов {_rn_gen} за {today}.

ИМЕНА: все наименования сторон в данных уже сокращены по правилам (ОПФ убрана, ФИО → инициалы, «в лице филиала…» удалено и т.п.). НЕ переписывай их и НЕ возвращай ОПФ обратно. В секциях «Новые дела» имена физлиц приходят полными — там оставляй как есть.

ДАТЫ: бери ровно из переданных полей данных. Не используй today() и не угадывай. Если у дела есть пометка «Заседание состоялось давно» — реальная дата уже в поле «Дата апелляционного определения», не пиши «сегодня».

ФОРМАТ: HTML для Telegram. Разрешены только теги <b>, <i>, <a href="URL">. Никакого Markdown (* _ ` [ ]). Спецсимволы &lt; &gt; &amp; экранируй.

СТРУКТУРА — два больших блока по инстанциям. Заголовок подсекции выводи только если есть данные. Большой блок (🏛 ПЕРВАЯ ИНСТАНЦИЯ / ⚖️ АПЕЛЛЯЦИЯ) выводи только если хотя бы одна его подсекция непуста.

СУД в скобках: поле {{суд}} в любой строке бери ДОСЛОВНО из записи того же дела в данных (поля «суд», «Суд 1 инстанции», «court»). Названия судов уже приходят сокращённо — например, «Сургутский гор. суд», «Нефтеюганский рай. суд». Выводи их как есть, НЕ расшифровывай «гор.» → «городской» и «рай.» → «районный». Если у дела поля с судом нет — не пиши суд в скобках вообще. ЗАПРЕЩЕНО переносить название суда из соседней записи. {_appeal_court_rule} Значение «Суд 1 инстанции» уместно только в секциях про апелляционные дела, где прямо просят показать суд 1 инстанции (5.1).

ИНВАРИАНТ ИНСТАНЦИЙ (КРИТИЧНО): номер дела однозначно определяет, в какой большой блок оно попадает. Если номер начинается с `33-` (формат `33-XXXX/YYYY`) — это АПЕЛЛЯЦИОННОЕ дело, и оно идёт ТОЛЬКО в большой блок «⚖️ АПЕЛЛЯЦИЯ» (подсекции 5.1–5.5). Никогда не размещай номера на `33-` в подсекциях 3.1–3.6 блока «🏛 ПЕРВАЯ ИНСТАНЦИЯ». Все остальные номера 1-й инстанции (`2-…/YYYY`, `М-…/YYYY`, `9-…/YYYY` и т.п.) идут ТОЛЬКО в блок «🏛 ПЕРВАЯ ИНСТАНЦИЯ». Нарушение этого правила = критическая ошибка, дело не должно «всплыть не в той инстанции» ни при каких условиях.

ССЫЛКА НА КАРТОЧКУ ДЕЛА (КРИТИЧНО): в КАЖДОЙ строке, где упоминается номер дела (3.1–3.6, 4, 5.1–5.5), номер ОБЯЗАТЕЛЬНО оборачивается в `<a href="URL"><b>номер</b></a>`, где URL — поле «URL» того же дела из данных (это ссылка на карточку на сайте суда, sudrf.ru). Голый номер без `<a href>` = БРАК. Если URL в данных пустой — всё равно выведи `<b>номер</b>` (без ссылки), но это исключение, а не норма.

БАНК В ХВОСТЕ СТРОКИ: во всех строках, где есть фраза «банк — {{роль}}» (3.2, 3.5, 5.1, 5.4 и т.п.): если «Сбербанк» / «ПАО Сбербанк» / «Сбербанк России» явно упомянут в сторонах (истец или ответчик) — блок «банк — {{роль}}» и «<b>, банк — {{роль}}</b>» НЕ пиши. Хвост нужен ТОЛЬКО когда банк = Третье лицо и в сторонах не фигурирует. Правило действует на все секции промпта без исключения.

ИЗМЕНЕНИЕ РОЛИ БАНКА (fi_bank_role_changed): если у дела в разделе «ИЗМЕНЕНИЯ ПО ДЕЛАМ ПЕРВОЙ ИНСТАНЦИИ» есть строка «ИЗМЕНЕНИЕ РОЛИ БАНКА: <старая> → <новая>» — это значит, что суд исключил банк из числа ответчиков (или перевёл в иную роль). Правила: (а) выведи это событие в 3.2 «Изменения» отдельной строкой «🔄 роль банка: <старая> → <новая> ({{подсказка причины, если есть}}). Дальнейшие исходы — нейтральны». (б) Если у этого же дела одновременно есть «ВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.» (3.5) или «ОПУБЛИКОВАНЫ ТЕКСТЫ РЕШЕНИЙ 1 ИНСТ.» (3.6) — в строке исхода добавь хвост «<b>Для банка:</b> нейтрально — банк не сторона согласно карточке» вместо «в пользу банка»/«против банка», даже если в данных есть поле «В чью пользу для банка». (в) НЕ помечай результат как «против банка» или «в пользу банка» при изменении роли — банк больше не сторона, исход к нему не относится.

ПРАВИЛА РЕЗОЛЮТИВНЫХ СЕКЦИЙ (применяются к 3.5 и 5.4):
• ИТОГ цитируй ДОСЛОВНО из поля «ИТОГ»; не переформулируй и не подменяй шаблоном.
• Если блока «ИТОГ» в данных нет — дело в секцию НЕ включай.
• Имя судьи НЕ указывай.
• Поле «В чью пользу для банка» пустое/отсутствует → блок «<b>Для банка:</b> …» НЕ пиши вообще; не подставляй «—», «0», «не определено». Строка тогда заканчивается на «банк — {{роль}}» без хвоста.
• Если ИТОГ = «прекращено / оставлено без рассмотрения / возвращено / снято» — добавь в конце строки короткую причину из «Последнее событие» (мировое соглашение, отказ от иска, неявка и т.п.), если она есть.
• «Составлено мотивированное определение» не упоминай — это служебный шаг.

ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (применяются к 3.6 и 5.5):
Формат — ТРИ строки на дело, между делами пустая строка.
Строка «<b>Почему:</b>» — 4-5 коротких предложений с КОНКРЕТНЫМ обоснованием из мотивировки. Структура (порядок гибкий, но СУЩНОСТЬ обязательна): (а) какую конкретную норму применил суд — со ссылкой на статью/пункт/часть кодекса или закона (ст. 16 ЗоЗПП, п. 1 ст. 167 ГК и т.п.); (б) какой ключевой довод стороны принял или отклонил — и почему (например, «Банк не доказал возможность отказа потребителя», «истец не подтвердил факт оплаты», «довод о пропуске срока отклонён, т.к. течение срока прерывалось»); (в) какое фактическое обстоятельство стало решающим (что именно не доказала / подтвердила сторона); (г) опционально — практическое следствие для банка одной фразой (закрывает риск / создаёт прецедент / усиливает позицию по аналогичным спорам). Пример: «Суд сослался на ст. 16 ЗоЗПП — услуга навязана при выдаче ипотеки. Банк не доказал возможность отказа потребителя от страхования. Довод об отсутствии нарушения прав потребителя отклонён, поскольку условие включено в типовую форму договора. Для банка — риск массовых исков по аналогичным договорам.»
Имя судьи НЕ указывай.
ЗАПРЕЩЕНО:
- писать общие глаголы БЕЗ существа: «пересмотрел», «установил», «отклонил доводы», «согласился с выводами», «рассмотрел доводы», «проверил законность», «исследовал материалы дела» — если рядом нет ни конкретной нормы, ни конкретного факта/довода, фраза = ЗАПРЕЩЕНА. Лучше написать короче (3 предложения), чем 5 предложений воды;
- пересказывать ФАКТУРУ спора вместо МОТИВИРОВКИ итога (фактура — это строка 1, а не строка «Почему»);
- выдумывать ИТОГ или апеллянта — если поля нет в данных, соответствующую строку («<b>Итог:</b>» / «<b>Апеллянт:</b>») НЕ пиши, не подставляй «—», «0», «не указано», «не определено»;
- упоминать процедуру заседания: явку/неявку сторон и представителей, ходатайства о рассмотрении в отсутствие стороны, отложения, извещения, вручение корреспонденции, полномочия представителей, аудиопротоколирование;
- писать штампы «замечаний на протокол не поступало», «судебные извещения вручены», «извещены надлежащим образом», «дело рассмотрено в отсутствие надлежаще извещённого»;
- копировать «в удовлетворении требований отказать» / «требования подлежат удовлетворению» / «доводы апелляционной жалобы не влекут отмены решения» без указания, КАКУЮ норму суд применил и КАКОЙ довод принял/отклонил.

1. Заголовок: 📊 Дайджест судебных дел | Суды {_rn_gen} | {today}
2. 📋 <b>Сводку</b> НЕ пиши — Python сам вставит её детерминированно по факту вывода (он точно знает, сколько дел в каждой подсекции, и не ошибётся в счётчиках). Сразу после заголовка 📊 переходи к большому блоку 🏛 ПЕРВАЯ ИНСТАНЦИЯ. Если случайно вывел блок «📋 Сводка» — он будет вырезан и заменён.

2bis. НУМЕРАЦИЯ ПОДСЕКЦИЙ: номера типа «3.1.», «3.6.», «5.1.», «5.1a.», «6.2.» в этом промпте — ВНУТРЕННИЕ идентификаторы для ссылок между правилами (например, «не дублируй в 3.2», «дело попадает в 3.6»). В ВЫВОДЕ дайджеста нумерацию НЕ показывай. Заголовки подсекций выводи СТРОГО в виде «<emoji> <b>Название (N):</b>» — БЕЗ префикса «X.Y.». Пример: пиши «📥 <b>Новые дела (3):</b>», а НЕ «5.1. 📥 <b>Новые дела (3):</b>». Это касается всех 13 подсекций (3.1–3.6, 5.1, 5.1a, 5.2, 5.4–5.5, 6.1–6.2). Номер 5.3 во внутренней нумерации пропущен (см. ниже у 5.2 «Изменения»).

3. 🏛 <b>ПЕРВАЯ ИНСТАНЦИЯ</b>
   3.1. 📥 <b>Новые иски (N):</b> — ДВЕ строки на дело. 🛑 ЖЁСТКОЕ ПРАВИЛО: если в данных дела есть поле «Дата подачи иска» — строка 2 ОБЯЗАТЕЛЬНА, её отсутствие = БРАК. Не сворачивай дело в одну строку, не клади дату в конец строки 1. КРИТИЧНО: строки 1 и 2 ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Между разными делами — одна пустая строка.
        • строка 1: <a href="URL"><b>номер</b></a> (URL ТОЛЬКО из поля URL этого дела в данных, ничего не выдумывай) — {{стороны (имена физлиц полностью)}} | категория: {{категория}} | {{суд}}, банк — {{роль}} (хвост «банк — …» по правилу БАНК В ХВОСТЕ).
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки) — ТОЛЬКО если в данных есть поле «Дата подачи иска»: <b>{{ДД.ММ.ГГГГ}}</b> — 📥 иск зарегистрирован в суде.
        КРИТИЧНО: эмодзи 📥 ставь ПОСЛЕ <b>даты</b>, НЕ перед — иначе строка путается с заголовком подсекции. Если поля «Дата подачи иска» нет — строку 2 не пиши, не подставляй today()/«—»/«не указано».
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (две строки одного дела):
            <a href="https://...sudrf.ru/..."><b>М-476/2026</b></a> — Шахова Ирина Владимировна vs Сбербанк | категория: услуг кредитных организаций | Мегионский гор. суд, банк — Ответчик
            <b>06.05.2026</b> — 📥 иск зарегистрирован в суде
        ❌ НЕПРАВИЛЬНО (одна строка, дата проглочена):
            <a href="https://...sudrf.ru/..."><b>М-476/2026</b></a> (Мегионский гор. суд) — Шахова Ирина Владимировна vs Сбербанк | категория: ..., банк — Ответчик
   3.2. 📅 <b>Изменения (N):</b> — ДВЕ строки на дело (исключения: ОТЛОЖЕНИЕ заседания и НАЗНАЧЕНИЕ заседания после подготовки/собеседования — ТРИ строки, см. ниже). КРИТИЧНО: строки одного дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка ставится ТОЛЬКО между разными делами. Нарушение: «строка1 \n ПУСТО \n строка2» — НЕ делать так никогда. `N` в заголовке = количество дел, ФАКТИЧЕСКИ выведенных ниже в этой подсекции (не общее число изменений в данных). Пример: у одного дела в данных И перенос заседания, И рассмотрение с начала → это ОДНО дело, одна запись (3 строки, потому что есть отложение), N=1. Не плюсуй события как отдельные единицы. Если дело вынесено в 3.3 или 3.5 — в 3.2 его НЕ повторяй, кроме случая, когда у него в этом же дайджесте есть отдельное побочное событие типа заседание/отложение. Смена статуса «В производстве → Решено» в 3.2 допустима ТОЛЬКО если этого дела нет в 3.5 (например, карточка суда ещё не опубликовала «Результат»). Если дело есть в 3.5 — в 3.2 статус не повторяй.
        • строка 1 (первая строка дела, БЕЗ пустой строки после): 📅 <b>ДД.ММ.ГГГГ ЧЧ:ММ</b> — <a href="URL"><b>номер</b></a> ({{суд}})
          — если это назначенное заседание, дата жирным СПЕРЕДИ.
          Для событий без даты (смена статуса, публикация акта, «рассмотрение начато с начала», «назначено первое заседание (дата и время не опубликованы)» и т.п.) — строка 1 без даты впереди: <a href="URL"><b>номер</b></a> ({{суд}}).
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки между ними): {{стороны кратко}} | событие (подготовка дела / беседа / предварительное заседание / заседание / назначено первое заседание (дата и время не опубликованы) / 📥 принято к производству — заседание не назначено / статус X→Y / 📄 мотивированное решение изготовлено ДД.ММ, полный текст не опубликован / 🔚 иск возвращён: ПРИЧИНА / в архив / рассмотрение с начала). Маркер «ПРИНЯТО К ПРОИЗВОДСТВУ, ЗАСЕДАНИЕ НЕ НАЗНАЧЕНО» (материал М-… стал делом 2-…) копируй В строку 2 ДОСЛОВНО как «📥 принято к производству — заседание не назначено» (+ «(было М-…)», если в данных указан прежний материал); даты в строку 1 НЕ подставляй. КРИТИЧНО: фразу «📄 мотивированное решение изготовлено …, полный текст не опубликован» бери ДОСЛОВНО из строки «Мотивированное решение изготовлено …» во входных данных дела — это событие появляется, когда в карточке проставлена дата резолютивки, но полного текста (мотивировки) ещё нет. Если у того же дела в данных есть поле «МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ» — дело идёт ТОЛЬКО в 3.6 «Опубликованные тексты решений», в 3.2 эту строку НЕ дублируй.
          — Если в данных дела стоит фраза «Назначено первое заседание (дата и время не опубликованы)» — копируй её В строку 2 ДОСЛОВНО, НЕ выдумывай дату/время, НЕ добавляй префикс 📅 ДД.ММ.ГГГГ в строку 1. Это означает: на сайте суда дата заседания не опубликована, мы только зафиксировали факт назначения.
          — Если в данных дела стоит один из маркеров процессуального завершения 1-й инст. — «🔚 ИСК ВОЗВРАЩЁН[: причина]», «🔚 ОТКАЗАНО В ПРИНЯТИИ ИСКА[: причина]», «➡️ ДЕЛО ПЕРЕДАНО ПО ПОДСУДНОСТИ[: куда]» — копируй его в строку 2 ДОСЛОВНО маленькими буквами, вместе с причиной и хвостом «(для банка: …)», если они есть. Примеры: «🔚 иск возвращён: дело не подсудно данному суду (для банка: в пользу банка)», «➡️ дело передано по подсудности». Если причины нет — только сам маркер. ПРИОРИТЕТ: при наличии любого из этих маркеров НЕ пиши параллельно «Назначено первое заседание …», «статус: В производстве → Решено» или сырой текст события («Решение вопроса о принятии иска … Возвращение иска …») для этого же дела, и НЕ выводи его в 3.5 — завершение уже всё объясняет одной строкой.
        • ОТЛОЖЕНИЕ ЗАСЕДАНИЯ (источник — поле «ОТЛОЖЕНО» во входных данных дела) — ТРИ строки, БЕЗ стрелочек, БЕЗ старой даты. Формат строго:
          – строка 1: <a href="URL"><b>номер</b></a> ({{суд}})  [БЕЗ даты впереди]
          – строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{стороны кратко}} | категория: {{категория из «Категория спора»}}
          – строка 3 (СРАЗУ под 2, БЕЗ пустой строки): 🔁 Заседание отложено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b>
          ЗАПРЕЩЕНО: писать «⏪», «⏩», «старая дата → новая дата», «перенесено с …», указывать дату, с которой перенесли. Берётся ТОЛЬКО новая дата (из строки «ОТЛОЖЕНО (…): заседание отложено на ДД.ММ.ГГГГ ЧЧ:ММ»). Если у дела рядом с «ОТЛОЖЕНО» есть другое событие (статус, акт) — оно НЕ идёт отдельной строкой; формат остаётся 3-строчным, ОТЛОЖЕНИЕ доминирует.
        • ПЕРЕРЫВ В ЗАСЕДАНИИ (источник — поле «ПЕРЕРЫВ» во входных данных дела) — ст. 157 ГПК: то же заседание ПРОДОЛЖЕНО на новую дату, это НЕ отложение и НЕ «рассмотрение с начала» (решение может быть вынесено в тот же день). ТРИ строки, как у отложения, но строка 3 ДОСЛОВНО: 🔁 в заседании объявлен перерыв до <b>ДД.ММ.ГГГГ ЧЧ:ММ</b>. Берётся ТОЛЬКО дата из строки «ПЕРЕРЫВ (…): в заседании объявлен перерыв до ДД.ММ.ГГГГ ЧЧ:ММ». ЗАПРЕЩЕНО: писать «отложено», «перенесено», «рассмотрение начато с начала» для перерыва.
        • НАЗНАЧЕНИЕ ЗАСЕДАНИЯ — применяется ВСЕГДА, когда в данных дела есть строка «НАЗНАЧЕНО (…): заседание назначено на ДД.ММ.ГГГГ ЧЧ:ММ» или «Назначено первое заседание: ДД.ММ.ГГГГ ЧЧ:ММ» (включая случаи, когда у того же дела основное событие — «подготовка дела (собеседование)», «беседа», «предварительное заседание»: тогда событие идёт в строку 2, а дата заседания — отдельной строкой 3, чтобы юрист сразу видел, к когда готовиться). ТРИ строки, аналогично отложению, но без слова «отложено». Формат строго:
          – строка 1: <a href="URL"><b>номер</b></a> ({{суд}})  [БЕЗ даты впереди]
          – строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{стороны кратко}} | {{ИЛИ событие из карточки (подготовка дела (собеседование) / беседа / предварительное заседание), если в данных есть «Событие: …»; ИНАЧЕ — категория: {{категория из «Категория спора»}}}}
          – строка 3 (СРАЗУ под 2, БЕЗ пустой строки): 📅 Заседание назначено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b>
          ЗАПРЕЩЕНО: писать «отложено», «перенесено». Слово «первое» в строке 3 НЕ пиши — формат единый «Заседание назначено на …». Берётся ТОЛЬКО новая дата из строки «НАЗНАЧЕНО (…): заседание назначено на ДД.ММ.ГГГГ ЧЧ:ММ» (или из «Назначено первое заседание: …» для fi_hearing_new).
        • Для «рассмотрение с начала» (событие «fi_hearing_restart» в данных) строка 2 ДОЛЖНА КОПИРОВАТЬ ДОСЛОВНО (байт-в-байт, включая теги <b>, эмодзи 🔄 и пробелы) фразу: «<b>🔄 рассмотрение начато с начала</b>», далее в скобках ({{дата события}}); следующее заседание {{ДД.ММ.ГГГГ ЧЧ:ММ}} — дату следующего заседания берёшь ДОСЛОВНО из поля «Следующее заседание» того же дела в данных, не из соседней записи. Если поля «Следующее заседание» нет — дату не подставляй. ЗАПРЕЩЕНО: писать «начано» вместо «начато», пропускать теги <b>/</b>, менять эмодзи. НИКОГДА не выделяй «рассмотрение с начала» в отдельную строку/подсекцию — оно идёт в 3.2 как обычное событие. Применяй фразу «рассмотрение начато с начала» ТОЛЬКО при наличии события «fi_hearing_restart» (строка «РАССМОТРЕНИЕ НАЧАТО С НАЧАЛА») в данных дела — НЕ для перерыва (поле «ПЕРЕРЫВ») и НЕ для отложения (поле «ОТЛОЖЕНО»).
   3.3. 📨 <b>Поданы апелляционные жалобы (N):</b> — ОДНА строка на дело (подсекция показывается только если N&gt;0). `N` = число строк ниже.
        <a href="URL"><b>номер</b></a> ({{суд}}) — {{стороны кратко}} | <b>апеллянт:</b> {{Роль Имя}} (дата подачи в скобках, если есть).
        Берётся из событий «fi_appeal_filed» в данных. НЕ дублируй это дело в 3.2 даже если у него есть ещё и смена статуса — событие подачи жалобы приоритетнее и идёт в свою подсекцию.
   3.4. 📨 <b>Кассационные события (N):</b> — ОДНА строка на дело (подсекция показывается только если N&gt;0). Касс. жалоба подаётся через суд 1-й инстанции, поэтому событие видно в карточке 1-й инст. даже если само дело уже прошло апелляцию. `N` = число строк ниже.
        <a href="URL"><b>номер</b></a> ({{суд}}) — {{стороны кратко}} | 📨 подана касс. жалоба ({{дата}}) ИЛИ 📤 направлено в касс. суд ({{дата}}).
        Берётся из событий «fi_cassation_filed» и «fi_sent_to_cassation» в данных. Оба типа мержим в одну строку если присутствуют у одного дела. НЕ дублируй это дело в 3.2.
   3.5. ⚖️ <b>Вынесенные решения (N):</b> — решение суда первой инстанции по существу дела (или процессуальное завершение: прекращение, без рассмотрения). ДВЕ строки на дело, между делами пустая строка (подсекция показывается только если N&gt;0). `N` = число дел ниже.
        • строка 1: <a href="URL"><b>номер</b></a> ({{суд}}) — Решение от {{дата решения}}. <b>ИТОГ:</b> {{дословно поле ИТОГ}}. Категория: {{дословно}}.
        • строка 2: Стороны: {{истец}} vs {{ответчик}}, банк — {{роль}}. <b>Для банка:</b> {{дословно «В чью пользу для банка»}}.
        Применяются ПРАВИЛА РЕЗОЛЮТИВНЫХ СЕКЦИЙ (см. выше).
        Берётся из событий «fi_resolved» в данных (секция «ВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.»). Дело, попавшее в 3.5, в 3.2 НЕ дублируется — кроме случая, когда у того же дела есть ещё отдельное побочное событие (заседание/отложение). Процессуальное завершение (маркеры «🔚 ИСК ВОЗВРАЩЁН», «🔚 ОТКАЗАНО В ПРИНЯТИИ ИСКА», «➡️ ДЕЛО ПЕРЕДАНО ПО ПОДСУДНОСТИ» в данных) в 3.5 НЕ выводится — решения по существу там нет; оно уже отражено ОДНОЙ строкой в 3.2 «Изменения», и в секцию «ВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.» такие дела не попадают.
   3.6. 📄 <b>Опубликованные тексты решений (N):</b> — полный текст решения 1-й инст. (выходит через 14+ дней после заседания, иногда не публикуется вовсе).
        🛑 БЛОКИРУЮЩЕЕ ПРАВИЛО (нарушение = критический брак): дело попадает в 3.6 ИСКЛЮЧИТЕЛЬНО если в его данных явно есть непустое поле «МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ:» с фактическим текстом мотивировки. ИСТОЧНИК ДАННЫХ ДЛЯ 3.6 — ТОЛЬКО секция «ОПУБЛИКОВАНЫ ТЕКСТЫ РЕШЕНИЙ 1 ИНСТ.» во входных данных. Если этой секции нет или дела в ней нет — дело НЕ попадает в 3.6 НИ ПРИ КАКИХ УСЛОВИЯХ. Запрещено: класть дело в 3.6 на основании фразы «Изготовлено мотивированное решение в окончательной форме» в last_event/event (это событие fi_final_event/fi_act_published, идёт в 3.2, не в 3.6). Запрещено выдумывать «Итог», «Почему», «требуется уточнение», «полный текст ещё не опубликован» — если фактической мотивировки в данных нет, дело идёт в 3.2 с фразой «📄 мотивированное решение изготовлено ДД.ММ, полный текст не опубликован», а не в 3.6.
        КРИТИЧНО: ТРИ строки ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка ставится ТОЛЬКО между разными делами:
        • строка 1: <a href="URL"><b>номер</b></a> — Решение от {{Дата решения}}: {{стороны кратко}}. (Дата — ДОСЛОВНО из поля «Дата решения» в данных. Если поля нет — пиши без даты: «<a href="URL"><b>номер</b></a>: {{стороны кратко}}», но НЕ подставляй today()/«—»/«не указано».)
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки): <b>Итог:</b> {{удовлетворено / удовлетворено частично / отказано / прекращено / оставлено без рассмотрения / возвращено — дословно из «ИТОГ (из карточки)»}}. <b>Для банка:</b> {{дословно из поля «В чью пользу для банка»}}.
        • строка 3 (СРАЗУ под строкой 2, БЕЗ пустой строки): <b>Почему:</b> см. ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (выше).
        Применяются ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (формат трёх строк, блок ЗАПРЕЩЕНО, правило про пустое «Для банка» и отсутствующий ИТОГ — см. выше).
        Берётся из событий «fi_act_text_published» в данных (секция «ОПУБЛИКОВАНЫ ТЕКСТЫ РЕШЕНИЙ 1 ИНСТ.»).

5. ⚖️ <b>АПЕЛЛЯЦИЯ</b>
   5.1. 📥 <b>Новые дела (N):</b> — ТРИ строки на дело. 🛑🛑🛑 ЖЁСТКОЕ ПРАВИЛО (нарушение = критический брак, повторяю трижды): для КАЖДОГО дела в этой секции ОБЯЗАТЕЛЬНЫ строка 2 (суд + категория + банк-роль) и строка 3 (дата поступления, если есть в данных). Сокращать дело до одной строки «номер — стороны» — ЗАПРЕЩЕНО, это критическая потеря данных: юрист по такой строке НЕ ПОНИМАЕТ, какой суд, какая категория, в какой роли банк, нужно ли участие. ВСЕГДА выводи строку 2, ВСЕГДА выводи строку 3 (если дата есть). Если данные «Суд 1 инстанции», «категория», «роль банка» есть в источнике (а они есть в 99% случаев) — они ОБЯЗАНЫ попасть в строку 2.

        КРИТИЧНО: строки 1, 2 и 3 ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка — ТОЛЬКО между разными делами. Номер ОБЯЗАТЕЛЬНО оборачивай в <a href="URL"><b>номер</b></a> — без ссылки строка считается БРАКОМ.
        • строка 1: <a href="URL"><b>номер</b></a> — {{истец}} vs {{ответчик}} (имена физлиц полностью — см. правило ИМЕНА в шапке)
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки): Суд 1 инст.: {{суд 1 инстанции}} | категория: {{категория}} | банк — {{роль}}
          (хвост «банк — …» — по правилу БАНК В ХВОСТЕ; категория уже ПОДГОТОВЛЕНА Python — копируй ДОСЛОВНО, НЕ обрезай, НЕ удлиняй, НЕ переписывай. Цепочек «X → Y → Z» в данных уже нет: тебе подаётся ТОЛЬКО конечный сегмент.)
        • строка 3 (СРАЗУ под строкой 2, БЕЗ пустой строки) — ТОЛЬКО если в данных есть поле «Дата поступления в апел. суд»: <b>{{ДД.ММ.ГГГГ}}</b> — 📥 поступило в апел. суд.
        КРИТИЧНО: дату поступления больше НЕ оставлять в строке 2 — только отдельной строкой 3. Эмодзи 📥 ставь ПОСЛЕ <b>даты</b>, НЕ перед — иначе строка путается с заголовком подсекции. Если поля «Дата поступления в апел. суд» нет — строку 3 не пиши, не подставляй today()/«—»/«не указано».
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (три строки одного дела):
            <a href="https://...sudrf.ru/..."><b>33-3611/2026</b></a> — Сбербанк vs Мурзубаева Данна Алибековна
            Суд 1 инст.: Ханты-Мансийский рай. суд | категория: прочие исковые дела | банк — Истец
            <b>08.05.2026</b> — 📥 поступило в апел. суд
        ❌ НЕПРАВИЛЬНО (одна строка, всё проглочено — критический брак):
            <a href="https://...sudrf.ru/..."><b>33-3611/2026</b></a> — Сбербанк vs Мурзубаева Данна Алибековна
        ❌ НЕПРАВИЛЬНО (без роли банка — юрист не понимает, истец банк или ответчик):
            <a href="https://...sudrf.ru/..."><b>33-3611/2026</b></a> — Сбербанк vs Мурзубаева Данна Алибековна
            Суд 1 инст.: Ханты-Мансийский рай. суд | категория: прочие исковые дела
   5.1a. ⚠ <b>Переход к правилам 1-й инстанции (N):</b> — РЕДКОЕ и КРИТИЧНОЕ событие (ч.5 ст.330 ГПК). ОДНА строка на дело (подсекция показывается только если N&gt;0):
        ⚠ <a href="URL"><b>номер</b></a> — апелляция перешла к рассмотрению дела по правилам производства в суде первой инстанции ({{дата, если есть}}). {{стороны кратко}} | роль банка. НИКОГДА не выкидывать при нехватке места. Берётся из событий «appeal_to_fi_rules» в данных.
   5.2. 📅 <b>Изменения (N):</b> — ТРИ строки на дело. КРИТИЧНО: строки 1, 2 и 3 ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка — ТОЛЬКО между разными делами. Эта секция РЕДКАЯ и ВАЖНАЯ — никогда не выкидывай при нехватке места. Источник — события «ОТЛОЖЕНО:» (заседание отложено) и «НАЗНАЧЕНО:» / «Новое событие: Судебное заседание …» (заседание назначено / новое заседание) во входных данных. `N` = количество дел.
        • строка 1: <a href="URL"><b>номер</b></a> — БЕЗ даты впереди, {_appeal_line1_rule}.
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки): {{истец}} vs {{ответчик}} | категория: {{категория}}
        • строка 3 (СРАЗУ под строкой 2, БЕЗ пустой строки) — ОДИН из вариантов:
          – 🔁 Заседание отложено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b> — если в данных дела есть «ОТЛОЖЕНО:»;
          – 📅 Заседание назначено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b> — если в данных есть «НАЗНАЧЕНО:» / «Новое событие: Судебное заседание …» (но НЕТ «ОТЛОЖЕНО:» в этом же деле).
        Если у одного дела есть И «ОТЛОЖЕНО:», И «НАЗНАЧЕНО:» — выводи ТОЛЬКО отложение (одно дело, одна запись из трёх строк). Дата+время ОБЯЗАТЕЛЬНО в <b>…</b>. Время БЕРЁТСЯ ОБЯЗАТЕЛЬНО, если в данных есть «ДД.ММ.ГГГГ ЧЧ:ММ»; писать только дату — допустимо ТОЛЬКО когда времени в данных нет совсем. Старую дату при отложении не указывай.
   (Номер 5.3 во внутренней нумерации намеренно пропущен: бывшие «Отложенные» 5.2 и «Назначенные» 5.3 объединены в одну секцию 5.2 «Изменения». Все ссылки на 5.4 и 5.5 ниже сохраняют прежние номера.)
   5.4. ⚖️ <b>Вынесенные акты (N):</b> — резолютивная часть (выходит через 1-3 дня после заседания). Только дела с блоком ИТОГ. ТРИ строки на дело, между делами пустая строка. Формат — как в 5.2 «Отложенные заседания»: первая строка — номер + стороны, вторая — категория + банк-роль, третья — итог. Дату определения встраиваем в строку «Итог», чтобы строка 1 оставалась короткой и читаемой.
        🛑 БЛОКИРУЮЩЕЕ ПРАВИЛО (нарушение = критический брак): каждое дело из «ИЗМЕНЕНИЯ ПО ДЕЛАМ» с полем «ИТОГ: …» и БЕЗ поля «МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА» ОБЯЗАТЕЛЬНО появляется в секции 5.4. Поле «Апеллянт» в 5.4 не используется и не выводится — его пустота / отсутствие НЕ повод пропустить дело. Любые правила про поле «Апеллянт» (включая запрет на его вычисление в 5.5) к секции 5.4 НЕ применяются.

        🛑 ИСКЛЮЧЕНИЕ ИЗ БЛОКИРУЮЩЕГО (нарушение = критический брак): если поле «ИТОГ: …» дословно начинается с «Заседание отложено», «Заседание назначено», «Рассмотрение начато с начала» или «Назначено первое заседание» — это НЕ результат рассмотрения, а текст события заседания (суд иногда нестандартно заполняет поле «Результат» текстом события). Такое дело идёт в 5.2 «Изменения» (как обычное отложение/назначение заседания), в 5.4 НЕ выводится; никакая «Метка исхода» в 5.4 для него не выставляется. Этот фильтр имеет приоритет над блокирующим правилом выше.

        🛑 СТРОГО ЗАПРЕЩЕНО в строке 1: писать «— Апелляционное определение от ДД.ММ.ГГГГ.», «: апелляционное определение», «— Определение от …». Строка 1 — ТОЛЬКО номер + стороны, ничего больше. Дата идёт ИСКЛЮЧИТЕЛЬНО в скобках строки 3 «Итог (ДД.ММ.ГГГГ): …». Любое упоминание «Апелляционное определение» в строке 1 = критический брак, нарушает запрос юриста на формат «как в отложениях».

        КРИТИЧНО: строки 1, 2 и 3 ОДНОГО дела идут ПОДРЯД, БЕЗ пустых строк между ними:
        • строка 1: <a href="URL"><b>номер</b></a> — {{истец}} vs {{ответчик}} (имена физлиц полностью). НИЧЕГО больше — ни даты, ни «Апелляционное определение», ни итога.
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки): категория: {{категория}}, банк — {{роль}} (хвост «банк — …» по правилу «банк в хвосте»).
        • строка 3 (СРАЗУ под 2, БЕЗ пустой строки): <b>Итог ({{ДД.ММ.ГГГГ}}):</b> {{ИТОГ дословно}}. <b>Для банка:</b> {{дословно «В чью пользу для банка»}}.
        Дату ({{ДД.ММ.ГГГГ}}) — ДОСЛОВНО из поля «Дата апелляционного определения» в данных. Если поля нет — пиши «<b>Итог:</b> …» БЕЗ скобок, не подставляй today()/«—»/«не указано».
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (три строки одного дела):
            <a href="https://...sudrf.ru/..."><b>33-876/2026</b></a> — Сбербанк vs Галиева Т.М., Муканбетов Т.С.
            категория: Кредитный договор, банк — Истец
            <b>Итог (05.05.2026):</b> ИСК (заявление) УДОВЛЕТВОРЕН. <b>Для банка:</b> в пользу банка.
        ❌ НЕПРАВИЛЬНО (дата в строке 1 — старый формат, юрист просил убрать):
            <a href="https://...sudrf.ru/..."><b>33-876/2026</b></a> — Апелляционное определение от 05.05.2026.
            Сбербанк vs Галиева Т.М. | категория: Кредитный договор | банк — Истец
            <b>Итог:</b> ИСК (заявление) УДОВЛЕТВОРЕН.
        Применяются ПРАВИЛА РЕЗОЛЮТИВНЫХ СЕКЦИЙ (см. выше). Для апелляции дополнительный перечень ИТОГ = «возвращена / без рассмотрения / прекращено / снято» — в строке 3 после «Итог: …» добавь короткую причину из «Последнее событие».
   5.5. 📄 <b>Опубликованные тексты актов (N):</b> — полный текст акта (выходит через 14+ дней после заседания, иногда вовсе не публикуется). Только дела с полем «МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА». КРИТИЧНО: ТРИ строки ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка — ТОЛЬКО между разными делами:
        • строка 1: <a href="URL"><b>номер</b></a> — Апелляционное определение от {{Дата апелляционного определения}}: {{стороны кратко}}. (Дата — ДОСЛОВНО из поля «Дата апелляционного определения» / «Дата заседания» если есть; если нет — пиши без даты «<a href="URL"><b>номер</b></a>: {{стороны кратко}}», не выдумывай.)
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки): <b>Апеллянт:</b> {{РОЛЬ}} {{имя}} — РОЛЬ и имя берёшь ДОСЛОВНО из поля «Апеллянт» в данных (формат «Истец <имя>» / «Ответчик <имя>» / «Иное лицо <имя>»). Примеры: «<b>Апеллянт:</b> Ответчик Буклей А.Л.», «<b>Апеллянт:</b> Истец Сбербанк», «<b>Апеллянт:</b> Иное лицо Фин. уполномоченный». Если поле «Апеллянт» пустое — блок «<b>Апеллянт:</b> …» не пиши вообще (полностью пропусти), не подставляй «не указано», «—», «0». НЕ пиши просто «Иное лицо» без имени, если имя в данных есть. <b>Итог:</b> {{удовлетворено / отказано / отменено полностью / отменено в части / изменено / без изменения — дословно из «ИТОГ (из карточки)» если он есть, иначе извлеки из мотивировки}}.
          📍 ОБЛАСТЬ ДЕЙСТВИЯ: следующее правило относится ИСКЛЮЧИТЕЛЬНО к секции 5.5 «Опубликованные тексты актов». В секции 5.4 «Вынесенные акты» строки «<b>Апеллянт:</b> …» нет вообще — там это правило НЕ применяется. Не используй его как повод пропустить дело из 5.4.
          🛑 ЗАПРЕЩЕНО ВЫЧИСЛЯТЬ АПЕЛЛЯНТА КОСВЕННО (внутри 5.5). Поле «Апеллянт» в данных — ЕДИНСТВЕННЫЙ источник истины для строки «<b>Апеллянт:</b> …». Если поле «Апеллянт» отсутствует ИЛИ пусто → строки «<b>Апеллянт:</b> …» в 5.5 НЕТ. Точка. Не подставляй ни одну из сторон по умолчанию — ни «Истец Сбербанк», ни ответчика, ни «Иное лицо». САМО ДЕЛО при этом из 5.5 не выкидывай: строки 1 (стороны+итог) и 3 (Почему) выводятся как обычно — пропускается ТОЛЬКО строка 2 «Апеллянт».
        • строка 3 (СРАЗУ под строкой 2, БЕЗ пустой строки): <b>Почему:</b> см. ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (выше). Если из одних сторон неочевидно, кто оспаривал решение и чего добивался (напр., «Сбербанк vs Фин. уполномоченный» — обе стороны институциональные) И при этом поле «Апеллянт» в данных НЕПУСТО — начни «Почему» с короткой фразы «<Роль апеллянта> <имя> оспаривал <что>…», чтобы читатель сразу понял направление жалобы. ЕСЛИ ПОЛЕ «АПЕЛЛЯНТ» ПУСТО — НЕ начинай «Почему» с фраз, приписывающих процессуальное действие конкретной стороне («Банк оспаривал…», «Истец требовал отмены…»); излагай обезличенно («Суд указал…», «Доводы о … отклонены…»). Это правило про обезличенный стиль — ТОЛЬКО про секцию 5.5, не повод пропускать дело ни в 5.5, ни тем более в 5.4.
        Применяются ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (формат трёх строк, блок ЗАПРЕЩЕНО — см. выше).

ВАЖНО про 5.4 и 5.5: это РАЗНЫЕ события, разведённые во времени, но если в текущем дайджесте у одного дела есть И ИТОГ, И МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА — выводи дело ТОЛЬКО в 5.5 «Опубликованные тексты актов» (там и ИТОГ из карточки, и мотивировка). В 5.4 такие дела НЕ дублируй. Раздельно дело пойдёт по секциям только когда события приходят в разные дайджесты (резолютивка сегодня, мотивировка через 14+ дней) — в этом случае каждая секция получает «свой» прогон.

ВАЖНО про 3.5 и 3.6: то же правило — если в текущем дайджесте у дела есть И поле «ИТОГ» из «ВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.», И «МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ» — выводи ТОЛЬКО в 3.6, в 3.5 не дублируй. В разных прогонах дело распределяется по своим секциям естественным образом.

6. ⚖️🔬 <b>КАССАЦИЯ</b> — большой блок, выводится только если есть данные в секциях «НОВЫЕ ДЕЛА КАССАЦИИ» или «КАССАЦИОННЫЕ СОБЫТИЯ» в «Данные» ниже. Между этим большим блоком и предыдущим (⚖️ АПЕЛЛЯЦИЯ) — одна пустая строка, без «⸻». Внутри блока:
   6.1. 📥 <b>Новые касс. дела (N):</b> — дело впервые видно через 7kas (мы пропустили 1-ю инст./апел.). Источник — секция «НОВЫЕ ДЕЛА КАССАЦИИ» в данных. ТРИ строки на дело, между делами пустая строка, внутри одного дела пустых строк НЕТ. КРИТИЧНО: заголовок строки 1 — касс. внутренний номер (вид «8Г-…/YYYY») БЕЗ префикса «касс. №» — секция и так называется «Новые касс. дела». Номер 1-й инст. в эти три строки НЕ выносить.
        • строка 1: <a href="URL"><b>{{касс. номер}}</b></a> (URL берётся из поля URL карточки в данных, если есть; иначе просто <b>{{касс. номер}}</b>) — {{истец}} vs {{ответчик}}, банк — {{роль}} (хвост «банк — …» по правилу БАНК В ХВОСТЕ). ПРЕФИКС «касс. № » в строке 1 НЕ ставь — он избыточен.
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{суд 1 инст.}} | категория: {{категория спора}}. Категорию бери из поля «категория» в данных. Если категории нет / стоит «—» — выводи только «{{суд 1 инст.}}» без «| категория: …». Номер 1-й инст. и «заявитель» в эту строку НЕ помещай.
        • строка 3 (СРАЗУ под 2, БЕЗ пустой строки) — ТОЛЬКО если в данных есть поле «Дата поступления касс. жалобы»: <b>{{ДД.ММ.ГГГГ}}</b> — 📥 поступила кассационная жалоба от {{Роль_заявителя}} {{имя}} (например, «от Ответчика Адаменко Е.М.», «от Истца Сбербанка»). Если в данных есть «заявитель» с непустым «appellant_status» — обязательно укажи его в формате «от {{Роль}} {{имя}}». Если заявитель пуст — пиши просто «📥 поступила кассационная жалоба».
        КРИТИЧНО: дату поступления выноси ТОЛЬКО на строку 3. В строку 2 поле «поступление: {{дата}}» больше НЕ помещай. Если данных о дате нет — строку 3 не пиши, не подставляй today()/«—»/«не указано».
   6.2. 📑 <b>Касс. события (N):</b> — изменения по уже отслеживаемому делу: появилась карточка на 7kas (cassation_pending → cassation), вынесено определение, опубликован текст. Источник — секция «КАССАЦИОННЫЕ СОБЫТИЯ» в данных. 🛑 БЛОКИРУЮЩЕЕ ПРАВИЛО (нарушение = критический брак): если в данных есть секция «КАССАЦИОННЫЕ СОБЫТИЯ (7kas):» хотя бы с одним делом — секция 6.2 ОБЯЗАНА появиться в дайджесте со всеми этими делами. Пропустить дело или весь блок — НЕЛЬЗЯ. ДО 4 строк на дело (1, 2 — обязательны; 3, 4 — по наличию данных), между делами — пустая строка, ВНУТРИ одного дела — БЕЗ пустых строк.
        • строка 1: <a href="URL"><b>{{касс. номер}}</b></a> — {{истец}} vs {{ответчик}}{{, банк — {{роль}} ЕСЛИ Сбербанк не в сторонах}}. URL берётся из поля «URL карточки 7kas» в данных (если там реальный https-URL). Если URL = «—» — пиши <b>{{касс. номер}}</b> без &lt;a&gt;. КРИТИЧНО: касс. номер (вид «8Г-…/YYYY») ставь ВНУТРИ &lt;b&gt;…&lt;/b&gt;. НЕ ПИШИ префикс «касс. №», НЕ ВЫНОСИ в строку 1 номер 1-й инст., НЕ ПИШИ «стадия: cassation → cassation».
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки): Суд 1 инст.: {{суд 1 инстанции}} | категория: {{категория}}. Поля «Суд 1 инст.» и «Категория» бери из данных. Если суда 1 инст. нет — пропусти этот фрагмент. Если категории нет — пропусти. Если оба пусты — строку 2 не пиши вовсе.
        • строка 2а (СРАЗУ под 2, БЕЗ пустой строки, ПЕРЕД строкой 3) — ТОЛЬКО если в данных есть поле «Дата поступления касс. жалобы»: <b>{{ДД.ММ.ГГГГ}}</b> — 📥 поступила кассационная жалоба от {{Роль_заявителя}} {{имя}} — ровно как в 6.1. Это первая линковка карточки на 7kas (дело доехало до КСОЮ и получило 8Г-номер). Python отдаёт это поле ТОЛЬКО на таком событии, поэтому: есть поле — строку пиши обязательно, нет — не пиши и НЕ подставляй другие даты. Если при этом «Метка стадии» = «📥 Принято к производству» — строку 4 (Итог) НЕ пиши: получилось бы два «📥» подряд об одном и том же.
        • строка 3 (СРАЗУ под предыдущей, БЕЗ пустой строки) — ТОЛЬКО если в данных есть «Дата заседания: ДД.ММ.ГГГГ [ЧЧ:ММ]» И ПРИ ЭТОМ НЕТ поля «Метка исхода (готовая для «Итог»)» (т.е. дело ещё в производстве, не решено): 📅 Назначено судебное заседание на <b>{{ДД.ММ.ГГГГ в ЧЧ:ММ}}</b>. Если в данных только дата без времени — «на <b>{{ДД.ММ.ГГГГ}}</b>» без «в ЧЧ:ММ». КРИТИЧНО: фраза начинается с «Назначено судебное заседание на», старый формат «📅 Заседание: …» НЕ использовать.
          🛑 ЕСЛИ В ДАННЫХ ЕСТЬ «Метка исхода» — строку 3 «📅 Назначено судебное заседание…» НЕ ПИШИ ВООБЩЕ. Заседание уже состоялось, его исход важнее даты, а формулировка «Назначено» в прошедшем времени обманывает (вводит юриста в заблуждение, что заседание ещё впереди). Дату заседания не дублируй: она и так встроена в Итог через «Дата вынесения опред.» и саму метку исхода.
        • строка 3а (СРАЗУ под предыдущей, БЕЗ пустой строки) — ТОЛЬКО если в данных есть «БЕЗ ДВИЖЕНИЯ: срок устранения недостатков до ДД.ММ.ГГГГ»: ⏸ жалоба оставлена без движения — срок устранения недостатков до <b>{{ДД.ММ.ГГГГ}}</b>. При этой строке маркер стадии «📥 Принято к производству» в Итог не пиши (жалоба очевидно принята, раз суд дал срок).
        • строка 4 (СРАЗУ под предыдущей строкой, БЕЗ пустой строки) — ТОЛЬКО если в данных есть «Метка исхода (готовая для «Итог»)» ИЛИ «Метка стадии (готовая для «Итог»)»: <b>Итог:</b> {{ДОСЛОВНО метка с эмодзи}}{{; подана {{Ролью}} {{имя}} если есть «Заявитель» с непустым «appellant_status»}}{{; ПРИЧИНА если есть «Причина (для «Итог»)»}}. Приоритет: «Метка исхода» > «Метка стадии». Роль заявителя — в творительном падеже (Ответчиком / Истцом / Иным лицом / Третьим лицом). «Причина» добавляется в конец через `; ` (точка с запятой + пробел) ДОСЛОВНО — это конкретный текст из карточки 7kas (например, «поданы лицом, не имеющим права на обращение в суд кассационной инстанции»). Если ни одной метки нет — строку 4 НЕ пиши.
          🛑 ОБЯЗАТЕЛЬНО: если в данных есть «Метка исхода» (любая — возврат / прекращение / отмена / изменение / без изменения / удовлетворение) — строку 4 (Итог) ВЫВОДИ ВСЕГДА. Пропустить её = критический брак. Подавлять строку 4 можно ТОЛЬКО при «Метке стадии» = «📥 Принято к производству» при одновременном наличии строки 3 ИЛИ строки 2а (см. исключение ниже).
          🛑 ИСКЛЮЧЕНИЕ (подавление избыточного маркера стадии): если выведена строка 3 (есть «Дата заседания» в данных И НЕТ «Метки исхода») ИЛИ строка 2а (есть «Дата поступления касс. жалобы») И «Метка стадии» = «📥 Принято к производству» — строку 4 (Итог) НЕ пиши. «Принято к производству» — это маркер стадии, а не финальный исход; назначенное заседание уже сообщает юристу, что жалоба в производстве.
        Если в данных есть «МОТИВИРОВОЧНАЯ ЧАСТЬ ОПРЕДЕЛЕНИЯ» — добавь ещё одну строку (5) сразу под 4: <b>Почему:</b> 3-4 КОРОТКИХ предложения по ПРАВИЛАМ МОТИВИРОВОЧНЫХ СЕКЦИЙ.
        Перевод исхода/стадии: НЕ переводи сам поля «Изучение жалобы (raw)»/«ИСХОД (raw enum)» — Python уже подготовил готовую метку, её и используй ДОСЛОВНО.
        🏦 в начале строки 1 ставь ТОЛЬКО если в данных явно `банк_заявитель=True` (Сбербанк подал кассационную жалобу). При `банк_заявитель=False` — 🏦 НЕ ставить.
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (дело с финальным исходом + причина):
            <a href="https://7kas.sudrf.ru/..."><b>8Г-6846/2026</b></a> — Сбербанк vs Чернов В.В.
            Суд 1 инст.: Мегионский гор. суд | категория: Кредитный договор
            <b>Итог:</b> 🔚 Жалоба возвращена; подана Ответчиком Чернова В.В.; поданы лицом, не имеющим права на обращение в суд кассационной инстанции
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (дело с датой заседания — Итог подавлен по правилу):
            <a href="https://7kas.sudrf.ru/..."><b>8Г-6851/2026</b></a> — Сбербанк vs Чернов В.В.
            Суд 1 инст.: Сургутский гор. суд | категория: Кредитный договор
            📅 Назначено судебное заседание на <b>02.06.2026 в 17:00</b>
        ✅ ПРАВИЛЬНЫЙ ПРИМЕР (дело с готовым исходом — «Назначено» НЕ выводится, заседание уже состоялось):
            <a href="https://7kas.sudrf.ru/..."><b>8Г-5540/2026</b></a> — Сбербанк vs Администрация г. Ханты-Мансийска
            Суд 1 инст.: Ханты-Мансийский рай. суд | категория: об ответственности наследников
            <b>Итог:</b> Оставлено без изменения; подана Ответчиком МТУ Росимущества
        ❌ НЕПРАВИЛЬНО (общая метка «🛑 Прекращено / отозвано / возвращено» вместо конкретной — Python всегда расщепляет):
            <b>Итог:</b> 🛑 Прекращено / отозвано / возвращено
        ❌ НЕПРАВИЛЬНО (дублирующий «📥 Принято к производству» при наличии назначенной даты заседания):
            📅 Назначено судебное заседание на <b>02.06.2026 в 17:00</b>
            <b>Итог:</b> 📥 Принято к производству; подана Ответчиком Чернова В.В.
        ❌ НЕПРАВИЛЬНО («📅 Назначено» при наличии «Метки исхода» — обманывает юриста: дата в прошлом, а формулировка как у будущего события; Итог исчезает):
            📅 Назначено судебное заседание на <b>20.05.2026 в 09:01</b>
            (нет строки «Итог:» — но в данных была «Метка исхода: Оставлено без изменения»)
        ❌ НЕПРАВИЛЬНО (старый формат с номером 1-й инст. в заголовке и префиксом «касс. №»):
            <b>2-946/2025</b> — касс. № <b>8Г-6851/2026</b>
            Сбербанк vs Чернов В.В. | категория: Кредитный договор, банк — истец
        ❌ НЕПРАВИЛЬНО (выкинута часть строк или весь блок при наличии данных в источнике): любой пропуск дела из «КАССАЦИОННЫЕ СОБЫТИЯ» — критический брак.

7. 📌 Финальную плашку «В производстве: всего N (1 инст.: X | апел.: Y | касс.: Z)» и ссылку «📊 Дашборд» НЕ пиши — Python сам их допишет в самом конце детерминированно (точные числа total_active* у него уже есть, гарантированно совпадут с дашбордом). Если случайно вывел эти строки — они будут вырезаны и заменены свежими.

ОФОРМЛЕНИЕ: без маркеров списка («• », «- »); названия больших блоков и секций — <b>жирным</b>; номера дел — <b>жирным</b> внутри ссылок. РАЗДЕЛИТЕЛИ И ПУСТЫЕ СТРОКИ (обязательны, без них границы теряются):
(а) перед заголовком каждой подсекции 📥/📅/⚖️/📄/🔁/📨/⚠ ВНУТРИ одного большого блока — отдельная строка-разделитель «⸻» (ТОЛЬКО этот символ, без HTML-тегов и пробелов вокруг), окружённая пустыми строками: пустая строка → ⸻ → пустая строка → заголовок секции. Перед самой первой подсекцией большого блока (сразу после <b>🏛 ПЕРВАЯ ИНСТАНЦИЯ</b> или <b>⚖️ АПЕЛЛЯЦИЯ</b>) разделитель НЕ ставь — там и так понятно, где начало; ПОСЛЕ заголовка подсекции (📥 Новые иски (N): / 📅 Изменения (N): / 📄 Опубликованные… / 🔁 Отложенные… и т.п.) — ровно ОДНА пустая строка, потом первое дело;
(б) между РАЗНЫМИ делами в одной подсекции — ровно одна пустая строка, даже в однострочных подсекциях 3.3/3.5/5.1/5.4 (без «⸻»);
(б1) ВНУТРИ ОДНОГО ДЕЛА (когда у дела две или три строки — секции 3.2, 3.6, 5.1, 5.2, 5.4, 5.5) пустая строка МЕЖДУ строками одного дела — ЗАПРЕЩЕНА. Все строки одного дела идут подряд, плотным блоком. Пустая строка появляется ТОЛЬКО когда начинается следующее дело;
(в) между большими блоками (🏛 ПЕРВАЯ ИНСТАНЦИЯ → ⚖️ АПЕЛЛЯЦИЯ) — ровно одна пустая строка, без «⸻» (граница и так заметна по жирному заголовку большого блока);
(в1) 🛑 ОБЯЗАТЕЛЬНЫЙ ПОРЯДОК БОЛЬШИХ БЛОКОВ (КРИТИЧНО, нарушение = брак): сначала <b>🏛 ПЕРВАЯ ИНСТАНЦИЯ</b>, потом <b>⚖️ АПЕЛЛЯЦИЯ</b>, потом <b>⚖️🔬 КАССАЦИЯ</b>. Никогда не меняй этот порядок, даже если по апелляции данных больше — юрист первым делом смотрит свои 1-й инст. дела, а не апелляционные;
(г) после <b>🏛 ПЕРВАЯ ИНСТАНЦИЯ</b> и после <b>⚖️ АПЕЛЛЯЦИЯ</b> — ровно одна пустая строка перед первой подсекцией (отступ для дыхания).

СТИЛЬ: кратко, по-деловому, на русском. Без вступлений. Не дублируй информацию между секциями (за исключением 5.4↔5.5, см. выше).

ЛИМИТ: примерно {config.DIGEST_CHAR_LIMIT} символов — это БОЛЬШОЙ запас, фактический дайджест обычно в 2-3 раза короче. НЕ ЭКОНОМЬ место за счёт пропуска требуемых строк или событий: НИКОГДА не сворачивай дело из 3.1/5.1/6.1 в одну строку, если требуется 2-3; НИКОГДА не выкидывай события из 3.2 (включая «📄 мотивированное решение изготовлено …»), 3.5, 3.6, 5.x, 6.x — если событие есть в данных, оно ОБЯЗАНО появиться в дайджесте. Сокращать допустимо ТОЛЬКО мотивировочные секции 3.6/5.5 (тексты «Почему: …») и ТОЛЬКО при реальном переполнении лимита; всё остальное — формат, строки 2-3, заголовки, даты — выводи полностью. Секцию 📅 «Изменения» (как в 1-й инст. 3.2, так и в апелляции 5.2) — НЕ выкидывать никогда. Ссылка на дашборд — ВСЕГДА в конце.

ВАЖНО: в разделе «Данные» ниже перечислены только ИЗМЕНЕНИЯ за сегодня, а не все дела. Общие числа берутся ИСКЛЮЧИТЕЛЬНО из пункта 6 выше.

Данные:
{chr(10).join(context_parts)}"""

    if config.LLM_PROVIDER in ("gigachat", "openrouter"):
        if config.LLM_PROVIDER == "gigachat":
            log.info(f"LLM: GigaChat (model={config.GIGACHAT_MODEL}, scope={config.GIGACHAT_SCOPE})")
            text = llm._call_gigachat(prompt)
        else:
            log.info(f"LLM: OpenRouter (model={llm._resolve_openrouter_model()})")
            text = llm._call_openrouter_digest(prompt)
        if not text:
            return generate_template_digest(
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
                total_active_cassation=total_active_cassation,
                total_active_bank=total_active_bank,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            )
        text = _validate_digest_new_sections(text, fi_new_cases, new_cases)
        text = _ensure_appeal_new_case_full_layout(text, new_cases)
        text = _warn_misplaced_appeal_cases(text)
        text = _renumber_section_headers(text)
        text = _purge_3_6_without_act_text(text, fi_changes or [])
        text = _drop_zero_count_sections(text)
        # Сводку (📋) полностью переписываем по факту вывода — раньше
        # _recount_summary_line редактировал только если LLM использовал
        # ровно <i>1 инст.:</i>/<i>Апелл.:</i>/<i>Касс.:</i> обёртки.
        # Теперь любая «свободная» сводка от LLM вырезается целиком и
        # заменяется детерминированной (см. _replace_summary_block).
        text = _replace_summary_block(text)
        # Срезаем «5.1.», «6.2.» и т.п. префиксы из заголовков подсекций —
        # юрист просил без нумерации. Идём после _renumber/_recount, чтобы
        # счётчики (N) пересчитались до удаления префикса. См. _strip_section_numbering.
        text = _strip_section_numbering(text)
        # Срезаем «X → Y → Z» в строках «категория: …» — LLM иногда
        # подставляет родительскую категорию вопреки промпту.
        text = _shorten_categories_in_html(text)
        # Гарантируем финальную плашку «📌 В производстве …» и ссылку
        # «📊 Дашборд». LLM иногда упирается в max_tokens и обрезается
        # перед ними, а считать total_active*-цифры он не должен (мы
        # передаём их сюда напрямую).
        text = _ensure_footer(
            text,
            total_active=total_active,
            total_active_fi=total_active_fi,
            total_active_appeal=total_active_appeal,
            total_active_cassation=total_active_cassation,
            total_active_bank=total_active_bank,
        )
        text = _normalize_section_spacing(text)
        text = _wrap_all_bare_case_numbers(text, url_by_num)
        # HTML не обрезаем — см. generate_template_digest: дашборд показывает
        # дайджест целиком, а send_telegram сам режет его на сообщения.
        return _close_open_tags(text)

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": config.ANTHROPIC_API_KEY,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01",
            },
            # Низкая температура: дайджест требует дословного цитирования
            # ИТОГа и категории — креативность модели тут вредит. Для моделей
            # нового поколения (opus 4.8/sonnet 5) temperature удалён из API —
            # пейлоад собирает llm._claude_payload (adaptive-мышление + effort).
            json=llm._claude_payload(
                max_tokens=4096, temperature=0.2,
                messages=[{"role": "user", "content": prompt}],
            ),
            timeout=llm._claude_timeout(60),
        )
        r.raise_for_status()
        data = r.json()
        text = "".join(
            block["text"] for block in data.get("content", [])
            if block.get("type") == "text"
        )
        text = text.strip()
        # Страховка: модель иногда оборачивает HTML в Markdown-кодовый блок
        # (```html ... ```), несмотря на инструкцию в промпте. Срезаем.
        if text.startswith("```"):
            first_nl = text.find("\n")
            if first_nl != -1:
                text = text[first_nl + 1:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        if not text:
            return generate_template_digest(
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
                total_active_cassation=total_active_cassation,
                total_active_bank=total_active_bank,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            )
        text = _validate_digest_new_sections(text, fi_new_cases, new_cases)
        text = _ensure_appeal_new_case_full_layout(text, new_cases)
        text = _warn_misplaced_appeal_cases(text)
        text = _renumber_section_headers(text)
        text = _purge_3_6_without_act_text(text, fi_changes or [])
        text = _drop_zero_count_sections(text)
        # Сводку (📋) полностью переписываем по факту вывода — раньше
        # _recount_summary_line редактировал только если LLM использовал
        # ровно <i>1 инст.:</i>/<i>Апелл.:</i>/<i>Касс.:</i> обёртки.
        # Теперь любая «свободная» сводка от LLM вырезается целиком и
        # заменяется детерминированной (см. _replace_summary_block).
        text = _replace_summary_block(text)
        # Срезаем «5.1.», «6.2.» и т.п. префиксы из заголовков подсекций —
        # юрист просил без нумерации. Идём после _renumber/_recount, чтобы
        # счётчики (N) пересчитались до удаления префикса. См. _strip_section_numbering.
        text = _strip_section_numbering(text)
        # Срезаем «X → Y → Z» в строках «категория: …» — LLM иногда
        # подставляет родительскую категорию вопреки промпту.
        text = _shorten_categories_in_html(text)
        # Гарантируем финальную плашку «📌 В производстве …» и ссылку
        # «📊 Дашборд». LLM иногда упирается в max_tokens и обрезается
        # перед ними, а считать total_active*-цифры он не должен (мы
        # передаём их сюда напрямую).
        text = _ensure_footer(
            text,
            total_active=total_active,
            total_active_fi=total_active_fi,
            total_active_appeal=total_active_appeal,
            total_active_cassation=total_active_cassation,
            total_active_bank=total_active_bank,
        )
        text = _normalize_section_spacing(text)
        text = _wrap_all_bare_case_numbers(text, url_by_num)
        # HTML не обрезаем — дашборд показывает дайджест целиком, а send_telegram
        # сам режет его на сообщения по лимиту Telegram через split_message.
        return _close_open_tags(text)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        body = (e.response.text or "")[:500] if e.response is not None else ""
        log.error(f"Claude API HTTP {status}: {body}")
        return generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
            total_active_bank=total_active_bank,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )
    except requests.RequestException as e:
        log.error(f"Claude API сетевая ошибка: {e}")
        return generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
            total_active_bank=total_active_bank,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        log.error(f"Claude API неожиданный ответ: {e}")
        return generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
            total_active_bank=total_active_bank,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )


# ── Пост-процессор: страховка от LLM-галлюцинаций в «новых» секциях ──────────
