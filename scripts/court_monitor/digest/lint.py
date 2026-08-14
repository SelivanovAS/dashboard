# -*- coding: utf-8 -*-
"""Программный линтер готового дайджеста (без LLM).

Детерминированные проверки HTML после сборки: все номера дел из контекста
присутствуют и обёрнуты в `<a><b>номер</b></a>`, счётчики `(N)` в заголовках
подсекций соответствуют числу дел под ними, теги сбалансированы, футер и
ссылка на дашборд на месте, лимит Telegram не превышен.

Линтер НИЧЕГО не блокирует: дайджест уже отправлен к моменту вызова.
При аномалии вызывающая сторона (runs.main_json / main_replay_last) шлёт
сервисный 🩺-алерт в Telegram — по образцу детектора здоровья парсеров.
Kill-switch: env `DIGEST_LINT=0` (см. config.DIGEST_LINT).

Отличие от `_validate_polished_html` (llm.py): тот валидирует ПРАВКУ
LLM-полировщика против черновика и работает только при DIGEST_POLISH=1;
линтер же проверяет сам черновик против ИСХОДНОГО контекста данных и
работает всегда.
"""

from __future__ import annotations

import re

from court_monitor import config
from court_monitor.config import log
from court_monitor.digest.postprocess import (
    _DIGEST_HEADER_RE, _close_open_tags, _line_has_case_number,
)
from court_monitor.digest.template import split_bank_intake_fold
from court_monitor.textutil import _bare_case_number

# Контракт фронта и attach_act_analyses: номер дела в <a ...><b>номер</b></a>.
_ANCHOR_RE = re.compile(r"<a[^>]*><b>([^<]+)</b></a>")

# Счётчик в заголовке подсекции: «📥 <b>Новые иски (3):</b>».
_HEADER_COUNT_RE = re.compile(r"<b>[^<]*\((\d+)\):</b>")

# Теги, которых Telegram parse_mode=HTML не понимает. Шаблон их не эмитит —
# проверка ловит регрессии рендера (дубль _FORBIDDEN_TAGS_RE из llm.py,
# намеренно локальный: линтер не должен тянуть LLM-модуль).
_FORBIDDEN_TAGS_RE = re.compile(
    r"<\s*(p|ul|ol|li|h[1-6]|br|div|span|strong|em|table|tr|td|th)\b",
    re.IGNORECASE,
)

# Маркер обрезки (truncate_html_message). В боевой генерации больше не
# ставится — HTML не обрезаем; оставлен как страховочный сигнал линтеру.
_TRUNCATED_MARKER = "сообщение обрезано"


def _expected_number_alternatives(
    *,
    new_cases: list[dict] | None,
    changes: list[dict] | None,
    fi_new_cases: list[dict] | None,
    fi_changes: list[dict] | None,
    cass_changes: list[dict] | None,
    cass_discovered: list[dict] | None,
) -> list[set[str]]:
    """Список «альтернативных наборов» номеров: дело представлено в HTML,
    если найден ХОТЯ БЫ ОДИН номер из его набора. Для кассации шаблон
    рендерит касс. внутренний номер (8Г-…) вместо номера 1-й инст. —
    принимаем оба."""
    expected: list[set[str]] = []

    def _add(*nums: str) -> None:
        alts = {(n or "").strip() for n in nums} - {""}
        if alts:
            expected.append(alts)

    for c in new_cases or []:
        _add(c.get("Номер дела", ""))
    for c in fi_new_cases or []:
        _add(c.get("id", ""))
    for ch in changes or []:
        _add(ch.get("case", ""))
    # Свёрнутые «заведено N новых исков банка» номеров в HTML не дают —
    # ждать их нельзя (разгон Урала 14.08.2026: иначе дайджест-паводок из
    # 116 строк переехал бы в 🩺-алерт «потерян номер дела» на те же 116
    # строк). Делит список ТОТ ЖЕ хелпер, что и рендер, — два независимых
    # расчёта порога разъехались бы молча.
    _folded_ids = {id(ch) for ch in
                   split_bank_intake_fold([ch for ch in (fi_changes or [])
                                           if ch.get("track")])[1]}
    for ch in fi_changes or []:
        if id(ch) in _folded_ids:
            continue
        _add(ch.get("case", ""))
    for ch in cass_changes or []:
        _add(ch.get("case", ""), ch.get("cassation_internal_number", ""))
    for c in cass_discovered or []:
        cass = c.get("cassation") or {}
        _add(c.get("id", ""), cass.get("case_number", ""))
    return expected


def _check_section_counters(html: str) -> list[str]:
    """Сверить `(N)` каждого заголовка подсекции с числом дел под ним.

    Дело = строка с номером дела (обёрнутым или голым —
    `_line_has_case_number`, тот же критерий, каким postprocess
    пересчитывает `(N)` у LLM-дайджеста). НЕ считаем по отступам:
    формат full-LLM пути не имеет отступов, и счётчик по отступам давал
    ложный алерт «по факту дел 0» (A/B 03.07.2026). Граница секции —
    любая строка-заголовок (_DIGEST_HEADER_RE: и подсекции, и большие
    блоки, и футер «📌 <b>В производстве…»)."""
    problems: list[str] = []
    lines = html.split("\n")
    current_header: str | None = None
    declared = 0
    counted = 0

    def _flush() -> None:
        nonlocal current_header
        if current_header is not None and counted != declared:
            problems.append(
                f"счётчик секции «{current_header}»: заявлено {declared}, "
                f"по факту дел {counted}"
            )
        current_header = None

    for ln in lines:
        if _DIGEST_HEADER_RE.match(ln):
            _flush()
            m = _HEADER_COUNT_RE.search(ln)
            if m:
                declared = int(m.group(1))
                counted = 0
                # Человекочитаемое имя секции без тегов.
                current_header = re.sub(r"<[^>]+>", "", ln).strip()
            continue
        if current_header is not None and _line_has_case_number(ln):
            counted += 1
    _flush()
    return problems


def lint_digest_html(
    html: str,
    *,
    new_cases: list[dict] | None = None,
    changes: list[dict] | None = None,
    fi_new_cases: list[dict] | None = None,
    fi_changes: list[dict] | None = None,
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
    is_empty: bool = False,
) -> list[str]:
    """Проверить готовый HTML дайджеста против контекста данных.

    Возвращает список человекочитаемых проблем (пустой = всё чисто).
    Никогда не бросает исключений: любая внутренняя ошибка — warning в лог
    и пустой список (линтер не имеет права ронять прогон)."""
    try:
        return _lint_digest_html_inner(
            html,
            new_cases=new_cases, changes=changes,
            fi_new_cases=fi_new_cases, fi_changes=fi_changes,
            cass_changes=cass_changes, cass_discovered=cass_discovered,
            is_empty=is_empty,
        )
    except Exception as exc:
        log.warning(f"digest-lint: внутренняя ошибка линтера: {exc}",
                    exc_info=True)
        return []


def _lint_digest_html_inner(
    html: str,
    *,
    new_cases: list[dict] | None,
    changes: list[dict] | None,
    fi_new_cases: list[dict] | None,
    fi_changes: list[dict] | None,
    cass_changes: list[dict] | None,
    cass_discovered: list[dict] | None,
    is_empty: bool,
) -> list[str]:
    has_context = bool(
        new_cases or changes or fi_new_cases
        or fi_changes or cass_changes or cass_discovered
    )
    if is_empty or not has_context:
        # Тихий день: рендерит render_no_changes_digest, структурные
        # проверки секций/номеров неприменимы.
        return []

    problems: list[str] = []
    if not (html or "").strip():
        return ["дайджест пуст при непустом контексте данных"]

    # Готовый HTML дайджеста больше не обрезаем по длине: дашборд показывает
    # его целиком, а send_telegram через split_message сам режет на сообщения.
    # Поэтому проверки «длина > 2×4096» тут нет — это не дефект. Маркер обрезки
    # оставляем как страховку: если какой-то другой путь всё же обрежет текст,
    # линтер это заметит и погасит пономерной шум.
    truncated = _TRUNCATED_MARKER in html
    if truncated:
        problems.append(
            "дайджест обрезан — часть дел не видна "
            "(проверки полноты номеров пропущены)"
        )

    if _close_open_tags(html) != html:
        problems.append("несбалансированные HTML-теги")

    forbidden = _FORBIDDEN_TAGS_RE.search(html)
    if forbidden:
        problems.append(
            f"запрещённый для Telegram тег: {forbidden.group(0)!r}"
        )

    if not truncated:
        if config.DASHBOARD_URL not in html:
            problems.append("пропала ссылка на дашборд")
        if "В производстве" not in html:
            problems.append("пропал футер «📌 В производстве»")

        anchored = {
            _bare_case_number(m.group(1))
            for m in _ANCHOR_RE.finditer(html)
        } - {""}
        for alts in _expected_number_alternatives(
            new_cases=new_cases, changes=changes,
            fi_new_cases=fi_new_cases, fi_changes=fi_changes,
            cass_changes=cass_changes, cass_discovered=cass_discovered,
        ):
            if not any(n in html for n in alts):
                problems.append(
                    "потерян номер дела: " + " / ".join(sorted(alts))
                )
            elif not any(_bare_case_number(n) in anchored for n in alts):
                problems.append(
                    "номер без <a><b>-обёртки: " + " / ".join(sorted(alts))
                )

        problems.extend(_check_section_counters(html))

    return problems
