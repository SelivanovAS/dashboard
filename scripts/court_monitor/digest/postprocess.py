# -*- coding: utf-8 -*-
"""Обработка HTML дайджеста: счётчики и заголовки секций,
обрезка до лимита Telegram
(truncate_html_message), починка незакрытых тегов.
"""

from __future__ import annotations

import re

from court_monitor import config

_DIGEST_CASE_LINK_RE = re.compile(r'<a[^>]*>\s*<b>\s*([^<]+?)\s*</b>\s*</a>')

# Старые сохранённые дайджесты могут содержать номера перед заголовками.
# Линтер и счётчики читают их вместе с текущим HTML шаблона.
_SUBSECTION_NUM_PREFIX = r'(?:\d+(?:\.\d+[a-z]?)*\.\s+)?'

# Линия считается заголовком подсекции/блока, если начинается с одного из
# этих эмодзи + <b>. Включает «🏦 ИСКИ БАНКА (N)» шаблонного рендера — без него
# линтер не видел границы секции «📑 Касс. события» и считал банк-строки
# чужим счётчиком (инцидент 12.08.2026: «заявлено 1, по факту дел 22»).
# Нужно только для поиска границы секции — не обязано
# быть полным, главное — не ловить строки-дела.
_DIGEST_HEADER_RE = re.compile(
    # `⚖️🔬` СТРОГО до `⚖️` — иначе regex матчит `⚖️` в строке
    # «⚖️🔬 <b>КАССАЦИЯ</b>» и спотыкается на следующем `🔬`, не дойдя
    # до `<b>`. Тогда заголовок большого блока КАССАЦИИ не распознаётся,
    # и счётчик относит его к предыдущей подсекции. Порядок альтернатив
    # важен для распознавания границ.
    #
    # После `<b>` обязательно русская/латинская буква — иначе строка
    # события вида «📅 <b>22.05.2026 10:12</b> — передача дела судье»
    # ловится как заголовок подсекции и обрывает счётчик `_count_digest_subsections`
    # на первом деле (видно было в сводке «Апелл.: 1 изменение» при реальных 4).
    r'^\s*' + _SUBSECTION_NUM_PREFIX
    + r'(?:⚖️🔬|📥|📅|📨|🔄|⚠|🔁|⚖️|📄|📑|🏛|🏦|🔀|📌|📊|📋)\s*<b>\s*[А-ЯA-Zа-яa-z]'
)

# Номер дела вида «2-216/2026», «М-449/2026», «33-3479/2026», «9-12/2025».
# Счётчики и линтер учитывают также номера без обёртки <a href>.
_BARE_CASE_NUMBER_RE = re.compile(
    r'(?<![\w/-])([0-9A-Za-zА-Яа-яЁё]+-\d+/\d{4})(?![\w/-])'
)


def _line_has_case_number(line: str) -> bool:
    """Строка содержит номер дела (в обёртке `<a href><b>num</b></a>` или голый).

    Используется счётчиками подсекций: пересчитываем `(N)` по числу строк
    с номером, а не только по строкам с обёрнутой ссылкой. Голый номер
    появляется, когда LLM забыл обернуть; такие строки всё равно нужно
    учитывать как «дело».
    """
    if _DIGEST_CASE_LINK_RE.search(line):
        return True
    return bool(_BARE_CASE_NUMBER_RE.search(line))


# Подзаголовки подсекций со счётчиком (N): для счётчиков готового HTML.
# Группа 2 содержит N. Префикс распознаёт также «5.1.»/«6.2.» в старых
# сохранённых дайджестах.
_SUBSECTION_HEADERS_WITH_COUNT = [
    # «📅 Изменения» используется в ОБЕИХ инстанциях (3.2 «1 инст.», 5.2
    # «апелляция» после объединения старых 5.2 «Отложенные» + 5.3
    # «Назначенные»). Один регекс покрывает оба места.
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'📅\s*<b>\s*Изменения\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Изменения"),
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'📨\s*<b>\s*Поданы апелляционные жалобы\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "1 инст./Апел. жалобы"),
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'📨\s*<b>\s*Кассационные события\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "1 инст./Кассация"),
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'⚖️\s*<b>\s*Вынесенные решения\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "1 инст./Решения"),
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'📄\s*<b>\s*Опубликованные тексты решений\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "1 инст./Тексты решений"),
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'⚖️\s*<b>\s*Вынесенные акты\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Апел./Акты"),
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'📄\s*<b>\s*Опубликованные тексты актов\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Апел./Тексты актов"),
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'⚠\s*<b>\s*Переход к правилам 1-й инстанции\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Апел./Переход к правилам 1 инст."),
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'🔀\s*<b>\s*Перешли в апелляцию\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Перешли в апелляцию"),
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'📥\s*<b>\s*Новые касс\. дела\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Касс./Новые дела"),
    (re.compile(r'^(\s*' + _SUBSECTION_NUM_PREFIX + r'📑\s*<b>\s*Касс\. события\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Касс./События"),
]


def _count_digest_subsections(html: str) -> list[tuple[str, str, int]]:
    """Парсит HTML дайджеста и возвращает [(block, label, count), ...]
    по каждой найденной подсекции с (N).

    block: "fi" / "appeal" / "bridge" / "cass" — нужен только для
    дальнейшей группировки потребителем (свод по инстанциям и т.п.).
    label: «1 инст./Изменения», «Апел./Отложено», «Перешли в апелляцию»,
    «Касс./События» и т.п. — берётся из `_SUBSECTION_HEADERS_WITH_COUNT`,
    либо синтезируется для «Новые иски» / «Новые дела» (у них другие
    регексы).
    count: число строк-дел между этим заголовком и следующим заголовком
    секции/подсекции, независимо от сохранённого значения (N).
    """
    lines = html.split("\n")
    sections: list[tuple[str, str, int]] = []

    n = len(lines)
    i = 0
    while i < n:
        ln = lines[i]
        matched = False
        for pat, label in _SUBSECTION_HEADERS_WITH_COUNT:
            m = pat.match(ln)
            if not m:
                continue
            j = i + 1
            count = 0
            while j < n and not _DIGEST_HEADER_RE.match(lines[j]):
                if _line_has_case_number(lines[j]):
                    count += 1
                j += 1
            block = (
                "fi" if label.startswith("1 инст.") else
                "bridge" if label == "Перешли в апелляцию" else
                "cass" if label.startswith("Касс.") else
                "appeal"
            )
            sections.append((block, label, count))
            i = j
            matched = True
            break
        # Также «Новые иски» / «Новые дела» — у них специальный формат.
        if not matched:
            m_fi = re.match(
                r'^(\s*' + _SUBSECTION_NUM_PREFIX
                + r'📥\s*<b>\s*Новые иски\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$',
                ln,
            )
            m_ap = re.match(
                r'^(\s*' + _SUBSECTION_NUM_PREFIX
                + r'📥\s*<b>\s*Новые дела\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$',
                ln,
            )
            m = m_fi or m_ap
            if m:
                j = i + 1
                count = 0
                while j < n and not _DIGEST_HEADER_RE.match(lines[j]):
                    if _line_has_case_number(lines[j]):
                        count += 1
                    j += 1
                if m_fi:
                    sections.append(("fi", "1 инст./Новые иски", count))
                else:
                    sections.append(("appeal", "Апел./Новые дела", count))
                i = j
                matched = True

        if not matched:
            i += 1

    return sections


# Лейблы подсекций, которые в шапке считаются как «новые» / «переходы»
# (а не общие «изменения»). Остальные лейблы из `_SUBSECTION_HEADERS_WITH_COUNT`
# и «Касс./Новые дела» относятся к одной из этих категорий или к «изменениям».
_DIGEST_SUMMARY_NEW_LABELS = frozenset({
    "1 инст./Новые иски", "Апел./Новые дела", "Касс./Новые дела",
})
_DIGEST_SUMMARY_STAGE_LABELS = frozenset({"Перешли в апелляцию"})


def summarize_digest_counters(html: str) -> dict[str, int]:
    """Возвращает {'new': N, 'changes': N, 'stages': N} по фактически
    выведенным в дайджесте подсекциям.

    Используется в шапке фронта («Дайджест / dd.mm.yyyy / 📋 Изменений: N»)
    и в body web-push, чтобы не показывать сырое число change-объектов до
    дедупа (которое в LLM-сборке режется правилами 3.2↔3.5, 3.3 поглощает
    смену статуса и т.п.).
    """
    sections = _count_digest_subsections(html)
    new_n = sum(c for _b, lbl, c in sections if lbl in _DIGEST_SUMMARY_NEW_LABELS)
    stages_n = sum(c for _b, lbl, c in sections if lbl in _DIGEST_SUMMARY_STAGE_LABELS)
    changes_n = sum(
        c for _b, lbl, c in sections
        if lbl not in _DIGEST_SUMMARY_NEW_LABELS
        and lbl not in _DIGEST_SUMMARY_STAGE_LABELS
    )
    return {"new": new_n, "changes": changes_n, "stages": stages_n}


def _close_open_tags(html: str) -> str:
    """Закрыть все незакрытые HTML-теги (b, i, a) в конце строки."""
    stack: list[str] = []
    for m in re.finditer(r'<(/?)([bia])\b[^>]*>', html):
        is_close, tag_name = m.group(1), m.group(2)
        if is_close:
            if stack and stack[-1] == tag_name:
                stack.pop()
        else:
            stack.append(tag_name)
    # Закрываем оставшиеся теги в обратном порядке
    for tag in reversed(stack):
        html += f"</{tag}>"
    return html


def _strip_orphan_close_tags(html: str) -> str:
    """Убрать закрывающие теги без соответствующих открывающих."""
    stack: list[str] = []
    result_parts: list[str] = []
    last_end = 0
    for m in re.finditer(r'<(/?)([bia])\b[^>]*>', html):
        is_close, tag_name = m.group(1), m.group(2)
        if is_close:
            if stack and stack[-1] == tag_name:
                stack.pop()
                result_parts.append(html[last_end:m.end()])
                last_end = m.end()
            else:
                # Сиротский закрывающий тег — пропускаем
                result_parts.append(html[last_end:m.start()])
                last_end = m.end()
        else:
            stack.append(tag_name)
            result_parts.append(html[last_end:m.end()])
            last_end = m.end()
    result_parts.append(html[last_end:])
    return "".join(result_parts)


_TRUNCATED_SUFFIX = "\n\n…<i>сообщение обрезано</i>"


def truncate_html_message(text: str, limit: int = 4096, *,
                          suffix: str = _TRUNCATED_SUFFIX) -> str:
    """
    Обрезать HTML-сообщение до лимита Telegram, не ломая теги.
    Добавляет `suffix` в конце, если пришлось резать (по умолчанию —
    «…сообщение обрезано»).
    """
    if len(text) <= limit:
        return _close_open_tags(text)

    # Обрезаем с запасом под suffix и закрытие тегов.
    cut = text[:limit - len(suffix) - 20]

    # Убираем незакрытые теги в конце
    last_close = cut.rfind(">")
    last_open = cut.rfind("<")
    if last_open > last_close:
        cut = cut[:last_open]

    # Обрезаем до последнего перевода строки для чистоты
    last_nl = cut.rfind("\n")
    if last_nl > len(cut) - 200:
        cut = cut[:last_nl]

    cut = cut.rstrip() + suffix
    cut = _close_open_tags(cut)

    return cut


def truncate_digest_for_telegram(html: str, limit: int | None = None) -> str:
    """Компактная версия дайджеста для Telegram.

    Полный HTML уходит на дашборд (`save_last_digest`) без обрезки, а в
    Telegram шлём короткую версию: если дайджест не влезает в `limit`
    (по умолчанию 2 сообщения = 2×TELEGRAM_MSG_LIMIT), режем и в конце
    ставим заметку + рабочую ссылку на дашборд. Обычный
    `truncate_html_message` срезал бы финальный футер с этой ссылкой
    вместе с хвостом дел, оставив юриста без входа в полный текст.
    """
    if limit is None:
        # Не ровно 2×4096: split_message режет по границам \n\n с запасом ~50,
        # поэтому «под завязку» 8192 разложились бы на 3 сообщения (хвост-
        # огрызок). Берём ~7600 — надёжно умещается в 2 сообщения.
        limit = config.TELEGRAM_MSG_LIMIT * 2 - 600
    if len(html) <= limit:
        return _close_open_tags(html)
    suffix = (
        "\n\n…<i>дайджест длинный — здесь показаны не все дела.</i>\n"
        f'<a href="{config.DASHBOARD_URL}">📊 Полный дайджест в дашборде</a>'
    )
    return truncate_html_message(html, limit, suffix=suffix)
