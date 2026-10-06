# -*- coding: utf-8 -*-
"""Пост-обработка HTML дайджеста: валидация секций «Новые дела» против
галлюцинаций, перенумерация/чистка заголовков, футер, нормализация
отступов, сводные счётчики, обрезка до лимита Telegram
(truncate_html_message), починка незакрытых тегов.
"""

from __future__ import annotations

import re

from court_monitor import config
from court_monitor.config import log
from court_monitor.courts import case_card_url, fi_card_url
from court_monitor.textutil import (
    _bare_case_number, escape_html, plural_ru, shorten_court_name,
)

_DIGEST_CASE_LINK_RE = re.compile(r'<a[^>]*>\s*<b>\s*([^<]+?)\s*</b>\s*</a>')

# Опциональный префикс «N.M.» / «N.Ma.» (например, «5.1.», «5.1a.», «6.2.»),
# который LLM ставит перед эмодзи заголовка подсекции по требованию промпта
# (см. generate_digest, разделы 3.X / 5.X / 6.X). Все пост-процессоры
# заголовков (_DIGEST_HEADER_RE, _SUBSECTION_HEADERS_WITH_COUNT,
# _drop_zero_count_sections, _recount_summary_line) обязаны поглощать этот
# префикс — иначе регексп не совпадёт и шапка пропустится без обработки.
_SUBSECTION_NUM_PREFIX = r'(?:\d+(?:\.\d+[a-z]?)*\.\s+)?'

# Линия считается заголовком подсекции/блока, если начинается с одного из
# этих эмодзи + <b>. Покрывает все заголовки, которые порождает промпт
# `generate_digest`, плюс «🏦 ИСКИ БАНКА (N)» шаблонного рендера — без него
# линтер не видел границы секции «📑 Касс. события» и считал банк-строки
# чужим счётчиком (инцидент 12.08.2026: «заявлено 1, по факту дел 22»).
# Нужно только для поиска границы секции — не обязано
# быть полным, главное — не ловить строки-дела.
_DIGEST_HEADER_RE = re.compile(
    # `⚖️🔬` СТРОГО до `⚖️` — иначе regex матчит `⚖️` в строке
    # «⚖️🔬 <b>КАССАЦИЯ</b>» и спотыкается на следующем `🔬`, не дойдя
    # до `<b>`. Тогда заголовок большого блока КАССАЦИИ не распознаётся,
    # и `_drop_zero_count_sections` проглатывает его как «контент пустой
    # подсекции». Поэтому порядок альтернатив здесь — load-bearing.
    #
    # После `<b>` обязательно русская/латинская буква — иначе строка
    # события вида «📅 <b>22.05.2026 10:12</b> — передача дела судье»
    # ловится как заголовок подсекции и обрывает счётчик `_compute_summary_lines`
    # на первом деле (видно было в сводке «Апелл.: 1 изменение» при реальных 4).
    r'^\s*' + _SUBSECTION_NUM_PREFIX
    + r'(?:⚖️🔬|📥|📅|📨|🔄|⚠|🔁|⚖️|📄|📑|🏛|🏦|🔀|📌|📊|📋)\s*<b>\s*[А-ЯA-Zа-яa-z]'
)

# Голый номер дела вида «2-216/2026», «М-449/2026», «33-3479/2026»,
# «9-12/2025». Не обёрнут в <a href>. Используется как fallback,
# когда LLM забыл обернуть номер в ссылку — пост-процессор обернёт сам.
_BARE_CASE_NUMBER_RE = re.compile(
    r'(?<![\w/-])([0-9A-Za-zА-Яа-яЁё]+-\d+/\d{4})(?![\w/-])'
)

# Большой блок «🏛 ПЕРВАЯ ИНСТАНЦИЯ» / «⚖️ АПЕЛЛЯЦИЯ» / «⚖️🔬 КАССАЦИЯ» / «🔀 Перешли в апелляцию».
_FI_BLOCK_HEADER_RE = re.compile(r'^\s*🏛\s*<b>\s*ПЕРВАЯ ИНСТАНЦИЯ\s*</b>\s*$')
_APPEAL_BLOCK_HEADER_RE = re.compile(r'^\s*⚖️\s*<b>\s*АПЕЛЛЯЦИЯ\s*</b>\s*$')
_CASSATION_BLOCK_HEADER_RE = re.compile(r'^\s*⚖️🔬\s*<b>\s*КАССАЦИЯ\s*</b>\s*$')

# Номер апелляционного дела всегда начинается с «33-». Используем для
# инварианта: апелляционные номера запрещены в блоке 1-й инстанции.
_APPEAL_NUM_RE = re.compile(r'^33-\d+/\d{4}')


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


def _wrap_all_bare_case_numbers(text: str, url_by_num: dict[str, str]) -> str:
    """Обернуть ВСЕ голые номера дел в дайджесте в <a href><b>номер</b></a>.

    Раньше `_drop_hallucinated_from_section` оборачивал номера только в
    подсекциях 3.1 и 5.1. В 5.3/5.4/3.5/3.2/3.6 — если LLM забыл `<a href>`,
    номер уходил в Telegram чёрным жирным текстом, а в дашборде —
    зелёным без подчёркивания. Здесь — глобальная страховка: проходим
    по всем строкам, и для каждого голого номера, для которого знаем URL
    из контекста, оборачиваем через `_wrap_bare_number_in_link` (умеет
    обходить уже существующие `<a href>` и игнорировать одиночные номера
    внутри ссылок).

    Не трогает заголовки секций (`_DIGEST_HEADER_RE`) и итоговые строки
    «В производстве…» — там `(2-…/…)` или похожих токенов нет.
    """
    if not url_by_num:
        return text
    # Сегменты вида `<a ...>...</a>` пропускаем целиком — внутри уже есть
    # номер дела, оборачивать повторно нельзя. В тексте «между» сегментами
    # ищем `_BARE_CASE_NUMBER_RE` и оборачиваем, если URL известен.
    a_tag = re.compile(r"<a\s[^>]*>.*?</a>", re.IGNORECASE | re.DOTALL)
    wrapped: list[str] = []

    def replace_in_segment(seg: str) -> str:
        def repl(m: re.Match) -> str:
            num = m.group(1)
            url = url_by_num.get(num) or url_by_num.get(_bare_case_number(num))
            if not url:
                return m.group(0)
            wrapped.append(num)
            return f'<a href="{url}"><b>{num}</b></a>'
        return _BARE_CASE_NUMBER_RE.sub(repl, seg)

    lines = text.split("\n")
    for i, line in enumerate(lines):
        if not line.strip() or _DIGEST_HEADER_RE.match(line):
            continue
        out: list[str] = []
        last = 0
        for m in a_tag.finditer(line):
            out.append(replace_in_segment(line[last:m.start()]))
            out.append(m.group(0))
            last = m.end()
        out.append(replace_in_segment(line[last:]))
        lines[i] = "".join(out)

    if wrapped:
        log.info(
            f"Пост-процессор дайджеста: глобально обёрнуто "
            f"{len(wrapped)} голых номеров в <a href> ({wrapped})"
        )
    return "\n".join(lines)


def _wrap_bare_number_in_link(line: str, url_by_num: dict[str, str]) -> str:
    """Обернуть первый голый номер дела в строке в <a href><b>номер</b></a>.

    Используется когда LLM забыл оформить номер как ссылку. URL берём из
    словаря {номер → url}, заполненного из `fi_new_cases` / `appeal_new_cases_csv`
    через fi_card_url/case_card_url. Если номера нет в словаре — строку
    оставляем как есть (только <b>номер</b>) — это запасной вариант.
    """
    if "<a href" in line:
        return line
    m = _BARE_CASE_NUMBER_RE.search(line)
    if not m:
        return line
    num = m.group(1)
    bare = _bare_case_number(num)
    url = url_by_num.get(num) or url_by_num.get(bare) or ""
    if url:
        replacement = f'<a href="{url}"><b>{num}</b></a>'
    else:
        replacement = f'<b>{num}</b>'
    return line[:m.start()] + replacement + line[m.end():]


def _ensure_appeal_new_case_full_layout(
    html: str,
    appeal_new_cases: list[dict] | None,
) -> str:
    """Достроить строки 2/3 у дел в секции 5.1 «Новые дела апелляции».

    Backstop для упорного поведения LLM (особенно Haiku): несмотря на
    тройной запрет в промпте, иногда дело сворачивается до одной строки
    «номер — стороны». Юрист не видит ни суда 1 инст., ни категории, ни
    роли банка. Эта функция идёт после `_validate_digest_new_sections` и:

    - находит секцию «📥 Новые дела (N):»;
    - для каждой строки с `<a href><b>номер</b></a>`, по которой есть
      запись в `appeal_new_cases` (CSV-payload), смотрит на следующую
      строку: если в ней нет ни «Суд 1 инст.», ни «категория:» —
      считает, что строка 2 пропущена, и вставляет её сама из CSV;
    - если в данных есть «Дата поступления», но после строки 2 нет
      `<b>дата</b> — 📥 поступило в апел. суд` — вставляет и строку 3.

    Идемпотентна: повторный прогон ничего не добавит, т.к. уже видит «Суд 1 инст.»
    в строке 2. Отступы у вставленных строк — без лидирующих пробелов,
    как в стиле LLM-вывода (фронт и Telegram рендерят одинаково).
    """
    if not appeal_new_cases:
        return html

    by_num = {
        c.get("Номер дела", ""): c
        for c in appeal_new_cases
        if c.get("Номер дела")
    }
    if not by_num:
        return html

    section_re = re.compile(
        r'^\s*📥\s*<b>\s*Новые дела\s*\(\s*\d+\s*\)\s*:\s*</b>'
    )
    case_link_re = re.compile(
        r'<a[^>]*>\s*<b>\s*([^<]+?)\s*</b>\s*</a>'
    )

    lines = html.split("\n")
    out: list[str] = []
    in_section = False
    i = 0
    while i < len(lines):
        ln = lines[i]
        # Конец секции — следующий заголовок
        if in_section and _DIGEST_HEADER_RE.match(ln) and not section_re.match(ln):
            in_section = False
        if section_re.match(ln):
            in_section = True
            out.append(ln)
            i += 1
            continue

        if in_section:
            m = case_link_re.search(ln)
            if m:
                num = m.group(1).strip()
                case = by_num.get(num)
                if case:
                    out.append(ln)
                    next_line = lines[i + 1] if i + 1 < len(lines) else ""
                    next_stripped = next_line.strip()
                    has_line2 = (
                        next_stripped
                        and ("Суд 1 инст." in next_stripped
                             or "категория:" in next_stripped)
                    )
                    if not has_line2:
                        court = shorten_court_name(
                            case.get("Суд 1 инстанции", "") or ""
                        )
                        cat = case.get("Категория", "") or ""
                        role = case.get("Роль банка", "") or ""
                        parts: list[str] = []
                        if court:
                            parts.append(f"Суд 1 инст.: {escape_html(court)}")
                        if cat:
                            parts.append(f"категория: {escape_html(cat)}")
                        if role:
                            parts.append(f"банк — {escape_html(role)}")
                        if parts:
                            out.append(" | ".join(parts))
                            log.info(
                                "Пост-процессор 5.1: достроил строку 2 "
                                f"для дела {num}"
                            )
                        filing = case.get("Дата поступления", "") or ""
                        if filing:
                            out.append(
                                f"<b>{escape_html(filing)}</b> "
                                "— 📥 поступило в апел. суд"
                            )
                            log.info(
                                "Пост-процессор 5.1: достроил строку 3 "
                                f"для дела {num}"
                            )
                    i += 1
                    continue

        out.append(ln)
        i += 1

    return "\n".join(out)


def _validate_digest_new_sections(
    html: str,
    fi_new_cases: list[dict] | None,
    appeal_new_cases: list[dict] | None,
) -> str:
    """Срезать галлюцинации LLM в секциях «Новые иски» (3.1) и «Новые дела» (5.1).

    LLM иногда переносит дела из «Изменений» в «Новые», выдумывая им
    дату подачи (инцидент 24.04.2026: 2-5844/2026 и 2-216/2026 попали
    в «Новые иски» из fi_changes). Здесь сверяем номера со списками
    реально новых дел, лишнее вырезаем, счётчик (N) пересчитываем,
    пустую секцию удаляем вместе с заголовком.

    Вторая задача — гарантировать, что каждая строка дела начинается
    с `<a href><b>номер</b></a>`. Если LLM забыл обернуть — берём URL
    из словаря и оборачиваем сами (инцидент 29.04.2026: М-449/2026
    в «Новых исках» и 33-3479/2026 в «Новых делах апелляции» вышли
    голыми номерами без ссылки).
    """
    allowed_fi: set[str] = set()
    url_by_num_fi: dict[str, str] = {}
    for c in fi_new_cases or []:
        fi = c.get("first_instance") or {}
        url = fi_card_url(fi)
        for key in (c.get("id"), fi.get("case_number")):
            k = (key or "").strip()
            if k:
                allowed_fi.add(k)
                allowed_fi.add(_bare_case_number(k))
                if url:
                    url_by_num_fi[k] = url
                    url_by_num_fi[_bare_case_number(k)] = url

    allowed_appeal: set[str] = set()
    url_by_num_appeal: dict[str, str] = {}
    for c in appeal_new_cases or []:
        n = (c.get("Номер дела") or "").strip()
        if n:
            allowed_appeal.add(n)
            allowed_appeal.add(_bare_case_number(n))
            url = case_card_url(c)
            if url:
                url_by_num_appeal[n] = url
                url_by_num_appeal[_bare_case_number(n)] = url

    html = _drop_hallucinated_from_section(
        html,
        header_re=re.compile(
            r'^\s*📥\s*<b>\s*Новые иски\s*\(\s*(\d+)\s*\)\s*:\s*</b>\s*$'
        ),
        allowed=allowed_fi,
        url_by_num=url_by_num_fi,
        label="1 инст./Новые иски",
    )
    html = _drop_hallucinated_from_section(
        html,
        header_re=re.compile(
            r'^\s*📥\s*<b>\s*Новые дела\s*\(\s*(\d+)\s*\)\s*:\s*</b>\s*$'
        ),
        allowed=allowed_appeal,
        url_by_num=url_by_num_appeal,
        label="апелляция/Новые дела",
    )
    return html


def _drop_hallucinated_from_section(
    html: str,
    *,
    header_re: "re.Pattern[str]",
    allowed: set[str],
    url_by_num: dict[str, str] | None = None,
    label: str,
) -> str:
    url_by_num = url_by_num or {}
    lines = html.split("\n")
    out: list[str] = []
    i = 0
    while i < len(lines):
        m = header_re.match(lines[i])
        if not m:
            out.append(lines[i])
            i += 1
            continue

        # Границы секции: от следующей строки до следующего заголовка
        # (эмодзи + <b>) либо до конца дайджеста.
        j = i + 1
        while j < len(lines) and not _DIGEST_HEADER_RE.match(lines[j]):
            j += 1

        kept: list[str] = []
        removed: list[str] = []
        wrapped: list[str] = []
        for ln in lines[i + 1:j]:
            stripped = ln.strip()
            if not stripped:
                continue  # пустые строки-разделители в «Новых» не ожидаются
            # Визуальный разделитель `⸻` между подсекциями — это не
            # строка-дело и не галлюцинация LLM; пропускаем без warning'а.
            if stripped == "⸻":
                kept.append(ln)
                continue
            mnum = _DIGEST_CASE_LINK_RE.search(ln)
            if not mnum:
                # LLM забыл обернуть номер в <a href> — пытаемся починить.
                fixed = _wrap_bare_number_in_link(ln, url_by_num)
                mnum = _DIGEST_CASE_LINK_RE.search(fixed)
                if not mnum:
                    log.warning(
                        f"Пост-процессор дайджеста: в секции «{label}» строка "
                        f"без номера дела, пропускаю: {stripped[:80]}"
                    )
                    continue
                ln = fixed
                wrapped.append(mnum.group(1).strip())
            num = mnum.group(1).strip()
            if num in allowed or _bare_case_number(num) in allowed:
                kept.append(ln)
            else:
                removed.append(num)

        case_lines_count = sum(1 for ln in kept if ln.strip() != "⸻")

        if not kept or case_lines_count == 0:
            if removed:
                log.warning(
                    f"Пост-процессор дайджеста: секция «{label}» удалена "
                    f"целиком — LLM выдумал {len(removed)} дел ({removed})"
                )
            i = j
            continue

        if removed:
            log.warning(
                f"Пост-процессор дайджеста: из секции «{label}» удалено "
                f"{len(removed)} галлюцинированных дел ({removed})"
            )
        if wrapped:
            log.warning(
                f"Пост-процессор дайджеста: в секции «{label}» {len(wrapped)} "
                f"номеров обёрнуты в <a href> вручную (LLM забыл): {wrapped}"
            )

        old_count = m.group(1)
        new_header = lines[i].replace(
            f"({old_count})", f"({case_lines_count})", 1
        )
        out.append(new_header)
        out.extend(kept)
        i = j

    return "\n".join(out)


# Подзаголовки подсекций со счётчиком (N): — для пост-процессора
# `_renumber_section_headers`. Каждый паттерн ловит шапку и группу 1 = N.
# Префикс `_SUBSECTION_NUM_PREFIX` поглощает «5.1.»/«6.2.» — без него
# регекспы не распознают заголовки, которые LLM выводит с нумерацией
# согласно промпту.
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


def _renumber_section_headers(html: str) -> str:
    """Пересчитать `(N)` в шапке каждой подсекции по факту.

    LLM иногда заявляет «Новые иски (2):» а выводит одно дело, либо наоборот.
    `_validate_digest_new_sections` уже правит «Новые иски/дела» (3.1/5.1).
    Эта функция покрывает оставшиеся секции с (N): 3.2 «Изменения»
    (1 инст.), 3.3 «Поданы апел. жалобы», 3.4 «Кассация», 3.5 «Вынесенные
    решения», 3.6 «Тексты решений», 4 «Перешли в апелляцию», 5.1a «Переход
    к правилам», 5.2 «Изменения» (апел., объединили бывшие «Отложенные» +
    «Назначенные»), 5.4 «Вынесенные акты», 5.5 «Тексты актов». Считаем
    строки с `<a href>` номером до следующего заголовка
    (`_DIGEST_HEADER_RE`).
    """
    lines = html.split("\n")
    out: list[str] = list(lines)
    n = len(lines)
    for i in range(n):
        ln = lines[i]
        for pat, label in _SUBSECTION_HEADERS_WITH_COUNT:
            m = pat.match(ln)
            if not m:
                continue
            # Считаем строки-дела до следующего заголовка
            j = i + 1
            count = 0
            while j < n and not _DIGEST_HEADER_RE.match(lines[j]):
                if _line_has_case_number(lines[j]):
                    count += 1
                j += 1
            old_count = m.group(2)
            if str(count) != old_count:
                log.warning(
                    f"Пост-процессор дайджеста: секция «{label}» — "
                    f"шапка обещала ({old_count}) дел, фактически {count}; "
                    f"переписано."
                )
                out[i] = f"{m.group(1)}{count}{m.group(3)}"
            break
    return "\n".join(out)


def _classify_line(line: str) -> str:
    """Определить тип строки для нормализатора отступов.

    Типы:
    - "EMPTY" — пустая строка
    - "BIG_HEADER" — `<b>🏛 ПЕРВАЯ ИНСТАНЦИЯ</b>` / `<b>⚖️ АПЕЛЛЯЦИЯ</b>` /
      `<b>🔀 Перешли в апелляцию (N)</b>`
    - "SUB_HEADER" — заголовок подсекции с эмодзи + <b>…(N):</b>
    - "SEPARATOR" — `⸻`
    - "CASE_LINE" — содержит `<a href` (строка-дело со ссылкой)
    - "CONT_LINE" — продолжение строки дела (без ссылки, не пустая,
      не разделитель, не заголовок) — например, строка 2 двухстрочной
      записи с «стороны | событие» или «Итог: …», «Почему: …», «Заседание
      отложено на …»
    - "TITLE" — заголовок дайджеста (📊 Дайджест …) и сводка (📋 Сводка)
    - "FOOTER" — итоговая строка `📌 В производстве …` и ссылка на дашборд
    """
    s = line.strip()
    if not s:
        return "EMPTY"
    if s == "⸻":
        return "SEPARATOR"
    # 📊 Дайджест… / 📋 <b>Сводка</b> / <i>1 инст.:</i> … / <i>Апелл.:</i> …
    if s.startswith("📊") or s.startswith("📋") or s.startswith("<i>"):
        return "TITLE"
    if s.startswith("📌"):
        return "FOOTER"
    if (_FI_BLOCK_HEADER_RE.match(line)
            or _APPEAL_BLOCK_HEADER_RE.match(line)
            or _CASSATION_BLOCK_HEADER_RE.match(line)):
        return "BIG_HEADER"
    # «🔀 Перешли в апелляцию» — это самостоятельный мостик, ведёт себя как
    # большой блок (между ним и соседними блоками — одна пустая строка, без ⸻).
    if re.match(r'^\s*🔀\s*<b>\s*Перешли в апелляцию', line):
        return "BIG_HEADER"
    if _DIGEST_HEADER_RE.match(line) and "(" in s and "):" in s:
        return "SUB_HEADER"
    # Заголовок подсекции без счётчика (например, «📨 Поданы апелляционные
    # жалобы:» — старый формат). Считаем тоже SUB_HEADER, чтобы отступы
    # ставились корректно.
    if _DIGEST_HEADER_RE.match(line):
        return "SUB_HEADER"
    if "<a href" in line:
        return "CASE_LINE"
    # Ссылка на дашборд в самом конце
    if 'href="' in line and "Дашборд" in line:
        return "FOOTER"
    # Голый номер дела (LLM забыл обернуть, и пост-процессор не нашёл URL).
    # Считаем такую строку CASE_LINE — иначе нормализатор отступов спутает её
    # с продолжением предыдущего дела.
    if _BARE_CASE_NUMBER_RE.search(line):
        return "CASE_LINE"
    return "CONT_LINE"


# Финальная плашка («📌 В производстве: всего N (1 инст.: X | апел.: Y | касс.: Z)»)
# и ссылка на дашборд («📊 Дашборд»). Эти регексы используются, чтобы
# срезать существующие варианты, прежде чем добавить детерминированные —
# защита от дублей при разных формулировках LLM.
_FOOTER_BADGE_RE = re.compile(r'^\s*📌\s*<b>\s*В\s+производстве:')
_DASHBOARD_LINK_RE = re.compile(
    r'^\s*<a\s+href="[^"]*">\s*📊\s*Дашборд\s*</a>\s*$'
)


def _ensure_footer(
    html: str,
    *,
    total_active: int,
    total_active_fi: int,
    total_active_appeal: int,
    total_active_cassation: int,
    total_active_bank: int = 0,
) -> str:
    """Гарантировать наличие финальной плашки `📌 В производстве …` и
    ссылки `📊 Дашборд` в конце дайджеста.

    LLM в полном режиме иногда упирается в max_tokens и обрезается перед
    финальной плашкой; реже — путает порядок (ссылка раньше плашки) или
    пишет лишнюю свою формулировку. Удаляем существующие плашку/ссылку
    и всегда добавляем детерминированные в самый конец (плашка → ссылка).
    Числа берутся ровно те же, что считает `main()` для template-ветки
    (см. финальные строки `generate_template_digest`).
    """
    lines = html.split("\n")
    cleaned = [
        ln for ln in lines
        if not _FOOTER_BADGE_RE.match(ln) and not _DASHBOARD_LINK_RE.match(ln)
    ]
    # Срезаем хвостовые пустые строки — добавим свои отступы.
    while cleaned and cleaned[-1].strip() == "":
        cleaned.pop()

    badge = (
        f"📌 <b>В производстве: всего {total_active}"
        f" (1 инст.: {total_active_fi} | апел.: {total_active_appeal}"
        f" | касс.: {total_active_cassation})"
        # Иски банка — отдельная картотека, в сумму «всего» не входит
        # (09.08.2026); 0 = трек выключен, приписки нет.
        + (f" · 🏦 иски банка: {total_active_bank} в производстве"
           if total_active_bank else "")
        + "</b>"
    )
    link = f'<a href="{config.DASHBOARD_URL}">📊 Дашборд</a>'
    cleaned.append("")
    cleaned.append(badge)
    cleaned.append(link)
    return "\n".join(cleaned)


def _normalize_section_spacing(html: str) -> str:
    """Привести межсекционные отступы к каноничному виду.

    Промпт (правила (а)/(б)/(б1)/(в)/(г)) описывает отступы подробно, но
    LLM их нарушает: то перед `⸻` нет пустой строки, то после заголовка
    подсекции нет пустой строки, то между двумя строками одного дела
    появляется пустая. Эта функция переписывает отступы по типам строк:

    - перед SUB_HEADER (если предыдущая значимая строка не BIG_HEADER) —
      `пустая → ⸻ → пустая`;
    - после BIG_HEADER до первого SUB_HEADER — ровно одна пустая строка;
    - после SUB_HEADER — ровно одна пустая строка перед первым CASE_LINE;
    - между CASE_LINE и CONT_LINE (продолжение того же дела) — ноль пустых;
    - между двумя CASE_LINE / между блоком одного дела и блоком другого —
      ровно одна пустая строка;
    - между BIG_HEADER блоками — ровно одна пустая строка, без ⸻;
    - перед FOOTER (`📌 В производстве …`) — одна пустая строка.

    Идемпотентна: повторный прогон ничего не меняет.
    """
    lines = html.split("\n")
    # Удаляем все ⸻ и пустые строки — оставим только значимые. Потом
    # вставим разделители заново.
    significant: list[tuple[str, str]] = []  # (type, line)
    for ln in lines:
        t = _classify_line(ln)
        if t in ("EMPTY", "SEPARATOR"):
            continue
        significant.append((t, ln))

    if not significant:
        return html

    out: list[str] = []
    prev_type: str | None = None
    for idx, (t, ln) in enumerate(significant):
        if prev_type is None:
            out.append(ln)
            prev_type = t
            continue

        # Решаем, что вставить ПЕРЕД этой строкой.
        if t == "TITLE":
            s = ln.strip()
            prev_s = out[-1].strip() if out else ""
            # 📊 Дайджест… → 📋 Сводка: одна пустая строка между ними.
            # 📋 Сводка → <i>1 инст.:</i>: одна пустая строка.
            # <i>1 инст.:</i> → <i>Апелл.:</i>: БЕЗ пустой строки (две строки
            # сводки идут подряд, см. правило промпта).
            if (s.startswith("<i>") and prev_s.startswith("<i>")):
                pass  # две строки сводки — без пустой
            elif prev_type == "TITLE":
                out.append("")
        elif t == "BIG_HEADER":
            # Между большими блоками — одна пустая строка, без ⸻.
            out.append("")
        elif t == "SUB_HEADER":
            if prev_type == "BIG_HEADER":
                # После большого блока — одна пустая строка перед первой подсекцией.
                out.append("")
            else:
                # Перед последующими подсекциями того же блока: пустая → ⸻ → пустая.
                out.append("")
                out.append("⸻")
                out.append("")
        elif t == "CASE_LINE":
            if prev_type == "SUB_HEADER":
                # После заголовка подсекции — одна пустая строка перед первым делом.
                out.append("")
            elif prev_type == "CASE_LINE":
                # Между двумя CASE_LINE — пустая строка (это два разных дела).
                out.append("")
            elif prev_type == "CONT_LINE":
                # Конец одного дела, начало следующего — пустая строка.
                out.append("")
            elif prev_type == "BIG_HEADER":
                # CASE_LINE прямо после большого блока — нештатно, но
                # вставим одну пустую строку для безопасности.
                out.append("")
        elif t == "CONT_LINE":
            # Продолжение того же дела — ноль пустых строк перед.
            # Однако если предыдущая значимая строка — SUB_HEADER, это
            # странно (CONT_LINE без CASE_LINE сверху); оставим как есть.
            pass
        elif t == "FOOTER":
            # 📌 В производстве … или ссылка на дашборд — одна пустая
            # строка перед футером.
            out.append("")

        out.append(ln)
        prev_type = t

    return "\n".join(out)


def _count_digest_subsections(html: str) -> list[tuple[str, str, int]]:
    """Парсит HTML дайджеста и возвращает [(block, label, count), ...]
    по каждой найденной подсекции с (N).

    block: "fi" / "appeal" / "bridge" / "cass" — нужен только для
    дальнейшей группировки потребителем (свод по инстанциям и т.п.).
    label: «1 инст./Изменения», «Апел./Отложено», «Перешли в апелляцию»,
    «Касс./События» и т.п. — берётся из `_SUBSECTION_HEADERS_WITH_COUNT`,
    либо синтезируется для «Новые иски» / «Новые дела» (у них другие
    регексы).
    count: число строк-дел, попавших между этим заголовком и следующим
    заголовком секции/подсекции. Совпадает с тем, что выставит
    `_renumber_section_headers` в `(N):`.
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


def _plural_ru(n: int, forms: tuple[str, str, str]) -> str:
    """Русский множественный выбор: (1, 2-4, 5+).

    Обёртка над канонической textutil.plural_ru (появилась 06.07.2026
    для сводки дайджеста) — оставлена ради прежней сигнатуры (tuple)
    и ре-экспорта в фасаде update_cases.
    """
    return plural_ru(n, *forms)


def _compute_summary_lines(html: str) -> tuple[str | None, str | None, str | None]:
    """Собрать три детерминированные строки сводки `📋` по факту вывода.

    Возвращает (1_инст.-строка, Апелл.-строка, Касс.-строка). Каждая
    может быть None — если по этой инстанции в дайджесте ни одной
    подсекции с (N) не выведено.

    Считает заново (не через `_count_digest_subsections`), потому что
    тот не различает контекст big-header'а: лейбл «Изменения» одинаков
    для 1 инст. (3.2) и апелляции (5.2), а нам нужно знать инстанцию.
    Идём по строкам, отслеживаем текущий BIG_HEADER, и для каждой
    подсекции (`📅 Изменения`, `📥 Новые иски`, `⚖️ Вынесенные акты`
    и т.п.) присваиваем счётчик правильной инстанции.
    """
    # Категории внутри одной инстанции — раздельно, чтобы выбрать нужную
    # формулировку при плюрализации.
    counters = {
        "fi": {
            "new_cases": 0,        # 3.1 Новые иски
            "changes": 0,          # 3.2 Изменения
            "appeal_filed": 0,     # 3.3 Поданы апел. жалобы
            "cassation": 0,        # 3.4 Кассационные события
            "resolved": 0,         # 3.5 + 3.6 (решения + тексты)
        },
        "appeal": {
            "new_cases": 0,        # 5.1 Новые дела
            "changes": 0,          # 5.2 Изменения (объединяет «Отложенные»+«Назначенные»)
            "acts": 0,             # 5.4 + 5.5 (акты + тексты актов)
        },
        "cass": {
            "new_cases": 0,        # 6.1 Новые касс. дела
            "events": 0,           # 6.2 Касс. события
        },
    }

    # Регексы подсекций (с count в скобках), без префикса инстанции —
    # инстанцию определим по окружающему BIG_HEADER. Для «📅 Изменения»
    # и «📥 Новые иски/дела» один регекс работает в обеих инстанциях.
    sub_patterns = [
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'📅\s*<b>\s*Изменения\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"fi": "changes", "appeal": "changes"}),
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'📥\s*<b>\s*Новые иски\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"fi": "new_cases"}),
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'📥\s*<b>\s*Новые дела\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"appeal": "new_cases"}),
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'📨\s*<b>\s*Поданы апелляционные жалобы\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"fi": "appeal_filed"}),
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'📨\s*<b>\s*Кассационные события\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"fi": "cassation"}),
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'⚖️\s*<b>\s*Вынесенные решения\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"fi": "resolved"}),
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'📄\s*<b>\s*Опубликованные тексты решений\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"fi": "resolved"}),
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'⚖️\s*<b>\s*Вынесенные акты\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"appeal": "acts"}),
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'📄\s*<b>\s*Опубликованные тексты актов\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"appeal": "acts"}),
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'📥\s*<b>\s*Новые касс\. дела\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"cass": "new_cases"}),
        (re.compile(r'^\s*' + _SUBSECTION_NUM_PREFIX
                    + r'📑\s*<b>\s*Касс\. события\s*\(\s*\d+\s*\)\s*:\s*</b>\s*$'),
         {"cass": "events"}),
    ]

    lines = html.split("\n")
    n = len(lines)
    cur_big = ""  # "fi" / "appeal" / "cass" / ""
    i = 0
    while i < n:
        ln = lines[i]
        if _FI_BLOCK_HEADER_RE.match(ln):
            cur_big = "fi"
            i += 1
            continue
        if _APPEAL_BLOCK_HEADER_RE.match(ln):
            cur_big = "appeal"
            i += 1
            continue
        if _CASSATION_BLOCK_HEADER_RE.match(ln):
            cur_big = "cass"
            i += 1
            continue
        matched = False
        for pat, mapping in sub_patterns:
            if not pat.match(ln):
                continue
            bucket = mapping.get(cur_big)
            if bucket is None:
                # Подсекция в неожиданной инстанции — пропускаем (например,
                # «📅 Изменения» внутри `🔀 Перешли в апелляцию` мостика).
                break
            # Считаем строки-дела до следующего заголовка.
            j = i + 1
            count = 0
            while j < n and not _DIGEST_HEADER_RE.match(lines[j]):
                if _line_has_case_number(lines[j]):
                    count += 1
                j += 1
            counters[cur_big][bucket] += count
            i = j
            matched = True
            break
        if not matched:
            i += 1

    fi = counters["fi"]
    ap = counters["appeal"]
    ca = counters["cass"]

    fi_parts: list[str] = []
    if fi["new_cases"]:
        fi_parts.append(
            f"{fi['new_cases']} "
            f"{_plural_ru(fi['new_cases'], ('новый иск', 'новых иска', 'новых исков'))}"
        )
    if fi["changes"]:
        fi_parts.append(
            f"{fi['changes']} "
            f"{_plural_ru(fi['changes'], ('изменение', 'изменения', 'изменений'))}"
        )
    if fi["appeal_filed"]:
        fi_parts.append(
            f"{fi['appeal_filed']} "
            f"{_plural_ru(fi['appeal_filed'], ('апел. жалоба', 'апел. жалобы', 'апел. жалоб'))}"
        )
    if fi["cassation"]:
        fi_parts.append(
            f"{fi['cassation']} касс. "
            f"{_plural_ru(fi['cassation'], ('событие', 'события', 'событий'))}"
        )
    if fi["resolved"]:
        fi_parts.append(
            f"{fi['resolved']} "
            f"{_plural_ru(fi['resolved'], ('решение', 'решения', 'решений'))}"
        )

    ap_parts: list[str] = []
    if ap["new_cases"]:
        ap_parts.append(
            f"+{ap['new_cases']} "
            f"{_plural_ru(ap['new_cases'], ('дело', 'дела', 'дел'))}"
        )
    if ap["changes"]:
        ap_parts.append(
            f"{ap['changes']} "
            f"{_plural_ru(ap['changes'], ('изменение', 'изменения', 'изменений'))}"
        )
    if ap["acts"]:
        ap_parts.append(
            f"{ap['acts']} "
            f"{_plural_ru(ap['acts'], ('акт', 'акта', 'актов'))}"
        )

    cass_parts: list[str] = []
    if ca["new_cases"]:
        cass_parts.append(
            f"+{ca['new_cases']} "
            f"{_plural_ru(ca['new_cases'], ('дело', 'дела', 'дел'))}"
        )
    if ca["events"]:
        cass_parts.append(
            f"{ca['events']} "
            f"{_plural_ru(ca['events'], ('событие', 'события', 'событий'))}"
        )

    fi_line = f"<i>1 инст.:</i> {', '.join(fi_parts)}" if fi_parts else None
    ap_line = f"<i>Апелл.:</i> {', '.join(ap_parts)}" if ap_parts else None
    cass_line = f"<i>Касс.:</i> {', '.join(cass_parts)}" if cass_parts else None
    return fi_line, ap_line, cass_line


# Заголовок блока «📋 Сводка». Допускаем оба варианта: с <b> и без —
# LLM иногда забывает обёртку.
_SUMMARY_HEADER_RE = re.compile(
    r'^\s*📋\s*(?:<b>\s*Сводка\s*</b>|Сводка)\s*$'
)
# Маркер «следующего большого блока» — где заканчивается зона «📋 Сводка»
# (включая всё, что LLM туда написал — даже если без <i> обёрток). Это
# либо заголовок раздела (🏛/⚖️/⚖️🔬), либо финальная плашка/ссылка/⸻.
_SUMMARY_END_RE = re.compile(
    r'^\s*(?:🏛|⚖️🔬|⚖️|📌|📊|⸻|<a\s)'
)


def _replace_summary_block(html: str) -> str:
    """Полностью переписать блок «📋 Сводка» детерминированной Python-сборкой.

    Раньше `_recount_summary_line` лишь редактировала строки `<i>1 инст.:</i>`
    и т.п. ВНУТРИ существующего блока — но LLM иногда писал сводку в
    свободной форме (например, «Апелл.: 7 актов» без <i>-обёрток или
    без всех трёх инстанций), и якорь не находился, сводка оставалась
    неполной. Новая функция вырезает ВЕСЬ блок (от строки «📋 Сводка»
    до начала следующего большого блока) и вставляет три детерминированные
    строки с факт-счётчиками. Если по всем трём инстанциям пусто — блок
    удаляется вовсе. Если LLM не вывел даже заголовок «📋 Сводка» —
    вставляем блок сразу после первой строки `📊`.
    """
    fi_line, ap_line, cass_line = _compute_summary_lines(html)
    new_lines = [ln for ln in (fi_line, ap_line, cass_line) if ln is not None]

    src_lines = html.split("\n")
    n = len(src_lines)

    # Найти заголовок «📋 Сводка» (если есть).
    header_idx = -1
    for i, ln in enumerate(src_lines):
        if _SUMMARY_HEADER_RE.match(ln):
            header_idx = i
            break

    # Собрать готовый блок (заголовок + строки + одна пустая строка снизу).
    if new_lines:
        block = ["📋 <b>Сводка</b>", ""] + new_lines + [""]
    else:
        block = []

    if header_idx == -1:
        # Заголовка нет — вставим после первой строки `📊` (заголовок дайджеста).
        if not block:
            return html
        for i, ln in enumerate(src_lines):
            if ln.lstrip().startswith("📊"):
                # Если сразу после `📊` уже пустая строка — встроимся ПОСЛЕ неё.
                insert_at = i + 1
                if insert_at < n and src_lines[insert_at].strip() == "":
                    insert_at += 1
                out = src_lines[:insert_at] + block + src_lines[insert_at:]
                return "\n".join(out)
        # Якоря нет вовсе — вставим в самое начало.
        return "\n".join(block + src_lines)

    # Найти, где блок заканчивается (следующая «большая» строка).
    end_idx = header_idx + 1
    while end_idx < n and not _SUMMARY_END_RE.match(src_lines[end_idx]):
        end_idx += 1
    # Срежем хвостовые пустые строки перед end_idx — block их добавит сам.
    while end_idx > header_idx + 1 and src_lines[end_idx - 1].strip() == "":
        end_idx -= 1
    # Срежем ведущие пустые строки в block, если перед header_idx уже есть.
    out = src_lines[:header_idx] + block + src_lines[end_idx:]
    return "\n".join(out)


_LIST_PRINT_FACTS_FOR_LOG = False  # глушилка, для возможной отладки


def _warn_misplaced_appeal_cases(html: str) -> str:
    """Залогировать апелляционные номера (`33-…`), оказавшиеся в блоке 1 инст.

    Прецедент 29.04.2026: LLM поместил дело 33-2677/2026 в подсекцию
    «📅 Изменения» (бывш. «📅 Назначенные заседания» в апел. 5.3) внутри
    блока 1-й инстанции, хотя по инварианту все `33-…` номера принадлежат
    блоку «⚖️ АПЕЛЛЯЦИЯ».

    Удалять/переносить такие строки опасно — рядом обычно есть полезная
    мотивировка, которую юрист хочет видеть, даже если секция выбрана
    неправильно. Поэтому пост-процессор только логирует предупреждение,
    а корень фиксится в промпте (явный инвариант «33- = апелляция»).
    Если повторится — можно будет добавить перенос строк в правильный блок.
    """
    lines = html.split("\n")
    n = len(lines)

    fi_start = None
    fi_end = n
    for i, ln in enumerate(lines):
        if _FI_BLOCK_HEADER_RE.match(ln):
            fi_start = i
        elif fi_start is not None and (
            _APPEAL_BLOCK_HEADER_RE.match(ln)
            or re.match(r'^\s*🔀\s*<b>\s*Перешли в апелляцию', ln)
        ):
            fi_end = i
            break

    if fi_start is None:
        return html

    misplaced: list[str] = []
    for ln in lines[fi_start + 1:fi_end]:
        m = _DIGEST_CASE_LINK_RE.search(ln)
        num = m.group(1).strip() if m else ""
        if not num:
            mb = _BARE_CASE_NUMBER_RE.search(ln)
            num = mb.group(1) if mb else ""
        if num and _APPEAL_NUM_RE.match(num):
            misplaced.append(num)

    if misplaced:
        log.warning(
            f"Пост-процессор дайджеста: в блоке «🏛 ПЕРВАЯ ИНСТАНЦИЯ» "
            f"найдены апелляционные номера ({misplaced}) — LLM нарушил "
            f"инвариант «33- = апелляция». Не трогаю содержимое, чтобы "
            f"не потерять полезную мотивировку; править — в промпте."
        )
    return html


def _shorten_categories_in_html(html: str) -> str:
    """Срезать родительские сегменты в строках «категория: X → Y → Z».

    LLM иногда вопреки правилу промпта («категория уже ПОДГОТОВЛЕНА Python —
    копируй ДОСЛОВНО») реконструирует полную цепочку категорий из своего
    знания (например, «Иски, связанные с возмещением ущерба → Иные о
    возмещении имущественного вреда»). Юрист просит только конечный
    сегмент. Срезаем по «→», оставляя последний.

    Регексп ловит «категория: …» / «Категория: …» до ближайшего `|`, `<`
    или конца строки — это покрывает форматы «| категория: X | банк — …»
    и «| категория: X» в концовке строки.
    """
    pattern = re.compile(r'((?:к|К)атегория:\s*)([^|<\n]+?)(\s*(?:\||$))', re.MULTILINE)

    def _replace(m: re.Match) -> str:
        prefix, cat, suffix = m.group(1), m.group(2).strip(), m.group(3)
        if "→" in cat:
            parts = [p.strip() for p in cat.split("→") if p.strip()]
            if parts:
                cat = parts[-1]
        return f"{prefix}{cat}{suffix}"

    return pattern.sub(_replace, html)


def _drop_zero_count_sections(html: str) -> str:
    """Удалить подсекции с заголовком вида «… (0):».

    После пересчёта счётчиков (`_renumber_section_headers`,
    `_validate_digest_new_sections`) могут появиться шапки `(0):` —
    это значит, что под ними не оказалось ни одного дела. В дайджест
    выводить их вредно — занимают место и сбивают читателя. Удаляем
    шапку и всё содержимое до следующего заголовка (`_DIGEST_HEADER_RE`).

    Также удаляются строки-разделители `⸻`, оказавшиеся подряд из-за
    удалённой между ними подсекции — `_normalize_section_spacing`
    дальше всё равно перепишет, но лишний `⸻` поломает классификацию.
    """
    lines = html.split("\n")
    out: list[str] = []
    n = len(lines)
    i = 0
    while i < n:
        ln = lines[i]
        # Шапка с (0): опциональный «N.M.»-префикс + любой эмодзи + <b>…(0):</b>.
        # 📑 здесь обязателен — это эмодзи подсекции «Касс. события (N):».
        if re.match(
            r'^\s*' + _SUBSECTION_NUM_PREFIX
            + r'(?:📥|📅|📨|🔁|⚖️|📄|📑|⚠|🔀)\s*<b>[^<]*\(\s*0\s*\)\s*:\s*</b>\s*$',
            ln,
        ):
            j = i + 1
            while j < n and not _DIGEST_HEADER_RE.match(lines[j]):
                j += 1
            i = j
            continue
        out.append(ln)
        i += 1
    return "\n".join(out)


def _strip_section_numbering(html: str) -> str:
    """Удалить префикс «N.M.» / «N.Ma.» перед заголовками подсекций.

    LLM иногда возвращает заголовок вида `5.1. 📥 <b>Новые дела (3):</b>`
    вопреки явному правилу 2bis в промпте. Юрист просил без нумерации —
    стрипим префикс из любой строки, которая ПОСЛЕ стрипа становится
    валидным заголовком подсекции (по `_DIGEST_HEADER_RE`). Другие строки
    (например, абзац с фразой «5.1 — это апелляция») остаются нетронутыми,
    т.к. они не матчатся `_DIGEST_HEADER_RE`.
    """
    lines = html.split("\n")
    out: list[str] = []
    prefix_re = re.compile(r'^(\s*)(\d+(?:\.\d+[a-z]?)*\.\s+)(.*)$')
    for ln in lines:
        m = prefix_re.match(ln)
        if m:
            stripped = m.group(1) + m.group(3)
            if _DIGEST_HEADER_RE.match(stripped):
                out.append(stripped)
                continue
        out.append(ln)
    return "\n".join(out)


def _purge_3_6_without_act_text(html: str, fi_changes: list[dict]) -> str:
    """Страховка от галлюцинаций LLM в 3.6 «Опубликованные тексты решений».

    LLM иногда кладёт дело в 3.6 на основании фразы «мотивированное решение
    изготовлено» в last_event/event, хотя у дела нет fi_act_text_published
    (то есть фактического текста мотивировки). Тогда LLM ВЫДУМЫВАЕТ Итог,
    Почему, «требуется уточнение по карточке суда» и прочее — выдаёт юристу
    фейк. Это критический брак.

    Функция парсит секцию 3.6, для каждого дела проверяет в `fi_changes`
    наличие типа `fi_act_text_published` или непустого `details.act_text`.
    Если нет — удаляет блок дела целиком (3 строки + хвостовой пробел).
    Заголовок секции пересчитывается; при N=0 секция удаляется полностью
    (через `_drop_zero_count_sections` на следующем шаге).
    """
    # Множество легитимных дел для 3.6: те, у кого есть fi_act_text_published
    # ИЛИ непустой act_text в деталях.
    legit_cases: set[str] = set()
    for ch in fi_changes or []:
        types = ch.get("type") or []
        details = ch.get("details") or {}
        if (
            "fi_act_text_published" in types
            or (details.get("act_text") or "").strip()
        ):
            num = (ch.get("case") or "").strip()
            if num:
                legit_cases.add(num)

    lines = html.split("\n")
    n = len(lines)

    # Найти начало секции 3.6.
    sec_re = re.compile(
        r'^\s*📄\s*<b>\s*Опубликованные тексты решений\s*\(\s*(\d+)\s*\)\s*:\s*</b>\s*$'
    )
    sec_start = -1
    sec_count = 0
    for i, ln in enumerate(lines):
        m = sec_re.match(ln)
        if m:
            sec_start = i
            sec_count = int(m.group(1))
            break
    if sec_start < 0:
        return html  # секции нет — нечего чистить

    # Найти конец секции — следующая шапка подсекции / большого блока.
    sec_end = n
    for j in range(sec_start + 1, n):
        if (
            _DIGEST_HEADER_RE.match(lines[j])
            or _FI_BLOCK_HEADER_RE.match(lines[j])
            or _APPEAL_BLOCK_HEADER_RE.match(lines[j])
            or lines[j].strip().startswith("📌")
        ):
            sec_end = j
            break

    body_lines = lines[sec_start + 1: sec_end]

    # Разбить тело секции на блоки дел: блок начинается на строке с номером
    # в <a href><b>номер</b></a> или в голом <b>номер</b>, заканчивается
    # перед следующим таким блоком ИЛИ в конце секции.
    case_link_re = re.compile(
        r'<a[^>]*>\s*<b>\s*([^<]+?)\s*</b>\s*</a>|<b>\s*([0-9A-Za-zА-Яа-яЁё]+-\d+/\d{4})\s*</b>'
    )
    block_indices: list[tuple[int, str]] = []
    for k, ln in enumerate(body_lines):
        m = case_link_re.search(ln)
        if m:
            num = (m.group(1) or m.group(2) or "").strip()
            # Очищаем от возможных хвостов вида «(2-3719/2025;)»: берём только
            # номер до первого пробела/скобки.
            num_main = re.split(r'[\s(]', num, maxsplit=1)[0].strip()
            if num_main:
                block_indices.append((k, num_main))

    # Сформируем новые body_lines, пропуская нелегитимные блоки.
    keep_blocks: list[tuple[int, int, str]] = []  # (start, end, num)
    for idx, (start, num) in enumerate(block_indices):
        end = (
            block_indices[idx + 1][0]
            if idx + 1 < len(block_indices)
            else len(body_lines)
        )
        keep_blocks.append((start, end, num))

    new_body: list[str] = []
    kept = 0
    dropped: list[str] = []
    if not keep_blocks:
        # В секции нет распознанных дел — сохраняем как есть (вдруг что-то
        # нестандартное, лучше не трогать).
        return html
    # Префикс перед первым блоком (пустые строки и т.п.) — сохраним.
    prefix = body_lines[: keep_blocks[0][0]]
    new_body.extend(prefix)
    for start, end, num in keep_blocks:
        block = body_lines[start:end]
        if num in legit_cases:
            new_body.extend(block)
            kept += 1
        else:
            dropped.append(num)
            # Если блок заканчивался пустой строкой-разделителем, мы её
            # тоже выкидываем — финальная нормализация всё равно расставит
            # пробелы заново.

    if not dropped:
        return html  # ничего не удалили, ранний возврат

    log.warning(
        "purge 3.6: удалены дела без fi_act_text_published: %s "
        "(оставлено %d из %d)",
        ", ".join(dropped), kept, sec_count,
    )

    # Пересоберём итоговый html: до секции, новый заголовок, новое тело,
    # после секции.
    new_header = re.sub(
        r'\(\s*\d+\s*\)',
        f"({kept})",
        lines[sec_start],
        count=1,
    )
    return "\n".join(
        lines[:sec_start]
        + [new_header]
        + new_body
        + lines[sec_end:]
    )


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
