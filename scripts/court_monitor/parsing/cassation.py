# -*- coding: utf-8 -*-
"""Парсинг кассации 7kas.sudrf.ru: поисковая выдача с фильтром по 1-й
инстанции ХМАО, карточка дела, извлечение текста определения (cont_doc1),
детерминированная классификация исхода (classify_cassation_outcome).
"""

from __future__ import annotations

import re
from urllib.parse import urlparse
from datetime import datetime, date

from court_monitor.config import log
from court_monitor.courts import match_hmao_first_instance
from court_monitor.lifecycle import _DATE_DDMMYYYY_RX, _SUSPENDED_RX
from court_monitor.parsing.search import (
    _find_results_table, _is_real_sberbank, determine_bank_role_from_participants,
)
from court_monitor.parsing.tables import extract_tables, cell_text, cell_href
from court_monitor.textutil import (
    _strip_html, _bare_case_number, parse_date,
    _CASE_ID_RE, _CASE_UID_RE, _HTML_SCRIPT_RE, _HTML_STYLE_RE,
)

# ── Парсинг поиска и карточки кассации (7kas.sudrf.ru) ───────────────────────

# Регулярки для разбора объединённой ячейки td2 в результатах поиска 7kas.
# Формат:
#   КАТЕГОРИЯ: ... → ... Жалобу подал(а): X. Суд (судебный участок) первой
#   инстанции: Y. Номер дела в первой инстанции: 2-XXX/YYYY
# В отличие от 1-й инст./апел. (ИСТЕЦ/ОТВЕТЧИК), стороны на 7kas в выдаче не
# приводятся — только заявитель кассации. Стороны берём из карточки (УЧАСТНИКИ).
_CASS_CATEGORY_RE = re.compile(
    r"КАТЕГОРИЯ:\s*(.+?)(?=Жалобу\s+подал|Суд\s|Номер дела|$)", re.IGNORECASE
)
_CASS_CASSATOR_RE = re.compile(
    r"Жалобу\s+подал\(а\):\s*(.+?)(?=Суд\s|Судья\s*\(|Номер дела|$)", re.IGNORECASE
)
_CASS_FI_COURT_RE = re.compile(
    r"Суд\s*\([^)]*\)\s*первой\s+инстанции:\s*(.+?)(?=Номер дела|Категория|$)",
    re.IGNORECASE,
)
# Президиум облсуда (кассация по делам МИРОВЫХ судей, с 04.09.2026): вместо
# «Суд (…) первой инстанции» выдача пишет «Судья (мировой судья) первой
# инстанции: ФИО (Судебный уч. №3, Ханты-Мансийский р-н)» — судья + участок
# одной ячейкой. Разводим: ФИО → fi_judge, скобки → fi_court_long
# «Мировой судья (участок)», флаг fi_magistrate.
_CASS_FI_MAGISTRATE_RE = re.compile(
    r"Судья\s*\(мировой судья\)\s*первой\s+инстанции:\s*(.+?)"
    r"(?=Номер дела|Категория|$)",
    re.IGNORECASE,
)
# «ФИО (участок)» — скобки с участком/мировым судьёй; у карточки та же форма
# в строке «Судья (мировой судья) первой инстанции».
_MAGISTRATE_JUDGE_RE = re.compile(
    r"^(?P<judge>.*?)\s*\((?P<court>[^)]*(?:судебн\w*\s*уч|миров)[^)]*)\)\s*$",
    re.IGNORECASE | re.S,
)
_CASS_FI_CASE_NUM_RE = re.compile(
    r"Номер дела в первой инстанции:\s*([^\s<]+)", re.IGNORECASE
)
# Внутренний номер в первой ячейке: 7kas — «8Г-XXX/YYYY», президиум облсуда —
# «4Г-XXX/YYYY» (с 04.09.2026). Параллельный кассационный номер (88-XXX/YYYY
# у 7kas, «44Г-N/YYYY» у президиума — в квадратных скобках после внутреннего)
# тут не всегда показан — у 7kas берём из карточки, у президиума из скобок.
_CASS_INTERNAL_NUM_RE = re.compile(r"\d+[ГГ]-\d+/\d{4}")
_CASS_BRACKET_NUM_RE = re.compile(r"\[[^\]]*?(\d+[ГГ]-\d+/\d{4})")


def split_magistrate_judge(value: str) -> tuple[str, str]:
    """«Миненко Ю.В. (Судебный уч. №3, Ханты-Мансийский р-н)» → (судья, суд).

    Суд — строкой «Мировой судья (участок)»: реестра мировых судей у нас
    нет, а фронту/дайджесту нужна человекочитаемая подпись 1-й инстанции.
    Скобок с участком нет → ("" , "") — значит, это не мировой судья.
    """
    m = _MAGISTRATE_JUDGE_RE.match((value or "").strip())
    if not m:
        return "", ""
    judge = m.group("judge").strip().rstrip(",")
    court = m.group("court").strip()
    return judge, f"Мировой судья ({court})"


def parse_cassation_search_page(html: str) -> list[dict]:
    """Парсит страницу поиска 7kas.sudrf.ru (гражданская кассация).

    Особенности:
    - Только первая страница результатов (пагинация НЕ обходится).
    - Колонки: №(ссылка) | дата поступл. | category+cassator+fi_court+fi_num
      (объединённая) | … | (опционально) судья и результат.
    - HMAO-фильтр: оставляем только дела с 1-й инстанцией в одном из 20
      ХМАО-судов или Суд ХМАО-Югры. Остальные регионы 7-го округа отбрасываем.

    Возвращает список dict с case_id, case_uid, cassation_internal_number,
    filing_date, category, cassator, fi_court_long, fi_court_config,
    fi_case_number, и опционально result_text.
    """
    tables = extract_tables(html)
    results_table = _find_results_table(tables)
    if not results_table:
        log.warning("7kas: таблица результатов не найдена")
        return []

    found = []
    for row in results_table:
        if len(row) < 3:
            continue
        # Первая ячейка — внутренний номер 8Г-XXX/YYYY со ссылкой на карточку
        case_cell = row[0]
        case_text = cell_text(case_cell).strip()
        m_internal = _CASS_INTERNAL_NUM_RE.search(case_text)
        if not m_internal:
            continue  # заголовок или служебная строка
        cassation_internal_number = m_internal.group(0)
        # «4Г-17/2026 [44Г-2/2026]» — второй номер в скобках (после передачи
        # жалобы в президиум); _bare_case_number скобки не режет.
        m_bracket = _CASS_BRACKET_NUM_RE.search(case_text)
        cassation_number = m_bracket.group(1) if m_bracket else ""
        href = cell_href(case_cell)
        cid, cuid = "", ""
        if href:
            m_id = _CASE_ID_RE.search(href)
            m_uid = _CASE_UID_RE.search(href)
            if m_id:
                cid = m_id.group(1)
            if m_uid:
                cuid = m_uid.group(1)
        if not cid or not cuid:
            continue

        filing_date = cell_text(row[1]).strip() if len(row) > 1 else ""

        combined = cell_text(row[2]) if len(row) > 2 else ""
        category, cassator, fi_court_long, fi_case_number = "", "", "", ""
        fi_judge, fi_magistrate = "", False
        m = _CASS_CATEGORY_RE.search(combined)
        if m:
            category = m.group(1).strip().rstrip("→ \xa0")
        m = _CASS_CASSATOR_RE.search(combined)
        if m:
            cassator = m.group(1).strip().rstrip(". \xa0")
        m = _CASS_FI_COURT_RE.search(combined)
        if m:
            fi_court_long = m.group(1).strip().rstrip(". \xa0")
        else:
            m = _CASS_FI_MAGISTRATE_RE.search(combined)
            if m:
                fi_judge, fi_court_long = split_magistrate_judge(
                    m.group(1).strip().rstrip(". \xa0")
                )
                if fi_court_long:
                    fi_magistrate = True
                else:
                    fi_judge = m.group(1).strip().rstrip(". \xa0")
        m = _CASS_FI_CASE_NUM_RE.search(combined)
        if m:
            fi_case_number = m.group(1).strip().rstrip(". \xa0")

        # Результат рассмотрения и дата вынесения, если уже есть в выдаче
        # (в готовых делах сидят в td4..td6). На уровне поиска не критичны —
        # точный исход берём из карточки.
        result_text = ""
        for j in range(3, min(8, len(row))):
            t = cell_text(row[j]).strip()
            if t and any(kw in t.upper() for kw in (
                "ОСТАВЛЕНО", "УДОВЛЕТВОРЕН", "ОТМЕНЕН", "ИЗМЕНЕН",
                "ПРЕКРАЩЕН", "ВОЗВРАЩЕН", "ОТОЗВАН"
            )):
                result_text = t
                break

        # Фильтр по 1-й инстанции: только ХМАО.
        fi_court_config = match_hmao_first_instance(fi_court_long)
        # Сохраняем все, чтобы вышестоящий код мог логировать «отброшено N
        # не-ХМАО». На реальном прогоне non-ХМАО отсеивается до запроса карточки.

        found.append({
            "case_id": cid,
            "case_uid": cuid,
            "cassation_internal_number": cassation_internal_number,
            "cassation_number": cassation_number,
            "filing_date": filing_date,
            "category": category,
            "cassator": cassator,
            "fi_court_long": fi_court_long,
            "fi_court_config": fi_court_config,
            "fi_case_number": fi_case_number,
            "fi_judge": fi_judge,
            "fi_magistrate": fi_magistrate,
            "result_text": result_text,
        })

    return found


_CASS_ACT_DIV_RE = re.compile(
    r"<div[^>]*id=['\"]cont_doc1['\"][^>]*>(.*?)"
    r"(?=<div[^>]*id=['\"](?:cont|next|footer|copyright)['\"]|</body>)",
    re.S | re.I,
)
# Заголовок «Дело №88-XXXX/YYYY» в начале текста определения (7kas); у
# президиума облсуда — «Дело № 44Г-N/YYYY» / «4Г-N/YYYY».
_CASS_ACT_DELO_NUM_RE = re.compile(
    r"Дело\s*№\s*(88-?\d+/\d{4}|\d+[ГГ]-\d+/\d{4})", re.IGNORECASE
)
# Заголовок карточки «ДЕЛО № 4Г-66/2026» (div.casenumber) — у 7kas тот же
# блок несёт «8Г-…». Карточка президиума приходит в прогон БЕЗ строки выдачи
# (дамп → карточка → перечитка), поэтому номер читаем и с самой страницы.
_CASS_PAGE_NUM_RE = re.compile(r"ДЕЛО\s*№\s*(\d+[ГГ]-\d+/\d{4})", re.IGNORECASE)


def _extract_cassation_act_text(html: str) -> tuple[str, str]:
    """Извлечь текст определения из hidden div cont_doc1 на карточке 7kas.

    Возвращает (act_text, cassation_number_88) — текст и официальный касс.
    номер 88-XXXX/YYYY (если найден в заголовке акта). Если div пуст или
    короче 200 символов — возвращает ("", "")."""
    m = _CASS_ACT_DIV_RE.search(html)
    if not m:
        return "", ""
    body = m.group(1)
    body = _HTML_SCRIPT_RE.sub("", body)
    body = _HTML_STYLE_RE.sub("", body)
    text = _strip_html(body)
    if len(text) < 200:
        return "", ""
    cass_num = ""
    m_num = _CASS_ACT_DELO_NUM_RE.search(text)
    if m_num:
        cass_num = m_num.group(1)
        # Нормализуем: 88-XXXX (без знака №) — единый формат.
        if cass_num.startswith("88-"):
            pass
        elif cass_num.startswith("88") and len(cass_num) > 2:
            cass_num = "88-" + cass_num[2:]
    return text, cass_num


def classify_cassation_outcome(
    result_text: str,
    result_for_appeal: str = "",
    review_result: str = "",
) -> str:
    """Детерминированно мапнуть структурированные поля карточки 7kas
    в нормализованный enum исхода кассации.

    Источники (в порядке приоритета):
    - `result_text` — «Результат рассмотрения» (таблица ДЕЛО).
    - `result_for_appeal` — «Результат в отношении решения апел. инст.».
    - `review_result` — «Результат изучения жалобы» (таблица ЖАЛОБЫ).

    Значения enum (синхронизированы со схемой cassation блока):
    - cassation_dismissed_no_transfer — отказ в передаче в коллегию.
    - cassation_upheld — оставлено без изменения (жалоба отклонена).
    - cassation_modified — изменено.
    - cassation_reversed — отменено.
    - cassation_remanded — отменено и направлено на новое рассмотрение.
    - cassation_terminated — прекращено / возвращено / отозвано.
    - cassation_other — не удалось классифицировать (нестандартная формулировка).
    Пустая строка — если карточка ещё в производстве (нет финального исхода).
    """
    rt = (result_text or "").upper()
    rfa = (result_for_appeal or "").upper()
    rev = (review_result or "").upper()

    # 1) Отказ в передаче — определяется по ЖАЛОБЫ.review_result.
    if rev and "ОТКАЗАНО" in rev and "ПЕРЕДАЧ" in rev:
        return "cassation_dismissed_no_transfer"
    # 2) Возврат / прекращение / отзыв.
    for kw in ("ВОЗВРАЩЕН", "ПРЕКРАЩЕН", "ОТОЗВАН"):
        if kw in rt or kw in rev:
            return "cassation_terminated"
    # 3) Финальный исход после рассмотрения коллегией. Берём связку
    # result_text (что с жалобой) + result_for_appeal (что с актом апел.
    # или 1-й инст.).
    if rt and "ОСТАВЛЕНО" in rt and "УДОВЛЕТВОР" in rt:
        # Жалоба отклонена. Дальше различаем «без изменения» vs «отмена».
        if "БЕЗ ИЗМЕНЕНИЯ" in rfa:
            return "cassation_upheld"
        # «Жалоба отклонена», но апел. изменили — редко, но возможно (касс.
        # рассмотрел и оставил жалобу без удовл., но сама апел. была изменена).
        # Для нашего трекинга это всё равно «оставлено в силе».
        return "cassation_upheld"
    if rt and "УДОВЛЕТВОР" in rt:
        # Кассация удовлетворила жалобу — нужно понять, что стало с актом.
        if "НАПРАВЛ" in rfa or "НА НОВОЕ" in rfa:
            return "cassation_remanded"
        if "ОТМЕНЕН" in rfa:
            return "cassation_reversed"
        if "ИЗМЕНЕН" in rfa:
            return "cassation_modified"
        # Удовлетворили, но result_for_appeal пуст — считаем отменой.
        return "cassation_reversed"
    # 3b) Президиум облсуда пишет исход прямо в result_text без слова
    # «удовлетворено»: «СУДЕБНЫЙ ПРИКАЗ ОТМЕНЕН», «АПЕЛЛЯЦИОННОЕ ОПРЕДЕЛЕНИЕ
    # ОТМЕНЕНО - с направлением на новое рассмотрение» (выдача 04.09.2026).
    if rt and ("НАПРАВЛ" in rt or "НА НОВОЕ" in rt):
        return "cassation_remanded"
    if rt and "ОТМЕНЕН" in rt:
        return "cassation_reversed"
    if rt and "ИЗМЕНЕН" in rt and "БЕЗ ИЗМЕНЕНИЯ" not in rt:
        return "cassation_modified"
    if rt and "БЕЗ ИЗМЕНЕНИЯ" in rt:
        return "cassation_upheld"
    # 4) Без явного «оставлено/удовлетворено», но в rfa есть указание.
    if "НАПРАВЛ" in rfa or "НА НОВОЕ" in rfa:
        return "cassation_remanded"
    if "ОТМЕНЕН" in rfa:
        return "cassation_reversed"
    if "ИЗМЕНЕН" in rfa:
        return "cassation_modified"
    if "БЕЗ ИЗМЕНЕНИЯ" in rfa:
        return "cassation_upheld"
    # 5) Финальный исход не определяется — карточка в производстве.
    if rt or rfa:
        return "cassation_other"
    return ""


def cassation_remanded_to(result_for_appeal: str, act_text: str = "") -> str:
    """Определить, куда направлено дело при `cassation_remanded`.

    Возвращает 'first_instance' | 'appeal' | '' (неизвестно)."""
    rfa = (result_for_appeal or "").lower()
    txt = (act_text or "")[:3000].lower()  # Только начало акта — там обычно резолютивная часть.
    blob = rfa + " " + txt
    if "новое рассмотрение в суд первой инстанции" in blob or "в суд первой инстанции" in blob:
        return "first_instance"
    if "новое рассмотрение в суд апелляционной" in blob or "в суд апелляционной" in blob:
        return "appeal"
    # Президиум: «АПЕЛЛЯЦИОННОЕ ОПРЕДЕЛЕНИЕ ОТМЕНЕНО - с направлением на новое
    # рассмотрение» / «СУДЕБНЫЙ ПРИКАЗ ОТМЕНЕН …» — адресат по отменённому акту.
    if "апелляционное определение" in rfa:
        return "appeal"
    if "судебный приказ" in rfa or "мирово" in rfa:
        return "first_instance"
    if "первой инстанции" in rfa:
        return "first_instance"
    if "апелляционн" in rfa:
        return "appeal"
    return ""


# Готовые русские подписи для нормализованных enum-исходов кассации
# (см. classify_cassation_outcome). Используются в обеих ветках дайджеста —
# и LLM-контексте, и template-fallback, чтобы перевод не делал LLM
# (снижает риск галлюцинации). cassation_other / "" → пустая строка
# (вызывающая сторона пропускает блок «Итог»).
CASSATION_OUTCOME_RU: dict[str, str] = {
    "cassation_dismissed_no_transfer": "🚫 Отказ в передаче",
    "cassation_upheld": "Оставлено без изменения",
    "cassation_modified": "Изменено",
    "cassation_reversed": "Отменено",
    "cassation_remanded": "🔁 Отменено и направлено на новое рассмотрение",
    "cassation_terminated": "🛑 Прекращено / отозвано / возвращено",
    "cassation_other": "",
}


def _extract_cassation_terminated_reason(
    review_result: str, result_text: str
) -> str:
    """Извлечь короткую причину после разделителя в поле 7kas.

    Типичный формат: «<вердикт> - <причина>» (тире-минус с пробелами).
    Срезаем шумовой префикс «кассационные жалоба, представление» — он
    дублирует «жалоба возвращена», который уже стоит в метке.
    """
    for src in (review_result or "", result_text or ""):
        if not src:
            continue
        for sep in (" - ", " — "):
            if sep in src:
                reason = src.split(sep, 1)[1].strip()
                reason = re.sub(
                    r"^кассационн\S+\s+жалоб\S+,?\s*(представлени\S+\s*)?",
                    "", reason, flags=re.I,
                ).strip()
                return reason.rstrip(".").strip()
    return ""


def cassation_terminated_label(
    review_result: str, result_text: str = ""
) -> tuple[str, str]:
    """Расщепить общий enum cassation_terminated на конкретику + причину.

    Классификатор `classify_cassation_outcome` сваливает три исхода
    (возврат / прекращение / отзыв) в один enum, потому что state-machine
    архивации одинаков для всех трёх. Для дайджеста этого мало — юристу
    нужна конкретика. Парсим текст review_result/result_text и определяем,
    какой именно исход + извлекаем причину.

    Возвращает (label, reason). reason может быть пустой, если разделителя
    «<вердикт> - <причина>» в данных нет.
    """
    blob = ((result_text or "") + " " + (review_result or "")).upper()
    if "ВОЗВРАЩЕН" in blob:
        label = "🔚 Жалоба возвращена"
    elif "ПРЕКРАЩЕН" in blob:
        label = "🛑 Производство прекращено"
    elif "ОТОЗВАН" in blob:
        label = "↩️ Жалоба отозвана"
    else:
        # Страховка: ни один из трёх ключей не нашёлся, хотя классификатор
        # выставил cassation_terminated. Возвращаем общую метку.
        label = CASSATION_OUTCOME_RU["cassation_terminated"]
    reason = _extract_cassation_terminated_reason(review_result, result_text)
    return label, reason


def cassation_review_label(review_result: str, outcome: str = "") -> str:
    """Короткая русская метка для ранней стадии кассации, когда финальный
    `outcome` ещё не выставлен, но `review_result` (поле «Результат изучения
    жалобы» из таблицы ЖАЛОБЫ на 7kas) уже заполнен.

    Используется как готовая подпись «Итог:» в дайджесте, чтобы LLM не
    переводила длинные 7kas-формулировки сама.

    Возвращает пустую строку если `outcome` непустой (берём CASSATION_OUTCOME_RU)
    или формулировка не распознана.
    """
    if outcome:
        return ""
    rev = (review_result or "").upper()
    if not rev:
        return ""
    if "ОТКАЗАНО" in rev and "ПЕРЕДАЧ" in rev:
        return "🚫 Отказ в передаче"
    if "ВОЗВРАЩЕН" in rev:
        return "🛑 Возвращено"
    if "ВОЗБУЖДЕНО" in rev or "ПЕРЕДАН" in rev or "ПРИНЯТ" in rev:
        return "📥 Принято к производству"
    return ""


def parse_cassation_card(html: str, court_base_url: str = "") -> dict | None:
    """Парсит карточку гражданского касс. дела с 7kas.sudrf.ru.

    Возвращает dict с разобранными полями карточки или None, если карточка
    не парсится (нет блока «РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ»).

    Состав:
    - judicial_uid, filing_date, category, act_kind, judge, decision_date,
      result_text, result_for_appeal — из таблицы ДЕЛО.
    - fi_region_code, fi_court_long, fi_case_number, fi_decision_date,
      fi_judge, fi_court_config (CourtConfig из match_hmao_first_instance,
      None — если суд НЕ-ХМАО) — из таблицы РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ.
    - hearing_date, hearing_time, hearings (массив событий) — из СЛУШАНИЯ.
    - cassator, cassator_status, review_result — из ЖАЛОБЫ.
    - participants (список dict {role, name, inn?}), sber_present (bool),
      bank_role — из УЧАСТНИКИ + SBER_PATTERNS.
    - act_text, cassation_number, act_published — из hidden div cont_doc1.
    """
    if not html:
        return None
    info: dict = {
        "judicial_uid": "",
        "filing_date": "",
        "category": "",
        "act_kind": "",
        "from_supreme_court": "",
        "judge": "",
        "decision_date": "",
        "result_text": "",
        "result_for_appeal": "",
        "fi_region_code": "",
        "fi_court_long": "",
        "fi_case_number": "",
        "fi_decision_date": "",
        "fi_judge": "",
        "fi_court_config": None,
        "hearing_date": "",
        "hearing_time": "",
        "hearings": [],
        "cassator": "",
        "cassator_status": "",
        "review_result": "",
        "suspended_until": "",
        "suspended_event_date": "",
        "participants": [],
        "sber_present": False,
        "bank_role": "",
        "act_text": "",
        "cassation_number": "",
        "act_published": False,
        # Президиум облсуда (с 04.09.2026): номер с заголовка страницы,
        # домен суда (по нему блок cassation находит СВОЙ CourtConfig при
        # перечитке — иначе «откатился» бы на 7kas), признак мирового судьи.
        "page_case_number": "",
        "court_domain": "",
        "fi_magistrate": False,
    }
    if court_base_url:
        info["court_domain"] = (urlparse(court_base_url).hostname or "").lower()
    m_page = _CASS_PAGE_NUM_RE.search(html)
    if m_page:
        info["page_case_number"] = m_page.group(1)

    tables = extract_tables(html)
    # Раскладываем по семантическим заголовкам. Заголовки СЛУШАНИЯ/ЖАЛОБЫ/
    # УЧАСТНИКИ/РАССМОТРЕНИЕ — внутри первой строки соответствующих таблиц.
    # А вот заголовок «ДЕЛО» отрисовывается ВНЕ таблицы (рендерится поверх),
    # поэтому таблица ДЕЛО детектируется по сигнатурному полю «Уникальный
    # идентификатор дела» в первой ячейке.
    sections: dict[str, list] = {}
    for tbl in tables:
        if not tbl:
            continue
        first_row_text = " ".join(cell_text(c) for c in tbl[0]).strip().upper()
        # ДЕЛО — детект по «УНИКАЛЬНЫЙ ИДЕНТИФИКАТОР»
        if "УНИКАЛЬНЫЙ ИДЕНТИФИКАТОР" in first_row_text and "ДЕЛО" not in sections:
            sections["ДЕЛО"] = tbl
            continue
        for tag in (
            "РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ",
            "СЛУШАНИЯ",
            "ЖАЛОБЫ",
            "УЧАСТНИКИ",
        ):
            if first_row_text.startswith(tag) and tag not in sections:
                sections[tag] = tbl
                break

    # Без блока 1-й инст. карточка нам не нужна (нет ключа для линковки).
    fi_tbl = sections.get("РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ")
    if not fi_tbl:
        return None

    # ── Таблица ДЕЛО ─────────────────────────────────────────────────────
    # Особенность: row 0 имеет 3 ячейки (section_header + field_label + value),
    # row 1+ — обычные (field_label + value). Нормализуем до пар (key, val).
    delo_tbl = sections.get("ДЕЛО") or []
    for row in delo_tbl:
        if len(row) >= 3 and cell_text(row[0]).strip().upper() == "ДЕЛО":
            key = cell_text(row[1]).strip().rstrip(":").lower()
            val = cell_text(row[2]).strip()
        elif len(row) >= 2:
            key = cell_text(row[0]).strip().rstrip(":").lower()
            val = cell_text(row[1]).strip()
        else:
            continue
        if "уникальный идентификатор" in key:
            info["judicial_uid"] = val
        elif "дата поступления" in key:
            info["filing_date"] = val
        elif "категория" in key:
            info["category"] = val.replace("\xa0", " ").strip()
        elif "вид обжалуемого" in key:
            info["act_kind"] = val
        elif "из верховного суда" in key:
            info["from_supreme_court"] = val
        elif key == "судья":
            info["judge"] = val
        elif "дата рассмотрения" in key:
            info["decision_date"] = val
        elif "результат рассмотрения" in key:
            info["result_text"] = val
        elif "результат в отношении" in key:
            info["result_for_appeal"] = val

    # ── Таблица РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ ─────────────────────────
    for row in fi_tbl:
        if len(row) < 2:
            continue
        key = cell_text(row[0]).strip().rstrip(":").lower()
        val = cell_text(row[1]).strip()
        if "регион суда" in key:
            # Формат «86 - Ханты-Мансийский ...» — берём первое число.
            m = re.match(r"\s*(\d+)", val)
            if m:
                info["fi_region_code"] = m.group(1)
        elif "суд (судебный участок) первой" in key or "суд (мировой судья) первой" in key:
            # Иногда таблица содержит поле «Суд (мировой судья) первой
            # инстанции» вместо обычного. Захватываем оба варианта.
            if not info["fi_court_long"]:
                info["fi_court_long"] = val
        elif "номер дела в первой" in key:
            # 7kas иногда отдаёт «гибридный» номер `2-208/2026 (2-1148/2025;)`
            # — режем хвост, чтобы матч в `link_cassation_cases` сработал
            # независимо от формы, в которой у нас лежит id. Симметрия с
            # parse_case_card → re.search(r"\d+-\d+/\d{4}", value).
            info["fi_case_number"] = _bare_case_number(val) or val
        elif "дата решения первой" in key:
            info["fi_decision_date"] = val
        elif "судья (мировой судья) первой" in key or "судья первой" in key:
            if not info["fi_judge"]:
                info["fi_judge"] = val

    # Карточка президиума строки «Суд первой инстанции» не несёт — только
    # «Судья (мировой судья) первой инстанции: ФИО (участок)». Разводим ФИО и
    # участок: суд — строкой «Мировой судья (…)», реестра участков у нас нет.
    if not info["fi_court_long"] and info["fi_judge"]:
        judge, court = split_magistrate_judge(info["fi_judge"])
        if court:
            info["fi_judge"] = judge
            info["fi_court_long"] = court
            info["fi_magistrate"] = True

    info["fi_court_config"] = match_hmao_first_instance(info["fi_court_long"])

    # ── Таблица СЛУШАНИЯ ─────────────────────────────────────────────────
    sl_tbl = sections.get("СЛУШАНИЯ") or []
    for row in sl_tbl[2:]:  # row 0 — заголовок «СЛУШАНИЯ», row 1 — шапка колонок
        cells = [cell_text(c).strip() for c in row]
        if len(cells) < 2 or not cells[0]:
            continue
        ev = {
            "name": cells[0] if len(cells) > 0 else "",
            "date": cells[1] if len(cells) > 1 else "",
            "time": cells[2] if len(cells) > 2 else "",
            "place": cells[3] if len(cells) > 3 else "",
            "result_event": cells[4] if len(cells) > 4 else "",
            "ground": cells[5] if len(cells) > 5 else "",
            "note": cells[6] if len(cells) > 6 else "",
            "posted_at": cells[7] if len(cells) > 7 else "",
        }
        info["hearings"].append(ev)
        if ev["date"]:
            info["hearing_date"] = ev["date"]
        if ev["time"]:
            info["hearing_time"] = ev["time"]

    # ── Таблица ЖАЛОБЫ ───────────────────────────────────────────────────
    zh_tbl = sections.get("ЖАЛОБЫ") or []
    if len(zh_tbl) >= 3:
        # row 1 — шапка («Дата поступления», «Процесс. статус», «Заявитель», ...,
        # «Результат изучения жалобы»), row 2 — данные. Если строк больше —
        # берём последнюю (актуальная жалоба).
        data_row = [cell_text(c).strip() for c in zh_tbl[-1]]
        if len(data_row) >= 3:
            info["cassator_status"] = data_row[1]  # ИСТЕЦ/ОТВЕТЧИК
            info["cassator"] = data_row[2]
        # «Результат изучения» — последняя ячейка с непустым значением,
        # содержащим ключевые слова «возбуждено» / «отказано».
        for c in reversed(data_row):
            if c and any(
                kw in c.upper()
                for kw in ("ВОЗБУЖДЕНО", "ОТКАЗАНО", "ПЕРЕДАНО", "ВОЗВРАЩЕНО")
            ):
                info["review_result"] = c
                break
        # «Без движения»: фиксированный порядок колонок на 7kas:
        #   data_row[5] = «Дата опр. об оставл. жалобы без движения / напр. уведомления»
        #   data_row[6] = «Срок для устранения недостатков»
        # Слова «без движения» сидят в ЗАГОЛОВКЕ колонки, в данных — только
        # даты, поэтому regex-поиск по тексту не работает; читаем по индексу.
        if len(data_row) > 5 and data_row[5]:
            m_ev = _DATE_DDMMYYYY_RX.match(data_row[5])
            if m_ev:
                info["suspended_event_date"] = data_row[5]
                if len(data_row) > 6 and data_row[6]:
                    m_su = _DATE_DDMMYYYY_RX.match(data_row[6])
                    if m_su:
                        info["suspended_until"] = data_row[6]
                # Если суда столбца «Срок для устранения недостатков» нет
                # или он пуст — оставляем suspended_until="", а событие
                # «Жалоба оставлена без движения» добавляется ниже только
                # при наличии конкретного срока (см. блок hearings.append).

    # ── Таблица УЧАСТНИКИ ────────────────────────────────────────────────
    uch_tbl = sections.get("УЧАСТНИКИ") or []
    for row in uch_tbl[2:]:  # row 0 — заголовок, row 1 — шапка колонок
        cells = [cell_text(c).strip() for c in row]
        if len(cells) < 2 or not cells[0]:
            continue
        info["participants"].append({
            "role": cells[0],
            "name": cells[1],
            "inn": cells[2] if len(cells) > 2 else "",
        })

    # Sber-presence + bank_role через единый хелпер. Дочки (страхование/НПФ/
    # лизинг/факторинг/УК) фильтруются внутри _is_real_sberbank — параллель с
    # is_subsidiary_only_case. Если хелпер вернул "" — Сбербанка нет среди
    # участников, дело отбросим в link_cassation_cases.
    info["sber_present"] = any(_is_real_sberbank(p["name"]) for p in info["participants"])
    # synonyms=True: в кассации по приказному производству банк — ВЗЫСКАТЕЛЬ
    # (экономически истец), должник — ответная сторона; без синонимов роль
    # читалась бы «Третье лицо» (карточка президиума 4Г-66/2026).
    info["bank_role"] = determine_bank_role_from_participants(
        info["participants"], synonyms=True
    )

    # ── Жалоба оставлена без движения [до DD.MM.YYYY] ────────────────────
    # Основной путь — структурный парсинг колонок 5/6 таблицы ЖАЛОБЫ выше.
    # Этот блок — fallback на случай, если 7kas разместит маркер в другой
    # секции/формате (СЛУШАНИЯ result_event, ДЕЛО result_text и т.п.).
    # Запускается только если структурный парсинг не нашёл suspended_until.
    suspended_until = info.get("suspended_until", "")
    suspended_event_date = info.get("suspended_event_date", "")
    if not suspended_until:
        for section_name in ("ЖАЛОБЫ", "СЛУШАНИЯ", "ДЕЛО"):
            if suspended_until:
                break
            tbl = sections.get(section_name) or []
            for row in tbl:
                joined = " | ".join(cell_text(c) for c in row)
                if not _SUSPENDED_RX.search(joined):
                    continue
                raw_dates = _DATE_DDMMYYYY_RX.findall(joined)
                if not raw_dates:
                    continue
                try:
                    parsed = [date(int(y), int(m), int(d)) for d, m, y in raw_dates]
                except ValueError:
                    log.debug(
                        f"7kas: невалидная дата в строке «без движения» "
                        f"({section_name}): {joined!r}"
                    )
                    continue
                suspended_until = max(parsed).strftime("%d.%m.%Y")
                if len(parsed) > 1:
                    suspended_event_date = min(parsed).strftime("%d.%m.%Y")
                break
    if suspended_until:
        info["suspended_until"] = suspended_until
        if suspended_event_date and not info.get("suspended_event_date"):
            info["suspended_event_date"] = suspended_event_date
        # Дублируем статус в hearings, чтобы drawer нарисовал событие
        # в хронологии (buildTimeline → pushEvents). Smart-skip для кассации
        # работает через явное поле cassation.suspended_until (see
        # should_skip_case), event'ы для этого не нужны.
        has_existing = any(
            h
            and _SUSPENDED_RX.search(
                f"{h.get('name','')} {h.get('result_event','')} {h.get('note','')}"
            )
            for h in info["hearings"]
        )
        if not has_existing:
            # name содержит полный текст «… до DD.MM.YYYY», чтобы юрист видел
            # дедлайн прямо в timeline; cleanTimelineText (фронт) поправлен —
            # 4-значный год больше не съедается trailing-паттерном.
            # date — дата вынесения определения (когда оставили); fallback на
            # suspended_until если в строке нашлась только одна дата.
            info["hearings"].append({
                "name": f"Жалоба оставлена без движения до {suspended_until}",
                "date": suspended_event_date or suspended_until,
                "time": "",
                "place": "",
                "result_event": "",
                "ground": "",
                "note": "",
                "posted_at": "",
            })

    # ── Текст судебного акта (cont_doc1) ────────────────────────────────
    act_text, cass_num = _extract_cassation_act_text(html)
    info["act_text"] = act_text
    info["cassation_number"] = cass_num
    info["act_published"] = bool(act_text)

    return info
