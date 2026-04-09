#!/usr/bin/env python3
"""
Автоматический мониторинг судебных дел ПАО Сбербанк
Суд ХМАО-Югры (апелляция) — oblsud--hmao.sudrf.ru

Запускается по расписанию через GitHub Actions.
1. Читает текущий CSV из репозитория
2. Парсит первую страницу поиска (новые дела)
3. Обновляет карточки активных дел
4. Генерирует дайджест через Claude API
5. Отправляет в Telegram
6. Сохраняет обновлённый CSV
"""

import csv
import io
import json
import logging
import os
import re
import sys
import time
import random
from datetime import datetime, timedelta
from html import escape as html_escape
from html.parser import HTMLParser

import requests

# ── Настройки ────────────────────────────────────────────────────────────────

BASE_URL = "https://oblsud--hmao.sudrf.ru"
SEARCH_URL = (
    f"{BASE_URL}/modules.php?name=sud_delo&srv_num=1&name_op=r&delo_id=5"
    "&case_type=0&new=5&G2_PARTS__NAMESS=%D1%E1%E5%F0%E1%E0%ED%EA"
    "&delo_table=g2_case&Submit=%CD%E0%E9%F2%E8"
)
CARD_URL_TPL = (
    f"{BASE_URL}/modules.php?name=sud_delo&srv_num=1&name_op=case"
    "&case_id={case_id}&case_uid={case_uid}&delo_id=5&new=5"
)

CSV_PATH = os.environ.get("CSV_PATH", "data/sberbank_cases.csv")
DIGESTED_ACTS_PATH = os.environ.get(
    "DIGESTED_ACTS_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", ".digested_acts")
)
ARCHIVE_DAYS = 30  # Дела решённые 30+ дней назад не обновляем
REQUEST_DELAY = (2, 3)  # Задержка между запросами к суду (сек)
FETCH_MAX_RETRIES = 3   # Кол-во попыток загрузки страницы
DASHBOARD_URL = "https://selivanovas.github.io/dashboard/sberbank_dashboard.html"

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Лимит Telegram на одно сообщение
TELEGRAM_MSG_LIMIT = 4096

CSV_COLUMNS = [
    "Номер дела", "Дата поступления", "Истец", "Ответчик", "Категория",
    "Суд 1 инстанции", "Роль банка", "Статус", "Последнее событие",
    "Дата события", "Время заседания", "Акт опубликован", "Результат",
    "Ссылка", "Заметки", "Апеллянт", "Дата публикации акта", "Дата заседания"
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("court-monitor")

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
})


# ── Утилиты ──────────────────────────────────────────────────────────────────

def polite_delay():
    """Случайная задержка между запросами."""
    time.sleep(random.uniform(*REQUEST_DELAY))


def fetch_page(url: str) -> str:
    """Скачать страницу с сайта суда (win-1251) с повторными попытками."""
    for attempt in range(1, FETCH_MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=30)
            r.raise_for_status()
            return r.content.decode("windows-1251", errors="replace")
        except requests.RequestException as e:
            if attempt < FETCH_MAX_RETRIES:
                wait = attempt * 5
                log.warning(f"Попытка {attempt}/{FETCH_MAX_RETRIES} не удалась для {url}: {e}. Повтор через {wait}с...")
                time.sleep(wait)
            else:
                log.error(f"Ошибка загрузки {url} после {FETCH_MAX_RETRIES} попыток: {e}")
    return ""


def parse_date(s: str) -> datetime | None:
    """Парсинг даты формата ДД.ММ.ГГГГ."""
    s = s.strip()
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def load_digested_acts() -> set:
    """Загрузить множество номеров дел, чьи акты уже попали в дайджест."""
    if not os.path.exists(DIGESTED_ACTS_PATH):
        return set()
    with open(DIGESTED_ACTS_PATH, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def save_digested_acts(acts: set):
    """Сохранить множество номеров дел, чьи акты уже попали в дайджест."""
    os.makedirs(os.path.dirname(DIGESTED_ACTS_PATH) or ".", exist_ok=True)
    with open(DIGESTED_ACTS_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(acts)) + "\n")


def is_archived(case: dict) -> bool:
    """Дело архивное = решено более ARCHIVE_DAYS дней назад."""
    if case.get("Статус", "").strip() != "Решено":
        return False
    date_str = case.get("Дата события", "").strip()
    if not date_str:
        return False
    d = parse_date(date_str)
    if not d:
        return False
    return (datetime.now() - d).days > ARCHIVE_DAYS


def case_id_uid(link_str: str) -> tuple[str, str]:
    """Извлечь case_id и case_uid из поля Ссылка (формат 'id|uid')."""
    parts = link_str.strip().split("|")
    if len(parts) == 2:
        return parts[0], parts[1]
    return "", ""


def escape_html(text: str) -> str:
    """Экранировать спецсимволы HTML для Telegram."""
    return html_escape(str(text), quote=False)


def case_card_url(case: dict) -> str:
    """Построить полный URL карточки дела."""
    cid, cuid = case_id_uid(case.get("Ссылка", ""))
    if cid and cuid:
        return CARD_URL_TPL.format(case_id=cid, case_uid=cuid)
    return ""


def case_link_html(case: dict) -> str:
    """Номер дела как кликабельная HTML-ссылка (или просто текст, если нет URL)."""
    url = case_card_url(case)
    num = escape_html(case.get("Номер дела", "???"))
    if url:
        return f'<a href="{url}"><b>{num}</b></a>'
    return f'<b>{num}</b>'


def parties_short(case: dict) -> str:
    """Стороны в формате 'Истец (истец) vs Ответчик (ответчик)'."""
    plaintiff = escape_html(case.get("Истец", ""))
    defendant = escape_html(case.get("Ответчик", ""))
    return f"{plaintiff} (истец) vs {defendant} (ответчик)"


def extract_motive_part(act_text: str, max_len: int = 1000) -> str:
    """
    Извлечь мотивировочную часть из текста судебного акта.
    Ищем от 'установил(а):' до 'руководствуясь' / 'определила' — это суть решения.
    Если не нашли — берём последние max_len символов (ближе к резолюции).
    """
    if not act_text:
        return ""

    text = act_text.strip()

    # Пробуем вырезать мотивировочную часть
    # Коллегия пишет "установила:", судья — "установил:"
    start_match = re.search(
        r'(?:у\s*с\s*т\s*а\s*н\s*о\s*в\s*и\s*л\s*[аи]?\s*:|УСТАНОВИЛ[АИ]?\s*:)',
        text, re.IGNORECASE
    )
    end_match = re.search(
        r'(?:руководствуясь|РУКОВОДСТВУЯСЬ|на\s+основании\s+изложенного|'
        r'судебная\s+коллегия\s+(?:определила|приходит)|'
        r'о\s*п\s*р\s*е\s*д\s*е\s*л\s*и\s*л\s*[аи]?\s*:)',
        text, re.IGNORECASE
    )

    if start_match and end_match and end_match.start() > start_match.end():
        motive = text[start_match.end():end_match.start()].strip()
        if len(motive) > 100:  # Достаточно содержательный кусок
            return motive[:max_len]

    # Fallback 2: ищем хотя бы начало (установил(а):) и берём max_len символов после
    if start_match:
        after = text[start_match.end():].strip()
        if len(after) > 100:
            return after[:max_len]

    # Fallback 3: берём последнюю часть текста (ближе к решению)
    if len(text) > max_len:
        return "..." + text[-(max_len - 3):]
    return text


# ── Классификация итога апелляции и стороны ──────────────────────────────────

# Служебные движения карточки, которые НЕ являются содержательным изменением
# и не должны попадать в дайджест как "новое событие". Иначе LLM, видя у дела
# дату заседания и стороны, может выдумать секцию "вынесен судебный акт" с today.
SERVICE_EVENT_PATTERNS = (
    "мотивированн",                              # «составлено мотивированное определение/решение»
    "сдано в отдел судебного делопроизводства",
    "передано в экспедицию",
    "сдано в архив",
    "регистрация ап",                            # «регистрация апелляционной жалобы …»
)


def classify_verdict(result: str, last_event: str = "") -> str:
    """Возвращает короткий нормализованный ярлык итога апелляции.
    Принимает СЫРОЕ поле «Результат» из карточки суда + «Последнее событие»."""
    r = (result or "").lower()
    if "отменено полностью" in r and ("новым решением" in r or "новог" in r):
        return "решение отменено полностью, вынесено новое решение"
    if "отменено в части" in r:
        return "решение отменено в части"
    if "отменено полностью" in r:
        return "решение отменено полностью"
    if "изменено" in r:
        return "решение изменено"
    if "оставлено без изменения" in r:
        return "решение оставлено без изменения, жалоба — без удовлетворения"
    if "возвращен" in r:  # «Жалоба, представление возвращены заявителю»
        return "жалоба возвращена"
    if "без рассмотрения" in r:
        return "жалоба оставлена без рассмотрения"
    if "прекращено" in r:
        return "производство по жалобе прекращено"
    if "отказано в принятии" in r:
        return "отказано в принятии жалобы"
    if "снято с рассмотрения" in r:
        return "снято с рассмотрения"
    return (result or "").strip() or "итог не распознан"


def bank_side_outcome(role: str, appellant: str, verdict_label: str) -> str:
    """«в пользу банка» / «против банка» / «нейтрально (банк — третье лицо)» /
    «не определено»."""
    role_l = (role or "").lower()
    if "третье" in role_l:
        return "нейтрально (банк — третье лицо)"
    app = (appellant or "").strip().lower()
    if app not in ("банк", "иное лицо"):
        # При пустом/неизвестном апеллянте НЕ угадываем.
        return "не определено"
    appellant_is_bank = (app == "банк")
    upheld = "оставлено без изменения" in verdict_label
    overturned = ("отменено" in verdict_label) or ("изменено" in verdict_label)
    returned = ("возвращена" in verdict_label
                or "без рассмотрения" in verdict_label
                or "прекращено" in verdict_label
                or "отказано в принятии" in verdict_label)
    if returned or upheld:
        return "против банка" if appellant_is_bank else "в пользу банка"
    if overturned:
        return "в пользу банка" if appellant_is_bank else "против банка"
    return "не определено"



# ── Простой HTML-парсер для извлечения таблиц ────────────────────────────────

class TableExtractor(HTMLParser):
    """Извлекает все <table> со страницы как списки строк (списков ячеек)."""

    def __init__(self):
        super().__init__()
        self.tables = []
        self._current_table = None
        self._current_row = None
        self._current_cell = None
        self._in_cell = False
        self._cell_tag = None
        # Для извлечения href из ссылок внутри ячеек
        self._current_href = ""

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table":
            self._current_table = []
        elif tag == "tr" and self._current_table is not None:
            self._current_row = []
        elif tag in ("td", "th") and self._current_row is not None:
            self._current_cell = ""
            self._in_cell = True
            self._cell_tag = tag
            self._current_href = ""
        elif tag == "a" and self._in_cell:
            self._current_href = attrs_dict.get("href", "")

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell += data

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._in_cell:
            cell_text = self._current_cell.strip()
            # Сохраняем href если есть, через специальный маркер
            if self._current_href:
                cell_text = f"{cell_text}\x00HREF:{self._current_href}"
            if self._current_row is not None:
                self._current_row.append(cell_text)
            self._in_cell = False
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None:
            if self._current_table is not None:
                self._current_table.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._current_table is not None:
            self.tables.append(self._current_table)
            self._current_table = None


def extract_tables(html: str) -> list:
    """Извлечь все таблицы из HTML."""
    parser = TableExtractor()
    parser.feed(html)
    return parser.tables


def cell_text(cell: str) -> str:
    """Получить текст ячейки (без href-маркера)."""
    return cell.split("\x00HREF:")[0].strip() if cell else ""


def cell_href(cell: str) -> str:
    """Получить href из ячейки."""
    if "\x00HREF:" in cell:
        return cell.split("\x00HREF:")[1].strip()
    return ""


# ── Парсинг страницы поиска ──────────────────────────────────────────────────

def _parse_combined_cell(text: str) -> dict:
    """
    Разбирает объединённую ячейку с категорией, сторонами и судом.
    Формат: 'КАТЕГОРИЯ: ...ИСТЕЦ(ЗАЯВИТЕЛЬ): ...ОТВЕТЧИК: ...Суд ... первой инстанции: ...'
    """
    result = {"category": "", "plaintiff": "", "defendant": "", "court": ""}

    m = re.search(r"КАТЕГОРИЯ:\s*(.+?)(?=ИСТЕЦ|ЗАЯВИТЕЛЬ|ОТВЕТЧИК|Суд\s|$)", text)
    if m:
        result["category"] = m.group(1).strip().rstrip("→ \xa0")

    m = re.search(r"(?:ИСТЕЦ|ЗАЯВИТЕЛЬ)\(?[^)]*\)?:\s*(.+?)(?=ОТВЕТЧИК|Суд\s|Номер дела|$)", text)
    if m:
        result["plaintiff"] = m.group(1).strip()

    m = re.search(r"ОТВЕТЧИК:\s*(.+?)(?=Суд\s|Номер дела|$)", text)
    if m:
        result["defendant"] = m.group(1).strip()

    m = re.search(r"Суд\s*\([^)]*\)\s*первой инстанции:\s*(.+?)(?=Номер дела|$)", text)
    if m:
        result["court"] = m.group(1).strip()

    return result


# Паттерны страховых дочерних компаний Сбербанка
_SBER_INSURANCE_RE = re.compile(
    r'(ооо\s+)?с[кc]\s+[«""\"]?сбербанк\s+страхован\w*(\s+жизн\w*)?[»""\"]?'
    r'|[«""\"]?сбербанк\s+страхован\w*(\s+жизн\w*)?[»""\"]?\s*(с[кc]\s+)?ооо',
    re.IGNORECASE,
)


def is_insurance_only_case(plaintiff: str, defendant: str) -> bool:
    """Вернуть True, если «сбербанк» упоминается только в названии страховой компании.

    Если «сбербанк» вообще не встречается в сторонах — возвращаем False
    (дело найдено по поиску, значит банк упомянут где-то ещё, например как третье лицо).
    """
    combined = (plaintiff + " " + defendant).lower()
    if "сбербанк" not in combined:
        return False
    cleaned = _SBER_INSURANCE_RE.sub("", combined)
    return "сбербанк" not in cleaned


def parse_search_page(html: str) -> list[dict]:
    """
    Парсит страницу результатов поиска.
    Таблица результатов — 6-я на странице (индекс 5).
    Столбцы: Номер дела (ссылка) | Дата поступления |
             Категория/Стороны/Суд (объединённая) | Судья | ...
    """
    tables = extract_tables(html)
    if len(tables) < 6:
        log.warning(f"Ожидалось ≥6 таблиц, найдено {len(tables)}")
        return []

    results_table = tables[5]
    cases = []

    for row in results_table:
        if len(row) < 3:
            continue

        # Первый столбец — номер дела со ссылкой
        case_number_cell = row[0]
        case_number = cell_text(case_number_cell)

        # Пропускаем заголовок и строки без номера дела
        if not re.match(r"\d+-\d+/\d{4}", case_number):
            continue

        href = cell_href(case_number_cell)

        # Извлекаем case_id и case_uid из href
        cid, cuid = "", ""
        if href:
            m_id = re.search(r"case_id=(\d+)", href)
            m_uid = re.search(r"case_uid=([a-f0-9\-]+)", href)
            if m_id:
                cid = m_id.group(1)
            if m_uid:
                cuid = m_uid.group(1)

        date_received = cell_text(row[1]) if len(row) > 1 else ""

        # Третий столбец — объединённая ячейка с категорией, сторонами и судом
        combined = cell_text(row[2]) if len(row) > 2 else ""
        parsed = _parse_combined_cell(combined)
        category = parsed["category"]
        plaintiff = parsed["plaintiff"]
        defendant = parsed["defendant"]
        court = parsed["court"]

        # Пропускаем дела, где «Сбербанк» — только страховая компания
        if is_insurance_only_case(plaintiff, defendant):
            log.info(f"Пропуск дела {case_number}: только Сбербанк Страхование")
            continue

        # Определяем роль банка
        role = "Третье лицо"
        sber_patterns = ["сбербанк", "сбербанк россии", "пао сбербанк"]
        plaintiff_lower = plaintiff.lower()
        defendant_lower = defendant.lower()
        if any(p in plaintiff_lower for p in sber_patterns):
            role = "Истец"
        elif any(p in defendant_lower for p in sber_patterns):
            role = "Ответчик"

        link = f"{cid}|{cuid}" if cid and cuid else ""

        cases.append({
            "Номер дела": case_number,
            "Дата поступления": date_received,
            "Истец": plaintiff,
            "Ответчик": defendant,
            "Категория": category,
            "Суд 1 инстанции": court,
            "Роль банка": role,
            "Статус": "В производстве",
            "Последнее событие": "",
            "Дата события": "",
            "Время заседания": "",
            "Акт опубликован": "Нет",
            "Результат": "",
            "Ссылка": link,
            "Заметки": "",
            "Апеллянт": "",
            "Дата публикации акта": "",
        })

    return cases


# ── Парсинг карточки дела ────────────────────────────────────────────────────

def parse_case_card(html: str) -> dict:
    """
    Парсит карточку дела. Извлекает:
    - Последнее событие и дату из таблицы ДВИЖЕНИЕ ДЕЛА (table 6, индекс 5-6)
    - Результат из таблицы ДЕЛО (table 4, индекс 3)
    - Наличие опубликованного акта
    - Текст судебного акта (если есть)
    """
    info = {
        "Последнее событие": "",
        "Дата события": "",
        "Время заседания": "",
        "Статус": "В производстве",
        "Результат": "",
        "Акт опубликован": "Нет",
        "Дата публикации акта": "",
        "act_text": "",  # Текст акта (для дайджеста, не сохраняется в CSV)
        "_appellant_raw": "",  # Сырой текст об апеллянте (для определения в update_active_cases)
    }

    tables = extract_tables(html)
    if len(tables) < 6:
        log.warning(f"Карточка: ожидалось ≥6 таблиц, найдено {len(tables)}")
        return info

    # ── Таблица ДЕЛО (обычно индекс 3) ──
    # Ищем таблицу с результатом рассмотрения
    for tbl_idx in range(min(5, len(tables))):
        tbl = tables[tbl_idx]
        for row in tbl:
            row_text = " ".join(cell_text(c) for c in row).lower()
            if "результат" in row_text and len(row) >= 2:
                result_text = cell_text(row[-1]).strip()
                if result_text and result_text.lower() not in ("результат", ""):
                    info["Результат"] = result_text

    # ── Таблица ДВИЖЕНИЕ ДЕЛА (обычно индекс 5 или 6) ──
    # Ищем таблицу с событиями: содержит столбцы "Событие" / "Дата"
    movement_table = None
    for tbl_idx in range(len(tables)):
        tbl = tables[tbl_idx]
        if len(tbl) > 1:
            header = " ".join(cell_text(c) for c in tbl[0]).lower()
            if "событие" in header or "движение" in header:
                movement_table = tbl
                break
            # Также ищем по наличию типичных событий
            for row in tbl[1:3]:
                row_text = " ".join(cell_text(c) for c in row).lower()
                if any(kw in row_text for kw in [
                    "передача", "заседание", "экспедиц", "делопроизводств"
                ]):
                    movement_table = tbl
                    break
            if movement_table:
                break

    if movement_table and len(movement_table) > 1:
        # Последняя строка данных = последнее событие
        events_data = []
        for row in movement_table[1:]:  # Пропускаем заголовок
            if len(row) >= 2:
                event_text_parts = []
                date_val = ""
                time_val = ""
                for c in row:
                    ct = cell_text(c)
                    d = parse_date(ct)
                    if d and not date_val:
                        date_val = ct
                    else:
                        # Ищем время в ячейке (формат HH:MM или H:MM)
                        time_match = re.search(r'\b(\d{1,2}:\d{2})\b', ct)
                        if time_match and not time_val:
                            time_val = time_match.group(1)
                        if ct:
                            event_text_parts.append(ct)
                event_desc = ". ".join(event_text_parts).strip(". ")
                if event_desc:
                    events_data.append((date_val, time_val, event_desc))

        if events_data:
            last_date, last_time, last_event = events_data[-1]
            info["Последнее событие"] = last_event
            info["Дата события"] = last_date
            # Время заседания — только из событий-заседаний, не из "сдано в отдел"
            for ev_date, ev_time, ev_desc in reversed(events_data):
                if "заседани" in ev_desc.lower() and ev_time:
                    info["Время заседания"] = ev_time
                    break
            # Дата заседания — ищем последнее заседание
            for ev_date, ev_time, ev_desc in reversed(events_data):
                if "заседани" in ev_desc.lower() and ev_date:
                    info["Дата заседания"] = ev_date
                    break
            # Если заседания не было — ищем дату определения/решения
            # (для дел снятых с рассмотрения, прекращённых, возвращённых)
            if not info.get("Дата заседания"):
                decision_kw = ["определени", "снято", "прекращен", "возвращен",
                               "без изменени", "отменен", "изменен"]
                for ev_date, ev_time, ev_desc in reversed(events_data):
                    ev_low = ev_desc.lower()
                    if ev_date and any(kw in ev_low for kw in decision_kw):
                        info["Дата заседания"] = ev_date
                        break

    # ── Определяем апеллянта ──
    # 1. Ищем в таблицах карточки: поле "Заявитель жалобы" / "Податель жалобы"
    appellant_raw = ""
    for tbl_idx in range(min(len(tables), 8)):
        tbl = tables[tbl_idx]
        for row in tbl:
            row_text = " ".join(cell_text(c) for c in row).lower()
            if any(kw in row_text for kw in [
                "заявитель жалобы", "податель жалобы", "апеллянт",
                "лицо, подавшее жалобу", "кто подал жалобу",
            ]) and len(row) >= 2:
                val = cell_text(row[-1]).strip()
                if val and val.lower() not in (
                    "заявитель жалобы", "податель жалобы", "апеллянт",
                    "лицо, подавшее жалобу", "кто подал жалобу", "",
                ):
                    appellant_raw = val
                    break
        if appellant_raw:
            break

    # 2. Ищем в событиях движения дела: "поступила жалоба от ..."
    if not appellant_raw and movement_table and len(movement_table) > 1:
        for row in movement_table[1:]:
            ev = " ".join(cell_text(c) for c in row)
            m = re.search(
                r'(?:поступи\w+|подан\w+|принят\w+)\s+'
                r'(?:апелляционн\w+\s+)?жалоб\w+\s+'
                r'(?:от\s+)?(.{3,80}?)(?:\.|,|$)',
                ev, re.IGNORECASE,
            )
            if m:
                appellant_raw = m.group(1).strip()
                break
            # Альтернативный паттерн: "жалоба ФИО / наименование"
            m2 = re.search(
                r'жалоб\w+\s+(.{3,80}?)'
                r'(?:\s+на\s+решение|\s+на\s+определение|\.|,|$)',
                ev, re.IGNORECASE,
            )
            if m2:
                candidate = m2.group(1).strip()
                # Исключаем служебные слова
                if not re.match(
                    r'^(без движения|оставлен|возвращен|на решение|'
                    r'на определение|рассмотрен)',
                    candidate, re.IGNORECASE,
                ):
                    appellant_raw = candidate
                    break

    # 3. Ищем в полном HTML: паттерн "апелляционная жалоба ... (имя)"
    if not appellant_raw:
        m = re.search(
            r'(?:апелляционн\w+\s+)?жалоб\w+\s+(?:от\s+)?'
            r'([А-ЯЁа-яё][А-ЯЁа-яё\s.\-]{2,60}?)'
            r'(?:\s+на\s+решение|\s+на\s+определение|<|,)',
            html, re.IGNORECASE,
        )
        if m:
            appellant_raw = m.group(1).strip()

    info["_appellant_raw"] = appellant_raw

    # ── Определяем статус ──
    result = info["Результат"].lower()
    last_event = info["Последнее событие"].lower()
    resolved_keywords = [
        "без изменения", "отменено", "изменено", "снято с рассмотрения",
        "прекращено", "оставлено без рассмотрения", "возвращено",
        "передано в экспедицию", "сдано в отдел"
    ]
    if any(kw in result for kw in resolved_keywords):
        info["Статус"] = "Решено"
    elif any(kw in last_event for kw in ["экспедиц", "делопроизводств"]):
        info["Статус"] = "Решено"

    # ── Судебный акт ──
    html_lower = html.lower()

    # Способ 1: Текст акта встроен в страницу (вкладка «Судебные акты», div#cont5)
    # Это основной способ для сайта oblsud--hmao.sudrf.ru
    # Ищем div с id=cont_doc1 (текст первого судебного акта)
    doc_match = re.search(
        r"""id\s*=\s*['"]?cont_doc1['"]?[^>]*>(.+?)"""
        r"""(?=<div[^>]*id\s*=\s*['"]?cont_doc\d|<div[^>]*id\s*=\s*['"]?cont[^_]|$)""",
        html, re.DOTALL
    )
    if doc_match:
        act_raw = doc_match.group(1)
        act_text = re.sub(r'<[^>]+>', ' ', act_raw)
        act_text = re.sub(r'&nbsp;', ' ', act_text)
        act_text = re.sub(r'\s+', ' ', act_text).strip()
        if len(act_text) > 200:
            info["Акт опубликован"] = "Да"
            info["act_text"] = act_text[:8000]

    # Способ 2: Ссылка на отдельную страницу с текстом акта
    if not info["act_text"] and ("судебный акт" in html_lower or "текст акта" in html_lower):
        act_match = re.search(
            r'href="([^"]*(?:act_text|print_page|case_doc)[^"]*)"',
            html, re.IGNORECASE
        )
        if act_match:
            info["Акт опубликован"] = "Да"
            act_url = act_match.group(1)
            if not act_url.startswith("http"):
                act_url = BASE_URL + "/" + act_url.lstrip("/")
            info["_act_url"] = act_url

    # Способ 3: Блок <div> с текстом акта (class содержит "act")
    if not info["act_text"]:
        act_div_match = re.search(
            r'<div[^>]*class="[^"]*act[^"]*"[^>]*>(.*?)</div>',
            html, re.DOTALL | re.IGNORECASE
        )
        if act_div_match:
            act_text = re.sub(r'<[^>]+>', ' ', act_div_match.group(1))
            act_text = re.sub(r'\s+', ' ', act_text).strip()
            if len(act_text) > 50:
                info["Акт опубликован"] = "Да"
                info["act_text"] = act_text[:8000]

    # Определяем наличие вкладки «Судебные акты» даже без текста
    if not info.get("act_text") and "СУДЕБНЫЕ АКТЫ" in html:
        info["Акт опубликован"] = "Да"

    # Также ищем по паттерну "Опубликовано" + дата
    # Исключаем блок publishInfo (метаинформация страницы, не акт)
    html_no_pubinfo = re.sub(
        r'<div[^>]*class="[^"]*publishInfo[^"]*"[^>]*>.*?</div>',
        '', html, flags=re.DOTALL | re.IGNORECASE
    )
    pub_match = re.search(
        r'(?:опубликован|дата публикации)[^<]*?(\d{2}\.\d{2}\.\d{4})',
        html_no_pubinfo, re.IGNORECASE
    )
    if pub_match:
        pub_date_str = pub_match.group(1)
        info["Акт опубликован"] = "Да"
        info["Дата публикации акта"] = pub_date_str

    return info


def fetch_act_text(act_url: str) -> str:
    """Скачать текст судебного акта по URL."""
    polite_delay()
    html = fetch_page(act_url)
    if not html:
        return ""
    # Убираем теги, извлекаем текст
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:5000]  # Сырой текст, обрезается позже


def next_tuesday(from_date: datetime | None = None) -> datetime:
    """Вычислить дату ближайшего вторника (включая сегодня, если сегодня вторник)."""
    d = from_date or datetime.now()
    # weekday(): 0=пн, 1=вт, 2=ср, ...
    days_until_tuesday = (1 - d.weekday()) % 7
    if days_until_tuesday == 0 and d.hour >= 18:
        # Если сегодня вторник, но уже вечер — берём следующий
        days_until_tuesday = 7
    return (d + timedelta(days=days_until_tuesday)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def is_wednesday() -> bool:
    """Сегодня среда?"""
    return datetime.now().weekday() == 2


def get_upcoming_hearings(cases: list[dict]) -> list[dict]:
    """
    Найти дела с назначенными заседаниями с четверга по следующую среду.
    Вызывать только по средам (проверка is_wednesday() — на стороне вызывающего кода).
    Возвращает список дел, отсортированный по времени заседания.
    """
    today = datetime.now().date()
    range_start = today + timedelta(days=1)   # четверг
    range_end = today + timedelta(days=7)     # следующая среда
    upcoming = []

    hearing_keywords = ["заседание", "назначено", "слушание", "рассмотрение"]

    for case in cases:
        if is_archived(case):
            continue
        if case.get("Статус", "").strip() == "Решено":
            continue
        # Пропускаем приостановленные дела
        event_low = case.get("Последнее событие", "").lower()
        if "приостановлен" in event_low:
            continue

        event = event_low
        date_str = case.get("Дата события", "").strip()

        if not date_str:
            continue

        d = parse_date(date_str)
        if not d:
            continue

        # Попадает в диапазон среда — следующий вторник
        if range_start <= d.date() <= range_end:
            if any(kw in event for kw in hearing_keywords):
                upcoming.append(case)

    # Сортируем по времени заседания (дела без времени — в конец)
    def sort_key(c):
        t = c.get("Время заседания", "").strip()
        if t:
            try:
                parts = t.split(":")
                return int(parts[0]) * 60 + int(parts[1])
            except (ValueError, IndexError):
                pass
        return 9999  # Без времени — в конец

    upcoming.sort(key=sort_key)
    return upcoming


def build_summary_line(new_cases: list[dict], changes: list[dict]) -> str:
    """Сводка-саммари одной строкой: +N новых, M событий, K решений, L актов."""
    parts = []
    if new_cases:
        parts.append(f"+{len(new_cases)} нов.")
    events = sum(1 for ch in changes if "new_event" in ch["type"])
    results = sum(1 for ch in changes if "new_result" in ch["type"])
    acts = sum(1 for ch in changes if "new_act" in ch["type"])
    statuses = sum(1 for ch in changes if "status_change" in ch["type"])
    postponed = sum(1 for ch in changes if "hearing_postponed" in ch["type"])
    if events:
        parts.append(f"{events} событ.")
    if postponed:
        parts.append(f"{postponed} отлож.")
    if results:
        parts.append(f"{results} суд. акт.")
    if acts:
        parts.append(f"{acts} акт.")
    if statuses:
        parts.append(f"{statuses} смена статуса")
    return " | ".join(parts) if parts else "без изменений"


def category_short(cat: str) -> str:
    """Сокращённое название категории для компактного вывода."""
    cat_lower = cat.lower().strip()
    mapping = {
        "кредитные правоотношения": "кредит",
        "о взыскании": "взыскание",
        "трудовые споры": "труд. спор",
        "о защите прав потребителей": "защ. потребителей",
        "жилищные споры": "жилищн. спор",
        "страховые правоотношения": "страхование",
        "наследственные дела": "наследство",
    }
    for key, short in mapping.items():
        if key in cat_lower:
            return short
    # Если не нашли — обрезаем до 20 символов
    if len(cat) > 22:
        return cat[:20] + "…"
    return cat


# ── Основная логика обновления ───────────────────────────────────────────────

def load_csv(path: str) -> list[dict]:
    """Загрузить CSV в список словарей."""
    if not os.path.exists(path):
        log.warning(f"CSV не найден: {path}")
        return []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def save_csv(cases: list[dict], path: str):
    """Сохранить список словарей в CSV."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cases)
    log.info(f"CSV сохранён: {path} ({len(cases)} дел)")


def find_new_cases(search_cases: list[dict], existing_numbers: set) -> list[dict]:
    """Найти дела из поиска, которых нет в текущей базе."""
    new = []
    for c in search_cases:
        num = c.get("Номер дела", "").strip()
        if num and num not in existing_numbers:
            new.append(c)
    return new


def update_active_cases(cases: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Обновить карточки активных (не архивных) дел.
    Возвращает (обновлённые_дела, список_изменений).
    """
    _digested_acts = load_digested_acts()
    changes = []

    for case in cases:
        if is_archived(case):
            continue

        cid, cuid = case_id_uid(case.get("Ссылка", ""))
        if not cid or not cuid:
            continue

        url = CARD_URL_TPL.format(case_id=cid, case_uid=cuid)
        polite_delay()
        html = fetch_page(url)
        if not html:
            log.warning(f"Не удалось загрузить карточку {case['Номер дела']}")
            continue

        card_info = parse_case_card(html)

        # Сравниваем и фиксируем изменения
        old_status = case.get("Статус", "")
        old_event = case.get("Последнее событие", "")
        old_act = case.get("Акт опубликован", "")
        old_result = case.get("Результат", "")

        new_status = card_info.get("Статус", old_status)
        new_event = card_info.get("Последнее событие", "")
        new_act = card_info.get("Акт опубликован", old_act)
        new_result = card_info.get("Результат", "")

        change = {"case": case["Номер дела"], "type": [], "details": {}}

        # Новый статус
        if new_status != old_status and new_status:
            change["type"].append("status_change")
            change["details"]["old_status"] = old_status
            change["details"]["new_status"] = new_status

        # Новое событие
        if new_event and new_event != old_event:
            # Не создаём new_event для служебных движений (мотивированное
            # определение, передача в экспедицию/архив, сдача в отдел
            # делопроизводства, регистрация апелляционной жалобы). Иначе LLM,
            # видя у дела дату заседания и стороны, фантазирует «вынесен
            # судебный акт» с today.
            ev_l = new_event.lower()
            if not any(p in ev_l for p in SERVICE_EVENT_PATTERNS):
                change["type"].append("new_event")
                change["details"]["event"] = new_event
                change["details"]["event_date"] = card_info.get("Дата события", "")
                change["details"]["hearing_date"] = card_info.get("Дата заседания", "")

        # Новый акт
        act_text = card_info.get("act_text", "")
        if not act_text and card_info.get("_act_url"):
            act_text = fetch_act_text(card_info["_act_url"])
        if new_act == "Да" and old_act != "Да":
            change["type"].append("new_act")
            change["details"]["act_text"] = extract_motive_part(act_text, 1000)
        elif (new_act == "Да" and old_act == "Да"
              and act_text
              and case["Номер дела"] not in _digested_acts):
            # Акт уже был помечен ранее, но текст не извлекался.
            # Добавляем в дайджест один раз.
            motive = extract_motive_part(act_text, 1000)
            if motive and len(motive) > 100:
                change["type"].append("new_act")
                change["details"]["act_text"] = motive

        # Новый результат
        if new_result and new_result != old_result:
            change["type"].append("new_result")
            change["details"]["result"] = new_result
            # Обогащаем контекст: дата заседания, последнее событие
            # (содержит причину возврата/прекращения), фрагмент мотивировки
            change["details"]["hearing_date"] = card_info.get("Дата заседания", "")
            change["details"]["last_event"] = new_event
            if act_text:
                change["details"]["act_excerpt"] = extract_motive_part(act_text, 600)
            # Нормализованный ярлык — модель должна использовать его дословно,
            # а не пересказывать сырое поле «Результат» своими словами.
            change["details"]["verdict_label"] = classify_verdict(
                new_result, new_event
            )
            # Флаг «заседание состоялось давно»: если карточка обновилась
            # с большим лагом после самого заседания, читателю важно увидеть
            # реальную дату, а не сегодняшнюю.
            hd = parse_date(card_info.get("Дата заседания", ""))
            if hd and (datetime.now() - hd) > timedelta(days=5):
                change["details"]["hearing_long_ago"] = True

        # Отложение заседания: было назначено заседание на дату X,
        # теперь — на другую дату Y, при этом дело по-прежнему в производстве
        # (нет new_result). Для апелляции это редкое и важное событие.
        old_hearing = case.get("Дата заседания", "").strip()
        new_hearing = card_info.get("Дата заседания", "").strip()
        old_hearing_time = case.get("Время заседания", "").strip()
        new_hearing_time = card_info.get("Время заседания", "").strip()
        old_h_dt = parse_date(old_hearing)
        new_h_dt = parse_date(new_hearing)
        if (old_h_dt and new_h_dt
                and new_h_dt.date() != old_h_dt.date()
                and new_status != "Решено"
                and not new_result):
            change["type"].append("hearing_postponed")
            change["details"]["old_hearing_date"] = old_hearing
            change["details"]["old_hearing_time"] = old_hearing_time
            change["details"]["new_hearing_date"] = new_hearing
            change["details"]["new_hearing_time"] = new_hearing_time

        # Обновляем поля дела
        if new_event:
            case["Последнее событие"] = new_event
        if card_info.get("Дата события"):
            case["Дата события"] = card_info["Дата события"]
        # Обновляем время заседания (может быть пустым если событие — не заседание)
        case["Время заседания"] = card_info.get("Время заседания", "")
        if new_status:
            case["Статус"] = new_status
        if new_result:
            case["Результат"] = new_result
        if new_act == "Да":
            case["Акт опубликован"] = "Да"
        if card_info.get("Дата публикации акта"):
            case["Дата публикации акта"] = card_info["Дата публикации акта"]
        if card_info.get("Дата заседания"):
            case["Дата заседания"] = card_info["Дата заседания"]

        # ── Определяем апеллянта ──
        appellant_raw = card_info.get("_appellant_raw", "")
        if appellant_raw and not case.get("Апеллянт"):
            sber_patterns = ["сбербанк", "сбербанк россии", "пао сбербанк", "пао сбер"]
            raw_lower = appellant_raw.lower()
            if any(p in raw_lower for p in sber_patterns):
                case["Апеллянт"] = "Банк"
            else:
                case["Апеллянт"] = "Иное лицо"

        if change["type"]:
            change["details"]["plaintiff"] = case.get("Истец", "")
            change["details"]["defendant"] = case.get("Ответчик", "")
            change["details"]["role"] = case.get("Роль банка", "")
            change["details"]["category"] = case.get("Категория", "")
            change["details"]["appellant"] = case.get("Апеллянт", "")
            change["details"]["case_url"] = case_card_url(case)
            # bank_outcome считаем только когда есть нормализованный verdict_label —
            # т.е. в этом change есть new_result. Зависит от роли + апеллянта.
            if "new_result" in change["type"]:
                change["details"]["bank_outcome"] = bank_side_outcome(
                    change["details"]["role"],
                    change["details"]["appellant"],
                    change["details"].get("verdict_label", ""),
                )
            changes.append(change)

        # Запоминаем дела, чьи акты вошли в дайджест
        if "new_act" in change["type"]:
            _digested_acts.add(case["Номер дела"])

        log.info(f"  {case['Номер дела']}: {'→ '.join(change['type']) or 'без изменений'}")

    save_digested_acts(_digested_acts)
    return cases, changes


# ── Сокращение наименований сторон ────────────────────────────────────────────

_OPF_RE = re.compile(
    r'\b(?:ПАО|ООО|АО|ОАО|ЗАО|НАО|НПО|'
    r'Публичное акционерное общество|'
    r'Общество с ограниченной ответственностью|'
    r'Акционерное общество|'
    r'Открытое акционерное общество|'
    r'Закрытое акционерное общество|'
    r'Непубличное акционерное общество|'
    r'Научно-производственное объединение)\s*',
    re.IGNORECASE,
)
_CITY_RE = re.compile(r'\bгорода\b', re.IGNORECASE)
_MTU_RE = re.compile(r'^Межрегиональное территориальное управление\b.*', re.IGNORECASE)
_FIO_RE = re.compile(
    r'^([А-ЯЁа-яё-]+)\s+([А-ЯЁа-яё])[а-яё]+\s+([А-ЯЁа-яё])[а-яё]+$'
)


def _shorten_single(name: str, *, keep_fio_full: bool = False) -> str:
    """Сокращение одного наименования (без запятых)."""
    name = name.strip()
    if not name:
        return name
    # МТУ Росимущество
    if _MTU_RE.match(name):
        return "МТУ Росимущество"
    # Убрать ОПФ
    name = _OPF_RE.sub('', name).strip()
    # Убрать кавычки-ёлочки, оставшиеся после удаления ОПФ
    name = re.sub(r'[«»"]+', '', name).strip()
    # Сбербанк: убрать «в лице филиала ...», «в лице ... банка ...» и т.п.
    name = re.sub(r'\s+в лице\s+.*', '', name, flags=re.IGNORECASE).strip()
    # Сбербанк России → Сбербанк
    name = re.sub(r'^Сбербанк\s+России$', 'Сбербанк', name, flags=re.IGNORECASE)
    # «города» → «г.»
    name = _CITY_RE.sub('г.', name)
    # ФИО → Фамилия И.О.
    if not keep_fio_full:
        m = _FIO_RE.match(name)
        if m:
            name = f"{m.group(1)} {m.group(2).upper()}.{m.group(3).upper()}."
    return name


def shorten_party_name(name: str, *, keep_fio_full: bool = False) -> str:
    """Сокращение наименования стороны по правилам дайджеста.

    Если в поле несколько сторон через запятую — сокращает каждую отдельно.
    keep_fio_full=True — не сокращать ФИО физлиц (для секции «Новые дела»).
    """
    if not name or not name.strip():
        return name
    parts = name.split(",")
    shortened = [_shorten_single(p, keep_fio_full=keep_fio_full) for p in parts]
    return ", ".join(shortened)


# ── Claude API — генерация дайджеста ─────────────────────────────────────────

def hearing_line_html(case: dict) -> str:
    """Форматировать строку заседания: дата+время + номер дела (строка 1), стороны | категория (строка 2)."""
    link = case_link_html(case)
    cat = category_short(case.get("Категория", ""))
    time_str = case.get("Время заседания", "").strip()
    date_str = (case.get("Дата заседания") or case.get("Дата события") or "").strip()
    # Короткая форма ДД.ММ из ДД.ММ.ГГГГ
    short_date = ""
    if date_str:
        m = re.match(r"^(\d{1,2}\.\d{1,2})\.\d{2,4}$", date_str)
        short_date = m.group(1) if m else date_str
    plaintiff = escape_html(shorten_party_name(case.get("Истец", "")))
    defendant = escape_html(shorten_party_name(case.get("Ответчик", "")))

    when_parts = [p for p in [short_date, time_str] if p]
    when = " ".join(when_parts)
    line1 = f"<b>{escape_html(when)}</b> {link}" if when else link
    line2 = f"{plaintiff} vs {defendant} | {cat}"

    return f"{line1}\n{line2}"


def upcoming_header_html(upcoming: list[dict]) -> str:
    """Заголовок секции ближайших заседаний на следующую неделю."""
    if not upcoming:
        return ""
    # Собираем уникальные даты из дел
    dates = sorted({c.get("Дата события", "") for c in upcoming if c.get("Дата события", "")})
    date_part = ", ".join(escape_html(d) for d in dates) if dates else ""
    return f"📅 <b>Предстоящие заседания ({date_part}, {len(upcoming)} дел):</b>"

def generate_digest(new_cases: list[dict], changes: list[dict],
                    total_active: int, cases: list[dict] | None = None) -> str:
    """Сгенерировать дайджест через Claude API."""

    if cases is None:
        cases = []

    if not ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY не задан, дайджест будет шаблонным")
        return generate_template_digest(new_cases, changes, total_active, cases)

    today = datetime.now().strftime("%d.%m.%Y")
    summary = build_summary_line(new_cases, changes)
    upcoming = get_upcoming_hearings(cases) if is_wednesday() else []

    # ── Короткое сообщение если изменений нет ──
    if not new_cases and not changes:
        msg = (
            f"✅ <b>Мониторинг дел Сбербанка — {today}</b>\n\n"
            f"Всё спокойно, изменений нет.\n"
            f"В производстве: {total_active}"
        )
        if upcoming:
            msg += f"\n\n{upcoming_header_html(upcoming)}"
            for c in upcoming:
                msg += f"\n\n{hearing_line_html(c)}"
        msg += f'\n\n<a href="{DASHBOARD_URL}">📊 Дашборд</a>'
        return msg

    # ── Формируем контекст для Claude ──
    context_parts = [f"СВОДКА: {summary}"]

    if new_cases:
        context_parts.append("\nНОВЫЕ ДЕЛА:")
        for c in new_cases:
            url = case_card_url(c)
            pl = shorten_party_name(c['Истец'], keep_fio_full=True)
            df = shorten_party_name(c['Ответчик'], keep_fio_full=True)
            context_parts.append(
                f"- {c['Номер дела']} (URL: {url}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"категория: {c['Категория']}, роль банка: {c['Роль банка']}, "
                f"суд 1 инст.: {c['Суд 1 инстанции']}, "
                f"поступило: {c['Дата поступления']}"
            )

    if changes:
        context_parts.append("\nИЗМЕНЕНИЯ ПО ДЕЛАМ:")
        for ch in changes:
            d = ch["details"]
            url = d.get("case_url", "")
            line = f"- Дело {ch['case']} (URL: {url})"
            pl = shorten_party_name(d.get('plaintiff', ''))
            df = shorten_party_name(d.get('defendant', ''))
            line += f"\n  Стороны: {pl} (истец) vs {df} (ответчик)"
            line += f", роль банка: {d.get('role', '')}"
            if d.get("appellant"):
                line += f", апеллянт: {shorten_party_name(d['appellant'])}"

            for t in ch["type"]:
                if t == "new_event":
                    line += f"\n  Новое событие: {d.get('event', '')}"
                    if d.get("event_date"):
                        line += f" ({d['event_date']})"
                    if d.get("hearing_date"):
                        line += f"\n  Дата заседания: {d['hearing_date']}"
                if t == "new_result":
                    hearing_dt = d.get("hearing_date", "")
                    line += (f"\n  ИТОГ (использовать ДОСЛОВНО): "
                             f"{d.get('verdict_label', '')}")
                    line += (f"\n  В чью пользу для банка (ДОСЛОВНО): "
                             f"{d.get('bank_outcome', '')}")
                    line += (f"\n  Категория спора (ОБЯЗАТЕЛЬНО упомянуть): "
                             f"{d.get('category', '')}")
                    line += f"\n  Роль банка: {d.get('role', '')}"
                    if d.get("appellant"):
                        line += f"\n  Апеллянт: {d['appellant']}"
                    if hearing_dt:
                        line += (
                            "\n  Дата апелляционного определения "
                            "(использовать ИМЕННО эту дату, НЕ today): "
                            f"{hearing_dt}"
                        )
                    if d.get("hearing_long_ago"):
                        line += (
                            "\n  ВНИМАНИЕ: заседание состоялось ранее, "
                            "в дайджест попало по факту обновления карточки "
                            "сегодня — НЕ пиши «сегодня», обязательно укажи "
                            "реальную дату заседания."
                        )
                    if d.get("last_event"):
                        line += (f"\n  Последнее событие "
                                 f"(для причины возврата/прекращения): "
                                 f"{d['last_event']}")
                    if d.get("act_excerpt"):
                        line += f"\n  Цитата из мотивировки: {d['act_excerpt']}"
                    line += (f"\n  Сырое поле «Результат» (для контроля): "
                             f"{d.get('result', '')}")
                if t == "new_act":
                    line += "\n  Опубликован судебный акт"
                    if d.get("act_text"):
                        line += f"\n  МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА: {d['act_text']}"
                if t == "status_change":
                    line += (f"\n  Статус: {d.get('old_status', '')} "
                             f"→ {d.get('new_status', '')}")
                if t == "hearing_postponed":
                    old_dt = d.get("old_hearing_date", "")
                    old_tm = d.get("old_hearing_time", "")
                    new_dt = d.get("new_hearing_date", "")
                    new_tm = d.get("new_hearing_time", "")
                    old_part = f"{old_dt}" + (f" {old_tm}" if old_tm else "")
                    new_part = f"{new_dt}" + (f" {new_tm}" if new_tm else "")
                    line += (f"\n  ОТЛОЖЕНО: заседание перенесено "
                             f"с {old_part} на {new_part}")

            context_parts.append(line)

    if upcoming:
        dates = sorted({c.get("Дата события", "") for c in upcoming if c.get("Дата события", "")})
        context_parts.append(f"\nПРЕДСТОЯЩИЕ ЗАСЕДАНИЯ ({', '.join(dates)}):")
        for c in upcoming:
            url = case_card_url(c)
            cat = category_short(c.get("Категория", ""))
            time_str = c.get("Время заседания", "").strip()
            time_part = f" в {time_str}" if time_str else ""
            context_parts.append(
                f"- {c['Номер дела']}{time_part} (URL: {url}) — "
                f"{shorten_party_name(c.get('Истец', ''))} vs "
                f"{shorten_party_name(c.get('Ответчик', ''))}, {cat}"
            )

    prompt = f"""Ты — помощник юриста ПАО Сбербанк. Сформируй дайджест изменений \
по судебным делам в апелляционной инстанции Суда ХМАО-Югры за сегодня ({today}).

!!! ОБЯЗАТЕЛЬНЫЕ ПРАВИЛА СОКРАЩЕНИЯ НАИМЕНОВАНИЙ — СТРОГО СОБЛЮДАЙ ВО ВСЁМ ДАЙДЖЕСТЕ !!!

1. Юридические лица → УБРАТЬ организационно-правовую форму (ПАО, ООО, АО, ОАО, ЗАО, НАО, НПО) \
и полные написания (Публичное акционерное общество, Общество с ограниченной ответственностью и т.д.).
   ПАО Сбербанк → Сбербанк
   ООО «Ромашка» → Ромашка
   Публичное акционерное общество «Сбербанк России» → Сбербанк
2. «в лице филиала», «в лице Уральского банка», «в лице филиала - Югорское отделение № 5940» и подобное — УБРАТЬ. \
Просто «Сбербанк».
3. Государственные и муниципальные органы → «города» заменять на «г.».
   Администрация города Ханты-Мансийска → Администрация г. Ханты-Мансийска
4. Если наименование стороны начинается на «Межрегиональное территориальное управление» → \
заменять на «МТУ Росимущество».
5. «Финансовый уполномоченный по правам потребителей финансовых услуг» → «Фин. уполномоченный».
6. «наследственное имущество умершего заемщика ФИО» → «насл. имущество ФИО».
7. Физические лица → фамилия + инициалы ВЕЗДЕ, КРОМЕ секции «Новые дела» (📥).
   Иванов Пётр Сергеевич → Иванов П.С.
   Леухина Анна Павловна → Леухина А.П.
   В секции «Новые дела» (📥) ФИО физических лиц писать ПОЛНОСТЬЮ.

ФОРМАТ: сообщение для Telegram с HTML-разметкой.
Доступные теги: <b>жирный</b>, <i>курсив</i>, <a href="URL">текст ссылки</a>.
НЕЛЬЗЯ использовать Markdown (* _ ` [ ] и т.п.) — только HTML-теги.

ВАЖНО: спецсимволы в именах и названиях (<, >, &) экранируй как &lt; &gt; &amp;

СТРУКТУРА — включай ТОЛЬКО секции, по которым есть данные. Пустых секций не пиши:
1. Заголовок: 📊 Дайджест апелляционных дел | Суд ХМАО-Югры | дата
2. Сводка одной строкой (📋): краткий итог (N событий, N решений и т.д.)
3. Новые дела (📥) — номер дела как <a href="URL">номер</a>, кто подал к кому, о чём, суд 1 инст., роль банка
4. Назначенные заседания (📅) — каждое дело в две строки с пустой строкой между делами: \
первая строка: <b>ДД.ММ HH:MM</b> номер дела (ссылка жирным), \
вторая строка: стороны | категория. Роль банка если известна. \
ВАЖНО: ДД.ММ — это короткая дата заседания (бери из поля «Дата заседания» или из текста события «назначено на ...»), а HH:MM — время заседания. ОБА компонента обязательны. \
В эту секцию НЕ помещай дела, у которых в данных есть пометка «ОТЛОЖЕНО» — для них есть отдельная секция 🔁
4а. 🔁 <b>Отложенные заседания</b> (🔁) — секция включается, если в данных по делу есть пометка «ОТЛОЖЕНО: заседание перенесено с ... на ...». \
Это РЕДКОЕ и ВАЖНОЕ событие для апелляционной инстанции — ОБЯЗАТЕЛЬНО выделяй и НЕ выкидывай при нехватке места. \
Каждое дело — две строки с пустой строкой между делами: \
первая строка: 🔁 <b>номер дела</b> (ссылка жирным): ⏪ старая дата (ДД.ММ.ГГГГ HH:MM) → ⏩ новая дата (ДД.ММ.ГГГГ HH:MM), \
вторая строка: стороны | категория. Роль банка если известна. \
Бери даты строго из строки «ОТЛОЖЕНО:» в данных, не выдумывай.
5. Вынесенные судебные акты (⚖️) — для каждого дела ОДНОЙ строкой в формате: \
<a href="URL"><b>номер</b></a> — Апелляционное определение от ДД.ММ.ГГГГ. ИТОГ: <дословно из поля «ИТОГ»>. \
Категория: <из поля «Категория спора»>. Стороны: <истец> vs <ответчик>, банк — <роль>. \
Для банка: <дословно из поля «В чью пользу для банка»>. \
Дату ДД.ММ.ГГГГ бери ИЗ ПОЛЯ «Дата апелляционного определения», а НЕ из заголовка дайджеста и НЕ из today. \
Если у дела есть пометка «ВНИМАНИЕ: заседание состоялось ранее» — обязательно укажи реальную дату заседания, не пиши «сегодня». \
Если ИТОГ содержит «возвращена / без рассмотрения / прекращено / снято» — добавь причину из «Последнее событие». \
Если ИТОГ содержит «отменено / изменено» и есть «Цитата из мотивировки» — в 1 фразе ключевой довод суда. \
ЗАПРЕЩЕНО переформулировать поле «ИТОГ» своими словами или подменять его шаблоном. \
ЗАПРЕЩЕНО опускать категорию и сторону. \
ЗАПРЕЩЕНО включать в эту секцию дело, у которого в данных НЕТ блока «ИТОГ» (т.е. изменение пришло как «Новое событие», а не как новый результат). \
НЕ упоминай «составлено мотивированное определение/решение» — это служебный шаг, не интересный читателю.
6. Опубликованные акты (📄) — номер дела (ссылка), стороны, и ОБЯЗАТЕЛЬНО: \
а) итог: жалоба удовлетворена / отказано / частично удовлетворена; \
б) 1-2 предложения ПОЧЕМУ суд так решил (ключевые аргументы из мотивировочной части). \
Данные есть в поле «МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА». \
НЕ ПИШИ просто номера дел без содержания — это бесполезно
7. Предстоящие заседания (📅) — секция включается ТОЛЬКО по средам (дела с четверга по следующую среду). \
Каждое дело — две строки с пустой строкой между делами: \
первая строка: <b>время</b> номер дела (ссылка жирным), \
вторая строка: стороны | категория. Сортируй по времени. \
ПРИОРИТЕТ: если не хватает места — эту секцию можно опустить целиком
8. 📌 Итоговая строка: всего дел в производстве
9. В конце ОБЯЗАТЕЛЬНО: <a href="{DASHBOARD_URL}">📊 Дашборд</a> — ссылка на дашборд должна быть всегда

ОФОРМЛЕНИЕ:
- НЕ используй маркеры списка («• », «- » и т.п.) — каждый пункт просто с новой строки
- Названия секций выделяй <b>жирным</b>
- Номера дел выделяй <b>жирным</b> (внутри ссылки: <a href="URL"><b>номер</b></a>)
- В секции «Предстоящие заседания» формат двухстрочный: первая строка <b>время</b> ссылка, вторая строка стороны | категория. Между делами пустая строка
- Отступы и пустые строки для читаемости

СТИЛЬ: кратко, по-деловому, на русском. Без вступлений. Не повторяй одну информацию в разных секциях.
ЛИМИТ: уложись в 3800 символов. Если не помещается — сначала убери секцию «Предстоящие заседания», \
затем сокращай описания актов. Секцию 🔁 «Отложенные заседания» НИКОГДА не выкидывай. \
Ссылка на дашборд должна быть в конце ВСЕГДА.

Данные:
{chr(10).join(context_parts)}

Всего дел в производстве: {total_active}

НАПОМИНАНИЕ — ОБЯЗАТЕЛЬНО СОБЛЮДАЙ:
- Юрлица: везде без ОПФ (ПАО, ООО и т.д.), госорганы: «города» → «г.», «Межрегиональное территориальное управление...» → «МТУ Росимущество».
- «в лице филиала ...» — убирать, просто «Сбербанк».
- ФИО физлиц: сокращать до инициалов ВЕЗДЕ, кроме секции «Новые дела» (📥) — там полностью.
- Акты: НЕ ПРОСТО номера — обязательно итог (удовлетворена/отказано) и ПОЧЕМУ суд так решил.
- В секции ⚖️ ИТОГ, «в чью пользу для банка» и дату апелляционного определения брать ДОСЛОВНО из переданных полей. Категорию и сторону — обязательно. Не включать дела без блока «ИТОГ».
- ПРИОРИТЕТ при нехватке места: сначала убери «Предстоящие заседания», потом сокращай акты. Секцию 🔁 «Отложенные заседания» НИКОГДА не выкидывай. Ссылка на дашборд — ВСЕГДА.
- Всё должно уместиться в ОДНО сообщение до 3800 символов."""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 4096,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
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
                new_cases, changes, total_active, cases
            )
        # Допускаем до двух сообщений; split_message в send_telegram разобьёт
        return truncate_html_message(text, TELEGRAM_MSG_LIMIT)
    except Exception as e:
        log.error(f"Ошибка Claude API: {e}")
        return generate_template_digest(new_cases, changes, total_active, cases)


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


def truncate_html_message(text: str, limit: int = 4096) -> str:
    """
    Обрезать HTML-сообщение до лимита Telegram, не ломая теги.
    Добавляет '…' в конце если обрезано.
    """
    if len(text) <= limit:
        return _close_open_tags(text)

    # Обрезаем с запасом для закрытия тегов и '…'
    cut = text[:limit - 100]

    # Убираем незакрытые теги в конце
    last_close = cut.rfind(">")
    last_open = cut.rfind("<")
    if last_open > last_close:
        cut = cut[:last_open]

    # Обрезаем до последнего перевода строки для чистоты
    last_nl = cut.rfind("\n")
    if last_nl > len(cut) - 200:
        cut = cut[:last_nl]

    cut += "\n\n…<i>сообщение обрезано</i>"
    cut = _close_open_tags(cut)

    return cut


def generate_template_digest(new_cases: list[dict], changes: list[dict],
                             total_active: int,
                             cases: list[dict] | None = None) -> str:
    """Шаблонный дайджест (fallback без Claude API). Формат: HTML."""
    today = datetime.now().strftime("%d.%m.%Y")
    if cases is None:
        cases = []

    upcoming = get_upcoming_hearings(cases) if is_wednesday() else []

    # ── Короткое сообщение если изменений нет ──
    if not new_cases and not changes:
        msg = (
            f"✅ <b>Мониторинг дел Сбербанка — {today}</b>\n\n"
            f"Всё спокойно, изменений нет.\n"
            f"В производстве: {total_active}"
        )
        if upcoming:
            msg += f"\n\n{upcoming_header_html(upcoming)}"
            for c in upcoming:
                msg += f"\n\n{hearing_line_html(c)}"
        msg += f'\n\n<a href="{DASHBOARD_URL}">📊 Дашборд</a>'
        return msg

    # ── Полный дайджест ──
    summary = build_summary_line(new_cases, changes)
    lines = [f"📊 <b>Мониторинг дел Сбербанка — {today}</b>"]
    lines.append(f"📋 {escape_html(summary)}\n")

    if new_cases:
        lines.append(f"📥 <b>Новые дела ({len(new_cases)}):</b>")
        for c in new_cases:
            link = case_link_html(c)
            role = c.get("Роль банка", "")
            role_icon = {"Истец": "🏦→", "Ответчик": "→🏦", "Третье лицо": "👁"
                         }.get(role, "")
            cat = category_short(c.get("Категория", ""))
            pl = escape_html(shorten_party_name(c['Истец'], keep_fio_full=True))
            df = escape_html(shorten_party_name(c['Ответчик'], keep_fio_full=True))
            lines.append(
                f"  {link} {role_icon}"
                f"{pl} vs {df} "
                f"({cat})"
            )

    postponed = [ch for ch in changes if "hearing_postponed" in ch["type"]]
    postponed_nums = {ch["case"] for ch in postponed}
    # Не дублируем дело в "Новые события", если оно уже в "Отложенные"
    events = [ch for ch in changes
              if "new_event" in ch["type"] and ch["case"] not in postponed_nums]
    results = [ch for ch in changes if "new_result" in ch["type"]]
    acts = [ch for ch in changes if "new_act" in ch["type"]]

    if postponed:
        lines.append(f"\n🔁 <b>Отложенные заседания ({len(postponed)}):</b>")
        for ch in postponed:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = (f'<a href="{url}"><b>{case_num}</b></a>'
                    if url else f'<b>{case_num}</b>')
            old_dt = escape_html(d.get("old_hearing_date", ""))
            old_tm = escape_html(d.get("old_hearing_time", ""))
            new_dt = escape_html(d.get("new_hearing_date", ""))
            new_tm = escape_html(d.get("new_hearing_time", ""))
            old_part = old_dt + (f" {old_tm}" if old_tm else "")
            new_part = new_dt + (f" {new_tm}" if new_tm else "")
            plaintiff = escape_html(shorten_party_name(d.get("plaintiff", "")))
            defendant = escape_html(shorten_party_name(d.get("defendant", "")))
            cat = category_short(d.get("category", ""))
            lines.append(f"  🔁 {link}: ⏪ {old_part} → ⏩ {new_part}")
            if plaintiff and defendant:
                tail = f"     {plaintiff} vs {defendant}"
                if cat:
                    tail += f" | {escape_html(cat)}"
                lines.append(tail)

    if events:
        lines.append(f"\n📅 <b>Новые события ({len(events)}):</b>")
        for ch in events:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = f'<a href="{url}"><b>{case_num}</b></a>' if url else f'<b>{case_num}</b>'
            # Участники и категория
            plaintiff = escape_html(shorten_party_name(d.get("plaintiff", "")))
            defendant = escape_html(shorten_party_name(d.get("defendant", "")))
            parties = f"{plaintiff} vs {defendant}" if plaintiff and defendant else ""
            # Очищаем текст события от дат и времён
            event_raw = d.get("event", "")
            event_date = d.get("event_date", "")
            is_hearing = "заседани" in event_raw.lower()
            parts = event_raw.split(". ")
            clean_parts = []
            hearing_date = ""
            hearing_time = ""
            for p in parts:
                ps = p.strip()
                if parse_date(ps):
                    if is_hearing:
                        hearing_date = ps  # дата заседания
                    elif not event_date:
                        event_date = ps
                    continue
                if re.match(r'^\d{1,2}:\d{2}$', ps):
                    if is_hearing:
                        hearing_time = ps
                    continue  # убираем время публикации
                if ps:
                    clean_parts.append(ps)
            event_clean = escape_html(". ".join(clean_parts))
            # Формируем строку
            if is_hearing:
                sched_parts = [x for x in [hearing_date, hearing_time] if x]
                if sched_parts:
                    event_clean += f" — {escape_html(', '.join(sched_parts))}"
            else:
                if event_date:
                    event_clean += f". {escape_html(event_date)}"
            line = f"  {link}"
            if parties:
                line += f" — {parties}"
            line += f": {event_clean}"
            lines.append(line)

    if results:
        lines.append(f"\n⚖️ <b>Вынесенные судебные акты ({len(results)}):</b>")
        for ch in results:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = f'<a href="{url}"><b>{case_num}</b></a>' if url else f'<b>{case_num}</b>'
            result_text = escape_html(d.get("result", ""))
            role = d.get("role", "")
            role_note = f" (банк — {escape_html(role.lower())})" if role else ""
            hearing_dt = d.get("hearing_date", "")
            date_note = f". Определение от {escape_html(hearing_dt)}" if hearing_dt else ""
            cat = category_short(d.get("category", ""))
            cat_note = f" | {escape_html(cat)}" if cat else ""
            last_ev = d.get("last_event", "")
            ev_note = f"\n    Причина: {escape_html(last_ev)}" if last_ev else ""
            lines.append(
                f"  {link}: {result_text}{cat_note}{role_note}{date_note}{ev_note}"
            )

    if acts:
        lines.append(f"\n📄 <b>Опубликованы акты ({len(acts)}):</b>")
        for ch in acts:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = f'<a href="{url}"><b>{case_num}</b></a>' if url else f'<b>{case_num}</b>'
            lines.append(f"  {link}")

    if upcoming:
        lines.append(f"\n{upcoming_header_html(upcoming)}")
        for c in upcoming:
            lines.append(f"\n{hearing_line_html(c)}")

    lines.append(f"\nВ производстве: {total_active}")
    lines.append(f'<a href="{DASHBOARD_URL}">📊 Дашборд</a>')

    text = "\n".join(lines)
    # Допускаем до двух сообщений; split_message в send_telegram разобьёт
    return truncate_html_message(text, TELEGRAM_MSG_LIMIT)


# ── Telegram ─────────────────────────────────────────────────────────────────

def send_telegram(text: str):
    """Отправить сообщение в Telegram (HTML-формат)."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram не настроен, сообщение не отправлено")
        log.info(f"Дайджест:\n{text}")
        return

    # Разбиваем на части если превышен лимит
    parts = split_message(text, TELEGRAM_MSG_LIMIT)

    for i, part in enumerate(parts):
        try:
            # Финальная проверка: закрыть незакрытые теги
            part = _close_open_tags(part)
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": part,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=30,
            )
            if r.ok:
                log.info(f"Telegram: сообщение {i + 1}/{len(parts)} отправлено")
            else:
                log.error(f"Telegram ошибка: {r.status_code} {r.text}")
                # Пробуем без разметки если не прошло
                plain = re.sub(r'<[^>]+>', '', part)
                r2 = requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": plain,
                        "disable_web_page_preview": True,
                    },
                    timeout=30,
                )
                if r2.ok:
                    log.info("Telegram: отправлено без разметки")
                else:
                    log.error(f"Telegram повторная ошибка: {r2.text}")

            # Пауза между частями
            if i < len(parts) - 1:
                time.sleep(1)

        except Exception as e:
            log.error(f"Telegram исключение: {e}")


def split_message(text: str, limit: int = 4096) -> list[str]:
    """Разбить сообщение на части по лимиту, не разрывая строки и HTML-теги."""
    if len(text) <= limit:
        return [text]

    parts = []
    while text:
        if len(text) <= limit:
            parts.append(_close_open_tags(text))
            break

        # Ищем точку разреза — двойной перенос (между секциями)
        cut = text[:limit - 50]  # запас для закрытия тегов
        split_pos = cut.rfind("\n\n")
        if split_pos < limit // 2:
            split_pos = cut.rfind("\n")
        if split_pos < limit // 3:
            split_pos = limit - 60

        part = text[:split_pos].rstrip()
        part = _close_open_tags(part)
        parts.append(part)

        text = text[split_pos:].lstrip("\n")
        text = _strip_orphan_close_tags(text)

    return parts


# ── Проверка доступности сайта суда ──────────────────────────────────────────

def check_court_available() -> bool:
    """Проверить что сайт суда отвечает."""
    try:
        r = session.get(BASE_URL, timeout=15)
        return r.status_code == 200
    except Exception:
        return False


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Запуск мониторинга дел Сбербанка")
    log.info("=" * 60)

    # 1. Проверяем доступность суда
    if not check_court_available():
        msg = "⚠️ Сайт суда oblsud--hmao.sudrf.ru недоступен. Обновление отложено."
        log.error(msg)
        send_telegram(msg)
        sys.exit(1)

    log.info("Сайт суда доступен")

    # 2. Загружаем текущие данные
    cases = load_csv(CSV_PATH)
    existing_numbers = {c["Номер дела"].strip() for c in cases if c.get("Номер дела")}
    log.info(f"Загружено {len(cases)} дел из CSV")

    active_count = sum(1 for c in cases if not is_archived(c))
    archived_count = len(cases) - active_count
    log.info(f"Активных: {active_count}, архивных: {archived_count}")

    # 3. Поиск новых дел (первая страница)
    log.info("Загружаю первую страницу поиска...")
    search_html = fetch_page(SEARCH_URL)
    new_cases = []
    if search_html:
        search_cases = parse_search_page(search_html)
        log.info(f"На первой странице найдено {len(search_cases)} дел")
        new_cases = find_new_cases(search_cases, existing_numbers)
        log.info(f"Из них новых: {len(new_cases)}")

        # Для новых дел загружаем карточки
        for nc in new_cases:
            cid, cuid = case_id_uid(nc.get("Ссылка", ""))
            if cid and cuid:
                polite_delay()
                url = CARD_URL_TPL.format(case_id=cid, case_uid=cuid)
                card_html = fetch_page(url)
                if card_html:
                    card_info = parse_case_card(card_html)
                    nc["Последнее событие"] = card_info.get("Последнее событие", "")
                    nc["Дата события"] = card_info.get("Дата события", "")
                    nc["Время заседания"] = card_info.get("Время заседания", "")
                    nc["Статус"] = card_info.get("Статус", "В производстве")
                    nc["Результат"] = card_info.get("Результат", "")
                    nc["Акт опубликован"] = card_info.get("Акт опубликован", "Нет")
                    log.info(f"  Карточка {nc['Номер дела']}: OK")
    else:
        log.warning("Не удалось загрузить страницу поиска")

    # 4. Обновляем активные дела
    log.info(f"Обновляю {active_count} активных дел...")
    cases, changes = update_active_cases(cases)

    # 5. Добавляем новые дела в начало списка
    if new_cases:
        cases = new_cases + cases
        log.info(f"Добавлено {len(new_cases)} новых дел")

    # 6. Считаем итоги
    total_active = sum(1 for c in cases if c.get("Статус", "").strip() != "Решено")

    # 7. Генерируем дайджест
    log.info("Генерирую дайджест...")
    digest = generate_digest(new_cases, changes, total_active, cases)

    # 8. Отправляем в Telegram
    send_telegram(digest)

    # 9. Сохраняем CSV
    save_csv(cases, CSV_PATH)

    log.info("Готово!")
    log.info(f"Новых дел: {len(new_cases)}")
    log.info(f"Изменений: {len(changes)}")
    log.info(f"Всего дел: {len(cases)} (активных: {total_active})")


def main_force_postponement_digest(case_number: str,
                                    old_date: str,
                                    old_time: str = "") -> None:
    """
    Одноразовый «нагон»: отправить дайджест с отложением заседания
    по уже обновлённому в CSV делу. Используется, когда CSV был обновлён
    более ранним прогоном (или вручную) и стандартное сравнение
    `old_event != new_event` уже не сработает.

    Параметры:
      case_number — номер дела (например, "33-1052/2026")
      old_date    — старая дата заседания (ДД.ММ.ГГГГ)
      old_time    — старое время заседания (HH:MM), опционально
    """
    log.info("=" * 60)
    log.info(f"Force-digest-for: {case_number} (отложение)")
    log.info("=" * 60)

    cases = load_csv(CSV_PATH)
    log.info(f"Загружено {len(cases)} дел из CSV")

    target = None
    for c in cases:
        if c.get("Номер дела", "").strip() == case_number.strip():
            target = c
            break

    if not target:
        log.error(f"Дело {case_number} не найдено в CSV")
        sys.exit(1)

    new_date = target.get("Дата заседания", "").strip()
    new_time = target.get("Время заседания", "").strip()

    if not parse_date(old_date):
        log.error(f"Старая дата '{old_date}' не парсится как ДД.ММ.ГГГГ")
        sys.exit(1)
    if not parse_date(new_date):
        log.error(
            f"Новая дата заседания в CSV '{new_date}' пуста или не парсится"
        )
        sys.exit(1)

    change = {
        "case": case_number,
        "type": ["hearing_postponed"],
        "details": {
            "plaintiff": target.get("Истец", ""),
            "defendant": target.get("Ответчик", ""),
            "role": target.get("Роль банка", ""),
            "category": target.get("Категория", ""),
            "appellant": target.get("Апеллянт", ""),
            "case_url": case_card_url(target),
            "old_hearing_date": old_date,
            "old_hearing_time": old_time,
            "new_hearing_date": new_date,
            "new_hearing_time": new_time,
        },
    }

    total_active = sum(
        1 for c in cases if c.get("Статус", "").strip() != "Решено"
    )

    log.info(
        f"Синтетический change: {old_date} {old_time} → {new_date} {new_time}"
    )
    log.info("Генерирую дайджест...")
    digest = generate_digest([], [change], total_active, cases)
    send_telegram(digest)
    log.info("Готово!")


def main_digest_only():
    """Сформировать и отправить дайджест по текущим данным CSV (без обращения к сайту суда)."""
    log.info("=" * 60)
    log.info("Режим digest-only: дайджест по текущим данным")
    log.info("=" * 60)

    cases = load_csv(CSV_PATH)
    log.info(f"Загружено {len(cases)} дел из CSV")

    total_active = sum(1 for c in cases if c.get("Статус", "").strip() != "Решено")
    log.info(f"В производстве: {total_active}")

    log.info("Генерирую дайджест...")
    digest = generate_digest([], [], total_active, cases)

    send_telegram(digest)
    log.info("Готово!")


if __name__ == "__main__":
    if "--digest-only" in sys.argv:
        main_digest_only()
    elif "--force-digest-for" in sys.argv:
        # Парсинг: --force-digest-for <case> --old-date <date> [--old-time <time>]
        def _arg(name: str, required: bool = True) -> str:
            if name in sys.argv:
                idx = sys.argv.index(name)
                if idx + 1 < len(sys.argv):
                    return sys.argv[idx + 1]
            if required:
                log.error(f"Не задан аргумент {name}")
                sys.exit(2)
            return ""

        case_num = _arg("--force-digest-for")
        old_d = _arg("--old-date")
        old_t = _arg("--old-time", required=False)
        main_force_postponement_digest(case_num, old_d, old_t)
    else:
        main()
