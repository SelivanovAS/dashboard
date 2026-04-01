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
ARCHIVE_DAYS = 30  # Дела решённые 30+ дней назад не обновляем
REQUEST_DELAY = (2, 3)  # Задержка между запросами к суду (сек)
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
    "Ссылка", "Заметки", "Апеллянт", "Дата публикации акта"
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
    """Скачать страницу с сайта суда (win-1251)."""
    try:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        return r.content.decode("windows-1251", errors="replace")
    except requests.RequestException as e:
        log.error(f"Ошибка загрузки {url}: {e}")
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
        return f'<a href="{url}">{num}</a>'
    return num


def parties_short(case: dict) -> str:
    """Стороны в формате 'Истец (истец) vs Ответчик (ответчик)'."""
    plaintiff = escape_html(case.get("Истец", ""))
    defendant = escape_html(case.get("Ответчик", ""))
    return f"{plaintiff} (истец) vs {defendant} (ответчик)"


def extract_motive_part(act_text: str, max_len: int = 1000) -> str:
    """
    Извлечь мотивировочную часть из текста судебного акта.
    Ищем от 'установил:' до 'руководствуясь' — это суть решения.
    Если не нашли — берём последние max_len символов (ближе к резолюции).
    """
    if not act_text:
        return ""

    text = act_text.strip()

    # Пробуем вырезать мотивировочную часть
    start_match = re.search(r'(?:установил|УСТАНОВИЛ)\s*:', text)
    end_match = re.search(r'(?:руководствуясь|РУКОВОДСТВУЯСЬ)', text)

    if start_match and end_match and end_match.start() > start_match.end():
        motive = text[start_match.end():end_match.start()].strip()
        if len(motive) > 100:  # Достаточно содержательный кусок
            return motive[:max_len]

    # Fallback: берём последнюю часть текста (ближе к решению)
    if len(text) > max_len:
        return "..." + text[-(max_len - 3):]
    return text


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

def parse_search_page(html: str) -> list[dict]:
    """
    Парсит страницу результатов поиска.
    Таблица результатов — 6-я на странице (индекс 5).
    Столбцы: №пп | Номер дела (ссылка) | Дата поступления | Категория |
              Истец | Ответчик | Суд 1 инстанции | Результат
    """
    tables = extract_tables(html)
    if len(tables) < 6:
        log.warning(f"Ожидалось ≥6 таблиц, найдено {len(tables)}")
        return []

    results_table = tables[5]
    cases = []

    for row in results_table:
        # Пропускаем заголовок и пустые строки
        if len(row) < 7:
            continue
        # Первый столбец — номер п/п, проверяем что это число
        if not row[0].strip().replace("\x00", "").split("HREF:")[0].isdigit():
            continue

        case_number_cell = row[1] if len(row) > 1 else ""
        case_number = cell_text(case_number_cell)
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

        date_received = cell_text(row[2]) if len(row) > 2 else ""
        category = cell_text(row[3]) if len(row) > 3 else ""
        plaintiff = cell_text(row[4]) if len(row) > 4 else ""
        defendant = cell_text(row[5]) if len(row) > 5 else ""
        court = cell_text(row[6]) if len(row) > 6 else ""

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
            info["Время заседания"] = last_time

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
    # Ищем ссылку на текст акта или блок с текстом акта
    html_lower = html.lower()
    if "судебный акт" in html_lower or "текст акта" in html_lower:
        # Ищем ссылку на акт
        act_match = re.search(
            r'href="([^"]*(?:act_text|print_page|case_doc)[^"]*)"',
            html, re.IGNORECASE
        )
        if act_match:
            info["Акт опубликован"] = "Да"
            # URL акта — попробуем скачать
            act_url = act_match.group(1)
            if not act_url.startswith("http"):
                act_url = BASE_URL + "/" + act_url.lstrip("/")
            info["_act_url"] = act_url

    # Альтернативный поиск: блок <div> с текстом акта прямо на странице
    act_div_match = re.search(
        r'<div[^>]*class="[^"]*act[^"]*"[^>]*>(.*?)</div>',
        html, re.DOTALL | re.IGNORECASE
    )
    if act_div_match:
        act_text = re.sub(r'<[^>]+>', ' ', act_div_match.group(1))
        act_text = re.sub(r'\s+', ' ', act_text).strip()
        if len(act_text) > 50:
            info["Акт опубликован"] = "Да"
            info["act_text"] = act_text[:5000]  # Сырой текст, обрезается позже

    # Также ищем по паттерну "Опубликовано" + дата
    pub_match = re.search(
        r'(?:опубликован|дата публикации)[^<]*?(\d{2}\.\d{2}\.\d{4})',
        html, re.IGNORECASE
    )
    if pub_match:
        info["Акт опубликован"] = "Да"
        info["Дата публикации акта"] = pub_match.group(1)

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


def get_upcoming_hearings(cases: list[dict]) -> list[dict]:
    """
    Найти дела с назначенными заседаниями на ближайший вторник.
    Заседания в суде назначаются только по вторникам.
    Возвращает список дел, отсортированный по времени заседания.
    """
    target = next_tuesday()
    upcoming = []

    hearing_keywords = ["заседание", "назначено", "слушание", "рассмотрение"]

    for case in cases:
        if is_archived(case):
            continue
        if case.get("Статус", "").strip() == "Решено":
            continue

        event = case.get("Последнее событие", "").lower()
        date_str = case.get("Дата события", "").strip()

        if not date_str:
            continue

        d = parse_date(date_str)
        if not d:
            continue

        # Совпадает с ближайшим вторником
        if d.date() == target.date():
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
    if events:
        parts.append(f"{events} событ.")
    if results:
        parts.append(f"{results} решен.")
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
            change["type"].append("new_event")
            change["details"]["event"] = new_event
            change["details"]["event_date"] = card_info.get("Дата события", "")

        # Новый акт
        if new_act == "Да" and old_act != "Да":
            change["type"].append("new_act")
            # Пробуем скачать текст акта
            act_text = card_info.get("act_text", "")
            if not act_text and card_info.get("_act_url"):
                act_text = fetch_act_text(card_info["_act_url"])
            # Извлекаем мотивировочную часть
            change["details"]["act_text"] = extract_motive_part(act_text, 1000)

        # Новый результат
        if new_result and new_result != old_result:
            change["type"].append("new_result")
            change["details"]["result"] = new_result

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

        if change["type"]:
            change["details"]["plaintiff"] = case.get("Истец", "")
            change["details"]["defendant"] = case.get("Ответчик", "")
            change["details"]["role"] = case.get("Роль банка", "")
            change["details"]["category"] = case.get("Категория", "")
            change["details"]["appellant"] = case.get("Апеллянт", "")
            change["details"]["case_url"] = case_card_url(case)
            changes.append(change)

        log.info(f"  {case['Номер дела']}: {'→ '.join(change['type']) or 'без изменений'}")

    return cases, changes


# ── Claude API — генерация дайджеста ─────────────────────────────────────────

def hearing_line_html(case: dict) -> str:
    """Форматировать строку заседания: время — ссылка (категория, стороны)."""
    link = case_link_html(case)
    cat = category_short(case.get("Категория", ""))
    time_str = case.get("Время заседания", "").strip()
    plaintiff = escape_html(case.get("Истец", ""))
    defendant = escape_html(case.get("Ответчик", ""))

    parts = []
    if time_str:
        parts.append(f"<b>{escape_html(time_str)}</b>")
    parts.append(link)
    parts.append(f"{plaintiff} vs {defendant}, {cat}")

    return " — ".join(parts) if time_str else f"{link} — {plaintiff} vs {defendant}, {cat}"


def upcoming_header_html(upcoming: list[dict]) -> str:
    """Заголовок секции ближайших заседаний с датой вторника."""
    if not upcoming:
        return ""
    # Берём дату из первого дела (все на один вторник)
    date_str = upcoming[0].get("Дата события", "")
    return f"📅 <b>Заседания во вторник {escape_html(date_str)} ({len(upcoming)}):</b>"

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
    upcoming = get_upcoming_hearings(cases)

    # ── Короткое сообщение если изменений нет ──
    if not new_cases and not changes:
        msg = (
            f"✅ <b>Мониторинг дел Сбербанка — {today}</b>\n\n"
            f"Всё спокойно, изменений нет.\n"
            f"Активных дел: {total_active}"
        )
        if upcoming:
            msg += f"\n\n{upcoming_header_html(upcoming)}"
            for c in upcoming:
                msg += f"\n  • {hearing_line_html(c)}"
        msg += f'\n\n<a href="{DASHBOARD_URL}">📊 Дашборд</a>'
        return msg

    # ── Формируем контекст для Claude ──
    context_parts = [f"СВОДКА: {summary}"]

    if new_cases:
        context_parts.append("\nНОВЫЕ ДЕЛА:")
        for c in new_cases:
            url = case_card_url(c)
            context_parts.append(
                f"- {c['Номер дела']} (URL: {url}): "
                f"{c['Истец']} (истец) vs {c['Ответчик']} (ответчик), "
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
            line += f"\n  Стороны: {d.get('plaintiff', '')} (истец) vs {d.get('defendant', '')} (ответчик)"
            line += f", роль банка: {d.get('role', '')}"
            if d.get("appellant"):
                line += f", апеллянт: {d['appellant']}"

            for t in ch["type"]:
                if t == "new_event":
                    line += f"\n  Новое событие: {d.get('event', '')}"
                    if d.get("event_date"):
                        line += f" ({d['event_date']})"
                if t == "new_result":
                    line += f"\n  Результат: {d.get('result', '')}"
                if t == "new_act":
                    line += "\n  Опубликован судебный акт"
                    if d.get("act_text"):
                        line += f"\n  МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА: {d['act_text']}"
                if t == "status_change":
                    line += (f"\n  Статус: {d.get('old_status', '')} "
                             f"→ {d.get('new_status', '')}")

            context_parts.append(line)

    if upcoming:
        tue_date = upcoming[0].get("Дата события", "")
        context_parts.append(f"\nЗАСЕДАНИЯ ВО ВТОРНИК {tue_date}:")
        for c in upcoming:
            url = case_card_url(c)
            cat = category_short(c.get("Категория", ""))
            time_str = c.get("Время заседания", "").strip()
            time_part = f" в {time_str}" if time_str else ""
            context_parts.append(
                f"- {c['Номер дела']}{time_part} (URL: {url}) — "
                f"{c.get('Истец', '')} vs {c.get('Ответчик', '')}, {cat}"
            )

    prompt = f"""Ты — помощник юриста ПАО Сбербанк. Сформируй дайджест изменений \
по судебным делам в апелляционной инстанции Суда ХМАО-Югры за сегодня ({today}).

ФОРМАТ: сообщение для Telegram с HTML-разметкой.
Доступные теги: <b>жирный</b>, <i>курсив</i>, <a href="URL">текст ссылки</a>.
НЕЛЬЗЯ использовать Markdown (* _ ` [ ] и т.п.) — только HTML-теги.

ВАЖНО: спецсимволы в именах и названиях (<, >, &) экранируй как &lt; &gt; &amp;

СТРУКТУРА — включай ТОЛЬКО секции, по которым есть данные. Пустых секций не пиши:
1. Заголовок с датой и эмодзи 📊
2. Сводка одной строкой (📋): краткий итог
3. Новые дела (📥) — номер дела как <a href="URL">номер</a>, кто подал к кому, о чём, суд 1 инст., роль банка
4. Назначенные заседания (📅) — номер дела (ссылка), стороны, дата
5. Вынесенные решения (⚖️) — номер дела (ссылка), суть решения. \
Если известен апеллянт или роль банка — укажи, в чью пользу решение для банка
6. Опубликованные акты (📄) — номер дела (ссылка), 2-3 предложения сути из мотивировочной части
7. Заседания во вторник (📅) — заголовок с датой вторника, далее список: время (жирным), номер дела (ссылка), стороны, категория. Сортируй по времени
8. Итоговая строка: всего активных дел
9. В конце: <a href="{DASHBOARD_URL}">📊 Дашборд</a>

СТИЛЬ: кратко, по-деловому, на русском. Без вступлений. Не повторяй одну информацию в разных секциях.
ЛИМИТ: уложись в 3500 символов (лимит Telegram — 4096, нужен запас).

Данные:
{chr(10).join(context_parts)}

Всего активных дел: {total_active}"""

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
                "max_tokens": 2000,
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
        if not text:
            return generate_template_digest(
                new_cases, changes, total_active, cases
            )
        # Обрезаем до лимита Telegram с запасом
        return truncate_html_message(text, TELEGRAM_MSG_LIMIT)
    except Exception as e:
        log.error(f"Ошибка Claude API: {e}")
        return generate_template_digest(new_cases, changes, total_active, cases)


def truncate_html_message(text: str, limit: int = 4096) -> str:
    """
    Обрезать HTML-сообщение до лимита Telegram, не ломая теги.
    Добавляет '…' в конце если обрезано.
    """
    if len(text) <= limit:
        return text

    # Обрезаем с запасом для закрытия тегов и '…'
    cut = text[:limit - 50]

    # Убираем незакрытые теги в конце
    # Ищем последний полный тег
    last_close = cut.rfind(">")
    last_open = cut.rfind("<")
    if last_open > last_close:
        # Есть незакрытый тег — обрезаем до него
        cut = cut[:last_open]

    # Обрезаем до последнего перевода строки для чистоты
    last_nl = cut.rfind("\n")
    if last_nl > len(cut) - 200:
        cut = cut[:last_nl]

    cut += "\n\n…<i>сообщение обрезано</i>"

    # Закрываем открытые теги
    open_tags = re.findall(r'<(b|i|a)[^>]*>', cut)
    close_tags = re.findall(r'</(b|i|a)>', cut)
    for tag in reversed(open_tags):
        tag_name = tag.split()[0] if " " in tag else tag
        if close_tags.count(tag_name) < open_tags.count(tag_name):
            cut += f"</{tag_name}>"
            break  # Обычно достаточно одного

    return cut


def generate_template_digest(new_cases: list[dict], changes: list[dict],
                             total_active: int,
                             cases: list[dict] | None = None) -> str:
    """Шаблонный дайджест (fallback без Claude API). Формат: HTML."""
    today = datetime.now().strftime("%d.%m.%Y")
    if cases is None:
        cases = []

    upcoming = get_upcoming_hearings(cases)

    # ── Короткое сообщение если изменений нет ──
    if not new_cases and not changes:
        msg = (
            f"✅ <b>Мониторинг дел Сбербанка — {today}</b>\n\n"
            f"Всё спокойно, изменений нет.\n"
            f"Активных дел: {total_active}"
        )
        if upcoming:
            msg += f"\n\n{upcoming_header_html(upcoming)}"
            for c in upcoming:
                msg += f"\n  • {hearing_line_html(c)}"
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
            lines.append(
                f"  • {link} {role_icon} "
                f"{escape_html(c['Истец'])} vs {escape_html(c['Ответчик'])} "
                f"({cat})"
            )

    events = [ch for ch in changes if "new_event" in ch["type"]]
    results = [ch for ch in changes if "new_result" in ch["type"]]
    acts = [ch for ch in changes if "new_act" in ch["type"]]

    if events:
        lines.append(f"\n📅 <b>Новые события ({len(events)}):</b>")
        for ch in events:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = f'<a href="{url}">{case_num}</a>' if url else case_num
            # Участники и категория
            plaintiff = escape_html(d.get("plaintiff", ""))
            defendant = escape_html(d.get("defendant", ""))
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
            line = f"  • {link}"
            if parties:
                line += f" — {parties}"
            line += f": {event_clean}"
            lines.append(line)

    if results:
        lines.append(f"\n⚖️ <b>Решения ({len(results)}):</b>")
        for ch in results:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = f'<a href="{url}">{case_num}</a>' if url else case_num
            result_text = escape_html(d.get("result", ""))
            role = d.get("role", "")
            role_note = f" (банк — {escape_html(role.lower())})" if role else ""
            lines.append(
                f"  • {link}: {result_text}{role_note}"
            )

    if acts:
        lines.append(f"\n📄 <b>Опубликованы акты ({len(acts)}):</b>")
        for ch in acts:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = f'<a href="{url}">{case_num}</a>' if url else case_num
            lines.append(f"  • {link}")

    if upcoming:
        lines.append(f"\n{upcoming_header_html(upcoming)}")
        for c in upcoming:
            lines.append(f"  • {hearing_line_html(c)}")

    lines.append(f"\nАктивных дел: {total_active}")
    lines.append(f'<a href="{DASHBOARD_URL}">📊 Дашборд</a>')

    text = "\n".join(lines)
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
    """Разбить сообщение на части по лимиту, не разрывая строки."""
    if len(text) <= limit:
        return [text]

    parts = []
    while text:
        if len(text) <= limit:
            parts.append(text)
            break

        # Ищем точку разреза — двойной перенос (между секциями)
        cut = text[:limit]
        split_pos = cut.rfind("\n\n")
        if split_pos < limit // 2:
            # Если нет хорошей точки — режем по одинарному переносу
            split_pos = cut.rfind("\n")
        if split_pos < limit // 3:
            split_pos = limit - 10

        parts.append(text[:split_pos].rstrip())
        text = text[split_pos:].lstrip("\n")

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
    total_active = sum(1 for c in cases if not is_archived(c))

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


if __name__ == "__main__":
    main()
