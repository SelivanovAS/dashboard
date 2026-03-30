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

CSV_COLUMNS = [
    "Номер дела", "Дата поступления", "Истец", "Ответчик", "Категория",
    "Суд 1 инстанции", "Роль банка", "Статус", "Последнее событие",
    "Дата события", "Акт опубликован", "Результат", "Ссылка", "Заметки",
    "Апеллянт", "Дата публикации акта"
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
        last_row = movement_table[-1]
        events_data = []
        for row in movement_table[1:]:  # Пропускаем заголовок
            if len(row) >= 2:
                event_text_parts = []
                date_val = ""
                for c in row:
                    ct = cell_text(c)
                    d = parse_date(ct)
                    if d and not date_val:
                        date_val = ct
                    elif ct:
                        event_text_parts.append(ct)
                event_desc = ". ".join(event_text_parts).strip(". ")
                if event_desc:
                    events_data.append((date_val, event_desc))

        if events_data:
            last_date, last_event = events_data[-1]
            info["Последнее событие"] = last_event
            info["Дата события"] = last_date

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
            info["act_text"] = act_text[:5000]  # Ограничиваем размер

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
    return text[:5000]  # Ограничиваем для Claude API


def case_card_url(case: dict) -> str:
    """Построить полный URL карточки дела для вставки в дайджест."""
    cid, cuid = case_id_uid(case.get("Ссылка", ""))
    if cid and cuid:
        return CARD_URL_TPL.format(case_id=cid, case_uid=cuid)
    return ""


def get_upcoming_hearings(cases: list[dict], days_ahead: int = 14) -> list[dict]:
    """
    Найти дела с назначенными заседаниями в ближайшие N дней.
    Ищем по полям 'Последнее событие' (содержит слово 'заседание' / 'назначено')
    и 'Дата события' (будущая дата).
    """
    today = datetime.now()
    cutoff = today + timedelta(days=days_ahead)
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

        # Заседание назначено на будущую дату
        if d >= today and d <= cutoff:
            if any(kw in event for kw in hearing_keywords):
                upcoming.append(case)

    # Сортируем по дате
    upcoming.sort(key=lambda c: parse_date(c.get("Дата события", "")) or today)
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
            change["details"]["act_text"] = act_text

        # Новый результат
        if new_result and new_result != old_result:
            change["type"].append("new_result")
            change["details"]["result"] = new_result

        # Обновляем поля дела
        if new_event:
            case["Последнее событие"] = new_event
        if card_info.get("Дата события"):
            case["Дата события"] = card_info["Дата события"]
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
            changes.append(change)

        log.info(f"  {case['Номер дела']}: {'→ '.join(change['type']) or 'без изменений'}")

    return cases, changes


# ── Claude API — генерация дайджеста ─────────────────────────────────────────

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
            f"✅ *Мониторинг дел Сбербанка — {today}*\n\n"
            f"Всё спокойно, изменений нет.\n"
            f"Активных дел: {total_active}"
        )
        if upcoming:
            msg += f"\n\n📅 *Ближайшие заседания ({len(upcoming)}):*"
            for c in upcoming[:5]:
                msg += (
                    f"\n  • {c['Дата события']} — "
                    f"{c['Номер дела']}"
                )
        msg += f"\n\n[Дашборд]({DASHBOARD_URL})"
        return msg

    # ── Формируем контекст для Claude ──
    context_parts = [f"СВОДКА: {summary}"]

    if new_cases:
        context_parts.append("\nНОВЫЕ ДЕЛА:")
        for c in new_cases:
            context_parts.append(
                f"- {c['Номер дела']}: {c['Истец']} → {c['Ответчик']}, "
                f"категория: {c['Категория']}, роль банка: {c['Роль банка']}, "
                f"суд 1 инст.: {c['Суд 1 инстанции']}, "
                f"поступило: {c['Дата поступления']}"
            )

    if changes:
        context_parts.append("\nИЗМЕНЕНИЯ ПО ДЕЛАМ:")
        for ch in changes:
            line = f"- Дело {ch['case']}"
            d = ch["details"]
            line += f" ({d.get('plaintiff', '')} → {d.get('defendant', '')})"
            line += f", роль банка: {d.get('role', '')}"

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
                        line += f"\n  ТЕКСТ АКТА (фрагмент): {d['act_text'][:2000]}"
                if t == "status_change":
                    line += (f"\n  Статус: {d.get('old_status', '')} "
                             f"→ {d.get('new_status', '')}")

            context_parts.append(line)

    if upcoming:
        context_parts.append("\nБЛИЖАЙШИЕ ЗАСЕДАНИЯ (14 дней):")
        for c in upcoming[:7]:
            context_parts.append(
                f"- {c['Дата события']}: {c['Номер дела']} "
                f"({c.get('Истец', '')} → {c.get('Ответчик', '')})"
            )

    prompt = f"""Ты — помощник юриста ПАО Сбербанк. Сформируй дайджест изменений 
по судебным делам в апелляционной инстанции Суда ХМАО-Югры за сегодня ({today}).

Формат: сообщение для Telegram с эмодзи и Markdown-разметкой (*жирный*, _курсив_).
Структура:
1. Заголовок с датой
2. Сводка одной строкой (📋): краткий итог что изменилось
3. Новые дела (📥) — кто подал иск к кому, о чём, суд 1 инстанции
4. Назначенные заседания (📅) — номер дела, стороны, дата/время
5. Вынесенные решения (⚖️) — номер дела, суть решения, в чью пользу
6. Опубликованные акты (📄) — номер дела, 2-3 предложения почему суд пришёл к такому решению (из текста акта если дан)
7. Ближайшие заседания (🗓) — только если есть данные, номер дела и дата
8. Итоговая строка: всего активных дел
9. В самом конце строка: [Дашборд]({DASHBOARD_URL})

Пиши кратко, по-деловому, на русском. Без длинных вступлений.

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
        return text.strip() or generate_template_digest(
            new_cases, changes, total_active, cases
        )
    except Exception as e:
        log.error(f"Ошибка Claude API: {e}")
        return generate_template_digest(new_cases, changes, total_active, cases)


def generate_template_digest(new_cases: list[dict], changes: list[dict],
                             total_active: int,
                             cases: list[dict] | None = None) -> str:
    """Шаблонный дайджест (fallback без Claude API)."""
    today = datetime.now().strftime("%d.%m.%Y")
    if cases is None:
        cases = []

    upcoming = get_upcoming_hearings(cases)

    # ── Короткое сообщение если изменений нет ──
    if not new_cases and not changes:
        msg = (
            f"✅ *Мониторинг дел Сбербанка — {today}*\n\n"
            f"Всё спокойно, изменений нет.\n"
            f"Активных дел: {total_active}"
        )
        if upcoming:
            msg += f"\n\n📅 *Ближайшие заседания ({len(upcoming)}):*"
            for c in upcoming[:5]:
                msg += (
                    f"\n  • {c['Дата события']} — "
                    f"{c['Номер дела']}"
                )
        msg += f"\n\n[Дашборд]({DASHBOARD_URL})"
        return msg

    # ── Полный дайджест ──
    summary = build_summary_line(new_cases, changes)
    lines = [f"📊 *Мониторинг дел Сбербанка — {today}*"]
    lines.append(f"📋 {summary}\n")

    if new_cases:
        lines.append(f"📥 *Новые дела ({len(new_cases)}):*")
        for c in new_cases:
            role_icon = {"Истец": "🏦→", "Ответчик": "→🏦", "Третье лицо": "👁"
                         }.get(c["Роль банка"], "")
            lines.append(
                f"  • {c['Номер дела']} {role_icon} "
                f"{c['Истец']} → {c['Ответчик']}"
            )

    events = [ch for ch in changes if "new_event" in ch["type"]]
    results = [ch for ch in changes if "new_result" in ch["type"]]
    acts = [ch for ch in changes if "new_act" in ch["type"]]

    if events:
        lines.append(f"\n📅 *Новые события ({len(events)}):*")
        for ch in events:
            lines.append(
                f"  • {ch['case']}: {ch['details'].get('event', '')}"
            )

    if results:
        lines.append(f"\n⚖️ *Решения ({len(results)}):*")
        for ch in results:
            lines.append(
                f"  • {ch['case']}: {ch['details'].get('result', '')}"
            )

    if acts:
        lines.append(f"\n📄 *Опубликованы акты ({len(acts)}):*")
        for ch in acts:
            lines.append(f"  • {ch['case']}")

    if upcoming:
        lines.append(f"\n🗓 *Ближайшие заседания ({len(upcoming)}):*")
        for c in upcoming[:5]:
            lines.append(
                f"  • {c['Дата события']} — {c['Номер дела']}"
            )

    lines.append(f"\nАктивных дел: {total_active}")
    lines.append(f"[Дашборд]({DASHBOARD_URL})")
    return "\n".join(lines)


# ── Telegram ─────────────────────────────────────────────────────────────────

def send_telegram(text: str):
    """Отправить сообщение в Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram не настроен, сообщение не отправлено")
        log.info(f"Дайджест:\n{text}")
        return

    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        if r.ok:
            log.info("Telegram: сообщение отправлено")
        else:
            log.error(f"Telegram ошибка: {r.status_code} {r.text}")
            # Пробуем без Markdown если не прошло
            r2 = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": text.replace("*", "").replace("_", ""),
                    "disable_web_page_preview": True,
                },
                timeout=30,
            )
            if r2.ok:
                log.info("Telegram: отправлено без разметки")
            else:
                log.error(f"Telegram повторная ошибка: {r2.text}")
    except Exception as e:
        log.error(f"Telegram исключение: {e}")


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
