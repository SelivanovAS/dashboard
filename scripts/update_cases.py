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

from __future__ import annotations  # type-hints как строки — импорт на Python 3.9

import csv
import io
import json
import logging
import os
import re
import sys
import time
import traceback
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from html import escape as html_escape
from html.parser import HTMLParser

import requests

# ── Настройки ────────────────────────────────────────────────────────────────

# ── Конфигурация судов ───────────────────────────────────────────────────────

# Параметры URL для разных типов судопроизводства на sudrf.ru:
#   delo_id=5, delo_table=g2_case — апелляция (гражданские дела)
#   delo_id=1, delo_table=g_case  — первая инстанция (гражданские дела)
# Поле поиска по имени стороны также различается:
#   G2_PARTS__NAMESS — апелляция, G1_PARTS__NAMESS — первая инстанция

SBER_NAME_WIN1251 = "%D1%E1%E5%F0%E1%E0%ED%EA"  # «Сбербанк» в Windows-1251 URL-encoded


@dataclass
class CourtConfig:
    name: str          # «Суд ХМАО-Югры» / «Сургутский городской суд»
    domain: str        # oblsud--hmao.sudrf.ru
    delo_id: int       # 5 = апелляция, 1540005 = первая инстанция (гражданские)
    court_type: str    # "appeal" | "first_instance"
    enabled: bool = True
    srv_num: int = 1   # номер сервера (обычно 1, но бывает 2 — напр. Покачи)

    @property
    def base_url(self) -> str:
        return f"https://{self.domain}"

    @property
    def _delo_table(self) -> str:
        if self.delo_id == 5:
            return "g2_case"
        return "g1_case"

    @property
    def _name_field(self) -> str:
        """Имя поля для фильтрации по стороне (зависит от типа суда)."""
        if self.delo_id == 5:
            return "G2_PARTS__NAMESS"
        return "G1_PARTS__NAMESS"

    @property
    def _new_param(self) -> int:
        """Параметр &new= : 5 для апелляции, 0 для 1 инстанции."""
        return 5 if self.delo_id == 5 else 0

    def search_url(self, party_name_encoded: str = SBER_NAME_WIN1251) -> str:
        return (
            f"{self.base_url}/modules.php?name=sud_delo&srv_num={self.srv_num}&name_op=r"
            f"&delo_id={self.delo_id}&case_type=0&new={self._new_param}"
            f"&{self._name_field}={party_name_encoded}"
            f"&delo_table={self._delo_table}&Submit=%CD%E0%E9%F2%E8"
        )

    def card_url(self, case_id: str, case_uid: str) -> str:
        return (
            f"{self.base_url}/modules.php?name=sud_delo&srv_num={self.srv_num}&name_op=case"
            f"&case_id={case_id}&case_uid={case_uid}"
            f"&delo_id={self.delo_id}&new=5"
        )


# Апелляционный суд (текущий — единственный источник данных)
APPEAL_COURT = CourtConfig(
    name="Суд ХМАО-Югры",
    domain="oblsud--hmao.sudrf.ru",
    delo_id=5,
    court_type="appeal",
)

# Реестр судов первой инстанции ХМАО-Югры (delo_id=1540005 — гражданские дела 1 инст.)
FIRST_INSTANCE_COURTS: list[CourtConfig] = [
    CourtConfig("Сургутский городской суд",       "surggor--hmao.sudrf.ru",   1540005, "first_instance"),
    CourtConfig("Сургутский районный суд",         "surgray--hmao.sudrf.ru",   1540005, "first_instance"),
    CourtConfig("Нижневартовский городской суд",   "vartovgor--hmao.sudrf.ru", 1540005, "first_instance"),
    CourtConfig("Нижневартовский районный суд",    "vartovray--hmao.sudrf.ru", 1540005, "first_instance"),
    CourtConfig("Нижневартовский районный суд (г. Покачи)", "vartovray--hmao.sudrf.ru", 1540005, "first_instance", srv_num=2),
    CourtConfig("Ханты-Мансийский районный суд",   "hmray--hmao.sudrf.ru",     1540005, "first_instance"),
    CourtConfig("Урайский городской суд",          "uray--hmao.sudrf.ru",      1540005, "first_instance"),
    CourtConfig("Няганский городской суд",         "nyagan--hmao.sudrf.ru",    1540005, "first_instance"),
    CourtConfig("Нефтеюганский районный суд",      "uganskray--hmao.sudrf.ru", 1540005, "first_instance"),
    CourtConfig("Когалымский городской суд",       "kogalym--hmao.sudrf.ru",   1540005, "first_instance"),
    CourtConfig("Кондинский районный суд",         "kondinsk--hmao.sudrf.ru",  1540005, "first_instance"),
    CourtConfig("Лангепасский городской суд",      "langepas--hmao.sudrf.ru",  1540005, "first_instance"),
    CourtConfig("Мегионский городской суд",        "megion--hmao.sudrf.ru",    1540005, "first_instance"),
    CourtConfig("Советский районный суд",          "sovetsk--hmao.sudrf.ru",   1540005, "first_instance"),
    CourtConfig("Югорский районный суд",           "ugorsk--hmao.sudrf.ru",    1540005, "first_instance"),
    CourtConfig("Белоярский городской суд",        "bel--hmao.sudrf.ru",       1540005, "first_instance"),
    CourtConfig("Пыть-Яхский городской суд",      "pth--hmao.sudrf.ru",       1540005, "first_instance"),
    CourtConfig("Берёзовский районный суд",        "berezovo--hmao.sudrf.ru",  1540005, "first_instance"),
    CourtConfig("Радужнинский городской суд",      "rdj--hmao.sudrf.ru",       1540005, "first_instance"),
    CourtConfig("Октябрьский районный суд",        "oktb--hmao.sudrf.ru",      1540005, "first_instance"),
]

# Совместимость: глобальные константы на переходный период
BASE_URL = APPEAL_COURT.base_url
SEARCH_URL = APPEAL_COURT.search_url()
CARD_URL_TPL = (
    f"{BASE_URL}/modules.php?name=sud_delo&srv_num=1&name_op=case"
    "&case_id={case_id}&case_uid={case_uid}&delo_id=5&new=5"
)

CSV_PATH = os.environ.get("CSV_PATH", "data/sberbank_cases.csv")
CSV_ARCHIVE_PATH = os.environ.get(
    "CSV_ARCHIVE_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "sberbank_cases_archive.csv")
)
JSON_PATH = os.environ.get("JSON_PATH", "data/cases.json")
JSON_ARCHIVE_PATH = os.environ.get(
    "JSON_ARCHIVE_PATH",
    os.path.join(os.path.dirname(JSON_PATH) or "data", "cases_archive.json")
)
DIGESTED_ACTS_PATH = os.environ.get(
    "DIGESTED_ACTS_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", ".digested_acts")
)
ARCHIVE_DAYS = 30  # Дела решённые 30+ дней назад уезжают в архив
REQUEST_DELAY = (2, 3)  # Задержка между запросами к суду (сек)
FETCH_MAX_RETRIES = 3   # Кол-во попыток загрузки страницы
DASHBOARD_URL = "https://selivanovas.github.io/dashboard/sberbank_dashboard.html"

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Лимит Telegram на одно сообщение
TELEGRAM_MSG_LIMIT = 4096
# Целевой лимит длины дайджеста (передаётся в промпт). Меньше TELEGRAM_MSG_LIMIT,
# чтобы оставить запас на случай если модель чуть превысит.
DIGEST_CHAR_LIMIT = 7600

# Паттерны для опознания «Сбербанка» среди сторон дела (lowercase substring match).
# Используется и при первичном парсинге поисковой выдачи, и при определении
# апеллянта на стадии обновления карточки. Должен быть один источник истины,
# иначе роль банка проставляется неконсистентно.
SBER_PATTERNS = ("сбербанк", "сбербанк россии", "пао сбербанк", "пао сбер")

CSV_COLUMNS = [
    "Номер дела", "Дата поступления", "Истец", "Ответчик", "Категория",
    "Суд 1 инстанции", "Судья 1 инстанции", "Роль банка", "Статус",
    "Последнее событие", "Дата события", "Время заседания",
    "Акт опубликован", "Результат", "Ссылка", "Заметки", "Апеллянт",
    "Дата публикации акта", "Дата заседания", "Судья-докладчик"
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


# ── Метрики прогона ──────────────────────────────────────────────────────────

# Глобальные счётчики прогона — собираются по ходу выполнения,
# сбрасываются в начале каждого main()/main_digest_only().
METRICS: dict[str, int] = {
    "requests_ok": 0,
    "requests_failed": 0,
    "requests_retried": 0,   # попытки fetch_page после неудачи
    "telegram_sent": 0,      # успешно отправленных сообщений (после split)
    "telegram_failed": 0,    # полностью не отправленных частей
}


def _metrics_reset() -> None:
    for k in METRICS:
        METRICS[k] = 0


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
            METRICS["requests_ok"] += 1
            if attempt > 1:
                METRICS["requests_retried"] += 1
            return r.content.decode("windows-1251", errors="replace")
        except requests.RequestException as e:
            if attempt < FETCH_MAX_RETRIES:
                wait = attempt * 5
                log.warning(f"Попытка {attempt}/{FETCH_MAX_RETRIES} не удалась для {url}: {e}. Повтор через {wait}с...")
                time.sleep(wait)
            else:
                METRICS["requests_failed"] += 1
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


# ── Регулярные выражения, используемые в hot loops ───────────────────────────
# Скомпилированы один раз на уровне модуля.
_HTML_TAG_RE = re.compile(r'<[^>]+>')
_HTML_NBSP_RE = re.compile(r'&nbsp;')
_WS_RE = re.compile(r'\s+')
_HTML_SCRIPT_RE = re.compile(r'<script[^>]*>.*?</script>', re.DOTALL)
_HTML_STYLE_RE = re.compile(r'<style[^>]*>.*?</style>', re.DOTALL)

_CASE_NUM_RE = re.compile(r'\d+-\d+/\d{4}')
_TIME_RE = re.compile(r'\b(\d{1,2}:\d{2})\b')
_CASE_ID_RE = re.compile(r'case_id=(\d+)')
_CASE_UID_RE = re.compile(r'case_uid=([a-f0-9\-]+)')


def _strip_html(text: str) -> str:
    """Убрать HTML-теги, &nbsp; и схлопнуть пробелы. Используется для извлечения
    чистого текста из фрагментов карточки дела и судебных актов."""
    text = _HTML_TAG_RE.sub(' ', text)
    text = _HTML_NBSP_RE.sub(' ', text)
    return _WS_RE.sub(' ', text).strip()


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


def case_card_url(case: dict, court: CourtConfig | None = None) -> str:
    """Построить полный URL карточки дела."""
    cid, cuid = case_id_uid(case.get("Ссылка", ""))
    if cid and cuid:
        if court:
            return court.card_url(cid, cuid)
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


# Паттерны дочерних структур Сбербанка, которые НЕ являются ПАО Сбербанк
# (страхование, НПФ, УК и т.п.). Порядок не важен — все применяются последовательно.
_SBER_SUBSIDIARY_PATTERNS = [
    # Сбербанк страхование [жизни] — СК ООО/АО «Сбербанк страхование жизни» и варианты
    re.compile(r'сбербанк\s+страхован\w*(?:\s+жизн\w*)?', re.IGNORECASE),
    # НПФ Сбербанк — АО «НПФ Сбербанк», «Негосударственный пенсионный фонд Сбербанк»
    re.compile(r'нпф\s+сбербанк', re.IGNORECASE),
    re.compile(r'негосударственн\w*\s+пенсионн\w*\s+фонд\w*\s+сбербанк', re.IGNORECASE),
    # Сбербанк Управление Активами — УК
    re.compile(r'сбербанк\s+управлен\w*\s+актив\w*', re.IGNORECASE),
    # Сбербанк Лизинг
    re.compile(r'сбербанк\s+лизинг\w*', re.IGNORECASE),
    # Сбербанк Факторинг
    re.compile(r'сбербанк\s+факторинг\w*', re.IGNORECASE),
]


def is_subsidiary_only_case(plaintiff: str, defendant: str) -> bool:
    """Вернуть True, если «сбербанк» упоминается только в названии дочерней структуры
    (страхование, НПФ, лизинг и т.п.), а не самого ПАО Сбербанк.

    Если «сбербанк» вообще не встречается в сторонах — возвращаем False
    (дело найдено по поиску, значит банк упомянут где-то ещё, например как третье лицо).
    """
    combined = (plaintiff + " " + defendant).lower()
    if "сбербанк" not in combined:
        return False
    cleaned = combined
    for pat in _SBER_SUBSIDIARY_PATTERNS:
        cleaned = pat.sub("", cleaned)
    return "сбербанк" not in cleaned


# Backward-compat alias
is_insurance_only_case = is_subsidiary_only_case


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
        if not _CASE_NUM_RE.match(case_number):
            continue

        href = cell_href(case_number_cell)

        # Извлекаем case_id и case_uid из href
        cid, cuid = "", ""
        if href:
            m_id = _CASE_ID_RE.search(href)
            m_uid = _CASE_UID_RE.search(href)
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

        # Пропускаем дела, где «Сбербанк» — только дочерняя структура (страхование, НПФ и т.п.)
        if is_subsidiary_only_case(plaintiff, defendant):
            log.info(f"Пропуск дела {case_number}: только Сбербанк Страхование")
            continue

        # Определяем роль банка
        role = "Третье лицо"
        plaintiff_lower = plaintiff.lower()
        defendant_lower = defendant.lower()
        if any(p in plaintiff_lower for p in SBER_PATTERNS):
            role = "Истец"
        elif any(p in defendant_lower for p in SBER_PATTERNS):
            role = "Ответчик"

        link = f"{cid}|{cuid}" if cid and cuid else ""

        cases.append({
            "Номер дела": case_number,
            "Дата поступления": date_received,
            "Истец": plaintiff,
            "Ответчик": defendant,
            "Категория": category,
            "Суд 1 инстанции": court,
            "Судья 1 инстанции": "",
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
            "Судья-докладчик": "",
        })

    return cases


def _find_results_table(tables: list) -> list | None:
    """Найти таблицу результатов поиска по заголовку (\"№ дела\").

    Для апелляции это обычно индекс 5, для 1 инстанции — индекс 8+.
    Надёжнее искать по содержимому заголовка.
    """
    for tbl in tables:
        if len(tbl) < 2:
            continue
        header_text = " ".join(cell_text(c) for c in tbl[0]).lower()
        if "дела" in header_text and ("дата" in header_text or "поступлен" in header_text):
            return tbl
    return None


def parse_first_instance_search(html: str, court: CourtConfig) -> list[dict]:
    """Парсит страницу поиска суда первой инстанции.

    Отличия от parse_search_page (апелляция):
    - Таблица результатов ищется по заголовку, а не по индексу
    - 8 столбцов: № дела | Дата | Категория/Стороны | Судья | Дата решения | Решение | ...
    - Фильтр: только дела, где Сбербанк — ответчик
    - Номер дела может содержать '~' (материал) — берём первую часть
    """
    tables = extract_tables(html)
    results_table = _find_results_table(tables)
    if not results_table:
        log.warning(f"{court.name}: таблица результатов не найдена")
        return []

    cases = []
    for row in results_table:
        if len(row) < 3:
            continue

        case_number_cell = row[0]
        case_number_raw = cell_text(case_number_cell).strip()

        # Пропускаем заголовок и строки без номера дела
        if not _CASE_NUM_RE.match(case_number_raw):
            continue

        # Номер может быть «2-5628/2026 ~ М-3298/2026» — берём первый
        case_number = case_number_raw.split("~")[0].strip()

        # Пропускаем материалы (М-XXXX/YYYY)
        if case_number.startswith("М-") or case_number.startswith("м-"):
            continue

        href = cell_href(case_number_cell)
        cid, cuid = "", ""
        if href:
            m_id = _CASE_ID_RE.search(href)
            m_uid = _CASE_UID_RE.search(href)
            if m_id:
                cid = m_id.group(1)
            if m_uid:
                cuid = m_uid.group(1)

        date_received = cell_text(row[1]).strip() if len(row) > 1 else ""

        # Третий столбец — объединённая ячейка с категорией и сторонами
        combined = cell_text(row[2]) if len(row) > 2 else ""
        parsed = _parse_combined_cell(combined)
        plaintiff = parsed["plaintiff"]
        defendant = parsed["defendant"]
        category = parsed["category"]

        # Судья — 4й столбец
        judge = cell_text(row[3]).strip() if len(row) > 3 else ""

        # Дата решения и результат (столбцы 4-5, могут быть пустые)
        result_date = cell_text(row[4]).strip() if len(row) > 4 else ""
        result = cell_text(row[5]).strip() if len(row) > 5 else ""

        # Пропускаем дела, где «Сбербанк» — только дочерняя структура (страхование, НПФ и т.п.)
        if is_subsidiary_only_case(plaintiff, defendant):
            continue

        # Определяем роль банка
        role = "Третье лицо"
        plaintiff_lower = plaintiff.lower()
        defendant_lower = defendant.lower()
        if any(p in plaintiff_lower for p in SBER_PATTERNS):
            role = "Истец"
        elif any(p in defendant_lower for p in SBER_PATTERNS):
            role = "Ответчик"

        # Фильтр: только банк-ответчик
        if role != "Ответчик":
            continue

        link = f"{cid}|{cuid}" if cid and cuid else ""

        # Статус: если есть результат — решено
        status = "Решено" if result else "В производстве"

        cases.append({
            "case_number": case_number,
            "filing_date": date_received,
            "plaintiff": plaintiff,
            "defendant": defendant,
            "category": category,
            "court": court.name,
            "court_domain": court.domain,
            "judge": judge,
            "bank_role": role,
            "status": status,
            "result": result,
            "result_date": result_date,
            "link": link,
        })

    return cases


# ── Парсинг карточки дела ────────────────────────────────────────────────────

def _extract_act_text(html: str, court_base_url: str = "") -> tuple[str, str]:
    """Извлечь текст судебного акта из HTML карточки дела.

    Возвращает кортеж (act_text, act_url):
    - act_text: текст акта если найден встроенным в страницу (иначе "")
    - act_url: URL отдельной страницы с актом если найдена ссылка (иначе "")

    Используются 3 fallback-метода в порядке приоритета:
    1. div#cont_doc1 — основной способ для oblsud--hmao.sudrf.ru
    2. <a href="...act_text|print_page|case_doc...">
    3. <div class="...act...">
    """
    if not court_base_url:
        court_base_url = BASE_URL
    # Способ 1: Текст акта встроен в страницу (div#cont_doc1)
    doc_match = re.search(
        r"""id\s*=\s*['"]?cont_doc1['"]?[^>]*>(.+?)"""
        r"""(?=<div[^>]*id\s*=\s*['"]?cont_doc\d|<div[^>]*id\s*=\s*['"]?cont[^_]|$)""",
        html, re.DOTALL
    )
    if doc_match:
        act_text = _strip_html(doc_match.group(1))
        if len(act_text) > 200:
            return act_text[:8000], ""

    # Способ 2: Ссылка на отдельную страницу с текстом акта
    html_lower = html.lower()
    if "судебный акт" in html_lower or "текст акта" in html_lower:
        act_match = re.search(
            r'href="([^"]*(?:act_text|print_page|case_doc)[^"]*)"',
            html, re.IGNORECASE
        )
        if act_match:
            act_url = act_match.group(1)
            if not act_url.startswith("http"):
                act_url = court_base_url + "/" + act_url.lstrip("/")
            return "", act_url

    # Способ 3: Блок <div> с текстом акта (class содержит "act")
    act_div_match = re.search(
        r'<div[^>]*class="[^"]*act[^"]*"[^>]*>(.*?)</div>',
        html, re.DOTALL | re.IGNORECASE
    )
    if act_div_match:
        act_text = _strip_html(act_div_match.group(1))
        if len(act_text) > 50:
            return act_text[:8000], ""

    return "", ""


def parse_case_card(html: str, court_base_url: str = "") -> dict:
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
        "Судья 1 инстанции": "",
        "Судья-докладчик": "",
        "Номер дела 1 инстанции": "",  # Извлекается из таблицы «РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ»
        "act_text": "",  # Текст акта (для дайджеста, не сохраняется в CSV)
        "_appellant_raw": "",  # Сырой текст об апеллянте (для определения в update_active_cases)
    }

    tables = extract_tables(html)
    if len(tables) < 6:
        log.warning(f"Карточка: ожидалось ≥6 таблиц, найдено {len(tables)}")
        return info

    # ── Таблица ДЕЛО (обычно индекс 3) ──
    # Ищем таблицу с результатом рассмотрения, судьёй-докладчиком апелляции
    # и судьёй первой инстанции. Структура строк: <td><b>Лейбл</b></td><td>Значение</td>.
    for tbl_idx in range(min(5, len(tables))):
        tbl = tables[tbl_idx]
        for row in tbl:
            if len(row) < 2:
                continue
            label = cell_text(row[0]).strip()
            value = cell_text(row[-1]).strip()
            label_l = label.lower()
            # Матчим строго по лейблу первой ячейки: «Результат рассмотрения».
            # Ранее было `"результат" in row_text` — цеплялось за дисклеймер
            # sudrf («…набор значений полей «Результат рассмотрения»…»), который
            # у карточек 1 инстанции (delo_table=g1_case) живёт в отдельной
            # таблице и перетирал реальный результат мусорным текстом.
            if "результат рассмотрения" in label_l:
                if value and value.lower() not in (
                    "результат", "результат рассмотрения", label_l, "",
                ):
                    info["Результат"] = value
            # Номер дела в первой инстанции — лейбл вида:
            # «Номер дела в первой инстанции»
            # Значение: «2-498/2026 (2-9238/2025;)» — берём первый номер
            if "номер" in label_l and "первой инстанции" in label_l:
                if value:
                    # Извлечь первый номер дела (формат N-NNNN/YYYY)
                    fi_num_m = re.search(r'\d+-\d+/\d{4}', value)
                    if fi_num_m:
                        info["Номер дела 1 инстанции"] = fi_num_m.group(0)
            # Судья первой инстанции — приоритетнее, т.к. ключ длиннее
            # и содержит подстроку «судья». Лейбл вида:
            # «Судья (мировой судья) первой инстанции»
            if "первой инстанции" in label_l and "судья" in label_l:
                if value and value.lower() != label_l:
                    info["Судья 1 инстанции"] = value
            elif label_l == "судья":
                # Судья-докладчик апелляции (отдельная строка «Судья» без
                # «первой инстанции»)
                if value and value.lower() != "судья":
                    info["Судья-докладчик"] = value

    # Судья и номер дела 1 инстанции лежат в отдельной таблице
    # («РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ»), которая может быть за пределами
    # первых пяти таблиц. Если в основном цикле не нашли — пройдём по всем.
    if not info["Судья 1 инстанции"] or not info["Номер дела 1 инстанции"]:
        for tbl in tables:
            for row in tbl:
                if len(row) < 2:
                    continue
                label_l = cell_text(row[0]).strip().lower()
                value = cell_text(row[-1]).strip()
                if not info["Судья 1 инстанции"]:
                    if "первой инстанции" in label_l and "судья" in label_l:
                        if value and value.lower() != label_l:
                            info["Судья 1 инстанции"] = value
                if not info["Номер дела 1 инстанции"]:
                    if "номер" in label_l and "первой инстанции" in label_l:
                        fi_num_m = re.search(r'\d+-\d+/\d{4}', value)
                        if fi_num_m:
                            info["Номер дела 1 инстанции"] = fi_num_m.group(0)

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
                        time_match = _TIME_RE.search(ct)
                        if time_match and not time_val:
                            time_val = time_match.group(1)
                        if ct:
                            event_text_parts.append(ct)
                event_desc = ". ".join(event_text_parts).strip(". ")
                if event_desc:
                    events_data.append((date_val, time_val, event_desc))

        if events_data:
            # Полный список событий для timeline (сохраняется в JSON как events[])
            info["_events"] = [
                {"date": d, "time": t, "text": desc}
                for d, t, desc in events_data
            ]
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
        # Апелляция
        "без изменения", "отменено", "изменено", "снято с рассмотрения",
        "прекращено", "оставлено без рассмотрения", "возвращено",
        "передано в экспедицию", "сдано в отдел",
        # 1 инстанция (g1_case): реальные формулировки на карточках sudrf
        "отказано",                 # «ОТКАЗАНО в удовлетворении иска…»
        "удовлетворен",             # «Иск удовлетворён (в т.ч. частично)»
        "передано по подсудности",  # дело ушло в другой суд
    ]
    if any(kw in result for kw in resolved_keywords):
        info["Статус"] = "Решено"
    elif any(kw in last_event for kw in [
        "экспедиц", "делопроизводств",
        "передано в архив", "сдано в архив",  # 1 инстанция: закрытие
    ]):
        info["Статус"] = "Решено"

    # ── Судебный акт ──
    act_text, act_url = _extract_act_text(html, court_base_url)
    if act_text:
        info["Акт опубликован"] = "Да"
        info["act_text"] = act_text
    elif act_url:
        info["Акт опубликован"] = "Да"
        info["_act_url"] = act_url

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
    # Убираем script/style + теги, схлопываем пробелы
    text = _HTML_SCRIPT_RE.sub('', html)
    text = _HTML_STYLE_RE.sub('', text)
    return _strip_html(text)[:5000]  # Сырой текст, обрезается позже


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


def build_summary_line(new_cases: list[dict], changes: list[dict],
                       fi_new_cases: list[dict] | None = None,
                       stage_transitions: list[dict] | None = None,
                       fi_changes: list[dict] | None = None) -> str:
    """Сводка-саммари одной строкой: +N новых, M событий, K решений, L актов."""
    parts = []
    if fi_new_cases:
        parts.append(f"+{len(fi_new_cases)} нов. 1 инст.")
    if new_cases:
        parts.append(f"+{len(new_cases)} нов. апелл.")
    if stage_transitions:
        parts.append(f"{len(stage_transitions)} в апелляцию")
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
    if fi_changes:
        fi_hearings = sum(
            1 for ch in fi_changes
            if "fi_hearing_new" in ch["type"] or "fi_hearing_postponed" in ch["type"]
        )
        fi_status = sum(1 for ch in fi_changes if "fi_status_change" in ch["type"])
        fi_acts = sum(1 for ch in fi_changes if "fi_act_published" in ch["type"])
        fi_finals = sum(1 for ch in fi_changes if "fi_final_event" in ch["type"])
        if fi_hearings:
            parts.append(f"{fi_hearings} засед. 1 инст.")
        if fi_finals:
            parts.append(f"{fi_finals} финал 1 инст.")
        if fi_acts:
            parts.append(f"{fi_acts} акт 1 инст.")
        if fi_status:
            parts.append(f"{fi_status} статус 1 инст.")
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
    """Сохранить список словарей в CSV (атомарно: temp + os.replace)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cases)
    os.replace(tmp, path)
    log.info(f"CSV сохранён: {path} ({len(cases)} дел)")


def load_json(path: str) -> dict:
    """Загрузить JSON-базу дел. Возвращает корневой объект {version, updated_at, cases}."""
    if not os.path.exists(path):
        log.warning(f"JSON не найден: {path}")
        return {"version": 1, "updated_at": "", "cases": []}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        # Поддержка старого формата (голый список)
        return {"version": 1, "updated_at": "", "cases": data}
    return data


def save_json(data: dict, path: str):
    """Сохранить JSON-базу дел атомарно (temp + os.replace)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    data["updated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, path)
    count = len(data.get("cases", []))
    log.info(f"JSON сохранён: {path} ({count} дел)")


def find_new_cases(search_cases: list[dict], existing_numbers: set) -> list[dict]:
    """Найти дела из поиска, которых нет в текущей базе."""
    new = []
    for c in search_cases:
        num = c.get("Номер дела", "").strip()
        if num and num not in existing_numbers:
            new.append(c)
    return new


# ── Связка дел первой инстанции ↔ апелляция ────────────────────────────────

def link_cases(cases: list[dict], appeal_fi_numbers: dict[str, str]) -> list[dict]:
    """Связать дела первой инстанции с апелляцией.

    Args:
        cases: список JSON-объектов дел (формат cases.json)
        appeal_fi_numbers: маппинг {номер_апелляции: номер_дела_1_инстанции},
            полученный из parse_case_card → info["Номер дела 1 инстанции"]

    Логика:
    - Для каждого апелляционного дела с известным номером 1 инстанции:
      1. Если дело 1 инстанции уже есть в cases → мержим appeal данные в него
      2. Если нет → обновляем id на номер 1 инстанции (для будущей привязки)
    - Возвращает обновлённый список cases (дедуплицированный).
    """
    if not appeal_fi_numbers:
        return cases

    # Индексы для быстрого поиска
    fi_index: dict[str, int] = {}   # номер_1_инст → индекс в cases
    appeal_index: dict[str, int] = {}  # номер_апелляции → индекс в cases
    for i, c in enumerate(cases):
        cid = c.get("id", "")
        stage = c.get("current_stage", "")
        # Индекс по номеру 1 инстанции (если дело начато с 1 инстанции)
        fi = c.get("first_instance")
        if fi and fi.get("case_number"):
            fi_index[fi["case_number"]] = i
        # Также индексируем по id (который может быть номером 1 инст. или апелляции)
        if cid and cid not in fi_index:
            fi_index.setdefault(cid, i)
        # Индекс по номеру апелляции
        appeal = c.get("appeal")
        if appeal and appeal.get("case_number"):
            appeal_index[appeal["case_number"]] = i

    linked_count = 0
    to_remove: set[int] = set()

    for appeal_num, fi_num in appeal_fi_numbers.items():
        if not fi_num:
            continue

        appeal_idx = appeal_index.get(appeal_num)
        fi_idx = fi_index.get(fi_num)

        if appeal_idx is None:
            continue  # апелляционное дело не в нашей базе — пропускаем

        appeal_case = cases[appeal_idx]

        if fi_idx is not None and fi_idx != appeal_idx:
            # Есть оба дела — мержим апелляцию в карточку 1 инстанции
            fi_case = cases[fi_idx]
            fi_case["appeal"] = appeal_case.get("appeal")
            fi_case["current_stage"] = "appeal"
            # Обновляем общие поля из апелляции если пусты в 1 инст.
            for field in ("plaintiff", "defendant", "category", "bank_role"):
                if not fi_case.get(field) and appeal_case.get(field):
                    fi_case[field] = appeal_case[field]
            to_remove.add(appeal_idx)
            linked_count += 1
            log.info(f"  Связка: {fi_num} (1 инст.) ← {appeal_num} (апелляция)")
        else:
            # Дела 1 инстанции нет в базе — обновляем id апелляционного дела
            # на номер 1 инстанции для будущей привязки
            if appeal_case.get("id") != fi_num:
                appeal_case["id"] = fi_num
                # Заполняем first_instance.case_number если пусто
                fi = appeal_case.get("first_instance")
                if fi and not fi.get("case_number"):
                    fi["case_number"] = fi_num
                elif fi is None:
                    appeal_case["first_instance"] = {
                        "case_number": fi_num,
                        "court": "", "court_domain": "", "judge": "",
                        "filing_date": "", "status": "", "result": "",
                        "last_event": "", "event_date": "",
                        "hearing_date": "", "hearing_time": "",
                        "link": "", "act_published": False, "act_date": "",
                        "events": [],
                    }
                linked_count += 1

    # Удаляем дубликаты (апелляционные дела, которые смержены в карточку 1 инст.)
    if to_remove:
        cases = [c for i, c in enumerate(cases) if i not in to_remove]
        log.info(f"  Удалено {len(to_remove)} дубликатов после связки")

    if linked_count:
        log.info(f"Связано дел: {linked_count}")

    return cases


def split_archived(cases: list[dict]) -> tuple[list[dict], list[dict]]:
    """Разделить дела на активные и архивные.

    Архивное = is_archived(case) (Статус «Решено» + Дата события > ARCHIVE_DAYS дней).
    Возвращает (active, archive).
    """
    active, archive = [], []
    for c in cases:
        if is_archived(c):
            archive.append(c)
        else:
            active.append(c)
    return active, archive


def update_active_cases(
    cases: list[dict],
    json_appeal_by_num: dict | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Обновить карточки активных (не архивных) дел.

    json_appeal_by_num — опциональный словарь {номер_дела: appeal_dict} для
    параллельного обновления полей `events` / `last_event` / `event_date` в
    JSON-хранилище (иначе эти поля в `appeal` dict устаревают).

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

        # Параллельно обновляем JSON-представление appeal-дела (если передано)
        if json_appeal_by_num is not None:
            ap = json_appeal_by_num.get(case.get("Номер дела", "").strip())
            if ap is not None:
                if card_info.get("_events"):
                    ap["events"] = card_info["_events"]
                new_ev_j = card_info.get("Последнее событие", "")
                if new_ev_j and new_ev_j != ap.get("last_event", ""):
                    ap["last_event"] = new_ev_j
                    ap["event_date"] = card_info.get("Дата события", "")
                new_st_j = card_info.get("Статус", "")
                if new_st_j and new_st_j != ap.get("status", ""):
                    ap["status"] = new_st_j
                new_res_j = card_info.get("Результат", "")
                if new_res_j and new_res_j != ap.get("result", ""):
                    ap["result"] = new_res_j
                new_hd_j = card_info.get("Дата заседания", "")
                if new_hd_j:
                    ap["hearing_date"] = new_hd_j
                new_ht_j = card_info.get("Время заседания", "")
                if new_ht_j:
                    ap["hearing_time"] = new_ht_j
                if card_info.get("Акт опубликован", "") == "Да" and not ap.get("act_published"):
                    ap["act_published"] = True
                    if card_info.get("Дата публикации акта"):
                        ap["act_date"] = card_info["Дата публикации акта"]

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
        # Судьи (1й инстанции и докладчик апелляции) — обновляем,
        # если карточка их вернула.
        if card_info.get("Судья 1 инстанции"):
            case["Судья 1 инстанции"] = card_info["Судья 1 инстанции"]
        if card_info.get("Судья-докладчик"):
            case["Судья-докладчик"] = card_info["Судья-докладчик"]

        # ── Определяем апеллянта ──
        appellant_raw = card_info.get("_appellant_raw", "")
        if appellant_raw and not case.get("Апеллянт"):
            raw_lower = appellant_raw.lower()
            if any(p in raw_lower for p in SBER_PATTERNS):
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
_FIN_OMBUD_RE = re.compile(
    r'^Финансовый уполномоченный.*$', re.IGNORECASE,
)
_HERITAGE_RE = re.compile(
    r'наследственное имущество умершего заемщика\s+', re.IGNORECASE,
)
_QUOTES_RE = re.compile(r'[«»"]+')
_V_LICE_RE = re.compile(r'\s+в лице\s+.*', re.IGNORECASE)
_SBER_RU_RE = re.compile(r'^Сбербанк\s+России$', re.IGNORECASE)


def _shorten_single(name: str, *, keep_fio_full: bool = False) -> str:
    """Сокращение одного наименования (без запятых)."""
    name = name.strip()
    if not name:
        return name
    # МТУ Росимущество
    if _MTU_RE.match(name):
        return "МТУ Росимущество"
    # Финансовый уполномоченный по правам потребителей финансовых услуг → Фин. уполномоченный
    if _FIN_OMBUD_RE.match(name):
        return "Фин. уполномоченный"
    # Убрать ОПФ
    name = _OPF_RE.sub('', name).strip()
    # Убрать кавычки-ёлочки, оставшиеся после удаления ОПФ
    name = _QUOTES_RE.sub('', name).strip()
    # Сбербанк: убрать «в лице филиала ...», «в лице ... банка ...» и т.п.
    name = _V_LICE_RE.sub('', name).strip()
    # Сбербанк России → Сбербанк
    name = _SBER_RU_RE.sub('Сбербанк', name)
    # «города» → «г.»
    name = _CITY_RE.sub('г.', name)
    # «наследственное имущество умершего заемщика ФИО» → «насл. имущество ФИО»
    name = _HERITAGE_RE.sub('насл. имущество ', name)
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

def generate_digest(new_cases: list[dict], changes: list[dict],
                    total_active: int, cases: list[dict] | None = None,
                    fi_new_cases: list[dict] | None = None,
                    stage_transitions: list[dict] | None = None,
                    fi_changes: list[dict] | None = None) -> str:
    """Сгенерировать дайджест через Claude API."""

    if cases is None:
        cases = []
    if fi_new_cases is None:
        fi_new_cases = []
    if stage_transitions is None:
        stage_transitions = []
    if fi_changes is None:
        fi_changes = []

    if not ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY не задан, дайджест будет шаблонным")
        return generate_template_digest(new_cases, changes, total_active, cases,
                                        fi_new_cases, stage_transitions, fi_changes)

    today = datetime.now().strftime("%d.%m.%Y")
    summary = build_summary_line(
        new_cases, changes, fi_new_cases, stage_transitions, fi_changes
    )

    # ── Короткое сообщение если изменений нет ──
    if (not new_cases and not changes and not fi_new_cases
            and not stage_transitions and not fi_changes):
        msg = (
            f"✅ <b>Мониторинг дел Сбербанка — {today}</b>\n\n"
            f"Всё спокойно, изменений нет.\n"
            f"В производстве: {total_active}"
        )
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
                    line += f"\n  ИТОГ: {d.get('verdict_label', '')}"
                    line += f"\n  В чью пользу для банка: {d.get('bank_outcome', '')}"
                    line += f"\n  Категория спора: {d.get('category', '')}"
                    line += f"\n  Роль банка: {d.get('role', '')}"
                    if d.get("appellant"):
                        line += f"\n  Апеллянт: {shorten_party_name(d['appellant'])}"
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

    if fi_new_cases:
        context_parts.append("\nНОВЫЕ ДЕЛА ПЕРВОЙ ИНСТАНЦИИ:")
        for c in fi_new_cases:
            fi = c.get("first_instance", {})
            court = fi.get("court", "")
            link = fi.get("link", "")
            pl = shorten_party_name(c.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(c.get("defendant", ""), keep_fio_full=True)
            context_parts.append(
                f"- {c['id']} (суд: {court}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"категория: {c.get('category', '')}, роль банка: {c.get('bank_role', '')}, "
                f"подано: {fi.get('filing_date', '')}"
            )

    if stage_transitions:
        context_parts.append("\nПЕРЕШЛИ В АПЕЛЛЯЦИЮ:")
        for t in stage_transitions:
            fi_num = t["fi_case_number"]
            ap_num = t["appeal_case_number"]
            pl = shorten_party_name(t.get("plaintiff", ""))
            df = shorten_party_name(t.get("defendant", ""))
            context_parts.append(
                f"- {fi_num} (1 инст.) → {ap_num} (апелляция): "
                f"{pl} vs {df}"
            )

    if fi_changes:
        context_parts.append("\nИЗМЕНЕНИЯ ПО ДЕЛАМ ПЕРВОЙ ИНСТАНЦИИ:")
        for ch in fi_changes:
            d = ch["details"]
            pl = shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(ch.get("defendant", ""), keep_fio_full=True)
            line = (
                f"- {ch['case']} ({ch.get('court', '')}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"роль банка: {ch.get('bank_role', '')}"
            )
            for t in ch["type"]:
                if t == "fi_hearing_new":
                    hd = d.get("hearing_date", "")
                    ht = d.get("hearing_time", "")
                    line += (f"\n  Назначено заседание: {hd}"
                             + (f" {ht}" if ht else ""))
                elif t == "fi_hearing_postponed":
                    old_d = d.get("old_hearing_date", "")
                    old_t = d.get("old_hearing_time", "")
                    new_d = d.get("hearing_date", "")
                    new_t = d.get("hearing_time", "")
                    old_p = f"{old_d}" + (f" {old_t}" if old_t else "")
                    new_p = f"{new_d}" + (f" {new_t}" if new_t else "")
                    line += f"\n  Заседание перенесено: {old_p} → {new_p}"
                elif t == "fi_status_change":
                    line += (f"\n  Статус: {d.get('old_status', '')} "
                             f"→ {d.get('new_status', '')}")
                elif t == "fi_act_published":
                    ad = d.get("act_date", "")
                    line += f"\n  Опубликован акт" + (f" ({ad})" if ad else "")
                elif t == "fi_final_event":
                    line += f"\n  Событие: {d.get('event', '')}"
                    if d.get("event_date"):
                        line += f" ({d['event_date']})"
            context_parts.append(line)

    prompt = f"""Ты — помощник юриста ПАО Сбербанк. Сформируй дайджест изменений по судебным делам Суда ХМАО-Югры за {today}.

ИМЕНА: все наименования сторон в данных уже сокращены по правилам (ОПФ убрана, ФИО → инициалы, «в лице филиала…» удалено и т.п.). НЕ переписывай их и НЕ возвращай ОПФ обратно. В секции 📥 «Новые дела» имена физлиц приходят полными — там оставляй как есть.

ДАТЫ: бери ровно из переданных полей данных. Не используй today() и не угадывай. Если у дела есть пометка «Заседание состоялось давно» — реальная дата уже в поле «Дата апелляционного определения», не пиши «сегодня».

ФОРМАТ: HTML для Telegram. Разрешены только теги <b>, <i>, <a href="URL">. Никакого Markdown (* _ ` [ ]). Спецсимволы &lt; &gt; &amp; экранируй.

СТРУКТУРА — включай только секции, по которым есть данные:

1. Заголовок: 📊 Дайджест судебных дел | Суд ХМАО-Югры | {today}
2. 📋 Сводка одной строкой (краткий итог: N событий, N решений и т.д.)
3. 🏛 Первая инстанция: новые иски — номер дела, суд, кто подал к кому, категория, роль банка, дата подачи. Имена физлиц приходят полными — оставляй как есть.
4. 🏛 Первая инстанция: изменения — только если есть данные в секции «ИЗМЕНЕНИЯ ПО ДЕЛАМ ПЕРВОЙ ИНСТАНЦИИ». Одна строка на дело: <b>номер</b> ({{суд}}) — {{стороны кратко}} | событие (назначено заседание ДД.ММ ЧЧ:ММ / перенесено ДД.ММ→ДД.ММ / статус X→Y / опубликован акт / мотивированное решение / возвращение иска / в архив). НЕ смешивай с секцией апелляционных актов.
5. 🔀 Перешли в апелляцию — номер дела 1 инст. → номер апелляции, стороны (кратко). Показывай только если есть данные в секции «ПЕРЕШЛИ В АПЕЛЛЯЦИЮ».
6. 📥 Новые дела апелляции — номер как <a href="URL"><b>номер</b></a>, кто подал к кому, о чём, суд 1 инстанции, роль банка
7. ⚖️ Вынесенные судебные акты — одна строка на дело:
   <a href="URL"><b>номер</b></a> — Апелляционное определение от <дата>. ИТОГ: <дословно поле ИТОГ>. Категория: <дословно>. Стороны: <истец> vs <ответчик>, банк — <роль>. Для банка: <дословно «В чью пользу для банка»>.
   • если ИТОГ = «возвращена / без рассмотрения / прекращено / снято» — добавь причину из «Последнее событие»
   • если ИТОГ = «отменено / изменено» и есть «Цитата из мотивировки» — добавь 1 фразу с ключевым доводом суда
   • НЕ переформулируй ИТОГ своими словами и не подменяй его шаблоном
   • НЕ включай дела, у которых в данных НЕТ блока «ИТОГ»
   • НЕ упоминай «составлено мотивированное определение» — это служебный шаг
8. 📄 Опубликованные акты — номер (ссылка), стороны, итог (удовлетворена / отказано / частично) и 1-2 предложения ПОЧЕМУ суд так решил (по полю «МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА»). Не пиши просто номера без содержания.

ШАБЛОН ЗАСЕДАНИЙ — две строки на дело, между делами пустая строка:
   строка 1: <b>дата/время</b> + <a href="URL"><b>номер</b></a>
   строка 2: стороны | категория. Роль банка если известна.

Три типа секций по этому шаблону:
9. 📅 Назначенные заседания — <b>ДД.ММ HH:MM</b>. НЕ помещай сюда дела с пометкой «ОТЛОЖЕНО».
10. 🔁 Отложенные заседания — формат строки 1: 🔁 <a href="URL"><b>номер</b></a>: ⏪ ДД.ММ.ГГГГ HH:MM → ⏩ ДД.ММ.ГГГГ HH:MM (даты строго из строки «ОТЛОЖЕНО:» в данных). Эта секция РЕДКАЯ и ВАЖНАЯ — никогда не выкидывай при нехватке места.

11. 📌 Итоговая строка: всего дел в производстве: {total_active} (из них 1 инст.: показать число если >0)
12. В конце: <a href="{DASHBOARD_URL}">📊 Дашборд</a> — обязательно всегда.

ОФОРМЛЕНИЕ: без маркеров списка («• », «- »); названия секций — <b>жирным</b>; номера дел — <b>жирным</b> внутри ссылок; пустые строки для читаемости.

СТИЛЬ: кратко, по-деловому, на русском. Без вступлений. Не дублируй информацию между секциями.

ЛИМИТ: {DIGEST_CHAR_LIMIT} символов. При нехватке места сокращать описания актов. Секцию 🔁 «Отложенные заседания» — НЕ выкидывать никогда. Ссылка на дашборд — ВСЕГДА в конце.

ВАЖНО: в разделе «Данные» ниже перечислены только ИЗМЕНЕНИЯ за сегодня, а не все дела. Общее число дел в производстве: {total_active} — используй именно это число в итоговой строке (пункт 8).

Данные:
{chr(10).join(context_parts)}"""

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
                # Низкая температура: дайджест требует дословного цитирования
                # ИТОГа и категории — креативность модели тут вредит. Стабильность
                # формата важнее разнообразия формулировок.
                "temperature": 0.2,
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
                new_cases, changes, total_active, cases,
                fi_new_cases, stage_transitions, fi_changes,
            )
        # До двух сообщений: лимит 2×4096; split_message в send_telegram разобьёт
        return truncate_html_message(text, TELEGRAM_MSG_LIMIT * 2)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        body = (e.response.text or "")[:500] if e.response is not None else ""
        log.error(f"Claude API HTTP {status}: {body}")
        return generate_template_digest(
            new_cases, changes, total_active, cases,
            fi_new_cases, stage_transitions, fi_changes,
        )
    except requests.RequestException as e:
        log.error(f"Claude API сетевая ошибка: {e}")
        return generate_template_digest(
            new_cases, changes, total_active, cases,
            fi_new_cases, stage_transitions, fi_changes,
        )
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        log.error(f"Claude API неожиданный ответ: {e}")
        return generate_template_digest(
            new_cases, changes, total_active, cases,
            fi_new_cases, stage_transitions, fi_changes,
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
                             cases: list[dict] | None = None,
                             fi_new_cases: list[dict] | None = None,
                             stage_transitions: list[dict] | None = None,
                             fi_changes: list[dict] | None = None) -> str:
    """Шаблонный дайджест (fallback без Claude API). Формат: HTML."""
    today = datetime.now().strftime("%d.%m.%Y")
    if cases is None:
        cases = []
    if fi_new_cases is None:
        fi_new_cases = []
    if stage_transitions is None:
        stage_transitions = []
    if fi_changes is None:
        fi_changes = []

    # ── Короткое сообщение если изменений нет ──
    if (not new_cases and not changes and not fi_new_cases
            and not stage_transitions and not fi_changes):
        msg = (
            f"✅ <b>Мониторинг дел Сбербанка — {today}</b>\n\n"
            f"Всё спокойно, изменений нет.\n"
            f"В производстве: {total_active}"
        )
        msg += f'\n\n<a href="{DASHBOARD_URL}">📊 Дашборд</a>'
        return msg

    # ── Полный дайджест ──
    summary = build_summary_line(
        new_cases, changes, fi_new_cases, stage_transitions, fi_changes
    )
    lines = [f"📊 <b>Мониторинг дел Сбербанка — {today}</b>"]
    lines.append(f"📋 {escape_html(summary)}\n")

    if fi_new_cases:
        lines.append(f"🏛 <b>Первая инстанция: новые иски ({len(fi_new_cases)}):</b>")
        for c in fi_new_cases:
            fi = c.get("first_instance", {})
            court = escape_html(fi.get("court", ""))
            role = c.get("bank_role", "")
            role_icon = {"Истец": "🏦→", "Ответчик": "→🏦", "Третье лицо": "👁"
                         }.get(role, "")
            cat = category_short(c.get("category", ""))
            pl = escape_html(shorten_party_name(c.get("plaintiff", ""), keep_fio_full=True))
            df = escape_html(shorten_party_name(c.get("defendant", ""), keep_fio_full=True))
            num = escape_html(c.get("id", ""))
            filing = escape_html(fi.get("filing_date", ""))
            lines.append(
                f"  <b>{num}</b> {role_icon}"
                f"{pl} vs {df} "
                f"({cat}) | {court}"
                + (f" | подано {filing}" if filing else "")
            )

    if fi_changes:
        lines.append(f"\n🏛 <b>Первая инстанция — изменения ({len(fi_changes)}):</b>")
        for ch in fi_changes:
            num = escape_html(ch.get("case", ""))
            court = escape_html(ch.get("court", ""))
            pl = escape_html(shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True))
            df = escape_html(shorten_party_name(ch.get("defendant", ""), keep_fio_full=True))
            d = ch["details"]
            events: list[str] = []
            for t in ch["type"]:
                if t == "fi_hearing_new":
                    hd = escape_html(d.get("hearing_date", ""))
                    ht = escape_html(d.get("hearing_time", ""))
                    events.append(f"📅 заседание {hd}" + (f" {ht}" if ht else ""))
                elif t == "fi_hearing_postponed":
                    old_p = escape_html(
                        d.get("old_hearing_date", "")
                        + (f" {d['old_hearing_time']}" if d.get("old_hearing_time") else "")
                    )
                    new_p = escape_html(
                        d.get("hearing_date", "")
                        + (f" {d['hearing_time']}" if d.get("hearing_time") else "")
                    )
                    events.append(f"🔁 {old_p} → {new_p}")
                elif t == "fi_status_change":
                    events.append(
                        f"статус: {escape_html(d.get('old_status', ''))} → "
                        f"{escape_html(d.get('new_status', ''))}"
                    )
                elif t == "fi_act_published":
                    ad = escape_html(d.get("act_date", ""))
                    events.append("📄 опубликован акт" + (f" ({ad})" if ad else ""))
                elif t == "fi_final_event":
                    events.append(f"⚖️ {escape_html(d.get('event', ''))}")
            ev_str = "; ".join(events) if events else ""
            lines.append(
                f"  <b>{num}</b> ({court}) — {pl} vs {df} | {ev_str}"
            )

    if stage_transitions:
        lines.append(f"\n🔀 <b>Перешли в апелляцию ({len(stage_transitions)}):</b>")
        for t in stage_transitions:
            fi_num = escape_html(t["fi_case_number"])
            ap_num = escape_html(t["appeal_case_number"])
            pl = escape_html(shorten_party_name(t.get("plaintiff", "")))
            df = escape_html(shorten_party_name(t.get("defendant", "")))
            lines.append(
                f"  <b>{fi_num}</b> → <b>{ap_num}</b>: {pl} vs {df}"
            )

    if new_cases:
        lines.append(f"\n📥 <b>Новые дела апелляции ({len(new_cases)}):</b>")
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

    lines.append(f"\nВ производстве: {total_active}")
    lines.append(f'<a href="{DASHBOARD_URL}">📊 Дашборд</a>')

    text = "\n".join(lines)
    # До двух сообщений: лимит 2×4096; split_message в send_telegram разобьёт
    return truncate_html_message(text, TELEGRAM_MSG_LIMIT * 2)


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
                METRICS["telegram_sent"] += 1
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
                    METRICS["telegram_sent"] += 1
                    log.info("Telegram: отправлено без разметки")
                else:
                    METRICS["telegram_failed"] += 1
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


# ── Run summary ──────────────────────────────────────────────────────────────

def _format_timings(timings: dict[str, float]) -> str:
    """Форматирует словарь этап→секунды в короткую строку."""
    order = ["load_csv", "search", "cards_update", "digest", "telegram", "save", "total"]
    seen = set(order)
    known = [(k, timings[k]) for k in order if k in timings]
    extra = [(k, v) for k, v in timings.items() if k not in seen]
    return " | ".join(f"{k} {v:.1f}s" for k, v in known + extra)


def log_run_summary(
    mode: str,
    timings: dict[str, float],
    extras: dict[str, object] | None = None,
) -> None:
    """
    Печатает итоговый блок метрик в лог и (если переменная установлена)
    в $GITHUB_STEP_SUMMARY — так он виден прямо в UI GitHub Actions.
    """
    extras = extras or {}
    req_line = (
        f"Requests: {METRICS['requests_ok']} ok / "
        f"{METRICS['requests_failed']} failed"
    )
    if METRICS["requests_retried"]:
        req_line += f" ({METRICS['requests_retried']} retried)"
    tg_line = (
        f"Telegram: {METRICS['telegram_sent']} sent"
        + (f", {METRICS['telegram_failed']} failed" if METRICS['telegram_failed'] else "")
    )
    lines = [
        "=" * 60,
        f"Run summary ({mode})",
        "=" * 60,
    ]
    if extras:
        # Превращаем extras в "k=v | k=v" в том порядке, в котором их передали
        lines.append(" | ".join(f"{k}: {v}" for k, v in extras.items()))
    lines.append(req_line)
    lines.append(tg_line)
    if timings:
        lines.append(f"Timing: {_format_timings(timings)}")
    lines.append("=" * 60)

    for line in lines:
        log.info(line)

    # GitHub Actions: при наличии $GITHUB_STEP_SUMMARY дописываем markdown-блок,
    # который появится в UI раздела Summary у запуска workflow.
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        try:
            md_lines = [
                f"### Run summary ({mode})",
                "",
            ]
            if extras:
                md_lines.append("| Метрика | Значение |")
                md_lines.append("| --- | --- |")
                for k, v in extras.items():
                    md_lines.append(f"| {k} | {v} |")
                md_lines.append("")
            md_lines.append(f"- {req_line}")
            md_lines.append(f"- {tg_line}")
            if timings:
                md_lines.append(f"- Timing: `{_format_timings(timings)}`")
            md_lines.append("")
            with open(summary_path, "a", encoding="utf-8") as f:
                f.write("\n".join(md_lines))
        except Exception as e:
            log.warning(f"Не удалось записать GITHUB_STEP_SUMMARY: {e}")


# ── Аварийный алерт ──────────────────────────────────────────────────────────

def send_crash_alert(mode: str, exc: BaseException) -> None:
    """
    Попытаться сообщить в Telegram, что прогон упал.
    Не должен сам кидать исключение, иначе перекроет исходное.
    """
    try:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        tb_tail = tb[-1500:]  # хвост трейсбека, чтобы не упереться в лимит Telegram
        text = (
            "⚠️ <b>Прогон упал</b>\n"
            f"Режим: <code>{html_escape(mode)}</code>\n"
            f"Ошибка: <code>{html_escape(type(exc).__name__)}: {html_escape(str(exc))}</code>\n\n"
            f"<pre>{html_escape(tb_tail)}</pre>"
        )
        send_telegram(text)
    except Exception as alert_err:
        log.error(f"Не удалось отправить crash-алерт в Telegram: {alert_err}")


# ── Проверка окружения ───────────────────────────────────────────────────────

def validate_environment(require_anthropic: bool = True) -> None:
    """
    Проверить, что нужные переменные окружения заданы.
    Падает сразу с понятным сообщением, не через 3 минуты парсинга.

    require_anthropic: False для режимов без дайджеста (например, dry-run).
    """
    missing: list[str] = []
    if require_anthropic and not ANTHROPIC_API_KEY:
        missing.append("ANTHROPIC_API_KEY")
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")

    if missing:
        log.error(
            "Не заданы обязательные переменные окружения: %s",
            ", ".join(missing),
        )
        sys.exit(2)


# ── Проверка доступности сайта суда ──────────────────────────────────────────

def check_court_available(court: CourtConfig | None = None) -> bool:
    """Проверить что сайт суда отвечает."""
    url = court.base_url if court else BASE_URL
    try:
        r = session.get(url, timeout=15)
        return r.status_code == 200
    except Exception:
        return False


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Запуск мониторинга дел Сбербанка")
    log.info("=" * 60)

    _metrics_reset()
    validate_environment()

    # Таймеры этапов: ключ = название этапа, значение = секунды.
    timings: dict[str, float] = {}
    t_total_start = time.perf_counter()

    # 1. Проверяем доступность суда
    if not check_court_available():
        msg = "⚠️ Сайт суда oblsud--hmao.sudrf.ru недоступен. Обновление отложено."
        log.error(msg)
        send_telegram(msg)
        sys.exit(1)

    log.info("Сайт суда доступен")

    # 2. Загружаем текущие данные
    t0 = time.perf_counter()
    cases = load_csv(CSV_PATH)
    timings["load_csv"] = time.perf_counter() - t0
    existing_numbers = {c["Номер дела"].strip() for c in cases if c.get("Номер дела")}
    log.info(f"Загружено {len(cases)} дел из CSV")

    active_count = sum(1 for c in cases if not is_archived(c))
    archived_count = len(cases) - active_count
    log.info(f"Активных: {active_count}, архивных: {archived_count}")

    # 3. Поиск новых дел (первая страница)
    t0 = time.perf_counter()
    log.info("Загружаю первую страницу поиска...")
    search_html = fetch_page(SEARCH_URL)
    new_cases = []
    if search_html:
        search_cases = parse_search_page(search_html)
        log.info(f"На первой странице найдено {len(search_cases)} дел")

        # Alert, если парсер вернул 0 дел, хотя CSV знает активные дела.
        # Обычно это признак изменения структуры страницы суда — важно
        # узнать об этом сразу, а не после того как CSV молча затёрт.
        if not search_cases and active_count > 0:
            warn = (
                "⚠️ Парсинг первой страницы поиска вернул 0 дел, "
                f"но в CSV {active_count} активных. "
                "Возможно, изменилась структура сайта суда — проверьте parse_search_page."
            )
            log.warning(warn)
            send_telegram(warn)

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
                    if card_info.get("Судья 1 инстанции"):
                        nc["Судья 1 инстанции"] = card_info["Судья 1 инстанции"]
                    if card_info.get("Судья-докладчик"):
                        nc["Судья-докладчик"] = card_info["Судья-докладчик"]
                    log.info(f"  Карточка {nc['Номер дела']}: OK")
    else:
        log.warning("Не удалось загрузить страницу поиска")
    timings["search"] = time.perf_counter() - t0

    # 4. Обновляем активные дела
    t0 = time.perf_counter()
    log.info(f"Обновляю {active_count} активных дел...")
    cases, changes = update_active_cases(cases)
    timings["cards_update"] = time.perf_counter() - t0

    # 5. Добавляем новые дела в начало списка
    if new_cases:
        cases = new_cases + cases
        log.info(f"Добавлено {len(new_cases)} новых дел")

    # 6. Считаем итоги
    total_active = sum(1 for c in cases if c.get("Статус", "").strip() != "Решено")

    # 7. Генерируем дайджест
    t0 = time.perf_counter()
    log.info("Генерирую дайджест...")
    digest = generate_digest(new_cases, changes, total_active, cases)
    timings["digest"] = time.perf_counter() - t0

    # 8. Отправляем в Telegram
    t0 = time.perf_counter()
    send_telegram(digest)
    timings["telegram"] = time.perf_counter() - t0

    # 9. Разделяем на активные и архивные (Решено + 30+ дней)
    t0 = time.perf_counter()
    active, newly_archived = split_archived(cases)
    if newly_archived:
        existing_archive = load_csv(CSV_ARCHIVE_PATH)
        existing_nums = {
            c.get("Номер дела", "").strip()
            for c in existing_archive if c.get("Номер дела")
        }
        to_add = [
            c for c in newly_archived
            if c.get("Номер дела", "").strip() not in existing_nums
        ]
        if to_add:
            save_csv(existing_archive + to_add, CSV_ARCHIVE_PATH)
            log.info(f"В архив перенесено: {len(to_add)} дел")
        else:
            log.info(f"В архиве уже есть все {len(newly_archived)} архивных дел")

    # 10. Сохраняем активные дела (главный CSV)
    save_csv(active, CSV_PATH)
    timings["save"] = time.perf_counter() - t0

    timings["total"] = time.perf_counter() - t_total_start

    log_run_summary(
        mode="main",
        timings=timings,
        extras={
            "Cases checked": active_count,
            "New": len(new_cases),
            "Changes": len(changes),
            "Active after": len(active),
            "Archived moved": len(newly_archived),
        },
    )


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

    validate_environment()

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


def _fi_search_to_json_case(fi: dict) -> dict:
    """Конвертировать результат parse_first_instance_search() в JSON-структуру дела."""
    return {
        "id": fi["case_number"],
        "current_stage": "first_instance",
        "plaintiff": fi.get("plaintiff", ""),
        "defendant": fi.get("defendant", ""),
        "category": fi.get("category", ""),
        "bank_role": fi.get("bank_role", "Ответчик"),
        "notes": "",
        "first_instance": {
            "case_number": fi["case_number"],
            "court": fi.get("court", ""),
            "court_domain": fi.get("court_domain", ""),
            "judge": fi.get("judge", ""),
            "filing_date": fi.get("filing_date", ""),
            "status": fi.get("status", "В производстве"),
            "result": fi.get("result", ""),
            "last_event": "",
            "event_date": "",
            "hearing_date": "",
            "hearing_time": "",
            "link": fi.get("link", ""),
            "act_published": False,
            "act_date": "",
            "events": [],
        },
        "appeal": None,
    }


def main_json():
    """Основной цикл с JSON-хранилищем: 1 инстанция + апелляция."""
    log.info("=" * 60)
    log.info("Запуск мониторинга дел Сбербанка (JSON-режим)")
    log.info("=" * 60)

    _metrics_reset()
    validate_environment()

    timings: dict[str, float] = {}
    t_total_start = time.perf_counter()

    # 1. Загружаем текущие данные JSON
    t0 = time.perf_counter()
    data = load_json(JSON_PATH)
    cases = data.get("cases", [])
    timings["load_json"] = time.perf_counter() - t0

    # Индексы для быстрого поиска по всем номерам дел
    existing_ids = set()
    for c in cases:
        existing_ids.add(c.get("id", ""))
        fi = c.get("first_instance")
        if fi and fi.get("case_number"):
            existing_ids.add(fi["case_number"])
        ap = c.get("appeal")
        if ap and ap.get("case_number"):
            existing_ids.add(ap["case_number"])

    log.info(f"Загружено {len(cases)} дел из JSON")

    # ── 2. Парсинг апелляции: новые дела ──
    t0 = time.perf_counter()
    csv_cases = load_csv(CSV_PATH)
    csv_existing = {c["Номер дела"].strip() for c in csv_cases if c.get("Номер дела")}
    csv_active_count = sum(1 for c in csv_cases if not is_archived(c))

    log.info("Загружаю страницу поиска апелляции...")
    search_html = fetch_page(APPEAL_COURT.search_url())
    appeal_new_cases_csv: list[dict] = []
    appeal_fi_numbers: dict[str, str] = {}

    if search_html:
        search_cases = parse_search_page(search_html)
        log.info(f"Апелляция: {len(search_cases)} дел на странице")

        if not search_cases and csv_active_count > 0:
            warn = (
                "⚠️ Парсинг апелляции вернул 0 дел, "
                f"но в CSV {csv_active_count} активных."
            )
            log.warning(warn)
            send_telegram(warn)

        appeal_new_cases_csv = find_new_cases(search_cases, csv_existing)
        log.info(f"Апелляция: {len(appeal_new_cases_csv)} новых")

        # Для новых дел загружаем карточки и извлекаем номер 1 инстанции
        for nc in appeal_new_cases_csv:
            cid, cuid = case_id_uid(nc.get("Ссылка", ""))
            if cid and cuid:
                polite_delay()
                url = APPEAL_COURT.card_url(cid, cuid)
                card_html = fetch_page(url)
                if card_html:
                    card_info = parse_case_card(card_html, APPEAL_COURT.base_url)
                    nc["Последнее событие"] = card_info.get("Последнее событие", "")
                    nc["Дата события"] = card_info.get("Дата события", "")
                    nc["Время заседания"] = card_info.get("Время заседания", "")
                    nc["Статус"] = card_info.get("Статус", "В производстве")
                    nc["Результат"] = card_info.get("Результат", "")
                    nc["Акт опубликован"] = card_info.get("Акт опубликован", "Нет")
                    if card_info.get("Судья 1 инстанции"):
                        nc["Судья 1 инстанции"] = card_info["Судья 1 инстанции"]
                    if card_info.get("Судья-докладчик"):
                        nc["Судья-докладчик"] = card_info["Судья-докладчик"]
                    fi_num = card_info.get("Номер дела 1 инстанции", "")
                    if fi_num:
                        appeal_fi_numbers[nc["Номер дела"]] = fi_num
                    log.info(f"  Карточка {nc['Номер дела']}: OK (1 инст: {fi_num or '?'})")

    timings["appeal_new"] = time.perf_counter() - t0

    # ── 3. Парсинг судов первой инстанции: новые дела ──
    t0 = time.perf_counter()
    fi_new_cases: list[dict] = []
    enabled_courts = [c for c in FIRST_INSTANCE_COURTS if c.enabled]
    log.info(f"Парсинг {len(enabled_courts)} судов первой инстанции...")

    for court in enabled_courts:
        polite_delay()
        search_html = fetch_page(court.search_url())
        if not search_html:
            log.warning(f"  {court.name}: не удалось загрузить поиск")
            continue

        fi_results = parse_first_instance_search(search_html, court)
        # Фильтр: только новые дела (первая страница поиска)
        new_fi = [
            r for r in fi_results
            if r["case_number"] not in existing_ids
        ]
        if new_fi:
            log.info(f"  {court.name}: {len(fi_results)} дел, {len(new_fi)} новых")
            for fi in new_fi:
                json_case = _fi_search_to_json_case(fi)
                fi_new_cases.append(json_case)
                existing_ids.add(fi["case_number"])
        else:
            log.info(f"  {court.name}: {len(fi_results)} дел, новых нет")

    timings["first_instance"] = time.perf_counter() - t0
    log.info(f"Итого новых дел 1 инстанции: {len(fi_new_cases)}")

    # ── 4. Обновление существующих дел ──
    # 4a. Апелляция: обновляем активные дела из CSV
    # Параллельно обновляем соответствующие appeal dicts в JSON (events, last_event, и т.п.)
    t0 = time.perf_counter()
    log.info(f"Обновляю {csv_active_count} активных дел апелляции...")
    json_appeal_by_num: dict = {}
    for c in cases:
        ap = c.get("appeal")
        if ap and ap.get("case_number"):
            json_appeal_by_num[ap["case_number"].strip()] = ap
    csv_cases, changes = update_active_cases(csv_cases, json_appeal_by_num)

    if appeal_new_cases_csv:
        csv_cases = appeal_new_cases_csv + csv_cases

    timings["appeal_update"] = time.perf_counter() - t0

    # 4b. Первая инстанция: обновляем активные дела из JSON
    t0 = time.perf_counter()
    fi_active = [
        c for c in cases
        if c.get("current_stage") == "first_instance"
        and c.get("first_instance", {}).get("case_number")
    ]
    log.info(f"Обновляю {len(fi_active)} активных дел 1 инстанции...")
    fi_court_map = {ct.domain: ct for ct in FIRST_INSTANCE_COURTS if ct.enabled}
    fi_update_count = 0
    fi_changes: list[dict] = []

    # Маркеры мусорного значения «Результат» из карточек 1 инстанции:
    # иногда парсер цепляет стандартную подсказку сайта вместо реального
    # результата. Игнорируем такие значения, чтобы не переписывать
    # осмысленные данные и не поднимать ложные события в дайджесте.
    _garbage_result_markers = ("Дата размещения", "Информация о размещении")

    for case_j in fi_active:
        fi = case_j.get("first_instance", {})
        court_domain = fi.get("court_domain", "")
        court_cfg = fi_court_map.get(court_domain)
        if not court_cfg:
            continue
        link_raw = fi.get("link", "")
        if not link_raw:
            continue
        # Извлекаем case_id и case_uid из ссылки
        pm = re.match(r'^(\d+)\|([a-f0-9-]+)$', link_raw)
        if not pm:
            continue
        cid, cuid = pm.group(1), pm.group(2)
        polite_delay()
        url = court_cfg.card_url(cid, cuid)
        html = fetch_page(url)
        if not html:
            log.warning(f"  {fi['case_number']}: не удалось загрузить карточку")
            continue
        card_info = parse_case_card(html, court_cfg.base_url)

        # Снимок до обновления — нужен для diff и дайджеста
        old_event = fi.get("last_event", "")
        old_status = fi.get("status", "")
        old_result = fi.get("result", "")
        old_hearing_date = fi.get("hearing_date", "")
        old_hearing_time = fi.get("hearing_time", "")
        old_act = bool(fi.get("act_published", False))

        new_ev = card_info.get("Последнее событие", "")
        new_status = card_info.get("Статус", "")
        new_result = card_info.get("Результат", "")
        new_hearing_date = card_info.get("Дата заседания", "")
        new_hearing_time = card_info.get("Время заседания", "")
        new_act = card_info.get("Акт опубликован", "") == "Да"

        # Гард 1: мусорный «Результат» — не пишем в JSON и игнорируем.
        if new_result and any(m in new_result for m in _garbage_result_markers):
            new_result = ""
        # Чистим уже сохранённый мусор: если old_result содержит маркер
        # дисклеймера (попал туда до фикса парсера), обнуляем поле —
        # даже если карточка вернула пустой new_result.
        old_has_garbage = bool(old_result) and any(
            m in old_result for m in _garbage_result_markers
        )
        if old_has_garbage and not new_result:
            fi["result"] = ""
            changed = True
            old_result = ""
        # Гард 2: регрессия статуса Решено → В производстве обычно означает,
        # что карточка не вернула статус корректно (мусор в поле result или
        # отсутствие нужного last_event). Не понижаем статус.
        if old_status == "Решено" and new_status == "В производстве":
            new_status = old_status

        # ── Обновляем поля первой инстанции ──
        changed = False
        if new_ev and new_ev != old_event:
            fi["last_event"] = new_ev
            fi["event_date"] = card_info.get("Дата события", "")
            changed = True
        if new_status and new_status != old_status:
            fi["status"] = new_status
            changed = True
        if new_result and new_result != old_result:
            fi["result"] = new_result
            changed = True
        if new_hearing_date:
            fi["hearing_date"] = new_hearing_date
        if new_hearing_time:
            fi["hearing_time"] = new_hearing_time
        if card_info.get("Судья"):
            fi["judge"] = card_info["Судья"]
        if new_act:
            fi["act_published"] = True
            if card_info.get("Дата публикации акта"):
                fi["act_date"] = card_info["Дата публикации акта"]
        # Полный список событий — обновляем всегда, если парсер его вернул
        if card_info.get("_events"):
            fi["events"] = card_info["_events"]
        if changed:
            fi_update_count += 1

        # ── Собираем события для дайджеста ──
        change = {
            "case": fi.get("case_number", ""),
            "court": fi.get("court", ""),
            "plaintiff": case_j.get("plaintiff", ""),
            "defendant": case_j.get("defendant", ""),
            "bank_role": case_j.get("bank_role", ""),
            "category": case_j.get("category", ""),
            "type": [],
            "details": {},
        }

        # Новое/перенесённое заседание
        if new_hearing_date and new_hearing_date != old_hearing_date:
            if not old_hearing_date:
                change["type"].append("fi_hearing_new")
            else:
                change["type"].append("fi_hearing_postponed")
                change["details"]["old_hearing_date"] = old_hearing_date
                change["details"]["old_hearing_time"] = old_hearing_time
            change["details"]["hearing_date"] = new_hearing_date
            change["details"]["hearing_time"] = new_hearing_time

        # Смена статуса (регрессии отфильтрованы выше)
        if new_status and new_status != old_status:
            change["type"].append("fi_status_change")
            change["details"]["old_status"] = old_status
            change["details"]["new_status"] = new_status

        # Публикация акта
        if new_act and not old_act:
            change["type"].append("fi_act_published")
            change["details"]["act_date"] = card_info.get("Дата публикации акта", "")

        # Финальные события в движении дела — значимые для юриста
        if new_ev and new_ev != old_event:
            ev_l = new_ev.lower()
            final_markers = (
                "в архив",
                "возвращение иска",
                "мотивированное решение",
                "мотивированного решения",
            )
            if any(m in ev_l for m in final_markers):
                change["type"].append("fi_final_event")
                change["details"]["event"] = new_ev
                change["details"]["event_date"] = card_info.get("Дата события", "")

        if change["type"]:
            fi_changes.append(change)

        log.info(f"  {fi['case_number']}: {'обновлено' if changed else 'без изменений'}")

    timings["fi_update"] = time.perf_counter() - t0
    log.info(f"Обновлено дел 1 инстанции: {fi_update_count}")

    # ── 5. Сохраняем CSV (обратная совместимость) ──
    t0 = time.perf_counter()
    active_csv, newly_archived_csv = split_archived(csv_cases)
    if newly_archived_csv:
        existing_archive = load_csv(CSV_ARCHIVE_PATH)
        existing_nums = {
            c.get("Номер дела", "").strip()
            for c in existing_archive if c.get("Номер дела")
        }
        to_add = [
            c for c in newly_archived_csv
            if c.get("Номер дела", "").strip() not in existing_nums
        ]
        if to_add:
            save_csv(existing_archive + to_add, CSV_ARCHIVE_PATH)
    save_csv(active_csv, CSV_PATH)

    # ── 6. Обновляем JSON-базу: добавляем новые дела 1 инстанции ──
    if fi_new_cases:
        cases = fi_new_cases + cases
        log.info(f"Добавлено {len(fi_new_cases)} дел 1 инстанции в JSON")

    # ── 7. Связка дел ──
    # Запоминаем стадии ДО связки, чтобы обнаружить переходы в апелляцию
    stage_before: dict[str, str] = {}
    if appeal_fi_numbers:
        fi_nums_set = set(appeal_fi_numbers.values())
        for c in cases:
            cid = c.get("id", "")
            fi = c.get("first_instance")
            fi_num = fi.get("case_number", "") if fi else ""
            if cid in fi_nums_set or fi_num in fi_nums_set:
                stage_before[cid] = c.get("current_stage", "")

    stage_transitions: list[dict] = []
    if appeal_fi_numbers:
        log.info(f"Связка дел: {len(appeal_fi_numbers)} апелляций с номерами 1 инстанции")
        cases = link_cases(cases, appeal_fi_numbers)

        # Обнаруживаем переходы: current_stage был first_instance → стал appeal
        for c in cases:
            cid = c.get("id", "")
            if cid in stage_before and stage_before[cid] == "first_instance":
                if c.get("current_stage") == "appeal":
                    ap = c.get("appeal", {}) or {}
                    stage_transitions.append({
                        "fi_case_number": cid,
                        "appeal_case_number": ap.get("case_number", ""),
                        "plaintiff": c.get("plaintiff", ""),
                        "defendant": c.get("defendant", ""),
                    })
        if stage_transitions:
            log.info(f"Переходов в апелляцию: {len(stage_transitions)}")

    # ── 8. Сохраняем JSON ──
    data["cases"] = cases
    save_json(data, JSON_PATH)
    timings["save"] = time.perf_counter() - t0

    # ── 9. Дайджест и Telegram ──
    # total_active: апелляция (CSV) + 1 инстанция (JSON, ещё не в апелляции)
    total_active_appeal = sum(
        1 for c in csv_cases if c.get("Статус", "").strip() != "Решено"
    )
    total_active_fi = sum(
        1 for c in cases if c.get("current_stage") == "first_instance"
    )
    total_active = total_active_appeal + total_active_fi
    t0 = time.perf_counter()
    log.info("Генерирую дайджест...")
    digest = generate_digest(
        appeal_new_cases_csv, changes, total_active, csv_cases,
        fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
        fi_changes=fi_changes,
    )
    timings["digest"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    send_telegram(digest)
    timings["telegram"] = time.perf_counter() - t0

    timings["total"] = time.perf_counter() - t_total_start

    log_run_summary(
        mode="main-json",
        timings=timings,
        extras={
            "FI courts": len(enabled_courts),
            "FI new": len(fi_new_cases),
            "FI updated": fi_update_count,
            "FI changes": len(fi_changes),
            "Stage transitions": len(stage_transitions),
            "Appeal new": len(appeal_new_cases_csv),
            "Appeal changes": len(changes),
            "JSON total": len(cases),
        },
    )


def main_digest_only():
    """Сформировать и отправить дайджест по текущим данным CSV (без обращения к сайту суда)."""
    log.info("=" * 60)
    log.info("Режим digest-only: дайджест по текущим данным")
    log.info("=" * 60)

    validate_environment()

    cases = load_csv(CSV_PATH)
    log.info(f"Загружено {len(cases)} дел из CSV")

    total_active = sum(1 for c in cases if c.get("Статус", "").strip() != "Решено")
    log.info(f"В производстве: {total_active}")

    log.info("Генерирую дайджест...")
    digest = generate_digest([], [], total_active, cases)

    send_telegram(digest)
    log.info("Готово!")


if __name__ == "__main__":
    # Выбор режима
    if "--digest-only" in sys.argv:
        mode_name = "digest-only"
        entry = main_digest_only
        entry_args: tuple = ()
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
        mode_name = f"force-digest-for {case_num}"
        entry = main_force_postponement_digest
        entry_args = (case_num, old_d, old_t)
    elif "--json" in sys.argv:
        mode_name = "main-json"
        entry = main_json
        entry_args = ()
    else:
        mode_name = "main"
        entry = main
        entry_args = ()

    # Оборачиваем прогон в try/except: любое необработанное исключение уходит
    # в Telegram, чтобы не потерять падение в логах Actions.
    try:
        entry(*entry_args)
    except SystemExit:
        # sys.exit(N) — штатный выход, алерт не нужен
        raise
    except BaseException as exc:
        log.exception("Необработанное исключение в прогоне")
        send_crash_alert(mode_name, exc)
        sys.exit(1)
