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

    def card_url_alt(self, case_id: str, case_uid: str) -> str:
        # Фолбэк с new=0: при появлении вкладки «обжалование решений,
        # определений (пост.)» карточка 1 инст. при new=5 отдаёт обрезанный
        # набор таблиц (только вкладка обжалования). new=0 возвращает
        # основную вкладку «Дело» с полным движением.
        return (
            f"{self.base_url}/modules.php?name=sud_delo&srv_num={self.srv_num}&name_op=case"
            f"&case_id={case_id}&case_uid={case_uid}"
            f"&delo_id={self.delo_id}&new=0"
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
# Снимок контекста последнего дайджеста — сохраняется перед отправкой
# в Telegram и используется режимом --replay-last для повторной генерации
# (например, чтобы переиграть с другой версией промпта).
LAST_DIGEST_CONTEXT_PATH = os.environ.get(
    "LAST_DIGEST_CONTEXT_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "last_digest_context.json")
)
# Окна жизненного цикла дела (state machine — см. advance_case_stage /
# is_case_archived). Старая модель ARCHIVE_DAYS/ARCHIVE_DAYS_FI отсчитывала
# архивацию от даты последнего события — ненадёжный якорь, не учитывал ни
# кассационный срок (3 мес), ни задержку мотивировки. Новые окна привязаны
# к стадиям процесса и датам заседаний.
FI_ARCHIVE_DAYS = 45            # 1-я инстанция: 45 дней от даты резолютивки
                                # без подачи апел. жалобы → архив.
APPEAL_NO_ACT_GRACE_DAYS = 30   # Апелляция: если акт не опубликован через
                                # 30 дней от апел. заседания — всё равно
                                # переходим в cassation_watch.
CASSATION_WATCH_DAYS = 120      # cassation_watch: 4 мес (≈3 мес срок + почта
                                # + регистрация) от апел. заседания. После —
                                # архив, если касс. жалоба так и не подана.
# Legacy: CSV-ветка архивации (apelljatsiя в CSV) ещё использует старое
# 30-дневное окно от «Даты события». Будет удалена вместе с CSV-веткой.
LEGACY_CSV_ARCHIVE_DAYS = 30
REQUEST_DELAY = (2, 3)  # Задержка между запросами к суду (сек)
FETCH_MAX_RETRIES = 3   # Кол-во попыток загрузки страницы
DASHBOARD_URL = "https://selivanovas.github.io/dashboard/sberbank_dashboard.html"

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Переключатель провайдера LLM: "claude" (по умолчанию) или "gigachat".
# Задаётся в workflow digest_only_gigachat.yml для отдельного прогона
# дайджеста через GigaChat. Основной мониторинг (update_cases.yml) остаётся
# на Claude и ничего не знает про этот флаг.
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "claude").strip().lower()
GIGACHAT_AUTH_KEY = os.environ.get("GIGACHAT_AUTH_KEY", "")
GIGACHAT_SCOPE = os.environ.get("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")
GIGACHAT_MODEL = os.environ.get("GIGACHAT_MODEL", "GigaChat")
GIGACHAT_OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
GIGACHAT_API_URL = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"

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


def _has_held_prior_hearing(events: list, new_hearing_dt: datetime | None) -> bool:
    """Есть ли в истории движения дела реально прошедшее заседание,
    отличное от нового назначения. Нужен чтобы отличить первое заседание
    (после передачи дела судье) от настоящего переноса.

    Если в истории был маркер «рассмотрение с начала», цикл считается
    сброшенным — заседания до последнего такого события не учитываем."""
    if not events or not new_hearing_dt:
        return False
    today = datetime.now().date()
    new_d = new_hearing_dt.date()
    reset_d = None
    for e in events:
        if not _RESTART_RE.search(e.get("text") or ""):
            continue
        ed = parse_date(e.get("date") or "")
        if ed and (reset_d is None or ed.date() > reset_d):
            reset_d = ed.date()
    for e in events:
        txt = (e.get("text") or "").lower()
        if "судебное заседани" not in txt:
            continue
        ed = parse_date(e.get("date") or "")
        if not ed:
            continue
        ed_d = ed.date()
        if reset_d and ed_d <= reset_d:
            continue
        if ed_d < today and ed_d != new_d:
            return True
    return False


_RESTART_RE = re.compile(r"рассмотрени\S*\s+дела\s+начато\s+с\s+начала", re.I)
_TO_FI_RULES_RE = re.compile(
    r"по\s+правилам\s+производства\s+в\s+суде\s+первой\s+инстанции"
    r"|перейти\s+к\s+рассмотрени\S*\s+по\s+правилам",
    re.I,
)


def _events_newly_match(
    old_events: list, new_events: list, pattern: re.Pattern
) -> dict | None:
    """Появилось ли в новом списке событий совпадение с паттерном, которого
    не было в старом. Возвращает dict события-триггера (date/text) или None.
    Сравнение — по (date, text), так как порядок не гарантирован."""
    if not new_events:
        return None
    old_keys = {
        ((e.get("date") or ""), (e.get("text") or ""))
        for e in (old_events or [])
    }
    for e in new_events:
        key = ((e.get("date") or ""), (e.get("text") or ""))
        if key in old_keys:
            continue
        if pattern.search(e.get("text") or ""):
            return {"date": e.get("date") or "", "text": e.get("text") or ""}
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
    """Legacy CSV-ветка: дело архивное = решено более LEGACY_CSV_ARCHIVE_DAYS
    дней назад. Используется для CSV-архива апелляции до его удаления."""
    if case.get("Статус", "").strip() != "Решено":
        return False
    date_str = case.get("Дата события", "").strip()
    if not date_str:
        return False
    d = parse_date(date_str)
    if not d:
        return False
    return (datetime.now() - d).days > LEGACY_CSV_ARCHIVE_DAYS


# ── State machine жизненного цикла дела ──────────────────────────────────────
# Стадии в поле current_stage:
#   first_instance    — парсим карточку 1-й инст., ждём апел. жалобу или 45 дней.
#   awaiting_appeal   — жалоба подана, перестали парсить 1-ю, ждём карточку
#                       в апел. суде (бессрочно).
#   appeal            — парсим карточку апел. суда.
#   cassation_watch   — апел. рассмотрел, вернулись к парсингу 1-й для поиска
#                       касс. жалобы (окно 4 мес от апел. заседания).
#   cassation_pending — касс. жалоба зарегистрирована, ждём парсер кассации.
# Архив — через is_case_archived.

def advance_case_stage(case: dict) -> str | None:
    """Выполнить возможный переход стадии для дела. Возвращает имя предыдущей
    стадии, если переход произошёл, иначе None.

    Переход first_instance → awaiting_appeal срабатывает, когда парсер 1-й
    инстанции записал appeal_filed_date. Переход awaiting_appeal → appeal
    делает link_cases при обнаружении апел. карточки — здесь не трогаем.
    Переход appeal → cassation_watch по факту публикации апел. акта или
    по истечении APPEAL_NO_ACT_GRACE_DAYS дней от апел. заседания.
    Переход cassation_watch → cassation_pending по касс. жалобе или
    направлению в кассационный суд."""
    stage = case.get("current_stage")
    fi = case.get("first_instance") or {}
    ap = case.get("appeal") or {}
    now = datetime.now()

    if stage == "first_instance":
        if fi.get("appeal_filed_date"):
            case["current_stage"] = "awaiting_appeal"
            return "first_instance"
        return None

    if stage == "awaiting_appeal":
        return None  # переход в appeal — задача link_cases

    if stage == "appeal":
        if ap.get("act_date"):
            case["current_stage"] = "cassation_watch"
            return "appeal"
        ap_hearing = parse_date(ap.get("hearing_date") or "")
        if ap_hearing and (now - ap_hearing).days >= APPEAL_NO_ACT_GRACE_DAYS:
            case["current_stage"] = "cassation_watch"
            return "appeal"
        return None

    if stage == "cassation_watch":
        if fi.get("cassation_filed_date") or fi.get("sent_to_cassation_date"):
            case["current_stage"] = "cassation_pending"
            case["cassation_pending_since"] = now.date().isoformat()
            return "cassation_watch"
        return None

    return None


def is_case_archived(case: dict) -> bool:
    """Унифицированная архивная проверка по стадии:
    - first_instance: «Решено» + 45 дней от hearing_date без апел. жалобы.
    - awaiting_appeal: никогда (ждём бессрочно, пока апел. карточка не найдётся).
    - appeal: никогда (переход в cassation_watch делает advance_case_stage).
    - cassation_watch: >120 дней от апел. hearing_date без касс. жалобы.
    - cassation_pending: никогда (ждём парсер кассации).
    Остальные (legacy «first_instance» без current_stage, «appeal» без JSON
    данных) — false, не трогаем."""
    stage = case.get("current_stage")
    now = datetime.now()
    fi = case.get("first_instance") or {}
    ap = case.get("appeal") or {}

    if stage == "first_instance":
        if fi.get("appeal_filed_date"):
            return False
        if fi.get("status", "").strip() != "Решено":
            return False
        hearing = parse_date(fi.get("hearing_date") or "")
        if hearing and (now - hearing).days > FI_ARCHIVE_DAYS:
            return True
        return False

    if stage in ("awaiting_appeal", "appeal", "cassation_pending"):
        return False

    if stage == "cassation_watch":
        ap_hearing = parse_date(ap.get("hearing_date") or "")
        if ap_hearing and (now - ap_hearing).days > CASSATION_WATCH_DAYS:
            return True
        return False

    return False


def migrate_stages(cases: list[dict]) -> int:
    """Идемпотентная миграция существующих дел под новую state-machine:
    - first_instance + appeal_filed_date → awaiting_appeal
    - appeal с опубликованным актом или заседанием старше 30 дней без акта
      → cassation_watch
    - cassation_watch с зарегистрированной касс. жалобой → cassation_pending
    Возвращает число мигрированных дел."""
    migrated = 0
    for case in cases:
        changed = True
        while changed:
            prev = advance_case_stage(case)
            changed = prev is not None
            if changed:
                migrated += 1
    return migrated


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


# Индекс судов первой инстанции по домену — для быстрого поиска CourtConfig.
# Несколько судов могут делить один домен (Нижневартовский районный + Покачи на
# vartovray--hmao.sudrf.ru, srv_num 1 и 2). По домену из карточки дела отличить
# их нельзя — выбираем первый (srv_num=1), это покрывает большинство дел.
_FI_COURTS_BY_DOMAIN: dict[str, CourtConfig] = {}
for _c in FIRST_INSTANCE_COURTS:
    _FI_COURTS_BY_DOMAIN.setdefault(_c.domain, _c)


def fi_card_url(fi_or_details: dict) -> str:
    """Построить URL карточки дела первой инстанции.

    Принимает либо dict первой инстанции (`first_instance` из cases.json),
    либо `details` из fi_changes — оба должны содержать `link` ('cid|cuid')
    и `court_domain`. Использует CourtConfig для конкретного суда, чтобы
    правильно подставить delo_id и srv_num (важно для Покачи: srv_num=2).
    """
    if not fi_or_details:
        return ""
    cid, cuid = case_id_uid(fi_or_details.get("link", ""))
    if not (cid and cuid):
        return ""
    domain = (fi_or_details.get("court_domain") or "").strip()
    court = _FI_COURTS_BY_DOMAIN.get(domain)
    if court:
        return court.card_url(cid, cuid)
    if not domain:
        return ""
    # Fallback: домен есть, но в реестре не нашёлся — собираем по дефолтным параметрам.
    return (
        f"https://{domain}/modules.php?name=sud_delo&srv_num=1&name_op=case"
        f"&case_id={cid}&case_uid={cuid}&delo_id=1540005&new=0"
    )


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


def classify_verdict_fi(result: str) -> str:
    """Нормализованный ярлык итога по делу 1-й инстанции.

    Принимает СЫРОЕ поле «Результат» из карточки суда. В отличие от
    апелляции, здесь только исходы первой инстанции (без «отменено/изменено»):
    удовлетворено [частично], отказано, прекращено, оставлено без рассмотрения,
    возвращено.
    """
    r = (result or "").lower()
    # Частичное удовлетворение — до общего «удовлетворено», иначе затмится.
    if ("удовлетворено частично" in r
            or "удовлетворено в части" in r
            or ("частично" in r and "удовлетв" in r)):
        return "удовлетворено частично"
    # «ОТКАЗАНО в удовлетворении иска» — до «удовлетворен», т.к. содержит оба.
    if "отказано" in r:
        return "отказано"
    if "удовлетворен" in r:
        return "удовлетворено"
    if "прекращено" in r:
        return "прекращено"
    if "без рассмотрения" in r:
        return "оставлено без рассмотрения"
    if "возвращен" in r:
        return "возвращено"
    return (result or "").strip() or "итог не распознан"


# Вытаскивает ИТОГ из хвоста last_event, когда поле «Результат» карточки
# пустое или попало под фильтр мусора. Ленивый захват до ближайшей даты
# вида dd.mm.yyyy или конца строки.
_FI_RESULT_FROM_EVENT_RX = re.compile(
    r"Вынесено решение по делу\.\s*(.+?)(?=\s*\d{2}\.\d{2}\.\d{4}|\s*$)",
    re.IGNORECASE | re.DOTALL,
)


def extract_result_from_event(event_text: str) -> str:
    """Вытаскивает ИТОГ из строки last_event.

    Возвращает «ОТКАЗАНО в удовлетворении иска…» из
    «Судебное заседание. 11:00. 311. Вынесено решение по делу. ОТКАЗАНО… 20.04.2026».
    Пустая строка, если маркер «Вынесено решение по делу» отсутствует
    или захват получился аномально длинным (склейка нескольких событий).
    """
    if not event_text:
        return ""
    m = _FI_RESULT_FROM_EVENT_RX.search(event_text)
    if not m:
        return ""
    captured = m.group(1).strip().rstrip(".").strip()
    if len(captured) > 400:
        return ""
    return captured


def classify_hearing_type(event_text: str) -> str:
    """Нормализованный ярлык типа заседания из текста события движения дела.

    Ярлыки соответствуют перечислению в разделе 3.2 промпта дайджеста:
    «подготовка дела / беседа / предварительное заседание / заседание».
    Распознаёт типовые заголовки карточек ГАС «Правосудие» по первой
    фразе текста события (до точки):
      «Предварительное судебное заседание. …» → «предварительное заседание»
      «Подготовка дела (собеседование). …»    → «подготовка дела»
      «Беседа. …»                              → «беседа»
      «Судебное заседание. …»                  → «заседание»
    Неизвестный/пустой текст — «заседание» (нейтральный дефолт).
    """
    if not event_text:
        return "заседание"
    t = event_text.lower().lstrip()
    if t.startswith("предварительное"):
        return "предварительное заседание"
    if t.startswith("подготовка дела"):
        return "подготовка дела"
    if t.startswith("беседа"):
        return "беседа"
    return "заседание"


def bank_side_outcome_fi(role: str, verdict_label: str) -> str:
    """Знак исхода для банка в 1-й инстанции — по роли + нормализованному ярлыку.

    Возвращает одну из: «в пользу банка», «против банка», «частично в пользу
    банка», «частично против банка», «нейтрально (банк — третье лицо)»,
    или пустую строку, если данных недостаточно.

    Для процессуальных завершений без решения по существу (прекращено,
    без рассмотрения, возвращено) знак определяется по роли: истец теряет
    возможность добиться удовлетворения → «против банка», к ответчику
    требования не рассмотрены → «в пользу банка». Точная причина
    (мировое соглашение, отказ от иска и т.п.) остаётся в last_event —
    юрист увидит её в строке события.
    """
    role_l = (role or "").lower()
    if "третье" in role_l:
        return "нейтрально (банк — третье лицо)"
    bank_is_plaintiff = "истец" in role_l
    bank_is_defendant = "ответчик" in role_l
    if not (bank_is_plaintiff or bank_is_defendant):
        return ""
    v = (verdict_label or "").lower()
    # Процессуальные завершения — по роли.
    if ("прекращено" in v or "без рассмотрения" in v or "возвращено" in v):
        return "против банка" if bank_is_plaintiff else "в пользу банка"
    # Решения по существу (частично — до общего «удовлетворено»).
    if "удовлетворено частично" in v:
        return ("частично в пользу банка" if bank_is_plaintiff
                else "частично против банка")
    if "удовлетворено" in v:
        return "в пользу банка" if bank_is_plaintiff else "против банка"
    if "отказано" in v:
        return "против банка" if bank_is_plaintiff else "в пользу банка"
    return ""


def bank_side_outcome(role: str, appellant: str, verdict_label: str) -> str:
    """«в пользу банка» / «против банка» / «нейтрально (банк — третье лицо)» /
    «» (пустая строка при нехватке данных — чтобы downstream не писал
    «не определено»)."""
    role_l = (role or "").lower()
    if "третье" in role_l:
        return "нейтрально (банк — третье лицо)"
    app = (appellant or "").strip().lower()
    if app not in ("банк", "иное лицо"):
        # При пустом/неизвестном апеллянте НЕ угадываем.
        return ""
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
    return ""



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

        # Номер может быть «2-5628/2026 ~ М-3298/2026» — берём первый.
        # Материалы (М-XXXX, 9-XXXX) тоже отслеживаем — юристу нужна
        # видимость по всем поступлениям против Сбербанка, не только по
        # основным гражданским делам.
        case_number = case_number_raw.split("~")[0].strip()

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


def _warn_if_card_degraded(card_info: dict, case_number: str) -> None:
    """Логируем обрезанную карточку только если из неё не удалось
    выдернуть ни одного события (иначе компактный шаблон — это норма)."""
    if card_info.get("_table_count", 0) >= 6:
        return
    if card_info.get("_events"):
        return
    log.warning(
        f"  {case_number}: карточка обрезана "
        f"({card_info.get('_table_count', 0)} таблиц), "
        f"движение не распозналось"
    )


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
        "_table_count": 0,      # len(tables) — нужно вызывающему коду для фолбэка card_url_alt
        "_fi_appeal_filed": False,  # В карточке 1 инст. подана апелляц. жалоба
        "_fi_appeal_filed_date": "",
        # Кассационные события в карточке 1 инст. (кассация подаётся через
        # суд 1-й инстанции). Нужны для state-machine cassation_watch.
        "_fi_cassation_filed": False,
        "_fi_cassation_filed_date": "",
        "_fi_sent_to_cassation": False,
        "_fi_sent_to_cassation_date": "",
    }

    tables = extract_tables(html)
    info["_table_count"] = len(tables)
    # Маркер «обжалование решений» нужен вызывающему коду для решения о фолбэке
    # на card_url_alt(new=0) — некоторые суды открывают вкладку обжалования
    # поверх основной «Дело», и основную надо запросить отдельным URL.
    if re.search(r'обжалован\w*\s+решен\w*', html, re.IGNORECASE):
        info["_fi_appeal_filed"] = True
    # Раньше здесь был ранний return при <6 таблиц — он отбрасывал живые
    # карточки с укороченным шаблоном (напр. Сургутский районный суд
    # отдаёт 4 таблицы, но с полным «ДВИЖЕНИЕ ДЕЛА»). Циклы ниже защищены
    # от малого числа таблиц, поэтому безопасно парсить всё, что есть.

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

    # Pattern 3 (fuzzy-поиск «жалоба + ФИО» по всему HTML) раньше жил здесь —
    # удалён после кейса 33-1161/2026, где карточка прошла «по правилам 1-й
    # инстанции» без апеллянта, но регекс вытащил имя одного из ответчиков
    # из не связанного контекста карточки. Лучше «не указано» в дайджесте,
    # чем неверный апеллянт — полагаемся только на структурные источники
    # (поле «Заявитель жалобы» в таблицах + событие движения).

    info["_appellant_raw"] = appellant_raw

    # ── События подачи жалоб в карточке 1-й инстанции ──
    # Апелляционная и кассационная жалобы подаются через суд 1-й инстанции —
    # отсюда же видно и событие «направлено в кассационный суд».
    # Регексы специфичны по стеблю «апелляционн» / «кассационн», чтобы не
    # путать апелляцию с кассацией (раньше «поступ.+жалоб» цеплял кассацию
    # как апелляцию). Флаг HTML-уровня «обжалование решений…» оставлен
    # выше как сигнал наличия вкладки обжалования (нужен для card_url_alt).
    if movement_table and len(movement_table) > 1:
        for row in movement_table[1:]:
            ev_text = " ".join(cell_text(c) for c in row)
            row_date = ""
            for c in row:
                ct = cell_text(c)
                if parse_date(ct):
                    row_date = ct
                    break
            # Кассационная жалоба — проверяем раньше апелляционной, т.к.
            # слово «кассационн» специфичнее «жалоб» без уточнения.
            if not info["_fi_cassation_filed"] and re.search(
                r'поступ\w+.{0,40}кассационн\w+\s+жалоб\w+',
                ev_text, re.IGNORECASE,
            ):
                info["_fi_cassation_filed"] = True
                info["_fi_cassation_filed_date"] = row_date
                continue
            # Направление дела в кассационный суд — отдельный сигнал.
            if not info["_fi_sent_to_cassation"] and re.search(
                r'(?:направлен\w+|передан\w+).{0,30}'
                r'(?:в\s+)?(?:\S+\s+){0,3}кассационн\w+',
                ev_text, re.IGNORECASE,
            ):
                info["_fi_sent_to_cassation"] = True
                info["_fi_sent_to_cassation_date"] = row_date
                continue
            # Апелляционная жалоба — требуем стебель «апелляционн», чтобы
            # не пересекаться с кассацией.
            if not info["_fi_appeal_filed_date"] and re.search(
                r'поступ\w+.{0,40}апелляционн\w+\s+(?:жалоб|представлени)\w+',
                ev_text, re.IGNORECASE,
            ):
                info["_fi_appeal_filed"] = True
                info["_fi_appeal_filed_date"] = row_date
                continue


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
    events = sum(1 for ch in changes
                 if "new_event" in ch["type"] or "hearing_new" in ch["type"])
    results = sum(1 for ch in changes if "new_result" in ch["type"])
    acts = sum(1 for ch in changes if "new_act" in ch["type"])
    postponed = sum(1 for ch in changes if "hearing_postponed" in ch["type"])
    to_fi_rules = sum(1 for ch in changes if "appeal_to_fi_rules" in ch["type"])
    if events:
        parts.append(f"{events} событ.")
    if postponed:
        parts.append(f"{postponed} отлож.")
    if to_fi_rules:
        parts.append(f"{to_fi_rules} перех. к 1-й инст.")
    if results:
        parts.append(f"{results} суд. акт.")
    if acts:
        parts.append(f"{acts} акт.")
    if fi_changes:
        fi_hearings = sum(
            1 for ch in fi_changes
            if "fi_hearing_new" in ch["type"] or "fi_hearing_postponed" in ch["type"]
        )
        fi_status = sum(1 for ch in fi_changes if "fi_status_change" in ch["type"])
        fi_acts = sum(1 for ch in fi_changes if "fi_act_published" in ch["type"])
        fi_finals = sum(1 for ch in fi_changes if "fi_final_event" in ch["type"])
        fi_resolved_n = sum(
            1 for ch in fi_changes if "fi_resolved" in ch["type"]
        )
        fi_act_texts = sum(
            1 for ch in fi_changes if "fi_act_text_published" in ch["type"]
        )
        fi_appeals_filed = sum(
            1 for ch in fi_changes if "fi_appeal_filed" in ch["type"]
        )
        fi_restarts = sum(
            1 for ch in fi_changes if "fi_hearing_restart" in ch["type"]
        )
        if fi_hearings:
            parts.append(f"{fi_hearings} засед. 1 инст.")
        if fi_restarts:
            parts.append(f"{fi_restarts} с начала")
        if fi_resolved_n:
            parts.append(f"{fi_resolved_n} реш. 1 инст.")
        if fi_appeals_filed:
            parts.append(f"{fi_appeals_filed} подано жалоб")
        if fi_finals:
            parts.append(f"{fi_finals} финал 1 инст.")
        if fi_acts:
            parts.append(f"{fi_acts} акт 1 инст.")
        if fi_act_texts:
            parts.append(f"{fi_act_texts} мотивир. 1 инст.")
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
            # Обычно исходная стадия — awaiting_appeal (жалоба подана, ждём
            # карточку) или first_instance (карточка пришла раньше жалобы —
            # редко, но возможно). Из cassation_watch/cassation_pending
            # обратно в appeal не переводим: эти стадии уже прошли апелляцию.
            prev_stage = fi_case.get("current_stage")
            if prev_stage in ("first_instance", "awaiting_appeal", None, ""):
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
    """Legacy CSV-аналог: дела с «Статус=Решено» + стариной «Дата события» > 30
    дней. Остаётся до удаления CSV-ветки архивации апелляции."""
    active, archive = [], []
    for c in cases:
        if is_archived(c):
            archive.append(c)
        else:
            active.append(c)
    return active, archive


def split_archived_json(cases: list[dict]) -> tuple[list[dict], list[dict]]:
    """Разделить JSON-дела на активные и архивные по state-machine
    (is_case_archived). Возвращает (active, archive)."""
    active, archive = [], []
    for c in cases:
        if is_case_archived(c):
            archive.append(c)
        else:
            active.append(c)
    return active, archive


def update_active_cases(
    cases: list[dict],
    json_appeal_by_num: dict | None = None,
    skip_apel_nums: set[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Обновить карточки активных (не архивных) дел.

    json_appeal_by_num — опциональный словарь {номер_дела: appeal_dict} для
    параллельного обновления полей `events` / `last_event` / `event_date` в
    JSON-хранилище (иначе эти поля в `appeal` dict устаревают).

    skip_apel_nums — номера апел. дел, чей JSON-родитель уже не в стадии
    "appeal" (напр. cassation_watch). Такие карточки не парсим: апел. уже
    прошла, парсинг — это лишние запросы и ложные обновления event_date.

    Возвращает (обновлённые_дела, список_изменений).
    """
    _digested_acts = load_digested_acts()
    changes = []

    for case in cases:
        if is_archived(case):
            continue
        if skip_apel_nums and case.get("Номер дела", "").strip() in skip_apel_nums:
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
        _warn_if_card_degraded(card_info, case["Номер дела"])

        # Параллельно обновляем JSON-представление appeal-дела (если передано).
        # Старый список событий фиксируем для детектора «по правилам 1-й инст.».
        old_events_ap: list = []
        if json_appeal_by_num is not None:
            ap = json_appeal_by_num.get(case.get("Номер дела", "").strip())
            if ap is not None:
                old_events_ap = list(ap.get("events") or [])
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
        # Снимок итога на момент публикации акта: результат обычно уже давно
        # стоит в карточке (акт публикуется через 14+ дней после заседания).
        # verdict_label в JSON не сохраняется — переклассифицируем из сырого
        # поля «Результат» (new_result приоритетнее — это значение из карточки).
        act_verdict_raw = new_result or old_result
        act_verdict_label = (classify_verdict(act_verdict_raw, new_event)
                             if act_verdict_raw else "")
        if new_act == "Да" and old_act != "Да":
            change["type"].append("new_act")
            change["details"]["act_text"] = extract_motive_part(act_text, 1800)
            if act_verdict_label:
                change["details"]["act_verdict_label"] = act_verdict_label
                change["details"]["act_verdict_raw"] = act_verdict_raw
        elif (new_act == "Да" and old_act == "Да"
              and act_text
              and case["Номер дела"] not in _digested_acts):
            # Акт уже был помечен ранее, но текст не извлекался.
            # Добавляем в дайджест один раз.
            motive = extract_motive_part(act_text, 1800)
            if motive and len(motive) > 100:
                change["type"].append("new_act")
                change["details"]["act_text"] = motive
                if act_verdict_label:
                    change["details"]["act_verdict_label"] = act_verdict_label
                    change["details"]["act_verdict_raw"] = act_verdict_raw

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
            # Настоящий перенос — только если в истории есть реально прошедшее
            # заседание. Иначе это первое назначение после передачи дела судье
            # (старое значение «Даты заседания» могло остаться от парсинга
            # даты публикации уведомления, а не от проведённого слушания).
            if _has_held_prior_hearing(card_info.get("_events") or [], new_h_dt):
                change["type"].append("hearing_postponed")
                change["details"]["old_hearing_date"] = old_hearing
                change["details"]["old_hearing_time"] = old_hearing_time
                change["details"]["new_hearing_date"] = new_hearing
                change["details"]["new_hearing_time"] = new_hearing_time
            else:
                change["type"].append("hearing_new")
                change["details"]["new_hearing_date"] = new_hearing
                change["details"]["new_hearing_time"] = new_hearing_time

        # Переход апелляции к рассмотрению по правилам производства в суде
        # первой инстанции (ч.5 ст.330 ГПК). Событие редкое и критичное —
        # выводим отдельной секцией в дайджесте.
        to_fi_rules_ev = _events_newly_match(
            old_events_ap, card_info.get("_events") or [], _TO_FI_RULES_RE
        )
        if to_fi_rules_ev:
            change["type"].append("appeal_to_fi_rules")
            change["details"]["transition_event"] = to_fi_rules_ev.get("text", "")
            change["details"]["transition_date"] = to_fi_rules_ev.get("date", "")

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
        # Роль апеллянта (Истец/Ответчик/Иное лицо) + сокращённое имя —
        # параллельный канал только для промпта, бинарный ярлык
        # case["Апеллянт"] сохраняем ради bank_side_outcome и CSV-схемы.
        appellant_role, appellant_name = classify_appellant_role(
            appellant_raw, case.get("Истец", ""), case.get("Ответчик", ""),
        )

        if change["type"]:
            change["details"]["plaintiff"] = case.get("Истец", "")
            change["details"]["defendant"] = case.get("Ответчик", "")
            change["details"]["role"] = case.get("Роль банка", "")
            change["details"]["category"] = case.get("Категория", "")
            change["details"]["appellant"] = case.get("Апеллянт", "")
            change["details"]["appellant_name"] = appellant_name
            change["details"]["appellant_role"] = appellant_role
            change["details"]["_appellant_raw"] = appellant_raw
            change["details"]["case_url"] = case_card_url(case)
            # bank_outcome считаем, когда есть нормализованный verdict_label
            # (new_result) или act_verdict_label (new_act — мотивировка в 5.5).
            # Без этого в 5.5 LLM видел только «роль банка» в общем блоке и
            # подставлял её в поле «Для банка» (например, «Третье лицо»
            # вместо реального исхода). Зависит от роли + апеллянта.
            if "new_result" in change["type"]:
                change["details"]["bank_outcome"] = bank_side_outcome(
                    change["details"]["role"],
                    change["details"]["appellant"],
                    change["details"].get("verdict_label", ""),
                )
            elif ("new_act" in change["type"]
                    and change["details"].get("act_verdict_label")):
                change["details"]["bank_outcome"] = bank_side_outcome(
                    change["details"]["role"],
                    change["details"]["appellant"],
                    change["details"]["act_verdict_label"],
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
# «Сбербанк — Югорское отделение № 5940», «Сбербанк - отделение ...» — дефисный вариант филиала (без запятой, на уровне _shorten_single)
_BRANCH_DASH_RE = re.compile(
    r'\s*[-–—]\s*(?:[А-ЯЁ][а-яё]+\s+)?отделение\b.*',
    re.IGNORECASE,
)
# «Сбербанк, Югорское отделение № 5940» — вариант через запятую (на уровне всей строки, до split по запятым)
_BRANCH_COMMA_RE = re.compile(
    r'(Сбербанк)\s*,\s*(?:[А-ЯЁ][а-яё]+\s+)?отделение\b[^,]*',
    re.IGNORECASE,
)
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
    # Сбербанк — Югорское отделение № 5940 — дефисный вариант филиала
    name = _BRANCH_DASH_RE.sub('', name).strip()
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
    # Сначала склеиваем «Сбербанк, Югорское отделение № 5940» до split,
    # иначе отдельная часть «отделение № 5940» проскочит в результат.
    name = _BRANCH_COMMA_RE.sub(r'\1', name)
    parts = name.split(",")
    shortened = [_shorten_single(p, keep_fio_full=keep_fio_full) for p in parts]
    return ", ".join(s for s in shortened if s)


def shorten_court_name(name: str) -> str:
    """«Сургутский городской суд» → «Сургутский гор. суд».

    Компактная форма для дайджеста и шаблонного fallback. В cases.json
    и FIRST_INSTANCE_COURTS названия хранятся полными — сокращаем только
    на выводе.
    """
    if not name:
        return name
    return (
        name
        .replace(" городской ", " гор. ")
        .replace(" районный ", " рай. ")
    )


def _norm_party_tokens(name: str) -> list[str]:
    """Разбить строку стороны на нормализованные токены для матчинга.

    Склеиваем филиальный запятый-вариант Сбербанка, сплитим по запятым,
    каждый токен прогоняем через _shorten_single и приводим к нижнему
    регистру со схлопнутыми пробелами. Пустые отбрасываем.
    """
    if not name or not name.strip():
        return []
    collapsed = _BRANCH_COMMA_RE.sub(r'\1', name)
    out = []
    for part in collapsed.split(","):
        short = _shorten_single(part, keep_fio_full=False)
        norm = re.sub(r'\s+', ' ', short).strip().lower()
        if norm:
            out.append(norm)
    return out


def classify_appellant_role(
    appellant_raw: str,
    plaintiff: str,
    defendant: str,
) -> tuple[str, str]:
    """Определить роль апеллянта и его сокращённое имя.

    Возвращает (role, short_name):
      role ∈ {"Истец", "Ответчик", "Иное лицо", ""}
      short_name — shorten_party_name(appellant_raw) или "" если пусто.

    Логика: сравниваем нормализованные токены apellant_raw с токенами
    истца и ответчика. Матч — равенство токенов или подстрока (в любом
    направлении) при длине содержащего ≥ 4 символов. Если нет матча —
    возвращаем «Иное лицо» (но имя всё равно сохраняем).
    """
    if not appellant_raw or not appellant_raw.strip():
        return ("", "")
    short_name = shorten_party_name(appellant_raw)
    app_tokens = _norm_party_tokens(appellant_raw)
    if not app_tokens:
        return ("Иное лицо", short_name)
    for role, party in (("Истец", plaintiff), ("Ответчик", defendant)):
        party_tokens = _norm_party_tokens(party)
        if not party_tokens:
            continue
        for a in app_tokens:
            for p in party_tokens:
                if a == p:
                    return (role, short_name)
                if len(p) >= 4 and a in p:
                    return (role, short_name)
                if len(a) >= 4 and p in a:
                    return (role, short_name)
    return ("Иное лицо", short_name)


# ── GigaChat API — альтернативный провайдер для digest_only ───────────────────

def _gigachat_access_token() -> str | None:
    """Получить OAuth access token GigaChat. Живёт 30 минут.

    Токен не кешируем: дайджест-раны короткие и одноразовые, а держать
    кеш между запусками workflow негде. Verify=False — на ubuntu-latest нет
    корневого сертификата Минцифры РФ, которым подписан ngw.devices.sberbank.ru.
    """
    if not GIGACHAT_AUTH_KEY:
        log.warning("GIGACHAT_AUTH_KEY не задан")
        return None
    try:
        import uuid
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = requests.post(
            GIGACHAT_OAUTH_URL,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "RqUID": str(uuid.uuid4()),
                "Authorization": f"Basic {GIGACHAT_AUTH_KEY}",
            },
            data={"scope": GIGACHAT_SCOPE},
            timeout=30,
            verify=False,
        )
        r.raise_for_status()
        return r.json().get("access_token")
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        body = (e.response.text or "")[:500] if e.response is not None else ""
        log.error(f"GigaChat OAuth HTTP {status}: {body}")
        return None
    except (requests.RequestException, KeyError, ValueError,
            json.JSONDecodeError) as e:
        log.error(f"GigaChat OAuth ошибка: {e}")
        return None


# System-инструкция для GigaChat. Claude-промпт в generate_digest описывает
# HTML-формат, но GigaChat (в т.ч. Max) охотно скатывается в Markdown (##, **, - )
# даже при явном запрете. Выносим жёсткие требования в role=system + даём
# микро-пример: так модель держит формат заметно стабильнее.
GIGACHAT_SYSTEM_PROMPT = (
    "Ты пишешь дайджест для отправки в Telegram с parse_mode=HTML. "
    "СТРОГИЕ ПРАВИЛА ФОРМАТА — нарушение = сломанная вёрстка:\n"
    "1. Разрешены ТОЛЬКО HTML-теги Telegram: <b>, <i>, <a href=\"URL\">текст</a>. "
    "Никакие <h1>, <h2>, <p>, <ul>, <li> не поддерживаются — не используй их.\n"
    "2. ЗАПРЕЩЕНО использовать Markdown: никаких ##, ###, **, *, ---, ``` "
    "и маркеров списков «- », «* », «• » в начале строк. "
    "Заголовки секций выделяй <b>…</b>, не решётками.\n"
    "3. Номера дел оформляй как ссылку: "
    "<a href=\"URL_из_данных\"><b>A40-123/2025</b></a>. "
    "Если URL есть в данных — обязательно вставь; не выдумывай URL.\n"
    "4. Итоговую строку пиши ДОСЛОВНО в формате из инструкции пользователя "
    "(«1 инст.», не «1 инстанция»).\n"
    "5. В конце обязательно ссылка на дашборд "
    "<a href=\"URL\">📊 Дашборд</a> — одной строкой, без «###».\n"
    "6. ПУСТЫЕ СЕКЦИИ ПОЛНОСТЬЮ ВЫКИДЫВАЙ. Если по подсекции нет данных — "
    "НЕ ПИШИ заголовок подсекции вообще. Никаких «Нет данных», «Нет дел», "
    "«Нет новых дел», «Нет отложенных заседаний», «Нет поданных жалоб», "
    "«Нет переходов в апелляцию», «Нет опубликованных актов», «—», «0» "
    "и любых иных «плашек-заглушек». Заголовок подсекции появляется "
    "ТОЛЬКО если под ним есть реальные строки с делами. Большой блок "
    "«🏛 ПЕРВАЯ ИНСТАНЦИЯ» / «⚖️ АПЕЛЛЯЦИЯ» выводи только если хотя бы "
    "одна его подсекция непуста. Исключение: итоговая строка "
    "«В производстве» и ссылка на дашборд — всегда.\n"
    "7. ОДИН ДЕНЬ = ОДНА СТРОКА НА СОБЫТИЕ. Не разбивай одно событие "
    "на две строки («опубликован акт» + отдельная строка с итогом). "
    "Если акт опубликован и в данных есть ИТОГ — пиши это одной строкой: "
    "«номер — суд — опубликован акт: <итог>». Не повторяй одно дело "
    "несколько раз внутри одной подсекции.\n"
    "8. ДАТЫ бери ТОЛЬКО из явно помеченных полей входных данных "
    "(«Дата поступления», «Дата события», «Дата заседания», «Дата "
    "апелляционного определения», «event_date», «hearing_date», "
    "«act_date» и т.п.). НЕ переноси дату из одного события в другое "
    "(дата подачи иска ≠ дата апелляционного акта). Если поле даты "
    "в данных пустое — не выдумывай и не подставляй сегодня; либо "
    "пиши «дата не указана», либо вовсе не упоминай дату в строке.\n"
    "9. Если одного и того же дела нет в разных секциях входных данных — "
    "не дублируй его в нескольких секциях дайджеста. Дело появляется "
    "в нескольких секциях ТОЛЬКО если оно явно присутствует в каждой "
    "из них во входных данных.\n"
    "Пример корректной строки:\n"
    "<b>📅 Изменения:</b>\n"
    "<a href=\"https://example.ru/case\"><b>А40-123/2025</b></a> — "
    "Сбер vs Иванов. Новое событие: заседание назначено на 15.05.2026.\n"
    "Отвечай ТОЛЬКО готовым HTML-текстом, без пояснений «вот ваш дайджест»."
)


def _normalize_markdown_to_telegram_html(text: str) -> str:
    """Конвертировать Markdown-артефакты в Telegram-HTML.

    Страховка поверх system-промпта: даже с жёсткой инструкцией GigaChat
    регулярно возвращает Markdown. Чистим, чтобы Telegram не порвал
    parse_mode=HTML на знаках «*» и не показал читателю «##».
    """
    # Markdown code-fence вокруг всего ответа (```html … ```)
    if text.startswith("```"):
        first_nl = text.find("\n")
        if first_nl != -1:
            text = text[first_nl + 1:]
    if text.endswith("```"):
        text = text[:-3]

    lines = text.split("\n")
    out: list[str] = []
    for line in lines:
        stripped = line.strip()
        # Горизонтальные разделители Markdown: строка из --- / *** / ___
        if re.fullmatch(r"[-*_]{3,}", stripped):
            continue
        # Заголовки: «## Заголовок» → «<b>Заголовок</b>».
        # Внутри заголовка убираем **…** и одиночные «*», чтобы не получить
        # вложенные <b><b>…</b></b> на следующем проходе (Telegram их не любит).
        m = re.match(r"^\s*#{1,6}\s+(.+?)\s*$", line)
        if m:
            content = m.group(1)
            content = re.sub(r"\*\*([^*\n]+?)\*\*", r"\1", content)
            content = re.sub(r"(?<![*\w])\*([^*\n]+?)\*(?!\w)", r"\1", content)
            line = f"<b>{content}</b>"
        else:
            # Маркеры списка в начале строки: «- x», «* x», «• x» → снимаем маркер
            line = re.sub(r"^(\s*)[-*•]\s+", r"\1", line)
        out.append(line)
    text = "\n".join(out)

    # Markdown-ссылки [text](url) → <a href="url">text</a>.
    # Делаем ДО конвертации **…**, иначе «**» внутри скобок ссылки перепутаются.
    text = re.sub(
        r"\[([^\]]+)\]\((https?://[^\s)]+)\)",
        r'<a href="\2">\1</a>',
        text,
    )
    # Жирный Markdown **x** → <b>x</b> (non-greedy, без переносов строк).
    text = re.sub(r"\*\*([^*\n]+?)\*\*", r"<b>\1</b>", text)
    # Одиночный «*x*» курсив — у GigaChat встречается редко, но на всякий случай.
    # Только если вокруг «*» точно слова, иначе пробьём звёздочки внутри текста.
    text = re.sub(r"(?<![*\w])\*([^*\n]+?)\*(?!\w)", r"<i>\1</i>", text)

    # Удаляем пустые подсекции «… (0): Нет …». Промпт просит их
    # полностью выкидывать, но GigaChat всё равно их пишет — чистим руками.
    # Паттерн: строка, где есть «(0)» и двоеточие (с закрывающим </b> или без).
    text = _drop_empty_count_sections(text)

    # Сдвоенные пустые строки после чистки разделителей — к одной пустой.
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _drop_empty_count_sections(text: str) -> str:
    """Удалить пустые подсекции вида «Заголовок: Нет X» / «Заголовок (0): Нет X».

    GigaChat клепает подзаголовки-заглушки тремя разными способами:
    1) «📨 Заголовок (0): Нет поданных жалоб» — одной строкой;
    2) «📨 Заголовок (0):» + на следующей строке «Нет поданных жалоб»;
    3) «📨 Заголовок: Нет данных» — без счётчика (2-Max любит этот вариант);
    4) «📨 Заголовок:» + «Нет данных» на следующей строке.
    Фильтр ловит все четыре: считает пустой любую строку, которая
    заканчивается на «:» и либо содержит «(0)», либо прямо на этой же
    или следующей строке идёт «Нет …». «Нет …» после непустой секции
    (например, «Нет оснований для отмены» в мотивировке) не тронется —
    проверка требует, чтобы заголовок заканчивался на «:».
    """
    # Стоп-фразы — то, чем GigaChat декорирует пустоту. Захватываем с
    # сохранением символа-продолжения (конец строки / следующая запись),
    # чтобы случайно не удалить половину осмысленного предложения.
    empty_phrase = re.compile(
        r"^\s*(?:<[^>]+>\s*)?"
        r"(?:Нет\s+\S[^\n]*|—|-|–|0)\s*$",
        re.IGNORECASE,
    )
    header_line = re.compile(r":\s*$")
    count_zero = re.compile(r"\(\s*0\s*\)\s*:")
    header_with_inline = re.compile(
        r"^(.*:)\s*"
        r"(?:Нет\s+\S[^\n]*|—|-|–|0)\s*$",
        re.IGNORECASE,
    )

    lines = text.split("\n")
    out: list[str] = []
    drop_next_if_nothing = False
    for line in lines:
        if drop_next_if_nothing:
            drop_next_if_nothing = False
            if empty_phrase.match(line):
                continue  # плашка «Нет X» после пустого заголовка — удаляем
            if not line.strip():
                continue  # и пустую строку-разделитель тоже
        # Однострочник «Заголовок: Нет X» или «Заголовок (0): Нет X»
        if header_with_inline.match(line) or count_zero.search(line):
            drop_next_if_nothing = True
            continue
        # Заголовок на отдельной строке, на следующей ожидается «Нет X».
        # Чтобы не срезать лишнего, срабатываем только если заголовок
        # короткий (≤80 символов) — не тянет на осмысленный предложение.
        stripped = line.strip()
        if header_line.search(stripped) and len(stripped) <= 80:
            drop_next_if_nothing = True
            # Заголовок пока оставим в out и удалим ретроактивно,
            # если подтвердится пустая фраза на следующей строке.
            out.append(line)
            continue
        out.append(line)

    # Второй проход: если после «drop_next_if_nothing» мы оставили заголовок,
    # но следующая строка была пустой фразой (и мы её скипнули) — надо
    # вернуться и снять этот заголовок тоже. Проще — найти «висячие»
    # заголовки (строка заканчивается на «:», а следующая непустая
    # строка — новый заголовок или конец текста) и удалить.
    cleaned: list[str] = []
    for i, line in enumerate(out):
        stripped = line.strip()
        if header_line.search(stripped) and len(stripped) <= 80:
            # Ищем следующую непустую строку
            j = i + 1
            while j < len(out) and not out[j].strip():
                j += 1
            if j >= len(out):
                continue  # висячий заголовок в самом конце — выкидываем
            nxt = out[j].strip()
            # Если следующая непустая строка — тоже заголовок (кончается «:»),
            # значит под нашим заголовком реально ничего не было → выкидываем.
            if header_line.search(nxt) and len(nxt) <= 80:
                continue
        cleaned.append(line)
    return "\n".join(cleaned)


def _call_gigachat(prompt: str) -> str | None:
    """Отправить prompt в GigaChat, вернуть HTML-текст дайджеста.

    Возвращает None при любой ошибке — вызывающая сторона откатится
    на generate_template_digest (как и для Claude).
    """
    token = _gigachat_access_token()
    if not token:
        return None
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = requests.post(
            GIGACHAT_API_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json={
                "model": GIGACHAT_MODEL,
                "temperature": 0.2,
                "max_tokens": 4096,
                "messages": [
                    {"role": "system", "content": GIGACHAT_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
            },
            timeout=60,
            verify=False,
        )
        r.raise_for_status()
        data = r.json()
        choices = data.get("choices", [])
        if not choices:
            return None
        text = (choices[0].get("message", {}) or {}).get("content", "").strip()
        if not text:
            return None
        text = _normalize_markdown_to_telegram_html(text)
        return text or None
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        body = (e.response.text or "")[:500] if e.response is not None else ""
        log.error(f"GigaChat API HTTP {status}: {body}")
        return None
    except requests.RequestException as e:
        log.error(f"GigaChat API сетевая ошибка: {e}")
        return None
    except (KeyError, ValueError, json.JSONDecodeError) as e:
        log.error(f"GigaChat API неожиданный ответ: {e}")
        return None


# ── Claude API — генерация дайджеста ─────────────────────────────────────────

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
) -> None:
    """Сохранить входные данные дайджеста в LAST_DIGEST_CONTEXT_PATH.

    Файл перезаписывается на каждом прогоне и нужен для режима --replay-last,
    чтобы прогнать дайджест заново на тех же данных (например, после правки
    промпта) без повторного парсинга сайтов суда.
    """
    payload = {
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "new_cases": new_cases or [],
        "changes": changes or [],
        "cases": cases or [],
        "fi_new_cases": fi_new_cases or [],
        "stage_transitions": stage_transitions or [],
        "fi_changes": fi_changes or [],
        "total_active_appeal": total_active_appeal,
        "total_active_fi": total_active_fi,
    }
    try:
        save_json(payload, LAST_DIGEST_CONTEXT_PATH)
        log.info(f"Контекст дайджеста сохранён: {LAST_DIGEST_CONTEXT_PATH}")
    except Exception as exc:
        # Сохранение контекста — вспомогательная операция, не должна ронять
        # основной прогон. Ошибку залогируем и поедем дальше.
        log.warning(f"Не удалось сохранить контекст дайджеста: {exc}")


def generate_digest(new_cases: list[dict], changes: list[dict], *,
                    cases: list[dict] | None = None,
                    fi_new_cases: list[dict] | None = None,
                    stage_transitions: list[dict] | None = None,
                    fi_changes: list[dict] | None = None,
                    total_active_appeal: int = 0,
                    total_active_fi: int = 0) -> str:
    """Сгенерировать дайджест через Claude API.

    total_active_appeal/total_active_fi передаются раздельно — раньше передавалась
    только сумма, и Claude выдумывал разбивку (типа «1 инст.: 2» при реальных 9).
    """

    if cases is None:
        cases = []
    if fi_new_cases is None:
        fi_new_cases = []
    if stage_transitions is None:
        stage_transitions = []
    if fi_changes is None:
        fi_changes = []

    total_active = total_active_appeal + total_active_fi

    if LLM_PROVIDER == "gigachat":
        if not GIGACHAT_AUTH_KEY:
            log.warning("GIGACHAT_AUTH_KEY не задан, дайджест будет шаблонным")
            return generate_template_digest(
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
            )
    elif not ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY не задан, дайджест будет шаблонным")
        return generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
        )

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
                f"суд 1 инст.: {shorten_court_name(c['Суд 1 инстанции'])}, "
                f"поступило: {c['Дата поступления']}"
            )

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
            app_str = _appellant_fmt(d)
            if app_str:
                line += f", апеллянт: {app_str}"

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
                    if d.get("bank_outcome"):
                        line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
                    line += f"\n  Категория спора: {d.get('category', '')}"
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
                    old_dt = d.get("old_hearing_date", "")
                    old_tm = d.get("old_hearing_time", "")
                    new_dt = d.get("new_hearing_date", "")
                    new_tm = d.get("new_hearing_time", "")
                    old_part = f"{old_dt}" + (f" {old_tm}" if old_tm else "")
                    new_part = f"{new_dt}" + (f" {new_tm}" if new_tm else "")
                    line += (f"\n  ОТЛОЖЕНО: заседание перенесено "
                             f"с {old_part} на {new_part}")
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

            context_parts.append(line)

    if fi_new_cases:
        context_parts.append("\nНОВЫЕ ДЕЛА ПЕРВОЙ ИНСТАНЦИИ:")
        for c in fi_new_cases:
            fi = c.get("first_instance", {})
            court = shorten_court_name(fi.get("court", ""))
            url = fi_card_url(fi)
            pl = shorten_party_name(c.get("plaintiff", ""), keep_fio_full=True)
            df = shorten_party_name(c.get("defendant", ""), keep_fio_full=True)
            context_parts.append(
                f"- {c['id']} (URL: {url}) (суд: {court}): "
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
                    hd = d.get("hearing_date", "")
                    ht = d.get("hearing_time", "")
                    htype = d.get("hearing_type", "заседание")
                    # «Первое» — потому что fi_hearing_new срабатывает только
                    # если раньше заседаний не было (см. место создания события).
                    # Без уточнения LLM принимает такое дело за новое исковое.
                    # Тип (беседа / предварительное / подготовка / заседание) —
                    # из того же события в движении дела.
                    line += (f"\n  Назначено первое {htype}: {hd}"
                             + (f" {ht}" if ht else ""))
                elif t == "fi_hearing_postponed":
                    old_d = d.get("old_hearing_date", "")
                    old_t = d.get("old_hearing_time", "")
                    new_d = d.get("hearing_date", "")
                    new_t = d.get("hearing_time", "")
                    htype = d.get("hearing_type", "заседание")
                    old_p = f"{old_d}" + (f" {old_t}" if old_t else "")
                    new_p = f"{new_d}" + (f" {new_t}" if new_t else "")
                    line += f"\n  Заседание перенесено ({htype}): {old_p} → {new_p}"
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
            fi_changes_buf.append(line)
        if fi_changes_buf:
            context_parts.append("\nИЗМЕНЕНИЯ ПО ДЕЛАМ ПЕРВОЙ ИНСТАНЦИИ:")
            context_parts.extend(fi_changes_buf)

    # Отдельный блок «Вынесены решения 1 инст.» — источник для раздела 3.5
    # промпта. Дела с fi_resolved приходят из fi_changes и физически
    # остаются в нём, но их статус+итог рендерятся именно здесь.
    fi_resolved_changes = [
        ch for ch in fi_changes if "fi_resolved" in ch["type"]
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
                line += f"\n  Категория спора: {d['category']}"
            if d.get("bank_outcome"):
                line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
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
            if d.get("act_date"):
                line += f"\n  Дата публикации акта: {d['act_date']}"
            if d.get("verdict_label"):
                line += f"\n  ИТОГ (из карточки): {d['verdict_label']}"
            if d.get("raw_result"):
                line += f"\n  Сырое поле «Результат»: {d['raw_result']}"
            if d.get("bank_outcome"):
                line += f"\n  В чью пользу для банка: {d['bank_outcome']}"
            if d.get("category"):
                line += f"\n  Категория спора: {d['category']}"
            if d.get("last_event"):
                line += f"\n  Последнее событие: {d['last_event']}"
            if d.get("act_text"):
                line += f"\n  МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ: {d['act_text']}"
            context_parts.append(line)

    prompt = f"""Ты — помощник юриста ПАО Сбербанк. Сформируй дайджест изменений по судебным делам судов ХМАО-Югры за {today}.

ИМЕНА: все наименования сторон в данных уже сокращены по правилам (ОПФ убрана, ФИО → инициалы, «в лице филиала…» удалено и т.п.). НЕ переписывай их и НЕ возвращай ОПФ обратно. В секциях «Новые дела» имена физлиц приходят полными — там оставляй как есть.

ДАТЫ: бери ровно из переданных полей данных. Не используй today() и не угадывай. Если у дела есть пометка «Заседание состоялось давно» — реальная дата уже в поле «Дата апелляционного определения», не пиши «сегодня».

ФОРМАТ: HTML для Telegram. Разрешены только теги <b>, <i>, <a href="URL">. Никакого Markdown (* _ ` [ ]). Спецсимволы &lt; &gt; &amp; экранируй.

СТРУКТУРА — два больших блока по инстанциям. Заголовок подсекции выводи только если есть данные. Большой блок (🏛 ПЕРВАЯ ИНСТАНЦИЯ / ⚖️ АПЕЛЛЯЦИЯ) выводи только если хотя бы одна его подсекция непуста.

СУД в скобках: поле {{суд}} в любой строке бери ДОСЛОВНО из записи того же дела в данных (поля «суд», «Суд 1 инстанции», «court»). Названия судов уже приходят сокращённо — например, «Сургутский гор. суд», «Нефтеюганский рай. суд». Выводи их как есть, НЕ расшифровывай «гор.» → «городской» и «рай.» → «районный». Если у дела поля с судом нет — не пиши суд в скобках вообще. ЗАПРЕЩЕНО переносить название суда из соседней записи. Для апелляционных дел (номер на `33-`) суд в скобках не пиши — все апелляции рассматриваются в Суде ХМАО-Югры, подсвечивать это не нужно. Значение «Суд 1 инстанции» уместно только в секциях про апелляционные дела, где прямо просят показать суд 1 инстанции (5.1).

БАНК В ХВОСТЕ СТРОКИ: во всех строках, где есть фраза «банк — {{роль}}» (3.2, 3.5, 5.1, 5.4 и т.п.): если «Сбербанк» / «ПАО Сбербанк» / «Сбербанк России» явно упомянут в сторонах (истец или ответчик) — блок «банк — {{роль}}» и «<b>, банк — {{роль}}</b>» НЕ пиши. Хвост нужен ТОЛЬКО когда банк = Третье лицо и в сторонах не фигурирует. Правило действует на все секции промпта без исключения.

ПРАВИЛА РЕЗОЛЮТИВНЫХ СЕКЦИЙ (применяются к 3.5 и 5.4):
• ИТОГ цитируй ДОСЛОВНО из поля «ИТОГ»; не переформулируй и не подменяй шаблоном.
• Если блока «ИТОГ» в данных нет — дело в секцию НЕ включай.
• Имя судьи НЕ указывай.
• Поле «В чью пользу для банка» пустое/отсутствует → блок «<b>Для банка:</b> …» НЕ пиши вообще; не подставляй «—», «0», «не определено». Строка тогда заканчивается на «банк — {{роль}}» без хвоста.
• Если ИТОГ = «прекращено / оставлено без рассмотрения / возвращено / снято» — добавь в конце строки короткую причину из «Последнее событие» (мировое соглашение, отказ от иска, неявка и т.п.), если она есть.
• «Составлено мотивированное определение» не упоминай — это служебный шаг.

ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (применяются к 3.6 и 5.5):
Формат — ТРИ строки на дело, между делами пустая строка.
Строка «<b>Почему:</b>» — 3-4 коротких предложения с КОНКРЕТНЫМ обоснованием из мотивировки: какую норму применил суд, что не доказала сторона, какие факты учёл. Пример: «Суд сослался на ст. 16 ЗоЗПП — услуга навязана при выдаче ипотеки. Банк не доказал возможность отказа потребителя. Довод об отсутствии нарушения прав потребителя отклонён.»
Имя судьи НЕ указывай.
ЗАПРЕЩЕНО:
- писать общие заглушки («суд рассмотрел доводы», «суд проверил законность», «суд исследовал материалы дела», «суд согласился с выводами») без конкретики;
- пересказывать ФАКТУРУ спора вместо МОТИВИРОВКИ итога (фактура — это строка 1, а не строка «Почему»);
- выдумывать ИТОГ или апеллянта — если поля нет в данных, соответствующую строку («<b>Итог:</b>» / «<b>Апеллянт:</b>») НЕ пиши, не подставляй «—», «0», «не указано», «не определено»;
- упоминать процедуру заседания: явку/неявку сторон и представителей, ходатайства о рассмотрении в отсутствие стороны, отложения, извещения, вручение корреспонденции, полномочия представителей, аудиопротоколирование;
- писать штампы «замечаний на протокол не поступало», «судебные извещения вручены», «извещены надлежащим образом», «дело рассмотрено в отсутствие надлежаще извещённого»;
- копировать «в удовлетворении требований отказать» / «требования подлежат удовлетворению» / «доводы апелляционной жалобы не влекут отмены решения» без указания, КАКУЮ норму суд применил и КАКОЙ довод принял/отклонил.

1. Заголовок: 📊 Дайджест судебных дел | Суды ХМАО-Югры | {today}
2. 📋 Сводка одной строкой. Группируй по инстанциям: <b>1 инст.:</b> X заседаний, Y решений, Z статусов | <b>Апелл.:</b> +N дел, M актов, K отложений. УПОМИНАЙ ТОЛЬКО те события, которые реально будут выведены в блоках 3/4/5 ниже. Если событие дедуплицировано правилами (смена статуса свёрнута в 3.5, подача жалобы в 3.3 поглощает 3.2 и т.п.) — в сводке его НЕ считай.

3. 🏛 <b>ПЕРВАЯ ИНСТАНЦИЯ</b>
   3.1. 📥 <b>Новые иски (N):</b> — одна строка на дело: <a href="URL"><b>номер</b></a> (URL ТОЛЬКО из поля URL этого дела в данных, ничего не выдумывай), стороны (имена физлиц полными), категория, суд, дата подачи, роль банка.
   3.2. 📅 <b>Изменения (N):</b> — ДВЕ строки на дело, между делами пустая строка. `N` в заголовке = количество дел, ФАКТИЧЕСКИ выведенных ниже в этой подсекции (не общее число изменений в данных). Пример: у одного дела в данных И перенос заседания, И рассмотрение с начала → это ОДНО дело, одна двухстрочная запись, N=1. Не плюсуй события как отдельные единицы. Если дело вынесено в 3.3 или 3.5 — в 3.2 его НЕ повторяй, кроме случая, когда у него в этом же дайджесте есть отдельное побочное событие типа заседание/отложение. Смена статуса «В производстве → Решено» в 3.2 допустима ТОЛЬКО если этого дела нет в 3.5 (например, карточка суда ещё не опубликовала «Результат»). Если дело есть в 3.5 — в 3.2 статус не повторяй.
        • строка 1: 📅 <b>ДД.ММ.ГГГГ ЧЧ:ММ</b> — <a href="URL"><b>номер</b></a> ({{суд}})
          — если это назначенное/перенесённое заседание, дата жирным СПЕРЕДИ.
          Для переноса: <b>⏪ ДД.ММ.ГГГГ ЧЧ:ММ → ⏩ ДД.ММ.ГГГГ ЧЧ:ММ</b> — <a href="URL"><b>номер</b></a> ({{суд}}).
          Для событий без даты (смена статуса, публикация акта, «рассмотрение начато с начала» и т.п.) — строка 1 без даты впереди: <a href="URL"><b>номер</b></a> ({{суд}}).
        • строка 2: {{стороны кратко}} | событие (подготовка дела / беседа / предварительное заседание / заседание / отложение / статус X→Y / опубликован акт / мотивированное решение / возвращение иска / в архив / рассмотрение с начала).
        • Для «рассмотрение с начала» (событие «fi_hearing_restart» в данных) строка 2 ДОЛЖНА КОПИРОВАТЬ ДОСЛОВНО (байт-в-байт, включая теги <b>, эмодзи 🔄 и пробелы) фразу: «<b>🔄 рассмотрение начато с начала</b>», далее в скобках ({{дата события}}); следующее заседание {{ДД.ММ.ГГГГ ЧЧ:ММ}} — дату следующего заседания берёшь ДОСЛОВНО из поля «Следующее заседание» того же дела в данных, не из соседней записи. Если поля «Следующее заседание» нет — дату не подставляй. ЗАПРЕЩЕНО: писать «начано» вместо «начато», пропускать теги <b>/</b>, менять эмодзи. НИКОГДА не выделяй «рассмотрение с начала» в отдельную строку/подсекцию — оно идёт в 3.2 как обычное событие.
   3.3. 📨 <b>Поданы апелляционные жалобы (N):</b> — ОДНА строка на дело (подсекция показывается только если N&gt;0). `N` = число строк ниже.
        <a href="URL"><b>номер</b></a> ({{суд}}) — {{стороны кратко}} | <b>апеллянт:</b> {{Роль Имя}} (дата подачи в скобках, если есть).
        Берётся из событий «fi_appeal_filed» в данных. НЕ дублируй это дело в 3.2 даже если у него есть ещё и смена статуса — событие подачи жалобы приоритетнее и идёт в свою подсекцию.
   3.4. 📨 <b>Кассационные события (N):</b> — ОДНА строка на дело (подсекция показывается только если N&gt;0). Касс. жалоба подаётся через суд 1-й инстанции, поэтому событие видно в карточке 1-й инст. даже если само дело уже прошло апелляцию. `N` = число строк ниже.
        <a href="URL"><b>номер</b></a> ({{суд}}) — {{стороны кратко}} | 📨 подана касс. жалоба ({{дата}}) ИЛИ 📤 направлено в касс. суд ({{дата}}).
        Берётся из событий «fi_cassation_filed» и «fi_sent_to_cassation» в данных. Оба типа мержим в одну строку если присутствуют у одного дела. НЕ дублируй это дело в 3.2.
   3.5. ⚖️ <b>Вынесенные решения (N):</b> — решение суда первой инстанции по существу дела (или процессуальное завершение: прекращение, без рассмотрения, возвращение). ДВЕ строки на дело, между делами пустая строка (подсекция показывается только если N&gt;0). `N` = число дел ниже.
        • строка 1: <a href="URL"><b>номер</b></a> ({{суд}}) — Решение от {{дата решения}}. <b>ИТОГ:</b> {{дословно поле ИТОГ}}. Категория: {{дословно}}.
        • строка 2: Стороны: {{истец}} vs {{ответчик}}, банк — {{роль}}. <b>Для банка:</b> {{дословно «В чью пользу для банка»}}.
        Применяются ПРАВИЛА РЕЗОЛЮТИВНЫХ СЕКЦИЙ (см. выше).
        Берётся из событий «fi_resolved» в данных (секция «ВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.»). Дело, попавшее в 3.5, в 3.2 НЕ дублируется — кроме случая, когда у того же дела есть ещё отдельное побочное событие (заседание/отложение).
   3.6. 📄 <b>Опубликованные тексты решений (N):</b> — полный текст решения 1-й инст. (выходит через 14+ дней после заседания, иногда не публикуется вовсе). Только дела с полем «МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ» в данных:
        • строка 1: <a href="URL"><b>номер</b></a>: {{стороны кратко}}
        • строка 2: <b>Итог:</b> {{удовлетворено / удовлетворено частично / отказано / прекращено / оставлено без рассмотрения / возвращено — дословно из «ИТОГ (из карточки)»}}. <b>Для банка:</b> {{дословно из поля «В чью пользу для банка»}}.
        • строка 3: <b>Почему:</b> см. ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (выше).
        Применяются ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (формат трёх строк, блок ЗАПРЕЩЕНО, правило про пустое «Для банка» и отсутствующий ИТОГ — см. выше).
        Берётся из событий «fi_act_text_published» в данных (секция «ОПУБЛИКОВАНЫ ТЕКСТЫ РЕШЕНИЙ 1 ИНСТ.»).

4. 🔀 <b>Перешли в апелляцию (N):</b> — самостоятельный блок-мостик. Показывай только если есть данные в секции «ПЕРЕШЛИ В АПЕЛЛЯЦИЮ». Формат: <b>fi_номер</b> → <b>ap_номер</b>: стороны.

5. ⚖️ <b>АПЕЛЛЯЦИЯ</b>
   5.1. 📥 <b>Новые дела (N):</b> — ДВЕ строки на дело, между делами пустая строка.
        • строка 1: <a href="URL"><b>номер</b></a> — {{истец}} vs {{ответчик}} (имена физлиц полностью — см. правило ИМЕНА в шапке)
        • строка 2: Суд 1 инст.: {{суд 1 инстанции}} | банк — {{роль}} (хвост «банк — …» — по правилу БАНК В ХВОСТЕ)
   5.1a. ⚠ <b>Переход к правилам 1-й инстанции (N):</b> — РЕДКОЕ и КРИТИЧНОЕ событие (ч.5 ст.330 ГПК). ОДНА строка на дело (подсекция показывается только если N&gt;0):
        ⚠ <a href="URL"><b>номер</b></a> — апелляция перешла к рассмотрению дела по правилам производства в суде первой инстанции ({{дата, если есть}}). {{стороны кратко}} | роль банка. НИКОГДА не выкидывать при нехватке места. Берётся из событий «appeal_to_fi_rules» в данных.
   5.2. 🔁 <b>Отложенные заседания (N):</b> — ДВЕ строки на дело, между делами пустая строка. Эта секция РЕДКАЯ и ВАЖНАЯ — никогда не выкидывай при нехватке места.
        • строка 1: 🔁 <a href="URL"><b>номер</b></a> — {{стороны кратко}} | категория: {{категория}}
        • строка 2: Перенесено: ДД.ММ.ГГГГ HH:MM → ДД.ММ.ГГГГ HH:MM (даты строго из строки «ОТЛОЖЕНО:» в данных)
   5.3. 📅 <b>Назначенные заседания (N):</b> — формат: строка 1 «<b>дата/время</b> + <a href="URL"><b>номер</b></a>», строка 2 «стороны | категория. Роль банка если известна», между делами пустая строка. НЕ помещай сюда дела с пометкой «ОТЛОЖЕНО».
   5.4. ⚖️ <b>Вынесенные акты (N):</b> — резолютивная часть (выходит через 1-3 дня после заседания). Только дела с блоком ИТОГ. Одна строка на дело:
        <a href="URL"><b>номер</b></a> — Апелляционное определение от <дата>. ИТОГ: <дословно поле ИТОГ>. Категория: <дословно>. Стороны: <истец> vs <ответчик>, банк — <роль>. Для банка: <дословно «В чью пользу для банка»>.
        Применяются ПРАВИЛА РЕЗОЛЮТИВНЫХ СЕКЦИЙ (см. выше). Для апелляции дополнительный перечень ИТОГ = «возвращена / без рассмотрения / прекращено / снято» — добавь причину из «Последнее событие».
   5.5. 📄 <b>Опубликованные тексты актов (N):</b> — полный текст акта (выходит через 14+ дней после заседания, иногда вовсе не публикуется). Только дела с полем «МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА»:
        • строка 1: <a href="URL"><b>номер</b></a>: {{стороны кратко}}
        • строка 2: <b>Апеллянт:</b> {{РОЛЬ}} {{имя}} — РОЛЬ и имя берёшь ДОСЛОВНО из поля «Апеллянт» в данных (формат «Истец <имя>» / «Ответчик <имя>» / «Иное лицо <имя>»). Примеры: «<b>Апеллянт:</b> Ответчик Буклей А.Л.», «<b>Апеллянт:</b> Истец Сбербанк», «<b>Апеллянт:</b> Иное лицо Фин. уполномоченный». Если поле «Апеллянт» пустое — блок «<b>Апеллянт:</b> …» не пиши вообще (полностью пропусти), не подставляй «не указано», «—», «0». НЕ пиши просто «Иное лицо» без имени, если имя в данных есть. <b>Итог:</b> {{удовлетворено / отказано / отменено полностью / отменено в части / изменено / без изменения — дословно из «ИТОГ (из карточки)» если он есть, иначе извлеки из мотивировки}}.
        • строка 3: <b>Почему:</b> см. ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (выше). Если из одних сторон неочевидно, кто оспаривал решение и чего добивался (напр., «Сбербанк vs Фин. уполномоченный» — обе стороны институциональные), начни «Почему» с короткой фразы «<Роль апеллянта> <имя> оспаривал <что>…», чтобы читатель сразу понял направление жалобы.
        Применяются ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (формат трёх строк, блок ЗАПРЕЩЕНО — см. выше).

ВАЖНО про 5.4 и 5.5: это РАЗНЫЕ события, разведённые во времени. Если у одного дела есть И ИТОГ, И МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА — оно появится В ОБЕИХ секциях, это корректно. Не объединяй и не дедублицируй.

ВАЖНО про 3.5 и 3.6: то же самое — РАЗНЫЕ события, разведённые во времени. Дело с вынесенным решением сразу попадает в 3.5; когда через 14+ дней опубликуют мотивировочную часть — оно же попадёт в 3.6. Обе секции показываем, не объединяем.

6. 📌 Итоговая строка: <b>В производстве: всего {total_active} (1 инст.: {total_active_fi} | апелляция: {total_active_appeal})</b>. Используй ИМЕННО эти три числа дословно — не считай, не угадывай, не округляй.
7. В конце: <a href="{DASHBOARD_URL}">📊 Дашборд</a> — обязательно всегда.

ОФОРМЛЕНИЕ: без маркеров списка («• », «- »); названия больших блоков и секций — <b>жирным</b>; номера дел — <b>жирным</b> внутри ссылок. ПУСТЫЕ СТРОКИ (обязательны, без них теряется граница): (а) перед каждой подсекцией 📥/📅/⚖️/📄/🔁/📨/⚠ — ровно одна пустая строка, отделяющая её от предыдущего дела или подсекции; (б) между делами в одной подсекции — ровно одна пустая строка, даже в однострочных подсекциях 3.3/3.5/5.1/5.4.

СТИЛЬ: кратко, по-деловому, на русском. Без вступлений. Не дублируй информацию между секциями (за исключением 5.4↔5.5, см. выше).

ЛИМИТ: {DIGEST_CHAR_LIMIT} символов. При нехватке места сокращать описания актов. Секцию 🔁 «Отложенные заседания» — НЕ выкидывать никогда. Ссылка на дашборд — ВСЕГДА в конце.

ВАЖНО: в разделе «Данные» ниже перечислены только ИЗМЕНЕНИЯ за сегодня, а не все дела. Общие числа берутся ИСКЛЮЧИТЕЛЬНО из пункта 6 выше.

Данные:
{chr(10).join(context_parts)}"""

    if LLM_PROVIDER == "gigachat":
        log.info(f"LLM: GigaChat (model={GIGACHAT_MODEL}, scope={GIGACHAT_SCOPE})")
        text = _call_gigachat(prompt)
        if not text:
            return generate_template_digest(
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
            )
        text = _validate_digest_new_sections(text, fi_new_cases, new_cases)
        return truncate_html_message(text, TELEGRAM_MSG_LIMIT * 2)

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
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
            )
        text = _validate_digest_new_sections(text, fi_new_cases, new_cases)
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


# ── Пост-процессор: страховка от LLM-галлюцинаций в «новых» секциях ──────────

_DIGEST_CASE_LINK_RE = re.compile(r'<a[^>]*>\s*<b>\s*([^<]+?)\s*</b>\s*</a>')

# Линия считается заголовком подсекции/блока, если начинается с одного из
# этих эмодзи + <b>. Покрывает все заголовки, которые порождает промпт
# `generate_digest`. Нужно только для поиска границы секции — не обязано
# быть полным, главное — не ловить строки-дела.
_DIGEST_HEADER_RE = re.compile(
    r'^\s*(?:📥|📅|📨|🔄|⚠|🔁|⚖️|📄|🏛|🔀|📌|📊|📋)\s*<b>'
)


def _bare_case_number(num: str) -> str:
    """«2-216/2026 (2-1156/2025;)» → «2-216/2026». Нужно потому, что поиск
    в судах возвращает только текущий номер, а в cases.json хранится полный
    с суффиксом переномерования."""
    s = (num or "").strip()
    if "(" in s:
        bare = s.split("(")[0].strip()
        return bare or s
    return s


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
    """
    allowed_fi: set[str] = set()
    for c in fi_new_cases or []:
        for key in (c.get("id"), (c.get("first_instance") or {}).get("case_number")):
            k = (key or "").strip()
            if k:
                allowed_fi.add(k)
                allowed_fi.add(_bare_case_number(k))

    allowed_appeal: set[str] = set()
    for c in appeal_new_cases or []:
        n = (c.get("Номер дела") or "").strip()
        if n:
            allowed_appeal.add(n)
            allowed_appeal.add(_bare_case_number(n))

    html = _drop_hallucinated_from_section(
        html,
        header_re=re.compile(
            r'^\s*📥\s*<b>\s*Новые иски\s*\(\s*(\d+)\s*\)\s*:\s*</b>\s*$'
        ),
        allowed=allowed_fi,
        label="1 инст./Новые иски",
    )
    html = _drop_hallucinated_from_section(
        html,
        header_re=re.compile(
            r'^\s*📥\s*<b>\s*Новые дела\s*\(\s*(\d+)\s*\)\s*:\s*</b>\s*$'
        ),
        allowed=allowed_appeal,
        label="апелляция/Новые дела",
    )
    return html


def _drop_hallucinated_from_section(
    html: str, *, header_re: "re.Pattern[str]", allowed: set[str], label: str
) -> str:
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
        for ln in lines[i + 1:j]:
            if not ln.strip():
                continue  # пустые строки-разделители в «Новых» не ожидаются
            mnum = _DIGEST_CASE_LINK_RE.search(ln)
            if not mnum:
                log.warning(
                    f"Пост-процессор дайджеста: в секции «{label}» строка "
                    f"без номера дела, пропускаю: {ln.strip()[:80]}"
                )
                continue
            num = mnum.group(1).strip()
            if num in allowed or _bare_case_number(num) in allowed:
                kept.append(ln)
            else:
                removed.append(num)

        if not kept:
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

        old_count = m.group(1)
        new_header = lines[i].replace(f"({old_count})", f"({len(kept)})", 1)
        out.append(new_header)
        out.extend(kept)
        i = j

    return "\n".join(out)


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


def generate_template_digest(new_cases: list[dict], changes: list[dict], *,
                             cases: list[dict] | None = None,
                             fi_new_cases: list[dict] | None = None,
                             stage_transitions: list[dict] | None = None,
                             fi_changes: list[dict] | None = None,
                             total_active_appeal: int = 0,
                             total_active_fi: int = 0) -> str:
    """Шаблонный дайджест (fallback без Claude API). Формат: HTML.

    Структура — два больших блока (🏛 ПЕРВАЯ ИНСТАНЦИЯ / ⚖️ АПЕЛЛЯЦИЯ),
    мостик «🔀 Перешли в апелляцию» между ними. Подсекция выводится только
    если есть данные; большой блок выводится только если хотя бы одна его
    подсекция непуста.
    """
    today = datetime.now().strftime("%d.%m.%Y")
    if cases is None:
        cases = []
    if fi_new_cases is None:
        fi_new_cases = []
    if stage_transitions is None:
        stage_transitions = []
    if fi_changes is None:
        fi_changes = []

    total_active = total_active_appeal + total_active_fi

    # ── Короткое сообщение если изменений нет ──
    if (not new_cases and not changes and not fi_new_cases
            and not stage_transitions and not fi_changes):
        msg = (
            f"✅ <b>Мониторинг дел Сбербанка — {today}</b>\n\n"
            f"Всё спокойно, изменений нет.\n"
            f"В производстве: всего {total_active}"
            f" (1 инст.: {total_active_fi} | апелляция: {total_active_appeal})"
        )
        msg += f'\n\n<a href="{DASHBOARD_URL}">📊 Дашборд</a>'
        return msg

    # ── Группировка changes по типам (для блока АПЕЛЛЯЦИЯ) ──
    postponed = [ch for ch in changes if "hearing_postponed" in ch["type"]]
    postponed_nums = {ch["case"] for ch in postponed}
    to_fi_rules = [ch for ch in changes if "appeal_to_fi_rules" in ch["type"]]
    # Не дублируем дело в "Назначенные", если оно уже в "Отложенные".
    # hearing_new — первое заседание апелляции; семантически то же самое, что и
    # «назначенное заседание», поэтому показываем тут же.
    events = [ch for ch in changes
              if ("new_event" in ch["type"] or "hearing_new" in ch["type"])
              and ch["case"] not in postponed_nums]
    # 5.4 и 5.5 — РАЗНЫЕ события (резолютивка и полный текст), разведённые
    # во времени. Дело может попасть в обе секции — это корректно.
    results = [ch for ch in changes if "new_result" in ch["type"]]
    acts = [ch for ch in changes if "new_act" in ch["type"]]

    # ── Блок ПЕРВАЯ ИНСТАНЦИЯ ──
    fi_block: list[str] = []
    if fi_new_cases:
        fi_block.append(f"📥 <b>Новые иски ({len(fi_new_cases)}):</b>")
        for c in fi_new_cases:
            fi = c.get("first_instance", {})
            court = escape_html(shorten_court_name(fi.get("court", "")))
            role = c.get("bank_role", "")
            role_icon = {"Истец": "🏦→", "Ответчик": "→🏦", "Третье лицо": "👁"
                         }.get(role, "")
            cat = category_short(c.get("category", ""))
            pl = escape_html(shorten_party_name(c.get("plaintiff", ""), keep_fio_full=True))
            df = escape_html(shorten_party_name(c.get("defendant", ""), keep_fio_full=True))
            num = escape_html(c.get("id", ""))
            filing = escape_html(fi.get("filing_date", ""))
            url = fi_card_url(fi)
            link = f'<a href="{url}"><b>{num}</b></a>' if url else f'<b>{num}</b>'
            fi_block.append(
                f"  {link} {role_icon}"
                f"{pl} vs {df} "
                f"({cat}) | {court}"
                + (f" | подано {filing}" if filing else "")
            )

    # Отделяем дела, у которых есть вынесенное решение — они поедут в 3.5.
    # В 3.2 «Изменения» их статус/резолюция не повторяются; оставляем
    # только побочные события того же дела (заседание/отложение и т.п.).
    # То же для fi_act_text_published — эти дела поедут в 3.6.
    fi_resolved_chs = [ch for ch in fi_changes if "fi_resolved" in ch["type"]]
    fi_act_text_chs = [
        ch for ch in fi_changes if "fi_act_text_published" in ch["type"]
    ]
    fi_changes_rendered: list[str] = []
    for ch in fi_changes:
        has_resolved = "fi_resolved" in ch["type"]
        has_act_text = "fi_act_text_published" in ch["type"]
        types_for_line = [
            t for t in ch["type"]
            if not (has_resolved and t in ("fi_resolved", "fi_status_change"))
            and t != "fi_act_text_published"
            and not (has_act_text and t == "fi_act_published")
        ]
        if not types_for_line:
            continue
        num = escape_html(ch.get("case", ""))
        court = escape_html(shorten_court_name(ch.get("court", "")))
        pl = escape_html(shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True))
        df = escape_html(shorten_party_name(ch.get("defendant", ""), keep_fio_full=True))
        d = ch["details"]
        url = fi_card_url(d)
        link = f'<a href="{url}"><b>{num}</b></a>' if url else f'<b>{num}</b>'
        ev_list: list[str] = []
        for t in types_for_line:
                if t == "fi_hearing_new":
                    hd = escape_html(d.get("hearing_date", ""))
                    ht = escape_html(d.get("hearing_time", ""))
                    htype = escape_html(d.get("hearing_type", "заседание"))
                    ev_list.append(f"📅 {htype} {hd}" + (f" {ht}" if ht else ""))
                elif t == "fi_hearing_postponed":
                    old_p = escape_html(
                        d.get("old_hearing_date", "")
                        + (f" {d['old_hearing_time']}" if d.get("old_hearing_time") else "")
                    )
                    new_p = escape_html(
                        d.get("hearing_date", "")
                        + (f" {d['hearing_time']}" if d.get("hearing_time") else "")
                    )
                    ev_list.append(f"🔁 {old_p} → {new_p}")
                elif t == "fi_status_change":
                    ev_list.append(
                        f"статус: {escape_html(d.get('old_status', ''))} → "
                        f"{escape_html(d.get('new_status', ''))}"
                    )
                elif t == "fi_act_published":
                    ad = escape_html(d.get("act_date", ""))
                    ev_list.append("📄 опубликован акт" + (f" ({ad})" if ad else ""))
                elif t == "fi_final_event":
                    ev_list.append(f"⚖️ {escape_html(d.get('event', ''))}")
                elif t == "fi_appeal_filed":
                    role = escape_html(d.get("appellant_role", ""))
                    name = escape_html(d.get("appellant_name", ""))
                    dt = escape_html(d.get("appeal_filed_date", ""))
                    app_str = f"{role} {name}".strip()
                    ev_list.append(
                        "📨 подана апелляц. жалоба"
                        + (f" ({dt})" if dt else "")
                        + (f", апеллянт: {app_str}" if app_str else "")
                    )
                elif t == "fi_cassation_filed":
                    dt = escape_html(d.get("cassation_filed_date", ""))
                    ev_list.append(
                        "📨 подана кассационная жалоба"
                        + (f" ({dt})" if dt else "")
                    )
                elif t == "fi_sent_to_cassation":
                    dt = escape_html(d.get("sent_to_cassation_date", ""))
                    ev_list.append(
                        "📤 направлено в кассац. суд"
                        + (f" ({dt})" if dt else "")
                    )
                elif t == "fi_hearing_restart":
                    rd = escape_html(d.get("restart_date", ""))
                    nhd = escape_html(d.get("next_hearing_date", ""))
                    nht = escape_html(d.get("next_hearing_time", ""))
                    part = "🔄 рассмотрение начато с начала" + (f" ({rd})" if rd else "")
                    if nhd:
                        part += f"; след. заседание {nhd}" + (f" {nht}" if nht else "")
                    ev_list.append(part)
        ev_str = "; ".join(ev_list) if ev_list else ""
        fi_changes_rendered.append(
            f"  {link} ({court}) — {pl} vs {df} | {ev_str}"
        )

    if fi_changes_rendered:
        if fi_block:
            fi_block.append("")
        fi_block.append(
            f"📅 <b>Изменения ({len(fi_changes_rendered)}):</b>"
        )
        fi_block.extend(fi_changes_rendered)

    # ── 3.5: Вынесенные решения 1 инстанции ──
    if fi_resolved_chs:
        if fi_block:
            fi_block.append("")
        fi_block.append(
            f"⚖️ <b>Вынесенные решения ({len(fi_resolved_chs)}):</b>"
        )
        for ch in fi_resolved_chs:
            num = escape_html(ch.get("case", ""))
            court = escape_html(shorten_court_name(ch.get("court", "")))
            pl = escape_html(shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True))
            df = escape_html(shorten_party_name(ch.get("defendant", ""), keep_fio_full=True))
            d = ch["details"]
            url = fi_card_url(d)
            link = f'<a href="{url}"><b>{num}</b></a>' if url else f'<b>{num}</b>'
            verdict = escape_html(d.get("verdict_label", ""))
            dec_date = escape_html(d.get("decision_date", ""))
            cat = escape_html(category_short(d.get("category", "")))
            bank_role = escape_html(ch.get("bank_role", ""))
            bank_out = escape_html(d.get("bank_outcome", ""))
            # В template держим компактно: одна строка. Формат симметричен
            # тому, что просит LLM в 3.5, но без лишних отступов.
            tail = (
                f" — Решение"
                + (f" от {dec_date}" if dec_date else "")
                + (f". <b>ИТОГ:</b> {verdict}" if verdict else "")
            )
            extras: list[str] = []
            if cat:
                extras.append(f"категория: {cat}")
            if bank_role:
                extras.append(f"банк — {bank_role.lower()}")
            if bank_out:
                extras.append(f"<b>для банка:</b> {bank_out}")
            extras_str = (" | " + "; ".join(extras)) if extras else ""
            fi_block.append(
                f"  {link} ({court}) — {pl} vs {df}{tail}{extras_str}"
            )

    # ── 3.6: Опубликованные тексты решений 1 инстанции ──
    # Fallback без LLM — выводим укороченный фрагмент мотивировки как есть,
    # без попытки написать осмысленное «Почему». Лучше так, чем пустота.
    if fi_act_text_chs:
        if fi_block:
            fi_block.append("")
        fi_block.append(
            f"📄 <b>Опубликованные тексты решений ({len(fi_act_text_chs)}):</b>"
        )
        for ch in fi_act_text_chs:
            num = escape_html(ch.get("case", ""))
            pl = escape_html(shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=True))
            df = escape_html(shorten_party_name(ch.get("defendant", ""), keep_fio_full=True))
            d = ch["details"]
            url = fi_card_url(d)
            link = f'<a href="{url}"><b>{num}</b></a>' if url else f'<b>{num}</b>'
            verdict = escape_html(d.get("verdict_label", ""))
            bank_out = escape_html(d.get("bank_outcome", ""))
            act_excerpt = (d.get("act_text") or "").strip()
            # Обрезаем до ~500 символов для компактности шаблона; добавляем «…»
            if len(act_excerpt) > 500:
                act_excerpt = act_excerpt[:500].rstrip() + "…"
            act_excerpt = escape_html(act_excerpt)
            fi_block.append(f"  {link}: {pl} vs {df}")
            itog_parts: list[str] = []
            if verdict:
                itog_parts.append(f"<b>Итог:</b> {verdict}")
            if bank_out:
                itog_parts.append(f"<b>Для банка:</b> {bank_out}")
            if itog_parts:
                fi_block.append("     " + ". ".join(itog_parts))
            if act_excerpt:
                fi_block.append(f"     <i>{act_excerpt}</i>")
            fi_block.append("")  # пустая строка-разделитель между делами
        # убрать хвостовую пустую строку, если добавили
        if fi_block and fi_block[-1] == "":
            fi_block.pop()

    # ── Мостик: переходы в апелляцию ──
    transition_block: list[str] = []
    if stage_transitions:
        transition_block.append(
            f"🔀 <b>Перешли в апелляцию ({len(stage_transitions)}):</b>"
        )
        for t in stage_transitions:
            fi_num = escape_html(t["fi_case_number"])
            ap_num = escape_html(t["appeal_case_number"])
            pl = escape_html(shorten_party_name(t.get("plaintiff", "")))
            df = escape_html(shorten_party_name(t.get("defendant", "")))
            transition_block.append(
                f"  <b>{fi_num}</b> → <b>{ap_num}</b>: {pl} vs {df}"
            )

    # ── Блок АПЕЛЛЯЦИЯ ──
    appeal_block: list[str] = []
    if new_cases:
        appeal_block.append(f"📥 <b>Новые дела ({len(new_cases)}):</b>")
        for c in new_cases:
            link = case_link_html(c)
            role = c.get("Роль банка", "")
            role_icon = {"Истец": "🏦→", "Ответчик": "→🏦", "Третье лицо": "👁"
                         }.get(role, "")
            cat = category_short(c.get("Категория", ""))
            pl = escape_html(shorten_party_name(c['Истец'], keep_fio_full=True))
            df = escape_html(shorten_party_name(c['Ответчик'], keep_fio_full=True))
            appeal_block.append(
                f"  {link} {role_icon}"
                f"{pl} vs {df} "
                f"({cat})"
            )

    if to_fi_rules:
        if appeal_block:
            appeal_block.append("")
        appeal_block.append(
            f"⚠ <b>Переход к правилам 1-й инст. ({len(to_fi_rules)}):</b>"
        )
        for ch in to_fi_rules:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = (f'<a href="{url}"><b>{case_num}</b></a>'
                    if url else f'<b>{case_num}</b>')
            plaintiff = escape_html(shorten_party_name(d.get("plaintiff", "")))
            defendant = escape_html(shorten_party_name(d.get("defendant", "")))
            tr_dt = escape_html(d.get("transition_date", ""))
            role = d.get("role", "")
            role_note = f" | банк — {escape_html(role.lower())}" if role else ""
            line = f"  ⚠ {link}"
            if tr_dt:
                line += f" ({tr_dt})"
            line += " — по правилам производства в суде первой инстанции"
            if plaintiff and defendant:
                line += f"\n     {plaintiff} vs {defendant}{role_note}"
            appeal_block.append(line)

    if postponed:
        if appeal_block:
            appeal_block.append("")
        appeal_block.append(f"🔁 <b>Отложенные заседания ({len(postponed)}):</b>")
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
            appeal_block.append(f"  🔁 {link}: ⏪ {old_part} → ⏩ {new_part}")
            if plaintiff and defendant:
                tail = f"     {plaintiff} vs {defendant}"
                if cat:
                    tail += f" | {escape_html(cat)}"
                appeal_block.append(tail)

    if events:
        if appeal_block:
            appeal_block.append("")
        appeal_block.append(f"📅 <b>Назначенные заседания ({len(events)}):</b>")
        for ch in events:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = f'<a href="{url}"><b>{case_num}</b></a>' if url else f'<b>{case_num}</b>'
            plaintiff = escape_html(shorten_party_name(d.get("plaintiff", "")))
            defendant = escape_html(shorten_party_name(d.get("defendant", "")))
            parties = f"{plaintiff} vs {defendant}" if plaintiff and defendant else ""
            event_raw = d.get("event", "")
            event_date = d.get("event_date", "")
            # Для чистого hearing_new (без new_event) синтезируем строку
            # «Судебное заседание. HH:MM. DD.MM.YYYY» — дальнейший парсинг
            # отделит дату и время, рендер пройдёт по ветке is_hearing.
            if not event_raw and "hearing_new" in ch["type"]:
                hd = d.get("new_hearing_date", "")
                ht = d.get("new_hearing_time", "")
                event_raw = "Судебное заседание" + (
                    f". {ht}" if ht else "") + (f". {hd}" if hd else "")
            is_hearing = "заседани" in event_raw.lower()
            parts = event_raw.split(". ")
            clean_parts = []
            hearing_date = ""
            hearing_time = ""
            for p in parts:
                ps = p.strip()
                if parse_date(ps):
                    if is_hearing:
                        hearing_date = ps
                    elif not event_date:
                        event_date = ps
                    continue
                if re.match(r'^\d{1,2}:\d{2}$', ps):
                    if is_hearing:
                        hearing_time = ps
                    continue
                if ps:
                    clean_parts.append(ps)
            event_clean = escape_html(". ".join(clean_parts))
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
            appeal_block.append(line)

    if results:
        if appeal_block:
            appeal_block.append("")
        # Резолютивная часть — выходит через 1-3 дня после заседания.
        appeal_block.append(f"⚖️ <b>Вынесенные акты ({len(results)}):</b>")
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
            appeal_block.append(
                f"  {link}: {result_text}{cat_note}{role_note}{date_note}{ev_note}"
            )

    if acts:
        if appeal_block:
            appeal_block.append("")
        # Полный текст с мотивировкой — обычно через 14+ дней (или никогда).
        appeal_block.append(f"📄 <b>Опубликованные тексты актов ({len(acts)}):</b>")
        for ch in acts:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = f'<a href="{url}"><b>{case_num}</b></a>' if url else f'<b>{case_num}</b>'
            # 1-2 фразы мотивировки. act_excerpt уже сжатый, act_text — сырой.
            excerpt = (d.get("act_excerpt") or d.get("act_text") or "").strip()
            if excerpt:
                # Первые 1-2 предложения, лимит ~250 символов.
                short_parts = re.split(r"(?<=[.!?])\s+", excerpt)[:2]
                short = " ".join(short_parts)[:250].rstrip(".") + "."
                appeal_block.append(
                    f"  {link}\n    Мотивировка: {escape_html(short)}"
                )
            else:
                appeal_block.append(f"  {link}")

    # ── Сборка ──
    summary = build_summary_line(
        new_cases, changes, fi_new_cases, stage_transitions, fi_changes
    )
    lines = [
        f"📊 <b>Мониторинг дел Сбербанка — {today}</b>",
        f"📋 {escape_html(summary)}",
    ]

    if fi_block:
        lines.append("")
        lines.append("🏛 <b>ПЕРВАЯ ИНСТАНЦИЯ</b>")
        lines.extend(fi_block)
    if transition_block:
        lines.append("")
        lines.extend(transition_block)
    if appeal_block:
        lines.append("")
        lines.append("⚖️ <b>АПЕЛЛЯЦИЯ</b>")
        lines.extend(appeal_block)

    lines.append("")
    lines.append(
        f"📌 <b>В производстве: всего {total_active}"
        f" (1 инст.: {total_active_fi} | апелляция: {total_active_appeal})</b>"
    )
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
    if require_anthropic:
        if LLM_PROVIDER == "gigachat":
            if not GIGACHAT_AUTH_KEY:
                missing.append("GIGACHAT_AUTH_KEY")
        elif not ANTHROPIC_API_KEY:
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
    # Архив подмешиваем только в индекс дедупликации, чтобы дела, которые
    # юрист уже отправил в архив, не появлялись снова как «новые».
    archived_csv = load_csv(CSV_ARCHIVE_PATH)
    timings["load_csv"] = time.perf_counter() - t0
    existing_numbers = {
        c["Номер дела"].strip()
        for c in cases + archived_csv
        if c.get("Номер дела")
    }
    log.info(f"Загружено {len(cases)} дел из CSV (+{len(archived_csv)} в архиве)")

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
                    _warn_if_card_degraded(card_info, nc["Номер дела"])
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
    # main() — это apellation-only режим (без JSON/FI), поэтому FI=0.
    total_active_appeal = sum(
        1 for c in cases if c.get("Статус", "").strip() != "Решено"
    )

    # 7. Генерируем дайджест
    t0 = time.perf_counter()
    log.info("Генерирую дайджест...")
    save_digest_context(
        new_cases, changes, cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=0,
    )
    digest = generate_digest(
        new_cases, changes, cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=0,
    )
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

    total_active_appeal = sum(
        1 for c in cases if c.get("Статус", "").strip() != "Решено"
    )

    log.info(
        f"Синтетический change: {old_date} {old_time} → {new_date} {new_time}"
    )
    log.info("Генерирую дайджест...")
    save_digest_context(
        [], [change], cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=0,
    )
    digest = generate_digest(
        [], [change], cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=0,
    )
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
            "act_text": "",
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
    # Архив подмешиваем только в индекс дедупликации, чтобы дела, которые
    # юрист уже отправил в архив, не появлялись снова как «новые» в дайджесте.
    archive_data = load_json(JSON_ARCHIVE_PATH)
    archived_cases = archive_data.get("cases", [])
    timings["load_json"] = time.perf_counter() - t0

    # Индексы для быстрого поиска по всем номерам дел
    existing_ids = set()
    for c in cases + archived_cases:
        cid = (c.get("id") or "").strip()
        if cid:
            existing_ids.add(cid)
            # Старые дела архивируются с переномерованием в id, например
            # «2-122/2026 (2-535/2025;)» — добавляем ещё и «голую» часть,
            # т.к. поиск суда возвращает только текущий номер.
            bare = cid.split("(")[0].strip()
            if bare and bare != cid:
                existing_ids.add(bare)
        fi = c.get("first_instance")
        if fi and fi.get("case_number"):
            existing_ids.add(fi["case_number"].strip())
        ap = c.get("appeal")
        if ap and ap.get("case_number"):
            existing_ids.add(ap["case_number"].strip())

    log.info(f"Загружено {len(cases)} дел из JSON (+{len(archived_cases)} в архиве)")

    # Миграция старой модели стадий (first_instance|appeal) на новую
    # state-machine. Идемпотентно: прогоняет advance_case_stage до фиксированной
    # точки. На повторных прогонах мигрирует только дела, у которых с прошлого
    # раза появились новые сигналы (жалоба/акт/истекло окно).
    migrated = migrate_stages(cases)
    if migrated:
        log.info(f"State-machine: мигрировано {migrated} переходов при загрузке")

    # ── 2. Парсинг апелляции: новые дела ──
    t0 = time.perf_counter()
    csv_cases = load_csv(CSV_PATH)
    csv_archived = load_csv(CSV_ARCHIVE_PATH)
    csv_existing = {
        c["Номер дела"].strip()
        for c in csv_cases + csv_archived
        if c.get("Номер дела")
    }
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
                    _warn_if_card_degraded(card_info, nc["Номер дела"])
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
    # 4a. Апелляция: обновляем карточки апел. только для стадии "appeal".
    # После перехода в cassation_watch апел. карточка больше не
    # парсится (см. user-decision: «30 дней после апел. заседания или
    # публикация акта — и мы перестаём парсить сайт апел. инстанции»).
    t0 = time.perf_counter()
    log.info(f"Обновляю {csv_active_count} активных дел апелляции...")
    json_appeal_by_num: dict = {}
    skip_apel_nums: set[str] = set()
    for c in cases:
        ap = c.get("appeal")
        if ap and ap.get("case_number"):
            num = ap["case_number"].strip()
            json_appeal_by_num[num] = ap
            if c.get("current_stage") != "appeal":
                skip_apel_nums.add(num)
    csv_cases, changes = update_active_cases(
        csv_cases, json_appeal_by_num, skip_apel_nums=skip_apel_nums,
    )

    if appeal_new_cases_csv:
        csv_cases = appeal_new_cases_csv + csv_cases

    timings["appeal_update"] = time.perf_counter() - t0

    # 4b. Первая инстанция: обновляем карточки 1-й инст. только для стадий,
    # где она активна — first_instance (стандартный мониторинг) и
    # cassation_watch (ищем касс. жалобу после апел. определения).
    # awaiting_appeal / appeal / cassation_pending — парсинг 1-й инст.
    # не нужен (см. advance_case_stage).
    t0 = time.perf_counter()
    fi_active = [
        c for c in cases
        if c.get("current_stage") in ("first_instance", "cassation_watch")
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

        # Фолбэк: при малом числе таблиц повторяем с new=0 — sudrf при наличии
        # вкладки «обжалование решений, определений (пост.)» по умолчанию
        # открывает её (≤4 таблиц) вместо основной «Дело» (≥6 таблиц с
        # движением). new=0 форсит основную вкладку.
        if card_info.get("_table_count", 0) < 6:
            polite_delay()
            alt_html = fetch_page(court_cfg.card_url_alt(cid, cuid))
            if alt_html:
                alt_info = parse_case_card(alt_html, court_cfg.base_url)
                if alt_info.get("_table_count", 0) > card_info.get("_table_count", 0):
                    # Флаги жалоб/направления могли быть выставлены только
                    # на короткой вкладке (HTML-маркеры/частичное движение).
                    # Переносим их в alt_info, чтобы события не потерялись.
                    for flag, date_key in (
                        ("_fi_appeal_filed", "_fi_appeal_filed_date"),
                        ("_fi_cassation_filed", "_fi_cassation_filed_date"),
                        ("_fi_sent_to_cassation", "_fi_sent_to_cassation_date"),
                    ):
                        if card_info.get(flag) and not alt_info.get(flag):
                            alt_info[flag] = True
                            if card_info.get(date_key) and not alt_info.get(date_key):
                                alt_info[date_key] = card_info[date_key]
                    card_info = alt_info
        _warn_if_card_degraded(card_info, fi["case_number"])

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
        # Полный список событий — обновляем всегда, если парсер его вернул.
        # Старый список фиксируем для детекторов «с начала» / «по правилам 1-й инст.»
        old_events_fi = list(fi.get("events") or [])
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
            # link и court_domain нужны fi_card_url() для построения ссылки на
            # карточку дела в дайджесте — без них модель и шаблон отдают «голый» номер.
            "details": {
                "link": fi.get("link", ""),
                "court_domain": fi.get("court_domain", ""),
            },
        }

        # Новое/перенесённое заседание
        if new_hearing_date and new_hearing_date != old_hearing_date:
            new_h_dt_fi = parse_date(new_hearing_date)
            has_held_fi = _has_held_prior_hearing(
                card_info.get("_events") or [], new_h_dt_fi
            )
            # Перенос — только если есть прошедшее заседание в истории движения.
            # Пустое old_hearing_date — однозначно первое; непустое без
            # прошедшего заседания — скорее всего артефакт прошлого парсинга.
            if not old_hearing_date or not has_held_fi:
                change["type"].append("fi_hearing_new")
            else:
                change["type"].append("fi_hearing_postponed")
                change["details"]["old_hearing_date"] = old_hearing_date
                change["details"]["old_hearing_time"] = old_hearing_time
            change["details"]["hearing_date"] = new_hearing_date
            change["details"]["hearing_time"] = new_hearing_time
            # Тип заседания (беседа / предварительное / подготовка / заседание) —
            # нужен LLM для 3.2, чтобы не писать обобщённое «заседание»
            # вместо конкретики. Ищем в _events запись с той же датой,
            # распознаём тип по первой фразе текста.
            matched_ev = next(
                (ev for ev in (card_info.get("_events") or [])
                 if ev.get("date") == new_hearing_date),
                None,
            )
            if matched_ev:
                change["details"]["hearing_type"] = classify_hearing_type(
                    matched_ev.get("text", "")
                )

        # Смена статуса (регрессии отфильтрованы выше)
        if new_status and new_status != old_status:
            change["type"].append("fi_status_change")
            change["details"]["old_status"] = old_status
            change["details"]["new_status"] = new_status

        # Вынесено решение по делу 1-й инст. — идемпотентный эмит для 3.5.
        # Триггер: status == «Решено» и флаг resolved_emitted ещё не
        # выставлен. Отсутствие флага = «ещё не эмитили» — при первом
        # прогоне после деплоя все уже решённые дела с валидным result
        # получат fi_resolved и догонят 3.5. Если карточка вернула
        # пустой/мусорный «Результат», пытаемся достать ИТОГ из
        # last_event (движение дела часто содержит «Вынесено решение
        # по делу. ОТКАЗАНО…» раньше, чем поле «Результат»).
        # Флаг ставим только при успешном эмите — иначе на следующем
        # прогоне попробуем ещё раз.
        if fi.get("status") == "Решено" and not fi.get("resolved_emitted", False):
            raw_result = (fi.get("result") or "").strip()
            if not raw_result:
                raw_result = extract_result_from_event(fi.get("last_event", ""))
            if raw_result:
                verdict = classify_verdict_fi(raw_result)
                bank_outcome = bank_side_outcome_fi(
                    case_j.get("bank_role", ""), verdict
                )
                change["type"].append("fi_resolved")
                change["details"]["raw_result"] = raw_result
                change["details"]["verdict_label"] = verdict
                change["details"]["bank_outcome"] = bank_outcome
                change["details"]["decision_date"] = fi.get("hearing_date", "")
                change["details"]["last_event"] = fi.get("last_event", "")
                change["details"]["category"] = case_j.get("category", "")
                fi["resolved_emitted"] = True
                changed = True

        # Публикация акта — только факт (флаг + дата).
        if new_act and not old_act:
            change["type"].append("fi_act_published")
            change["details"]["act_date"] = card_info.get("Дата публикации акта", "")

        # Захват текста опубликованного решения 1-й инстанции — для 3.6.
        # Отделено от fi_act_published, т.к. текст часто приходит ПОЗЖЕ
        # самой публикации (акт опубликован сегодня, мотивировочная часть —
        # через 14+ дней). Идемпотентно по fi["act_text"]: один раз поймали —
        # больше не тянем и не ретранслируем событие.
        old_act_text = (fi.get("act_text") or "").strip()
        if new_act and not old_act_text:
            act_text_fi = (card_info.get("act_text") or "").strip()
            if not act_text_fi and card_info.get("_act_url"):
                fetched = fetch_act_text(card_info["_act_url"])
                act_text_fi = (fetched or "").strip()
            if act_text_fi:
                # Обрезаем как у апелляции: 8000 символов в JSON,
                # 1800 — мотивировочная часть в контексте для LLM.
                fi["act_text"] = act_text_fi[:8000]
                changed = True
                verdict = classify_verdict_fi(fi.get("result", ""))
                change["type"].append("fi_act_text_published")
                change["details"]["act_text"] = extract_motive_part(
                    act_text_fi, 1800
                )
                change["details"]["act_date"] = (
                    change["details"].get("act_date")
                    or card_info.get("Дата публикации акта", "")
                )
                change["details"]["verdict_label"] = verdict
                change["details"]["raw_result"] = fi.get("result", "")
                change["details"]["bank_outcome"] = bank_side_outcome_fi(
                    case_j.get("bank_role", ""), verdict
                )
                change["details"]["category"] = case_j.get("category", "")
                change["details"]["last_event"] = fi.get("last_event", "")

        # Финальные события в движении дела — значимые для юриста
        if new_ev and new_ev != old_event:
            ev_l = new_ev.lower()
            # Маркеры значимых для юриста событий движения дела. Финальные
            # (архив/возвращение/решение) + досудебные (подготовка/беседа/
            # предварительное) + перенос. Имя типа исторически осталось
            # «fi_final_event», хотя сейчас покрывает не только финал.
            notable_markers = (
                # финальные
                "в архив",
                "возвращение иска",
                "мотивированное решение",
                "мотивированного решения",
                # досудебные (присутствие юриста обычно требуется)
                "подготовка дела",
                "беседа",
                "предварительное заседание",
                # перенос (страховка на случай, если hearing_date парсер
                # не успел обновить — тогда fi_hearing_postponed не сработает)
                "отложение",
            )
            if any(m in ev_l for m in notable_markers):
                change["type"].append("fi_final_event")
                change["details"]["event"] = new_ev
                change["details"]["event_date"] = card_info.get("Дата события", "")

        # «Рассмотрение дела начато с начала» — фиксируется, когда
        # соответствующее событие впервые появилось в истории.
        restart_ev = _events_newly_match(
            old_events_fi, card_info.get("_events") or [], _RESTART_RE
        )
        if restart_ev:
            change["type"].append("fi_hearing_restart")
            change["details"]["restart_event"] = restart_ev.get("text", "")
            change["details"]["restart_date"] = restart_ev.get("date", "")
            # Назначенное следующее заседание на момент «рассмотрения с начала».
            # Используется в 3.2 рядом с фразой «рассмотрение начато с начала»,
            # чтобы юрист сразу видел дату, когда дело пойдёт в работу заново.
            change["details"]["next_hearing_date"] = fi.get("hearing_date", "")
            change["details"]["next_hearing_time"] = fi.get("hearing_time", "")

        # Подана апелляционная жалоба — идемпотентно: стреляет один раз,
        # флаг fi["appeal_filed"] сохраняется в JSON и проверяется на след.
        # прогонах.
        new_appeal_filed = bool(card_info.get("_fi_appeal_filed"))
        old_appeal_filed = bool(fi.get("appeal_filed", False))
        if new_appeal_filed and not old_appeal_filed:
            appellant_raw = card_info.get("_appellant_raw", "")
            role, short = classify_appellant_role(
                appellant_raw,
                case_j.get("plaintiff", ""),
                case_j.get("defendant", ""),
            )
            change["type"].append("fi_appeal_filed")
            change["details"]["appellant_role"] = role
            change["details"]["appellant_name"] = short
            change["details"]["appeal_filed_date"] = (
                card_info.get("_fi_appeal_filed_date") or ""
            )
            fi["appeal_filed"] = True
            if card_info.get("_fi_appeal_filed_date"):
                fi["appeal_filed_date"] = card_info["_fi_appeal_filed_date"]
            changed = True

        # Подана кассационная жалоба — идемпотентный флаг + событие в дайджест.
        # Переход cassation_watch → cassation_pending делает advance_case_stage.
        new_cass_filed = bool(card_info.get("_fi_cassation_filed"))
        if new_cass_filed and not fi.get("cassation_filed", False):
            fi["cassation_filed"] = True
            cass_date = card_info.get("_fi_cassation_filed_date", "")
            if cass_date:
                fi["cassation_filed_date"] = cass_date
            change["type"].append("fi_cassation_filed")
            change["details"]["cassation_filed_date"] = cass_date
            changed = True

        # Дело направлено в кассационный суд — идемпотентный флаг + событие.
        new_sent_cass = bool(card_info.get("_fi_sent_to_cassation"))
        if new_sent_cass and not fi.get("sent_to_cassation", False):
            fi["sent_to_cassation"] = True
            sent_date = card_info.get("_fi_sent_to_cassation_date", "")
            if sent_date:
                fi["sent_to_cassation_date"] = sent_date
            change["type"].append("fi_sent_to_cassation")
            change["details"]["sent_to_cassation_date"] = sent_date
            changed = True

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

        # Обнаруживаем переходы: current_stage был first_instance/awaiting_appeal
        # → стал appeal (последствие link_cases).
        for c in cases:
            cid = c.get("id", "")
            prev = stage_before.get(cid)
            if prev in ("first_instance", "awaiting_appeal") and c.get("current_stage") == "appeal":
                ap = c.get("appeal", {}) or {}
                stage_transitions.append({
                    "fi_case_number": cid,
                    "appeal_case_number": ap.get("case_number", ""),
                    "plaintiff": c.get("plaintiff", ""),
                    "defendant": c.get("defendant", ""),
                    "from": prev,
                    "to": "appeal",
                })
        if stage_transitions:
            log.info(f"Переходов в апелляцию: {len(stage_transitions)}")

    # ── 7b. Прогон state-machine для всех дел ──
    # Переходы: first_instance → awaiting_appeal (по appeal_filed_date),
    # appeal → cassation_watch (акт или 30 дней без акта),
    # cassation_watch → cassation_pending (касс. жалоба или направление в касс. суд).
    # Пока только логируем. Формат отличается от stage_transitions (который
    # описывает только переходы в апелляцию), поэтому хранится отдельно —
    # дайджест подхватит в следующем коммите.
    lifecycle_transitions: list[dict] = []
    for c in cases:
        prev = advance_case_stage(c)
        if prev is None:
            continue
        lifecycle_transitions.append({
            "case_id": c.get("id", ""),
            "plaintiff": c.get("plaintiff", ""),
            "defendant": c.get("defendant", ""),
            "from": prev,
            "to": c.get("current_stage", ""),
        })
    if lifecycle_transitions:
        log.info(f"State-machine переходов: {len(lifecycle_transitions)}")
        for t in lifecycle_transitions:
            log.info(f"  {t['case_id']}: {t['from']} → {t['to']}")

    # ── 8. Архивирование JSON-дел по state-machine ──
    # is_case_archived выставляет архив только для стадий, прошедших полный
    # жизненный цикл (first_instance без жалобы 45+ дней или cassation_watch
    # без касс. жалобы 120+ дней).
    cases, fi_newly_archived = split_archived_json(cases)
    if fi_newly_archived:
        archive_data = load_json(JSON_ARCHIVE_PATH)
        archived_cases = archive_data.get("cases", [])
        existing_archive_ids = {
            (c.get("id") or "").strip() for c in archived_cases
        }
        to_add = [
            c for c in fi_newly_archived
            if (c.get("id") or "").strip() not in existing_archive_ids
        ]
        if to_add:
            archive_data["cases"] = archived_cases + to_add
            save_json(archive_data, JSON_ARCHIVE_PATH)
            log.info(
                f"В JSON-архив перенесено {len(to_add)} дел "
                f"(first_instance {FI_ARCHIVE_DAYS}д без жалобы или "
                f"cassation_watch {CASSATION_WATCH_DAYS}д без касс. жалобы)"
            )
        else:
            log.info(
                f"Архив-кандидатов: {len(fi_newly_archived)}, "
                "но все уже в архиве"
            )

    data["cases"] = cases
    save_json(data, JSON_PATH)
    timings["save"] = time.perf_counter() - t0

    # ── 9. Дайджест и Telegram ──
    # total_active: апелляция (CSV) + 1 инстанция (JSON, ещё не в апелляции).
    # FI считаем по статусу карточки, не по current_stage — иначе попадают
    # уже решённые дела и счётчик «1 инст.» получается завышенным.
    total_active_appeal = sum(
        1 for c in csv_cases if c.get("Статус", "").strip() != "Решено"
    )
    # FI-счётчик включает только дела, которые сейчас в мониторинге на 1-й
    # инстанции и ещё не вынесли решение. cassation_watch — это тоже парсинг
    # 1-й инстанции, но дело уже решено; в счётчик «активная 1-я инст.»
    # его не добавляем (исторически счётчик показывал «в производстве»).
    total_active_fi = sum(
        1 for c in cases
        if c.get("current_stage") == "first_instance"
        and (c.get("first_instance") or {}).get("status", "").strip() != "Решено"
    )
    t0 = time.perf_counter()
    log.info("Генерирую дайджест...")
    save_digest_context(
        appeal_new_cases_csv, changes, cases=csv_cases,
        fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
        fi_changes=fi_changes,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
    )
    digest = generate_digest(
        appeal_new_cases_csv, changes, cases=csv_cases,
        fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
        fi_changes=fi_changes,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
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


def main_replay_last():
    """Прогнать дайджест заново из LAST_DIGEST_CONTEXT_PATH.

    Используется для экспериментов с промптом/форматом: после любого
    продового прогона контекст лежит в `data/last_digest_context.json`,
    и этот режим пересоздаёт дайджест на тех же данных без повторного
    парсинга судов. Полезно, когда хочется проверить, как отработает
    изменённый промпт на реальных изменениях последнего дня.
    """
    log.info("=" * 60)
    log.info("Режим replay-last: дайджест из сохранённого контекста")
    log.info("=" * 60)

    validate_environment()

    if not os.path.exists(LAST_DIGEST_CONTEXT_PATH):
        log.error(
            f"Контекст не найден: {LAST_DIGEST_CONTEXT_PATH}. "
            "Сначала выполните полный прогон (--json или без флагов), "
            "чтобы сохранить контекст."
        )
        sys.exit(2)

    with open(LAST_DIGEST_CONTEXT_PATH, "r", encoding="utf-8") as f:
        ctx = json.load(f)

    saved_at = ctx.get("saved_at", "?")
    log.info(f"Контекст от {saved_at}: "
             f"changes={len(ctx.get('changes', []))}, "
             f"fi_changes={len(ctx.get('fi_changes', []))}, "
             f"new_cases={len(ctx.get('new_cases', []))}, "
             f"fi_new={len(ctx.get('fi_new_cases', []))}, "
             f"transitions={len(ctx.get('stage_transitions', []))}")

    log.info("Генерирую дайджест...")
    digest = generate_digest(
        ctx.get("new_cases", []),
        ctx.get("changes", []),
        cases=ctx.get("cases", []),
        fi_new_cases=ctx.get("fi_new_cases", []),
        stage_transitions=ctx.get("stage_transitions", []),
        fi_changes=ctx.get("fi_changes", []),
        total_active_appeal=ctx.get("total_active_appeal", 0),
        total_active_fi=ctx.get("total_active_fi", 0),
    )

    send_telegram(digest)
    log.info("Готово!")


def main_digest_only():
    """Сформировать и отправить дайджест по текущим данным CSV (без обращения к сайту суда)."""
    log.info("=" * 60)
    log.info("Режим digest-only: дайджест по текущим данным")
    log.info("=" * 60)

    validate_environment()

    cases = load_csv(CSV_PATH)
    log.info(f"Загружено {len(cases)} дел из CSV")

    total_active_appeal = sum(
        1 for c in cases if c.get("Статус", "").strip() != "Решено"
    )
    # FI-счётчик берём из JSON если он есть — без него «1 инст.» будет 0.
    json_data = load_json(JSON_PATH)
    json_cases = json_data.get("cases", [])
    total_active_fi = sum(
        1 for c in json_cases
        if c.get("current_stage") == "first_instance"
        and (c.get("first_instance") or {}).get("status", "").strip() != "Решено"
    )
    log.info(
        f"В производстве: всего {total_active_appeal + total_active_fi}"
        f" (1 инст.: {total_active_fi} | апелляция: {total_active_appeal})"
    )

    log.info("Генерирую дайджест...")
    digest = generate_digest(
        [], [], cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
    )

    send_telegram(digest)
    log.info("Готово!")


if __name__ == "__main__":
    # Выбор режима
    if "--replay-last" in sys.argv:
        mode_name = "replay-last"
        entry = main_replay_last
        entry_args: tuple = ()
    elif "--digest-only" in sys.argv:
        mode_name = "digest-only"
        entry = main_digest_only
        entry_args = ()
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
