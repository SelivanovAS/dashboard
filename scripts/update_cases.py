#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
import hashlib
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
from datetime import datetime, timedelta, date
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
    delo_id: int       # 5 = апелляция, 1540005 = 1 инст. (гражд.), 2800001 = касс. (гражд.)
    court_type: str    # "appeal" | "first_instance" | "cassation"
    enabled: bool = True
    srv_num: int = 1   # номер сервера (обычно 1, но бывает 2 — напр. Покачи)

    @property
    def base_url(self) -> str:
        return f"https://{self.domain}"

    @property
    def _delo_table(self) -> str:
        if self.delo_id == 5:
            return "g2_case"
        if self.delo_id == 2800001:
            # 7kas.sudrf.ru, гражданская кассация. Эмпирически найдено в форме
            # поиска (name_op=sf): таблица называется g33_case, не ka1_case.
            return "g33_case"
        return "g1_case"

    @property
    def _name_field(self) -> str:
        """Имя поля для фильтрации по стороне (зависит от типа суда)."""
        if self.delo_id == 5:
            return "G2_PARTS__NAMESS"
        if self.delo_id == 2800001:
            return "G33_PARTS__NAMESS"
        return "G1_PARTS__NAMESS"

    @property
    def _new_param(self) -> int:
        """Параметр &new= : 5 для апелляции, 0 для 1 инст., 2800001 для касс.
        (для кассации значение совпадает с delo_id — нестандарт, но эмпирически
        проверено: при new=0 поиск возвращает «Данных по запросу не обнаружено»)."""
        if self.delo_id == 5:
            return 5
        if self.delo_id == 2800001:
            return 2800001
        return 0

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
            f"&delo_id={self.delo_id}&new={self._new_param}"
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

# Седьмой кассационный суд общей юрисдикции (гражданские дела, delo_id=2800001).
# Покрывает 7 регионов (Свердловск, Челябинск, Курган, Пермь, Тюмень,
# Башкортостан, ХМАО, Оренбург, ЯНАО). Мы фильтруем по 1-й инст. ХМАО
# (см. match_hmao_first_instance), поэтому видим только «свои» дела.
CASSATION_COURT = CourtConfig(
    name="Седьмой кассационный суд общей юрисдикции",
    domain="7kas.sudrf.ru",
    delo_id=2800001,
    court_type="cassation",
)


def match_hmao_first_instance(long_court_name: str) -> CourtConfig | None:
    """Сопоставить длинное имя суда из карточки 7kas с одним из наших ХМАО-судов.

    На 7kas суд 1-й инстанции пишется в развёрнутой форме, например:
        «Урайский городской суд Ханты-Мансийского автономного округа-Югры»
    Внутри проекта мы храним короткие имена («Урайский городской суд»). Эта
    функция ищет короткое имя как подстроку в длинном.

    Особый случай: «Суд Ханты-Мансийского автономного округа - Югры» —
    окружной суд, иногда служит 1-й инстанцией для отдельных категорий.
    Возвращает APPEAL_COURT (это та же сущность по domain).

    None — если суд не из ХМАО (фильтр на уровне поиска).
    """
    if not long_court_name:
        return None
    name_norm = long_court_name.strip().lower()
    # Окружной суд ХМАО-Югры — может быть 1-й инстанцией для админ. дел и т.п.
    if "суд ханты-мансийского автономного округа" in name_norm:
        # Отсекаем районные/городские, у них суффикс «округа-Югры» в конце,
        # а тут именно «Суд ХМАО» в начале (без префикса города/района).
        if not any(
            kw in name_norm
            for kw in ("городской", "районный", "межрайонный", "мировой")
        ):
            return APPEAL_COURT
    # Перебираем 20 районных/городских судов — ищем короткое имя подстрокой.
    # Дедуп по domain: Покачи дублирует Нижневартовский районный (один domain).
    for cfg in FIRST_INSTANCE_COURTS:
        short = cfg.name.lower()
        # Покачи: name содержит круглые скобки, его не матчим как «Нижневартовский
        # районный суд» внутри длинной формы — он отделён скобками.
        if "(" in short:
            continue
        if short in name_norm:
            return cfg
    return None
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
# Кэш LLM-пересказов мотивировок: {sha1(act_text)[:16]: {summary, model,
# stage, generated_at}}. Хранится отдельно от .digested_acts (тот — set
# номеров дел, а здесь — мапа hash→текст). Кэш переживает --replay-last
# и повторные прогоны: один и тот же act_text не пересказываем дважды.
ACT_SUMMARIES_PATH = os.environ.get(
    "ACT_SUMMARIES_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", ".act_summaries.json")
)
# Снимок контекста последнего дайджеста — сохраняется перед отправкой
# в Telegram и используется режимом --replay-last для повторной генерации
# (например, чтобы переиграть с другой версией промпта).
LAST_DIGEST_CONTEXT_PATH = os.environ.get(
    "LAST_DIGEST_CONTEXT_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "last_digest_context.json")
)
# Готовый текст последнего дайджеста (HTML) — сохраняется после успешной
# отправки в Telegram, фронт читает этот файл и показывает свёрнутый блок
# «Последний дайджест» в дашборде.
LAST_DIGEST_PATH = os.environ.get(
    "LAST_DIGEST_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "last_digest.json")
)
# Журнал последней push-рассылки: какие payload'ы ушли каждой подписке.
# Используется админкой подписчиков для отладки персональной фильтрации
# (видеть, какой именно вариант — personal/general/skip — получила каждая).
LAST_PERSONAL_PUSHES_PATH = os.environ.get(
    "LAST_PERSONAL_PUSHES_PATH",
    os.path.join(os.path.dirname(CSV_PATH) or "data", "last_personal_pushes.json")
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
# Кассация (стадия cassation, парсер 7kas.sudrf.ru):
CASSATION_ACT_ARCHIVE_DAYS = 30      # 30 дней после публикации опред. → архив.
CASSATION_NO_ACT_PUBLISH_DAYS = 45   # 45 дней от даты вынесения опред. без
                                     # публикации текста → архив без акта.
# Legacy: CSV-ветка архивации (apelljatsiя в CSV) ещё использует старое
# 30-дневное окно от «Даты события». Будет удалена вместе с CSV-веткой.
LEGACY_CSV_ARCHIVE_DAYS = 30
REQUEST_DELAY = (2, 3)  # Задержка между запросами к суду (сек)
FETCH_MAX_RETRIES = 3   # Кол-во попыток загрузки страницы
DASHBOARD_URL = "https://selivanovas.github.io/dashboard/sberbank_dashboard.html"

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Web Push (PWA-уведомления)
PUSH_WORKER_URL = os.environ.get("PUSH_WORKER_URL", "").rstrip("/")
PUSH_SECRET = os.environ.get("PUSH_SECRET", "")
# Приватный VAPID-ключ в PEM-формате; хранится только в GitHub Secrets.
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "")

# Переключатель провайдера LLM: "claude" (по умолчанию) или "gigachat".
# Задаётся в workflow digest_only_gigachat.yml для отдельного прогона
# дайджеста через GigaChat. Основной мониторинг (update_cases.yml) остаётся
# на Claude и ничего не знает про этот флаг.
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "claude").strip().lower()

# Откат к старой архитектуре дайджеста (полный LLM-вызов с большим контекстом).
# По умолчанию используется гибридный путь: программный рендер
# (generate_template_digest) + LLM-микро-вызов только на пересказ
# мотивировок судебных актов (summarize_act_motivation). Флаг
# `DIGEST_FULL_LLM=1` возвращает старое поведение: ровно тот HTML,
# который выдавал Claude/GigaChat одним вызовом. Используется как
# escape hatch на случай регресса стилистики или необходимости A/B.
DIGEST_FULL_LLM = (
    os.environ.get("DIGEST_FULL_LLM", "").strip().lower() in ("1", "true", "yes")
)

# Включение LLM-полировщика готового HTML (вариант C1 итерации 2).
# Программа собирает черновик через generate_template_digest + пересказы
# актов; при `DIGEST_POLISH=1` черновик уходит в polish_digest_html, где
# LLM делает косметические правки (капитализация, жирные даты, склонения,
# сокращение длинных категорий). Валидатор проверяет контракт <a><b>NUM</b></a>;
# при провале — откат к черновику. По умолчанию выключен — для безопасности.
DIGEST_POLISH = (
    os.environ.get("DIGEST_POLISH", "").strip().lower() in ("1", "true", "yes")
)
GIGACHAT_AUTH_KEY = os.environ.get("GIGACHAT_AUTH_KEY", "")
GIGACHAT_SCOPE = os.environ.get("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")
GIGACHAT_MODEL = os.environ.get("GIGACHAT_MODEL", "GigaChat")
GIGACHAT_OAUTH_URL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
GIGACHAT_API_URL = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"

# Лимит Telegram на одно сообщение
TELEGRAM_MSG_LIMIT = 4096
# Целевой лимит длины дайджеста (передаётся в промпт). Должен быть ЗАМЕТНО
# больше реального объёма — иначе Haiku 4.5 в режиме «экономии» сворачивает
# дела в одну строку и выкидывает события, чтобы уложиться. На выходе
# truncate_html_message(text, TELEGRAM_MSG_LIMIT * 2) = 8192, а Telegram
# split_message режет на 2 сообщения по 4096, так что фактически лимита
# особо и нет — нет смысла зажимать LLM.
DIGEST_CHAR_LIMIT = 12000

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


def _has_held_prior_event(
    events: list,
    new_hearing_dt: datetime | None,
    text_predicate,
) -> bool:
    """Общая логика обхода истории движения дела: есть ли событие,
    удовлетворяющее `text_predicate(text) -> bool`, которое уже прошло
    (date < today) и приходится не на ту же дату, что и `new_hearing_dt`.

    Если в истории есть маркер «рассмотрение с начала», цикл считается
    сброшенным — события до последнего такого reset'а игнорируются."""
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
        if not text_predicate(e.get("text") or ""):
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


def _has_held_prior_hearing(events: list, new_hearing_dt: datetime | None) -> bool:
    """Есть ли в истории движения дела реально прошедшее **судебное
    заседание** (regular или предварительное), отличное от нового
    назначения. Нужен для отличия настоящего переноса заседания."""
    return _has_held_prior_event(
        events, new_hearing_dt,
        lambda t: "судебное заседани" in t.lower(),
    )


def _has_held_prior_session(events: list, new_hearing_dt: datetime | None) -> bool:
    """Есть ли в истории ЛЮБОЕ прошедшее сессионное событие — судебное
    заседание, предварительное, подготовка дела (собеседование), беседа.
    Нужно, чтобы отличить настоящее «первое заседание» (когда ничего ещё
    не было) от перехода «подготовка → судебное заседание».

    Используется `_SESSION_START_RX` (строгая привязка к началу строки),
    а не `_HEARING_MARKERS_RX`, чтобы не путать реальные сессии с
    бюрократическими «определениями о подготовке дела»."""
    return _has_held_prior_event(
        events, new_hearing_dt,
        lambda t: bool(_SESSION_START_RX.search(t)),
    )


_RESTART_RE = re.compile(r"рассмотрени\S*\s+дела\s+начато\s+с\s+начала", re.I)

# Маркер «настоящего» session-события движения дела: текст ДОЛЖЕН
# начинаться с одного из заголовков ГАС «Правосудие». Это отличает
# реальное заседание/собеседование от бюрократических записей вроде
# «Вынесено определение о подготовке дела к судебному разбирательству»
# (тоже содержит «подготовке дела», но это решение, а не сессия).
# Семантически совпадает с classify_hearing_type, но в виде regex.
_SESSION_START_RX = re.compile(
    r"^\s*(судебное\s+заседани"
    r"|предварительн\w*\s+(?:судебн\w*\s+)?заседани"
    r"|подготовк\w*\s+дела"
    r"|собеседовани"
    r"|беседа\b)",
    re.IGNORECASE,
)
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
# 1-я инст.: помимо цифр-префикса (2-X/Y) допускаем буквенные префиксы —
# «М-» (материалы: иск подан, но ещё не зарегистрирован гражданским 2-XXX).
# Без них пропадает видимость свежепоступивших исков против Сбера.
_FI_CASE_NUM_RE = re.compile(r'(?:[А-ЯA-Z]+|\d+)-\d+/\d{4}')
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


def _load_act_summaries() -> dict:
    """Загрузить кэш LLM-пересказов мотивировок: {hash: {summary, ...}}."""
    if not os.path.exists(ACT_SUMMARIES_PATH):
        return {}
    try:
        with open(ACT_SUMMARIES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError) as e:
        log.warning(f"Не удалось прочитать {ACT_SUMMARIES_PATH}: {e}")
        return {}


def _save_act_summaries(cache: dict) -> None:
    """Сохранить кэш пересказов атомарно (tmp + replace)."""
    os.makedirs(os.path.dirname(ACT_SUMMARIES_PATH) or ".", exist_ok=True)
    tmp = ACT_SUMMARIES_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.replace(tmp, ACT_SUMMARIES_PATH)


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
#   cassation         — карточка найдена на 7kas, парсим до публикации акта.
#   awaiting_relink   — кассация отменила и направила на новое рассмотрение
#                       (1-я или апел.); ждём, что соответствующий парсер
#                       подцепит дело по номеру (бессрочно).
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
    направлению в кассационный суд.
    Переход cassation_pending → cassation делает link_cassation_cases
    при появлении карточки на 7kas — здесь не трогаем.
    Переход cassation → awaiting_relink при `outcome == cassation_remanded`
    (отменено и направлено на новое); дело ждёт появления новой карточки
    в нижестоящей инстанции."""
    stage = case.get("current_stage")
    fi = case.get("first_instance") or {}
    ap = case.get("appeal") or {}
    cs = case.get("cassation") or {}
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

    if stage == "cassation_pending":
        return None  # переход в cassation — задача link_cassation_cases

    if stage == "cassation":
        # Отменено и направлено на новое — переходим в awaiting_relink (ждём
        # появления новой карточки в нижестоящей инстанции). Архивации нет:
        # это re-open того же дела на втором круге.
        if cs.get("outcome") == "cassation_remanded":
            case["current_stage"] = "awaiting_relink"
            return "cassation"
        return None

    if stage == "awaiting_relink":
        return None  # переход обратно в first_instance/appeal — задача link_cases

    return None


def is_case_archived(case: dict) -> bool:
    """Унифицированная архивная проверка по стадии:
    - first_instance: «Решено» + 45 дней от hearing_date без апел. жалобы.
    - awaiting_appeal: никогда (ждём бессрочно, пока апел. карточка не найдётся).
    - appeal: никогда (переход в cassation_watch делает advance_case_stage).
    - cassation_watch: >120 дней от апел. hearing_date без касс. жалобы.
    - cassation_pending: никогда (ждём парсер кассации).
    - cassation: финальный исход (не remanded) + 30 дней после публикации акта,
      ИЛИ 45 дней от decision_date без публикации акта → архив.
    - awaiting_relink: никогда (ждём появления карточки в нижестоящей инст.).
    Остальные (legacy «first_instance» без current_stage, «appeal» без JSON
    данных) — false, не трогаем."""
    stage = case.get("current_stage")
    now = datetime.now()
    fi = case.get("first_instance") or {}
    ap = case.get("appeal") or {}
    cs = case.get("cassation") or {}

    if stage == "first_instance":
        if fi.get("appeal_filed_date"):
            return False
        if fi.get("status", "").strip() != "Решено":
            return False
        hearing = parse_date(fi.get("hearing_date") or "")
        if hearing and (now - hearing).days > FI_ARCHIVE_DAYS:
            return True
        return False

    if stage in ("awaiting_appeal", "appeal", "cassation_pending", "awaiting_relink"):
        return False

    if stage == "cassation_watch":
        ap_hearing = parse_date(ap.get("hearing_date") or "")
        if ap_hearing and (now - ap_hearing).days > CASSATION_WATCH_DAYS:
            return True
        return False

    if stage == "cassation":
        # Финальные исходы (не remanded) → можно архивировать.
        outcome = cs.get("outcome") or ""
        if outcome == "cassation_remanded":
            return False  # ждём awaiting_relink, advance_case_stage переведёт.
        if outcome and outcome != "cassation_other":
            # Опубликован акт: 30 дней после act_date → архив.
            act_d = parse_date(cs.get("act_date") or "")
            if act_d and (now - act_d).days > CASSATION_ACT_ARCHIVE_DAYS:
                return True
            # Акт не опубликован, но определение вынесено: 45 дней от
            # decision_date без публикации → архив без акта.
            dec_d = parse_date(cs.get("decision_date") or "")
            if (dec_d and not cs.get("act_published")
                    and (now - dec_d).days > CASSATION_NO_ACT_PUBLISH_DAYS):
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


def _bank_in_parties(plaintiff: str, defendant: str) -> bool:
    """True если «Сбербанк» явно упомянут в любой из сторон.

    Используется для правила БАНК В ХВОСТЕ: когда банк уже виден в сторонах,
    хвост «банк — Истец/Ответчик» в строке дайджеста избыточен. Хвост нужен
    ТОЛЬКО для редкого случая «банк = Третье лицо» (в сторонах не фигурирует).
    """
    s = ((plaintiff or "") + " " + (defendant or "")).lower()
    return "сбербанк" in s


def _section_break(block: list[str]) -> None:
    """Вставить визуальный разделитель «⸻» перед следующей секцией.

    Ничего не делает для пустого блока — у самой первой секции разделитель
    не нужен. Иначе добавляет: пустую строку, строку с `⸻`, ещё одну
    пустую строку. Так Telegram и PWA рисуют видимую границу между
    подсекциями (📥 Новые → 📅 Изменения → 🔁 Отложенные → ⚖️ Вынесенные …).
    """
    if not block:
        return
    block.append("")
    block.append("⸻")
    block.append("")


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


# ── Smart-skip парсинга ─────────────────────────────────────────────────────
# Маркеры из текста последнего события, при которых известна дата следующей
# активности и парсинг до неё бессмысленен. Синхронизированы с фронтовой
# логикой nextDateLabel в app.js:272-298.
_HEARING_MARKERS_RX = re.compile(
    r"(судебное\s+заседани|предварительн\w*\s+(?:судебн\w*\s+)?заседани|"
    r"подготовк\w*\s+дела|собеседовани|^\s*беседа\b)",
    re.IGNORECASE,
)
_SUSPENDED_RX = re.compile(r"без\s+движения", re.IGNORECASE)
_DATE_DDMMYYYY_RX = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b")


def get_next_planned_date(events: list[dict]) -> tuple[date | None, str]:
    """Из последнего события вытаскивает дату следующей запланированной
    активности. Возвращает (datetime.date, kind) либо (None, '').
    kind ∈ {'hearing', 'suspended'} — для skip-метрики в логе.

    hearing → берём event['date'] (карточка ГАС добавляет запись на дату
        заседания заранее).
    suspended → берём ПОСЛЕДНЮЮ дату DD.MM.YYYY из event['text'] (event.date —
        день вынесения определения, а срок исправления указан в тексте).
    """
    if not events:
        return None, ""
    last = events[-1] or {}
    text = (last.get("text") or "").strip()
    if not text:
        return None, ""
    text_l = text.lower()

    # «Без движения» проверяем первым: текст события заседания не содержит
    # этого маркера, а наоборот может содержать «оставлено без изменения»
    # (это про апел. результат, не наш случай — слово другое).
    if _SUSPENDED_RX.search(text_l):
        all_dates = _DATE_DDMMYYYY_RX.findall(text)
        if all_dates:
            d, m, y = all_dates[-1]
            try:
                return date(int(y), int(m), int(d)), "suspended"
            except ValueError:
                return None, ""
        return None, ""

    if _HEARING_MARKERS_RX.search(text_l):
        ev_date_raw = (last.get("date") or "").strip()
        m = _DATE_DDMMYYYY_RX.match(ev_date_raw)
        if m:
            try:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1))), "hearing"
            except ValueError:
                return None, ""
    return None, ""


def should_skip_case(
    case_dict: dict,
    today: date,
    force_parse_days: int = 21,
) -> tuple[bool, str]:
    """Решает, можно ли пропустить парсинг карточки.

    1. По current_stage выбирает блок first_instance / appeal.
    2. Force-parse: если last_checked_at нет или ≥ force_parse_days дней назад
       → не скипать (страховка от тихой отмены/переноса заседания).
    3. Иначе get_next_planned_date(events). Если planned >= today (включая
       сам день N) → skip. Парсим строго с N+1.
    """
    stage = case_dict.get("current_stage", "")
    if stage in ("first_instance", "cassation_watch"):
        block = case_dict.get("first_instance") or {}
    elif stage == "appeal":
        block = case_dict.get("appeal") or {}
    elif stage == "cassation":
        block = case_dict.get("cassation") or {}
    else:
        return False, ""

    last_checked_raw = block.get("last_checked_at", "")
    last_checked: date | None = None
    if last_checked_raw:
        try:
            last_checked = date.fromisoformat(last_checked_raw)
        except ValueError:
            last_checked = None
    if last_checked is None or (today - last_checked).days >= force_parse_days:
        return False, ""

    # Кассация: явное поле hearing_date в блоке (формат DD.MM.YYYY) — будущее
    # заседание известно без чтения events.
    if stage == "cassation":
        hd_raw = (block.get("hearing_date") or "").strip()
        m_hd = _DATE_DDMMYYYY_RX.match(hd_raw)
        if m_hd:
            try:
                hd = date(int(m_hd.group(3)), int(m_hd.group(2)), int(m_hd.group(1)))
                if hd >= today:
                    return True, f"future_hearing({hd.strftime('%d.%m.%Y')})"
            except ValueError:
                pass

    planned, kind = get_next_planned_date(block.get("events") or [])
    if planned and planned >= today:
        ymd = planned.strftime("%d.%m.%Y")
        if kind == "hearing":
            return True, f"future_hearing({ymd})"
        return True, f"suspended_until({ymd})"
    return False, ""


# Праздники/нерабочие дни РФ 2026-2027 (фиксированные даты + переносы).
# Перенесённые рабочие субботы намеренно не учитываем — если такая суббота
# попадёт, мы всё равно скипнем её как weekday>=5, что для cron безопасно.
_RU_HOLIDAYS: frozenset[date] = frozenset({
    # 2026
    date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 4),
    date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8),
    date(2026, 2, 23),
    date(2026, 3, 8), date(2026, 3, 9),
    date(2026, 5, 1),
    date(2026, 5, 9), date(2026, 5, 11),
    date(2026, 6, 12),
    date(2026, 11, 4),
    # 2027
    date(2027, 1, 1), date(2027, 1, 2), date(2027, 1, 3), date(2027, 1, 4),
    date(2027, 1, 5), date(2027, 1, 6), date(2027, 1, 7), date(2027, 1, 8),
    date(2027, 2, 23),
    date(2027, 3, 8),
    date(2027, 5, 1), date(2027, 5, 3),
    date(2027, 5, 9), date(2027, 5, 10),
    date(2027, 6, 12), date(2027, 6, 14),
    date(2027, 11, 4),
})


def is_russian_working_day(d: date) -> bool:
    """True, если d — рабочий день в РФ (не сб/вс и не праздник)."""
    if d.weekday() >= 5:
        return False
    return d not in _RU_HOLIDAYS


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
        if not _FI_CASE_NUM_RE.match(case_number_raw):
            continue

        # Номер может быть «2-5628/2026 ~ М-3298/2026» — берём первый.
        # Материалы (М-XXXX, 9-XXXX) тоже отслеживаем — юристу нужна
        # видимость по всем поступлениям против Сбербанка, не только по
        # основным гражданским делам.
        parts = [p.strip() for p in case_number_raw.split("~")]
        case_number = parts[0]
        # Хвостовой М-номер сохраняем отдельно — нужен для «промоушена»
        # ранее сохранённой М-записи в гражданское 2-XXX (когда материал
        # регистрируется и в выдаче появляется комбо-номер).
        material_number = next(
            (p for p in parts[1:] if p.startswith("М-")), ""
        )

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
            "material_number": material_number,
            "filing_date": date_received,
            "plaintiff": plaintiff,
            "defendant": defendant,
            "category": category,
            "court": court.name,
            "court_domain": court.domain,
            "court_delo_id": court.delo_id,
            "court_srv_num": court.srv_num,
            "judge": judge,
            "bank_role": role,
            "status": status,
            "result": result,
            "result_date": result_date,
            "link": link,
        })

    return cases


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
    r"Жалобу\s+подал\(а\):\s*(.+?)(?=Суд\s|Номер дела|$)", re.IGNORECASE
)
_CASS_FI_COURT_RE = re.compile(
    r"Суд\s*\([^)]*\)\s*первой\s+инстанции:\s*(.+?)(?=Номер дела|Категория|$)",
    re.IGNORECASE,
)
_CASS_FI_CASE_NUM_RE = re.compile(
    r"Номер дела в первой инстанции:\s*([^\s<]+)", re.IGNORECASE
)
# Внутренний номер 7kas (8Г-XXX/YYYY) в первой ячейке. Параллельный
# кассационный (88-XXX/YYYY) тут не всегда показан — берём из карточки.
_CASS_INTERNAL_NUM_RE = re.compile(r"8[ГГ]-\d+/\d{4}")


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
        m = _CASS_CATEGORY_RE.search(combined)
        if m:
            category = m.group(1).strip().rstrip("→ \xa0")
        m = _CASS_CASSATOR_RE.search(combined)
        if m:
            cassator = m.group(1).strip().rstrip(". \xa0")
        m = _CASS_FI_COURT_RE.search(combined)
        if m:
            fi_court_long = m.group(1).strip().rstrip(". \xa0")
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
            "filing_date": filing_date,
            "category": category,
            "cassator": cassator,
            "fi_court_long": fi_court_long,
            "fi_court_config": fi_court_config,
            "fi_case_number": fi_case_number,
            "result_text": result_text,
        })

    return found


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


# Hidden div на карточке 7kas, в котором размещается полный текст определения
# (вкладка «Судебные акты» переключается JS, но HTML отдаёт сразу). Пуст до
# публикации мотивированного определения.
_CASS_ACT_DIV_RE = re.compile(
    r"<div[^>]*id=['\"]cont_doc1['\"][^>]*>(.*?)"
    r"(?=<div[^>]*id=['\"](?:cont|next|footer|copyright)['\"]|</body>)",
    re.S | re.I,
)
# Заголовок «Дело №88-XXXX/YYYY» в начале текста определения.
_CASS_ACT_DELO_NUM_RE = re.compile(r"Дело\s*№\s*(88-?\d+/\d{4})", re.IGNORECASE)


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
    if "первой инстанции" in rfa:
        return "first_instance"
    if "апелляционн" in rfa:
        return "appeal"
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
        "participants": [],
        "sber_present": False,
        "bank_role": "",
        "act_text": "",
        "cassation_number": "",
        "act_published": False,
    }

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
            info["fi_case_number"] = val
        elif "дата решения первой" in key:
            info["fi_decision_date"] = val
        elif "судья (мировой судья) первой" in key or "судья первой" in key:
            if not info["fi_judge"]:
                info["fi_judge"] = val

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

    # Sber-presence + bank_role: проверяем вхождение SBER_PATTERNS в имена
    # участников. Если ни в одном имени Сбербанка нет (поиск иногда матчит
    # по случайному совпадению в тексте) — sber_present=False, дело отбросим.
    for p in info["participants"]:
        nm = p["name"].lower()
        if any(pat in nm for pat in SBER_PATTERNS):
            info["sber_present"] = True
            role = p["role"].upper()
            if "ИСТЕЦ" in role or "ЗАЯВИТЕЛЬ" in role:
                info["bank_role"] = "Истец"
            elif "ОТВЕТЧИК" in role:
                info["bank_role"] = "Ответчик"
            else:
                info["bank_role"] = "Третье лицо"
            break

    # ── Текст судебного акта (cont_doc1) ────────────────────────────────
    act_text, cass_num = _extract_cassation_act_text(html)
    info["act_text"] = act_text
    info["cassation_number"] = cass_num
    info["act_published"] = bool(act_text)

    return info


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
                       fi_changes: list[dict] | None = None,
                       *,
                       cass_changes: list[dict] | None = None,
                       cass_discovered: list[dict] | None = None) -> str:
    """Сводка-саммари одной строкой: +N новых, M событий, K решений, L актов."""
    parts = []
    if fi_new_cases:
        parts.append(f"+{len(fi_new_cases)} нов. 1 инст.")
    if new_cases:
        parts.append(f"+{len(new_cases)} нов. апелл.")
    if cass_discovered:
        parts.append(f"+{len(cass_discovered)} нов. касс.")
    # Мостик stage_transitions из дайджеста убран: дело и так попадает
    # в 5.1 «Новые дела апелляции», отдельная пометка юристу не нужна.
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
            if ("fi_hearing_new" in ch["type"]
                or "fi_hearing_next" in ch["type"]
                or "fi_hearing_postponed" in ch["type"])
        )
        fi_status = sum(1 for ch in fi_changes if "fi_status_change" in ch["type"])
        fi_acts = sum(1 for ch in fi_changes if "fi_act_published" in ch["type"])
        fi_finals = sum(1 for ch in fi_changes if "fi_final_event" in ch["type"])
        fi_motivs = sum(
            1 for ch in fi_changes if "fi_motivirovka_emitted" in ch["type"]
        )
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
        if fi_motivs:
            parts.append(f"{fi_motivs} мотивир. готов. 1 инст.")
        if fi_act_texts:
            parts.append(f"{fi_act_texts} мотивир. 1 инст.")
        if fi_status:
            parts.append(f"{fi_status} статус 1 инст.")
    if cass_changes:
        cass_acts = sum(1 for ch in cass_changes if "new_act" in ch["type"])
        cass_outcomes = sum(1 for ch in cass_changes if "outcome_change" in ch["type"])
        cass_reviews = sum(1 for ch in cass_changes if "review_result_change" in ch["type"])
        cass_news = sum(1 for ch in cass_changes if "new_cassation" in ch["type"])
        if cass_news:
            parts.append(f"{cass_news} касс. карточ.")
        if cass_reviews:
            parts.append(f"{cass_reviews} реш. изуч. жалоб")
        if cass_outcomes:
            parts.append(f"{cass_outcomes} касс. итог.")
        if cass_acts:
            parts.append(f"{cass_acts} касс. акт.")
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
            prev_stage = fi_case.get("current_stage")
            # Особый случай: awaiting_relink — кассация отменила и направила
            # на новое рассмотрение, пришла новая апел. карточка. Снимок старых
            # блоков идёт в history, открываем новый раунд апелляции.
            if prev_stage == "awaiting_relink":
                _snapshot_round_to_history(fi_case, "cassation_remanded_to_appeal")
                fi_case["appeal"] = appeal_case.get("appeal")
                fi_case["current_stage"] = "appeal"
            else:
                fi_case["appeal"] = appeal_case.get("appeal")
                # Обычно исходная стадия — awaiting_appeal (жалоба подана, ждём
                # карточку) или first_instance (карточка пришла раньше жалобы —
                # редко, но возможно). Из cassation_watch/cassation_pending
                # обратно в appeal не переводим: эти стадии уже прошли апелляцию.
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


def _snapshot_round_to_history(case: dict, reason: str) -> None:
    """Для дела в awaiting_relink: сохранить текущие first_instance/appeal/
    cassation блоки как «прошлый раунд» в case["history"][]. Сбросить эти
    блоки до пустого состояния. Увеличить case["round"] (по умолчанию 1 → 2).

    `reason` — короткая метка причины (e.g. «cassation_remanded_to_fi»).
    Используется при повторном открытии дела после отмены кассацией.
    """
    history = case.setdefault("history", [])
    snapshot = {
        "round": case.get("round", 1),
        "archived_at": date.today().isoformat(),
        "reason": reason,
        "first_instance": case.get("first_instance"),
        "appeal": case.get("appeal"),
        "cassation": case.get("cassation"),
    }
    history.append(snapshot)
    case["round"] = (case.get("round", 1) or 1) + 1
    case["first_instance"] = None
    case["appeal"] = None
    case["cassation"] = None


def relink_awaiting_relink_first_instance(
    cases: list[dict],
    fi_results_by_court: list,
) -> list[dict]:
    """Найти дела со стадией `awaiting_relink`, чьи номера снова появились в
    выдаче 1-й инстанции (касс. отменила и направила на новое рассмотрение).

    Args:
        cases: cases.json
        fi_results_by_court: список пар (CourtConfig, list[fi_search_result]).
            Каждый fi_search_result содержит case_number, court_*, link и т.д.

    Возвращает список (case, fi_result, court) для дел, где сработал re-link
    (для логирования / дайджеста). Сами cases мутируются на месте: history
    наполняется, текущий round инкрементируется, current_stage становится
    `first_instance`, first_instance блок инициализируется новой карточкой.
    """
    if not cases or not fi_results_by_court:
        return []
    awaiting = {
        (c.get("id") or "").strip(): c for c in cases
        if c.get("current_stage") == "awaiting_relink"
    }
    if not awaiting:
        return []
    # На вход приходит либо список пар (court, results), либо (для совместимости)
    # dict — нормализуем оба варианта в итерируемые пары.
    if isinstance(fi_results_by_court, dict):
        pairs = list(fi_results_by_court.items())
    else:
        pairs = list(fi_results_by_court)
    relinked: list[dict] = []
    for court, results in pairs:
        for fi in results:
            num = (fi.get("case_number") or "").strip()
            if not num or num not in awaiting:
                continue
            case = awaiting[num]
            _snapshot_round_to_history(case, "cassation_remanded_to_fi")
            case["current_stage"] = "first_instance"
            new_fi_block = _fi_search_to_json_case(fi)["first_instance"]
            case["first_instance"] = new_fi_block
            relinked.append({"case": case, "fi": fi, "court": court})
            log.info(
                f"  Re-link (awaiting_relink → first_instance): {num} "
                f"в {getattr(court, 'name', court)} (round={case.get('round', 1)})"
            )
            del awaiting[num]
    return relinked


def _cassation_card_to_block(info: dict) -> dict:
    """Сконвертировать результат parse_cassation_card в JSON-блок cassation
    (схема описана в плане; см. case["cassation"]). Включает производный
    outcome через classify_cassation_outcome и remanded_to."""
    outcome = classify_cassation_outcome(
        info.get("result_text", ""),
        info.get("result_for_appeal", ""),
        info.get("review_result", ""),
    )
    remanded_to = ""
    if outcome == "cassation_remanded":
        remanded_to = cassation_remanded_to(
            info.get("result_for_appeal", ""), info.get("act_text", "")
        )
    cassator_status = (info.get("cassator_status") or "").upper()
    appellant_is_bank = bool(
        info.get("cassator")
        and any(p in info["cassator"].lower() for p in SBER_PATTERNS)
    )
    link = ""
    # Карточка сама не отдаёт case_id/case_uid, поэтому link собирается выше
    # (в main_json) при обходе результатов поиска и кладётся в info["link"].
    if info.get("link"):
        link = info["link"]
    block = {
        "case_number": info.get("cassation_internal_number", ""),
        "cassation_number": info.get("cassation_number", ""),
        "court": CASSATION_COURT.name,
        "court_domain": CASSATION_COURT.domain,
        "judge": info.get("judge", ""),
        "filing_date": info.get("filing_date", ""),
        "fi_decision_date": info.get("fi_decision_date", ""),
        "act_kind": info.get("act_kind", ""),
        "category": info.get("category", ""),
        "judicial_uid": info.get("judicial_uid", ""),
        "appellant": info.get("cassator", ""),
        "appellant_is_bank": appellant_is_bank,
        "appellant_status": cassator_status,
        "review_result": info.get("review_result", ""),
        "hearing_date": info.get("hearing_date", ""),
        "hearing_time": info.get("hearing_time", ""),
        "decision_date": info.get("decision_date", ""),
        "result_text": info.get("result_text", ""),
        "result_for_appeal": info.get("result_for_appeal", ""),
        "act_published": bool(info.get("act_published")),
        "act_date": info.get("decision_date", "") if info.get("act_published") else "",
        "act_text": info.get("act_text", ""),
        "outcome": outcome,
        "remanded_to": remanded_to,
        "events": list(info.get("hearings") or []),
        "link": link,
        "last_checked_at": date.today().isoformat(),
        "discovered_via_cassation": False,
    }
    return block


def link_cassation_cases(
    cases: list[dict],
    cass_finds: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Связать найденные на 7kas дела с существующими в `cases.json` ИЛИ
    создать новые (discovery), если 1-инст. номера нет в БД.

    Args:
        cases: список JSON-объектов дел (формат cases.json).
        cass_finds: список dict — каждый = parse_cassation_card(card_html)
                    + дополненные поля `link` (case_id|case_uid) и
                    `cassation_internal_number` из результатов поиска.

    Возвращает (обновлённый список cases, список изменений для дайджеста,
    список новых дел discovered).

    Логика:
    - Для каждой находки берём fi_case_number (Номер дела в первой инст.).
    - Если case с таким id уже есть — мержим cassation блок, обновляем
      current_stage. Перевод стадии:
      - cassation_pending → cassation;
      - first_instance / awaiting_appeal / appeal / cassation_watch → cassation
        (это дело, которое мы прошляпили на промежуточных стадиях, но 7kas
        уже его рассматривает — догоняем).
      - awaiting_relink — если кассация во второй раз приехала по тому же
        делу, обновляем cassation блок и оставляем стадию (либо снова в cassation).
      - cassation — если уже была cassation, обновляем (новое заседание,
        акт опубликован и т.п.).
    - Если case нет — создаём новое со стадией `cassation` и стабом
      first_instance из карточки 7kas (court + case_number + judge +
      decision_date). discovered_via_cassation=True.
    """
    if not cass_finds:
        return cases, [], []

    fi_index: dict[str, int] = {}
    for i, c in enumerate(cases):
        cid = c.get("id", "")
        if cid:
            fi_index.setdefault(cid, i)
        fi = c.get("first_instance")
        if fi and fi.get("case_number"):
            fi_index.setdefault(fi["case_number"], i)

    cass_changes: list[dict] = []
    discovered: list[dict] = []

    for info in cass_finds:
        fi_num = (info.get("fi_case_number") or "").strip()
        if not fi_num:
            log.warning(
                f"7kas: пропуск без fi_case_number — "
                f"{info.get('cassation_internal_number') or '?'}"
            )
            continue
        cass_block = _cassation_card_to_block(info)
        idx = fi_index.get(fi_num)
        if idx is not None:
            case = cases[idx]
            old_cass = case.get("cassation") or {}
            old_act_published = bool(old_cass.get("act_published"))
            old_outcome = old_cass.get("outcome", "")
            old_review = old_cass.get("review_result", "")
            # Сохраняем discovered_via_cassation если он был выставлен ранее.
            cass_block["discovered_via_cassation"] = bool(
                old_cass.get("discovered_via_cassation")
            )
            case["cassation"] = cass_block
            # Обновим стадию.
            prev_stage = case.get("current_stage", "")
            if prev_stage in (
                "cassation_pending", "first_instance", "awaiting_appeal",
                "appeal", "cassation_watch", "awaiting_relink", "", None,
            ):
                case["current_stage"] = "cassation"
            # Зафиксируем изменения для дайджеста.
            change = {
                "case": fi_num,
                "cassation_internal_number": cass_block["case_number"],
                "type": [],
                "details": {
                    "stage_prev": prev_stage,
                    "stage_now": case["current_stage"],
                    "outcome": cass_block["outcome"],
                    "review_result": cass_block["review_result"],
                    "result_text": cass_block["result_text"],
                    "result_for_appeal": cass_block["result_for_appeal"],
                    "decision_date": cass_block["decision_date"],
                    "hearing_date": cass_block["hearing_date"],
                    "appellant": cass_block["appellant"],
                    "appellant_is_bank": cass_block["appellant_is_bank"],
                    "act_kind": cass_block["act_kind"],
                },
            }
            if not old_cass:
                change["type"].append("new_cassation")
            if cass_block["review_result"] and cass_block["review_result"] != old_review:
                change["type"].append("review_result_change")
            if cass_block["outcome"] and cass_block["outcome"] != old_outcome:
                change["type"].append("outcome_change")
            if cass_block["act_published"] and not old_act_published:
                change["type"].append("new_act")
                # Текст определения — уже в cass_block["act_text"]. В дайджест
                # пробрасываем мотивировочную часть.
                change["details"]["act_text"] = extract_motive_part(
                    cass_block["act_text"], 1800
                )
                change["details"]["act_date"] = cass_block["act_date"]
            if change["type"]:
                cass_changes.append(change)
            log.info(
                f"  7kas → {fi_num} ({cass_block['case_number']}): "
                f"{prev_stage}→{case['current_stage']}, outcome={cass_block['outcome'] or '—'}"
            )
        else:
            # Discovery: дела в cases.json нет. Создаём со стадией cassation
            # и стабом 1-й инст. (только то, что видит 7kas).
            cass_block["discovered_via_cassation"] = True
            fi_court_cfg = info.get("fi_court_config")
            fi_court_short = fi_court_cfg.name if fi_court_cfg else info.get("fi_court_long", "")
            fi_court_domain = fi_court_cfg.domain if fi_court_cfg else ""
            new_case = {
                "id": fi_num,
                "current_stage": "cassation",
                "plaintiff": "",
                "defendant": "",
                "category": cass_block["category"],
                "bank_role": info.get("bank_role", ""),
                "notes": "Найдено через парсер кассации (7kas)",
                "discovered_via_cassation": True,
                "first_instance": {
                    "case_number": fi_num,
                    "court": fi_court_short,
                    "court_domain": fi_court_domain,
                    "judge": info.get("fi_judge", ""),
                    "filing_date": "",
                    "status": "Решено",
                    "result": "",
                    "last_event": "",
                    "event_date": "",
                    "hearing_date": info.get("fi_decision_date", ""),
                    "hearing_time": "",
                    "link": "",
                    "act_published": False,
                    "act_date": "",
                    "act_text": "",
                    "events": [],
                },
                "appeal": None,
                "cassation": cass_block,
            }
            # Заполнить plaintiff/defendant из УЧАСТНИКОВ (если есть Сбербанк
            # как ответчик/истец, противоположную сторону тоже сохраним).
            for p in info.get("participants") or []:
                role = (p.get("role") or "").upper()
                name = p.get("name") or ""
                if "ИСТЕЦ" in role and not new_case["plaintiff"]:
                    new_case["plaintiff"] = name
                elif "ОТВЕТЧИК" in role and not new_case["defendant"]:
                    new_case["defendant"] = name
            cases.append(new_case)
            discovered.append(new_case)
            cass_changes.append({
                "case": fi_num,
                "cassation_internal_number": cass_block["case_number"],
                "type": ["discovered_in_cassation"],
                "details": {
                    "stage_now": "cassation",
                    "outcome": cass_block["outcome"],
                    "review_result": cass_block["review_result"],
                    "result_text": cass_block["result_text"],
                    "result_for_appeal": cass_block["result_for_appeal"],
                    "decision_date": cass_block["decision_date"],
                    "hearing_date": cass_block["hearing_date"],
                    "appellant": cass_block["appellant"],
                    "appellant_is_bank": cass_block["appellant_is_bank"],
                    "fi_court": fi_court_short,
                    "fi_case_number": fi_num,
                    "act_kind": cass_block["act_kind"],
                },
            })
            if cass_block["act_published"]:
                cass_changes[-1]["type"].append("new_act")
                cass_changes[-1]["details"]["act_text"] = extract_motive_part(
                    cass_block["act_text"], 1800
                )
                cass_changes[-1]["details"]["act_date"] = cass_block["act_date"]
            log.info(
                f"  7kas → DISCOVERY: {fi_num} ({cass_block['case_number']}, "
                f"{fi_court_short}), outcome={cass_block['outcome'] or '—'}"
            )

    if cass_changes:
        log.info(
            f"7kas: касс. изменений {len(cass_changes)}, "
            f"discovery новых дел {len(discovered)}"
        )
    return cases, cass_changes, discovered


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
) -> tuple[list[dict], list[dict], dict]:
    """
    Обновить карточки активных (не архивных) дел.

    json_appeal_by_num — опциональный словарь {номер_дела: appeal_dict} для
    параллельного обновления полей `events` / `last_event` / `event_date` в
    JSON-хранилище (иначе эти поля в `appeal` dict устаревают).

    skip_apel_nums — номера апел. дел, чей JSON-родитель уже не в стадии
    "appeal" (напр. cassation_watch). Такие карточки не парсим: апел. уже
    прошла, парсинг — это лишние запросы и ложные обновления event_date.

    Возвращает (обновлённые_дела, список_изменений, smart-skip-статы).
    """
    _digested_acts = load_digested_acts()
    changes = []
    today = date.today()
    skipped_future = 0
    skipped_suspended = 0
    force_parsed = 0
    parsed = 0
    eligible_total = 0  # активные не-архивные не-skip_apel — те, по кому решаем парсить или skip

    for case in cases:
        if is_archived(case):
            continue
        if skip_apel_nums and case.get("Номер дела", "").strip() in skip_apel_nums:
            continue
        eligible_total += 1

        # Smart-skip: если есть JSON-двойник апел-дела, проверяем известную
        # будущую дату. Для CSV-row без JSON-родителя — фолбэк, парсим как раньше.
        num = case.get("Номер дела", "").strip()
        ap_dict_skip = (json_appeal_by_num or {}).get(num)
        if ap_dict_skip is not None:
            shim = {"current_stage": "appeal", "appeal": ap_dict_skip}
            skip, reason = should_skip_case(shim, today)
            if skip:
                if reason.startswith("future_hearing"):
                    skipped_future += 1
                else:
                    skipped_suspended += 1
                log.debug(f"  skip {num}: {reason}")
                continue
            planned_fp, _kfp = get_next_planned_date(ap_dict_skip.get("events") or [])
            if planned_fp and planned_fp >= today:
                force_parsed += 1

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
        parsed += 1

        # Параллельно обновляем JSON-представление appeal-дела (если передано).
        # Старый список событий фиксируем для детектора «по правилам 1-й инст.».
        old_events_ap: list = []
        if json_appeal_by_num is not None:
            ap = json_appeal_by_num.get(case.get("Номер дела", "").strip())
            if ap is not None:
                ap["last_checked_at"] = today.isoformat()
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
                change["details"]["hearing_time"] = card_info.get("Время заседания", "")

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
            change["details"]["hearing_date"] = card_info.get("Дата заседания", "")
            change["details"]["act_date"] = card_info.get("Дата публикации акта", "")
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
                change["details"]["hearing_date"] = card_info.get("Дата заседания", "")
                change["details"]["act_date"] = card_info.get("Дата публикации акта", "")
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
    return cases, changes, {
        "skipped_future": skipped_future,
        "skipped_suspended": skipped_suspended,
        "force_parsed": force_parsed,
        "parsed": parsed,
        "total": eligible_total,
    }


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


# ── LLM-пересказ мотивировки судебного акта (микро-вызов) ───────────────────
# Используется программным рендером дайджеста (этап 3b плана миграции):
# вместо сырого 500-символьного excerpt'а мотивировки в секциях 5.5/3.6/касс.
# подставляем 1-2 фразы «почему» от LLM. Кэш по sha1(act_text), один пересказ
# = одна оплата за всё время; --replay-last повторно не платит.

_ACT_KIND_BY_STAGE = {
    "first_instance": "решение суда первой инстанции",
    "appeal": "апелляционное определение",
    "cassation": "кассационное определение",
}


def _build_act_summary_prompt(act_text: str, case_meta: dict) -> str:
    """Собрать prompt для LLM-пересказа мотивировки. Метаданные дела
    помогают модели не выдумывать стороны и итог."""
    stage = (case_meta.get("stage") or "").strip()
    kind = _ACT_KIND_BY_STAGE.get(stage, "судебный акт")
    plaintiff = (case_meta.get("plaintiff") or "").strip()
    defendant = (case_meta.get("defendant") or "").strip()
    bank_role = (case_meta.get("bank_role") or "").strip()
    verdict = (case_meta.get("verdict_label") or "").strip()
    category = (case_meta.get("category") or "").strip()

    meta_parts: list[str] = []
    if plaintiff or defendant:
        meta_parts.append(
            f"стороны: {plaintiff or '—'} (истец) / {defendant or '—'} (ответчик)"
        )
    if bank_role:
        meta_parts.append(f"роль банка: {bank_role}")
    if verdict:
        meta_parts.append(f"итог: {verdict}")
    if category:
        meta_parts.append(f"категория: {category}")
    meta_str = "; ".join(meta_parts)

    return (
        f"Ниже — текст мотивировочной части ({kind}). "
        + (f"Контекст — {meta_str}. " if meta_str else "")
        + "Сделай краткое резюме сути решения суда — одно или два коротких "
        "предложения. Только ОСНОВАНИЯ, по которым суд пришёл к итогу. "
        "Запрещено: фразы «суд указал», «было установлено», вода, цитаты "
        "целиком, эмодзи, HTML-теги, Markdown. Никаких префиксов «Кратко:», "
        "«Резюме:» — только сам пересказ.\n\n"
        f"ТЕКСТ АКТА:\n{act_text}"
    )


def _call_claude_simple(
    prompt: str, *, max_tokens: int = 400, temperature: float = 0.2
) -> str | None:
    """Минимальный вызов Anthropic API. Возвращает текст или None.

    Дублирует часть `generate_digest`, но с маленьким max_tokens и без
    post-обработки HTML — для пересказа мотивировки нужен plain text.
    """
    if not ANTHROPIC_API_KEY:
        return None
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
                "max_tokens": max_tokens,
                "temperature": temperature,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        text = "".join(
            block["text"] for block in data.get("content", [])
            if block.get("type") == "text"
        ).strip()
        return text or None
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        body = (e.response.text or "")[:500] if e.response is not None else ""
        log.warning(f"Claude API (summary) HTTP {status}: {body}")
        return None
    except (requests.RequestException, KeyError, ValueError,
            json.JSONDecodeError) as e:
        log.warning(f"Claude API (summary): {e}")
        return None


def _call_gigachat_simple(prompt: str) -> str | None:
    """Минимальный вызов GigaChat для пересказа акта — без жёсткого
    GIGACHAT_SYSTEM_PROMPT (он заточен под формат дайджеста). На любой
    ошибке — None, вызывающая сторона упадёт на сырой excerpt.
    """
    token = _gigachat_access_token()
    if not token:
        return None
    try:
        import urllib3  # noqa: PLC0415
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
                "max_tokens": 400,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
            verify=False,
        )
        r.raise_for_status()
        data = r.json()
        choices = data.get("choices") or []
        if not choices:
            return None
        text = (choices[0].get("message", {}) or {}).get("content", "").strip()
        return text or None
    except (requests.RequestException, KeyError, ValueError,
            json.JSONDecodeError) as e:
        log.warning(f"GigaChat (summary): {e}")
        return None


_SUMMARY_PREFIX_RE = re.compile(
    r"^\s*(?:кратко|резюме|итого|вкратце)\s*[:\-—]\s*",
    re.IGNORECASE,
)


def _clean_summary(text: str) -> str:
    """Убрать кавычки, шаблонные префиксы и лишние пробелы."""
    s = (text or "").strip().strip('"').strip("'").strip("«»").strip()
    s = _SUMMARY_PREFIX_RE.sub("", s)
    # Если модель начала с code-fence — срежем.
    if s.startswith("```"):
        nl = s.find("\n")
        if nl != -1:
            s = s[nl + 1:]
    if s.endswith("```"):
        s = s[:-3]
    return s.strip()


def summarize_act_motivation(
    act_text: str,
    *,
    case_meta: dict,
    use_cache: bool = True,
) -> str | None:
    """Сделать 1-2 фразы пересказа мотивировки судебного акта через LLM.

    Args:
      act_text: мотивировочная часть (из extract_motive_part или сырой текст
                акта). Слишком короткий (<100 символов) — не пересказываем.
      case_meta: {stage, bank_role, verdict_label, plaintiff, defendant,
                  category} — всё уже есть в change["details"] в точке
                  сборки дайджеста.
      use_cache: для тестов можно отключить.

    Returns:
      Plain-text строка без HTML/Markdown или None при любой ошибке/пустом
      ответе. Вызывающая сторона при None должна откатиться на сырой
      excerpt мотивировки.
    """
    act = (act_text or "").strip()
    if not act or len(act) < 100:
        return None

    key = hashlib.sha1(act.encode("utf-8")).hexdigest()[:16]
    cache = _load_act_summaries() if use_cache else {}
    if use_cache and key in cache:
        cached_summary = (cache[key] or {}).get("summary")
        if cached_summary:
            return cached_summary

    prompt = _build_act_summary_prompt(act, case_meta)
    if LLM_PROVIDER == "gigachat":
        raw = _call_gigachat_simple(prompt)
    else:
        raw = _call_claude_simple(prompt)
    if not raw:
        return None
    summary = _clean_summary(raw)
    if not summary:
        return None

    if use_cache:
        cache[key] = {
            "summary": summary,
            "model": _current_digest_model_name(),
            "stage": (case_meta.get("stage") or ""),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }
        try:
            _save_act_summaries(cache)
        except OSError as e:
            log.warning(f"Не удалось сохранить кэш пересказов: {e}")

    return summary


_DIGEST_POLISH_SYSTEM_PROMPT = (
    "Ты редактор Telegram-дайджеста о судебных делах для юриста ПАО Сбербанк.\n"
    "Тебе приходит ЧЕРНОВИК HTML, который собрала программа. Твоя задача — "
    "сделать ТОЛЬКО косметические правки, перечисленные ниже. Структура "
    "и набор секций должны остаться неизменными.\n\n"
    "ЧТО МОЖНО ПРАВИТЬ:\n"
    "1. Капитализация: первая буква строки события после эмодзи — заглавная "
    "(«🔁 заседание отложено» → «🔁 Заседание отложено»).\n"
    "2. <b>...</b> вокруг даты+времени в строках про назначение/отложение "
    "заседания («Заседание отложено на 09.06.2026 15:00» → «Заседание "
    "отложено на <b>09.06.2026 15:00</b>»).\n"
    "3. Дедуп между секциями: если одно дело одновременно в «Назначенные "
    "заседания» и «Вынесенные акты» — оставить ТОЛЬКО в «Вынесенные акты».\n"
    "4. Сокращение категорий: длинные цепочки «X →Y →Z →W» → последний "
    "хвост «W». Например, «Споры, связанные с наследственными отношениями "
    "→Споры, связанные с наследованием имущества →об ответственности "
    "наследников по долгам наследодателя» → «об ответственности наследников "
    "по долгам наследодателя».\n"
    "5. Склонение ролей в касс. жалобе: «от Ответчик X» → «от Ответчика X», "
    "«от Истец X» → «от Истца X», «от Третье лицо X» → «от третьего лица X».\n"
    "6. Дубль пробелов в инициалах: «Е. М.» → «Е.М.».\n\n"
    "ЖЁСТКИЕ ЗАПРЕТЫ:\n"
    "- НЕ удалять <a href>-ссылки и НЕ менять текст внутри "
    "<a><b>...</b></a> для номеров дел.\n"
    "- НЕ добавлять, НЕ удалять, НЕ переименовывать секции.\n"
    "- Использовать ТОЛЬКО теги <b>, <i>, <a href>. Запрещены <p>, "
    "<ul>, <li>, <h1>...<h6>, <br>, Markdown.\n"
    "- НЕ выдумывать события, даты, имена.\n"
    "- НЕ менять порядок дел внутри секций.\n"
    "- НЕ менять номера дел, итоги, суммы, даты — только косметика.\n\n"
    "Верни ТОЛЬКО исправленный HTML, без пояснений, без обёртки в "
    "```html...```."
)


_FORBIDDEN_TAGS_RE = re.compile(
    r"<\s*(p|ul|ol|li|h[1-6]|br|div|span|strong|em|table|tr|td|th)\b",
    re.IGNORECASE,
)


def _collect_case_numbers(
    new_cases: list[dict] | None = None,
    changes: list[dict] | None = None,
    fi_new_cases: list[dict] | None = None,
    fi_changes: list[dict] | None = None,
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
) -> set[str]:
    """Собрать множество номеров дел из всех source-структур дайджеста.
    Используется валидатором полировщика — каждый номер должен остаться
    в HTML после правки. Возвращает уникальные номера в исходном виде
    (без обрезки), стрипом по краям.
    """
    nums: set[str] = set()
    for c in new_cases or []:
        n = (c.get("Номер дела") or "").strip()
        if n:
            nums.add(n)
    for c in fi_new_cases or []:
        n = (c.get("id") or "").strip()
        if n:
            nums.add(n)
    for c in cass_discovered or []:
        # У cass_discovered «id» — номер 1-й инст., но в дайджесте они
        # рендерятся под касс. внутренним номером (case_number) из
        # cassation-блока. Берём тот, что виден в HTML.
        cass = c.get("cassation") or {}
        n = (cass.get("case_number") or c.get("id") or "").strip()
        if n:
            nums.add(n)
    for ch in changes or []:
        n = (ch.get("case") or "").strip()
        if n:
            nums.add(n)
    for ch in fi_changes or []:
        n = (ch.get("case") or "").strip()
        if n:
            nums.add(n)
    for ch in cass_changes or []:
        n = (ch.get("case") or "").strip()
        if n:
            nums.add(n)
    return nums


def _validate_polished_html(
    polished: str,
    *,
    draft: str,
    expected_case_numbers: set[str],
    max_length: int,
) -> tuple[bool, str]:
    """Проверить, что полированный HTML не нарушил контракт черновика.

    Возвращает (ok, reason). reason — короткое объяснение, что не так,
    для лога. Гарантии:
    - Длина <= max_length.
    - Каждый номер дела из expected_case_numbers есть в HTML.
    - Каждый номер обёрнут в <a ...><b>NUM</b></a> хотя бы один раз.
    - Нет запрещённых тегов (<p>, <ul>, <li>, <h*>, <br>, <div>, ...).
    - HTML непустой и содержит DASHBOARD_URL.
    """
    if not polished or len(polished.strip()) < 100:
        return False, "пустой или слишком короткий ответ"
    if len(polished) > max_length:
        return False, f"длина {len(polished)} > лимита {max_length}"
    forbidden = _FORBIDDEN_TAGS_RE.search(polished)
    if forbidden:
        return False, f"запрещённый тег: {forbidden.group(0)!r}"
    if DASHBOARD_URL not in polished:
        return False, "пропала ссылка на дашборд"
    # Проверяем наличие номеров дел и контракта <a><b>NUM</b></a>.
    case_link_re = re.compile(r"<a[^>]*><b>([^<]+)</b></a>")
    polished_anchors = {
        _bare_case_number(m.group(1))
        for m in case_link_re.finditer(polished)
    }
    polished_anchors.discard("")
    for num in expected_case_numbers:
        bare = _bare_case_number(num)
        if not bare:
            continue
        if num not in polished and bare not in polished:
            return False, f"пропал номер дела {num!r}"
        if bare not in polished_anchors:
            return False, f"номер {num!r} потерял обёртку <a><b>...</b></a>"
    return True, ""


def polish_digest_html(
    draft: str,
    *,
    expected_case_numbers: set[str],
) -> str:
    """Прогнать черновой HTML дайджеста через LLM-полировщик.

    Алгоритм:
    1. Шлём draft в Claude/GigaChat с DIGEST_POLISH_SYSTEM_PROMPT.
    2. Если ответ пустой / LLM упал → возвращаем draft.
    3. Прогоняем через _validate_polished_html.
    4. Если валидация не прошла → log warning + draft.
    5. Иначе → возвращаем полировку.

    Идея — никогда не сделать хуже черновика. Контракт <a><b>NUM</b></a>
    + DASHBOARD_URL гарантированы.
    """
    if not draft:
        return draft
    max_length = TELEGRAM_MSG_LIMIT * 2

    user_prompt = f"ЧЕРНОВИК HTML:\n\n{draft}"
    if LLM_PROVIDER == "gigachat":
        polished = _call_gigachat_polish(
            _DIGEST_POLISH_SYSTEM_PROMPT, user_prompt
        )
    else:
        polished = _call_claude_polish(
            _DIGEST_POLISH_SYSTEM_PROMPT, user_prompt
        )
    if not polished:
        log.info("Полировщик: пустой ответ LLM, использую черновик")
        return draft

    # Срезаем code-fence, если LLM всё-таки обернул в Markdown.
    polished = polished.strip()
    if polished.startswith("```"):
        nl = polished.find("\n")
        if nl != -1:
            polished = polished[nl + 1:]
    if polished.endswith("```"):
        polished = polished[:-3]
    polished = polished.strip()

    ok, reason = _validate_polished_html(
        polished,
        draft=draft,
        expected_case_numbers=expected_case_numbers,
        max_length=max_length,
    )
    if not ok:
        log.warning(f"Полировщик: валидация не прошла ({reason}), откат к черновику")
        return draft
    log.info(f"Полировщик: применена полировка ({len(draft)} → {len(polished)} chars)")
    return polished


def _call_claude_polish(system_prompt: str, user_prompt: str) -> str | None:
    """Вызов Anthropic API для полировщика. Отдельная функция (а не
    `_call_claude_simple`), потому что у полировщика есть system-prompt
    и существенно больший max_tokens (выходной HTML может быть длинным).
    """
    if not ANTHROPIC_API_KEY:
        return None
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
                "temperature": 0.1,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
            },
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        text = "".join(
            block["text"] for block in data.get("content", [])
            if block.get("type") == "text"
        ).strip()
        return text or None
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        body = (e.response.text or "")[:500] if e.response is not None else ""
        log.warning(f"Claude API (polish) HTTP {status}: {body}")
        return None
    except (requests.RequestException, KeyError, ValueError,
            json.JSONDecodeError) as e:
        log.warning(f"Claude API (polish): {e}")
        return None


def _call_gigachat_polish(system_prompt: str, user_prompt: str) -> str | None:
    """Вызов GigaChat для полировщика."""
    token = _gigachat_access_token()
    if not token:
        return None
    try:
        import urllib3  # noqa: PLC0415
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
                "temperature": 0.1,
                "max_tokens": 4096,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            },
            timeout=60,
            verify=False,
        )
        r.raise_for_status()
        data = r.json()
        choices = data.get("choices") or []
        if not choices:
            return None
        text = (choices[0].get("message", {}) or {}).get("content", "").strip()
        return text or None
    except (requests.RequestException, KeyError, ValueError,
            json.JSONDecodeError) as e:
        log.warning(f"GigaChat (polish): {e}")
        return None


def _render_act_summary_or_excerpt(
    act_text: str,
    case_meta: dict,
    *,
    summarizer,
    max_excerpt_len: int = 500,
) -> str:
    """Вернуть текст для строки «Мотивировка» в дайджесте.

    Если задан `summarizer` (callable вида `summarize_act_motivation`)
    и он вернул непустой пересказ — используем его. Иначе —
    обрезанный excerpt act_text (старое поведение шаблона).

    Возврат — строка, уже прошедшая `escape_html`, готовая к вставке
    в HTML под `<i>…</i>`. Пустая строка — если act_text пуст.
    """
    text = (act_text or "").strip()
    if not text:
        return ""
    if summarizer is not None:
        try:
            summary = summarizer(text, case_meta=case_meta)
        except Exception as e:
            log.warning(f"act_summarizer упал: {e}")
            summary = None
        if summary:
            return escape_html(summary)
    if len(text) > max_excerpt_len:
        text = text[:max_excerpt_len].rstrip() + "…"
    return escape_html(text)


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
    total_active_cassation: int = 0,
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
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
        "total_active_cassation": total_active_cassation,
        "cass_changes": cass_changes or [],
        "cass_discovered": cass_discovered or [],
    }
    try:
        save_json(payload, LAST_DIGEST_CONTEXT_PATH)
        log.info(f"Контекст дайджеста сохранён: {LAST_DIGEST_CONTEXT_PATH}")
    except Exception as exc:
        # Сохранение контекста — вспомогательная операция, не должна ронять
        # основной прогон. Ошибку залогируем и поедем дальше.
        log.warning(f"Не удалось сохранить контекст дайджеста: {exc}")


def save_last_digest(html: str, summary: str = "", *, is_empty: bool = False) -> None:
    """Сохранить готовый HTML дайджеста в LAST_DIGEST_PATH.

    Фронт читает этот файл, чтобы показать блок «Последний дайджест»
    в дашборде. Вызывается после успешной отправки в Telegram.

    `is_empty=True` — дайджест-заглушка (изменений не было). Используется
    `load_last_meaningful_digest()`, чтобы не цитировать «пустой» дайджест
    в качестве «предыдущего» в следующий тихий день.
    """
    if not html:
        return
    payload = {
        "version": 1,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "summary": summary or "",
        "html": html,
        "is_empty": bool(is_empty),
    }
    try:
        save_json(payload, LAST_DIGEST_PATH)
        log.info(f"Дайджест сохранён для фронта: {LAST_DIGEST_PATH}")
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

def _extract_case_paragraphs_from_digest(html: str, case_id: str) -> str:
    """Из HTML дайджеста вернуть склейку абзацев, в которых первый
    `<a><b>НОМЕР</b></a>` соответствует `case_id` (после нормализации
    `_bare_case_number`). Пустую строку — если ничего не нашлось."""
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
            stripped = para.strip()
            if stripped:
                out.append(stripped)
    return "\n\n".join(out)


def _current_digest_model_name() -> str:
    """Имя модели, которой только что генерили дайджест — для метки
    `act_analysis.model`. Совпадает с тем, что реально использовалось в
    `generate_digest()`."""
    if LLM_PROVIDER == "gigachat":
        return f"gigachat:{GIGACHAT_MODEL}"
    return "claude-haiku-4-5-20251001"


def attach_act_analyses(
    cases: list[dict],
    digest_html: str,
    *,
    all_changes: list[dict] | None = None,
    is_empty: bool = False,
) -> int:
    """Записать LLM-разбор опубликованного акта в `cases.json`.

    Для каждого `change`, у которого тип содержит `new_act` (апелляция)
    или `fi_act_text_published` (1-я инст.), вырезает из `digest_html`
    относящийся к делу абзац(ы) и кладёт в
    `case[<stage>]["act_analysis"] = {html, source, act_date, generated_at, model}`.

    Если в дайджесте абзац не нашёлся — fallback: HTML-обёрнутая
    мотивировка из `change["details"]["act_text"]` с пометкой
    `source: "raw_act"`. Если и её нет — поле просто не пишем.

    Поле перезаписывается ТОЛЬКО для дел с новым событием в этом прогоне;
    у остальных дел `act_analysis` сохраняется с прошлых прогонов и
    переживает любое количество последующих дайджестов. Идемпотентно:
    при повторном прогоне на тех же данных `generated_at` не обновляется.

    Возвращает кол-во дел, у которых поле реально изменилось.
    """
    if is_empty or not digest_html or not all_changes:
        return 0

    # Индекс «bare-номер дела → объект case»: матчим как по верхнему
    # `id`, так и по `first_instance.case_number` / `appeal.case_number` —
    # change["case"] для апелляции содержит апел. номер, для 1-й инст. —
    # номер 1-й инст., и оба должны находить нужное дело.
    by_id: dict[str, dict] = {}
    for c in cases:
        for raw in (
            c.get("id"),
            (c.get("first_instance") or {}).get("case_number"),
            (c.get("appeal") or {}).get("case_number"),
        ):
            bare = _bare_case_number(raw or "")
            if bare:
                by_id.setdefault(bare, c)

    model_name = _current_digest_model_name()
    now_iso = datetime.now().isoformat(timespec="seconds")
    updated = 0

    for ch in all_changes:
        types = set(ch.get("type") or [])
        if "new_act" in types:
            stage = "appeal"
        elif "fi_act_text_published" in types:
            stage = "first_instance"
        else:
            continue

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

        html_fragment = _extract_case_paragraphs_from_digest(digest_html, bare)
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


def load_last_meaningful_digest() -> dict | None:
    """Прочитать `last_digest.json` и вернуть payload последнего непустого
    дайджеста — или None, если такого нет.

    Используется в ветках «no-changes», чтобы добавить в сообщение блок
    «Предыдущий дайджест от …». Защита от self-reference: если payload
    помечен `is_empty=True` или html содержит маркеры «no-changes»,
    возвращается None.
    """
    try:
        if not os.path.exists(LAST_DIGEST_PATH):
            return None
        with open(LAST_DIGEST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        log.warning(f"Не удалось прочитать {LAST_DIGEST_PATH}: {exc}")
        return None
    if not isinstance(data, dict):
        return None
    if data.get("is_empty"):
        return None
    html = data.get("html") or ""
    if not html:
        return None
    # Совместимость со старыми payload без is_empty: считаем пустым по тексту.
    if "Всё спокойно, изменений нет" in html or "изменений не было" in html:
        return None
    return data


def _format_iso_date_ru(iso: str) -> str:
    """ISO datetime → 'dd.mm.yyyy'. На ошибках возвращает исходную строку."""
    if not iso:
        return ""
    try:
        return datetime.fromisoformat(iso).strftime("%d.%m.%Y")
    except Exception:
        return iso


def render_no_changes_digest(today: str, total_active_line: str) -> str:
    """Сообщение для дня без изменений.

    Если есть последний непустой дайджест — добавляем его ниже как
    «Предыдущий дайджест от …». Иначе — fallback на старый короткий вид
    со ссылкой на дашборд.
    """
    header = (
        f"✅ <b>Мониторинг дел Сбербанка — {today}</b>\n\n"
        f"За {today} изменений не было.\n"
        f"{total_active_line}"
    )
    prev = load_last_meaningful_digest()
    if not prev:
        return header + f'\n\n<a href="{DASHBOARD_URL}">📊 Дашборд</a>'
    prev_date = _format_iso_date_ru(prev.get("generated_at", ""))
    prev_html = prev.get("html", "").strip()
    sep = "━━━━━━━━━━━━━━━━━━"
    suffix = (
        f"\n\n{sep}\n"
        f"📋 <b>Предыдущий дайджест"
        + (f" от {prev_date}" if prev_date else "")
        + ":</b>\n\n"
        f"{prev_html}"
    )
    return header + suffix


def generate_digest(new_cases: list[dict], changes: list[dict], *,
                    cases: list[dict] | None = None,
                    fi_new_cases: list[dict] | None = None,
                    stage_transitions: list[dict] | None = None,
                    fi_changes: list[dict] | None = None,
                    total_active_appeal: int = 0,
                    total_active_fi: int = 0,
                    total_active_cassation: int = 0,
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

    total_active = total_active_appeal + total_active_fi + total_active_cassation

    # ── Гибридный путь (по умолчанию) ────────────────────────────────────
    # Программный рендер (generate_template_digest) + LLM-микро-вызов
    # только на пересказ мотивировок (summarize_act_motivation).
    # При DIGEST_POLISH=1 готовый HTML дополнительно проходит через
    # polish_digest_html (косметика + валидатор контракта).
    # Старый полный LLM-вызов остаётся за DIGEST_FULL_LLM=1 для отката.
    if not DIGEST_FULL_LLM:
        log.info(
            "LLM: гибрид (программный рендер + микро-LLM на пересказы актов"
            + (", + полировщик HTML" if DIGEST_POLISH else "")
            + ")"
        )
        draft = generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
            act_summarizer=summarize_act_motivation,
        )
        if DIGEST_POLISH:
            expected_nums = _collect_case_numbers(
                new_cases=new_cases, changes=changes,
                fi_new_cases=fi_new_cases, fi_changes=fi_changes,
                cass_changes=cass_changes, cass_discovered=cass_discovered,
            )
            return polish_digest_html(
                draft, expected_case_numbers=expected_nums
            )
        return draft

    # ── Старая ветка: полный LLM-вызов (за флагом DIGEST_FULL_LLM=1) ─────
    if LLM_PROVIDER == "gigachat":
        if not GIGACHAT_AUTH_KEY:
            log.warning("GIGACHAT_AUTH_KEY не задан, дайджест будет шаблонным")
            return generate_template_digest(
                new_cases, changes, cases=cases,
                fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
                fi_changes=fi_changes,
                total_active_appeal=total_active_appeal,
                total_active_fi=total_active_fi,
                total_active_cassation=total_active_cassation,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            )
    elif not ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY не задан, дайджест будет шаблонным")
        return generate_template_digest(
            new_cases, changes, cases=cases,
            fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
            fi_changes=fi_changes,
            total_active_appeal=total_active_appeal,
            total_active_fi=total_active_fi,
            total_active_cassation=total_active_cassation,
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
    context_parts = [f"СВОДКА: {summary}"]

    if new_cases:
        context_parts.append("\nНОВЫЕ ДЕЛА:")
        for c in new_cases:
            url = case_card_url(c)
            pl = shorten_party_name(c['Истец'], keep_fio_full=True)
            df = shorten_party_name(c['Ответчик'], keep_fio_full=True)
            line = (
                f"- {c['Номер дела']} (URL: {url}): "
                f"{pl} (истец) vs {df} (ответчик), "
                f"категория: {c['Категория']}, роль банка: {c['Роль банка']}, "
                f"суд 1 инст.: {shorten_court_name(c['Суд 1 инстанции'])}"
            )
            # Дату поступления выносим отдельным полем — в дайджесте она
            # уходит на самостоятельную строку «<b>дата</b> — 📥 поступило
            # в апел. суд» (см. пункт 5.1 промпта).
            filing = c.get('Дата поступления', '')
            if filing:
                line += f"\n  Дата поступления в апел. суд: {filing}"
            context_parts.append(line)

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

            context_parts.append(line)

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
                f"категория: {c.get('category', '')}, роль банка: {c.get('bank_role', '')}"
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
                        line += f"\n  Категория спора: {ch['category']}"
                elif t == "fi_hearing_postponed":
                    new_d = d.get("hearing_date", "")
                    new_t = d.get("hearing_time", "")
                    htype = d.get("hearing_type", "заседание")
                    new_p = f"{new_d}" + (f" {new_t}" if new_t else "")
                    # Старую дату НЕ передаём в текст контекста: юрист просит
                    # видеть только новую дату, без «⏪ старая → ⏩ новая».
                    line += f"\n  ОТЛОЖЕНО ({htype}): заседание отложено на {new_p}"
                    if ch.get("category"):
                        line += f"\n  Категория спора: {ch['category']}"
                elif t == "fi_status_change":
                    line += (f"\n  Статус: {d.get('old_status', '')} "
                             f"→ {d.get('new_status', '')}")
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
            if d.get("category"):
                line += f"\n  Категория спора: {d['category']}"
            if d.get("last_event"):
                line += f"\n  Последнее событие: {d['last_event']}"
            if d.get("act_text"):
                line += f"\n  МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ: {d['act_text']}"
            context_parts.append(line)

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
            line += (
                f"{shorten_party_name(c.get('plaintiff', ''), keep_fio_full=True)} (истец) vs "
                f"{shorten_party_name(c.get('defendant', ''), keep_fio_full=True)} (ответчик), "
            )
            line += f"роль банка: {c.get('bank_role', '?')}, "
            line += f"1-я инст. №: {c.get('id', '')}, "
            line += f"суд 1 инст.: {shorten_court_name(fi.get('court', '') or '?')}, "
            line += f"категория: {cass.get('category', '') or c.get('category', '') or '—'}, "
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
    if cass_changes:
        context_parts.append("\nКАССАЦИОННЫЕ СОБЫТИЯ (7kas):")
        for ch in cass_changes:
            d = ch.get("details") or {}
            if "discovered_in_cassation" in ch.get("type", []):
                continue  # уже в блоке «НОВЫЕ ДЕЛА КАССАЦИИ» выше
            line = (
                f"- 1-я инст. № {ch.get('case', '')} → касс. № "
                f"{ch.get('cassation_internal_number', '')}: "
                f"стадия {d.get('stage_prev', '?')} → {d.get('stage_now', '?')}"
            )
            if d.get("appellant"):
                line += f", заявитель: {d['appellant']} (банк_заявитель={d.get('appellant_is_bank', False)})"
            if d.get("review_result"):
                line += f"\n  Изучение жалобы: {d['review_result']}"
            if d.get("outcome"):
                line += f"\n  ИСХОД: {d['outcome']}"
            if d.get("result_text"):
                line += f"\n  Результат рассмотрения: {d['result_text']}"
            if d.get("result_for_appeal"):
                line += f"\n  В отношении апел. инст.: {d['result_for_appeal']}"
            if d.get("decision_date"):
                line += f"\n  Дата вынесения опред.: {d['decision_date']}"
            if d.get("hearing_date"):
                line += f"\n  Дата заседания: {d['hearing_date']}"
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

    prompt = f"""Ты — помощник юриста ПАО Сбербанк. Сформируй дайджест изменений по судебным делам судов ХМАО-Югры за {today}.

ИМЕНА: все наименования сторон в данных уже сокращены по правилам (ОПФ убрана, ФИО → инициалы, «в лице филиала…» удалено и т.п.). НЕ переписывай их и НЕ возвращай ОПФ обратно. В секциях «Новые дела» имена физлиц приходят полными — там оставляй как есть.

ДАТЫ: бери ровно из переданных полей данных. Не используй today() и не угадывай. Если у дела есть пометка «Заседание состоялось давно» — реальная дата уже в поле «Дата апелляционного определения», не пиши «сегодня».

ФОРМАТ: HTML для Telegram. Разрешены только теги <b>, <i>, <a href="URL">. Никакого Markdown (* _ ` [ ]). Спецсимволы &lt; &gt; &amp; экранируй.

СТРУКТУРА — два больших блока по инстанциям. Заголовок подсекции выводи только если есть данные. Большой блок (🏛 ПЕРВАЯ ИНСТАНЦИЯ / ⚖️ АПЕЛЛЯЦИЯ) выводи только если хотя бы одна его подсекция непуста.

СУД в скобках: поле {{суд}} в любой строке бери ДОСЛОВНО из записи того же дела в данных (поля «суд», «Суд 1 инстанции», «court»). Названия судов уже приходят сокращённо — например, «Сургутский гор. суд», «Нефтеюганский рай. суд». Выводи их как есть, НЕ расшифровывай «гор.» → «городской» и «рай.» → «районный». Если у дела поля с судом нет — не пиши суд в скобках вообще. ЗАПРЕЩЕНО переносить название суда из соседней записи. Для апелляционных дел (номер на `33-`) суд в скобках не пиши — все апелляции рассматриваются в Суде ХМАО-Югры, подсвечивать это не нужно. Значение «Суд 1 инстанции» уместно только в секциях про апелляционные дела, где прямо просят показать суд 1 инстанции (5.1).

ИНВАРИАНТ ИНСТАНЦИЙ (КРИТИЧНО): номер дела однозначно определяет, в какой большой блок оно попадает. Если номер начинается с `33-` (формат `33-XXXX/YYYY`) — это АПЕЛЛЯЦИОННОЕ дело, и оно идёт ТОЛЬКО в большой блок «⚖️ АПЕЛЛЯЦИЯ» (подсекции 5.1–5.5). Никогда не размещай номера на `33-` в подсекциях 3.1–3.6 блока «🏛 ПЕРВАЯ ИНСТАНЦИЯ». Все остальные номера 1-й инстанции (`2-…/YYYY`, `М-…/YYYY`, `9-…/YYYY` и т.п.) идут ТОЛЬКО в блок «🏛 ПЕРВАЯ ИНСТАНЦИЯ». Нарушение этого правила = критическая ошибка, дело не должно «всплыть не в той инстанции» ни при каких условиях.

ССЫЛКА НА КАРТОЧКУ ДЕЛА (КРИТИЧНО): в КАЖДОЙ строке, где упоминается номер дела (3.1–3.6, 4, 5.1–5.5), номер ОБЯЗАТЕЛЬНО оборачивается в `<a href="URL"><b>номер</b></a>`, где URL — поле «URL» того же дела из данных (это ссылка на карточку на сайте суда, sudrf.ru). Голый номер без `<a href>` = БРАК. Если URL в данных пустой — всё равно выведи `<b>номер</b>` (без ссылки), но это исключение, а не норма.

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
Строка «<b>Почему:</b>» — 4-5 коротких предложений с КОНКРЕТНЫМ обоснованием из мотивировки. Структура (порядок гибкий, но СУЩНОСТЬ обязательна): (а) какую конкретную норму применил суд — со ссылкой на статью/пункт/часть кодекса или закона (ст. 16 ЗоЗПП, п. 1 ст. 167 ГК и т.п.); (б) какой ключевой довод стороны принял или отклонил — и почему (например, «Банк не доказал возможность отказа потребителя», «истец не подтвердил факт оплаты», «довод о пропуске срока отклонён, т.к. течение срока прерывалось»); (в) какое фактическое обстоятельство стало решающим (что именно не доказала / подтвердила сторона); (г) опционально — практическое следствие для банка одной фразой (закрывает риск / создаёт прецедент / усиливает позицию по аналогичным спорам). Пример: «Суд сослался на ст. 16 ЗоЗПП — услуга навязана при выдаче ипотеки. Банк не доказал возможность отказа потребителя от страхования. Довод об отсутствии нарушения прав потребителя отклонён, поскольку условие включено в типовую форму договора. Для банка — риск массовых исков по аналогичным договорам.»
Имя судьи НЕ указывай.
ЗАПРЕЩЕНО:
- писать общие глаголы БЕЗ существа: «пересмотрел», «установил», «отклонил доводы», «согласился с выводами», «рассмотрел доводы», «проверил законность», «исследовал материалы дела» — если рядом нет ни конкретной нормы, ни конкретного факта/довода, фраза = ЗАПРЕЩЕНА. Лучше написать короче (3 предложения), чем 5 предложений воды;
- пересказывать ФАКТУРУ спора вместо МОТИВИРОВКИ итога (фактура — это строка 1, а не строка «Почему»);
- выдумывать ИТОГ или апеллянта — если поля нет в данных, соответствующую строку («<b>Итог:</b>» / «<b>Апеллянт:</b>») НЕ пиши, не подставляй «—», «0», «не указано», «не определено»;
- упоминать процедуру заседания: явку/неявку сторон и представителей, ходатайства о рассмотрении в отсутствие стороны, отложения, извещения, вручение корреспонденции, полномочия представителей, аудиопротоколирование;
- писать штампы «замечаний на протокол не поступало», «судебные извещения вручены», «извещены надлежащим образом», «дело рассмотрено в отсутствие надлежаще извещённого»;
- копировать «в удовлетворении требований отказать» / «требования подлежат удовлетворению» / «доводы апелляционной жалобы не влекут отмены решения» без указания, КАКУЮ норму суд применил и КАКОЙ довод принял/отклонил.

1. Заголовок: 📊 Дайджест судебных дел | Суды ХМАО-Югры | {today}
2. 📋 <b>Сводка</b> — отдельные строки, по одной на инстанцию (НЕ через «|» в одну строку). Между заголовком «📋 <b>Сводка</b>» и самими строками — ОДНА пустая строка (отступ). Между строками сводки пустой строки нет (все идут плотным блоком). Формат ДОСЛОВНО:
   <i>1 инст.:</i> X заседаний, Y решений, Z статусов
   <i>Апелл.:</i> +N дел, M актов, K отложений
   <i>Касс.:</i> +K дел, L событий
   Строки 1 инст. и Апелл. пиши ВСЕГДА (даже если «нет событий» — например: «<i>Апелл.:</i> нет событий»). Строку «<i>Касс.:</i>» пиши ТОЛЬКО если в данных есть «НОВЫЕ ДЕЛА КАССАЦИИ» или «КАССАЦИОННЫЕ СОБЫТИЯ» — иначе её не выводи вообще (без «нет событий»). УПОМИНАЙ ТОЛЬКО те события, которые реально будут выведены в блоках 3/4/5/6 ниже. Если событие дедуплицировано правилами (смена статуса свёрнута в 3.5, подача жалобы в 3.3 поглощает 3.2 и т.п.) — в сводке его НЕ считай. После сводки — одна пустая строка перед большим блоком 🏛 ПЕРВАЯ ИНСТАНЦИЯ.

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
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки между ними): {{стороны кратко}} | событие (подготовка дела / беседа / предварительное заседание / заседание / назначено первое заседание (дата и время не опубликованы) / статус X→Y / 📄 мотивированное решение изготовлено ДД.ММ, полный текст не опубликован / возвращение иска / в архив / рассмотрение с начала). КРИТИЧНО: фразу «📄 мотивированное решение изготовлено …, полный текст не опубликован» бери ДОСЛОВНО из строки «Мотивированное решение изготовлено …» во входных данных дела — это событие появляется, когда в карточке проставлена дата резолютивки, но полного текста (мотивировки) ещё нет. Если у того же дела в данных есть поле «МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ» — дело идёт ТОЛЬКО в 3.6 «Опубликованные тексты решений», в 3.2 эту строку НЕ дублируй.
          — Если в данных дела стоит фраза «Назначено первое заседание (дата и время не опубликованы)» — копируй её В строку 2 ДОСЛОВНО, НЕ выдумывай дату/время, НЕ добавляй префикс 📅 ДД.ММ.ГГГГ в строку 1. Это означает: на сайте суда дата заседания не опубликована, мы только зафиксировали факт назначения.
        • ОТЛОЖЕНИЕ ЗАСЕДАНИЯ (источник — поле «ОТЛОЖЕНО» во входных данных дела) — ТРИ строки, БЕЗ стрелочек, БЕЗ старой даты. Формат строго:
          – строка 1: <a href="URL"><b>номер</b></a> ({{суд}})  [БЕЗ даты впереди]
          – строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{стороны кратко}} | категория: {{категория из «Категория спора»}}
          – строка 3 (СРАЗУ под 2, БЕЗ пустой строки): 🔁 Заседание отложено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b>
          ЗАПРЕЩЕНО: писать «⏪», «⏩», «старая дата → новая дата», «перенесено с …», указывать дату, с которой перенесли. Берётся ТОЛЬКО новая дата (из строки «ОТЛОЖЕНО (…): заседание отложено на ДД.ММ.ГГГГ ЧЧ:ММ»). Если у дела рядом с «ОТЛОЖЕНО» есть другое событие (статус, акт) — оно НЕ идёт отдельной строкой; формат остаётся 3-строчным, ОТЛОЖЕНИЕ доминирует.
        • НАЗНАЧЕНИЕ ЗАСЕДАНИЯ ПОСЛЕ ПОДГОТОВКИ/СОБЕСЕДОВАНИЯ (источник — поле «НАЗНАЧЕНО» во входных данных дела) — ТРИ строки, аналогично отложению, но без слова «отложено». Это переход от подготовительного этапа к слушанию (НЕ «первое заседание» — собеседование уже было; и НЕ «отложение» — это смена этапа). Формат строго:
          – строка 1: <a href="URL"><b>номер</b></a> ({{суд}})  [БЕЗ даты впереди]
          – строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{стороны кратко}} | категория: {{категория из «Категория спора»}}
          – строка 3 (СРАЗУ под 2, БЕЗ пустой строки): 📅 Заседание назначено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b>
          ЗАПРЕЩЕНО: писать «первое заседание», «отложено», «перенесено». Берётся ТОЛЬКО новая дата из строки «НАЗНАЧЕНО (…): заседание назначено на ДД.ММ.ГГГГ ЧЧ:ММ».
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
          (хвост «банк — …» — по правилу БАНК В ХВОСТЕ; категорию бери ДОСЛОВНО из поля «категория», но если она длинная с цепочкой «→ → →» — оставь только последний/самый конкретный сегмент после последней стрелки)
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
   5.2. 🔁 <b>Отложенные заседания (N):</b> — ДВЕ строки на дело. КРИТИЧНО: строки 1 и 2 ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка — ТОЛЬКО между разными делами. Эта секция РЕДКАЯ и ВАЖНАЯ — никогда не выкидывай при нехватке места.
        • строка 1: 🔁 <a href="URL"><b>номер</b></a> — {{стороны кратко}} | категория: {{категория}}
        • строка 2 (СРАЗУ под строкой 1, БЕЗ пустой строки): Заседание отложено на <b>ДД.ММ.ГГГГ ЧЧ:ММ</b> (дата+время ОБЯЗАТЕЛЬНО в <b>…</b>, как в 1-й инстанции — иначе юрист не сразу видит дату; берётся ТОЛЬКО новая дата из строки «ОТЛОЖЕНО:» в данных, старую не указывай).
   5.3. 📅 <b>Назначенные заседания (N):</b> — ДВЕ строки на дело. КРИТИЧНО: строки 1 и 2 ОДНОГО дела идут ПОДРЯД, БЕЗ пустой строки между ними. Пустая строка — ТОЛЬКО между разными делами. Формат:
        • строка 1: <b>ДД.ММ.ГГГГ ЧЧ:ММ</b> — <a href="URL"><b>номер</b></a> (дата+время ОБЯЗАТЕЛЬНО в <b>…</b>; время БЕРЁТСЯ ОБЯЗАТЕЛЬНО, если в данных есть «Дата заседания: ДД.ММ.ГГГГ ЧЧ:ММ»; писать только дату — допустимо ТОЛЬКО когда времени в данных нет совсем).
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{стороны кратко}} | категория: {{категория}} (роль банка по правилу «банк в хвосте»).
        НЕ помещай сюда дела с пометкой «ОТЛОЖЕНО».
   5.4. ⚖️ <b>Вынесенные акты (N):</b> — резолютивная часть (выходит через 1-3 дня после заседания). Только дела с блоком ИТОГ. ТРИ строки на дело, между делами пустая строка. Формат — как в 5.2 «Отложенные заседания»: первая строка — номер + стороны, вторая — категория + банк-роль, третья — итог. Дату определения встраиваем в строку «Итог», чтобы строка 1 оставалась короткой и читаемой.

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
        • строка 3 (СРАЗУ под строкой 2, БЕЗ пустой строки): <b>Почему:</b> см. ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (выше). Если из одних сторон неочевидно, кто оспаривал решение и чего добивался (напр., «Сбербанк vs Фин. уполномоченный» — обе стороны институциональные), начни «Почему» с короткой фразы «<Роль апеллянта> <имя> оспаривал <что>…», чтобы читатель сразу понял направление жалобы.
        Применяются ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ (формат трёх строк, блок ЗАПРЕЩЕНО — см. выше).

ВАЖНО про 5.4 и 5.5: это РАЗНЫЕ события, разведённые во времени, но если в текущем дайджесте у одного дела есть И ИТОГ, И МОТИВИРОВОЧНАЯ ЧАСТЬ АКТА — выводи дело ТОЛЬКО в 5.5 «Опубликованные тексты актов» (там и ИТОГ из карточки, и мотивировка). В 5.4 такие дела НЕ дублируй. Раздельно дело пойдёт по секциям только когда события приходят в разные дайджесты (резолютивка сегодня, мотивировка через 14+ дней) — в этом случае каждая секция получает «свой» прогон.

ВАЖНО про 3.5 и 3.6: то же правило — если в текущем дайджесте у дела есть И поле «ИТОГ» из «ВЫНЕСЕНЫ РЕШЕНИЯ 1 ИНСТ.», И «МОТИВИРОВОЧНАЯ ЧАСТЬ РЕШЕНИЯ» — выводи ТОЛЬКО в 3.6, в 3.5 не дублируй. В разных прогонах дело распределяется по своим секциям естественным образом.

6. ⚖️🔬 <b>КАССАЦИЯ</b> — большой блок, выводится только если есть данные в секциях «НОВЫЕ ДЕЛА КАССАЦИИ» или «КАССАЦИОННЫЕ СОБЫТИЯ» в «Данные» ниже. Между этим большим блоком и предыдущим (⚖️ АПЕЛЛЯЦИЯ) — одна пустая строка, без «⸻». Внутри блока:
   6.1. 📥 <b>Новые касс. дела (N):</b> — дело впервые видно через 7kas (мы пропустили 1-ю инст./апел.). Источник — секция «НОВЫЕ ДЕЛА КАССАЦИИ» в данных. ТРИ строки на дело, между делами пустая строка, внутри одного дела пустых строк НЕТ. КРИТИЧНО: заголовок строки 1 — касс. внутренний номер (вид «8Г-…/YYYY») БЕЗ префикса «касс. №» — секция и так называется «Новые касс. дела». Номер 1-й инст. в эти три строки НЕ выносить.
        • строка 1: <a href="URL"><b>{{касс. номер}}</b></a> (URL берётся из поля URL карточки в данных, если есть; иначе просто <b>{{касс. номер}}</b>) — {{истец}} vs {{ответчик}}, банк — {{роль}} (хвост «банк — …» по правилу БАНК В ХВОСТЕ). ПРЕФИКС «касс. № » в строке 1 НЕ ставь — он избыточен.
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{суд 1 инст.}} | категория: {{категория спора}}. Категорию бери из поля «категория» в данных. Если категории нет / стоит «—» — выводи только «{{суд 1 инст.}}» без «| категория: …». Номер 1-й инст. и «заявитель» в эту строку НЕ помещай.
        • строка 3 (СРАЗУ под 2, БЕЗ пустой строки) — ТОЛЬКО если в данных есть поле «Дата поступления касс. жалобы»: <b>{{ДД.ММ.ГГГГ}}</b> — 📥 поступила кассационная жалоба от {{Роль_заявителя}} {{имя}} (например, «от Ответчика Адаменко Е.М.», «от Истца Сбербанка»). Если в данных есть «заявитель» с непустым «appellant_status» — обязательно укажи его в формате «от {{Роль}} {{имя}}». Если заявитель пуст — пиши просто «📥 поступила кассационная жалоба».
        КРИТИЧНО: дату поступления выноси ТОЛЬКО на строку 3. В строку 2 поле «поступление: {{дата}}» больше НЕ помещай. Если данных о дате нет — строку 3 не пиши, не подставляй today()/«—»/«не указано».
   6.2. 📑 <b>Касс. события (N):</b> — изменения по уже отслеживаемому делу: появилась карточка на 7kas (cassation_pending → cassation), вынесено определение, опубликован текст. Источник — секция «КАССАЦИОННЫЕ СОБЫТИЯ» в данных. КРИТИЧНО: строки одного дела идут ПОДРЯД, БЕЗ пустых строк между ними. Между делами — одна пустая строка.
        • строка 1: <a href="URL"><b>номер 1-й инст.</b></a> — касс. № <b>{{касс. номер}}</b> | стадия: {{stage_prev}} → {{stage_now}}.
        • строка 2 (СРАЗУ под 1, БЕЗ пустой строки): {{стороны кратко}} | категория: {{категория}}, банк — {{роль}} (хвост «банк — …» по правилу БАНК В ХВОСТЕ).
        • строка 3 (СРАЗУ под 2, БЕЗ пустой строки) — ТОЛЬКО если есть `outcome` или `result_text` в данных:
          <b>Итог:</b> {{дословно поле «Результат рассмотрения»}}{{, "В отношении апел.: " + поле «В отношении апел. инст.» если есть}}.
        • строка 4 (СРАЗУ под 3, БЕЗ пустой строки) — ТОЛЬКО если есть «МОТИВИРОВОЧНАЯ ЧАСТЬ ОПРЕДЕЛЕНИЯ»:
          <b>Почему:</b> 3-4 КОРОТКИХ предложения с конкретным обоснованием (см. ПРАВИЛА МОТИВИРОВОЧНЫХ СЕКЦИЙ — те же запреты, что для 3.6/5.5: никаких «рассмотрел доводы», «исследовал материалы», «без изменений» без причины — нужны нормы и факты).
        ИСХОД (`outcome`) переводи в человеческую формулировку: cassation_dismissed_no_transfer = «отказ в передаче»; cassation_upheld = «оставлено в силе»; cassation_modified = «изменено»; cassation_reversed = «отменено»; cassation_remanded = «отменено и направлено на новое»; cassation_terminated = «прекращено / отозвано»; cassation_other / пусто = пропусти строку «Итог».
        Дело с `appellant_is_bank=true` (Сбербанк подал жалобу) — выделяй: добавь в начало строки 1 эмодзи 🏦.

7. 📌 Итоговая строка: <b>В производстве: всего {total_active} (1 инст.: {total_active_fi} | апел.: {total_active_appeal} | касс.: {total_active_cassation})</b>. Используй ИМЕННО эти ЧЕТЫРЕ числа дословно — не считай, не угадывай, не округляй. Касс. — это дела на стадиях `cassation_pending` и `cassation` (жалоба ушла в кассац. суд / уже рассматривается на 7kas).
8. В конце: <a href="{DASHBOARD_URL}">📊 Дашборд</a> — обязательно всегда.

ОФОРМЛЕНИЕ: без маркеров списка («• », «- »); названия больших блоков и секций — <b>жирным</b>; номера дел — <b>жирным</b> внутри ссылок. РАЗДЕЛИТЕЛИ И ПУСТЫЕ СТРОКИ (обязательны, без них границы теряются):
(а) перед заголовком каждой подсекции 📥/📅/⚖️/📄/🔁/📨/⚠ ВНУТРИ одного большого блока — отдельная строка-разделитель «⸻» (ТОЛЬКО этот символ, без HTML-тегов и пробелов вокруг), окружённая пустыми строками: пустая строка → ⸻ → пустая строка → заголовок секции. Перед самой первой подсекцией большого блока (сразу после <b>🏛 ПЕРВАЯ ИНСТАНЦИЯ</b> или <b>⚖️ АПЕЛЛЯЦИЯ</b>) разделитель НЕ ставь — там и так понятно, где начало; ПОСЛЕ заголовка подсекции (📥 Новые иски (N): / 📅 Изменения (N): / 📄 Опубликованные… / 🔁 Отложенные… и т.п.) — ровно ОДНА пустая строка, потом первое дело;
(б) между РАЗНЫМИ делами в одной подсекции — ровно одна пустая строка, даже в однострочных подсекциях 3.3/3.5/5.1/5.4 (без «⸻»);
(б1) ВНУТРИ ОДНОГО ДЕЛА (когда у дела две или три строки — секции 3.2, 3.6, 5.1, 5.2, 5.3, 5.4, 5.5) пустая строка МЕЖДУ строками одного дела — ЗАПРЕЩЕНА. Все строки одного дела идут подряд, плотным блоком. Пустая строка появляется ТОЛЬКО когда начинается следующее дело;
(в) между большими блоками (🏛 ПЕРВАЯ ИНСТАНЦИЯ → ⚖️ АПЕЛЛЯЦИЯ) — ровно одна пустая строка, без «⸻» (граница и так заметна по жирному заголовку большого блока);
(г) после <b>🏛 ПЕРВАЯ ИНСТАНЦИЯ</b> и после <b>⚖️ АПЕЛЛЯЦИЯ</b> — ровно одна пустая строка перед первой подсекцией (отступ для дыхания).

СТИЛЬ: кратко, по-деловому, на русском. Без вступлений. Не дублируй информацию между секциями (за исключением 5.4↔5.5, см. выше).

ЛИМИТ: примерно {DIGEST_CHAR_LIMIT} символов — это БОЛЬШОЙ запас, фактический дайджест обычно в 2-3 раза короче. НЕ ЭКОНОМЬ место за счёт пропуска требуемых строк или событий: НИКОГДА не сворачивай дело из 3.1/5.1/6.1 в одну строку, если требуется 2-3; НИКОГДА не выкидывай события из 3.2 (включая «📄 мотивированное решение изготовлено …»), 3.5, 3.6, 5.x, 6.x — если событие есть в данных, оно ОБЯЗАНО появиться в дайджесте. Сокращать допустимо ТОЛЬКО мотивировочные секции 3.6/5.5 (тексты «Почему: …») и ТОЛЬКО при реальном переполнении лимита; всё остальное — формат, строки 2-3, заголовки, даты — выводи полностью. Секцию 🔁 «Отложенные заседания» — НЕ выкидывать никогда. Ссылка на дашборд — ВСЕГДА в конце.

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
                total_active_cassation=total_active_cassation,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            )
        text = _validate_digest_new_sections(text, fi_new_cases, new_cases)
        text = _ensure_appeal_new_case_full_layout(text, new_cases)
        text = _warn_misplaced_appeal_cases(text)
        text = _renumber_section_headers(text)
        text = _purge_3_6_without_act_text(text, fi_changes or [])
        text = _drop_zero_count_sections(text)
        text = _recount_summary_line(text)
        text = _normalize_section_spacing(text)
        text = _wrap_all_bare_case_numbers(text, url_by_num)
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
                total_active_cassation=total_active_cassation,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            )
        text = _validate_digest_new_sections(text, fi_new_cases, new_cases)
        text = _ensure_appeal_new_case_full_layout(text, new_cases)
        text = _warn_misplaced_appeal_cases(text)
        text = _renumber_section_headers(text)
        text = _purge_3_6_without_act_text(text, fi_changes or [])
        text = _drop_zero_count_sections(text)
        text = _recount_summary_line(text)
        text = _normalize_section_spacing(text)
        text = _wrap_all_bare_case_numbers(text, url_by_num)
        # До двух сообщений: лимит 2×4096; split_message в send_telegram разобьёт
        return truncate_html_message(text, TELEGRAM_MSG_LIMIT * 2)
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
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
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


def _bare_case_number(num: str) -> str:
    """«2-216/2026 (2-1156/2025;)» → «2-216/2026». Нужно потому, что поиск
    в судах возвращает только текущий номер, а в cases.json хранится полный
    с суффиксом переномерования."""
    s = (num or "").strip()
    if "(" in s:
        bare = s.split("(")[0].strip()
        return bare or s
    return s


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
_SUBSECTION_HEADERS_WITH_COUNT = [
    (re.compile(r'^(\s*📅\s*<b>\s*Изменения\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "1 инст./Изменения"),
    (re.compile(r'^(\s*📨\s*<b>\s*Поданы апелляционные жалобы\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "1 инст./Апел. жалобы"),
    (re.compile(r'^(\s*📨\s*<b>\s*Кассационные события\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "1 инст./Кассация"),
    (re.compile(r'^(\s*⚖️\s*<b>\s*Вынесенные решения\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "1 инст./Решения"),
    (re.compile(r'^(\s*📄\s*<b>\s*Опубликованные тексты решений\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "1 инст./Тексты решений"),
    (re.compile(r'^(\s*🔁\s*<b>\s*Отложенные заседания\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Апел./Отложено"),
    (re.compile(r'^(\s*📅\s*<b>\s*Назначенные заседания\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Апел./Назначено"),
    (re.compile(r'^(\s*⚖️\s*<b>\s*Вынесенные акты\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Апел./Акты"),
    (re.compile(r'^(\s*📄\s*<b>\s*Опубликованные тексты актов\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Апел./Тексты актов"),
    (re.compile(r'^(\s*⚠\s*<b>\s*Переход к правилам 1-й инстанции\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Апел./Переход к правилам 1 инст."),
    (re.compile(r'^(\s*🔀\s*<b>\s*Перешли в апелляцию\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Перешли в апелляцию"),
    (re.compile(r'^(\s*📥\s*<b>\s*Новые касс\. дела\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Касс./Новые дела"),
    (re.compile(r'^(\s*📑\s*<b>\s*Касс\. события\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$'),
     "Касс./События"),
]


def _renumber_section_headers(html: str) -> str:
    """Пересчитать `(N)` в шапке каждой подсекции по факту.

    LLM иногда заявляет «Новые иски (2):» а выводит одно дело, либо наоборот.
    `_validate_digest_new_sections` уже правит «Новые иски/дела» (3.1/5.1).
    Эта функция покрывает оставшиеся секции с (N): 3.2 «Изменения»,
    3.3 «Поданы апел. жалобы», 3.4 «Кассация», 3.5 «Вынесенные решения»,
    3.6 «Тексты решений», 4 «Перешли в апелляцию», 5.1a «Переход к правилам»,
    5.2 «Отложенные», 5.3 «Назначенные», 5.4 «Вынесенные акты», 5.5
    «Тексты актов». Считаем строки с `<a href>` номером до следующего
    заголовка (`_DIGEST_HEADER_RE`).
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


def _recount_summary_line(html: str) -> str:
    """Перегенерировать строки сводки `📋 Сводка` по факту вывода.

    Считает в дайджесте после генерации:
    - 1 инст.: число дел в 3.2 «Изменения», 3.5 «Решения», 3.3 «Жалобы»,
      3.4 «Касс.», 3.6 «Тексты решений»;
    - Апел.: число дел в 5.1 «Новые», 5.2 «Отложенные», 5.3 «Назначенные»,
      5.4 «Акты», 5.5 «Тексты актов».

    Отдельно «Новые иски (1 инст.)» (3.1) — Y. Формат сохраняем близким
    к промпту, но цифры — из факта, а не из обещания LLM.
    """
    lines = html.split("\n")

    # Карта: тип секции → (block, fact-counter)
    # block: "fi" / "appeal" / "bridge"
    # counters считаем по факту строк-дел после соответствующего заголовка.
    sections: list[tuple[str, str, int]] = []  # (block, label, count)

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
                "appeal"
            )
            sections.append((block, label, count))
            i = j
            matched = True
            break
        # Также «Новые иски» / «Новые дела» — у них специальный формат.
        if not matched:
            m_fi = re.match(
                r'^(\s*📥\s*<b>\s*Новые иски\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$',
                ln,
            )
            m_ap = re.match(
                r'^(\s*📥\s*<b>\s*Новые дела\s*\(\s*)(\d+)(\s*\)\s*:\s*</b>\s*)$',
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

    fi_new = sum(c for b, lbl, c in sections if lbl == "1 инст./Новые иски")
    fi_changes = sum(c for b, lbl, c in sections if lbl == "1 инст./Изменения")
    fi_resolved = sum(c for b, lbl, c in sections if lbl in (
        "1 инст./Решения", "1 инст./Тексты решений",
    ))
    fi_appeal_filed = sum(c for b, lbl, c in sections if lbl == "1 инст./Апел. жалобы")
    fi_cassation = sum(c for b, lbl, c in sections if lbl == "1 инст./Кассация")
    ap_new = sum(c for b, lbl, c in sections if lbl == "Апел./Новые дела")
    ap_acts = sum(c for b, lbl, c in sections if lbl in (
        "Апел./Акты", "Апел./Тексты актов",
    ))
    ap_postponed = sum(c for b, lbl, c in sections if lbl == "Апел./Отложено")
    ap_scheduled = sum(c for b, lbl, c in sections if lbl == "Апел./Назначено")
    cass_new = sum(c for b, lbl, c in sections if lbl == "Касс./Новые дела")
    cass_events = sum(c for b, lbl, c in sections if lbl == "Касс./События")

    # Собираем фразы для каждой инстанции.
    def _plural(n: int, forms: tuple[str, str, str]) -> str:
        n = abs(n) % 100
        n1 = n % 10
        if 10 < n < 20:
            return forms[2]
        if 1 < n1 < 5:
            return forms[1]
        if n1 == 1:
            return forms[0]
        return forms[2]

    fi_parts: list[str] = []
    if fi_new:
        fi_parts.append(f"{fi_new} {_plural(fi_new, ('новый иск', 'новых иска', 'новых исков'))}")
    if fi_changes:
        fi_parts.append(f"{fi_changes} {_plural(fi_changes, ('изменение', 'изменения', 'изменений'))}")
    if fi_appeal_filed:
        fi_parts.append(
            f"{fi_appeal_filed} {_plural(fi_appeal_filed, ('апел. жалоба', 'апел. жалобы', 'апел. жалоб'))}"
        )
    if fi_cassation:
        fi_parts.append(
            f"{fi_cassation} касс. {_plural(fi_cassation, ('событие', 'события', 'событий'))}"
        )
    if fi_resolved:
        fi_parts.append(f"{fi_resolved} {_plural(fi_resolved, ('решение', 'решения', 'решений'))}")
    fi_summary = ", ".join(fi_parts) if fi_parts else "нет событий"

    ap_parts: list[str] = []
    if ap_new:
        ap_parts.append(f"+{ap_new} {_plural(ap_new, ('дело', 'дела', 'дел'))}")
    if ap_scheduled:
        ap_parts.append(
            f"{ap_scheduled} {_plural(ap_scheduled, ('заседание', 'заседания', 'заседаний'))}"
        )
    if ap_postponed:
        ap_parts.append(
            f"{ap_postponed} {_plural(ap_postponed, ('отложение', 'отложения', 'отложений'))}"
        )
    if ap_acts:
        ap_parts.append(f"{ap_acts} {_plural(ap_acts, ('акт', 'акта', 'актов'))}")
    ap_summary = ", ".join(ap_parts) if ap_parts else "нет событий"

    cass_parts: list[str] = []
    if cass_new:
        cass_parts.append(
            f"+{cass_new} {_plural(cass_new, ('дело', 'дела', 'дел'))}"
        )
    if cass_events:
        cass_parts.append(
            f"{cass_events} {_plural(cass_events, ('событие', 'события', 'событий'))}"
        )
    cass_summary = ", ".join(cass_parts)
    # Строку «Касс.:» выводим только при наличии хоть одного кассационного
    # события — пустая строка «нет событий» тут не нужна, юрист просил
    # упоминать кассацию ТОЛЬКО когда она есть.
    new_cass_line = f"<i>Касс.:</i> {cass_summary}" if cass_summary else None

    new_fi_line = f"<i>1 инст.:</i> {fi_summary}"
    new_ap_line = f"<i>Апелл.:</i> {ap_summary}"

    out: list[str] = []
    fi_replaced = False
    ap_replaced = False
    cass_replaced = False
    for ln in lines:
        s = ln.strip()
        if s.startswith("<i>1 инст.:</i>"):
            out.append(new_fi_line)
            fi_replaced = True
            continue
        if s.startswith("<i>Апелл.:</i>"):
            out.append(new_ap_line)
            ap_replaced = True
            # Если LLM не вывел строку «Касс.:», но события есть —
            # вставляем её сразу под «Апелл.:» (без пустой строки между).
            if new_cass_line is not None:
                # Проверим, есть ли уже строка «Касс.:» где-то ниже —
                # если есть, не дублируем (заменим её в блоке ниже).
                pass
            continue
        if s.startswith("<i>Касс.:</i>"):
            if new_cass_line is not None:
                out.append(new_cass_line)
                cass_replaced = True
            # Если событий по кассации нет — строку «Касс.:» удаляем
            # (LLM мог вывести её ошибочно с «нет событий»).
            continue
        out.append(ln)

    # Если LLM не вывел строку «Касс.:», но события есть — добавляем её
    # сразу после «Апелл.:» (вторичный проход по out).
    if (new_cass_line is not None
            and ap_replaced
            and not cass_replaced):
        out2: list[str] = []
        inserted = False
        for ln in out:
            out2.append(ln)
            if not inserted and ln.strip().startswith("<i>Апелл.:</i>"):
                out2.append(new_cass_line)
                inserted = True
        out = out2

    # Если LLM почему-то не вывел сводку — не вмешиваемся.
    if not fi_replaced and not ap_replaced:
        return html
    return "\n".join(out)


_LIST_PRINT_FACTS_FOR_LOG = False  # глушилка, для возможной отладки


def _warn_misplaced_appeal_cases(html: str) -> str:
    """Залогировать апелляционные номера (`33-…`), оказавшиеся в блоке 1 инст.

    Прецедент 29.04.2026: LLM поместил дело 33-2677/2026 в подсекцию
    «📅 Назначенные заседания» внутри блока 1-й инстанции, хотя по
    инварианту все `33-…` номера принадлежат блоку «⚖️ АПЕЛЛЯЦИЯ».

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
        # Шапка с (0): любой эмодзи + <b>…(0):</b>
        if re.match(
            r'^\s*(?:📥|📅|📨|🔁|⚖️|📄|⚠|🔀)\s*<b>[^<]*\(\s*0\s*\)\s*:\s*</b>\s*$',
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
                             total_active_fi: int = 0,
                             total_active_cassation: int = 0,
                             cass_changes: list[dict] | None = None,
                             cass_discovered: list[dict] | None = None,
                             act_summarizer=None) -> str:
    """Шаблонный дайджест (fallback без Claude API). Формат: HTML.

    Структура — два больших блока (🏛 ПЕРВАЯ ИНСТАНЦИЯ / ⚖️ АПЕЛЛЯЦИЯ),
    мостик «🔀 Перешли в апелляцию» между ними. Подсекция выводится только
    если есть данные; большой блок выводится только если хотя бы одна его
    подсекция непуста.

    `act_summarizer` — опциональный callable вида
    `summarize_act_motivation(act_text, *, case_meta) -> str | None`.
    Если задан, в секциях 5.5 (апел. опубл. акты), 3.6 (1-й инст. опубл.
    решения) и кассации (new_act) вместо обрезанного excerpt'а
    подставляется LLM-пересказ. None или ошибка callable → fallback
    на excerpt (старое поведение).
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
    if cass_changes is None:
        cass_changes = []
    if cass_discovered is None:
        cass_discovered = []

    total_active = total_active_appeal + total_active_fi + total_active_cassation

    # ── Короткое сообщение если изменений нет ──
    # stage_transitions намеренно НЕ учитываем: мостик в дайджест больше
    # не выводится, прогон с одними переходами = пустой.
    if (not new_cases and not changes and not fi_new_cases
            and not fi_changes
            and not cass_changes and not cass_discovered):
        return render_no_changes_digest(
            today,
            f"В производстве: всего {total_active}"
            f" (1 инст.: {total_active_fi} | апел.: {total_active_appeal}"
            f" | касс.: {total_active_cassation})",
        )

    # ── Группировка changes по типам (для блока АПЕЛЛЯЦИЯ) ──
    postponed = [ch for ch in changes if "hearing_postponed" in ch["type"]]
    postponed_nums = {ch["case"] for ch in postponed}
    to_fi_rules = [ch for ch in changes if "appeal_to_fi_rules" in ch["type"]]
    # Не дублируем дело в "Назначенные", если оно уже в "Отложенные".
    # hearing_new — первое заседание апелляции; семантически то же самое, что и
    # «назначенное заседание», поэтому показываем тут же.
    # Если у дела одновременно `new_event` и `new_result` (типичная связка для
    # дня заседания: появилось событие «Вынесено решение» и зафиксирован итог),
    # выводим ТОЛЬКО в 5.4 «Вынесенные акты» — иначе тот же текст про
    # «Вынесено решение» вылез бы дважды (5.3 «Назначенные» + 5.4).
    events = [ch for ch in changes
              if ("new_event" in ch["type"] or "hearing_new" in ch["type"])
              and ch["case"] not in postponed_nums
              and "new_result" not in ch["type"]]
    # 5.4 и 5.5 — РАЗНЫЕ события (резолютивка и полный текст), но если в
    # ОДНОМ прогоне сработали оба — показываем дело ТОЛЬКО в 5.5 (там и
    # ИТОГ из карточки, и мотивировка). Иначе пользователь видит дубль.
    # Если события разнесены во времени — в разных прогонах каждая секция
    # получит «свой» change (защита сохраняется).
    results = [ch for ch in changes
               if "new_result" in ch["type"] and "new_act" not in ch["type"]]
    acts = [ch for ch in changes if "new_act" in ch["type"]]

    # ── Блок ПЕРВАЯ ИНСТАНЦИЯ ──
    fi_block: list[str] = []
    if fi_new_cases:
        fi_block.append(f"📥 <b>Новые иски ({len(fi_new_cases)}):</b>")
        for c in fi_new_cases:
            fi = c.get("first_instance", {})
            court = escape_html(shorten_court_name(fi.get("court", "")))
            role = c.get("bank_role", "")
            cat = category_short(c.get("category", ""))
            pl_raw = c.get("plaintiff", "")
            df_raw = c.get("defendant", "")
            pl = escape_html(shorten_party_name(pl_raw, keep_fio_full=True))
            df = escape_html(shorten_party_name(df_raw, keep_fio_full=True))
            num = escape_html(c.get("id", ""))
            filing = escape_html(fi.get("filing_date", ""))
            url = fi_card_url(fi)
            link = f'<a href="{url}"><b>{num}</b></a>' if url else f'<b>{num}</b>'
            # БАНК В ХВОСТЕ: иконку показываем только когда банк = третье лицо.
            if _bank_in_parties(pl_raw, df_raw):
                role_icon = ""
            else:
                role_icon = {"Истец": "🏦→", "Ответчик": "→🏦",
                             "Третье лицо": "👁"}.get(role, "")
            prefix = f"{role_icon} " if role_icon else ""
            # Строка 1: номер, стороны, категория, суд (без даты подачи).
            fi_block.append(
                f"  {link} {prefix}{pl} vs {df} ({cat}) | {court}"
            )
            # Строка 2: дата подачи отдельной строкой, эмодзи 📥 ПОСЛЕ
            # <b>дата</b>, чтобы не попасть под _DIGEST_HEADER_RE.
            if filing:
                fi_block.append(
                    f"     <b>{filing}</b> — 📥 иск зарегистрирован в суде"
                )

    # Отделяем дела, у которых есть вынесенное решение — они поедут в 3.5.
    # В 3.2 «Изменения» их статус/резолюция не повторяются; оставляем
    # только побочные события того же дела (заседание/отложение и т.п.).
    # То же для fi_act_text_published — эти дела поедут в 3.6.
    # 3.5 vs 3.6 — то же правило, что и для апелляции (5.4 vs 5.5): если в
    # одном прогоне у дела сработали И вынесение решения, И публикация полного
    # текста — выводим дело ТОЛЬКО в 3.6 «Опубликованные тексты решений».
    fi_resolved_chs = [
        ch for ch in fi_changes
        if "fi_resolved" in ch["type"]
        and "fi_act_text_published" not in ch["type"]
    ]
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
                    if d.get("hearing_date_unpublished"):
                        ev_list.append(
                            "📅 назначено первое заседание "
                            "(дата и время не опубликованы)"
                        )
                    else:
                        hd = escape_html(d.get("hearing_date", ""))
                        ht = escape_html(d.get("hearing_time", ""))
                        htype = escape_html(d.get("hearing_type", "заседание"))
                        ev_list.append(f"📅 {htype} {hd}" + (f" {ht}" if ht else ""))
                elif t == "fi_hearing_next":
                    new_p = escape_html(
                        d.get("hearing_date", "")
                        + (f" {d['hearing_time']}" if d.get("hearing_time") else "")
                    )
                    ev_list.append(f"📅 заседание назначено на {new_p}")
                elif t == "fi_hearing_postponed":
                    new_p = escape_html(
                        d.get("hearing_date", "")
                        + (f" {d['hearing_time']}" if d.get("hearing_time") else "")
                    )
                    # Только новая дата (старую больше не показываем —
                    # по запросу пользователя).
                    ev_list.append(f"🔁 заседание отложено на {new_p}")
                elif t == "fi_status_change":
                    ev_list.append(
                        f"статус: {escape_html(d.get('old_status', ''))} → "
                        f"{escape_html(d.get('new_status', ''))}"
                    )
                elif t == "fi_act_published":
                    ad = escape_html(d.get("act_date", ""))
                    ev_list.append(
                        "📄 мотивированное решение изготовлено"
                        + (f" {ad}" if ad else "")
                        + ", полный текст не опубликован"
                    )
                elif t == "fi_final_event":
                    ev_raw = d.get('event', '') or ''
                    ev_low = ev_raw.lower()
                    # Спец-обработка фразы «Изготовлено мотивированное
                    # решение в окончательной форме» — эквивалент
                    # fi_act_published; нормализуем под единую формулировку.
                    if ('изготовлено' in ev_low
                            and 'мотивированное решение' in ev_low):
                        m = re.search(r'(\d{2}\.\d{2}\.\d{4})', ev_raw)
                        ad = escape_html(
                            m.group(1) if m else (d.get('event_date') or '')
                        )
                        ev_list.append(
                            "📄 мотивированное решение изготовлено"
                            + (f" {ad}" if ad else "")
                            + ", полный текст не опубликован"
                        )
                    else:
                        ev_list.append(f"⚖️ {escape_html(ev_raw)}")
                elif t == "fi_motivirovka_emitted":
                    md = escape_html(d.get('motivirovka_date', ''))
                    ev_list.append(
                        "📄 мотивированное решение изготовлено"
                        + (f" {md}" if md else "")
                        + ", полный текст не опубликован"
                    )
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
        _section_break(fi_block)
        fi_block.append(
            f"📅 <b>Изменения ({len(fi_changes_rendered)}):</b>"
        )
        fi_block.extend(fi_changes_rendered)

    # ── 3.5: Вынесенные решения 1 инстанции ──
    if fi_resolved_chs:
        _section_break(fi_block)
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
            # БАНК В ХВОСТЕ: «банк — роль» только когда банк не в сторонах.
            if bank_role and not _bank_in_parties(
                    ch.get("plaintiff", ""), ch.get("defendant", "")):
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
        _section_break(fi_block)
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
            # 3.6: либо LLM-пересказ мотивировки (если act_summarizer задан),
            # либо обрезанный excerpt — old behaviour для template-fallback.
            act_excerpt = _render_act_summary_or_excerpt(
                d.get("act_text") or "",
                {
                    "stage": "first_instance",
                    "bank_role": ch.get("bank_role", ""),
                    "verdict_label": d.get("verdict_label", ""),
                    "plaintiff": ch.get("plaintiff", ""),
                    "defendant": ch.get("defendant", ""),
                    "category": d.get("category", ""),
                },
                summarizer=act_summarizer,
                max_excerpt_len=500,
            )
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

    # ── Блок АПЕЛЛЯЦИЯ ──
    appeal_block: list[str] = []
    if new_cases:
        appeal_block.append(f"📥 <b>Новые дела ({len(new_cases)}):</b>")
        for c in new_cases:
            link = case_link_html(c)
            role = c.get("Роль банка", "")
            cat = category_short(c.get("Категория", ""))
            pl_raw = c.get('Истец', '')
            df_raw = c.get('Ответчик', '')
            pl = escape_html(shorten_party_name(pl_raw, keep_fio_full=True))
            df = escape_html(shorten_party_name(df_raw, keep_fio_full=True))
            court_fi = escape_html(
                shorten_court_name(c.get('Суд 1 инстанции', '') or '')
            )
            filing = escape_html(c.get('Дата поступления', '') or '')
            # БАНК В ХВОСТЕ: если Сбербанк уже в сторонах — иконка/хвост лишние.
            if _bank_in_parties(pl_raw, df_raw):
                role_icon = ""
                role_tail = ""
            else:
                role_icon = {"Истец": "🏦→", "Ответчик": "→🏦",
                             "Третье лицо": "👁"}.get(role, "")
                role_tail = (f" | банк — {escape_html(role.lower())}"
                             if role else "")
            prefix = f"{role_icon} " if role_icon else ""
            # Строка 1: номер + стороны.
            appeal_block.append(f"  {link} {prefix}{pl} vs {df}")
            # Строка 2: суд 1 инст. | категория | банк (если не в сторонах).
            line2_parts: list[str] = []
            if court_fi:
                line2_parts.append(f"Суд 1 инст.: {court_fi}")
            if cat:
                line2_parts.append(f"категория: {escape_html(cat)}")
            if line2_parts or role_tail:
                appeal_block.append(
                    "     " + " | ".join(line2_parts) + role_tail
                )
            # Строка 3: дата поступления отдельной строкой, эмодзи 📥
            # ПОСЛЕ <b>дата</b>, чтобы не попасть под _DIGEST_HEADER_RE.
            if filing:
                appeal_block.append(
                    f"     <b>{filing}</b> — 📥 поступило в апел. суд"
                )

    if to_fi_rules:
        _section_break(appeal_block)
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
        _section_break(appeal_block)
        appeal_block.append(f"🔁 <b>Отложенные заседания ({len(postponed)}):</b>")
        for ch in postponed:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = (f'<a href="{url}"><b>{case_num}</b></a>'
                    if url else f'<b>{case_num}</b>')
            new_dt = escape_html(d.get("new_hearing_date", ""))
            new_tm = escape_html(d.get("new_hearing_time", ""))
            new_part = new_dt + (f" {new_tm}" if new_tm else "")
            plaintiff = escape_html(shorten_party_name(d.get("plaintiff", "")))
            defendant = escape_html(shorten_party_name(d.get("defendant", "")))
            court = escape_html(shorten_court_name(ch.get("court", "")))
            cat = category_short(d.get("category", ""))
            # Строка 1: «🔁 номер — стороны (суд)». Суд показываем, только
            # когда он есть в ch (apel-уровень обычно без него — там и так
            # понятно, что это Суд ХМАО-Югры).
            line1 = f"  🔁 {link}"
            if plaintiff and defendant:
                line1 += f" — {plaintiff} vs {defendant}"
            if court:
                line1 += f" ({court})"
            if cat:
                line1 += f" | {escape_html(cat)}"
            appeal_block.append(line1)
            # Строка 2: только новая дата/время — старую не показываем
            # (по запросу пользователя).
            if new_part:
                appeal_block.append(f"     Заседание отложено на {new_part}")

    if events:
        _section_break(appeal_block)
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
        _section_break(appeal_block)
        # Резолютивная часть — выходит через 1-3 дня после заседания.
        appeal_block.append(f"⚖️ <b>Вынесенные акты ({len(results)}):</b>")
        for ch in results:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = f'<a href="{url}"><b>{case_num}</b></a>' if url else f'<b>{case_num}</b>'
            result_text = escape_html(d.get("result", ""))
            role = d.get("role", "")
            # БАНК В ХВОСТЕ: показываем «(банк — роль)» только когда банк не в сторонах.
            if role and not _bank_in_parties(
                    d.get("plaintiff", ""), d.get("defendant", "")):
                role_note = f" (банк — {escape_html(role.lower())})"
            else:
                role_note = ""
            hearing_dt = d.get("hearing_date", "")
            date_note = f". Определение от {escape_html(hearing_dt)}" if hearing_dt else ""
            cat = category_short(d.get("category", ""))
            cat_note = f" | {escape_html(cat)}" if cat else ""
            # Строка «Причина: <last_event>» убрана: last_event обычно дублирует
            # уже сказанное в этой же строке (result_text повторяет «Вынесено
            # решение …»), а в Claude-варианте такой строки не было.
            appeal_block.append(
                f"  {link}: {result_text}{cat_note}{role_note}{date_note}"
            )

    if acts:
        _section_break(appeal_block)
        # Полный текст с мотивировкой — обычно через 14+ дней (или никогда).
        appeal_block.append(f"📄 <b>Опубликованные тексты актов ({len(acts)}):</b>")
        for ch in acts:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = f'<a href="{url}"><b>{case_num}</b></a>' if url else f'<b>{case_num}</b>'
            # 5.5: act_excerpt — уже сжатый шаблоном, act_text — сырой.
            # Если act_summarizer задан, шлём в LLM сырой act_text (он
            # содержит больше деталей); иначе — берём готовый excerpt
            # либо обрезаем сырой по двум предложениям/250 символам.
            raw_act = (d.get("act_text") or "").strip()
            ready_excerpt = (d.get("act_excerpt") or "").strip()
            if act_summarizer is not None and raw_act:
                summary_or_excerpt = _render_act_summary_or_excerpt(
                    raw_act,
                    {
                        "stage": "appeal",
                        "bank_role": d.get("role", ""),
                        "verdict_label": (
                            d.get("act_verdict_label")
                            or d.get("verdict_label", "")
                        ),
                        "plaintiff": d.get("plaintiff", ""),
                        "defendant": d.get("defendant", ""),
                        "category": d.get("category", ""),
                    },
                    summarizer=act_summarizer,
                    max_excerpt_len=500,
                )
            elif ready_excerpt or raw_act:
                src = ready_excerpt or raw_act
                # Старая логика: первые 1-2 предложения, лимит ~250.
                short_parts = re.split(r"(?<=[.!?])\s+", src)[:2]
                short = " ".join(short_parts)[:250].rstrip(".") + "."
                summary_or_excerpt = escape_html(short)
            else:
                summary_or_excerpt = ""
            if summary_or_excerpt:
                appeal_block.append(
                    f"  {link}\n    Мотивировка: {summary_or_excerpt}"
                )
            else:
                appeal_block.append(f"  {link}")

    # ── Сборка ──
    summary = build_summary_line(
        new_cases, changes, fi_new_cases, stage_transitions, fi_changes,
        cass_changes=cass_changes, cass_discovered=cass_discovered,
    )
    lines = [
        f"📊 <b>Мониторинг дел Сбербанка — {today}</b>",
        f"📋 {escape_html(summary)}",
    ]

    if fi_block:
        lines.append("")
        lines.append("🏛 <b>ПЕРВАЯ ИНСТАНЦИЯ</b>")
        lines.extend(fi_block)
    if appeal_block:
        lines.append("")
        lines.append("⚖️ <b>АПЕЛЛЯЦИЯ</b>")
        lines.extend(appeal_block)

    # ── Блок КАССАЦИЯ ──
    cass_block: list[str] = []
    _OUTCOME_RU = {
        "cassation_dismissed_no_transfer": "отказ в передаче жалобы",
        "cassation_upheld": "оставлено без изменения",
        "cassation_modified": "изменено",
        "cassation_reversed": "отменено",
        "cassation_remanded": "отменено и направлено на новое рассмотрение",
        "cassation_terminated": "прекращено / отозвано / возвращено",
        "cassation_other": "",
    }
    if cass_discovered:
        cass_block.append(f"📥 <b>Новые касс. дела ({len(cass_discovered)}):</b>")
        for c in cass_discovered:
            cass = c.get("cassation") or {}
            fi_b = c.get("first_instance") or {}
            num_cs = escape_html(cass.get("case_number", ""))
            url = ""
            if cass.get("link"):
                cid_, cuid_ = case_id_uid(cass["link"])
                if cid_ and cuid_:
                    url = CASSATION_COURT.card_url(cid_, cuid_)
            # Заголовок строки = касс. внутренний номер БЕЗ префикса «касс. №»
            # (избыточен: секция «Новые касс. дела» сама уже это указывает).
            link = (f'<a href="{url}"><b>{num_cs}</b></a>'
                    if url else f'<b>{num_cs}</b>')
            pl_raw = c.get("plaintiff", "")
            df_raw = c.get("defendant", "")
            pl = escape_html(shorten_party_name(pl_raw, keep_fio_full=True))
            df = escape_html(shorten_party_name(df_raw, keep_fio_full=True))
            role = c.get("bank_role", "") or ""
            tail = "" if _bank_in_parties(pl_raw, df_raw) or not role \
                else f", банк — {escape_html(role.lower())}"
            sber_flag = "🏦 " if cass.get("appellant_is_bank") else ""
            cass_block.append(f"  {sber_flag}{link} — {pl} vs {df}{tail}")
            appellant = escape_html(cass.get("appellant", "") or "")
            # Роль заявителя в Title Case для строки 3 («от Ответчика Иванова»).
            appellant_status_raw = (cass.get("appellant_status", "") or "").strip()
            appellant_role = escape_html(appellant_status_raw.capitalize())
            # Строка 2: суд 1 инст. + категория. Без номера 1-й инст. и «заявитель».
            court_short = escape_html(
                shorten_court_name(fi_b.get("court", "") or "")
            )
            cat_raw = (cass.get("category") or c.get("category") or "").strip()
            cat = escape_html(cat_raw)
            line2 = f"     {court_short}" if court_short else "     "
            if cat:
                line2 += f" | категория: {cat}"
            cass_block.append(line2)
            filing = escape_html(cass.get("filing_date", "") or "")
            if filing:
                # Эмодзи 📥 ставим ПОСЛЕ <b>дата</b>, иначе строка попадёт
                # под _DIGEST_HEADER_RE и будет принята за заголовок секции.
                # Заявителя выводим в формате «от Роль Имя» (например,
                # «от Ответчика Адаменко Е.М.»).
                from_str = ""
                if appellant_role and appellant:
                    from_str = f" от {appellant_role} {appellant}"
                elif appellant:
                    from_str = f" от {appellant}"
                cass_block.append(
                    f"     <b>{filing}</b> — 📥 поступила касс. жалоба"
                    + from_str
                )
            cass_block.append("")
        if cass_block and cass_block[-1] == "":
            cass_block.pop()

    cass_events_only = [
        ch for ch in cass_changes
        if "discovered_in_cassation" not in ch.get("type", [])
    ]
    if cass_events_only:
        if cass_block:
            _section_break(cass_block)
        cass_block.append(f"📑 <b>Касс. события ({len(cass_events_only)}):</b>")
        for ch in cass_events_only:
            d = ch.get("details") or {}
            num_fi = escape_html(ch.get("case", ""))
            num_cs = escape_html(ch.get("cassation_internal_number", ""))
            link = f"<b>{num_fi}</b>"
            sber_flag = "🏦 " if d.get("appellant_is_bank") else ""
            stage_prev = escape_html(d.get("stage_prev", "") or "")
            stage_now = escape_html(d.get("stage_now", "") or "")
            cass_block.append(
                f"  {sber_flag}{link} — касс. № <b>{num_cs}</b> | "
                f"стадия: {stage_prev} → {stage_now}"
            )
            outcome = d.get("outcome", "") or ""
            outcome_ru = _OUTCOME_RU.get(outcome, "")
            result_text = escape_html(d.get("result_text", "") or "")
            rfa = escape_html(d.get("result_for_appeal", "") or "")
            itog_parts: list[str] = []
            if outcome_ru:
                itog_parts.append(f"<b>Итог:</b> {escape_html(outcome_ru)}")
            elif result_text:
                itog_parts.append(f"<b>Итог:</b> {result_text}")
            if rfa:
                itog_parts.append(f"апел.: {rfa}")
            if itog_parts:
                cass_block.append("     " + " | ".join(itog_parts))
            # Касс. new_act: тот же приём, что и для 3.6/5.5 — пересказ
            # через act_summarizer (если задан) либо excerpt.
            act_excerpt = _render_act_summary_or_excerpt(
                d.get("act_text") or "",
                {
                    "stage": "cassation",
                    "bank_role": d.get("bank_role", ""),
                    "verdict_label": outcome_ru or result_text,
                    "plaintiff": d.get("plaintiff", ""),
                    "defendant": d.get("defendant", ""),
                    "category": d.get("category", ""),
                },
                summarizer=act_summarizer,
                max_excerpt_len=500,
            )
            if act_excerpt:
                cass_block.append(f"     <i>{act_excerpt}</i>")
            cass_block.append("")
        if cass_block and cass_block[-1] == "":
            cass_block.pop()

    if cass_block:
        lines.append("")
        lines.append("⚖️🔬 <b>КАССАЦИЯ</b>")
        lines.extend(cass_block)

    lines.append("")
    lines.append(
        f"📌 <b>В производстве: всего {total_active}"
        f" (1 инст.: {total_active_fi} | апел.: {total_active_appeal}"
        f" | касс.: {total_active_cassation})</b>"
    )
    lines.append(f'<a href="{DASHBOARD_URL}">📊 Дашборд</a>')

    text = "\n".join(lines)
    # До двух сообщений: лимит 2×4096; split_message в send_telegram разобьёт
    return truncate_html_message(text, TELEGRAM_MSG_LIMIT * 2)


# ── Telegram ─────────────────────────────────────────────────────────────────

def _filter_events_by_watchlist(
    watchlist: set[str],
    *,
    fi_new_cases: list[dict],
    fi_changes: list[dict],
    stage_transitions: list[dict],
    appeal_new_cases_csv: list[dict],
    changes: list[dict],
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
) -> dict:
    """Отфильтровать списки событий по идентификаторам дел в watchlist.

    Идентификатор в watchlist = `caseNumber` с фронта (для апел. дел = номер
    апелляции, для 1-й инст. = номер 1-й инст.). Маппинг полей:
    · changes (apel)        → ch["case"]
    · fi_changes            → ch["case"] (= fi.case_number)
    · cass_changes          → ch["case"] (= номер 1-й инст., наш ключ id)
    · fi_new_cases          → c["id"]            (НЕ фильтруем, общесистемно)
    · appeal_new_cases_csv  → c["Номер дела"]    (НЕ фильтруем, общесистемно)
    · cass_discovered       → c["id"]            (НЕ фильтруем, общесистемно)
    · stage_transitions     → fi_case_number ИЛИ appeal_case_number
      (юрист может отслеживать дело по любому из них).

    Новые дела (`fi_new_cases`, `appeal_new_cases_csv`, `cass_discovered`)
    считаем общесистемным сигналом: они появились впервые и логически не
    могут быть в чьём-либо watchlist. Поэтому возвращаем их целиком всем
    подписчикам.
    """
    return {
        "fi_new_cases": list(fi_new_cases or []),
        "fi_changes": [
            ch for ch in (fi_changes or [])
            if (ch.get("case") or "").strip() in watchlist
        ],
        "stage_transitions": [
            t for t in (stage_transitions or [])
            if (t.get("fi_case_number") or "").strip() in watchlist
            or (t.get("appeal_case_number") or "").strip() in watchlist
        ],
        "appeal_new_cases_csv": list(appeal_new_cases_csv or []),
        "changes": [
            ch for ch in (changes or [])
            if (ch.get("case") or "").strip() in watchlist
        ],
        "cass_changes": [
            ch for ch in (cass_changes or [])
            if (ch.get("case") or "").strip() in watchlist
        ],
        "cass_discovered": list(cass_discovered or []),
    }


def _drop_dead_subscription(endpoint: str) -> None:
    """Удалить мёртвую подписку из KV через `/unsubscribe` на Worker.

    Вызывается автоматически после WebPushException 410/404. Тихая —
    любая ошибка логируется и не валит прогон, очистка best-effort.
    """
    if not PUSH_WORKER_URL or not PUSH_SECRET or not endpoint:
        return
    try:
        r = requests.post(
            f"{PUSH_WORKER_URL}/unsubscribe",
            headers={
                "Authorization": f"Bearer {PUSH_SECRET}",
                "Content-Type": "application/json",
            },
            json={"endpoint": endpoint},
            timeout=10,
        )
        if r.ok:
            log.info(f"Web Push: мёртвая подписка удалена из KV ({endpoint[:60]})")
        else:
            log.warning(
                f"Web Push: /unsubscribe вернул {r.status_code} для {endpoint[:60]}"
            )
    except Exception as exc:
        log.warning(f"Web Push: не удалось удалить подписку: {exc}")


def _make_per_sub_callback(
    *,
    fi_new_cases: list[dict],
    fi_changes: list[dict],
    changes: list[dict],
    stage_transitions: list[dict],
    appeal_new_cases_csv: list[dict],
    push_summary: str,
    cass_changes: list[dict] | None = None,
    cass_discovered: list[dict] | None = None,
):
    """Фабрика callback'а для `send_web_push(per_subscriber=...)`.

    Логика отправки push с учётом подписки на дела:
    · watchlist пуст и событий вообще нет → None (ничего не шлём).
    · watchlist пуст, но есть любые события (новые дела ИЛИ изменения ИЛИ
      переходы стадий) → общий push с push_summary, без фильтрации.
    · watchlist непуст → персональный push: `_filter_events_by_watchlist`
      пропускает все новые дела целиком + только изменения по своим делам.
      Заголовок «Мониторинг дел — твои дела», click_url с `?mine=1`.
    · watchlist непуст, но и своих изменений, и новых дел нет → None.

    Используется в main_json (живой крон), main_replay_last,
    main_push_last_digest — чтобы тестовые режимы вели себя как боевой.
    """
    cass_changes = cass_changes or []
    cass_discovered = cass_discovered or []

    def _per_sub(sub: dict):
        wl_raw = sub.get("watchlist") or []
        wl = {str(x).strip() for x in wl_raw if str(x).strip()}

        if not wl:
            # Пустой watchlist — общесистемный push при любых событиях.
            total_global = (
                len(fi_new_cases) + len(appeal_new_cases_csv) + len(cass_discovered)
                + len(fi_changes) + len(changes) + len(cass_changes)
                + len(stage_transitions)
            )
            if total_global == 0:
                return None
            return (
                "Мониторинг дел — обновление",
                push_summary,
                "/sberbank_dashboard.html?digest=open",
            )

        f = _filter_events_by_watchlist(
            wl,
            fi_new_cases=fi_new_cases,
            fi_changes=fi_changes,
            stage_transitions=stage_transitions,
            appeal_new_cases_csv=appeal_new_cases_csv,
            changes=changes,
            cass_changes=cass_changes,
            cass_discovered=cass_discovered,
        )
        n_new = (
            len(f["fi_new_cases"])
            + len(f["appeal_new_cases_csv"])
            + len(f.get("cass_discovered") or [])
        )
        n_chg = (
            len(f["fi_changes"]) + len(f["changes"])
            + len(f.get("cass_changes") or [])
        )
        n_st = len(f["stage_transitions"])
        if n_new + n_chg + n_st == 0:
            return None
        # Перечень: до 3 номеров, остаток сворачиваем в «и ещё N».
        ids: list[str] = []
        for c in f["fi_new_cases"]:
            ids.append((c.get("id") or "").strip())
        for c in f["appeal_new_cases_csv"]:
            ids.append((c.get("Номер дела") or "").strip())
        for c in (f.get("cass_discovered") or []):
            ids.append((c.get("id") or "").strip())
        for ch in f["fi_changes"]:
            ids.append((ch.get("case") or "").strip())
        for ch in f["changes"]:
            ids.append((ch.get("case") or "").strip())
        for ch in (f.get("cass_changes") or []):
            ids.append((ch.get("case") or "").strip())
        for t in f["stage_transitions"]:
            ids.append(
                (t.get("appeal_case_number") or t.get("fi_case_number") or "").strip()
            )
        ids_uniq: list[str] = []
        seen: set[str] = set()
        for x in ids:
            if x and x not in seen:
                seen.add(x)
                ids_uniq.append(x)
        head = ", ".join(ids_uniq[:3])
        tail = f" и ещё {len(ids_uniq) - 3}" if len(ids_uniq) > 3 else ""
        total = n_new + n_chg + n_st
        body = (
            f"Изменения по {len(ids_uniq)} "
            f"{'делу' if len(ids_uniq) == 1 else 'делам'}: {head}{tail}"
            + (f" · всего событий: {total}" if total > len(ids_uniq) else "")
        )
        return (
            "Мониторинг дел — твои дела",
            body,
            "/sberbank_dashboard.html?digest=open&mine=1",
        )

    return _per_sub


def send_web_push(
    title: str,
    body: str,
    *,
    click_url: str | None = None,
    owner_only: bool = False,
    per_subscriber=None,
) -> None:
    """Отправить Web Push PWA-подписчикам через Cloudflare Worker + pywebpush.

    `click_url` — относительный или абсолютный URL, который Service Worker откроет
    по клику на уведомление. По умолчанию открывается дашборд с раскрытым блоком
    последнего дайджеста.

    `owner_only=True` — слать только устройствам, помеченным владельческими
    (через POST /mark-owner). Используется в тестовых режимах (`--replay-last`,
    `--digest-only`), чтобы пробные пуши не улетали коллегам.

    `per_subscriber` — опциональный callable(sub_dict) → (title, body, click_url)
    либо None. Если задан, push-payload строится индивидуально для каждой
    подписки. Возврат None означает «для этой подписки нет персональных
    событий — пропустить». Используется для персонализации основного крона
    по watchlist подписчика.
    """
    if not PUSH_WORKER_URL or not PUSH_SECRET or not VAPID_PRIVATE_KEY:
        log.info("Web Push: переменные не настроены, пропуск")
        return
    try:
        # Получаем список подписок от Worker
        list_url = f"{PUSH_WORKER_URL}/subscriptions"
        if owner_only:
            list_url += "?role=owner"
        r = requests.get(
            list_url,
            headers={"Authorization": f"Bearer {PUSH_SECRET}"},
            timeout=10,
        )
        if not r.ok:
            log.warning(f"Web Push: не удалось получить подписки: {r.status_code}")
            return
        subscriptions = r.json()
        if not subscriptions:
            scope = "владельческих" if owner_only else ""
            log.info(f"Web Push: нет {scope}подписчиков".replace("  ", " ").strip())
            return
        log.info(
            f"Web Push: отправляю {len(subscriptions)} "
            f"{'владельческим ' if owner_only else ''}подписчикам"
        )

        import warnings as _w
        _w.filterwarnings("ignore")
        from pywebpush import webpush, WebPushException  # noqa: PLC0415
        from py_vapid import Vapid  # noqa: PLC0415

        # pywebpush.from_string не понимает PEM-строку из env (баг py_vapid 1.9.x);
        # явно создаём Vapid из bytes и передаём объект.
        vapid = Vapid.from_pem(VAPID_PRIVATE_KEY.encode())

        default_url = click_url or "/sberbank_dashboard.html?digest=open"
        ok_count = 0
        skipped = 0
        n_general = 0
        n_personal = 0
        # Журнал отправленных payload'ов — потом сохраним в
        # data/last_personal_pushes.json для админки.
        dump_items: list[dict] = []
        for sub in subscriptions:
            ep_full = sub.get("endpoint") or ""
            ep_short = ep_full[-32:] if ep_full else "?"
            wl_raw = sub.get("watchlist") or []
            wl_size = len(wl_raw) if isinstance(wl_raw, list) else 0
            is_owner = bool(sub.get("is_owner"))
            if per_subscriber is not None:
                personalised = per_subscriber(sub)
                if personalised is None:
                    skipped += 1
                    log.info(
                        f"Web Push: ⊘ skip ({'owner' if is_owner else 'user'}, "
                        f"watchlist={wl_size}) …{ep_short}"
                    )
                    dump_items.append({
                        "endpoint": ep_full,
                        "endpoint_tail": ep_short,
                        "is_owner": is_owner,
                        "watchlist_size": wl_size,
                        "watchlist": list(wl_raw) if isinstance(wl_raw, list) else [],
                        "variant": "skip",
                        "title": None,
                        "body": None,
                        "click_url": None,
                    })
                    continue
                p_title, p_body, p_url = personalised
                variant = (
                    "personal" if "твои дела" in (p_title or "")
                    else "general"
                )
                if variant == "personal":
                    n_personal += 1
                else:
                    n_general += 1
                log.info(
                    f"Web Push: → {variant} "
                    f"({'owner' if is_owner else 'user'}, watchlist={wl_size}) "
                    f"…{ep_short}"
                )
                payload = json.dumps(
                    {
                        "title": p_title,
                        "body": p_body,
                        "data": {"url": p_url or default_url},
                    },
                    ensure_ascii=False,
                )
                dump_items.append({
                    "endpoint": ep_full,
                    "endpoint_tail": ep_short,
                    "is_owner": is_owner,
                    "watchlist_size": wl_size,
                    "watchlist": list(wl_raw) if isinstance(wl_raw, list) else [],
                    "variant": variant,
                    "title": p_title,
                    "body": p_body,
                    "click_url": p_url or default_url,
                })
            else:
                payload = json.dumps(
                    {"title": title, "body": body, "data": {"url": default_url}},
                    ensure_ascii=False,
                )
                dump_items.append({
                    "endpoint": ep_full,
                    "endpoint_tail": ep_short,
                    "is_owner": is_owner,
                    "watchlist_size": wl_size,
                    "watchlist": list(wl_raw) if isinstance(wl_raw, list) else [],
                    "variant": "broadcast",
                    "title": title,
                    "body": body,
                    "click_url": default_url,
                })
            try:
                webpush(
                    subscription_info=sub,
                    data=payload,
                    vapid_private_key=vapid,
                    vapid_claims={"sub": "mailto:7selivanov.a@gmail.com"},
                    ttl=43200,  # 12 часов: push-сервис держит сообщение,
                                # пока устройство не выйдет в сеть
                )
                ok_count += 1
            except WebPushException as exc:
                ep_full = sub.get("endpoint") or ""
                ep_short = ep_full[:60] or "?"
                log.warning(f"Web Push: ошибка для {ep_short}: {exc}")
                # Автоочистка: 410 Gone и 404 Not Found — это «подписка
                # мертва навсегда» (RFC 8030). Удаляем её из KV, чтобы не
                # тащить балласт каждый прогон.
                resp = getattr(exc, "response", None)
                status = getattr(resp, "status_code", None) if resp is not None else None
                if status in (404, 410) and ep_full:
                    _drop_dead_subscription(ep_full)
        suffix = f", пропущено по watchlist: {skipped}" if skipped else ""
        if per_subscriber is not None:
            suffix += f"; персональных: {n_personal}, общих: {n_general}"
        log.info(f"Web Push: отправлено {ok_count}/{len(subscriptions)}{suffix}")
        # Сохраняем журнал последней рассылки — админка читает этот файл,
        # чтобы показать «что получила каждая подписка». Перезаписывается
        # на каждом прогоне (только последняя рассылка, без истории).
        try:
            save_json({
                "version": 1,
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "title_default": title,
                "body_default": body,
                "owner_only": owner_only,
                "items": dump_items,
            }, LAST_PERSONAL_PUSHES_PATH)
        except Exception as exc:
            log.warning(f"Web Push: не удалось сохранить журнал push: {exc}")
    except Exception as exc:
        log.error(f"Web Push: исключение: {exc}")


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
    cases, changes, _skip_stats = update_active_cases(cases)
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
    save_last_digest(
        digest,
        summary=f"🆕 Новых: {len(new_cases)} · 📋 Изменений: {len(changes)}",
        is_empty=not (new_cases or changes),
    )
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
            "delo_id": fi.get("court_delo_id", 0),
            "srv_num": fi.get("court_srv_num", 1),
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


def _apel_csv_row_to_json_case(
    row: dict,
    fi_number_lookup: dict[str, str] | None = None,
) -> dict:
    """Конвертировать CSV-строку апел. дела (после обогащения parse_case_card)
    в JSON-структуру для cases.json. Без этой конверсии новое апел. дело
    оседает только в CSV: link_cases ищет апел. в существующем JSON-индексе
    и молча пропускает то, чего там ещё нет.

    fi_number_lookup — словарь {номер_апелляции → номер_1_инст}, который
    main_json собирает по результатам парсинга апел. карточек. Если запись
    есть, кладём её в first_instance.case_number сразу, чтобы новое дело
    с самого начала имело корректный якорь для link_cassation_cases (иначе
    кассация на 7kas не находит существующее дело по `fi_case_number` и
    создаёт двойник через discovery — см. кейс 33-1643/2026 ↔ 8Г-7248/2026).
    Без словаря — поведение прежнее (`""`)."""
    case_num = (row.get("Номер дела") or "").strip()
    fi_case_number = ""
    if fi_number_lookup and case_num:
        fi_case_number = (fi_number_lookup.get(case_num) or "").strip()
    return {
        "id": case_num,
        "current_stage": "appeal",
        "plaintiff": row.get("Истец", ""),
        "defendant": row.get("Ответчик", ""),
        "category": row.get("Категория", ""),
        "bank_role": row.get("Роль банка", ""),
        "notes": row.get("Заметки", ""),
        "first_instance": {
            "case_number": fi_case_number,
            "court": row.get("Суд 1 инстанции", ""),
            "court_domain": "",
            "judge": row.get("Судья 1 инстанции", ""),
            "filing_date": "",
            "status": "",
            "result": "",
            "last_event": "",
            "event_date": "",
            "hearing_date": "",
            "hearing_time": "",
            "link": "",
            "act_published": False,
            "act_date": "",
            "events": [],
        },
        "appeal": {
            "case_number": case_num,
            "court": APPEAL_COURT.name,
            "judge_reporter": row.get("Судья-докладчик", ""),
            "filing_date": row.get("Дата поступления", ""),
            "status": row.get("Статус", "В производстве"),
            "result": row.get("Результат", ""),
            "last_event": row.get("Последнее событие", ""),
            "event_date": row.get("Дата события", ""),
            "hearing_date": row.get("Дата заседания", ""),
            "hearing_time": row.get("Время заседания", ""),
            "link": row.get("Ссылка", ""),
            "act_published": row.get("Акт опубликован", "Нет") == "Да",
            "act_date": row.get("Дата публикации акта", ""),
            "appellant": row.get("Апеллянт", ""),
            "events": [],
        },
    }


def main_json():
    """Основной цикл с JSON-хранилищем: 1 инстанция + апелляция."""
    log.info("=" * 60)
    log.info("Запуск мониторинга дел Сбербанка (JSON-режим)")
    log.info("=" * 60)

    # Smart-skip нерабочих дней РФ (включается при автозапуске через
    # Worker — он передаёт SKIP_NON_WORKING_DAYS=1 / --smart-skip).
    # Ручной запуск из UI работает без skip.
    smart_skip_mode = (
        "--smart-skip" in sys.argv
        or os.environ.get("SKIP_NON_WORKING_DAYS") == "1"
    )
    today = date.today()
    if smart_skip_mode and not is_russian_working_day(today):
        log.info(f"{today.isoformat()} — нерабочий день РФ, парсинг пропущен.")
        return

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

    # Индекс существующих cases по id — нужен для промоушена М-записей
    # в 2-XXX, когда материал регистрируется и в выдаче появляется
    # комбо-номер «2-XXX/YYYY ~ М-NNN/YYYY». Без промоушена в JSON
    # остался бы orphan-материал рядом с новой 2-XXX-записью.
    case_by_id: dict[str, dict] = {
        (c.get("id") or "").strip(): c for c in cases
    }

    # Собираем все результаты поиска по 1-й инст. — нужны и для new_fi
    # фильтра ниже, и для re-link дел, вернувшихся из кассации (awaiting_relink).
    # Используем список пар, а не dict — CourtConfig не хешируется.
    fi_results_by_court: list = []

    for court in enabled_courts:
        polite_delay()
        search_html = fetch_page(court.search_url())
        if not search_html:
            log.warning(f"  {court.name}: не удалось загрузить поиск")
            continue

        fi_results = parse_first_instance_search(search_html, court)
        fi_results_by_court.append((court, fi_results))

        # Промоушен материала → 2-XXX до фильтра new_fi.
        for r in fi_results:
            mat = (r.get("material_number") or "").strip()
            if not mat or mat == r["case_number"]:
                continue
            old = case_by_id.get(mat)
            if old is None:
                continue
            new_id = r["case_number"]
            log.info(f"  Промоушен материала: {mat} → {new_id}")
            old["id"] = new_id
            fi = old.setdefault("first_instance", {})
            fi["case_number"] = new_id
            if r.get("judge"):
                fi["judge"] = r["judge"]
            if r.get("link"):
                fi["link"] = r["link"]
            if r.get("status"):
                fi["status"] = r["status"]
            case_by_id.pop(mat, None)
            case_by_id[new_id] = old
            existing_ids.discard(mat)
            existing_ids.add(new_id)

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

    # Re-link дел, вернувшихся из кассации в 1-ю инст. (awaiting_relink →
    # first_instance, новый раунд). Делается ПОСЛЕ накопления fi_results_by_court
    # и ДО фильтра new_fi, потому что таким делам нужен полный сброс блоков
    # first_instance/appeal/cassation в history, а не очередное обновление.
    relinked_to_fi = relink_awaiting_relink_first_instance(cases, fi_results_by_court)
    if relinked_to_fi:
        # Список case.id, которые мы только что воскресили, — чтобы дальше
        # их не дублировать в new_fi (они уже в cases с current_stage=first_instance).
        for r in relinked_to_fi:
            existing_ids.add(r["case"]["id"])

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
    csv_cases, changes, ap_skip_stats = update_active_cases(
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
    # Smart-skip счётчики
    fi_skipped_future = 0
    fi_skipped_suspended = 0
    fi_force_parsed = 0
    fi_parsed = 0

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

        # Smart-skip: пропускаем карточки с известной будущей активностью
        # (заседание/беседа/подг./предв./«без движения») до даты+1.
        skip, reason = should_skip_case(case_j, today)
        if skip:
            if reason.startswith("future_hearing"):
                fi_skipped_future += 1
            else:
                fi_skipped_suspended += 1
            log.debug(f"  skip {fi.get('case_number','?')}: {reason}")
            continue
        # Force-parse счётчик: парсим, но planned_date в будущем — значит
        # last_checked_at был ≥21 дня назад (страховочный прогон).
        planned_fp, _kind_fp = get_next_planned_date(fi.get("events") or [])
        if planned_fp and planned_fp >= today:
            fi_force_parsed += 1

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

        # Smart-skip: фиксируем дату успешного парсинга карточки (используется
        # для force-parse раз в 21 день).
        fi["last_checked_at"] = today.isoformat()
        fi_parsed += 1

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
            events_fi = card_info.get("_events") or []
            # Ищем session-событие на эту же дату (Судебное заседание /
            # Подготовка дела / Собеседование / Беседа / Предварительное).
            # Если ничего не нашлось — поле «Дата заседания» в карточке
            # суда не подкреплено реальным событием движения дела
            # (артефакт парсинга, обычно совпадает с датой подачи иска).
            matched_ev = next(
                (ev for ev in events_fi
                 if ev.get("date") == new_hearing_date
                 and _SESSION_START_RX.search(ev.get("text") or "")),
                None,
            )
            if not matched_ev:
                # Фантомная дата — эмитим fi_hearing_new с честной пометкой
                # «дата и время не опубликованы», без подсовывания фантома
                # в детали (рендер ничего не вытащит).
                change["type"].append("fi_hearing_new")
                change["details"]["hearing_date_unpublished"] = True
            else:
                new_h_dt_fi = parse_date(new_hearing_date)
                # Узкая проверка: в прошлом было настоящее судебное
                # заседание (regular/предварительное)?
                has_court_hearing = _has_held_prior_hearing(
                    events_fi, new_h_dt_fi
                )
                # Широкая проверка: было хоть какое-то session-событие
                # (включая подготовку/собеседование/беседу)?
                has_any_session = _has_held_prior_session(
                    events_fi, new_h_dt_fi
                )
                # Тройная классификация:
                #   - первое (ничего не было)
                #   - перенос (было суд. заседание → переносим)
                #   - переход «подготовка → заседание» (был только
                #     подготовительный этап — собеседование / беседа)
                if not old_hearing_date or not has_any_session:
                    change["type"].append("fi_hearing_new")
                elif has_court_hearing:
                    change["type"].append("fi_hearing_postponed")
                    change["details"]["old_hearing_date"] = old_hearing_date
                    change["details"]["old_hearing_time"] = old_hearing_time
                else:
                    change["type"].append("fi_hearing_next")
                change["details"]["hearing_date"] = new_hearing_date
                change["details"]["hearing_time"] = new_hearing_time
                # Тип заседания (беседа / предварительное / подготовка /
                # заседание) — нужен LLM для 3.2, чтобы не писать
                # обобщённое «заседание» вместо конкретики.
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
                change["details"]["decision_date"] = (
                    change["details"].get("decision_date")
                    or fi.get("hearing_date", "")
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

        # Мотивировка изготовлена, но текст акта (act_text) ещё не получен —
        # юристу нужно знать, чтобы пойти забрать решение в суде. Идемпотентно
        # через флаг fi["motivirovka_emitted"]: эмит происходит один раз —
        # в момент, когда впервые видим маркер мотивировки в last_event.
        # Не зависит от изменения last_event между прогонами (`fi_final_event`
        # стреляет ТОЛЬКО при изменении, и если карточка обновилась раньше,
        # юрист пропустит сигнал). Сброс флага не делаем: появление act_text
        # закроет тему естественным путём через fi_act_text_published (3.6).
        last_ev_str = (fi.get("last_event") or "")
        last_ev_l = last_ev_str.lower()
        has_motiv_marker = (
            "изготовлено" in last_ev_l
            and "мотивированное решение" in last_ev_l
        )
        already_have_act_text = bool((fi.get("act_text") or "").strip())
        already_emitted = bool(fi.get("motivirovka_emitted", False))
        # Не дублируем: если в этом же прогоне уже сработал fi_final_event
        # на той же фразе «изготовлено мотивированное решение» — он уже
        # говорит LLM ту же вещь. Ставим только флаг (чтобы в следующем
        # прогоне fi_motivirovka_emitted не повторил).
        ff_event_l = ""
        if "fi_final_event" in change["type"]:
            ff_event_l = (change["details"].get("event") or "").lower()
        final_already_covers_motiv = (
            "изготовлено" in ff_event_l
            and "мотивированное решение" in ff_event_l
        )
        if (has_motiv_marker
                and not already_have_act_text
                and not already_emitted):
            if final_already_covers_motiv:
                # fi_final_event уже понесёт сообщение — просто ставим флаг,
                # чтобы в следующем прогоне fi_motivirovka_emitted не выстрелил.
                fi["motivirovka_emitted"] = True
                changed = True
            else:
                m_md = re.search(r'(\d{2}\.\d{2}\.\d{4})', last_ev_str)
                motivirovka_date = (
                    m_md.group(1) if m_md else (fi.get("event_date") or "")
                )
                change["type"].append("fi_motivirovka_emitted")
                change["details"]["motivirovka_date"] = motivirovka_date
                fi["motivirovka_emitted"] = True
                changed = True

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
    fi_total = len(fi_active)
    fi_skip_total = fi_skipped_future + fi_skipped_suspended
    log.info(
        f"1 инст: {fi_parsed}/{fi_total} парсинг "
        f"(skip {fi_skip_total}: {fi_skipped_future} заседание, "
        f"{fi_skipped_suspended} без движения; force-parsed {fi_force_parsed})"
    )
    ap_skip_total = ap_skip_stats["skipped_future"] + ap_skip_stats["skipped_suspended"]
    log.info(
        f"Апелляция: {ap_skip_stats['parsed']}/{ap_skip_stats['total']} парсинг "
        f"(skip {ap_skip_total}: {ap_skip_stats['skipped_future']} заседание, "
        f"{ap_skip_stats['skipped_suspended']} без движения; "
        f"force-parsed {ap_skip_stats['force_parsed']})"
    )
    log.info(f"Обновлено дел 1 инстанции: {fi_update_count}")

    # ── 4c. Кассация (7kas.sudrf.ru) ──
    # Поиск только первая страница (по решению пользователя). Фильтр HMAO —
    # внутри parse_cassation_search_page по match_hmao_first_instance.
    # Дополнительно проверяем sber_present в карточке (УЧАСТНИКИ), т.к.
    # поиск иногда матчит по случайному совпадению в тексте.
    t0 = time.perf_counter()
    cass_changes: list[dict] = []
    cass_discovered: list[dict] = []
    cass_eligible = 0
    cass_parsed = 0
    cass_skipped_future = 0
    cass_skipped_suspended = 0
    try:
        log.info("⚖️ Поиск дел Сбербанка на 7kas.sudrf.ru...")
        polite_delay()
        cass_search_html = fetch_page(CASSATION_COURT.search_url())
        if cass_search_html:
            cass_search_results = parse_cassation_search_page(cass_search_html)
            hmao_results = [r for r in cass_search_results if r["fi_court_config"]]
            log.info(
                f"  7kas: всего {len(cass_search_results)} дел, "
                f"HMAO {len(hmao_results)}, не-HMAO отброшено "
                f"{len(cass_search_results) - len(hmao_results)}"
            )

            # Индекс существующих дел по номеру 1-й инст. — для smart-skip
            # (discovery-кейсы остаются вне индекса и парсятся всегда).
            cass_fi_index: dict[str, dict] = {}
            for c in cases:
                fi = c.get("first_instance") or {}
                n = (fi.get("case_number") or c.get("id") or "").strip()
                if n:
                    cass_fi_index.setdefault(n, c)

            today_for_skip = date.today()
            cass_finds: list[dict] = []
            for r in hmao_results:
                cass_eligible += 1
                fi_num_search = (r.get("fi_case_number") or "").strip()
                existing_case = cass_fi_index.get(fi_num_search) if fi_num_search else None
                if existing_case and existing_case.get("current_stage") == "cassation":
                    skip, reason = should_skip_case(existing_case, today_for_skip)
                    if skip:
                        if "future_hearing" in reason:
                            cass_skipped_future += 1
                        else:
                            cass_skipped_suspended += 1
                        log.info(
                            f"  7kas: skip {r['cassation_internal_number']} "
                            f"({fi_num_search}): {reason}"
                        )
                        continue
                polite_delay()
                card_url = CASSATION_COURT.card_url(r["case_id"], r["case_uid"])
                card_html = fetch_page(card_url)
                if not card_html:
                    log.warning(
                        f"  7kas: не удалось загрузить карточку "
                        f"{r['cassation_internal_number']}"
                    )
                    continue
                info = parse_cassation_card(card_html, CASSATION_COURT.base_url)
                if not info:
                    log.warning(
                        f"  7kas: не удалось распарсить карточку "
                        f"{r['cassation_internal_number']}"
                    )
                    continue
                if not info.get("sber_present"):
                    log.info(
                        f"  7kas: пропуск {r['cassation_internal_number']} — "
                        f"Сбербанка нет в УЧАСТНИКАХ"
                    )
                    continue
                # Подмержим поля из выдачи (link, cassation_internal_number,
                # fi_court_config, fi_case_number — у info уже всё это есть, но
                # link нет: его нужно собрать из case_id|case_uid).
                info["link"] = f"{r['case_id']}|{r['case_uid']}"
                info["cassation_internal_number"] = r["cassation_internal_number"]
                # Если в карточке fi_case_number пустой (редко) — берём из выдачи.
                if not info.get("fi_case_number") and r.get("fi_case_number"):
                    info["fi_case_number"] = r["fi_case_number"]
                cass_finds.append(info)
                cass_parsed += 1

            cases, cass_changes, cass_discovered = link_cassation_cases(
                cases, cass_finds
            )
        else:
            log.warning("7kas: пустой ответ от поиска")
    except Exception as exc:
        # Кассация — третий парсер, его падение не должно ронять весь прогон.
        # Просто логируем и идём дальше с пустыми cass_changes/cass_discovered.
        log.warning(f"7kas: ошибка прогона: {exc}", exc_info=True)
    cass_skip_total = cass_skipped_future + cass_skipped_suspended
    log.info(
        f"Кассация: {cass_parsed}/{cass_eligible} парсинг "
        f"(skip {cass_skip_total}: {cass_skipped_future} заседание, "
        f"{cass_skipped_suspended} без движения)"
    )
    timings["cassation"] = time.perf_counter() - t0

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

    # ── 6b. Новые апел. дела → JSON. Без этого link_cases ниже их не увидит
    # (он индексирует только существующий cases) и дело осядет только в CSV.
    if appeal_new_cases_csv:
        apel_new_json = [_apel_csv_row_to_json_case(r, appeal_fi_numbers) for r in appeal_new_cases_csv]
        cases = apel_new_json + cases
        log.info(f"Добавлено {len(apel_new_json)} апел. дел в JSON")

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
    # Касс. — дела на стадиях `cassation_pending` (жалоба ушла, ждём карточку
    # на 7kas) и `cassation` (карточка появилась, рассматривается). Архивные
    # отсечены через is_case_archived.
    total_active_cassation = sum(
        1 for c in cases
        if c.get("current_stage") in ("cassation_pending", "cassation")
        and not is_case_archived(c)
    )
    t0 = time.perf_counter()
    log.info("Генерирую дайджест...")
    save_digest_context(
        appeal_new_cases_csv, changes, cases=csv_cases,
        fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
        fi_changes=fi_changes,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
        total_active_cassation=total_active_cassation,
        cass_changes=cass_changes,
        cass_discovered=cass_discovered,
    )
    digest = generate_digest(
        appeal_new_cases_csv, changes, cases=csv_cases,
        fi_new_cases=fi_new_cases, stage_transitions=stage_transitions,
        fi_changes=fi_changes,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
        total_active_cassation=total_active_cassation,
        cass_changes=cass_changes,
        cass_discovered=cass_discovered,
    )
    timings["digest"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    send_telegram(digest)
    timings["telegram"] = time.perf_counter() - t0

    # Web Push — краткое уведомление при наличии изменений, разбивка по типам
    push_new = len(fi_new_cases) + len(appeal_new_cases_csv) + len(cass_discovered)
    push_changes = len(fi_changes) + len(changes) + len(cass_changes)
    push_stages = len(stage_transitions)
    push_summary = ""
    if push_new + push_changes + push_stages > 0:
        parts = []
        if push_new:
            parts.append(f"🆕 Новых: {push_new}")
        if push_changes:
            parts.append(f"📋 Изменений: {push_changes}")
        if push_stages:
            parts.append(f"🔄 Переходов: {push_stages}")
        push_summary = " · ".join(parts)

        send_web_push(
            title="Мониторинг дел — обновление",
            body=push_summary,
            per_subscriber=_make_per_sub_callback(
                fi_new_cases=fi_new_cases,
                fi_changes=fi_changes,
                changes=changes,
                stage_transitions=stage_transitions,
                appeal_new_cases_csv=appeal_new_cases_csv,
                push_summary=push_summary,
                cass_changes=cass_changes,
                cass_discovered=cass_discovered,
            ),
        )

    # Сохраняем готовый дайджест для фронта (блок «Последний дайджест»).
    digest_is_empty = not (push_new + push_changes + push_stages)
    save_last_digest(
        digest,
        summary=push_summary,
        is_empty=digest_is_empty,
    )

    # Привязываем LLM-разбор опубликованных актов к делам в cases.json,
    # чтобы юрист видел его в drawer (и чтобы он жил дольше одного дня).
    # Поле `act_analysis` обновляется только у дел с new_act /
    # fi_act_text_published в этом прогоне; остальные не трогаем.
    act_analyses_updated = attach_act_analyses(
        cases,
        digest,
        all_changes=list(changes) + list(fi_changes),
        is_empty=digest_is_empty,
    )
    if act_analyses_updated:
        # Дописываем поле в уже сохранённый ранее cases.json. save_json
        # поверх — единственный безопасный способ донести изменение до
        # фронта (atomic-write через временный файл уже встроен).
        data["cases"] = cases
        save_json(data, JSON_PATH)

    timings["total"] = time.perf_counter() - t_total_start

    log_run_summary(
        mode="main-json",
        timings=timings,
        extras={
            "FI courts": len(enabled_courts),
            "FI new": len(fi_new_cases),
            "FI updated": fi_update_count,
            "FI changes": len(fi_changes),
            "FI parse": f"{fi_parsed}/{fi_total}",
            "FI skip": fi_skip_total,
            "FI force": fi_force_parsed,
            "Stage transitions": len(stage_transitions),
            "Appeal new": len(appeal_new_cases_csv),
            "Appeal changes": len(changes),
            "Appeal parse": f"{ap_skip_stats['parsed']}/{ap_skip_stats['total']}",
            "Appeal skip": ap_skip_total,
            "Appeal force": ap_skip_stats["force_parsed"],
            "JSON total": len(cases),
        },
    )


def main_replay_last(push_all: bool = False):
    """Прогнать дайджест заново из LAST_DIGEST_CONTEXT_PATH.

    Используется для экспериментов с промптом/форматом: после любого
    продового прогона контекст лежит в `data/last_digest_context.json`,
    и этот режим пересоздаёт дайджест на тех же данных без повторного
    парсинга судов. Полезно, когда хочется проверить, как отработает
    изменённый промпт на реальных изменениях последнего дня.

    `push_all=False` (по умолчанию) — push только устройствам-владельцам;
    `push_all=True` — push всем PWA-подписчикам (включая коллег).
    Управляется флагом `--push-all` в CLI.
    Telegram-чат (личный/группа) выбирается через env `TELEGRAM_CHAT_ID`
    в workflow.
    """
    log.info("=" * 60)
    log.info(
        "Режим replay-last: дайджест из сохранённого контекста "
        f"(push: {'все устройства' if push_all else 'только владельцу'})"
    )
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

    # Fallback: если контекст сохранён до появления total_active_cassation
    # (старый ctx-payload), считаем из data/cases.json — там state-machine
    # с current_stage. ctx["cases"] хранит CSV-апелляцию без current_stage,
    # из неё кассацию не вытащить.
    total_active_cassation = ctx.get("total_active_cassation")
    if not total_active_cassation:
        try:
            json_cases = load_json(JSON_PATH).get("cases", [])
            total_active_cassation = sum(
                1 for c in json_cases
                if c.get("current_stage") in ("cassation_pending", "cassation")
                and not is_case_archived(c)
            )
        except Exception as exc:
            log.warning(f"Не удалось пересчитать total_active_cassation: {exc}")
            total_active_cassation = 0

    saved_at = ctx.get("saved_at", "?")
    log.info(f"Контекст от {saved_at}: "
             f"changes={len(ctx.get('changes', []))}, "
             f"fi_changes={len(ctx.get('fi_changes', []))}, "
             f"cass_changes={len(ctx.get('cass_changes', []))}, "
             f"new_cases={len(ctx.get('new_cases', []))}, "
             f"fi_new={len(ctx.get('fi_new_cases', []))}, "
             f"cass_disc={len(ctx.get('cass_discovered', []))}, "
             f"transitions={len(ctx.get('stage_transitions', []))}, "
             f"касс.={total_active_cassation}")

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
        total_active_cassation=total_active_cassation,
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )

    send_telegram(digest)
    replay_is_empty = not (
        ctx.get("new_cases") or ctx.get("changes")
        or ctx.get("fi_new_cases") or ctx.get("stage_transitions")
        or ctx.get("fi_changes")
        or ctx.get("cass_changes") or ctx.get("cass_discovered")
    )
    summary = build_summary_line(
        ctx.get("new_cases", []),
        ctx.get("changes", []),
        ctx.get("fi_new_cases", []),
        ctx.get("stage_transitions", []),
        ctx.get("fi_changes", []),
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )
    save_last_digest(digest, summary=summary or "(replay)", is_empty=replay_is_empty)

    # Replay переигрывает дайджест на тех же данных — обновим разбор актов
    # в cases.json (актуально, если правили промпт и хотим, чтобы новый
    # вариант разбора попал в drawer карточки дела).
    try:
        data = load_json(JSON_PATH)
        cases = data.get("cases", [])
        updated = attach_act_analyses(
            cases,
            digest,
            all_changes=list(ctx.get("changes", [])) + list(ctx.get("fi_changes", [])),
            is_empty=replay_is_empty,
        )
        if updated:
            data["cases"] = cases
            save_json(data, JSON_PATH)
    except Exception as exc:
        log.warning(f"act_analysis (replay): не удалось обновить cases.json: {exc}")

    body = summary if summary else f"Открой приложение — дайджест от {saved_at[:10]}"
    title = (
        "Мониторинг дел — тестовая рассылка"
        if push_all else "Мониторинг дел — тестовая рассылка (только владельцу)"
    )
    send_web_push(
        title=title,
        body=body,
        click_url="/sberbank_dashboard.html?digest=open",
        owner_only=not push_all,
        per_subscriber=_make_per_sub_callback(
            fi_new_cases=ctx.get("fi_new_cases", []),
            fi_changes=ctx.get("fi_changes", []),
            changes=ctx.get("changes", []),
            stage_transitions=ctx.get("stage_transitions", []),
            appeal_new_cases_csv=ctx.get("new_cases", []),
            push_summary=summary or body,
            cass_changes=ctx.get("cass_changes", []),
            cass_discovered=ctx.get("cass_discovered", []),
        ),
    )
    log.info("Готово!")


def main_push_last_digest(owner_only: bool = False):
    """Тестовый прогон: переигрывает последний дайджест через LLM из
    `data/last_digest_context.json` и шлёт push. В Telegram не отправляет —
    это режим только для проверки PWA-доставки и текущего вида дайджеста
    после правок промпта.

    `owner_only=False` (по умолчанию) — push на ВСЕ устройства;
    `owner_only=True` — только устройствам-владельцам (без коллег).
    Управляется флагом `--owner-only` в CLI.

    Шаги:
      1. Читаем контекст последнего продового прогона.
      2. Прогоняем `generate_digest` (Claude / GigaChat / template-fallback).
      3. Перезаписываем `data/last_digest.json` — фронт покажет свежий вид.
      4. Шлём web push с учётом `owner_only`.
    """
    log.info("=" * 60)
    log.info(
        "Режим push-last-digest: пуш по последнему дайджесту "
        f"({'только владельцу' if owner_only else 'все устройства'})"
    )
    log.info("=" * 60)

    # validate_environment проверит ANTHROPIC/GIGACHAT_AUTH_KEY и Telegram —
    # Telegram нам не нужен, но send_web_push также читает PUSH_*-переменные;
    # их валидация останется внутри send_web_push (логирует и тихо выходит,
    # если не настроены).
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

    # Fallback: см. main_replay_last — если ctx сохранён до появления
    # total_active_cassation, считаем из data/cases.json (state-machine).
    total_active_cassation = ctx.get("total_active_cassation")
    if not total_active_cassation:
        try:
            json_cases = load_json(JSON_PATH).get("cases", [])
            total_active_cassation = sum(
                1 for c in json_cases
                if c.get("current_stage") in ("cassation_pending", "cassation")
                and not is_case_archived(c)
            )
        except Exception as exc:
            log.warning(f"Не удалось пересчитать total_active_cassation: {exc}")
            total_active_cassation = 0

    saved_at = ctx.get("saved_at", "?")
    log.info(f"Контекст от {saved_at}: "
             f"changes={len(ctx.get('changes', []))}, "
             f"fi_changes={len(ctx.get('fi_changes', []))}, "
             f"cass_changes={len(ctx.get('cass_changes', []))}, "
             f"new_cases={len(ctx.get('new_cases', []))}, "
             f"fi_new={len(ctx.get('fi_new_cases', []))}, "
             f"cass_disc={len(ctx.get('cass_discovered', []))}, "
             f"transitions={len(ctx.get('stage_transitions', []))}, "
             f"касс.={total_active_cassation}")

    log.info("Генерирую дайджест через LLM...")
    digest = generate_digest(
        ctx.get("new_cases", []),
        ctx.get("changes", []),
        cases=ctx.get("cases", []),
        fi_new_cases=ctx.get("fi_new_cases", []),
        stage_transitions=ctx.get("stage_transitions", []),
        fi_changes=ctx.get("fi_changes", []),
        total_active_appeal=ctx.get("total_active_appeal", 0),
        total_active_fi=ctx.get("total_active_fi", 0),
        total_active_cassation=total_active_cassation,
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )

    is_empty = not (
        ctx.get("new_cases") or ctx.get("changes")
        or ctx.get("fi_new_cases") or ctx.get("stage_transitions")
        or ctx.get("fi_changes")
        or ctx.get("cass_changes") or ctx.get("cass_discovered")
    )
    summary = build_summary_line(
        ctx.get("new_cases", []),
        ctx.get("changes", []),
        ctx.get("fi_new_cases", []),
        ctx.get("stage_transitions", []),
        ctx.get("fi_changes", []),
        cass_changes=ctx.get("cass_changes", []),
        cass_discovered=ctx.get("cass_discovered", []),
    )
    save_last_digest(digest, summary=summary, is_empty=is_empty)

    body = summary if summary else f"Открой приложение — дайджест от {saved_at[:10]}"
    title = (
        "Мониторинг дел — тестовая рассылка (только владельцу)"
        if owner_only else "Мониторинг дел — тестовая рассылка"
    )
    log.info(f"Push body: {body!r}")
    send_web_push(
        title=title,
        body=body,
        click_url="/sberbank_dashboard.html?digest=open",
        owner_only=owner_only,
        per_subscriber=_make_per_sub_callback(
            fi_new_cases=ctx.get("fi_new_cases", []),
            fi_changes=ctx.get("fi_changes", []),
            changes=ctx.get("changes", []),
            stage_transitions=ctx.get("stage_transitions", []),
            appeal_new_cases_csv=ctx.get("new_cases", []),
            push_summary=summary or body,
            cass_changes=ctx.get("cass_changes", []),
            cass_discovered=ctx.get("cass_discovered", []),
        ),
    )
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
    total_active_cassation = sum(
        1 for c in json_cases
        if c.get("current_stage") in ("cassation_pending", "cassation")
        and not is_case_archived(c)
    )
    log.info(
        f"В производстве: всего"
        f" {total_active_appeal + total_active_fi + total_active_cassation}"
        f" (1 инст.: {total_active_fi} | апел.: {total_active_appeal}"
        f" | касс.: {total_active_cassation})"
    )

    log.info("Генерирую дайджест...")
    digest = generate_digest(
        [], [], cases=cases,
        total_active_appeal=total_active_appeal,
        total_active_fi=total_active_fi,
        total_active_cassation=total_active_cassation,
    )

    send_telegram(digest)
    send_web_push(
        title="Мониторинг дел — проверка",
        body="Дайджест по текущим данным",
        owner_only=True,
    )
    # digest-only вызывается с пустыми new_cases/changes — это всегда
    # «no-changes» дайджест по текущим данным.
    save_last_digest(digest, summary="(digest-only)", is_empty=True)
    log.info("Готово!")


if __name__ == "__main__":
    # Выбор режима
    if "--replay-last" in sys.argv:
        push_all = "--push-all" in sys.argv
        mode_name = (
            "replay-last (push-all)" if push_all else "replay-last"
        )
        entry = main_replay_last
        entry_args: tuple = (push_all,)
    elif "--digest-only" in sys.argv:
        mode_name = "digest-only"
        entry = main_digest_only
        entry_args = ()
    elif "--push-last-digest" in sys.argv:
        # `--owner-only` ограничивает рассылку устройствами-владельцами;
        # без флага push идёт всем подписчикам PWA.
        owner_only = "--owner-only" in sys.argv
        mode_name = (
            "push-last-digest (owner-only)" if owner_only else "push-last-digest"
        )
        entry = main_push_last_digest
        entry_args = (owner_only,)
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
