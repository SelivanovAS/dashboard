# -*- coding: utf-8 -*-
"""Парсинг поисковой выдачи: апелляция (Суд ХМАО) и 20 судов 1-й инстанции.
Фильтр «настоящий Сбербанк» против дочек/страховых, определение роли банка.
"""

from __future__ import annotations

import re

from court_monitor import config
from court_monitor.config import log
from court_monitor.courts import CourtConfig, JUDICIAL_UID_RE
from court_monitor.parsing.tables import extract_tables, cell_text, cell_href
from court_monitor.textutil import (
    _strip_html, _CASE_NUM_RE, _FI_CASE_NUM_RE, _TIME_RE,
    _CASE_ID_RE, _CASE_UID_RE, parse_date,
)

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


def _is_real_sberbank(name: str) -> bool:
    """True, если имя содержит ПАО Сбербанк (не дочку: страхование/НПФ/лизинг/УК).
    Возвращает False для пустых имён и для строк без подстроки «сбербанк»."""
    nm = (name or "").lower()
    if "сбербанк" not in nm:
        return False
    cleaned = nm
    for pat in _SBER_SUBSIDIARY_PATTERNS:
        cleaned = pat.sub("", cleaned)
    return "сбербанк" in cleaned


def determine_bank_role_from_participants(participants: list[dict]) -> str:
    """Вернуть фактическую роль ПАО Сбербанк по списку участников карточки.

    participants: список dict с ключами 'role' (вид участника, напр. ИСТЕЦ /
    ОТВЕТЧИК / ТРЕТЬЕ ЛИЦО) и 'name' (наименование стороны).

    Возвращает:
    - «Истец» / «Ответчик» / «Третье лицо» — если ПАО Сбербанк найден среди
      участников хотя бы один раз. При нескольких вхождениях приоритет:
      Ответчик > Истец > Третье лицо (банк может быть упомянут в разных ролях,
      но «Ответчик» — самая значимая для исхода).
    - "" (пустая строка) — если ПАО Сбербанка нет среди участников вообще
      (только дочки или вовсе нет). Внешний код решает, что с этим делать:
      для 1-й инстанции = «Третье лицо» (нейтрально), для кассации = drop.
    """
    found_roles: set[str] = set()
    for p in participants or []:
        if not _is_real_sberbank(p.get("name") or ""):
            continue
        role_up = (p.get("role") or "").upper()
        if "ОТВЕТЧИК" in role_up:
            found_roles.add("Ответчик")
        elif "ИСТЕЦ" in role_up or "ЗАЯВИТЕЛЬ" in role_up:
            found_roles.add("Истец")
        else:
            found_roles.add("Третье лицо")
    if "Ответчик" in found_roles:
        return "Ответчик"
    if "Истец" in found_roles:
        return "Истец"
    if "Третье лицо" in found_roles:
        return "Третье лицо"
    return ""


# Синонимы процессуальных ролей: приказное/особое/административное
# производство подписывает стороны иначе, чем исковое. Без них у дел
# категории «прочие» стороны оставались пустыми, и запись в дайджесте
# схлопывалась до голого номера (инцидент 24.07.2026, 8Г-12479/2026 —
# кассация Урала: в карточке 7kas роли были не ИСТЕЦ/ОТВЕТЧИК).
# «АДМИНИСТРАТИВНЫЙ ИСТЕЦ/ОТВЕТЧИК» отдельно не перечисляем: подстроки
# ИСТЕЦ/ОТВЕТЧИК ловит первый проход.
_PLAINTIFF_ROLE_SYNONYMS = ("ЗАЯВИТЕЛЬ", "ВЗЫСКАТЕЛЬ")
_DEFENDANT_ROLE_SYNONYMS = ("ЗАИНТЕРЕСОВАННОЕ ЛИЦО", "ДОЛЖНИК")


def parties_from_participants(participants: list[dict]) -> tuple[str, str]:
    """Вернуть (истцовая сторона, ответная сторона) по списку участников.

    participants: список dict с ключами 'role' (вид участника) и 'name' —
    в форме, которую отдают parse_case_card и parse_cassation_card.

    Два прохода: сначала точные ИСТЕЦ/ОТВЕТЧИК, потом синонимы. Порядок
    важен — «ЗАЯВИТЕЛЬ», стоящий в таблице выше настоящего ИСТЦА (типично
    для карточек 7kas, где заявитель кассации попадает в УЧАСТНИКИ), не
    должен перебивать сторону по существу спора.

    Роли, которые сторонами не являются (ПРЕДСТАВИТЕЛЬ, ПРОКУРОР,
    ТРЕТЬЕ ЛИЦО), не берём вовсе. Если ничего не распозналось — ("", "").
    """
    plaintiff = ""
    defendant = ""
    for exact_pass in (True, False):
        for p in participants or []:
            role_up = (p.get("role") or "").upper()
            name = (p.get("name") or "").strip()
            if not name:
                continue
            if exact_pass:
                is_pl = "ИСТЕЦ" in role_up
                is_df = "ОТВЕТЧИК" in role_up
            else:
                is_pl = any(s in role_up for s in _PLAINTIFF_ROLE_SYNONYMS)
                is_df = any(s in role_up for s in _DEFENDANT_ROLE_SYNONYMS)
            if is_pl and not plaintiff:
                plaintiff = name
            elif is_df and not defendant:
                defendant = name
    return plaintiff, defendant


def parse_search_page(html: str) -> list[dict]:
    """
    Парсит страницу результатов поиска.
    Таблица результатов ищется по заголовку («№ дела» + дата) через
    _find_results_table — как у 1-й инстанции. Раньше брали жёстко 6-ю
    таблицу (индекс 5), но 14.07.2026 апелляция добавила блок в вёрстку,
    индексы уехали и поиск «молча» вернул 0 при живой выдаче (инцидент
    поймал детектор здоровья парсеров).
    Столбцы: Номер дела (ссылка) | Дата поступления |
             Категория/Стороны/Суд (объединённая) | Судья | ...
    """
    tables = extract_tables(html)
    results_table = _find_results_table(tables)
    if results_table is None:
        log.warning(
            f"Апелляция: таблица результатов поиска не найдена "
            f"(таблиц на странице: {len(tables)})"
        )
        return []

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
        if any(p in plaintiff_lower for p in config.SBER_PATTERNS):
            role = "Истец"
        elif any(p in defendant_lower for p in config.SBER_PATTERNS):
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

    Индексы плавают и меняются при правках вёрстки суда (апелляция: до
    14.07.2026 — 5, после — 6; 1-я инстанция — 8+), поэтому единственный
    надёжный способ — искать по содержимому заголовка. Используется и
    апелляционным parse_search_page, и parse_first_instance_search.
    """
    for tbl in tables:
        if len(tbl) < 2:
            continue
        header_text = " ".join(cell_text(c) for c in tbl[0]).lower()
        if "дела" in header_text and ("дата" in header_text or "поступлен" in header_text):
            return tbl
    return None


_SRV_NUM_RE = re.compile(r"srv_num=(\d+)")


# ── Детект страницы с проверочным кодом (CAPTCHA) ────────────────────────────
# Некоторые суды sudrf.ru закрывают поиск проверочным кодом (картинка на форме
# name_op=sf). Парсер бьёт напрямую в name_op=r, минуя форму, поэтому такая
# страница приходит как 200 + непустой HTML без таблицы результатов — и молча
# читается как «дел нет». Хелпер отличает её от легитимно пустой выдачи, чтобы
# runs.py мог поднять 🩺-алерт «суд требует ввод кода — проверить вручную».
#
# ⚠️ Это READ-ONLY классификация: код НЕ читаем, не декодируем, не распознаём и
# не отправляем — только сообщаем, что страница им закрыта. Обход антибот-защиты
# не делаем (суды включили код против авто-сбора; авто-решатель привёл бы к бану
# IP всего мониторинга). Проход кода — только через ввод человеком, отдельно.

# Сильные маркеры разметки капчи sudrf (низкий риск ложняка).
_CAPTCHA_STRONG_MARKERS = (
    "captcha.php",
    'name="captcha"',
    "name='captcha'",
    'id="captcha"',
    "id='captcha'",
    "captcha_image",
    "captchaimage",
)
# Текстовые подсказки страницы ввода кода (матчим по html.lower()).
_CAPTCHA_PHRASES = (
    "код с картинки",
    "код с изображения",
    "проверочный код",
    "введите код",
    "изображённый на картинке",
    "изображенный на картинке",
    "защита от автоматических запросов",
    "проверка на робот",
)
# Признак легитимно пустой выдачи — это НЕ код.
_NO_DATA_MARK = "данных по запросу не обнаружено"

# Фразы для КАРТОЧКИ дела — строгое подмножество _CAPTCHA_PHRASES плюс текст
# страницы-ошибки. Генерические «проверочный код»/«введите код» сюда НЕ входят:
# карточка содержит полные тексты актов, а сбер-споры о мошенничестве дословно
# цитируют СМС («ввела проверочный код», «введите код из сообщения») — на
# карточках такие фразы дают ложняк и дело выпало бы из мониторинга навсегда.
# Оставшиеся фразы привязаны к картинке/роботам — в текстах актов не встречаются.
_CAPTCHA_CARD_PHRASES = (
    "неверно указан проверочный код",  # страница-ошибка name_op=* без кода
    "код с картинки",
    "код с изображения",
    "изображённый на картинке",
    "изображенный на картинке",
    "защита от автоматических запросов",
    "проверка на робот",
)


def is_no_data_page(html: str) -> bool:
    """True, если страница — легитимно пустая выдача sudrf («Данных по
    запросу не обнаружено»). Для целевых запросов (дослинк апелляции по
    номеру 1-й инст.) это штатный ответ «апелляция ещё не зарегистрирована» —
    вызывающий код не гоняет такую страницу через parse_search_page, чтобы
    не плодить WARNING «таблица результатов не найдена»."""
    return bool(html) and _NO_DATA_MARK in html.lower()


def detect_captcha_challenge(html: str) -> bool:
    """READ-ONLY: True, только если html — страница с проверочным кодом.

    Код НЕ читает, не декодирует, не распознаёт и не отправляет — только
    классифицирует. Ожидает уже декодированный из win-1251 str (как отдаёт
    netutil.fetch_page). Условие «0 строк результата» остаётся у вызывающего
    кода — здесь только про сам HTML.

    Голый name_op=sf / подстроку "captcha" маркерами НЕ берём: форма поиска
    присутствует и на нормальных страницах результатов (ложняк). Финальный
    набор маркеров подтверждается по реальному дампу (scripts/probe_captcha.py).
    """
    if not html:
        return False
    low = html.lower()
    if _NO_DATA_MARK in low:
        return False  # легитимно пустая выдача, а не код
    if any(m in low for m in _CAPTCHA_STRONG_MARKERS):
        return True
    return any(p in low for p in _CAPTCHA_PHRASES)


def detect_captcha_challenge_card(html: str) -> bool:
    """READ-ONLY: True, если вместо КАРТОЧКИ дела пришла страница с кодом.

    Отличается от detect_captcha_challenge (поиск) набором фраз: карточка
    содержит полные тексты судебных актов, где генерические фразы
    («проверочный код», «введите код») встречаются в цитатах СМС по делам
    о мошенничестве — см. _CAPTCHA_CARD_PHRASES. Маркеры разметки капчи
    (_CAPTCHA_STRONG_MARKERS) остаются в силе: легитимная карточка
    формы капчи не содержит.
    """
    if not html:
        return False
    low = html.lower()
    if any(m in low for m in _CAPTCHA_STRONG_MARKERS):
        return True
    return any(p in low for p in _CAPTCHA_CARD_PHRASES)


# ── Детект «страница-не-карточка»: заглушка недоступности / антибот-блок ─────
# Аутейдж 20.07.2026: суды sudrf отдавали HTTP 200 со штатной страницей
# «Информация временно недоступна…» вместо карточек. Капча-детектор её не ловит
# (фраз кода там нет), parse_case_card видел 0 таблиц, а FI-цикл засчитывал
# заглушку как успешную проверку (бумп last_checked_at + fi_parsed) — прогон
# отчитался «спарсено 47 из 75» при ~1 реально прочитанной карточке.

# Фразы-«хром» страницы недоступности ГАС «Правосудие». В карточках и текстах
# актов их не бывает; одиночное совпадение (теоретическая цитата в акте)
# отсекается правилом «≥2 фраз ИЛИ 1 фраза при отсутствии якоря-УИД».
_OUTAGE_MARKERS = (
    "информация временно недоступна",
    "приносим свои извинения",
    "обратитесь непосредственно в суд",
)
# Инфраструктурные маркеры блокировщиков (разметка) — в судебных документах
# не встречаются, безопасны на любых страницах. meta-refresh сюда намеренно
# НЕ входит (слишком генерический; добавлять только по реальному дампу пробы).
_ANTIBOT_MARKUP_MARKERS = (
    "ddos-guard",
    "_incap_",
    "incapsula",
    "cf-chl",
)
# Текстовые антибот-фразы: гейтятся отсутствием якоря-УИД — на живой карточке
# (где такая фраза мыслима лишь как цитата в акте) УИД есть всегда.
_ANTIBOT_TEXT_MARKERS = (
    "слишком много запросов",
    "too many requests",
)
# Единственный живой якорь настоящей карточки — лейбл УИД в таблице «ДЕЛО»
# (см. parse_case_card). Маркер name_op=case в ТЕЛЕ карточек не встречается
# (проверено по всем фикстурам: 0 вхождений) — в якоря его не брать.
_CARD_UID_ANCHOR = "уникальный идентификатор"


def looks_like_non_card_page(html: str, url: str = "") -> bool:
    """READ-ONLY: True, если вместо карточки пришла заглушка/блок-страница.

    Признаки (см. комментарии к наборам маркеров выше):
    1) ≥2 фраз штатной страницы недоступности sudrf — любой URL (целая
       заглушка несёт все три, а случайная цитата в акте — максимум одну);
    2) маркеры РАЗМЕТКИ антибот-блокировщиков — любой URL (в судебных
       документах не встречаются);
    3) ослабленные правила ТОЛЬКО для URL карточек (name_op=case) при
       отсутствии якоря-УИД: одиночная фраза недоступности, текстовые
       антибот-фразы, структурный фолбэк «почти нет таблиц». Страницы
       ТЕКСТОВ АКТОВ (fetch_act_text) идут тем же fetch_card_checked, но
       карточками не являются: УИД-лейбла там нет, а полный текст акта может
       дословно цитировать и «приносим свои извинения» (переписка банка), и
       «обратитесь непосредственно в суд», и «слишком много запросов» — без
       гейта по URL акт блокировался бы навсегда с ежедневным ложным алертом.

    Страница «Данных по запросу не обнаружено» — НЕ блок (легитимный ответ
    sudrf; на карточном URL значит «сид протух», это другой класс проблемы).
    Легитимная компактная карточка-«огрызок» (4 таблицы + УИД, напр.
    case_card_truncated.html) сюда тоже НЕ попадает — остаётся в cards_degraded.
    """
    if not html:
        return False
    low = html.lower()
    if _NO_DATA_MARK in low:
        return False
    outage_hits = sum(1 for m in _OUTAGE_MARKERS if m in low)
    if outage_hits >= 2:
        return True
    if any(m in low for m in _ANTIBOT_MARKUP_MARKERS):
        return True
    if "name_op=case" not in (url or ""):
        return False
    has_uid = _CARD_UID_ANCHOR in low
    if has_uid:
        return False
    if outage_hits == 1:
        return True
    if any(m in low for m in _ANTIBOT_TEXT_MARKERS):
        return True
    # Число тегов <table> без полного парса: у настоящих карточек их ≥4
    # (даже у «огрызков»), у заглушек — 0-1.
    return low.count("<table") <= 1


def find_fi_case_link(html: str, case_number: str) -> str:
    """Найти в выдаче поиска 1-й инст. строку ровно этого дела → "cid|cuid".

    Для бэкфилла ссылок на карточку (см. linking.backfill_fi_links): целевой
    поиск по номеру (G1_CASE__CASE_NUMBERSS) сервер ведёт подстрокой, поэтому
    границу номера проверяем сами — текст ячейки должен быть равен номеру или
    продолжаться скобкой/тильдой (комбо-номер вида
    «2-716/2025 (2-9422/2024;) ~ М-7693/2024»), чтобы запрос «2-71/2025» не
    сматчил строку «2-716/2025». Возвращает "case_id|case_uid" или "".
    """
    if not case_number:
        return ""
    tables = extract_tables(html)
    results_table = _find_results_table(tables)
    if not results_table:
        return ""
    boundary = re.compile(rf'^{re.escape(case_number)}\s*(?:$|[(~])')
    for row in results_table:
        if not row:
            continue
        num_cell = row[0]
        if not boundary.match(cell_text(num_cell).strip()):
            continue
        href = cell_href(num_cell)
        if not href:
            continue
        m_id = _CASE_ID_RE.search(href)
        m_uid = _CASE_UID_RE.search(href)
        if m_id and m_uid:
            return f"{m_id.group(1)}|{m_uid.group(1)}"
    return ""


def parse_first_instance_search(
    html: str, court: CourtConfig, stats: dict | None = None,
    keep_all_roles: bool = False,
) -> list[dict]:
    """Парсит страницу поиска суда первой инстанции.

    Отличия от parse_search_page (апелляция):
    - Таблица результатов ищется по заголовку, а не по индексу
    - 8 столбцов: № дела | Дата | Категория/Стороны | Судья | Дата решения | Решение | ...
    - Фильтр: только дела, где Сбербанк — ответчик
    - Номер дела может содержать '~' (материал) — берём первую часть

    stats: если передан dict, в него пишется stats["sber_rows"] — число строк
    «настоящего Сбербанка» ДО фильтра роли. Это сигнал здоровья парсера:
    вал исков самого банка вытесняет ответчик-дела со страницы 1 и обнуляет
    len(результата) без всякой поломки (Октябрьский р/с, 14.07.2026).
    Там же stats["subsidiary_rows"] — сколько строк отсеяно как «только дочка
    Сбера» (страхование, НПФ и т.п.), и stats["subsidiary_cases"] — их номера
    (импортёр показывает их оператору построчно).

    keep_all_roles=True — вернуть дела ВСЕХ ролей банка (истец/ответчик/третье
    лицо), а не только «банк-ответчик». Режим импортёра дампов
    (scripts/import_search_dump.py): оператор капчёвого суда заводит все
    сберовские дела из выдачи. Отсев дочек действует в обоих режимах.
    """
    if stats is not None:
        stats["sber_rows"] = 0
        stats["subsidiary_rows"] = 0
        stats["subsidiary_cases"] = []
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
            if stats is not None:
                stats["subsidiary_rows"] += 1
                stats["subsidiary_cases"].append(case_number)
            continue

        # Строка «настоящего Сбербанка» (не дочки) до фильтра роли — метрика
        # здоровья. «сбербанк» в объединённой ячейке, а не только в сторонах:
        # банк может фигурировать третьим лицом вне И:/О:.
        if stats is not None and "сбербанк" in combined.lower():
            stats["sber_rows"] += 1

        # Определяем роль банка
        role = "Третье лицо"
        plaintiff_lower = plaintiff.lower()
        defendant_lower = defendant.lower()
        if any(p in plaintiff_lower for p in config.SBER_PATTERNS):
            role = "Истец"
        elif any(p in defendant_lower for p in config.SBER_PATTERNS):
            role = "Ответчик"

        # Фильтр: только банк-ответчик (боевой автопоиск). Импортёр дампов
        # (keep_all_roles=True) берёт все сберовские роли.
        if not keep_all_roles and role != "Ответчик":
            continue

        link = f"{cid}|{cuid}" if cid and cuid else ""

        # srv_num из href самого суда — авторитетнее конфига для двухсерверных
        # судов (Камышловский/Красноуфимский: два сервера на одном домене,
        # резолв CourtConfig по домену даёт первый). Пишется отдельным ключом:
        # боевой путь продолжает брать court.srv_num, использует его импортёр.
        href_srv = None
        if href:
            m_srv = _SRV_NUM_RE.search(href)
            if m_srv:
                href_srv = int(m_srv.group(1))

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
            "href_srv_num": href_srv,
            "judge": judge,
            "bank_role": role,
            "status": status,
            "result": result,
            "result_date": result_date,
            "link": link,
        })

    return cases
