# -*- coding: utf-8 -*-
"""Парсинг карточки дела (1-я инстанция и апелляция): вкладки «Дело»,
«Движение дела», «Стороны», «Обжалование»; извлечение текста судебного акта.
_warn_if_card_degraded пишет счётчик «огрызков» в config.METRICS.
"""

from __future__ import annotations

import re
from datetime import datetime

from court_monitor import config
from court_monitor.config import log
from court_monitor.courts import JUDICIAL_UID_RE
from court_monitor.lifecycle import (
    _ACCEPTANCE_RX, _INTERLOCUTORY_PREP_RX, _SESSION_START_RX,
    _SUSPENDED_RX, _TERMINAL_FI_EVENT_RX, extract_result_from_event,
)
from court_monitor.courts import BASE_URL
from court_monitor.netutil import fetch_card_checked, polite_delay
from court_monitor.parsing.search import determine_bank_role_from_participants
from court_monitor.parsing.tables import extract_tables, cell_text, cell_href
from court_monitor.textutil import (
    _strip_html, _CASE_ID_RE, _CASE_UID_RE, _TIME_RE, parse_date,
    _HTML_SCRIPT_RE, _HTML_STYLE_RE,
)

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


def _warn_if_card_degraded(
    card_info: dict,
    case_number: str,
    case_block: dict | None = None,
    court: str = "",
) -> None:
    """Логируем обрезанную карточку только если из неё не удалось
    выдернуть ни одного события (иначе компактный шаблон — это норма).

    Если у дела последний сохранённый event — «без движения», обрезанная
    карточка ожидаема (sudrf отдаёт огрызок, smart-skip парсит раз в 7
    дней). Понижаем до debug, чтобы не шуметь в логе каждую неделю.

    court — короткое имя суда для обхода 1-й инстанции (20 судов, по
    номеру дела суд не восстановить); апелляция/кассация не передают.
    """
    if card_info.get("_table_count", 0) >= 6:
        return
    if card_info.get("_events"):
        return
    court_tag = f" ({court})" if court else ""
    msg = (
        f"  {case_number}{court_tag}: карточка обрезана "
        f"({card_info.get('_table_count', 0)} таблиц), "
        f"движение не распозналось"
    )
    if case_block:
        events = case_block.get("events") or []
        if events:
            last_text = ((events[-1] or {}).get("text") or "").lower()
            if _SUSPENDED_RX.search(last_text):
                log.debug(msg + " (suspended — ожидаемо)")
                return
    config.METRICS["cards_degraded"] += 1
    log.warning(msg)


def card_is_empty_shell(card_info: dict) -> bool:
    """Страница вовсе без таблиц — не карточка (заглушка/блок, не пойманные
    детектором маркеров looks_like_non_card_page): у настоящих карточек ≥4
    таблиц даже у «огрызков». FI-цикл использует как второй рубеж после
    fetch_card_checked — такую страницу не считать успешной проверкой и не
    бумпать last_checked_at (аутейдж sudrf 20.07.2026 маскировался под
    «проверено сегодня», сводка врала «спарсено 47 из 75»)."""
    return card_info.get("_table_count", 0) == 0


# Заголовки колонок таблиц «Движение дела» и «Движение жалобы» → ключи
# события. Проверяются по префиксу и в порядке убывания длины, поэтому
# «дата размещения» забирает свою колонку раньше, чем до неё доберётся
# «дата», а «основание для выбранного результата события» — раньше, чем
# «результат». В шапке к тексту заголовка бывает приклеена подсказка
# («Дата размещения\xa0Информация о размещении…»), отсюда префикс, а не
# точное равенство.
# Ключи col_date/col_time намеренно НЕ называются date/time: базовые date и
# time вычисляются прежней логикой (первая ячейка-дата, регексп времени),
# и перезапись их сырым текстом ячейки каскадом сломала бы _SESSION_START_RX
# → «Дата/Время заседания» → самоизлечение фантомной hearing_date.
_ЗАГОЛОВКИ_КОЛОНОК = (
    ("основание для выбранного результата события", "ground"),
    ("наименование события", "name"),
    ("дата размещения", "posted_at"),
    ("место проведения", "place"),
    ("результат события", "result_event"),
    ("примечание", "note"),
    ("основание", "ground"),
    ("результат", "result_event"),
    ("событие", "name"),
    ("время", "col_time"),
    ("дата", "col_date"),
)


def _карта_колонок(row: list) -> dict:
    """Карта {ключ: индекс ячейки} по строке-шапке таблицы."""
    карта: dict[str, int] = {}
    for idx, cell in enumerate(row):
        текст = " ".join(cell_text(cell).replace("\xa0", " ").lower().split())
        if not текст:
            continue
        for префикс, ключ in _ЗАГОЛОВКИ_КОЛОНОК:
            if текст.startswith(префикс):
                карта.setdefault(ключ, idx)
                break
    return карта


def _найти_шапку_колонок(tbl: list, обязательные: set) -> tuple:
    """Индекс строки-шапки колонок и её карта.

    Шапка — НЕ нулевая строка таблицы: tbl[0] это заголовок («ДВИЖЕНИЕ ДЕЛА»),
    по которому таблица и опознаётся, а настоящая шапка идёт следом и до сих
    пор оседала в `_events` мусорным событием с пустой датой (по одному на
    каждую карточку с событиями). Ищем по содержимому в первых трёх строках.
    """
    for idx in range(min(3, len(tbl))):
        карта = _карта_колонок(tbl[idx])
        if обязательные <= set(карта):
            return idx, карта
    return -1, {}


def _колонки_строки(row: list, карта: dict, ширина: int) -> dict:
    """Разложить строку по карте колонок. Пустые ячейки не возвращаем.

    Ширина строки обязана совпасть с шапкой: при расхождении раскладывать
    наугад опаснее, чем не раскладывать вовсе (та же философия, что у
    looks_like_non_card_page — честный отказ вместо тихой порчи). Тогда
    событие остаётся только с legacy-полем text, и фронт корректно
    откатывается на показ склеенной строки целиком.
    """
    if not карта or len(row) != ширина:
        if карта:
            config.METRICS["movement_odd_width"] += 1
        return {}
    колонки = {}
    for ключ, idx in карта.items():
        if ключ in ("col_date", "col_time") or idx >= len(row):
            continue
        значение = cell_text(row[idx]).strip()
        if значение:
            колонки[ключ] = значение
    return колонки


def parse_case_card(html: str, court_base_url: str = "") -> dict:
    """
    Парсит карточку дела. Извлекает:
    - Последнее событие и дату из таблицы ДВИЖЕНИЕ ДЕЛА
    - Результат, УИД и судей из таблицы ДЕЛО / РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ
    - Наличие опубликованного акта
    - Текст судебного акта (если есть)
    Таблицы ищутся по содержимому (лейблы/заголовки), не по индексам —
    индексы плавают при правках вёрстки суда (баннеры апелляции 14.07.2026).
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
        "Номер дела (карточка)": "",  # Собственный гражд. номер из заголовка «ДЕЛО № …» (для промоушена М→2)
        "УИД": "",  # Уникальный идентификатор дела (86RS...) — сквозной мост 1-я инст. ↔ апел. ↔ касс.
        "act_text": "",  # Текст акта (для дайджеста, не сохраняется в CSV)
        # Раздельные сырые имена подателей жалоб из карточки 1-й инст.:
        # «Вид жалобы» во вкладке «Обжалование решений» и стебель «кассационн»/
        # «апелляционн» в regex событий движения — теперь разводят сигналы по
        # видам, чтобы фронт показывал бейдж «Кассатор» уже на cassation_watch,
        # не дожидаясь карточки 7kas. Legacy `_appellant_raw` (= apel or cass)
        # сохраняем для CSV-маппинга и `_appellant_fmt` в дайджесте.
        "_fi_appellant_raw": "",
        "_fi_cassator_raw": "",
        "_appellant_raw": "",  # Legacy-алиас, заполняется в конце parse_case_card
        "_table_count": 0,      # len(tables) — индикатор «обрезанной» карточки для _warn_if_card_degraded
        "_fi_appeal_filed": False,  # В карточке 1 инст. подана апелляц. жалоба
        "_fi_appeal_filed_date": "",
        # «Направлено в вышестоящую инстанцию» для апелляц. жалобы — это
        # отправка дела в Суд ХМАО-Югры. Нужно фронту, чтобы юрист видел
        # дату направления в drawer'е до того, как появится апел. карточка.
        "_fi_sent_to_appeal": False,
        "_fi_sent_to_appeal_date": "",
        # Полные движения жалобы из вкладки «Обжалование решений». Каждый
        # элемент: {"date": "DD.MM.YYYY", "text": "Регистрация / Без движения
        # / Направлено в вышестоящую и т.п."}. Используется фронтом для
        # отрисовки хронологии и ключевой даты «жалоба предъявлена».
        "_fi_appeal_events": [],
        # Кассационные события в карточке 1 инст. (кассация подаётся через
        # суд 1-й инстанции). Нужны для state-machine cassation_watch.
        "_fi_cassation_filed": False,
        "_fi_cassation_filed_date": "",
        "_fi_cassation_events": [],
        "_fi_sent_to_cassation": False,
        "_fi_sent_to_cassation_date": "",
        # Участники дела из таблицы «Лица, участвующие в деле» (sudrf 1-й инст.)
        # / «УЧАСТНИКИ» (7kas). Используется для пересчёта актуальной роли
        # банка: при исключении из ответчиков bank_role переключается на
        # «Третье лицо» автоматически (см. determine_bank_role_from_participants).
        "participants": [],
        "bank_role_from_participants": "",
    }

    tables = extract_tables(html)
    info["_table_count"] = len(tables)

    # Собственный номер дела из заголовка карточки «ДЕЛО № 2-XXXX/YYYY ~ М-NNNN/YYYY».
    # На карточке 1-й инстанции постоянный гражданский номер живёт ТОЛЬКО здесь
    # (поле «Номер дела 1 инстанции» — это перекрёстная ссылка с карточек
    # вышестоящих судов и тут всегда пустое). Берём гражданский номер лишь если
    # он стоит РАНЬШЕ материала М-… — т.е. дело уже возбуждено. Для непринятого
    # материала («ДЕЛО № М-…») вернёт '' и промоушен не тронет запись.
    _title_m = re.search(r"ДЕЛО\s*№(.{0,60})", html or "", re.DOTALL)
    if _title_m:
        _seg = re.sub(r"<[^>]+>", " ", _title_m.group(1))
        _first = re.search(r"(\d+-\d+/\d{4})|(М-\d+/\d{4})", _seg)
        if _first and _first.group(1):
            info["Номер дела (карточка)"] = _first.group(1)

    # Вкладка «обжалование решений, определений (пост.)» — sudrf нередко
    # открывает её по умолчанию (≤4 таблиц) вместо «ДЕЛО». Раньше тут стоял
    # ранний флаг `_fi_appeal_filed=True` по regex «обжалован.*решен» — он
    # ставился без даты даже на пустых вкладках. Дата ниже извлекается
    # из таблицы «ДВИЖЕНИЕ ЖАЛОБЫ»; флаг — только при реальной регистрации.
    # Раньше здесь был ранний return при <6 таблиц — он отбрасывал живые
    # карточки с укороченным шаблоном (напр. Сургутский районный суд
    # отдаёт 4 таблицы, но с полным «ДВИЖЕНИЕ ДЕЛА»). Циклы ниже защищены
    # от малого числа таблиц, поэтому безопасно парсить всё, что есть.

    # ── Таблица ДЕЛО ──
    # Ищем таблицу с результатом рассмотрения, судьёй-докладчиком апелляции
    # и судьёй первой инстанции. Структура строк: <td><b>Лейбл</b></td><td>Значение</td>.
    # Проходим ВСЕ таблицы: раньше сканировались первые 5 («ДЕЛО» обычно
    # была на индексе 3), но 14.07.2026 апелляция добавила баннеры в шапку
    # («График заседаний Президиума», QR-код), «ДЕЛО» уехала на индекс 5 —
    # и Результат/УИД/судья-докладчик молча терялись. Каждое поле
    # заполняется один раз (первое вхождение по порядку документа): «ДЕЛО»
    # идёт раньше «РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ» и «ДВИЖЕНИЯ ДЕЛА»,
    # так что совпадающие лейблы поздних таблиц ничего не перетирают.
    for tbl_idx in range(len(tables)):
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
            if "результат рассмотрения" in label_l and not info["Результат"]:
                if value and value.lower() not in (
                    "результат", "результат рассмотрения", label_l, "",
                ):
                    info["Результат"] = value
            # Номер дела в первой инстанции — лейбл вида:
            # «Номер дела в первой инстанции»
            # Значение: «2-498/2026 (2-9238/2025;)» — берём первый номер
            if ("номер" in label_l and "первой инстанции" in label_l
                    and not info["Номер дела 1 инстанции"]):
                if value:
                    # Извлечь первый номер дела (формат N-NNNN/YYYY)
                    fi_num_m = re.search(r'\d+-\d+/\d{4}', value)
                    if fi_num_m:
                        info["Номер дела 1 инстанции"] = fi_num_m.group(0)
            # Уникальный идентификатор дела (УИД, формат 86RS0020-01-2025-000203-13).
            # На апел. карточке значение завёрнуто в <a href=...r_juid...> — cell_text
            # его разворачивает. УИД сквозной для всех инстанций → мост к кассации.
            if "уникальный идентификатор" in label_l and not info["УИД"]:
                uid_m = JUDICIAL_UID_RE.search(value)
                if uid_m:
                    info["УИД"] = uid_m.group(0)
            # Судья первой инстанции — лейбл вида:
            # «Судья (мировой судья) первой инстанции».
            # Именно startswith: рядом лежит «Суд (мировой судья) первой
            # инстанции» (название суда) — он тоже содержит подстроки
            # «судья» + «первой инстанции», но начинается с «суд », и при
            # fill-once перетирал бы судью названием суда.
            if label_l.startswith("судья") and "первой инстанции" in label_l:
                if (value and value.lower() != label_l
                        and not info["Судья 1 инстанции"]):
                    info["Судья 1 инстанции"] = value
            elif label_l == "судья":
                # Судья-докладчик апелляции (отдельная строка «Судья» без
                # «первой инстанции»)
                if (value and value.lower() != "судья"
                        and not info["Судья-докладчик"]):
                    info["Судья-докладчик"] = value

    # ── Таблица ДВИЖЕНИЕ ДЕЛА (обычно индекс 5 или 6) ──
    # Ищем таблицу с событиями: содержит столбцы "Событие" / "Дата".
    # ВАЖНО: исключаем «ДВИЖЕНИЕ ЖАЛОБЫ» с вкладки обжалования — иначе наши
    # апеллянт-эвристики (pattern «жалоб + ФИО») цепляют мусор вроде
    # «(представления) в суде». «Движение жалобы» парсится отдельно ниже
    # как источник дат регистрации/направления.
    # NB: дискриминатор — заголовок «движение жалобы». Колонку «Дата
    # размещения» как маркер брать НЕЛЬЗЯ — она есть и в основной таблице
    # «ДВИЖЕНИЕ ДЕЛА» (регресс 13.05–21.05.2026: парсер выкидывал движение
    # дела у всех карточек).
    movement_table = None
    for tbl_idx in range(len(tables)):
        tbl = tables[tbl_idx]
        if len(tbl) > 1:
            top_text = " ".join(
                " ".join(cell_text(c) for c in r)
                for r in tbl[:3]
            ).lower()
            if "движение жалобы" in top_text:
                continue
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
        # Шапка колонок — отдельная строка внутри таблицы (не movement_table[0],
        # там заголовок «ДВИЖЕНИЕ ДЕЛА»). Нужна для раскладки события по
        # колонкам и чтобы саму шапку не записать в события.
        шапка_idx, карта_колонок = _найти_шапку_колонок(
            movement_table, {"name", "col_date"}
        )
        ширина_шапки = len(movement_table[шапка_idx]) if шапка_idx >= 0 else 0
        # Последняя строка данных = последнее событие
        events_data = []
        for row_idx, row in enumerate(movement_table[1:], start=1):
            if row_idx == шапка_idx:
                continue
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
                    колонки = _колонки_строки(row, карта_колонок, ширина_шапки)
                    events_data.append((date_val, time_val, event_desc, колонки))

        if events_data:
            # Полный список событий для timeline (сохраняется в JSON как events[]).
            # Поле text — прежняя склейка, БАЙТ-В-БАЙТ: по паре (date, text)
            # события дедуплицирует _events_newly_match, и смена формата
            # объявила бы всю историю каждого дела новой. Колонки только
            # дописываются рядом, непустые.
            info["_events"] = [
                {"date": d, "time": t, "text": desc, **cols}
                for d, t, desc, cols in events_data
            ]
            last_date, last_time, last_event, _ = events_data[-1]
            info["Последнее событие"] = last_event
            info["Дата события"] = last_date
            # Время заседания — только из session-событий (судебное заседание,
            # предварительное, подготовка дела/собеседование, беседа). Раньше
            # тут стоял naive `"заседани" in ev_desc`, но он не ловит
            # «Подготовка дела (собеседование)» — у дел 1-й инст. это часто
            # ПЕРВОЕ назначение, и без него парсер не находил будущую дату.
            for ev_date, ev_time, ev_desc, _ in reversed(events_data):
                if _SESSION_START_RX.search(ev_desc) and ev_time:
                    info["Время заседания"] = ev_time
                    break
            # Дата заседания — последнее session-событие.
            for ev_date, ev_time, ev_desc, _ in reversed(events_data):
                if _SESSION_START_RX.search(ev_desc) and ev_date:
                    info["Дата заседания"] = ev_date
                    break
            # Если заседания не было — ищем дату определения/решения
            # (для дел снятых с рассмотрения, прекращённых, возвращённых)
            if not info.get("Дата заседания"):
                decision_kw = ["определени", "снято", "прекращен", "возвращен",
                               "без изменени", "отменен", "изменен"]
                for ev_date, ev_time, ev_desc, _ in reversed(events_data):
                    ev_low = ev_desc.lower()
                    if (ev_date and any(kw in ev_low for kw in decision_kw)
                            and not _INTERLOCUTORY_PREP_RX.search(ev_low)
                            and not _ACCEPTANCE_RX.search(ev_low)):
                        info["Дата заседания"] = ev_date
                        break

    # ── Определяем апеллянта / кассатора ──
    # Pattern 1: поля в таблицах карточки. Лейблы «заявитель жалобы»/«податель
    # жалобы»/«апеллянт» исторически относятся к апелляции — кассационных
    # эквивалентов в шапке не встречалось. Пишем в _fi_appellant_raw.
    appellant_raw = ""
    # Полный проход по таблицам (раньше — первые 8): баннеры в шапке
    # апелляции 14.07.2026 сдвинули контент, жёсткие границы ненадёжны.
    # Лейблы специфичные («заявитель жалобы» и т.п.) — ложных матчей в
    # баннерах/участниках нет, цикл и так fill-once (break после первого).
    for tbl_idx in range(len(tables)):
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
    if appellant_raw:
        info["_fi_appellant_raw"] = appellant_raw

    # Pattern 2: события движения дела. Detect стебля «кассационн» — пишем в
    # _fi_cassator_raw, иначе в _fi_appellant_raw. Каждый канал заполняется
    # один раз (fill-once), оба могут получить данные с разных строк таблицы.
    if movement_table and len(movement_table) > 1:
        for row in movement_table[1:]:
            ev = " ".join(cell_text(c) for c in row)
            is_cassation_ev = bool(re.search(r'кассационн', ev, re.IGNORECASE))
            target_key = "_fi_cassator_raw" if is_cassation_ev else "_fi_appellant_raw"
            if info[target_key]:
                continue  # этот канал уже заполнен
            m = re.search(
                r'(?:поступи\w+|подан\w+|принят\w+)\s+'
                r'(?:апелляционн\w+\s+|кассационн\w+\s+)?жалоб\w+\s+'
                r'(?:от\s+)?(.{3,80}?)(?:\.|,|$)',
                ev, re.IGNORECASE,
            )
            if m:
                info[target_key] = m.group(1).strip()
                continue
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
                    info[target_key] = candidate

    # Pattern 3 (fuzzy-поиск «жалоба + ФИО» по всему HTML) раньше жил здесь —
    # удалён после кейса 33-1161/2026, где карточка прошла «по правилам 1-й
    # инстанции» без апеллянта, но регекс вытащил имя одного из ответчиков
    # из не связанного контекста карточки. Лучше «не указано» в дайджесте,
    # чем неверный апеллянт — полагаемся только на структурные источники
    # (поле «Заявитель жалобы» в таблицах + событие движения).


    # ── События подачи жалоб в карточке 1-й инстанции ──
    # Апелляционная и кассационная жалобы подаются через суд 1-й инстанции —
    # отсюда же видно и событие «направлено в кассационный суд».
    # Регексы специфичны по стеблю «апелляционн» / «кассационн», чтобы не
    # путать апелляцию с кассацией (раньше «поступ.+жалоб» цеплял кассацию
    # как апелляцию).
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
            # «представление» — прокурорский аналог жалобы (симметрично
            # апелляционному регексу ниже).
            if not info["_fi_cassation_filed"] and re.search(
                r'поступ\w+.{0,40}кассационн\w+\s+(?:жалоб|представлени)\w+',
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

    # ── Вкладка «Обжалование решений, определений (пост.)» ──
    # На основной «ДЕЛО» (new=0) события «Поступила апелляционная жалоба» у
    # многих судов нет — это движение жалобы живёт в отдельной вкладке.
    # Структура: блоки «ЖАЛОБА № N» со строками «Вид жалобы (представления)»,
    # «Заявитель», «Вышестоящий суд», далее таблица «ДВИЖЕНИЕ ЖАЛОБЫ» со
    # строками «Регистрация жалобы (представления) в суде» / «Направлено
    # в вышестоящую инстанцию». При коротком ответе sudrf отдаёт именно эту
    # вкладку (≤4 таблицы) — оттуда и берём даты, fallback на new=0 потом
    # дополнит движение дела.
    #
    # Маркер вкладки апелляции в шапке (есть в табах карточки): «обжалование
    # решений, определений (пост.)». Если он встречается — короткие таблицы
    # с лейблами «Заявитель жалобы» / «Дата поступления жалобы» считаются
    # сигналом об апел. жалобе даже без таблицы «ДВИЖЕНИЕ ЖАЛОБЫ» (фолбэк
    # на new=0 мог не дотянуться, но факт жалобы терять нельзя).
    # Полный проход по таблицам (раньше — первые 3): баннеры в шапке
    # 14.07.2026 («График заседаний Президиума», «График работы», QR-код)
    # сдвигают контент, и жёсткая граница молча теряла бы маркер. На полных
    # карточках маркер в таблицы вообще не попадает (табы там — <ul>, живая
    # проверка 14.07.2026) — он виден только в укороченной выдаче вкладки.
    # Чтобы полный проход не ловил «Результат обжалования решения…» из
    # таблиц «РАССМОТРЕНИЕ В НИЖЕСТОЯЩЕМ СУДЕ» / вкладки обжалования, regex
    # требует полное название вкладки — «…решений, определений» (скобка в
    # «решение (определение)» продолжением не считается).
    has_appeal_tab_marker = False
    for tbl in tables:
        for row in tbl:
            row_lc = " ".join(cell_text(c) for c in row).lower()
            if re.search(
                r'обжалован\w*\s+решени\w*\s*,?\s*(?:и\s+)?определени\w*',
                row_lc,
            ):
                has_appeal_tab_marker = True
                break
        if has_appeal_tab_marker:
            break
    current_kind: str | None = None  # "appeal" | "cassation"
    for tbl in tables:
        # Карта колонок таблицы «ДВИЖЕНИЕ ЖАЛОБЫ» (Событие | Дата | Результат |
        # Основание | Примечание | Дата размещения). Нужна, чтобы разложить
        # событие жалобы так же, как событие движения дела. Существующий
        # разбор ниже берёт примечание отрицательным индексом row[-2] и
        # остаётся нетронутым: смешивать его с положительными индексами
        # нельзя — якоря совпадают только при ровно ожидаемой ширине.
        шапка_ж_idx, карта_ж = _найти_шапку_колонок(tbl, {"name", "col_date"})
        ширина_ж = len(tbl[шапка_ж_idx]) if шапка_ж_idx >= 0 else 0
        for row in tbl:
            if len(row) < 1:
                continue
            row_text = " ".join(cell_text(c) for c in row)
            row_lc = row_text.lower()
            label = cell_text(row[0]).strip().lower()
            value = cell_text(row[-1]).strip() if len(row) >= 2 else ""

            # Новый блок «ЖАЛОБА № N» сбрасывает контекст вида.
            if re.search(r'жалоба\s*№', row_lc):
                current_kind = None
                continue
            # Вид жалобы (представления) — определяет апелляцию vs кассацию.
            if "вид жалобы" in label or "вид жалобы" in row_lc[:40]:
                val_lc = value.lower() if value else row_lc
                if "апелляционн" in val_lc:
                    current_kind = "appeal"
                elif "кассационн" in val_lc:
                    current_kind = "cassation"
                continue
            # Заявитель — короткое поле на вкладке обжалования («ИСТЕЦ» /
            # «ОТВЕТЧИК»). Не путать с «заявитель жалобы» из таблиц апел.
            # карточки. Заполняем только если соответствующий канал пуст.
            if label == "заявитель":
                target_key = "_fi_cassator_raw" if current_kind == "cassation" else "_fi_appellant_raw"
                if value and value.lower() != "заявитель" and not info[target_key]:
                    info[target_key] = value
                continue

            # Короткая шапка жалобы (без «ДВИЖЕНИЕ ЖАЛОБЫ»): «Заявитель
            # жалобы» / «Дата поступления жалобы». Бывает на укороченной
            # вкладке апелляции при new=5, когда фолбэк на new=0 не сработал.
            # Привязываем к маркеру вкладки и к contextual current_kind: для
            # касс. блока пишем в _fi_cassator_raw + _fi_cassation_filed.
            if has_appeal_tab_marker:
                if current_kind == "cassation":
                    if label == "заявитель жалобы":
                        info["_fi_cassation_filed"] = True
                        if value and value.lower() != label and not info["_fi_cassator_raw"]:
                            info["_fi_cassator_raw"] = value
                        continue
                    if label == "дата поступления жалобы":
                        if value and parse_date(value):
                            info["_fi_cassation_filed"] = True
                            if not info["_fi_cassation_filed_date"]:
                                info["_fi_cassation_filed_date"] = value
                        continue
                else:
                    if label == "заявитель жалобы":
                        info["_fi_appeal_filed"] = True
                        if value and value.lower() != label and not info["_fi_appellant_raw"]:
                            info["_fi_appellant_raw"] = value
                        continue
                    if label == "дата поступления жалобы":
                        if value and parse_date(value):
                            info["_fi_appeal_filed"] = True
                            if not info["_fi_appeal_filed_date"]:
                                info["_fi_appeal_filed_date"] = value
                        continue

            # События движения жалобы — нужна реальная дата (или дата
            # размещения как fallback: для «срок для возражений» в колонке
            # «Дата» стоит 01.01.1900, а реальная — в «Дата размещения»).
            row_date = ""
            publish_date = ""
            for c in row:
                ct = cell_text(c).strip()
                if ct == "01.01.1900":
                    continue
                if parse_date(ct):
                    if not row_date:
                        row_date = ct
                    publish_date = ct  # последняя валидная дата = «Дата размещения»
            effective_date = row_date or publish_date
            if not effective_date or current_kind not in ("appeal", "cassation"):
                continue

            # Текст события — название из 1-й колонки + примечание (если есть,
            # например «Срок до DD.MM.YYYY»). Это даёт юристу полный контекст
            # в хронологии: «Установлен срок для возражений · Срок до 27.04.2026».
            event_label = cell_text(row[0]).strip()
            if not event_label or parse_date(event_label):
                # Пустая ячейка или строка-дата (заголовок) — пропускаем.
                continue
            note = ""
            if len(row) >= 5:
                # Структура «Движения жалобы»: Событие | Дата | Результат |
                # Основание | Примечание | Дата размещения. Примечание — [-2].
                note = cell_text(row[-2]).strip()
                if parse_date(note):  # колонка примечания пустая, попала дата
                    note = ""
            event_text = (
                f"{event_label} · {note}" if note and note.lower() != event_label.lower()
                else event_label
            )
            events_list = (
                info["_fi_appeal_events"] if current_kind == "appeal"
                else info["_fi_cassation_events"]
            )
            # Дедуп по (date, label) — заголовки таблиц / повторные строки.
            if not any(
                e.get("date") == effective_date and e.get("text", "").startswith(event_label)
                for e in events_list
            ):
                # Колонки — рядом с прежним text (он не меняется: по нему
                # дедуплицирует условие выше и _events_newly_match).
                колонки_ж = _колонки_строки(row, карта_ж, ширина_ж)
                events_list.append(
                    {"date": effective_date, "text": event_text, **колонки_ж}
                )

            # Регистрация жалобы → дата подачи апел. или касс. жалобы.
            if re.search(r'регистрац\w*\s+жалоб', row_lc):
                if current_kind == "appeal":
                    if not info["_fi_appeal_filed_date"]:
                        info["_fi_appeal_filed"] = True
                        info["_fi_appeal_filed_date"] = effective_date
                elif current_kind == "cassation":
                    if not info["_fi_cassation_filed_date"]:
                        info["_fi_cassation_filed"] = True
                        info["_fi_cassation_filed_date"] = effective_date
                continue
            # Направлено в вышестоящую инстанцию:
            # - кассация → уход дела в касс. суд (state-machine: cassation_pending);
            # - апелляция → отправка в Суд ХМАО-Югры (информационно для drawer'а;
            #   переход в `appeal` делает link_cases по самой апел. карточке).
            if re.search(r'направлен\w+.{0,30}вышестоящ', row_lc):
                if current_kind == "cassation" and not info["_fi_sent_to_cassation_date"]:
                    info["_fi_sent_to_cassation"] = True
                    info["_fi_sent_to_cassation_date"] = effective_date
                elif current_kind == "appeal" and not info["_fi_sent_to_appeal_date"]:
                    info["_fi_sent_to_appeal"] = True
                    info["_fi_sent_to_appeal_date"] = effective_date
                continue

    # ── Рантайм-страж: подача апел./касс. жалобы есть, но не распознана ──
    # Если в карточке есть строка «Регистрация жалобы» (жалоба фактически подана
    # через 1-ю инст.) И где-то рядом фигурирует «апелляционн»/«кассационн», но
    # ни апел., ни касс. флаг не выставлен — значит, вид жалобы не определился
    # (`current_kind` остался None, напр. из-за новой вёрстки вкладки
    # «Обжалование»). Это ровно тот тихий сбой, что прятал подачу апелляции по
    # делу 2-3063/2026. Логируем предупреждение, чтобы будущие неучтённые
    # варианты вёрстки не пропадали молча. Скан независим от основного цикла
    # (тот пропускает строки при `current_kind not in (...)`).
    # NB1: regex требует «регистрац…» вплотную к «жалоб», поэтому строка движения
    #   дела «Регистрация иска (заявления, жалобы)» сюда не попадает.
    # NB2: маркер «апелляционн|кассационн» отсекает ЧАСТНЫЕ жалобы (на определения)
    #   — их мы как подачу апел./касс. не трекаем, и страж по ним молчит.
    if not (info["_fi_appeal_filed"] or info["_fi_cassation_filed"]):
        all_text = " ".join(
            cell_text(c) for tbl in tables for row in tbl for c in row
        ).lower()
        saw_complaint_registration = bool(re.search(r'регистрац\w*\s+жалоб', all_text))
        looks_like_appeal_or_cassation = bool(re.search(r'апелляционн|кассационн', all_text))
        if saw_complaint_registration and looks_like_appeal_or_cassation:
            ident = info.get("УИД") or info.get("case_number") or "?"
            log.warning(
                f"Карточка {ident}: найдена «Регистрация жалобы» (апел./касс.), но "
                f"вид жалобы не распознан — флаг не выставлен; проверить вёрстку "
                f"вкладки «Обжалование решений»."
            )

    # Доимплить «Результат», если карточка sudrf оставила его пустым,
    # а в «Движении дела» уже есть событие «Вынесено [заочное] решение по делу».
    # Бывает, что после решения добавлены админ-шаги («Изготовлено
    # мотивированное», «Сдано в отдел», «Дело оформлено») — last_event
    # смотрит на них, а вердикт остаётся «похоронен» в середине events.
    # Заочные решения (ст. 233 ГПК) на сайте пишутся как «Вынесено заочное
    # решение по делу» — отдельный матч без «заочное» их пропускал.
    if not info.get("Результат") and info.get("_events"):
        for ev in reversed(info["_events"]):
            text = ev.get("text") or ""
            if re.search(r"вынесено\s+(?:заочное\s+)?решение\s+по\s+делу",
                         text.lower()):
                verdict = extract_result_from_event(text)
                if verdict:
                    info["Результат"] = verdict
                    break

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
    elif _TERMINAL_FI_EVENT_RX.search(info["Последнее событие"]):
        # Терминальный возврат 1-й инстанции без заполненного «Результата»:
        # «Материалы возвращены в связи с истечением срока…», возврат иска/
        # заявления, отказ в принятии. Поле «Результат» пустое, поэтому в
        # верхний if не попадаем — помечаем отдельным статусом «Возвращено»
        # (терминальный для архивации, см. is_case_archived). Передача по
        # подсудности сюда не доходит — у неё «Результат» заполнен → «Решено».
        info["Статус"] = "Возвращено"

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

    # ── Лица, участвующие в деле ────────────────────────────────────────
    # Нужно для пересчёта актуальной роли банка. На sudrf 1-й инст. таблица
    # называется «Лица, участвующие в деле», на апелляции — «Стороны по делу»
    # / «Участники», на 7kas — «УЧАСТНИКИ». Заголовок секции — в первой строке
    # таблицы (рендерится внутри <td>/<th>). Шапка колонок — в строке 1.
    part_header_rx = re.compile(
        r"(лица,\s*участвующи|^участники\b|стороны\s+по\s+делу)",
        re.IGNORECASE,
    )
    for tbl in tables:
        if not tbl:
            continue
        first_row_text = " ".join(cell_text(c) for c in tbl[0]).strip()
        if not part_header_rx.search(first_row_text):
            continue
        for row in tbl[1:]:
            cells = [cell_text(c).strip() for c in row]
            if len(cells) < 2 or not cells[0]:
                continue
            role_up = cells[0].upper()
            # Пропустить строку-шапку колонок (если есть)
            if (
                "ВИД ЛИЦА" in role_up
                or "ВИД УЧАСТНИКА" in role_up
                or role_up in ("ФИО", "НАИМЕНОВАНИЕ", "ЛИЦО", "РОЛЬ")
            ):
                continue
            # Скип служебных строк без распознаваемой роли стороны
            if not any(
                kw in role_up
                for kw in ("ИСТЕЦ", "ОТВЕТЧИК", "ТРЕТЬЕ", "ЗАЯВИТ", "ПРОКУР", "ПРЕДСТАВ")
            ):
                continue
            info["participants"].append({"role": cells[0], "name": cells[1]})
        break

    info["bank_role_from_participants"] = (
        determine_bank_role_from_participants(info["participants"])
    )

    # Legacy-алиас: единое поле для CSV-маппинга (case["Апеллянт"] в
    # update_active_cases) и `_appellant_fmt` в дайджесте. Апеллянт имеет
    # приоритет — кассатор берётся только когда апеллянта нет.
    info["_appellant_raw"] = info["_fi_appellant_raw"] or info["_fi_cassator_raw"]

    return info


def fetch_act_text(act_url: str, *, context: str | None = None) -> str:
    """Скачать текст судебного акта по URL (context — номер дела для логов).

    Через fetch_card_checked: если страницу акта закроют проверочным кодом,
    _strip_html превратил бы её в «текст акта»-мусор, который ушёл бы в
    LLM-пересказ и дайджест. Карточный детектор здесь безопасен — генерические
    фразы («проверочный код» из СМС-цитат в актах о мошенничестве) он не матчит.
    """
    polite_delay()
    html = fetch_card_checked(act_url, context=context)
    if not html:
        return ""
    # Убираем script/style + теги, схлопываем пробелы
    text = _HTML_SCRIPT_RE.sub('', html)
    text = _HTML_STYLE_RE.sub('', text)
    return _strip_html(text)[:5000]  # Сырой текст, обрезается позже


# Hidden div на карточке 7kas, в котором размещается полный текст определения
# (вкладка «Судебные акты» переключается JS, но HTML отдаёт сразу). Пуст до
# публикации мотивированного определения.
