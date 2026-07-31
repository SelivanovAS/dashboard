# -*- coding: utf-8 -*-
"""Текстовые утилиты: парсинг дат, очистка HTML, экранирование,
сокращение наименований сторон и судов, производственный календарь.

Чистые функции без внешнего состояния — верхний «лист» пакета,
может импортироваться любым модулем.
"""

from __future__ import annotations

import re
from datetime import datetime, date, timedelta
from html import escape as html_escape

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
# 1-я инст.: помимо цифр-префикса (2-X/Y) допускаем буквенные префиксы —
# «М-» (материалы: иск подан, но ещё не зарегистрирован гражданским 2-XXX).
# Без них пропадает видимость свежепоступивших исков против Сбера.
# Средний сегмент — номер ПОСТОЯННОГО СУДЕБНОГО ПРИСУТСТВИЯ: Покачи (вторая
# площадка Нижневартовского районного, srv_num=2) нумерует дела трёхчастно —
# «2-2-279/2026», «9-2-65/2026 ~ М-2-309/2026». Без него parse_first_instance_search
# отдавал по Покачи 0 строк при сотнях сберовских дел в выдаче: суд был невидим
# и для сборщика исков банка, и для боевого автопоиска ответчик-дел.
_FI_CASE_NUM_RE = re.compile(r'(?:[А-ЯA-Z]+|\d+)-(?:\d+-)?\d+/\d{4}')
_TIME_RE = re.compile(r'\b(\d{1,2}:\d{2})\b')
_CASE_ID_RE = re.compile(r'case_id=(\d+)')
_CASE_UID_RE = re.compile(r'case_uid=([a-f0-9\-]+)')


def _strip_html(text: str) -> str:
    """Убрать HTML-теги, &nbsp; и схлопнуть пробелы. Используется для извлечения
    чистого текста из фрагментов карточки дела и судебных актов."""
    text = _HTML_TAG_RE.sub(' ', text)
    text = _HTML_NBSP_RE.sub(' ', text)
    return _WS_RE.sub(' ', text).strip()


def case_id_uid(link_str: str) -> tuple[str, str]:
    """Извлечь case_id и case_uid из поля Ссылка (формат 'id|uid')."""
    parts = link_str.strip().split("|")
    if len(parts) == 2:
        return parts[0], parts[1]
    return "", ""


def escape_html(text: str) -> str:
    """Экранировать спецсимволы HTML для Telegram."""
    return html_escape(str(text), quote=False)


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


# ── Исчисление процессуальных сроков (гл. 9 ГПК) ─────────────────────────────
# Три функции ниже — арифметика сроков для bank_legal_force_est (расчётная
# дата вступления решения в силу). Правила ГПК, на которые они опираются:
# ст. 107 ч. 3 — течение срока начинается на следующий день после якорной
# даты, в сроки, исчисляемые днями, нерабочие дни не включаются;
# ст. 108 — месячный срок истекает в соответствующее число последнего месяца,
# последний день-нерабочий переносится на следующий рабочий.


def next_working_day(d: date) -> date:
    """Ближайший рабочий день, начиная с d (d рабочий → сам d).

    Перенос последнего дня срока с нерабочего дня — ч. 2 ст. 108 ГПК.
    """
    while not is_russian_working_day(d):
        d += timedelta(days=1)
    return d


def add_working_days(anchor: date, n: int) -> date:
    """Последний день срока в n РАБОЧИХ дней от якорной даты.

    Ст. 107 ГПК: течение начинается на СЛЕДУЮЩИЙ день после anchor, нерабочие
    дни в срок не входят. Возвращает дату n-го рабочего дня (= последний день
    срока; сам он рабочий по построению, переносить нечего).
    """
    cur = anchor
    while n > 0:
        cur += timedelta(days=1)
        if is_russian_working_day(cur):
            n -= 1
    return cur


def month_term_last_day(anchor: date) -> date:
    """Последний день МЕСЯЧНОГО срока, текущего от якорной даты.

    Ст. 108 ГПК + п. 16 ПП ВС №16 (2022): срок истекает в соответствующее
    число следующего месяца (мотивировка 31.07 → последний день 31.08);
    в следующем месяце нет такого числа → последний день этого месяца
    (31.01 → 28/29.02); последний день нерабочий → следующий рабочий.
    """
    year, month = anchor.year, anchor.month + 1
    if month > 12:
        year, month = year + 1, 1
    day = anchor.day
    while True:
        try:
            last = date(year, month, day)
            break
        except ValueError:  # 31-е в месяце без 31-го → последний день месяца
            day -= 1
    return next_working_day(last)


# ── Сокращение наименований сторон ────────────────────────────────────────────

# Творительный падеж процессуальных ролей — для конструкции «жалоба подана
# Ответчиком Ивановым И.И.» (вместо корявого «от Ответчика Иванова И.И.»).
ROLE_INSTRUMENTAL = {
    "Истец":       "Истцом",
    "Ответчик":    "Ответчиком",
    "Иное лицо":   "Иным лицом",
    "Третье лицо": "Третьим лицом",
}

# Родительный падеж статусов подателя жалобы — для строки «Итог» дайджеста
# («Оставлено без изменения (жалоба заявителя ЖНК Единство)»). Значения в
# нижнем регистре: роль стоит в середине фразы. «Заявитель» — статус из
# карточек 7kas, которого нет в ROLE_INSTRUMENTAL (из-за этого старый формат
# «подана Заявитель X» выходил без склонения).
ROLE_GENITIVE = {
    "Истец":       "истца",
    "Ответчик":    "ответчика",
    "Заявитель":   "заявителя",
    "Иное лицо":   "иного лица",
    "Третье лицо": "третьего лица",
    # 7kas отдаёт роль «ПРОКУРОР» (дела с участием прокуратуры — иски в защиту
    # прав потребителей). Без строки выходило «жалоба прокурор Прокуратура Х.».
    "Прокурор":    "прокурора",
    "Взыскатель":  "взыскателя",
    "Должник":     "должника",
}


def plural_ru(n: int, one: str, few: str, many: str) -> str:
    """Русская форма по числу: 1 дело / 2 дела / 5 дел.

    Используется сводкой дайджеста («2 новые апелляции», «5 заседаний»).
    """
    n_abs = abs(n)
    if n_abs % 10 == 1 and n_abs % 100 != 11:
        return one
    if 2 <= n_abs % 10 <= 4 and not 12 <= n_abs % 100 <= 14:
        return few
    return many

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
# Матчит обе формы региональных управлений Росимущества — полную
# («Межрегиональное территориальное управление Росимущества …») и принятую
# в нашей БД сокращённую («МТУ Росимущества в Тюменской области, ХМАО-Югре,
# ЯНАО»). Используется и в `_shorten_single` (на одиночное имя без запятых),
# и в `shorten_party_name` (pre-pass до сплита, иначе перечисление регионов
# через запятую развалит вход и сократится только первая часть).
_MTU_RE = re.compile(
    r'^(?:Межрегиональное\s+территориальное\s+управление|МТУ\s+Росимущества?)\b.*',
    re.IGNORECASE,
)
# Суффикс тюркских отчеств («кызы» — дочь, «оглы» — сын): четырёхсловное
# ФИО. В сокращении суффикс СОХРАНЯЕМ (решение юриста 30.07.2026):
# «Гаджиева Лейла Хандадаш кызы» → «Гаджиева Л.Х. кызы». Негативный
# просмотр в слоте отчества не даёт трёхсловному «Гаджиева Лейла кызы»
# (без отчества) превратить суффикс в инициал «К.» — такое имя не трогаем.
_FIO_SUFFIX_WORDS = r'(?:[кК]ызы|[гГ]ызы|[оО]глы|[уУ]глы)'
_FIO_SUFFIX = r'(?:\s+(' + _FIO_SUFFIX_WORDS + r'))?'
_FIO_PATRONYMIC = (
    r'(?!' + _FIO_SUFFIX_WORDS + r'(?:\s|$))([А-ЯЁа-яё])[а-яё]+'
)
_FIO_RE = re.compile(
    r'^([А-ЯЁа-яё-]+)\s+([А-ЯЁа-яё])[а-яё]+\s+' + _FIO_PATRONYMIC
    + _FIO_SUFFIX + r'$'
)
# То же ФИО, но с ПОЛНЫМ захватом имени (группа 2) — для разведения коллизии
# инициалов: «Фамилия Имя О.» вместо «Фамилия И.О.».
_FIO_FULL_RE = re.compile(
    r'^([А-ЯЁа-яё-]+)\s+([А-ЯЁа-яё][а-яё]+)\s+' + _FIO_PATRONYMIC
    + _FIO_SUFFIX + r'$'
)
# «ИП Фамилия Имя Отчество» — предприниматель-физлицо: маркер «ИП»
# сохраняем, ФИО за ним сокращаем как обычное.
_IP_PREFIX_RE = re.compile(
    r'^(?:ИП|Индивидуальный\s+предприниматель)\s+'
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
# Полные организационные формы кооперативов/товариществ → привычные
# аббревиатуры. Карточки судов (особенно 7kas) пишут форму полностью,
# в cases.json обычно уже кратко — без нормализации одна и та же
# организация выглядит в дайджесте по-разному («ЖНК Единство» в шапке
# дела vs «Жилищный накопительный кооператив Единство» в строке «Итог»).
_ORG_FORM_ABBRS = (
    (re.compile(r'\bЖилищный\s+накопительный\s+кооператив\b', re.IGNORECASE), 'ЖНК'),
    (re.compile(r'\bЖилищно-строительный\s+кооператив\b', re.IGNORECASE), 'ЖСК'),
    (re.compile(r'\bТоварищество\s+собственников\s+жилья\b', re.IGNORECASE), 'ТСЖ'),
    (re.compile(r'\bТоварищество\s+собственников\s+недвижимости\b', re.IGNORECASE), 'ТСН'),
    (re.compile(r'\bГаражно-строительный\s+кооператив\b', re.IGNORECASE), 'ГСК'),
    (re.compile(r'\bКредитный\s+потребительский\s+кооператив\b', re.IGNORECASE), 'КПК'),
    (re.compile(r'\bСадоводческое\s+некоммерческое\s+товарищество\b', re.IGNORECASE), 'СНТ'),
)


def _shorten_single(name: str, *, keep_fio_full: bool = False,
                    fio_mode: str = "initials") -> str:
    """Сокращение одного наименования (без запятых).

    fio_mode="first_full" — ФИО сокращать как «Фамилия Имя О.» (имя целиком),
    а не «Фамилия И.О.»: используется для разведения коллизии инициалов у
    однофамильцев (см. `_resolve_initial_collisions`).
    """
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
    # Полные формы кооперативов/товариществ → аббревиатуры (ЖНК/ТСЖ/…)
    for _form_re, _abbr in _ORG_FORM_ABBRS:
        name = _form_re.sub(_abbr, name)
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
    # ФИО → Фамилия И.О. (или Фамилия Имя О. при разведении коллизии
    # инициалов). «ИП …» — отщепляем маркер, сокращаем ФИО, маркер возвращаем;
    # суффикс «кызы»/«оглы» (4-е слово) сохраняем в хвосте.
    if not keep_fio_full:
        fio = name
        prefix = ""
        m_ip = _IP_PREFIX_RE.match(fio)
        if m_ip:
            prefix = "ИП "
            fio = fio[m_ip.end():]
        if fio_mode == "first_full":
            m = _FIO_FULL_RE.match(fio)
            if m:
                name = f"{prefix}{m.group(1)} {m.group(2)} {m.group(3).upper()}."
                if m.group(4):
                    name += f" {m.group(4)}"
        else:
            m = _FIO_RE.match(fio)
            if m:
                name = (f"{prefix}{m.group(1)} "
                        f"{m.group(2).upper()}.{m.group(3).upper()}.")
                if m.group(4):
                    name += f" {m.group(4)}"
    return name


def _resolve_initial_collisions(parts: list[str], shortened: list[str]) -> None:
    """На месте разводит одинаковые «Фамилия И.О.» от РАЗНЫХ полных ФИО.

    Частый случай — однофамильцы с совпавшими инициалами (напр. ответчики
    «Бундюк Денис Олегович» и «Бундюк Диана Олеговна» → оба «Бундюк Д.О.»).
    Такие разводим, разворачивая имя до полного: «Бундюк Денис О.» /
    «Бундюк Диана О.». Один и тот же человек, перечисленный дважды
    (одинаковое полное имя), не трогаем — это настоящий дубль.
    """
    by_short: dict[str, list[int]] = {}
    for i, sh in enumerate(shortened):
        by_short.setdefault(sh, []).append(i)
    for idxs in by_short.values():
        if len(idxs) < 2:
            continue
        if len({parts[i].strip() for i in idxs}) < 2:
            continue  # одно и то же полное имя — не разводим
        for i in idxs:
            expanded = _shorten_single(parts[i], fio_mode="first_full")
            if expanded:
                shortened[i] = expanded


def shorten_party_name(name: str, *, keep_fio_full: bool = False) -> str:
    """Сокращение наименования стороны по правилам дайджеста.

    Если в поле несколько сторон через запятую — сокращает каждую отдельно.
    keep_fio_full=True — не сокращать ФИО физлиц (для секции «Новые дела»).
    Совпавшие инициалы однофамильцев разводятся (`_resolve_initial_collisions`).
    """
    if not name or not name.strip():
        return name
    # Pre-pass для МТУ Росимущества: их региональное название обычно
    # содержит запятые («МТУ Росимущества в Тюменской области, ХМАО-Югре,
    # ЯНАО»), и сплит по запятой развалил бы строку — сократилась бы только
    # первая часть, остальные «ХМАО-Югре» / «ЯНАО» уехали бы в результат.
    if _MTU_RE.match(name.strip()):
        return "МТУ Росимущество"
    # Сначала склеиваем «Сбербанк, Югорское отделение № 5940» до split,
    # иначе отдельная часть «отделение № 5940» проскочит в результат.
    name = _BRANCH_COMMA_RE.sub(r'\1', name)
    parts = name.split(",")
    shortened = [_shorten_single(p, keep_fio_full=keep_fio_full) for p in parts]
    if not keep_fio_full:
        _resolve_initial_collisions(parts, shortened)
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


# Получатель исполнительного листа — подразделение ФССП с очень длинным
# официальным именем (в ops/writ_probe/report.txt есть 105-символьное
# «Отделение судебных приставов по взысканию задолженности с юридических лиц
# по г. Тюмени и Тюменскому району»). Раньше дайджест резал его по [:60] —
# посреди слова. Кроме приставов встречается «Взыскатель» — его не трогаем.
# ⚠️ Зеркало shortBailiff из app.js: правила держать согласованными, обе
# реализации проверяются одними фикстурами в test_frontend_writs.py.
_BAILIFF_RULES = (
    (re.compile(r"Межрайонное\s+отделение\s+судебных\s+приставов", re.I), "МОСП"),
    (re.compile(r"Отделени[ея]\s+судебных\s+приставов", re.I), "ОСП"),
    (re.compile(r"Управлени[ея]\s+Федеральной\s+службы\s+судебных\s+приставов", re.I),
     "УФССП"),
    (re.compile(r"по\s+взысканию\s+задолженности\s+с\s+юридических\s+лиц", re.I),
     "по взысканию задолж. с юрлиц"),
    (re.compile(r"\s+район(ам|у|а|е)(?=[\s,.)]|$)", re.I), r" р-н\1"),
)


def shorten_bailiff_name(name: str) -> str:
    """«Отделение судебных приставов по г. Сургуту» → «ОСП по г. Сургуту»."""
    if not name:
        return name
    out = name
    for rx, repl in _BAILIFF_RULES:
        out = rx.sub(repl, out, count=0 if r"\1" in repl else 1)
    return out


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


_APPELLANT_SIDE_ROLE_MAP = {
    "истец": "Истец",
    "ответчик": "Ответчик",
    "третье лицо": "Третье лицо",
    "иное лицо": "Иное лицо",
}
# Слова-спутники в поле «Заявитель», сами по себе стороной не являющиеся:
# «представитель» — представитель какой-то из сторон (чьей — поле не говорит).
_APPELLANT_AUX_WORDS = {"представитель"}


def appellant_role_words(appellant_raw: str) -> tuple[str, ...] | None:
    """Разобрать «Заявителя» жалобы, если это слова-роли, а не имя.

    Вкладка «Обжалование» карточки 1-й инст. в поле «Заявитель» даёт
    процессуальные роли, в том числе СОСТАВНЫЕ: «ИСТЕЦ, ПРЕДСТАВИТЕЛЬ»
    (жалоба стороны истца, подана представителем), «ИСТЕЦ, ТРЕТЬЕ ЛИЦО»
    (жалобы двух разных участников), голое «ПРЕДСТАВИТЕЛЬ».

    Возвращает:
      None — вход не является набором слов-ролей (настоящее имя/ФИО);
      кортеж различимых сторон-ролей из частей строки — может быть пуст
      (голый «ПРЕДСТАВИТЕЛЬ»: чей — неизвестно) или длиннее 1
      («ИСТЕЦ, ТРЕТЬЕ ЛИЦО»: податель неоднозначен).
    """
    parts = [p.strip().lower() for p in (appellant_raw or "").split(",")]
    parts = [p for p in parts if p]
    if not parts:
        return None
    roles: list[str] = []
    for p in parts:
        role = _APPELLANT_SIDE_ROLE_MAP.get(p)
        if role is not None:
            if role not in roles:
                roles.append(role)
        elif p not in _APPELLANT_AUX_WORDS:
            return None  # настоящее имя (или незнакомое слово) — не роли
    return tuple(roles)


def classify_appellant_role(
    appellant_raw: str,
    plaintiff: str,
    defendant: str,
) -> tuple[str, str]:
    """Определить роль апеллянта и его сокращённое имя.

    Возвращает (role, short_name):
      role ∈ {"Истец", "Ответчик", "Третье лицо", "Иное лицо", ""}
      short_name — shorten_party_name(appellant_raw) или "" если пусто.

    Логика: сравниваем нормализованные токены apellant_raw с токенами
    истца и ответчика. Матч — равенство токенов или подстрока (в любом
    направлении) при длине содержащего ≥ 4 символов. Если нет матча —
    возвращаем «Иное лицо» (но имя всё равно сохраняем).

    Особый случай: вкладка «Обжалование» карточки 1-й инст. в поле
    «Заявитель» даёт не имя, а слово-роль («ИСТЕЦ» / «ОТВЕТЧИК»), в том
    числе составную («ИСТЕЦ, ПРЕДСТАВИТЕЛЬ»). Такой вход распознаём
    напрямую до токенного матчинга (иначе он не совпадёт ни с именем
    истца, ни ответчика и ложно уйдёт в «Иное лицо» — кейс 33-5089/2026:
    бейдж «Апеллянт» встал на процессуального противника банка).
    Ровно одна сторона-роль в составе → она и есть роль (представитель —
    представитель этой же стороны). Ноль («ПРЕДСТАВИТЕЛЬ») или несколько
    разных ролей → сторона неопределима: role="", имя сохраняем как есть,
    is_bank считает _appellant_is_bank (даст None — фронт спрячет бейдж).
    """
    if not appellant_raw or not appellant_raw.strip():
        return ("", "")
    role_words = appellant_role_words(appellant_raw)
    if role_words is not None:
        if len(role_words) == 1:
            return (role_words[0], role_words[0])
        return ("", appellant_raw.strip())
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


def _bare_case_number(num: str) -> str:
    """«2-216/2026 (2-1156/2025;)» → «2-216/2026». Нужно потому, что поиск
    в судах возвращает только текущий номер, а в cases.json хранится полный
    с суффиксом переномерования."""
    s = (num or "").strip()
    if "(" in s:
        bare = s.split("(")[0].strip()
        return bare or s
    return s
