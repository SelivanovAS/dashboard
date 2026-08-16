#!/usr/bin/env python3
"""Генератор материалов теста скорости «СберСуд против ручного обхода судов».

Два сотрудника получают ОДИНАКОВЫЙ пул дел и одинаковый бланк вопросов: один
собирает ответы в СберСуде, второй — вручную на сайтах судов. Метрика —
время + точность.

Скрипт офлайновый (только чтение data/*.json, ни одного HTTP-запроса) и не
входит в конвейер прогона. Пул подбирается заново при каждом запуске: даты
заседаний протухают, поэтому материалы генерируются за день-два до теста.

    python3 scripts/make_speed_test.py                # → docs/Тестирование_скорости_СберСуд.md
    python3 scripts/make_speed_test.py --stdout       # печать в консоль
    python3 scripts/make_speed_test.py --min-days 14  # заседания не ближе чем через 14 дней
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from court_monitor import config  # noqa: E402
from court_monitor.courts import (  # noqa: E402
    CASSATION_COURT,
    appeal_court_by_domain,
    fi_card_url,
    match_fi_court_by_short_name,
)
from court_monitor.storage import load_bank_json  # noqa: E402
from court_monitor.textutil import appellant_role_words, case_id_uid  # noqa: E402

DOC_PATH = os.path.join("docs", "Тестирование_скорости_СберСуд.md")

# Сколько дел в каждом блоке пула (итого 10 — согласовано с юристом).
BLOCK_SIZES = {"А": 3, "Б": 3, "В": 2, "Г": 2}

# Заседание должно быть достаточно далеко: до дня теста суд не успеет его
# провести и переназначить — иначе эталон разъедется с карточкой.
MIN_DAYS_AHEAD = 7

ШТРАФ_ЗА_ОШИБКУ_МИН = 2


# --------------------------------------------------------------------------
# Вспомогательное


def parse_date(value) -> datetime.date | None:
    """Дата из данных: и «ДД.ММ.ГГГГ», и ISO — оба формата в cases.json."""
    s = str(value or "")
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", s)
    if m:
        try:
            return datetime.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            return None
    try:
        return datetime.date.fromisoformat(s[:10])
    except ValueError:
        return None


def ru_date(value) -> str:
    d = parse_date(value)
    return d.strftime("%d.%m.%Y") if d else "—"


def hearing_time(fi: dict) -> str:
    """Время заседания; 00:00 — заглушка ГАС, её не показываем."""
    t = (fi.get("hearing_time") or "").strip()
    return "" if t in ("", "00:00") else t


def fi_url(fi: dict) -> str:
    """Ссылка на карточку 1-й инстанции с правильной судебной площадкой.

    fi_card_url резолвит суд по домену и схлопывает двухсерверные домены до
    srv_num=1 (Покачи живёт на том же домене, что Нижневартовский районный,
    но на площадке 2). Имя суда в записи различает их точно, поэтому сначала
    пробуем резолв по имени.
    """
    court = match_fi_court_by_short_name(fi.get("court") or "")
    cid, cuid = case_id_uid(fi.get("link", ""))
    if court and cid and cuid:
        return court.card_url(cid, cuid)
    return fi_card_url(fi)


def clean_case_number(number: str) -> str:
    """Комбо-номер «2-3063/2026 (2-10385/2025;)» → «2-3063/2026».

    Участник ищет дело по номеру — хвост прошлого года только помешает.
    """
    return re.sub(r"\s*\(.*$", "", (number or "").strip()).strip()


def appeal_card_url(ap: dict) -> str:
    cid, cuid = case_id_uid(ap.get("link", ""))
    if not (cid and cuid):
        return ""
    return appeal_court_by_domain(ap.get("court_domain")).card_url(cid, cuid)


def cassation_card_url(cs: dict) -> str:
    cid, cuid = case_id_uid(cs.get("link", ""))
    if not (cid and cuid):
        return ""
    return CASSATION_COURT.card_url(cid, cuid)


def short_parties(case: dict) -> str:
    p = (case.get("plaintiff") or "").strip()
    d = (case.get("defendant") or "").strip()
    cut = lambda s: (s[:55] + "…") if len(s) > 56 else s  # noqa: E731
    return f"{cut(p) or '—'} → {cut(d) or '—'}"


def appellant_answer(block: dict) -> str:
    """Кто подал жалобу — именем или процессуальной ролью.

    Карточка суда в поле «Заявитель» даёт то настоящее имя, то слово-роль
    («ОТВЕТЧИК»), поэтому эталон печатает то же, что участник увидит на сайте.
    """
    raw = (block.get("appellant") or "").strip()
    roles = appellant_role_words(raw)
    if roles is None:
        return raw or (block.get("appellant_status") or "").strip() or "—"
    if roles:
        return " / ".join(roles)
    return raw or "—"


# --------------------------------------------------------------------------
# Отбор пула


def court_of(case: dict) -> str:
    return ((case.get("first_instance") or {}).get("court") or "").strip()


def _take(rows: list[tuple], limit: int, занятые_суды: set[str]) -> list[dict]:
    """Взять limit дел, по возможности из ещё не занятых судов.

    Суды разводятся глобально между блоками: ручной участник должен обойти
    несколько сайтов, а не листать один и тот же поиск.
    """
    picked = []
    for _, _, c in rows:
        if court_of(c) in занятые_суды:
            continue
        picked.append(c)
        занятые_суды.add(court_of(c))
        if len(picked) >= limit:
            return picked
    for _, _, c in rows:                      # добор, если разных судов мало
        if c not in picked:
            picked.append(c)
        if len(picked) >= limit:
            break
    return picked[:limit]


def pick_first_instance(cases, limit, floor, занятые_суды) -> list[dict]:
    """Блок А: банк-ответчик в 1-й инстанции с заседанием впереди."""
    rows = []
    for c in cases:
        if c.get("current_stage") != "first_instance" or c.get("bank_role") != "Ответчик":
            continue
        fi = c.get("first_instance") or {}
        hd = parse_date(fi.get("hearing_date"))
        # Судья и дата поступления обязательны — это ответы на два из трёх
        # вопросов блока; без них вопрос попал бы в бланк без эталона.
        if hd and hd >= floor and (fi.get("judge") or "").strip() and fi.get("filing_date"):
            rows.append((hd, c["id"], c))
    rows.sort(key=lambda r: (r[0], r[1]))
    return _take(rows, limit, занятые_суды)


def pick_bank(cases, limit, floor, занятые_суды) -> list[dict]:
    """Блок Б: иски банка — по одному делу трёх разных типов.

    Типы разные намеренно: тест проверяет не только «найти карточку», но и
    разные её вкладки — движение дела, решение, исполнительные листы.
    """
    with_writ, default_no_writ, upcoming = [], [], []
    for c in cases:
        fi = c.get("first_instance") or {}
        writs = [w for w in (fi.get("writs") or []) if w.get("status") == "Выдан"]
        hd = parse_date(fi.get("hearing_date"))
        decision = parse_date(fi.get("decision_date"))
        if writs:
            key = parse_date(writs[-1].get("issue_date")) or datetime.date(2000, 1, 1)
            with_writ.append((key, c["id"], c))
        elif fi.get("default_judgment") and decision:
            default_no_writ.append((decision, c["id"], c))
        elif hd and hd >= floor and not decision:
            upcoming.append((hd, c["id"], c))

    with_writ.sort(key=lambda r: (r[0], r[1]), reverse=True)   # свежий ИЛ
    default_no_writ.sort(key=lambda r: (r[0], r[1]), reverse=True)
    upcoming.sort(key=lambda r: (r[0], r[1]))

    picked = []
    for bucket in (with_writ, default_no_writ, upcoming):
        if len(picked) >= limit:
            break
        picked.extend(_take(bucket, 1, занятые_суды))
    return picked[:limit]


def pick_appeal(cases, limit, floor, занятые_суды) -> list[dict]:
    """Блок В: апелляция с назначенным заседанием, роли банка по возможности разные."""
    rows = []
    for c in cases:
        if c.get("current_stage") != "appeal":
            continue
        ap = c.get("appeal") or {}
        hd = parse_date(ap.get("hearing_date"))
        # Податель жалобы обязателен: иначе вопрос попадёт в бланк без эталона
        # и оба участника гарантированно получат штраф.
        if hd and hd >= floor and appellant_answer(ap) != "—":
            rows.append((hd, c["id"], c))
    rows.sort(key=lambda r: (r[0], r[1]))

    picked, seen_roles = [], set()
    for _, _, c in rows:
        if c.get("bank_role") in seen_roles:
            continue
        picked.append(c)
        seen_roles.add(c.get("bank_role"))
        занятые_суды.add(court_of(c))
        if len(picked) >= limit:
            return picked
    for _, _, c in rows:                      # добор, если ролей не хватило
        if c not in picked:
            picked.append(c)
            занятые_суды.add(court_of(c))
        if len(picked) >= limit:
            break
    return picked[:limit]


def pick_cassation(cases, limit, floor, занятые_суды) -> list[dict]:
    """Блок Г: кассация — одно дело с заседанием впереди, одно с готовым исходом.

    8Г-номер производства лежит в `cassation.case_number`; поле
    `cassation_number` в данных пустое — по нему фильтровать нельзя.
    """
    upcoming, decided = [], []
    for c in cases:
        if c.get("current_stage") != "cassation":
            continue
        cs = c.get("cassation") or {}
        if not cassation_number(cs):
            continue
        hd = parse_date(cs.get("hearing_date"))
        if cs.get("outcome"):
            decided.append((hd or datetime.date(2000, 1, 1), c["id"], c))
        elif hd and hd >= floor:
            upcoming.append((hd, c["id"], c))
    upcoming.sort(key=lambda r: (r[0], r[1]))
    decided.sort(key=lambda r: (r[0], r[1]), reverse=True)

    picked = _take(upcoming, 1, занятые_суды)
    if len(picked) < limit:
        picked.extend(_take(decided, limit - len(picked), занятые_суды))
    if len(picked) < limit:
        picked.extend(c for _, _, c in upcoming if c not in picked)
    return picked[:limit]


def cassation_number(cs: dict) -> str:
    return (cs.get("case_number") or cs.get("cassation_number") or "").strip()


def cassation_outcome(cs: dict) -> str:
    """Итог рассмотрения кассации словами суда.

    `review_result` у большинства дел — стадия («ВОЗБУЖДЕНО КАССАЦИОННОЕ
    ПРОИЗВОДСТВО…»), а не итог, поэтому первым идёт `result_text`.
    """
    text = (cs.get("result_text") or "").strip()
    if text:
        return text
    review = (cs.get("review_result") or "").strip()
    if review and not review.upper().startswith("ВОЗБУЖДЕНО"):
        return review
    return ""


# --------------------------------------------------------------------------
# Вопросы и эталонные ответы


def задание(case: dict, block: str) -> dict:
    """Что участник видит (входные данные) и что должен найти (вопросы+эталон)."""
    fi = case.get("first_instance") or {}
    ap = case.get("appeal") or {}
    cs = case.get("cassation") or {}

    вход = {
        "номер": clean_case_number(fi.get("case_number") or case["id"]),
        "суд": fi.get("court") or "—",
        "стороны": short_parties(case),
    }
    ссылки = [("карточка 1-й инстанции", fi_url(fi))]

    if block == "А":
        t = hearing_time(fi)
        # Третий вопрос — дата поступления иска, а не «последнее событие»:
        # у дела с назначенным заседанием последнее событие и есть это
        # заседание, вопрос дублировал бы первый.
        вопросы = [
            ("Дата и время следующего заседания",
             ru_date(fi.get("hearing_date")) + (f" в {t}" if t else " (время не указано)")),
            ("Судья", (fi.get("judge") or "—").strip()),
            ("Дата поступления иска в суд", ru_date(fi.get("filing_date"))),
        ]
    elif block == "Б":
        writs = [w for w in (fi.get("writs") or []) if w.get("status") == "Выдан"]
        decision = fi.get("decision_date")
        if writs:
            w = writs[-1]
            номер_ил = (w.get("blank_number") or w.get("electronic_id") or "—").strip()
            ответ_ил = f"да, выдан {ru_date(w.get('issue_date'))}, № {номер_ил}"
        else:
            ответ_ил = "нет, исполнительный лист не выдан"
        вопросы = [
            ("Вынесено ли решение и когда",
             f"да, {ru_date(decision)}" if decision else "нет, дело в производстве"),
            ("Заочное ли решение",
             "да, заочное" if fi.get("default_judgment") else "нет (обычное решение)"
             if decision else "решения нет"),
            ("Выдан ли исполнительный лист", ответ_ил),
        ]
    elif block == "В":
        t = hearing_time(ap)
        вопросы = [
            ("Номер дела в апелляции", ap.get("case_number") or case["id"]),
            ("Дата и время заседания в апелляции",
             ru_date(ap.get("hearing_date")) + (f" в {t}" if t else " (время не указано)")),
            ("Кто подал апелляционную жалобу", appellant_answer(ap)),
        ]
        url = appeal_card_url(ap)
        if url:
            ссылки.append(("карточка апелляции", url))
    else:  # Г
        исход = cassation_outcome(cs)
        вопросы = [
            ("Номер кассационного производства (8Г-…)", cassation_number(cs) or "—"),
            ("Кто подал кассационную жалобу", appellant_answer(cs)),
        ]
        # Вопрос по состоянию дела: рассмотренное спрашиваем про итог,
        # нерассмотренное — про дату заседания (иначе половина бланка пустая).
        if исход:
            вопросы.append(("Итог рассмотрения в кассации", исход))
        else:
            вопросы.append(("Дата заседания в кассации", ru_date(cs.get("hearing_date"))))
        url = appeal_card_url(ap)
        if url:
            ссылки.append(("карточка апелляции", url))
        url = cassation_card_url(cs)
        if url:
            ссылки.append(("карточка 7-го КСОЮ", url))

    return {
        "блок": block,
        "id": case["id"],
        "вход": вход,
        "вопросы": вопросы,
        "ссылки": [(n, u) for n, u in ссылки if u],
    }


# --------------------------------------------------------------------------
# Рендер документа


BLOCK_TITLES = {
    "А": "Дела в первой инстанции (банк — ответчик)",
    "Б": "Иски банка (банк — истец)",
    "В": "Дела в апелляции",
    "Г": "Дела в кассации",
}


def render(задания: list[dict], сегодня: datetime.date, всего_дел: dict) -> str:
    L: list[str] = []
    add = L.append
    всего_фактов = sum(len(z["вопросы"]) for z in задания)

    add("# Тест скорости: СберСуд против ручного поиска по судам")
    add("")
    add(f"> Материалы сгенерированы {сегодня.strftime('%d.%m.%Y')} "
        f"(`python3 scripts/make_speed_test.py`).")
    add("> Даты заседаний протухают — перегенерировать за день-два до теста.")
    add("")
    add(f"**Пул:** {len(задания)} дел, {всего_фактов} проверяемых фактов, "
        "три инстанции, суды по возможности не повторяются.")
    add(f"Отобраны из рабочей картотеки: {всего_дел['основные']} активных дел "
        f"основной картотеки и {всего_дел['банк']} дел трека «Иски банка».")
    add("")
    add("---")
    add("")

    # -- Регламент ---------------------------------------------------------
    add("## Регламент")
    add("")
    add("**Задача обоим участникам одна:** по каждому делу из списка найти актуальные "
        "сведения и записать их в бланк. Побеждает тот, кто быстрее сдаст бланк — "
        "с учётом штрафа за ошибки.")
    add("")
    add("**Участник 1 — СберСуд.** Любые возможности дашборда: поиск, фильтры, "
        "карточка дела, дайджест, «Мои дела».")
    add("")
    add("**Участник 2 — ручной поиск.** Только официальные источники: сайты районных "
        "и городских судов (ГАС «Правосудие», sudrf.ru), сайт Суда ХМАО-Югры, сайт "
        "7-го кассационного суда. Способ поиска — любой привычный. Нельзя пользоваться "
        "СберСудом и коммерческими агрегаторами.")
    add("")
    add("**Условия, одинаковые для обоих:**")
    add("")
    add("- Тест проводится утром рабочего дня, после планового прогона СберСуда "
        "(данные обновляются в 08:30 по ХМАО) — иначе преимущество будет нечестным "
        "в другую сторону.")
    add("- Оба стартуют одновременно по команде наблюдателя, бланки выдаются в закрытом "
        "виде и открываются на старте.")
    add("- Заранее список дел никому не показывать.")
    add("- Интернет, компьютер и браузер — одинаковые по классу; заранее открытые "
        "вкладки с сайтами судов не готовить.")
    add("- Вопросы наблюдателю по существу дел не задавать; уточнения только по форме бланка.")
    add("")
    add("**Что фиксирует наблюдатель:**")
    add("")
    add("- время старта и время сдачи бланка каждым участником (общее время);")
    add("- по возможности — отметку времени после каждого дела: это даёт разбивку "
        "«на каких делах разрыв больше всего» (обычно на апелляции и кассации).")
    add("")

    # -- Подсчёт -----------------------------------------------------------
    add("## Как считается результат")
    add("")
    add("1. **Чистое время** — от старта до сдачи заполненного бланка.")
    add("2. **Точность** — каждый факт сверяется с эталонным листом арбитра "
        f"(всего фактов: {всего_фактов}).")
    add(f"3. **Штраф** — за каждый неверный или незаполненный факт "
        f"+{ШТРАФ_ЗА_ОШИБКУ_МИН} минуты к времени. Без штрафа участник может "
        "выиграть, сдав пустой бланк.")
    add("4. **Итог** — чистое время + штраф. Отдельно фиксируется процент верных фактов.")
    add("")
    add("| Показатель | Участник 1 (СберСуд) | Участник 2 (ручной поиск) |")
    add("|---|---|---|")
    add("| Время старта | | |")
    add("| Время сдачи бланка | | |")
    add("| Чистое время | | |")
    add("| Верных фактов из " + str(всего_фактов) + " | | |")
    add(f"| Штраф (ошибок × {ШТРАФ_ЗА_ОШИБКУ_МИН} мин) | | |")
    add("| **Итоговое время** | | |")
    add("")
    add("**Важная оговорка про расхождения.** Если ответ СберСуда расходится с карточкой "
        "суда, это НЕ ошибка участника: значит, суд обновил карточку после ночного "
        "прогона. Такие случаи арбитр отмечает отдельной строкой — для доклада это "
        "честный и полезный факт (данные СберСуда отстают максимум на один прогон).")
    add("")
    add("---")
    add("")

    # -- Бланк -------------------------------------------------------------
    add("## Бланк участника")
    add("")
    add("> Один и тот же бланк выдаётся обоим участникам. Печатать со следующей строки "
        "до раздела «Эталонный лист» — эталон участникам не показывать.")
    add("")
    add("Участник: ______________________   Инструмент: ☐ СберСуд ☐ ручной поиск")
    add("")
    add("Время старта: ______   Время сдачи: ______")
    add("")

    for block in ("А", "Б", "В", "Г"):
        свои = [z for z in задания if z["блок"] == block]
        if not свои:
            continue
        add(f"### Блок {block}. {BLOCK_TITLES[block]}")
        add("")
        for i, z in enumerate(свои, 1):
            add(f"**{block}{i}. Дело № {z['вход']['номер']}** — {z['вход']['суд']}")
            add("")
            add(f"Стороны: {z['вход']['стороны']}")
            add("")
            for вопрос, _ in z["вопросы"]:
                add(f"- {вопрос}: ______________________________________")
            add("")
            add("Время после этого дела: ______")
            add("")
        add("")

    add("---")
    add("")

    # -- Эталон ------------------------------------------------------------
    add("## Эталонный лист (только для арбитра)")
    add("")
    add("> Ответы взяты из данных СберСуда на дату генерации. **В день теста арбитр "
        "обязан сверить их с карточками судов по ссылкам ниже** — суд мог обновить "
        "карточку. Эталон — это карточка суда, а не файл СберСуда.")
    add("")

    for block in ("А", "Б", "В", "Г"):
        свои = [z for z in задания if z["блок"] == block]
        if not свои:
            continue
        add(f"### Блок {block}. {BLOCK_TITLES[block]}")
        add("")
        for i, z in enumerate(свои, 1):
            add(f"**{block}{i}. Дело № {z['вход']['номер']}** — {z['вход']['суд']}  ")
            add(f"*(внутренний id в картотеке: {z['id']})*")
            add("")
            for вопрос, ответ in z["вопросы"]:
                add(f"- **{вопрос}:** {ответ}")
            add("")
            for имя, url in z["ссылки"]:
                add(f"  - [{имя}]({url})")
            add("")
        add("")

    add("---")
    add("")
    add("## Заметки для доклада")
    add("")
    add("- Пул подобран программно по типам стадий, а не вручную: «удобные» дела "
        "не выбирались. Это стоит проговорить вслух — первый вопрос из зала будет "
        "именно про подбор.")
    add("- Разрыв ожидаемо растёт на делах в апелляции и кассации: ручному участнику "
        "нужно найти дело на сайте другого суда, а связку «первая инстанция → "
        "апелляционный номер → кассационный номер» ему приходится восстанавливать "
        "самому.")
    add("- Полезно записать не только итог, но и то, где ручной участник застревал "
        "(капча, поиск по фамилии, дело нашлось не с первого раза) — в докладе "
        "это живее любой цифры.")
    add("")
    return "\n".join(L) + "\n"


# --------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description="Материалы теста скорости СберСуд vs ручной поиск")
    ap.add_argument("--stdout", action="store_true", help="печатать документ, не записывать файл")
    ap.add_argument("--min-days", type=int, default=MIN_DAYS_AHEAD,
                    help=f"заседание не ближе чем через N дней (по умолчанию {MIN_DAYS_AHEAD})")
    ap.add_argument("--out", default=DOC_PATH, help="путь итогового файла")
    args = ap.parse_args()

    сегодня = datetime.date.today()
    floor = сегодня + datetime.timedelta(days=args.min_days)

    with open(config.JSON_PATH, encoding="utf-8") as f:
        основные = json.load(f).get("cases", [])
    банк = load_bank_json(config.JSON_BANK_PATH, config.JSON_BANK_EVENTS_PATH).get("cases", [])

    занятые_суды: set[str] = set()
    блоки = [
        ("А", pick_first_instance(основные, BLOCK_SIZES["А"], floor, занятые_суды)),
        ("Б", pick_bank(банк, BLOCK_SIZES["Б"], floor, занятые_суды)),
        ("В", pick_appeal(основные, BLOCK_SIZES["В"], floor, занятые_суды)),
        ("Г", pick_cassation(основные, BLOCK_SIZES["Г"], floor, занятые_суды)),
    ]

    задания: list[dict] = []
    for block, дела in блоки:
        if len(дела) < BLOCK_SIZES[block]:
            print(f"⚠️  Блок {block}: найдено {len(дела)} дел из {BLOCK_SIZES[block]} — "
                  f"попробуйте меньший --min-days", file=sys.stderr)
        for case in дела:
            задания.append(задание(case, block))

    doc = render(задания, сегодня, {"основные": len(основные), "банк": len(банк)})

    if args.stdout:
        sys.stdout.write(doc)
    else:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(doc)
        print(f"✅ {args.out}: {len(задания)} дел, "
              f"{sum(len(z['вопросы']) for z in задания)} проверяемых фактов")
        суды = {z["вход"]["суд"] for z in задания}
        print(f"   судов в пуле: {len(суды)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
