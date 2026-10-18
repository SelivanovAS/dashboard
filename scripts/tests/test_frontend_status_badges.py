"""Стражи бейджей карточки дела (разбор юриста 14.08.2026)."""

from __future__ import annotations

from .js_support import read_source as _read

import re


# ===== 1. Дата статуса =====


def test_transfer_to_judge_regex_matches_real_wording():
    """Регексп обязан ловить «Передача материалов судье» — реальный текст суда.

    До 14.08.2026 искалось «передача дела судье»: 0 совпадений в cases.json,
    19 подходящих дел трека молча показывались как «Не назначено».
    """
    src = _read("app.js")
    m = re.search(r"transferToJudge\s*=[^;]*?/([^/]+)/i\.test", src)
    assert m, "Проверка передачи судье исчезла из prepareCaseViewModel."
    паттерн = re.compile(m.group(1), re.I)
    assert паттерн.search("Передача материалов судье. 16:01. 05.08.2026"), (
        "Регексп не матчит формулировку суда «Передача МАТЕРИАЛОВ судье» — "
        "лейбл «Передано судье» снова станет мёртвым."
    )
    assert паттерн.search("Передача дела судье"), "Старая формулировка тоже должна ловиться."
    assert not паттерн.search("Регистрация иска (заявления, жалобы) в суде"), (
        "Регексп стал слишком широким — «Передано судье» встанет на любое дело."
    )


def test_arrived_in_court_label_uses_filing_date():
    """«Не назначено» → «Поступило в суд» + дата поступления (решение юриста)."""
    src = _read("app.js")
    m = re.search(r"const\s+arrivedInCourt\s*=([^;]+);", src)
    assert m, "Предикат arrivedInCourt исчез — вернулось «Не назначено» без даты."
    предикат = m.group(1).replace(" ", "")
    assert "ds==='awaiting'" in предикат, "Лейбл должен подменяться только у awaiting."
    assert "c.dateReceived" in предикат, (
        "Лейбл не проверяет дату поступления: без filing_date «Поступило в суд» "
        "было бы утверждением без даты — юрист просил обратное."
    )
    assert "stageGroup(c)==='first_instance'" in предикат, (
        "Лейбл ушёл на апелляцию/кассацию: c.dateReceived — это filing_date "
        "ИСКА (1-я инст.), про поступление дела в апел. суд данных нет."
    )
    assert "!hasHearingDate(c)" in предикат, (
        "Лейбл встаёт у дел с ПРОШЕДШИМ заседанием (там статус тоже awaiting), "
        "и его дата справа читалась бы как дата поступления — 33-5546/2026."
    )
    assert "'Поступило в суд'" in src


def test_status_date_reaches_the_date_column():
    """vm.statusDate обязан читаться в buildHearingHtml.

    Ровно на этом месте уже был мёртвый код: statusInlineDate/statusBelowDate
    считались в prepareCaseViewModel и не читались НИГДЕ — бейджи так и стояли
    без дат. Поле без потребителя = правка, которой не видно на экране.
    """
    src = _read("app.js")
    fn = re.search(r"function buildHearingHtml\(.*?\n\}", src, re.S)
    assert fn, "buildHearingHtml не найдена."
    тело = fn.group(0)
    assert "vm.statusDate" in тело, (
        "buildHearingHtml не читает vm.statusDate — дата статуса никуда не "
        "попадёт, как это уже случилось со statusInlineDate."
    )
    assert "status-date" in тело, "Дата статуса печатается не своим классом."
    for мёртвое in ("statusInlineDate", "statusBelowDate"):
        assert мёртвое not in src, (
            f"{мёртвое} вернулся: поле считается и не читается — оно снова "
            "разойдётся с тем, что видно на экране."
        )
    css = _read("styles.css")
    m = re.search(r"^\.status-date\s*\{([^}]*)\}", css, re.M)
    assert m, "Класс .status-date пропал из styles.css."
    правило = m.group(1).replace(" ", "")
    assert "var(--fg-3)" in правило, (
        "Дата статуса красится наравне с датой заседания — она перетянет взгляд "
        "с реально назначенных заседаний."
    )


def test_status_date_yields_to_court_deadline():
    """При «Без движения до …» (кассация) дата события не подменяет срок суда.

    Предикат ОДИН на обе стороны — hasHearingDate: prepareCaseViewModel решает,
    считать ли дату статуса, а buildHearingHtml — печатать ли колонку. Разъедься
    они, дата статуса встала бы рядом с датой заседания или потерялась совсем
    (голого c.nextDate мало: он бывает заполнен при непечатаемой метке).
    """
    src = _read("app.js")
    m = re.search(r"else if\(\(ds==='paused'\|\|ds==='suspended'\)([^;]+);", src)
    assert m, "Ветка статусной даты для paused/suspended не найдена."
    ветка = m.group(0).replace(" ", "")
    assert "!hasHearingDate(c)" in ветка, (
        "Дата события затирает суд. срок устранения недостатков "
        "(suspended_until) — «б/дв. до ДД.ММ» важнее даты самого события."
    )
    fn = re.search(r"function buildHearingHtml\(.*?\n\}", src, re.S).group(0)
    assert "if(!hasHearingDate(c))" in fn.replace(" ", ""), (
        "buildHearingHtml завёл свою копию условия «есть ли дата» — она "
        "разойдётся с предикатом даты статуса."
    )


def test_hearing_type_falls_back_to_last_event_without_events():
    """Тип заседания в bank-картотеке — из last_event, раз events там нет.

    В треке «Иски банка» events грузятся лениво (ensureBankEvents, при открытии
    drawer), и в списке их нет вовсе: все заседания показывались общим
    «Назначено», хотя в основной картотеке рядом стоят «Беседа»/«Предв-ое СЗ»/
    «Основное СЗ». Фолбэк односторонний (не узнал — оставил «Назначено»),
    поэтому и безопасен; проверка на 478 делах трека 14.08.2026 дала 355
    попаданий, 0 ошибок типа, а среди дел с БУДУЩИМ заседанием — 264 из 264.
    ⚠️ Только при пустом events[]: где события есть, точный матч по дате
    заседания надёжнее текста последнего события.
    """
    src = _read("app.js")
    m = re.search(r"else if\(nextDate&&!primaryEvents\.length\)\{[^}]*\}", src)
    assert m, (
        "Фолбэк типа заседания по last_event пропал — картотека «Иски банка» "
        "вернётся к сплошному «Назначено»."
    )
    assert "classifyEvent(evText)" in m.group(0), (
        "Фолбэк завёл свой классификатор вместо общего classifyEvent."
    )
    ветка_с_events = re.search(r"if\(nextDate&&primaryEvents\.length\)", src)
    assert ветка_с_events and ветка_с_events.start() < m.start(), (
        "Точный матч по дате из events обязан идти ПЕРВЫМ: там, где события "
        "есть, текст последнего события — менее надёжный источник."
    )


def test_conversation_hearing_recognised():
    """«Беседа. …» — тоже подготовительный этап (бэк это знал, фронт нет)."""
    src = _read("app.js")
    fn = re.search(r"function classifyEvent\(.*?\n\}", src, re.S)
    assert fn, "classifyEvent не найдена."
    assert "беседа" in fn.group(0), (
        "classifyEvent не знает «Беседа» — заголовок карточки ГАС наравне с "
        "«Подготовка дела (собеседование)»; такие дела покажут «Назначено»."
    )


def test_generic_hearing_label_names_the_type():
    """Неопознанное заседание называется «Заседание», а не «Назначено».

    После фолбэка по last_event generic-ветка осталась ровно у кассационных
    дел: карточка 7kas публикует дату заседания, но не название события
    (текст в cassation.events пустой). Бейдж стоит в одном ряду с «Беседа»/
    «Предв-ое СЗ»/«Основное СЗ» — он называет ТИП; состояние «назначено» и
    так видно по дате рядом (решение юриста 14.08.2026).
    """
    src = _read("app.js")
    m = re.search(r"const STATUS_LABELS=\{[^}]*\}", src)
    assert m, "STATUS_LABELS не найдены."
    карта = m.group(0)
    assert re.search(r"scheduled:\s*'Заседание'", карта), (
        "Generic-бейдж снова «Назначено»: в ряду с «Беседа»/«Предв-ое СЗ» он "
        "должен называть тип заседания, а не состояние."
    )
    assert re.search(r"awaiting:\s*'Не назначено'", карта), (
        "Ветка awaiting («дата не назначена») потеряла свою подпись."
    )
