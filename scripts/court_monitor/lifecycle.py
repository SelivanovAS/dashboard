# -*- coding: utf-8 -*-
"""Жизненный цикл дела: классификация событий карточки, state machine
стадий (advance_case_stage / is_case_archived / migrate_stages), дедупликация,
snapshot раундов после кассационного remanded, разделение активных/архивных.

Окна дней (FI_ARCHIVE_DAYS и др.) читаются как config.X.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, date

from court_monitor import config
from court_monitor.config import log
from court_monitor.textutil import parse_date, _bare_case_number

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
# «Объявлен перерыв» в заседании (ст. 157 ГПК) — то же заседание продолжается
# на новую дату/время. Это НЕ отложение (ст. 169) и НЕ рассмотрение с начала:
# отдельное событие fi_hearing_recess вместо fi_hearing_postponed.
_RECESS_RE = re.compile(r"объявл\w*\s+перерыв", re.I)

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
# Интерлокутивные «Вынесено определение …» — процессуальные решения о
# движении дела, которые НЕ являются датой заседания. Нужен, чтобы
# fallback-поиск «Даты заседания» в parse_case_card не принял дату
# определения о подготовке/назначении/отложении за дату слушания —
# реальная дата придёт отдельной session-строкой (см. _SESSION_START_RX).
# Инцидент: дело 2-406/2026 (определение о подготовке дела → ложное
# «заседание 03.06 без времени»).
_INTERLOCUTORY_PREP_RX = re.compile(
    r"подготовк\w*\s+дела"
    r"|о\s+назначени\w*\s+(?:предварительн\w*\s+)?(?:судебн\w*\s+)?заседани"
    r"|об\s+отложени",
    re.IGNORECASE,
)
# Акты принятия иска к производству / возбуждения дела — это процессуальное
# принятие, а НЕ дата заседания. Нужен по той же причине, что и
# _INTERLOCUTORY_PREP_RX: fallback «Даты заседания» в parse_case_card не должен
# хватать дату определения о принятии у свежепринятого дела (status «В
# производстве», заседание ещё не назначено). Инцидент: М-3524/2026 →
# 2-6430/2026 — фантомное «заседание» = дата публикации определения о принятии.
_ACCEPTANCE_RX = re.compile(
    r"о\s+принятии"
    r"|принят\w*\s+.*?к\s+производству"
    r"|приняти\w*\s+иск"
    r"|возбужден\w*\s+(?:гражданск\w*\s+)?дел",
    re.IGNORECASE,
)
_TO_FI_RULES_RE = re.compile(
    r"по\s+правилам\s+производства\s+в\s+суде\s+первой\s+инстанции"
    r"|перейти\s+к\s+рассмотрени\S*\s+по\s+правилам",
    re.I,
)
# Терминальные события 1-й инстанции, оформленные через «фантомную»
# Дату заседания в карточке (суд назначил «дату» определения, но
# реального судебного заседания не было). Покрывает возврат иска,
# отказ в принятии, передачу по подсудности. Используется в эмиссии
# fi_returned — чтобы не путать с настоящим «первое заседание».
_TERMINAL_FI_EVENT_RX = re.compile(
    r"возвращени\S*\s+иск"
    r"|возвращени\S*\s+заявлени"
    r"|материал\S*\s+возвращ"
    r"|отказан\S*\s+в\s+принят"
    r"|передан\S*\s+по\s+подсудност",
    re.IGNORECASE,
)


def _extract_return_reason(text: str) -> str:
    """Вынуть короткую причину возврата иска из текста события 1-й инст.

    На вход — что-то вроде «Решение вопроса о принятии иска… Возвращение
    иска (заявления, жалобы) заявления. ДЕЛО НЕ ПОДСУДНО ДАННОМУ СУДУ.
    08.05.2026». Возвращаем «дело не подсудно данному суду» (lowercase,
    без хвостовой даты). Если причина не распознана — пустая строка.
    """
    if not text:
        return ""
    # Берём сегменты между точками (а не текст целиком — он зашумлён
    # временем и датами публикации).
    segments = [s.strip() for s in re.split(r"[.;]", text) if s.strip()]
    reason_patterns = (
        "не подсудно", "не подсуден", "по подсудности",
        "не подведомств", "пропущен срок", "не оплачен",
        "не подписан", "не указан", "истец не явил",
    )
    for seg in segments:
        seg_low = seg.lower()
        if any(p in seg_low for p in reason_patterns):
            # Срезаем хвостовую дату ДД.ММ.ГГГГ если она прилипла.
            cleaned = re.sub(r"\s*\d{2}\.\d{2}\.\d{4}\s*$", "", seg)
            return cleaned.strip().lower()
    return ""


def _fi_return_reason_for_render(d: dict) -> str:
    """Причина возврата иска/материала для строки 3.2 «Изменения».

    Сначала коротко распознанная (`return_reason` из `_extract_return_reason`),
    иначе — первый осмысленный сегмент `event_text` без хвостовых времени/даты.
    Нужна, чтобы возврат материала, который больше не попадает в 3.5
    «Вынесенные решения», всё равно нёс понятную причину
    («материалы возвращены в связи с истечением срока…»)."""
    reason = (d.get("return_reason") or "").strip()
    if reason:
        return reason
    ev = (d.get("event_text") or "").strip()
    if not ev:
        return ""
    seg = re.split(r"[.;]", ev)[0].strip()
    seg = re.sub(r"\s*\d{1,2}:\d{2}\s*$", "", seg)   # прилипшее «09:50»
    return seg.lower()


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


def _is_latest_session_event(ev: dict, events: list) -> bool:
    """True, если ev — самое позднее по дате session-событие списка.

    Защита от ретроактивных правок карточки: суд может ДОПИСАТЬ текст
    («Рассмотрение дела начато с начала») в старую запись движения, и она
    всплывёт как «новая» в _events_newly_match (ключ = дата+текст). Настоящий
    перезапуск объявляется на актуальном заседании, т.е. это последнее
    session-событие по дате. Инцидент 30.06.2026 (дело 2-857/2026): суд дописал
    «начато с начала» в предварительное заседание 30.09.2025 — на 9 месяцев
    позже последнего заседания, у уже решённого дела.
    """
    ev_dt = parse_date(ev.get("date") or "")
    if not ev_dt:
        return False
    for e in events or []:
        if not _SESSION_START_RX.search(e.get("text") or ""):
            continue
        d = parse_date(e.get("date") or "")
        if d and d > ev_dt:
            return False
    return True


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
    return (datetime.now() - d).days > config.LEGACY_CSV_ARCHIVE_DAYS


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

def bank_is_third_party(case: dict) -> bool:
    """True, если роль банка в деле — «Третье лицо» (регистронезависимо).

    Пустая или нераспознанная роль → False: такие дела продолжаем парсить,
    правило «не следим за третьими лицами в cassation_watch» применяется
    только при явной роли.
    """
    return (case.get("bank_role") or "").strip().lower() == "третье лицо"


def should_parse_fi_card(case: dict) -> bool:
    """Нужно ли на этом прогоне парсить карточку 1-й инстанции по делу.

    - `first_instance`   — да (мониторинг дела + ловим апел. жалобу).
    - `cassation_watch`  — да (после апелляции ловим касс. жалобу в 1-й инст.),
      КРОМЕ дел, где банк — третье лицо: их карточку не парсим, кассацию по
      ним обнаружит поиск 7kas по имени банка (`link_cassation_cases` —
      «догоняем» из cassation_watch либо воскрешение из архива). Решение
      юриста 13.07.2026: раннее предупреждение fi_cassation_filed для третьих
      лиц не стоит ежедневного парсинга 120-дневного окна.
    - `awaiting_appeal`  — да, ПОКА дело не направлено в апелляцию: продолжаем
      следить за карточкой 1-й инст. (промежуточные события, «направлено в
      вышестоящую инстанцию»). После `sent_to_appeal` — ждём только появления
      апел. карточки (её найдёт `link_cases`).
    - `cassation_pending`— да, ПОКА дело не направлено в кассацию: следим за
      карточкой 1-й инст. до «направлено в кассационный суд». После
      `sent_to_cassation` — ждём только появления карточки на 7kas
      (её найдёт `link_cassation_cases`). Роль банка здесь не проверяем:
      жалоба уже подана, парсить осталось недолго.
    - прочие стадии (`appeal`/`cassation`/`awaiting_relink`) — нет: там либо
      парсим карточку вышестоящего суда, либо ждём появления дела по номеру.

    Появление карточки в вышестоящем суде уводит дело в `appeal`/`cassation`
    (см. `link_cases`/`link_cassation_cases`), где предикат тоже вернёт False —
    второе условие «стоп» из ТЗ юриста.
    """
    stage = case.get("current_stage")
    fi = case.get("first_instance") or {}
    if not fi.get("case_number"):
        return False
    if stage == "cassation_watch":
        # Пустая/неизвестная роль → парсим (безопасный дефолт).
        return not bank_is_third_party(case)
    if stage == "first_instance":
        return True
    if stage == "awaiting_appeal":
        return not (fi.get("sent_to_appeal") or fi.get("sent_to_appeal_date"))
    if stage == "cassation_pending":
        return not (fi.get("sent_to_cassation") or fi.get("sent_to_cassation_date"))
    return False


def appeal_card_linked(case: dict) -> bool:
    """True, если апел. карточка уже связана с делом (link_cases нашёл её
    или дело заведено «с апелляции»). Используется, чтобы не дублировать
    в дайджест эхо-событие fi_appeal_filed из карточки 1-й инст.: юрист
    уже знает об апелляции из самой апел. карточки. Stub-блоки appeal без
    case_number (например, от _apply_fi_appellant) связкой не считаются.
    На 2-м круге после remanded блок обнулён (_snapshot_round_to_history) —
    новая жалоба нового круга не глушится."""
    ap = case.get("appeal") or {}
    return bool((ap.get("case_number") or "").strip())


def cassation_card_linked(case: dict) -> bool:
    """True, если карточка 7kas уже связана (link_cassation_cases или
    discovery). Аналог appeal_card_linked для кассации: глушит эхо-события
    fi_cassation_filed / fi_sent_to_cassation в дайджесте. Пред-заполненный
    блок cassation только с appellant_* (без case_number) связкой не считается."""
    cs = case.get("cassation") or {}
    return bool((cs.get("case_number") or "").strip())


# «Догоняющий» класс событий 1-й инст.: для дела с уже связанной вышестоящей
# карточкой это пересказ давно известного (решение, на которое подана жалоба;
# его акт; служебные статусы). Паводок 07.07.2026: первый парс 60 карточек
# «с апелляции» дал 272 таких события и дайджест на 48 КБ.
FI_ECHO_CATCHUP_TYPES = (
    "fi_resolved", "fi_act_published", "fi_act_text_published",
    "fi_motivirovka_emitted", "fi_final_event", "fi_status_change",
)


def suppress_fi_echo_events(case: dict, change: dict) -> list[str]:
    """Убрать из change["type"] эхо-события, если вышестоящая карточка уже
    связана с делом (ТЗ юриста 07.07.2026). Глушится ТОЛЬКО доставка в
    дайджест/push: флаги и данные в first_instance к этому моменту уже
    записаны и питают state machine / бейджи / drawer как обычно.

    - апелляция связана → эхо: fi_appeal_filed + весь FI_ECHO_CATCHUP_TYPES;
    - кассация связана → эхо: fi_cassation_filed, fi_sent_to_cassation,
      fi_appeal_filed (апел. жалоба — древняя история для дела в кассации)
      + FI_ECHO_CATCHUP_TYPES.
    Живые события (заседания, возвраты, смена роли банка) не трогаем.

    Отдельно (независимо от связки) схлопывает дубль об одном акте в одном
    прогоне: fi_act_published + fi_act_text_published → остаётся только
    текст (строку «изготовлено» рендер и так прятал, но счётчик сводки
    считал оба — «40 решений изготовлено · 40 текстов решений»).

    Возвращает список убранных ЭХО-типов (для лога); схлопывание дубля в
    него не входит. Тяжёлый details["act_text"] вычищается вместе с
    подавленным fi_act_text_published, чтобы не разбухал context-снимок.
    """
    types = change.get("type") or []
    if not types:
        return []
    drop: set[str] = set()
    ap_linked = appeal_card_linked(case)
    cs_linked = cassation_card_linked(case)
    if ap_linked or cs_linked:
        drop.add("fi_appeal_filed")
        drop.update(FI_ECHO_CATCHUP_TYPES)
    if cs_linked:
        drop.update(("fi_cassation_filed", "fi_sent_to_cassation"))
    removed = [t for t in types if t in drop]
    if removed:
        change["type"] = [t for t in types if t not in drop]
        if "fi_act_text_published" in removed:
            (change.get("details") or {}).pop("act_text", None)
    ts = change["type"]
    if "fi_act_published" in ts and "fi_act_text_published" in ts:
        ts.remove("fi_act_published")
    return removed


# Анонсы заседаний: событие несёт details["hearing_date"]. Дата в прошлом —
# не анонс, а раскопанная история карточки (первый парс после backfill).
_FI_HEARING_ANNOUNCE_TYPES = (
    "fi_hearing_new", "fi_hearing_next",
    "fi_hearing_postponed", "fi_hearing_recess",
)
# Жалобы/направления: тип → ключ details с датой самого события.
_FI_DATED_COMPLAINT_TYPES = {
    "fi_appeal_filed": "appeal_filed_date",
    "fi_cassation_filed": "cassation_filed_date",
    "fi_sent_to_cassation": "sent_to_cassation_date",
}


def suppress_stale_fi_events(change: dict, today: date | None = None) -> list[str]:
    """Убрать из change["type"] стародатные события (дополнение к
    suppress_fi_echo_events — то ловит «вышестоящее дело уже известно»,
    это — «новость протухла», даже если вышестоящей карточки нет):

    - анонс заседания (fi_hearing_new/next/postponed/recess) с датой
      СТРОГО в прошлом — «заседание назначено на 17.12.2025» в июле-2026
      не новость (сегодняшняя дата — ещё анонс);
    - жалоба/направление в касс. суд с датой старше
      config.DIGEST_STALE_EVENT_DAYS (первый парс старой карточки).

    Дата отсутствует/не парсится — событие остаётся (fail-open: лучше
    лишняя строка, чем молча съеденная новость). Флаги, стадии и данные
    JSON не трогаем — фильтруется только доставка в дайджест/push.
    Возвращает список убранных типов (для лога).
    """
    types = change.get("type") or []
    if not types:
        return []
    if today is None:
        today = date.today()
    details = change.get("details") or {}
    removed: list[str] = []
    kept: list[str] = []
    for t in types:
        stale = False
        if t in _FI_HEARING_ANNOUNCE_TYPES:
            d = parse_date(details.get("hearing_date") or "")
            if d and d.date() < today:
                stale = True
        elif t in _FI_DATED_COMPLAINT_TYPES:
            d = parse_date(details.get(_FI_DATED_COMPLAINT_TYPES[t]) or "")
            if d and (today - d.date()).days > config.DIGEST_STALE_EVENT_DAYS:
                stale = True
        (removed if stale else kept).append(t)
    if removed:
        change["type"] = kept
    return removed


def dedupe_fi_changes(fi_changes: list[dict]) -> list[dict]:
    """Схлопнуть одинаковые fi_changes от разных записей одного FI-дела.

    Одно дело 1-й инст. может жить в двух записях cases.json (апелляция по
    существу + частная жалоба — у каждой свой 33-номер, обе в
    cassation_watch). Обе парсят ОДНУ карточку 1-й инст. и дают идентичные
    события — в дайджесте дело двоится (инцидент 07.07: 2-155/2025 пришло
    дважды). Ключ — (номер дела, типы, details) целиком: разные события
    одного дела не склеиваются."""
    seen: set[str] = set()
    out: list[dict] = []
    for ch in fi_changes:
        key = json.dumps(
            [ch.get("case"), ch.get("type"), ch.get("details")],
            ensure_ascii=False, sort_keys=True, default=str,
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(ch)
    return out


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
        if ap_hearing and (now - ap_hearing).days >= config.APPEAL_NO_ACT_GRACE_DAYS:
            case["current_stage"] = "cassation_watch"
            return "appeal"
        return None

    if stage == "cassation_watch":
        # Переходим и по флагу без даты: короткая вкладка «Обжалование»
        # умеет показать касс. жалобу («Заявитель жалобы») без «Даты
        # поступления» — см. parse_case_card. Без этого дело зависает в
        # cassation_watch и через 120 дней уходит в архив с фактически
        # поданной жалобой (зеркало кейса 2-208/2026 по 1-й инст.).
        if (fi.get("cassation_filed_date") or fi.get("sent_to_cassation_date")
                or fi.get("cassation_filed") or fi.get("sent_to_cassation")):
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
    - first_instance: «Решено»/«Возвращено» + FI_ARCHIVE_DAYS (60) от hearing_date без апел. жалобы.
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
        # Защита от потери даты: если флаг жалобы/кассации стоит, но дата
        # не извлечена (короткая вкладка, расхождение шаблонов sudrf) —
        # держим в активных, парсер next-cron вытащит дату из «ДВИЖЕНИЕ
        # ЖАЛОБЫ». См. кейс 2-208/2026: дело уходило в архив раньше, чем
        # парсер обнаруживал апел. жалобу.
        if fi.get("appeal_filed") or fi.get("cassation_filed") or fi.get("sent_to_cassation"):
            return False
        if fi.get("status", "").strip() not in ("Решено", "Возвращено"):
            return False
        # Якорь окна — дата заседания, а если её нет, дата последнего события
        # карточки. Запасной якорь нужен для исков, возвращённых на стадии
        # принятия: заседания не было, а строку «Решение вопроса о принятии
        # иска → Возвращение искового заявления» парсер намеренно не берёт за
        # дату решения (_ACCEPTANCE_RX в cards.py — иначе у свежепринятых дел
        # появлялась фантомная «дата заседания», кейс М-3524/2026). Без
        # запасного якоря такое дело висело в активных вечно и опрашивалось
        # каждый прогон — кейс 9-1012/2026, возвращён 08.06.2026, найден через
        # 7 дней (мимо _discovered_already_resolved_old, который проставляет
        # якорь делам старше FI_ARCHIVE_DAYS). То же правило уже действует на
        # фронте — isArchived в app.js считает от lastEventDate.
        # Обе даты пусты → не архивируем (защита от пустых данных прежняя).
        anchor = (parse_date(fi.get("hearing_date") or "")
                  or parse_date(fi.get("event_date") or ""))
        if anchor and (now - anchor).days > config.FI_ARCHIVE_DAYS:
            return True
        return False

    if stage in ("awaiting_appeal", "appeal", "cassation_pending", "awaiting_relink"):
        return False

    if stage == "cassation_watch":
        # Страховка (в норме advance_case_stage уже перевёл бы такое дело в
        # cassation_pending): при любом признаке касс. жалобы — даже флаге
        # без даты — из архива исключаем. Аналогична защите first_instance
        # от «флага без даты» выше.
        if (fi.get("cassation_filed") or fi.get("sent_to_cassation")
                or fi.get("cassation_filed_date")
                or fi.get("sent_to_cassation_date")):
            return False
        ap_hearing = parse_date(ap.get("hearing_date") or "")
        if ap_hearing and (now - ap_hearing).days > config.CASSATION_WATCH_DAYS:
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
            if act_d and (now - act_d).days > config.CASSATION_ACT_ARCHIVE_DAYS:
                return True
            # Акт не опубликован, но определение вынесено: 45 дней от
            # decision_date без публикации → архив без акта.
            dec_d = parse_date(cs.get("decision_date") or "")
            if (dec_d and not cs.get("act_published")
                    and (now - dec_d).days > config.CASSATION_NO_ACT_PUBLISH_DAYS):
                return True
        return False

    return False


def migrate_appeal_court_fields(cases: list[dict], default_court) -> int:
    """Идемпотентный бэкфилл суда в блоках `appeal`: court_domain / court /
    delo_id. Записи эпохи единственной апелляции суда не хранили — URL всегда
    пересобирался из глобального APPEAL_COURT. С мульти-апелляцией (Свердловская
    обл. + ЯНАО = два апел-суда) домен обязателен: по нему строятся ссылки
    (courts.appeal_court_by_domain) и составной ключ связки link_cases.

    default_court — CourtConfig апел-суда для бэкфилла (для существующих данных
    региона он один — исторический). Возвращает число дополненных блоков.
    """
    migrated = 0
    for case in cases:
        ap = case.get("appeal")
        if not isinstance(ap, dict) or not ap:
            continue
        changed = False
        if not (ap.get("court_domain") or "").strip():
            ap["court_domain"] = default_court.domain
            changed = True
        if not (ap.get("court") or "").strip():
            ap["court"] = default_court.name
            changed = True
        if not ap.get("delo_id"):
            ap["delo_id"] = default_court.delo_id
            changed = True
        if changed:
            migrated += 1
    return migrated


def migrate_stages(cases: list[dict]) -> int:
    """Идемпотентная миграция существующих дел под новую state-machine:
    - first_instance + appeal_filed_date → awaiting_appeal
    - appeal с опубликованным актом или заседанием старше 30 дней без акта
      → cassation_watch
    - cassation_watch с зарегистрированной касс. жалобой → cassation_pending
    Возвращает число мигрированных дел."""
    # Идемпотентно заполняем initial_bank_role у дел, где его ещё нет.
    # Используется в дайджесте, чтобы показать «было: <роль>» при изменении
    # bank_role (напр. банк исключён из ответчиков → стал «Третье лицо»).
    for case in cases:
        if not case.get("initial_bank_role") and case.get("bank_role"):
            case["initial_bank_role"] = case["bank_role"]
    migrated = 0
    for case in cases:
        changed = True
        while changed:
            prev = advance_case_stage(case)
            changed = prev is not None
            if changed:
                migrated += 1
    # Чистка кассации: если у блока одновременно есть suspended_until и
    # hearing_date, и hearing_date позже (или совпадает) — суспенд устарел
    # (заседание назначено уже после периода «без движения»). Иначе фронт
    # отрисует чип «б/дв.», а skip-logic будет долго не парсить дело.
    for case in cases:
        cs = case.get("cassation") or {}
        su_raw = (cs.get("suspended_until") or "").strip()
        hd_raw = (cs.get("hearing_date") or "").strip()
        if not su_raw or not hd_raw:
            continue
        m_su = _DATE_DDMMYYYY_RX.match(su_raw)
        m_hd = _DATE_DDMMYYYY_RX.match(hd_raw)
        if not (m_su and m_hd):
            continue
        try:
            su = date(int(m_su.group(3)), int(m_su.group(2)), int(m_su.group(1)))
            hd = date(int(m_hd.group(3)), int(m_hd.group(2)), int(m_hd.group(1)))
        except ValueError:
            log.debug(
                f"  {case.get('id', '?')}: не разобрал даты кассации "
                f"(suspended_until={su_raw!r}, hearing_date={hd_raw!r})"
            )
            continue
        if hd >= su:
            cs["suspended_until"] = ""
    return migrated


def dedupe_orphan_by_base_number(cases: list[dict]) -> int:
    """Идемпотентный дедуп «сирот» по базовому номеру 1-й инст.

    Сирота — запись с `current_stage="appeal"`, у которой `first_instance` —
    stub (нет `events`, `act_text`, `link`, `act_date`), а `appeal.case_number`
    заполнен. Возникает, если `link_cases` не сматчил апел. карточку с
    реальной записью 1-й инст. из-за «гибридного» номера
    `2-208/2026 (2-1148/2025;)` vs `2-208/2026`. До правки матчера это
    случалось регулярно; после правки — резервный щит на случай регрессии.

    Хозяин — запись с тем же `_bare_case_number(id)`, у которой есть реальные
    данные карточки 1-й инст. (`events` или `act_text`), и стадия
    `first_instance`/`awaiting_appeal`.

    Сливаем сироту в хозяина: дозаполняем `appeal` хозяина, не перезаписывая
    уже заполненные поля. Стадию хозяина переводим в `appeal`. Сироту
    удаляем из `cases` in-place.

    Возвращает число слитых сирот.
    """
    groups: dict[str, list[int]] = {}
    for i, c in enumerate(cases):
        base = _bare_case_number(c.get("id", ""))
        if base:
            groups.setdefault(base, []).append(i)

    to_remove: set[int] = set()
    merged = 0

    for base, idxs in groups.items():
        if len(idxs) < 2:
            continue

        orphans: list[int] = []
        owners: list[int] = []

        for i in idxs:
            c = cases[i]
            stage = c.get("current_stage", "")
            fi = c.get("first_instance") or {}
            apl = c.get("appeal") or {}

            if (
                stage == "appeal"
                and apl.get("case_number")
                and not c.get("discovered_via_cassation")
                and not _has_real_fi(c)
            ):
                orphans.append(i)
                continue

            if (
                stage in ("first_instance", "awaiting_appeal")
                and (fi.get("events") or fi.get("act_text"))
            ):
                owners.append(i)

        if len(orphans) == 1 and len(owners) == 1:
            orph = cases[orphans[0]]
            host = cases[owners[0]]
            orph_appeal = orph.get("appeal") or {}
            host_appeal = host.get("appeal")

            if not host_appeal:
                host["appeal"] = dict(orph_appeal)
            else:
                # Дозаполняем пустые поля у хозяина; `act_text`/`events`/
                # `link`/`act_date` хозяина не перезаписываем никогда.
                protected = {"act_text", "events", "link", "act_date"}
                for k, v in orph_appeal.items():
                    cur = host_appeal.get(k)
                    is_empty = cur in (None, "", [], False)
                    if k in protected:
                        if k not in host_appeal or is_empty:
                            host_appeal[k] = v
                    elif is_empty:
                        host_appeal[k] = v

            host["current_stage"] = "appeal"
            to_remove.add(orphans[0])
            merged += 1
            log.info(
                f"Дедуп: {base} сирота {orph.get('id', '?')} слита "
                f"в {host.get('id', '?')}"
            )
        elif len(orphans) + len(owners) > 2 or (orphans and not owners):
            log.warning(
                f"Дедуп: {base} неоднозначная группа "
                f"(сирот: {len(orphans)}, хозяев: {len(owners)}) — не трогаю"
            )

    if to_remove:
        cases[:] = [c for i, c in enumerate(cases) if i not in to_remove]

    return merged


def dedupe_cassation_by_internal_number(cases: list[dict]) -> int:
    """Идемпотентный дедуп записей с одинаковым `cassation.case_number`.

    Один и тот же `8Г-XXXX/YYYY` может оказаться в нескольких записях,
    если 7kas в разные периоды возвращал разный `fi_case_number` для одной
    касс. жалобы (после cassation_remanded → round+1; либо рассинхрон
    апел./1-инст. номера в выдаче 7kas). До правки `link_cassation_cases`
    индексировал записи только по 1-инст./апел. номерам — поэтому в БД
    возникал discovery-двойник с `discovered_via_cassation=true`. С правкой
    регрессия закрыта; эта функция чистит уже накопившиеся пары.

    Хост (winner) выбирается по приоритетам (от важного к менее важному):
    1. `discovered_via_cassation=False` сильнее, чем `True` — настоящее
       дело сильнее discovery-stub.
    2. Заполненный `appeal.case_number` сильнее.
    3. `_has_real_fi(case)` (FI прошёл реальный парсер) сильнее.
    4. Свежее `cassation.last_checked_at` сильнее.
    5. Длиннее JSON-сериализация — «информационная плотность» как tie-breaker.

    Полевой мердж в host:
    - top-level (`plaintiff`/`defendant`/`category`/`bank_role`) —
      дозаполняем пустые из loser, заполненные у host не перетираем.
    - `appeal` берём от loser только если у host `None`.
    - `first_instance` — оставляем у host, если у host real_fi или у
      loser тоже stub; иначе берём блок loser целиком.
    - `cassation` — заменяем целиком только если у loser свежее
      `last_checked_at`. `discovered_via_cassation` итогового блока — AND
      обоих флагов (если хоть у одного из дублей `False` — итог `False`).
    - `discovered_via_cassation` на верхнем уровне — то же AND.
    - `history` — мердж по `round`, без дублей.
    - `notes` — дописываем след слияния.

    Группы из ≥3 записей — все loser'ы сливаются в один host в порядке
    убывания «силы».

    Возвращает число удалённых loser-записей.
    """
    groups: dict[str, list[int]] = {}
    for i, c in enumerate(cases):
        cass = c.get("cassation") or {}
        cn = (cass.get("case_number") or "").strip()
        if cn:
            groups.setdefault(cn, []).append(i)

    def _score(c: dict) -> tuple:
        cass = c.get("cassation") or {}
        appeal = c.get("appeal") or {}
        last_checked = (cass.get("last_checked_at") or "").strip()
        return (
            0 if c.get("discovered_via_cassation") else 1,
            1 if (appeal and appeal.get("case_number")) else 0,
            1 if _has_real_fi(c) else 0,
            last_checked,
            len(json.dumps(c, ensure_ascii=False)),
        )

    today_iso = date.today().isoformat()
    to_remove: set[int] = set()
    merged = 0

    for cn, idxs in groups.items():
        if len(idxs) < 2:
            continue
        sorted_idxs = sorted(idxs, key=lambda i: _score(cases[i]), reverse=True)
        host_i = sorted_idxs[0]
        host = cases[host_i]

        for loser_i in sorted_idxs[1:]:
            loser = cases[loser_i]

            for k in ("plaintiff", "defendant", "category", "bank_role"):
                if not host.get(k) and loser.get(k):
                    host[k] = loser[k]

            if host.get("appeal") is None and loser.get("appeal"):
                host["appeal"] = loser["appeal"]

            if not _has_real_fi(host) and _has_real_fi(loser):
                host["first_instance"] = loser.get("first_instance") or {}

            host_cass = host.get("cassation") or {}
            loser_cass = loser.get("cassation") or {}
            host_lc = (host_cass.get("last_checked_at") or "").strip()
            loser_lc = (loser_cass.get("last_checked_at") or "").strip()
            if loser_lc and loser_lc > host_lc:
                merged_cass = dict(loser_cass)
                merged_cass["discovered_via_cassation"] = bool(
                    host_cass.get("discovered_via_cassation")
                ) and bool(loser_cass.get("discovered_via_cassation"))
                host["cassation"] = merged_cass

            host["discovered_via_cassation"] = bool(
                host.get("discovered_via_cassation")
            ) and bool(loser.get("discovered_via_cassation"))

            loser_history = loser.get("history") or []
            if loser_history:
                host_history = host.get("history") or []
                seen_rounds = {
                    h.get("round") for h in host_history if isinstance(h, dict)
                }
                for h in loser_history:
                    r = h.get("round") if isinstance(h, dict) else None
                    if r not in seen_rounds:
                        host_history.append(h)
                        seen_rounds.add(r)
                host["history"] = host_history

            merge_tag = (
                f"дубль {loser.get('id', '?')} (касс. {cn}) "
                f"слит автоматически {today_iso}"
            )
            old_notes = host.get("notes") or ""
            if merge_tag not in old_notes:
                sep = " • " if old_notes else ""
                host["notes"] = (old_notes + sep + merge_tag).strip()

            to_remove.add(loser_i)
            merged += 1
            log.info(
                f"Дедуп касс.: {cn} loser {loser.get('id', '?')} "
                f"слит в host {host.get('id', '?')}"
            )

    if to_remove:
        cases[:] = [c for i, c in enumerate(cases) if i not in to_remove]

    return merged


def dedupe_cassation_by_uid(cases: list[dict]) -> int:
    """Idempotent-дедуп: слить касс. discovery-двойник в реальную апел./watch-
    запись по совпадению УИД (`86RS...`).

    Класс бага: апел.-запись (`33-XXXX`, стадии `appeal`/`cassation_watch`) не
    имела `first_instance.case_number`, поэтому `link_cassation_cases` не находил
    её по fi_case_number с 7kas и плодил `discovered_via_cassation`-дубль
    (`2-278/2025` ↔ `33-2082/2026`). После того как апел.-запись получает УИД
    (бэкфилл из апел. карточки), discovery-дубль и якорь делят один УИД — здесь
    их и сшиваем.

    Сливаем ТОЛЬКО когда в группе ровно один host — НЕ-discovery (anchor с
    appeal/first_instance), а остальные — `discovered_via_cassation`. Если
    не-discovery записей ≥2 — не трогаем (это разные дела с коллизией данных,
    хотя по УИД такого быть не должно). Одиночные discovery (без anchor) тоже
    оставляем — это настоящие находки кассации.

    `id` host сохраняется (как в ручных сшивках bea0f7d) — иначе ломаются
    watchlist-подписки и фронт.

    Возвращает число удалённых discovery-двойников.
    """
    groups: dict[str, list[int]] = {}
    for i, c in enumerate(cases):
        seen: set[str] = set()
        for block in ("first_instance", "appeal", "cassation"):
            b = c.get(block) or {}
            uid = (b.get("judicial_uid") or "").strip()
            if uid:
                seen.add(uid)
        for uid in seen:
            groups.setdefault(uid, []).append(i)

    today_iso = date.today().isoformat()
    to_remove: set[int] = set()
    merged = 0

    for uid, idxs in groups.items():
        idxs = [i for i in idxs if i not in to_remove]
        if len(idxs) < 2:
            continue
        anchors = [i for i in idxs if not cases[i].get("discovered_via_cassation")]
        losers = [i for i in idxs if cases[i].get("discovered_via_cassation")]
        if len(anchors) != 1 or not losers:
            if len(anchors) > 1:
                log.warning(
                    f"Дедуп касс./УИД: {uid} — {len(anchors)} не-discovery "
                    f"записей, не трогаю"
                )
            continue

        host = cases[anchors[0]]
        for loser_i in losers:
            loser = cases[loser_i]

            for k in ("plaintiff", "defendant", "category", "bank_role"):
                if not host.get(k) and loser.get(k):
                    host[k] = loser[k]

            # Дозаполняем пустые поля first_instance host из loser (у discovery
            # есть case_number/court_domain/judicial_uid/decision_date).
            host_fi = host.get("first_instance")
            loser_fi = loser.get("first_instance") or {}
            if isinstance(host_fi, dict):
                for k, v in loser_fi.items():
                    if v and not host_fi.get(k):
                        host_fi[k] = v
            elif loser_fi:
                host["first_instance"] = loser_fi

            # Переносим касс. блок: discovery-запись несёт реальную карточку 7kas.
            host_cass = host.get("cassation") or {}
            loser_cass = loser.get("cassation") or {}
            host_lc = (host_cass.get("last_checked_at") or "").strip()
            loser_lc = (loser_cass.get("last_checked_at") or "").strip()
            if loser_cass and (not host_cass or loser_lc >= host_lc):
                merged_cass = dict(loser_cass)
                merged_cass["discovered_via_cassation"] = False
                host["cassation"] = merged_cass

            # Стадию подтягиваем к кассации (host обычно в pre-cassation).
            if host.get("current_stage") in (
                "first_instance", "awaiting_appeal", "appeal",
                "cassation_watch", "awaiting_relink", "", None,
            ):
                host["current_stage"] = loser.get("current_stage") or "cassation"

            host["discovered_via_cassation"] = False

            loser_history = loser.get("history") or []
            if loser_history:
                host_history = host.get("history") or []
                seen_rounds = {
                    h.get("round") for h in host_history if isinstance(h, dict)
                }
                for h in loser_history:
                    r = h.get("round") if isinstance(h, dict) else None
                    if r not in seen_rounds:
                        host_history.append(h)
                        seen_rounds.add(r)
                host["history"] = host_history

            cn = (loser_cass.get("case_number") or "?")
            merge_tag = (
                f"дубль {loser.get('id', '?')} (касс. {cn}, УИД {uid}) "
                f"слит автоматически {today_iso}"
            )
            old_notes = host.get("notes") or ""
            if merge_tag not in old_notes:
                sep = " • " if old_notes else ""
                host["notes"] = (old_notes + sep + merge_tag).strip()

            to_remove.add(loser_i)
            merged += 1
            log.info(
                f"Дедуп касс./УИД: {uid} loser {loser.get('id', '?')} "
                f"слит в host {host.get('id', '?')}"
            )

    if to_remove:
        cases[:] = [c for i, c in enumerate(cases) if i not in to_remove]

    return merged


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
    "передача дела судье",                       # первый шаг после регистрации, юристу не нужен
    "передача материалов судье",                 # формулировка 1-й инст., тот же смысл
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
# вида dd.mm.yyyy или конца строки. «Заочное» (ст. 233 ГПК) — отдельный
# вид решения, формулировка карточки «Вынесено заочное решение по делу».
_FI_RESULT_FROM_EVENT_RX = re.compile(
    r"Вынесено\s+(?:заочное\s+)?решение\s+по\s+делу\.\s*(.+?)(?=\s*\d{2}\.\d{2}\.\d{4}|\s*$)",
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


def extract_fi_verdict_from_events(events: list) -> str:
    """Сырой ИТОГ 1-й инст. из истории заседаний.

    Нужен, когда статус уже «Решено» (его поднял парсер служебным движением),
    но вердикт не нашёлся ни в поле «Результат», ни в last_event. Узкий набор
    ТЕРМИНАЛЬНЫХ диспозиций, которые живут только в тексте session-события:
      • «Вынесено решение по делу. <ИТОГ>» (→ extract_result_from_event),
      • «Иск … оставлены без рассмотрения» → «оставлено без рассмотрения»,
      • «Производство по делу прекращено»  → «прекращено».
    Сознательно НЕ матчит интерлокутивные события: «оставлено без движения»
    (≠ «без рассмотрения»), «производство приостановлено» (≠ «прекращено»),
    «заседание отложено», «рассмотрение начато с начала». Если ничего не
    распознали — пустая строка (тихий пропуск, не ложный отчёт).
    """
    for ev in reversed(events or []):
        text = ev.get("text") or ""
        r = extract_result_from_event(text)
        if r:
            return r
        low = text.lower()
        if "оставлен" in low and "без рассмотрени" in low:
            return "оставлено без рассмотрения"
        if "прекращ" in low and "производств" in low:
            return "прекращено"
    return ""


# Содержимое поля «Результат» карточки, которое суд ошибочно (или нестандартно)
# заполняет текстом события вместо итога рассмотрения. Семантически такие
# значения — это «заседание перенесено/назначено», а не «дело решено».
# Если их пропустить как `new_result`, дело уезжает в секцию «Вынесенные акты»
# дайджеста, хотя никакого акта нет. См. _is_event_text_in_result_field.
_RESULT_FIELD_EVENT_RX = re.compile(
    r"^\s*(?:"
    r"заседание\s+отлож\w+"          # «Заседание отложено на ...»
    r"|заседание\s+назначен\w+"       # «Заседание назначено на ...»
    r"|рассмотрени\w*\s+начат\w*\s+с\s+начала"  # «Рассмотрение начато с начала»
    r"|назначен\w*\s+первое\s+заседани"          # «Назначено первое заседание ...»
    r")",
    re.IGNORECASE,
)


def _is_event_text_in_result_field(text: str) -> bool:
    """Распознать, что в поле «Результат» карточки лежит текст события
    («Заседание отложено/назначено», «Рассмотрение начато с начала»,
    «Назначено первое заседание»), а не итог рассмотрения.

    Используется и парсером (чтобы не выставлять new_result), и template-
    рендерером (страховка фильтра секции «Вынесенные акты»), чтобы такие
    дела не попадали в дайджест как резолютивные акты.
    """
    if not text:
        return False
    return bool(_RESULT_FIELD_EVENT_RX.match(text))


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
                log.debug(f"Невалидная дата «без движения» в событии: {text!r}")
                return None, ""
        return None, ""

    if _HEARING_MARKERS_RX.search(text_l):
        ev_date_raw = (last.get("date") or "").strip()
        m = _DATE_DDMMYYYY_RX.match(ev_date_raw)
        if m:
            try:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1))), "hearing"
            except ValueError:
                log.debug(f"Невалидная дата заседания в событии: {ev_date_raw!r}")
                return None, ""
    return None, ""


def should_skip_case(
    case_dict: dict,
    today: date,
    force_parse_days: int = 21,
) -> tuple[bool, str]:
    """Решает, можно ли пропустить парсинг карточки.

    0. config.SMART_SKIP_CASES=False (ручной прогон без галки smart_skip) —
       ничего не скипаем: полный прогон всех активных карточек.
    1. По current_stage выбирает блок first_instance / appeal.
    2. Force-parse: если last_checked_at нет или ≥ force_parse_days дней назад
       → не скипать (страховка от тихой отмены/переноса заседания).
    3. Иначе get_next_planned_date(events). Если planned >= today (включая
       сам день N) → skip. Парсим строго с N+1.
    """
    if not config.SMART_SKIP_CASES:
        return False, ""
    stage = case_dict.get("current_stage", "")
    if stage in ("first_instance", "cassation_watch"):
        block = case_dict.get("first_instance") or {}
    elif stage == "appeal":
        block = case_dict.get("appeal") or {}
    elif stage == "cassation":
        block = case_dict.get("cassation") or {}
    else:
        return False, ""

    # Материал ещё под временным М-номером: НЕ скипать, иначе промоушен
    # М→2 по карточке (см. e7a1513, блок «Промоушен материала по карточке»
    # в main_json) не сработает, пока висит будущая дата собеседования/
    # заседания — карточку перестаём грузить, а постоянный 2-XXXX виден
    # только на ней. Парсим каждый прогон, пока суд не присвоит 2-XXXX
    # (тогда case_number сменится и этот гард самоотключится). Инцидент:
    # М-1401/2026 завис под М-номером из-за собеседования 03.06.2026.
    if (block.get("case_number") or case_dict.get("id") or "").strip().startswith("М-"):
        return False, "material_pending_promotion"

    last_checked_raw = block.get("last_checked_at", "")
    last_checked: date | None = None
    if last_checked_raw:
        try:
            last_checked = date.fromisoformat(last_checked_raw)
        except ValueError:
            log.debug(
                f"  {case_dict.get('id', '?')}: невалидный last_checked_at "
                f"{last_checked_raw!r} — парсим принудительно"
            )
            last_checked = None
    if last_checked is None or (today - last_checked).days >= force_parse_days:
        return False, ""

    # Кассация: явные поля hearing_date / suspended_until в блоке (DD.MM.YYYY).
    # events карточки 7kas хранят текст в поле name (не text), поэтому
    # get_next_planned_date по ним не сработает — читаем явные поля.
    # «>=», как и у 1-й инст./апелляции: день N скипаем, парсим с N+1
    # (решение юриста 08.07.2026). Акт «единоличного рассмотрения»,
    # опубликованный в сам день N, подхватится на следующем прогоне.
    if stage == "cassation":
        hd_raw = (block.get("hearing_date") or "").strip()
        m_hd = _DATE_DDMMYYYY_RX.match(hd_raw)
        if m_hd:
            try:
                hd = date(int(m_hd.group(3)), int(m_hd.group(2)), int(m_hd.group(1)))
                if hd >= today:
                    return True, f"future_hearing({hd.strftime('%d.%m.%Y')})"
            except ValueError:
                log.debug(
                    f"  {case_dict.get('id', '?')}: невалидная hearing_date "
                    f"кассации {hd_raw!r}"
                )
        su_raw = (block.get("suspended_until") or "").strip()
        m_su = _DATE_DDMMYYYY_RX.match(su_raw)
        if m_su:
            try:
                su = date(int(m_su.group(3)), int(m_su.group(2)), int(m_su.group(1)))
                if su > today:
                    return True, f"suspended_until({su.strftime('%d.%m.%Y')})"
            except ValueError:
                log.debug(
                    f"  {case_dict.get('id', '?')}: невалидная suspended_until "
                    f"кассации {su_raw!r}"
                )

    planned, kind = get_next_planned_date(block.get("events") or [])
    if planned and planned >= today:
        ymd = planned.strftime("%d.%m.%Y")
        if kind == "hearing":
            return True, f"future_hearing({ymd})"
        return True, f"suspended_until({ymd})"

    # Дело «без движения» без явной будущей даты исправления — парсим раз
    # в 7 дней. Суды 1-й инст. ХМАО часто не указывают срок устранения
    # (или он уже прошёл), а ежедневный парс бесполезен: новое определение
    # появится только после подачи исправлений юристом.
    events = block.get("events") or []
    if events:
        last_ev_text = ((events[-1] or {}).get("text") or "").lower()
        if _SUSPENDED_RX.search(last_ev_text):
            days_since = (today - last_checked).days
            if days_since < 7:
                return True, f"suspended_weekly({days_since}d/7d)"
    return False, ""


def skip_reason_ru(reason: str) -> str:
    """Человекочитаемая причина skip для лога.

    Внутренние коды `should_skip_case` («future_hearing(12.08.2026)») остаются
    как были — на них завязана логика подсчёта; переводим только при печати.
    Неизвестный код возвращается как есть.
    """
    m = re.match(r"future_hearing\((.+)\)$", reason)
    if m:
        return f"заседание {m.group(1)} ещё впереди"
    m = re.match(r"suspended_until\((.+)\)$", reason)
    if m:
        return f"без движения до {m.group(1)}"
    m = re.match(r"suspended_weekly\((\d+)d/(\d+)d\)$", reason)
    if m:
        return f"без движения без срока, парсим раз в {m.group(2)} дн. (прошло {m.group(1)})"
    if reason == "material_pending_promotion":
        return "материал под М-номером, ждём промоушен"
    return reason


def fi_resolution_contradicted_by_future_hearing(fi: dict, today: date) -> bool:
    """True, если блок 1-й инст. помечен «Решено», но последнее session-событие
    — заседание в БУДУЩЕМ, а «Вынесено решение по делу» в движении нет.

    Сценарии: «Рассмотрение дела начато с начала» (привлечение соответчика и
    т.п.) либо преждевременный/ошибочный «Результат» в выдаче суда. Дело
    фактически НЕ рассмотрено — назначено новое заседание. Реально решённые
    дела (есть событие «Вынесено решение по делу», даже если позже назначено
    заседание по судебным расходам) под правило НЕ попадают.
    Инцидент: 2-233/2026 — «Иск удовлетворён» из выдачи + заседание 15.07.2026.
    """
    if (fi.get("status") or "").strip() != "Решено":
        return False

    def _as_date(x):
        # parse_date возвращает datetime; today — date. Приводим к date.
        return x.date() if isinstance(x, datetime) else x

    hd = _as_date(parse_date((fi.get("hearing_date") or "").strip()))
    if not hd or hd <= today:
        return False
    events = fi.get("events") or []
    has_future_session = any(
        _as_date(parse_date(ev.get("date") or "")) == hd
        and _SESSION_START_RX.search(ev.get("text") or "")
        for ev in events
    )
    if not has_future_session:
        return False
    has_decision = any(
        re.search(r"вынесено\s+(?:заочное\s+)?решение\s+по\s+делу",
                  (ev.get("text") or "").lower())
        for ev in events
    )
    return not has_decision


def repair_spurious_fi_resolutions(cases: list[dict], today: date) -> int:
    """Чинит дела 1-й инст. с ложным «Решено» при назначенном будущем заседании
    (см. fi_resolution_contradicted_by_future_hearing). Идемпотентно, как
    migrate_stages: на повторных прогонах ничего не меняет. hearing_date НЕ
    трогаем — фронт покажет предстоящее заседание."""
    n = 0
    for case in cases:
        if case.get("current_stage") not in ("first_instance", "cassation_watch"):
            continue
        fi = case.get("first_instance") or {}
        if fi_resolution_contradicted_by_future_hearing(fi, today):
            fi["status"] = "В производстве"
            fi["result"] = ""
            fi["result_date"] = ""
            fi["resolved_emitted"] = False
            n += 1
            log.info(
                f"  {case.get('id', '?')}: снят ложный «Решено» "
                f"(назначено заседание {fi.get('hearing_date')})"
            )
    return n


def bank_side_outcome_fi(role: str, verdict_label: str) -> str:
    """Знак исхода для банка в 1-й инстанции — по роли + нормализованному ярлыку.

    Возвращает одну из: «в пользу банка», «против банка», «частично в пользу
    банка», «частично против банка», или пустую строку (если данных
    недостаточно ИЛИ банк = Третье лицо — роль показывается отдельным
    хвостом «банк — Третье лицо», незачем дублировать в «Для банка»).

    Для процессуальных завершений без решения по существу (прекращено,
    без рассмотрения, возвращено) знак определяется по роли: истец теряет
    возможность добиться удовлетворения → «против банка», к ответчику
    требования не рассмотрены → «в пользу банка». Точная причина
    (мировое соглашение, отказ от иска и т.п.) остаётся в last_event —
    юрист увидит её в строке события.
    """
    role_l = (role or "").lower()
    if "третье" in role_l:
        # Пусто → промпт (правило 6043) опускает блок «Для банка», а
        # роль остаётся в хвосте строки 2 («…, банк — Третье лицо»).
        return ""
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
    """«в пользу банка» / «против банка» / «» (пустая строка при нехватке
    данных или для роли «Третье лицо» — чтобы downstream не дублировал «банк —
    третье лицо», который и так есть в хвосте строки 2 по правилу промпта)."""
    role_l = (role or "").lower()
    if "третье" in role_l:
        return ""
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


def _parse_iso_date(s: str) -> datetime | None:
    """Распарсить ISO-дату (`YYYY-MM-DD` или полный ISO-таймстамп) — формат, в
    котором хранится `archived_at`. Отдельно от parse_date, который понимает
    только судебный формат `ДД.ММ.ГГГГ`."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _infer_archived_at(case: dict) -> str:
    """Вывести дату архивации дела из самой поздней даты его стадий —
    для бэкфилла поля `archived_at` у дел, попавших в архив до появления
    штампа. Порядок проб: кассация → апелляция → 1-я инстанция. Если ни одна
    дата не распарсилась — сегодня (консервативно: подержим в горячем ещё год,
    а не потеряем в холодном раньше времени)."""
    cs = case.get("cassation") or {}
    ap = case.get("appeal") or {}
    fi = case.get("first_instance") or {}
    candidates = [
        cs.get("act_date"), cs.get("decision_date"),
        ap.get("hearing_date"),
        fi.get("act_date"), fi.get("hearing_date"),
    ]
    for raw in candidates:
        d = parse_date(raw or "")
        if d:
            return d.date().isoformat()
    return date.today().isoformat()


def _has_real_fi(case: dict) -> bool:
    """Карточка 1-й инст. заполнена реальными данными парсера (не stub-блок,
    автозаполняемый при создании сироты-апелляции по `appeal_fi_numbers`).

    Тот же 4-полевой критерий, что использует `dedupe_orphan_by_base_number`
    для отличия orphan-stub от хозяина — оба места должны идти от одного
    предиката, иначе матч и мердж рассинхронятся.
    """
    fi = case.get("first_instance") or {}
    return bool(
        fi.get("events") or fi.get("act_text")
        or fi.get("link") or fi.get("act_date")
    )
