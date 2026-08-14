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
from court_monitor.textutil import (
    parse_date, _bare_case_number,
    classify_appellant_role, appellant_role_words, _norm_party_tokens,
)

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
    r"|единоличн\w*\s+рассмотрени"
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


# ── Процессуальное завершение 1-й инстанции ─────────────────────────────────
# Возврат иска, отказ в принятии, передача по подсудности. Все три — НЕ решения
# по существу, и в дайджесте живут строкой в 3.2 «Изменения», а не в 3.5
# «Вынесенные решения» (решение юриста 29.07.2026 после инцидента 9-336/2026:
# возврат печатался ДВАЖДЫ — сырым текстом события в 3.2 и как «Итог:
# возвращено» в 3.5).
FI_TERMINATION_RETURNED = "returned"
FI_TERMINATION_REFUSAL = "refusal"
FI_TERMINATION_TRANSFER = "transfer"
# Присоединение к другому делу (ст. 151 ГПК, соединение в одно производство).
# Как и transfer — не исход по существу: дело продолжается под другим номером,
# «победы/поражения» банка тут нет. Отличие от остальных трёх видов: карточка
# суда статус НЕ флипает (остаётся «В производстве»), поэтому гейт статуса в
# fi_termination_details для merged ослаблен. Номер дела-приёмника суд не
# публикует ни в «Результате», ни в движении — его подбирает
# resolve_bank_merged_targets (linking.py) и всегда помечает предположением.
FI_TERMINATION_MERGED = "merged"

# Присоединение — единственный вид завершения, который читается ТОЛЬКО из поля
# «Результат». Оно отражает текущее состояние карточки, а история движения —
# нет: у дела, где объединение потом отменили, событие остаётся в списке
# навсегда, и скан истории (ветка events в classify_fi_termination) навесил бы
# ложный merged. Гейт статуса для merged ослаблен, так что защититься статусом
# «В производстве», как у остальных видов, здесь нельзя.
_FI_MERGED_RX = re.compile(
    r"присоединен\w*\s+к\s+другому\s+делу"
    r"|(?:объединен|соединен)\w*\s+в\s+одно\s+производств",
    re.IGNORECASE,
)

# Поле «Результат» карточки — НЕ движение дела: sudrf пишет туда шапку с
# причиной без пробела («Заявление ВОЗВРАЩЕНО заявителюДЕЛО НЕ ПОДСУДНО
# ДАННОМУ СУДУ»), и _TERMINAL_FI_EVENT_RX (заточен под тексты движения,
# «возвращени\S*\s+иск») такую строку НЕ матчит. Отсюда отдельный набор.
# Возврат якорим существительным вплотную к «возвращ» — иначе «госпошлина
# возвращена [заявителю]» в резолютивке по существу читалась бы как
# процессуальный возврат: дело ушло бы в 3.2 с ложным «🔚 иск возвращён»,
# а resolved_emitted закрыл бы канал 3.5 для настоящего итога (ревью
# 29.07.2026: безъякорная альтернатива «возвращ… заявител» этот гард
# обходила — убрана; все реальные шаблоны ГАС начинаются существительным).
_FI_RESULT_TERMINATION_RX = (
    (FI_TERMINATION_TRANSFER,
     re.compile(r"передан\w*\s+по\s+(?:подсудност|подведомствен)", re.I)),
    (FI_TERMINATION_REFUSAL,
     re.compile(r"отказан\w*\s+в\s+принят", re.I)),
    (FI_TERMINATION_RETURNED,
     re.compile(r"(?:заявлени\w*|иск\w*|материал\w*)\s+возвращ", re.I)),
    (FI_TERMINATION_MERGED, _FI_MERGED_RX),
)
# Тексты движения дела. Порядок — от специфичного к общему (передача и отказ
# в принятии проверяются до возврата: «отказано в принятии иска» содержит
# и «иск», но возвратом не является).
_FI_EVENT_TERMINATION_RX = (
    (FI_TERMINATION_TRANSFER,
     re.compile(r"передан\w*\s+по\s+подсудност"
                r"|передан\w*\s+на\s+рассмотрение\s+другого\s+суда", re.I)),
    (FI_TERMINATION_REFUSAL,
     re.compile(r"отказан\w*\s+в\s+принят", re.I)),
    (FI_TERMINATION_RETURNED,
     re.compile(r"возвращени\w*\s+иск|возвращени\w*\s+заявлени"
                r"|материал\w*\s+возвращ", re.I)),
)
# Шапка возврата в поле «Результат» — срезаем, остаток и есть причина.
# ⚠️ Хвост шапки («заявителю») ловим ТОЛЬКО строчными буквами и БЕЗ re.I:
# sudrf клеит шапку с причиной без пробела («…заявителюДЕЛО НЕ ПОДСУДНО…»),
# а `\w*` под IGNORECASE сожрал бы и заглавное начало причины, оставив
# огрызок («указаний судьи» вместо «невыполнение указаний судьи»).
_FI_RESULT_RETURN_HEAD_RX = re.compile(
    r"^\s*(?i:(?:заявлени|иск|материал)\w*\s+возвращ\w*)"
    r"(?:\s*(?i:заявител)[а-яё]*)?"
    r"|^\s*(?i:отказан\w*\s+в\s+принят\w*)(?:\s+[а-яё]+)?"
)
# Куда ушло дело при передаче по подсудности (kind=transfer). Две формы
# текста события: с предлогом («…передано в Няганский городской суд») и
# именем суда отдельным сегментом после точки («…на рассмотрение другого
# суда. Няганский городской суд»).
_FI_TRANSFER_TARGET_RX = re.compile(
    r"\bв\s+([А-ЯЁ][^.;]{3,60}?суд\w*)"
    r"|другого\s+суда\W+\s*([А-ЯЁ][^.;]{3,60}?суд\w*)"
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


def _extract_termination_reason_from_result(result: str) -> str:
    """Причина завершения из поля «Результат» карточки.

    `_extract_return_reason` режет текст по точкам — в поле «Результат» точек
    нет (sudrf клеит шапку и причину без разделителя), поэтому там он отдаёт
    либо всю строку целиком («заявление возвращено заявителюдело не подсудно
    данному суду»), либо пустоту. Здесь срезаем шапку — остаток и есть
    причина: «дело не подсудно данному суду», «невыполнение указаний судьи».
    """
    text = (result or "").strip()
    if not text:
        return ""
    cleaned = _FI_RESULT_RETURN_HEAD_RX.sub("", text, count=1).strip()
    # Хвостовая дата и осиротевшие знаки после срезки шапки.
    cleaned = re.sub(r"\s*\d{2}\.\d{2}\.\d{4}\s*$", "", cleaned)
    cleaned = cleaned.strip(" ,.;:-—")
    if not cleaned or cleaned.lower() == text.lower():
        return ""
    return cleaned.lower()


def _last_reason_segment(text: str) -> str:
    """Последний осмысленный сегмент текста события — фолбэк причины.

    Карточка кладёт причину В КОНЕЦ строки движения, перед датой публикации:
    «Отказано в принятии заявления. 09:30. НЕ ПОДЛЕЖИТ РАССМОТРЕНИЮ В ПОРЯДКЕ
    ГПК. 01.07.2026» → «не подлежит рассмотрению в порядке гпк». Первый
    сегмент (как в `_fi_return_reason_for_render`) для отказа/возврата — это
    сама формулировка действия, она дублировала бы ярлык строки.
    Служебные сегменты (голые время и дата) пропускаем.
    """
    # Хвостовую дату срезаем ДО разбиения: её собственные точки иначе
    # раскрошили бы «01.07.2026» на сегменты и последним оказался бы «2026».
    text = re.sub(r"\s*\d{2}\.\d{2}\.\d{4}\s*$", "", (text or "").strip())
    for seg in reversed([s.strip() for s in re.split(r"[.;]", text)]):
        if not seg or re.fullmatch(r"[\d:\s]+", seg):
            continue
        return seg.lower()
    return ""


def _match_termination_event(text: str) -> str:
    """Вид завершения по тексту события движения, «» если не завершение.

    События про ВСТРЕЧНЫЙ иск («Отказано в принятии встречного искового
    заявления», «Возвращение встречного иска») — промежуточные определения
    живого дела, а не его завершение: основной иск рассматривается дальше.
    Без этого гарда дело получало бы ложное «🔚 отказано в принятии иска»
    и навсегда закрытый канал 3.5 (ревью 29.07.2026).
    """
    if not text or "встречн" in text.lower():
        return ""
    return next(
        (k for k, rx in _FI_EVENT_TERMINATION_RX if rx.search(text)), ""
    )


def classify_fi_termination(
    result: str, last_event: str, events: list | None = None
) -> tuple[str, str, str] | None:
    """Вид процессуального завершения дела 1-й инстанции.

    Возвращает `(kind, reason, event_text)` либо None, если завершения нет:
      kind       — FI_TERMINATION_RETURNED / _REFUSAL / _TRANSFER;
      reason     — короткая причина маленькими буквами («дело не подсудно
                   данному суду»), «» если не распозналась;
      event_text — сырой текст события движения (он же фолбэк причины на
                   рендере старых контекстов), «» если завершение видно
                   только по полю «Результат».

    ВИД: непустой «Результат» — единственный арбитр, когда он есть: суд
    заполнил исход, и если это НЕ завершение (решение по существу,
    «Иск удовлетворён…») — возвращаем None, историю движения НЕ смотрим.
    Иначе старый возврат/отказ в глубине events (отменённое определение,
    возврат на стадии принятия прошлого круга) перехватывал бы настоящий
    итог: эмитился бы ложный «🔚 иск возвращён», а канал 3.5 закрывался бы
    навсегда (ревью 29.07.2026). Фолбэк по last_event/events — только при
    пустом «Результате» (фантомная дата, статус карточки «Возвращено»).

    ⚠️ Функция — чистый классификатор; гейт по СТАТУСУ карточки (не звать
    для живых дел «В производстве») — на вызывающей стороне,
    `fi_termination_details`.

    ПРИЧИНУ, наоборот, сперва берём из ТЕКСТА СОБЫТИЯ — там она отделена
    точкой и вынимается чисто; поле «Результат» идёт вторым номером через
    `_extract_termination_reason_from_result`. Для передачи по подсудности
    причина = «в … суд» из текста события, иначе пусто: строка «➡️ дело
    передано по подсудности» уже всё сказала, дублировать нечего.
    """
    result = (result or "").strip()
    last_event = (last_event or "").strip()

    result_kind = ""
    if result:
        for k, rx in _FI_RESULT_TERMINATION_RX:
            if rx.search(result):
                result_kind = k
                break
        if not result_kind:
            # Исход есть, и он не завершение — это решение по существу.
            return None

    # Текст события движения: сперва last_event, иначе свежайшее подходящее
    # событие из истории (обратный обход — события идут по возрастанию даты).
    event_text = ""
    event_kind = _match_termination_event(last_event)
    if event_kind:
        event_text = last_event
    else:
        for ev in reversed(events or []):
            text = (ev.get("text") or "") if isinstance(ev, dict) else ""
            matched = _match_termination_event(text)
            if matched:
                event_text, event_kind = text, matched
                break

    # Событие другого вида, чем «Результат» (старый возврат в истории при
    # передаче по подсудности и т.п.) — источником причины не считаем.
    if result_kind and event_kind and event_kind != result_kind:
        event_text = ""

    kind = result_kind or event_kind
    if not kind:
        return None

    if kind == FI_TERMINATION_TRANSFER:
        m = _FI_TRANSFER_TARGET_RX.search(event_text)
        target = (m.group(1) or m.group(2) or "").strip() if m else ""
        reason = f"в {target}".lower() if target else ""
    else:
        reason = (
            _extract_return_reason(event_text)
            or _extract_termination_reason_from_result(result)
            or _last_reason_segment(event_text)
        )
    return kind, reason, event_text


# «Иск не принят к производству» — три вида завершения, при которых тяжбы не
# было вовсе: суд вернул заявление, отказал в его принятии или передал дело в
# другой суд. Отдельный класс нужен КАНАЛАМ ПРИЁМА (импортёр дампов, автопоиск
# 1-й инст.): заводить такое дело в мониторинг незачем — предмета нет, а дело
# 60 дней занимает активную картотеку, каждый прогон качает карточку и один раз
# объявляется «новым иском» (разбор юриста 14.08.2026).
#
# MERGED в класс НЕ входит: присоединённое дело живёт дальше под номером
# приёмника — это не «не принято». Прекращение и «оставлено без рассмотрения»
# сюда тоже не относятся (решение юриста): производство было, частная жалоба
# возможна, и дело оживает под ТЕМ ЖЕ номером.
FI_NOT_ACCEPTED_KINDS = (
    FI_TERMINATION_RETURNED, FI_TERMINATION_REFUSAL, FI_TERMINATION_TRANSFER,
)

# Подписи для отчёта оператору и лога прогона — без эмодзи (в дайджесте у тех же
# видов своя вёрстка, _FI_TERMINATION_LABELS в digest/template.py). Карта одна на
# оба канала приёма: две копии разъехались бы молча.
FI_NOT_ACCEPTED_RU = {
    FI_TERMINATION_RETURNED: "заявление возвращено",
    FI_TERMINATION_REFUSAL: "отказано в принятии",
    FI_TERMINATION_TRANSFER: "передано по подсудности",
}


def fi_not_accepted_kind(result: str) -> str:
    """Вид «иск не принят к производству» по полю «Результат»; "" — дело берём.

    ⚠️ Классификатор зовём ТОЛЬКО по «Результату», с пустыми `last_event`/
    `events`: при пустом результате он уходит в историю движения, а там у
    живого дела лежит отменённый возврат прошлого круга — канал приёма молча
    отверг бы нормальное дело (тот же урок, что у `bank_writ_expected`,
    14.08.2026). Пустой «Результат» = решения ещё нет = дело берём.
    """
    kind = classify_fi_termination((result or "").strip(), "", [])
    if kind and kind[0] in FI_NOT_ACCEPTED_KINDS:
        return kind[0]
    return ""


def fi_is_merged(fi: dict) -> bool:
    """Дело присоединено к другому (ст. 151 ГПК) по ТЕКУЩЕМУ состоянию карточки.

    Читаем поле «Результат», а не флаг: флаг ставит эмит, а предикат нужен и до
    него (и он же — арбитр при отмене объединения, см. repair_cancelled_merges).
    """
    return bool(_FI_MERGED_RX.search((fi or {}).get("result") or ""))


def merged_target_reason(fi: dict) -> str:
    """Хвост строки дайджеста с номером дела-приёмника — либо пусто.

    Номер всегда сопровождается пометкой о происхождении: суд его не публикует,
    и подбор (resolve_bank_merged_targets) — предположение по совпадению сторон.
    Юрист должен видеть разницу между догадкой системы и вписанным вручную.
    """
    fi = fi or {}
    # В записи хранится полный id («2-191/2026 (2-979/2025;)») — по нему фронт
    # находит дело; юристу показываем голый номер, как везде в интерфейсе.
    num = _bare_case_number(fi.get("merged_into") or "")
    if not num:
        return ""
    if fi.get("merged_into_guess"):
        return f"№ {num} (предположительно)"
    return f"№ {num}"


def repair_cancelled_merges(cases: list[dict]) -> int:
    """Снять следы присоединения с дел, где объединение отменили.

    Зеркало repair_spurious_fi_resolutions для merged: та функция гейтится по
    статусу «Решено»/«Возвращено» (у присоединённого дела статус остаётся
    «В производстве»), поэтому merged она не покроет никогда. Без ремонта флаги
    залипают: дело раз в неделю опрашивается, через 30 дней уходит в архив, а
    resolved_emitted навсегда закрывает канал 3.5 — настоящее решение по
    существу в дайджест не попало бы.

    Идемпотентно: на повторных прогонах ничего не меняет.
    """
    n = 0
    for case in cases:
        fi = case.get("first_instance") or {}
        if not fi.get("merged"):
            continue
        if fi_is_merged(fi):
            continue
        for key in ("merged", "merged_at", "merged_into",
                    "merged_into_domain", "merged_into_guess"):
            fi.pop(key, None)
        fi["termination_emitted"] = False
        fi["resolved_emitted"] = False
        # decision_date снимаем обязательно, а не для симметрии: эмит завершения
        # заморозил в ней дату определения об объединении, а будущий fi_resolved
        # ставит её через setdefault — с залипшим ключом настоящая дата решения
        # не записалась бы никогда, и classify_writ_kind считал бы тип листа от
        # чужого якоря (обеспечительный молча стал бы «на исполнение»).
        fi.pop("decision_date", None)
        n += 1
        log.info(
            f"  {case.get('id', '?')}: объединение отменено — "
            f"дело возвращено в обычный ритм"
        )
    return n


def fi_termination_details(fi: dict, bank_role: str) -> dict | None:
    """`details` события `fi_returned` для дайджеста — либо None.

    None означает одно из четырёх: (а) завершения в карточке нет; (б) о нём
    уже отчитались (`fi["termination_emitted"]`); (в) канал 3.5 по делу уже
    отработал в прошлых прогонах (`fi["resolved_emitted"]`) — это защита от
    ретро-паводка: на первом прогоне после деплоя нельзя объявить возвратами
    все дела, чей исход юрист уже получил строкой «Вынесено решение»;
    (г) статус карточки НЕ терминальный. Гейт по статусу обязателен:
    у ЖИВОГО дела («В производстве») в истории движения может лежать
    отменённый частной жалобой возврат или возврат прошлого круга —
    классификатор нашёл бы его, дайджест получил бы ложное «🔚 иск
    возвращён», а `resolved_emitted` навсегда закрыл бы 3.5 для будущего
    настоящего решения (ревью 29.07.2026). Терминальные статусы: «Решено»
    (возврат с заполненным «Результатом» — resolved_keywords карточки) и
    «Возвращено» (терминальное событие при пустом «Результате»).

    Флаги НЕ мутирует — их ставит вызывающая сторона строго при успешном
    эмите (FI-цикл `main_json`, фаза 4b), по образцу `fi["resolved_emitted"]`
    в блоке захвата текста акта.
    """
    fi = fi or {}
    if fi.get("termination_emitted") or fi.get("resolved_emitted"):
        return None
    status = (fi.get("status") or "").strip()
    result = (fi.get("result") or "").strip()
    # Присоединение к другому делу — исключение из гейта (г): карточка держит
    # статус «В производстве» (resolved_keywords его не флипают), и при общем
    # гейте завершение не эмитилось бы никогда. Подмена гейта безопасна: merged
    # читается ТОЛЬКО из поля «Результат» (см. _FI_MERGED_RX), то есть отражает
    # текущее состояние карточки, а не старое определение из истории движения.
    if not (result and _FI_MERGED_RX.search(result)):
        if status not in ("Решено", "Возвращено"):
            return None
        if status == "Решено" and not result:
            # «Решено» без результата — служебный статус (экспедиция/архив);
            # завершение из одной истории движения тут не объявляем, чтобы не
            # реагировать на старые определения (см. гейт (г) выше).
            return None
    found = classify_fi_termination(
        result, fi.get("last_event", ""), fi.get("events") or []
    )
    if not found:
        return None
    kind, reason, event_text = found
    if kind == FI_TERMINATION_MERGED:
        # Номер дела-приёмника суд не публикует — его подбирает
        # resolve_bank_merged_targets. К моменту эмита (фаза 4b) он обычно ещё
        # не известен: подбор требует всего списка дел и идёт после FI-цикла,
        # он же и допишет причину в change["details"]. Здесь — для случая,
        # когда номер уже стоит в записи (подобран прошлым прогоном до того,
        # как эмит стал возможен, или вписан юристом вручную).
        reason = merged_target_reason(fi)
    # Знак исхода для банка. Передача по подсудности — НЕ исход: дело живёт
    # дальше в другом суде, «в пользу банка» там было бы враньём.
    # classify_verdict_fi здесь не годится: на «Передано по подсудности» она
    # возвращает сырую строку — это и есть источник жалобы юриста
    # «Вынесено решение. Итог: Передано по подсудности».
    verdict_for_bank = {
        FI_TERMINATION_RETURNED: "возвращено",
        FI_TERMINATION_REFUSAL: "отказано",
    }.get(kind, "")
    bank_outcome = (
        bank_side_outcome_fi(bank_role, verdict_for_bank)
        if verdict_for_bank else ""
    )
    # Дата события-завершения — для строки дайджеста «➡️ дело передано по
    # подсудности (29.06.2026)»: суд заполняет «Результат» с лагом в недели
    # (2-822/2026: передача 29.06, объявлена 07.08), и без даты юрист не
    # понимает, когда это случилось. Ищем дату события с этим текстом;
    # фолбэк — первая дата внутри самого текста (sudrf клеит её в хвост).
    termination_date = ""
    if event_text:
        termination_date = next(
            (ev.get("date") or "" for ev in (fi.get("events") or [])
             if (ev.get("text") or "") == event_text and ev.get("date")),
            "",
        )
        if not termination_date:
            m_dt = re.search(r'\d{2}\.\d{2}\.\d{4}', event_text)
            if m_dt:
                termination_date = m_dt.group(0)
    return {
        "termination_kind": kind,
        "return_reason": reason,
        "event_text": event_text,
        "bank_outcome": bank_outcome,
        "termination_date": termination_date,
    }


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


# ── Трек «Иски банка» (банк — истец, data/cases_bank.json) ───────────────────
# Лёгкий трек: дела заводятся импортёром реестра (import_bank_registry.py),
# на прогон подмешиваются в общий список и проходят обычный FI-цикл; отличия —
# недельный опрос после решения (should_skip_case), свои архивные окна
# (is_case_archived) и отдельная секция дайджеста. Подана апел. жалоба →
# дело «переезжает» в основной cases.json (bank_case_left_track) и дальше
# живёт стандартным треком, как нынешние истцовые дела «с апелляции».

def is_bank_plaintiff_track(case: dict) -> bool:
    """True для дел лёгкого трека исков банка (track="plaintiff_light")."""
    return (case.get("track") or "").strip() == "plaintiff_light"


def bank_case_left_track(case: dict) -> bool:
    """True, если дело покинуло лёгкий трек → переезд в основной cases.json.

    Признаки: подана апел. жалоба (видна в карточке 1-й инст. — «флаг без
    даты» тоже считается, как в is_case_archived) ИЛИ стадия уже не
    first_instance (link_cases/migrate_stages увели дело выше).

    ⚠️ Гейт особого порядка закрывает ТОЛЬКО ветку признаков жалобы. Ранний
    выход на всю функцию заморозил бы в лёгком треке дело, которое link_cases
    законно перевёл в стадию `appeal`: оно перестало бы и архивироваться, и
    парситься, а апелляционный блок писался бы в cases_bank.json — файл,
    который основной фронт не грузит. Дело исчезло бы бесшумно.
    """
    if not is_bank_plaintiff_track(case):
        return False
    if (case.get("current_stage") or "first_instance") != "first_instance":
        return True
    fi = case.get("first_instance") or {}
    if default_cancellation_blocks_appeal(fi):
        return False
    return bool(
        fi.get("appeal_filed") or fi.get("appeal_filed_date")
        or fi.get("sent_to_appeal") or fi.get("sent_to_appeal_date")
    )


# Детект признаков заочного производства по текстам событий карточки.
# Матчим по ev["text"] (склейка ячеек включает и name, и result_event) после
# lower() + ё→е: суды пишут «невручённой» и «неврученной» вперемешку.
_ANY_DECISION_RX = re.compile(r"вынесено\s+(заочное\s+)?решение")
_MOTIVATED_DECISION_RX = re.compile(r"изготовлено\s+мотивированное\s+решение")
_DEFAULT_COPY_RX = re.compile(r"копия\s+заочного\s+решения")
_COPY_SERVED_RX = re.compile(r"вручена")
_COPY_RETURNED_RX = re.compile(r"возвратилась\s+невручен")


def fi_decision_date_from_events(events) -> str:
    """Дата ПОСЛЕДНЕГО события «Вынесено (заочное) решение» — «ДД.ММ.ГГГГ»|"".

    Якорь classify_writ_kind до того, как `fi.decision_date` заморожена эмитом
    fi_resolved: разовому сборщику исков банка запись взять неоткуда, а
    карточка несёт событие решения с первого же парса. Последнее решение
    побеждает — после отмены заочного (ст. 241 ГПК) и нового рассмотрения
    якорем должно быть решение текущего круга.
    """
    found = ""
    for ev in events or []:
        text = (ev.get("text") or "").lower().replace("ё", "е")
        if text and _ANY_DECISION_RX.search(text) and ev.get("date"):
            found = ev["date"]
    return found


def bank_default_judgment_info(fi: dict) -> dict:
    """Признаки заочного решения и мотивировки из событий 1-й инстанции.

    Возвращает ровно те ключи, что штампуются в запись (split_bank_track):
    - default_judgment: bool — «Вынесено заочное решение по делу» (ст. 233 ГПК);
    - motivirovka_date: "ДД.ММ.ГГГГ"|"" — дата события «Изготовлено
      мотивированное решение в окончательной форме» (последнего, если их
      несколько); имя поля зеркалит details["motivirovka_date"] события
      fi_motivirovka_emitted;
    - default_copy_served_date: "ДД.ММ.ГГГГ"|"" — «Копия заочного решения
      ответчику (истцу) вручена»;
    - default_copy_returned: bool — «Копия заочного решения возвратилась
      невручённой»;
    - default_copy_returned_date: "ДД.ММ.ГГГГ"|"" — дата события о возврате
      копии (для события дайджеста fi_default_copy_returned, 09.08.2026).

    Событие «Отправка копии заочного решения» сознательно НЕ используется:
    формула ВС (Обзор №2 (2015), в. 14) считает трёхдневный срок направления
    от принятия решения, а не от фактической отправки.

    events пуст (лёгкая запись без склейки, тест) → фолбэк на уже
    проштампованные поля fi — самосогласованность при чтении архивных
    записей вне пайплайна.
    """
    events = fi.get("events") or []
    if not events:
        return {
            "default_judgment": bool(fi.get("default_judgment")),
            "motivirovka_date": fi.get("motivirovka_date") or "",
            "default_copy_served_date": fi.get("default_copy_served_date") or "",
            "default_copy_returned": bool(fi.get("default_copy_returned")),
            "default_copy_returned_date":
                fi.get("default_copy_returned_date") or "",
        }
    info = {
        "default_judgment": False,
        "motivirovka_date": "",
        "default_copy_served_date": "",
        "default_copy_returned": False,
        "default_copy_returned_date": "",
    }
    for ev in events:
        text = (ev.get("text") or "").lower().replace("ё", "е")
        if not text:
            continue
        m = _ANY_DECISION_RX.search(text)
        if m:
            # Тип определяет ПОСЛЕДНЕЕ решение-событие: после отмены заочного
            # (ст. 241 ГПК) и нового рассмотрения обычное решение снимает
            # заочность — событие первого круга остаётся в истории навсегда.
            info["default_judgment"] = bool(m.group(1))
            # Границей круга обнуляем и производные признаки: после «отмена →
            # снова заочное» вручение копии первого круга иначе осталось бы
            # последним известным, и bank_legal_force_est дал бы вступление в
            # силу раньше реального (завышенный «⏳ ждёт ИЛ» и преждевременный
            # архив живого дела).
            info["motivirovka_date"] = ""
            info["default_copy_served_date"] = ""
            info["default_copy_returned"] = False
            info["default_copy_returned_date"] = ""
        if _MOTIVATED_DECISION_RX.search(text) and ev.get("date"):
            info["motivirovka_date"] = ev["date"]  # последнее событие побеждает
        if _DEFAULT_COPY_RX.search(text):
            if _COPY_SERVED_RX.search(text) and ev.get("date"):
                info["default_copy_served_date"] = ev["date"]
            if _COPY_RETURNED_RX.search(text):
                info["default_copy_returned"] = True
                info["default_copy_returned_date"] = ev.get("date") or ""
    return info


# ── Особый порядок отмены заочного решения (ст. 237-243 ГПК) ──────────────
# Ответчик подаёт заявление об отмене в ТОТ ЖЕ суд 1-й инстанции (7 дн со дня
# вручения копии, ст. 237 ч. 1); суд рассматривает его в заседании за 10 дн
# (ст. 240) и выносит определение об ОТКАЗЕ либо об ОТМЕНЕ решения с
# возобновлением рассмотрения по существу (ст. 241, 243). Это НЕ апелляция:
# апелляционный ход у ответчика открывается только со дня определения об
# отказе (ст. 237 ч. 2) — до этого зарегистрированная судом апел. жалоба не
# должна уводить иск банка из лёгкого трека (кейс 2-616/2026: жалоба 23.07,
# заявление об отмене 28.07, заседание по нему 10.08).
#
# ⚠️ Матчим по ev["text"], а не по колонке ev["name"]: в основной картотеке
# 43% событий 1-й инстанции идут без разобранных колонок (legacy-склейки), а
# дело 2-616/2026 живёт именно там. result_event используем как уточнение
# исхода, когда колонка есть.
_DEFAULT_CANCEL_FILED_RX = re.compile(
    r"регистрац\w*\s+заявлени\w*\s+об\s+отмене\s+заочн")
_DEFAULT_CANCEL_HEARING_RX = re.compile(
    r"рассмотрени\w*\s+заявлени\w*\s+об\s+отмене\s+заочн")
# Исходы — БЕЛЫЙ СПИСОК. Правило «любой непустой результат = отказ» негодно:
# в колонке «Результат события» того же заседания реально встречаются
# «Заседание отложено» (124 раза по корпусу), «Объявлен перерыв» (25),
# «Производство приостановлено» (20) — заявление тогда ещё не рассмотрено.
_DEFAULT_CANCEL_GRANTED_RX = re.compile(r"заочн\w*\s+решени\w*\s+отменен")
_DEFAULT_CANCEL_REFUSED_RX = re.compile(
    r"отказан\w*\s+в\s+удовлетворении\s+заявлени"
    r"|в\s+удовлетворении\s+заявлени\w*[^.]{0,60}отказан"
    r"|отказан\w*\s+в\s+отмене\s+заочн")


def default_cancellation_state(fi: dict, today: date | None = None) -> dict:
    """Состояние особого порядка отмены заочного решения по событиям карточки.

    Возвращает {"filed_date", "hearing_date", "outcome", "outcome_date"} с
    датами «ДД.ММ.ГГГГ»|"" и outcome:
      ""          — порядок не запускался (заявления в карточке нет);
      "pending"   — заявление подано, определения ещё нет;
      "cancelled" — заочное решение отменено (ст. 241), дело рассматривается
                    заново;
      "refused"   — в отмене отказано; с этого дня течёт месяц на апелляцию
                    (ст. 237 ч. 2);
      "unknown"   — заявление подано, но исход не читается: суд не заполнил
                    «Результат» дольше BANK_DEFAULT_CANCEL_PENDING_MAX_DAYS
                    либо уже вынес новое решение. Последствий не имеет —
                    окна и ритм опроса возвращаются к обычным (без потолка
                    дело висело бы в активных вечно и парсилось бы каждым
                    прогоном: ветка pending снимает и архивацию, и недельный
                    ритм).

    Новое заявление обнуляет исход прошлого круга — заочное решение может
    выноситься повторно (ст. 243 запрещает повторное заявление только по
    решению, вынесенному ПОСЛЕ отмены).
    """
    today = today or date.today()
    filed_date = hearing_date = outcome = outcome_date = ""
    last_decision: date | None = None
    for ev in fi.get("events") or []:
        text = (ev.get("text") or "").lower().replace("ё", "е")
        if not text:
            continue
        ev_dt = parse_date(ev.get("date") or "")
        if _ANY_DECISION_RX.search(text):
            if ev_dt:
                last_decision = ev_dt.date()
            continue
        if _DEFAULT_CANCEL_FILED_RX.search(text):
            filed_date = ev.get("date") or filed_date
            hearing_date = outcome = outcome_date = ""
            continue
        if _DEFAULT_CANCEL_HEARING_RX.search(text):
            hearing_date = ev.get("date") or hearing_date
            # Колонка «Результат события» точнее склейки; её нет у legacy-
            # записей — тогда ищем исход в самом тексте события.
            result_col = (ev.get("result_event") or "").strip()
            src = (result_col or text).lower().replace("ё", "е")
            if _DEFAULT_CANCEL_GRANTED_RX.search(src):
                outcome, outcome_date = "cancelled", ev.get("date") or ""
            elif _DEFAULT_CANCEL_REFUSED_RX.search(src):
                outcome, outcome_date = "refused", ev.get("date") or ""
    if not filed_date:
        return {"filed_date": "", "hearing_date": "",
                "outcome": "", "outcome_date": ""}
    if not outcome:
        filed_d = parse_date(filed_date)
        anchor_dt = parse_date(hearing_date) or filed_d
        if (last_decision and filed_d
                and last_decision > filed_d.date()):
            # После подачи заявления суд уже вынес новое решение — порядок
            # отработал, а результат в карточке не отражён.
            outcome = "unknown"
        elif (anchor_dt and (today - anchor_dt.date()).days
                > config.BANK_DEFAULT_CANCEL_PENDING_MAX_DAYS):
            outcome = "unknown"
        else:
            outcome = "pending"
    return {"filed_date": filed_date, "hearing_date": hearing_date,
            "outcome": outcome, "outcome_date": outcome_date}


def default_cancellation_pending(fi: dict, today: date | None = None) -> bool:
    """Заявление об отмене подано, определения суда ещё нет."""
    return default_cancellation_state(fi or {}, today)["outcome"] == "pending"


# Срок для представления возражений на апелляционную жалобу (ст. 325 ГПК).
# Строка живёт НЕ в «Движении дела», а во вкладке карточки «Обжалование
# решений, определений» → вложенная таблица «Движение жалобы», и приходит в
# fi["appeal_events"] (см. _fi_appeal_events в parsing/cards.py). У части
# записей колонки разобраны (name + note), у части осталась только склейка
# text — смотрим оба, как default_cancellation_state.
# ⚠️ Слово «возражени» обязательно: рядом в той же таблице живёт «Оставление
# жалобы (представления) без движения · Срок для устранения недостатков до …»
# — это другой срок, и он в дедлайн возражений попадать не должен.
_OBJECTIONS_TERM_RX = re.compile(r"срок\w*\s+для\s+пред\w*\s+возражени")
_OBJECTIONS_DUE_RX = re.compile(r"срок\s+до\s+(\d{1,2}\.\d{1,2}\.\d{4})")


def appeal_objections_deadline(fi: dict) -> tuple[str, str] | None:
    """(дата установления, срок) из движения апел. жалобы в ISO — или None.

    При нескольких строках побеждает МАКСИМАЛЬНЫЙ срок: суд его продлевает, а
    прежняя строка из карточки никуда не девается.
    """
    best_due: date | None = None
    best_set = ""
    for ev in fi.get("appeal_events") or []:
        haystack = " ".join(
            str(ev.get(k) or "") for k in ("name", "text", "note")
        ).lower().replace("ё", "е")
        if not _OBJECTIONS_TERM_RX.search(haystack):
            continue
        m = _OBJECTIONS_DUE_RX.search(haystack)
        if not m:
            continue
        due_dt = parse_date(m.group(1))
        if not due_dt:
            continue
        if best_due is None or due_dt.date() > best_due:
            best_due = due_dt.date()
            set_dt = parse_date(ev.get("date") or "")
            best_set = set_dt.date().isoformat() if set_dt else ""
    if best_due is None:
        return None
    return best_set, best_due.isoformat()


# ── Апеллянт из карточки 1-й инстанции ───────────────────────────────────────
# Живут ЗДЕСЬ, а не в runs.py, потому что их зовёт ещё и bank_intake: приём
# иска банка должен класть апеллянта в запись сразу, а импортировать runs он
# не может (runs сам импортирует bank_intake — вышел бы цикл). В runs.py
# оставлены ре-экспорты прежних приватных имён: их зовут и код, и тесты.
#
# «Грязное» имя апеллянта — сохранённое слово-роль вместо настоящего имени.
# Такие записи перезаписываются на каждом прогоне, поэтому is_bank для «голой»
# роли самовосстанавливается при изменении логики без миграции данных.
# Составные слова-роли («ИСТЕЦ, ПРЕДСТАВИТЕЛЬ») ловит appellant_role_words —
# проверять через is_dirty_appellant_name.
_DIRTY_APPELLANT_NAMES = ("", "истец", "ответчик", "третье лицо", "иное лицо", "банк")


def is_dirty_appellant_name(name: str) -> bool:
    """True, если сохранённое имя апеллянта — не настоящее имя.

    «Грязное» = пустое, слово-роль из _DIRTY_APPELLANT_NAMES или набор
    слов-ролей, включая составные («ИСТЕЦ, ПРЕДСТАВИТЕЛЬ», «ПРЕДСТАВИТЕЛЬ»).
    Такие значения безопасно перезаписывать пересчётом — настоящее
    найденное имя не трогаем.
    """
    s = (name or "").strip()
    if s.lower() in _DIRTY_APPELLANT_NAMES:
        return True
    return appellant_role_words(s) is not None


def bank_sole_role_holder(case_j: dict, role: str) -> bool:
    """True, если банк — единственная сторона данной роли в деле.

    Сторона разбирается готовым _norm_party_tokens: филиальные запятые
    Сбера склеиваются («ПАО Сбербанк в лице филиала — Югорское отделение
    №5940» остаётся одним токеном), а имя с «настоящими» внутренними
    запятыми (МТУ Росимущества в Тюменской области, ХМАО-Югре, ЯНАО)
    распадается на несколько токенов и даёт консервативное False.
    """
    party = case_j.get("plaintiff" if role == "Истец" else "defendant", "")
    tokens = _norm_party_tokens(party)
    return len(tokens) == 1 and any(p in tokens[0] for p in config.SBER_PATTERNS)


def appellant_is_bank(raw: str, role: str, case_j: dict) -> bool | None:
    """is_bank подателя жалобы (апеллянта/кассатора) по сырому «Заявителю».

    Слово-роль само по себе не содержит признаков банка. Банк — податель,
    только когда роль подателя совпадает с ролью банка И банк — единственная
    сторона этой роли: при соответчиках жалобу «ОТВЕТЧИКА» мог подать любой
    из них → None («знаем, что определить нельзя» — фронт не выводит ни
    'bank', ни 'other'). Составные слова-роли разбирает appellant_role_words:
    одна сторона в составе («ИСТЕЦ, ПРЕДСТАВИТЕЛЬ») — считаем по ней; ноль
    («ПРЕДСТАВИТЕЛЬ»: чей — неизвестно) или несколько («ИСТЕЦ, ТРЕТЬЕ ЛИЦО»)
    → None, а не False — иначе фронт вешал бы бейдж на противника банка
    (кейс 33-5089/2026). Именной вход — только сам ПАО Сбербанк: дочки
    (страхование/НПФ/лизинг) отсеиваются name_is_real_sberbank (кейс
    8Г-11469/2026: 🏦 «жалоба банка» вставал на ООО «Сбербанк страхование
    жизни», решение юриста 09.08.2026).
    """
    role_words = appellant_role_words(raw)
    if role_words is not None:
        if len(role_words) != 1:
            return None  # податель по словам-ролям неопределим
        side = role_words[0]
        if side in ("Истец", "Ответчик"):
            if side != case_j.get("bank_role", ""):
                return False
            if bank_sole_role_holder(case_j, side):
                return True
            return None
        return None if case_j.get("bank_role") == "Третье лицо" else False
    return config.name_is_real_sberbank(raw)


def apply_fi_appellant(fi: dict, case_j: dict, card_info: dict) -> bool:
    """Проставить апеллянта из карточки 1-й инстанции.

    Источник — `card_info["_fi_appellant_raw"]` (поле «Заявитель» вкладки
    обжалования / «заявитель жалобы»; бывает слово-роль «ИСТЕЦ»/«ОТВЕТЧИК»
    или ФИО). Считаем роль/имя/is_bank ОДИН раз и пишем И в first_instance
    (`fi.appeal_appellant_*` — источник бейджа «Апеллянт» уже в раннем окне
    first_instance/awaiting_appeal, до появления карточки в апел. суде, т.к.
    карточка апел. суда подателя жалобы не публикует), И — если блок уже
    создан link_cases — в appeal (`appeal.appellant_*`). Перезаписываем
    пустое/«грязное» legacy-значение (роль вместо имени); настоящее найденное
    имя не трогаем.

    Возвращает True, если что-то изменилось (для флага `changed`).
    """
    raw = (card_info.get("_fi_appellant_raw") or "").strip()
    if not raw:
        return False
    role, short = classify_appellant_role(
        raw, case_j.get("plaintiff", ""), case_j.get("defendant", "")
    )
    is_bank = appellant_is_bank(raw, role, case_j)

    changed = False
    # Сентинел отличает «ключа нет» от «записан null»: is_bank=None должен
    # ЯВНО попасть в JSON (null «знаем, что неопределимо» блокирует на фронте
    # legacy-вывод 'other' из слова-роли; отсутствие ключа — не блокирует).
    _missing = object()

    # first_instance — источник для бейджа в раннем окне.
    old_fi_name = (fi.get("appeal_appellant") or "").strip()
    if is_dirty_appellant_name(old_fi_name):
        if short and short != old_fi_name:
            fi["appeal_appellant"] = short
            changed = True
        if fi.get("appeal_appellant_is_bank", _missing) != is_bank:
            fi["appeal_appellant_is_bank"] = is_bank
            changed = True
        if role and fi.get("appeal_appellant_status") != role:
            fi["appeal_appellant_status"] = role
            changed = True

    # appeal — те же значения, если блок уже создан link_cases.
    appeal_block = case_j.get("appeal")
    if appeal_block:
        old_app_name = (appeal_block.get("appellant") or "").strip()
        if is_dirty_appellant_name(old_app_name):
            if short and short != old_app_name:
                appeal_block["appellant"] = short
                changed = True
            if appeal_block.get("appellant_is_bank", _missing) != is_bank:
                appeal_block["appellant_is_bank"] = is_bank
                changed = True
            if role and appeal_block.get("appellant_status") != role:
                appeal_block["appellant_status"] = role
                changed = True

    return changed


def stamp_objections_deadline(fi: dict) -> str:
    """Проставить срок возражений в запись; вернуть срок ISO или "".

    Пустой результат поля СНИМАЕТ — тот же принцип, что у `writ_expected` и
    `legal_force_est` в `split_bank_track`: иначе перепарс карточки, где суд
    убрал ошибочную строку, оставил бы фантомный срок навсегда.
    """
    fi = fi if isinstance(fi, dict) else {}
    got = appeal_objections_deadline(fi)
    if not got:
        fi.pop("objections_due", None)
        fi.pop("objections_set_at", None)
        return ""
    set_at, due = got
    fi["objections_due"] = due
    if set_at:
        fi["objections_set_at"] = set_at
    else:
        fi.pop("objections_set_at", None)
    return due


def default_judgment_vacated(fi: dict, today: date | None = None) -> bool:
    """Заочное решение отменено, а запись всё ещё держит его как действующее.

    ⚠️ Сравниваем дату отмены с ЗАМОРОЖЕННОЙ `decision_date`, а не «нет ли
    более позднего решения»: при недельном ритме опроса (BANK_WRIT_CHECK_DAYS)
    отмена и новое решение по ст. 243 попадают в одно окно парса, и проверка
    «нет более позднего решения» не сработала бы никогда — дело осталось бы с
    датой отменённого решения и resolved_emitted=True, а новое решение не
    попало бы в дайджест вовсе. С этим условием предикат самоисцеляется: как
    только эмит fi_resolved переставит decision_date на новое решение, он
    гаснет сам.
    """
    fi = fi or {}
    st = default_cancellation_state(fi, today)
    if st["outcome"] != "cancelled":
        return False
    cancelled_dt = parse_date(st["outcome_date"])
    if not cancelled_dt:
        return False
    # decision_date_vacated — та же дата после отката (см. classify_writ_kind):
    # без неё предикат, однажды сработав, считал бы «решения нет» вечно даже
    # после нового решения по существу.
    frozen_dt = (parse_date(fi.get("decision_date") or "")
                 or parse_date(fi.get("decision_date_vacated") or ""))
    return not frozen_dt or cancelled_dt > frozen_dt


def default_cancellation_blocks_appeal(fi: dict,
                                       today: date | None = None) -> bool:
    """Апелляционного хода у ответчика пока нет — признак жалобы не считается.

    Гейт переезда иска банка в основную картотеку и смены стадии. Закрывает
    ровно два окна: заявление на рассмотрении и решение отменено (жалоба на
    отменённое решение беспредметна). Жалоба, поданная уже ПОСЛЕ
    возобновления, дело выпускает как обычно.

    `sent_to_appeal` не глушится никогда: дело физически ушло в облсуд —
    жёсткое доказательство настоящей апелляции.
    """
    fi = fi or {}
    if fi.get("sent_to_appeal") or fi.get("sent_to_appeal_date"):
        return False
    st = default_cancellation_state(fi, today)
    if st["outcome"] == "pending":
        return True
    if st["outcome"] != "cancelled":
        return False
    if not default_judgment_vacated(fi, today):
        return False
    filed_dt = parse_date(fi.get("appeal_filed_date") or "")
    cancelled_dt = parse_date(st["outcome_date"])
    if filed_dt and cancelled_dt and filed_dt > cancelled_dt:
        return False
    return True


def bank_legal_force_est(fi: dict) -> date | None:
    """Расчётная дата вступления решения в силу (иск банка, без апелляции).

    Возвращает ПЕРВЫЙ день, когда решение в силе (следующий календарный день
    после последнего дня срока обжалования; без сдвига на рабочий — в силу
    решение вступает и в выходной). Сроки — по ГПК: дни рабочие (ст. 107),
    месяц календарный (ст. 108, month_term_last_day).

    Обычное решение: мотивировка (act_date → событие «Изготовлено
    мотивированное решение» → расчётно decision_date + 10 раб. дн, ст. 199)
    + месяц на апелляцию (ст. 321).

    Заочное решение (ст. 233-237, детект bank_default_judgment_info):
    - копия вручена ответчику → вручение + 7 раб. дн (заявление об отмене)
      + месяц на апелляцию от истечения этого срока;
    - сведений о вручении нет / возвратилась невручённой → формула ВС
      (Обзор №2 (2015), в. 14): якорь + 3 раб. дн + 7 раб. дн + месяц;
      якорь — мотивировка, если она позже даты решения, иначе дата решения
      (без добавки 10 раб. дн: копию суд высылает от принятия решения).

    Все даты пусты → None (решения ещё нет или карточка без дат — потолок
    ожидания ИЛ считается от других якорей).
    """
    from court_monitor.textutil import add_working_days, month_term_last_day

    # Особый порядок отмены (ст. 237-243 ГПК) останавливает счёт: пока
    # заявление на рассмотрении — срок не течёт, а после отмены решения нет
    # вовсе. Пустой результат заставляет split_bank_track снять ключ
    # legal_force_est, и фронтовый бейдж «⏳ ждёт ИЛ» гаснет сам.
    _cancel = default_cancellation_state(fi)
    if _cancel["outcome"] == "pending" or default_judgment_vacated(fi):
        return None
    if _cancel["outcome"] == "refused":
        # В отмене отказано — с этого дня течёт месяц на апелляцию
        # (ст. 237 ч. 2), а не от вручения копии.
        refused_dt = parse_date(_cancel["outcome_date"])
        if refused_dt:
            return month_term_last_day(refused_dt.date()) + timedelta(days=1)

    info = bank_default_judgment_info(fi)
    # decision_date — замороженная дата решения; hearing_date остаётся
    # последним фолбэком для архивных записей (migrate_stages идёт только по
    # активным) и дел, воскрешённых из архива.
    base_dt = (parse_date(fi.get("decision_date") or "")
               or parse_date(fi.get("hearing_date") or ""))
    base = base_dt.date() if base_dt else None
    motiv_dt = (parse_date(fi.get("act_date") or "")
                or parse_date(info["motivirovka_date"]))
    motiv = motiv_dt.date() if motiv_dt else None

    if info["default_judgment"]:
        served_dt = parse_date(info["default_copy_served_date"])
        if served_dt:
            cancel_last = add_working_days(
                served_dt.date(), config.BANK_DEFAULT_CANCEL_WORKDAYS)
            last = month_term_last_day(cancel_last)
        else:
            anchor = motiv if (motiv and base and motiv > base) else (base or motiv)
            if not anchor:
                return None
            sent_last = add_working_days(
                anchor, config.BANK_DEFAULT_COPY_SEND_WORKDAYS)
            cancel_last = add_working_days(
                sent_last, config.BANK_DEFAULT_CANCEL_WORKDAYS)
            last = month_term_last_day(cancel_last)
    else:
        if not motiv and base:
            motiv = add_working_days(base, config.BANK_MOTIVATION_TERM_WORKDAYS)
        if not motiv:
            return None
        last = month_term_last_day(motiv)
    return last + timedelta(days=1)


def classify_writ_kind(writ: dict, fi: dict) -> str:
    """Тип исполнительного листа: "enforcement" | "interim" | "unknown".

    Суд тип листа не публикует (в таблице «ИСПОЛНИТЕЛЬНЫЕ ЛИСТЫ» только
    дата/номер/статус/получатель), но дата выдачи разделяет типы безошибочно
    (данные пилота 26.07.2026, 24 листа: 16/8, зазор без пограничных):
    - лист ДО даты резолютивки (реально — в первые дни после подачи иска,
      за 27..163 дн до решения) — обеспечительные меры (арест имущества,
      лист выдаётся сразу после определения об обеспечении, ст. 142 ГПК);
    - лист ПОСЛЕ решения (реально +40..55 дн — вступление в силу +
      изготовление) — принудительное исполнение решения.
    Решения ещё нет → любой лист может быть только обеспечительным.
    Нет даты выдачи → unknown (в архивных окнах не считается исполнением).

    ⚠️ Якорь — ЗАМОРОЖЕННАЯ decision_date, а не hearing_date. Последняя у
    решённого дела держит дату решения, но перечитывается каждым прогоном из
    последнего session-события карточки и уедет вперёд, назначь суд заседание
    по судебным расходам / индексации / разъяснению решения. Лист на
    исполнение тогда оказался бы «до заседания» и молча стал бы
    обеспечительным — вместе с бейджем, KPI «С ИЛ» и окном архива, причём
    дайджест бы об этом промолчал (гард case_decided). hearing_date остаётся
    фолбэком для архивных записей и дел, воскрешённых из архива.
    """
    issue = parse_date(writ.get("issue_date") or "")
    if not issue:
        return "unknown"
    # decision_date_vacated — та же замороженная дата, убранная откатом
    # отменённого заочного решения (ст. 241 ГПК). Читаем её вторым якорем,
    # чтобы отмена не перевернула тип уже выданного листа: без неё фолбэк
    # уходит на дрейфующую hearing_date.
    anchor = (parse_date(fi.get("decision_date") or "")
              or parse_date(fi.get("decision_date_vacated") or "")
              or parse_date(fi.get("hearing_date") or ""))
    if not anchor:
        return "interim"
    return "enforcement" if issue >= anchor else "interim"


def bank_writ_expected(fi: dict) -> bool:
    """Появится ли по этому иску банка исполнительный лист.

    False — значит ждать нечего: в иске банку ОТКАЗАНО полностью либо дело
    завершено процессуально (присоединено к другому — исполнять будут по
    делу-приёмнику; возвращено; отказано в ПРИНЯТИИ; передано по подсудности —
    лист выдаст суд, куда дело ушло). Частичное удовлетворение листом
    сопровождается, поэтому «удовлетворён частично» остаётся True.

    Единственный источник правды для всей цепочки: архивное окно и ритм опроса
    здесь, а фронт читает готовый штамп first_instance.writ_expected (его
    ставит split_bank_track) — своей копии правила в JS нет, расходиться нечему.

    Вид завершения решает `classify_fi_termination`, а не строковый матч
    (разгон Урала 14.08.2026, дело 9-125/2026 Пуровского районного): прежняя
    версия ЯВНО исключала «отказано в принятии» — в расчёте на то, что у
    такого дела своя ветка завершения со статусом карточки «Возвращено». На
    практике карточка отдала «Решено», дело ушло в ветку «Решено без ИЛ» и
    180 дней числилось в очереди на лист, которого не будет; тем же дефектом
    в ХМАО задеты 9-31/2026 (возврат) и 2-1588/2026, 2-8088/2026 (передача по
    подсудности) — все со статусом «Решено».

    ⚠️ Классификатор зовём ТОЛЬКО по полю «Результат»: last_event/events не
    передаём. При пустом «Результате» он уходит в историю движения, а там у
    ЖИВОГО дела лежит отменённый возврат прошлого круга — дело молча
    перестало бы ждать лист. Тот же гейт, что в `fi_termination_details`.
    """
    fi = fi or {}
    if fi.get("merged") or fi_is_merged(fi):
        return False
    result = (fi.get("result") or "")
    if classify_fi_termination(result, "", []) is not None:
        return False
    if "отказано" in result.lower():
        return False
    return True


def _is_bank_track_archived(fi: dict, now: datetime) -> bool:
    """Архивные окна лёгкого трека исков банка (ветка is_case_archived).

    Обычное FI_ARCHIVE_DAYS=60 от резолютивки здесь не годится: исполнительный
    лист появляется на +40..90+ день (мотивировка → вступление в силу →
    выдача), дело уходило бы в архив ровно в окне ожидания ИЛ.

    - признак жалобы/направления выше → не архивируем (дело покинет трек);
    - присоединено к другому делу: BANK_MERGED_ARCHIVE_DAYS (30) от даты
      определения — окно на отмену объединения (статус карточки при этом
      остаётся «В производстве», до общих веток дело бы не дошло);
    - листа не будет (в иске отказано ИЛИ дело завершено процессуально —
      см. bank_writ_expected): BANK_DENIED_ARCHIVE_DAYS (30) от мотивировки —
      окно на жалобу банка (апелляционную по ст. 321 ГПК при отказе в иске,
      частную по ст. 332 — на определение о возврате/отказе в принятии/
      передаче). Ветка стоит ДО поиска листов: 180-дневный потолок ожидания
      ИЛ к таким делам не применим;
    - статус не «Решено»/«Возвращено» → активное, не архивируем;
    - «Возвращено» (возврат/прекращение): BANK_RETURNED_ARCHIVE_DAYS (30) от
      event_date/hearing_date — окно на частную жалобу банка;
    - «Решено» + ИЛ выдан: BANK_WRIT_ARCHIVE_DAYS (14) от последней даты
      выдачи листа — окно на смену статуса («Отозван»/«Возвращен»);
    - «Решено» без ИЛ: потолок BANK_WRIT_WAIT_MAX_DAYS (180) от расчётного
      вступления в силу (фолбэк — hearing_date/event_date), иначе пул
      опрашивался бы вечно.
    """
    if (fi.get("appeal_filed") or fi.get("appeal_filed_date")
            or fi.get("cassation_filed") or fi.get("sent_to_cassation")):
        return False
    # Заявление об отмене заочного решения на рассмотрении — держим дело в
    # активных до определения суда (решение юриста 03.08.2026): исход может
    # вернуть иск на новое рассмотрение по существу. Потолок ожидания —
    # BANK_DEFAULT_CANCEL_PENDING_MAX_DAYS внутри самого предиката.
    if default_cancellation_pending(fi):
        return False
    status = (fi.get("status") or "").strip()
    if fi.get("merged") or fi_is_merged(fi):
        anchor = (parse_date(fi.get("merged_at") or "")
                  or parse_date(fi.get("event_date") or "")
                  or parse_date(fi.get("hearing_date") or ""))
        return bool(anchor) and (now - anchor).days > config.BANK_MERGED_ARCHIVE_DAYS
    if status == "Решено" and not bank_writ_expected(fi):
        # Якорь — мотивировка: месячный срок обжалования по ст. 321 ГПК течёт
        # от неё, так что 30 дней ≈ ровно окно на жалобу банка. Резолютивка и
        # дата заседания — фолбэки для карточек, где мотивировку не публиковали.
        # Последний фолбэк event_date несёт процессуальные завершения со
        # статусом «Решено» (9-125/2026: отказ в принятии, ни решения, ни
        # заседания в карточке нет вовсе — без него дело осталось бы активным
        # навсегда).
        anchor = (parse_date(fi.get("act_date") or "")
                  or parse_date(fi.get("motivirovka_date") or "")
                  or parse_date(fi.get("decision_date") or "")
                  or parse_date(fi.get("hearing_date") or "")
                  or parse_date(fi.get("event_date") or ""))
        return bool(anchor) and (now - anchor).days > config.BANK_DENIED_ARCHIVE_DAYS
    if status == "Возвращено":
        anchor = (parse_date(fi.get("event_date") or "")
                  or parse_date(fi.get("hearing_date") or ""))
        return bool(anchor) and (now - anchor).days > config.BANK_RETURNED_ARCHIVE_DAYS
    if status != "Решено":
        return False
    # Окно «лист выдан → архив» считается ТОЛЬКО по листам на исполнение
    # решения: обеспечительный лист выдаётся в начале дела (задолго до
    # решения), и без classify_writ_kind дело 2-6005 (решено, есть лишь
    # обеспечительный лист) ушло бы в архив, не дождавшись листа на
    # исполнение — вопрос юриста 26.07.2026.
    issue_dates = [
        d for d in (
            parse_date(w.get("issue_date") or "")
            for w in (fi.get("writs") or [])
            if classify_writ_kind(w, fi) == "enforcement"
        )
        if d
    ]
    if issue_dates:
        # Заочному решению — 3 месяца вместо 14 дней (решение юриста
        # 03.08.2026): ответчик подаёт заявление об отмене в тот же суд
        # (ст. 237 ГПК), и суды реально отменяют такие решения спустя 1-2
        # месяца. ⚠️ Заочность ПЕРЕСЧИТЫВАЕМ по событиям, а не читаем штамп
        # fi["default_judgment"]: у архивных записей его нет вовсе — именно
        # поэтому три заочных дела Сургутского гор. выглядели «обычными» и
        # ушли в архив по 14-дневному окну 27.07.2026.
        window = (config.BANK_DEFAULT_WRIT_ARCHIVE_DAYS
                  if bank_default_judgment_info(fi)["default_judgment"]
                  else config.BANK_WRIT_ARCHIVE_DAYS)
        return (now - max(issue_dates)).days > window
    est = bank_legal_force_est(fi)
    if est:
        return (now.date() - est).days > config.BANK_WRIT_WAIT_MAX_DAYS
    anchor = (parse_date(fi.get("hearing_date") or "")
              or parse_date(fi.get("event_date") or ""))
    return bool(anchor) and (now - anchor).days > config.BANK_WRIT_WAIT_MAX_DAYS


# Рутинные типы событий track-дел — гасятся в дайджесте при
# config.BANK_DIGEST_ROUTINE=0 (рычаг масштабирования: при ~1000 исков банка
# заседания затопили бы Telegram-лимит). Содержательные типы (решение, акт,
# возврат, жалобы, ИЛ) не входят и доставляются всегда.
BANK_ROUTINE_EVENT_TYPES = (
    "fi_hearing_new", "fi_hearing_next", "fi_hearing_postponed",
    "fi_hearing_recess", "fi_hearing_restart", "fi_status_change",
    "fi_accepted_no_hearing", "fi_final_event",
)


def filter_bank_routine_events(fi_changes: list[dict]) -> list[dict]:
    """Убрать рутину track-дел из fi_changes (при BANK_DIGEST_ROUTINE=0).

    Обычные (не track) записи не трогаются. Track-запись, у которой после
    фильтра не осталось типов, выпадает целиком. Применяется в main_json ДО
    save_digest_context — replay и push видят тот же список.
    """
    kept: list[dict] = []
    for ch in fi_changes:
        if ch.get("track") != "plaintiff_light":
            kept.append(ch)
            continue
        types = [
            t for t in (ch.get("type") or [])
            if t not in BANK_ROUTINE_EVENT_TYPES
        ]
        if types:
            kept.append({**ch, "type": types})
    return kept


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
# fi_returned здесь с 29.07.2026: раньше он был живым событием стадии
# first_instance (ветка фантомной даты), теперь несёт ИСХОД (процессуальное
# завершение, см. fi_termination_details) — у дела со связанной апелляцией
# (частная жалоба на возврат) это тот же догоняющий пересказ: первый парс
# FI-карточки дела «с апелляции» объявлял бы полугодовой возврат новостью.
FI_ECHO_CATCHUP_TYPES = (
    "fi_resolved", "fi_act_published", "fi_act_text_published",
    "fi_motivirovka_emitted", "fi_final_event", "fi_status_change",
    "fi_returned",
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
    Живые события (заседания, касс. сигналы до связки 7kas, смена роли
    банка) не трогаем.

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
    # Заседание по решённому делу трека исков банка (индексация, расходы,
    # отсрочка): тот же анонс с details["hearing_date"] — прошлая дата не
    # новость, а раскопанная история (эмит и так гейтится будущей датой,
    # фильтр — ремень к подтяжкам).
    "fi_post_decision_hearing",
)
# Жалобы/направления: тип → ключ details с датой самого события.
_FI_DATED_COMPLAINT_TYPES = {
    "fi_appeal_filed": "appeal_filed_date",
    "fi_cassation_filed": "cassation_filed_date",
    "fi_sent_to_cassation": "sent_to_cassation_date",
    # Якорь срока возражений — САМ СРОК, а не дата его установления: он в
    # будущем, поэтому штатный дедлайн фильтр не тронет, а давно просроченный
    # (карточка старого дела) не проскочит. В эхо-класс тип НЕ входит:
    # апелляционная карточка срок для возражений не публикует, и для дела со
    # связанной апелляцией это не «догоняющая» новость.
    "fi_objections_deadline_set": "objections_due",
    # «Решение вступило в силу (расч.)» трека исков банка: страховка от
    # массового импорта давно решённых дел — «вступление» многомесячной
    # давности не новость. fi_writ_overdue здесь НЕТ намеренно: поздно
    # обнаруженный зависший лист — тем более алерт.
    "fi_legal_force_reached": "legal_force_date",
}
# «Догоняющие» события об акте/решении: тип → ключи details с датой (первая
# читаемая побеждает — у части карточек «Дата публикации акта» пуста, и
# единственный якорь остаётся в дате решения). Гасятся ТОЛЬКО на первом парсе
# заведённого дела, см. suppress_stale_fi_events.
_FI_CATCHUP_DATED_TYPES = {
    "fi_resolved": ("decision_date",),
    "fi_act_published": ("act_date", "decision_date"),
    "fi_act_text_published": ("act_date", "decision_date"),
    "fi_motivirovka_emitted": ("motivirovka_date", "decision_date"),
    "fi_final_event": ("event_date",),
    # Особый порядок отмены заочного решения: на первом парсе только что
    # заведённой карточки давно завершённый порядок — не новость (импортёр
    # Урала заводит карточки с многолетней историей). Тот же класс, что
    # кейс 2-592/2025.
    "fi_default_cancellation_filed": ("cancel_filed_date",),
    "fi_default_cancellation_hearing": ("cancel_hearing_date",),
    "fi_default_judgment_vacated": ("cancel_outcome_date",),
    "fi_default_cancellation_refused": ("cancel_outcome_date",),
    # Возврат копии заочного решения: тот же класс догоняющих — у только что
    # заведённой карточки давний возврат копии не новость (второй слой
    # анти-паводка после посева в migrate_stages).
    "fi_default_copy_returned": ("copy_returned_date",),
    # Вручение копии заочного решения: парное к возврату, тот же класс.
    "fi_default_copy_served": ("copy_served_date",),
}


def suppress_stale_fi_events(change: dict, today: date | None = None, *,
                             first_parse: bool = False) -> list[str]:
    """Убрать из change["type"] стародатные события (дополнение к
    suppress_fi_echo_events — то ловит «вышестоящее дело уже известно»,
    это — «новость протухла», даже если вышестоящей карточки нет):

    - анонс заседания (fi_hearing_new/next/postponed/recess) с датой
      СТРОГО в прошлом — «заседание назначено на 17.12.2025» в июле-2026
      не новость (сегодняшняя дата — ещё анонс);
    - жалоба/направление в касс. суд с датой старше
      config.DIGEST_STALE_EVENT_DAYS (первый парс старой карточки);
    - `first_parse=True` — «догоняющие» события об акте/решении
      (_FI_CATCHUP_DATED_TYPES) с датой старше того же порога.

    Третье правило требует ОБОИХ условий намеренно. Только по возрасту
    фильтровать нельзя: суд штатно публикует текст акта через недели после
    решения, и в основной картотеке это настоящая новость. Только по «первому
    парсу» — тоже нельзя: свежий иск с решением на той же неделе объявить
    надо. Вместе они описывают ровно один случай — раскопки истории только
    что заведённой карточки (2-592/2025: решение 06.10.2025, заведено
    31.07.2026, объявлено «текст решения опубликован» 03.08.2026 и тем же
    прогоном ушло в архив). Гейт приёма (bank_intake.entry_is_spent) ловит
    такие дела раньше — это страховка для тех, кто дожил до архивного окна
    уже внутри трека.

    ⚠️ Исполнительные листы (fi_writ_issued / fi_writ_status_changed) в
    правило не входят: ради них трек исков банка и существует, а «старый»
    лист может быть выдан задолго до постановки дела на мониторинг.

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
        elif first_parse and t in _FI_CATCHUP_DATED_TYPES:
            d = next(
                (p for p in (parse_date(details.get(k) or "")
                             for k in _FI_CATCHUP_DATED_TYPES[t]) if p),
                None,
            )
            if d and (today - d.date()).days > config.DIGEST_STALE_EVENT_DAYS:
                stale = True
        (removed if stale else kept).append(t)
    if removed:
        change["type"] = kept
        # Тяжёлый пересказ мотивировки уезжает вместе с подавленным событием —
        # иначе он остался бы в снимке контекста (и в оплаченном LLM-пересказе
        # акта) ради строки, которую никто не увидит. Тот же приём, что в
        # suppress_fi_echo_events.
        if "fi_act_text_published" in removed:
            details.pop("act_text", None)
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
            # Иск банка в особом порядке отмены заочного решения стадию не
            # меняет: апелляционного хода у ответчика ещё нет (ст. 237 ч. 2).
            # ⚠️ Сужение до трека обязательно — код общий: банк-ОТВЕТЧИК с
            # заочным решением против него иначе не дошёл бы до
            # awaiting_appeal, а relink_awaiting_appeal требует именно эту
            # стадию (на Урале это единственный канал связки с апелляцией).
            if (is_bank_plaintiff_track(case)
                    and default_cancellation_blocks_appeal(fi)):
                return None
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
        # Лёгкий трек исков банка — свои окна (ожидание исполнительного листа
        # дольше обычного 60-дневного окна, см. _is_bank_track_archived).
        if is_bank_plaintiff_track(case):
            return _is_bank_track_archived(fi, now)
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


def repair_vacated_default_judgments(cases: list[dict]) -> int:
    """Чинит дела, застигнутые особым порядком отмены заочного решения.

    Два ремонта, оба идемпотентные и по правилам (не по списку номеров):

    1. Заочное решение ОТМЕНЕНО (ст. 241 ГПК), а запись держит его как
       действующее: статус «Решено», resolved_emitted, замороженная
       decision_date, расчётное вступление в силу. Тот же откат, что делает
       FI-цикл при живом парсе, — нужен потому, что решённое дело трека
       опрашивается раз в неделю, и до ближайшего парса дашборд показывал бы
       «⏳ ждёт ИЛ» по несуществующему решению (кейс 2-243/2026, Югорский).

    2. Иск банка, уехавший в основную картотеку по жалобе, поданной ДО
       определения по заявлению об отмене: апелляционного хода у ответчика
       ещё нет (ст. 237 ч. 2), дело должно оставаться в лёгком треке
       (кейс 2-616/2026, Пыть-Яхский: жалоба 23.07, заявление об отмене 28.07,
       заседание по нему 10.08). Возвращаем маркер трека и стадию — дальше
       split_bank_track уложит дело в cases_bank.json сам.

    Возвращает число изменённых дел.
    """
    fixed = 0
    for case in cases:
        fi = case.get("first_instance") or {}
        if not fi:
            continue
        touched = False
        if default_judgment_vacated(fi):
            if (fi.get("status") or "").strip() in ("Решено", "Возвращено"):
                fi["status"] = "В производстве"
                touched = True
            if fi.get("result"):
                fi["result"] = ""
                touched = True
            if fi.get("decision_date"):
                fi["decision_date_vacated"] = fi.pop("decision_date")
                touched = True
            for flag in ("resolved_emitted", "motivirovka_emitted"):
                if fi.get(flag):
                    fi[flag] = False
                    touched = True
        # Возврат в лёгкий трек. Признак «дело оттуда уехало» — track_origin,
        # который ставит split_bank_track; апелляционной карточки при этом
        # быть не должно (её появление — законный выход из трека).
        if (case.get("track_origin") == "plaintiff_light"
                and not case.get("track")
                and not case.get("appeal")
                and case.get("current_stage") in ("first_instance",
                                                  "awaiting_appeal")
                and default_cancellation_blocks_appeal(fi)):
            case["track"] = "plaintiff_light"
            case.pop("track_origin", None)
            case["current_stage"] = "first_instance"
            touched = True
        if touched:
            fixed += 1
    if fixed:
        log.info(f"Ремонт особого порядка отмены заочного решения: {fixed}")
    return fixed


def migrate_intake_checked_stamp(cases: list[dict]) -> int:
    """Проставить дату проверки делам трека, заведённым до появления штампа.

    `make_bank_entry` пишет `last_checked_at` при заведении с 14.08.2026 —
    карточку читает сам импорт. Записи, созданные раньше, штампа не имеют, и
    ветка force-parse в `should_skip_case` перебивает у них и будущее
    заседание, и недельный ритм ИЛ: прогон читает карточку заново каждый день
    (на Урале так набралось 259 записей из 298 за один день разгона).

    Гейты нарочно избыточны — ни один не должен позволить проштамповать
    запись, чью карточку никто не читал:
    - только трек «Иски банка» (в основной картотеке дела заводятся со СТРОКИ
      выдачи, без карточки — там штамповать нечего);
    - есть блок `import` с разбираемой датой `at` (её и ставим: сегодняшняя
      дата соврала бы, дав делу лишнюю неделю тишины);
    - `events` непусты — доказательство, что карточка читалась. Гейт
      КОНСЕРВАТИВНЫЙ, а не исчерпывающий: свежий материал (М-номер) приходит
      с карточки без таблицы движения, и штампа не получит — не беда, у него
      своя ветка `material_pending_promotion` ВЫШЕ чтения `last_checked_at`;
    - штампа ещё нет (у воскрешённых из архива он сохранён и может быть
      старым — перетирать его нельзя, там ждёт force-parse по 21 дню).

    ⚠️ Жить может ТОЛЬКО внутри `migrate_stages`: у трека split-хранение, и на
    диске `first_instance.events` пуст у ВСЕХ записей (события лежат в
    `cases_bank_events.json`, склеивает `load_bank_json`). Вынесенная в
    отдельный скрипт над файлом, эта миграция проштампует ноль записей и
    будет выглядеть молчаливо сломанной.
    """
    stamped = 0
    for case in cases:
        if not is_bank_plaintiff_track(case):
            continue
        imp = case.get("import")
        if not isinstance(imp, dict):
            continue
        fi = case.get("first_instance") or {}
        if fi.get("last_checked_at") or not fi.get("events"):
            continue
        day = (imp.get("at") or "")[:10]
        try:
            date.fromisoformat(day)
        except ValueError:
            continue
        fi["last_checked_at"] = day
        # Первый парс прогоном у этих дел ещё не случился — маркер сохраняет
        # его признак ровно так же, как у новых записей (см. first_card_parse).
        fi["intake_card_parse"] = True
        stamped += 1
    return stamped


def migrate_stages(cases: list[dict]) -> int:
    """Идемпотентная миграция существующих дел под новую state-machine:
    - first_instance + appeal_filed_date → awaiting_appeal
    - appeal с опубликованным актом или заседанием старше 30 дней без акта
      → cassation_watch
    - cassation_watch с зарегистрированной касс. жалобой → cassation_pending
    Возвращает число мигрированных дел."""
    # ⚠️ Ремонт особого порядка отмены заочного решения идёт ПЕРВЫМ: бэкфилл
    # decision_date ниже вернул бы снятую дату из hearing_date, а цикл
    # advance_case_stage — стадию awaiting_appeal. Правила, а не список
    # номеров: миграция идемпотентна и отработает на территории Урала.
    repair_vacated_default_judgments(cases)
    # Дата проверки делам трека, заведённым до 14.08.2026 (карточку читал
    # импорт, а штампа не ставил) — иначе force-parse перебивает у них
    # smart-skip целиком. Идемпотентно: второй прогон уже не находит записей.
    _intake_stamped = migrate_intake_checked_stamp(cases)
    if _intake_stamped:
        log.info(
            f"Иски банка: дата проверки при заведении проставлена: "
            f"{_intake_stamped}"
        )
    # Идемпотентно заполняем initial_bank_role у дел, где его ещё нет.
    # Используется в дайджесте, чтобы показать «было: <роль>» при изменении
    # bank_role (напр. банк исключён из ответчиков → стал «Третье лицо»).
    for case in cases:
        if not case.get("initial_bank_role") and case.get("bank_role"):
            case["initial_bank_role"] = case["bank_role"]
    # Идемпотентный бэкфилл замороженной даты решения. Ветка записи в
    # update_active_cases срабатывает только на ЭМИТЕ fi_resolved, а дела,
    # импортированные уже решёнными (import_bank_registry ставит
    # resolved_emitted=True без эмита), через неё никогда не пройдут.
    # Сегодня бэкфилл точен: hearing_date у решённых дел ещё равен настоящей
    # дате решения (дрейфа ни в одном деле нет) — чем позже, тем хуже.
    for case in cases:
        fi = case.get("first_instance") or {}
        if fi.get("decision_date"):
            continue
        if (fi.get("status") or "").strip() in ("Решено", "Возвращено"):
            if fi.get("hearing_date"):
                fi["decision_date"] = fi["hearing_date"]
    # Срок для возражений на апел. жалобу: идемпотентный штамп из движения
    # жалобы. Здесь он берётся из УЖЕ СОХРАНЁННЫХ appeal_events (свежие FI-цикл
    # вольёт позже и проштампует сам) — цель прохода анти-паводковая: сроки,
    # лежащие в данных со времён до этой правки, дайджест задним числом не
    # объявит. Тот же приём, что resolved_emitted у make_bank_entry.
    # ⚠️ Засеваем ТОЛЬКО ИСТЁКШИЙ срок. Посев любого срока подряд глушил бы
    # ровно то, ради чего правка и делается: у дела, заведённого авто-подхватом
    # с живым сроком, эмит-блок ещё не отработал, `objections_emitted` пуст —
    # безусловный посев на следующем прогоне закрыл бы дедлайн навсегда.
    _today = date.today()
    for case in cases:
        fi = case.get("first_instance") or {}
        due = stamp_objections_deadline(fi)
        if (due and "objections_emitted" not in fi
                and date.fromisoformat(due) < _today):
            fi["objections_emitted"] = due
    # Возврат копии заочного решения (09.08.2026): посев эмит-флага делам,
    # где возврат уже случился ДО появления события fi_default_copy_returned
    # (2-4427/2026, 2-2803/2026) — иначе первый прогон после деплоя объявил
    # бы месячной давности факты новостями. Флаг — ЗНАЧЕНИЕМ (датой), в
    # унисон с эмит-блоком FI-цикла; уже стоящий (в т.ч. от эмита) не трогаем.
    for case in cases:
        fi = case.get("first_instance") or {}
        if "default_copy_returned_emitted" in fi:
            continue
        _dj = bank_default_judgment_info(fi)
        if _dj["default_copy_returned"]:
            fi["default_copy_returned_emitted"] = (
                _dj.get("default_copy_returned_date") or "unknown"
            )
    # Вручение копии заочного решения (13.08.2026): посев эмит-флага делам,
    # где вручение уже в истории событий, — зеркало посева возврата копии
    # выше, тот же анти-паводок для события fi_default_copy_served.
    for case in cases:
        fi = case.get("first_instance") or {}
        if "default_copy_served_emitted" in fi:
            continue
        _dj = bank_default_judgment_info(fi)
        if _dj["default_judgment"] and _dj["default_copy_served_date"]:
            fi["default_copy_served_emitted"] = _dj["default_copy_served_date"]
    # ⚠️ Календарные события трека («вступило в силу», «ИЛ просрочен») тут НЕ
    # сеются намеренно. Расчётная дата силы пересекает «сегодня» без изменения
    # данных карточки, а migrate_stages идёт на загрузке — РАНЬШЕ календарного
    # прохода collect_bank_calendar_events (runs.py): вечный посев «est уже
    # наступила» глушил бы не только бэклог деплоя, но и ВСЕ будущие события
    # (в день наступления даты посев успевал бы первым). Анти-паводок у них —
    # эпоха фичи config.BANK_CALENDAR_EVENTS_SINCE внутри самого прохода.
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


def _fi_court_key(c: dict) -> str:
    """Ключ суда 1-й инст. записи для дедупа: домен, фолбэк — резолв
    короткого имени через реестр региона. Пустая строка = суд неизвестен
    (легаси-стаб) — такой ключ матчит любой."""
    from court_monitor.courts import match_fi_court_by_short_name

    fi = c.get("first_instance") or {}
    dom = (fi.get("court_domain") or "").strip().lower()
    if dom:
        return dom
    cfg = match_fi_court_by_short_name(fi.get("court") or "")
    return cfg.domain if cfg else ""


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
    `first_instance`/`awaiting_appeal`. Хозяин подбирается С УЧЁТОМ СУДА
    (`_fi_court_key`): номера дел не уникальны между судами — 2-813/2026
    12.08.2026 жил сразу в трёх судах bank-трека, и слияние поперёк судов
    склеило бы чужие дела. Пустой ключ суда (легаси-стаб) матчит любой —
    прежнее лечение стабов без суда сохраняется.

    Сливаем сироту в хозяина: дозаполняем `appeal` хозяина, не перезаписывая
    уже заполненные поля. Стадию хозяина переводим в `appeal`. Сироту
    удаляем из `cases` in-place.

    Группы без сирот — не аномалия, а голые коллизии номеров между судами
    (у bank-трека это норма): сливать нечего, WARNING не печатаем.

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

        if not orphans:
            # Голая коллизия номеров между судами — сливать нечего.
            continue

        # Хозяина подбираем с учётом суда (см. докстроку): при одном сироте
        # отбрасываем хозяев заведомо чужих судов.
        if len(orphans) == 1:
            orph_key = _fi_court_key(cases[orphans[0]])
            eligible = [
                i for i in owners
                if not orph_key
                or not _fi_court_key(cases[i])
                or _fi_court_key(cases[i]) == orph_key
            ]
        else:
            eligible = owners

        if len(orphans) == 1 and len(eligible) == 1:
            orph = cases[orphans[0]]
            host = cases[eligible[0]]
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
        else:
            courts = " | ".join(
                ((cases[i].get("first_instance") or {}).get("court") or "?")
                for i in orphans + owners
            )
            log.warning(
                f"Дедуп: {base} неоднозначная группа "
                f"(сирот: {len(orphans)}, хозяев: {len(owners)}; "
                f"суды: {courts}) — не трогаю"
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
    не-discovery записей ≥2 — не трогаем; при этом несколько не-discovery
    записей на один УИД — ШТАТНО, а не коллизия: УИД принадлежит делу 1-й
    инст., а апел. производств (33-…) у него может быть несколько — основная
    жалоба + частная / возвращённая и переподанная (разбор 12.08.2026: все
    5 «подозрительных» УИД оказались такими парами). Поэтому WARNING
    печатаем только когда в группе есть discovery-двойник, которому не
    выбрать якорь; без двойника сливать нечего и предупреждать не о чем.
    Одиночные discovery (без anchor) тоже оставляем — это настоящие
    находки кассации.

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
            if losers and len(anchors) > 1:
                log.warning(
                    f"Дедуп касс./УИД: {uid} — discovery-двойник при "
                    f"{len(anchors)} не-discovery записях, якорь "
                    f"неоднозначен — не трогаю"
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
      «Единоличное рассмотрение (без вызова лиц…). …» → «единоличное рассмотрение»
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
    if t.startswith("единоличн"):
        return "единоличное рассмотрение"
    return "заседание"


# ── Smart-skip парсинга ─────────────────────────────────────────────────────
# Маркеры из текста последнего события, при которых известна дата следующей
# активности и парсинг до неё бессмысленен. Синхронизированы с фронтовой
# логикой nextDateLabel в app.js:272-298.
_HEARING_MARKERS_RX = re.compile(
    r"(судебное\s+заседани|предварительн\w*\s+(?:судебн\w*\s+)?заседани|"
    r"единоличн\w*\s+рассмотрени|"
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


def known_future_date_skip(
    block: dict, stage: str, today: date, case_id: str = "?",
    max_ahead_days: int | None = None,
) -> tuple[bool, str] | None:
    """Известная будущая дата в блоке → причина пропуска, иначе None.

    Два источника, ровно как раньше жило внутри `should_skip_case`:
    - кассация — ЯВНЫЕ поля `hearing_date`/`suspended_until` (DD.MM.YYYY):
      events карточки 7kas держат текст в `name`, а не в `text`, и
      `get_next_planned_date` по ним не срабатывает вовсе;
    - все стадии — последняя запись `events` через `get_next_planned_date`.
      Для кассации это ФОЛБЭК: явные поля проверяются первыми и, не дав
      скипа, пропускают дальше (порядок сохранён с 08.07.2026).

    «>=» у заседания: день N скипаем, парсим с N+1 (решение юриста
    08.07.2026) — акт «единоличного рассмотрения», опубликованный в сам
    день N, подхватится следующим прогоном.

    `max_ahead_days` — ГОРИЗОНТ ДОВЕРИЯ: дата дальше него известной не
    считается (дело 2-1725/2026: суд назначил заседание на 20.08.2029 при
    соседних событиях от 29.07.2026 — опечатка в годе). Проверяется у КАЖДОГО
    кандидата отдельно, с падением на следующий источник: абсурдный
    `hearing_date` кассации не должен глушить законный `suspended_until` и
    событийный фолбэк.

    Вынесено в отдельную функцию 14.08.2026: та же проверка нужна ВЫШЕ, в
    ветке force-parse — известная будущая дата сильнее 21-дневной страховки
    (решение юриста). Две копии правил разъехались бы молча.
    """
    def _trusted(d: date) -> bool:
        return max_ahead_days is None or (d - today).days <= max_ahead_days

    if stage == "cassation":
        hd_raw = (block.get("hearing_date") or "").strip()
        m_hd = _DATE_DDMMYYYY_RX.match(hd_raw)
        if m_hd:
            try:
                hd = date(int(m_hd.group(3)), int(m_hd.group(2)), int(m_hd.group(1)))
                if hd >= today and _trusted(hd):
                    return True, f"future_hearing({hd.strftime('%d.%m.%Y')})"
            except ValueError:
                log.debug(
                    f"  {case_id}: невалидная hearing_date кассации {hd_raw!r}"
                )
        su_raw = (block.get("suspended_until") or "").strip()
        m_su = _DATE_DDMMYYYY_RX.match(su_raw)
        if m_su:
            try:
                su = date(int(m_su.group(3)), int(m_su.group(2)), int(m_su.group(1)))
                if su > today and _trusted(su):
                    return True, f"suspended_until({su.strftime('%d.%m.%Y')})"
            except ValueError:
                log.debug(
                    f"  {case_id}: невалидная suspended_until кассации {su_raw!r}"
                )

    planned, kind = get_next_planned_date(block.get("events") or [])
    if planned and planned >= today and _trusted(planned):
        ymd = planned.strftime("%d.%m.%Y")
        if kind == "hearing":
            return True, f"future_hearing({ymd})"
        return True, f"suspended_until({ymd})"
    return None


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
        # Известная будущая дата СИЛЬНЕЕ страховки force-parse (решение
        # юриста 14.08.2026): пока заседание впереди, читать карточку не за
        # чем — суд до заседания её не меняет, а 21-дневная страховка
        # заставляла перечитывать дела с заседанием через два месяца.
        # ⚠️ Цена решения шире, чем кажется: до дня заседания невидим не
        # только перенос на более РАННЮЮ дату, но и любые досудебные движения
        # карточки — объединение дел, частная жалоба, обеспечительные меры,
        # отказ от иска, мировое, замена стороны. При законном заседании через
        # 90 дней новость опаздывает на все 90 (прежде — максимум на 21).
        # Юрист это принял; фиксируем целиком, чтобы решение не пересматривали
        # по половине картины.
        # ⚠️ ГОРИЗОНТ ДОВЕРИЯ (KNOWN_DATE_TRUST_DAYS) передаётся ТОЛЬКО здесь,
        # и асимметрия с проверкой ниже — суть правки. Дата из карточки суда
        # может быть любой: 2-1725/2026 приехало с заседанием 20.08.2029 при
        # соседних событиях от 29.07.2026, и безусловное доверие похоронило бы
        # дело на три года. Ограничить горизонт ВНУТРИ хелпера (то есть и для
        # проверки ниже) нельзя: тогда такое дело перестанет скипаться вовсе и
        # будет читаться КАЖДЫЙ прогон — 366 раз в год вместо 17. Так же оно
        # возвращается ровно к прежнему страховочному ритму «раз в 21 день» и
        # само подхватит исправленную судом дату.
        known = known_future_date_skip(
            block, stage, today, case_dict.get("id", "?"),
            max_ahead_days=config.KNOWN_DATE_TRUST_DAYS,
        )
        if known:
            return known
        return False, ""

    # Трек «Иски банка»: решённое дело опрашивается раз в BANK_WRIT_CHECK_DAYS
    # (7 дн) на всём пост-решенческом отрезке — до расчётного вступления в силу
    # недельный опрос ловит раннюю апел. жалобу ответчика, после — ищет
    # исполнительный лист во вкладке «ИСПОЛНИТЕЛЬНЫЕ ЛИСТЫ». Ежедневный парс
    # бесполезен: события там штучные. До решения дело живёт обычным
    # smart-skip по датам заседаний (ветки ниже).
    # Присоединённое к другому делу живёт в том же недельном ритме, но по своей
    # причине: статус карточки у него остаётся «В производстве», и без явной
    # ветки оно опрашивалось бы КАЖДЫМ прогоном (заседание в прошлом) все 30
    # дней своего окна. Ждём тут единственного — отмены объединения.
    if (stage == "first_instance" and is_bank_plaintiff_track(case_dict)
            and (block.get("merged") or fi_is_merged(block))):
        days_since = (today - last_checked).days
        if days_since < config.BANK_WRIT_CHECK_DAYS:
            return True, f"merged_weekly({days_since}d/{config.BANK_WRIT_CHECK_DAYS}d)"
        return False, ""
    # Особый порядок отмены заочного решения: суд обязан рассмотреть заявление
    # за 10 дней (ст. 240 ГПК), и недельный ритм ИЛ тут не годится. Но и
    # парсить каждым прогоном нельзя: событие «Рассмотрение заявления об отмене
    # заочного решения» не матчится ни _SESSION_START_RX, ни _HEARING_MARKERS_RX,
    # поэтому get_next_planned_date по нему вернёт None и общие ветки ниже
    # дело не притормозят (78 заочных дел в ХМАО, на Урале сотни). Скипаем
    # ровно до дня заседания по заявлению.
    if (stage == "first_instance" and is_bank_plaintiff_track(case_dict)
            and default_cancellation_pending(block, today)):
        _cancel_hd = parse_date(
            default_cancellation_state(block, today)["hearing_date"])
        if _cancel_hd and _cancel_hd.date() > today:
            return True, f"default_cancel_hearing({_cancel_hd.date().isoformat()})"
        return False, ""
    if (stage == "first_instance" and is_bank_plaintiff_track(case_dict)
            and (block.get("status") or "").strip() in ("Решено", "Возвращено")):
        days_since = (today - last_checked).days
        if days_since < config.BANK_WRIT_CHECK_DAYS:
            return True, f"writ_weekly({days_since}d/{config.BANK_WRIT_CHECK_DAYS}d)"
        return False, ""

    known = known_future_date_skip(block, stage, today,
                                   case_dict.get("id", "?"))
    if known:
        return known

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
    m = re.match(r"writ_weekly\((\d+)d/(\d+)d\)$", reason)
    if m:
        return (f"иск банка решён, ждём ИЛ/жалобу — опрос раз в "
                f"{m.group(2)} дн. (прошло {m.group(1)})")
    m = re.match(r"merged_weekly\((\d+)d/(\d+)d\)$", reason)
    if m:
        return (f"дело присоединено к другому — опрос раз в "
                f"{m.group(2)} дн. (прошло {m.group(1)})")
    m = re.match(r"default_cancel_hearing\((.+)\)$", reason)
    if m:
        return (f"заявление об отмене заочного решения — заседание "
                f"{m.group(1)} ещё впереди")
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
    """Чинит дела 1-й инст. с ложным терминальным статусом при назначенном
    будущем заседании (см. fi_resolution_contradicted_by_future_hearing).
    Кроме «Решено» покрывает «Возвращено» (с 29.07.2026): возврат, отменённый
    частной жалобой, оживляет ту же карточку — назначаются заседания, а
    статус и флаги resolved_emitted/termination_emitted остались бы
    терминальными, и настоящее решение по существу никогда не попало бы в
    дайджест. Идемпотентно, как migrate_stages: на повторных прогонах ничего
    не меняет. hearing_date НЕ трогаем — фронт покажет предстоящее заседание."""
    n = 0
    for case in cases:
        if case.get("current_stage") not in ("first_instance", "cassation_watch"):
            continue
        fi = case.get("first_instance") or {}
        status = (fi.get("status") or "").strip()
        if status not in ("Решено", "Возвращено"):
            continue
        # Проба со статусом «Решено»: сама проверка противоречия
        # (будущее session-событие без «Вынесено решение по делу»)
        # одинакова для обоих терминальных статусов.
        probe = dict(fi, status="Решено")
        if fi_resolution_contradicted_by_future_hearing(probe, today):
            fi["status"] = "В производстве"
            fi["result"] = ""
            fi["result_date"] = ""
            fi["resolved_emitted"] = False
            fi["termination_emitted"] = False
            n += 1
            log.info(
                f"  {case.get('id', '?')}: снят ложный «{status}» "
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
