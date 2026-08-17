# -*- coding: utf-8 -*-
"""Программный рендер дайджеста (generate_template_digest) и его строительные
блоки: сводная строка, секционные разделители, сокращение категорий,
рендер пересказа/фрагмента акта, «тихий» дайджест без изменений.

⚠ Отступы строк дайджеста настраивал юрист: строки одного дела ПОДРЯД,
пустая строка — только между разными делами. Не менять вёрстку.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta

from court_monitor import config
from court_monitor.config import log
from court_monitor.courts import (
    CASSATION_COURT, case_card_url, case_link_html, fi_card_url,
)
from court_monitor.regions import get_region
from court_monitor.digest.postprocess import _close_open_tags
from court_monitor.lifecycle import (
    bank_side_outcome, bank_side_outcome_fi,
    _is_event_text_in_result_field, _fi_return_reason_for_render,
)
from court_monitor.parsing import (
    CASSATION_OUTCOME_RU, cassation_review_label, cassation_terminated_label,
)
from court_monitor.storage import load_json
from court_monitor.textutil import (
    escape_html, shorten_party_name, shorten_court_name, shorten_bailiff_name,
    _bare_case_number, parties_short, parse_date, case_id_uid, ROLE_GENITIVE,
    plural_ru, appellant_role_words, fi_closure_reason, extract_motive_part,
)

def _bank_in_parties(plaintiff: str, defendant: str) -> bool:
    """True если «Сбербанк» явно упомянут в любой из сторон.

    Используется для правила БАНК В ХВОСТЕ: когда банк уже виден в сторонах,
    хвост «банк — Истец/Ответчик» в строке дайджеста избыточен. Хвост нужен
    ТОЛЬКО для редкого случая «банк = Третье лицо» (в сторонах не фигурирует).
    """
    s = ((plaintiff or "") + " " + (defendant or "")).lower()
    return "сбербанк" in s


# ФИО физлиц в дайджесте: False — инициалы («Подкин Н.С.», выбор юриста
# 06.07.2026 — полные ФИО раздували строки сторон на 2-3 физлицах), True —
# полные ФИО (прежнее поведение). НЕ влияет на payload LLM-пересказов
# мотивировок: туда имена идут полными (keep_fio_full=_DIGEST_FIO_FULL), чтобы LLM
# сматчил стороны с текстом акта.
_DIGEST_FIO_FULL = False


# Шумовые сегменты текста события (через «. »): тип сессии, время, зал,
# даты. Всё, что остаётся сверх них, — содержательная часть события
# (исход, приостановление, экспертиза и т.п.). Зал бывает с этажом впереди
# («3 этаж зал № 8», Свердловский облсуд), «единоличное рассмотрение
# (без вызова лиц, участвующих в деле)» — апелляционный тип сессии оттуда же.
# Голое число — остаток «каб. 12»: точка внутри ячейки режет её при split
# на «каб» и «12».
_EVENT_NOISE_SEGMENT_RE = re.compile(
    r"^(?:"
    r"судебное заседание|предварительное судебное заседание|"
    r"единоличное рассмотрение(?:\s*\([^)]*\))?|"
    r"подготовка дела(?:\s*\(собеседование\))?|беседа|собеседование|"
    r"(?:\d+\s*этаж[а-яё]*[.,]?\s*)?зал\b.*|каб(?:инет)?\.?\s*№?\s*\d*|"
    r"\d{1,2}:\d{2}|\d{2}\.\d{2}\.\d{4}|\d+"
    r")$",
    re.IGNORECASE,
)


# Приостановление/возобновление производства. Ключевой процессуальный
# статус (ст. 216 ГПК: экспертиза, розыск ответчика и т.п.) — дайджест
# обязан назвать его прямо, а не показывать сырой текст события и тем
# более не «Заседание назначено на <прошедшую дату>» (кейс 33-3793/2026,
# 02.07.2026: производство приостановлено из-за экспертизы).
_SUSPENDED_RE = re.compile(
    r"производств[а-яё]*[^.]*приостановлен[а-яё]*"
    r"|приостановлен[а-яё]*[^.]*производств[а-яё]*",
    re.IGNORECASE,
)
_RESUMED_RE = re.compile(
    r"производств[а-яё]*[^.]*возобновлен[а-яё]*"
    r"|возобновлен[а-яё]*[^.]*производств[а-яё]*",
    re.IGNORECASE,
)
# «Единоличное рассмотрение (без вызова лиц, участвующих в деле)» —
# апелляционная форма без заседания (Свердловский облсуд): времени нет
# (ГАС ставит 00:00), явка не нужна — рендерим особой строкой без времени.
_SOLO_SESSION_RE = re.compile(r"^\s*единоличн\w*\s+рассмотрени", re.IGNORECASE)


def _strip_noise_segments(event: str) -> list[str]:
    """Содержательные сегменты склейки события: шум (тип сессии, время,
    зал/этаж, даты — _EVENT_NOISE_SEGMENT_RE) отброшен. Общий код причины
    приостановления и 📌-цитаты."""
    segs: list[str] = []
    for seg in (event or "").split(". "):
        seg = seg.strip().rstrip(".").lstrip(". ")
        if not seg or _EVENT_NOISE_SEGMENT_RE.match(seg):
            continue
        segs.append(seg)
    return segs


def _suspension_reason_from_event(event: str) -> str:
    """Причина приостановления из хвоста текста события.

    «Судебное заседание. 15:00. Зал 142. Производство по делу
    приостановлено. НАЗНАЧЕНИЕ СУДОМ ЭКСПЕРТИЗЫ. 02.07.2026»
    → «назначение судом экспертизы» (шумовые сегменты — время/зал/даты —
    отброшены, КРИЧАЩИЙ РЕГИСТР ГАС «Правосудие» приведён к строчным).
    Пустая строка — если содержательного хвоста нет."""
    m = _SUSPENDED_RE.search(event or "")
    if not m:
        return ""
    reason = "; ".join(_strip_noise_segments((event or "")[m.end():]))
    if reason and reason.isupper():
        reason = reason.lower()
    return reason


def _event_quote(event: str, ev_date: str) -> str:
    """Цитата события для «📌 …»: шумовые сегменты склейки (тип сессии,
    время, зал, даты — в т.ч. хвостовая «Дата размещения», из-за которой
    цитата врала датой) срезаны, дата события добавлена скобками из details.
    Голый анонс (одни шумовые сегменты) → первый сегмент (тип сессии);
    совсем пусто → сырой текст (fail-open). КРИЧАЩИЙ РЕГИСТР ГАС
    опускается, первая буква — заглавная."""
    segs = _strip_noise_segments(event)
    if not segs:
        head = (event or "").split(". ")[0].strip().rstrip(".")
        segs = [head] if head else []
    quote = "; ".join(segs)
    if quote and quote.isupper():
        quote = quote.lower()
        quote = quote[:1].upper() + quote[1:]
    if quote and ev_date and ev_date not in quote:
        quote += f" ({ev_date})"
    return quote or (event or "")


def _event_text_is_informative(event: str) -> bool:
    """True, если текст события несёт содержание сверх «голого» анонса
    заседания (тип сессии + время + зал + даты).

    «Судебное заседание. 14:30. 03.07.2026» → False (голый анонс);
    «Судебное заседание. 15:00. Зал 142. Производство по делу
    приостановлено. НАЗНАЧЕНИЕ СУДОМ ЭКСПЕРТИЗЫ. 02.07.2026» → True.

    Нужен секции 5.2: содержательное событие показываем текстом
    («📌 …»), а не строкой «Заседание назначено на <вчера>» — иначе
    состоявшееся заседание с исходом маскируется под будущее
    (A/B 03.07.2026, дело 33-3793/2026: оба варианта потеряли
    приостановление производства и экспертизу)."""
    for seg in (event or "").split(". "):
        seg = seg.strip().rstrip(".")
        if not seg:
            continue
        if not _EVENT_NOISE_SEGMENT_RE.match(seg):
            return True
    return False


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


# Заголовок подсекции — единственные строки блока, чей <b>-текст кончается
# на «(N):» («📅 Изменения (3):», «📥 Новые дела (1):» …). Строки дел/событий
# такой формы не имеют, поэтому по ней надёжно отличаем заголовок раздела.
_SUBSECTION_COUNT_HEADER_RE = re.compile(r'\(\d+\):</b>\s*$')


def _air_after_subsection_headers(block: list[str]) -> list[str]:
    """Пустая строка после каждого заголовка раздела «… (N):».

    Просьба юриста 06.07.2026: воздух не только между разделами (⸻), но и
    сразу после самого заголовка раздела — чтобы он не «слипался» с первым
    делом. Одиночная пустая строка (не двойная — двойные только перед
    заголовками крупных секций).
    """
    out: list[str] = []
    for i, ln in enumerate(block):
        out.append(ln)
        if _SUBSECTION_COUNT_HEADER_RE.search(ln):
            nxt = block[i + 1] if i + 1 < len(block) else ""
            if nxt != "":
                out.append("")
    return out


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
                       cass_discovered: list[dict] | None = None,
                       bank_changes: list[dict] | None = None) -> str:
    """Сводка-саммари одной строкой: N новых дел, M заседаний, K итогов.

    Формат 06.07.2026 (просьба юриста): слова вместо аббревиатур
    («+1 нов. апелл.» → «📥 1 новая апелляция»), разделитель « · »,
    склонение по числу через plural_ru. Счётчики и состав частей прежние.
    """
    parts = []
    if fi_new_cases:
        n = len(fi_new_cases)
        parts.append(
            f"📥 {n} {plural_ru(n, 'новое дело', 'новых дела', 'новых дел')}"
            " в 1-й инст."
        )
    if new_cases:
        n = len(new_cases)
        parts.append(
            f"📥 {n} "
            + plural_ru(n, 'новая апелляция', 'новые апелляции',
                        'новых апелляций')
        )
    if cass_discovered:
        n = len(cass_discovered)
        parts.append(
            f"📥 {n} "
            + plural_ru(n, 'новая кассация', 'новые кассации',
                        'новых кассаций')
        )
    # Мостик stage_transitions из дайджеста убран: дело и так попадает
    # в 5.1 «Новые дела апелляции», отдельная пометка юристу не нужна.
    events = sum(1 for ch in changes
                 if "new_event" in ch["type"] or "hearing_new" in ch["type"])
    # «Ложные» new_result, содержащие текст события в поле «Результат»,
    # из подсчёта вырезаем — иначе template-сводка показывает лишние
    # «N суд. акт.». Парсер их теперь не создаёт, но защищаемся от
    # старых контекстов (--replay-last) и legacy JSON.
    results = sum(
        1 for ch in changes
        if "new_result" in ch["type"]
        and not _is_event_text_in_result_field(
            (ch.get("details") or {}).get("result", "")
        )
    )
    acts = sum(1 for ch in changes if "new_act" in ch["type"])
    postponed = sum(1 for ch in changes if "hearing_postponed" in ch["type"])
    to_fi_rules = sum(1 for ch in changes if "appeal_to_fi_rules" in ch["type"])
    # «Голый» status_change (без других визуальных типов) — рендерится
    # в 5.2 строкой «статус: X → Y», считаем и в сводке.
    app_status = sum(
        1 for ch in changes
        if "status_change" in ch["type"]
        and not (set(ch["type"]) & {"new_event", "hearing_new",
                                    "hearing_postponed", "new_result",
                                    "new_act", "appeal_to_fi_rules"})
    )
    if events:
        parts.append(
            f"📌 {events} "
            + plural_ru(events, 'событие', 'события', 'событий')
            + " в апелляции"
        )
    if postponed:
        parts.append(
            f"🔁 {postponed} "
            + plural_ru(postponed, 'отложение', 'отложения', 'отложений')
            + " в апелляции"
        )
    if to_fi_rules:
        parts.append(
            f"⚠ {to_fi_rules} "
            + plural_ru(to_fi_rules, 'переход', 'перехода', 'переходов')
            + " к правилам 1-й инст."
        )
    if results:
        parts.append(
            f"⚖️ {results} "
            + plural_ru(results, 'итог апелляции', 'итога апелляции',
                        'итогов апелляции')
        )
    if acts:
        parts.append(
            f"📄 {acts} "
            + plural_ru(acts, 'текст акта', 'текста актов', 'текстов актов')
            + " апелляции"
        )
    if app_status:
        parts.append(
            f"🔄 {app_status} "
            + plural_ru(app_status, 'смена статуса', 'смены статуса',
                        'смен статуса')
            + " в апелляции"
        )
    # Передачи по подсудности банк-трека — в общий счётчик «➡️ … по
    # подсудности»: тело дайджеста показывает их в секции «Иски банка», и
    # сводка «1 дело» при двух передачах в теле дезориентировала (выпуск
    # 07.08.2026: 2-822/2026 в основном треке + 2-8088/2026 в банковском).
    bank_transfers = sum(
        1 for ch in (bank_changes or [])
        if "fi_returned" in ch["type"]
        and ((ch.get("details") or {}).get("termination_kind") or "")
        == "transfer"
    )
    if fi_changes or bank_transfers:
        fi_changes = fi_changes or []
        fi_hearings = sum(
            1 for ch in fi_changes
            if ("fi_hearing_new" in ch["type"]
                or "fi_hearing_next" in ch["type"]
                or "fi_hearing_postponed" in ch["type"]
                or "fi_hearing_recess" in ch["type"]
                # Заседание по решённому делу — тоже заседание 1-й инст.
                # (расходы/индексация; двойного счёта с банк-треком нет —
                # track-записи изъяты из fi_changes до сводки).
                or "fi_post_decision_hearing" in ch["type"])
        )
        fi_status = sum(1 for ch in fi_changes if "fi_status_change" in ch["type"])
        fi_acts = sum(1 for ch in fi_changes if "fi_act_published" in ch["type"])
        fi_finals = sum(1 for ch in fi_changes if "fi_final_event" in ch["type"])
        fi_motivs = sum(
            1 for ch in fi_changes if "fi_motivirovka_emitted" in ch["type"]
        )
        fi_resolved_n = sum(
            1 for ch in fi_changes
            if "fi_resolved" in ch["type"]
            # возврат материала считаем изменением, а не решением (см. 3.5)
            and "fi_returned" not in ch["type"]
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
        # Процессуальные завершения — по видам: «возвратов исков» не должно
        # покрывать передачу по подсудности (это разные вещи для юриста).
        fi_term_kinds: dict[str, int] = {}
        for ch in fi_changes:
            if "fi_returned" not in ch["type"]:
                continue
            kind = ((ch.get("details") or {}).get("termination_kind")
                    or "returned")
            fi_term_kinds[kind] = fi_term_kinds.get(kind, 0) + 1
        fi_returns = fi_term_kinds.get("returned", 0)
        fi_refusals = fi_term_kinds.get("refusal", 0)
        fi_transfers = fi_term_kinds.get("transfer", 0) + bank_transfers
        fi_merges = fi_term_kinds.get("merged", 0)
        fi_cass_filed = sum(
            1 for ch in fi_changes if "fi_cassation_filed" in ch["type"]
        )
        fi_sent_cass = sum(
            1 for ch in fi_changes if "fi_sent_to_cassation" in ch["type"]
        )
        fi_accepted = sum(
            1 for ch in fi_changes if "fi_accepted_no_hearing" in ch["type"]
        )
        # Особый порядок отмены заочного решения (ст. 237-243 ГПК). В сводку
        # выносим два состояния, меняющие судьбу взыскания: подано заявление
        # и решение отменено. Заседание по заявлению и отказ в отмене строку
        # в 3.2 «Изменения» уже имеют — сводку ими не удлиняем.
        fi_default_cancels = sum(
            1 for ch in fi_changes
            if "fi_default_cancellation_filed" in ch["type"]
        )
        fi_default_vacated = sum(
            1 for ch in fi_changes
            if "fi_default_judgment_vacated" in ch["type"]
        )
        # fi_bank_role_changed в сводку осознанно НЕ выносим: смена роли —
        # редкий служебный признак, строка в 3.2 «Изменения» его уже несёт.
        if fi_hearings:
            parts.append(
                f"📅 {fi_hearings} "
                + plural_ru(fi_hearings, 'заседание', 'заседания',
                            'заседаний')
                + " в 1-й инст."
            )
        if fi_restarts:
            parts.append(
                f"🔄 {fi_restarts} "
                + plural_ru(fi_restarts, 'рассмотрение с начала',
                            'рассмотрения с начала',
                            'рассмотрений с начала')
            )
        if fi_resolved_n:
            parts.append(
                f"⚖️ {fi_resolved_n} "
                + plural_ru(fi_resolved_n, 'решение', 'решения', 'решений')
                + " 1-й инст."
            )
        if fi_returns:
            parts.append(
                f"🔚 {fi_returns} "
                + plural_ru(fi_returns, 'возврат иска', 'возврата исков',
                            'возвратов исков')
            )
        if fi_refusals:
            parts.append(
                f"🔚 {fi_refusals} "
                + plural_ru(fi_refusals, 'отказ в принятии иска',
                            'отказа в принятии исков',
                            'отказов в принятии исков')
            )
        if fi_transfers:
            parts.append(
                f"➡️ {fi_transfers} "
                + plural_ru(fi_transfers, 'дело', 'дела', 'дел')
                + " — по подсудности"
            )
        if fi_merges:
            parts.append(
                f"🔗 {fi_merges} "
                + plural_ru(fi_merges, 'дело', 'дела', 'дел')
                + " — присоединено к другим"
            )
        if fi_appeals_filed:
            parts.append(
                f"📨 {fi_appeals_filed} "
                + plural_ru(fi_appeals_filed, 'апел. жалоба',
                            'апел. жалобы', 'апел. жалоб')
            )
        if fi_cass_filed:
            parts.append(
                f"📨 {fi_cass_filed} "
                + plural_ru(fi_cass_filed, 'касс. жалоба', 'касс. жалобы',
                            'касс. жалоб')
            )
        if fi_sent_cass:
            parts.append(
                f"📤 {fi_sent_cass} "
                + plural_ru(fi_sent_cass, 'дело', 'дела', 'дел')
                + " — в касс. суд"
            )
        if fi_accepted:
            parts.append(
                f"📥 {fi_accepted} "
                + plural_ru(fi_accepted, 'дело принято', 'дела принято',
                            'дел принято')
                + " к производству"
            )
        if fi_default_cancels:
            parts.append(
                f"🌙 {fi_default_cancels} "
                + plural_ru(fi_default_cancels,
                            'заявление об отмене заочного',
                            'заявления об отмене заочных',
                            'заявлений об отмене заочных')
            )
        if fi_default_vacated:
            parts.append(
                f"⚠️ {fi_default_vacated} "
                + plural_ru(fi_default_vacated, 'заочное решение отменено',
                            'заочных решения отменено',
                            'заочных решений отменено')
            )
        if fi_finals:
            parts.append(
                f"🏁 {fi_finals} "
                + plural_ru(fi_finals, 'финальное событие',
                            'финальных события', 'финальных событий')
                + " 1-й инст."
            )
        if fi_acts:
            parts.append(
                f"📄 {fi_acts} "
                + plural_ru(fi_acts, 'решение изготовлено',
                            'решения изготовлено', 'решений изготовлено')
                + " (1-я инст.)"
            )
        if fi_motivs:
            parts.append(
                f"📄 {fi_motivs} "
                + plural_ru(fi_motivs, 'мотивировка готова',
                            'мотивировки готовы', 'мотивировок готово')
                + " (1-я инст.)"
            )
        if fi_act_texts:
            parts.append(
                f"📄 {fi_act_texts} "
                + plural_ru(fi_act_texts, 'текст решения',
                            'текста решений', 'текстов решений')
                + " (1-я инст.)"
            )
        if fi_status:
            parts.append(
                f"🔄 {fi_status} "
                + plural_ru(fi_status, 'смена статуса', 'смены статуса',
                            'смен статуса')
                + " (1-я инст.)"
            )
    if cass_changes:
        cass_acts = sum(1 for ch in cass_changes if "new_act" in ch["type"])
        cass_outcomes = sum(1 for ch in cass_changes if "outcome_change" in ch["type"])
        cass_reviews = sum(1 for ch in cass_changes if "review_result_change" in ch["type"])
        cass_news = sum(1 for ch in cass_changes if "new_cassation" in ch["type"])
        cass_hearings = sum(
            1 for ch in cass_changes if "cass_hearing_scheduled" in ch["type"]
        )
        if cass_news:
            parts.append(
                f"📥 {cass_news} "
                + plural_ru(cass_news, 'касс. карточка', 'касс. карточки',
                            'касс. карточек')
            )
        if cass_hearings:
            parts.append(
                f"📅 {cass_hearings} "
                + plural_ru(cass_hearings, 'заседание кассации',
                            'заседания кассации', 'заседаний кассации')
            )
        if cass_reviews:
            parts.append(
                f"🔍 {cass_reviews} "
                + plural_ru(cass_reviews, 'итог изучения жалобы',
                            'итога изучения жалоб', 'итогов изучения жалоб')
            )
        if cass_outcomes:
            parts.append(
                f"🏁 {cass_outcomes} "
                + plural_ru(cass_outcomes, 'итог кассации',
                            'итога кассации', 'итогов кассации')
            )
        if cass_acts:
            parts.append(
                f"📄 {cass_acts} "
                + plural_ru(cass_acts, 'касс. акт', 'касс. акта',
                            'касс. актов')
            )
    # Трек «Иски банка» — одна агрегатная строка (детализация по типам
    # раздула бы сводку: секция и так компактная, одна строка на дело).
    if bank_changes:
        # Свёрнутые заведения (разгон территории) считаем ОТДЕЛЬНОЙ частью:
        # вложить второй разделитель внутрь части нельзя — части сводки уже
        # склеены через «·». Без свёртки строка прежняя посимвольно.
        bank_detailed, bank_folded = split_bank_intake_fold(bank_changes)
        n = len(bank_detailed)
        # ИЛ в сводке — раздельно по типам: «🧾 ИЛ» — на исполнение решения,
        # «🛡» — обеспечительные (арест). kind в details ставит эмиссия.
        enf_n = interim_n = 0
        for ch in bank_detailed:
            if "fi_writ_issued" not in (ch.get("type") or []):
                continue
            writs = (ch.get("details") or {}).get("writs") or []
            if any(w.get("kind") == "interim" for w in writs):
                interim_n += 1
            if any(w.get("kind") != "interim" for w in writs):
                enf_n += 1
        # n = число ДЕЛ (записей секции, у дела может быть несколько
        # событий) — прежняя подпись «N событий» врала (разбор 13.08.2026).
        if n:
            part = (
                f"🏦 {n} " + plural_ru(n, "дело", "дела", "дел")
                + " с событиями по искам банка"
            )
            tails = []
            if enf_n:
                tails.append(f"🧾 {enf_n} ИЛ")
            if interim_n:
                tails.append(f"🛡 {interim_n} обеспечит.")
            if tails:
                part += f" ({', '.join(tails)})"
            parts.append(part)
        if bank_folded:
            f_n = len(bank_folded)
            parts.append(
                f"🆕 {f_n} "
                + plural_ru(f_n, "новый иск", "новых иска", "новых исков")
                + " банка заведено"
            )
    return " · ".join(parts) if parts else "без изменений"


def short_category_chain(cat: str) -> str:
    """Категория для дайджеста: последний сегмент после «→».

    «Споры… → Жилищные → Иные жилищные споры» → «Иные жилищные споры».
    Короткие категории (без стрелок) возвращаются как есть. Применяется
    ДО подачи категории в LLM-контекст и в template-рендер — юрист
    просил видеть только итоговый сегмент, без полной цепочки.
    """
    if not cat:
        return cat
    # Унифицируем разные варианты стрелок (обычная, длинная, ASCII).
    normalized = cat.replace("->", "→").replace("→", "→")
    if "→" not in normalized:
        return cat
    parts = [p.strip() for p in normalized.split("→") if p.strip()]
    return parts[-1] if parts else cat


def category_short(cat: str, *, truncate: bool = True) -> str:
    """Сокращённое название категории для компактного вывода.

    truncate=False — НЕ резать незнакомую категорию по ~20 символам с «…»:
    вернуть последний сегмент целиком (для секций, где есть место на полную
    формулировку — просьба юриста 07.07.2026: «об освобождении…» терял смысл).
    Маппинг известных категорий в короткую форму («кредит») работает в обоих
    режимах — на вход всегда подаётся уже вычлененный последний сегмент.
    """
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
    if not truncate:
        return cat
    # Если не нашли — обрезаем по границе слова (~20 символов), чтобы не
    # получать обрывки вида «иные, связанные с на…».
    if len(cat) > 22:
        head = cat[:21]
        space = head.rfind(" ")
        head = head[:space] if space > 0 else cat[:20]
        return head.rstrip(" ,;:—–-") + "…"
    return cat


def _fmt_hearing_dt(date: str, time: str) -> str:
    """Дата+время заседания: «ДД.ММ.ГГГГ в ЧЧ:ММ» (предлог «в» перед временем,
    просьба юриста 09.07.2026, единообразие со всеми секциями; в кассации так
    уже было). Без времени — только дата. «00:00» — заглушка ГАС «времени
    нет» (единоличное рассмотрение без вызова лиц), скрываем: полуночных
    заседаний не бывает. Не экранирует (это делают вызовы)."""
    date = (date or "").strip()
    time = (time or "").strip()
    if time in ("00:00", "0:00"):
        time = ""
    if not date:
        return ""
    return f"{date} в {time}" if time else date


def _hearing_type_paren(d: dict) -> str:
    """Скобочный хвост « (беседа)» для строк заседаний 3.2 (13.08.2026).

    Тип печатаем только НЕ-родовой: «заседание» — дефолт, скобки не нужны.
    Скобки вместо «назначено {тип} на» — род существительного («назначена
    беседа», «назначена подготовка дела») сломал бы единый шаблон строки."""
    ht = (d.get("hearing_type") or "").strip()
    return f" ({escape_html(ht)})" if ht and ht != "заседание" else ""


# ── Основная логика обновления ───────────────────────────────────────────────

def _act_summary_or_excerpt_with_kind(
    act_text: str,
    case_meta: dict,
    *,
    summarizer,
    max_excerpt_len: int = 500,
) -> tuple[str, str]:
    """Текст мотивировки для дайджеста + признак его происхождения.

    Возвращает (text, kind):
      - ("…", "summary") — LLM-пересказ от `summarizer` (рендерится с
        маркером «<b>Почему:</b>» — на нём держится attach_act_analyses
        и разбор акта в drawer'е карточки дела);
      - ("…", "excerpt") — обрезанный сырой фрагмент (маркер «Почему»
        НЕ ставим: «Почему» из сырого куска текста выглядело бы враньём);
      - ("", "") — act_text пуст.

    text уже прошёл `escape_html`, готов к вставке в HTML.
    """
    text = (act_text or "").strip()
    if not text:
        return "", ""
    if summarizer is not None:
        try:
            summary = summarizer(text, case_meta=case_meta)
        except Exception as e:
            log.warning(f"act_summarizer упал: {e}")
            summary = None
        if summary:
            return escape_html(summary), "summary"
    if len(text) > max_excerpt_len:
        text = text[:max_excerpt_len].rstrip() + "…"
    return escape_html(text), "excerpt"


def _render_act_summary_or_excerpt(
    act_text: str,
    case_meta: dict,
    *,
    summarizer,
    max_excerpt_len: int = 500,
) -> str:
    """Совместимость: только текст, без признака (см. *_with_kind)."""
    return _act_summary_or_excerpt_with_kind(
        act_text, case_meta,
        summarizer=summarizer, max_excerpt_len=max_excerpt_len,
    )[0]


def load_last_meaningful_digest() -> dict | None:
    """Прочитать `last_digest.json` и вернуть payload последнего непустого
    дайджеста — или None, если такого нет.

    Используется в ветках «no-changes», чтобы добавить в сообщение блок
    «Предыдущий дайджест от …». Защита от self-reference: если payload
    помечен `is_empty=True` или html содержит маркеры «no-changes»,
    возвращается None.
    """
    try:
        if not os.path.exists(config.LAST_DIGEST_PATH):
            return None
        with open(config.LAST_DIGEST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        log.warning(f"Не удалось прочитать {config.LAST_DIGEST_PATH}: {exc}")
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
        return header + f'\n\n<a href="{config.DASHBOARD_URL}">📊 Дашборд</a>'
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


# Административное перемещение «дело передано/сдано в архив» — клерикальное
# событие ПОСЛЕ вынесения решения (само решение уже уходит в «⚖️ Вынесенные
# решения»). Юристу в дайджесте не нужно (просьба 07.07.2026): гасим его
# fi_final_event и в теле, и в сводке. Флаг/стадия в JSON не затронуты —
# фильтруется только доставка (как эхо- и стародатный фильтры).
_FI_ARCHIVE_EVENT_RE = re.compile(r"в\s+архив\b", re.IGNORECASE)


def _strip_archive_final_events(fi_changes: list[dict]) -> list[dict]:
    """Убрать fi_final_event «дело передано в архив» из fi_changes.

    Возвращает новый список: у change'ей с архивным fi_final_event тип
    вычищается (change с прочими типами — например, fi_resolved — остаётся
    и рендерится в своей секции); change, где кроме архива ничего не было,
    выбрасывается целиком. Оригинальные dict'ы не мутируем — контекст
    переиспользуется на replay.
    """
    out: list[dict] = []
    for ch in fi_changes:
        types = ch.get("type") or []
        ev = (ch.get("details") or {}).get("event", "") or ""
        if "fi_final_event" in types and _FI_ARCHIVE_EVENT_RE.search(ev):
            kept = [t for t in types if t != "fi_final_event"]
            if not kept:
                continue
            ch = {**ch, "type": kept}
        out.append(ch)
    return out


def _is_motiv_event(ev: str) -> bool:
    """Событие про мотивировку («Изготовлено мотивированное решение…»,
    «Мотивированное решение изготовлено…», «Составлено мотивированное…»).

    Исключение из _strip_echoed_terminal_events: такое событие — не пересказ
    исхода, а самостоятельный факт (мотивировка готова, можно забирать).
    Нарочно шире, чем нормализация в рендере (та требует слова
    «изготовлено»): не распознанная рендером формулировка уйдёт сырой
    ⚖️-строкой, но факт не потеряется. Проверка подстрокой без порядка
    слов — как у остальных детекторов мотивировки (runs.py
    final_already_covers_motiv, рендер fi_final_event); порядкозависимый
    регексп здесь молча терял факт на «Мотивированное решение
    изготовлено…» (ревью 29.07.2026)."""
    return "мотивированн" in (ev or "").lower()


def _strip_echoed_terminal_events(fi_changes: list[dict]) -> list[dict]:
    """Убрать fi_final_event, который лишь пересказывает уже показанный исход.

    Если у дела в одном прогоне есть исход — fi_resolved (уедет в 3.5
    «Вынесенные решения») или fi_returned (строка «🔚 иск возвращён: …» в
    3.2) — то сырая строка события карточки повторяет его другими словами, и
    дело печатается дважды. Инцидент 9-336/2026 (29.07.2026, Урал): возврат
    иска пришёл и как «⚖️ Решение вопроса о принятии иска… Возвращение
    иска… ДЕЛО НЕ ПОДСУДНО…» в «Изменениях», и как «Итог: возвращено» в
    «Вынесенных решениях». Решение юриста: гасить сырую строку.

    Исключение — «Изготовлено мотивированное решение …»: рендер нормализует
    её в отдельный полезный факт (мотивировка готова, можно идти забирать),
    он остаётся рядом с решением.

    Гасим ДО сводки и тела — как _strip_archive_final_events: сводка считает
    fi_finals по этому же списку, и правка только в теле дала бы «🏁 1
    финальное событие», под которым в секции ничего нет. Оригинальные dict'ы
    не мутируем — контекст переиспользуется на replay.
    """
    out: list[dict] = []
    for ch in fi_changes:
        types = ch.get("type") or []
        if "fi_final_event" in types and (
            "fi_resolved" in types or "fi_returned" in types
        ):
            ev = (ch.get("details") or {}).get("event", "") or ""
            if not _is_motiv_event(ev):
                kept = [t for t in types if t != "fi_final_event"]
                if not kept:
                    continue
                ch = {**ch, "type": kept}
        out.append(ch)
    return out


def _merge_motiv_into_resolved(fi_changes: list[dict]) -> list[dict]:
    """Склеить «решение + мотивировка» одного дела в одну запись 3.5.

    До 09.08.2026 fi_resolved уходил в 3.5 «Вынесенные решения», а
    мотивировочное событие того же прогона (fi_motivirovka_emitted или
    мотивировочный fi_final_event — исключение эхо-фильтра выше) — отдельной
    строкой в 3.2 «Изменения»: дело печаталось ДВАЖДЫ с полным списком
    сторон (кейс Урала 2-484/2026: 32 соответчика в обеих строках, вопрос
    юриста 09.08.2026). Мотив-типы вычищаются, дата уезжает в
    details["motiv_merged_date"] — рендер 3.5 допишет «Мотивировка
    изготовлена …» в ту же запись. Применяется к ОСНОВНОМУ треку ПОСЛЕ
    отделения bank_changes: в банк-секции дело и так одна строка (дубля
    нет), а склейка потеряла бы факт мотивировки. Комбинации с
    fi_act_text_published / fi_returned не трогаем — у них своя вёрстка.
    Оригинальные dict'ы не мутируем (replay); идемпотентно — после прохода
    мотив-типов при fi_resolved не остаётся. Зовётся ДО build_summary_line:
    счётчики «🏁 финальных»/«📄 мотивировок» отражают склейку сами.
    """
    out: list[dict] = []
    for ch in fi_changes:
        types = ch.get("type") or []
        if (ch.get("track") == "plaintiff_light"
                or "fi_resolved" not in types
                or "fi_returned" in types
                or "fi_act_text_published" in types):
            out.append(ch)
            continue
        d = ch.get("details") or {}
        motiv_date: str | None = None
        kept = list(types)
        if "fi_motivirovka_emitted" in kept:
            motiv_date = (d.get("motivirovka_date") or "").strip()
            kept = [t for t in kept if t != "fi_motivirovka_emitted"]
        ev_raw = (d.get("event") or "").strip()
        if "fi_final_event" in kept and ev_raw and _is_motiv_event(ev_raw):
            if not motiv_date:
                motiv_date = _motiv_date_from_event(ev_raw, d).strip()
            kept = [t for t in kept if t != "fi_final_event"]
        if motiv_date is None:
            out.append(ch)
            continue
        out.append({**ch, "type": kept,
                    "details": {**d, "motiv_merged_date": motiv_date}})
    return out


# Виды процессуального завершения 1-й инстанции. Ключ — details
# ["termination_kind"] (ставит lifecycle.fi_termination_details). Старые
# контексты (--replay-last до 29.07.2026) ключа не несут — фолбэк на
# «возврат», прежнюю формулировку.
_FI_TERMINATION_LABELS = {
    "returned": "🔚 иск возвращён",
    "refusal": "🔚 отказано в принятии иска",
    "transfer": "➡️ дело передано по подсудности",
    "merged": "🔗 дело присоединено к другому делу",
}


# Процессуальные закрытия, идущие каналом fi_resolved (статус «Решено» их
# и приносит): решения по существу тут НЕТ, «вынесено решение: прекращено»
# дезориентировало юриста (разбор 12.08.2026, дела 2-3974/2026 и 2-6650/2026).
# Ключ — details["verdict_label"], значение — шапка строки вместо
# «вынесено решение»; причина добывается fi_closure_reason из raw_result.
_FI_CLOSURE_HEADS = {
    "прекращено": "производство по делу прекращено",
    "оставлено без рассмотрения": "иск оставлен без рассмотрения",
}


# Слова-роли из карточки: вкладка «Обжалование» в поле «Заявитель» отдаёт
# «ИСТЕЦ»/«ОТВЕТЧИК» вместо имени. В дайджесте показываем НАИМЕНОВАНИЕ лица,
# а не статус (просьба юриста 07.07.2026: «апеллянт: Истец Истец» → имя лица).
_BARE_ROLE_WORDS = {"Истец", "Ответчик", "Третье лицо", "Иное лицо"}

# Настоящее имя/наименование: хотя бы одно слово из ≥2 букв ВНЕ скобок.
# Поле «Заявитель» карточки иногда отдаёт обрывок разметки — «(жалобы)»
# (дело 33-13721/2026, Свердловский облсуд): классификатор не считает это
# словом-ролью и сохраняет как имя, а дайджест печатал «(жалоба иного лица
# (жалобы))».
_NAME_HAS_WORD_RE = re.compile(r"[А-ЯЁа-яёA-Za-z]{2,}")


def _is_meaningful_appellant_name(name: str) -> bool:
    """True, если строка похожа на имя/наименование, а не на служебный
    обрывок карточки."""
    outside = re.sub(r"\([^)]*\)", " ", name or "")
    return bool(_NAME_HAS_WORD_RE.search(outside))


def _fi_appellant_display(role: str, name: str,
                          pl_disp: str, df_disp: str) -> str:
    """Наименование подателя жалобы (апелляц./кассац.) для строки дайджеста.

    Показываем ИМЯ лица, а не слово-статус, и один раз. Если карточка дала
    только слово-роль («Истец»/«Ответчик») — резолвим его в сторону дела по
    роли (истец → истец дела, ответчик → ответчик дела). pl_disp/df_disp уже
    прошли shorten_party_name + escape_html; настоящее имя экранируем здесь.
    Пустая строка — если ни имени, ни резолвимой роли нет (голое «третье/иное
    лицо»): статус в строке не показываем.
    """
    role = (role or "").strip()
    name = (name or "").strip()
    if not name or name in _BARE_ROLE_WORDS:
        if role == "Истец":
            return pl_disp
        if role == "Ответчик":
            return df_disp
        return ""
    return escape_html(name)


def _appeal_complaint_suffix(d: dict, pl_disp: str, df_disp: str) -> str:
    """« (жалоба ответчика Русских А.В.)» для строки «Вынесенных актов».

    Юристу важно, ПО ЧЬЕЙ жалобе рассмотрено дело (просьба 30.07.2026);
    стиль «(жалоба {род.} {имя})» — как в строке «Итог» кассации. Бинарный
    ярлык «Банк» — короткое «(жалоба банка)»; иначе роль + резолв имени
    через _fi_appellant_display. Пустые поля апеллянта эмит добирает из
    зеркала тихого бэкфилла (appeal.appellant*, см. runs.py) — апел.
    карточка подателя жалобы не публикует. Пустая строка — апеллянт
    неизвестен (старые контексты replay: все чтения через d.get())."""
    if (d.get("appellant") or "").strip() == "Банк":
        return " (жалоба банка)"
    role = (d.get("appellant_role") or "").strip()
    name = (d.get("appellant_name") or "").strip()
    resolved_from_role = False
    if name and (appellant_role_words(name) is not None
                 or not _is_meaningful_appellant_name(name)):
        # Слово-роль вместо имени (в т.ч. составное «ИСТЕЦ, ОТВЕТЧИК» или
        # голый «ПРЕДСТАВИТЕЛЬ» — classify_appellant_role сохраняет их в
        # short_name как есть) либо служебный обрывок «(жалобы)»: это не
        # наименование лица — печатать нельзя, резолвим только через роль.
        name = ""
    if not name:
        resolved_from_role = True
    who = _fi_appellant_display(role, name, pl_disp, df_disp)
    if resolved_from_role and "," in who:
        # Сторона — несколько лиц, а карточка дала лишь слово-роль: КТО из
        # соответчиков подал жалобу, неизвестно. Перечислить всех — домысел
        # (дело 33-5018/2026: три ответчика), печатаем одну роль.
        who = ""
    role_gen = ROLE_GENITIVE.get(role, "")
    if who and role_gen:
        return f" (жалоба {escape_html(role_gen)} {who})"
    if who:
        return f" (жалоба {who})"
    if role_gen:
        return f" (жалоба {escape_html(role_gen)})"
    return ""


def _is_motiv_made_event(event: str) -> bool:
    """Строгий детект «Изготовлено мотивированное решение…» для НОРМАЛИЗАЦИИ
    формулировки. Не путать с широким `_is_motiv_event` (эхо-фильтр,
    группировка): не распознанная здесь формулировка уйдёт сырой ⚖️-строкой,
    но факт не потеряется."""
    ev_low = (event or "").lower()
    return 'изготовлено' in ev_low and 'мотивированное решение' in ev_low


def _motiv_date_from_event(event: str, d: dict) -> str:
    """Дата мотивировки из текста события; фолбэк — details["event_date"]."""
    m = re.search(r'(\d{2}\.\d{2}\.\d{4})', event or '')
    return m.group(1) if m else (d.get('event_date') or '')


# ── Секция «Иски банка» (лёгкий трек, банк — истец) ──────────────────────────
# Компактный формат «одна строка на дело»: при масштабе пилота (сотни дел)
# полная вёрстка секции 3.2 не влезла бы в Telegram-бюджет (~7600 симв.).
# Секция рендерится ПОСЛЕДНЕЙ — при обрезке страдает первой, основная
# повестка выживает. Короткие подписи типов без деталей; типы с датой/итогом
# (заседания, решение, ИЛ) обогащаются в _bank_event_phrases.
_BANK_TYPE_LABELS = {
    "fi_hearing_recess": "⏸ перерыв в заседании",
    "fi_hearing_restart": "🔄 рассмотрение с начала",
    # fi_returned подписывается по виду завершения (_FI_TERMINATION_LABELS) —
    # см. _bank_event_phrases; здесь только фолбэк формы.
    "fi_returned": "🔚 иск возвращён",
    "fi_accepted_no_hearing": "📥 иск принят к производству",
    # Дело заведено авто-подхватом с выдачи суда (блок 3b прогона). Раньше
    # трек пополнялся только вручную и юрист сам знал, что добавил; теперь
    # картотека растёт сама — молчать об этом нельзя.
    "fi_bank_claim_registered": "🆕 иск банка взят на мониторинг",
    "fi_act_published": "📄 решение изготовлено",
    "fi_act_text_published": "📄 текст решения опубликован",
    "fi_motivirovka_emitted": "📄 мотивировка изготовлена",
    "fi_final_event": "⚖️ движение по делу",
    # fi_status_change — спец-ветка в _bank_event_phrases: рядом с
    # fi_resolved подавляется, одиночная выводится с деталями «X → Y».
    # Особый порядок отмены заочного решения (ст. 237-243 ГПК) — дело из трека
    # НЕ уходит: апелляционного хода у ответчика ещё нет (ст. 237 ч. 2).
    "fi_default_cancellation_filed": "🌙 подано заявление об отмене заочного решения",
    "fi_default_cancellation_hearing": "📅 заседание по заявлению об отмене",
    "fi_default_judgment_vacated": "⚠️ заочное решение отменено — дело рассматривается заново",
    "fi_default_cancellation_refused": "✅ в отмене заочного решения отказано",
    # Возврат копии запускает формулу ВС для срока вступления в силу — юрист
    # должен видеть его, а не вычислять по карточке (разбор 07.08.2026).
    "fi_default_copy_returned": "🌙 копия заочного решения возвратилась невручённой",
    "fi_objections_deadline_set": "⏳ установлен срок для возражений на жалобу",
    "fi_appeal_filed": "📨 апел. жалоба ответчика — дело уходит в общий трек",
    "fi_cassation_filed": "📨 касс. жалоба",
    "fi_sent_to_cassation": "📤 направлено в касс. суд",
    "fi_bank_role_changed": "ℹ️ роль банка изменилась",
    # Календарные события ожидания ИЛ (13.08.2026, collect_bank_calendar_
    # events в runs.py): наступление расчётной даты силы и алерт зависшего
    # листа. Ради листов трек и существует — раньше дайджест сообщал только
    # факт выдачи и молчал, пока лист не выдан.
    "fi_legal_force_reached": "✅ решение вступило в силу (расч.) — ожидаем ИЛ",
    "fi_writ_overdue": "⚠️ ИЛ не выдан после вступления в силу",
    # Заседание по решённому делу (индексация, расходы, отсрочка): гард
    # case_decided глушит обычный hearing-блок, этот тип — его законная
    # пост-решенческая ветка (только трек, только будущие даты).
    "fi_post_decision_hearing": "📅 заседание по решённому делу",
    # Парное к «возвратилась невручённой»: вручение запускает 7-дневный
    # срок на заявление об отмене (ст. 237 ГПК) и пересчёт даты силы.
    "fi_default_copy_served": "🌙 копия заочного решения вручена ответчику",
}


# Электронный ИД листа с числовым суффиксом («86RS0018#2-201/2026#6»):
# общий префикс + номер листа. Основа схлопывания пачек в дайджесте.
_WRIT_EID_SUFFIX_RE = re.compile(r'^(.*)#(\d+)$')


def _writ_suffix_ranges(nums: list[int]) -> str:
    """Номера листов диапазонами: [1..6] → «1–6», [4,6] → «4, 6»,
    [4,5,6,8,9] → «4–6, 8–9». Для фразы пачки однотипных ИЛ."""
    nums = sorted(set(nums))
    parts: list[str] = []
    i = 0
    while i < len(nums):
        j = i
        while j + 1 < len(nums) and nums[j + 1] == nums[j] + 1:
            j += 1
        parts.append(str(nums[i]) if i == j else f"{nums[i]}–{nums[j]}")
        i = j + 1
    return ", ".join(parts)


def _writ_numbers(writ: dict) -> str:
    """Номер(а) исполнительного листа одной строкой.

    Электронный ИД («86RS0004#2-7806/2026#1») и бумажный бланк
    («ФС № 039166358») — РАЗНЫЕ реквизиты одного листа, а не фолбэк друг для
    друга. Было `electronic_id or blank_number`: заполни суд обе колонки —
    бумажный номер молча пропал бы из Telegram. Зеркало фронта
    (buildWritsSectionHtml в app.js).
    """
    nums = [n for n in (
        (writ.get("electronic_id") or "").strip(),
        (writ.get("blank_number") or "").strip(),
    ) if n]
    return " · ".join(nums)


def _bank_hearing_time(d: dict) -> str:
    """Время заседания для банк-строки; «00:00» — заглушка ГАС «времени
    нет», скрываем (то же правило, что _fmt_hearing_dt в 3.2)."""
    ht = (d.get("hearing_time") or "").strip()
    return "" if ht in ("00:00", "0:00") else ht


# Куда возвращено дело при cassation_remanded — enum из cassation_remanded_to
# (parsing/cassation.py). Русская фраза для хвоста «Итог: … → …»; неизвестное
# значение хвоста не даёт.
_REMANDED_TO_RU = {
    "appeal": "в суд апелляционной инстанции",
    "first_instance": "в суд первой инстанции",
}


def _bank_event_phrases(ch: dict) -> list[str]:
    """Компактные фразы событий одного дела для секции «Иски банка»."""
    d = ch.get("details") or {}
    out: list[str] = []
    for t in ch.get("type") or []:
        if t == "fi_writ_issued":
            # Пачки однотипных листов схлопываются (просьба юриста
            # 10.08.2026: 2-201/2026 дало 6 фраз, различие — суффикс #N).
            # Группа = (kind, дата выдачи, получатель); схлопываем ТОЛЬКО
            # электронные ИД с общим префиксом и без бумажного бланка —
            # реквизит «ФС №…» терять нельзя (fail-open в прежний формат).
            writ_groups: dict[tuple, list[dict]] = {}
            writ_order: list[tuple] = []
            for w in d.get("writs") or [{}]:
                key = (w.get("kind") or "",
                       (w.get("issue_date") or "").strip(),
                       (w.get("recipient") or "").strip())
                if key not in writ_groups:
                    writ_groups[key] = []
                    writ_order.append(key)
                writ_groups[key].append(w)
            for key in writ_order:
                grp = writ_groups[key]
                kind, issue_date, rec = key
                suffixes: list[int] = []
                prefixes: set[str] = set()
                collapsible = len(grp) >= 2
                if collapsible:
                    for w in grp:
                        if (w.get("blank_number") or "").strip():
                            collapsible = False
                            break
                        m = _WRIT_EID_SUFFIX_RE.match(
                            (w.get("electronic_id") or "").strip())
                        if not m:
                            collapsible = False
                            break
                        prefixes.add(m.group(1))
                        suffixes.append(int(m.group(2)))
                    if len(prefixes) != 1:
                        collapsible = False
                if collapsible:
                    n = len(grp)
                    if kind == "interim":
                        ph = ("🛡 <b>выдано "
                              + f"{n} "
                              + plural_ru(n, 'обеспечительный лист',
                                          'обеспечительных листа',
                                          'обеспечительных листов')
                              + " (арест)</b>")
                    else:
                        ph = ("🧾 <b>выдано "
                              + f"{n} "
                              + plural_ru(n, 'исполнительный лист',
                                          'исполнительных листа',
                                          'исполнительных листов')
                              + "</b>")
                    if issue_date:
                        ph += f" {escape_html(issue_date)}"
                    # Первый лист — ПОЛНЫМ реквизитом (просьба юриста
                    # 10.08.2026: им оперируют копипастой), остальные —
                    # номерами диапазоном: «(…#1, №2–6)».
                    first_sfx = min(suffixes)
                    rest = sorted(set(suffixes) - {first_sfx})
                    full_first = f"{next(iter(prefixes))}#{first_sfx}"
                    ph += (f" ({escape_html(full_first)}"
                           + (f", №{_writ_suffix_ranges(rest)}" if rest else "")
                           + ")")
                    if rec:
                        ph += f" → {escape_html(shorten_bailiff_name(rec))}"
                    out.append(ph)
                    continue
                for w in grp:
                    # Тип листа различает дата выдачи (classify_writ_kind):
                    # обеспечительный (арест) выдаётся в начале дела, лист
                    # на исполнение — после вступления решения в силу.
                    if w.get("kind") == "interim":
                        ph = "🛡 <b>выдан обеспечительный лист (арест)</b>"
                    else:
                        ph = "🧾 <b>выдан исполнительный лист</b>"
                    if w.get("issue_date"):
                        ph += f" {escape_html(w['issue_date'])}"
                    num = _writ_numbers(w)
                    if num:
                        ph += f" ({escape_html(num)})"
                    rec_w = (w.get("recipient") or "").strip()
                    if rec_w:
                        ph += f" → {escape_html(shorten_bailiff_name(rec_w))}"
                    out.append(ph)
        elif t == "fi_writ_status_changed":
            for w in d.get("writ_status_changes") or []:
                # Номер обязателен именно здесь: у дела бывает несколько листов
                # одной даты в один ОСП, и без номера непонятно, КАКОЙ из них
                # отозван (Советский, 2-37/2026: #1 Возвращен, #2 Выдан).
                num = _writ_numbers(w)
                out.append(
                    "🧾 лист"
                    + (f" {escape_html(num)}" if num
                       else (f" {escape_html(w.get('issue_date', ''))}"
                             if w.get("issue_date") else ""))
                    + f": {escape_html(w.get('old_status') or '?')}"
                    + f" → <b>{escape_html(w.get('status') or '?')}</b>"
                )
        elif t == "fi_resolved":
            # Дата решения и заочность — контекст, без которого строка
            # дезориентирует (разбор 07.08.2026: суд объявляет решения
            # с лагом в недели). default_judgment в details опционален —
            # старые контексты replay живут без пометки.
            v = (d.get("verdict_label") or "").strip()
            dd = (d.get("decision_date") or "").strip()
            closure_head = _FI_CLOSURE_HEADS.get(v)
            if closure_head:
                # Прекращено / без рассмотрения — определение, не решение;
                # причина из «Результата» карточки (raw_result есть и в
                # старых контекстах — replay печатает исправленную строку).
                reason = fi_closure_reason(
                    d.get("raw_result", ""), d.get("last_event", ""))
                ph = (f"⚖️ <b>{closure_head}</b>"
                      + (f" {escape_html(dd)}" if dd else "")
                      + (f" ({escape_html(reason)})" if reason else ""))
            else:
                ph = ("⚖️ <b>вынесено решение</b>"
                      + (f" {escape_html(dd)}" if dd else "")
                      + (f": {escape_html(v)}" if v else ""))
            if d.get("default_judgment"):
                ph += " (🌙 заочное)"
            out.append(ph)
        elif t == "fi_act_text_published":
            # Своя ветка вместо генерик-подписи: «текст решения опубликован»
            # без даты решения и заочности читался как свежий исход, хотя
            # суд задним числом выложил тексты июньских решений (2-4427/2026:
            # заочное 03.06, копия не вручена — а строка молчала).
            dd = (d.get("decision_date") or "").strip()
            ph = ("📄 текст решения"
                  + (f" от {escape_html(dd)}" if dd else "")
                  + " опубликован")
            if d.get("default_judgment"):
                ph += " (🌙 заочное)"
            out.append(ph)
        elif t in ("fi_hearing_new", "fi_hearing_next"):
            # Время — как в основном треке (разбор 13.08.2026: являться-то по
            # времени); placeholder 00:00 суда скрываем, как в 3.2.
            hp = (d.get("hearing_date") or "").strip()
            ht = _bank_hearing_time(d)
            ph = ("📅 заседание"
                  + (f" <b>{escape_html(hp)}</b>" if hp else " назначено"))
            if hp and ht:
                ph += f" в {escape_html(ht)}"
            out.append(ph)
        elif t == "fi_hearing_postponed":
            hp = (d.get("hearing_date") or "").strip()
            ht = _bank_hearing_time(d)
            ph = ("🔁 отложено"
                  + (f" на <b>{escape_html(hp)}</b>" if hp else ""))
            if hp and ht:
                ph += f" в {escape_html(ht)}"
            out.append(ph)
        elif t == "fi_returned":
            # Вид процессуального завершения; фолбэк — прежняя форма
            # («возврат») для контекстов без termination_kind.
            kind = (d.get("termination_kind") or "returned").strip()
            label = _FI_TERMINATION_LABELS.get(
                kind, _FI_TERMINATION_LABELS["returned"])
            # Причину возврата в компакт-строке не печатаем (лимит Telegram),
            # но у присоединения «причина» — это номер дела-приёмника: без него
            # строка не отвечает на главный вопрос «куда смотреть дальше».
            reason = (d.get("return_reason") or "").strip()
            if kind == "merged" and reason:
                label += f": {escape_html(reason)}"
            # Дата события-завершения: суд заполняет «Результат» с лагом в
            # недели, и без даты юрист не понимает, когда это случилось
            # (2-8088/2026: передача 07.07, объявлена 07.08). Ключ
            # опционален — старые контексты replay без скобок.
            td = (d.get("termination_date") or "").strip()
            if td:
                label += f" ({escape_html(td)})"
            out.append(label)
        elif t == "fi_bank_claim_registered":
            ph = _BANK_TYPE_LABELS["fi_bank_claim_registered"]
            filed = (d.get("filing_date") or "").strip()
            if filed:
                ph += f" (подан {escape_html(filed)})"
            # Дело с уже поданной жалобой этим же прогоном уезжает в основную
            # картотеку — иначе юрист ищет его в лёгком треке и не находит.
            if d.get("left_track"):
                ph += " — по делу подана жалоба, дело в общем треке"
            out.append(ph)
        elif t == "fi_status_change":
            # Рядом с исходом («⚖️ вынесено решение», «🔚 иск возвращён»)
            # смена статуса — эхо того же факта (зеркало дедупа секции
            # 3.2, см. types_for_line); одиночная — с деталями: голая
            # подпись «смена статуса» юристу ничего не говорила
            # (фидбэк 30.07.2026).
            if any(x in (ch.get("type") or [])
                   for x in ("fi_resolved", "fi_returned")):
                continue
            old_s = (d.get("old_status") or "").strip()
            new_s = (d.get("new_status") or "").strip()
            if old_s or new_s:
                out.append(
                    f"ℹ️ статус: {escape_html(old_s)} → {escape_html(new_s)}"
                )
            else:
                # Старый контекст без деталей (--replay-last) — прежняя форма.
                out.append("ℹ️ смена статуса")
        elif t == "fi_default_copy_returned":
            dt = (d.get("copy_returned_date") or "").strip()
            out.append(_BANK_TYPE_LABELS["fi_default_copy_returned"]
                       + (f" ({escape_html(dt)})" if dt else ""))
        elif t == "fi_final_event":
            # Содержимое события вместо пустого генерика «движение по делу»
            # (разбор 07.08.2026, 2-5178/2026: за подписью пряталось
            # «Изготовлено мотивированное решение» — факт, от которого течёт
            # срок на апелляцию). Мотивировка нормализуется как в 3.2, прочее
            # цитируется коротко; пустой event — прежний генерик (replay).
            ev_raw = (d.get("event") or "").strip()
            if ev_raw and _is_motiv_made_event(ev_raw):
                md = _motiv_date_from_event(ev_raw, d)
                out.append("📄 мотивировка изготовлена"
                           + (f" ({escape_html(md)})" if md else ""))
            elif ev_raw:
                quote = _event_quote(ev_raw, d.get("event_date", ""))
                if len(quote) > 100:
                    quote = quote[:100].rsplit(" ", 1)[0].rstrip(",;:") + "…"
                out.append(f"⚖️ {escape_html(quote)}")
            else:
                out.append(_BANK_TYPE_LABELS["fi_final_event"])
        # ── Обогащение датами (разбор дайджеста 13.08.2026): половина строк
        # секции была голыми подписями. Каждая ветка replay-safe: ключа в
        # details нет (старый контекст) → прежняя подпись из _BANK_TYPE_LABELS.
        elif t == "fi_objections_deadline_set":
            # Дата срока — и есть новость; без неё строка не говорила ничего.
            due = (d.get("objections_due") or "").strip()
            out.append(
                f"⏳ возражения на жалобу — до <b>{escape_html(due)}</b>"
                if due else _BANK_TYPE_LABELS[t]
            )
        elif t == "fi_default_cancellation_filed":
            # От даты подачи течёт 10-дневный срок рассмотрения (ст. 240 ГПК).
            dt = (d.get("cancel_filed_date") or "").strip()
            out.append(_BANK_TYPE_LABELS[t]
                       + (f" ({escape_html(dt)})" if dt else ""))
        elif t == "fi_default_cancellation_hearing":
            # Дата заседания по заявлению — это явка представителя банка.
            dt = (d.get("cancel_hearing_date") or "").strip()
            out.append(
                f"📅 заседание по заявлению об отмене — <b>{escape_html(dt)}</b>"
                if dt else _BANK_TYPE_LABELS[t]
            )
        elif t in ("fi_default_judgment_vacated",
                   "fi_default_cancellation_refused"):
            # У отказа дата особенно важна: с неё открывается апелляционный
            # ход ответчика (ст. 237 ч. 2 ГПК) — юрист считает срок.
            dt = (d.get("cancel_outcome_date") or "").strip()
            out.append(_BANK_TYPE_LABELS[t]
                       + (f" ({escape_html(dt)})" if dt else ""))
        elif t == "fi_appeal_filed":
            dt = (d.get("appeal_filed_date") or "").strip()
            out.append(
                f"📨 апел. жалоба ответчика от {escape_html(dt)} — "
                "дело уходит в общий трек"
                if dt else _BANK_TYPE_LABELS[t]
            )
        elif t == "fi_cassation_filed":
            dt = (d.get("cassation_filed_date") or "").strip()
            out.append(_BANK_TYPE_LABELS[t]
                       + (f" ({escape_html(dt)})" if dt else ""))
        elif t == "fi_sent_to_cassation":
            dt = (d.get("sent_to_cassation_date") or "").strip()
            out.append(_BANK_TYPE_LABELS[t]
                       + (f" ({escape_html(dt)})" if dt else ""))
        elif t == "fi_motivirovka_emitted":
            # От мотивировки течёт месяц на апелляцию (ст. 321 ГПК) и
            # считается вступление в силу. Дата была только у ветки
            # fi_final_event — асимметрию убираем.
            dt = (d.get("motivirovka_date") or "").strip()
            out.append(_BANK_TYPE_LABELS[t]
                       + (f" ({escape_html(dt)})" if dt else ""))
        elif t == "fi_act_published":
            dt = (d.get("act_date") or "").strip()
            out.append(_BANK_TYPE_LABELS[t]
                       + (f" ({escape_html(dt)})" if dt else ""))
        elif t == "fi_hearing_recess":
            # details несут НОВУЮ дату продолжения (runs.py пишет hearing_*
            # для всех исходов классификации) — раньше она терялась.
            hp = (d.get("hearing_date") or "").strip()
            ht = _bank_hearing_time(d)
            if hp:
                ph = f"⏸ перерыв — продолжение <b>{escape_html(hp)}</b>"
                if ht:
                    ph += f" в {escape_html(ht)}"
                out.append(ph)
            else:
                out.append(_BANK_TYPE_LABELS[t])
        elif t == "fi_bank_role_changed":
            old_r = (d.get("old_role") or "").strip()
            new_r = (d.get("new_role") or "").strip()
            out.append(
                f"ℹ️ роль банка: {escape_html(old_r)} → {escape_html(new_r)}"
                if (old_r or new_r) else _BANK_TYPE_LABELS[t]
            )
        # ── Календарные события ожидания ИЛ + пост-решенческие (13.08.2026) ──
        elif t == "fi_legal_force_reached":
            dt = (d.get("legal_force_date") or "").strip()
            out.append(
                f"✅ решение вступило в силу (расч. {escape_html(dt)}) — "
                "ожидаем ИЛ"
                if dt else _BANK_TYPE_LABELS[t]
            )
        elif t == "fi_writ_overdue":
            days = d.get("overdue_days")
            dt = (d.get("legal_force_date") or "").strip()
            if days:
                ph = (f"⚠️ <b>ИЛ не выдан {days} дн. после вступления "
                      f"в силу</b>")
                if dt:
                    ph += f" (в силе с {escape_html(dt)})"
                out.append(ph)
            else:
                out.append(_BANK_TYPE_LABELS[t])
        elif t == "fi_post_decision_hearing":
            hp = (d.get("hearing_date") or "").strip()
            ht = _bank_hearing_time(d)
            topic = (d.get("hearing_topic") or "").strip()
            if hp:
                ph = (f"📅 заседание по решённому делу — "
                      f"<b>{escape_html(hp)}</b>")
                if ht:
                    ph += f" в {escape_html(ht)}"
                if topic:
                    ph += f" ({escape_html(topic)})"
                out.append(ph)
            else:
                out.append(_BANK_TYPE_LABELS[t])
        elif t == "fi_default_copy_served":
            # Срок на заявление об отмене — рабочие дни (ст. 107 ГПК),
            # как в расчёте BANK_DEFAULT_CANCEL_WORKDAYS.
            dt = (d.get("copy_served_date") or "").strip()
            ph = _BANK_TYPE_LABELS[t]
            if dt:
                ph += f" {escape_html(dt)}"
            ph += " (7 раб. дн. на заявление об отмене)"
            out.append(ph)
        else:
            label = _BANK_TYPE_LABELS.get(t)
            if label:
                out.append(label)
    return out


# Группы «по важности» для сортировки секции (решение юриста 17.08.2026,
# прежний порядок — 09.08.2026): исполнительные листы → решения и акты →
# иные → заседания (ближайшие сверху) → новые иски. Рабочая очередь юриста
# начинается с листов (ради них трек и существует), а заведение дел — фон,
# ему место в конце: там же встаёт строка-свёртка массового подхвата.
# До 09.08.2026 строки шли в порядке очереди обработки прогона и не
# читались вовсе (разбор дайджеста 07.08.2026).
_BANK_GROUP_WRITS = 0
_BANK_GROUP_DECISIONS = 1
# «Иные» — ДЕФОЛТ функции ниже, своего набора типов у группы нет: сюда
# падают завершения (возврат/отказ в принятии/передача/присоединение —
# решение юриста 17.08.2026), сроки возражений, отмена заочного, жалобы,
# смена статуса, немотивировочный fi_final_event.
_BANK_GROUP_OTHER = 2
_BANK_GROUP_HEARINGS = 3
_BANK_GROUP_INTAKE = 4
# ⚠️ Словарь, а не кортеж: «иные» сидят в СЕРЕДИНЕ порядка, и `len(...)`
# индексом группы больше не работает. Индексы держим именованными
# константами — новая группа в конце не должна сдвигать «иные».
_BANK_GROUP_ORDER = {
    # Календарные события ожидания ИЛ — в группе листов: «вступило в силу»
    # и «лист завис» суть этапы той же цепочки «решение → сила → выдача».
    _BANK_GROUP_WRITS: frozenset({
        "fi_writ_issued", "fi_writ_status_changed",
        "fi_legal_force_reached", "fi_writ_overdue"}),
    _BANK_GROUP_DECISIONS: frozenset({
        "fi_resolved", "fi_act_text_published", "fi_act_published",
        "fi_motivirovka_emitted"}),
    _BANK_GROUP_HEARINGS: frozenset({
        "fi_hearing_new", "fi_hearing_next", "fi_hearing_postponed",
        "fi_hearing_recess", "fi_hearing_restart",
        "fi_post_decision_hearing"}),
    _BANK_GROUP_INTAKE: frozenset({
        "fi_bank_claim_registered", "fi_accepted_no_hearing"}),
}


def _bank_change_group(ch: dict) -> int:
    """Индекс группы «по важности»; у дела с несколькими типами — старшая.

    Мотивировочный fi_final_event (детект по details["event"]) — та же
    группа, что решения: содержательно это «мотивировка изготовлена».
    ⚠️ Гард сравнивает с группой РЕШЕНИЙ, а не с нулём: нулевая группа —
    листы, и дело, получившее в одном прогоне и лист, и мотивировку,
    уехало бы из листов в решения.
    """
    types = ch.get("type") or []
    # ⚠️ Сначала СОБИРАЕМ совпавшие группы и только потом берём старшую:
    # «иные» сидят в середине порядка, и min() с дефолтом клампил бы
    # заседания и новые иски (индексы больше «иных») в саму группу «иные».
    matched = [i for i, grp in _BANK_GROUP_ORDER.items()
               if any(t in grp for t in types)]
    best = min(matched) if matched else _BANK_GROUP_OTHER
    if (best > _BANK_GROUP_DECISIONS and "fi_final_event" in types
            and _is_motiv_event((ch.get("details") or {}).get("event") or "")):
        best = _BANK_GROUP_DECISIONS
    return best


def split_bank_intake_fold(
    bank_changes: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Разделить банк-секцию на подробные строки и свёрнутые «заведения».

    Возвращает `(детализируемые, свёрнутые)`. Свёртываются только записи, где
    `fi_bank_claim_registered` — ЕДИНСТВЕННЫЙ тип: дело, которое тем же
    прогоном получило решение или лист, обязано печататься подробно. Дела с
    `details["left_track"]` тоже остаются подробно — это единственный сигнал,
    что искать дело надо уже не в лёгком треке.

    Порог — `config.BANK_INTAKE_DIGEST_FOLD` (0 = не сворачивать), условие
    «больше порога». Разгон Урала 14.08.2026: 116 одинаковых строк «взят на
    мониторинг» раздули дайджест до 60 КБ и утопили настоящие события.

    ⚠️ Один источник правды для рендера И для линтера (`lint.py`,
    `_expected_number_alternatives`) — у свёрнутых дел номера в HTML не будет,
    и без общего хелпера линтер объявил бы все 116 потерянными: дайджест-
    паводок просто переехал бы в 🩺-алерт. Тем же хелпером гейтится
    `llm._collect_case_numbers` (валидатор полировщика).
    """
    limit = config.BANK_INTAKE_DIGEST_FOLD
    if limit <= 0:
        return list(bank_changes), []
    foldable = [
        ch for ch in bank_changes
        if (ch.get("type") or []) == ["fi_bank_claim_registered"]
        and not (ch.get("details") or {}).get("left_track")
    ]
    if len(foldable) <= limit:
        return list(bank_changes), []
    folded_ids = {id(ch) for ch in foldable}
    detailed = [ch for ch in bank_changes if id(ch) not in folded_ids]
    return detailed, foldable


def _bank_intake_fold_line(folded: list[dict]) -> str:
    """Строка-свёртка. Без номера дела (иначе её посчитает счётчик секции
    `_check_section_counters`) и без ссылки — «📊 Дашборд» и так в футере."""
    courts = {(ch.get("court") or "").strip() for ch in folded} - {""}
    tail = (f" в {len(courts)} судах" if len(courts) > 1
            else (f" ({shorten_court_name(next(iter(courts)))})" if courts else ""))
    return (f"🆕 <b>заведено {len(folded)} "
            f"{plural_ru(len(folded), 'новый иск', 'новых иска', 'новых исков')} "
            f"банка</b>{tail} — список на дашборде")


def bank_act_why_eligible(ch: dict) -> bool:
    """Положен ли банк-делу пересказ «Почему» (13.08.2026, решение юриста).

    Только fi_act_text_published с текстом и исходом ПРОТИВ банка:
    bank_outcome ∉ {"", "в пользу банка"} — bank_side_outcome_fi даёт ровно
    5 значений, и предикат покрывает отказ/частичное/прекращение с учётом
    роли. Полные удовлетворения не пересказываем — мотивировка шаблонна.
    Общий гейт рендера банк-секции и attach_act_analyses в runs.py
    (второй должен молчать там, где молчит первый — иначе в drawer
    утекала бы строка события под видом «AI анализа»)."""
    if "fi_act_text_published" not in (ch.get("type") or []):
        return False
    d = ch.get("details") or {}
    if not (d.get("act_text") or "").strip():
        return False
    return (d.get("bank_outcome") or "").strip() not in ("", "в пользу банка")


def _bank_track_block(bank_changes: list[dict], *,
                      act_summarizer=None) -> list[str]:
    """Строки секции «Иски банка»: заголовок + одна строка на дело.

    Дела отсортированы по группам важности (_BANK_GROUP_ORDER), между
    группами пустая строка; внутри группы порядок исходный (стабильная
    сортировка), заседания — по дате, ближайшие сверху. Подзаголовков у
    групп нет осознанно: счётчик «ИСКИ БАНКА (N)» сверяет линтер
    (_check_section_counters — строка с номером = дело), новые заголовки
    с (N) пришлось бы синхронизировать с ним, а Telegram-лимит и так
    режет секцию первой.

    act_summarizer (13.08.2026, решение юриста): у fi_act_text_published
    с исходом ПРОТИВ банка (bank_outcome ∉ {"", "в пользу банка"} —
    bank_side_outcome_fi даёт ровно 5 значений, и предикат покрывает
    отказ/частичное/прекращение с учётом роли) вторым абзацем печатается
    LLM-пересказ «Почему». Полные удовлетворения не пересказываем —
    мотивировка шаблонна, а секция и так самая длинная.
    """
    detailed, folded = split_bank_intake_fold(bank_changes)
    if folded:
        log.info(
            f"Дайджест: свёрнуто {len(folded)} заведений исков банка "
            f"(порог {config.BANK_INTAKE_DIGEST_FOLD})"
        )

    def _sort_key(pair: tuple[int, dict]) -> tuple[int, float, int]:
        idx, ch = pair
        grp = _bank_change_group(ch)
        ts = 0.0
        if grp == _BANK_GROUP_HEARINGS:
            hd = parse_date(
                ((ch.get("details") or {}).get("hearing_date") or "").strip()
            )
            ts = hd.timestamp() if hd else float("inf")
        return (grp, ts, idx)

    ordered: list[dict | str] = [ch for _, ch in
                                 sorted(enumerate(detailed), key=_sort_key)]
    if folded:
        # Свёрнутая строка — на месте группы «новые иски» (с 17.08.2026 она
        # последняя, т.е. свёртка закрывает секцию): порядок «по важности»
        # и разделители ⸻ отрабатывает тот же цикл ниже.
        pos = len([ch for ch in ordered
                   if _bank_change_group(ch) < _BANK_GROUP_INTAKE])
        ordered.insert(pos, _bank_intake_fold_line(folded))
    # Счётчик заголовка = число ПОДРОБНЫХ дел (линтер считает строки с
    # номерами). Все дела свёрнуты — заголовок вовсе без (N): «(0)» рядом со
    # строкой «заведено 116» читается как поломка, а секцию без счётчика
    # _check_section_counters штатно пропускает.
    block = [f"🏦 <b>ИСКИ БАНКА ({len(detailed)}):</b>" if detailed
             else "🏦 <b>ИСКИ БАНКА:</b>", ""]
    prev_grp: int | None = None
    for ch in ordered:
        if isinstance(ch, str):
            if prev_grp is not None:
                block.extend(["", "⸻", ""])
            prev_grp = _BANK_GROUP_INTAKE
            block.append(ch)
            continue
        grp = _bank_change_group(ch)
        # Воздух (просьба юриста 10.08.2026): пустая строка между КАЖДЫМ
        # делом — 10-15 строк группы вплотную не читались; граница групп
        # важности — разделителем «⸻» (как между подсекциями), иначе с
        # повсеместным воздухом она стала бы невидимой. Линтер не задет:
        # _check_section_counters считает только строки с номерами дел.
        if prev_grp is not None:
            if grp != prev_grp:
                block.extend(["", "⸻", ""])
            else:
                block.append("")
        prev_grp = grp
        num = escape_html(ch.get("case", ""))
        court = escape_html(shorten_court_name(ch.get("court", "")))
        df = escape_html(shorten_party_name(
            ch.get("defendant", ""), keep_fio_full=_DIGEST_FIO_FULL))
        d = ch.get("details") or {}
        url = fi_card_url(d)
        link = f'<a href="{url}"><b>{num}</b></a>' if url else f'<b>{num}</b>'
        head = f"{link} ({court})" + (f" — {df}" if df else "")
        phrases = _bank_event_phrases(ch)
        block.append(f"{head}: {'; '.join(phrases)}" if phrases else head)
        # «Почему» при исходе против банка (bank_act_why_eligible) — строкой
        # СРАЗУ за строкой дела (без пустой строки: абзац head+Почему —
        # контракт attach_act_analyses, он же несёт разбор в drawer). Без
        # номера дела — линтер считает дела по строкам с номерами. Печатаем
        # ТОЛЬКО kind=="summary": сырой excerpt в компакт-секции не
        # показываем (Telegram-бюджет; при отказе LLM остаётся прежняя одна
        # строка, drawer получит raw_act-фолбэк от attach_act_analyses).
        # Пустой act_text (старые контексты replay) — прежний рендер.
        if bank_act_why_eligible(ch):
            why, why_kind = _act_summary_or_excerpt_with_kind(
                (d.get("act_text") or "").strip(),
                {
                    "stage": "first_instance",
                    "bank_role": ch.get("bank_role", ""),
                    "verdict_label": d.get("verdict_label", ""),
                    "plaintiff": shorten_party_name(
                        ch.get("plaintiff", ""), keep_fio_full=True
                    ),
                    "defendant": shorten_party_name(
                        ch.get("defendant", ""), keep_fio_full=True
                    ),
                    "category": d.get("category", ""),
                },
                summarizer=act_summarizer,
            )
            if why and why_kind == "summary":
                block.append(f"<b>Почему:</b> <i>{why}</i>")
    return block


def generate_template_digest(new_cases: list[dict], changes: list[dict], *,
                             cases: list[dict] | None = None,
                             fi_new_cases: list[dict] | None = None,
                             stage_transitions: list[dict] | None = None,
                             fi_changes: list[dict] | None = None,
                             total_active_appeal: int = 0,
                             total_active_fi: int = 0,
                             total_active_cassation: int = 0,
                             total_active_bank: int = 0,
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
    решения), кассации (new_act) и «🏦 ИСКИ БАНКА» (fi_act_text_published
    с исходом против банка — только пересказ, без excerpt-фолбэка)
    вместо обрезанного excerpt'а подставляется LLM-пересказ. None или
    ошибка callable → fallback на excerpt (старое поведение).

    Поля details, которые шаблон НЕ выводит ОСОЗНАННО (не дыры покрытия):
    - `old_hearing_date`/`old_hearing_time` — юрист просил показывать
      только новую дату отложения;
    - `event_text` у fi_returned — рендерится только распознанная причина
      (`_fi_return_reason_for_render`);
    - `restart_event` у fi_hearing_restart — сырой текст события, в строке
      достаточно даты и следующего заседания;
    - `appellant`/`appellant_name`/`appellant_role`/`_appellant_raw` у
      апел. changes — использовались только full-LLM промптом; в 5.4/5.5
      апеллянта не выводим (юрист не просил);
    - `hearing_long_ago`, `act_verdict_raw`, `last_event`, `act_excerpt`
      (при живом act_text) — вспомогательный контекст для LLM-путей;
    - `stage_prev`/`stage_now`, `act_kind`, `decision_date`,
      `result_for_appeal` у кассации — служебные поля линковки;
    - stage_transitions — намеренно не секция дайджеста (см. ниже).
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

    # Гасим административное «дело передано в архив» ДО сводки и тела —
    # чтобы счётчики сводки и содержимое секций считались по одному списку.
    fi_changes = _strip_archive_final_events(fi_changes)
    # Там же — сырое событие карточки, пересказывающее уже показанный исход
    # (инцидент 9-336/2026: одно событие в двух секциях).
    fi_changes = _strip_echoed_terminal_events(fi_changes)

    # ── Трек «Иски банка» (банк — истец) ──
    # Track-события приезжают в общем fi_changes с маркером change["track"]
    # (сигнатуры/контекст/replay не трогаются — маркер едет в данных) и
    # рендерятся отдельной компактной секцией; из основного списка убираются,
    # чтобы не раздувать счётчики и подсекции 1-й инстанции.
    bank_changes = [
        ch for ch in fi_changes if ch.get("track") == "plaintiff_light"
    ]
    if bank_changes:
        fi_changes = [
            ch for ch in fi_changes if ch.get("track") != "plaintiff_light"
        ]
    # Склейка «решение + мотивировка» одного дела — ПОСЛЕ отделения банк-трека
    # (в его компакт-строке дубля нет) и ДО сводки (кейс Урала 2-484/2026).
    fi_changes = _merge_motiv_into_resolved(fi_changes)

    total_active = total_active_appeal + total_active_fi + total_active_cassation
    # Иски банка — отдельная картотека: в сумму «всего» не входят, футер
    # упоминает их отдельной припиской (09.08.2026); 0 = трек выключен.
    bank_footer_tail = (
        f" · 🏦 иски банка: {total_active_bank} в производстве"
        if total_active_bank else ""
    )

    # ── Короткое сообщение если изменений нет ──
    # stage_transitions намеренно НЕ учитываем: мостик в дайджест больше
    # не выводится, прогон с одними переходами = пустой.
    if (not new_cases and not changes and not fi_new_cases
            and not fi_changes and not bank_changes
            and not cass_changes and not cass_discovered):
        return render_no_changes_digest(
            today,
            f"В производстве: всего {total_active}"
            f" (1 инст.: {total_active_fi} | апел.: {total_active_appeal}"
            f" | касс.: {total_active_cassation})" + bank_footer_tail,
        )

    # ── Группировка changes по типам (для блока АПЕЛЛЯЦИЯ) ──
    # Порядок вычисления корзин — от специфичного к общему: сначала
    # отложения и переходы (по типу), затем акты (5.5), резолютивки (5.4)
    # и в конце события (5.2). Членство в «событиях» определяется как
    # «change не попал в results/acts», а не проверкой по типам: иначе
    # «ложный» new_result (текст события в поле «Результат») исчезал из
    # ВСЕХ секций (results отбрасывал его по гарду, events — по типу), а
    # связка new_event+new_act дублировалась (5.2 + 5.5).
    postponed = [ch for ch in changes if "hearing_postponed" in ch["type"]]
    postponed_nums = {ch["case"] for ch in postponed}
    to_fi_rules = [ch for ch in changes if "appeal_to_fi_rules" in ch["type"]]
    # 5.4 и 5.5 — РАЗНЫЕ события (резолютивка и полный текст), но если в
    # ОДНОМ прогоне сработали оба — показываем дело ТОЛЬКО в 5.5 (там и
    # ИТОГ из карточки, и мотивировка). Иначе пользователь видит дубль.
    # Если события разнесены во времени — в разных прогонах каждая секция
    # получит «свой» change (защита сохраняется).
    acts = [ch for ch in changes if "new_act" in ch["type"]]
    _acts_ids = {id(ch) for ch in acts}
    # Подстраховка: если в `result` лежит текст события (см. одноимённую
    # утилиту), это «ложный» итог — дело принадлежит секции 5.2
    # «Изменения», а не 5.4 «Вынесенные акты». Парсер с гардом такие
    # `new_result` больше не выставляет, но фильтр защищает на случай
    # старого payload (например, `--replay-last` после регрессии).
    results = [ch for ch in changes
               if "new_result" in ch["type"]
               and id(ch) not in _acts_ids
               and not _is_event_text_in_result_field(
                   (ch.get("details") or {}).get("result", "")
               )]
    _results_ids = {id(ch) for ch in results}
    # Не дублируем дело в "Назначенные", если оно уже в "Отложенные".
    # hearing_new — первое заседание апелляции; семантически то же самое,
    # что и «назначенное заседание», поэтому показываем тут же.
    events = [ch for ch in changes
              if ("new_event" in ch["type"] or "hearing_new" in ch["type"])
              and ch["case"] not in postponed_nums
              and id(ch) not in _results_ids
              and id(ch) not in _acts_ids]
    # «Голый» status_change — change, не попавший ни в одну корзину выше.
    # Раньше такой change молча выпадал из дайджеста (у full-LLM пути он
    # выводился строкой «Статус: X → Y»). Показываем в 5.2 «Изменения».
    _known_ids = (
        {id(ch) for ch in postponed} | {id(ch) for ch in to_fi_rules}
        | _acts_ids | _results_ids | {id(ch) for ch in events}
    )
    status_only = [ch for ch in changes
                   if "status_change" in ch["type"]
                   and id(ch) not in _known_ids]

    # ── Блок ПЕРВАЯ ИНСТАНЦИЯ ──
    fi_block: list[str] = []
    if fi_new_cases:
        fi_block.append(f"📥 <b>Новые иски ({len(fi_new_cases)}):</b>")
        for c in fi_new_cases:
            fi = c.get("first_instance", {})
            court = escape_html(shorten_court_name(fi.get("court", "")))
            role = c.get("bank_role", "")
            cat = category_short(
                short_category_chain(c.get("category", "")), truncate=False
            )
            pl_raw = c.get("plaintiff", "")
            df_raw = c.get("defendant", "")
            pl = escape_html(shorten_party_name(pl_raw, keep_fio_full=_DIGEST_FIO_FULL))
            df = escape_html(shorten_party_name(df_raw, keep_fio_full=_DIGEST_FIO_FULL))
            num = escape_html(c.get("id", ""))
            filing = escape_html(fi.get("filing_date", ""))
            url = fi_card_url(fi)
            link = f'<a href="{url}"><b>{num}</b></a>' if url else f'<b>{num}</b>'
            # БАНК В ХВОСТЕ: иконка — для истца/ответчика; «третье лицо» —
            # словами в хвосте (выбор юриста 06.07.2026: иконка 👁 неочевидна).
            if _bank_in_parties(pl_raw, df_raw):
                role_icon = ""
                role_tail = ""
            else:
                role_icon = {"Истец": "🏦→", "Ответчик": "→🏦"}.get(role, "")
                role_tail = (" | банк — третье лицо"
                             if role == "Третье лицо" else "")
            prefix = f"{role_icon} " if role_icon else ""
            # КОМПАКТ-ВЁРСТКА (выбор юриста 03.07.2026, все секции): без
            # левых отступов — Telegram сохраняет ведущие пробелы только
            # на первой физической строке, при переносе «лесенка»
            # ломается. Структуру дают пустые строки между делами,
            # жирный номер-ссылка в начале дела и эмодзи-маркеры строк.
            # Строка 1: номер, стороны, категория, суд (без даты подачи).
            fi_block.append(
                f"{link} {prefix}{pl} vs {df} ({cat}) | {court}{role_tail}"
            )
            # Строка 2: дата подачи отдельной строкой, эмодзи 📥 ПОСЛЕ
            # <b>дата</b>, чтобы не попасть под _DIGEST_HEADER_RE.
            if filing:
                fi_block.append(
                    f"<b>{filing}</b> — 📥 иск зарегистрирован в суде"
                )
            # Строки 3-4 — только у дел, заведённых импортом дампа: с
            # 14.08.2026 он читает карточку сразу, и прогон уже не объявит
            # «заседание назначено»/«решение вынесено» отдельными событиями
            # (диффу не с чем сравнивать — данные приехали вместе с делом).
            # У дел, найденных поиском, эти поля пусты — строки не печатаются
            # и вёрстка секции прежняя посимвольно.
            hearing = escape_html(fi.get("hearing_date", ""))
            if hearing:
                htime = _bank_hearing_time(fi)
                fi_block.append(
                    f"<b>{hearing}</b> — 📅 заседание назначено"
                    + (f" в {escape_html(htime)}" if htime else "")
                )
            fi_result = (fi.get("result") or "").strip()
            if fi_result:
                fi_block.append(f"⚖️ Итог: {escape_html(fi_result[:120])}")
            # Пустая строка между делами (правило вёрстки юриста: строки
            # одного дела подряд, пустая строка — между разными делами).
            fi_block.append("")
        if fi_block and fi_block[-1] == "":
            fi_block.pop()

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
        # Возврат материала — уже в 3.2 «Изменения», в 3.5 не дублируем.
        and "fi_returned" not in ch["type"]
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
        pl = escape_html(shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=_DIGEST_FIO_FULL))
        df = escape_html(shorten_party_name(ch.get("defendant", ""), keep_fio_full=_DIGEST_FIO_FULL))
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
                        # Даты предстоящих заседаний — жирным (просьба юриста
                        # 06.07.2026): дата — главное в строке, глаз должен
                        # цепляться сразу. Прошедшие даты (актов, подач)
                        # не выделяем — иначе не выделено ничего.
                        hd = escape_html(d.get("hearing_date", ""))
                        ht = escape_html(d.get("hearing_time", ""))
                        htype = escape_html(d.get("hearing_type", "заседание"))
                        hp = _fmt_hearing_dt(hd, ht)
                        # «назначено» перед типом заседания (просьба юриста
                        # 07.07.2026): «📅 заседание 04.08.2026» →
                        # «📅 назначено заседание 04.08.2026». Предлог «на»
                        # перед датой (просьба 09.07.2026): «назначено
                        # заседание на 04.08.2026 в 10:30».
                        ev_list.append(
                            f"📅 назначено {htype} на <b>{hp}</b>"
                            if hp else f"📅 назначено {htype}"
                        )
                elif t == "fi_hearing_next":
                    new_p = escape_html(
                        _fmt_hearing_dt(
                            d.get("hearing_date", ""), d.get("hearing_time", "")
                        )
                    )
                    # Тип заседания скобками и только не-родовой (13.08.2026):
                    # «беседа»/«подготовка дела» раньше схлопывались в
                    # «заседание». Скобки вместо «назначено {тип} на» — род
                    # («назначена беседа») сломал бы шаблон.
                    ht32 = _hearing_type_paren(d)
                    ev_list.append(
                        f"📅 заседание назначено на <b>{new_p}</b>{ht32}"
                    )
                elif t == "fi_hearing_postponed":
                    new_p = escape_html(
                        _fmt_hearing_dt(
                            d.get("hearing_date", ""), d.get("hearing_time", "")
                        )
                    )
                    # Только новая дата (старую больше не показываем —
                    # по запросу пользователя).
                    ht32 = _hearing_type_paren(d)
                    ev_list.append(
                        f"🔁 заседание отложено на <b>{new_p}</b>{ht32}"
                    )
                elif t == "fi_hearing_recess":
                    new_p = escape_html(
                        _fmt_hearing_dt(
                            d.get("hearing_date", ""), d.get("hearing_time", "")
                        )
                    )
                    ev_list.append(
                        f"🔁 в заседании объявлен перерыв до <b>{new_p}</b>"
                    )
                elif t == "fi_status_change":
                    ev_list.append(
                        f"статус: {escape_html(d.get('old_status', ''))} → "
                        f"{escape_html(d.get('new_status', ''))}"
                    )
                elif t == "fi_returned":
                    # Процессуальное завершение: возврат иска / отказ в
                    # принятии / передача по подсудности. Вид — из details,
                    # фолбэк «возврат» для старых контекстов (--replay-last
                    # до 29.07.2026 ключа termination_kind не несёт).
                    kind = (d.get("termination_kind") or "returned").strip()
                    label = _FI_TERMINATION_LABELS.get(
                        kind, _FI_TERMINATION_LABELS["returned"]
                    )
                    reason = (d.get("return_reason") or "").strip()
                    if not reason and kind not in ("transfer", "merged"):
                        # Старый контекст: причины в details нет — достаём из
                        # event_text прежним хелпером. Для передачи и
                        # присоединения фолбэк НЕ применяем: он отдал бы первый
                        # сегмент события («судебное заседание»), а не «куда».
                        reason = _fi_return_reason_for_render(d)
                    part = escape_html(label)
                    if reason:
                        part += f": {escape_html(reason)}"
                    bank_out = (d.get("bank_outcome") or "").strip()
                    if bank_out:
                        # Знак исхода для банка. Без него при переносе дела из
                        # 3.5 в 3.2 эта информация терялась бы (решение юриста
                        # 29.07.2026).
                        part += f" (для банка: {escape_html(bank_out)})"
                    # Дата события-завершения (09.08.2026): суд заполняет
                    # «Результат» с лагом в недели (2-822/2026: передача
                    # 29.06, объявлена 07.08) — без даты строка читается как
                    # свежая новость. Ключ опционален (replay).
                    td = (d.get("termination_date") or "").strip()
                    if td:
                        part += f" ({escape_html(td)})"
                    ev_list.append(part)
                elif t == "fi_act_published":
                    ad = escape_html(d.get("act_date", ""))
                    ev_list.append(
                        "📄 мотивированное решение изготовлено"
                        + (f" {ad}" if ad else "")
                        + ", полный текст не опубликован"
                    )
                elif t == "fi_final_event":
                    ev_raw = d.get('event', '') or ''
                    # Спец-обработка фразы «Изготовлено мотивированное
                    # решение в окончательной форме» — эквивалент
                    # fi_act_published; нормализуем под единую формулировку.
                    if _is_motiv_made_event(ev_raw):
                        ad = escape_html(_motiv_date_from_event(ev_raw, d))
                        ev_list.append(
                            "📄 мотивированное решение изготовлено"
                            + (f" {ad}" if ad else "")
                            + ", полный текст не опубликован"
                        )
                    else:
                        ev_list.append(f"⚖️ {escape_html(ev_raw)}")
                        # Запланированная дата ближайшего заседания (для
                        # «подготовки дела»/«беседы»/«предварительного
                        # заседания») — юристу нужна, к когда готовиться.
                        # ТОЛЬКО будущая: прошедшая дата из старой карточки
                        # («заседание назначено на 24.06» в июльском
                        # дайджесте) — мусор первого парса, не анонс.
                        sh_d = escape_html(
                            d.get("scheduled_hearing_date", "")
                        )
                        sh_t = escape_html(
                            d.get("scheduled_hearing_time", "")
                        )
                        sh_parsed = parse_date(sh_d) if sh_d else None
                        if sh_parsed and sh_parsed.date() >= datetime.now().date():
                            sh_p = _fmt_hearing_dt(sh_d, sh_t)
                            ev_list.append(
                                f"📅 заседание назначено на <b>{sh_p}</b>"
                            )
                elif t == "fi_motivirovka_emitted":
                    md = escape_html(d.get('motivirovka_date', ''))
                    ev_list.append(
                        "📄 мотивированное решение изготовлено"
                        + (f" {md}" if md else "")
                        + ", полный текст не опубликован"
                    )
                elif t == "fi_appeal_filed":
                    dt = escape_html(d.get("appeal_filed_date", ""))
                    who = _fi_appellant_display(
                        d.get("appellant_role", ""),
                        d.get("appellant_name", ""), pl, df,
                    )
                    ev_list.append(
                        "📨 подана апелляц. жалоба"
                        + (f" ({dt})" if dt else "")
                        + (f", апеллянт: {who}" if who else "")
                    )
                elif t == "fi_cassation_filed":
                    dt = escape_html(d.get("cassation_filed_date", ""))
                    who = _fi_appellant_display(
                        d.get("cassator_role", ""),
                        d.get("cassator_name", ""), pl, df,
                    )
                    ev_list.append(
                        "📨 подана кассационная жалоба"
                        + (f" ({dt})" if dt else "")
                        + (f", податель: {who}" if who else "")
                    )
                elif t == "fi_sent_to_cassation":
                    dt = escape_html(d.get("sent_to_cassation_date", ""))
                    ev_list.append(
                        "📤 направлено в кассац. суд"
                        + (f" ({dt})" if dt else "")
                    )
                elif t == "fi_objections_deadline_set":
                    # Срок жирным: это рабочий дедлайн юриста, а не справка —
                    # возражения на жалобу подаются в суд 1-й инстанции.
                    dt = escape_html(d.get("objections_due", ""))
                    ev_list.append(
                        "⏳ установлен срок для возражений на жалобу"
                        + (f" — до <b>{dt}</b>" if dt else "")
                    )
                elif t == "fi_default_cancellation_filed":
                    dt = escape_html(d.get("cancel_filed_date", ""))
                    ev_list.append(
                        "🌙 подано заявление об отмене заочного решения"
                        + (f" ({dt})" if dt else "")
                    )
                elif t == "fi_default_cancellation_hearing":
                    dt = escape_html(d.get("cancel_hearing_date", ""))
                    ev_list.append(
                        "📅 заседание по заявлению об отмене заочного решения"
                        + (f" <b>{dt}</b>" if dt else "")
                    )
                elif t == "fi_default_judgment_vacated":
                    dt = escape_html(d.get("cancel_outcome_date", ""))
                    ev_list.append(
                        "⚠️ <b>заочное решение отменено</b>"
                        + (f" ({dt})" if dt else "")
                        + " — дело рассматривается заново"
                    )
                elif t == "fi_default_cancellation_refused":
                    # Без «✅» и «пошёл месяц» (13.08.2026): формулировка была
                    # с позиции банка-истца, а в основной картотеке банк —
                    # ответчик, и отказ по ЕГО заявлению «галочкой» не
                    # отметишь. Банк-секция хранит свою (там истец).
                    dt = escape_html(d.get("cancel_outcome_date", ""))
                    ev_list.append(
                        "⚖️ в отмене заочного решения отказано"
                        + (f" ({dt})" if dt else "")
                        + " — открыт месячный срок на апелляцию"
                    )
                elif t == "fi_default_copy_returned":
                    # Запускает формулу ВС для срока вступления в силу
                    # (решение + 3 раб. дн + 7 раб. дн + месяц) — юристу
                    # важно видеть сам факт (разбор 07.08.2026).
                    dt = escape_html(d.get("copy_returned_date", ""))
                    ev_list.append(
                        "🌙 копия заочного решения возвратилась невручённой"
                        + (f" ({dt})" if dt else "")
                    )
                elif t == "fi_default_copy_served":
                    # Парное к возврату (13.08.2026): эмит общий для обоих
                    # треков, а ветка была только в банк-секции — в основной
                    # 3.2 выходила «голая» строка дела без текста события.
                    dt = escape_html(d.get("copy_served_date", ""))
                    ev_list.append(
                        "🌙 копия заочного решения вручена ответчику"
                        + (f" ({dt})" if dt else "")
                        + " — 7 раб. дн. на заявление об отмене"
                    )
                elif t == "fi_post_decision_hearing":
                    # Заседание по решённому делу (13.08.2026): в основной
                    # картотеке это судебные расходы/индексация ПРОТИВ банка,
                    # отсрочки должников — гард case_decided глушил их
                    # полностью. Формат — зеркало банк-ветки.
                    pdh = escape_html(
                        _fmt_hearing_dt(
                            d.get("hearing_date", ""), d.get("hearing_time", "")
                        )
                    )
                    topic = escape_html(d.get("hearing_topic", "") or "")
                    part = "📅 заседание по решённому делу"
                    if pdh:
                        part += f" — <b>{pdh}</b>"
                    if topic:
                        part += f" ({topic})"
                    ev_list.append(part)
                elif t == "fi_hearing_restart":
                    rd = escape_html(d.get("restart_date", ""))
                    nhd = escape_html(d.get("next_hearing_date", ""))
                    nht = escape_html(d.get("next_hearing_time", ""))
                    part = "🔄 рассмотрение начато с начала" + (f" ({rd})" if rd else "")
                    if nhd:
                        nhp = _fmt_hearing_dt(nhd, nht)
                        part += f"; след. заседание <b>{nhp}</b>"
                    ev_list.append(part)
                elif t == "fi_bank_role_changed":
                    old_r = escape_html(d.get("old_role", ""))
                    new_r = escape_html(d.get("new_role", ""))
                    hint = escape_html(d.get("reason_hint", "") or "")
                    msg = f"🔄 роль банка: {old_r} → {new_r}"
                    if hint:
                        msg += f" ({hint})"
                    msg += ". Дальнейшие исходы — нейтральны."
                    ev_list.append(msg)
                elif t == "fi_accepted_no_hearing":
                    mat = escape_html(d.get("material_number", ""))
                    ev_list.append(
                        "📥 принято к производству — заседание не назначено"
                        + (f" (было {mat})" if mat else "")
                    )
        # Событие — на ОТДЕЛЬНОЙ строке под «номер (суд) — стороны» (просьба
        # юриста 06.07.2026). Строки одного дела идут подряд без пустой между
        # ними (правило вёрстки); пустая строка ставится только между делами.
        head = f"{link} ({court}) — {pl} vs {df}"
        ev_str = "; ".join(ev_list) if ev_list else ""
        fi_changes_rendered.append(
            f"{head}\n{ev_str}" if ev_str else head
        )

    if fi_changes_rendered:
        _section_break(fi_block)
        fi_block.append(
            f"📅 <b>Изменения ({len(fi_changes_rendered)}):</b>"
        )
        # Пустая строка между событиями (просьба юриста 06.07.2026: события
        # шли подряд и на 3+ делах сливались в стену текста; правило
        # «пустая строка между делами» — как в «Новых делах»).
        for i, ev_line in enumerate(fi_changes_rendered):
            if i:
                fi_block.append("")
            fi_block.append(ev_line)

    # ── 3.5: Вынесенные решения 1 инстанции ──
    if fi_resolved_chs:
        _section_break(fi_block)
        fi_block.append(
            f"⚖️ <b>Вынесенные решения ({len(fi_resolved_chs)}):</b>"
        )
        for ch in fi_resolved_chs:
            num = escape_html(ch.get("case", ""))
            court = escape_html(shorten_court_name(ch.get("court", "")))
            pl = escape_html(shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=_DIGEST_FIO_FULL))
            df = escape_html(shorten_party_name(ch.get("defendant", ""), keep_fio_full=_DIGEST_FIO_FULL))
            d = ch["details"]
            url = fi_card_url(d)
            link = f'<a href="{url}"><b>{num}</b></a>' if url else f'<b>{num}</b>'
            verdict = escape_html(d.get("verdict_label", ""))
            dec_date = escape_html(d.get("decision_date", ""))
            # Категория ЦЕЛИКОМ (truncate=False): в двухстрочной вёрстке 3.5
            # место есть, «об освобождении…» с обрезкой юриста не устраивал.
            cat = escape_html(
                category_short(
                    short_category_chain(d.get("category", "")), truncate=False
                )
            )
            bank_role = escape_html(ch.get("bank_role", ""))
            bank_out = escape_html(d.get("bank_outcome", ""))
            # Двухстрочная вёрстка (просьба юриста 07.07.2026): строка 1 —
            # стороны + категория (+ роль банка, если банк не в сторонах);
            # строка 2 — «{дата} вынесено решение. Итог: …. Для банка: …».
            # Строки одного дела идут ПОДРЯД (пустая — только между делами).
            head_extras: list[str] = []
            if cat:
                head_extras.append(f"категория: {cat}")
            # БАНК В ХВОСТЕ: «банк — роль» только когда банк не в сторонах.
            if bank_role and not _bank_in_parties(
                    ch.get("plaintiff", ""), ch.get("defendant", "")):
                head_extras.append(f"банк — {bank_role.lower()}")
            head_tail = (" | " + " | ".join(head_extras)) if head_extras else ""
            fi_block.append(f"{link} ({court}) — {pl} vs {df}{head_tail}")
            # Строка 2: дата вынесения + исход.
            closure_head = _FI_CLOSURE_HEADS.get(
                (d.get("verdict_label") or "").strip())
            if closure_head:
                # Процессуальное закрытие: не «вынесено решение», а
                # определение — шапка называет его прямо, причина в скобках,
                # дублирующий «Итог: прекращено» не печатается.
                reason = fi_closure_reason(
                    d.get("raw_result", ""), d.get("last_event", ""))
                head = (f"{dec_date} {closure_head}" if dec_date
                        else closure_head[:1].upper() + closure_head[1:])
                decision_parts: list[str] = [
                    head + (f" ({escape_html(reason)})" if reason else "")
                ]
            else:
                decision_parts = [
                    f"{dec_date} вынесено решение" if dec_date
                    else "Вынесено решение"
                ]
                if verdict:
                    decision_parts.append(f"<b>Итог:</b> {verdict}")
            if bank_out:
                decision_parts.append(f"<b>Для банка:</b> {bank_out}")
            # Банк исключён из сторон / переведён в 3-е лицо — явный нейтралитет
            # (иначе «иск удовлетворён» читается как «против банка»).
            if "fi_bank_role_changed" in ch["type"]:
                decision_parts.append(
                    "<b>Для банка:</b> нейтрально — банк не сторона согласно карточке"
                )
            # Мотивировка того же прогона, приклеенная _merge_motiv_into_
            # resolved (кейс Урала 2-484/2026): раньше печаталась отдельной
            # строкой в 3.2 — дело выходило дважды.
            if "motiv_merged_date" in d:
                md = escape_html((d.get("motiv_merged_date") or "").strip())
                decision_parts.append(
                    "Мотивировка изготовлена"
                    + (f" {md}" if md else "")
                    + ", полный текст не опубликован"
                )
            fi_block.append(". ".join(decision_parts))
            # Пустая строка между делами (воздух, просьба юриста 06.07.2026).
            fi_block.append("")
        if fi_block and fi_block[-1] == "":
            fi_block.pop()

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
            pl = escape_html(shorten_party_name(ch.get("plaintiff", ""), keep_fio_full=_DIGEST_FIO_FULL))
            df = escape_html(shorten_party_name(ch.get("defendant", ""), keep_fio_full=_DIGEST_FIO_FULL))
            d = ch["details"]
            url = fi_card_url(d)
            link = f'<a href="{url}"><b>{num}</b></a>' if url else f'<b>{num}</b>'
            verdict = escape_html(d.get("verdict_label", ""))
            bank_out = escape_html(d.get("bank_outcome", ""))
            # 3.6: либо LLM-пересказ мотивировки (если act_summarizer задан),
            # либо обрезанный excerpt — old behaviour для template-fallback.
            # Стороны прогоняем через shorten_party_name — LLM иначе тянет в
            # «Почему» громоздкие имена вроде «МТУ Росимущества в Тюменской
            # области, ХМАО-Югре, ЯНАО».
            act_excerpt, act_kind = _act_summary_or_excerpt_with_kind(
                d.get("act_text") or "",
                {
                    "stage": "first_instance",
                    "bank_role": ch.get("bank_role", ""),
                    "verdict_label": d.get("verdict_label", ""),
                    "plaintiff": shorten_party_name(
                        ch.get("plaintiff", ""), keep_fio_full=True
                    ),
                    "defendant": shorten_party_name(
                        ch.get("defendant", ""), keep_fio_full=True
                    ),
                    "category": d.get("category", ""),
                },
                summarizer=act_summarizer,
                max_excerpt_len=500,
            )
            fi_block.append(f"{link} — {pl} vs {df}")
            itog_parts: list[str] = []
            if verdict:
                # Дата решения (13.08.2026): суд публикует тексты задним
                # числом, и «Итог» без даты читался как свежий исход — тот
                # же урок, что в банк-секции (разбор 07.08). Ключ опционален,
                # старые контексты живут без скобок.
                dd36 = escape_html(d.get("decision_date", "") or "")
                itog_parts.append(
                    f"<b>Итог:</b> {verdict}"
                    + (f" (решение от {dd36})" if dd36 else "")
                )
            if bank_out:
                itog_parts.append(f"<b>Для банка:</b> {bank_out}")
            if "fi_bank_role_changed" in ch["type"]:
                itog_parts.append(
                    "<b>Для банка:</b> нейтрально — банк не сторона согласно карточке"
                )
            if itog_parts:
                fi_block.append(". ".join(itog_parts))
            # LLM-пересказ — с маркером «Почему:» (контракт attach_act_analyses
            # и drawer'а карточки дела); сырой excerpt — просто курсивом.
            if act_excerpt and act_kind == "summary":
                fi_block.append(f"<b>Почему:</b> <i>{act_excerpt}</i>")
            elif act_excerpt:
                fi_block.append(f"<i>{act_excerpt}</i>")
            fi_block.append("")  # пустая строка-разделитель между делами
        # убрать хвостовую пустую строку, если добавили
        if fi_block and fi_block[-1] == "":
            fi_block.pop()

    # Актуальный cases.json — один load на рендер (13.08.2026): пометка
    # «повторное рассмотрение после кассации» у новых апелляций (ниже) и
    # подтяжка родительских полей кассационных событий (секция КАССАЦИЯ
    # переиспользует). Переданный kwarg `cases` — legacy CSV-строки апелляции,
    # для обоих потребителей не годится. Нет файла/битый — обе фичи молча
    # деградируют (пометки нет, поля пустые), как и раньше.
    try:
        full_cases_for_cass = load_json(config.JSON_PATH).get("cases", []) or []
    except (OSError, json.JSONDecodeError):
        full_cases_for_cass = []
    # Индекс «(домен апел. суда, bare апел. номера) → дело» для пометки
    # второго круга: к моменту рендера link_cases уже отработал и cases.json
    # сохранён (фазы 8 → 9), у дела нового круга в appeal лежит НОВЫЙ
    # 33-номер. Фолбэк-ключ с пустым доменом — для строк без _appeal_domain
    # (старые контексты replay).
    case_by_appeal_num: dict[tuple, dict] = {}
    for c_idx in full_cases_for_cass:
        ap_idx = c_idx.get("appeal") or {}
        _ap_bare = _bare_case_number((ap_idx.get("case_number") or "").strip())
        if not _ap_bare:
            continue
        _ap_dom = (ap_idx.get("court_domain") or "").strip()
        case_by_appeal_num.setdefault((_ap_dom, _ap_bare), c_idx)
        case_by_appeal_num.setdefault(("", _ap_bare), c_idx)

    # ── Блок АПЕЛЛЯЦИЯ ──
    appeal_block: list[str] = []
    if new_cases:
        appeal_block.append(f"📥 <b>Новые дела ({len(new_cases)}):</b>")
        for c in new_cases:
            link = case_link_html(c)
            role = c.get("Роль банка", "")
            cat = category_short(
                short_category_chain(c.get("Категория", "")), truncate=False
            )
            pl_raw = c.get('Истец', '')
            df_raw = c.get('Ответчик', '')
            pl = escape_html(shorten_party_name(pl_raw, keep_fio_full=_DIGEST_FIO_FULL))
            df = escape_html(shorten_party_name(df_raw, keep_fio_full=_DIGEST_FIO_FULL))
            court_fi = escape_html(
                shorten_court_name(c.get('Суд 1 инстанции', '') or '')
            )
            filing = escape_html(c.get('Дата поступления', '') or '')
            # БАНК В ХВОСТЕ: если Сбербанк уже в сторонах — иконка/хвост лишние.
            # «Третье лицо» — только словами в хвосте, без иконки 👁 (выбор
            # юриста 06.07.2026: иконка неочевидна и дублировала текст).
            if _bank_in_parties(pl_raw, df_raw):
                role_icon = ""
                role_tail = ""
            else:
                role_icon = {"Истец": "🏦→", "Ответчик": "→🏦"}.get(role, "")
                role_tail = (f" | банк — {escape_html(role.lower())}"
                             if role else "")
            prefix = f"{role_icon} " if role_icon else ""
            # Строка 1: номер + стороны (компакт-вёрстка, без отступов).
            # Номер дела 1-й инст. — хвостом СТРОКИ 1 (13.08.2026): юрист
            # сразу видит, какое дело поехало наверх. ⚠️ Именно в строку 1:
            # линтер и postprocess считают дела по строкам с номерами —
            # отдельная строка удвоила бы счётчик секции. Ключа нет (старые
            # контексты) → без хвоста.
            fi_no = escape_html(
                (c.get("Номер дела 1 инстанции") or "").strip()
            )
            appeal_block.append(
                f"{link} {prefix}{pl} vs {df}"
                + (f" (1-я инст.: {fi_no})" if fi_no else "")
            )
            # Строка 2: суд 1 инст. | категория | банк (если не в сторонах).
            line2_parts: list[str] = []
            if court_fi:
                line2_parts.append(f"Суд 1 инст.: {court_fi}")
            if cat:
                line2_parts.append(f"категория: {escape_html(cat)}")
            if line2_parts or role_tail:
                appeal_block.append(
                    " | ".join(line2_parts) + role_tail
                )
            # Повторное рассмотрение после кассации (13.08.2026): второй круг
            # был неотличим от обычной новой апелляции, а приоритет у него
            # другой. Матч по свежему cases.json; несматч → без пометки.
            _rnd_case = (case_by_appeal_num.get(
                ((c.get("_appeal_domain") or "").strip(),
                 _bare_case_number((c.get("Номер дела") or "").strip())))
                or case_by_appeal_num.get(
                    ("", _bare_case_number((c.get("Номер дела") or "").strip()))
                ))
            if (_rnd_case and int(_rnd_case.get("round") or 1) >= 2
                    and any(str((h or {}).get("reason") or "").startswith(
                        "cassation_remanded")
                        for h in (_rnd_case.get("history") or []))):
                appeal_block.append("🔁 повторное рассмотрение после кассации")
            # Строка 3: дата поступления отдельной строкой, эмодзи 📥
            # ПОСЛЕ <b>дата</b>, чтобы не попасть под _DIGEST_HEADER_RE.
            if filing:
                appeal_block.append(
                    f"<b>{filing}</b> — 📥 поступило в апел. суд"
                )
            # Пустая строка между делами (правило вёрстки юриста).
            appeal_block.append("")
        if appeal_block and appeal_block[-1] == "":
            appeal_block.pop()

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
            line = f"⚠ {link}"
            if tr_dt:
                line += f" ({tr_dt})"
            line += " — по правилам производства в суде первой инстанции"
            if plaintiff and defendant:
                line += f"\n{plaintiff} vs {defendant}{role_note}"
            appeal_block.append(line)
            # Пустая строка между делами (правило вёрстки юриста).
            appeal_block.append("")
        if appeal_block and appeal_block[-1] == "":
            appeal_block.pop()

    # Объединяем «Отложенные» и «Назначенные» апелляции в одну секцию
    # «📅 Изменения» (по запросу юриста, как 3.2 в 1-й инст.). Формат —
    # компакт (выбор юриста 03.07.2026): «номер — стороны | категория»
    # одной строкой, событие — второй; без левых отступов (в Telegram
    # они ломаются переносом строк). `events` уже исключает дела из
    # `postponed_nums`, дублирования нет.
    # Сюда же — «голые» status_change (строка 2: «статус: X → Y»).
    combined_apel_changes = postponed + events + status_only
    if combined_apel_changes:
        _section_break(appeal_block)
        appeal_block.append(
            f"📅 <b>Изменения ({len(combined_apel_changes)}):</b>"
        )
        for ch in combined_apel_changes:
            d = ch["details"]
            url = d.get("case_url", "")
            case_num = escape_html(ch["case"])
            link = (f'<a href="{url}"><b>{case_num}</b></a>'
                    if url else f'<b>{case_num}</b>')
            plaintiff = escape_html(shorten_party_name(d.get("plaintiff", "")))
            defendant = escape_html(shorten_party_name(d.get("defendant", "")))
            cat = category_short(
                short_category_chain(d.get("category", "")), truncate=False
            )
            is_postponed = "hearing_postponed" in ch["type"]
            # Дата+время заседания. Для отложений — new_hearing_*; для
            # назначений: new_hearing_* → ПОЛЯ КАРТОЧКИ hearing_date/time →
            # разбор текста события (legacy-payload без полей). Поля
            # карточки приоритетнее хвоста текста: в «Судебное заседание.
            # 14:30. 03.07.2026» хвостовая дата — дата размещения записи,
            # а не заседания (A/B 03.07.2026, дело 33-4521/2026: реальное
            # заседание 14.07.2026, хвост текста — 03.07.2026).
            event_raw = (d.get("event") or "").strip()
            hd = escape_html(d.get("new_hearing_date", ""))
            ht = escape_html(d.get("new_hearing_time", ""))
            if not hd and not is_postponed:
                hd = escape_html(d.get("hearing_date", ""))
                ht = ht or escape_html(d.get("hearing_time", ""))
            if not hd and not is_postponed:
                # Из «Судебное заседание. 11:30. 03.06.2026» вытаскиваем
                # дату и время.
                for p in event_raw.split(". "):
                    ps = p.strip()
                    if parse_date(ps) and not hd:
                        hd = escape_html(ps)
                    elif re.match(r'^\d{1,2}:\d{2}$', ps) and not ht:
                        ht = escape_html(ps)
            hp = _fmt_hearing_dt(hd, ht)
            # Строка 1: «номер — стороны | категория». Суд показываем только
            # когда в регионе НЕСКОЛЬКО апел-судов (details["appeal_court"]
            # пишет runs.py лишь при len(APPEAL_COURTS)>1; у ХМАО суд один —
            # ключа нет, рендер прежний байт-в-байт).
            ap_court_note = escape_html(d.get("appeal_court", ""))
            line1 = link
            if plaintiff and defendant:
                line1 += f" — {plaintiff} vs {defendant}"
            if cat:
                line1 += f" | категория: {escape_html(cat)}"
            if ap_court_note:
                line1 += f" | {ap_court_note}"
            appeal_block.append(line1)
            # Строка 2: 🔁 отложено / 📅 назначено / 📌 текст события /
            # статус. Содержательное событие (исход, приостановление,
            # экспертиза — см. _event_text_is_informative) показываем
            # текстом, а не «Заседание назначено на <прошедшую дату>».
            # Если дату вытащить не удалось — тоже текст события (иначе
            # карточка дела информационно пуста). «📌 текст» без <b> не
            # ловится _DIGEST_HEADER_RE — за заголовок не примут.
            # Для «голого» status_change — строка «статус: X → Y» (формат
            # как в 3.2 первой инстанции).
            informative_event = (
                "new_event" in ch["type"]
                and _event_text_is_informative(event_raw)
            )
            is_new_event = "new_event" in ch["type"]
            ev_date = escape_html(
                d.get("event_date", "") or d.get("hearing_date", "")
            )
            ev_date_raw = (
                d.get("event_date", "") or d.get("hearing_date", "")
            ).strip()
            # Анонс с прошедшей датой (в т.ч. ложная «Дата размещения» из
            # хвоста legacy-склейки — она всегда в прошлом) не выдаём за
            # «назначено» — проваливаемся в 📌-цитату факта. Считаем от
            # финального hd (details ИЛИ хвост склейки), чтобы гасить оба
            # источника.
            hd_dt = parse_date(hd)
            hearing_past = (
                hd_dt is not None and hd_dt.date() < datetime.now().date()
            )
            if is_postponed and hp:
                appeal_block.append(
                    f"🔁 Заседание отложено на <b>{hp}</b>"
                )
            elif is_new_event and _SUSPENDED_RE.search(event_raw):
                # Приостановление производства — интерпретируем, а не
                # цитируем: «⏸ Производство по делу приостановлено —
                # назначение судом экспертизы (02.07.2026)».
                line_susp = "⏸ Производство по делу приостановлено"
                reason = _suspension_reason_from_event(event_raw)
                if reason:
                    line_susp += f" — {escape_html(reason)}"
                if ev_date:
                    line_susp += f" ({ev_date})"
                appeal_block.append(line_susp)
            elif is_new_event and _RESUMED_RE.search(event_raw):
                line_res = "▶️ Производство по делу возобновлено"
                if ev_date:
                    line_res += f" ({ev_date})"
                if hp and hd != ev_date:
                    # Вместе с возобновлением суд обычно назначает заседание.
                    line_res += f"; заседание <b>{hp}</b>"
                appeal_block.append(line_res)
            elif informative_event:
                appeal_block.append(
                    f"📌 {escape_html(_event_quote(event_raw, ev_date_raw))}"
                )
            elif hp and not (hearing_past and is_new_event and event_raw):
                if _SOLO_SESSION_RE.match(event_raw):
                    # Утверждённый формат 30.07.2026: без времени (у ГАС
                    # там заглушка 00:00) и с пометкой «без вызова лиц» —
                    # юристу сразу видно, что являться не нужно.
                    appeal_block.append(
                        "📅 Единоличное рассмотрение (без вызова лиц) — "
                        f"<b>{hd}</b>"
                    )
                else:
                    appeal_block.append(
                        f"📅 Заседание назначено на <b>{hp}</b>"
                    )
            elif "new_event" in ch["type"] and event_raw:
                appeal_block.append(
                    f"📌 {escape_html(_event_quote(event_raw, ev_date_raw))}"
                )
            elif "status_change" in ch["type"]:
                appeal_block.append(
                    f"статус: {escape_html(d.get('old_status', ''))} → "
                    f"{escape_html(d.get('new_status', ''))}"
                )
            # Пустая строка между делами (правило вёрстки юриста: без
            # разделителя двухстрочные карточки дел визуально слипаются).
            appeal_block.append("")
        if appeal_block and appeal_block[-1] == "":
            appeal_block.pop()

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
            pl_raw = d.get("plaintiff", "")
            df_raw = d.get("defendant", "")
            pl = escape_html(
                shorten_party_name(pl_raw, keep_fio_full=_DIGEST_FIO_FULL)
            )
            df = escape_html(
                shorten_party_name(df_raw, keep_fio_full=_DIGEST_FIO_FULL)
            )
            cat = escape_html(
                category_short(
                    short_category_chain(d.get("category", "")), truncate=False
                )
            )
            hearing_dt = escape_html(d.get("hearing_date", ""))
            # Двухстрочная вёрстка (просьба юриста 09.07.2026): строка 1 —
            # стороны + категория (+ роль банка, если банк не в сторонах);
            # строка 2 — «{дата} вынесено определение — {результат}».
            head_extras: list[str] = []
            if cat:
                head_extras.append(f"категория: {cat}")
            # БАНК В ХВОСТЕ: «банк — роль» только когда банк не в сторонах.
            if role and not _bank_in_parties(pl_raw, df_raw):
                head_extras.append(f"банк — {escape_html(role.lower())}")
            head_tail = (" | " + " | ".join(head_extras)) if head_extras else ""
            line1 = link
            if pl and df:
                line1 += f" — {pl} vs {df}"
            line1 += head_tail
            appeal_block.append(line1)
            # Строка 2: дата вынесения определения + итог + по чьей жалобе
            # (просьба юриста 30.07.2026). С 13.08.2026 вместо сырого поля
            # «Результат» — нормализованный ярлык и знак «Для банка»: они
            # считались при эмите всегда, но выводились только в секции
            # текстов актов, а итог юрист читает здесь. Сырое поле не
            # дублируем: classify_verdict сам фолбэчит в сырую строку, так
            # что информация не теряется. Старый контекст без verdict_label
            # (--replay-last) — прежняя форма.
            from_str = _appeal_complaint_suffix(d, pl, df)
            verdict54 = escape_html(d.get("verdict_label", "") or "")
            bank54 = escape_html(d.get("bank_outcome", "") or "")
            if verdict54:
                line2 = (f"{hearing_dt} вынесено определение."
                         if hearing_dt else "Вынесено определение.")
                line2 += f" <b>Итог:</b> {verdict54}."
                if bank54:
                    line2 += f" <b>Для банка:</b> {bank54}."
                line2 += from_str
                appeal_block.append(line2)
            else:
                appeal_block.append(
                    f"{hearing_dt} вынесено определение — {result_text}{from_str}"
                    if hearing_dt
                    else f"Вынесено определение — {result_text}{from_str}"
                )
            # Пустая строка между делами (двухстрочные карточки иначе слипаются).
            appeal_block.append("")
        if appeal_block and appeal_block[-1] == "":
            appeal_block.pop()

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
                summary_or_excerpt, sum_kind = _act_summary_or_excerpt_with_kind(
                    raw_act,
                    {
                        "stage": "appeal",
                        "bank_role": d.get("role", ""),
                        "verdict_label": (
                            d.get("act_verdict_label")
                            or d.get("verdict_label", "")
                        ),
                        # Сокращаем имена в payload: иначе LLM в пересказе
                        # тянет полные «МТУ Росимущества в …, ХМАО-Югре, …».
                        "plaintiff": shorten_party_name(
                            d.get("plaintiff", ""), keep_fio_full=True
                        ),
                        "defendant": shorten_party_name(
                            d.get("defendant", ""), keep_fio_full=True
                        ),
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
                summary_or_excerpt, sum_kind = escape_html(short), "excerpt"
            else:
                summary_or_excerpt, sum_kind = "", ""
            # Строка 1: «номер — стороны» (компакт-вёрстка, без отступов).
            pl55 = escape_html(shorten_party_name(
                d.get("plaintiff", ""), keep_fio_full=_DIGEST_FIO_FULL
            ))
            df55 = escape_html(shorten_party_name(
                d.get("defendant", ""), keep_fio_full=_DIGEST_FIO_FULL
            ))
            line1_55 = link
            if pl55 and df55:
                line1_55 += f" — {pl55} vs {df55}"
            appeal_block.append(line1_55)
            # Итог из карточки + «в чью пользу» — симметрично 3.6 (данные
            # уже в details: act_verdict_label / bank_outcome).
            verdict55 = escape_html(
                d.get("act_verdict_label") or d.get("verdict_label") or ""
            )
            bank_out55 = escape_html(d.get("bank_outcome", ""))
            itog55: list[str] = []
            if verdict55:
                itog55.append(f"<b>Итог:</b> {verdict55}")
            if bank_out55:
                itog55.append(f"<b>Для банка:</b> {bank_out55}")
            if itog55:
                appeal_block.append(". ".join(itog55))
            # LLM-пересказ — с маркером «Почему:» (контракт attach_act_analyses
            # и drawer'а); сырой excerpt — по-старому «Мотивировка: …».
            if summary_or_excerpt and sum_kind == "summary":
                appeal_block.append(
                    f"<b>Почему:</b> <i>{summary_or_excerpt}</i>"
                )
            elif summary_or_excerpt:
                appeal_block.append(f"Мотивировка: {summary_or_excerpt}")
            # Пустая строка между делами — правило вёрстки юриста; заодно
            # attach_act_analyses режет 5.5 на абзацы по-делово, а не одним
            # куском на всю секцию.
            appeal_block.append("")
        if appeal_block and appeal_block[-1] == "":
            appeal_block.pop()

    # ── Сборка ──
    summary = build_summary_line(
        new_cases, changes, fi_new_cases, stage_transitions, fi_changes,
        cass_changes=cass_changes, cass_discovered=cass_discovered,
        bank_changes=bank_changes,
    )
    # Заголовок «📋 Сводка» отдельной строкой, счётчики — под ним (просьба
    # юриста 06.07.2026). Строка счётчиков начинается с 📥/📅/… + цифра —
    # под _DIGEST_HEADER_RE (нужен <b>+буква) не попадает, за заголовок не
    # примут.
    # Заголовок дайджеста — из активного региона (для ХМАО digest_title даёт
    # прежнюю строку байт-в-байт; форк территории получает свой заголовок
    # без правки кода).
    lines = [
        f"📊 <b>{get_region().digest_title} — {today}</b>",
        "📋 <b>Сводка</b>",
        escape_html(summary),
    ]

    # Две пустые строки перед заголовком крупной секции + одна ПОСЛЕ него
    # (просьба юриста 06.07.2026: воздух после заголовка секции и раздела).
    # `_air_after_subsection_headers` добавляет воздух после «… (N):».
    if fi_block:
        lines.extend(["", ""])
        lines.append("🏛 <b>ПЕРВАЯ ИНСТАНЦИЯ</b>")
        lines.append("")
        lines.extend(_air_after_subsection_headers(fi_block))
    if appeal_block:
        lines.extend(["", ""])
        lines.append("⚖️ <b>АПЕЛЛЯЦИЯ</b>")
        lines.append("")
        lines.extend(_air_after_subsection_headers(appeal_block))

    # ── Блок КАССАЦИЯ ──
    cass_block: list[str] = []
    # Готовые подписи исхода берём из module-level CASSATION_OUTCOME_RU
    # (см. рядом с classify_cassation_outcome). Так LLM-ветка и template-ветка
    # дайджеста используют один и тот же словарь — без дублирования и расхождений.
    # Словарь cases-by-id для подтягивания plaintiff/defendant/category/
    # bank_role/first_instance.court по родительскому case (в cass_changes.details
    # этих полей нет — раньше шаблон выводил пустые «{не указаны}»).
    # cass_changes ссылаются на FI-номер. Сам cases.json загружен ОДИН раз
    # выше, перед блоком АПЕЛЛЯЦИЯ (13.08.2026, общий с пометкой второго
    # круга) — переданный `cases` может быть в legacy CSV-формате и содержать
    # только апел. дела (33-XXXX), что для касс. событий с FI-ключами
    # не подходит.
    cases_by_id_for_cass: dict[str, dict] = {}
    for c_idx in (full_cases_for_cass or cases or []):
        for k_idx in (
            c_idx.get("id") or "",
            (c_idx.get("first_instance") or {}).get("case_number") or "",
            c_idx.get("Номер дела") or "",
        ):
            if k_idx:
                cases_by_id_for_cass.setdefault(k_idx, c_idx)

    def _g_cass(parent: dict, eng: str, ru: str) -> str:
        return (parent.get(eng) or parent.get(ru) or "").strip() if parent else ""
    if cass_discovered:
        # Индекс discovery-change'ей (13.08.2026): их details["act_text"] уже
        # обрезан extract_motive_part(...,1800) и загейчен .cassation_acts в
        # linking.py — правильный источник текста для пересказа. Прямое чтение
        # case["cassation"]["act_text"] ниже — только фолбэк для legacy-replay
        # (полный акт до ~10 КБ уходил в LLM целиком и мимо дедупа).
        disc_ch_by_key: dict[str, dict] = {}
        for _dch in cass_changes:
            if "discovered_in_cassation" in (_dch.get("type") or []):
                for _k in (_dch.get("cassation_internal_number"),
                           _dch.get("case")):
                    if _k:
                        disc_ch_by_key.setdefault(_k, _dch)
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
            pl = escape_html(shorten_party_name(pl_raw, keep_fio_full=_DIGEST_FIO_FULL))
            df = escape_html(shorten_party_name(df_raw, keep_fio_full=_DIGEST_FIO_FULL))
            role = c.get("bank_role", "") or ""
            tail = "" if _bank_in_parties(pl_raw, df_raw) or not role \
                else f", банк — {escape_html(role.lower())}"
            sber_flag = "🏦 " if cass.get("appellant_is_bank") else ""
            # appellant — имя стороны-заявителя из карточки 7kas (например,
            # «МТУ Росимущества в Тюменской области, ХМАО-Югре, ЯНАО»).
            # Прогоняем через shorten_party_name — иначе строка «📥 поступила
            # касс. жалоба от Ответчика …» становится непомерно длинной.
            appellant = escape_html(
                shorten_party_name(cass.get("appellant", "") or "", keep_fio_full=_DIGEST_FIO_FULL)
            )
            # Роль заявителя в родительном падеже для строки 3
            # («от ответчика Иванова И.И.») — раньше выходило «от Истец X»
            # (та же болезнь, что «подана Заявитель» в «Итоге», фикс 06.07.2026).
            appellant_status_raw = (cass.get("appellant_status", "") or "").strip()
            _role_title = appellant_status_raw.capitalize()
            appellant_role = escape_html(
                ROLE_GENITIVE.get(_role_title, _role_title.lower())
            ) if _role_title else ""
            filing = escape_html(cass.get("filing_date", "") or "")
            # Строка 1: касс. номер — стороны. Стороны могут быть неизвестны
            # (в карточке 7kas роли участников не свелись к истцу/ответчику) —
            # тогда фрагмент со сторонами не печатаем вовсе, иначе выходило
            # « —  vs ». Заявителя в этом случае покажет строка «📥 поступила
            # касс. жалоба от …» ниже; если и её нет (нет даты поступления) —
            # выносим заявителя прямо в строку 1, чтобы дело было опознаваемо.
            parties_str_d = f"{pl} vs {df}" if (pl and df) else (pl or df or "")
            line1_disc = f"{sber_flag}{link}"
            if parties_str_d:
                line1_disc += f" — {parties_str_d}{tail}"
            elif appellant and not filing:
                line1_disc += f" — заявитель: {appellant}"
                if appellant_status_raw:
                    line1_disc += f" ({escape_html(appellant_status_raw.lower())})"
            cass_block.append(line1_disc)
            # Строка 2: суд 1 инст. + категория. Без номера 1-й инст. и «заявитель».
            court_short = escape_html(
                shorten_court_name(fi_b.get("court", "") or "")
            )
            cat_raw = (cass.get("category") or c.get("category") or "").strip()
            cat = escape_html(short_category_chain(cat_raw))
            line2_disc_parts: list[str] = []
            if court_short:
                line2_disc_parts.append(court_short)
            if cat:
                line2_disc_parts.append(f"категория: {cat}")
            if line2_disc_parts:
                cass_block.append(" | ".join(line2_disc_parts))
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
                    f"<b>{filing}</b> — 📥 поступила касс. жалоба"
                    + from_str
                )
            # Discovery с уже известным исходом: дело нашлось на 7kas
            # постфактум, когда определение уже вынесено/опубликовано.
            # Раньше карточка «нового дела» молчала об исходе — он терялся
            # из дайджеста. Метки — те же, что в «Касс. событиях».
            outcome_d = (cass.get("outcome") or "").strip()
            reason_d = ""
            if outcome_d == "cassation_terminated":
                label_d, reason_d = cassation_terminated_label(
                    cass.get("review_result", ""), cass.get("result_text", "")
                )
            else:
                label_d = CASSATION_OUTCOME_RU.get(outcome_d, "")
            if not label_d:
                label_d = cassation_review_label(
                    cass.get("review_result", ""), outcome_d
                )
            # Куда возвращено при remanded — зеркало «Касс. событий».
            if outcome_d == "cassation_remanded" and label_d:
                _rem_ru_d = _REMANDED_TO_RU.get(
                    (cass.get("remanded_to") or "").strip()
                )
                if _rem_ru_d:
                    label_d += f" → {_rem_ru_d}"
            if label_d == "📥 Принято к производству":
                # Дублирует строку «📥 поступила касс. жалоба» выше.
                label_d = ""
            if label_d:
                itog_line = f"<b>Итог:</b> {escape_html(label_d)}"
                if reason_d:
                    itog_line += f"; {escape_html(reason_d)}"
                cass_block.append(itog_line)
            # Текст мотивировки — из details discovery-change'а (обрезан и
            # задедуплен linking-ом). Change найден, а текста нет — акт уже
            # объявлялся (.cassation_acts) или не опубликован: молчим и
            # summarizer не зовём. Change не найден — legacy-контекст replay,
            # фолбэк на прямое чтение с той же обрезкой 1800.
            _disc_ch = (disc_ch_by_key.get(cass.get("case_number") or "")
                        or disc_ch_by_key.get(c.get("id") or ""))
            if _disc_ch is not None:
                disc_act = ((_disc_ch.get("details") or {})
                            .get("act_text") or "")
            else:
                disc_act = extract_motive_part(cass.get("act_text") or "", 1800)
            disc_excerpt, disc_kind = _act_summary_or_excerpt_with_kind(
                disc_act,
                {
                    "stage": "cassation",
                    "bank_role": role,
                    "verdict_label": label_d,
                    "plaintiff": shorten_party_name(pl_raw, keep_fio_full=True),
                    "defendant": shorten_party_name(df_raw, keep_fio_full=True),
                    "category": cat_raw,
                },
                summarizer=act_summarizer,
                max_excerpt_len=500,
            )
            if disc_excerpt and disc_kind == "summary":
                cass_block.append(f"<b>Почему:</b> <i>{disc_excerpt}</i>")
            elif disc_excerpt:
                cass_block.append(f"<i>{disc_excerpt}</i>")
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
            # URL карточки 7kas (если есть link) — для строки 1.
            url_card = ""
            if d.get("link"):
                cid_, cuid_ = case_id_uid(d["link"])
                if cid_ and cuid_:
                    url_card = CASSATION_COURT.card_url(cid_, cuid_)
            # URL карточки 7kas теперь оборачивает КАССАЦИОННЫЙ номер,
            # не номер 1-й инст. Юрист просил убрать «2-XXX — касс. № 8Г-…»
            # и сразу выводить касс. номер + стороны на строке 1.
            link_html = (
                f'<a href="{url_card}"><b>{num_cs}</b></a>'
                if url_card else f"<b>{num_cs}</b>"
            )
            sber_flag = "🏦 " if d.get("appellant_is_bank") else ""
            # Подтягиваем стороны / категорию / роль / суд 1 инст. из
            # родительского case (в cass_changes.details этих полей нет).
            parent = cases_by_id_for_cass.get(ch.get("case", "")) or {}
            fi_p = parent.get("first_instance") or {}
            pl_raw = _g_cass(parent, "plaintiff", "Истец")
            df_raw = _g_cass(parent, "defendant", "Ответчик")
            pl = escape_html(shorten_party_name(pl_raw, keep_fio_full=_DIGEST_FIO_FULL))
            df = escape_html(shorten_party_name(df_raw, keep_fio_full=_DIGEST_FIO_FULL))
            cat_raw = _g_cass(parent, "category", "Категория")
            cat_short = short_category_chain(cat_raw)
            role_raw = _g_cass(parent, "bank_role", "Роль банка")
            # Строка 1: касс. номер — стороны[, банк — роль]. Хвост «банк — …»
            # — по правилу БАНК В ХВОСТЕ (если Сбербанк в сторонах — нет).
            parties_str = (
                f"{pl} vs {df}" if (pl and df) else (pl or df or "")
            )
            role_tail_l1 = (
                f", банк — {escape_html(role_raw.lower())}"
                if role_raw and not _bank_in_parties(pl_raw, df_raw)
                else ""
            )
            line1_main = f"{sber_flag}{link_html}"
            if parties_str:
                line1_main += f" — {parties_str}{role_tail_l1}"
            else:
                # Сторон у дела нет (типично для дел, заведённых discovery'ем
                # с 7kas: в карточке роли участников не свелись к истцу/
                # ответчику). Без фолбэка строка вырождалась в голый 8Г-номер
                # — по такой записи юрист не понимал, о каком деле речь
                # (инцидент 24.07.2026, 8Г-12479/2026). Показываем заявителя
                # жалобы: он есть в details у любого касс. события.
                appellant_raw = (d.get("appellant", "") or "").strip()
                if appellant_raw:
                    app_short = escape_html(
                        shorten_party_name(appellant_raw, keep_fio_full=_DIGEST_FIO_FULL)
                    )
                    line1_main += f" — заявитель: {app_short}"
                    st_raw = (d.get("appellant_status", "") or "").strip()
                    if st_raw:
                        line1_main += f" ({escape_html(st_raw.lower())})"
                    if role_raw and not _bank_in_parties(appellant_raw, ""):
                        line1_main += f", банк — {escape_html(role_raw.lower())}"
            cass_block.append(line1_main)
            # Строка 2: Суд 1 инст.: ... | категория: ... (без сторон/роли).
            fi_court_raw = (
                (fi_p.get("court") or "")
                or _g_cass(parent, "court", "Суд 1 инстанции")
            )
            line2_parts: list[str] = []
            if fi_court_raw:
                line2_parts.append(
                    f"Суд 1 инст.: {escape_html(shorten_court_name(fi_court_raw))}"
                )
            if cat_short:
                line2_parts.append(f"категория: {escape_html(cat_short)}")
            if line2_parts:
                cass_block.append(" | ".join(line2_parts))
            # Строка 3: «<b>дата</b> — 📥 поступила касс. жалоба от Роль Имя» —
            # ТОЛЬКО на первой линковке карточки 7kas. Единственное место в
            # цикле, где читается ch["type"]: `filing_date` живёт в details у
            # ВСЕХ типов (одна сборка из cass_block), и без гейта строка
            # повторялась бы на каждом последующем событии дела — итоге,
            # акте — как будто жалоба поступает заново. До 31.07.2026 строки
            # не было вовсе: свежая карточка без заседания и итога выходила
            # двумя нейтральными строками, ничего не сообщая (баг 09–31.07).
            # Эмодзи 📥 — ПОСЛЕ <b>дата</b>, иначе строка попадёт под
            # _DIGEST_HEADER_RE и будет принята за заголовок секции.
            arrival_printed = False
            filing_cs = (d.get("filing_date", "") or "").strip()
            if filing_cs and "new_cassation" in (ch.get("type") or []):
                ap_raw_cs = (d.get("appellant", "") or "").strip()
                ap_short_cs = escape_html(
                    shorten_party_name(ap_raw_cs, keep_fio_full=_DIGEST_FIO_FULL)
                )
                # Роль в родительный падеж — как в «Новых касс. делах»
                # («от ответчика Иванова И.И.», не «от Ответчик …»).
                st_title_cs = (d.get("appellant_status", "") or "").strip().capitalize()
                ap_role_cs = escape_html(
                    ROLE_GENITIVE.get(st_title_cs, st_title_cs.lower())
                ) if st_title_cs else ""
                from_cs = ""
                if ap_role_cs and ap_short_cs:
                    from_cs = f" от {ap_role_cs} {ap_short_cs}"
                elif ap_short_cs:
                    from_cs = f" от {ap_short_cs}"
                cass_block.append(
                    f"<b>{escape_html(filing_cs)}</b> — 📥 поступила касс. жалоба"
                    + from_cs
                )
                arrival_printed = True
            # Строка 4: «📅 Назначено судебное заседание на ДД.ММ.ГГГГ в ЧЧ:ММ».
            # Юрист просил полную русскую фразу вместо терсе «📅 Заседание: …».
            # Подавляем при готовом outcome: заседание уже состоялось, итог
            # важнее даты, а формулировка «Назначено …» в прошлом обманывает
            # (выглядит как будущее событие).
            hd = (d.get("hearing_date", "") or "").strip()
            ht = (d.get("hearing_time", "") or "").strip()
            outcome_present = bool((d.get("outcome", "") or "").strip())
            if hd and not outcome_present:
                # Через _fmt_hearing_dt: тот же формат «в ЧЧ:ММ» + скрытие
                # заглушки 00:00 (единственное место, где дата+время
                # клеились вручную).
                hearing_str = f"<b>{escape_html(_fmt_hearing_dt(hd, ht))}</b>"
                cass_block.append(
                    f"📅 Назначено судебное заседание на {hearing_str}"
                )
            # «Без движения» (13.08.2026): срок устранения недостатков —
            # строка только при типе cass_suspended (сам suspended_until в
            # details есть у любого события, повторять его всякий раз не надо).
            suspended_printed = False
            su_cs = (d.get("suspended_until", "") or "").strip()
            if su_cs and "cass_suspended" in (ch.get("type") or []):
                cass_block.append(
                    "⏸ жалоба оставлена без движения — срок устранения "
                    f"недостатков до <b>{escape_html(su_cs)}</b>"
                )
                suspended_printed = True
            # Строка 5: Итог — готовая подпись из CASSATION_OUTCOME_RU /
            # cassation_review_label. + «от Роль Имя» из заявителя. Для
            # cassation_terminated раскрываем общую метку до конкретики
            # (возврат / прекращение / отзыв) + причина.
            outcome = d.get("outcome", "") or ""
            outcome_reason_ru = ""
            if outcome == "cassation_terminated":
                outcome_label_ru, outcome_reason_ru = cassation_terminated_label(
                    d.get("review_result", ""), d.get("result_text", "")
                )
            else:
                outcome_label_ru = CASSATION_OUTCOME_RU.get(outcome, "")
            review_label_ru = cassation_review_label(
                d.get("review_result", ""), outcome
            )
            label = outcome_label_ru or review_label_ru
            # Куда возвращено при remanded (13.08.2026): enum вычислялся
            # всегда, но никуда не выводился — юрист не знал, ждать ли дело
            # в апелляции или в 1-й инстанции. Неизвестное значение — без
            # хвоста (replay-safe).
            if outcome == "cassation_remanded" and label:
                _rem_ru = _REMANDED_TO_RU.get(
                    (d.get("remanded_to", "") or "").strip()
                )
                if _rem_ru:
                    label += f" → {_rem_ru}"
            # Подавляем стадийный маркер «Принято к производству», если уже
            # есть строка с датой заседания — повторять «принято» избыточно,
            # юрист и так видит, что заседание назначено. Строка поступления
            # гасит его по той же причине: карточка часто приезжает сразу с
            # «ВОЗБУЖДЕНО КАССАЦИОННОЕ ПРОИЗВОДСТВО…» (cassation_review_label),
            # и вышли бы два «📥» подряд об одном и том же. Строка «без
            # движения» — тоже: жалоба очевидно принята, раз суд дал срок.
            if ((hd or arrival_printed or suspended_printed)
                    and label == "📥 Принято к производству"):
                label = ""
            if label:
                # Сокращаем имя заявителя (та же причина, что и в секции
                # «Новые касс. дела»): громоздкие «МТУ Росимущества в …»
                # ломают строку Итог.
                appellant = shorten_party_name(
                    (d.get("appellant", "") or "").strip(), keep_fio_full=_DIGEST_FIO_FULL
                )
                ap_status = (d.get("appellant_status", "") or "").strip()
                # «(жалоба заявителя ЖНК Единство)» вместо оборванного
                # «; подана Заявителем X» (переформулировка 06.07.2026:
                # видно, что речь о жалобе). Роль в родительный падеж через
                # ROLE_GENITIVE (нижний регистр — середина фразы); имя
                # стороны без изменений (склонение фамилий — отдельная
                # история). Если у нас только имя без роли — «(жалоба X)».
                from_str = ""
                if appellant and ap_status:
                    role_title = ap_status.capitalize()
                    role_gen = ROLE_GENITIVE.get(role_title, role_title.lower())
                    from_str = f" (жалоба {escape_html(role_gen)} {escape_html(appellant)})"
                elif appellant:
                    from_str = f" (жалоба {escape_html(appellant)})"
                reason_tail = (
                    f"; {escape_html(outcome_reason_ru)}"
                    if outcome_reason_ru else ""
                )
                cass_block.append(
                    f"<b>Итог:</b> {escape_html(label)}{from_str}{reason_tail}"
                )
            # Строка 5: Почему — пересказ мотивировки через act_summarizer.
            # Сокращаем имена сторон: pl_raw/df_raw — сырые поля parent case,
            # для LLM-пересказа они слишком длинные («МТУ Росимущества в …»).
            act_excerpt, act_kind = _act_summary_or_excerpt_with_kind(
                d.get("act_text") or "",
                {
                    "stage": "cassation",
                    "bank_role": role_raw,
                    "verdict_label": label,
                    "plaintiff": shorten_party_name(pl_raw, keep_fio_full=True),
                    "defendant": shorten_party_name(df_raw, keep_fio_full=True),
                    "category": cat_raw,
                },
                summarizer=act_summarizer,
                max_excerpt_len=500,
            )
            # LLM-пересказ — с маркером «Почему:» (контракт attach_act_analyses
            # и drawer'а); сырой excerpt — просто курсивом.
            if act_excerpt and act_kind == "summary":
                cass_block.append(f"<b>Почему:</b> <i>{act_excerpt}</i>")
            elif act_excerpt:
                cass_block.append(f"<i>{act_excerpt}</i>")
            cass_block.append("")
        if cass_block and cass_block[-1] == "":
            cass_block.pop()

    if cass_block:
        lines.extend(["", ""])
        lines.append("⚖️🔬 <b>КАССАЦИЯ</b>")
        lines.append("")
        lines.extend(_air_after_subsection_headers(cass_block))

    # ── Блок ИСКИ БАНКА — последним: при обрезке Telegram (~7600 симв.)
    # страдает первым, основная повестка выживает.
    if bank_changes:
        lines.extend(["", ""])
        lines.extend(_bank_track_block(bank_changes,
                                       act_summarizer=act_summarizer))

    lines.extend(["", ""])
    lines.append(
        f"📌 <b>В производстве: всего {total_active}"
        f" (1 инст.: {total_active_fi} | апел.: {total_active_appeal}"
        f" | касс.: {total_active_cassation})"
        + bank_footer_tail + "</b>"
    )
    lines.append(f'<a href="{config.DASHBOARD_URL}">📊 Дашборд</a>')

    text = "\n".join(lines)
    # HTML не обрезаем: дашборд рендерит дайджест целиком, а send_telegram
    # через split_message сам разложит его на сообщения по лимиту Telegram.
    # Раньше здесь стоял truncate_html_message(…, 2×4096) — на многособытийных
    # днях он резал хвост дайджеста (вплоть до футера и ссылки на дашборд).
    return _close_open_tags(text)


# ── Telegram ─────────────────────────────────────────────────────────────────
