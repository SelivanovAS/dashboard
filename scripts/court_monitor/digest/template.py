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
from court_monitor.digest.postprocess import truncate_html_message
from court_monitor.lifecycle import (
    bank_side_outcome, bank_side_outcome_fi,
    _is_event_text_in_result_field, _fi_return_reason_for_render,
)
from court_monitor.parsing import (
    CASSATION_OUTCOME_RU, cassation_review_label, cassation_terminated_label,
)
from court_monitor.storage import load_json
from court_monitor.textutil import (
    escape_html, shorten_party_name, shorten_court_name, _bare_case_number,
    parties_short, parse_date, case_id_uid, ROLE_INSTRUMENTAL,
)

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
        parts.append(f"{events} событ.")
    if postponed:
        parts.append(f"{postponed} отлож.")
    if to_fi_rules:
        parts.append(f"{to_fi_rules} перех. к 1-й инст.")
    if results:
        parts.append(f"{results} суд. акт.")
    if acts:
        parts.append(f"{acts} акт.")
    if app_status:
        parts.append(f"{app_status} статус апел.")
    if fi_changes:
        fi_hearings = sum(
            1 for ch in fi_changes
            if ("fi_hearing_new" in ch["type"]
                or "fi_hearing_next" in ch["type"]
                or "fi_hearing_postponed" in ch["type"]
                or "fi_hearing_recess" in ch["type"])
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
        fi_returns = sum(
            1 for ch in fi_changes if "fi_returned" in ch["type"]
        )
        fi_cass_filed = sum(
            1 for ch in fi_changes if "fi_cassation_filed" in ch["type"]
        )
        fi_sent_cass = sum(
            1 for ch in fi_changes if "fi_sent_to_cassation" in ch["type"]
        )
        fi_accepted = sum(
            1 for ch in fi_changes if "fi_accepted_no_hearing" in ch["type"]
        )
        # fi_bank_role_changed в сводку осознанно НЕ выносим: смена роли —
        # редкий служебный признак, строка в 3.2 «Изменения» его уже несёт.
        if fi_hearings:
            parts.append(f"{fi_hearings} засед. 1 инст.")
        if fi_restarts:
            parts.append(f"{fi_restarts} с начала")
        if fi_resolved_n:
            parts.append(f"{fi_resolved_n} реш. 1 инст.")
        if fi_returns:
            parts.append(f"{fi_returns} возвр. исков")
        if fi_appeals_filed:
            parts.append(f"{fi_appeals_filed} подано жалоб")
        if fi_cass_filed:
            parts.append(f"{fi_cass_filed} касс. жалоб")
        if fi_sent_cass:
            parts.append(f"{fi_sent_cass} в касс. суд")
        if fi_accepted:
            parts.append(f"{fi_accepted} принято к пр-ву")
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
            cat = category_short(short_category_chain(c.get("category", "")))
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
                elif t == "fi_hearing_recess":
                    new_p = escape_html(
                        d.get("hearing_date", "")
                        + (f" {d['hearing_time']}" if d.get("hearing_time") else "")
                    )
                    ev_list.append(f"🔁 в заседании объявлен перерыв до {new_p}")
                elif t == "fi_status_change":
                    ev_list.append(
                        f"статус: {escape_html(d.get('old_status', ''))} → "
                        f"{escape_html(d.get('new_status', ''))}"
                    )
                elif t == "fi_returned":
                    reason = escape_html(_fi_return_reason_for_render(d))
                    ev_list.append(
                        "🔚 иск возвращён" + (f": {reason}" if reason else "")
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
                        # Запланированная дата ближайшего заседания (для
                        # «подготовки дела»/«беседы»/«предварительного
                        # заседания») — юристу нужна, к когда готовиться.
                        sh_d = escape_html(
                            d.get("scheduled_hearing_date", "")
                        )
                        sh_t = escape_html(
                            d.get("scheduled_hearing_time", "")
                        )
                        if sh_d:
                            sh_p = sh_d + (f" {sh_t}" if sh_t else "")
                            ev_list.append(
                                f"📅 заседание назначено на {sh_p}"
                            )
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
            cat = escape_html(category_short(short_category_chain(d.get("category", ""))))
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
            # Если в том же change есть fi_bank_role_changed (банк исключён
            # из ответчиков / переведён в 3-е лицо) — явно поясняем нейтралитет,
            # т.к. иначе юрист видит «Иск удовлетворён» и думает, что это
            # против банка. _bank_in_parties может вернуть True (банк всё ещё
            # упоминается в defendant-строке поиска), поэтому хвост «банк — роль»
            # не выводится сам по себе.
            if "fi_bank_role_changed" in ch["type"]:
                extras.append(
                    "<b>для банка:</b> нейтрально — банк не сторона согласно карточке"
                )
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
            fi_block.append(f"  {link}: {pl} vs {df}")
            itog_parts: list[str] = []
            if verdict:
                itog_parts.append(f"<b>Итог:</b> {verdict}")
            if bank_out:
                itog_parts.append(f"<b>Для банка:</b> {bank_out}")
            if "fi_bank_role_changed" in ch["type"]:
                itog_parts.append(
                    "<b>Для банка:</b> нейтрально — банк не сторона согласно карточке"
                )
            if itog_parts:
                fi_block.append("     " + ". ".join(itog_parts))
            # LLM-пересказ — с маркером «Почему:» (контракт attach_act_analyses
            # и drawer'а карточки дела); сырой excerpt — просто курсивом.
            if act_excerpt and act_kind == "summary":
                fi_block.append(f"     <b>Почему:</b> <i>{act_excerpt}</i>")
            elif act_excerpt:
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
            cat = category_short(short_category_chain(c.get("Категория", "")))
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
            line = f"  ⚠ {link}"
            if tr_dt:
                line += f" ({tr_dt})"
            line += " — по правилам производства в суде первой инстанции"
            if plaintiff and defendant:
                line += f"\n     {plaintiff} vs {defendant}{role_note}"
            appeal_block.append(line)
            # Пустая строка между делами (правило вёрстки юриста).
            appeal_block.append("")
        if appeal_block and appeal_block[-1] == "":
            appeal_block.pop()

    # Объединяем «Отложенные» и «Назначенные» апелляции в одну секцию
    # «📅 Изменения» (по запросу юриста, как 3.2 в 1-й инст.). Формат —
    # три строки на дело: номер; стороны + категория; «🔁 Заседание
    # отложено на …» / «📅 Заседание назначено на …». `events` уже
    # исключает дела из `postponed_nums`, дублирования нет.
    # Сюда же — «голые» status_change (строка 3: «статус: X → Y»).
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
            cat = category_short(short_category_chain(d.get("category", "")))
            is_postponed = "hearing_postponed" in ch["type"]
            # Дата+время заседания. Для отложений — new_hearing_*; для
            # назначений берём new_hearing_* при наличии, иначе из event_raw
            # пытаемся вытащить (для legacy-новых событий).
            hd = escape_html(d.get("new_hearing_date", ""))
            ht = escape_html(d.get("new_hearing_time", ""))
            if not hd and not is_postponed:
                event_raw = d.get("event", "") or ""
                # Из «Судебное заседание. 11:30. 03.06.2026» вытаскиваем
                # дату и время.
                for p in event_raw.split(". "):
                    ps = p.strip()
                    if parse_date(ps) and not hd:
                        hd = escape_html(ps)
                    elif re.match(r'^\d{1,2}:\d{2}$', ps) and not ht:
                        ht = escape_html(ps)
            hp = hd + (f" {ht}" if ht else "")
            # Строка 1: только номер дела (суд не показываем — для
            # апелляции это всегда Суд ХМАО-Югры).
            appeal_block.append(f"  {link}")
            # Строка 2: стороны | категория.
            line2_parts: list[str] = []
            if plaintiff and defendant:
                line2_parts.append(f"{plaintiff} vs {defendant}")
            if cat:
                line2_parts.append(f"категория: {escape_html(cat)}")
            if line2_parts:
                appeal_block.append("     " + " | ".join(line2_parts))
            # Строка 3: 🔁 отложено / 📅 назначено. Если дату вытащить не
            # удалось — показываем сырой текст события (иначе карточка дела
            # информационно пуста: номер и стороны без сути). «📌 текст» без
            # <b> не ловится _DIGEST_HEADER_RE — за заголовок не примут.
            # Для «голого» status_change — строка «статус: X → Y» (формат
            # как в 3.2 первой инстанции).
            if hp:
                if is_postponed:
                    appeal_block.append(
                        f"     🔁 Заседание отложено на <b>{hp}</b>"
                    )
                else:
                    appeal_block.append(
                        f"     📅 Заседание назначено на <b>{hp}</b>"
                    )
            elif ("new_event" in ch["type"]
                    and (d.get("event") or "").strip()):
                appeal_block.append(
                    f"     📌 {escape_html(d['event'].strip())}"
                )
            elif "status_change" in ch["type"]:
                appeal_block.append(
                    f"     статус: {escape_html(d.get('old_status', ''))} → "
                    f"{escape_html(d.get('new_status', ''))}"
                )
            # Пустая строка между делами (правило вёрстки юриста: формат
            # трёхстрочный, без разделителя дела визуально слипаются).
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
            # БАНК В ХВОСТЕ: показываем «(банк — роль)» только когда банк не в сторонах.
            if role and not _bank_in_parties(
                    d.get("plaintiff", ""), d.get("defendant", "")):
                role_note = f" (банк — {escape_html(role.lower())})"
            else:
                role_note = ""
            hearing_dt = d.get("hearing_date", "")
            date_note = f". Определение от {escape_html(hearing_dt)}" if hearing_dt else ""
            cat = category_short(short_category_chain(d.get("category", "")))
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
            appeal_block.append(f"  {link}")
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
                appeal_block.append("     " + ". ".join(itog55))
            # LLM-пересказ — с маркером «Почему:» (контракт attach_act_analyses
            # и drawer'а); сырой excerpt — по-старому «Мотивировка: …».
            if summary_or_excerpt and sum_kind == "summary":
                appeal_block.append(
                    f"     <b>Почему:</b> <i>{summary_or_excerpt}</i>"
                )
            elif summary_or_excerpt:
                appeal_block.append(f"     Мотивировка: {summary_or_excerpt}")
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
    # Готовые подписи исхода берём из module-level CASSATION_OUTCOME_RU
    # (см. рядом с classify_cassation_outcome). Так LLM-ветка и template-ветка
    # дайджеста используют один и тот же словарь — без дублирования и расхождений.
    # Словарь cases-by-id для подтягивания plaintiff/defendant/category/
    # bank_role/first_instance.court по родительскому case (в cass_changes.details
    # этих полей нет — раньше шаблон выводил пустые «{не указаны}»).
    # cass_changes ссылаются на FI-номер. Подгружаем актуальный cases.json
    # (JSON-формат с FI-делами) — переданный `cases` может быть в legacy
    # CSV-формате и содержать только апел. дела (33-XXXX), что для касс.
    # событий с FI-ключами не подходит.
    try:
        full_cases_for_cass = load_json(config.JSON_PATH).get("cases", []) or []
    except (OSError, json.JSONDecodeError):
        full_cases_for_cass = []
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
            # appellant — имя стороны-заявителя из карточки 7kas (например,
            # «МТУ Росимущества в Тюменской области, ХМАО-Югре, ЯНАО»).
            # Прогоняем через shorten_party_name — иначе строка «📥 поступила
            # касс. жалоба от Ответчика …» становится непомерно длинной.
            appellant = escape_html(
                shorten_party_name(cass.get("appellant", "") or "", keep_fio_full=True)
            )
            # Роль заявителя в Title Case для строки 3 («от Ответчика Иванова»).
            appellant_status_raw = (cass.get("appellant_status", "") or "").strip()
            appellant_role = escape_html(appellant_status_raw.capitalize())
            # Строка 2: суд 1 инст. + категория. Без номера 1-й инст. и «заявитель».
            court_short = escape_html(
                shorten_court_name(fi_b.get("court", "") or "")
            )
            cat_raw = (cass.get("category") or c.get("category") or "").strip()
            cat = escape_html(short_category_chain(cat_raw))
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
            if label_d == "📥 Принято к производству":
                # Дублирует строку «📥 поступила касс. жалоба» выше.
                label_d = ""
            if label_d:
                itog_line = f"     <b>Итог:</b> {escape_html(label_d)}"
                if reason_d:
                    itog_line += f"; {escape_html(reason_d)}"
                cass_block.append(itog_line)
            disc_excerpt, disc_kind = _act_summary_or_excerpt_with_kind(
                cass.get("act_text") or "",
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
                cass_block.append(f"     <b>Почему:</b> <i>{disc_excerpt}</i>")
            elif disc_excerpt:
                cass_block.append(f"     <i>{disc_excerpt}</i>")
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
            pl = escape_html(shorten_party_name(pl_raw, keep_fio_full=True))
            df = escape_html(shorten_party_name(df_raw, keep_fio_full=True))
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
            line1_main = f"  {sber_flag}{link_html}"
            if parties_str:
                line1_main += f" — {parties_str}{role_tail_l1}"
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
                cass_block.append("     " + " | ".join(line2_parts))
            # Строка 3: «📅 Назначено судебное заседание на ДД.ММ.ГГГГ в ЧЧ:ММ».
            # Юрист просил полную русскую фразу вместо терсе «📅 Заседание: …».
            # Подавляем при готовом outcome: заседание уже состоялось, итог
            # важнее даты, а формулировка «Назначено …» в прошлом обманывает
            # (выглядит как будущее событие).
            hd = (d.get("hearing_date", "") or "").strip()
            ht = (d.get("hearing_time", "") or "").strip()
            outcome_present = bool((d.get("outcome", "") or "").strip())
            if hd and not outcome_present:
                if ht:
                    hearing_str = f"<b>{escape_html(hd)} в {escape_html(ht)}</b>"
                else:
                    hearing_str = f"<b>{escape_html(hd)}</b>"
                cass_block.append(
                    f"     📅 Назначено судебное заседание на {hearing_str}"
                )
            # Строка 4: Итог — готовая подпись из CASSATION_OUTCOME_RU /
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
            # Подавляем стадийный маркер «Принято к производству», если уже
            # есть строка с датой заседания — повторять «принято» избыточно,
            # юрист и так видит, что заседание назначено.
            if hd and label == "📥 Принято к производству":
                label = ""
            if label:
                # Сокращаем имя заявителя (та же причина, что и в секции
                # «Новые касс. дела»): громоздкие «МТУ Росимущества в …»
                # ломают строку Итог.
                appellant = shorten_party_name(
                    (d.get("appellant", "") or "").strip(), keep_fio_full=True
                )
                ap_status = (d.get("appellant_status", "") or "").strip()
                # «; подана Ответчиком Ивановым И.И.» вместо корявого
                # «— от Ответчика Иванова И.И.». Роль в творительный падеж
                # через ROLE_INSTRUMENTAL; имя стороны оставляем без изменений
                # (склонение фамилий — отдельная история). Если у нас только
                # имя без роли — пишем «подана X» без падежа роли.
                from_str = ""
                if appellant and ap_status:
                    role_title = ap_status.capitalize()
                    role_instr = ROLE_INSTRUMENTAL.get(role_title, role_title)
                    from_str = f"; подана {escape_html(role_instr)} {escape_html(appellant)}"
                elif appellant:
                    from_str = f"; подана {escape_html(appellant)}"
                reason_tail = (
                    f"; {escape_html(outcome_reason_ru)}"
                    if outcome_reason_ru else ""
                )
                cass_block.append(
                    f"     <b>Итог:</b> {escape_html(label)}{from_str}{reason_tail}"
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
                cass_block.append(f"     <b>Почему:</b> <i>{act_excerpt}</i>")
            elif act_excerpt:
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
    lines.append(f'<a href="{config.DASHBOARD_URL}">📊 Дашборд</a>')

    text = "\n".join(lines)
    # До двух сообщений: лимит 2×4096; split_message в send_telegram разобьёт
    return truncate_html_message(text, config.TELEGRAM_MSG_LIMIT * 2)


# ── Telegram ─────────────────────────────────────────────────────────────────
