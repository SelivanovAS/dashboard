# -*- coding: utf-8 -*-
"""LLM-слой дайджеста: Claude и GigaChat (простые вызовы и полировщик),
пересказ мотивировок судебных актов (summarize_act_motivation) с кэшем,
LLM-полировка готового HTML (polish_digest_html) с валидатором контракта.

⚠ Тексты промптов (GIGACHAT_SYSTEM_PROMPT, _build_act_summary_prompt,
_DIGEST_POLISH_SYSTEM_PROMPT) юрист настраивал долго — не менять ни на символ.

Патчабельные тестами функции (_call_claude_simple, _call_claude_polish,
polish_digest_html, summarize_act_motivation) из других модулей вызываются
только как llm.X(...) — патч этого модуля ловит все пути вызова.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime

import requests

from court_monitor import config
from court_monitor.config import log
from court_monitor.storage import _load_act_summaries, _save_act_summaries
from court_monitor.textutil import _bare_case_number

# ── GigaChat API — альтернативный провайдер для digest_only ───────────────────

def _gigachat_access_token() -> str | None:
    """Получить OAuth access token GigaChat. Живёт 30 минут.

    Токен не кешируем: дайджест-раны короткие и одноразовые, а держать
    кеш между запусками workflow негде. Verify=False — на ubuntu-latest нет
    корневого сертификата Минцифры РФ, которым подписан ngw.devices.sberbank.ru.
    """
    if not config.GIGACHAT_AUTH_KEY:
        log.warning("GIGACHAT_AUTH_KEY не задан")
        return None
    try:
        import uuid
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = requests.post(
            config.GIGACHAT_OAUTH_URL,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "RqUID": str(uuid.uuid4()),
                "Authorization": f"Basic {config.GIGACHAT_AUTH_KEY}",
            },
            data={"scope": config.GIGACHAT_SCOPE},
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
            config.GIGACHAT_API_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json={
                "model": config.GIGACHAT_MODEL,
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
        f"Перед тобой текст мотивировочной части ({kind}). "
        + (f"Контекст: {meta_str}. " if meta_str else "")
        + "Задача: одной короткой фразой — что стало РЕШАЮЩИМ аргументом "
        "суда. То, ради чего юрист откроет акт.\n\n"
        "ЖЁСТКО:\n"
        "- ровно одно предложение, до 220 символов;\n"
        "- начинай со сути, без процедуры;\n"
        "- без оборотов «суд указал», «суд применил», «суд установил», "
        "«суд подтвердил», «было установлено»;\n"
        "- без перечисления статей закона;\n"
        "- без сторон по имени и без названий организаций — они в шапке;\n"
        "- без фразы «Для банка», «Кратко:», «Резюме:», «Главное:»;\n"
        "- без эмодзи, HTML-тегов, Markdown, кавычек по краям.\n\n"
        "ПЛОХО: «Суд применил ст. 331 ГПК РФ о проверке решения. Сбербанк "
        "взыскивал задолженность по кредиту 5 млн рублей. Апелляционный "
        "суд подтвердил надлежащее исполнение банком обязательств. "
        "Решающим обстоятельством стало наличие действительного договора "
        "поручительства и ненадлежащее исполнение обязательств "
        "заёмщиком.»\n"
        "ХОРОШО: «Договор поручительства действителен, неисполнение "
        "заёмщиком установлено; поручитель не привёл возражений по "
        "существу обязательства.»\n\n"
        "ПЛОХО: «Суд применил ст. 167 ГПК РФ. Истец просил освободить "
        "автомобиль от ареста. Апелляционный суд отклонил довод о "
        "возникновении права собственности, поскольку запрет наложен в "
        "рамках ИП. Решающим обстоятельством стало отсутствие "
        "доказательств приобретения имущества до возникновения "
        "обязательства перед банком.»\n"
        "ХОРОШО: «Истец не доказал, что приобрёл автомобиль до наложения "
        "ареста по исполнительному производству должника.»\n\n"
        f"ТЕКСТ АКТА:\n{act_text}"
    )


def _call_claude_simple(
    prompt: str, *, max_tokens: int = 400, temperature: float = 0.2
) -> str | None:
    """Минимальный вызов Anthropic API. Возвращает текст или None.

    Дублирует часть `generate_digest`, но с маленьким max_tokens и без
    post-обработки HTML — для пересказа мотивировки нужен plain text.
    """
    if not config.ANTHROPIC_API_KEY:
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": config.ANTHROPIC_API_KEY,
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
            config.GIGACHAT_API_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json={
                "model": config.GIGACHAT_MODEL,
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

    # Версия "v2-ratio" в ключе: новый промпт (май 2026) требует одно
    # предложение ratio без «Для банка»; старые многословные пересказы из
    # кэша не должны возвращаться.
    key = hashlib.sha1((act + "|v2-ratio").encode("utf-8")).hexdigest()[:16]
    cache = _load_act_summaries() if use_cache else {}
    if use_cache and key in cache:
        cached_summary = (cache[key] or {}).get("summary")
        if cached_summary:
            return cached_summary

    prompt = _build_act_summary_prompt(act, case_meta)
    if config.LLM_PROVIDER == "gigachat":
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
    "5. Склонение ролей в касс. жалобе:\n"
    "   — в строке поступления («поступила касс. жалоба от …») — родительный "
    "падеж: «от Ответчик X» → «от Ответчика X», «от Истец X» → «от Истца X», "
    "«от Третье лицо X» → «от третьего лица X»;\n"
    "   — в строке Итог («…; подана …») — творительный падеж: «подана Ответчик "
    "X» → «подана Ответчиком X», «подана Истец X» → «подана Истцом X», "
    "«подана Иное лицо X» → «подана Иным лицом X», «подана Третье лицо X» → "
    "«подана Третьим лицом X».\n"
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
        # Шаблон рендерит КАССАЦИОННЫЙ внутренний номер (8Г-…), а не номер
        # 1-й инст. (по просьбе юриста, см. блок КАССАЦИЯ в template.py).
        # Валидатор должен требовать тот номер, что реально виден в HTML —
        # иначе полировщик ложно откатывался на любом касс. событии.
        n = (ch.get("cassation_internal_number")
             or ch.get("case") or "").strip()
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
    if config.DASHBOARD_URL not in polished:
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
    max_length = config.TELEGRAM_MSG_LIMIT * 2

    user_prompt = f"ЧЕРНОВИК HTML:\n\n{draft}"
    if config.LLM_PROVIDER == "gigachat":
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
    if not config.ANTHROPIC_API_KEY:
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": config.ANTHROPIC_API_KEY,
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
            config.GIGACHAT_API_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json={
                "model": config.GIGACHAT_MODEL,
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


def _current_digest_model_name() -> str:
    """Имя модели, которой только что генерили дайджест — для метки
    `act_analysis.model`. Совпадает с тем, что реально использовалось в
    `generate_digest()`."""
    if config.LLM_PROVIDER == "gigachat":
        return f"gigachat:{config.GIGACHAT_MODEL}"
    return "claude-haiku-4-5-20251001"
