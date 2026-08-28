# -*- coding: utf-8 -*-
"""LLM-слой дайджеста: Claude, GigaChat и OpenRouter (простые вызовы и
полировщик), пересказ мотивировок судебных актов (summarize_act_motivation)
с кэшем, LLM-полировка готового HTML (polish_digest_html) с валидатором
контракта.

⚠ Тексты промптов (GIGACHAT_SYSTEM_PROMPT, _build_act_summary_prompt,
_DIGEST_POLISH_SYSTEM_PROMPT) юрист настраивал долго — не менять ни на символ.

Патчабельные тестами функции (_call_claude_simple, _call_claude_polish,
_call_openrouter_simple, _call_openrouter_polish, _call_openrouter_digest,
polish_digest_html, summarize_act_motivation) из других модулей вызываются
только как llm.X(...) — патч этого модуля ловит все пути вызова.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import datetime

import requests

from court_monitor import config
from court_monitor.config import log
from court_monitor.storage import _load_act_summaries, _save_act_summaries
from court_monitor.textutil import _bare_case_number


# ── Настроен ли LLM ──────────────────────────────────────────────────────────

# Флаг «уже сказали, что LLM не настроен»: на прогоне без ключа предупреждение
# иначе печаталось бы на каждый акт (6 одинаковых строк за утро).
_llm_not_configured_reported = False


def missing_llm_key_name() -> str | None:
    """Имя незаданного ключа для текущего LLM_PROVIDER; None — ключ есть.

    Единственное место, где живёт соответствие «провайдер → его ключ»:
    отсюда его берут и `validate_environment` (ей нужно ИМЯ переменной для
    сообщения об ошибке — потому предикат возвращает строку, а не bool), и
    гарды пересказа/разбора актов. Копий не заводить: проект дважды ловил
    молча разъехавшиеся дубли одного правила.

    Практический смысл — Mac-резерв: ключей на машине юриста нет намеренно
    (LLM-дайджест делает GitHub-replay), и без предиката прогон считал бы
    ненастроенный провайдер отказом провайдера.
    """
    if config.LLM_PROVIDER == "gigachat":
        return None if config.GIGACHAT_AUTH_KEY else "GIGACHAT_AUTH_KEY"
    if config.LLM_PROVIDER == "openrouter":
        return None if config.OPENROUTER_API_KEY else "OPENROUTER_API_KEY"
    return None if config.ANTHROPIC_API_KEY else "ANTHROPIC_API_KEY"


def llm_is_configured() -> bool:
    """Есть ли ключ у текущего провайдера (обёртка над missing_llm_key_name)."""
    return missing_llm_key_name() is None


def _report_llm_not_configured(missing: str) -> None:
    """Одна строка за процесс: почему пересказов не будет."""
    global _llm_not_configured_reported
    if _llm_not_configured_reported:
        return
    _llm_not_configured_reported = True
    log.info(
        f"LLM не настроен (нет {missing}) — пересказы мотивировок пропускаем, "
        f"их сделает GitHub-replay"
    )


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


def _gigachat_api_url() -> str:
    """URL chat/completions под текущую модель GigaChat.

    Модели 3-го поколения (GigaChat-3-Ultra) доступны только на базовом
    адресе api.giga.chat; остальные — на gigachat.devices.sberbank.ru.
    Токен OAuth общий (ngw.devices.sberbank.ru, scope GIGACHAT_API_PERS).
    """
    if config.GIGACHAT_MODEL.strip().lower().startswith("gigachat-3"):
        return config.GIGACHAT_V3_API_URL
    return config.GIGACHAT_API_URL


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
            _gigachat_api_url(),
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


# ── OpenRouter API — третий провайдер (тестовый контур) ──────────────────────
# API OpenAI-совместимый, как у GigaChat, но без OAuth (Bearer-ключ) и со
# штатной проверкой TLS. Функции зеркальны gigachat-парам, чтобы не трогать
# патч-цели существующих тестов.

# Мемо резолва модели по рейтингу на процесс: один HTTP-запрос на прогон,
# мемоизируется и fallback (иначе дайджест с N актами сделал бы N
# неудачных запросов к shir-man.com).
_openrouter_resolved_model: str | None = None

# «Топ-N рейтинга» в OPENROUTER_MODEL (значения выпадающего списка
# test_digest.yml): конкретная модель подставляется на прогоне из свежего
# рейтинга — список в форме GitHub статичен и иначе протухал бы.
_OPENROUTER_RANK_RE = re.compile(r"^топ[\s-]*(\d+)", re.IGNORECASE)


def _openrouter_requested_rank() -> int | None:
    """Разобрать config.OPENROUTER_MODEL как место в рейтинге бесплатных
    моделей: пусто / «модель дня…» / «авто…» → 1, «топ-N…» → N.
    Любая другая строка — буквальный id модели → None.
    """
    raw = (config.OPENROUTER_MODEL or "").strip().lower()
    if not raw or raw.startswith("модель дня") or raw.startswith("авто"):
        return 1
    m = _OPENROUTER_RANK_RE.match(raw)
    if m:
        return max(1, int(m.group(1)))
    return None


def _resolve_openrouter_model() -> str:
    """Определить модель OpenRouter для текущего прогона.

    config.OPENROUTER_MODEL может быть: буквальным id модели (из текстового
    поля llm_model) — возвращается как есть; местом в рейтинге («модель дня
    (топ-1)», «топ-3» — значения выпадающего списка) или пустым (= топ-1) —
    тогда конкретный id берётся из свежего рейтинга бесплатных моделей
    config.OPENROUTER_TOP_MODELS_URL (models[N-1].id; если в рейтинге меньше
    N строк — последняя доступная). При недоступности рейтинга —
    config.OPENROUTER_FALLBACK_MODEL (маршрут openrouter/free, OpenRouter
    сам подбирает живую бесплатную модель).
    """
    global _openrouter_resolved_model
    rank = _openrouter_requested_rank()
    if rank is None:
        return config.OPENROUTER_MODEL
    if _openrouter_resolved_model:
        return _openrouter_resolved_model
    try:
        r = requests.get(config.OPENROUTER_TOP_MODELS_URL, timeout=15)
        r.raise_for_status()
        models = [m for m in (r.json().get("models") or []) if m]
        if not models:
            raise ValueError("пустой список models")
        if rank > len(models):
            log.warning(
                f"OpenRouter: в рейтинге только {len(models)} моделей, "
                f"топ-{rank} недоступен — беру последнюю"
            )
        model_id = (models[min(rank, len(models)) - 1].get("id") or "").strip()
        if not model_id:
            raise ValueError("пустой id модели в рейтинге")
        log.info(f"OpenRouter: топ-{rank} рейтинга — {model_id}")
        _openrouter_resolved_model = model_id
    except (requests.RequestException, KeyError, ValueError, IndexError,
            json.JSONDecodeError) as e:
        log.warning(
            f"OpenRouter: не удалось получить рейтинг моделей ({e}), "
            f"fallback {config.OPENROUTER_FALLBACK_MODEL}"
        )
        _openrouter_resolved_model = config.OPENROUTER_FALLBACK_MODEL
    return _openrouter_resolved_model


def _call_openrouter_chat(
    messages: list[dict], *, max_tokens: int, temperature: float,
    model: str | None = None,
) -> str | None:
    """Низкоуровневый chat/completions-вызов OpenRouter.

    model — переопределение модели (фолбэк-контур пересказов);
    None → _resolve_openrouter_model().

    Возвращает текст ответа или None при любой ошибке — вызывающая сторона
    откатывается так же, как при ошибке Claude/GigaChat.
    """
    if not config.OPENROUTER_API_KEY:
        log.warning("OPENROUTER_API_KEY не задан")
        return None
    model_id = model or _resolve_openrouter_model()
    try:
        r = requests.post(
            config.OPENROUTER_API_URL,
            headers={
                "Authorization": f"Bearer {config.OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": model_id,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "messages": messages,
            },
            timeout=60,
        )
        r.raise_for_status()
        data = r.json()
        choices = data.get("choices") or []
        if not choices:
            # Молчать нельзя: без лога такой сбой в прогоне неотличим от
            # «модель ответила пусто» уровнем выше.
            log.warning(f"OpenRouter API ({model_id}): пустой список choices в ответе")
            return None
        text = (choices[0].get("message", {}) or {}).get("content", "").strip()
        if not text:
            log.warning(f"OpenRouter API ({model_id}): пустой content в ответе модели")
            return None
        return text
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        body = (e.response.text or "")[:500] if e.response is not None else ""
        log.warning(f"OpenRouter API HTTP {status}: {body}")
        return None
    except (requests.RequestException, KeyError, ValueError,
            json.JSONDecodeError) as e:
        log.warning(f"OpenRouter API: {e}")
        return None


def _call_openrouter_simple(prompt: str, *, model: str | None = None) -> str | None:
    """Минимальный вызов OpenRouter для пересказа акта — без system-промпта
    (зеркально _call_gigachat_simple). Лимит токенов сильно выше, чем у
    Claude/GigaChat: reasoning-модели (DeepSeek R1, Nemotron и т.п.) тратят
    бюджет на размышления в content и с маленьким лимитом обрезаются
    посреди <think> — до финального ответа дело не доходит (наблюдалось
    на nemotron-3-super при 1200). Модели бесплатные, удорожания нет;
    4096 — как у полных digest/polish-вызовов OpenRouter."""
    return _call_openrouter_chat(
        [{"role": "user", "content": prompt}],
        max_tokens=4096, temperature=0.2, model=model,
    )


def _call_openrouter_polish(system_prompt: str, user_prompt: str) -> str | None:
    """Вызов OpenRouter для полировщика."""
    return _call_openrouter_chat(
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=4096, temperature=0.1,
    )


def _call_openrouter_digest(prompt: str) -> str | None:
    """Полный дайджест через OpenRouter (ветка DIGEST_FULL_LLM=1).

    System — тот же GIGACHAT_SYSTEM_PROMPT: он написан именно против
    сползания в Markdown, чем free-модели OpenRouter страдают так же,
    как GigaChat. Ответ прогоняется через нормализацию Markdown-артефактов.
    """
    text = _call_openrouter_chat(
        [
            {"role": "system", "content": GIGACHAT_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        max_tokens=4096, temperature=0.2,
    )
    if not text:
        return None
    return _normalize_markdown_to_telegram_html(text) or None


# ── LLM-пересказ мотивировки судебного акта (микро-вызов) ───────────────────
# Используется программным рендером дайджеста (этап 3b плана миграции):
# вместо сырого 500-символьного excerpt'а мотивировки в секциях 5.5/3.6/касс.
# подставляем пересказ «почему» от LLM (2-3 предложения). Кэш по
# sha1(act_text), один пересказ = одна оплата за всё время; --replay-last
# повторно не платит.

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
        f"Ты — помощник юриста банка. Перед тобой мотивировочная часть "
        f"({kind}). "
        + (f"Контекст: {meta_str}. " if meta_str else "")
        + "\n\n"
        "Задача: перескажи мотивировку 2-3 предложениями на русском языке "
        "(суммарно до 450 символов):\n"
        "1) решающий аргумент суда — то, ради чего юрист откроет акт;\n"
        "2) ключевые обстоятельства или доказательства, на которых он "
        "построен;\n"
        "3) при необходимости — какие доводы отклонены.\n\n"
        "В ответе — ТОЛЬКО сам пересказ. Без преамбул, пояснений, "
        "рассуждений, кавычек по краям, эмодзи, Markdown и HTML.\n\n"
        "Как формулировать:\n"
        "- пиши как вывод, а не как отчёт о процессе: вместо «суд установил, "
        "что X» — просто «X»;\n"
        "- начинай сразу с сути (факт/вывод), не с процедуры;\n"
        "- стороны — обезличенно: «истец», «ответчик», «заёмщик», "
        "«поручитель», «банк» — имена и названия организаций уже в шапке "
        "дайджеста, не повторяй их;\n"
        "- не перечисляй статьи законов;\n"
        "- не начинай со слов «Кратко», «Резюме», «Главное», «Для банка», "
        "«Ответ», «Пересказ».\n\n"
        "ХОРОШО: «Договор поручительства действителен, неисполнение "
        "заёмщиком установлено. Доводы поручителя о прекращении "
        "поручительства отклонены: срок согласован в договоре, "
        "обязательство не изменялось. Взыскание с поручителя правомерно.»\n"
        "ХОРОШО: «Истец не доказал, что приобрёл автомобиль до наложения "
        "ареста. Договор купли-продажи датирован позже возбуждения "
        "исполнительного производства, фактическое владение не "
        "подтверждено. Оснований для освобождения имущества от ареста "
        "нет.»\n"
        "ПЛОХО (процедура, статьи): «Суд применил ст. 331 ГПК РФ о "
        "проверке решения…»\n"
        "ПЛОХО (имена, пересказ фабулы): «Сбербанк взыскивал задолженность "
        "по кредиту…»\n\n"
        f"ТЕКСТ АКТА:\n{act_text}\n\n"
        "Ответ (2-3 предложения):"
    )


# ── Anthropic API: пейлоад с учётом поколения модели ─────────────────────────
# У Claude нового поколения (Opus 4.7+/Sonnet 5/Fable/Mythos) сэмплинг-параметры
# удалены из API: запрос с temperature получает 400 «`temperature` is
# deprecated for this model» (наблюдалось на opus 4.8 в тест-прогоне 14.07).
# Там же появились adaptive-мышление и output_config.effort (low/medium/high/
# xhigh/max). Боевой haiku-путь неизменен: temperature как раньше, без
# thinking/effort — haiku их не поддерживает (API вернёт ошибку).
_CLAUDE_MODERN_PREFIXES = (
    "claude-opus-4-7", "claude-opus-4-8", "claude-sonnet-5",
    "claude-fable", "claude-mythos",
)


def _claude_is_modern(model: str) -> bool:
    """True для моделей Claude без сэмплинг-параметров (с effort/adaptive)."""
    return model.startswith(_CLAUDE_MODERN_PREFIXES)


def _claude_payload(*, max_tokens: int, temperature: float,
                    messages: list[dict],
                    system: str | None = None) -> dict:
    """Собрать тело запроса /v1/messages под текущую config.CLAUDE_MODEL.

    Для «современных» моделей temperature не отправляется, включается
    adaptive-мышление (рекомендация Anthropic: с выключенным мышлением
    opus пишет рассуждения прямо в видимый ответ), а глубину задаёт
    config.CLAUDE_EFFORT (пусто = дефолт API, high). max_tokens при этом
    расширяется: токены размышлений считаются в лимит вывода, и боевые
    700 токенов пересказа opus сжёг бы на одно мышление.
    """
    payload: dict = {
        "model": config.CLAUDE_MODEL,
        "max_tokens": max_tokens,
        "messages": messages,
    }
    if system is not None:
        payload["system"] = system
    if _claude_is_modern(config.CLAUDE_MODEL):
        payload["thinking"] = {"type": "adaptive"}
        payload["max_tokens"] = max(max_tokens * 4, 8000)
        if config.CLAUDE_EFFORT:
            payload["output_config"] = {"effort": config.CLAUDE_EFFORT}
    else:
        payload["temperature"] = temperature
    return payload


def _claude_timeout(base: int) -> int:
    """HTTP-таймаут вызова Claude: adaptive-мышление opus/sonnet может
    занимать заметно дольше мгновенного haiku."""
    return 180 if _claude_is_modern(config.CLAUDE_MODEL) else base


def _call_claude_simple(
    prompt: str, *, max_tokens: int = 700, temperature: float = 0.2
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
            json=_claude_payload(
                max_tokens=max_tokens, temperature=temperature,
                messages=[{"role": "user", "content": prompt}],
            ),
            timeout=_claude_timeout(30),
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
            _gigachat_api_url(),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json={
                "model": config.GIGACHAT_MODEL,
                "temperature": 0.2,
                "max_tokens": 700,
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
    r"^\s*(?:кратко|резюме|итого|вкратце|суть|вывод|главное|пересказ|"
    r"ответ(?:\s*\([^)\n]*\))?)\s*[:\-—]\s*",
    re.IGNORECASE,
)

# Преамбула вида «Вот краткий пересказ:» одной строкой с ответом.
_SUMMARY_VOT_RE = re.compile(r"^\s*вот\s+[^:\n]{0,60}:\s*", re.IGNORECASE)

# Reasoning-блоки free-моделей OpenRouter (DeepSeek R1 и т.п.): размышления
# приходят прямо в content. Закрытые блоки вырезаем; незакрытый <think>
# означает, что ответ обрезался посреди размышлений (лимит токенов).
_THINK_BLOCK_RE = re.compile(
    r"<\s*(think|thinking|reasoning)\s*>.*?<\s*/\s*\1\s*>",
    re.IGNORECASE | re.DOTALL,
)
_THINK_OPEN_RE = re.compile(
    r"<\s*(?:think|thinking|reasoning)\s*>", re.IGNORECASE
)
_THINK_CLOSE_RE = re.compile(
    r"<\s*/\s*(?:think|thinking|reasoning)\s*>", re.IGNORECASE
)

# Жёсткий потолок длины пересказа (промпт просит до 450, даём запас).
_SUMMARY_HARD_LIMIT = 600


# Латинские «слова» длиной от трёх букв, которые в русском судебном пересказе
# законны. Всё прочее на латинице — признак code-switching бесплатной модели.
# Бренды суды пишут в кавычках, и кавычечные вставки гард пропускает отдельно.
_SUMMARY_LATIN_OK = frozenset({
    "sms", "qr", "pin", "vin", "atm", "pos", "mir", "usd", "eur", "rub",
    "visa", "mastercard", "unionpay", "sberbank", "id", "it", "ok",
})
_SUMMARY_LATIN_RUN_RE = re.compile(r"[A-Za-z]{3,}")
_SUMMARY_QUOTED_RE = re.compile(r"[«\"'][^«»\"']*[»\"']")


def summary_language_ok(s: str) -> bool:
    """Похож ли пересказ на русский текст (гард против сбоя провайдера).

    Два правила. (1) Доля кириллицы ≥ 40% — ловит ответ целиком не на русском.
    (2) Ни одного латинского прогона от трёх букв вне белого списка и вне
    кавычек — ловит code-switching, который первое правило пропускает:
    выпуск 21.08.2026 разослал юристу «послужили Creditный договор» и «доводы
    о lack of доказательств» (дело 2-3996/2026, модель OpenRouter free).
    Названия в кавычках («Renault») законны — их суды цитируют дословно.
    """
    s = (s or "").strip()
    if not s:
        return False
    if len(s) > 40:
        cyr = sum(1 for ch in s if "а" <= ch.lower() <= "я" or ch in "ёЁ")
        if cyr / len(s) < 0.4:
            return False
    bare = _SUMMARY_QUOTED_RE.sub(" ", s)
    return not any(m.group(0).lower() not in _SUMMARY_LATIN_OK
                   for m in _SUMMARY_LATIN_RUN_RE.finditer(bare))


def _clean_summary(text: str) -> str:
    """Почистить ответ LLM: reasoning-блоки, code-fence, Markdown, кавычки,
    шаблонные префиксы, переносы строк, гарды языка и длины.

    Пустая строка на выходе = ответ-мусор; вызывающая сторона вернёт None
    и откатится на сырой excerpt мотивировки.
    """
    s = text or ""

    # Reasoning-блоки: сперва закрытые, затем хвост незакрытого <think>,
    # затем «размышления …</think> ответ» без открывающего тега.
    s = _THINK_BLOCK_RE.sub("", s)
    m = _THINK_OPEN_RE.search(s)
    if m:
        s = s[:m.start()]
    parts = _THINK_CLOSE_RE.split(s)
    if len(parts) > 1:
        s = parts[-1]
    s = s.strip()

    # Если модель начала с code-fence — срежем.
    if s.startswith("```"):
        nl = s.find("\n")
        if nl != -1:
            s = s[nl + 1:]
    if s.endswith("```"):
        s = s[:-3]
    s = s.strip()

    # Markdown-разметку разворачиваем в чистый текст.
    s = re.sub(r"\*\*(.+?)\*\*", r"\1", s, flags=re.DOTALL)
    s = re.sub(r"__(.+?)__", r"\1", s, flags=re.DOTALL)
    s = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"\1", s)
    s = re.sub(r"`([^`\n]+)`", r"\1", s)
    s = re.sub(r"^\s*#{1,6}\s+", "", s, flags=re.MULTILINE)

    # Преамбула отдельной строкой («Вот пересказ:») + склейка переносов:
    # 2-3 предложения могут прийти с \n, в строке дайджеста они не нужны.
    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    if len(lines) > 1 and lines[0].endswith(":") and len(lines[0]) <= 60:
        lines = lines[1:]
    s = " ".join(lines)

    # Кавычки по краям и шаблонные префиксы (до и после — модель может
    # обернуть в кавычки весь ответ вместе с префиксом).
    s = s.strip().strip('"').strip("'").strip("«»").strip()
    s = _SUMMARY_PREFIX_RE.sub("", s)
    s = _SUMMARY_VOT_RE.sub("", s)
    s = s.strip().strip('"').strip("'").strip("«»").strip()

    # Гард языка: ответ не по-русски → мусор (англоцентричная free-модель).
    if not summary_language_ok(s):
        return ""

    # Гард длины: неукротимо длинный ответ режем по границе предложения.
    if len(s) > _SUMMARY_HARD_LIMIT:
        cut = s[:_SUMMARY_HARD_LIMIT]
        end = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
        if end <= 0:
            return ""
        s = cut[:end + 1]

    return s.strip()


def _act_cache_key(act: str) -> str:
    """Ключ кэша пересказов (.act_summaries.json) для текста акта.

    Версия "v3-detailed" в ключе: промпт (июль 2026) просит развёрнутый
    пересказ в 2-3 предложения; однофразные "v2-ratio"-пересказы из кэша
    не должны возвращаться. Версию бампаем ТОЛЬКО при смене стиля
    результата — правки надёжности промпта её не трогают (ключ не зависит
    от байтов промпта).

    Для gigachat/openrouter в ключ входит провайдер:модель — иначе тестовый
    прогон молча вернул бы кэшированный пересказ Claude, а его результат
    попал бы в боевой Claude-кэш. То же для НЕэталонной модели Claude
    (sonnet/opus в тестовом контуре): свой неймспейс, чтобы не читать
    haiku-кэш и не засорять боевой. Ключи эталонной haiku-модели остаются
    байт-в-байт прежними — боевой .act_summaries.json не переиндексируется.
    """
    base = act + "|v3-detailed"
    if config.LLM_PROVIDER in ("gigachat", "openrouter"):
        base += "|" + _current_digest_model_name()
    elif (
        config.LLM_PROVIDER == "claude"
        and config.CLAUDE_MODEL != config.DEFAULT_CLAUDE_MODEL
    ):
        base += "|" + config.CLAUDE_MODEL
        # Эффорт меняет результат пересказа — сравнение уровней не должно
        # молча читать кэш другого уровня. Для эталонной haiku эффорт не
        # отправляется вовсе, её ключи не трогаем.
        if config.CLAUDE_EFFORT:
            base += "|effort=" + config.CLAUDE_EFFORT
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def _openrouter_summary_attempts(
    prompt: str, model: str, attempts: int, who: str,
) -> tuple[str, str | None]:
    """До `attempts` вызовов модели `model` с нарастающей паузой между
    попытками (attempt * config.OPENROUTER_SUMMARY_RETRY_DELAY — как у
    fetch_page): перегруженный free-пул отдаёт 429 мгновенно, немедленный
    повтор упирается в ту же стену.

    Возвращает (summary, raw последней попытки); summary == "" — все
    попытки пусты или отбракованы чисткой.
    """
    raw: str | None = None
    for attempt in range(1, max(1, attempts) + 1):
        config.METRICS["llm_summary_calls"] += 1
        raw = _call_openrouter_simple(prompt, model=model)
        summary = _clean_summary(raw) if raw else ""
        if summary:
            return summary, raw
        if attempt < attempts:
            wait = attempt * config.OPENROUTER_SUMMARY_RETRY_DELAY
            log.warning(
                f"Пересказ акта{who}: попытка {attempt}/{attempts} "
                f"({model}) не дала текста — повтор через {wait}с..."
            )
            time.sleep(wait)
    return "", raw


def summarize_act_motivation(
    act_text: str,
    *,
    case_meta: dict,
    use_cache: bool = True,
) -> str | None:
    """Сделать пересказ мотивировки судебного акта (2-3 предложения) через LLM.

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

    Для провайдера openrouter сбой не финален сразу: до
    config.OPENROUTER_SUMMARY_RETRIES попыток на основной модели с
    нарастающей паузой, затем фолбэк-модель OPENROUTER_FALLBACK_MODEL
    (openrouter/free) с config.OPENROUTER_SUMMARY_FALLBACK_RETRIES
    попытками, затем — при config.LLM_SUMMARY_PROVIDER_FALLBACK и живом
    ANTHROPIC_API_KEY — одна попытка фолбэк-провайдера Claude, и только
    потом None.

    Если ключа текущего провайдера нет вовсе (Mac-резерв), пересказ
    пропускается ДО вызова: `llm_summary_skipped_no_key` вместо
    `llm_summary_failed`, одна строка в лог за процесс.
    """
    act = (act_text or "").strip()
    if not act or len(act) < 100:
        return None

    key = _act_cache_key(act)
    cache = _load_act_summaries() if use_cache else {}
    if use_cache and key in cache:
        cached_summary = (cache[key] or {}).get("summary")
        if cached_summary and not summary_language_ok(cached_summary):
            # Испорченный пересказ в кэше жил бы ВЕЧНО: кэш-хит стоит до всех
            # чисток, и гард в _clean_summary его никогда не увидит. Так
            # выпуск 21.08.2026 разослал «послужили Creditный договор»
            # (2-3996/2026) — запись осталась бы в .act_summaries.json
            # навсегда. Считаем промахом и перезапрашиваем; ключ вычищается
            # при записи нового пересказа ниже. Версию «v3-detailed» в
            # _act_cache_key НЕ бампаем — бамп заново оплатил бы все хорошие
            # пересказы ради одного испорченного.
            log.warning(
                "Пересказ из кэша не по-русски (сбой провайдера) — "
                "перезапрашиваем"
            )
            cached_summary = ""
        if cached_summary:
            config.METRICS["llm_summary_cache_hits"] += 1
            return cached_summary

    # Ключа нет вовсе (Mac-резерв) — это не «сбой пересказа»: вызова не было,
    # и считать его в llm_summary_failed нельзя, иначе черновой прогон каждое
    # утро поднимает 🩺-алерт «сбоев N из N» о несуществующем отказе
    # провайдера. Гард стоит ПОСЛЕ кэша осознанно: пересказ, оплаченный
    # replay'ем и закоммиченный в .act_summaries.json, обязан отдаваться и на
    # машине без ключей.
    if missing_key := missing_llm_key_name():
        config.METRICS["llm_summary_skipped_no_key"] += 1
        _report_llm_not_configured(missing_key)
        return None

    prompt = _build_act_summary_prompt(act, case_meta)

    def _call_once() -> str | None:
        if config.LLM_PROVIDER == "gigachat":
            return _call_gigachat_simple(prompt)
        return _call_claude_simple(prompt)

    pl = (case_meta.get("plaintiff") or "").strip()
    df = (case_meta.get("defendant") or "").strip()
    who = f" ({pl} vs {df})" if (pl or df) else ""

    model_label: str | None = None  # фактическая модель для записи кэша (фолбэк)
    if config.LLM_PROVIDER == "openrouter":
        # Free-модели капризны (обрыв reasoning посреди <think>, пустой
        # content, мгновенный 429 перегруженного пула): до N попыток с
        # паузами на основной модели, затем фолбэк-роутер openrouter/free.
        primary = _resolve_openrouter_model()
        summary, raw = _openrouter_summary_attempts(
            prompt, primary, config.OPENROUTER_SUMMARY_RETRIES, who)
        fallback = config.OPENROUTER_FALLBACK_MODEL
        # Гард fallback != primary: рейтинг shir-man упал (primary уже
        # openrouter/free) или её задали явно — не дублировать попытки.
        if not summary and fallback and fallback != primary:
            summary, raw = _openrouter_summary_attempts(
                prompt, fallback, config.OPENROUTER_SUMMARY_FALLBACK_RETRIES, who)
            if summary:
                config.METRICS["llm_summary_fallback_saved"] += 1
                log.info(f"Пересказ акта{who}: выручила фолбэк-модель {fallback}")
                model_label = f"openrouter:{fallback}"
        # Фолбэк-ПРОВАЙДЕР: бесплатный пул лёг целиком (и «модель дня», и
        # openrouter/free исчерпали попытки) — одна попытка на боевом Claude,
        # если его ключ есть в env (в replay/кроне прокинут всегда; на
        # Mac-резерве ключей нет вовсе — туда не доходим, missing_llm_key_name
        # отсёк раньше). Без этой ветки пересказ теряется НАВСЕГДА: акт
        # объявляется один раз, и сырой отрывок замерзает в дайджесте и
        # «AI анализе» drawer'а (инцидент 28.08.2026, Урал — оба акта
        # выпуска). Кэш-ключ остаётся в openrouter-неймспейсе, поле model
        # честно называет автора — тот же механизм, что у фолбэк-модели.
        if (not summary and config.LLM_SUMMARY_PROVIDER_FALLBACK
                and config.ANTHROPIC_API_KEY):
            config.METRICS["llm_summary_calls"] += 1
            claude_raw = _call_claude_simple(prompt)
            summary = _clean_summary(claude_raw) if claude_raw else ""
            if claude_raw:
                # Пустой ответ Claude не затирает raw: WARNING «отбракован
                # чисткой» ниже должен показывать голову последнего
                # НЕПУСТОГО ответа, а не молчать «пустой ответ LLM».
                raw = claude_raw
            if summary:
                config.METRICS["llm_summary_provider_fallback_saved"] += 1
                log.info(
                    f"Пересказ акта{who}: выручил фолбэк-провайдер claude "
                    f"({config.CLAUDE_MODEL})"
                )
                model_label = f"claude:{config.CLAUDE_MODEL}"
    else:
        config.METRICS["llm_summary_calls"] += 1
        raw = _call_once()
        summary = _clean_summary(raw) if raw else ""
    if not summary:
        config.METRICS["llm_summary_failed"] += 1
        if raw:
            log.warning(
                f"Пересказ акта{who}: ответ LLM отбракован чисткой, откат "
                f"на excerpt; голова ответа: {raw[:160]!r}"
            )
        else:
            log.warning(
                f"Пересказ акта{who}: пустой ответ LLM, откат на excerpt"
            )
        return None

    if use_cache:
        cache[key] = {
            "summary": summary,
            # Ключ остаётся в неймспейсе основной модели прогона (вычислен
            # выше), но поле model честно указывает фактического автора.
            "model": model_label or _current_digest_model_name(),
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
    # Фильтры рендера — симметрично линтеру (lint._expected_number_alternatives):
    # change с единственным клерикальным «дело передано в архив» и свёрнутые
    # «заведено N новых исков банка» номеров в HTML не дают. Без гейтов
    # валидатор полировщика при DIGEST_POLISH=1 отвергал бы КАЖДУЮ полировку
    # в дни разгона территории или архивного переноса решённого дела.
    from court_monitor.digest.template import (
        _strip_archive_final_events, split_bank_intake_fold,
    )
    _fi_rendered = _strip_archive_final_events(list(fi_changes or []))
    _folded_ids = {id(ch) for ch in
                   split_bank_intake_fold([ch for ch in _fi_rendered
                                           if ch.get("track")])[1]}
    for ch in _fi_rendered:
        if id(ch) in _folded_ids:
            continue
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
    elif config.LLM_PROVIDER == "openrouter":
        polished = _call_openrouter_polish(
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
            json=_claude_payload(
                max_tokens=4096, temperature=0.1,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            ),
            timeout=_claude_timeout(60),
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
            _gigachat_api_url(),
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
    if config.LLM_PROVIDER == "openrouter":
        return f"openrouter:{_resolve_openrouter_model()}"
    return config.CLAUDE_MODEL
