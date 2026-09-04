# -*- coding: utf-8 -*-
"""Журнал здоровья парсеров и детектор «молчаливой поломки»:
суд со стабильной историей результатов вдруг отдаёт 0 / HTTP-фейлы подряд /
все источники разом по нулям. update_parse_health возвращает (state, alerts) —
отправка алертов остаётся на вызывающем (main_json).
"""

from __future__ import annotations

import json
import os
import statistics
import tempfile
from datetime import datetime

from court_monitor import config
from court_monitor.config import log

# ── Здоровье парсеров (детектор молчаливой поломки) ─────────────────────────

def load_parse_health() -> dict:
    """Загрузить журнал здоровья парсеров ({version, updated_at, sources})."""
    if not os.path.exists(config.PARSE_HEALTH_PATH):
        return {"version": 1, "updated_at": "", "sources": {}}
    try:
        with open(config.PARSE_HEALTH_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        log.warning(f"parse-health: не удалось прочитать {config.PARSE_HEALTH_PATH}")
        return {"version": 1, "updated_at": "", "sources": {}}
    if not isinstance(data, dict):
        return {"version": 1, "updated_at": "", "sources": {}}
    data.setdefault("sources", {})
    return data


def save_parse_health(state: dict) -> None:
    """Атомарно сохранить итоговый публичный журнал здоровья.

    Непрерывная телеметрия живёт отдельно, но финальный ``parse_health.json``
    тоже нельзя оставлять обрезанным при сне Mac/заполненном диске: его читают
    гейт доставки и админка. Временный файл создаём в том же каталоге, затем
    fsync + replace. Ошибку не глотаем — вызывающий уже умеет превратить её в
    WARNING, а прежний валидный журнал при этом остаётся на месте.
    """
    path = config.PARSE_HEALTH_PATH
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = ""
    try:
        fd, tmp = tempfile.mkstemp(
            prefix=f".{os.path.basename(path)}.{os.getpid()}.",
            suffix=".tmp",
            dir=directory,
        )
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=1)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        tmp = ""
        # После replace данные уже консистентны. fsync каталога —
        # best-effort усиление на случай внезапного отключения питания.
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        try:
            dir_fd = os.open(directory, flags)
        except OSError:
            dir_fd = None
        if dir_fd is not None:
            try:
                os.fsync(dir_fd)
            except OSError:
                pass
            finally:
                os.close(dir_fd)
    finally:
        if tmp:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def searched_ok_today(state: dict | None = None) -> set[str]:
    """Ключи источников, чей поиск СЕГОДНЯ уже дал строки (>0).

    Дочитка слотов Mac-резерва (SKIP_CHECKED_TODAY): такие поиски повторный
    слот не запрашивает — новые дела этот суд уже отдал утренней попытке.
    Критерий строгий, «>0 строк»: WAF-заглушка парсится как «0 строк»
    (sber_rows=0 при живой медиане), и по одному «страница загрузилась»
    пропуск был бы ложным — слепой поиск обязан ретраиться каждым слотом.
    Побочка осознанная: суды с честным нулём (медиана <1) опрашиваются
    каждый слот. Капчёвая апелляция (search_gated) пишет None и не
    пропускается никогда — «снимут код — вернётся сам».
    Дата — локальная, той же семантики, что дочитка карточек
    (last_run_at пишется naive-local в этом же модуле).
    """
    state = state if state is not None else load_parse_health()
    today = datetime.now().date().isoformat()
    done: set[str] = set()
    for key, src in ((state or {}).get("sources") or {}).items():
        if not isinstance(src, dict):
            continue
        if str(src.get("last_run_at") or "")[:10] != today:
            continue
        if int(src.get("fail_streak") or 0) > 0:
            continue
        try:
            count = int(src.get("last_count") or 0)
        except (TypeError, ValueError):
            continue
        if count > 0:
            done.add(str(key))
    return done


def _note_captcha(
    src: dict, name: str, domain: str, now_iso: str, announced: set[str],
) -> list[str]:
    """Поиск источника пришёл проверочным кодом: штамп в записи + алерт.

    Первое обнаружение — строка с рецептом (что править в регион-конфиге),
    дальше ОДНО напоминание в день (решение юриста 04.09.2026: слотов за утро
    6–7, а суд, не помеченный в конфиге, нельзя забыть). `announced` — домены,
    чей текст уже добавлен в этом вызове: кассация пишет два ключа
    (total/matched) с ОДНОЙ страницы, штампуем оба, говорим один раз.
    """
    today = now_iso[:10]
    lines: list[str] = []
    if not src.get("captcha_since"):
        src["captcha_since"] = now_iso
        src["captcha_alerted_on"] = today
        if domain not in announced:
            announced.add(domain)
            lines.append(
                f"🔐 {name}: поиск закрыт проверочным кодом ({domain}) — "
                f"автопоиск новых дел встал, карточки читаются. Что делать: "
                f"search_gated=True в scripts/court_monitor/regions/"
                f"{config.REGION}.py (у апелляции ещё search_disabled=True), "
                f"запушить; дальше дела заводит дамп выдачи — админка → «Импорт»"
            )
    elif str(src.get("captcha_alerted_on") or "") != today:
        src["captcha_alerted_on"] = today
        if domain not in announced:
            announced.add(domain)
            since = str(src["captcha_since"])[:10]
            try:
                since = datetime.fromisoformat(since).strftime("%d.%m")
            except ValueError:
                pass
            lines.append(
                f"🔐 {name}: поиск всё ещё за проверочным кодом с {since} "
                f"({domain}) — суд не помечен в конфиге, дела заводит дамп "
                f"выдачи (админка → «Импорт»)"
            )
    return lines


def update_parse_health(
    observations: dict,
    labels: dict | None = None,
    state: dict | None = None,
    known_alive_today: int = 0,
    captcha: dict | None = None,
) -> tuple[dict, list[str]]:
    """Обновить журнал здоровья парсеров и вернуть (state, список алертов).

    observations: {ключ источника: int — сколько строк дал поиск в этом
    прогоне; None — страница поиска не загрузилась после всех ретраев}.
    labels: {ключ: человекочитаемое имя для алертов}.
    state: журнал (по умолчанию читается из config.PARSE_HEALTH_PATH; параметр —
    для тестов).
    known_alive_today: сколько источников слот ПРОПУСТИЛ как «поиск сегодня
    уже удался» (дочитка, searched_ok_today). При >0 глобальный алерт «все
    источники разом по нулям» не поднимается: наблюдаемыми остались лишь
    неудачники и честные нули, а живые суды в observations не попали.
    captcha: {ключ источника: домен} — чей поиск В ЭТОМ прогоне пришёл
    проверочным кодом (ключ обязан быть и в observations). Помеченные
    `search_gated` суды сюда не попадают — код там ожидаем.

    Правила капчи (поля записи `captcha_since` / `captcha_alerted_on`):
    - первое обнаружение — 🔐-алерт с рецептом; пока код держится — одно
      напоминание в день (слотов за утро 6–7); текст «поиск вернул 0
      результатов» для такого ключа не печатается (причину называет 🔐),
      но zero_streak/alerted_zero ведутся — точка в админке остаётся красной;
    - страница пришла без кода (любой int) при живом штампе — ✅ «код снят»,
      поля снимаются, дубль «снова отдаёт результаты» глушится;
    - None (страница не загрузилась) поля капчи не трогает.

    Правила алертов:
    - «стал нулём»: медиана последних успешных прогонов ≥1, а сегодня 0 —
      алерт на 1-м и 3-м нулевом прогоне подряд, дальше тишина до
      восстановления (тогда придёт «снова отдаёт результаты»). Суды, у
      которых 0 — норма (нет дел банка на первой странице), не алертят:
      медиана их истории < 1.
    - HTTP-фейл config.PARSE_HEALTH_FAIL_ALERT прогонов подряд — алерт на каждом
      кратном пороге (3, 6, 9…): одиночные сетевые сбои не шумят.
    - Все источники разом 0/фейл при живой истории — отдельный алерт
      (лежит sudrf целиком или глобально сменилась вёрстка).
    """
    labels = labels or {}
    captcha = captcha or {}
    state = state if state is not None else load_parse_health()
    sources = state.setdefault("sources", {})
    alerts: list[str] = []
    now_iso = datetime.now().isoformat(timespec="seconds")
    captcha_announced: set[str] = set()

    for key, count in observations.items():
        src = sources.setdefault(key, {
            "counts": [], "zero_streak": 0, "fail_streak": 0,
            "alerted_zero": False,
        })
        name = labels.get(key, key)
        # Человекочитаемое имя источника пишем в журнал: админка читает его
        # отсюда (s.label) вместо ручной карты COURT_NAMES — реестры регионов
        # больше не нужно синхронизировать с admin_page.js вручную.
        if key in labels:
            src["label"] = labels[key]
        if count is None:
            src["fail_streak"] = int(src.get("fail_streak", 0)) + 1
            src["last_run_at"] = now_iso
            if src["fail_streak"] % config.PARSE_HEALTH_FAIL_ALERT == 0:
                alerts.append(
                    f"{name}: страница поиска не загружается "
                    f"{src['fail_streak']} прогонов подряд"
                )
            continue
        src["fail_streak"] = 0
        captcha_now = key in captcha
        if captcha_now:
            alerts.extend(_note_captcha(
                src, name, str(captcha[key]), now_iso, captcha_announced,
            ))
        elif src.get("captcha_since"):
            # Страница загрузилась и кода на ней нет — снят. Снимаем штамп и
            # alerted_zero ДО ветки count > 0: иначе рядом встал бы дубль
            # «снова отдаёт результаты» об одном и том же событии.
            src.pop("captcha_since", None)
            src.pop("captcha_alerted_on", None)
            src["alerted_zero"] = False
            alerts.append(
                f"✅ {name}: проверочный код снят, поиск снова работает "
                f"(строк в выдаче: {count}) — если стоял "
                f"search_gated/search_disabled, автопоиск можно вернуть"
            )
        history = [c for c in src.get("counts", []) if isinstance(c, int)]
        median = statistics.median(history) if history else 0
        if count == 0 and median >= 1:
            src["zero_streak"] = int(src.get("zero_streak", 0)) + 1
            if src["zero_streak"] in (1, 3):
                src["alerted_zero"] = True
                if not captcha_now:
                    alerts.append(
                        f"{name}: поиск вернул 0 результатов, хотя обычно "
                        f"~{int(median)} ({src['zero_streak']}-й нулевой "
                        f"прогон подряд)"
                    )
        elif count > 0:
            if src.get("alerted_zero"):
                alerts.append(f"{name}: снова отдаёт результаты ({count})")
            src["zero_streak"] = 0
            src["alerted_zero"] = False
        src["counts"] = (history + [count])[-config.PARSE_HEALTH_HISTORY_LEN:]
        src["last_count"] = count
        src["last_run_at"] = now_iso

    # Глобальный ноль: ни один источник не дал результатов, при том что
    # раньше жизнь была (иначе первый прогон на пустой истории алертил бы).
    # Требуем ≥2 источников: для одиночного это дубль пер-судового алерта.
    # Дочитка (known_alive_today>0) глушит алерт: пропущенные источники живы
    # по определению — «всё разом мертво» при них ложь.
    if len(observations) >= 2 and not known_alive_today:
        all_dead = all((c is None or c == 0) for c in observations.values())
        had_life = any(
            any(isinstance(x, int) and x > 0
                for x in (sources.get(k, {}).get("counts") or []))
            for k in observations
        )
        if all_dead and had_life:
            alerts.append(
                "ВСЕ источники разом вернули 0 или не загрузились — похоже, "
                "лежит sudrf целиком либо глобально сменилась вёрстка"
            )

    state["updated_at"] = now_iso
    return state, alerts
