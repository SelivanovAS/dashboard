#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Гейт и вердикты утренних слотов Mac-резерва («один дайджест в день»).

ЗАЧЕМ. Суды режут часть адресов и «мигают» пер-хостово (18–20.08.2026), утро
идёт слотами 06:00–08:30 + доставочный 08:45 (расписание 21.08.2026). Решение
юриста 20.08.2026: дайджест приходит ОДИН раз; 21.08.2026 к нему добавлено
окно — не раньше 08:45, поэтому отправляет первый слот, ЗАВЕРШИВШИЙСЯ после
08:45 (общий CM_DELIVERY_WINDOW_MIN в lib_sber_net.sh), со всем накопленным с
06:00.
Вердикт `--run-complete` (поиски зрячие И прочитано ≥85% карточек плана) на
решение о доставке больше не влияет — только на формулировки алертов и на
то, продолжать ли дочитывать. Неполные попытки сохраняют данные и копят
новости в контексте дайджеста (save_digest_context мержит дельты), ничего не
отправляя; отправку решает parse_and_push ВЫБОРОМ СООБЩЕНИЯ КОММИТА —
replay_on_push стреляет только по маркеру «(Mac-парсинг)». Локальный факт
закрытия дня — `delivered_at` в data/last_digest_context.json. Перед ним
создаётся durable delivery journal; после marker-коммита его SHA сверяется с
remote, поэтому потерянный ответ `git push` не превращается ни в пропущенный,
ни в повторный marker. Сам Telegram/Web Push отправляет уже GitHub workflow.

РЕЖИМЫ (запуск из корня КЛОНА — регион и пути берутся из него):
  cloud_run_ok.py [--report]     гейт слота: 0 = дайджест сегодня уже
                                 отправлен (слот молчит), 1 = работать
  cloud_run_ok.py --run-complete 0 = сегодняшняя попытка удачна (поиски
                                 зрячие И карточки ≥ CARDS_READ_OK_RATIO)
  cloud_run_ok.py --progress     печатает строку-прогресс «прочитано X из Y
                                 карточек (Z%), поиски …» — тело алерта
  cloud_run_ok.py --has-pending  0 = в накоплении есть неотправленные новости
  cloud_run_ok.py --delivery-id напечатать ID текущего выпуска, не меняя
                                 контекст (для journal до mark)
  cloud_run_ok.py --mark-delivered  проставить delivered_at и напечатать
                                 стабильный delivery_id (идемпотентно)
  cloud_run_ok.py --unmark-delivered --delivery-id ID
                                 снять delivered_at только у
                                 указанного выпуска (пуш не удался)
  cloud_run_ok.py --health-alerts напечатать по строке 🩺-алерты детектора
                                 здоровья парсеров СЕГОДНЯШНЕГО прогона
                                 (last_run.alerts) — тело ретрансляции в
                                 Telegram из parse_and_push.sh: Python на
                                 Mac/VPS без токена, send_telegram молчит

ДАННЫЕ. Журнал здоровья data/parse_health.json: `sources` — поиски (источник
«зрячий сегодня» = last_run_at за сегодня И last_count > 0 И fail_streak == 0;
⚠️ без fail_streak нельзя — сетевой фейл бампает last_run_at, не трогая
last_count), `last_run` — карточная сводка прогона (пишет main_json, блок 4e:
cards_read/cards_planned — суммы пер-цикловых «спарсено X из Y», знаменатель
БЕЗ законных пропусков по ритму/датам).

⚠️ «Сегодня» сверяем и с UTC, и с местной датой: файлы пишут два автора —
облачный раннер (UTC) и этот Mac (+05), оба naive-ISO.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.getcwd(), "scripts"))

# Порог «удачной попытки» по карточкам — решение юриста 20.08.2026 (сначала
# назвал 75%, затем поправил на 85%). С 21.08 он влияет только на вердикт и
# решение следующего слота дочитывать; окно 08:45 доставляет накопленное даже
# ниже порога, чтобы не потерять единственный дневной выпуск.
CARDS_READ_OK_RATIO = 0.85

# Дельта-списки контекста (зеркало _CTX_DELTA_KEYS из digest/core.py — при
# расхождении --has-pending молча ослепнет, стережёт тест).
CTX_DELTA_KEYS = (
    "new_cases", "changes", "fi_new_cases", "stage_transitions",
    "fi_changes", "cass_changes", "cass_discovered",
)


def _today_dates() -> set:
    return {
        dt.datetime.now().date().isoformat(),
        dt.datetime.utcnow().date().isoformat(),
    }


def _load_json(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _health_state() -> dict:
    from court_monitor import config
    return _load_json(config.PARSE_HEALTH_PATH) or {}


def _context() -> dict:
    from court_monitor import config
    return _load_json(config.LAST_DIGEST_CONTEXT_PATH) or {}


def _context_delivery_id(ctx: dict) -> str | None:
    """Стабильный ID доставки: территория + ключ выпуска.

    `issue_key` не двигается между утренними попытками, а код
    региона разводит два клона. Fallback к `saved_at` здесь опасен:
    сломанный/старый контекст получил бы новый ID и мог уйти в
    replay повторно. По той же причине контекст обязан быть сохранён
    сегодня: если запись свежей дельты упала, вчерашний файл нельзя
    пометить сегодняшним delivered_at и отправить второй раз.
    """
    from court_monitor import config

    if str((ctx or {}).get("saved_at") or "")[:10] not in _today_dates():
        return None
    issue_key = str((ctx or {}).get("issue_key") or "").strip()
    if not issue_key:
        return None
    return f"{config.REGION}:{issue_key}"


def _save_context(path: str, ctx: dict) -> None:
    """Durable-атомарно заменить контекст; общая воронка mark/unmark."""
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
            json.dump(ctx, f, ensure_ascii=False, indent=1)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        tmp = ""
        try:
            dir_fd = os.open(
                directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
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


def delivered_today(ctx: dict) -> bool:
    return str((ctx or {}).get("delivered_at") or "")[:10] in _today_dates()


def context_pending(ctx: dict) -> bool:
    """Есть ли в накоплении дня неотправленные новости."""
    if not ctx or delivered_today(ctx):
        return False
    if str(ctx.get("saved_at") or "")[:10] not in _today_dates():
        return False
    return any(ctx.get(k) for k in CTX_DELTA_KEYS)


def cards_progress(state: dict) -> tuple[int, int] | None:
    """Стабильное дневное покрытие, fallback — последняя попытка."""
    lr = (state or {}).get("last_run") or {}
    if str(lr.get("at") or "")[:10] not in _today_dates():
        return None
    if "cards_planned_today" in lr:
        return (
            int(lr.get("cards_read_today") or 0),
            int(lr.get("cards_planned_today") or 0),
        )
    read = int(lr.get("cards_read") or 0)
    planned = int(lr.get("cards_planned") or 0)
    if planned <= 0 and read <= 0 and "cards_planned" not in lr:
        return None  # блок есть, но старого формата — судим только по поискам
    return read, planned


def cards_read_today_total(state: dict) -> int | None:
    """Сколько карточек прочитано за СЕГОДНЯ всеми попытками (накопительно).

    Ключ `cards_read_today` пишет блок 4e main_json (штампы last_checked_at
    за сегодня по активным делам). None — прогона сегодня не было или журнал
    старого формата (до 21.08.2026): формулировки тогда остаются прежними.
    """
    lr = (state or {}).get("last_run") or {}
    if str(lr.get("at") or "")[:10] not in _today_dates():
        return None
    if "cards_read_today" not in lr:
        return None
    return int(lr.get("cards_read_today") or 0)


def searches_state_today(state: dict) -> tuple[bool, bool]:
    """(прогон сегодня был, поиски зрячие).

    Единственная зрячая апелляция/кассация не имеет права закрыть
    аварию всех поисков 1-й инстанции: именно они заводят новые иски.
    Если сегодняшних FI-наблюдений нет вовсе (например, все суды
    территории search_gated), сохраняем legacy-оценку по остальным источникам.
    """
    sources = (state or {}).get("sources") or {}
    today = _today_dates()
    ran_today = False
    sighted = False
    fi_ran = False
    fi_sighted = False
    for key, src in sources.items():
        at = str(src.get("last_run_at") or "")
        if at[:10] not in today:
            continue
        ran_today = True
        ok = (src.get("last_count") or 0) > 0 and not src.get("fail_streak")
        sighted = sighted or ok
        if str(key).startswith("fi:"):
            fi_ran = True
            fi_sighted = fi_sighted or ok
    return ran_today, (fi_sighted if fi_ran else sighted)


def searches_by_instance_today(state: dict) -> dict[str, dict[str, int]]:
    """Поисковые источники сегодня, отдельно по трём инстанциям."""
    out = {
        name: {"ran": 0, "ok": 0, "failed": 0, "empty": 0}
        for name in ("first_instance", "appeal", "cassation")
    }
    today = _today_dates()
    for key, src in ((state or {}).get("sources") or {}).items():
        key = str(key)
        if key.startswith("fi:"):
            name = "first_instance"
        elif key.startswith("appeal:"):
            name = "appeal"
        elif key.startswith("cassation:"):
            # total — реальный HTTP источника, matched — производный фильтр
            # той же страницы. Не считаем один запрос двумя судами.
            if not key.endswith(":total"):
                continue
            name = "cassation"
        else:
            continue
        if str((src or {}).get("last_run_at") or "")[:10] not in today:
            continue
        row = out[name]
        row["ran"] += 1
        if int((src or {}).get("fail_streak") or 0) > 0:
            row["failed"] += 1
        elif int((src or {}).get("last_count") or 0) > 0:
            row["ok"] += 1
        else:
            row["empty"] += 1
    return out


_INSTANCE_LABELS = {
    "first_instance": "1-я инст.",
    "appeal": "апелляция",
    "cassation": "кассация",
}


def instance_searches_line(state: dict) -> str:
    rows = searches_by_instance_today(state)
    if not any(row["ran"] for row in rows.values()):
        return ""  # старый журнал без префиксов инстанций
    parts = []
    for name in ("first_instance", "appeal", "cassation"):
        row = rows[name]
        if not row["ran"]:
            value = "—"
        else:
            value = f"{row['ok']}/{row['ran']}"
            problems = []
            if row["failed"]:
                problems.append(f"ошибка {row['failed']}")
            if row["empty"]:
                problems.append(f"без результатов {row['empty']}")
            if problems:
                value += f" ({', '.join(problems)})"
        parts.append(f"{_INSTANCE_LABELS[name]} {value}")
    return "поиск: " + "; ".join(parts)


def instance_cards_line(state: dict) -> str:
    lr = (state or {}).get("last_run") or {}
    instances = lr.get("instances") if isinstance(lr.get("instances"), dict) else {}
    if not instances:
        return ""
    parts = []
    for name in ("first_instance", "appeal", "cassation"):
        row = instances.get(name) if isinstance(instances.get(name), dict) else {}
        read = int(row.get("read_today", row.get("read", 0)) or 0)
        planned = int(row.get("planned_today", row.get("planned", 0)) or 0)
        parts.append(f"{_INSTANCE_LABELS[name]} {read}/{planned}")
    return "карточки за день: " + "; ".join(parts)


def _instance_tail(state: dict) -> str:
    parts = [instance_cards_line(state), instance_searches_line(state)]
    body = "; ".join(part for part in parts if part)
    return f"; {body}" if body else ""


def _counted(
    number: int,
    nouns: tuple[str, str, str],
    predicates: tuple[str, str, str] | None = None,
) -> str:
    """Русская счётная форма: 1 карточка, 2 карточки, 5 карточек."""
    n = abs(int(number))
    form = 2 if 11 <= n % 100 <= 14 else (0 if n % 10 == 1 else (
        1 if 2 <= n % 10 <= 4 else 2
    ))
    text = f"{number} {nouns[form]}"
    return f"{text} {predicates[form]}" if predicates else text


def unavailability_tail(state: dict) -> str:
    """Хвост «портал недоступен» для вердикта и алерта (пусто — если нечего).

    ⚠️ Знаменатель прочитанного этим хвостом НЕ уменьшается, и это решение,
    а не недосмотр (24.08.2026). Соблазн считать «36 из 36 доступных» вместо
    «36 из 323» отвергнут: вердикт влияет ТОЛЬКО на текст алерта (на доставку
    с 21.08 не влияет), и «удачный прогон» в день полного аутейджа заглушил бы
    предупреждение ровно тогда, когда оно нужнее всего. Процент обязан
    отражать полноту ДАННЫХ у юриста, а вина портала — жить отдельной строкой.
    """
    lr = (state or {}).get("last_run") or {}
    cards = int(lr.get("cards_unreachable") or 0)
    other_unread = int(lr.get("cards_unread_other") or 0)
    # Старый журнал знает только состояние breaker на ФИНИШЕ. Новый хранит
    # накопительное число судов, где хотя бы одну карточку реально пропустили
    # без HTTP: суд мог ожить на half-open пробе и к финалу уже быть закрыт.
    courts = int(
        lr.get("courts_with_unrequested", lr.get("courts_unavailable")) or 0
    )
    if not cards and not courts and not other_unread:
        return ""
    outage = int(lr.get("courts_outage") or 0)
    tail = ""
    if cards:
        cards_phrase = _counted(
            cards,
            ("карточка", "карточки", "карточек"),
            ("не запрошена", "не запрошены", "не запрошено"),
        )
        tail = f"; {cards_phrase}"
    if courts:
        courts_phrase = _counted(courts, ("суд", "суда", "судов"))
        tail += (
            f" — снято с обхода: {courts_phrase}"
            if tail else f"; снято с обхода: {courts_phrase}"
        )
    if outage and tail:
        outage_phrase = _counted(outage, ("суда", "судов", "судов"))
        tail += f" (заглушка портала замечена у {outage_phrase})"
    if other_unread:
        other_phrase = _counted(
            other_unread,
            ("карточка", "карточки", "карточек"),
            ("не прочитана", "не прочитаны", "не прочитано"),
        )
        tail += (
            f"; ещё {other_phrase} по другим причинам"
            if tail else f"; {other_phrase} по другим причинам"
        )
    return tail


def health_alert_lines(state: dict) -> list[str]:
    """Строки 🩺-алерта последнего прогона (last_run.alerts), только сегодняшнего.

    Журнал персистится и коммитится каждым слотом, поэтому слот, упавший до
    блока 4e (или нерабочий день без прогона), отдал бы вчерашние строки
    повторно — дата обязательна. Дедуп повторов в пределах дня — на стороне
    parse_and_push.sh (файл health_alerts_sent.<дата>).
    """
    lr = (state or {}).get("last_run") or {}
    if str(lr.get("at") or "")[:10] not in _today_dates():
        return []
    alerts = lr.get("alerts")
    if not isinstance(alerts, list):
        return []
    return [str(a).strip() for a in alerts if str(a).strip()]


def run_complete_today(state: dict) -> tuple[bool, str]:
    """Удачна ли сегодняшняя попытка: поиски зрячие И карточки ≥ порога.

    Формулировки — для пульта, лога и алертов: их читает юрист, а «за утро
    так и не спарсилось» при доехавших карточках он 20.08.2026 прочитал как
    полный провал.
    """
    ran_today, sighted = searches_state_today(state)
    if not ran_today:
        return False, "прогона сегодня ещё не было"
    cards = cards_progress(state)
    # Хвост «за утро всего N» — только при ПОВТОРНОЙ попытке (накоплено
    # больше, чем прочитала текущая): слоты пересчитывают план заново, и
    # числа одной попытки юрист читает как итог дня (21.08.2026: «119 из
    # 362» дедлайна выглядели провалом при реальных ~70% покрытия). В первой
    # попытке total == read, и прежняя строка точнее.
    total_today = cards_read_today_total(state)
    if not sighted:
        tail = ""
        if cards:
            read, planned = cards
            tail = f" (карточки: прочитано {read} из {planned}"
            if total_today is not None and total_today > read:
                tail += f", за утро всего {total_today}"
            tail += ")"
        return False, (
            "поиски СЛЕПЫЕ — новые дела не искались, суды не пустили адрес"
            + tail + _instance_tail(state)
        )
    if cards:
        read, planned = cards
        cumulative = ""
        if total_today is not None and total_today > read:
            cumulative = f", за утро всего {total_today}"
        if planned > 0 and read / planned < CARDS_READ_OK_RATIO:
            pct = int(read / planned * 100)
            return False, (
                f"прочитано {read} из {planned} карточек ({pct}% — "
                f"порог {int(CARDS_READ_OK_RATIO * 100)}%){cumulative}"
                + unavailability_tail(state) + _instance_tail(state)
            )
        return True, (
            f"прочитано {read} из {planned} карточек{cumulative}, "
            f"поиски отвечали" + _instance_tail(state)
        )
    return True, (
        "поиски отвечали (карточной сводки нет — старый журнал)"
        + _instance_tail(state)
    )


def progress_line(state: dict) -> str:
    """Строка-прогресс для алерта после неполной попытки."""
    _, sighted = searches_state_today(state)
    cards = cards_progress(state)
    total_today = cards_read_today_total(state)
    if cards:
        read, planned = cards
        pct = int(read / planned * 100) if planned else 100
        if total_today is not None and total_today > read:
            # Повторная попытка: ведём накопительную сводку утра — числа
            # одной попытки с пересчитанным планом юрист читает как итог
            # дня (21.08.2026). «Недочитано» — остаток плана ЭТОЙ попытки:
            # с дочиткой слотов план и есть недочитанное на её старте.
            remaining = max(planned - read, 0)
            base = (
                f"за утро прочитано {total_today} карточек, недочитано "
                f"{remaining} (эта попытка: {read} из {planned}, {pct}%)"
            )
        else:
            base = f"прочитано {read} из {planned} карточек ({pct}%)"
    else:
        base = "карточной сводки прогона нет"
    return (base + (", поиски отвечали" if sighted else ", поиски молчали")
            + unavailability_tail(state) + _instance_tail(state))


def gate(state: dict, ctx: dict) -> tuple[bool, str]:
    """(пропустить ли слот, строка для пульта/лога)."""
    if delivered_today(ctx):
        return True, "✓ дайджест сегодня уже отправлен"
    ok, why = run_complete_today(state)
    if ok:
        # Попытка удачна, а доставки нет — сорвался доставочный коммит или
        # это самый первый слот после удачного облачного прогона без штампа.
        # Работать: парс дёшев, доставка закроет день.
        return False, f"✗ дайджест ещё не отправлен ({why}) — отправляем"
    return False, f"✗ {why} — копим и пробуем дальше"


def _mark_delivered() -> int:
    from court_monitor import config
    path = config.LAST_DIGEST_CONTEXT_PATH
    ctx = _load_json(path)
    if not ctx:
        print("контекст дайджеста не читается — штамп не поставлен")
        return 1
    delivery_id = _context_delivery_id(ctx)
    if not delivery_id:
        print(
            "контекст не свежий или в нём нет issue_key — "
            "delivery_id не построен, штамп не поставлен"
        )
        return 1
    stored_id = str(ctx.get("delivery_id") or "")
    if stored_id and stored_id != delivery_id:
        print(
            "delivery_id контекста не совпал с issue_key — "
            "штамп не изменён"
        )
        return 1
    changed = False
    if not stored_id:
        # Backfill для контекста, закрытого до появления delivery_id:
        # delivered_at не двигаем, только даём выпуску имя.
        ctx["delivery_id"] = delivery_id
        changed = True
    if not delivered_today(ctx):
        ctx["delivered_at"] = dt.datetime.now().isoformat(timespec="seconds")
        changed = True
    if changed:
        try:
            _save_context(path, ctx)
        except OSError as exc:
            print(f"не удалось сохранить штамп доставки: {exc}")
            return 1
    print(delivery_id)
    return 0


def _unmark_delivered(expected_delivery_id: str | None = None) -> int:
    """Снять delivered_at: пуш не удался, день обязан остаться открытым.

    Штамп ставится ДО пуша (иначе маркер «(Mac-парсинг)» уехал бы без него и
    replay разослал бы дайджест мимо отметки). Если пуш затем упал, локальный
    штамп закрывает день, а дайджест никуда не ушёл — 24.08.2026 этот
    сценарий был в одном шаге от реализации. Откат условный:
    снимаем только тот штамп, чей delivery_id записала транзакция.
    Иначе запоздавший rollback мог бы открыть уже следующий выпуск.

    Повтор того же отката идемпотентен: delivery_id остаётся в
    контексте, а уже снятого delivered_at нет.
    """
    from court_monitor import config
    path = config.LAST_DIGEST_CONTEXT_PATH
    ctx = _load_json(path)
    if not ctx:
        print("контекст дайджеста не читается — штамп не снят")
        return 1
    expected_delivery_id = str(expected_delivery_id or "").strip()
    stored_id = str(ctx.get("delivery_id") or "").strip()
    if not expected_delivery_id:
        print("delivery_id для отката не указан — штамп не снят")
        return 1
    if stored_id != expected_delivery_id:
        print(
            f"delivery_id не совпал: в контексте {stored_id or '—'}, "
            f"откат ждал {expected_delivery_id} — штамп не снят"
        )
        return 1
    if not ctx.pop("delivered_at", None):
        return 0
    try:
        _save_context(path, ctx)
    except OSError as exc:
        print(f"не удалось сохранить откат штампа доставки: {exc}")
        return 1
    print("штамп доставки снят — день снова открыт")
    return 0


def _region_name() -> str:
    # Имя территории — человеческое (get_region().name), его читает юрист.
    try:
        from court_monitor import config
        from court_monitor.regions import get_region
        return get_region().name or config.REGION
    except Exception:  # noqa: BLE001
        return "территория"


def main(argv: list[str]) -> int:
    if "--mark-delivered" in argv:
        return _mark_delivered()
    if "--unmark-delivered" in argv:
        try:
            delivery_id = argv[argv.index("--delivery-id") + 1]
        except (ValueError, IndexError):
            print("--unmark-delivered требует --delivery-id ID")
            return 2
        return _unmark_delivered(delivery_id)
    if "--delivery-id" in argv:
        delivery_id = _context_delivery_id(_context())
        if not delivery_id:
            print(
                "контекст дайджеста не свежий, не читается или "
                "в нём нет issue_key"
            )
            return 1
        print(delivery_id)
        return 0
    if "--has-pending" in argv:
        return 0 if context_pending(_context()) else 1
    state = _health_state()
    if "--health-alerts" in argv:
        lines = health_alert_lines(state)
        if lines:
            print("\n".join(lines))
        return 0
    if "--run-complete" in argv:
        ok, why = run_complete_today(state)
        print(f"{_region_name()}: {why}")
        return 0 if ok else 1
    if "--progress" in argv:
        print(progress_line(state))
        return 0
    skip, text = gate(state, _context())
    if "--report" in argv:
        print(f"{_region_name()}: {text}")
    return 0 if skip else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
