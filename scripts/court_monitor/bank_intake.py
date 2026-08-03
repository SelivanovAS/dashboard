# -*- coding: utf-8 -*-
"""Правила приёма дел в трек «Иски банка» (банк — истец).

Общий слой для трёх каналов ввода:
- реестр из внутренних систем банка (scripts/import_bank_registry.py),
- разовый сборщик выдачи (scripts/collect_bank_claims.py),
- авто-подхват в ежедневном прогоне (фаза 3b в runs.py).

Здесь только ПРАВИЛА и сборка записи: HTTP-запросы и парсинг карточки остаются
у вызывающего (иначе каналы не смогли бы подменять сеть по-своему, а тесты —
мокать `fetch_card_checked`/`parse_case_card` на уровне своего модуля).

Критерии исключения — решения юриста 26–31.07.2026, см. комментарии у
`_EXCLUDED_RESULT_RX` и `card_rejects`.
"""

from __future__ import annotations

import json
import os
import re
from datetime import date, timedelta

from court_monitor import config
from court_monitor.config import log
from court_monitor.lifecycle import (
    classify_writ_kind,
    fi_decision_date_from_events,
    is_case_archived,
)
from court_monitor.target_search import build_json_entry

# Итоги, с которыми иск банка в трек НЕ берём (список юриста 26.07.2026):
# «оставлено без рассмотрения», «передано по подсудности», «возвращено»,
# «прекращено». «Отказано» осознанно НЕ здесь — по нему возможна апелляция
# банка, ранний сигнал о сроке на жалобу важен.
# С 31.07.2026 — присоединение к другому делу (ст. 151 ГПК): дело живёт дальше
# под номером приёмника, а импортированное первым же прогоном объявилось бы
# завершённым и ушло в архив — чистый шум в дайджесте.
_EXCLUDED_RESULT_RX = re.compile(
    r"без\s+рассмотрени|подсудност|возвращ|прекращ"
    r"|присоединен\w*\s+к\s+другому\s+делу"
    r"|(?:объединен|соединен)\w*\s+в\s+одно\s+производств",
    re.IGNORECASE
)


def row_passes(row: dict) -> tuple[bool, str]:
    """Пропускать ли строку выдачи в трек. Возвращает (ok, причина-отказа)."""
    if row.get("bank_role") != "Истец":
        return False, "role"
    if _EXCLUDED_RESULT_RX.search(row.get("result") or ""):
        return False, "excluded_result"
    if "|" not in (row.get("link") or ""):
        return False, "no_link"
    return True, ""


def card_rejects(card_info: dict, *, skip_appeal: bool = True) -> str:
    """Причина не брать дело по данным КАРТОЧКИ; "" — берём.

    Возвращает "excluded_result" / "excluded_appeal" / "excluded_writ" —
    ключи совпадают со счётчиками каналов ввода.

    `skip_appeal` (решение юриста 31.07.2026): ручные каналы отбрасывают дела
    с признаком апелляции/кассации (`True`, поведение с 30.07.2026 — при
    историческом сборе такое дело побыло бы в треке мусорным транзитом), а
    авто-подхват прогона их БЕРЁТ (`False`): это свежий иск банка, который
    первым же прогоном переедет в основной cases.json (bank_case_left_track) и
    встанет на полный мониторинг апелляции — иначе апелляция по иску банка
    вообще вне охвата, автопоиск 1-й инстанции истцовые дела не заводит.
    """
    # Второй рубеж фильтра итогов: выдача отстаёт от карточки — у дела
    # 2-8442/2026 (dry-run 26.07.2026) в выдаче итога ещё не было, а
    # карточка уже знала «Передано по подсудности».
    card_result = card_info.get("Результат") or ""
    if _EXCLUDED_RESULT_RX.search(card_result):
        return "excluded_result"
    # Дело уже ушло (или уходит) в апелляцию/кассацию.
    if skip_appeal and (
            card_info.get("_fi_appeal_filed")
            or card_info.get("_fi_sent_to_appeal")
            or card_info.get("_fi_cassation_filed")
            or card_info.get("_fi_sent_to_cassation")):
        return "excluded_appeal"
    # Уже выдан ИЛ на исполнение решения — жизненный цикл трека пройден,
    # дело сразу ушло бы в bank-архив. Обеспечительные листы (выданы ДО
    # решения) не считаются — такое дело ещё ждёт «настоящего» ИЛ. Статус
    # листа не важен: «Отозван»/«Возвращен» — лист всё равно был выдан.
    #
    # ⚠️ Якорь — дата РЕШЕНИЯ из событий карточки, не «Дата заседания»
    # (ревизия 30.07.2026). `fi.decision_date` записи на этапе приёма ещё
    # нет, но фолбэк на hearing_date промахивается в обе стороны:
    # • дело БЕЗ решения — «Дата заседания» непуста (последнее session-
    #   событие), и обеспечительный лист, выданный ПОЗЖЕ последнего
    #   заседания, читался бы как «на исполнение» → живое дело молча не
    #   попало бы в трек, причём строка отчёта неотличима от честного
    #   исключения (в боевом пайплайне такого не бывает: там у
    #   нерешённого дела decision_date пуст → classify_writ_kind сразу
    #   возвращает "interim");
    # • дело С решением — «Дата заседания» уезжает вперёд, назначь суд
    #   пост-решенческое заседание (отмена заочного по ст. 237 ГПК,
    #   судебные расходы, индексация), и лист на исполнение стал бы
    #   «обеспечительным» → дело с пройденным циклом попало бы в трек.
    # Фолбэк на «Дату заседания» остаётся для решённой карточки без
    # события решения в истории движения.
    decision_date = fi_decision_date_from_events(card_info.get("_events"))
    if decision_date:
        fi_probe = {"decision_date": decision_date}
    elif (card_info.get("Статус") or "").strip() in ("Решено", "Возвращено"):
        fi_probe = {"hearing_date": card_info.get("Дата заседания", "")}
    else:
        fi_probe = {}
    if any(classify_writ_kind(w, fi_probe) == "enforcement"
           for w in card_info.get("_writs") or []):
        return "excluded_writ"
    return ""


def entry_is_spent(entry: dict) -> bool:
    """Отработало ли дело свой цикл ЕЩЁ ДО постановки на мониторинг.

    Проверка на выходе `make_bank_entry`, последним рубежом после
    `row_passes`/`card_rejects`: те смотрят на отдельные признаки (итог, лист,
    жалоба), а этот — на собранную запись целиком, БОЕВЫМ предикатом
    `is_case_archived`. Запись несёт `current_stage="first_instance"` и
    `track="plaintiff_light"`, поэтому проверка сама уходит в
    `_is_bank_track_archived` — своей копии архивных правил здесь нет и
    расходиться нечему.

    Смысл (разбор 03.08.2026): дело, которое уже подпадает под архивное окно,
    первый же прогон архивирует — но по дороге качает карточку и пишет о
    полугодовой давности решении строку в дайджест. Так вышло с 2-592/2025
    (решение 06.10.2025, в иске отказано, суд сдал дело в архив 12.11.2025):
    заведено 31.07.2026, объявлено «текст решения опубликован» 03.08.2026,
    тем же прогоном ушло в архив. 26 из 27 записей bank-архива прожили в треке
    не больше 3 дней — вся эта работа и весь этот шум не нужны.

    Дела с признаком жалобы предикат не трогает (в `_is_bank_track_archived`
    это первая же ветка) — авто-подхват продолжит заводить их для переезда в
    основной cases.json.
    """
    return is_case_archived(entry)


# ── Негативный кэш отказников ────────────────────────────────────────────────
# Причины, которые не изменятся сами: дело с таким итогом/листом трек не ждёт.
# Сетевые сбои сюда НЕ пишем — их надо ретраить следующим прогоном.
# already_spent тоже вечная: архивное окно со временем только «твердеет», и без
# записи в кэш авто-подхват качал бы карточку такого дела каждый прогон.
PERMANENT_REJECTIONS = ("excluded_result", "excluded_writ", "no_link",
                        "already_spent")


def seen_key(domain: str, case_number: str) -> str:
    """Ключ негативного кэша: «домен|номер» — номера дел не уникальны между
    судами (тот же принцип, что bank_events_key в storage.py)."""
    return f"{(domain or '').strip()}|{(case_number or '').strip()}"


def load_intake_seen(path: str | None = None) -> dict:
    """Прочитать негативный кэш. Битый/отсутствующий файл — пустой кэш:
    сервисные данные не должны ронять прогон."""
    path = path or config.BANK_INTAKE_SEEN_PATH
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        seen = data.get("seen")
        return seen if isinstance(seen, dict) else {}
    except (json.JSONDecodeError, OSError, AttributeError) as e:
        log.warning(f"Негативный кэш подхвата нечитаем ({e}) — считаем пустым")
        return {}


def remember_rejection(seen: dict, domain: str, case_number: str,
                       reason: str, today: date | None = None) -> bool:
    """Запомнить вечный отказ. True — записали (сетевые сбои не пишем)."""
    if reason not in PERMANENT_REJECTIONS:
        return False
    today = today or date.today()
    key = seen_key(domain, case_number)
    rec = seen.get(key) or {}
    rec["reason"] = reason
    rec.setdefault("first_seen", today.isoformat())
    rec["last_seen"] = today.isoformat()
    seen[key] = rec
    return True


def prune_intake_seen(seen: dict, today: date | None = None) -> dict:
    """Выкинуть записи, которых давно не видно в выдаче (строка уехала с
    первой страницы) — иначе файл рос бы вечно."""
    today = today or date.today()
    edge = (today - timedelta(days=config.BANK_INTAKE_SEEN_TTL_DAYS)).isoformat()
    return {k: v for k, v in seen.items()
            if (v.get("last_seen") or v.get("first_seen") or "") >= edge}


def save_intake_seen(seen: dict, path: str | None = None) -> None:
    """Записать кэш (с прунингом). Ошибки записи гасим — сервисный канал не
    имеет права уронить прогон."""
    path = path or config.BANK_INTAKE_SEEN_PATH
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "seen": prune_intake_seen(seen)},
                      f, ensure_ascii=False, indent=1)
    except OSError as e:
        log.warning(f"Негативный кэш подхвата не сохранён: {e}")


def _stamp_court_ids(fi: dict, fi_row: dict, court=None) -> None:
    """Проставить delo_id/srv_num записи 1-й инстанции.

    Прогону они не нужны (карточку он строит через CourtConfig.card_url по
    домену), но ссылку «в суд» из них собирают фронт (app.js buildCourtLink,
    фолбэк 1540005/1) и дайджест. Приоритет srv_num: href строки выдачи →
    конфиг строки → CourtConfig. href авторитетнее: на двухсерверных доменах
    (Покачи на vartovray--hmao.sudrf.ru, Камышловский/Красноуфимский) резолв
    суда по домену всегда отдаёт первый сервер.
    """
    delo_id = fi_row.get("court_delo_id") or (court.delo_id if court else 0)
    if delo_id:
        fi["delo_id"] = delo_id
    srv = (fi_row.get("href_srv_num") or fi_row.get("court_srv_num")
           or (court.srv_num if court else None))
    if srv:
        fi["srv_num"] = srv


def make_bank_entry(fi_row: dict, card_info: dict, operator: str,
                    now_iso: str, source: str = "bank_registry",
                    court=None) -> dict:
    """JSON-запись трека «Иски банка» из поисковой строки + карточки.

    build_json_entry + маркеры трека: track="plaintiff_light",
    import{announced:true} — иски банка в дайджесте не анонсируются как
    «новые иски» основной картотеки (решение юриста 25.07.2026); уже решённые
    получают resolved_emitted=True — старые решения задним числом в дайджест
    не льются. Общая для всех каналов ввода трека.

    `court` (CourtConfig) — источник delo_id/srv_num там, где их нет в строке:
    целевой поиск по номеру (parse_search_row) этих ключей не отдаёт вовсе.
    """
    entry = build_json_entry(fi_row, card_info)
    entry["track"] = "plaintiff_light"
    entry["initial_bank_role"] = fi_row.get("bank_role", "Истец")
    _stamp_court_ids(entry["first_instance"], fi_row, court)
    entry["import"] = {
        "operator": operator, "at": now_iso,
        "source": source, "announced": True,
    }
    fi = entry["first_instance"]
    if (fi.get("status") or "").strip() in ("Решено", "Возвращено"):
        fi["resolved_emitted"] = True
        # ⚠️ Замораживаем дату решения ПРЯМО ЗДЕСЬ. Строкой выше выставлен
        # resolved_emitted, а эмит fi_resolved — единственное место, где
        # decision_date замерзает; для импортированного решённого дела он уже
        # не выстрелит никогда. Без штампа поле осталось бы пустым, и якорем
        # для classify_writ_kind / bank_legal_force_est / архивного окна стал
        # бы фолбэк hearing_date — та самая дрейфующая дата, от которой лист
        # на исполнение молча становится обеспечительным (см. предупреждение
        # у classify_writ_kind). Карточка несёт событие решения с первого
        # парса, брать его неоткуда больше.
        decision_date = fi_decision_date_from_events(card_info.get("_events"))
        if decision_date:
            fi["decision_date"] = decision_date
    # Уже выданные листы переносим в запись сразу — тот же принцип, что
    # resolved_emitted: первый прогон не должен объявить старые ИЛ «новыми»
    # (без переноса FI-цикл эмитнул бы fi_writ_issued задним числом по всем
    # решённым делам пула). События пойдут только на листы, появившиеся
    # ПОСЛЕ постановки на мониторинг.
    if card_info.get("_writs"):
        fi["writs"] = card_info["_writs"]
    _stamp_appeal_flags(fi, card_info)
    return entry


def _stamp_appeal_flags(fi: dict, card_info: dict) -> None:
    """Перенести признаки жалобы/направления наверх из карточки в запись.

    Нужно авто-подхвату (он такие дела берёт, skip_appeal=False): по этим
    полям bank_case_left_track тем же прогоном переводит дело в основной
    cases.json на полный мониторинг апелляции. Без переноса поля появились бы
    только со следующим парсом карточки, а у решённого дела он через неделю
    (writ_weekly) — дело неделю висело бы в лёгком треке, где апелляцию никто
    не ищет. Ставим только поля: события эмитит FI-цикл, как обычно.
    """
    for card_key, fi_key in (
        ("_fi_appeal_filed", "appeal_filed"),
        ("_fi_appeal_filed_date", "appeal_filed_date"),
        ("_fi_sent_to_appeal", "sent_to_appeal"),
        ("_fi_sent_to_appeal_date", "sent_to_appeal_date"),
        ("_fi_cassation_filed", "cassation_filed"),
        ("_fi_cassation_filed_date", "cassation_filed_date"),
        ("_fi_sent_to_cassation", "sent_to_cassation"),
        ("_fi_sent_to_cassation_date", "sent_to_cassation_date"),
    ):
        if card_info.get(card_key):
            fi[fi_key] = card_info[card_key]
