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

import re

from court_monitor.lifecycle import (
    classify_writ_kind,
    fi_decision_date_from_events,
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


def make_bank_entry(fi_row: dict, card_info: dict, operator: str,
                    now_iso: str, source: str = "bank_registry") -> dict:
    """JSON-запись трека «Иски банка» из поисковой строки + карточки.

    build_json_entry + маркеры трека: track="plaintiff_light",
    import{announced:true} — иски банка в дайджесте не анонсируются как
    «новые иски» основной картотеки (решение юриста 25.07.2026); уже решённые
    получают resolved_emitted=True — старые решения задним числом в дайджест
    не льются. Общая для всех каналов ввода трека.
    """
    entry = build_json_entry(fi_row, card_info)
    entry["track"] = "plaintiff_light"
    entry["initial_bank_role"] = fi_row.get("bank_role", "Истец")
    entry["import"] = {
        "operator": operator, "at": now_iso,
        "source": source, "announced": True,
    }
    fi = entry["first_instance"]
    if (fi.get("status") or "").strip() in ("Решено", "Возвращено"):
        fi["resolved_emitted"] = True
    # Уже выданные листы переносим в запись сразу — тот же принцип, что
    # resolved_emitted: первый прогон не должен объявить старые ИЛ «новыми»
    # (без переноса FI-цикл эмитнул бы fi_writ_issued задним числом по всем
    # решённым делам пула). События пойдут только на листы, появившиеся
    # ПОСЛЕ постановки на мониторинг.
    if card_info.get("_writs"):
        fi["writs"] = card_info["_writs"]
    return entry
