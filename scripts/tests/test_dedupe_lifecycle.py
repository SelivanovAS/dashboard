# -*- coding: utf-8 -*-
"""Тесты дедупов lifecycle: сироты по базовому номеру и кассация по УИД.

Контекст (разбор лога 12.08.2026): оба дедупа шумели ложными WARNING'ами —
`dedupe_orphan_by_base_number` группирует по ГОЛОМУ номеру, а номера дел не
уникальны между судами (2-813/2026 жил сразу в трёх судах bank-трека);
`dedupe_cassation_by_uid` предупреждал о «2 не-discovery записях», хотя
несколько апел. производств одного дела 1-й инст. (основная жалоба + частная)
штатно делят один УИД, и сливать при этом нечего (discovery-двойников нет).
"""

from __future__ import annotations

import os
import sys
import unittest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(TESTS_DIR)
sys.path.insert(0, SCRIPTS_DIR)

from court_monitor.lifecycle import (  # noqa: E402
    dedupe_cassation_by_uid,
    dedupe_orphan_by_base_number,
)


def _orphan(num="2-208/2026", court="Сургутский городской суд", domain=""):
    """Апел.-стаб без реального FI (сирота)."""
    fi = {"court": court}
    if domain:
        fi["court_domain"] = domain
    return {
        "id": num,
        "current_stage": "appeal",
        "appeal": {"case_number": "33-100/2026"},
        "first_instance": fi,
    }


def _owner(num="2-208/2026", court="Сургутский городской суд",
           domain="surgut--hmao.sudrf.ru", stage="first_instance"):
    """Запись 1-й инст. с реальными данными карточки (хозяин)."""
    return {
        "id": num,
        "current_stage": stage,
        "first_instance": {
            "court": court,
            "court_domain": domain,
            "events": [{"date": "01.06.2026", "text": "Иск принят"}],
        },
    }


class TestDedupeOrphanCourtAware(unittest.TestCase):
    def test_same_court_pair_merged(self):
        cases = [
            _owner(domain="surgut--hmao.sudrf.ru"),
            _orphan(domain="surgut--hmao.sudrf.ru"),
        ]
        merged = dedupe_orphan_by_base_number(cases)
        self.assertEqual(merged, 1)
        self.assertEqual(len(cases), 1)
        self.assertEqual(cases[0]["current_stage"], "appeal")
        self.assertEqual(cases[0]["appeal"]["case_number"], "33-100/2026")

    def test_cross_court_pair_not_merged(self):
        # Сирота из Когалыма, хозяин из Сургута: раньше слились бы —
        # это склейка двух РАЗНЫХ дел с совпадающим номером.
        cases = [
            _owner(court="Сургутский городской суд",
                   domain="surgut--hmao.sudrf.ru"),
            _orphan(court="Когалымский городской суд",
                    domain="kogalym--hmao.sudrf.ru"),
        ]
        with self.assertLogs("court-monitor", level="WARNING") as logs:
            merged = dedupe_orphan_by_base_number(cases)
        self.assertEqual(merged, 0)
        self.assertEqual(len(cases), 2)
        self.assertTrue(
            any("неоднозначная группа" in ln for ln in logs.output),
            logs.output,
        )

    def test_bare_number_collision_without_orphans_is_silent(self):
        # Сценарий 2-813/2026: три полноценных дела в трёх судах —
        # сливать нечего, WARNING не печатается.
        cases = [
            _owner(num="2-813/2026", court="Советский районный суд",
                   domain="sovetsk--hmao.sudrf.ru"),
            _owner(num="2-813/2026", court="Когалымский городской суд",
                   domain="kogalym--hmao.sudrf.ru"),
            _owner(num="2-813/2026", court="Мегионский городской суд",
                   domain="megion--hmao.sudrf.ru"),
        ]
        import logging
        court_log = logging.getLogger("court-monitor")
        records: list[logging.LogRecord] = []
        handler = logging.Handler()
        handler.emit = records.append  # type: ignore[assignment]
        court_log.addHandler(handler)
        try:
            merged = dedupe_orphan_by_base_number(cases)
        finally:
            court_log.removeHandler(handler)
        self.assertEqual(merged, 0)
        self.assertEqual(len(cases), 3)
        self.assertFalse(
            [r for r in records if r.levelno >= logging.WARNING],
            [r.getMessage() for r in records],
        )

    def test_orphan_without_court_still_merges(self):
        # Легаси-стаб без суда: пустой ключ матчит любой — прежнее
        # лечение сохраняется.
        cases = [_owner(), _orphan(court="", domain="")]
        merged = dedupe_orphan_by_base_number(cases)
        self.assertEqual(merged, 1)
        self.assertEqual(len(cases), 1)

    def test_two_owners_different_courts_merges_into_matching(self):
        # Раньше «1 сирота + 2 хозяина» блокировались целиком; с учётом
        # суда чужой хозяин отсеивается и пара разблокируется.
        stranger = _owner(court="Когалымский городской суд",
                          domain="kogalym--hmao.sudrf.ru")
        cases = [
            stranger,
            _owner(domain="surgut--hmao.sudrf.ru"),
            _orphan(domain="surgut--hmao.sudrf.ru"),
        ]
        merged = dedupe_orphan_by_base_number(cases)
        self.assertEqual(merged, 1)
        self.assertEqual(len(cases), 2)
        self.assertEqual(cases[0], stranger)
        self.assertEqual(cases[1]["current_stage"], "appeal")

    def test_court_key_fallback_via_registry(self):
        # У сироты домена нет — суд резолвится по короткому имени через
        # реестр региона (ё/е-нормализация внутри матчера).
        from court_monitor.courts import match_fi_court_by_short_name
        cfg = match_fi_court_by_short_name("Сургутский городской суд")
        self.assertIsNotNone(cfg)
        cases = [
            _owner(domain=cfg.domain),
            _orphan(court="Сургутский городской суд", domain=""),
        ]
        merged = dedupe_orphan_by_base_number(cases)
        self.assertEqual(merged, 1)


if __name__ == "__main__":
    unittest.main()
