"""Ручная проверка пересказа на вымышленном примере, без картотеки и доставки."""
import json
import logging
import sys
import tempfile
from copy import deepcopy
from pathlib import Path

from court_monitor import config
from court_monitor.digest import llm

ACT = (
    'ВЫМЫШЛЕННЫЙ УЧЕБНЫЙ ПРИМЕР. АПЕЛЛЯЦИОННОЕ ОПРЕДЕЛЕНИЕ\nДело № 33-700/2026\n'
    'Банк просил взыскать долг. Заёмщик ссылался на отсутствие задолженности. '
    'Коллегия проверила расчёт и платёжные документы. Платежи полностью погасили долг. '
    'Определила: решение отменить, в удовлетворении иска банка отказать.'
)
META = {'stage': 'appeal', 'case_number': '33-700/2026', 'court_domain': 'example.invalid'}


def main():
    # Подробные HTTP-тела и секреты не попадают в диагностический артефакт.
    logging.disable(logging.CRITICAL)
    with tempfile.TemporaryDirectory() as temp:
        config.LLM_PROVIDER_STATE_PATH = str(Path(temp) / 'provider.json')
        meta = deepcopy(META)
        summary = llm.summarize_act_motivation(ACT, case_meta=meta, use_cache=False)
        report = {'region': config.REGION, 'fixture': 'fictional_paid_debt',
                  'summary': summary, 'result': meta['_summary_result']}
        Path(sys.argv[1]).write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({'region': config.REGION, 'status': report['result']['status'],
                          'model': report['result'].get('model')}, ensure_ascii=False))
        return 0 if summary else 1


if __name__ == '__main__':
    raise SystemExit(main())
