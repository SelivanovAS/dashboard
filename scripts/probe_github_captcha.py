"""Один настоящий поиск ХМАО: без картотеки, коммитов и доставки.

Явная сетевая проверка после установки GitHub OCR. Расходует вызовы модели.
Финский транспорт и локальные ключи в этом процессе недоступны.
"""
import json
import logging
import os
from pathlib import Path
import sys
import tempfile

os.environ['REGION'] = 'hmao'
os.environ['HMAO_APPEAL_CAPTCHA_ENABLED'] = '1'
os.environ['CM_SUMMARY_GENERATION'] = '0'
for name in ('OPENROUTER_API_KEY', 'CLOUDRU_API_KEY', 'ANTHROPIC_API_KEY', 'GIGACHAT_AUTH_KEY'):
    os.environ.pop(name, None)
os.environ['OPENROUTER_PROXY_URL'] = 'http://127.0.0.1:9'

from court_monitor import config, appeal_captcha, github_captcha
from court_monitor.regions import get_region


def main():
    logging.disable(logging.CRITICAL)
    if not github_captcha.enabled():
        raise SystemExit('GitHub OCR не включён')
    with tempfile.TemporaryDirectory(prefix='captcha-probe-') as temp:
        config.PARSE_TELEMETRY_FILE = str(Path(temp) / 'telemetry.json')
        pilot = appeal_captcha.Pilot()
        html = pilot.search(get_region('hmao').appeal_courts[0])
        report = dict(pilot.value, probe=True, local_model_keys=False,
                      finnish_transport_available=False, production_data_written=False)
        Path(sys.argv[1]).write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({k: report.get(k) for k in ('status', 'executor', 'github_run_id',
            'github_wait_seconds', 'model_calls', 'rows', 'seconds', 'reported_usd')}, ensure_ascii=False))
        return 0 if html and report.get('accepted') and report.get('executor') == 'github' else 1


if __name__ == '__main__':
    raise SystemExit(main())
