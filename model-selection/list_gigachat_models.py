"""Read the authorized model catalogue; never generate text or export credentials."""
import json
import os
from pathlib import Path
import sys

import requests
import urllib3

sys.path.insert(0, str(Path('scripts').resolve()))
from court_monitor.digest import llm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
out = Path(os.environ['BENCHMARK_OUTPUT'])
out.mkdir(parents=True, exist_ok=True)
token = llm._gigachat_access_token() or llm._gigachat_access_token()
results = []
if token:
    for url in ('https://api.giga.chat/v1/models',
                'https://gigachat.devices.sberbank.ru/api/v1/models'):
        for attempt in range(2):
            row = {'url': url, 'attempt': attempt + 1}
            try:
                response = requests.get(url, headers={'Authorization': f'Bearer {token}'},
                                        timeout=30, verify=False)
                row['http_status'] = response.status_code
                if response.ok:
                    data = response.json()
                    row['models'] = [{k: item.get(k) for k in ('id', 'type', 'owned_by')}
                                     for item in data.get('data', []) if isinstance(item, dict)]
                results.append(row)
                if response.status_code < 500:
                    break
            except (requests.RequestException, ValueError) as exc:
                row['error_type'] = type(exc).__name__
                results.append(row)
else:
    results.append({'error_type': 'oauth_unavailable'})
report = {'generation_requests': 0, 'results': results}
(out / 'models.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
print(json.dumps(report, ensure_ascii=False), flush=True)
