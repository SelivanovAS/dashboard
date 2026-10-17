"""Read only quota metadata, never print credentials or full account response."""
import json, os
from pathlib import Path
import requests
key=os.environ.get('OPENROUTER_API_KEY')
if not key:raise SystemExit('Missing key')
r=requests.get('https://openrouter.ai/api/v1/key',headers={'Authorization':'Bearer '+key},timeout=30)
if r.status_code!=200:raise SystemExit(f'Quota check HTTP {r.status_code}')
data=r.json().get('data',{})
quota=data.get('free_model_daily_requests')
Path('quota-output').mkdir(exist_ok=True)
safe={k:quota.get(k) for k in ('used','limit','remaining')} if isinstance(quota,dict) else {'available':False}
Path('quota-output/quota.json').write_text(json.dumps(safe))
print(json.dumps(safe))
if not isinstance(safe.get('remaining'),(int,float)) or safe['remaining']<50:
    raise SystemExit('Cannot confirm room for 50 free calls; no model or court calls made')
