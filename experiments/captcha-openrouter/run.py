"""Isolated image benchmark. Does not submit captcha answers or import project modules."""
import base64
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import random
import re
import time
import requests

OUT = Path('benchmark-output')
MODELS = ['nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free']
PROMPT = ('Read the five digits from left to right. Preserve leading zeros and repeated digits. '
          'Check that the last digit is included. Return only the five digits, without spaces '
          'or explanation. If you cannot read them, return UNKNOWN.')
URL = 'https://oblsud.hmao.sudrf.ru/modules.php?name=sud_delo&srv_num=1&name_op=sf&delo_id=5&new=5'

class Images(HTMLParser):
    def __init__(self):
        super().__init__()
        self.images = []
    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        uri = attrs.get('src', '')
        if tag == 'img' and re.match(r'data:\s*image/png;base64,', uri, re.I):
            data = base64.b64decode(re.sub(r'\s', '', uri.split(',', 1)[1]), validate=True)
            if data.startswith(b'\x89PNG\r\n\x1a\n') and len(data) < 200000:
                self.images.append(data)

def collect():
    saved = Path('benchmark-input')
    files = sorted(saved.glob('*.png'))
    files = [p for p in files if p.stem.isdigit()]
    if [p.name for p in files] != [f'{i:02}.png' for i in range(1, 21)]:
        raise ValueError('Expected exactly the 20 saved images; no court requests permitted')
    images = [(p.name, p.read_bytes()) for p in files]
    (OUT / 'sample-hashes.json').write_text(json.dumps({name: hashlib.sha256(data).hexdigest()
                                                    for name, data in images}, indent=2))
    return images


def evaluate(images, key):
    rng = random.Random(20261017)
    unavailable = set()
    with (OUT / 'responses.jsonl').open('x', encoding='utf-8') as stream:
        for name, image in images:
            order = MODELS.copy()
            rng.shuffle(order)
            for model in order:
                if model in unavailable:
                    continue
                payload = {'model': model, 'temperature': 0, 'max_tokens': 128,
                           'reasoning': {'enabled': False}, 'messages': [{'role': 'user', 'content': [
                    {'type': 'text', 'text': PROMPT}, {'type': 'image_url', 'image_url': {
                        'url': 'data:image/png;base64,' + base64.b64encode(image).decode()}}]}]}
                if model == 'qwen/qwen3-vl-30b-a3b-instruct':
                    payload['provider'] = {'order': ['DeepInfra'], 'allow_fallbacks': False}
                row = {'image': name, 'model': model}
                start = time.monotonic()
                try:
                    response = requests.post('https://openrouter.ai/api/v1/chat/completions',
                        headers={'Authorization': 'Bearer ' + key}, json=payload, timeout=(10, 90))
                    row['http_status'] = response.status_code
                    if response.status_code == 429:
                        try:
                            row['error_message'] = str(response.json().get('error', {}).get('message', ''))[:500]
                        except ValueError:
                            pass
                    response.raise_for_status()
                    body = response.json()
                    row.update(usage=body.get('usage'), request_id=body.get('id'), provider=body.get('provider'))
                    if body.get('error'):
                        row['api_error_code'] = body['error'].get('code')
                    choice = body['choices'][0]
                    answer = (choice['message'].get('content') or '').strip()
                    row.update(answer=answer, finish_reason=choice.get('finish_reason'),
                        valid=bool(re.fullmatch('[0-9]{5}', answer)) and choice.get('finish_reason') != 'length',
                        usage=body.get('usage'), provider=body.get('provider'), request_id=body.get('id'),
                        status='response')
                except (requests.RequestException, ValueError, KeyError, TypeError, IndexError) as exc:
                    row['status'] = type(exc).__name__
                row['seconds'] = round(time.monotonic() - start, 3)
                stream.write(json.dumps(row, ensure_ascii=False) + '\n')
                stream.flush()
                print(name, model, row['status'], flush=True)
                if row.get('http_status') == 429:
                    unavailable.add(model)
                if row.get('http_status') in (401, 402, 403):
                    raise ValueError('access, credits or rate limit; stopped without retries')
                time.sleep(4)

def main():
    OUT.mkdir(exist_ok=True)
    key = os.environ.get('OPENROUTER_API_KEY')
    if not key:
        raise SystemExit('Missing API key; no requests made')
    stage = 'collection'
    try:
        images = collect()
        stage = 'models'
        evaluate(images, key)
    except Exception as exc:
        # Never serialize response bodies, request headers or credential values.
        (OUT / 'failure.json').write_text(json.dumps({'stage': stage, 'error_type': type(exc).__name__, 'reason': str(exc) if type(exc) is ValueError else None}))
        raise SystemExit(f'{stage} failed: {type(exc).__name__}')

if __name__ == '__main__':
    main()
