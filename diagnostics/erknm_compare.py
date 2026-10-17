"""Изолированное сравнение двух моделей на одинаковых капчах ЕРКНМ."""
import asyncio
import base64
import io
import json
import logging
import os
from pathlib import Path
import re
import statistics
import time

import httpx
from PIL import Image

MODELS = ['nvidia/nemotron-3.5-super-vl:free', 'qwen/qwen3-vl-235b-a22b-instruct']
PROMPT = ('На картинке капча из 5 символов: заглавные русские буквы и цифры, латинских букв нет. '
          'Прочитай символы строго слева направо. Различай З и 3, О и 0, Б и 6, Ч и 4. '
          'Ответь только этими символами, без пробелов, кавычек и пояснений.')
OUT = Path('erknm-comparison')
ROWS = []
logging.getLogger('httpx').setLevel(logging.WARNING)


def normalize(text):
    return re.sub(r'[^0-9А-ЯЁ]', '', text.split('</think>')[-1].rsplit(':', 1)[-1].upper().translate(str.maketrans('ABCEHKMOPTXY', 'АВСЕНКМОРТХУ')))


def save():
    OUT.mkdir(exist_ok=True)
    (OUT / 'results.json').write_text(json.dumps(ROWS, ensure_ascii=False, indent=2))
    lines = ['# Сравнение капч ЕРКНМ', '', 'Одинаковые изображения, увеличение 3×, промпт рабочего бота; без резервов и повторных вызовов моделей.', '',
             '| Модель | Ответов | Подтверждено верных | Размечено сайтом | Медиана, с | Стоимость USD по API |', '|---|---:|---:|---:|---:|---:|']
    for model in MODELS:
        rs = [a for row in ROWS for a in row.get('answers', []) if a['model'] == model]
        judged = [(row, a) for row in ROWS for a in row.get('answers', []) if a['model'] == model and row.get('truth')]
        costs = [a.get('cost') for a in rs]
        cost = f'{sum(costs):.6f}' if costs and all(isinstance(c, (int, float)) for c in costs) else 'неполные данные'
        median = statistics.median([a['seconds'] for a in rs]) if rs else 0
        lines.append(f"| {model} | {sum(bool(a.get('code')) for a in rs)}/{len(rs)} | {sum(a.get('code') == row['truth'] for row, a in judged)} | {len(judged)} | {median:.2f} | {cost} |")
    lines += ['', 'Отказ сайта без принятого альтернативного кода не считается доказанной ошибкой OCR. Картинки сохранены для независимой проверки.', '', '```json', json.dumps(ROWS, ensure_ascii=False, indent=2), '```']
    report = '\n'.join(lines)
    (OUT / 'report.md').write_text(report)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        Path(os.environ['GITHUB_STEP_SUMMARY']).write_text(report)


async def answer(api, model, data):
    started = time.monotonic()
    result = {'model': model}
    payload = {'model': model, 'temperature': 0, 'max_tokens': 2048, 'messages': [{'role': 'user', 'content': [{'type': 'text', 'text': PROMPT}, {'type': 'image_url', 'image_url': {'url': data}}]}]}
    if model.startswith('nvidia/'):
        payload['reasoning'] = {'enabled': False}
    try:
        response = await api.post('https://openrouter.ai/api/v1/chat/completions', json=payload)
        result['http'] = response.status_code
        if response.status_code == 200:
            body = response.json()
            choice = body['choices'][0]
            raw = choice['message'].get('content') or ''
            code = normalize(raw)
            result.update(raw=raw[:2000], code=code if 4 <= len(code) <= 7 else '', finish=choice.get('finish_reason'), provider=body.get('provider'))
            usage = body.get('usage') or {}
            result.update(cost=usage.get('cost'), prompt_tokens=usage.get('prompt_tokens'), completion_tokens=usage.get('completion_tokens'))
        else:
            result['error'] = 'Ошибка API модели'
    except Exception as e:
        result['error'] = type(e).__name__
    result['seconds'] = round(time.monotonic() - started, 3)
    return result


async def main():
    OUT.mkdir(exist_ok=True)
    key = os.environ.get('OPENROUTER_API_KEY')
    if not key:
        raise RuntimeError('Секрет OPENROUTER_API_KEY пуст')
    async with httpx.AsyncClient(headers={'Authorization': f'Bearer {key}'}, timeout=90) as api:
        catalogue = await api.get('https://openrouter.ai/api/v1/models')
        catalogue.raise_for_status()
        available = {m['id']: m for m in catalogue.json()['data']}
        (OUT / 'models.json').write_text(json.dumps({m: available.get(m) for m in MODELS}, ensure_ascii=False, indent=2))
        missing = [m for m in MODELS if m not in available]
        if missing:
            print('Модели отсутствуют в текущем каталоге:', ', '.join(missing), flush=True)
            ROWS.append({'error': 'Модели отсутствуют в каталоге', 'models': missing})
            save()
            raise SystemExit(2)
        for number in range(1, 11):
            row = {'number': number, 'answers': [], 'validation': []}
            ROWS.append(row)
            try:
                async with httpx.AsyncClient(base_url='https://proverki.gov.ru', timeout=45, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36', 'Accept-Language': 'ru-RU,ru;q=0.9'}) as site:
                    response = await site.get('/public/api/captcha')
                    response.raise_for_status()
                    picture = Image.open(io.BytesIO(response.content)).convert('RGB')
                    picture.save(OUT / f'{number:02}.png')
                    picture = picture.resize((picture.width * 3, picture.height * 3), Image.Resampling.LANCZOS)
                    buf = io.BytesIO()
                    picture.save(buf, format='PNG')
                    data = 'data:image/png;base64,' + base64.b64encode(buf.getvalue()).decode()
                    received = time.monotonic()
                    row['answers'] = await asyncio.gather(*(answer(api, m, data) for m in MODELS))
                    await asyncio.sleep(max(0, 2 - (time.monotonic() - received)))
                    order = row['answers'] if number % 2 else list(reversed(row['answers']))
                    tried = set()
                    for item in order:
                        code = item.get('code')
                        if not code or code in tried:
                            continue
                        tried.add(code)
                        reply = await site.post('/public/api/inspections/token', params={'captcha': code})
                        row['validation'].append({'code': code, 'http': reply.status_code})
                        if reply.status_code == 200:
                            body = reply.json() if 'json' in reply.headers.get('content-type', '') else reply.text
                            token = body.get('token') if isinstance(body, dict) else body
                            if isinstance(token, str) and token.strip():
                                row['truth'] = code
                                break
                        await asyncio.sleep(2)
            except Exception as e:
                row['error'] = type(e).__name__
            save()
            print(json.dumps(row, ensure_ascii=False), flush=True)
            await asyncio.sleep(5)


if __name__ == '__main__':
    try:
        asyncio.run(main())
    finally:
        save()
