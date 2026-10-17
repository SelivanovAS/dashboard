"""Isolated live acceptance test: no case storage, delivery, or production imports."""
import base64, binascii, json, os, re, time
from pathlib import Path
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit, parse_qs
import requests
ORIGIN = 'https://oblsud.hmao.sudrf.ru'
FORM_URL = ORIGIN + '/modules.php?name=sud_delo&srv_num=1&name_op=sf&delo_id=5&new=5'
ALLOWED_HOSTS = {'oblsud.hmao.sudrf.ru','oblsud--hmao.sudrf.ru'}
MODELS = ['qwen/qwen3-vl-30b-a3b-instruct','nvidia/nemotron-3.5-super-vl:free']
PROMPT = 'Read the five digits from left to right. Preserve leading zeros and repeated digits. Check that the last digit is included. Return only the five digits, without spaces or explanation. If you cannot read them, return UNKNOWN.'

class _Stop(Exception):
    def __init__(self, status, http_status=None, *, reason=None):
        self.status, self.http_status, self.reason = status, http_status, reason

class _Form(HTMLParser):
    def __init__(self):
        super().__init__()
        self.active = False
        self.fields = {}
        self.images = []
        self.action = ''
        self.method = ''
        self.forms = 0

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'form':
            self.active = attrs.get('name') == 'calform'
            if self.active:
                self.forms += 1
                self.action = urljoin(FORM_URL, attrs.get('action') or '/modules.php')
                self.method = attrs.get('method', 'get').lower()
        if not self.active:
            return
        if tag == 'input' and attrs.get('type', '').lower() == 'hidden' and attrs.get('name'):
            self.fields[attrs['name']] = attrs.get('value', '')
        if tag == 'img' and attrs.get('src', '').lower().startswith('data:'):
            self.images.append(attrs['src'])

    def handle_endtag(self, tag):
        if tag == 'form':
            self.active = False

    def checked_image(self):
        target = urlsplit(self.action)
        checks = (
            (self.forms == 1, 'form_count'),
            (self.method == 'get', 'form_method'),
            (target.scheme == 'https' and target.netloc in ALLOWED_HOSTS
             and target.path == '/modules.php', 'form_target'),
            (self.fields.get('delo_id') == '5'
             and self.fields.get('delo_table') == 'g2_case', 'form_scope'),
            (self.fields.get('new') == '5', 'form_registry'),
            (bool(self.fields.get('captchaid')), 'captcha_id'),
            (bool(self.images), 'image_missing'),
            (len(self.images) == 1, 'image_count'),
        )
        for valid, reason in checks:
            if not valid:
                raise _Stop('court_form', reason=reason)
        uri = self.images[0]
        # Ограничение до удаления пробелов: длинная строка из пробелов тоже отказ.
        if len(uri) > 200_000:
            raise _Stop('court_form', reason='image_size')
        header, comma, payload = uri[5:].partition(',')
        mime, semicolon, encoding = header.rpartition(';')
        ascii_space = ' \t\r\n\f'
        if (not comma or not semicolon or mime.strip(ascii_space).lower() != 'image/png'
                or encoding.strip(ascii_space).lower() != 'base64'):
            raise _Stop('court_form', reason='image_format')
        # WHATWG data URL: ASCII-пробелы допустимы; байты PNG не преобразуем.
        image = payload.translate(str.maketrans('', '', ascii_space))
        try:
            raw = base64.b64decode(image, validate=True)
        except (ValueError, binascii.Error):
            raise _Stop('court_form', reason='image_base64') from None
        if not raw.startswith(b'\x89PNG\r\n\x1a\n'):
            raise _Stop('court_form', reason='image_signature')
        return base64.b64encode(raw).decode('ascii')

class _ResultRegistry(HTMLParser):
    """Проверяем раздел по ссылкам именно тех строк, которые пойдут на приём."""
    def __init__(self):
        super().__init__()
        self.cards = {}

    def handle_starttag(self, tag, attrs):
        if tag != 'a':
            return
        href = dict(attrs).get('href', '')
        target = urlsplit(urljoin(ORIGIN + '/modules.php', href))
        query = parse_qs(target.query, keep_blank_values=True)
        if query.get('name_op') != ['case']:
            return
        cid, uid = query.get('case_id', []), query.get('case_uid', [])
        if len(cid) != 1 or len(uid) != 1:
            return
        valid = (target.scheme == 'https' and target.netloc in ALLOWED_HOSTS
                 and target.path == '/modules.php'
                 and query.get('delo_id') == ['5'] and query.get('new') == ['5']
                 and query.get('srv_num', ['1']) == ['1'])
        key = cid[0] + '|' + uid[0]
        self.cards[key] = self.cards.get(key, True) and valid

    def matches(self, rows):
        return all(self.cards.get(row.get('Ссылка', '')) is True for row in rows)

def classify(html):
    low = html.lower()
    if 'неверно указан проверочный код' in low or 'неверный проверочный код' in low:
        return 'rejected'
    if re.search(r'name\s*=\s*["\x27]?captcha(?:["\x27\s>])', low):
        return 'unrecognized'
    registry = _ResultRegistry(); registry.feed(html)
    if registry.cards and all(registry.cards.values()):
        return 'accepted'
    if 'данных по запросу не обнаружено' in low:
        return 'accepted_empty'
    return 'unrecognized'

def court(session, url, **kwargs):
    response = session.get(url, timeout=(10,35), allow_redirects=False, **kwargs)
    if response.status_code != 200:
        raise _Stop('court_http',response.status_code)
    return response.content.decode('windows-1251',errors='replace')

def attempt(model, key):
    row = {'model':model,'submitted':False}
    stage = 'court_form'; start = time.monotonic()
    try:
        with requests.Session() as session:
            session.headers['User-Agent']='Mozilla/5.0'
            form=_Form(); form.feed(court(session,FORM_URL,params={'_benchmark':str(time.time_ns())}))
            image=form.checked_image()
            stage='model'; started=time.monotonic()
            payload={'model':model,'temperature':0,'max_tokens':128,'reasoning':{'enabled':False},'messages':[{'role':'user','content':[{'type':'text','text':PROMPT},{'type':'image_url','image_url':{'url':'data:image/png;base64,'+image}}]}]}
            if model==MODELS[0]:payload['provider']={'order':['DeepInfra'],'allow_fallbacks':False}
            response=requests.post('https://openrouter.ai/api/v1/chat/completions',headers={'Authorization':'Bearer '+key},json=payload,timeout=(10,60))
            row['model_seconds']=round(time.monotonic()-started,3);row['model_http']=response.status_code
            if response.status_code!=200:raise _Stop('model_http',response.status_code)
            body=response.json(); row.update(usage=body.get('usage'),provider=body.get('provider'))
            if body.get('error'):raise _Stop('model_error',body['error'].get('code'))
            choice=body['choices'][0];answer=(choice['message'].get('content') or '').strip()
            if choice.get('finish_reason')=='length' or not re.fullmatch('[0-9]{5}',answer):raise _Stop('invalid_answer')
            fields=dict(form.fields);fields.update(name='sud_delo',srv_num='1',name_op='r',delo_id='5',delo_table='g2_case',new='5',case_type='0',G2_PARTS__NAMESS='Сбербанк',captcha=answer,Submit='Найти',list='ON')
            time.sleep(3)
            stage='court_submission';row['submitted']=True
            html=court(session,ORIGIN+'/modules.php',params=[(k,v.encode('windows-1251')) for k,v in fields.items()],headers={'Referer':FORM_URL})
            row['status']=classify(html)
    except _Stop as exc:row.update(status=exc.status,error_code=exc.http_status,reason=exc.reason)
    except (requests.RequestException,ValueError,KeyError,TypeError,IndexError) as exc:row.update(status=stage+'_error',error_type=type(exc).__name__)
    row['seconds']=round(time.monotonic()-start,3)
    return row

def main():
    key=os.environ.get('OPENROUTER_API_KEY')
    if not key:raise SystemExit('Missing key')
    out=Path('benchmark-output');out.mkdir(exist_ok=True)
    with (out/'live-results.jsonl').open('x') as stream:
        for pair in range(10):
            for model in MODELS if pair%2==0 else list(reversed(MODELS)):
                row=attempt(model,key);row['pair']=pair+1;row['batch']=int(os.environ.get('BATCH_INDEX','1'))
                stream.write(json.dumps(row)+'\n');stream.flush();print(pair+1,model,row['status'],flush=True)
                if row.get('error_code') in (401,402,403,429) or row['status'] in ('court_form','court_http','court_form_error','court_submission_error','unrecognized'):
                    raise SystemExit('Stopped on access, rate limit, transport or unrecognized court response; partial results retained')
                time.sleep(5)

if __name__=='__main__':main()
