"""Квитанция на устройство до сетевого запроса. Повтор не дублирует уведомление."""
from hashlib import sha256
from pathlib import Path
import sqlite3
from urllib.parse import urlsplit

import requests


def valid_endpoint(value):
    try:
        target = urlsplit(value)
        host = target.hostname or ''
        known = (host in ('fcm.googleapis.com', 'web.push.apple.com', 'updates.push.services.mozilla.com')
                 or host.endswith(('.push.apple.com', '.push.services.mozilla.com', '.notify.windows.com',
                                   '.push.microsoft.com')))
        return bool(known and target.scheme == 'https' and target.port in (443, None)
                    and not target.username and not target.password and not target.fragment)
    except ValueError:
        return False


class PushSession(requests.Session):
    def post(self, url, **kwargs):
        if not valid_endpoint(url):
            raise ValueError('Invalid Web Push destination')
        response = super().post(url, allow_redirects=False, **kwargs)
        if 300 <= response.status_code < 400:
            raise ValueError('Unexpected Web Push redirect')
        return response


class Receipts:
    def __init__(self, path):
        self.path = Path(path)

    def claim(self, endpoint):
        key = sha256(endpoint.encode()).hexdigest()
        with sqlite3.connect(self.path) as db:
            db.execute('CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY, status TEXT NOT NULL)')
            inserted = db.execute("INSERT OR IGNORE INTO receipts VALUES (?, 'unconfirmed')", (key,))
            if not inserted.rowcount:
                return db.execute('SELECT status FROM receipts WHERE id=?', (key,)).fetchone()[0]
        return None

    def finish(self, endpoint, status):
        with sqlite3.connect(self.path) as db:
            db.execute('UPDATE receipts SET status=? WHERE id=?', (status, sha256(endpoint.encode()).hexdigest()))
