"""Межпроцессный лимит судов для регионов на одном исполнителе."""
from contextlib import contextmanager
import hashlib
import os
import time
from urllib.parse import urlsplit

from court_monitor import config


@contextmanager
def request_slot(url, remaining=lambda: None):
    directory = config.COURT_REQUEST_COORD_DIR
    if not directory:
        yield True
        return
    import fcntl
    # Портал перенаправляет oblsud--hmao на oblsud.hmao: это один сервер.
    host = (urlsplit(url).hostname or '').lower().replace('--', '.')
    os.makedirs(directory, mode=0o700, exist_ok=True)
    path = os.path.join(directory, hashlib.sha256(host.encode()).hexdigest() + '.lock')
    with open(path, 'a+') as handle:
        acquired = False
        try:
            while not acquired:
                left = remaining()
                if left is not None and left <= 0:
                    yield False
                    return
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                except BlockingIOError:
                    time.sleep(min(0.1, max(left or 0.1, 0.001)))
            handle.seek(0)
            try:
                previous = float(handle.read() or 0)
            except ValueError:
                previous = 0
            delay = max(0, 3 - (time.monotonic() - previous))
            # После перезагрузки monotonic обнуляется.
            if delay > 3:
                delay = 0
            left = remaining()
            if left is not None and left <= delay:
                yield False
                return
            if delay:
                time.sleep(delay)
            handle.seek(0)
            handle.truncate()
            handle.write(str(time.monotonic()))
            handle.flush()
            yield True
        finally:
            if acquired:
                fcntl.flock(handle, fcntl.LOCK_UN)
