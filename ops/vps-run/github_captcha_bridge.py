#!/usr/bin/env python3
"""Forced SSH command: только один существующий локальный канал OCR."""
import os
import re
import selectors
import signal
import socket
import sys


def main():
    request_id = os.environ.get('SSH_ORIGINAL_COMMAND', '')
    if not re.fullmatch(r'[0-9a-f]{32}', request_id):
        return 2
    signal.alarm(300)
    with socket.socket(socket.AF_UNIX) as channel:
        channel.connect('/run/court-github-ocr/' + request_id + '.sock')
        with selectors.DefaultSelector() as selector:
            selector.register(sys.stdin.buffer, selectors.EVENT_READ, 'input')
            selector.register(channel, selectors.EVENT_READ, 'socket')
            while True:
                events = selector.select(200)
                if not events:
                    return 3
                for key, _ in events:
                    if key.data == 'input':
                        data = os.read(sys.stdin.fileno(), 65536)
                        if not data:
                            return 0
                        channel.sendall(data)
                    else:
                        data = channel.recv(65536)
                        if not data:
                            return 0
                        sys.stdout.buffer.write(data)
                        sys.stdout.buffer.flush()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError):
        sys.exit(1)
