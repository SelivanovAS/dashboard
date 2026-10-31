"""Протокол не публикует капчу, ограничивает модель и отвергает чужой ответ."""
import base64
import socket
import struct
from threading import Thread

import pytest
import requests

from court_monitor import github_captcha as remote
from scripts import github_captcha_worker as worker


def payload(model=worker.MODEL):
    image = base64.b64encode(b'\x89PNG\r\n\x1a\nsynthetic').decode()
    return {'sequence': 1, 'timeout': [5, 10], 'payload': {'model': model,
        'messages': [{'content': [{}, {'image_url': {'url': 'data:image/png;base64,' + image}}]}]}}


@pytest.mark.parametrize('sequence,expected', [(1, '00444'), (2, None)])
def test_channel_roundtrip_preserves_leading_zero_and_rejects_wrong_request(tmp_path, sequence, expected):
    server, peer = socket.socketpair()
    client = remote.GithubCaptchaClient()
    client.path = tmp_path / 'socket'
    client.connection = server
    seen = []
    def run():
        with peer:
            seen.append(remote.receive_message(peer))
            remote.send_message(peer, {'sequence': sequence, 'status_code': 200,
                'body': {'choices': [{'message': {'content': '00444'}}]}})
    thread = Thread(target=run)
    thread.start()
    try:
        if expected:
            result = client.post({'model': worker.MODEL}, (1, 1))
            assert result.json()['choices'][0]['message']['content'] == expected
        else:
            with pytest.raises(requests.RequestException):
                client.post({}, (1, 1))
        assert seen[0]['sequence'] == 1
    finally:
        client.close()
        thread.join(timeout=3)
        assert not thread.is_alive()


def test_oversized_frame_rejected_before_body_is_read():
    server, peer = socket.socketpair()
    with server, peer:
        peer.sendall(struct.pack('!I', remote.MAX_MESSAGE + 1))
        with pytest.raises(ValueError):
            remote.receive_message(server)


@pytest.mark.parametrize('change', ['model', 'image', 'timeout'])
def test_worker_cannot_send_arbitrary_model_or_payload(monkeypatch, change):
    value = payload()
    if change == 'model':
        value['payload']['model'] = 'other/provider'
    elif change == 'image':
        value['payload']['messages'][0]['content'][1]['image_url']['url'] = 'https://example.org/image'
    else:
        value['timeout'] = [1, 3600]
    monkeypatch.setattr(worker.requests, 'Session', lambda: pytest.fail('Network forbidden'))
    with pytest.raises(ValueError):
        worker.model_request(value, 'test-key')


def test_worker_builds_fixed_prompt_and_does_not_forward_provider_error(monkeypatch):
    calls = []
    class Session:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def post(self, url, **kw):
            calls.append((url, kw))
            response = requests.Response()
            response.status_code = 403
            response._content = b'{"error":"sensitive-provider-diagnostics"}'
            return response
    monkeypatch.setattr(worker.requests, 'Session', Session)
    result = worker.model_request(payload(), 'test-key')
    assert result == {'sequence': 1, 'status_code': 403, 'body': {}}
    assert calls[0][1]['allow_redirects'] is False
    assert calls[0][1]['json']['messages'][0]['content'][0]['text'] == worker.PROMPT
    assert 'proxies' not in calls[0][1]
