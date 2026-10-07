"""Интеграционные проверки шлюза Тюмени: только loopback и подменённый Worker.

Внешних запросов, настоящих ключей, импорта и записей в KV нет.
"""
from __future__ import annotations

import hashlib
import http.client
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import threading
import unittest
from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch


REPO = Path(__file__).resolve().parents[2]
path = Path(os.environ.get("RELAY_PATH", REPO / "ops/vps-run/tyumen_upload_relay.py"))
spec = importlib.util.spec_from_file_location("relay_under_test", path)
relay = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = relay
spec.loader.exec_module(relay)


class _CompletionHandler(relay.Handler):
    def do_POST(self):
        try:
            super().do_POST()
        finally:
            # Ответ уже мог дойти до клиента; событие подтверждает также finally.
            self.server.post_completed.set()


class _CompletionServer(relay.RelayServer):
    def finish_request(self, request, client_address):
        _CompletionHandler(request, client_address, self)


class RelayHTTPTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.downloads = []
        self.refs = []
        self.failure_mode = None
        self.post_requested = False
        self.server = _CompletionServer(("127.0.0.1", 0), worker_post=self.fake_worker)
        self.server.post_completed = threading.Event()
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method, path, body=None, headers=None):
        if method == "POST":
            self.post_requested = True
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=3)
        try:
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def download(self, ref, token=None):
        return self.request("GET", relay.DOWNLOAD_PREFIX + ref["id"], headers={
            "Authorization": "Bearer " + (token or ref["token"])
        })

    def fake_worker(self, secret, body, origin):
        self.calls.append((secret, body, origin))
        if secret != "operator-test-only":
            return 403, {"Content-Type": "application/json"}, b'{"error":"Forbidden"}'
        if body == relay.AUTH_PROBE:
            if self.failure_mode == "ambiguous_auth":
                return 400, {}, b'{"error":"Something changed upstream"}'
            return 400, {}, json.dumps({"error": relay.VALIDATION_ERROR}).encode()
        ref = json.loads(body)["__gateway_upload"]
        self.refs.append(ref)
        self.assertEqual(self.download(ref, "0" * 64)[0], 404)
        self.assertEqual(self.request("GET", relay.DOWNLOAD_PREFIX + ref["id"])[0], 404)
        status, headers, original = self.download(ref)
        self.assertEqual(status, 200)
        self.assertEqual(int(headers["Content-Length"]), ref["bytes"])
        self.assertTrue(headers["Content-Type"].startswith("application/json"))
        self.assertEqual(hashlib.sha256(original).hexdigest(), ref["sha256"])
        self.downloads.append(original)
        if self.failure_mode == "timeout_after_download":
            raise TimeoutError("fake upstream stopped after consuming the payload")
        if self.failure_mode == "missing_digest":
            return 200, {}, b'{"ok":true}'
        digest = "0" * 64 if self.failure_mode == "wrong_digest" else ref["sha256"]
        status = 400 if self.failure_mode == "validation_failure" else 200
        return status, {"x-import-gateway-sha256": digest}, b'{"ok":true}'

    def assert_cleaned(self):
        if self.post_requested:
            self.assertTrue(self.server.post_completed.wait(timeout=3), "POST handler did not finish")
        self.assertEqual(len(self.server.store._items), 0)
        for ref in self.refs:
            self.assertEqual(self.download(ref)[0], 404)
        # Every request returns its concurrency slot, even an exception path.
        slots = []
        try:
            for _ in range(relay.MAX_ACTIVE):
                acquired = self.server.active.acquire(blocking=False)
                self.assertTrue(acquired)
                slots.append(acquired)
            self.assertFalse(self.server.active.acquire(blocking=False))
        finally:
            for _ in slots:
                self.server.active.release()

    def post(self, body=b'{"court_domain":""}', secret="operator-test-only", headers=None):
        return self.request("POST", relay.IMPORT_PATH + "?secret=" + secret, body, headers)

    def test_large_unicode_body_exact_bytes_and_private_download(self):
        body = json.dumps({"court_domain": "", "html": "<p>Тюмень ☃</p>" * 12000}, ensure_ascii=False).encode()
        self.assertGreater(len(body), 128 * 1024)
        status, headers, _ = self.post(body, headers={"Origin": "https://api2-tyumen.delosud.ru"})
        self.assertEqual(status, 200)
        self.assertEqual(self.downloads, [body])
        self.assertEqual(len(self.calls), 2)  # One auth check and one delivery, never a retry.
        self.assertTrue(all(c[2] == "https://api2-tyumen.delosud.ru" for c in self.calls))
        self.assertEqual(headers["Cache-Control"], "no-store, private")
        self.assert_cleaned()

    def test_unauthorized_never_stores_or_delivers_body(self):
        self.assertEqual(self.post(b"x" * 32768, secret="wrong-test-key")[0], 403)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][1], relay.AUTH_PROBE)
        self.assertEqual(self.downloads, [])
        self.assert_cleaned()

    def test_ambiguous_auth_fails_closed(self):
        self.failure_mode = "ambiguous_auth"
        self.assertEqual(self.post()[0], 502)
        self.assertEqual(len(self.calls), 1)
        self.assert_cleaned()

    def test_duplicate_role_key_cannot_select_one_authorized_value(self):
        self.assertEqual(self.post(secret="operator-test-only&secret=other")[0], 403)
        self.assertEqual(self.calls[0][0], "")
        self.assert_cleaned()

    def test_validated_worker_error_is_forwarded_and_cleaned(self):
        self.failure_mode = "validation_failure"
        self.assertEqual(self.post()[0], 400)
        self.assertEqual(len(self.calls), 2)
        self.assert_cleaned()

    def test_success_without_original_body_hash_fails_closed(self):
        self.failure_mode = "missing_digest"
        self.assertEqual(self.post()[0], 502)
        self.assertEqual(len(self.calls), 2)
        self.assert_cleaned()

    def test_wrong_body_hash_fails_closed(self):
        self.failure_mode = "wrong_digest"
        self.assertEqual(self.post()[0], 502)
        self.assertEqual(len(self.calls), 2)
        self.assert_cleaned()

    def test_timeout_after_worker_fetch_does_not_replay_upload(self):
        self.failure_mode = "timeout_after_download"
        status, _, response = self.post()
        self.assertEqual(status, 502)
        self.assertIn("Проверьте журнал", response.decode())
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(len(self.downloads), 1)
        self.assert_cleaned()

    def test_oversize_is_rejected_before_auth_or_buffering(self):
        self.assertEqual(self.post(b"x", headers={"Content-Length": str(relay.MAX_BYTES + 1)})[0], 413)
        self.assertEqual(self.calls, [])
        self.assert_cleaned()

    def test_chunked_upload_is_rejected_before_auth(self):
        self.assertEqual(self.post(b"x", headers={"Transfer-Encoding": "chunked"})[0], 411)
        self.assertEqual(self.calls, [])
        self.assert_cleaned()

    def test_full_capacity_rejects_without_worker_request(self):
        for _ in range(relay.MAX_ACTIVE):
            self.server.active.acquire()
        try:
            self.assertEqual(self.post()[0], 503)
            self.assertEqual(self.calls, [])
        finally:
            for _ in range(relay.MAX_ACTIVE):
                self.server.active.release()
        self.assert_cleaned()

    def test_unrelated_post_path_never_reaches_worker(self):
        self.assertEqual(self.request("POST", "/other", b"{}") [0], 404)
        self.assertEqual(self.calls, [])

    def test_allowed_preflight_never_calls_worker(self):
        status, headers, body = self.request("OPTIONS", relay.IMPORT_PATH, headers={
            "Origin": relay.ALLOWED_ORIGIN,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Content-Type",
        })
        self.assertEqual(status, 204)
        self.assertEqual(body, b"")
        self.assertEqual(headers["Access-Control-Allow-Origin"], relay.ALLOWED_ORIGIN)
        self.assertIn("POST", headers["Access-Control-Allow-Methods"])
        self.assertIn("Content-Type", headers["Access-Control-Allow-Headers"])
        self.assertEqual(self.calls, [])
        self.assert_cleaned()

    def test_preflight_never_grants_an_arbitrary_origin(self):
        status, headers, _ = self.request("OPTIONS", relay.IMPORT_PATH, headers={
            "Origin": "https://untrusted.example",
        })
        self.assertEqual(status, 204)
        self.assertNotEqual(headers["Access-Control-Allow-Origin"], "https://untrusted.example")
        self.assertNotEqual(headers["Access-Control-Allow-Origin"], "*")
        self.assertEqual(self.calls, [])

    def test_unrelated_preflight_is_not_an_upload_endpoint(self):
        self.assertEqual(self.request("OPTIONS", "/unrelated")[0], 404)
        self.assertEqual(self.calls, [])

    def test_expiry_removes_upload_and_denies_token(self):
        with patch.object(relay.time, "monotonic", return_value=10):
            upload_id, token = self.server.store.add(b"{}")
        with patch.object(relay.time, "monotonic", return_value=10 + relay.UPLOAD_TTL):
            self.assertIsNone(self.server.store.get(upload_id, token))
        self.assertEqual(len(self.server.store._items), 0)


class RealPostFunctionTests(unittest.TestCase):
    def test_upstream_redirect_is_not_followed_or_retried(self):
        requests = []

        class RedirectHandler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                requests.append(self.path)
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(307)
                self.send_header("Location", "/must-not-be-followed")
                self.send_header("Content-Length", "0")
                self.end_headers()

        server = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01}, daemon=True)
        thread.start()
        try:
            upstream = "http://%s:%d" % server.server_address
            with patch.object(relay, "UPSTREAM", upstream):
                with self.assertRaisesRegex(ValueError, "upstream_redirect"):
                    relay.post_worker("local-test-key", relay.AUTH_PROBE)
            self.assertEqual(len(requests), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    # Relay structured event logs contain no secrets; suppress routine test noise.
    with redirect_stdout(io.StringIO()):
        unittest.main(verbosity=2)
