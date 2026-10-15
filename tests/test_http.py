import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import requests

from csgo_monitor.http import PoliteClient, make_timestamp
from csgo_monitor.storage import Storage


class MakeTimestampTests(unittest.TestCase):
    def test_shape_and_checksum(self):
        with patch("csgo_monitor.http.time.time", return_value=1754000000.123):
            for _ in range(50):
                ts = make_timestamp()
                self.assertEqual(len(ts), 13)
                self.assertTrue(ts.isdigit())
                prefix = ts[:12]
                expected = sum(int(ch) for ch in prefix) % 10
                self.assertEqual(int(ts[12]), expected, ts)

class PoliteClientTimestampTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.storage = Storage(root / "db.sqlite3", root / "raw")

    def tearDown(self):
        self.tmp.cleanup()

    def test_shared_request_slots_are_atomic_across_storage_instances(self):
        root = Path(self.tmp.name)
        other = Storage(root / "db.sqlite3", root / "raw")
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.storage.reserve_request_slot, "sitea", 1.1)
            second = pool.submit(other.reserve_request_slot, "sitea", 1.1)
            waits = sorted((first.result(), second.result()))
        self.assertLess(waits[0], 0.5)
        self.assertGreater(waits[1], 0.7)

    def _fake_session(self, client):
        captured = {}

        def fake_request(method, url, params=None, json=None, timeout=None):
            captured["method"] = method
            captured["url"] = url
            captured["params"] = params
            captured["json"] = json
            captured["timeout"] = timeout

            class FakeResponse:
                status_code = 200
                headers = {"content-type": "application/json"}
                content = b'{"success": true}'

                def __init__(self, url):
                    self.url = url

                def json(self):
                    return {"success": True}

            return FakeResponse(url)

            return FakeResponse()

        client.session.request = fake_request
        return captured

    def test_siteb_post_gets_timestamp_in_body_and_query(self):
        client = PoliteClient("siteb", "https://www.siteb.com", self.storage,
                              delay=0, timestamp_fn=lambda: "1234567890123")
        captured = self._fake_session(client)
        status, payload = client.post("/api/skin/market/v3/page", {"nextId": "x"})
        self.assertEqual(status, 200)
        self.assertEqual(captured["json"]["timestamp"], "1234567890123")
        self.assertEqual(captured["params"]["timestamp"], "1234567890123")
        self.assertEqual(captured["method"], "POST")

    def test_siteb_get_with_params_gets_timestamp(self):
        client = PoliteClient("siteb", "https://www.siteb.com", self.storage,
                              delay=0, timestamp_fn=lambda: "1234567890123")
        captured = self._fake_session(client)
        client.get("/api/user/skin/v1/market-comparsion", {"itemId": "1"})
        self.assertEqual(captured["params"]["timestamp"], "1234567890123")
        self.assertEqual(captured["json"], None)

    def test_siteb_plain_get_untouched(self):
        client = PoliteClient("siteb", "https://www.siteb.com", self.storage,
                              delay=0, timestamp_fn=lambda: "1234567890123")
        captured = self._fake_session(client)
        client.get("/api/index/statistics/v1/summary")
        self.assertIsNone(captured["params"])
        self.assertIsNone(captured["json"])

    def test_open_api_host_untouched(self):
        client = PoliteClient("siteb", "https://open.siteb.com", self.storage,
                              delay=0, timestamp_fn=lambda: "1234567890123")
        captured = self._fake_session(client)
        client.post("/open/cs2/item/v1/kline", {"marketHashName": "AWP | Dragon Lore"})
        self.assertIsNone(captured["params"])
        self.assertEqual(captured["json"], {"marketHashName": "AWP | Dragon Lore"})

    def test_sitea_untouched(self):
        client = PoliteClient("sitea", "https://api.sitea.com", self.storage,
                              delay=0, timestamp_fn=lambda: "1234567890123")
        captured = self._fake_session(client)
        client.post("/proxies/api/v1/info/vol_data_detail", {"vol_id": 150})
        self.assertIsNone(captured["params"])
        self.assertEqual(captured["json"], {"vol_id": 150})

    def test_input_dict_not_mutated(self):
        client = PoliteClient("siteb", "https://www.siteb.com", self.storage,
                              delay=0, timestamp_fn=lambda: "1234567890123")
        captured = self._fake_session(client)
        body = {"nextId": "x"}
        client.post("/api/skin/market/v3/page", body)
        self.assertEqual(body, {"nextId": "x"})
        self.assertEqual(captured["json"]["timestamp"], "1234567890123")

    def test_timeout_separates_fast_connect_from_full_read_window(self):
        client = PoliteClient(
            "siteb", "https://www.siteb.com", self.storage,
            delay=0, timeout=35,
        )
        captured = self._fake_session(client)
        client.get("/api/index/statistics/v1/summary")
        self.assertEqual(captured["timeout"], (8.0, 35.0))

    def test_siteb_api_bootstrap_sets_device_context(self):
        client = PoliteClient(
            "siteb",
            "https://www.siteb.com",
            self.storage,
            delay=0,
            timestamp_fn=lambda: "1234567890123",
        )
        calls = []

        def fake_request(method, url, params=None, json=None, timeout=None):
            calls.append({
                "method": method,
                "url": url,
                "params": params,
                "json": json,
                "headers": dict(client.session.headers),
            })
            if url == "https://www.siteb.com/":
                client.session.cookies.set(
                    "SDT_DeviceId",
                    "device-123",
                    domain="www.siteb.com",
                    path="/",
                )

            class FakeResponse:
                status_code = 200
                headers = {"content-type": "application/json"}
                content = b'{"success": true, "errorCode": 0}'

                def __init__(self, response_url):
                    self.url = response_url

                def json(self):
                    return {"success": True, "errorCode": 0}

            return FakeResponse(url)

        client.session.request = fake_request
        client.post("/api/skin/market/v3/page", {"nextId": ""})

        self.assertEqual(
            [call["url"] for call in calls],
            [
                "https://www.siteb.com/",
                "https://www.siteb.com/api/skin/market/v3/page",
            ],
        )
        request = calls[1]
        self.assertEqual(request["headers"]["x-device-id"], "device-123")
        self.assertEqual(request["headers"]["x-device"], "1")
        self.assertEqual(request["headers"]["x-currency"], "CNY")
        self.assertEqual(request["json"]["timestamp"], "1234567890123")
        self.assertEqual(request["params"]["timestamp"], "1234567890123")

    def test_final_transient_response_does_not_wait_after_last_attempt(self):
        client = PoliteClient(
            "sitea",
            "https://api.example.test",
            self.storage,
            delay=0,
            retries=2,
        )
        calls = []

        def fake_request(method, url, params=None, json=None, timeout=None):
            calls.append((method, url))

            class FakeResponse:
                status_code = 503
                headers = {"content-type": "application/json"}
                content = b'{"success": false}'

                def __init__(self, response_url):
                    self.url = response_url

                def json(self):
                    return {"success": False}

            return FakeResponse(url)

        client.session.request = fake_request
        with patch("csgo_monitor.http.time.sleep") as sleep, patch.object(
            self.storage, "reserve_request_slot", return_value=0,
        ):
            status, payload = client.get("/api/demo")

        self.assertEqual(status, 503)
        self.assertEqual(payload, {"success": False})
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleep.call_count, 1)

    def test_proxy_transport_error_stops_followup_requests(self):
        client = PoliteClient(
            "sitea",
            "https://api.example.test",
            self.storage,
            delay=0,
            retries=4,
            proxy="http://127.0.0.1:7897",
        )
        calls = []

        def fake_request(method, url, params=None, json=None, timeout=None):
            calls.append((method, url))
            raise requests.exceptions.ProxyError("Unable to connect to proxy")

        client.session.request = fake_request
        with patch("csgo_monitor.http.time.sleep") as sleep:
            first = client.get("/api/demo")
            second = client.get("/api/other")

        self.assertEqual(first, (None, None))
        self.assertEqual(second, (None, None))
        self.assertEqual(len(calls), 1)
        self.assertEqual(sleep.call_count, 0)
        self.assertIn("proxy", client.transport_error.lower())

    def test_siteb_session_failure_stops_api_request(self):
        client = PoliteClient(
            "siteb",
            "https://www.siteb.com",
            self.storage,
            delay=0,
            retries=2,
            proxy="http://127.0.0.1:7897",
        )
        calls = []

        def fake_request(method, url, params=None, json=None, timeout=None):
            calls.append((method, url))
            raise requests.exceptions.ProxyError("Unable to connect to proxy")

        client.session.request = fake_request
        with patch("csgo_monitor.http.time.sleep") as sleep:
            result = client.post("/api/skin/market/v3/page", {"nextId": ""})

        self.assertEqual(result, (None, None))
        self.assertEqual(calls, [("GET", "https://www.siteb.com/")])
        self.assertEqual(sleep.call_count, 0)
        self.assertIn("proxy", client.transport_error.lower())


if __name__ == "__main__":
    unittest.main()
