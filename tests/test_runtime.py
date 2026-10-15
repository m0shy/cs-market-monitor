import json
import os
import pytest
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from csgo_monitor import cli
from csgo_monitor.contracts import EXIT_PARTIAL
from csgo_monitor.errors import classify_error
from csgo_monitor.routing import RouteSelector
from csgo_monitor.storage import Storage


class RuntimeStorageTests(unittest.TestCase):
    def test_failure_ticket_has_class_and_retry_state(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "db.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )

            attempts = storage.record_failure(
                "job",
                "item",
                "connection reset by peer",
            )

            self.assertEqual(attempts, 1)
            with storage.control_connect() as conn:
                row = conn.execute(
                    """SELECT error_class,failure_status,next_retry_at
                    FROM crawl_failures WHERE job=? AND item_id=?""",
                    ("job", "item"),
                ).fetchone()
            self.assertEqual(row["error_class"], "transport")
            self.assertEqual(row["failure_status"], "deferred")
            self.assertTrue(row["next_retry_at"])
            self.assertEqual(
                storage.unresolved_failures("job", due_only=True),
                [],
            )

    def test_coverage_writes_a_snapshot(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "db.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )

            storage.set_coverage("siteb", "task", "running", 3)

            with storage.control_connect() as conn:
                row = conn.execute(
                    """SELECT status,record_count FROM coverage_snapshots
                    WHERE source=? AND dataset=?""",
                    ("siteb", "task"),
                ).fetchone()
            self.assertEqual(tuple(row), ("running", 3))


class RouteSelectorTests(unittest.TestCase):
    def test_select_uses_first_healthy_route(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "db.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            settings = SimpleNamespace(
                runtime_routes={
                    "siteb_site_api": (
                        "http://bad",
                        "direct",
                    ),
                },
                route_cache_seconds=300,
                route_probe_timeout=1,
                route_failure_threshold=2,
                route_cooldown_seconds=600,
                siteb_open_base="https://open.siteb.com",
            )
            selector = RouteSelector(settings, storage)

            def fake_probe(route_class, route):
                return (503, {"success": False}) if route == "http://bad" else (
                    200,
                    {"success": True, "errorCode": 0},
                )

            with patch.object(selector, "_request_probe", side_effect=fake_probe):
                result = selector.select("siteb_site_api", force=True)

            self.assertIsNotNone(result)
            self.assertEqual(result.route, "direct")


class ResultContractTests(unittest.TestCase):
    def test_sitea_partial_result_uses_partial_exit_code(self):
        with tempfile.TemporaryDirectory() as temp:
            result_path = Path(temp) / "result.json"

            class Collector:
                TASKS = ("detail",)

                def __init__(self, *args, **kwargs):
                    self.task_results = {
                        "detail": {
                            "status": "partial",
                            "error_class": "parse",
                        }
                    }
                    self.last_task_errors = {"detail": "sample"}

                def run(self, *args, **kwargs):
                    return 7

            settings = SimpleNamespace(proxy_url="")
            with patch.object(
                cli,
                "load_settings",
                return_value=settings,
            ), patch.object(
                cli,
                "_storage",
                return_value=object(),
            ), patch.object(
                cli,
                "SiteAApiCollector",
                Collector,
            ), patch.dict(
                os.environ,
                {"CSGO_MONITOR_RESULT_PATH": str(result_path)},
            ):
                exit_code = cli.main(["sitea-api", "--tasks", "detail"])

            payload = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(exit_code, EXIT_PARTIAL)
            self.assertEqual(payload["status"], "partial")
            self.assertEqual(payload["error_class"], "parse")


class ErrorClassificationTests(unittest.TestCase):
    def test_common_runtime_errors_are_classified(self):
        self.assertEqual(
            classify_error("SSL: WRONG_VERSION_NUMBER"),
            "transport",
        )
        self.assertEqual(
            classify_error("quota reached", payload={"errorCode": 106}),
            "quota",
        )
        self.assertEqual(
            classify_error("service unavailable", status=503),
            "transport",
        )
        self.assertEqual(classify_error("HTTP 555"), "audit")


if __name__ == "__main__":
    unittest.main()


class SiteBWsNormalizeTests(unittest.TestCase):
    """实时推送帧归一化：统计/K线/上架三类帧的字段映射。"""

    @pytest.mark.skip(reason="coupled to site adapters removed in open-source build")
    def test_normalize_frame_types(self):
        import tempfile
        from pathlib import Path
        from csgo_monitor.storage import Storage
        from csgo_monitor.siteb_ws import SiteBWebsocketCollector

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(root / "db.sqlite3", root / "raw")
            collector = SiteBWebsocketCollector(storage)
            now = "2026-10-10T08:00:00+00:00"
            rows = collector._normalize({
                "type": "STEAM_TOTAL_STATISTICS_PUSH",
                "data": {"surviveNum": "100", "todayStatistics": {"turnover": 42.5}},
            }, now)
            metrics = {r["metric"]: r["close"] for r in rows}
            self.assertEqual(metrics["survive_num"], 100.0)
            self.assertEqual(metrics["turnover"], 42.5)

            kline = collector._normalize({
                "type": "HOUR_KLINE",
                "data": {"openTime": 1791608400, "openIndex": 730.0,
                         "highIndex": 737.2, "lowIndex": 729.9, "closeIndex": 736.9},
            }, now)
            self.assertEqual(len(kline), 1)
            self.assertEqual(kline[0]["dataset"], "ws_market_kline_h")
            self.assertEqual(kline[0]["open"], 730.0)

            listing = collector._normalize({
                "type": "NEW_FINGERPRINT_ASSET",
                "data": {"asset": {"assetInfo": {
                    "classinfoKey": "a_b", "evaluatePrice": 629.0,
                    "floatWear": "0.0848", "fingerprint": "x" * 64,
                }}},
            }, now)
            self.assertEqual(listing[0]["item_id"], "a_b")
            self.assertEqual(listing[0]["extra"]["floatWear"], "0.0848")
