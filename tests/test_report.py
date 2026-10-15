import json
import tempfile
import unittest
from pathlib import Path

from csgo_monitor.cli import parser
from csgo_monitor.report import REPORT_CACHE_JOB, build_report
from csgo_monitor.storage import Storage


class ReportTests(unittest.TestCase):
    def test_report_defaults_to_snapshot_and_caches_deep_metrics(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            storage = Storage(
                root / "market.sqlite3",
                root / "raw",
                root / "control.sqlite3",
            )
            storage.upsert_items("siteb", [{
                "item_id": "123",
                "name": "Test Item",
                "market_hash_name": "Test Item",
            }])
            storage.upsert_catalog_items(
                "siteb",
                ["123"],
                universe="siteb_open_api",
            )
            storage.upsert_series([{
                "source": "siteb",
                "dataset": "item_history",
                "item_id": "123",
                "metric": "sell_price",
                "platform": "ALL",
                "ts": "2026-08-30T00:00:00+00:00",
                "close": 1,
            }])
            storage.save_capture(
                "siteb",
                "GET",
                "https://www.siteb.com/api/demo",
                200,
                b'{"success":true,"data":{"value":1}}',
                "application/json",
            )
            storage.set_coverage(
                "siteb",
                "item_catalog_open_api",
                "ok",
                1,
            )

            json_path, md_path = build_report(storage, root)
            snapshot = json.loads(json_path.read_text(encoding="utf-8"))

            self.assertEqual(snapshot["statistics_mode"], "snapshot")
            self.assertEqual(snapshot["count_mode"], "rowid_high_water")
            self.assertEqual(snapshot["metric_mode"], "coverage_snapshot")
            self.assertIn("pipelines", snapshot)
            self.assertEqual(
                snapshot["pipelines"]["siteb_site_history"]["catalog"],
                1,
            )
            self.assertIsNone(
                snapshot["progress"]["siteb"]["items_with_history"]
            )
            self.assertIn(
                "# 数据覆盖率报告",
                md_path.read_text(encoding="utf-8"),
            )

            build_report(storage, root, deep=True)
            cached = storage.get_state_detail(REPORT_CACHE_JOB)
            self.assertEqual(cached["schema"], 1)
            self.assertEqual(
                cached["progress"]["siteb"]["items_with_history"],
                1,
            )

            build_report(storage, root)
            refreshed = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual(refreshed["metric_mode"], "cached_deep")
            self.assertEqual(
                refreshed["progress"]["siteb"]["items_with_history"],
                1,
            )

    def test_report_cli_exposes_deep_mode(self):
        args = parser().parse_args(["report", "--deep"])
        self.assertTrue(args.deep)


if __name__ == "__main__":
    unittest.main()
