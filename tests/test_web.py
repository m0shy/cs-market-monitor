import tempfile
import threading
import time
import unittest
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import web.app as web_app
from web.navigation import CHANNELS, freshness_status
from csgo_monitor.storage import Storage


class WebDataAvailabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.database = root / "market.sqlite3"
        self.storage = Storage(self.database, root / "raw")
        self.name = "M4A4 | Test Asset (Factory New)"
        self.open_id = web_app.open_id(self.name)
        self.numeric_id = "12345"
        self.storage.upsert_items("siteb", [
            {
                "item_id": self.open_id,
                "name": self.name,
                "market_hash_name": self.name,
            },
            {
                "item_id": self.numeric_id,
                "name": self.name,
                "market_hash_name": self.name,
            },
        ])
        self.storage.upsert_series([
            {
                "source": "siteb",
                "dataset": "item_kline",
                "item_id": self.open_id,
                "metric": "ohlc",
                "platform": "all",
                "ts": "2026-08-16",
                "open": 100,
                "high": 105,
                "low": 98,
                "close": 103,
            },
            {
                "source": "siteb",
                "dataset": "item_kline",
                "item_id": self.open_id,
                "metric": "ohlc",
                "platform": "all",
                "ts": "2026-08-17",
                "open": 103,
                "high": 108,
                "low": 101,
                "close": 107,
            },
        ])

    def tearDown(self):
        self.temp.cleanup()

    def test_item_charts_expose_more_than_one_year_and_all_history_metrics(self):
        first = datetime(2023, 9, 30, tzinfo=timezone.utc)
        candles = []
        history = []
        for day in range(1100):
            stamp = (first + timedelta(days=day)).isoformat()
            candles.append({
                "source": "siteb", "dataset": "item_kline",
                "item_id": self.open_id, "metric": "ohlc", "platform": "all",
                "ts": stamp, "open": 10, "high": 11, "low": 9, "close": 10,
            })
            for metric in ("sell_price", "buy_price", "transaction_count"):
                history.append({
                    "source": "siteb", "dataset": "item_history",
                    "item_id": self.numeric_id, "metric": metric,
                    "platform": "all", "ts": stamp, "close": 10,
                })
        self.storage.upsert_series(candles + history)
        with patch.object(web_app, "DB", self.database):
            client = TestClient(web_app.app)
            chart = client.get("/api/item/analysis", params={"name": self.name})
            history_result = client.get("/api/item/history", params={"name": self.name})
        self.assertEqual(chart.status_code, 200)
        self.assertEqual(len(chart.json()["points"]), 1102)
        self.assertEqual(len(history_result.json()["series"]["sell_price"]), 1100)
        self.assertEqual(len(history_result.json()["series"]["buy_price"]), 1100)
        self.assertTrue(chart.json()["range"]["start"].startswith("2023-09-30"))

    def test_main_chart_can_select_real_site_history_when_kline_is_stale(self):
        self.storage.upsert_series([
            {
                "source": "siteb", "dataset": "item_history",
                "item_id": self.numeric_id, "metric": "sell_price",
                "platform": "all", "ts": stamp, "close": price,
            }
            for stamp, price in (
                ("2023-09-30", 99), ("2025-08-16", 105),
                ("2026-09-29", 120),
            )
        ])
        with patch.object(web_app, "DB", self.database):
            client = TestClient(web_app.app)
            sources = client.get("/api/item/chart-sources", params={"name": self.name}).json()["sources"]
            chart = client.get("/api/item/analysis",
                               params={"name": self.name, "source": "site_history"}).json()
            candles = client.get("/api/item/analysis", params={"name": self.name}).json()
        site = next(source for source in sources if source["key"] == "site_history")
        self.assertEqual((site["start"], site["end"]), ("2023-09-30", "2026-09-29"))
        self.assertEqual(chart["mode"], "line")
        self.assertEqual(chart["source"], "site_history")
        self.assertEqual((chart["range"]["start"], chart["range"]["end"]),
                         ("2023-09-30", "2026-09-29"))
        self.assertFalse(chart["activity_available"])
        self.assertEqual([point["close"] for point in chart["points"]], [99, 105, 120])
        self.assertEqual((candles["range"]["start"], candles["range"]["end"]),
                         ("2026-08-16", "2026-08-17"))

    def test_viewed_item_schedules_its_public_page_ahead_of_sitemap_backlog(self):
        from urllib.parse import quote
        url = "https://www.siteb.com/cs2/" + quote(self.name, safe="-._~")
        with patch.object(web_app, "DB", self.database), \
                patch.object(web_app, "CONTROL_DB", self.database):
            result = web_app.api_item_chart_sources(self.name)
        self.assertEqual(result["sources"][0]["end"], "2026-08-17")
        with self.storage.control_connect() as conn:
            first = conn.execute(
                "SELECT status,priority,discovered_from FROM public_pages WHERE source=? AND url=?",
                ("siteb", url),
            ).fetchone()
            self.assertEqual(tuple(first), ("ready", -20, "viewed_item"))
            conn.execute(
                """UPDATE public_pages SET status='done',last_checked=?
                   WHERE source=? AND url=?""",
                (datetime.now(timezone.utc).isoformat(), "siteb", url),
            )
        with patch.object(web_app, "DB", self.database), \
                patch.object(web_app, "CONTROL_DB", self.database):
            web_app.api_item_chart_sources(self.name)
        with self.storage.control_connect() as conn:
            status = conn.execute(
                "SELECT status FROM public_pages WHERE source=? AND url=?",
                ("siteb", url),
            ).fetchone()[0]
        self.assertEqual(status, "done")

    def test_broad_full_view_uses_observed_weekly_daily_and_latest_index(self):
        self.storage.upsert_series([
            {"source": "siteb", "dataset": "sector_kline",
             "item_id": "BROAD:0:root", "metric": "market_index",
             "platform": "all.k3", "ts": "2023-07-02T00:00:00+00:00",
             "open": 100, "high": 110, "low": 99, "close": 105},
            {"source": "siteb", "dataset": "sector_kline",
             "item_id": "BROAD:0:root", "metric": "market_index",
             "platform": "all.k3", "ts": "2026-08-04T00:00:00+00:00",
             "open": 180, "high": 200, "low": 175, "close": 190},
            {"source": "siteb", "dataset": "market_kline",
             "item_id": "broad", "metric": "ohlc", "platform": "",
             "ts": "2026-08-11T00:00:00+00:00",
             "open": 200, "high": 210, "low": 190, "close": 205},
            {"source": "siteb", "dataset": "market_kline",
             "item_id": "broad", "metric": "ohlc", "platform": "",
             "ts": "2026-08-05T00:00:00+00:00",
             "open": 191, "high": 202, "low": 188, "close": 198},
            {"source": "siteb", "dataset": "market_index",
             "item_id": "", "metric": "market_index", "platform": "",
             "ts": "2026-09-29T09:00:00+00:00", "close": 220},
        ])
        with patch.object(web_app, "DB", self.database):
            all_time = web_app._market_analysis("siteb", "1day", 1200)
            weekly = web_app._market_analysis("siteb", "7day", 1200)
            original = web_app._market_analysis("siteb", "1day", 1200, "candles")
            broad_weekly = web_app._section_analysis("BROAD", "siteb", "7day", 800)
            broad_15day = web_app._section_analysis("BROAD", "siteb", "15day", 800)
        self.assertEqual(all_time["range"]["start"], "2023-07-02T00:00:00+00:00")
        self.assertEqual(all_time["range"]["end"], "2026-09-29T09:00:00+00:00")
        self.assertEqual(all_time["mode"], "candlestick")
        self.assertEqual(all_time["samples"], {"weekly": 2, "daily": 2,
                                               "observed": 0, "latest": 1})
        self.assertEqual(all_time["points"][0]["open"], 100)
        self.assertEqual(all_time["points"][0]["high"], 110)
        self.assertIsNone(all_time["points"][-1]["open"])
        self.assertEqual(all_time["coverage"]["ohlc"]["count"], 4)
        self.assertEqual(weekly["period"], "7day")
        self.assertEqual(weekly["mode"], "candlestick")
        self.assertEqual([point["close"] for point in weekly["points"]], [105, 190, 220])
        self.assertEqual(weekly["coverage"]["ohlc"]["count"], 2)
        self.assertEqual(broad_weekly["period"], "7day")
        self.assertEqual(broad_weekly["range"]["start"], "2023-07-02T00:00:00+00:00")
        self.assertEqual(broad_15day["period"], "15day")
        self.assertTrue(broad_15day["coverage"]["ohlc"]["available"])
        self.assertEqual(broad_15day["range"]["start"], "2026-08-11T00:00:00+00:00")
        self.assertEqual(original["range"]["end"], "2026-08-11T00:00:00+00:00")
        self.assertEqual(original["points"][0]["open"], 191)

    def test_market_kline_endpoint_uses_full_public_history(self):
        self.storage.upsert_series([
            {
                "source": "siteb", "dataset": "sector_kline",
                "item_id": "BROAD:0:root", "metric": "market_index",
                "platform": "all.k2", "ts": timestamp,
                "open": 100, "high": 105, "low": 99, "close": price,
            }
            for timestamp, price in (
                ("2023-06-20T15:00:04+00:00", 100),
                ("2026-10-05T16:55:10+00:00", 200),
            )
        ])
        with patch.object(web_app, "DB", self.database), \
                patch.dict(web_app._CACHE, {}, clear=True):
            with TestClient(web_app.app) as client:
                result = client.get("/api/market/kline")
        self.assertEqual(result.status_code, 200)
        points = result.json()["points"]
        self.assertEqual((points[0]["ts"], points[-1]["ts"]), (
            "2023-06-20T15:00:04+00:00",
            "2026-10-05T16:55:10+00:00",
        ))
        self.assertEqual([point["sample"] for point in points], [
            "站点日 K", "站点日 K",
        ])

    def test_full_market_and_broad_pages_do_not_truncate_public_daily_history(self):
        first = datetime(2015, 1, 1, tzinfo=timezone.utc)
        count = 4105
        self.storage.upsert_series([
            {"source": "siteb", "dataset": "sector_kline",
             "item_id": "BROAD:0:root", "metric": "market_index",
             "platform": "all.k2", "ts": (first + timedelta(days=day)).isoformat(),
             "open": 100, "high": 105, "low": 99, "close": 102}
            for day in range(count)
        ])
        with patch.object(web_app, "DB", self.database), patch.dict(web_app._CACHE, {}, clear=True):
            with TestClient(web_app.app) as client:
                full = client.get("/api/market/analysis").json()
                broad = client.get("/api/section/analysis").json()
                short = client.get("/api/market/analysis", params={"limit": 60}).json()
                native = client.get("/api/market/analysis",
                                    params={"view": "candles", "limit": 4000}).json()
                native_all = client.get("/api/market/analysis",
                                        params={"view": "candles"}).json()
                aggregate = client.get("/api/market/analysis",
                                       params={"view": "candles", "period": "7day"}).json()
        for result in (full, broad, native_all):
            self.assertEqual(result["range"]["count"], count)
            self.assertEqual(result["range"]["start"], first.isoformat())
        for result in (full, broad):
            self.assertFalse(result["history"]["truncated"])
            self.assertEqual(result["history"]["missing_daily_count"], 0)
            self.assertNotIn("2025 年前仅有周 K", result["message"])
        first_week_end = first + timedelta(days=6 - first.date().toordinal() % 7)
        self.assertEqual(aggregate["range"]["start"], first_week_end.isoformat())
        self.assertEqual(short["range"]["count"], 60)
        self.assertTrue(short["history"]["truncated"])
        self.assertEqual(short["history"]["available_count"], count)
        self.assertIn("请求限制", short["message"])
        self.assertEqual(native["range"]["count"], 4000)
        self.assertEqual(native["points"][-1]["sample"], "站点日 K")

    def test_sitea_market_analysis_defaults_to_all_collected_samples(self):
        first = datetime(2023, 1, 1, tzinfo=timezone.utc)
        count = 4105
        self.storage.upsert_series([
            {"source": "sitea", "dataset": "sub_kline",
             "item_id": "1", "metric": "ohlc", "platform": "1hour",
             "ts": (first + timedelta(hours=hour)).isoformat(),
             "open": 100, "high": 105, "low": 99, "close": 102}
            for hour in range(count)
        ])
        with patch.object(web_app, "DB", self.database), patch.dict(web_app._CACHE, {}, clear=True):
            with TestClient(web_app.app) as client:
                full = client.get("/api/market/analysis",
                                  params={"source": "sitea", "period": "1hour"}).json()
                short = client.get("/api/market/analysis",
                                   params={"source": "sitea", "period": "1hour", "limit": 60}).json()
        self.assertEqual(full["range"]["count"], count)
        self.assertEqual(full["range"]["start"], first.isoformat())
        self.assertEqual(full["as_of"], full["range"]["end"])
        self.assertEqual(full["status"], "stale")
        self.assertEqual(short["range"]["count"], 60)

    def test_market_summary_prefers_native_daily_ohlc_and_keeps_quote_separate(self):
        self.storage.upsert_series([
            {"source": "siteb", "dataset": "sector_kline",
             "item_id": "BROAD:0:root", "metric": "market_index", "platform": "all.k2",
             "ts": "2026-10-05T05:00:00+00:00",
             "open": 100, "high": 110, "low": 90, "close": 105},
            {"source": "siteb", "dataset": "market_kline",
             "item_id": "broad", "metric": "ohlc", "platform": "",
             "ts": "2026-10-05T05:10:00+00:00",
             "open": 900, "high": 999, "low": 800, "close": 950},
            *[
                {"source": "siteb", "dataset": "market_snapshot",
                 "item_id": "", "metric": metric, "platform": "",
                 "ts": "2026-10-05T05:20:00+00:00", "close": value}
                for metric, value in (("broadMarketIndex", 106), ("diffYesterday", 6),
                                      ("diffYesterdayRatio", 6))
            ],
        ])
        with patch.object(web_app, "DB", self.database):
            summary = web_app._market_source_summaries()["siteb"]
        self.assertEqual(summary["value"], 106)
        self.assertEqual(summary["as_of"], "2026-10-05T05:20:00+00:00")
        self.assertEqual([summary[key] for key in ("open", "high", "low", "candle_close")],
                         [100, 110, 90, 105])
        self.assertEqual(summary["candle_as_of"], "2026-10-05T05:00:00+00:00")
        self.assertEqual(summary["change_rate"], 6)

    def test_market_summary_replaces_stale_quotes_without_reusing_old_change(self):
        self.storage.upsert_series([
            *[
                {"source": source, "dataset": dataset,
                 "item_id": item_id, "metric": metric, "platform": "",
                 "ts": "2026-08-12T00:00:00+00:00", "close": value}
                for source, dataset, item_id, metric, value in (
                    ("siteb", "market_snapshot", "", "broadMarketIndex", 100),
                    ("siteb", "market_snapshot", "", "diffYesterdayRatio", 2),
                    ("siteb", "market_snapshot", "", "diffYesterday", 2),
                    ("sitea", "market_index_snapshot", "1", "now", 1000),
                    ("sitea", "market_index_snapshot", "1", "rate", 3),
                    ("sitea", "market_index_snapshot", "1", "amplitude", 30),
                )
            ],
            {"source": "siteb", "dataset": "sector_kline",
             "item_id": "BROAD:0:root", "metric": "market_index", "platform": "all.k2",
             "ts": "2026-10-05T05:00:00+00:00",
             "open": 100, "high": 110, "low": 90, "close": 105},
            {"source": "sitea", "dataset": "sub_kline",
             "item_id": "1", "metric": "ohlc", "platform": "1day",
             "ts": "2026-10-05T05:00:00+00:00",
             "open": 1000, "high": 1100, "low": 900, "close": 1050},
        ])
        with patch.object(web_app, "DB", self.database):
            summaries = web_app._market_source_summaries()
        for source, value in (("siteb", 105), ("sitea", 1050)):
            summary = summaries[source]
            self.assertEqual(summary["value"], value)
            self.assertEqual(summary["candle_close"], value)
            self.assertEqual(summary["as_of"], "2026-10-05T05:00:00+00:00")
            self.assertIsNone(summary["change"])
            self.assertIsNone(summary["change_rate"])

    def test_market_summary_does_not_invent_missing_candles(self):
        self.storage.upsert_series([
            {"source": "siteb", "dataset": "market_snapshot",
             "item_id": "", "metric": "broadMarketIndex", "platform": "",
             "ts": "2026-10-05T05:00:00+00:00", "close": 105},
        ])
        with patch.object(web_app, "DB", self.database):
            summary = web_app._market_source_summaries()["siteb"]
        self.assertEqual(summary["value"], 105)
        self.assertIsNone(summary["candle_as_of"])
        for field in ("open", "high", "low", "candle_close", "change", "change_rate"):
            self.assertIsNone(summary[field])

    def test_market_time_labels_use_beijing_dates_without_changing_date_only(self):
        self.assertEqual(web_app.market_time_label("2026-10-04T16:00:00+00:00"),
                         "2026-10-05 00:00")
        self.assertEqual(web_app.market_time_label("2026-10-05T00:00:00+08:00"),
                         "2026-10-05 00:00")
        self.assertEqual(web_app.market_time_label("2026-10-05T00:00:00"),
                         "2026-10-05 00:00")
        self.assertEqual(web_app.market_time_label("2026-10-05"), "2026-10-05")
        self.assertEqual(web_app.market_time_label(None), "-")
        self.assertEqual(web_app.market_time_label("unknown"), "unknown")

    def test_full_history_discloses_gaps_without_fabricating_candles(self):
        self.storage.upsert_series([
            {"source": "siteb", "dataset": "sector_kline",
             "item_id": "BROAD:0:root", "metric": "market_index",
             "platform": "all.k2", "ts": stamp,
             "open": 100, "high": 105, "low": 99, "close": 102}
            for stamp in ("2023-06-20T15:00:04+00:00", "2023-06-22T15:00:00+00:00")
        ])
        with patch.object(web_app, "DB", self.database):
            result = web_app._market_analysis("siteb", "1day", None)
            native = web_app._market_analysis("siteb", "1day", None, "candles")
        self.assertEqual(result["history"]["missing_daily_dates"], ["2023-06-21"])
        self.assertIn("缺少 1 个日期", result["message"])
        self.assertEqual(result["range"]["count"], 2)
        self.assertEqual(native["range"]["count"], 2)

    def test_broad_recent_hourly_observations_are_distinct_from_site_kline(self):
        self.storage.upsert_series([
            {"source": "siteb", "dataset": "sector_kline",
             "item_id": "BROAD:0:root", "metric": "market_index",
             "platform": "all.k3", "ts": "2026-08-30T00:00:00+00:00",
             "open": 90, "high": 101, "low": 88, "close": 100},
            *[
                {"source": "siteb", "dataset": "market_index",
                 "item_id": "", "metric": "market_index", "platform": "",
                 "ts": timestamp, "close": price}
                for timestamp, price in (
                    ("2026-08-31T01:00:00+00:00", 103),
                    ("2026-08-31T03:00:00+00:00", 109),
                    ("2026-08-31T04:00:00+00:00", 104),
                    ("2026-09-02T00:00:00+00:00", 98),
                )
            ],
        ])
        with patch.object(web_app, "DB", self.database):
            result = web_app._market_analysis("siteb", "1day", 1200)
            weekly = web_app._market_analysis("siteb", "7day", 1200)
        self.assertEqual(result["samples"],
                         {"weekly": 1, "daily": 0, "observed": 1, "latest": 1})
        self.assertEqual(result["coverage"]["ohlc"]["count"], 1)
        self.assertEqual(result["range"]["count"], 3)
        observed = result["points"][1]
        self.assertEqual([observed[key] for key in ("open", "high", "low", "close")],
                         [103, 109, 103, 104])
        self.assertEqual(observed["observations"], 3)
        self.assertIn("非完整日", observed["sample"])
        self.assertEqual(result["points"][-1]["sample"], "指数小时观测单点（无 K 线）")
        self.assertEqual([point["ts"][:10] for point in weekly["points"]],
                         ["2026-08-30", "2026-08-31", "2026-09-02"])

    def test_history_falls_back_to_open_kline_with_explicit_source(self):
        with patch.object(web_app, "DB", self.database):
            result = web_app.api_history(self.name)

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["source"], "siteb_open_kline")
        self.assertTrue(result["fallback"])
        self.assertEqual(
            [point["close"] for point in result["series"]["sell_price"]],
            [103, 107],
        )
        self.assertFalse(result["availability"]["site_history"]["available"])

    def test_intraday_index_keeps_distinct_hours_and_utc_offset(self):
        self.storage.upsert_series([
            {
                "source": "siteb", "dataset": "market_index",
                "item_id": "broad", "metric": "market_index", "platform": "all",
                "ts": f"2026-08-11T{hour:02d}:00:00+00:00",
                "close": price,
            }
            for hour, price in [(0, 861.02), (1, 860.42)]
        ])
        with patch.object(web_app, "DB", self.database):
            points = web_app._market_index_points()

        self.assertEqual([point["ts"] for point in points], [
            "2026-08-11T00:00:00+00:00",
            "2026-08-11T01:00:00+00:00",
        ])
        self.assertEqual([point["close"] for point in points], [861.02, 860.42])

    def test_intraday_index_uses_newest_series_and_recent_time_window(self):
        self.storage.upsert_series([
            {
                "source": "siteb", "dataset": "market_index", "item_id": item_id,
                "metric": "market_index", "platform": "",
                "ts": timestamp, "close": price,
            }
            for item_id, timestamp, price in (
                ("broad", "2026-08-12T02:00:00+00:00", 857.08),
                ("", "2026-09-24T02:00:00+00:00", 797.29),
                ("", "2026-09-27T23:00:00+00:00", 795.93),
                ("", "2026-09-28T00:00:00+00:00", 795.67),
            )
        ])
        with patch.object(web_app, "DB", self.database):
            points = web_app._market_index_points()
        self.assertEqual([point["ts"] for point in points], [
            "2026-09-27T23:00:00+00:00",
            "2026-09-28T00:00:00+00:00",
        ])
        self.assertEqual([point["close"] for point in points], [795.93, 795.67])
        with patch.object(web_app, "cached", return_value=points), patch.object(
            web_app, "freshness_status", return_value="stale"
        ) as freshness:
            response = web_app.api_market_index()
        self.assertEqual(response["as_of"], "2026-09-28T00:00:00+00:00")
        self.assertEqual(response["status"], "stale")
        freshness.assert_called_once_with(
            "2026-09-28T00:00:00+00:00", "ready", max_age_hours=6,
        )

    def test_site_history_replaces_fallback_when_collected(self):
        self.storage.upsert_series([
            {
                "source": "siteb",
                "dataset": "item_history",
                "item_id": self.numeric_id,
                "metric": "sell_price",
                "platform": "all",
                "ts": "2026-08-17",
                "close": 106,
            },
            {
                "source": "siteb",
                "dataset": "item_history",
                "item_id": self.numeric_id,
                "metric": "buy_price",
                "platform": "all",
                "ts": "2026-08-17",
                "close": 102,
            },
        ])

        with patch.object(web_app, "DB", self.database):
            result = web_app.api_history(self.name)

        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["source"], "siteb_site_history")
        self.assertFalse(result["fallback"])
        self.assertEqual(result["series"]["sell_price"][0]["close"], 106)
        self.assertTrue(result["availability"]["site_history"]["available"])

    def test_progress_separates_quota_deferred_failures(self):
        self.storage.set_coverage(
            "siteb", "item_catalog_open_api", "ok", 100
        )
        self.storage.set_state("siteb_api_extended", "10")
        self.storage.set_state("siteb_api_histories_b", "4")
        self.storage.set_state("siteb_api_histories_c", "5")
        self.storage.set_state("siteb_api_histories_d", "6")
        self.storage.set_state(
            "siteb_site_quota_until", str(int(time.time() + 3600))
        )
        self.storage.record_failure(
            "siteb_api_extended", "quota", "err106 daily quota"
        )
        self.storage.record_failure(
            "siteb_api_extended", "network", "network-fuse"
        )
        self.storage.record_failure(
            "siteb_api_histories_b", "quota", "quota-exhausted"
        )
        self.storage.record_failure(
            "siteb_api_histories_c", "other", "missing payload"
        )

        with self.storage.connect() as conn:
            result = web_app._acquisition_progress(conn)
        tasks = {task["key"]: task for task in result["tasks"]}

        self.assertEqual(tasks["siteb_extended"]["unresolved"], 1)
        self.assertIn("1 条配额失败", tasks["siteb_extended"]["note"])
        self.assertEqual(tasks["siteb_site_history"]["unresolved"], 1)
        self.assertIn("暂停至", tasks["siteb_site_history"]["blocker"])

    def test_runtime_status_exposes_workers_routes_and_failures(self):
        self.storage.update_heartbeat(
            "worker-a",
            "siteb-history-b",
            pid=123,
            status="running",
            route="direct",
            started_at="2026-08-26T15:00:00+00:00",
        )
        self.storage.record_route_health(
            "siteb_site_api",
            "direct",
            "ok",
            latency_ms=120,
        )
        self.storage.record_failure(
            "siteb_api_histories_b",
            "item-1",
            "connection reset",
        )

        with self.storage.connect() as conn:
            result = web_app._runtime_status(conn)

        self.assertEqual(result["heartbeats"][0]["task_id"], "siteb-history-b")
        self.assertEqual(result["routes"][0]["status"], "ok")
        self.assertEqual(result["failures"][0]["error_class"], "transport")

    def test_section_analysis_uses_section_snapshot_timeline(self):
        self.storage.upsert_series([
            {
                "source": "siteb",
                "dataset": "sector_snapshot",
                "item_id": "rifle",
                "metric": "itemTypeLevel1.topList.index",
                "platform": "all",
                "ts": "2026-08-16T10:00:00",
                "close": 100,
            },
            {
                "source": "siteb",
                "dataset": "sector_snapshot",
                "item_id": "knife",
                "metric": "itemTypeLevel1.defaultList.index",
                "platform": "all",
                "ts": "2026-08-16T10:00:00",
                "close": 120,
            },
            {
                "source": "siteb",
                "dataset": "sector_snapshot",
                "item_id": "rifle",
                "metric": "itemTypeLevel1.topList.index",
                "platform": "all",
                "ts": "2026-08-17T10:00:00",
                "close": 110,
            },
            {
                "source": "siteb",
                "dataset": "sector_snapshot",
                "item_id": "knife",
                "metric": "itemTypeLevel1.defaultList.index",
                "platform": "all",
                "ts": "2026-08-17T10:00:00",
                "close": 100,
            },
        ])

        with patch.object(web_app, "DB", self.database):
            result = web_app._section_analysis("LEVEL1", "siteb", "1day", 60)

        self.assertEqual(result["scope"], "section_snapshot")
        self.assertEqual(result["mode"], "line")
        self.assertEqual([point["ts"] for point in result["points"]], [
            "2026-08-16T10:00:00",
            "2026-08-17T10:00:00",
        ])
        self.assertEqual([point["close"] for point in result["points"]], [110, 105])
        self.assertEqual(result["activity_label"], "成分观察数")
        self.assertEqual(result["coverage"]["volume"]["count"], 2)

    def test_cached_merges_concurrent_builds_and_force_refreshes(self):
        key = f"test-cache-{time.time_ns()}"
        calls = 0
        calls_lock = threading.Lock()
        started = threading.Event()
        release = threading.Event()

        def build():
            nonlocal calls
            with calls_lock:
                calls += 1
            started.set()
            release.wait(timeout=2)
            return {"value": 1}

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(web_app.cached, key, 60, build)
                for _ in range(4)
            ]
            self.assertTrue(started.wait(timeout=1))
            time.sleep(0.05)
            release.set()
            results = [future.result(timeout=2) for future in futures]

        self.assertEqual(calls, 1)
        self.assertEqual(results, [{"value": 1}] * 4)
        refreshed = web_app.cached(
            key,
            60,
            lambda: {"value": 2},
            refresh=True,
        )
        self.assertEqual(refreshed, {"value": 2})

    def test_trend_board_keeps_full_rows_for_client_filters(self):
        rows = [
            {
                "id": "rifle-1",
                "name": "Rifle",
                "price": {"buff": 100},
                "avg7": {"buff": 90},
                "diff": {"buff": 11.1},
                "broker_sell": 10,
                "ts": "2026-08-16T10:00:00",
                "profile": {"item_type": "CSGO_Type_Rifle"},
            },
            {
                "id": "knife-1",
                "name": "Knife",
                "price": {"buff": 200},
                "avg7": {"buff": 210},
                "diff": {"buff": -4.8},
                "broker_sell": 20,
                "ts": "2026-08-16T10:00:00",
                "profile": {"item_type": "CSGO_Type_Knife"},
            },
        ]
        with patch.object(web_app, "DB", self.database), patch.object(
            web_app, "cached", side_effect=lambda key, ttl, fn, **kwargs: {"hot": rows},
        ):
            result = web_app._trend_board(category="", platform="buff", limit=1)

        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(len(result["all_rows"]), 2)
        self.assertEqual(result["total_count"], 2)

    def test_channel_navigation_and_stale_status(self):
        paths = [channel["href"] for channel in CHANNELS]
        self.assertEqual(len(paths), len(set(paths)))
        self.assertEqual(len(paths), 6)
        self.assertEqual(
            freshness_status("2020-01-01T12:00:00+08:00", "ready"), "stale",
        )
        self.assertEqual(freshness_status(None, "collecting"), "collecting")

    def test_legacy_redirects_preserve_query_and_new_pages_render(self):
        with patch.object(web_app, "DB", self.database), patch.object(
            web_app, "CONTROL_DB", self.database,
        ), TestClient(web_app.app) as client:
            for old, expected in (
                ("/items?q=AK-47", "/market/items?q=AK-47"),
                ("/market?source=sitea", "/research/index?source=sitea"),
                ("/tracker", "/monitor/radar"),
                ("/cs2/Test%20Asset?from=%2Frank",
                 "/market/items/Test%20Asset?from=%2Frank"),
            ):
                response = client.get(old, follow_redirects=False)
                self.assertEqual(response.status_code, 308, old)
                self.assertEqual(response.headers["location"], expected)
            for path, expected_text in (
                ("/market/compare", "跨平台比较"),
                ("/research/breadth", "市场宽度"),
                ("/monitor/item-tracking", "单品追踪"),
                ("/data/sources", "数据来源与口径"),
            ):
                response = client.get(path)
                self.assertEqual(response.status_code, 200, path)
                self.assertIn(expected_text, response.text)
                self.assertIn('id="main-nav"', response.text)

    def test_event_detail_uses_extended_window_and_preserves_filtered_return(self):
        events = [
            {
                "key": f"event-{index}", "name": self.name, "platform": "buff",
                "metric": "buy_price", "old": 10.25, "new": 11.75,
                "ratio": 14.63, "ts": "2026-09-28T09:00:00",
            }
            for index in range(100)
        ]
        with web_app._CACHE_LOCK:
            web_app._CACHE.pop("change_board", None)
            web_app._CACHE.pop("change_board:300", None)
        with patch.object(web_app, "_change_board", side_effect=lambda limit=60: events[:limit]), \
             TestClient(web_app.app) as client:
            client.get("/api/tracker-live")
            response = client.get(
                "/monitor/events/event-89?from=%2Fmonitor%2Fevents%3Fq%3DM4A4%26metric%3Dbuy_price"
            )
            self.assertEqual(response.status_code, 200)
            self.assertIn("求购价", response.text)
            self.assertIn("¥10.25", response.text)
            self.assertIn('href="/monitor/events?q=M4A4&amp;metric=buy_price"', response.text)
            self.assertIn('href="/market/items/', response.text)
            invalid = client.get("/monitor/events/event-89?from=https%3A%2F%2Fexample.com")
            self.assertIn('href="/monitor/events"', invalid.text)
            missing = client.get("/monitor/events/missing")
            self.assertEqual(missing.status_code, 404)
        with web_app._CACHE_LOCK:
            web_app._CACHE.pop("change_board", None)
            web_app._CACHE.pop("change_board:300", None)

    def test_item_detail_can_return_to_an_event(self):
        with patch.object(web_app, "DB", self.database), TestClient(web_app.app) as client:
            response = client.get(
                f"/market/items/{self.name}?from=%2Fmonitor%2Fevents%2Fevent-1"
            )
        self.assertEqual(response.status_code, 200)
        self.assertIn('href="/monitor/events/event-1"', response.text)

    def test_quote_zero_price_is_missing_and_snapshot_time_is_real(self):
        self.storage.upsert_series([
            {
                "source": "siteb", "dataset": "platform_snapshot",
                "item_id": self.open_id, "metric": "sell_price", "platform": "buff",
                "ts": "2026-08-10T12:00:00", "close": 0,
            },
            {
                "source": "siteb", "dataset": "platform_snapshot",
                "item_id": self.open_id, "metric": "sell_num", "platform": "c5",
                "ts": "2026-08-11T12:00:00", "close": 0,
            },
            {
                "source": "siteb", "dataset": "platform_snapshot",
                "item_id": self.numeric_id, "metric": "sell_price", "platform": "buff",
                "ts": "2026-08-12T12:00:00", "close": 58.75,
            },
        ])
        with patch.object(web_app, "DB", self.database):
            response = web_app.api_items_quote(
                f"{self.open_id},{self.numeric_id},unknown"
            )["quotes"]
        self.assertIsNone(response[self.open_id]["buff_price"])
        self.assertEqual(response[self.open_id]["broker_sell"], 0)
        self.assertEqual(response[self.open_id]["ts"], "2026-08-11T12:00:00")
        self.assertEqual(response[self.numeric_id]["buff_price"], 58.75)
        self.assertIsNone(response["unknown"]["broker_sell"])
        self.assertFalse(response["unknown"]["available"])

    def test_compare_filters_keep_context_and_breadth_separates_missing_baseline(self):
        sample = [
            {
                "id": "one", "name": "AK-47 | Sample", "price": {"buff": 100, "c5": 120},
                "diff": {"buff": 5}, "avg7": {"buff": 95}, "ts": "2026-09-28",
                "profile": {}, "broker_sell": 20,
            },
            {
                "id": "two", "name": "M4A4 | Sample", "price": {"buff": 100, "steam": 104},
                "diff": {"buff": None}, "avg7": {}, "ts": "2026-09-28",
                "profile": {}, "broker_sell": 10,
            },
        ]
        with patch.object(web_app, "cached", return_value={
            "hot": sample, "gain": sample, "loss": [],
        }), TestClient(web_app.app) as client:
            response = client.get("/market/compare?q=AK-47&platform=c5&min_spread=10")
            self.assertEqual(response.status_code, 200)
            self.assertIn("AK-47 | Sample", response.text)
            self.assertNotIn("M4A4 | Sample", response.text)
            self.assertIn("筛选结果 1 件 / 样本 2 件", response.text)
            self.assertIn("from=/market/compare%3Fq%3DAK-47", response.text)
            breadth = client.get("/research/breadth")
            self.assertEqual(breadth.status_code, 200)
            self.assertIn("缺少 7 日基准", breadth.text)
            self.assertIn("缺少基准 1 件", breadth.text)


if __name__ == "__main__":
    unittest.main()


class RealtimeApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.database = root / "market.sqlite3"
        self.storage = Storage(self.database, root / "raw")
        now = datetime.now(timezone.utc).isoformat()
        self.storage.upsert_series([
            {
                "source": "siteb", "dataset": "ws_market_statistics",
                "item_id": "", "metric": "survive_num", "platform": "",
                "ts": now, "close": 1132994814,
            },
            {
                "source": "siteb", "dataset": "ws_market_index",
                "item_id": "", "metric": "broad", "platform": "",
                "ts": now, "close": 737.12,
            },
            {
                "source": "siteb", "dataset": "ws_new_listing",
                "item_id": "7993_123", "metric": "new_listing", "platform": "",
                "ts": now, "close": 629.0,
                "extra": {"floatWear": "0.0848", "assetId": 53968754833},
            },
            {
                "source": "siteb", "dataset": "ws_market_kline_h",
                "item_id": "", "metric": "broad", "platform": "",
                "ts": "2026-10-10T05:00:00+00:00",
                "open": 730.0, "high": 737.2, "low": 729.9, "close": 736.9,
            },
        ])

    def tearDown(self):
        self.temp.cleanup()

    def test_realtime_latest_snapshot_shape(self):
        with patch.object(web_app, "DB", self.database), \
                patch.object(web_app, "CONTROL_DB", self.database):
            with TestClient(web_app.app) as client:
                payload = client.get("/api/realtime/latest").json()
        self.assertIn("survive_num", payload["statistics"])
        self.assertEqual(payload["statistics"]["survive_num"]["value"], 1132994814)
        self.assertEqual(payload["index"]["latest"]["value"], 737.12)
        self.assertEqual(len(payload["listings"]), 1)
        self.assertEqual(payload["listings"][0]["float_wear"], "0.0848")
        self.assertEqual(payload["kline"]["h"][0]["close"], 736.9)
        self.assertTrue(payload["data_cursor"])

    def test_realtime_events_incremental(self):
        with patch.object(web_app, "DB", self.database), \
                patch.object(web_app, "CONTROL_DB", self.database):
            with TestClient(web_app.app) as client:
                empty = client.get(
                    "/api/realtime/events",
                    params={"since": "2099-01-01T00:00:00+00:00"},
                ).json()
                self.assertEqual(empty["events"], [])
                all_events = client.get(
                    "/api/realtime/events", params={"since": ""}
                ).json()
        kinds = {event["kind"] for event in all_events["events"]}
        self.assertIn("statistics", kinds)
        self.assertIn("index", kinds)
        self.assertIn("listing", kinds)
        self.assertIn("kline_h", kinds)

    def test_realtime_page_renders(self):
        with patch.object(web_app, "DB", self.database), \
                patch.object(web_app, "CONTROL_DB", self.database):
            with TestClient(web_app.app) as client:
                response = client.get("/realtime")
        self.assertEqual(response.status_code, 200)
        self.assertIn("rt-stats-grid", response.text)
        self.assertIn("realtime.js", response.text)
