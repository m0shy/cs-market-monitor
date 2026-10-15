# -*- coding: utf-8 -*-
"""features.py 纯函数与 feature_store 集成测试。"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, "src")

from csgo_monitor.features import (  # noqa: E402
    f_amihud,
    f_beta,
    f_drawdown,
    f_lvl_migration,
    f_ret,
    f_tail_ratio,
    f_vol_mad,
    mad,
    median,
    pct_rank,
    robust_z,
)


class MathTests(unittest.TestCase):
    def test_median_mad(self):
        self.assertEqual(median([1, 2, 3, 4, 5]), 3)
        self.assertAlmostEqual(mad([1, 2, 3, 4, 5]), 1.0)

    def test_robust_z(self):
        trail = [1.0, 1.2, 0.9, 1.1, 1.0, 1.3, 0.8, 1.05, 1.15, 0.95,
                 1.0, 1.1, 0.9, 1.2, 1.0, 1.25, 0.85, 1.05, 1.1, 0.95]
        z = robust_z(1.6, trail)
        self.assertIsNotNone(z)
        self.assertGreater(z, 2.0)

    def test_pct_rank(self):
        self.assertAlmostEqual(pct_rank(3.0, [1, 2, 3, 4, 5]), 0.6)

    def test_ret(self):
        # 8 个价格点的 7 日收益 = 末值/首值 - 1
        self.assertAlmostEqual(f_ret([100, 101, 102, 103, 104, 105, 106, 110], 7),
                               110 / 100 - 1)

    def test_drawdown(self):
        self.assertAlmostEqual(f_drawdown([100, 120, 90], 30), 1 - 90 / 120)

    def test_vol_mad_positive(self):
        rets = [0.01, -0.02, 0.03, -0.01, 0.02, -0.03, 0.01, 0.02]
        v = f_vol_mad(rets, 30)
        self.assertIsNotNone(v)
        self.assertGreater(v, 0)

    def test_tail_ratio(self):
        rets = [0.01, -0.02, 0.02, -0.01, 0.03, -0.03, 0.01, 0.02,
                -0.01, 0.015, 0.1, -0.06, 0.012, -0.018, 0.025]
        self.assertIsNotNone(f_tail_ratio(rets, 30))

    def test_amihud_skips_zero_tx(self):
        days = [
            {"avg_p": 100.0, "tx": 10.0},
            {"avg_p": 100.0, "tx": 0.0},   # 零成交日必须被跳过
            {"avg_p": 101.0, "tx": 5.0},
            {"avg_p": None, "tx": None},   # 缺失日跳过
            {"avg_p": 101.0, "tx": 8.0},
        ]
        v = f_amihud(days, 7)
        self.assertIsNotNone(v)
        self.assertGreater(v, 0)

    def test_lvl_migration(self):
        v = f_lvl_migration([0.03, 0.04, 0.03, 0.05], 7)
        self.assertIsNotNone(v)
        self.assertGreater(v, 0)

    def test_beta(self):
        idx = [0.01, 0.02, -0.01, 0.01, 0.02, -0.02, 0.01, 0.01,
               0.02, -0.01, 0.01, 0.02]
        rets = [0.02, 0.04, -0.02, 0.02, 0.04, -0.04, 0.02, 0.02,
                0.04, -0.02, 0.02, 0.04]
        b = f_beta(rets, idx, 30)
        self.assertAlmostEqual(b, 2.0, places=1)


class FeatureStoreTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        from csgo_monitor.storage import Storage
        db = Path(self.tmp.name) / "m.sqlite3"
        self.storage = Storage(db, Path(self.tmp.name) / "raw", None)

    def tearDown(self):
        self.tmp.cleanup()

    def test_versioned_append(self):
        rows = [{"item_id": "i1", "freq": "1d", "name": "ret_7d",
                 "ts": "2026-10-11", "value": 0.05}]
        self.assertEqual(self.storage.upsert_features(rows), 1)
        rows[0]["value"] = 0.06
        self.assertEqual(self.storage.upsert_features(rows), 1)  # v2 追加
        frame = self.storage.read_feature_frame(["ret_7d"])
        self.assertEqual(frame[("i1", "2026-10-11")]["ret_7d"], 0.06)  # 最新版本

    def test_compute_end_to_end(self):
        # 造 12 天 item_history，跑 L1+L2
        from datetime import datetime, timedelta, timezone
        rows = []
        base_day = datetime(2026, 10, 1, tzinfo=timezone.utc)
        for d in range(12):
            ts = (base_day + timedelta(days=d)).isoformat()
            px = 100.0 + d
            for metric, val in (
                ("transaction_count", 20.0 + d),
                ("survive_num", 5000.0),
                ("sell_num", 300.0),
                ("buy_num", 150.0),
                ("transaction_avg_price", px),
                ("sell_price", px * 1.02),
                ("buy_price", px * 0.95),
            ):
                rows.append({
                    "source": "siteb", "dataset": "item_history",
                    "item_id": "it1", "metric": metric, "platform": "all",
                    "ts": ts, "close": val,
                })
        self.storage.upsert_series(rows)
        from csgo_monitor.features import compute_l1, compute_l2
        items, written = compute_l1(self.storage)
        self.assertEqual(items, 1)
        self.assertGreater(written, 8)  # 至少 8 个特征
        frame = self.storage.read_feature_frame(["ret_7d", "dsr", "miss_rate_7d"])
        keys = [k for k in frame if k[0] == "it1"]
        self.assertTrue(keys)
        feats = frame[keys[0]]
        self.assertIn("ret_7d", feats)
        self.assertIn("dsr", feats)
        l2_items, l2_rows = compute_l2(self.storage)
        # 单品截面不足 20 件时 L2 多数字段为 None——只验证不崩、结构合法
        self.assertGreaterEqual(l2_items, 0)


if __name__ == "__main__":
    unittest.main()
