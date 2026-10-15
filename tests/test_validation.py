# -*- coding: utf-8 -*-
"""验证脚手架统计原语测试（最终架构 §5 纪律）。"""
import sys
import unittest

sys.path.insert(0, "src")

from csgo_monitor.validation import (  # noqa: E402
    diebold_mariano,
    mde,
    newey_west_t,
    replay_features,
    segment_days,
    spearman,
)


class StatsTests(unittest.TestCase):
    def test_spearman_perfect(self):
        self.assertAlmostEqual(spearman(list(range(20)), list(range(20))), 1.0)
        self.assertAlmostEqual(spearman(list(range(20)), list(range(20, 0, -1))), -1.0)

    def test_spearman_insufficient(self):
        self.assertIsNone(spearman([1, 2], [2, 1]))

    def test_spearman_quintile_sized(self):
        # 红审 H2 回归：五分位单调性需要 n=5 可算（原 n<10 恒 None）
        self.assertAlmostEqual(spearman([1, 2, 3, 4, 5], [10, 20, 30, 40, 50]), 1.0)
        self.assertAlmostEqual(spearman([1, 2, 3, 4, 5], [5, 4, 3, 2, 1]), -1.0)

    def test_newey_west_t_sign(self):
        # 含噪声的正/负均值序列（常序列零方差→None 是正确行为）
        pos = [0.01 + ((i % 5) - 2) * 0.001 for i in range(40)]
        neg = [-0.01 + ((i % 5) - 2) * 0.001 for i in range(40)]
        self.assertGreater(newey_west_t(pos), 2.0)
        self.assertLess(newey_west_t(neg), -2.0)
        self.assertIsNone(newey_west_t([0.1] * 5))

    def test_dm_detects_outperformance(self):
        model = [0.02 + ((i % 4) - 2) * 0.001 for i in range(40)]
        base = [0.0 + ((i % 4) - 2) * 0.001 for i in range(40)]
        t = diebold_mariano(model, base)
        self.assertIsNotNone(t)
        self.assertGreater(t, 3.0)

    def test_mde_scales(self):
        # 样本越大 MDE 越小
        self.assertGreater(mde(0.1, 50), mde(0.1, 200))
        self.assertIsNone(mde(0.1, 1))

    def test_segment_days_event_vs_stable(self):
        events = [{"date": "2025-10-22", "type": "policy"}]
        self.assertEqual(segment_days("2025-10-25", events), "switch:policy")
        self.assertEqual(segment_days("2025-06-01", events), "stable")


class ReplayTests(unittest.TestCase):
    def test_replay_features_structure(self):
        days = []
        for i in range(60):
            days.append({
                "day": f"2026-{7 + i // 28:02d}-{1 + i % 28:02d}",
                "tx": 20.0 + (i % 5), "survive": 5000.0 - i,
                "sell_n": 300.0, "buy_n": 150.0 + i * 0.5,
                "avg_p": 100.0 * (1.0 + 0.002 * i),
                "steam_p": 120.0, "buff_p": 100.0,
            })
        feats = replay_features(days)
        self.assertIn("ret_7d", feats)
        self.assertIn("dsr_z90", feats)
        self.assertIn("amihud_7d", feats)
        self.assertIn("spd_fb", feats)
        self.assertGreater(feats["ret_7d"], 0)

    def test_zero_volume_discipline(self):
        # 只有 10 天数据 → 不产特征（零成交/短历史纪律）
        self.assertEqual(replay_features([{"day": "2026-01-01"}] * 10), {})


if __name__ == "__main__":
    unittest.main()
