# -*- coding: utf-8 -*-
"""fees.py 双侧净得口径测试（最终架构 §1）。"""
import math
import sys
import unittest

sys.path.insert(0, "src")

from csgo_monitor.fees import (  # noqa: E402
    gross_steam_buy,
    knife_ratio,
    net_buff_sell,
    net_steam_sell,
    spd_fb,
    steam_fee,
    taker_roundtrip_cost,
)


class FeeTests(unittest.TestCase):
    def test_fee_minimum_floor(self):
        # $0.03 级（知识库2 §1.4）：两项各触底 0.01 → 总费 0.02
        self.assertAlmostEqual(steam_fee(0.03), 0.02)

    def test_fee_known_case(self):
        # 知识库2 §1.2 实测例量级：求购 0.33 → 买方付 0.37-0.38
        # （亚美元价位社区口径存在 ±0.01 分歧，知识库2 §1.7）
        f = steam_fee(0.33)
        self.assertGreaterEqual(f, 0.04)
        self.assertLessEqual(f, 0.05)
        self.assertAlmostEqual(gross_steam_buy(0.33), 0.33 + f)

    def test_fee_standard_band(self):
        # 中价段：约 15%
        f = steam_fee(100.0)
        self.assertGreaterEqual(f, 14.9)
        self.assertLessEqual(f, 15.01)

    def test_net_sell_never_negative(self):
        # 红审修复回归：极低价段费用不得使净得为负
        for px in (0.01, 0.02, 0.03, 0.05):
            self.assertGreaterEqual(net_steam_sell(px), 0.0)

    def test_net_sell_positive_and_below_flat(self):
        self.assertAlmostEqual(net_steam_sell(100.0), 100.0 - steam_fee(100.0))
        # 低价段净得不为负
        self.assertGreater(net_steam_sell(0.05), 0.0)

    def test_net_buff(self):
        self.assertAlmostEqual(net_buff_sell(100.0), 97.5)

    def test_spd_fb_bilateral(self):
        # Steam 116 / BUFF 100：净得 98.6 vs 97.5 → +1.1% 量级（非 16%）
        v = spd_fb(116.0, 100.0)
        self.assertIsNotNone(v)
        self.assertGreater(v, 0.0)
        self.assertLess(v, 0.03)  # 平乘口径会是 0.16，双侧净得显著更小

    def test_knife_ratio_breakeven_band(self):
        # 知识库2 §2.3 定义：net_buff/net_steam；80 vs 100 → 78/85 ≈ 0.918
        r = knife_ratio(80.0, 100.0)
        self.assertIsNotNone(r)
        self.assertGreater(r, 0.90)
        self.assertLess(r, 0.93)

    def test_taker_roundtrip(self):
        self.assertAlmostEqual(taker_roundtrip_cost(105.0, 100.0), 5.0 / 105.0)
        self.assertIsNone(taker_roundtrip_cost(100.0, 105.0))

    def test_invalid_inputs(self):
        self.assertEqual(steam_fee(0), 0.0)
        self.assertEqual(steam_fee(-5), 0.0)
        self.assertIsNone(spd_fb(0.0, 100.0))
        self.assertIsNone(knife_ratio(10.0, 0.0))


if __name__ == "__main__":
    unittest.main()
