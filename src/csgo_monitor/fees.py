# -*- coding: utf-8 -*-
"""双侧净得费用口径（最终架构 §1 前置变换，2026-10-12 P0）。

背景（知识库2 §1.1/1.3，红队#9 修正）：
- Steam 费用由挂单方（maker）承担：挂卖单 A 成交卖方实得 A−fee(A)；
  挂求购 B 成交买方实付 B+fee(B)。买卖两口口径天然不对称，
  一切价差/位置类计算必须先过净得变换，否则系统性偏移。
- Steam 费率 = 5% Steam 费 + 10% 游戏费，各含最低 0.01 货币单位
  （社区反编译共识，知识库2 §1.1）。低价段（<¥2）0.85 平乘严重
  失真，必须用精确函数。
- BUFF 卖家佣金 2.5%（知识库1 §2.1）。

口径约定：本系统价格数据为人民币计（行情站B 聚合），
最小计价单位按 0.01 元处理。
"""

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN

STEAM_FEE_RATE_STEAM = 0.05
STEAM_FEE_RATE_GAME = 0.10
STEAM_FEE_MIN = 0.01          # 各项最低费（元）
BUFF_SELLER_FEE_RATE = 0.025  # BUFF 卖家佣金
CENT = Decimal("0.01")
# 高于该价时 floor 修正可忽略、平乘近似误差 <0.5%（架构 §1）
FLAT_MULTIPLIER_THRESHOLD = 20.0


def steam_fee(price: float) -> float:
    """Steam 挂单成交的总费用（买方支付口径），精确到分、向下取整。

    社区复刻公式（非复合：两项费都以基价为基数，合计约 15%）：
      fee(P) = max(0.01, floor(0.05·P·100)/100)
             + max(0.01, floor(0.10·P·100)/100)
    中价段恒为 ~15%（知识库1 §2.1 口径）；亚美元价位实测案例存在
    ±0.01 社区口径分歧（知识库2 §1.7），对 CNY 数据影响限于 <¥2 段。
    """
    if price is None or price <= 0:
        return 0.0
    p = Decimal(str(price))
    part1 = (p * Decimal(str(STEAM_FEE_RATE_STEAM)) * 100).to_integral_value(
        rounding=ROUND_DOWN) / 100
    part2 = (p * Decimal(str(STEAM_FEE_RATE_GAME)) * 100).to_integral_value(
        rounding=ROUND_DOWN) / 100
    f1 = max(Decimal(str(STEAM_FEE_MIN)), part1)
    f2 = max(Decimal(str(STEAM_FEE_MIN)), part2)
    return float(f1 + f2)


def net_steam_sell(price: float) -> float:
    """Steam 挂卖单成交后卖方净得（元，下限 0——费用不得使净得为负）。"""
    if price is None or price <= 0:
        return 0.0
    return max(0.0, float(Decimal(str(price)) - Decimal(str(steam_fee(price)))))


def gross_steam_buy(cost: float) -> float:
    """买方挂求购单成交实付 = 出价 + fee(出价)（求购单费用由买方承担）。"""
    if cost is None or cost <= 0:
        return 0.0
    return float(Decimal(str(cost)) + Decimal(str(steam_fee(cost))))


def net_buff_sell(price: float, fee_rate: float = BUFF_SELLER_FEE_RATE) -> float:
    """BUFF 卖出后卖方净得（元，比例佣金）。"""
    if price is None or price <= 0:
        return 0.0
    return price * (1.0 - fee_rate)


def spd_fb(steam_sell: float | None, buff_sell: float | None) -> float | None:
    """双侧净得口径的 Steam-BUFF 价差（最终架构 §2 L1 特征 spd_fb）。

    spd_fb = (net_steam(steam_sell) − net_buff(buff_sell)) / net_buff(buff_sell)
    >0：Steam 净得溢价（余额贴水/出逃压力方向之一，需与量联判）；
    <0：BUFF 端折价或 Steam 便宜（倒余额资金流入方向）。
    任一端缺价（None）时返回 None（调用方 features/validation 均传可空值）。
    """
    nb = net_buff_sell(buff_sell) if buff_sell and buff_sell > 0 else None
    ns = net_steam_sell(steam_sell) if steam_sell and steam_sell > 0 else None
    if nb is None or ns is None or nb <= 0:
        return None
    return (ns - nb) / nb


def knife_ratio(buff_sell: float, steam_sell: float) -> float | None:
    """挂刀比例（知识库2 §2.3 / 知识库3 §3.2 定义）：
    第三方到手卖价 ÷ Steam 挂单到手价 = net_buff(buff) / net_steam(steam)。

    实操带：≤0.9 值得出手（含利润要求）；Steam 场内双边做市的
    理论保本线为 B/A<0.739（0.85A>1.15B，知识库2 §1.3），两者口径
    不同——本函数是跨平台挂刀口径。
    """
    nb = net_buff_sell(buff_sell) if buff_sell and buff_sell > 0 else None
    ns = net_steam_sell(steam_sell) if steam_sell and steam_sell > 0 else None
    if nb is None or ns is None or ns <= 0:
        return None
    return nb / ns


def taker_roundtrip_cost(ask: float, bid: float) -> float | None:
    """吃单来回成本（架构 §1：两腿均无费，表观价差即 taker 成本）。"""
    if not ask or not bid or ask <= 0 or bid <= 0 or ask < bid:
        return None
    return (ask - bid) / ask
