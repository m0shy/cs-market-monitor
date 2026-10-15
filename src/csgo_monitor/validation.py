# -*- coding: utf-8 -*-
"""验证脚手架（最终架构 §5：任何特征/信号上线前的裁判席）。

纪律（红队强化版）：
1. leakage-free：特征按日重放——某日的特征只用该日及之前的原始数据
   （从 item_history 直接重建，天然前视安全）；前向收益标签对齐 t+h。
2. purged 分割：训练/验证/测试之间的间隔 ≥ 标签窗 + 7 天冷却
   （embargo：h=4 周 → embargo 35 天）。
3. 分段计分：事件窗（切换点）与稳定期分开报——防止 persistence
   偏好淘汰"识别切换"的组件（红队#7）。
4. 功效前置：先报 MDE（给定样本量能检测到的最小 |IC|），
   |mean IC| < MDE 的判决一律为"证据不足"而非"有效"。
5. DM 检验对基线（persistence/买入持有），t≥3 才算胜出（对己对人
   同一标准，红队#11）。
6. 费用后净额：多空组合扣 3.5% 三方回合摩擦（库6 §7.3）。

首批验证特征（架构 §5.7）：dsr_z90 / amihud_7d / ret_7d（动量）/
rev_1w4w（反转，与动量对决即本市场首个学术级实验）/ spd_fb /
zero_vol_rise。

运行：python -m csgo_monitor validate-features --sample 1500 --months 18
"""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .fees import spd_fb
from .storage import Storage

log = logging.getLogger(__name__)

HORIZONS = (7, 14, 28)          # 前向收益窗（天）
ROUND_TRIP_COST = 0.035         # 三方回合摩擦（库6 §7.3）
DM_T_THRESHOLD = 3.0            # 红队#11：对己对人都用 t≥3
EVENTS_PATH = Path(__file__).resolve().parents[2] / "docs" / "market" / "events.json"


# ---------------------------------------------------------------------------
# 统计原语（可单测）
# ---------------------------------------------------------------------------

def _rank(v: list[float]) -> list[float]:
    n = len(v)
    order = sorted(range(n), key=lambda i: v[i])
    r = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and v[order[j + 1]] == v[order[i]]:
            j += 1
        avg = (i + j) / 2 + 1
        for k in range(i, j + 1):
            r[order[k]] = avg
        i = j + 1
    return r


def spearman(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 5:
        return None
    rx, ry = _rank(xs), _rank(ys)
    mx, my = sum(rx) / n, sum(ry) / n
    cov = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    vx = sum((a - mx) ** 2 for a in rx)
    vy = sum((a - my) ** 2 for a in ry)
    if vx <= 0 or vy <= 0:
        return None
    return cov / math.sqrt(vx * vy)


def newey_west_t(values: list[float], lags: int = 5) -> float | None:
    """HAC t 统计量（IC 序列的自相关修正检验，库7 §2.4）。"""
    n = len(values)
    if n < 10:
        return None
    mean = sum(values) / n
    dev = [v - mean for v in values]
    gamma0 = sum(d * d for d in dev) / n
    lrv = gamma0
    for l in range(1, min(lags, n - 1) + 1):
        gl = sum(dev[i] * dev[i - l] for i in range(l, n)) / n
        lrv += 2.0 * (1.0 - l / (lags + 1.0)) * gl
    if lrv <= 0:
        return None
    return mean / math.sqrt(lrv / n)


def diebold_mariano(model_rets: list[float], base_rets: list[float],
                    lags: int = 5) -> float | None:
    """DM t（损失差 = 模型日收益 − 基线日收益；>阈值为显著胜出）。"""
    if len(model_rets) != len(base_rets) or len(model_rets) < 10:
        return None
    d = [m - b for m, b in zip(model_rets, base_rets)]
    return newey_west_t(d, lags)


def mde(ic_std: float, n: int, alpha: float = 0.05, power: float = 0.80) -> float | None:
    """最小可检测效应（双侧 α、目标功效下的 |mean IC| 下限）。"""
    if n < 5 or ic_std <= 0:
        return None
    z_a, z_p = 1.96, 0.84
    return (z_a + z_p) * ic_std / math.sqrt(n)


def median(xs: list[float]) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def mad(xs: list[float], med: float | None = None) -> float | None:
    if not xs:
        return None
    m = med if med is not None else median(xs)
    assert m is not None
    return median([abs(x - m) for x in xs])


def robust_z(x: float, trail: Iterable[float]) -> float | None:
    trail = list(trail)
    if not trail:
        return None
    med = median(trail)
    if med is None:
        return None
    m = mad(trail, med)
    if not m or m <= 0:
        return None
    return (x - med) / (1.4826 * m)


def load_events() -> list[dict[str, str]]:
    if EVENTS_PATH.exists():
        try:
            return json.loads(EVENTS_PATH.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            return []
    return []


def segment_days(day: str, events: list[dict[str, str]], radius: int = 14) -> str:
    """日期分段：事件窗=切换点，其余=稳定期（分段计分纪律）。"""
    try:
        d = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return "unknown"
    for ev in events:
        try:
            e = datetime.fromisoformat(str(ev.get("date", ""))).replace(
                tzinfo=timezone.utc)
        except ValueError:
            continue
        if abs((d - e).total_seconds()) <= radius * 86400:
            return f"switch:{ev.get('type', 'event')}"
    return "stable"


# ---------------------------------------------------------------------------
# 特征逐日重放（PIT 安全：某日特征只用 ≤该日 数据）
# ---------------------------------------------------------------------------

def _daily_series(days: list[dict[str, Any]], key: str) -> list[float]:
    return [d[key] for d in days if d.get(key) is not None]  # type: ignore


def replay_features(
    days: list[dict[str, Any]],
) -> dict[str, float]:
    """在单个饰品的时间线上，对最后一天计算首批验证特征。

    days 为升序日快照（avg_p/tx/survive/sell_n/buy_n/steam_p/buff_p）。
    调用方对每个历史日 t 截断 days[:t+1] 后调用本函数——逐日重放。
    """
    out: dict[str, float] = {}
    if len(days) < 30:
        return out
    px = _daily_series(days, "avg_p")
    if len(px) >= 8 and px[-8] > 0:
        r7 = px[-1] / px[-8] - 1.0
        out["ret_7d"] = r7
        if len(px) >= 29 and px[-29] > 0:
            r28 = px[-1] / px[-29] - 1.0
            out["rev_1w4w"] = r7 - r28
    # amihud_7d：|日收益|/成交额，仅有成交日
    pairs = [(d["avg_p"], d["tx"]) for d in days[-7:]
             if d.get("avg_p") and d.get("tx") and d["tx"] > 0]  # type: ignore
    illiq = []
    for i in range(1, len(pairs)):
        prev, cur = pairs[i - 1][0], pairs[i][0]
        if prev > 0:
            turnover = pairs[i][0] * pairs[i][1]
            if turnover > 0:
                illiq.append(abs(cur / prev - 1.0) / turnover)
    if len(illiq) >= 2:
        out["amihud_7d"] = sum(illiq) / len(illiq)
    # dsr_z90：当日 dsr 对自身 90 日历史的 robust z
    last = days[-1]
    if last.get("sell_n") and last.get("buy_n") and last["sell_n"] > 0:  # type: ignore
        dsr_now = last["buy_n"] / last["sell_n"]  # type: ignore
        trail = [d["buy_n"] / d["sell_n"]  # type: ignore
                 for d in days[-91:-1]
                 if d.get("sell_n") and d.get("buy_n") and d["sell_n"] > 0]  # type: ignore
        z = robust_z(dsr_now, trail)
        if z is not None and abs(z) <= 100.0:  # 与生产端同步的退化守卫
            out["dsr_z90"] = z
    # spd_fb：双侧净得口径
    if last.get("steam_p") and last.get("buff_p"):
        s = spd_fb(last["steam_p"], last["buff_p"])  # type: ignore
        if s is not None:
            out["spd_fb"] = s
    # zero_vol_rise：7 日涨幅分位高 ∧ 换手低 ∧ 库存降（布尔嫌疑标记）
    if "ret_7d" in out and len(days) >= 14:
        tx7 = sum(d["tx"] or 0.0 for d in days[-7:])  # type: ignore
        tx_prev = sum(d["tx"] or 0.0 for d in days[-14:-7])  # type: ignore
        d7 = days[-8] if len(days) >= 8 else None
        sg = None
        if d7 is not None and d7.get("survive") and last.get("survive") \
                and d7["survive"] > 0:  # type: ignore
            sg = (last["survive"] - d7["survive"]) / d7["survive"]  # type: ignore
        if tx_prev > 0 and sg is not None:
            out["zero_vol_rise"] = 1.0 if (
                out["ret_7d"] > 0.10 and tx7 < tx_prev * 0.7 and sg < 0
            ) else 0.0
    return out


def _load_matrix(
    storage: Storage, months: int, sample: int, mode: str = "top",
) -> dict[str, list[dict[str, Any]]]:
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=months * 31)).isoformat()
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """SELECT item_id, substr(ts,1,10) AS day,
                MAX(CASE WHEN metric='transaction_count' THEN close END) AS tx,
                MAX(CASE WHEN metric='survive_num' THEN close END) AS survive,
                MAX(CASE WHEN metric='sell_num' THEN close END) AS sell_n,
                MAX(CASE WHEN metric='buy_num' THEN close END) AS buy_n,
                MAX(CASE WHEN metric='transaction_avg_price' THEN close END) AS avg_p
            FROM (
                SELECT item_id, metric, close, ts,
                       ROW_NUMBER() OVER (
                           PARTITION BY item_id, substr(ts,1,10), metric
                           ORDER BY ts DESC) AS rn
                FROM series
                WHERE dataset='item_history' AND source='siteb'
                  AND metric IN ('transaction_count','survive_num','sell_num',
                                 'buy_num','transaction_avg_price')
                  AND ts >= ?
            ) WHERE rn = 1
            GROUP BY item_id, day
            ORDER BY item_id, day
            LIMIT 3000000""",
            (start,),
        ).fetchall()
    for r in rows:
        out[str(r["item_id"])].append({
            "day": str(r["day"]),
            "tx": r["tx"], "survive": r["survive"],
            "sell_n": r["sell_n"], "buy_n": r["buy_n"],
            "avg_p": r["avg_p"],
        })
    if sample > 0 and len(out) > sample:
        def activity(days: list[dict[str, Any]]) -> float:
            return sum(d["tx"] or 0.0 for d in days[-7:])  # type: ignore
        if mode == "cold":
            # 冷门观察通道（架构 §3 L5）：操纵者画像段——
            # 近30日日均成交 5-50 且价格 ¥100-1000，取活跃度最低者
            def cold_score(days):
                recent = days[-30:]
                if not recent:
                    return None
                avg_tx = sum(d["tx"] or 0.0 for d in recent) / len(recent)
                prices = [d["avg_p"] for d in recent if d.get("avg_p")]
                med_p = median(prices) if prices else None
                if med_p is None or not (100.0 <= med_p <= 1000.0):
                    return None
                if not (5.0 * 30 <= sum(d["tx"] or 0.0 for d in recent) <= 50.0 * 30):
                    return None
                return avg_tx
            scored = [(cold_score(v), i) for i, v in out.items()]
            scored = [(sc, i) for sc, i in scored if sc is not None]
            scored.sort(key=lambda x: x[0])
            keep = [i for _, i in scored[:sample]]
        else:
            keep = sorted(out, key=lambda i: activity(out[i]), reverse=True)[:sample]
        out = {i: out[i] for i in keep}
    # 跨平台价：单独拉 platform_history 的 steam/buff 日序列
    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        cross = conn.execute(
            """SELECT item_id, substr(ts,1,10) AS day, platform,
                AVG(close) AS p
            FROM series
            WHERE dataset IN ('platform_history','platform_snapshot')
              AND source='siteb' AND metric='sell_price'
              AND platform IN ('steam','buff') AND ts >= ?
            GROUP BY item_id, day, platform""",
            (start,),
        ).fetchall()
    plat: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    for r in cross:
        plat[str(r["item_id"])].setdefault(str(r["day"]), {})[str(r["platform"])] = float(r["p"])
    for item, days in out.items():
        pdays = plat.get(item, {})
        for d in days:
            p = pdays.get(d["day"], {})
            if "steam" in p:
                d["steam_p"] = p["steam"]
            if "buff" in p:
                d["buff_p"] = p["buff"]
    return out


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run_validation(
    storage: Storage, *, sample: int = 1500, months: int = 18,
    min_cross: int = 25, mode: str = "top",
) -> dict[str, Any]:
    import time as _time
    t0 = _time.monotonic()
    matrix = _load_matrix(storage, months, sample, mode=mode)
    events = load_events()
    features = ("dsr_z90", "amihud_7d", "ret_7d", "rev_1w4w",
                "spd_fb", "zero_vol_rise")

    # day -> feature -> [(item, value, fwd_ret_h)]
    ic_data: dict[str, dict[int, dict[str, list[tuple[float, float]]]]] = {
        f: {h: defaultdict(list) for h in HORIZONS} for f in features
    }
    for item, days in matrix.items():
        if len(days) < 45:
            continue
        n = len(days)
        for t in range(30, n):
            feats = replay_features(days[:t + 1])
            if not feats:
                continue
            day = days[t].get("day") or ""
            px_t = days[t].get("avg_p")
            if not px_t or not day:
                continue
            try:
                day_t = datetime.strptime(day, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc)
            except ValueError:
                continue
            for h in HORIZONS:
                # 前向收益按日历对齐：目标日 = day_t + h 天，取该日或之后
                # 第一条有成交价的观测（红审 H4 修复：原按行索引会把
                # 稀疏品的 7 日窗实际拉长到 9-15 日历日）
                target = (day_t + timedelta(days=h)).strftime("%Y-%m-%d")
                tol = (day_t + timedelta(days=h + 7)).strftime("%Y-%m-%d")
                px_f = None
                # 红审II修复：容差内"无价"应继续找下一条有价观测
                # （原 break 会系统性丢弃稀疏品标签，与幸存者偏差同向）
                for j in range(t + 1, n):
                    dj = days[j].get("day") or ""
                    if dj < target:
                        continue
                    if dj > tol:
                        break
                    if days[j].get("avg_p"):
                        px_f = days[j]["avg_p"]
                        break
                if px_f and px_t > 0:
                    fwd = px_f / px_t - 1.0
                    for f in features:
                        if f in feats:
                            ic_data[f][h][day].append((feats[f], fwd))

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "sample_items": len(matrix), "months": months,
        "round_trip_cost": ROUND_TRIP_COST,
        "features": {}, "notes": [],
    }

    for f in features:
        f_rep: dict[str, Any] = {"horizons": {}}
        for h in HORIZONS:
            days_map = ic_data[f][h]
            days_sorted = sorted(days_map)
            ics: list[float] = []
            ls_rets: list[float] = []       # 多空（Q5-Q1）日收益（费用前）
            bh_rets: list[float] = []       # 买入持有基线
            pers_rets: list[float] = []     # persistence 基线（动量延续）
            seg_ics: dict[str, list[float]] = defaultdict(list)
            mono_hits = 0
            for day in days_sorted:
                pairs = days_map[day]
                if len(pairs) < min_cross:
                    continue
                xs = [p[0] for p in pairs]
                ys = [p[1] for p in pairs]
                ic = spearman(xs, ys)
                if ic is None:
                    continue
                ics.append(ic)
                seg = segment_days(day, events)
                seg_ics[seg].append(ic)
                # 五分组多空（费用前毛收益；净额在汇总时统一扣 3.5%）
                order = sorted(pairs, key=lambda p: p[0])
                k = len(order) // 5
                if k >= 3:
                    q1 = sum(p[1] for p in order[:k]) / k
                    q5 = sum(p[1] for p in order[-k:]) / k
                    allm = sum(ys) / len(ys)
                    ls_rets.append(q5 - q1)
                    bh_rets.append(allm)
                    # persistence 基线：预测 = 过去收益延续（按 ret 特征
                    # 排序的同向组合）；对非动量特征退化为全均值
                    pers_rets.append(allm if f != "ret_7d" else
                                     sum(p[1] for p in sorted(
                                         pairs, key=lambda p: -p[0])[:k]) / k)
                    quints = [sum(p[1] for p in order[i * k:(i + 1) * k]) / k
                              for i in range(5)]
                    qs = spearman([1, 2, 3, 4, 5], quints)
                    if qs is not None and abs(qs) >= 0.8:
                        mono_hits += 1
            n_days = len(ics)
            if n_days < 20:
                f_rep["horizons"][h] = {"n_days": n_days, "verdict": "样本不足"}
                continue
            mean_ic = sum(ics) / n_days
            std_ic = (sum((i - mean_ic) ** 2 for i in ics)
                      / max(1, n_days - 1)) ** 0.5
            t_nw = newey_west_t(ics, lags=max(5, h))
            mde_v = mde(std_ic, n_days)
            # 多空净额（费用后，每期一个回合）
            ls_net = [r - ROUND_TRIP_COST for r in ls_rets]
            ann_ls = (sum(ls_net) / len(ls_net)) * (365.0 / h) if ls_net else None
            # 红审 H3 修复：对照真实买入持有（全市场等权日收益，持有
            # 不付每期回合费），而非零收益；NW lags 取 max(5, h) 抵消
            # h 日重叠收益窗的自相关拖尾（红审 H7）
            dm_bh = (diebold_mariano(ls_net, bh_rets, lags=max(5, h))
                     if ls_net and len(bh_rets) == len(ls_net) else None)
            # 纪律 5 兑现：同时对照 persistence 基线（此前收集了从未消费）
            dm_pers = (diebold_mariano(ls_net, pers_rets, lags=max(5, h))
                       if ls_net and len(pers_rets) == len(ls_net) else None)
            verdict = "证据不足"
            if mde_v is not None and t_nw is not None:
                if abs(mean_ic) >= mde_v and abs(t_nw) >= 2.0:
                    if dm_bh is None or abs(dm_bh) < DM_T_THRESHOLD:
                        verdict = "IC 有效(待DM)"
                    elif dm_bh <= -DM_T_THRESHOLD:
                        verdict = "IC有效但显著劣于持有(疑似伪影)"
                    elif dm_bh > 0 and ann_ls and ann_ls > 0:
                        verdict = "有效+净额胜出"
                    else:
                        verdict = "IC有效但净额未胜出"
            f_rep["horizons"][h] = {
                "n_days": n_days,
                "mean_ic": round(mean_ic, 5),
                "ic_std": round(std_ic, 5),
                "icir": round(mean_ic / std_ic, 3) if std_ic > 0 else None,
                "nw_t": round(t_nw, 2) if t_nw is not None else None,
                "mde": round(mde_v, 5) if mde_v is not None else None,
                "ls_gross_period": round(sum(ls_rets) / len(ls_rets), 5) if ls_rets else None,
                "ls_net_annualized": round(ann_ls, 4) if ann_ls is not None else None,
                "dm_vs_hold": round(dm_bh, 2) if dm_bh is not None else None,
                "dm_vs_persist": round(dm_pers, 2) if dm_pers is not None else None,
                "monotonicity_rate": round(mono_hits / max(1, len(ls_rets)), 3),
                "segments": {
                    seg: {
                        "n": len(v),
                        "mean_ic": round(sum(v) / len(v), 5),
                    } for seg, v in sorted(seg_ics.items())
                },
                "verdict": verdict,
            }
        report["features"][f] = f_rep

    report["elapsed_s"] = round(_time.monotonic() - t0, 1)
    report["notes"] = [
        f"费用口径：多空每期扣 {ROUND_TRIP_COST:.1%} 三方回合摩擦",
        f"DM 门槛 t≥{DM_T_THRESHOLD}（对己对人同一标准）",
        "分段：switch=事件窗±14天 / stable=其余（红队#7 分段计分）",
        "MDE：|mean IC| < MDE 时判决为证据不足（功效前置纪律）",
        "宇宙偏差披露1（幸存者）：当日 404/下架品不进样本——IC 存在",
        "  乐观偏差，幅度不可测；PIT 目录快照积累后可修正",
        "宇宙偏差披露2（回填修订）：series 表同一主键被回填数据原地",
        "  覆盖，重放读到的是最终修订值而非当时观测值（无 first_observed",
        "  审计列）；实时型与回填型数据混存，IC 偏乐观",
        "zero_vol_rise 的重放变体与生产截面分位版口径不同，IC 结论",
        "  不可外推（事件研究法才是其正确评估，见 validation-round1.md）",
    ]
    return report
