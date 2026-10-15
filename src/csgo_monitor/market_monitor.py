# -*- coding: utf-8 -*-
"""CS 饰品市场监控信号引擎 V3（2026-10-11，六参谋审判后彻底重写）。

V2 的教训（75% 假阳性的根因）：
1. 固定阈值 vs 个体差异：日成交 3 笔的饰品"涨 50%"= 1→2 笔（噪声）
2. 小分母噪声：82% 信号触在日均 6-8 笔的刀/手套上
3. 结构性饱和：绝版品存世增速恒≈0 → "供给断零"对它们永远为真
4. Δsell_num 一个方程三个未知数（成交-撤单+新挂），不可区分
5. 数据过期：信号用了 2-8 天前的快照

V3 设计原则：
- 硬性流动性门槛：日成交 ≥20 笔 AND 价格 ≥¥100
- 数据新鲜度门槛：只用 ≤24h 内的最新快照
- 个体化 z-score：中位数/MAD 稳健统计，绝不用固定百分比
- 信号精简：从 10 类砍到 4 类，命名诚实（不叫"大商建仓"因为不可证）
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)

SIGNAL_DATASET = "market_signal"
MIN_TX_DAILY = 20
MIN_PRICE = 100.0
MAX_DATA_AGE_H = 24
BASELINE_DAYS = 42
RECENT_DAYS = 3
Z_STRONG = 2.0
Z_MODERATE = 1.5


def _utcnow():
    return datetime.now(timezone.utc)

def _iso(dt):
    return dt.isoformat()

def _f(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None

def _robust_z(value, baseline):
    if not baseline or len(baseline) < 5:
        return 0.0
    bs = sorted(baseline)
    n = len(bs)
    median = bs[n // 2]
    mad = sorted(abs(b - median) for b in baseline)[n // 2]
    if mad < 1e-10:
        return 3.0 if abs(value - median) > 1e-10 else 0.0
    return (value - median) / (1.4826 * mad)

def _get_item_info(storage, item_ids):
    info = {}
    if not item_ids:
        return info
    batch = item_ids[:500]
    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        marks = ",".join("?" * len(batch))
        for row in conn.execute(
            f"SELECT item_id, name, raw_json FROM items WHERE item_id IN ({marks})", batch):
            try:
                raw = json.loads(row["raw_json"] or "{}")
            except (ValueError, TypeError):
                raw = {}
            info[str(row["item_id"])] = {
                "name": row["name"] or str(row["item_id"]),
                "image_url": raw.get("imageUrl") or raw.get("imgUrl") or "",
            }
    return info


def compute_signals_v3(storage):
    now = _utcnow()
    signals = []

    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(
            """SELECT item_id, substr(ts,1,10) AS day, MAX(ts) AS last_ts,
                      MAX(CASE WHEN metric='sell_num' THEN close END) AS sn,
                      MAX(CASE WHEN metric='transaction_count' THEN close END) AS tx,
                      MAX(CASE WHEN metric='sell_price' THEN close END) AS sp,
                      MAX(CASE WHEN metric='transaction_avg_price' THEN close END) AS ap
            FROM (
                SELECT item_id, metric, close, ts,
                       ROW_NUMBER() OVER (
                           PARTITION BY item_id, substr(ts,1,10), metric
                           ORDER BY ts DESC) AS rn
                FROM series
                WHERE dataset='item_history' AND source='siteb'
                  AND metric IN ('sell_num','transaction_count',
                                 'sell_price','transaction_avg_price')
                  AND ts >= ?
            ) WHERE rn = 1
            GROUP BY item_id, day""",
            (_iso(now - timedelta(days=BASELINE_DAYS + 5)),),
        ).fetchall()

    series_map = defaultdict(list)
    for row in rows:
        series_map[str(row["item_id"])].append({
            "day": str(row["day"]), "last_ts": str(row["last_ts"]),
            "sn": _f(row["sn"]), "tx": _f(row["tx"]),
            "sp": _f(row["sp"]), "ap": _f(row["ap"]),
        })

    for item, obs in series_map.items():
        obs.sort(key=lambda o: o["day"])
        if len(obs) < BASELINE_DAYS // 2:
            continue
        latest = obs[-1]

        try:
            lt = datetime.fromisoformat(latest["last_ts"].replace("Z", "+00:00"))
            if lt.tzinfo is None:
                lt = lt.replace(tzinfo=timezone.utc)
            age_h = (now - lt).total_seconds() / 3600
            if age_h > MAX_DATA_AGE_H:
                continue
        except ValueError:
            continue

        tx_latest = latest.get("tx") or 0
        if tx_latest < MIN_TX_DAILY:
            continue
        price_latest = latest.get("ap") or latest.get("sp") or 0
        if price_latest < MIN_PRICE:
            continue

        baseline_obs = obs[:-RECENT_DAYS]
        recent_obs = obs[-RECENT_DAYS:]
        if len(baseline_obs) < 10:
            continue

        sn_hist = [o["sn"] for o in baseline_obs if o["sn"] is not None]
        tx_hist = [o["tx"] for o in baseline_obs if o["tx"] is not None]
        sp_hist = [o["ap"] or o["sp"] for o in baseline_obs if (o["ap"] or o["sp"]) is not None]

        sn_recent = sum(o["sn"] or 0 for o in recent_obs) / len(recent_obs)
        tx_recent = sum(o["tx"] or 0 for o in recent_obs) / len(recent_obs)
        sp_rv = [o["ap"] or o["sp"] for o in recent_obs if (o["ap"] or o["sp"]) is not None]
        sp_recent = sum(sp_rv) / len(sp_rv) if sp_rv else None

        z_sn = _robust_z(sn_recent, sn_hist) if sn_hist else 0
        z_tx = _robust_z(tx_recent, tx_hist) if tx_hist else 0
        price_ret = 0.0
        if sp_recent and sp_hist:
            bm = sorted(sp_hist)[len(sp_hist) // 2]
            if bm and bm > 0:
                price_ret = sp_recent / bm - 1
        z_price = _robust_z(sp_recent, sp_hist) if sp_recent and sp_hist else 0

        def _sig(metric, name, conditions, severity):
            if all(c[1] for c in conditions):
                signals.append({
                    "source": "siteb", "dataset": SIGNAL_DATASET,
                    "item_id": item, "metric": metric,
                    "platform": "", "ts": _iso(now),
                    "close": sum(1 for c in conditions if c[1]),
                    "extra": {
                        "signal": name, "severity": severity,
                        "z": {"sn": round(z_sn, 2), "tx": round(z_tx, 2), "price": round(z_price, 2)},
                        "price_ret": round(price_ret, 4),
                        "tx_daily": round(tx_recent, 1),
                        "price": round(price_latest, 1),
                        "data_age_h": round(age_h, 1),
                        "baseline_d": len(baseline_obs),
                        "conditions": [c[0] for c in conditions if c[1]],
                        "caveat": "挂单变化可能来自撤单" if metric == "supply_absorption" else "",
                    },
                })

        _sig("supply_absorption", "供给收缩+承接", [
            (f"挂单z<-{Z_STRONG}", z_sn < -Z_STRONG),
            ("成交z>+1", z_tx > 1.0),
            ("价格不跌>2%", price_ret > -0.02),
            (f"数据新鲜<{MAX_DATA_AGE_H}h", age_h <= MAX_DATA_AGE_H),
        ], "critical")

        _sig("volume_anomaly", "成交异动", [
            (f"成交z>+{Z_STRONG + 0.5}", z_tx > Z_STRONG + 0.5),
            ("价格方向z>1", abs(z_price) > 1.0),
            (f"日成交>={MIN_TX_DAILY}", tx_recent >= MIN_TX_DAILY),
            (f"数据新鲜<{MAX_DATA_AGE_H}h", age_h <= MAX_DATA_AGE_H),
        ], "warn")

        _sig("distribution", "派发信号", [
            (f"挂单z>+{Z_STRONG}", z_sn > Z_STRONG),
            ("价格z<-1.5", z_price < -Z_MODERATE),
            ("成交z>+1(放量)", z_tx > 1.0),
            (f"数据新鲜<{MAX_DATA_AGE_H}h", age_h <= MAX_DATA_AGE_H),
        ], "critical")

    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        lu_hist = [_f(r["close"]) for r in conn.execute(
            "SELECT close FROM series WHERE dataset='indicator_daily' "
            "AND metric='sector_lu_ratio' AND item_id='' AND ts >= ? "
            "ORDER BY ts", (_iso(now - timedelta(days=42)),))]
        lu_hist = [v for v in lu_hist if v is not None]
        if len(lu_hist) >= 10:
            z_lu = _robust_z(lu_hist[-1], lu_hist[:-1])
            if abs(z_lu) >= Z_STRONG:
                signals.append({
                    "source": "siteb", "dataset": SIGNAL_DATASET,
                    "item_id": "", "metric": "index_context",
                    "platform": "", "ts": _iso(now), "close": z_lu,
                    "extra": {"signal": "风险偏好极端",
                              "type": "expansion" if z_lu > 0 else "contraction",
                              "severity": "warn", "z_score": round(z_lu, 2),
                              "conditions": ["L/U稳健z>=2s"],
                              "caveat": "强趋势z可能低估sigma"},
                })

    item_ids = list({s["item_id"] for s in signals if s["item_id"]})
    if item_ids:
        info = _get_item_info(storage, item_ids)
        for s in signals:
            meta = info.get(s["item_id"], {})
            s["extra"]["item_name"] = meta.get("name", s["item_id"])
            s["extra"]["image_url"] = meta.get("image_url", "")

    count = storage.upsert_series(signals) if signals else 0
    by_type = defaultdict(int)
    for s in signals:
        by_type[s["metric"]] += 1
    log.info("market monitor V3: %d signals %s", len(signals), dict(by_type))
    return {
        "status": "ok", "error_class": "",
        "metrics": {"total_signals": len(signals), "by_type": dict(by_type),
                    "rows_written": count},
        "details": {},
    }
