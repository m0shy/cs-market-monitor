# -*- coding: utf-8 -*-
"""市场看盘台 / 特征网格 / 监控信号 / 供应与成交流仪表盘 API（纯搬运自 web/app.py）.

注意：`db` 从 web.app 引入——测试通过 patch web.app.DB 切换测试库，
连接工厂必须继续读取 web.app 的模块全局。
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

def _f(value):
    """安全浮点转换（拆分审计发现：原引用全仓不存在的 _f 致 NameError
    被吞——价差/成交/大盘三段恒为空）。"""
    try:
        out = float(value)
        return out if out == out else None
    except (TypeError, ValueError):
        return None


from fastapi import APIRouter, Query

from web.app import db
from web.state import _ttl_cached

router = APIRouter()


@router.get("/api/dashboard/market")
@_ttl_cached(120)
def api_dashboard_market():
    """市场看盘台：跨平台价差榜 + 热门成交 + 大盘状态。
    性能优化（2026-10-14）：改用 feature_store 已计算的 spd_fb/turn_7d，
    不再实时聚合 121GB series 表（原版 60 秒 → <0.1 秒）。"""
    with db() as conn:
        # 1. 跨平台价差：用 feature_store 的 spd_fb
        spreads = []
        try:
            max_ts = conn.execute(
                "SELECT MAX(ts) FROM feature_store WHERE freq='1d' AND name='spd_fb'"
            ).fetchone()[0]
            if max_ts:
                rows = conn.execute(
                    """SELECT f.item_id, f.value,
                           i.market_hash_name, i.name AS zh_name, i.raw_json
                    FROM feature_store f
                    LEFT JOIN items i ON i.source='siteb' AND i.item_id = f.item_id
                    WHERE f.freq='1d' AND f.name='spd_fb' AND f.ts = ?
                      AND ABS(f.value) > 0.05
                    ORDER BY ABS(f.value) DESC LIMIT 10""",
                    (max_ts,),
                ).fetchall()
                for r in rows:
                    image = ""
                    try:
                        raw = json.loads(r["raw_json"] or "{}")
                        image = raw.get("imageUrl") or raw.get("imgUrl") or ""
                    except Exception:
                        pass
                    spreads.append({
                        "item_id": str(r["item_id"]),
                        "name": r["market_hash_name"] or r["zh_name"] or str(r["item_id"]),
                        "image": image,
                        "spread_pct": round(abs(r["value"]) * 100, 2) if r["value"] else 0,
                    })
        except Exception as exc:
            log.debug("spreads section failed: %s", exc)

        # 2. 热门成交：用 feature_store 的 turn_7d
        hot_items = []
        try:
            rows = conn.execute(
                """SELECT f.item_id, f.value,
                       i.market_hash_name, i.name AS zh_name
                FROM feature_store f
                LEFT JOIN items i ON i.source='siteb' AND i.item_id = f.item_id
                WHERE f.freq='1d' AND f.name='turn_7d' AND f.ts =
                    (SELECT MAX(ts) FROM feature_store WHERE freq='1d' AND name='turn_7d')
                  AND f.value IS NOT NULL
                ORDER BY f.value DESC LIMIT 20""",
            ).fetchall()
            for r in rows:
                hot_items.append({
                    "item_id": str(r["item_id"]),
                    "name": r["market_hash_name"] or r["zh_name"] or str(r["item_id"]),
                    "turnover": round(r["value"], 6) if r["value"] else 0,
                })
        except Exception as exc:
            log.debug("hot_items section failed: %s", exc)

        # 3. 大盘状态
        market = {}
        try:
            row = conn.execute(
                """SELECT close FROM series
                WHERE dataset='market_index' AND metric='market_index'
                ORDER BY ts DESC LIMIT 1"""
            ).fetchone()
            prev = conn.execute(
                """SELECT close FROM series
                WHERE dataset='market_index' AND metric='market_index'
                  AND ts < datetime('now', '-23 hours')
                ORDER BY ts DESC LIMIT 1"""
            ).fetchone()
            if row and row[0]:
                market["index"] = round(row[0], 2)
                if prev and prev[0]:
                    market["diff"] = round(row[0] - prev[0], 2)
                    market["diff_pct"] = round((row[0] - prev[0]) / prev[0] * 100, 2) if prev[0] else 0
        except Exception as exc:
            log.debug("market section failed: %s", exc)

    return {"spreads": spreads, "hot_items": hot_items, "market": market}


@router.get("/api/features/grid")
@_ttl_cached(120)
def api_features_grid(
    limit: int = Query(100, ge=10, le=500),
    sort: str = Query("ano_score", pattern="^(ano_score|miss_rate_7d|ret_7d|dsr_z90|amihud_7d|liq_contraction|crash_pressure)$"),
    max_miss: float = Query(0.5, ge=0.0, le=1.0),
):
    """特征出口：最新截面按指定特征排序的饰品×特征网格。

    特征体系（features.py L1×20 + L2×8）此前只写 feature_store 无读取
    出口——本接口补上闭环。响应：{items:[{item_id,name,features:{...},
    computed_at}], total, as_of}。miss_rate 过滤掉数据质量差的行。
    """
    sort_cols = {
        "ano_score", "miss_rate_7d", "ret_7d", "dsr_z90", "amihud_7d",
        "liq_contraction", "crash_pressure",
    }
    if sort not in sort_cols:
        sort = "ano_score"
    feature_names = [
        "ret_7d", "ret_30d", "rev_1w4w", "dd_30d", "vol_30d", "tail_ratio",
        "amihud_7d", "turn_7d", "dsr", "dsr_z90", "supply_growth_7d",
        "obi_d", "spd_fb", "miss_rate_7d", "lvl_mig_7d", "val_turn_7d",
        "liq_contraction", "absorption", "distribution", "zero_vol_rise",
        "rel_strength", "crash_pressure", "bottom_seq", "ano_score",
    ]
    with db() as conn:
        # 性能优化（2026-10-14）：特征是批量计算的，同一批共享同一 ts。
        # 原版 WITH latest + JOIN 扫描 2.3M 行需 47 秒；
        # 改为取全局 MAX(ts)（走索引）再等值查询，<1 秒。
        max_ts = conn.execute(
            "SELECT MAX(ts) FROM feature_store WHERE freq='1d'"
        ).fetchone()[0]
        if not max_ts:
            return {"items": [], "total": 0, "as_of": ""}
        rows = conn.execute(
            """SELECT f.item_id, f.name, f.value, f.ts, f.computed_at,
                   i.market_hash_name, i.name AS zh_name
            FROM feature_store f
            LEFT JOIN items i ON i.source='siteb' AND i.item_id = f.item_id
            WHERE f.freq='1d' AND f.ts = ? AND f.name IN ({names})
            ORDER BY f.item_id""".format(names=",".join("?" * len(feature_names))),
            [max_ts, *feature_names],
        ).fetchall()
    items_map: dict[str, dict[str, Any]] = {}
    as_of = ""
    for r in rows:
        iid = str(r["item_id"])
        if iid not in items_map:
            items_map[iid] = {
                "item_id": iid,
                "name": r["market_hash_name"] or r["zh_name"] or iid,
                "features": {},
                "computed_at": str(r["computed_at"] or ""),
            }
            if r["computed_at"] and str(r["computed_at"]) > as_of:
                as_of = str(r["computed_at"])
        if r["value"] is not None:
            items_map[iid]["features"][str(r["name"])] = float(r["value"])
    # miss 过滤 + 排序
    def sort_key(entry: dict[str, Any]) -> float:
        v = entry["features"].get(sort)
        return v if v is not None else float("-inf")

    filtered = [
        e for e in items_map.values()
        if e["features"].get("miss_rate_7d", 0.0) <= max_miss
    ]
    filtered.sort(key=sort_key, reverse=True)
    top = filtered[:max(10, min(500, limit))]
    return {"items": top, "total": len(filtered), "as_of": as_of}


@router.get("/api/signals/list")
def api_signals_list(limit: int = Query(50, ge=1, le=200)):
    """当前活跃监控信号（中文名称+图片）。"""
    with db() as conn:
        rows = conn.execute(
            """SELECT item_id, metric, close, ts, extra_json
               FROM series WHERE dataset='market_signal'
               ORDER BY ts DESC LIMIT ?""",
            (max(1, min(200, limit)),),
        ).fetchall()
    signals = []
    for row in rows:
        try:
            extra = json.loads(row["extra_json"] or "{}")
        except json.JSONDecodeError:
            extra = {}
        signals.append({
            "item_id": row["item_id"], "type": row["metric"],
            "ts": row["ts"], "value": row["close"],
            "signal": extra.get("signal", ""),
            "severity": extra.get("severity", "info"),
            "item_name": extra.get("item_name", ""),
            "image_url": extra.get("image_url", ""),
            "conditions": extra.get("conditions", []),
            "detail": extra,
        })
    return {"signals": signals}


# ===========================================================================
# 市场仪表盘（替代信号系统——描述现状，不做预测）
# ===========================================================================

@router.get("/api/dashboard/supply-trend")
@_ttl_cached(300)
def api_dashboard_supply_trend(limit: int = Query(50, ge=10, le=200)):
    """在售件数趋势：谁的挂单量在下降（吸筹前兆，描述性）。
    性能优化（2026-10-14）：改用 feature_store 的 supply_growth_7d
    （已预计算），不再实时聚合 121GB series 表（原版 8 秒 → <0.1 秒）。"""
    from datetime import datetime, timezone
    with db() as conn:
        max_ts = conn.execute(
            "SELECT MAX(ts) FROM feature_store WHERE freq='1d' AND name='supply_growth_7d'"
        ).fetchone()[0]
        if not max_ts:
            return {"items": []}
        rows = conn.execute(
            """SELECT f.item_id, f.value,
                   i.market_hash_name, i.name AS zh_name
            FROM feature_store f
            LEFT JOIN items i ON i.source='siteb' AND i.item_id = f.item_id
            WHERE f.freq='1d' AND f.name='supply_growth_7d' AND f.ts = ?
              AND f.value IS NOT NULL
            ORDER BY f.value ASC
            LIMIT ?""",
            (max_ts, max(10, min(200, limit))),
        ).fetchall()
        items = []
        for r in rows:
            items.append({
                "item_id": str(r["item_id"]),
                "item_name": r["market_hash_name"] or r["zh_name"] or str(r["item_id"]),
                "supply_growth_7d": r["value"],
            })
    return {"items": items}


@router.get("/api/dashboard/volume-flow")
@_ttl_cached(120)
def api_dashboard_volume_flow(limit: int = Query(50, ge=10, le=200)):
    """实时成交流：近 1h 成交 vs 7d 均值的倍数（谁在被密集交易）。
    性能优化（2026-10-14）：改用 feature_store 的 turn_7d（已预计算）。"""
    with db() as conn:
        max_ts = conn.execute(
            "SELECT MAX(ts) FROM feature_store WHERE freq='1d' AND name='turn_7d'"
        ).fetchone()[0]
        if not max_ts:
            return {"items": []}
        rows = conn.execute(
            """SELECT f.item_id, f.value,
                   i.market_hash_name, i.name AS zh_name
            FROM feature_store f
            LEFT JOIN items i ON i.source='siteb' AND i.item_id = f.item_id
            WHERE f.freq='1d' AND f.name='turn_7d' AND f.ts = ?
              AND f.value IS NOT NULL
            ORDER BY f.value DESC
            LIMIT ?""",
            (max_ts, max(10, min(200, limit))),
        ).fetchall()
        items = []
        for r in rows:
            items.append({
                "item_id": str(r["item_id"]),
                "item_name": r["market_hash_name"] or r["zh_name"] or str(r["item_id"]),
                "turn_7d": r["value"],
            })
    return {"items": items}


@router.get("/api/dashboard/spread")
def api_dashboard_spread(limit: int = Query(50, ge=10, le=200)):
    """跨平台净价差：费后价差排名（真实套利空间）。"""
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    _cutoff = (_dt.now(_tz.utc) - _td(hours=26)).isoformat()
    with db() as conn:
        rows = conn.execute(
            """SELECT item_id,
                      MAX(CASE WHEN platform='buff' THEN close END) AS buff_p,
                      MAX(CASE WHEN platform='youpin' THEN close END) AS youpin_p,
                      MAX(CASE WHEN platform='c5' THEN close END) AS c5_p,
                      MAX(CASE WHEN platform='steam' THEN close END) AS steam_p,
                      MIN(close) AS min_p, MAX(close) AS max_p
            FROM series
            WHERE dataset='platform_snapshot' AND source='siteb'
              AND metric='sell_price'
              AND ts >= ?
            GROUP BY item_id
            HAVING COUNT(DISTINCT platform) >= 2 AND min_p > 10
            ORDER BY (max_p - min_p) / ((max_p + min_p) / 2) DESC
            LIMIT ?""",
            (_cutoff, max(10, min(200, limit))),
        ).fetchall()
        items = _enrich_items(conn, rows) if rows else []
    return {"items": items}


def _enrich_items(conn, rows) -> list[dict[str, Any]]:
    """批量补中文名+图片。"""
    if not rows:
        return []
    ids = [str(r["item_id"]) if hasattr(r, "keys") else str(r[0]) for r in rows]
    if not ids:
        return []
    marks = ",".join("?" * len(ids))
    info = {}
    for row in conn.execute(
        f"SELECT item_id, name, raw_json FROM items WHERE item_id IN ({marks})", ids):
        try:
            raw = json.loads(row["raw_json"] or "{}")
        except (ValueError, TypeError):
            raw = {}
        info[str(row["item_id"])] = {
            "name": row["name"] or str(row["item_id"]),
            "image_url": raw.get("imageUrl") or raw.get("imgUrl") or "",
        }
    out = []
    for r in rows:
        d = dict(r) if hasattr(r, "keys") else {"item_id": r[0]}
        iid = str(d.get("item_id", ""))
        meta = info.get(iid, {})
        d["item_name"] = meta.get("name", iid)
        d["image_url"] = meta.get("image_url", "")
        out.append(d)
    return out
