# -*- coding: utf-8 -*-
"""三方数据交叉验证：行情站B vs 行情站A vs Skinport。

同一饰品（按 market_hash_name 匹配）在独立数据源的价格对比。
两个及以上源偏离超阈值 → 标记异常，供人工/自动审查。

输出写入 series 表 dataset='data_quality'，metric='price_divergence'。

运行：python -m csgo_monitor cross-validate
"""

from __future__ import annotations

import json
import logging
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

from .storage import Storage

log = logging.getLogger(__name__)

QUALITY_DATASET = "data_quality"

# 偏离阈值：两个源的价格差异超过此比例视为可疑
DIVERGENCE_WARN = 0.50       # 10% 偏离 → warn
DIVERGENCE_CRITICAL = 1.00   # 30% 偏离 → critical

# Skinport 缓存（5 分钟刷新一次）
_skinport_cache: dict = {"data": [], "fetched_at": 0.0}


def _utcnow():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.isoformat()


def _f(v):
    try:
        f = float(v)
        return f if f == f and f > 0 else None
    except (TypeError, ValueError):
        return None


def _fetch_skinport() -> list[dict]:
    """拉取 Skinport 全目录（免 key、无限流、~25k 饰品）。"""
    import time
    now = time.time()
    if _skinport_cache["data"] and now - _skinport_cache["fetched_at"] < 300:
        return _skinport_cache["data"]

    req = urllib.request.Request(
        "https://api.skinport.com/v1/items?app_id=730&currency=CNY",
        headers={"User-Agent": "Mozilla/5.0 Chrome/131", "Accept-Encoding": "br"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read()
            enc = r.headers.get("Content-Encoding", "")
            if enc == "br":
                import brotli
                body = brotli.decompress(body)
            data = json.loads(body.decode())
            _skinport_cache["data"] = data
            _skinport_cache["fetched_at"] = now
            log.info("Skinport fetched %d items", len(data))
            return data
    except Exception as exc:
        log.warning("Skinport fetch failed: %s", exc)
        return _skinport_cache.get("data", [])


def run_cross_validate(storage: Storage) -> dict[str, Any]:
    """执行三方交叉验证。"""
    now = _utcnow()
    cutoff = _iso(now - timedelta(hours=26))
    anomalies: list[dict[str, Any]] = []
    stats = {"total_compared": 0, "agree": 0, "warn": 0, "critical": 0,
             "single_source": 0}

    # 1. 构建 item_id → market_hash_name 全局映射
    from collections import defaultdict as _dd
    from statistics import median as _median
    name_map: dict[str, str] = {}
    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        for row in conn.execute(
            """SELECT item_id, market_hash_name FROM items
            WHERE market_hash_name IS NOT NULL AND market_hash_name != ''"""):
            name_map[str(row["item_id"])] = str(row["market_hash_name"])

    def _collect(dataset: str, source: str, metric: str, platform: str = ""):
        """收集某数据集的价格，按 market_hash_name 分组取中位数。"""
        groups: dict[str, list[float]] = _dd(list)
        with storage.read_connect() as conn:
            conn.row_factory = __import__("sqlite3").Row
            sql = """SELECT s.item_id, s.close FROM series s
                WHERE s.dataset=? AND s.source=? AND s.metric=? AND s.ts >= ?"""
            args = [dataset, source, metric, cutoff]
            if platform:
                sql += " AND s.platform=?"
                args.append(platform)
            sql += " AND s.close > 0.5"
            for row in conn.execute(sql, args):
                name = name_map.get(str(row["item_id"]))
                v = _f(row["close"])
                if name and v:
                    groups[name].append(v)
        return {name: _median(vals) for name, vals in groups.items() if vals}

    siteb_prices = _collect("platform_snapshot", "siteb", "sell_price", "buff")
    # 行情站A：用 item_history（在售价，与 行情站B 口径一致）
    # exchange_snapshot 的 sell_price 是挂刀/回收价，口径不同
    sitea_prices = _collect("item_history", "sitea", "sell_price")

    # 3. Skinport 全目录
    skinport_data = _fetch_skinport()
    skinport_prices: dict[str, float] = {}
    for item in skinport_data:
        name = item.get("market_hash_name", "")
        v = _f(item.get("min_price"))
        if name and v and v > 0.5:
            skinport_prices[name] = v

    log.info("cross-validate: siteb=%d sitea=%d skinport=%d",
             len(siteb_prices), len(sitea_prices), len(skinport_prices))

    # 4. 三方对比
    all_names = set(siteb_prices) & (set(sitea_prices) | set(skinport_prices))
    for name in all_names:
        sdt = siteb_prices.get(name)
        csq = sitea_prices.get(name)
        skp = skinport_prices.get(name)
        prices = {"siteb": sdt, "sitea": csq, "skinport": skp}
        available = {k: v for k, v in prices.items() if v}
        if len(available) < 2:
            stats["single_source"] += 1
            continue

        stats["total_compared"] += 1
        values = list(available.values())
        median = sorted(values)[len(values) // 2]

        # 计算最大偏离
        max_div = 0.0
        outlier_source = ""
        for src, v in available.items():
            div = abs(v - median) / median if median > 0 else 0
            if div > max_div:
                max_div = div
                outlier_source = src

        if max_div >= DIVERGENCE_CRITICAL:
            stats["critical"] += 1
            anomalies.append({
                "source": "siteb", "dataset": QUALITY_DATASET,
                "item_id": name, "metric": "price_divergence",
                "platform": "", "ts": _iso(now), "close": max_div,
                "extra": {
                    "severity": "critical",
                    "outlier": outlier_source,
                    "prices": {k: round(v, 2) for k, v in available.items()},
                    "divergence": round(max_div, 3),
                    "signal": f"{outlier_source}价格偏离中位数{max_div*100:.0f}%",
                },
            })
        elif max_div >= DIVERGENCE_WARN:
            stats["warn"] += 1
            anomalies.append({
                "source": "siteb", "dataset": QUALITY_DATASET,
                "item_id": name, "metric": "price_divergence",
                "platform": "", "ts": _iso(now), "close": max_div,
                "extra": {
                    "severity": "warn",
                    "outlier": outlier_source,
                    "prices": {k: round(v, 2) for k, v in available.items()},
                    "divergence": round(max_div, 3),
                    "signal": f"{outlier_source}价格偏离{max_div*100:.0f}%",
                },
            })
        else:
            stats["agree"] += 1

    if anomalies:
        storage.upsert_series(anomalies)

    log.info("cross-validate: %d compared, %d agree, %d warn, %d critical",
             stats["total_compared"], stats["agree"],
             stats["warn"], stats["critical"])
    return {
        "status": "ok", "error_class": "",
        "metrics": stats,
        "details": {},
    }
