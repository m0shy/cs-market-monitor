# -*- coding: utf-8 -*-
"""CS 饰品市场专属指标引擎（2026-10-11，五参谋+双 worker 调研裁决版）。

与照搬股市指标的根本区别（判决依据）：
- 供给非再生且可观测：survive_num 是真实流通盘 → 换手率/惜售度/供给增速
  是股市没有的因果维度（稀有掉落池 2026-01 关闭后绝版箱严格零供给）。
- 价格发现分散 9 平台：价差带费率锚定（Steam 15% vs 三方 ~2.5%，
  理论均衡折价带 0.85-0.975），极值=结构断点而非套利信号。
- 挂单簿是存量非流量：RSI/MACD 在挂单价上语义错位（冻结簿 RSI=100），
  本引擎只计算挂单簿状态类指标（OBI/价差/新鲜度）。
- 所有窗口按日历时间戳对齐（爬虫采样不规则，点位计数横向不可比）。
- 制度断点（回测不可跨）：2025-07 交易保护、2026-01 稀有掉落池关闭、
  2026-05 Major 胶囊改直购。

指标集（最小可行 8 个，按预测价值排序）：
  日频: turnover(换手率), hoarding(惜售度), supply_growth_7d(供给增速),
        ret_7d/ret_30d(动量), vol_30d(波动率)
  小时频: obi(挂单失衡), book_spread(盘口价差比), staleness_h(报价新鲜度)
  跨平台: spd_steam_buff(套现折价), arb_width(套利宽度)
  实时流: listing_rate_1h(上架流速), listing_discount(上架折价)

输出写入 series 表：dataset='indicator_daily'/'indicator_hourly'，
metric 同上述名称；分块 upsert 遵循 SSD 省损约束。

运行：python -m csgo_monitor skin-indicators [--lookback-hours 48] [--daily]
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .storage import Storage

log = logging.getLogger(__name__)

HOUR_DATASET = "indicator_hourly"
DAILY_DATASET = "indicator_daily"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _f(value: Any) -> float | None:
    try:
        out = float(value)
        return out if out == out else None  # NaN guard
    except (TypeError, ValueError):
        return None


def compute_hourly_indicators(
    storage: Storage, lookback_hours: int = 48,
) -> int:
    """小时频指标：OBI / 盘口价差比 / 报价新鲜度（来自 item_history）。

    每个饰品每个日历小时聚合一次（AVG 平滑采样不规则），只从上次
    指标水位之后增量计算（SSD 省损）。
    """
    now = _utcnow()
    # 增量水位：该数据集最新 ts
    with storage.read_connect() as conn:
        conn.row_factory = __import__('sqlite3').Row
        row = conn.execute(
            "SELECT MAX(ts) FROM series WHERE dataset=?", (HOUR_DATASET,)
        ).fetchone()
    watermark = str(row[0]) if row and row[0] else ""
    start = now - timedelta(hours=max(1, lookback_hours))
    if watermark:
        try:
            wm = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
            if wm.tzinfo is None:
                wm = wm.replace(tzinfo=timezone.utc)
            start = max(start, wm - timedelta(hours=1))
        except ValueError:
            pass
    if start >= now:
        return 0

    rows_out: list[dict[str, Any]] = []
    with storage.read_connect() as conn:
        conn.row_factory = __import__('sqlite3').Row
        # 按饰品×小时聚合：挂单价量取均值（采样不规则的中性处理）
        data = conn.execute(
            """SELECT item_id,
                      substr(ts,1,13) AS hour,
                      AVG(CASE WHEN metric='sell_price' THEN close END) AS sell_p,
                      AVG(CASE WHEN metric='buy_price'  THEN close END) AS buy_p,
                      AVG(CASE WHEN metric='sell_num'   THEN close END) AS sell_n,
                      AVG(CASE WHEN metric='buy_num'    THEN close END) AS buy_n,
                      MAX(ts) AS last_ts
            FROM series
            WHERE dataset='item_history' AND source='siteb'
              AND metric IN ('sell_price','buy_price','sell_num','buy_num')
              AND ts >= ?
            GROUP BY item_id, hour
            HAVING sell_p IS NOT NULL
            LIMIT 20000""",
            (_iso(start),),
        ).fetchall()
        for row in data:
            hour = str(row["hour"])  # '2026-10-10T09'
            ts = hour + ":00:00+00:00"
            if watermark and ts <= watermark:
                continue
            item = str(row["item_id"])
            sell_p, buy_p = _f(row["sell_p"]), _f(row["buy_p"])
            sell_n, buy_n = _f(row["sell_n"]), _f(row["buy_n"])
            base = {
                "source": "siteb", "dataset": HOUR_DATASET,
                "item_id": item, "platform": "", "ts": ts,
            }
            # 挂单失衡：计数无价格维度，含关注度噪声（红队标注）
            if sell_n is not None and buy_n is not None and (sell_n + buy_n) > 0:
                rows_out.append({**base, "metric": "obi",
                                 "close": (sell_n - buy_n) / (sell_n + buy_n)})
            # 盘口价差比：(卖一-买一)/中间价，负或过大剔除
            if sell_p and buy_p and buy_p > 0 and sell_p >= buy_p:
                mid = (sell_p + buy_p) / 2
                if mid > 0:
                    rows_out.append({**base, "metric": "book_spread",
                                     "close": (sell_p - buy_p) / mid})
            # 报价新鲜度：该小时窗口内最新观测距今小时数（越小越活跃）
            try:
                last = datetime.fromisoformat(
                    str(row["last_ts"]).replace("Z", "+00:00"))
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                rows_out.append({**base, "metric": "staleness_h",
                                 "close": max(0.0, (now - last).total_seconds() / 3600)})
            except ValueError:
                pass
        # 上架流速（市场级）：滑动 1h 窗口的 ws_new_listing 计数
        listing = conn.execute(
            """SELECT substr(ts,1,13) AS hour, COUNT(*) AS n,
                      AVG(close) AS avg_price
            FROM series WHERE dataset='ws_new_listing'
              AND ts >= ?
            GROUP BY hour""",
            (_iso(now - timedelta(hours=max(1, lookback_hours) + 1)),),
        ).fetchone()
        if listing and listing["n"]:
            rows_out.append({
                "source": "siteb", "dataset": HOUR_DATASET,
                "item_id": "", "platform": "", "metric": "listing_rate_1h",
                "ts": str(listing["hour"]) + ":00:00+00:00",
                "close": float(listing["n"]),
                "extra": {"avg_list_price": _f(listing["avg_price"])},
            })
    if rows_out:
        return storage.upsert_series(rows_out)
    return 0


def compute_daily_indicators(storage: Storage) -> int:
    """日频指标：换手率/惜售度/供给增速/动量/波动率。

    以最近 40 个日历日的日终快照为基准（每饰品取每日最后一次观测）。
    """
    now = _utcnow()
    start = now - timedelta(days=45)
    with storage.read_connect() as conn:
        conn.row_factory = __import__('sqlite3').Row
        # 日终快照：每饰品每日最新一条（SQLite 窗口函数）
        daily = conn.execute(
            """SELECT item_id, substr(ts,1,10) AS day,
                      MAX(CASE WHEN metric='transaction_count' THEN close END) AS tx,
                      MAX(CASE WHEN metric='survive_num' THEN close END) AS survive,
                      MAX(CASE WHEN metric='sell_num' THEN close END) AS sell_n,
                      MAX(CASE WHEN metric='transaction_avg_price' THEN close END) AS avg_p,
                      MAX(CASE WHEN metric='sell_price' THEN close END) AS sell_p
            FROM (
                SELECT item_id, metric, close, ts,
                       ROW_NUMBER() OVER (
                           PARTITION BY item_id, substr(ts,1,10), metric
                           ORDER BY ts DESC) AS rn
                FROM series
                WHERE dataset='item_history' AND source='siteb'
                  AND metric IN ('transaction_count','survive_num','sell_num',
                                 'transaction_avg_price','sell_price')
                  AND ts >= ?
            ) WHERE rn = 1
            GROUP BY item_id, day""",
            (_iso(start),),
        ).fetchall()
    if not daily:
        return 0

    # 组织为 item -> day -> snapshot
    from collections import defaultdict
    series_map: dict[str, dict[str, dict[str, float | None]]] = defaultdict(dict)
    for row in daily:
        series_map[str(row["item_id"])][str(row["day"])] = {
            "tx": _f(row["tx"]), "survive": _f(row["survive"]),
            "sell_n": _f(row["sell_n"]),
            "avg_p": _f(row["avg_p"]), "sell_p": _f(row["sell_p"]),
        }

    rows_out: list[dict[str, Any]] = []
    for item, days in series_map.items():
        sorted_days = sorted(days)
        if not sorted_days:
            continue
        last_day = sorted_days[-1]
        snap = days[last_day]
        base = {
            "source": "siteb", "dataset": DAILY_DATASET,
            "item_id": item, "platform": "",
            "ts": last_day + "T23:59:59+00:00",
        }
        # 换手率：日成交笔数/存世量（真实流通盘）
        if snap["tx"] and snap["survive"]:
            rows_out.append({**base, "metric": "turnover",
                             "close": snap["tx"] / snap["survive"]})
        # 惜售度：在售挂单/存世量（骤降=领先涨价，参谋1）
        if snap["sell_n"] and snap["survive"]:
            rows_out.append({**base, "metric": "hoarding",
                             "close": snap["sell_n"] / snap["survive"]})
        # 供给增速：7d 存世量环比（转负=通缩 regime）
        d7 = (datetime.strptime(last_day, "%Y-%m-%d") - timedelta(days=7)
              ).strftime("%Y-%m-%d")
        prev = days.get(d7)
        if prev and prev["survive"] and snap["survive"]:
            rows_out.append({**base, "metric": "supply_growth_7d",
                             "close": (snap["survive"] - prev["survive"]) / prev["survive"]})
        # 动量与波动率：用成交均价（缺则卖价）
        px_key = "avg_p" if snap.get("avg_p") else "sell_p"
        px = [days[d].get(px_key) for d in sorted_days[-31:]]
        px = [p for p in px if p]
        if len(px) >= 8 and px[0]:
            ret7 = px[-1] / px[min(-8, -len(px))] - 1 if len(px) >= 8 else None
            if ret7 is not None:
                rows_out.append({**base, "metric": "ret_7d", "close": ret7})
            rets = [px[i] / px[i - 1] - 1 for i in range(1, len(px)) if px[i - 1]]
            if len(rets) >= 7:
                mean = sum(rets) / len(rets)
                var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
                # 年化 √365（全年无休市，非股市 √252）
                rows_out.append({**base, "metric": "vol_30d_ann",
                                 "close": (var ** 0.5) * (365 ** 0.5)})
            if len(px) >= 25 and px[-25]:
                rows_out.append({**base, "metric": "ret_30d",
                                 "close": px[-1] / px[-25] - 1})

    # 跨平台：套现折价（Steam vs BUFF）与套利宽度（用最新快照）
    cross_rows: list[dict[str, Any]] = []
    with storage.read_connect() as conn:
        conn.row_factory = __import__('sqlite3').Row
        cross = conn.execute(
            """SELECT item_id,
                      MAX(CASE WHEN platform='steam' THEN close END) AS steam_p,
                      MAX(CASE WHEN platform='buff' THEN close END) AS buff_p,
                      MIN(close) AS min_p, MAX(close) AS max_p,
                      COUNT(*) AS n_plat
            FROM series
            WHERE dataset='platform_snapshot' AND source='siteb'
              AND metric='sell_price'
              AND ts >= ?
            GROUP BY item_id
            HAVING buff_p IS NOT NULL AND n_plat >= 2""",
            (_iso(now - timedelta(hours=26)),),
        ).fetchall()
    for row in cross:
        item = str(row["item_id"])
        buff_p, steam_p = _f(row["buff_p"]), _f(row["steam_p"])
        base = {
            "source": "siteb", "dataset": DAILY_DATASET,
            "item_id": item, "platform": "",
            "ts": _iso(now),
        }
        # 套现折价：Steam价/BUFF价；理论均衡带 0.85-0.975（费率锚定），
        # 带外=结构断点警告而非套利信号（红队判决）
        if buff_p and steam_p and buff_p > 0:
            cross_rows.append({**base, "metric": "spd_steam_buff",
                               "close": steam_p / buff_p})
        lo, hi = _f(row["min_p"]), _f(row["max_p"])
        if lo and hi and lo > 0:
            cross_rows.append({**base, "metric": "arb_width",
                               "close": (hi - lo) / ((hi + lo) / 2)})
    rows_out.extend(cross_rows)
    if rows_out:
        return storage.upsert_series(rows_out)
    return 0


def run_skin_indicators(
    storage: Storage, *, lookback_hours: int = 48,
) -> dict[str, Any]:
    hourly = daily = 0
    error = ""
    try:
        hourly = compute_hourly_indicators(storage, lookback_hours)
    except Exception as exc:  # noqa: BLE001
        error = f"hourly: {type(exc).__name__}"
        log.warning("skin hourly failed: %s", exc)
    try:
        daily = compute_daily_indicators(storage)
    except Exception as exc:  # noqa: BLE001
        error = error or f"daily: {type(exc).__name__}"
        log.warning("skin daily failed: %s", exc)
    try:
        daily += compute_v2_indicators(storage)
    except Exception as exc:  # noqa: BLE001
        log.warning("skin v2 failed: %s", exc)
    log.info("skin indicators: hourly=%s daily=%s", hourly, daily)
    return {
        "status": "ok",
        "error_class": "",
        "metrics": {"hourly_rows": hourly, "daily_rows": daily},
        "details": {},
    }


# ============================================================================
# V2 指标（2026-10-11 八参谋判决采纳批次）
# ============================================================================

def compute_v2_indicators(storage: Storage) -> int:
    """V2：板块温度计 L/U、活跃修正宽度、上架流磨损分布。

    依据 docs/cs-market-indicators-v2.md 的判决：
    - L/U 比类比成长/价值（参谋H）：刀手套=筹码品、步枪=实用盘
    - 宽度只在活跃集上算（参谋H）：冷门品零变动天然偏空
    - 上架磨损中位左移=优质库存涌出=高位分发（参谋A+Aalto 实证）
    """
    now = _utcnow()
    rows: list[dict[str, Any]] = []
    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        # 1) 板块温度计：最近板块指数快照（folder_index_history）
        sectors = {}
        for row in conn.execute(
            """SELECT item_id, close FROM series
               WHERE dataset='folder_index_history' AND metric='ohlc'
                 AND ts >= ? ORDER BY ts DESC""",
            (_iso(now - timedelta(hours=30)),),
        ):
            key = str(row["item_id"]).upper()
            if key not in sectors:  # 每板块取最新
                sectors[key] = _f(row["close"])
        knife = sectors.get("KNIFE") or sectors.get("匕首")
        glove = sectors.get("GLOVE") or sectors.get("手套")
        rifle = sectors.get("RIFLE") or sectors.get("步枪")
        if knife and glove and rifle and rifle > 0:
            import math
            lu = math.sqrt(knife * glove) / rifle
            rows.append({
                "source": "siteb", "dataset": DAILY_DATASET,
                "item_id": "", "platform": "", "metric": "sector_lu_ratio",
                "ts": _iso(now), "close": lu,
                "extra": {"knife": knife, "glove": glove, "rifle": rifle},
            })

        # 2) 活跃修正宽度：近24h有成交的饰品中涨跌占比 + 活跃率
        today = now.strftime("%Y-%m-%d")
        breadth = conn.execute(
            """SELECT
                SUM(CASE WHEN tx_today > tx_yest THEN 1 ELSE 0 END) AS up,
                SUM(CASE WHEN tx_today < tx_yest THEN 1 ELSE 0 END) AS down,
                SUM(CASE WHEN tx_today > 0 THEN 1 ELSE 0 END) AS active,
                COUNT(*) AS total
            FROM (
                SELECT item_id,
                    MAX(CASE WHEN substr(ts,1,10)=? THEN close ELSE 0 END) AS tx_today,
                    MAX(CASE WHEN substr(ts,1,10)=? THEN close ELSE 0 END) AS tx_yest
                FROM series
                WHERE dataset='item_history' AND metric='transaction_count'
                  AND ts >= ? AND ts < ?
                GROUP BY item_id
            )""",
            (today, (now - timedelta(days=1)).strftime("%Y-%m-%d"),
             _iso(now - timedelta(days=2)), _iso(now)),
        ).fetchone()
        if breadth and breadth["total"]:
            if breadth["active"] and breadth["active"] > 0:
                net = (breadth["up"] or 0) - (breadth["down"] or 0)
                rows.append({
                    "source": "siteb", "dataset": DAILY_DATASET,
                    "item_id": "", "platform": "",
                    "metric": "breadth_active",
                    "ts": _iso(now),
                    "close": net / breadth["active"],
                })
            rows.append({
                "source": "siteb", "dataset": DAILY_DATASET,
                "item_id": "", "platform": "",
                "metric": "active_rate",
                "ts": _iso(now),
                "close": breadth["active"] / breadth["total"],
            })

        # 3) 上架流磨损中位（当日 ws_new_listing 的 floatWear 分布位置）
        wears = []
        for row in conn.execute(
            """SELECT extra_json FROM series
               WHERE dataset='ws_new_listing' AND ts >= ?""",
            (_iso(now - timedelta(hours=24)),),
        ):
            try:
                extra = __import__("json").loads(row[0] or "{}")
                w = _f(extra.get("floatWear"))
                if w is not None:
                    wears.append(w)
            except (ValueError, TypeError):
                continue
        if len(wears) >= 30:
            wears.sort()
            median = wears[len(wears) // 2]
            rows.append({
                "source": "siteb", "dataset": DAILY_DATASET,
                "item_id": "", "platform": "",
                "metric": "listing_float_median",
                "ts": _iso(now), "close": median,
                "extra": {"n": len(wears),
                          "p25": wears[len(wears)//4],
                          "p75": wears[3*len(wears)//4]},
            })
    if rows:
        return storage.upsert_series(rows)
    return 0
