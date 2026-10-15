# -*- coding: utf-8 -*-
"""供给耗竭追踪器（L2 约束层核心，docs/market/architecture-v3-sentiment.md §四）。

设计要点（与情绪层 L1 交叉使用，单看供给不看需求会上当）：
- 供给非再生且可观测：survive_num 是真实流通盘，月度消耗率/在售占比/
  连续收缩天数构成"筹码被锁死"的三维证据。
- burn_rate 采用净变化口径 (survive_now - survive_30d_ago)/survive_30d_ago，
  负值 = 存世在减少（消耗>开箱补充），正值 = 供给净扩张。
- 供给耗竭≠价格必涨：需求同步萎缩就是有价无市（Beanie Babies 教训），
  因此必须同时输出 demand_trend（transaction_count 方向）做交叉校验。
- 品类级聚合用 item_tags 的 category 标签（knife/glove/weapon/sticker/
  case/agent，其余一律归 other）；item_tags 缺失或为空时只出全市场汇总。

指标写 series 表 dataset='supply_exhaustion'（品类级 + 全市场）供 Web 消费：
  metric = exhaustion_score / burn_rate / listing_ratio / absorption_days，
  item_id = 品类名（全市场为空串），ts 对齐数据日避免重复运行膨胀。

运行：python -m csgo_monitor supply-tracker
"""

from __future__ import annotations

import logging
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .storage import Storage

log = logging.getLogger(__name__)

SOURCE = "siteb"
HISTORY_DATASET = "item_history"      # 输入：日级饰品观测（survive/sell/tx）
DATASET = "supply_exhaustion"         # 输出：本模块专属数据集

# 品类清单：item_tags 里这六个之外的 category（patch/music_kit 等）与
# 无标签饰品一律归入 other，保证聚合口径稳定且互斥。
CANONICAL_CATEGORIES = ("knife", "glove", "weapon", "sticker", "case", "agent")
MARKET_KEY = "market"                 # 全市场汇总的展示名（series 行 item_id=''）

# 回看窗口：30d 消耗 + 7d 增速 + 需求 7v7 对照，再加缓冲
LOOKBACK_DAYS = 80
BURN_WINDOW_DAYS = 30        # 月度消耗率窗口
DAY_TOLERANCE = 3            # 目标基准日缺失时允许的日历容差
GROWTH_WINDOW_DAYS = 7       # supply_growth_7d 窗口
ABSORPTION_GAP_DAYS = 3      # 吸收天数计数允许的日历空洞（爬虫缺勤）
DEMAND_WINDOW_DAYS = 7       # 需求趋势：近 7 天 vs 前 7 天
DEMAND_MOVE_THRESHOLD = 0.05  # 日均成交变化超 ±5% 才记方向

# exhaustion_score 归一化与权重（0-1 加权合成）
BURN_FULL = 0.02             # 月消耗 2% = burn_score 满分
LISTING_FULL = 0.30          # 在售占比 >=30% = listing_score 零分
ABSORPTION_FULL = 30         # 连续收缩 30 天 = absorption_score 满分
WEIGHT_BURN = 0.4
WEIGHT_LISTING = 0.3
WEIGHT_ABSORPTION = 0.3

# 供需缺口分级（阈值与三线对齐信号的 0.6 对齐）
GAP_TIGHT = 0.60             # exhaustion >= 0.6 → supply_tight
GAP_SLACK = 0.35             # exhaustion <= 0.35 → oversupplied

# 数据质量门槛：不足则不出该实体结果（宁缺毋滥，防小样本噪声）
MIN_DATA_DAYS = 8
MIN_ITEMS = 5


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


def _parse_day(day: str) -> datetime.date:
    return datetime.strptime(day, "%Y-%m-%d").date()


def _nearest_on_or_before(
    sorted_days: list[str], target: str, tol_days: int,
) -> str | None:
    """target 当日或之前 tol_days 天内最近的可用日（ISO 日期字典序=时间序）。"""
    idx = bisect_right(sorted_days, target) - 1
    if idx < 0:
        return None
    day = sorted_days[idx]
    delta = (_parse_day(target) - _parse_day(day)).days
    return day if 0 <= delta <= tol_days else None


def _tags_available(conn: Any) -> bool:
    """item_tags 表存在且含 category 标签才做品类聚合。"""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='item_tags'"
    ).fetchone()
    if not row:
        return False
    count = conn.execute(
        "SELECT COUNT(*) FROM item_tags WHERE tag_type='category'"
    ).fetchone()
    return bool(count and int(count[0]) > 0)


def _load_daily_supply(
    storage: Storage, now: datetime, *, with_tags: bool,
) -> list[tuple[str, str, float, float, float, int]]:
    """日级快照聚合：[(category, day, survive, sell, tx, n_items)]。

    每饰品每日每指标取最后一次观测（与 skin_indicators 同口径），
    标签侧每饰品只取一个 category（多标签时按字典序去重防重复计数）；
    无标签饰品归 other。全市场汇总由调用方跨品类累加（每组互斥）。
    """
    start = _iso(now - timedelta(days=LOOKBACK_DAYS))
    if with_tags:
        cat_expr = """COALESCE(
                   CASE WHEN t.tag_value IN ('knife','glove','weapon',
                                             'sticker','case','agent')
                        THEN t.tag_value ELSE 'other' END, 'other')"""
        tag_join = """
        LEFT JOIN (
            SELECT item_id, tag_value,
                   ROW_NUMBER() OVER (
                       PARTITION BY item_id ORDER BY tag_value) AS trn
            FROM item_tags WHERE tag_type='category'
        ) t ON t.item_id = d.item_id AND t.trn = 1"""
    else:
        # item_tags 缺失/为空：无品类维度，全部归入单一组（只出全市场汇总）
        cat_expr = "'market'"
        tag_join = ""
    sql = f"""
        SELECT {cat_expr} AS cat,
               d.day,
               SUM(d.survive) AS survive, SUM(d.sell) AS sell,
               SUM(d.tx) AS tx, COUNT(*) AS n_items
        FROM (
            SELECT h.item_id, h.day,
                   MAX(CASE WHEN h.metric='survive_num' THEN h.close END) AS survive,
                   MAX(CASE WHEN h.metric='sell_num' THEN h.close END) AS sell,
                   MAX(CASE WHEN h.metric='transaction_count' THEN h.close END) AS tx
            FROM (
                SELECT item_id, metric, close,
                       substr(ts,1,10) AS day, ts,
                       ROW_NUMBER() OVER (
                           PARTITION BY item_id, substr(ts,1,10), metric
                           ORDER BY ts DESC) AS rn
                FROM series
                WHERE dataset='{HISTORY_DATASET}' AND source='{SOURCE}'
                  AND metric IN ('survive_num','sell_num','transaction_count')
                  AND ts >= ?
            ) h
            WHERE h.rn = 1
            GROUP BY h.item_id, h.day
        ) d{tag_join}
        GROUP BY d.day, cat
        ORDER BY d.day
    """
    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        rows = conn.execute(sql, (start,)).fetchall()
    return [
        (str(r["cat"]), str(r["day"]),
         _f(r["survive"]) or 0.0, _f(r["sell"]) or 0.0,
         _f(r["tx"]) or 0.0, int(r["n_items"]))
        for r in rows
    ]


def _organize(
    day_rows: list[tuple[str, str, float, float, float, int]],
) -> tuple[dict[str, dict[str, dict[str, float]]], dict[str, dict[str, float]]]:
    """按品类组织日序列，同时跨品类累加全市场口径（标签已互斥去重）。"""
    by_cat: dict[str, dict[str, dict[str, float]]] = defaultdict(dict)
    market: dict[str, dict[str, float]] = defaultdict(
        lambda: {"survive": 0.0, "sell": 0.0, "tx": 0.0, "n": 0.0})
    for cat, day, survive, sell, tx, n_items in day_rows:
        by_cat[cat][day] = {
            "survive": survive, "sell": sell, "tx": tx, "n": float(n_items),
        }
        agg = market[day]
        agg["survive"] += survive
        agg["sell"] += sell
        agg["tx"] += tx
        agg["n"] += n_items
    return dict(by_cat), dict(market)


def _demand_trend(
    days: dict[str, dict[str, float]], sorted_days: list[str],
) -> tuple[str, float | None]:
    """需求方向：近 7 个可用日 vs 前 7 个可用日的日均成交（周内季节中性）。"""
    w = DEMAND_WINDOW_DAYS
    if len(sorted_days) < w + 3:
        return "0", None
    recent = [days[d]["tx"] for d in sorted_days[-w:]]
    prior = [days[d]["tx"] for d in sorted_days[-2 * w:-w]]
    if not prior:
        return "0", None
    mean_r = sum(recent) / len(recent)
    mean_p = sum(prior) / len(prior)
    if mean_p > 0:
        change = (mean_r - mean_p) / mean_p
    else:
        change = 1.0 if mean_r > 0 else 0.0
    if change > DEMAND_MOVE_THRESHOLD:
        return "+", round(change, 4)
    if change < -DEMAND_MOVE_THRESHOLD:
        return "-", round(change, 4)
    return "0", round(change, 4)


def _absorption_days(
    days: dict[str, dict[str, float]], sorted_days: list[str],
) -> int:
    """连续收缩天数：supply_growth_7d < 0 的连续天数（从最新日向前数）。

    7d 前基准缺失的日期不计入；日历空洞超过容差即断链（防跨缺勤拼接）。
    """
    growth: list[tuple[str, float]] = []
    for day in sorted_days:
        target = (_parse_day(day) - timedelta(days=GROWTH_WINDOW_DAYS)
                  ).isoformat()
        base = _nearest_on_or_before(sorted_days, target, DAY_TOLERANCE)
        if not base or base >= day:
            continue
        s_now, s_base = days[day]["survive"], days[base]["survive"]
        if s_base > 0:
            growth.append((day, (s_now - s_base) / s_base))
    streak = 0
    for i in range(len(growth) - 1, -1, -1):
        day, g = growth[i]
        if g >= 0:
            break
        if i < len(growth) - 1:
            gap = (_parse_day(growth[i + 1][0]) - _parse_day(day)).days
            if gap > ABSORPTION_GAP_DAYS:
                break
        streak += 1
    return streak


def _compute_supply_metrics(
    days: dict[str, dict[str, float]], *, category: str,
) -> dict[str, Any] | None:
    """对一条日级供给序列计算耗竭指标族；数据不足返回 None。"""
    sorted_days = sorted(days)
    if len(sorted_days) < MIN_DATA_DAYS:
        return None
    last_day = sorted_days[-1]
    snap = days[last_day]
    if snap["n"] < MIN_ITEMS:
        return None

    # 1) 月度消耗率：负值=存世在减少（消耗>开箱补充）
    base_day = _nearest_on_or_before(
        sorted_days,
        (_parse_day(last_day) - timedelta(days=BURN_WINDOW_DAYS)).isoformat(),
        DAY_TOLERANCE,
    )
    burn_rate: float | None = None
    if base_day:
        s_base = days[base_day]["survive"]
        s_now = snap["survive"]
        if s_base > 0:
            burn_rate = (s_now - s_base) / s_base
    # 历史不足 30d 时 burn 维度记 0 分（exhaustion 偏保守，不补权）
    burn_score = (
        min(1.0, max(0.0, -burn_rate / BURN_FULL))
        if burn_rate is not None else 0.0
    )

    # 2) 在售占存世比例：低=惜售/囤积（筹码锁定）
    listing_ratio: float | None = None
    if snap["survive"] > 0:
        listing_ratio = snap["sell"] / snap["survive"]
    listing_score = (
        min(1.0, max(0.0, (LISTING_FULL - listing_ratio) / LISTING_FULL))
        if listing_ratio is not None else 0.0
    )

    # 3) 供给持续收缩天数：supply_growth_7d 连续为负
    absorption = _absorption_days(days, sorted_days)
    absorption_score = min(1.0, absorption / ABSORPTION_FULL)

    # 4) 综合评分与供需缺口
    exhaustion = (
        WEIGHT_BURN * burn_score
        + WEIGHT_LISTING * listing_score
        + WEIGHT_ABSORPTION * absorption_score
    )
    if exhaustion >= GAP_TIGHT:
        gap = "supply_tight"
    elif exhaustion <= GAP_SLACK:
        gap = "oversupplied"
    else:
        gap = "balanced"

    # 需求方向（必输出：供给耗竭×需求死亡=有价无市）
    trend, demand_change = _demand_trend(days, sorted_days)

    # 有价无市风险：供给紧 + 需求还在萎缩
    risk = "dead_demand" if gap == "supply_tight" and trend == "-" else ""

    return {
        "category": category,
        "burn_rate": round(burn_rate, 6) if burn_rate is not None else None,
        "listing_ratio": (
            round(listing_ratio, 6) if listing_ratio is not None else None
        ),
        "absorption_days": int(absorption),
        "exhaustion_score": round(exhaustion, 4),
        "demand_trend": trend,
        "gap": gap,
        # 上下文（诊断与 Web 展示用）
        "items": int(snap["n"]),
        "data_days": len(sorted_days),
        "burn_base_days": (
            (_parse_day(last_day) - _parse_day(base_day)).days
            if base_day else 0
        ),
        "supply_now": round(snap["survive"], 1),
        "demand_change": demand_change,
        "scores": {
            "burn": round(burn_score, 4),
            "listing": round(listing_score, 4),
            "absorption": round(absorption_score, 4),
        },
        "risk_flag": risk,
        "as_of_day": last_day,
    }


def _write_series(
    storage: Storage,
    market: dict[str, Any] | None,
    categories: list[dict[str, Any]],
) -> int:
    """写 series 表（dataset='supply_exhaustion'）：品类级 + 全市场。

    ts 对齐数据日（非运行时刻），重复运行幂等 upsert 不膨胀。
    """
    rows: list[dict[str, Any]] = []
    entities: list[tuple[str, dict[str, Any]]] = []
    if market:
        entities.append(("", market))
    entities.extend(
        (payload["category"], payload) for payload in categories
    )
    for item_key, payload in entities:
        ts = f"{payload['as_of_day']}T23:59:59+00:00"
        extra = {
            "category": payload["category"],
            "demand_trend": payload["demand_trend"],
            "gap": payload["gap"],
            "risk_flag": payload["risk_flag"],
            "items": payload["items"],
            "demand_change": payload["demand_change"],
        }
        for metric, value in (
            ("exhaustion_score", payload["exhaustion_score"]),
            ("burn_rate", payload["burn_rate"]),
            ("listing_ratio", payload["listing_ratio"]),
            ("absorption_days", float(payload["absorption_days"])),
        ):
            if value is None:
                continue
            rows.append({
                "source": SOURCE, "dataset": DATASET,
                "item_id": item_key, "metric": metric,
                "platform": "", "ts": ts, "close": value, "extra": extra,
            })
    return storage.upsert_series(rows) if rows else 0


def run_supply_tracker(storage: Storage) -> dict[str, Any]:
    """供给耗竭追踪主入口（supervisor 兼容结果）。"""
    now = _utcnow()
    tagged = False
    try:
        with storage.read_connect() as conn:
            tagged = _tags_available(conn)
        day_rows = _load_daily_supply(storage, now, with_tags=tagged)
        by_cat, market_days = _organize(day_rows)
    except Exception as exc:  # noqa: BLE001
        log.exception("supply tracker load failed: %s", exc)
        return {
            "status": "failed",
            "error_class": type(exc).__name__,
            "metrics": {"rows_written": 0, "categories": 0, "tagged": tagged},
            "details": {"market": None, "categories": [], "tagged": tagged},
        }

    market_payload: dict[str, Any] | None = None
    categories: list[dict[str, Any]] = []
    try:
        market_payload = _compute_supply_metrics(market_days, category=MARKET_KEY)
        if market_payload is None and market_days:
            last_day = max(market_days)
            log.warning(
                "supply tracker: market data thin (days=%d items=%d)",
                len(market_days), int(market_days[last_day]["n"]),
            )
        if tagged:
            for cat in sorted(by_cat):
                payload = _compute_supply_metrics(by_cat[cat], category=cat)
                if payload is None:
                    log.info("supply tracker: category %s skipped (thin)", cat)
                    continue
                categories.append(payload)
        elif day_rows:
            log.info("supply tracker: item_tags empty, market-only mode")
    except Exception as exc:  # noqa: BLE001
        log.exception("supply tracker compute failed: %s", exc)

    rows_written = 0
    try:
        rows_written = _write_series(storage, market_payload, categories)
    except Exception as exc:  # noqa: BLE001
        log.exception("supply tracker write failed: %s", exc)

    if market_payload is None and not categories:
        log.warning("supply tracker: no output (no usable history)")
        return {
            "status": "partial",
            "error_class": "no_data",
            "metrics": {"rows_written": 0, "categories": 0, "tagged": tagged},
            "details": {"market": None, "categories": [], "tagged": tagged},
        }

    log.info(
        "supply tracker: market exhaustion=%s gap=%s, %d categories, %d rows",
        market_payload["exhaustion_score"] if market_payload else "n/a",
        market_payload["gap"] if market_payload else "n/a",
        len(categories), rows_written,
    )
    return {
        "status": "ok",
        "error_class": "",
        "metrics": {
            "rows_written": rows_written,
            "categories": len(categories),
            "tagged": tagged,
            "market_exhaustion_score": (
                market_payload["exhaustion_score"] if market_payload else None
            ),
            "market_gap": market_payload["gap"] if market_payload else None,
            "market_demand_trend": (
                market_payload["demand_trend"] if market_payload else None
            ),
        },
        "details": {
            "market": market_payload,
            "categories": categories,
            "tagged": tagged,
            "window_days": LOOKBACK_DAYS,
        },
    }
