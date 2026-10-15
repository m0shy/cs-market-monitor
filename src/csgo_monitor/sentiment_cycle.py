# -*- coding: utf-8 -*-
"""L1 情绪周期状态机（架构 v3 主导层，2026-10-13）。

取代 v2 量化因子范式的"从价格推断 regime"——本轮范式转换为
"情绪直接观测"（docs/market/architecture-v3-sentiment.md）。

五阶段循环：狂热 → 恐慌 → 出清 → 冷漠 → 萌芽 → (循环)

当前只用现有可观测字段（搜索量/社交/KOL 为 P0 待建，接入后扩展）：
  - transaction_count（item_history 日聚合到市场级成交量）
  - breadth_active（逐日重算：成交活跃品中涨跌家数差占比）
  - listing_rate（ws_new_listing 日计数）
  - spd_steam_buff / spd_fb（跨平台价差，恐慌溢价方向）
  - market_index（大盘指数，注意 ts 无时区后缀按 UTC+8 解释）
  - survive_num（存世量，配对 7 日环比——只比两日共有饰品，
    对采集覆盖集变化稳健）

诚实边界（务必保留，防止过度自信）：
1. 所有阈值是 v1 先验，未经回测校准（Phase 3 校准）。
2. 采集覆盖坍缩时（tracked 饰品数骤减），成交量类证据会被
   污染为"萎缩"——覆盖比 <0.5 时该类证据置为不可判（None），
   宁可输出低置信，绝不输出假"冷漠"。
3. 无阶段完全匹配时输出部分得分最高的阶段，置信度封顶 0.5。
4. 每个阶段判定都附完整依据快照（各指标当前值 vs 阈值），
   写入 series 供 Web 透出——判定过程必须可审计。

输出写入 series：dataset='sentiment_cycle'
  metric='stage'      close=阶段序号（STAGE_INDEX），extra=完整判定快照
  metric='confidence' close=置信度
  metric='ind_<名>'   各指标当前值 + 基线/阈值/是否满足（透明审计）

运行：python -m csgo_monitor sentiment-cycle（CLI 接线后）或调度调用
run_sentiment_cycle(storage)。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from statistics import fmean
from typing import Any

from .fees import spd_fb as _spd_fb_fn
from .storage import Storage

log = logging.getLogger(__name__)

DATASET = "sentiment_cycle"

# 阶段 → close 序号（写入 series 的 stage 行）
STAGE_INDEX = {"狂热": 0, "恐慌": 1, "出清": 2, "冷漠": 3, "萌芽": 4}
# 循环中的下一阶段（转移提示用）
NEXT_STAGE = {"狂热": "恐慌", "恐慌": "出清", "出清": "冷漠",
              "冷漠": "萌芽", "萌芽": "狂热"}
# 多阶段同时命中时的裁决优先级（急性状态优先）
PRIORITY = ("恐慌", "狂热", "出清", "萌芽", "冷漠")

# ---------------------------------------------------------------------------
# v1 初始阈值（architecture-v3-sentiment.md §三，待 Phase 3 回测校准）
# ---------------------------------------------------------------------------
TH_BREADTH_FRENZY = 0.7        # 狂热：活跃宽度 > 0.7
TH_TX_FRENZY = 2.0             # 狂热：成交量 > 30 日中位 ×2
TH_INDEX_NEAR_HIGH = 0.05      # 狂热：大盘距 90 日高点 5% 以内
TH_INDEX_DROP_PANIC = 0.05     # 恐慌：大盘单日跌幅 > 5%
TH_LISTING_PANIC = 3.0         # 恐慌：挂单流速 > 中位 ×3
TH_SPD_PANIC = 2.0             # 恐慌：spd_fb > 中位 ×2
TH_RET30_CLEAR = -0.10         # 出清：大盘 30 日回报 < -10%
TH_TX_CLEAR = 0.5              # 出清：成交量 < 中位 ×0.5
TH_TX_APATHY = 0.3             # 冷漠：成交量 < 中位 ×0.3
TH_BREADTH_APATHY = 0.3        # 冷漠：活跃宽度 < 0.3
TH_BREADTH_SPROUT_LO = 0.3     # 萌芽：宽度进入 0.3-0.5
TH_BREADTH_SPROUT_HI = 0.5
TH_SPROUT_DAYS = 7             # 萌芽：连续 7 天成交 > 前 30 日中位

# 均衡折价带（skin_indicators 费率锚定结论：0.85-0.975，带外=结构断点）
SPD_RATIO_BAND = (0.85, 0.975)

# 窗口参数
LOOKBACK_DAYS = 37      # 原始数据回看（30 日基线 + 3 日近期 + 冗余）
BASELINE_DAYS = 30      # 基线窗口
RECENT_DAYS = 3         # "当前"平滑窗口（日快照噪声大，取近 3 日均）
MIN_BASELINE_DAYS = 10  # 基线最少样本数（不足 → 该指标不可判）
COVERAGE_OK = 0.8       # 覆盖比 ≥0.8 视为健康
COVERAGE_FLOOR = 0.5    # 覆盖比 <0.5 视为证据不可用（防假"冷漠"）
MIN_PAIR_ITEMS = 100    # 配对环比最少共有饰品数
MAX_GROUP_ROWS = 500_000  # 分组查询行数护栏（SSD/内存保护）
MAX_STAGE_HISTORY = 120   # 回读阶段历史行数上限

# 指标质量 → 置信度权重（透明可调）
QUALITY_WEIGHT = {"ok": 1.0, "degraded": 0.6, "insufficient": 0.3,
                  "structural": 0.7}


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


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _round(value: Any, digits: int = 4) -> Any:
    if isinstance(value, float):
        return round(value, digits)
    return value


# ---------------------------------------------------------------------------
# 数据加载（全部 storage.read_connect() 只读）
# ---------------------------------------------------------------------------

def _load_market_index_daily(
    storage: Storage, days: int = 95,
) -> dict[str, float]:
    """大盘指数日频序列：day -> 日终值。

    market_index 的 ts 无时区后缀时按 UTC+8 解释（features.py 巡检结论，
    2026-09 之后的行为），带 +00:00 后缀的旧行直接按 UTC 解析。
    同日多条取最新（日终口径）。
    """
    now = _utcnow()
    with storage.read_connect() as conn:
        rows = conn.execute(
            """SELECT ts, close FROM series
               WHERE dataset='market_index' AND metric='market_index'
                 AND item_id IN ('', 'broad') AND ts >= ?
               ORDER BY ts""",
            (_iso(now - timedelta(days=days + 2)),),
        ).fetchall()
    daily: dict[str, float] = {}
    for r in rows:
        v = _f(r["close"])
        raw = str(r["ts"])
        if v is None or v <= 0:
            continue
        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
            day = dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
        except ValueError:
            continue
        daily[day] = v  # ORDER BY ts → 后写覆盖前写 = 日终值
    return daily


def _load_item_daily(
    storage: Storage,
) -> tuple[dict[str, dict[str, float | int]], dict[str, dict[str, float]],
           dict[str, dict[str, float]]]:
    """item_history 的 transaction_count / survive_num 聚合。

    返回 (day_agg, tx_by_item, survive_by_item)：
      day_agg[day] = {"tx_sum": 市场级日成交合计, "items": 当日有数饰品数}
      *_by_item[item][day] = 该饰品该日日终值（MAX，即日内聚合后取尾）
    单次扫描同时取两个 metric（同一索引区间，省一半 I/O）。
    """
    now = _utcnow()
    with storage.read_connect() as conn:
        rows = conn.execute(
            """SELECT item_id, substr(ts,1,10) AS day,
                      MAX(CASE WHEN metric='transaction_count' THEN close END) AS tx,
                      MAX(CASE WHEN metric='survive_num' THEN close END) AS survive
            FROM series
            WHERE dataset='item_history' AND source='siteb'
              AND metric IN ('transaction_count','survive_num')
              AND ts >= ?
            GROUP BY item_id, day
            LIMIT ?""",
            (_iso(now - timedelta(days=LOOKBACK_DAYS + 2)), MAX_GROUP_ROWS),
        ).fetchall()
    day_agg: dict[str, dict[str, float | int]] = {}
    tx_by_item: dict[str, dict[str, float]] = {}
    survive_by_item: dict[str, dict[str, float]] = {}
    for r in rows:
        item = str(r["item_id"])
        day = str(r["day"])
        tx, survive = _f(r["tx"]), _f(r["survive"])
        if tx is not None:
            tx_by_item.setdefault(item, {})[day] = tx
            agg = day_agg.setdefault(day, {"tx_sum": 0.0, "items": 0})
            agg["tx_sum"] = float(agg["tx_sum"]) + max(0.0, tx)
            agg["items"] = int(agg["items"]) + 1
        if survive is not None:
            survive_by_item.setdefault(item, {})[day] = survive
    if len(rows) >= MAX_GROUP_ROWS:
        log.warning("sentiment item_daily hit row guard %d", MAX_GROUP_ROWS)
    return day_agg, tx_by_item, survive_by_item


def _derive_breadth(
    tx_by_item: dict[str, dict[str, float]],
) -> dict[str, dict[str, float]]:
    """逐日重算 breadth_active（不依赖 indicator_daily 的预计算——
    库内 breadth_active 历史为空且其窗口逻辑受采集覆盖影响）。

    对相邻两日都有成交数据的饰品（活跃集）：
      breadth = (涨家数 - 跌家数) / 活跃家数
    与 skin_indicators V2 口径一致；flat 家数留在分母（稀释效应，
    冷漠期 breadth 天然趋 0，符合语义）。
    """
    days = sorted({d for series in tx_by_item.values() for d in series})
    out: dict[str, dict[str, float]] = {}
    for i in range(1, len(days)):
        prev_day, day = days[i - 1], days[i]
        up = down = active = 0
        for item, series in tx_by_item.items():
            a, b = series.get(prev_day), series.get(day)
            if a is None or b is None:
                continue
            if a <= 0 and b <= 0:
                continue  # 两日均无成交 → 非活跃品，不进分母
            active += 1
            if b > a:
                up += 1
            elif b < a:
                down += 1
        if active > 0:
            out[day] = {"breadth": (up - down) / active,
                        "active": float(active), "up": float(up),
                        "down": float(down)}
    return out


def _load_listing_daily(
    storage: Storage,
) -> dict[str, float]:
    """挂单流速：ws_new_listing 按日计数（上架量 = 供给端急售压力）。"""
    now = _utcnow()
    with storage.read_connect() as conn:
        rows = conn.execute(
            """SELECT substr(ts,1,10) AS day, COUNT(*) AS n
            FROM series WHERE dataset='ws_new_listing' AND ts >= ?
            GROUP BY day""",
            (_iso(now - timedelta(days=LOOKBACK_DAYS + 2)),),
        ).fetchall()
    return {str(r["day"]): float(r["n"] or 0) for r in rows}


def _load_spd_fb_daily_median(
    storage: Storage,
) -> dict[str, float]:
    """spd_fb（双侧净得口径 Steam-BUFF 价差）的市场级日中位数。

    基线取自 feature_store（features.py 日更，已有 ~40 天历史），
    比起 indicator_daily 的逐小时重写行（日均膨胀 35 万行）更省。
    """
    now = _utcnow()
    with storage.read_connect() as conn:
        rows = conn.execute(
            """SELECT substr(ts,1,10) AS day, value FROM feature_store
               WHERE freq='1d' AND name='spd_fb' AND ts >= ?""",
            ((now - timedelta(days=LOOKBACK_DAYS + 7)).strftime("%Y-%m-%d"),),
        ).fetchall()
    by_day: dict[str, list[float]] = {}
    for r in rows:
        v = _f(r["value"])
        if v is not None:
            by_day.setdefault(str(r["day"]), []).append(v)
    return {d: m for d, vals in by_day.items()
            if (m := _median(vals)) is not None}


def _load_spd_current(
    storage: Storage, window_hours: int = 48,
    min_buff_price: float = 20.0,
) -> dict[str, Any]:
    """当前跨平台价差（steam vs buff，最近 48h 快照的逐品挂价）。

    同时产出两个口径：
      spd_steam_buff = steam/buff 原始比价（均衡带 0.85-0.975）
      spd_fb         = 双侧净得价差（fees.py，恐慌溢价方向）
    市场级取逐品中位数。**必须限定 buff 价 ≥ ¥20**：低价段（<¥2）
    Steam 最小计价+费用地板主导，实测比价可达数百倍（库内极值 444x），
    会把全目录中位数推到均衡带外——均衡带结论只在中价段成立
    （fees.py FLAT_MULTIPLIER_THRESHOLD 同源口径）。
    """
    now = _utcnow()
    with storage.read_connect() as conn:
        rows = conn.execute(
            """SELECT item_id,
                      MAX(CASE WHEN platform='steam' THEN close END) AS steam_p,
                      MAX(CASE WHEN platform='buff' THEN close END) AS buff_p
            FROM series
            WHERE dataset='platform_snapshot' AND source='siteb'
              AND metric='sell_price' AND platform IN ('steam','buff')
              AND ts >= ?
            GROUP BY item_id
            HAVING steam_p IS NOT NULL AND buff_p IS NOT NULL
                   AND steam_p > 0 AND buff_p >= ?
            LIMIT ?""",
            (_iso(now - timedelta(hours=window_hours)), min_buff_price,
             MAX_GROUP_ROWS),
        ).fetchall()
    ratios, fb_vals = [], []
    for r in rows:
        steam_p, buff_p = _f(r["steam_p"]), _f(r["buff_p"])
        if not steam_p or not buff_p:
            continue
        ratios.append(steam_p / buff_p)
        fb = _spd_fb_fn(steam_p, buff_p)
        if fb is not None:
            fb_vals.append(fb)
    return {
        "spd_steam_buff": _median(ratios),
        "spd_fb": _median(fb_vals),
        "n_items": float(len(ratios)),
        "window_hours": float(window_hours),
        "min_buff_price": float(min_buff_price),
    }


def _survive_paired_growth(
    survive_by_item: dict[str, dict[str, float]],
    last_day: str, lag_days: int = 7,
) -> tuple[float | None, int]:
    """存世量配对环比：只统计 last_day 与 lag 天前都有数据的饰品。

    配对口径对采集覆盖集变化稳健（覆盖坍缩时普通 Σ 会假性下跌）。
    返回 (环比, 配对饰品数)；配对数不足时返回 (None, n)。
    """
    from datetime import datetime as _dt
    try:
        d_last = _dt.strptime(last_day, "%Y-%m-%d")
        d_prev = d_last - timedelta(days=lag_days)
    except ValueError:
        return None, 0
    prev_day = d_prev.strftime("%Y-%m-%d")
    n = 0
    sum_now = sum_prev = 0.0
    for item, series in survive_by_item.items():
        a, b = series.get(prev_day), series.get(last_day)
        if a is None or b is None or a <= 0 or b <= 0:
            continue
        n += 1
        sum_prev += a
        sum_now += b
    if n < MIN_PAIR_ITEMS or sum_prev <= 0:
        return None, n
    return sum_now / sum_prev - 1.0, n


def _load_prev_stage(
    storage: Storage,
) -> list[dict[str, Any]]:
    """回读本数据集的阶段历史（算阶段持续天数与转移背景）。"""
    with storage.read_connect() as conn:
        rows = conn.execute(
            """SELECT ts, close, extra_json FROM series
               WHERE dataset=? AND metric='stage' AND source='siteb'
               ORDER BY ts DESC LIMIT ?""",
            (DATASET, MAX_STAGE_HISTORY),
        ).fetchall()
    out = []
    for r in rows:
        try:
            extra = json.loads(r["extra_json"] or "{}")
        except (ValueError, TypeError):
            extra = {}
        out.append({"ts": str(r["ts"]), "stage": str(extra.get("stage") or ""),
                    "confidence": _f(extra.get("confidence"))})
    return out


# ---------------------------------------------------------------------------
# 证据构建（每个指标：当前值 / 基线 / 比值 / 阈值 / 是否满足 / 质量）
# ---------------------------------------------------------------------------

def _baseline_median(
    series: dict[str, float], end_day: str, exclude_recent: int,
) -> tuple[float | None, int]:
    """end_day 之前 exclude_recent 天开始再往前 BASELINE_DAYS 天的中位数。

    返回 (中位数, 实际样本天数)。样本 < MIN_BASELINE_DAYS → (None, n)。
    """
    from datetime import datetime as _dt
    try:
        d0 = _dt.strptime(end_day, "%Y-%m-%d") - timedelta(days=exclude_recent)
    except ValueError:
        return None, 0
    window = [v for day, v in series.items() if day < d0.strftime("%Y-%m-%d")]
    window = sorted(window)[-BASELINE_DAYS:]
    if len(window) < MIN_BASELINE_DAYS:
        return None, len(window)
    return _median(window), len(window)


def _recent_mean(
    series: dict[str, float], end_day: str, days: int = RECENT_DAYS,
) -> tuple[float | None, int]:
    """end_day（含）往回 days 天的均值。"""
    from datetime import datetime as _dt
    try:
        d_end = _dt.strptime(end_day, "%Y-%m-%d")
    except ValueError:
        return None, 0
    keys = {(d_end - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(days)}
    vals = [series[k] for k in keys if k in series]
    if not vals:
        return None, 0
    return fmean(vals), len(vals)


def _cond(name: str, current: Any, threshold: str, met: bool | None,
          note: str = "") -> dict[str, Any]:
    return {"name": name, "current": _round(current), "threshold": threshold,
            "met": met, "note": note}


def build_evidence(storage: Storage) -> dict[str, Any]:
    """汇聚全部指标证据。任何单源失败不致命（质量标记 insufficient）。"""
    ev: dict[str, Any] = {"errors": []}

    # 1) 成交量 + 存世量（同一扫描）
    day_agg: dict[str, dict[str, float | int]] = {}
    tx_by_item: dict[str, dict[str, float]] = {}
    survive_by_item: dict[str, dict[str, float]] = {}
    try:
        day_agg, tx_by_item, survive_by_item = _load_item_daily(storage)
    except Exception as exc:  # noqa: BLE001
        ev["errors"].append(f"item_daily: {type(exc).__name__}")
        log.warning("sentiment item_daily failed: %s", exc)

    tx_days = sorted(day_agg)
    eval_day = tx_days[-1] if tx_days else ""
    tx_series = {d: float(day_agg[d]["tx_sum"]) for d in tx_days}
    items_series = {d: float(day_agg[d]["items"]) for d in tx_days}

    tx_cur = tx_base = tx_ratio = None
    tx_quality, coverage_ratio = "insufficient", None
    tx_days_base = 0
    if eval_day:
        tx_cur, _ = _recent_mean(tx_series, eval_day)
        tx_base, tx_days_base = _baseline_median(
            tx_series, eval_day, exclude_recent=RECENT_DAYS)
        # 覆盖比：评估日饰品数 / 基线期饰品数中位（防假"冷漠"的关键护栏）
        items_base, _ = _baseline_median(
            items_series, eval_day, exclude_recent=RECENT_DAYS)
        items_cur, _ = _recent_mean(items_series, eval_day)
        if items_base and items_cur is not None:
            coverage_ratio = items_cur / items_base
        if tx_cur is not None and tx_base is not None and tx_base > 0:
            tx_ratio = tx_cur / tx_base
            if coverage_ratio is None or coverage_ratio >= COVERAGE_OK:
                tx_quality = "ok"
            elif coverage_ratio >= COVERAGE_FLOOR:
                tx_quality = "degraded"
            else:
                # 覆盖坍缩：成交量证据不可判（坍缩的采集 ≠ 坍缩的市场）
                tx_quality = "insufficient"
                tx_ratio = None
        if tx_quality == "insufficient" and coverage_ratio is not None \
                and coverage_ratio < COVERAGE_FLOOR:
            ev["errors"].append(
                f"tx_coverage_collapse:{coverage_ratio:.2f}")
    ev["tx"] = {
        "current": tx_cur, "baseline_median": tx_base, "ratio": tx_ratio,
        "quality": tx_quality, "coverage_ratio": coverage_ratio,
        "eval_day": eval_day, "baseline_days": tx_days_base,
        "eval_basis": f"近{RECENT_DAYS}日均/前{BASELINE_DAYS}日中位"
                      "(市场级日成交合计)",
    }

    # 2) 活跃宽度（逐日重算，同源同覆盖标记）
    breadth_series: dict[str, float] = {}
    try:
        breadth_series = {d: v["breadth"]
                          for d, v in _derive_breadth(tx_by_item).items()}
    except Exception as exc:  # noqa: BLE001
        ev["errors"].append(f"breadth: {type(exc).__name__}")
        log.warning("sentiment breadth failed: %s", exc)
    breadth_cur = breadth_series.get(eval_day) if eval_day else None
    breadth_base, _ = (_baseline_median(breadth_series, eval_day, 0)
                       if eval_day else (None, 0))
    # 宽度与成交量同源同覆盖：直接继承成交量的覆盖质量标记
    breadth_quality = "ok" if breadth_cur is not None and tx_quality == "ok" \
        else ("insufficient" if breadth_cur is None else tx_quality)
    ev["breadth"] = {
        "current": breadth_cur, "baseline_median": breadth_base,
        "quality": breadth_quality, "eval_day": eval_day,
        "eval_basis": "活跃品中(涨-跌)/活跃，相邻日成交对",
    }

    # 3) 大盘指数（90 日高点 / 单日跌幅 / 30 日回报）
    index_daily: dict[str, float] = {}
    try:
        index_daily = _load_market_index_daily(storage, days=95)
    except Exception as exc:  # noqa: BLE001
        ev["errors"].append(f"market_index: {type(exc).__name__}")
        log.warning("sentiment market_index failed: %s", exc)
    idx_days = sorted(index_daily)
    idx_last = idx_ret30 = idx_drop1d = idx_from_high = None
    idx_high_window = 0
    idx_quality = "insufficient"
    if len(idx_days) >= 2:
        idx_last = index_daily[idx_days[-1]]
        prev = index_daily[idx_days[-2]]
        if prev > 0:
            idx_drop1d = idx_last / prev - 1.0
        # 30 日回报：允许 25-32 天容差（缺数据日）
        from datetime import datetime as _dt
        d_end = _dt.strptime(idx_days[-1], "%Y-%m-%d")
        for back in range(25, 33):
            key = (d_end - timedelta(days=back)).strftime("%Y-%m-%d")
            if key in index_daily and index_daily[key] > 0:
                idx_ret30 = idx_last / index_daily[key] - 1.0
                break
        # 90 日高点（窗口不足 90 天时按实际天数，透明标注）
        window = [index_daily[d] for d in idx_days[-90:]]
        idx_high_window = len(window)
        high = max(window) if window else None
        if high and high > 0:
            idx_from_high = idx_last / high - 1.0
        idx_quality = "ok" if idx_high_window >= 60 else "degraded"
    ev["index"] = {
        "last": idx_last, "drop_1d": idx_drop1d, "ret_30d": idx_ret30,
        "pct_from_high_90d": idx_from_high, "high_window_days": idx_high_window,
        "quality": idx_quality,
        "eval_basis": "日终值；无时区 ts 按 UTC+8 归一",
    }

    # 4) 挂单流速（ws_new_listing 仅有 ~3 天历史 → 基线不足时不可判）
    listing_series: dict[str, float] = {}
    try:
        listing_series = _load_listing_daily(storage)
    except Exception as exc:  # noqa: BLE001
        ev["errors"].append(f"listing: {type(exc).__name__}")
        log.warning("sentiment listing failed: %s", exc)
    listing_cur = listing_base = listing_ratio = None
    listing_days_base = 0
    if listing_series:
        l_days = sorted(listing_series)
        listing_cur, _ = _recent_mean(listing_series, l_days[-1])
        listing_base, listing_days_base = _baseline_median(
            listing_series, l_days[-1], exclude_recent=RECENT_DAYS)
        if listing_cur is not None and listing_base and listing_base > 0:
            listing_ratio = listing_cur / listing_base
    listing_quality = ("ok" if listing_ratio is not None and listing_days_base >= MIN_BASELINE_DAYS
                       else "insufficient")
    ev["listing"] = {
        "current": listing_cur, "baseline_median": listing_base,
        "ratio": listing_ratio, "quality": listing_quality,
        "baseline_days": listing_days_base,
        "eval_basis": f"近{RECENT_DAYS}日均/前{BASELINE_DAYS}日中位"
                      "(日上架计数)",
    }

    # 5) 跨平台价差：spd_fb 基线（feature_store）+ 当前快照（48h）
    spd_fb_daily: dict[str, float] = {}
    try:
        spd_fb_daily = _load_spd_fb_daily_median(storage)
    except Exception as exc:  # noqa: BLE001
        ev["errors"].append(f"spd_fb_daily: {type(exc).__name__}")
        log.warning("sentiment spd_fb_daily failed: %s", exc)
    spd_fb_base = spd_fb_ratio = None
    spd_fb_days_base = 0
    spd_fb_cur = None
    spd_fb_source = "none"
    if spd_fb_daily:
        s_days = sorted(spd_fb_daily)
        spd_fb_base, spd_fb_days_base = _baseline_median(
            spd_fb_daily, s_days[-1], exclude_recent=0)
        # 当前值优先取 feature_store 最新日（与基线同一饰品集，口径可比）；
        # 仅当其滞后 >2 天才回退到 48h 快照（快照是全目录中价段，口径不同，
        # 降质标记 degraded）。
        spd_fb_cur = spd_fb_daily[s_days[-1]]
        spd_fb_source = f"feature_store@{s_days[-1]}"
        try:
            d_latest = datetime.strptime(s_days[-1], "%Y-%m-%d")
            age = (_utcnow() - d_latest.replace(tzinfo=timezone.utc)).days
        except ValueError:
            age = 99
        if age > 2:
            spd_fb_cur, spd_fb_source = None, "stale_feature_store"
    spd_cur: dict[str, Any] = {"spd_steam_buff": None, "spd_fb": None,
                               "n_items": 0.0, "window_hours": 48.0}
    try:
        spd_cur = _load_spd_current(storage)
    except Exception as exc:  # noqa: BLE001
        ev["errors"].append(f"spd_current: {type(exc).__name__}")
        log.warning("sentiment spd_current failed: %s", exc)
    if spd_fb_cur is None and spd_cur.get("spd_fb") is not None:
        spd_fb_cur = spd_cur.get("spd_fb")
        spd_fb_source = f"platform_snapshot_{spd_cur.get('window_hours')}h"
    if spd_fb_cur is not None and spd_fb_base is not None:
        # spd_fb 中位可能近 0，×2 阈值在近零基线下无意义 → 用幅度差防护
        if abs(spd_fb_base) >= 0.005:
            spd_fb_ratio = spd_fb_cur / spd_fb_base
        elif spd_fb_cur > abs(spd_fb_base) + 0.02:
            spd_fb_ratio = 2.0  # 基线近零但当前显著放大（≥2 个百分点）
    spd_fb_quality = "insufficient"
    if spd_fb_ratio is not None and spd_fb_days_base >= MIN_BASELINE_DAYS:
        spd_fb_quality = ("ok" if spd_fb_source.startswith("feature_store")
                          else "degraded")
    ratio_cur = spd_cur.get("spd_steam_buff")
    band_note = ""
    if ratio_cur is not None:
        band_note = (f"均衡带{SPD_RATIO_BAND[0]}-{SPD_RATIO_BAND[1]}"
                     + ("（带内）" if SPD_RATIO_BAND[0] <= ratio_cur
                        <= SPD_RATIO_BAND[1] else "（带外=结构断点）"))
    ev["spd_fb"] = {
        "current": spd_fb_cur, "baseline_median": spd_fb_base,
        "ratio": spd_fb_ratio, "quality": spd_fb_quality,
        "baseline_days": spd_fb_days_base, "source": spd_fb_source,
        "eval_basis": "市场级日中位(feature_store)/前30日中位",
    }
    ev["spd_steam_buff"] = {
        "current": ratio_cur, "quality": "structural",
        "n_items": spd_cur.get("n_items"),
        "note": band_note or "快照不足",
        "eval_basis": (f"最近{spd_cur.get('window_hours')}h 逐品中位"
                       f"（buff≥¥{spd_cur.get('min_buff_price', 20):.0f}中价段）"),
    }

    # 6) 存世量配对环比（出清阶段判据："survive_num 回升"）
    survive_growth, survive_pairs = (None, 0)
    if eval_day:
        try:
            survive_growth, survive_pairs = _survive_paired_growth(
                survive_by_item, eval_day, lag_days=7)
        except Exception as exc:  # noqa: BLE001
            ev["errors"].append(f"survive: {type(exc).__name__}")
            log.warning("sentiment survive failed: %s", exc)
    ev["survive"] = {
        "paired_growth_7d": survive_growth, "pairs": survive_pairs,
        "quality": ("ok" if survive_growth is not None else "insufficient"),
        "eval_basis": f"评估日 vs 7日前，仅共有的{survive_pairs}饰品配对",
    }

    # 7) 萌芽的连续回升天数（成交 > 前 30 日中位，从评估日回数）
    sprout_streak = 0
    sprout_met = None
    try:
        base_prior, base_n = _baseline_median(
            tx_series, eval_day, exclude_recent=TH_SPROUT_DAYS)
        if base_prior and base_prior > 0 and base_n >= MIN_BASELINE_DAYS:
            from datetime import datetime as _dt
            d0 = _dt.strptime(eval_day, "%Y-%m-%d")
            streak_days = []
            for i in range(TH_SPROUT_DAYS):
                key = (d0 - timedelta(days=i)).strftime("%Y-%m-%d")
                if key in tx_series:
                    streak_days.append(tx_series[key])
            sprout_streak = 0
            for v in streak_days:  # 从最近往前数连续满足天数
                if v > base_prior:
                    sprout_streak += 1
                else:
                    break
            # 语义：窗口内全部 7 天都高于基线（缺口天视为不满足）
            sprout_met = (len(streak_days) >= TH_SPROUT_DAYS
                          and sprout_streak >= TH_SPROUT_DAYS)
        ev["sprout_streak"] = {
            "current": float(sprout_streak),
            "threshold": f"连续{TH_SPROUT_DAYS}天>前{BASELINE_DAYS}日中位",
            "met": sprout_met,
            "baseline_days": base_n if base_prior is not None else 0,
        }
    except Exception as exc:  # noqa: BLE001
        ev["errors"].append(f"sprout: {type(exc).__name__}")
        log.warning("sentiment sprout failed: %s", exc)
        ev["sprout_streak"] = {"current": 0.0, "met": None}

    ev["eval_day"] = eval_day
    return ev


# ---------------------------------------------------------------------------
# 阶段规则（architecture-v3-sentiment.md §三 初始阈值）
# ---------------------------------------------------------------------------

def _tx_usable(ev: dict[str, Any]) -> bool:
    return ev["tx"]["ratio"] is not None


def evaluate_stages(ev: dict[str, Any]) -> list[dict[str, Any]]:
    """按五阶段规则逐条判定。返回带完整依据的报告列表（可审计）。"""
    tx_ratio = ev["tx"]["ratio"]
    breadth = ev["breadth"]["current"]
    idx = ev["index"]
    reports: list[dict[str, Any]] = []

    def usable(v: Any) -> bool:
        return v is not None

    # -- 恐慌：大盘单日跌>5% ∨ (挂单>中位×3 ∧ spd_fb>中位×2) ----------
    conds_panic = [
        _cond("大盘单日跌幅", idx["drop_1d"],
              f"跌幅>{TH_INDEX_DROP_PANIC:.0%}",
              (idx["drop_1d"] < -TH_INDEX_DROP_PANIC
               if usable(idx["drop_1d"]) else None)),
    ]
    leg_price = any(c["met"] is True for c in conds_panic)
    conds_panic2 = [
        _cond("挂单流速vs中位", ev["listing"]["ratio"],
              f">{TH_LISTING_PANIC}×",
              (ev["listing"]["ratio"] > TH_LISTING_PANIC
               if usable(ev["listing"]["ratio"]) else None)),
        _cond("spd_fb vs 中位", ev["spd_fb"]["ratio"],
              f">{TH_SPD_PANIC}×",
              (ev["spd_fb"]["ratio"] > TH_SPD_PANIC
               if usable(ev["spd_fb"]["ratio"]) else None)),
    ]
    leg_flow = all(c["met"] is True for c in conds_panic2)
    reports.append({
        "stage": "恐慌", "matched": bool(leg_price or leg_flow),
        "matched_leg": "单日暴跌" if leg_price else
                       ("量价恐慌" if leg_flow else ""),
        "conditions": conds_panic + conds_panic2,
    })

    # -- 狂热：宽度>0.7 ∧ 成交>中位×2 ∧ 大盘距90日高点5%内 -------------
    conds = [
        _cond("breadth_active", breadth, f">{TH_BREADTH_FRENZY}",
              (breadth > TH_BREADTH_FRENZY if usable(breadth) else None)),
        _cond("成交vs中位", tx_ratio, f">{TH_TX_FRENZY}×",
              (tx_ratio > TH_TX_FRENZY if usable(tx_ratio) else None)),
        _cond("大盘距90日高点", idx["pct_from_high_90d"],
              f">={-TH_INDEX_NEAR_HIGH:.0%}",
              (idx["pct_from_high_90d"] >= -TH_INDEX_NEAR_HIGH
               if usable(idx["pct_from_high_90d"]) else None)),
    ]
    evaluable = [c for c in conds if c["met"] is not None]
    reports.append({
        "stage": "狂热",
        "matched": bool(evaluable) and len(evaluable) == len(conds)
        and all(c["met"] for c in evaluable),
        "matched_leg": "三条件共振",
        "conditions": conds,
    })

    # -- 出清：ret_30d<-10% ∧ 成交<中位×0.5 ∧ 存世量回升 ----------------
    conds = [
        _cond("大盘ret_30d", idx["ret_30d"], f"<{TH_RET30_CLEAR:.0%}",
              (idx["ret_30d"] < TH_RET30_CLEAR
               if usable(idx["ret_30d"]) else None)),
        _cond("成交vs中位", tx_ratio, f"<{TH_TX_CLEAR}×",
              (tx_ratio < TH_TX_CLEAR if usable(tx_ratio) else None)),
        _cond("存世量配对7d环比", ev["survive"]["paired_growth_7d"],
              ">0（回升）",
              (ev["survive"]["paired_growth_7d"] > 0
               if usable(ev["survive"]["paired_growth_7d"]) else None)),
    ]
    evaluable = [c for c in conds if c["met"] is not None]
    reports.append({
        "stage": "出清",
        "matched": bool(evaluable) and len(evaluable) == len(conds)
        and all(c["met"] for c in evaluable),
        "matched_leg": "阴跌+缩量+供给回升",
        "conditions": conds,
    })

    # -- 冷漠：成交<中位×0.3 ∧ 宽度<0.3 ----------------------------------
    conds = [
        _cond("成交vs中位", tx_ratio, f"<{TH_TX_APATHY}×",
              (tx_ratio < TH_TX_APATHY if usable(tx_ratio) else None)),
        _cond("breadth_active", breadth, f"<{TH_BREADTH_APATHY}",
              (breadth < TH_BREADTH_APATHY if usable(breadth) else None)),
    ]
    evaluable = [c for c in conds if c["met"] is not None]
    reports.append({
        "stage": "冷漠",
        "matched": bool(evaluable) and len(evaluable) == len(conds)
        and all(c["met"] for c in evaluable),
        "matched_leg": "缩量+宽度塌陷",
        "conditions": conds,
    })

    # -- 萌芽：连续7天成交>前30日中位 ∧ 宽度0.3-0.5 ----------------------
    sprout = ev.get("sprout_streak", {})
    conds = [
        _cond("成交连续回升天数", sprout.get("current"),
              f">={TH_SPROUT_DAYS}天>前{BASELINE_DAYS}日中位",
              sprout.get("met")),
        _cond("breadth_active", breadth,
              f"{TH_BREADTH_SPROUT_LO}-{TH_BREADTH_SPROUT_HI}",
              (TH_BREADTH_SPROUT_LO <= breadth <= TH_BREADTH_SPROUT_HI
               if usable(breadth) else None)),
    ]
    evaluable = [c for c in conds if c["met"] is not None]
    reports.append({
        "stage": "萌芽",
        "matched": bool(evaluable) and len(evaluable) == len(conds)
        and all(c["met"] for c in evaluable),
        "matched_leg": "谷底回升+宽度温和修复",
        "conditions": conds,
    })
    return reports


def _score_report(report: dict[str, Any]) -> tuple[float, int]:
    evaluable = [c for c in report["conditions"] if c["met"] is not None]
    if not evaluable:
        return 0.0, 0
    met = sum(1 for c in evaluable if c["met"])
    return met / len(evaluable), len(evaluable)


def _quality_factor(ev: dict[str, Any], report: dict[str, Any]) -> float:
    """命中的规则所用指标的质量均值（降置信而不掩蔽）。"""
    mapping = {
        "大盘单日跌幅": "index", "大盘ret_30d": "index",
        "大盘距90日高点": "index",
        "成交vs中位": "tx", "成交连续回升天数": "tx",
        "breadth_active": "breadth",
        "挂单流速vs中位": "listing",
        "spd_fb vs 中位": "spd_fb",
        "存世量配对7d环比": "survive",
    }
    weights = []
    for c in report["conditions"]:
        key = mapping.get(c["name"])
        if key and key in ev:
            weights.append(QUALITY_WEIGHT.get(ev[key]["quality"], 0.3))
    return fmean(weights) if weights else 0.3


def _transition_hints(
    stage: str, ev: dict[str, Any], stage_age_days: float,
    matched: bool, best_partial: str | None,
) -> list[str]:
    """转移提示：当前阶段的下一转移条件 + 当前值（架构 §三转移规则）。"""
    hints: list[str] = []
    idx, tx, breadth = ev["index"], ev["tx"], ev["breadth"]
    nxt = NEXT_STAGE.get(stage, "")
    if nxt == "恐慌":
        drop = idx.get("drop_1d")
        hints.append(
            f"→恐慌：大盘单日跌幅>5%（最新 {_fmt_pct(drop)}）；"
            "或 Valve 政策突变（L3 事件系统待建）")
    elif nxt == "出清":
        listing = ev["listing"]
        peak_note = "挂单历史不足，无法算峰值回落" \
            if listing.get("quality") == "insufficient" else "挂单峰值回落50%待观测"
        hints.append(
            f"→出清：恐慌持续>7天（当前阶段已 {_fmt_days(stage_age_days)}）"
            f"且 listing_rate 从峰值回落50%（{peak_note}）")
    elif nxt == "冷漠":
        hints.append(
            f"→冷漠：ret_30d 收窄至±3%（当前 {_fmt_pct(idx.get('ret_30d'))}）"
            f"且成交稳定低位（当前 {_fmt_x(tx.get('ratio'))}×中位）")
    elif nxt == "萌芽":
        streak = ev.get("sprout_streak", {}).get("current")
        hints.append(
            f"→萌芽：成交连续7天>前30日中位"
            f"（当前连续 {streak or 0} 天，需≥{TH_SPROUT_DAYS}）")
    elif nxt == "狂热":
        hints.append(
            f"→狂热：breadth_active>0.7（当前 {_fmt_num(breadth.get('current'))}）"
            "且 KOL bullish>60%（待建）且搜索量同比>100%（P0 待建）")
    if not matched:
        hints.append(
            f"无阶段完全匹配（部分得分最高：{best_partial or '无'}）——"
            "置信度封顶 0.5，阈值待 Phase 3 回测校准")
    for err in ev.get("errors", []):
        if err.startswith("tx_coverage_collapse"):
            hints.append(
                f"数据警示：成交采集覆盖坍缩（覆盖比 {err.split(':')[-1]}），"
                "成交量类证据已停用——修复采集前勿据本判定调仓")
    if ev["listing"].get("quality") == "insufficient":
        hints.append("数据警示：ws_new_listing 历史不足 10 日，挂单流速证据不可判")
    if ev["spd_fb"].get("quality") == "insufficient":
        hints.append("数据警示：spd_fb 基线不足，恐慌溢价证据不可判")
    return hints


def _fmt_pct(v: Any) -> str:
    return f"{v * 100:+.2f}%" if isinstance(v, (int, float)) else "缺数据"


def _fmt_x(v: Any) -> str:
    return f"{v:.2f}" if isinstance(v, (int, float)) else "缺数据"


def _fmt_num(v: Any) -> str:
    return f"{v:.3f}" if isinstance(v, (int, float)) else "缺数据"


def _fmt_days(v: Any) -> str:
    return f"{v:.1f} 天" if isinstance(v, (int, float)) else "未知"


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------

def run_sentiment_cycle(storage: Storage) -> dict[str, Any]:
    """情绪周期判定一轮：读 → 判 → 写 series → 返回 supervisor 兼容结果。"""
    now = _utcnow()
    try:
        ev = build_evidence(storage)
        reports = evaluate_stages(ev)
    except Exception as exc:  # noqa: BLE001
        # 理论上 build_evidence 内部已逐源保护，这里兜底（宁可失败可见）
        log.exception("sentiment cycle failed")
        return {
            "status": "failed", "error_class": "data",
            "stage": "未知", "confidence": 0.0,
            "indicators": {}, "transition_hints": [
                f"判定失败：{type(exc).__name__}"],
            "metrics": {"stage": "未知", "confidence": 0.0},
            "details": {"error": str(exc)},
        }

    # 裁决：完全命中按优先级取急性状态；否则取部分得分最高（置信封顶）
    matched_reports = [r for r in reports if r["matched"]]
    prev_stage_rows = []
    try:
        prev_stage_rows = _load_prev_stage(storage)
    except Exception as exc:  # noqa: BLE001
        log.warning("sentiment prev_stage read failed: %s", exc)

    if matched_reports:
        by_priority = sorted(
            matched_reports,
            key=lambda r: PRIORITY.index(r["stage"]) if r["stage"] in PRIORITY else 99)
        chosen = by_priority[0]
        stage = chosen["stage"]
        confidence = round(0.6 + 0.4 * _quality_factor(ev, chosen), 3)
        matched = True
    else:
        scored = []
        for r in reports:
            score, n = _score_report(r)
            if n > 0:
                scored.append((score, n, r))
        if scored:
            scored.sort(key=lambda t: (-t[0], -t[1]))
            score, _, chosen = scored[0]
            stage = chosen["stage"]
            confidence = round(
                min(0.5, 0.55 * score * _quality_factor(ev, chosen)), 3)
        else:
            stage, chosen, confidence = "冷漠", None, 0.0
        matched = False

    # 阶段持续天数（与上一条记录同阶段则累计，否则重置为 0）
    stage_age_days = 0.0
    prev_stage = prev_stage_rows[0]["stage"] if prev_stage_rows else ""
    for row in prev_stage_rows:
        if row["stage"] != stage:
            break
        try:
            ts = datetime.fromisoformat(
                row["ts"].replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            stage_age_days = max(
                stage_age_days,
                (now - ts).total_seconds() / 86400.0)
        except ValueError:
            continue

    hints = _transition_hints(
        stage, ev, stage_age_days, matched,
        best_partial=(chosen["stage"] if chosen else None))

    indicators = {
        "transaction_count": {k: _round(v) for k, v in ev["tx"].items()},
        "breadth_active": {k: _round(v) for k, v in ev["breadth"].items()},
        "market_index": {k: _round(v) for k, v in ev["index"].items()},
        "listing_rate": {k: _round(v) for k, v in ev["listing"].items()},
        "spd_fb": {k: _round(v) for k, v in ev["spd_fb"].items()},
        "spd_steam_buff": {k: _round(v) for k, v in ev["spd_steam_buff"].items()},
        "survive_num": {k: _round(v) for k, v in ev["survive"].items()},
    }

    result = {
        "stage": stage,
        "confidence": confidence,
        "indicators": indicators,
        "transition_hints": hints,
    }

    # -- 写 series（供 Web 消费；upsert 幂等，分块提交）------------------
    rows_written = 0
    try:
        base = {"source": "siteb", "dataset": DATASET,
                "item_id": "", "platform": "", "ts": _iso(now)}
        rows = [{
            **base, "metric": "stage",
            "close": float(STAGE_INDEX.get(stage, -1)),
            "extra": {
                "stage": stage, "confidence": confidence,
                "matched": matched, "eval_day": ev.get("eval_day", ""),
                "prev_stage": prev_stage, "stage_age_days":
                    round(stage_age_days, 2),
                "rule": chosen if chosen else "无完全命中",
                "quality_note": {
                    k: ev[k]["quality"] for k in
                    ("tx", "breadth", "index", "listing", "spd_fb", "survive")
                    if k in ev},
                "errors": ev.get("errors", []),
            },
        }, {
            **base, "metric": "confidence", "close": confidence,
        }]
        metric_map = {
            "transaction_count": "tx_volume",
            "breadth_active": "breadth_active",
            "market_index": "market_index",
            "listing_rate": "listing_rate",
            "spd_fb": "spd_fb",
            "spd_steam_buff": "spd_steam_buff",
            "survive_num": "survive_growth_7d",
        }
        for key, metric in metric_map.items():
            record = dict(indicators[key])
            current = record.pop(
                "current", record.pop("paired_growth_7d",
                                      record.pop("last", None)))
            rows.append({**base, "metric": f"ind_{metric}",
                         "close": current, "extra": record})
        rows_written = storage.upsert_series(rows)
    except Exception as exc:  # noqa: BLE001
        log.exception("sentiment cycle write failed")

    log.info(
        "sentiment cycle: stage=%s confidence=%.2f matched=%s "
        "eval_day=%s rows=%d",
        stage, confidence, matched, ev.get("eval_day", ""), rows_written)

    return {
        "status": "ok",
        "error_class": "",
        **result,
        "metrics": {
            "stage": stage,
            "confidence": confidence,
            "matched": matched,
            "rows_written": rows_written,
            "eval_day": ev.get("eval_day", ""),
        },
        "details": {
            "indicators": indicators,
            "rules": reports,
            "transition_hints": hints,
            "stage_age_days": round(stage_age_days, 2),
            "prev_stage": prev_stage,
            "data_errors": ev.get("errors", []),
        },
    }
