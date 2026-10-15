# -*- coding: utf-8 -*-
"""特征引擎（最终架构 §2：L1 基础特征 20 个 + L2 组合特征 8 个）。

设计纪律（docs/market/final-architecture.md）：
- 一切价格类计算先过双侧净得口径（fees.py，红队#9 修正）；
- 零成交纪律：零成交日的价格特征保留缺失、不 ffill；缺失率本身
  作特征（miss_rate_7d）；
- 时间窗按日历对齐，z 化一律 trailing 窗 + robust（median/MAD）；
- 特征写入 feature_store（与 series 原始观测分离，版本化保 PIT）；
- L2 权重不在含标签样本上定（等权起步）。

L1（每饰品日频，freq='1d'）：
  动量: ret_7d ret_30d rev_1w4w dd_30d lvl_mig_7d(低价段)
  波动: vol_30d range_7d tail_ratio
  流动性: amihud_7d turn_7d val_turn_7d
  供需: dsr dsr_z90 supply_growth_7d obi_d
  跨平台: spd_fb xpl_corr_30d
  大盘: beta_idx_30d idio_ret_7d
  质量: miss_rate_7d
L2（组合）：
  liq_contraction absorption distribution zero_vol_rise
  rel_strength crash_pressure bottom_seq ano_score

运行：python -m csgo_monitor features [--max-items 500]

架构 v3 降级说明（2026-10-13）：
- 本模块的原"预测信号"定位已被架构 v3 降级为"L4 执行工具"层
- 有效保留：amihud_7d/spd_fb/turn_7d/miss_rate_7d（执行辅助）
- 已验证无效并归档：ret_7d/ret_30d/rev_1w4w/vol_30d/tail_ratio（动量/反转族）
- L2 组合特征冻结：absorption/distribution/zero_vol_rise/liq_contraction 等
  依赖无效 L1 成分的组合不再作为信号（代码保留供复验）
- 详细论证见 docs/market/architecture-v3-sentiment.md
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .fees import spd_fb
from .storage import Storage

log = logging.getLogger(__name__)

FREQ = "1d"
LOW_PRICE_TICK_THRESHOLD = 20.0   # 低于此价的档位迁移专用段（架构 §1）
MIN_DAYS_FOR_FEATURES = 8


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _f(value: Any) -> float | None:
    try:
        out = float(value)
        return out if out == out else None
    except (TypeError, ValueError):
        return None


def median(xs: list[float]) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def mad(xs: list[float], med: float | None = None) -> float | None:
    """中位绝对偏差（robust z 的尺度，架构反模式#6：禁固定阈值）。"""
    if not xs:
        return None
    m = med if med is not None else median(xs)
    assert m is not None
    return median([abs(x - m) for x in xs])


def robust_z(x: float, trail: list[float]) -> float | None:
    """trailing 窗 robust z = (x - median) / (1.4826 * MAD)。"""
    if not trail:
        return None
    med = median(trail)
    if med is None:
        return None
    m = mad(trail, med)
    if not m or m <= 0:
        return None
    return (x - med) / (1.4826 * m)


def pct_rank(x: float, trail: list[float]) -> float | None:
    """trailing 窗分位（0-1）。"""
    if not trail:
        return None
    below = sum(1 for v in trail if v <= x)
    return below / len(trail)


# ---------------------------------------------------------------------------
# L1 纯函数（可单测）
# ---------------------------------------------------------------------------

def f_ret(px: list[float], window: int) -> float | None:
    if len(px) < window + 1 or not px[-window - 1]:
        return None
    return px[-1] / px[-window - 1] - 1.0


def f_drawdown(px: list[float], window: int = 30) -> float | None:
    seg = px[-window:]
    if not seg:
        return None
    peak = max(seg)
    if not peak or peak <= 0:
        return None
    return 1.0 - seg[-1] / peak


def f_vol_mad(rets: list[float], window: int = 30) -> float | None:
    """年化波动（MAD 口径，1.4826·MAD·√365，架构 §2 L1）。"""
    seg = rets[-window:]
    if len(seg) < 7:
        return None
    m = mad(seg)
    if m is None or m <= 0:
        return None
    return 1.4826 * m * math.sqrt(365)


def f_tail_ratio(rets: list[float], window: int = 30) -> float | None:
    seg = sorted(rets[-window:])
    if len(seg) < 10:
        return None
    n = len(seg)
    p10, p50, p90 = seg[n // 10], seg[n // 2], seg[(9 * n) // 10]
    if p50 is None or p50 == p10 or p50 == p90:
        return None
    return (p90 - p50) / (p50 - p10) if (p50 - p10) != 0 else None


def f_amihud(days: list[dict[str, float | None]], window: int = 7) -> float | None:
    """ILLIQ = mean(|ret_d| / 日成交额)，仅取有成交日（零成交日单独
    由 miss_rate 表达，红队/架构 §2 双重失真修正）。"""
    pairs = []
    for d in days[-window:]:
        px, tx = d.get("avg_p"), d.get("tx")
        if px is None or not tx or tx <= 0:
            continue
        pairs.append((px, tx))
    if len(pairs) < 3:
        return None
    illiq = []
    for i in range(1, len(pairs)):
        prev, cur = pairs[i - 1][0], pairs[i][0]
        if prev and prev > 0:
            ret = abs(cur / prev - 1.0)
            turnover = cur * pairs[i][1]
            if turnover > 0:
                illiq.append(ret / turnover)
    return sum(illiq) / len(illiq) if illiq else None


def f_miss_rate(hours_zero: int, hours_total: int) -> float | None:
    if hours_total <= 0:
        return None
    return hours_zero / hours_total


def f_lvl_migration(prices: list[float], window: int = 7) -> float | None:
    """低价段档位迁移（tick 量子化段专用，架构 §1）。"""
    seg = [p for p in prices[-window:] if p]
    if len(seg) < 3 or min(seg) <= 0:
        return None
    tick = 0.01
    moves = [abs(seg[i] - seg[i - 1]) / tick for i in range(1, len(seg))]
    return sum(moves) / len(moves)


def f_beta(rets: list[float], idx_rets: list[float], window: int = 30) -> float | None:
    n = min(len(rets), len(idx_rets), window)
    if n < 10:
        return None
    r, m = rets[-n:], idx_rets[-n:]
    mr = sum(m) / n
    var = sum((x - mr) ** 2 for x in m)
    if var <= 0:
        return None
    rr = sum(r) / n
    cov = sum((r[i] - rr) * (m[i] - mr) for i in range(n))
    return cov / var


# ---------------------------------------------------------------------------
# 批量计算
# ---------------------------------------------------------------------------

def _load_daily_matrix(
    storage: Storage, days: int, max_items: int,
) -> dict[str, list[dict[str, Any]]]:
    """item -> 按日排序的快照列表（item_history 日终 + 零成交小时占比）。

    日记录的字段：day(str) + tx/survive/sell_n/buy_n/avg_p/sell_p/buy_p
    （float|None）——与 validation._load_matrix 同一结构。
    """
    now = _utcnow()
    start = _iso(now - timedelta(days=days))
    cutoff = now.strftime("%Y-%m-%d")
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        query = """
            SELECT item_id, day,
                   MAX(CASE WHEN metric='transaction_count' THEN close END) AS tx,
                   MAX(CASE WHEN metric='survive_num' THEN close END) AS survive,
                   MAX(CASE WHEN metric='sell_num' THEN close END) AS sell_n,
                   MAX(CASE WHEN metric='buy_num' THEN close END) AS buy_n,
                   MAX(CASE WHEN metric='transaction_avg_price' THEN close END) AS avg_p,
                   MAX(CASE WHEN metric='sell_price' THEN close END) AS sell_p,
                   MAX(CASE WHEN metric='buy_price' THEN close END) AS buy_p
            FROM (
                SELECT item_id, metric, close,
                       substr(ts,1,10) AS day,
                       ROW_NUMBER() OVER (
                           PARTITION BY item_id, substr(ts,1,10), metric
                           ORDER BY ts DESC) AS rn
                FROM series
                WHERE dataset='item_history' AND source='siteb'
                  AND metric IN ('transaction_count','survive_num','sell_num',
                                 'buy_num','transaction_avg_price',
                                 'sell_price','buy_price')
                  AND ts >= ? AND substr(ts,1,10) < ?
            ) WHERE rn = 1
            GROUP BY item_id, day
        """
        params: list[Any] = [start, cutoff]
        rows = conn.execute(query + " LIMIT 2000001", params).fetchall()
        if len(rows) > 2000000:
            raise RuntimeError(
                "daily matrix over 2,000,000 rows: use --max-items to shard; "
                "silent truncation would corrupt features")
        for row in rows:
            out[str(row["item_id"])].append({
                "day": str(row["day"]),
                "tx": _f(row["tx"]), "survive": _f(row["survive"]),
                "sell_n": _f(row["sell_n"]), "buy_n": _f(row["buy_n"]),
                "avg_p": _f(row["avg_p"]), "sell_p": _f(row["sell_p"]),
                "buy_p": _f(row["buy_p"]),
            })
    for item in out:
        out[item].sort(key=lambda d: d["day"])
    if max_items > 0 and len(out) > max_items:
        # 活跃度截取：按近 7 日成交笔数降序保留前 N 件（首次冒烟用）
        def activity(days: list[dict[str, Any]]) -> float:
            return sum(d["tx"] or 0.0 for d in days[-7:])
        keep = sorted(out, key=lambda i: activity(out[i]), reverse=True)[:max_items]
        return {i: out[i] for i in keep}
    return out


def _load_market_index_daily(storage: Storage, days: int) -> list[float]:
    now = _utcnow()
    with storage.read_connect() as conn:
        # market_index 的 ts 为 UTC+8 本地时间且无时区后缀（巡检发现）：
        # 窗口下界放宽 1 天并在 Python 侧按 +08:00 归一到 UTC 日
        rows = conn.execute(
            """SELECT ts, close
            FROM series
            WHERE dataset='market_index' AND metric='market_index'
              AND ts >= ?
            ORDER BY ts""",
            (_iso(now - timedelta(days=days + 1)),),
        ).fetchall()
    normalized: list[tuple[str, float]] = []
    for r in rows:
        raw, v = str(r[0]), _f(r[1])
        if v is None or v <= 0:
            continue
        try:
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
            normalized.append((dt.astimezone(timezone.utc).strftime(
                "%Y-%m-%d"), v))
        except ValueError:
            continue
    rows = normalized
    last_day = ""
    last_val = 0.0
    rets: list[float] = []
    for r in rows:
        v = r[1]
        day = r[0]
        if day != last_day:
            if last_day and last_val > 0:
                rets.append(v / last_val - 1.0)
            last_day, last_val = day, v
        else:
            last_val = v  # 同日取最新
    return rets


def compute_l1(storage: Storage, *, max_items: int = 0,
               lookback_days: int = 40) -> tuple[int, int]:
    """计算并写入 L1 特征。返回 (饰品数, 特征行数)。"""
    matrix = _load_daily_matrix(storage, lookback_days + 100, max_items)
    idx_rets = _load_market_index_daily(storage, lookback_days + 35)

    # 跨平台净得价差：每平台取"特征日当天及以前的最新一条"
    # （红审修复：原 MAX(close)+now 窗口既数值偏高又对特征日构成前视）
    now = _utcnow()
    if matrix:
        bound_day = max(d["day"] for days in matrix.values() for d in days[-1:])
        bound_ts = bound_day + "T23:59:59+00:00"
    else:
        bound_day = now.strftime("%Y-%m-%d")
        bound_ts = _iso(now)
    win_start = _iso(now - timedelta(days=3))
    spd_map: dict[str, float] = {}
    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        cross = conn.execute(
            """SELECT item_id,
                   MAX(CASE WHEN platform='steam' THEN close END) AS steam_p,
                   MAX(CASE WHEN platform='buff' THEN close END) AS buff_p,
                   MAX(CASE WHEN platform='steam' THEN ts END) AS steam_ts,
                   MAX(CASE WHEN platform='buff' THEN ts END) AS buff_ts
            FROM (
                SELECT item_id, platform, close, ts,
                       ROW_NUMBER() OVER (
                           PARTITION BY item_id, platform
                           ORDER BY ts DESC) AS rn
                FROM series
                WHERE dataset='platform_snapshot' AND source='siteb'
                  AND metric='sell_price'
                  AND ts >= ? AND ts <= ?
            ) WHERE rn = 1
            GROUP BY item_id
            HAVING steam_p IS NOT NULL AND buff_p IS NOT NULL""",
            (win_start, bound_ts),
        ).fetchall()
    item_last_day: dict[str, str] = {}
    for item, days in matrix.items():
        if days:
            item_last_day[item] = days[-1]["day"]
    skipped_stale = 0
    for row in cross:
        item = str(row["item_id"])
        # 真前视过滤（审查A B1 修复：快照 ts 晚于该 item 特征日的
        # 上界则弃用——last_day 滞后于全局 bound 的品不能用未来快照）
        bound_i = (item_last_day.get(item) or bound_day) + "T23:59:59+00:00"
        if str(row["steam_ts"] or "") > bound_i or str(row["buff_ts"] or "") > bound_i:
            skipped_stale += 1
            continue
        v = spd_fb(_f(row["steam_p"]), _f(row["buff_p"]))
        if v is not None:
            spd_map[item] = v
    if skipped_stale:
        log.debug("spd_fb skipped %d items with future snapshots", skipped_stale)
    if not spd_map and matrix:
        log.warning("spd_fb empty: platform_snapshot window yielded nothing "
                    "(bound=%s win_start=%s)——数据链停摆或窗口错配",
                    bound_ts, win_start)

    rows_out: list[dict[str, Any]] = []
    items_with_features = 0
    for item, days in matrix.items():
        if len(days) < MIN_DAYS_FOR_FEATURES:
            continue
        last = days[-1]
        ts = last["day"] + "T23:59:59+00:00"
        base = {"item_id": item, "freq": FREQ, "ts": ts}
        emitted = 0

        # ---- 价格序列（零成交纪律：avg_p 缺失日用 sell_p 仅作窗口锚，
        # 收益计算只在两个端点都有成交的日子之间进行）
        px_days = [d for d in days if d.get("avg_p")]
        px = [d["avg_p"] for d in px_days]  # type: ignore[arg-type]
        rets: list[float] = []
        for i in range(1, len(px)):
            if px[i - 1] and px[i - 1] > 0:
                rets.append(px[i] / px[i - 1] - 1.0)

        def emit(name: str, value: float | None):
            nonlocal emitted
            if value is not None and value == value:
                rows_out.append({**base, "name": name, "value": float(value)})
                emitted += 1

        # 动量族
        emit("ret_7d", f_ret(px, 7))
        emit("ret_30d", f_ret(px, 30) if len(px) >= 31 else None)
        r7, r28 = f_ret(px, 7), f_ret(px, 28)
        emit("rev_1w4w", (r7 - r28) if (r7 is not None and r28 is not None) else None)
        emit("dd_30d", f_drawdown(px, 30))
        low_seg = [d["sell_p"] for d in days[-7:] if d.get("sell_p")]  # type: ignore
        if low_seg and min(low_seg) < LOW_PRICE_TICK_THRESHOLD:
            emit("lvl_mig_7d", f_lvl_migration(low_seg, 7))
        # 波动族
        emit("vol_30d", f_vol_mad(rets, 30))
        emit("tail_ratio", f_tail_ratio(rets, 30))
        # 流动性族
        emit("amihud_7d", f_amihud(days, 7))
        tx7 = sum(d["tx"] for d in days[-7:] if d.get("tx"))  # type: ignore
        if last.get("survive") and tx7:
            emit("turn_7d", tx7 / last["survive"])  # type: ignore
            val7 = sum((d["tx"] or 0) * (d["avg_p"] or 0) for d in days[-7:])
            if val7 > 0:
                emit("val_turn_7d", val7 / last["survive"])  # type: ignore
        # 供需族
        if last.get("sell_n") and last.get("buy_n"):
            sn, bn = last["sell_n"], last["buy_n"]  # type: ignore
            if sn > 0:
                dsr = bn / sn
                emit("dsr", dsr)
                trail = []
                for d in days[-91:-1]:
                    if d.get("sell_n") and d.get("buy_n") and d["sell_n"] > 0:  # type: ignore
                        trail.append(d["buy_n"] / d["sell_n"])  # type: ignore
                z90 = robust_z(dsr, trail)
                # MAD 退化的 trail 会产生数百级 z（巡检II：MAX=3233）
                if z90 is not None and abs(z90) <= 100.0:
                    emit("dsr_z90", z90)
        d7 = (datetime.strptime(last["day"], "%Y-%m-%d")
              - timedelta(days=7)).strftime("%Y-%m-%d")
        prev = next((d for d in reversed(days[:-1]) if d["day"] <= d7), None)
        if prev and prev.get("survive") and last.get("survive") and prev["survive"] > 0:  # type: ignore
            emit("supply_growth_7d",
                 (last["survive"] - prev["survive"]) / prev["survive"])  # type: ignore
        if last.get("sell_n") is not None and last.get("buy_n") is not None:
            s_, b_ = last["sell_n"] or 0.0, last["buy_n"] or 0.0
            if (s_ + b_) > 0:
                emit("obi_d", (s_ - b_) / (s_ + b_))
        # 跨平台族（|spd|>1 视为垃圾快照价：Steam 净得超 BUFF 两倍
        # 在正常市场无经济意义——巡检II发现 14.6% 越界混入）
        if item in spd_map and abs(spd_map[item]) <= 1.0:
            emit("spd_fb", spd_map[item])
        # 大盘族
        if len(rets) >= 15 and len(idx_rets) >= 15:
            beta = f_beta(rets, idx_rets, 30)
            emit("beta_idx_30d", beta)
            if beta is not None and r7 is not None and idx_rets:
                idx7 = sum(idx_rets[-7:]) if len(idx_rets) >= 7 else None
                if idx7 is not None:
                    emit("idio_ret_7d", r7 - beta * idx7)
        # 质量族：近 7 日零成交日占比（缺 avg_p 的交易日近似零成交）
        seg = days[-7:]
        zero = sum(1 for d in seg if not d.get("tx") or d["tx"] == 0)  # type: ignore
        emit("miss_rate_7d", f_miss_rate(zero, len(seg)))

        if emitted:
            items_with_features += 1

    written = storage.upsert_features(rows_out) if rows_out else 0
    return items_with_features, written


def compute_l2(storage: Storage, lookback_days: int = 40) -> tuple[int, int]:
    """L2 组合特征：当日截面 z（跨饰品）+ 自身 trailing z（跨时间）。

    从 feature_store 读当日 L1 截面与近 90 日历史，合成 8 个组合。
    """
    now = _utcnow()
    with storage.read_connect() as conn:
        conn.row_factory = __import__("sqlite3").Row
        # 当日截面（每饰品取最新 ts）
        latest = conn.execute(
            """SELECT f.item_id, f.name, f.value, f.ts FROM feature_store f
            JOIN (SELECT item_id, name, ts, MAX(version) AS mv FROM feature_store
                  WHERE freq=? GROUP BY item_id, name, ts) g
              ON g.item_id = f.item_id AND g.name = f.name
                 AND g.ts = f.ts AND g.mv = f.version
            WHERE f.freq=? AND f.ts = (
                SELECT MAX(ts) FROM feature_store h
                WHERE h.freq=? AND h.item_id = f.item_id)""",
            (FREQ, FREQ, FREQ),
        ).fetchall()
        cross: dict[str, dict[str, float]] = defaultdict(dict)
        cross_ts: dict[str, str] = {}
        for r in latest:
            item, name, value = str(r["item_id"]), str(r["name"]), _f(r["value"])
            if value is not None:
                cross[item][name] = value
                cross_ts[item] = str(r["ts"])

    def xs_values(name: str) -> list[float]:
        return [c[name] for c in cross.values() if name in c]

    def xs_z(item: str, name: str, pool: list[float]) -> float | None:
        v = cross.get(item, {}).get(name)
        if v is None or len(pool) < 20:
            return None
        return robust_z(v, pool)

    # 截面池
    pools = {
        n: xs_values(n) for n in (
            "amihud_7d", "turn_7d", "ret_7d", "vol_30d", "dd_30d",
            "dsr", "supply_growth_7d", "spd_fb",
        )
    }

    rows_out: list[dict[str, Any]] = []
    items = 0

    # 读取近 15 日 turn_7d 历史（算换手自身变化量，
    # 防止组合特征塌缩成单一成分——相关矩阵实测 ρ=1.0 教训）
    hist_days = 15
    with storage.read_connect() as conn:
        rows_h = conn.execute(
            """SELECT f.item_id, f.ts, f.name, f.value FROM feature_store f
            JOIN (SELECT item_id, name, ts, MAX(version) AS mv
                  FROM feature_store WHERE freq=? AND name IN
                  ('turn_7d','dsr','supply_growth_7d')
                  GROUP BY item_id, name, ts) g
              ON g.item_id=f.item_id AND g.name=f.name AND g.ts=f.ts
                 AND g.mv=f.version
            WHERE f.freq=? AND f.name IN
              ('turn_7d','dsr','supply_growth_7d') AND f.ts >= ?""",
            (FREQ, FREQ,
             _iso(now - timedelta(days=hist_days))),
        ).fetchall()
    turn_hist: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for r in rows_h:
        if str(r["name"]) == "turn_7d":
            turn_hist[str(r["item_id"])].append((str(r["ts"]), float(r["value"])))

    def z_turn_growth(item: str) -> float | None:
        """7 日换手自身的 7 日变化（相对自身历史，非截面）。"""
        h = sorted(turn_hist.get(item, []))
        if len(h) < 5:
            return None
        vals = [v for _, v in h]
        cur = vals[-1]
        past = vals[:-1]
        med = median(past)
        if med is None:
            return None
        m = mad(past, med)
        if not m or m <= 0:
            return None
        return (cur - med) / (1.4826 * m)

    for item, feats in cross.items():
        ts = cross_ts.get(item, _iso(now))
        base = {"item_id": item, "freq": FREQ, "ts": ts}
        emitted = 0

        def emit(name: str, value: float | None):
            nonlocal emitted
            if value is not None and value == value:
                rows_out.append({**base, "name": name, "value": float(value)})
                emitted += 1

        z_amihud = xs_z(item, "amihud_7d", pools["amihud_7d"])
        z_turn = xs_z(item, "turn_7d", pools["turn_7d"])
        z_dd = xs_z(item, "dd_30d", pools["dd_30d"])
        z_dsr = xs_z(item, "dsr", pools["dsr"])
        z_ret = xs_z(item, "ret_7d", pools["ret_7d"])

        # 1) 流动性收缩（成交族内合成：amihud 与 turn 天然负相关属机械
        #    关系，下游打分时本组合与 amihud/turn 合计一票——依赖矩阵规则）
        parts = [z for z in (z_amihud, -z_turn if z_turn is not None else None) if z is not None]
        if parts:
            emit("liq_contraction", sum(parts) / len(parts))
        # 2) 净吸收：换手上行 ∧ 库存收缩（两腿独立，防塌缩——实测教训）
        sg = feats.get("supply_growth_7d")
        ztg = z_turn_growth(item)
        if sg is not None and ztg is not None:
            emit("absorption", ztg - sg * 10.0)  # 库存腿量纲放大约束在 z 量级
        # 3) 派发温度：涨势中 换手扩张+在售回升+求购走弱（三腿独立）
        if z_ret is not None and sg is not None and ztg is not None:
            bull = max(0.0, z_ret)
            emit("distribution",
                 bull * (ztg + max(0.0, sg) * 10.0) - (z_dsr or 0.0))
        # 4) 零成交上涨三联征（操纵/知情/收藏三解不可分——只标记嫌疑）
        r = feats.get("ret_7d")
        t7 = feats.get("turn_7d")
        if (r is not None and t7 is not None and pools["ret_7d"]
                and pools["turn_7d"] and sg is not None):
            r_pct = pct_rank(r, pools["ret_7d"])
            t_pct = pct_rank(t7, pools["turn_7d"])
            if r_pct is not None and t_pct is not None:
                emit("zero_vol_rise",
                     1.0 if (r_pct > 0.9 and t_pct < 0.3 and sg < 0) else 0.0)
        # 5) 相对强弱（K 型分化个体腿：动量 - 大盘动量）
        if "idio_ret_7d" in feats:
            emit("rel_strength", feats["idio_ret_7d"])
        # 6) 崩盘压力（等权，架构 §2 L2：权重不在含标签样本上定）
        parts6 = [z for z in (z_amihud, -z_dsr if z_dsr is not None else None,
                              z_dd) if z is not None]
        if parts6:
            emit("crash_pressure", sum(parts6) / len(parts6))
        # 7) 底部序列（分阶段打分 0-3，单一极值无效——架构 §4）
        score = 0
        if z_dd is not None and z_dd > 2.0:
            score += 1  # 深回撤
        if feats.get("vol_30d") is not None and pools["vol_30d"]:
            v_pct = pct_rank(feats["vol_30d"], pools["vol_30d"])
            if v_pct is not None and v_pct > 0.8:
                score += 1  # 高波动（恐慌放量阶段）
        if r is not None and r > 0:
            score += 1  # 催化反弹
        emit("bottom_seq", float(score))
        # 8) 异常分数（当日特征向量的 robust 距离，作特征不作警报）
        zs = [z for z in (z_amihud, z_turn, z_ret, z_dd, z_dsr) if z is not None]
        if len(zs) >= 3:
            emit("ano_score", sum(abs(z) for z in zs) / len(zs))

        if emitted:
            items += 1

    written = storage.upsert_features(rows_out) if rows_out else 0
    return items, written


def run_features(storage: Storage, *, max_items: int = 0) -> dict[str, Any]:
    l1_items = l1_rows = l2_items = l2_rows = 0
    errors: list[str] = []
    try:
        l1_items, l1_rows = compute_l1(storage, max_items=max_items)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"l1:{type(exc).__name__}:{exc}")
        log.warning("L1 features failed: %s", exc, exc_info=True)
    try:
        l2_items, l2_rows = compute_l2(storage)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"l2:{type(exc).__name__}:{exc}")
        log.warning("L2 features failed: %s", exc, exc_info=True)
    log.info("features: l1_items=%s l1_rows=%s l2_items=%s l2_rows=%s errors=%s",
             l1_items, l1_rows, l2_items, l2_rows, errors or "none")
    return {
        "status": "ok" if not errors else "partial",
        "error_class": "" if not errors else "local",
        "metrics": {
            "l1_items": l1_items, "l1_rows": l1_rows,
            "l2_items": l2_items, "l2_rows": l2_rows,
        },
        "details": {"errors": errors[:3]},
    }
