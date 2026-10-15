# -*- coding: utf-8 -*-
"""市场情报一体化仪表盘 API（/research/intelligence 页面消费）.

五个端点（全部只读，进程内小 TTL 缓存）：
  GET /api/intelligence/sentiment  情绪周期最新一轮判定（series dataset='sentiment_cycle'）
  GET /api/intelligence/supply     供给耗竭最新一批（dataset='supply_exhaustion'）
  GET /api/intelligence/events     事件日历（docs/market/events.json，直接读文件）
  GET /api/intelligence/execution  执行工具指标汇总（feature_store 最新截面）
  GET /api/intelligence/summary    综合以上四源生成的分析师晨报（描述性，非交易建议）

连接工厂沿用 web.app.db（sqlite 只读连接，PRAGMA query_only=ON）——
与 web/routes/dashboard.py 同一约定：测试通过 patch web.app.DB 切换测试库。
引擎数据缺失时返回明确的默认响应，前端优雅降级显示"等待引擎首次运行"。
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from statistics import median
from typing import Any

from fastapi import APIRouter

from csgo_monitor.sentiment_cycle import NEXT_STAGE
from web.app import db
from web.state import PROJECT, _ttl_cached

log = logging.getLogger(__name__)

router = APIRouter()

EVENTS_PATH = PROJECT / "docs" / "market" / "events.json"

# ---------------------------------------------------------------------------
# 1) 情绪周期（sentiment_cycle）
# ---------------------------------------------------------------------------

# 六张指标卡：ind_* metric -> 展示名（顺序即卡片顺序）
SENTIMENT_INDICATORS: tuple[tuple[str, str], ...] = (
    ("ind_tx_volume", "成交量"),
    ("ind_breadth_active", "市场宽度"),
    ("ind_market_index", "大盘指数"),
    ("ind_listing_rate", "挂单流速"),
    ("ind_spd_fb", "跨平台价差"),
    ("ind_survive_growth_7d", "存世量"),
)
INDICATOR_NAME_MAP = dict(SENTIMENT_INDICATORS)

# 情绪引擎未把 transition_hints 持久化到 series——按架构文档的转移规则
# 从已存字段（matched/errors/quality_note）+ 阶段顺序推导，不伪造数值。
NEXT_STAGE_HINT = {
    "恐慌": "→恐慌：大盘单日跌幅>5%，或 Valve 政策突变（L3 事件系统待建）",
    "出清": "→出清：恐慌持续>7天且挂单流速从峰值回落 50%",
    "冷漠": "→冷漠：ret_30d 收窄至±3% 且成交稳定低位",
    "萌芽": "→萌芽：成交连续 7 天>前 30 日中位",
    "狂热": "→狂热：breadth_active>0.7 且 KOL bullish>60%（KOL 信号系统待建）",
}
QUALITY_LABEL = {"ok": "可用", "degraded": "降质", "insufficient": "不可判",
                 "structural": "结构性"}


def _f(value: Any) -> float | None:
    try:
        out = float(value)
        return out if out == out else None  # NaN guard
    except (TypeError, ValueError):
        return None


def _round(value: Any, digits: int = 4) -> Any:
    if isinstance(value, float):
        return round(value, digits)
    return value


def _sentiment_hints(extra: dict[str, Any]) -> list[str]:
    """从 stage 行 extra 推导转移提示（与引擎 _transition_hints 同源语义）。"""
    hints: list[str] = []
    stage = str(extra.get("stage") or "")
    if not extra.get("matched"):
        hints.append(
            f"无阶段完全匹配（部分得分最高：{stage or '无'}）——"
            "置信度封顶 0.5，阈值待 Phase 3 回测校准")
    for err in extra.get("errors") or []:
        if str(err).startswith("tx_coverage_collapse"):
            ratio = str(err).split(":")[-1]
            hints.append(
                f"数据警示：成交采集覆盖坍缩（覆盖比 {ratio}），"
                "成交量类证据已停用——修复采集前勿据本判定调仓")
    quality = extra.get("quality_note") or {}
    if quality.get("listing") == "insufficient":
        hints.append("数据警示：ws_new_listing 历史不足 10 日，挂单流速证据不可判")
    if quality.get("spd_fb") == "insufficient":
        hints.append("数据警示：spd_fb 基线不足，恐慌溢价证据不可判")
    if quality.get("tx") == "insufficient" and not any(
        str(e).startswith("tx_coverage_collapse") for e in extra.get("errors") or []
    ):
        hints.append("数据警示：成交量证据不可判（样本或基线不足）")
    nxt = NEXT_STAGE.get(stage, "")
    if nxt and nxt in NEXT_STAGE_HINT:
        hints.append(NEXT_STAGE_HINT[nxt])
    return hints


@_ttl_cached(60)
def _sentiment_snapshot() -> dict[str, Any]:
    with db() as conn:
        latest = conn.execute(
            """SELECT MAX(ts) AS mts FROM series
               WHERE dataset='sentiment_cycle' AND source='siteb'"""
        ).fetchone()
        if not latest or not latest["mts"]:
            return {"stage": "unknown",
                    "note": "等待情绪周期引擎首次运行"}
        rows = conn.execute(
            """SELECT metric, close, extra_json FROM series
               WHERE dataset='sentiment_cycle' AND source='siteb' AND ts=?
               ORDER BY metric""",
            (latest["mts"],),
        ).fetchall()

    stage_row = next((r for r in rows if r["metric"] == "stage"), None)
    if stage_row is None:
        return {"stage": "unknown", "note": "等待情绪周期引擎首次运行"}
    try:
        extra = json.loads(stage_row["extra_json"] or "{}")
    except (TypeError, ValueError):
        extra = {}

    indicators: dict[str, dict[str, Any]] = {}
    for metric, label in SENTIMENT_INDICATORS:
        row = next((r for r in rows if r["metric"] == metric), None)
        payload: dict[str, Any] = {
            "label": label,
            "current": _round(_f(row["close"])) if row else None,
        }
        if row:
            try:
                payload.update(json.loads(row["extra_json"] or "{}"))
            except (TypeError, ValueError):
                pass
        indicators[metric] = payload

    return {
        "stage": str(extra.get("stage") or "unknown"),
        "confidence": _f(extra.get("confidence")) or _f(
            next((r["close"] for r in rows if r["metric"] == "confidence"), None)),
        "matched": bool(extra.get("matched")),
        "eval_day": str(extra.get("eval_day") or ""),
        "prev_stage": str(extra.get("prev_stage") or ""),
        "stage_age_days": _f(extra.get("stage_age_days")),
        "ts": str(latest["mts"]),
        "indicators": indicators,
        "rule": extra.get("rule") or {},
        "quality_note": extra.get("quality_note") or {},
        "transition_hints": _sentiment_hints(extra),
    }


@router.get("/api/intelligence/sentiment")
def api_intelligence_sentiment():
    """情绪周期最新一轮判定（阶段/置信度/六指标/转移提示）。"""
    return _sentiment_snapshot()


@router.get("/api/intelligence/sentiment-history")
@_ttl_cached(300)
def api_intelligence_sentiment_history(days: int = 90) -> dict[str, Any]:
    """情绪周期历史轨迹：阶段序号 + 置信度 + 大盘指数（绘图用）。

    阶段序号 0-4 对应 狂热/恐慌/出清/冷漠/萌芽；同一 ts 的三个 metric
    透视成一行，只回最近 N 天，供前端画阶段色带 + 置信度曲线。
    """
    days = max(7, min(365, days))
    from datetime import timedelta, timezone as _tz
    cutoff = (datetime.now(_tz.utc) - timedelta(days=days)).isoformat()
    points: list[dict[str, Any]] = []
    with db() as conn:
        rows = conn.execute(
            """SELECT ts,
                      MAX(CASE WHEN metric='stage' THEN close END) AS stage_idx,
                      MAX(CASE WHEN metric='confidence' THEN close END) AS conf,
                      MAX(CASE WHEN metric='ind_market_index' THEN close END) AS mkt
               FROM series
               WHERE dataset='sentiment_cycle' AND source='siteb'
                 AND metric IN ('stage','confidence','ind_market_index')
                 AND ts >= ?
               GROUP BY ts
               ORDER BY ts""",
            (cutoff,),
        ).fetchall()
    # 引擎每天可能运行多次：按天去重保留最后一次判定，避免曲线抖动
    by_day: dict[str, dict[str, Any]] = {}
    for r in rows:
        if r["stage_idx"] is None:
            continue
        day = str(r["ts"])[:10]
        by_day[day] = {
            "ts": str(r["ts"]),
            "stage_idx": _round(_f(r["stage_idx"])),
            "confidence": _round(_f(r["conf"])),
            "market_index": _round(_f(r["mkt"])),
        }
    points = [by_day[d] for d in sorted(by_day)]
    return {"points": points, "count": len(points)}


# ---------------------------------------------------------------------------
# 2) 供给耗竭（supply_exhaustion）
# ---------------------------------------------------------------------------

SUPPLY_METRICS = ("exhaustion_score", "burn_rate", "listing_ratio",
                  "absorption_days")


@_ttl_cached(60)
def _supply_snapshot() -> dict[str, Any]:
    with db() as conn:
        latest = conn.execute(
            """SELECT MAX(ts) AS mts FROM series
               WHERE dataset='supply_exhaustion' AND source='siteb'"""
        ).fetchone()
        if not latest or not latest["mts"]:
            return {"market": None, "categories": [],
                    "note": "等待供给追踪引擎首次运行"}
        rows = conn.execute(
            """SELECT item_id, metric, close, extra_json, ts FROM series
               WHERE dataset='supply_exhaustion' AND source='siteb' AND ts=?""",
            (latest["mts"],),
        ).fetchall()

    entities: dict[str, dict[str, Any]] = {}

    def entity(key: str) -> dict[str, Any]:
        return entities.setdefault(
            key, {"as_of": str(latest["mts"])[:10]})

    for row in rows:
        key = str(row["item_id"])
        payload = entity(key)
        payload[row["metric"]] = _round(_f(row["close"]), 6)
        if row["extra_json"]:
            try:
                payload.update(json.loads(row["extra_json"]))
            except (TypeError, ValueError):
                pass

    market = entities.pop("", None)
    categories = [entities[k] for k in sorted(entities)]
    for payload in ([market] if market else []) + categories:
        payload["is_market"] = payload is market
    result: dict[str, Any] = {"market": market, "categories": categories}
    if market is None and not categories:
        result["note"] = "等待供给追踪引擎首次运行"
    return result


@router.get("/api/intelligence/supply")
def api_intelligence_supply():
    """供给耗竭最新一批：全市场汇总 + 品类级评分（item_id=品类名）。"""
    return _supply_snapshot()


# ---------------------------------------------------------------------------
# 3) 事件日历（docs/market/events.json）
# ---------------------------------------------------------------------------

EVENT_TYPE_LABELS = {
    "policy": "政策", "supply": "供给", "demand": "需求",
    "market": "行情", "event": "事件", "manipulation": "坐庄",
}


@_ttl_cached(300)
def _events_snapshot() -> dict[str, Any]:
    try:
        raw = json.loads(EVENTS_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        log.warning("events.json unreadable: %s", exc)
        return {"today": date.today().isoformat(), "upcoming": [],
                "recent": [], "stats": {}, "total": 0,
                "note": "事件日历文件缺失或不可读（docs/market/events.json）"}
    today = date.today()
    upcoming: list[dict[str, Any]] = []
    past: list[dict[str, Any]] = []
    stats: dict[str, int] = {}
    for entry in raw if isinstance(raw, list) else []:
        if not isinstance(entry, dict):
            continue
        try:
            event_day = date.fromisoformat(str(entry.get("date") or ""))
        except ValueError:
            continue
        days = (event_day - today).days
        payload = {
            "date": entry.get("date"),
            "type": str(entry.get("type") or ""),
            "type_label": EVENT_TYPE_LABELS.get(
                str(entry.get("type") or ""), "其他"),
            "title": entry.get("title") or "",
            "impact": entry.get("impact") or "",
            "source": entry.get("source") or "",
            "approx": bool(entry.get("approx")),
            "days": days,
        }
        stats[payload["type_label"]] = stats.get(payload["type_label"], 0) + 1
        if days >= 0:
            upcoming.append(payload)
        else:
            past.append(payload)
    upcoming.sort(key=lambda item: item["days"])
    past.sort(key=lambda item: -item["days"])
    return {
        "today": today.isoformat(),
        "upcoming": upcoming,
        "recent": past[:5],
        "stats": stats,
        "total": len(upcoming) + len(past),
    }


@router.get("/api/intelligence/events")
def api_intelligence_events():
    """事件日历：未来事件（倒计时）+ 最近 5 个历史事件 + 分类型统计。"""
    return _events_snapshot()


# ---------------------------------------------------------------------------
# 4) 执行工具指标（feature_store 最新截面）
# ---------------------------------------------------------------------------

EXECUTION_FEATURES = ("amihud_7d", "spd_fb", "turn_7d")


# feature_store 按 (item_id, name, ts, version) 主键存多版本；
# 截面查询 GROUP BY item_id+name 并取 MAX(version)——SQLite 文档化的
# 裸列取值行为保证 value 来自最新版本行。


@_ttl_cached(120)
def _execution_snapshot() -> dict[str, Any]:
    with db() as conn:
        marks = ",".join("?" for _ in EXECUTION_FEATURES)
        latest = conn.execute(
            f"""SELECT MAX(ts) AS mts FROM feature_store
                WHERE freq='1d' AND name IN ({marks})""",
            EXECUTION_FEATURES,
        ).fetchone()
        if not latest or not latest["mts"]:
            return {"available": False,
                    "note": "等待特征管线首次运行（feature_store 无截面）"}
        ts = str(latest["mts"])
        rows = conn.execute(
            f"""SELECT f.item_id, f.name AS feature, f.value, MAX(f.version),
                       COALESCE(i.market_hash_name, i.name, f.item_id) AS item_name
                FROM feature_store f
                LEFT JOIN items i ON i.source='siteb' AND i.item_id=f.item_id
                WHERE f.freq='1d' AND f.ts=? AND f.name IN ({marks})
                GROUP BY f.item_id, f.name""",
            (ts, *EXECUTION_FEATURES),
        ).fetchall()

        prev = conn.execute(
            """SELECT MAX(ts) AS mts FROM feature_store
               WHERE freq='1d' AND name='spd_fb' AND ts<?""",
            (ts,),
        ).fetchone()
        prev_rows = []
        if prev and prev["mts"]:
            prev_rows = conn.execute(
                """SELECT f.item_id, f.value, MAX(f.version)
                   FROM feature_store f
                   WHERE f.freq='1d' AND f.ts=? AND f.name='spd_fb'
                   GROUP BY f.item_id""",
                (str(prev["mts"]),),
            ).fetchall()

    by_feature: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_feature.setdefault(str(row["feature"]), []).append(
            {"item_id": str(row["item_id"]), "name": str(row["item_name"]),
             "value": _f(row["value"])})

    def values(feature: str) -> list[float]:
        return [entry["value"] for entry in by_feature.get(feature, [])
                if entry["value"] is not None]

    amihud_vals = values("amihud_7d")
    spd_vals = values("spd_fb")
    turn_vals = values("turn_7d")
    if not amihud_vals and not spd_vals and not turn_vals:
        return {"available": False, "as_of": ts,
                "note": "特征截面为空（特征管线可能未完成当日计算）"}

    def top_bottom(feature: str, count: int) -> dict[str, list[dict[str, Any]]]:
        entries = [e for e in by_feature.get(feature, [])
                   if e["value"] is not None]
        entries.sort(key=lambda entry: -entry["value"])
        return {"top": entries[:count], "bottom": entries[-count:][::-1]}

    prev_median = median([_f(r["value"]) for r in prev_rows
                          if _f(r["value"]) is not None]) if prev_rows else None
    spd_median = median(spd_vals) if spd_vals else None
    spd_trend = None
    if spd_median is not None and prev_median is not None and prev_median != 0:
        spd_trend = _round(spd_median / prev_median - 1.0, 4)

    return {
        "available": True,
        "as_of": ts,
        "n_items": len({entry["item_id"]
                        for entries in by_feature.values()
                        for entry in entries}),
        "amihud": {
            "median": _round(median(amihud_vals), 8) if amihud_vals else None,
            "n": len(amihud_vals),
            **top_bottom("amihud_7d", 10),
        },
        "spd_fb": {
            "median": _round(spd_median, 4) if spd_median is not None else None,
            "n": len(spd_vals),
            "prev_median": _round(prev_median, 4) if prev_median is not None else None,
            "prev_as_of": str(prev["mts"])[:10] if prev and prev["mts"] else None,
            "trend": spd_trend,
        },
        "turnover": top_bottom("turn_7d", 5),
    }


@router.get("/api/intelligence/execution")
def api_intelligence_execution():
    """执行工具指标：流动性温度（amihud）/ 跨平台价差（spd_fb）/ 换手极值。"""
    return _execution_snapshot()


# ---------------------------------------------------------------------------
# 5) 综合晨报（summary）
# ---------------------------------------------------------------------------

STAGE_ADVICE = {
    "狂热": "情绪偏热，谨慎追高、分批止盈",
    "恐慌": "恐慌未止，等 spd_fb 溢价回落后再接",
    "出清": "左侧观察期，等缩量与存世回升共振再分批",
    "冷漠": "缩量磨底，耐心等萌芽信号",
    "萌芽": "成交回暖初现，可小幅试探活跃品类",
}
GAP_LABEL = {"supply_tight": "供给收紧", "balanced": "供需均衡",
             "oversupplied": "供给过剩"}
DEMAND_ARROW = {"+": "上行", "-": "下行", "0": "持平"}


def _pct(value: Any, digits: int = 1) -> str:
    number = _f(value)
    return f"{number * 100:+.{digits}f}%" if number is not None else "缺数据"


@_ttl_cached(60)
def _summary_text() -> dict[str, Any]:
    sentiment = _sentiment_snapshot()
    supply = _supply_snapshot()
    events = _events_snapshot()
    execution = _execution_snapshot()

    parts: list[str] = []
    stage = str(sentiment.get("stage") or "")
    if stage and stage != "unknown":
        confidence = sentiment.get("confidence")
        confidence_text = (
            f"置信度{confidence:.2f}" if isinstance(confidence, float) else "置信度未知")
        matched_text = "判定完全命中" if sentiment.get("matched") else "判定未完全命中"
        parts.append(f"情绪周期处于「{stage}」阶段（{confidence_text}，{matched_text}）")
    else:
        parts.append("情绪周期引擎尚未产出判定")

    market = supply.get("market")
    if market:
        gap_text = GAP_LABEL.get(str(market.get("gap")), str(market.get("gap") or ""))
        trend_text = DEMAND_ARROW.get(str(market.get("demand_trend")), "未知")
        risk = str(market.get("risk_flag") or "")
        sentence = (f"供给端全市场耗竭评分{market.get('exhaustion_score', 0):.2f}"
                    f"（{gap_text}），需求{trend_text}"
                    f"（7v7 {_pct(market.get('demand_change'))}）")
        if risk == "dead_demand":
            sentence += "，触发有价无市风险警示"
        parts.append(sentence)
    else:
        parts.append("供给追踪引擎尚未产出数据")

    upcoming = events.get("upcoming") or []
    if upcoming:
        first = upcoming[0]
        days = int(first.get("days") or 0)
        when = "今天" if days == 0 else f"{days}天后"
        parts.append(f"下一事件：{first.get('title')}（{when}）")
    else:
        recent = events.get("recent") or []
        if recent:
            parts.append(f"最近事件：{recent[0].get('title')}（"
                         f"{abs(int(recent[0].get('days') or 0))}天前）")

    if execution.get("available"):
        amihud = execution.get("amihud", {}).get("median")
        spd = execution.get("spd_fb", {}).get("median")
        execution_bits = []
        if amihud is not None:
            execution_bits.append(f"Amihud中位{amihud:.1e}")
        if spd is not None:
            execution_bits.append(f"跨平台净价差中位{_pct(spd, 1).replace('+', '')}")
        if execution_bits:
            parts.append("执行面：" + "、".join(execution_bits))
    else:
        parts.append("执行面特征截面待生成")

    advice = STAGE_ADVICE.get(stage)
    if advice:
        parts.append(f"操作参考：{advice}")

    text = "。".join(parts) + "。"
    if len(text) > 200:
        text = text[:199] + "…"

    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "text": text,
        "stage": stage or None,
        "supply_gap": (market or {}).get("gap"),
        "next_event": (upcoming[0] if upcoming else None),
        "disclaimer": "以上为描述性分析，非交易建议",
    }


@router.get("/api/intelligence/summary")
def api_intelligence_summary():
    """综合四源生成的市场晨报（中文自然语言，≤200 字，描述性）。"""
    return _summary_text()


@router.get("/api/intelligence/all")
def api_intelligence_all():
    """合并端点：一次返回全部数据（各子端点自带缓存，直接调用即可）."""
    try:
        sentiment = api_intelligence_sentiment()
    except Exception:
        sentiment = {}
    try:
        supply = api_intelligence_supply()
    except Exception:
        supply = {}
    try:
        events = api_intelligence_events()
    except Exception:
        events = {}
    try:
        execution = api_intelligence_execution()
    except Exception:
        execution = {}
    try:
        summary = api_intelligence_summary()
    except Exception:
        summary = {}
    return {
        "sentiment": sentiment,
        "supply": supply,
        "events": events,
        "execution": execution,
        "summary": summary,
    }
