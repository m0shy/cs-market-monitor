# -*- coding: utf-8 -*-
"""Web 实时数据中心：聚合采集器落库的实时数据集与运行状态。

数据来源（全部本地库/文件，无额外站点请求）：
- series 实时数据集（采集器 siteb-ws 常驻服务写入）：
  ws_market_statistics / ws_market_index / ws_market_kline_h|d|w /
  ws_new_listing
- site_metadata.ws_collector：WS 采集器自身心跳
- runtime/supervisor-heartbeat.json：监督器活动任务
- control.crawl_state：站点共享暂停（额度/限流）状态

对外提供两类读取：
- realtime_snapshot(conn)：全量快照（页面初次加载/REST）
- realtime_events(conn, since)：增量事件（SSE 推流，游标为最大 ts）
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT.parent / "csgo-monitor-data" / "crawler"

STATISTICS_DATASET = "ws_market_statistics"
INDEX_DATASET = "ws_market_index"
LISTING_DATASET = "ws_new_listing"
KLINE_DATASETS = {
    "h": "ws_market_kline_h",
    "d": "ws_market_kline_d",
    "w": "ws_market_kline_w",
}

# 指标中文名（页面展示用）
STATISTIC_LABELS = {
    "survive_num": "存世量",
    "holders_num": "持有人数",
    "add_num": "今日新增",
    "add_valuation": "新增估值",
    "trade_num": "成交笔数",
    "turnover": "成交额",
}

LISTING_KEEP = 50
INDEX_KEEP = 360
KLINE_KEEP = 72


def _parse_ts(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _latest_rows(conn: sqlite3.Connection, dataset: str, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """SELECT item_id, metric, platform, ts, open, high, low, close,
                  volume, turnover, extra_json
        FROM series WHERE dataset=? ORDER BY ts DESC LIMIT ?""",
        (dataset, limit),
    ).fetchall()


def realtime_snapshot(conn: sqlite3.Connection) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    snapshot: dict[str, Any] = {
        "server_time": now.isoformat(),
        "statistics": {},
        "index": {"series": [], "latest": None},
        "kline": {},
        "listings": [],
        "ws_collector": _ws_collector_state(conn),
        "supervisor": _supervisor_state(),
        "pauses": _pause_state(conn),
    }

    # ---- 实时统计：每个 metric 取最新一条 ----
    for row in _latest_rows(conn, STATISTICS_DATASET, 120):
        metric = str(row["metric"])
        if metric in snapshot["statistics"]:
            continue
        snapshot["statistics"][metric] = {
            "value": row["close"],
            "ts": str(row["ts"]),
            "label": STATISTIC_LABELS.get(metric, metric),
        }

    # ---- 大盘指数：最近序列 + 最新点 ----
    index_rows = list(reversed(_latest_rows(conn, INDEX_DATASET, INDEX_KEEP)))
    snapshot["index"]["series"] = [
        {"ts": str(r["ts"]), "value": r["close"]} for r in index_rows
    ]
    if index_rows:
        last = index_rows[-1]
        snapshot["index"]["latest"] = {
            "ts": str(last["ts"]), "value": last["close"],
        }

    # ---- 实时 K 线（h/d/w）----
    for key, dataset in KLINE_DATASETS.items():
        rows = list(reversed(_latest_rows(conn, dataset, KLINE_KEEP)))
        snapshot["kline"][key] = [
            {
                "ts": str(r["ts"]), "open": r["open"], "high": r["high"],
                "low": r["low"], "close": r["close"],
            }
            for r in rows
        ]

    # ---- 新上架饰品实时流 ----
    for row in _latest_rows(conn, LISTING_DATASET, LISTING_KEEP):
        try:
            extra = json.loads(row["extra_json"] or "{}")
        except json.JSONDecodeError:
            extra = {}
        snapshot["listings"].append({
            "ts": str(row["ts"]),
            "item_id": str(row["item_id"]),
            "price": row["close"],
            "float_wear": extra.get("floatWear"),
            "asset_id": extra.get("assetId"),
        })

    latest_any = max(
        [v["ts"] for v in snapshot["statistics"].values()]
        + [item["ts"] for item in snapshot["listings"]]
        + [str(r["ts"]) for r in index_rows],
        default="",
    )
    snapshot["data_cursor"] = latest_any
    latest_parsed = _parse_ts(latest_any)
    snapshot["fresh_seconds"] = (
        round((now - latest_parsed).total_seconds(), 1)
        if latest_parsed is not None else None
    )
    return snapshot


def realtime_events(
    conn: sqlite3.Connection, since: str
) -> dict[str, Any]:
    """返回游标之后的新数据（SSE 增量推流用）。"""
    now = datetime.now(timezone.utc)
    events: list[dict[str, Any]] = []
    cursor = since
    for dataset, kind in (
        (STATISTICS_DATASET, "statistics"),
        (INDEX_DATASET, "index"),
        (LISTING_DATASET, "listing"),
        (KLINE_DATASETS["h"], "kline_h"),
        (KLINE_DATASETS["d"], "kline_d"),
        (KLINE_DATASETS["w"], "kline_w"),
    ):
        # since 为空时返回"最新 200 条"（裸 GET 的直觉语义）；
        # 带 since 时维持游标式增量（ORDER ASC）。
        if since:
            rows = conn.execute(
                """SELECT item_id, metric, ts, open, high, low, close, extra_json
                FROM series WHERE dataset=? AND ts>? ORDER BY ts ASC LIMIT 200""",
                (dataset, since),
            ).fetchall()
        else:
            rows = list(reversed(conn.execute(
                """SELECT item_id, metric, ts, open, high, low, close, extra_json
                FROM series WHERE dataset=? ORDER BY ts DESC LIMIT 200""",
                (dataset,),
            ).fetchall()))
        for row in rows:
            payload = {
                "ts": str(row["ts"]),
                "metric": str(row["metric"]),
                "item_id": str(row["item_id"]),
                "value": row["close"],
            }
            if row["open"] is not None:
                payload.update({
                    "open": row["open"], "high": row["high"],
                    "low": row["low"], "close": row["close"],
                })
            if kind == "listing" and row["extra_json"]:
                try:
                    payload["extra"] = json.loads(row["extra_json"])
                except json.JSONDecodeError:
                    pass
            events.append({"kind": kind, "payload": payload})
            if str(row["ts"]) > cursor:
                cursor = str(row["ts"])
    return {
        "server_time": now.isoformat(),
        "cursor": cursor,
        "events": events,
    }


def _ws_collector_state(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        "SELECT payload, updated_at FROM site_metadata WHERE source='siteb' AND key='ws_collector'"
    ).fetchone()
    if not row:
        return {"running": False}
    try:
        payload = json.loads(row[0])
    except (TypeError, json.JSONDecodeError):
        return {"running": False}
    last_message = _parse_ts(payload.get("last_message_at"))
    now = datetime.now(timezone.utc)
    updated = _parse_ts(row[1])
    return {
        "running": bool(last_message and (now - last_message).total_seconds() < 600),
        "connected_at": payload.get("connected_at"),
        "last_message_at": payload.get("last_message_at"),
        "seconds_since_message": (
            round((now - last_message).total_seconds())
            if last_message else None
        ),
        "counts": payload.get("counts") or {},
        "rows_written": payload.get("rows_written"),
        "updated_at": str(row[1]) if updated else None,
        "proxy": payload.get("proxy"),
    }


def _supervisor_state() -> dict[str, Any]:
    path = DATA_ROOT / "runtime" / "supervisor-heartbeat.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"running": False}
    updated = _parse_ts(payload.get("updated_at"))
    now = datetime.now(timezone.utc)
    return {
        "running": payload.get("status") == "running"
        and updated is not None
        and (now - updated).total_seconds() < 90,
        "pid": payload.get("pid"),
        "updated_at": payload.get("updated_at"),
        "active": sorted((payload.get("active") or {}).keys()),
        "deferred_runs": payload.get("deferred_runs"),
        "failures": payload.get("failures"),
    }


def _pause_state(conn: sqlite3.Connection) -> dict[str, Any]:
    pauses: dict[str, Any] = {}
    for job, label in (
        ("siteb_site_quota_until", "siteb"),
        ("sitea_public_pause_until", "sitea"),
    ):
        try:
            row = conn.execute(
                "SELECT cursor, detail_json, updated_at FROM control.crawl_state WHERE job=?",
                (job,),
            ).fetchone()
        except sqlite3.OperationalError:
            continue
        if not row:
            continue
        try:
            until = float(row[0] or 0)
        except (TypeError, ValueError):
            until = 0.0
        now = datetime.now(timezone.utc).timestamp()
        if until > now:
            try:
                detail = json.loads(row[1] or "{}")
            except json.JSONDecodeError:
                detail = {}
            pauses[label] = {
                "active": True,
                "error_class": detail.get("error_class", ""),
                "resume_at": datetime.fromtimestamp(until, timezone.utc).isoformat(),
                "reason": str(detail.get("reason", ""))[:120],
            }
    return pauses
