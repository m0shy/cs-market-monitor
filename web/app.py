# -*- coding: utf-8 -*-
"""CS2 饰品市场监控 - Web 展示框架（深度优化版）.

结构：首页(大盘+导购雷达) -> 饰品市场(搜索/列表/批量报价) -> 饰品详情
      (K线/平台价格/历史/技术指标/特殊款式/异动) + 大盘/排行榜/异动监测/开箱
数据源：行情站B Open API 实时快照(platform_snapshot/platform_avg) + 站点 API 历史。

性能：所有聚合查询走 idx_series_snap_latest / idx_series_dataset_item 索引；
      大盘/榜单/异动均有进程内 TTL 缓存；ECharts 本地化，无外网依赖。
"""
from __future__ import annotations

import hashlib
import html
import json
import logging
import math
import os
import re
import sqlite3
import time as _time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import closing, contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.responses import RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.gzip import GZipMiddleware

from csgo_monitor.pipelines import PIPELINE_STATUS_JOB
from web.cache import _CACHE, _CACHE_LOCK, cached
from web.navigation import CHANNELS, channel_for, freshness_status
from web.state import DATA_ROOT, ROOT, _ttl_cached

DB = Path(os.environ.get("CSGO_MONITOR_DATABASE", DATA_ROOT / "market.sqlite3"))
CONTROL_DB = Path(
    os.environ.get("CSGO_MONITOR_CONTROL_DATABASE", DATA_ROOT / "control.sqlite3")
)

app = FastAPI(title="CS2 饰品市场监控", docs_url=None, redoc_url=None,
              openapi_url=None)  # 安全：不暴露 API 结构图（本地服务最小暴露面）


@app.middleware("http")
async def _security_headers(request, call_next):
    """安全响应头（本地服务最小暴露面）：nosniff 防类型嗅探、
    DENY 防框架嵌套、no-referrer 防引用泄漏。"""
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    return response


log = logging.getLogger(__name__)


def market_time_label(value: str | None) -> str:
    if not value:
        return "-"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return value
    market_timezone = timezone(timedelta(hours=8))
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=market_timezone)
    return instant.astimezone(market_timezone).strftime("%Y-%m-%d %H:%M")


app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=5)
app.mount("/static", StaticFiles(directory=ROOT / "static"), name="static")
def ts_fmt(value, fmt="%m-%d %H:%M"):
    """统一时间显示：绝对时间 + 悬停相对时间（替代 13 处 ts[:16] 切片）。"""
    from datetime import datetime, timezone
    if not value:
        return "—"
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return f'{dt.astimezone().strftime(fmt)}'
    except (ValueError, TypeError):
        return str(value)[:16]


templates = Jinja2Templates(directory=ROOT / "templates")
templates.env.filters["tslabel"] = ts_fmt
templates.env.globals.update(
    channels=CHANNELS, channel_for=channel_for, freshness_status=freshness_status,
    market_time_label=market_time_label,
    app_js_version=(ROOT / "static" / "app.js").stat().st_mtime_ns,
    realtime_js_version=(
        (ROOT / "static" / "realtime.js").stat().st_mtime_ns
        if (ROOT / "static" / "realtime.js").exists() else 0
    ),
)

LEGACY_PATHS = {
    "/home": "/", "/mkt": "/market/items", "/selection": "/market/items",
    "/items": "/market/items", "/market": "/research/index",
    "/section": "/research/sections", "/trend": "/research/trends",
    "/rank": "/research/rankings", "/ladders": "/research/rankings",
    "/tracker": "/monitor/radar", "/cs2/tracker": "/monitor/radar",
    "/changes": "/monitor/events", "/inventory": "/assets/inventory",
    "/inventory/estimate": "/assets/inventory",
    "/inventory/changes": "/assets/inventory/changes",
    "/inventory/history": "/assets/inventory/history",
    "/my/collect": "/assets/collection", "/hanging": "/assets/arbitrage",
    "/case": "/assets/supply", "/skindiy": "/assets/skin-diy",
    "/acquisition/progress": "/data/coverage",
    "/cs2-updates": "/data/updates",
}


@app.middleware("http")
async def canonical_pages(request: Request, call_next):
    """Keep bookmarked pages working while exposing one canonical URL per view."""
    # url.path 已做百分号解码：名字含 '#'（武器箱/音乐盒）时，解码后的 '#'
    # 拼回重定向目标会被浏览器当 fragment 截断，路由参数变空报"未找到"。
    # 必须用原始编码路径（raw_path）拼接，保持 %23 等转义原样透传。
    path = request.scope.get("raw_path")
    path = (path.decode("latin-1") if isinstance(path, bytes) else path) or request.url.path
    destination = LEGACY_PATHS.get(urllib.parse.unquote(path))
    if not destination:
        for old, new in (
            ("/item/", "/market/items/"),
            ("/cs2/", "/market/items/"),
            ("/inventory/", "/assets/inventory/"),
        ):
            if path.startswith(old):
                destination = new + path[len(old):]
                break
    if destination:
        query = request.url.query
        return RedirectResponse(destination + ("?" + query if query else ""), status_code=308)
    return await call_next(request)

# 平台画像：导购+倒余额为重点（与增量监控同一套口径）
PLATFORM_ROLES = {
    "buff": ("参考价源", "secondary"),
    "youpin": ("导购 + 租赁", "focus"),
    "c5": ("导购 + 租赁", "focus"),
    "steam": ("倒余额 + 导购", "focus"),
    "haloskins": ("海外导购", "focus"),
    "waxpeer": ("海外", "other"),
    "skinport": ("海外", "other"),
    "dmarket": ("海外", "other"),
    "csmoney": ("海外", "other"),
    "all": ("全站", "other"),
}
BROKER_PLATFORMS = ("youpin", "c5", "steam", "haloskins")
IMAGE_PROXY_HOSTS = {"img.zbt.com", "game-aesy.c5game.com"}
IMAGE_PLACEHOLDER = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="256" height="198" viewBox="0 0 256 198">'
    '<rect width="256" height="198" fill="#eef0f2"/>'
    '<path d="M90 106h76v42H90z" fill="none" stroke="#aab4bd" stroke-width="6"/>'
    '<path d="m96 138 18-18 16 14 14-12 22 16" fill="none" stroke="#7f8b96" stroke-width="6"/>'
    '<circle cx="116" cy="116" r="6" fill="#7f8b96"/>'
    '</svg>'
).encode("utf-8")
FAVICON = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">'
    '<rect width="64" height="64" fill="#14171a"/>'
    '<path d="M11 11h42v42H11z" fill="none" stroke="#69d2bd" stroke-width="3"/>'
    '<text x="32" y="39" text-anchor="middle" fill="#69d2bd" '
    'font-family="Arial,sans-serif" font-size="20" font-weight="700">C2</text>'
    '</svg>'
).encode("utf-8")
ITEM_TYPE_LABELS = {
    "CSGO_Type_Pistol": "手枪",
    "CSGO_Type_Rifle": "步枪",
    "CSGO_Type_SniperRifle": "狙击枪",
    "CSGO_Type_SMG": "冲锋枪",
    "CSGO_Type_Shotgun": "霰弹枪",
    "CSGO_Type_Machinegun": "机枪",
    "CSGO_Type_Knife": "刀具",
    "Type_Hands": "手套",
    "CSGO_Tool_Sticker": "印花",
    "CSGO_Type_Spray": "涂鸦",
    "CSGO_Type_WeaponCase": "武器箱",
    "CSGO_Type_MusicKit": "音乐盒",
    "CSGO_Type_Collectible": "收藏品",
}

SECTION_TYPE_ORDER = ("BROAD", "HOT", "LEVEL1", "LEVEL2", "LEVEL3")
SECTION_TYPE_CONFIG = {
    "BROAD": {
        "label": "大盘总览",
        "eyebrow": "Broad market",
        "title": "行情站B 大盘与板块指数",
        "description": "把大盘方向、市场宽度和热门成分放在同一张观察台中，先判断整体环境，再下钻到具体品类。",
        "sample_label": "热门成分",
    },
    "HOT": {
        "label": "热门板块",
        "eyebrow": "Hot sections",
        "title": "热门板块观察",
        "description": "按导购挂单与近期活跃度整理当前最受关注的市场成分，适合快速发现流量集中处。",
        "sample_label": "热门成分",
    },
    "LEVEL1": {
        "label": "一级板块",
        "eyebrow": "Level 1 sections",
        "title": "一级板块轮动",
        "description": "以武器、刀具、手套等一级品类组织行情，比较不同市场分组的相对强弱。",
        "sample_label": "一级板块成分",
    },
    "LEVEL2": {
        "label": "二级板块",
        "eyebrow": "Level 2 sections",
        "title": "二级板块轮动",
        "description": "把一级品类继续拆成可比较的交易主题，便于定位当前的上涨或回撤集中区。",
        "sample_label": "二级板块成分",
    },
    "LEVEL3": {
        "label": "成分饰品",
        "eyebrow": "Constituents",
        "title": "成分饰品观察",
        "description": "直接查看板块内的饰品快照、导购挂单和价格偏离，点击任意标的进入完整详情。",
        "sample_label": "成分饰品",
    },
}
PERIOD_LABELS = {
    "1day": "日线",
    "7day": "7 天",
    "15day": "15 天",
    "30day": "30 天",
}
PERIOD_DAYS = {
    "1day": 1,
    "7day": 7,
    "15day": 15,
    "30day": 30,
}
SECTION_PATHS = {
    "HOT": "hot",
    "LEVEL1": "itemTypeLevel1",
    "LEVEL2": "itemTypeLevel2",
    "LEVEL3": "itemTypeLevel3",
}
PLATFORM_LABELS = {
    "buff": "BUFF",
    "youpin": "悠悠",
    "c5": "C5",
    "steam": "Steam",
    "haloskins": "Halo",
    "waxpeer": "Waxpeer",
    "skinport": "Skinport",
    "dmarket": "DMarket",
    "csmoney": "CS.MONEY",
}


@contextmanager
def db():
    database = DB.resolve()
    control_database = CONTROL_DB.resolve()
    if (
        "CSGO_MONITOR_CONTROL_DATABASE" not in os.environ
        and control_database.parent != database.parent
    ):
        control_database = database
    if not control_database.exists():
        control_database = database
    conn = sqlite3.connect(
        database.as_uri() + "?mode=ro",
        uri=True,
        timeout=15,
    )
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA cache_size=-65536")
        conn.execute("PRAGMA mmap_size=1073741824")
        if control_database != database:
            conn.execute(
                "ATTACH DATABASE ? AS control",
                (control_database.as_uri() + "?mode=ro",),
            )
        conn.execute("PRAGMA query_only=ON")
        yield conn
    finally:
        conn.close()


def _control_table(conn: sqlite3.Connection, name: str) -> str:
    schemas = {
        str(row[1])
        for row in conn.execute("PRAGMA database_list").fetchall()
    }
    return f"control.{name}" if "control" in schemas else name


def open_id(market_hash_name: str) -> str:
    digest = hashlib.sha256(market_hash_name.encode("utf-8")).hexdigest()[:24]
    return f"open:{digest}"


def safe_return_to(value: str | None, fallback: str = "/items") -> str:
    """Keep internal return links while dropping external or malformed targets."""
    raw = str(value or fallback)
    parsed = urllib.parse.urlsplit(raw)
    if (
        not raw.startswith("/")
        or raw.startswith("//")
        or parsed.scheme
        or parsed.netloc
    ):
        return fallback
    path = parsed.path or "/"
    return urllib.parse.urlunsplit(("", "", path, parsed.query, ""))


def numeric_ids(conn: sqlite3.Connection, market_hash_name: str) -> list[str]:
    return [str(r["item_id"]) for r in conn.execute(
        """SELECT item_id FROM items WHERE source='siteb' AND market_hash_name=?
            AND item_id NOT LIKE 'open:%' AND item_id NOT LIKE 'sitemap:%'""",
        (market_hash_name,),
    )]


def item_ids(conn: sqlite3.Connection, market_hash_name: str) -> list[str]:
    """All local 行情站B identifiers for a market hash name, including its open id."""
    oid = open_id(market_hash_name)
    return [oid, *[
        str(row["item_id"]) for row in conn.execute(
            """SELECT item_id FROM items WHERE source='siteb' AND market_hash_name=?
                AND item_id<>?""",
            (market_hash_name, oid),
        )
    ]]


def _item_availability(conn: sqlite3.Connection, market_hash_name: str) -> dict[str, Any]:
    """Return fast, item-scoped data coverage without scanning global series."""
    oid = open_id(market_hash_name)
    catalog_row = conn.execute(
        """SELECT 1 FROM items WHERE source='siteb' AND item_id=?
            AND market_hash_name=? LIMIT 1""",
        (oid, market_hash_name),
    ).fetchone()
    ids = item_ids(conn, market_hash_name)
    marks = _marks(ids)
    kline = conn.execute(
        f"""SELECT COUNT(DISTINCT ts) points, MIN(ts) earliest, MAX(ts) latest
            FROM series INDEXED BY idx_series_dataset_item
            WHERE source='siteb' AND dataset='item_kline'
              AND item_id IN ({marks}) AND metric='ohlc' AND platform='all'""",
        ids,
    ).fetchone()
    platform = conn.execute(
        f"""SELECT COUNT(DISTINCT platform) platforms, COUNT(DISTINCT ts) snapshots,
                   MIN(ts) earliest, MAX(ts) latest
            FROM series INDEXED BY idx_series_dataset_item
            WHERE source='siteb' AND dataset='platform_snapshot'
              AND item_id IN ({marks}) AND metric='sell_price'""",
        ids,
    ).fetchone()
    numeric = numeric_ids(conn, market_hash_name)
    history = None
    if numeric:
        numeric_marks = _marks(numeric)
        history = conn.execute(
            f"""SELECT COUNT(*) points, COUNT(DISTINCT metric) metrics,
                       MIN(ts) earliest, MAX(ts) latest
                FROM series INDEXED BY idx_series_dataset_item
                WHERE source='siteb' AND dataset='item_history'
                  AND item_id IN ({numeric_marks}) AND platform='all'""",
            numeric,
        ).fetchone()
    failure_table = _control_table(conn, "crawl_failures")
    failure = conn.execute(
        f"""SELECT job,last_error,updated_at FROM {failure_table}
            WHERE item_id=? AND resolved=0 AND job LIKE 'siteb_api_histories%'
            ORDER BY updated_at DESC LIMIT 1""",
        (oid,),
    ).fetchone()

    kline_points = int(kline["points"] or 0)
    platform_count = int(platform["platforms"] or 0)
    platform_snapshots = int(platform["snapshots"] or 0)
    history_points = int(history["points"] or 0) if history else 0
    history_metrics = int(history["metrics"] or 0) if history else 0
    pending_reason = str(failure["last_error"] or "") if failure else ""
    if "106" in pending_reason:
        pending_reason = "行情站B 今日公开访问额度已满，等待额度重置后续采"
    elif "kline-quota" in pending_reason:
        pending_reason = "行情站B K 线额度暂满，站点历史任务仍会继续"
    elif "network-fuse" in pending_reason:
        pending_reason = "采集网络暂时熔断，任务恢复后会自动续采"
    elif pending_reason:
        pending_reason = "站点历史已进入重试队列"
    else:
        pending_reason = "站点买卖历史仍在分区回填"

    return {
        "name": market_hash_name,
        "catalog": {
            "available": bool(catalog_row),
            "status": "ready" if catalog_row else "missing",
        },
        "kline": {
            "available": kline_points > 0,
            "status": "ready" if kline_points > 0 else "collecting",
            "points": kline_points,
            "earliest": kline["earliest"],
            "latest": kline["latest"],
        },
        "platforms": {
            "available": platform_count > 0,
            "status": "ready" if platform_count > 0 else "collecting",
            "count": platform_count,
            "snapshots": platform_snapshots,
            "earliest": platform["earliest"],
            "latest": platform["latest"],
        },
        "site_history": {
            "available": history_points > 0,
            "status": "ready" if history_points > 0 else "collecting",
            "points": history_points,
            "metrics": history_metrics,
            "earliest": history["earliest"] if history else None,
            "latest": history["latest"] if history else None,
            "numeric_id_ready": bool(numeric),
            "reason": "" if history_points > 0 else pending_reason,
            "retry_at": failure["updated_at"] if failure else None,
        },
    }


def _progress_task(
    key: str,
    label: str,
    current: int,
    total: int,
    *,
    updated_at: str | None = None,
    unresolved: int = 0,
    blocker: str = "",
    note: str = "",
) -> dict[str, Any]:
    current = max(0, min(int(current), int(total))) if total else max(0, int(current))
    if total and current >= total and unresolved == 0:
        status = "complete"
    elif current > 0:
        status = "running"
    else:
        status = "pending"
    return {
        "key": key,
        "label": label,
        "current": current,
        "total": total,
        "remaining": max(total - current, 0),
        "percent": round(current * 100 / total, 2) if total else 0,
        "status": status,
        "updated_at": updated_at,
        "unresolved": unresolved,
        "blocker": blocker,
        "note": note,
    }


def _quota_label(detail: str) -> str:
    if "quota-exhausted" in detail or "106" in detail:
        return "行情站B 今日公开访问额度已满"
    if "kline-quota" in detail:
        return "行情站B K 线接口额度暂满"
    if "network-fuse" in detail:
        return "采集网络暂时熔断"
    return ""


def _acquisition_progress(conn: sqlite3.Connection) -> dict[str, Any]:
    """Read crawler progress from compact state tables only."""
    state_table = _control_table(conn, "crawl_state")
    failure_table = _control_table(conn, "crawl_failures")
    coverage_table = _control_table(conn, "coverage")
    state_rows = conn.execute(
        f"""SELECT job,cursor,updated_at,detail_json FROM {state_table}
            WHERE job IN (
              'sitea_api_detail_p0','sitea_api_detail_p1',
              'siteb_open_histories_kline','siteb_api_extended',
              'siteb_api_histories_b','siteb_api_histories_c',
              'siteb_api_histories_d','siteb_site_quota_until',
              'runtime:pipeline_status:v1'
            )"""
    ).fetchall()
    states = {str(row["job"]): row for row in state_rows}
    failure_rows = conn.execute(
        f"""SELECT job,COUNT(*) unresolved,
                  SUM(CASE WHEN last_error LIKE '%106%'
                                OR lower(last_error) LIKE '%quota%'
                           THEN 1 ELSE 0 END) quota_deferred
            FROM {failure_table}
            WHERE resolved=0 AND job IN (
              'sitea_api_detail_p0','sitea_api_detail_p1',
              'siteb_api_extended','siteb_api_histories_b',
              'siteb_api_histories_c','siteb_api_histories_d'
            ) GROUP BY job"""
    ).fetchall()
    failures = {str(row["job"]): int(row["unresolved"] or 0) for row in failure_rows}
    quota_failures = {
        str(row["job"]): int(row["quota_deferred"] or 0)
        for row in failure_rows
    }
    coverage_rows = conn.execute(
        f"""SELECT source,dataset,record_count,checked_at,note FROM {coverage_table}
            WHERE (source='siteb' AND dataset='item_catalog_open_api')
               OR (source='sitea' AND dataset IN ('api.detail_p0','api.detail_p1'))"""
    ).fetchall()
    coverage = {
        (str(row["source"]), str(row["dataset"])): row
        for row in coverage_rows
    }
    catalog_row = coverage.get(("siteb", "item_catalog_open_api"))
    catalog_total = int(catalog_row["record_count"] or 0) if catalog_row else 0

    def cursor(job: str) -> int:
        row = states.get(job)
        try:
            return int(row["cursor"] or 0) if row else 0
        except (TypeError, ValueError):
            return 0

    def updated(job: str) -> str | None:
        row = states.get(job)
        return str(row["updated_at"]) if row and row["updated_at"] else None

    def detail(job: str) -> str:
        row = states.get(job)
        if not row or not row["detail_json"]:
            return ""
        try:
            payload = json.loads(str(row["detail_json"]))
        except (TypeError, ValueError):
            return ""
        if not isinstance(payload, dict):
            return ""
        return str(payload.get("detail") or payload.get("error") or "")

    snapshot_row = states.get(PIPELINE_STATUS_JOB)
    if snapshot_row and snapshot_row["detail_json"]:
        try:
            snapshot = json.loads(str(snapshot_row["detail_json"]))
        except (TypeError, ValueError):
            snapshot = {}
        pipeline_rows = (
            snapshot.get("pipelines", {})
            if isinstance(snapshot, dict)
            else {}
        )
        required = {
            "sitea_detail",
            "siteb_open_kline",
            "siteb_site_history",
            "siteb_extended",
        }
        if isinstance(pipeline_rows, dict) and required <= set(pipeline_rows):
            quota = snapshot.get("quota", {})
            quota_blocker = ""
            if isinstance(quota, dict) and quota.get("active"):
                try:
                    resume_at = datetime.fromisoformat(
                        str(quota.get("resume_at") or "")
                    ).astimezone()
                    quota_blocker = (
                        "行情站B 公开访问额度暂停至 "
                        + resume_at.strftime("%Y-%m-%d %H:%M")
                    )
                except ValueError:
                    quota_blocker = "行情站B 公开访问额度当前暂停"

            def pipeline_task(
                key: str,
                label: str,
                *,
                blocker: str = "",
            ) -> dict[str, Any]:
                row = pipeline_rows[key]
                failures = row.get("failures", {})
                terminal = int(failures.get("terminal") or 0)
                note = (
                    f"模块完成：{json.dumps(row.get('module_done', {}), ensure_ascii=False)}"
                )
                if terminal:
                    note += f"；{terminal} 条已确认数据限制"
                if row.get("refresh_due"):
                    note += f"；{int(row['refresh_due'])} 条待刷新"
                if row.get("unverified_empty"):
                    note += f"；{int(row['unverified_empty'])} 条空响应待核实"
                task = _progress_task(
                    key,
                    label,
                    int(row.get("completed") or 0),
                    int(row.get("catalog") or 0),
                    updated_at=row.get("updated_at"),
                    unresolved=int(failures.get("actionable") or 0),
                    blocker=blocker,
                    note=note,
                )
                task["status"] = str(row.get("status") or task["status"])
                return task

            siteb_total = int(
                pipeline_rows["siteb_open_kline"].get("catalog") or 0
            )
            tasks = [
                pipeline_task("sitea_detail", "行情站A 全量详情"),
                _progress_task(
                    "siteb_catalog",
                    "行情站B 饰品目录",
                    siteb_total,
                    siteb_total,
                    updated_at=(
                        str(catalog_row["checked_at"])
                        if catalog_row
                        else None
                    ),
                ),
                pipeline_task(
                    "siteb_open_kline",
                    "行情站B 公开日 K 扫描",
                ),
                pipeline_task(
                    "siteb_extended",
                    "行情站B 扩展详情",
                    blocker=quota_blocker,
                ),
                pipeline_task(
                    "siteb_site_history",
                    "行情站B 站点历史扫描",
                    blocker=quota_blocker,
                ),
            ]
            return {
                "generated_at": datetime.now().astimezone().isoformat(),
                "catalog_items": siteb_total,
                "all_complete": all(
                    task["status"] in {"complete", "limited"}
                    for task in tasks
                ),
                "tasks": tasks,
            }

    sitea_jobs = ("sitea_api_detail_p0", "sitea_api_detail_p1")
    sitea_total = 0
    for dataset in ("api.detail_p0", "api.detail_p1"):
        row = coverage.get(("sitea", dataset))
        match = re.search(r"cursor=\d+/(\d+)", str(row["note"] or "")) if row else None
        sitea_total += int(match.group(1)) if match else 0
    sitea_total = sitea_total or 26090
    sitea_attempted = sum(cursor(job) for job in sitea_jobs)
    sitea_unresolved = sum(failures.get(job, 0) for job in sitea_jobs)
    sitea_ready = max(0, min(sitea_total, sitea_attempted) - sitea_unresolved)

    history_jobs = (
        "siteb_api_histories_b",
        "siteb_api_histories_c",
        "siteb_api_histories_d",
    )
    history_scanned = min(sum(cursor(job) for job in history_jobs), catalog_total)
    history_quota_deferred = sum(quota_failures.get(job, 0) for job in history_jobs)
    history_unresolved = max(
        0,
        sum(failures.get(job, 0) for job in history_jobs)
        - history_quota_deferred,
    )
    extended_quota_deferred = quota_failures.get("siteb_api_extended", 0)
    extended_unresolved = max(
        0,
        failures.get("siteb_api_extended", 0) - extended_quota_deferred,
    )
    blockers = [label for label in (_quota_label(detail(job)) for job in history_jobs) if label]
    quota_until = cursor("siteb_site_quota_until")
    shared_quota_blocker = ""
    if quota_until > datetime.now().timestamp():
        resume_at = datetime.fromtimestamp(quota_until).astimezone()
        shared_quota_blocker = (
            "行情站B 公开访问额度暂停至 "
            + resume_at.strftime("%Y-%m-%d %H:%M")
        )
        blockers = [
            shared_quota_blocker,
            *[
                label for label in blockers
                if label != "行情站B 今日公开访问额度已满"
            ],
        ]
    history_stamps = [updated(job) for job in history_jobs]
    sitea_stamps = [updated(job) for job in sitea_jobs]
    tasks = [
        _progress_task(
            "sitea_detail",
            "行情站A 全量详情",
            sitea_ready,
            sitea_total,
            updated_at=max((stamp for stamp in sitea_stamps if stamp), default=None),
            unresolved=sitea_unresolved,
            note="完成数已扣除待重试请求",
        ),
        _progress_task(
            "siteb_catalog",
            "行情站B 饰品目录",
            catalog_total,
            catalog_total,
            updated_at=str(catalog_row["checked_at"]) if catalog_row else None,
        ),
        _progress_task(
            "siteb_kline",
            "行情站B 公开日 K 扫描",
            cursor("siteb_open_histories_kline"),
            catalog_total,
            updated_at=updated("siteb_open_histories_kline"),
            note="已请求全部目录；源站未返回行情的条目仍可能为空",
        ),
        _progress_task(
            "siteb_extended",
            "行情站B 扩展详情",
            cursor("siteb_api_extended"),
            catalog_total,
            updated_at=updated("siteb_api_extended"),
            unresolved=extended_unresolved,
            blocker=shared_quota_blocker or _quota_label(
                detail("siteb_api_extended")
            ),
            note=(
                f"{extended_quota_deferred} 条配额失败已延后到额度恢复后重试"
                if extended_quota_deferred else ""
            ),
        ),
        _progress_task(
            "siteb_site_history",
            "行情站B 站点历史扫描",
            history_scanned,
            catalog_total,
            updated_at=max((stamp for stamp in history_stamps if stamp), default=None),
            unresolved=history_unresolved,
            blocker="；".join(dict.fromkeys(blockers)),
            note=(
                "扫描位置不等于成功回填数；"
                f"{history_quota_deferred} 条配额失败已延后，"
                "其余失败项进入重试队列"
            ),
        ),
    ]
    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "catalog_items": catalog_total,
        "all_complete": all(task["status"] == "complete" for task in tasks),
        "tasks": tasks,
    }


def _runtime_status(conn: sqlite3.Connection) -> dict[str, Any]:
    now = datetime.now().astimezone()
    heartbeat_table = _control_table(conn, "worker_heartbeats")
    route_table = _control_table(conn, "route_health_state")
    run_table = _control_table(conn, "task_runs")
    failure_table = _control_table(conn, "crawl_failures")
    lease_table = _control_table(conn, "task_leases")
    slice_table = _control_table(conn, "task_slices")
    state_table = _control_table(conn, "crawl_state")
    heartbeat_rows = conn.execute(
        f"""SELECT worker_id,task_id,pid,status,route,owner_id,lease_until,
                  started_at,
                  heartbeat_at,detail_json
           FROM {heartbeat_table} ORDER BY task_id,worker_id"""
    ).fetchall()
    heartbeats = []
    for row in heartbeat_rows:
        heartbeat_at = str(row["heartbeat_at"] or "")
        stale = False
        try:
            timestamp = datetime.fromisoformat(heartbeat_at)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=now.tzinfo)
            stale = (now - timestamp).total_seconds() > 90
        except ValueError:
            stale = True
        heartbeats.append(
            {
                "worker_id": str(row["worker_id"]),
                "task_id": str(row["task_id"]),
                "pid": row["pid"],
                "status": str(row["status"]),
                "route": str(row["route"] or ""),
                "owner_id": str(row["owner_id"] or ""),
                "lease_until": row["lease_until"],
                "started_at": row["started_at"],
                "heartbeat_at": heartbeat_at,
                "stale": stale,
            }
        )
    route_rows = conn.execute(
        f"""SELECT route_class,route,status,latency_ms,error_class,note,
                  consecutive_failures,checked_at,cooldown_until
           FROM {route_table}
           ORDER BY route_class,route"""
    ).fetchall()
    routes = [dict(row) for row in route_rows]
    run_rows = conn.execute(
        f"""SELECT run_id,task_id,route,status,started_at,finished_at,
                  exit_code,elapsed_seconds,error_class,result_json
           FROM {run_table}
           WHERE rowid IN (
             SELECT MAX(rowid) FROM {run_table} GROUP BY task_id
           )
           ORDER BY task_id"""
    ).fetchall()
    runs = [dict(row) for row in run_rows]
    failure_rows = conn.execute(
        f"""SELECT error_class,failure_status,COUNT(*) count
           FROM {failure_table} WHERE resolved=0
           GROUP BY error_class,failure_status
           ORDER BY count DESC,error_class"""
    ).fetchall()
    failures = [dict(row) for row in failure_rows]
    leases = [
        dict(row)
        for row in conn.execute(
            f"""SELECT task_id,owner_id,run_id,pid,status,acquired_at,
            lease_until,updated_at FROM {lease_table} ORDER BY task_id"""
        ).fetchall()
    ]
    slices = [
        dict(row)
        for row in conn.execute(
            f"""SELECT task_id,status,COALESCE(outcome,'') outcome,
            COUNT(*) count,MAX(updated_at) updated_at
            FROM {slice_table}
            GROUP BY task_id,status,COALESCE(outcome,'')
            ORDER BY task_id,status,outcome"""
        ).fetchall()
    ]
    quota_row = conn.execute(
        f"""SELECT cursor,updated_at,detail_json FROM {state_table}
        WHERE job='siteb_site_quota_until'"""
    ).fetchone()
    try:
        quota_until = float(quota_row["cursor"] or 0) if quota_row else 0.0
    except (TypeError, ValueError):
        quota_until = 0.0
    quota = {
        "active": quota_until > now.timestamp(),
        "resume_timestamp": int(quota_until) if quota_until else 0,
        "resume_at": (
            datetime.fromtimestamp(quota_until).astimezone().isoformat()
            if quota_until
            else None
        ),
        "updated_at": quota_row["updated_at"] if quota_row else None,
    }
    return {
        "generated_at": now.isoformat(),
        "heartbeats": heartbeats,
        "routes": routes,
        "latest_runs": runs,
        "failures": failures,
        "leases": leases,
        "slices": slices,
        "quota": quota,
        "healthy_workers": sum(
            not item["stale"] and item["status"] == "running"
            for item in heartbeats
        ),
        "stale_workers": sum(item["stale"] for item in heartbeats),
    }


def _marks(values: list[str]) -> str:
    return ",".join("?" for _ in values)


def _profile_from_raw(name: str, rows: list[sqlite3.Row]) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        try:
            raw = json.loads(str(row["raw_json"] or "{}"))
        except (TypeError, ValueError):
            continue
        if isinstance(raw, dict):
            candidates.append(raw)
    rich = max(
        candidates,
        key=lambda raw: (
            bool(raw.get("imageUrl")),
            bool(raw.get("rarityName")),
            len(raw),
        ),
        default={},
    )
    item_type = str(rich.get("itemType") or "")
    analysis_tags = []
    for entry in rich.get("analysisTags") or []:
        if not isinstance(entry, dict):
            continue
        label = re.sub(r"<[^>]+>", "", str(entry.get("style") or entry.get("key") or ""))
        label = html.unescape(label).strip()
        if label:
            analysis_tags.append({
                "label": label,
                "description": str(entry.get("desc") or ""),
                "color": str(entry.get("background") or "#748091"),
            })
    return {
        "market_hash_name": name,
        "market_short_name": rich.get("marketShortName") or "",
        "short_name": rich.get("shortName") or "",
        "image_url": rich.get("imageUrl") or "",
        "rarity": rich.get("rarityName") or "",
        "rarity_color": rich.get("rarityColor") or "#808793",
        "quality": rich.get("qualityName") or "",
        "quality_color": rich.get("qualityColor") or "#808793",
        "exterior": rich.get("exteriorName") or "",
        "exterior_color": rich.get("exteriorColor") or "#d5a354",
        "item_type": item_type,
        "item_type_label": ITEM_TYPE_LABELS.get(item_type, "其他"),
        "survive_num": rich.get("surviveNum"),
        "holders_num": rich.get("holdersNum"),
        "transaction_count": rich.get("transactionCount"),
        "turnover_rate": rich.get("turnoverRate"),
        "volume_ratio": rich.get("volumeRatio"),
        "diff_1d": rich.get("diff1Day"),
        "diff_7d": rich.get("diff7Day"),
        "diff_30d": rich.get("diff30Day"),
        "diff_6m": rich.get("diff6Month"),
        "analysis_tags": analysis_tags,
        "platform_links": {
            str(entry.get("platform") or ""): entry.get("link")
            for entry in rich.get("sellingPriceList") or []
            if isinstance(entry, dict) and entry.get("platform")
        },
    }


def item_profiles(conn: sqlite3.Connection, names: list[str]) -> dict[str, dict[str, Any]]:
    names = list(dict.fromkeys(name for name in names if name))
    if not names:
        return {}
    rows = conn.execute(
        f"""SELECT market_hash_name, raw_json FROM items
            WHERE source='siteb' AND market_hash_name IN ({_marks(names)})""",
        names,
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["market_hash_name"]), []).append(row)
    return {name: _profile_from_raw(name, grouped.get(name, [])) for name in names}


def _items_browser_metadata() -> dict[str, Any]:
    """Cache the catalog facets shared by every screener request."""
    def load() -> dict[str, Any]:
        with db() as conn:
            style_names = {
                str(row[0]) for row in conn.execute(
                    """SELECT market_hash_name FROM items WHERE source='siteb'
                       AND (market_hash_name LIKE '%Doppler%'
                            OR market_hash_name LIKE '%Crimson Kimono%')"""
                )
            }
            type_options = [
                dict(row) for row in conn.execute(
                    """SELECT json_extract(raw_json, '$.itemType') value, COUNT(*) count
                       FROM items WHERE source='siteb'
                         AND json_extract(raw_json, '$.itemType') IS NOT NULL
                       GROUP BY value ORDER BY count DESC LIMIT 18"""
                )
            ]
            rarity_options = [
                dict(row) for row in conn.execute(
                    """SELECT json_extract(raw_json, '$.rarityName') value, COUNT(*) count
                       FROM items WHERE source='siteb'
                         AND json_extract(raw_json, '$.rarityName') IS NOT NULL
                       GROUP BY value ORDER BY count DESC"""
                )
            ]
            exterior_options = [
                dict(row) for row in conn.execute(
                    """SELECT json_extract(raw_json, '$.exteriorName') value, COUNT(*) count
                       FROM items WHERE source='siteb'
                         AND json_extract(raw_json, '$.exteriorName') IS NOT NULL
                       GROUP BY value ORDER BY count DESC"""
                )
            ]
        return {
            "style_names": style_names,
            "type_options": type_options,
            "rarity_options": rarity_options,
            "exterior_options": exterior_options,
        }

    return cached("items_browser_metadata", 600, load)


def related_items(
    conn: sqlite3.Connection,
    name: str,
    profile: dict[str, Any],
    limit: int = 12,
) -> list[dict[str, Any]]:
    """Return 行情站B-style wear/category siblings for the detail workspace."""
    market_short_name = str(profile.get("market_short_name") or "").strip()
    rows: list[sqlite3.Row]
    if market_short_name:
        rows = conn.execute(
            """SELECT DISTINCT market_hash_name
                FROM items
                WHERE source='siteb'
                  AND item_id NOT LIKE 'open:%'
                  AND item_id NOT LIKE 'sitemap:%'
                  AND json_extract(raw_json, '$.marketShortName')=?
                ORDER BY CASE json_extract(raw_json, '$.exteriorName')
                    WHEN '崭新出厂' THEN 1
                    WHEN '略有磨损' THEN 2
                    WHEN '久经沙场' THEN 3
                    WHEN '破损不堪' THEN 4
                    WHEN '战痕累累' THEN 5
                    ELSE 6 END, market_hash_name
                LIMIT ?""",
            (market_short_name, limit),
        ).fetchall()
    else:
        base_name = re.sub(r"\s*\([^()]+\)\s*$", "", name).strip()
        rows = conn.execute(
            """SELECT DISTINCT market_hash_name
                FROM items
                WHERE source='siteb' AND item_id LIKE 'open:%'
                  AND market_hash_name LIKE ?
                ORDER BY market_hash_name LIMIT ?""",
            (f"{base_name}%", limit),
        ).fetchall()
    names = [str(row["market_hash_name"]) for row in rows]
    if name not in names:
        names.insert(0, name)
    profiles = item_profiles(conn, names)
    return [{
        "name": sibling,
        "active": sibling == name,
        "profile": profiles.get(sibling, {}),
    } for sibling in names]


def _ema(values: list[float], period: int) -> list[float | None]:
    alpha = 2 / (period + 1)
    current: float | None = None
    result: list[float | None] = []
    for value in values:
        current = value if current is None else alpha * value + (1 - alpha) * current
        result.append(current)
    return result


def _attach_indicators(points: list[dict[str, Any]]) -> None:
    """Attach chart-aligned MA/BOLL/MACD/RSI values to K-line points."""
    if not points:
        return
    closes = [float(point["close"]) for point in points]
    ema12 = _ema(closes, 12)
    ema26 = _ema(closes, 26)
    macd = [(a or 0) - (b or 0) for a, b in zip(ema12, ema26)]
    signal = _ema(macd, 9)

    for index, point in enumerate(points):
        sma7 = sma30 = std30 = None
        if index >= 6:
            window = closes[index - 6:index + 1]
            sma7 = sum(window) / 7
        if index >= 29:
            window = closes[index - 29:index + 1]
            sma30 = sum(window) / 30
            std30 = math.sqrt(sum((value - sma30) ** 2 for value in window) / 30)

        rsi = None
        if index >= 14:
            changes = [closes[pos] - closes[pos - 1] for pos in range(index - 13, index + 1)]
            gains = sum(max(change, 0) for change in changes) / 14
            losses = sum(max(-change, 0) for change in changes) / 14
            rsi = 100.0 if losses == 0 else 100 - (100 / (1 + gains / losses))

        previous = closes[index - 1] if index else 0
        point.update({
            "sma7": sma7,
            "sma30": sma30,
            "ema12": ema12[index],
            "ema26": ema26[index],
            "macd": macd[index],
            "macd_signal": signal[index],
            "rsi14": rsi,
            "bollinger_mid": sma30,
            "bollinger_upper": sma30 + 2 * std30 if sma30 is not None and std30 is not None else None,
            "bollinger_lower": sma30 - 2 * std30 if sma30 is not None and std30 is not None else None,
            "return_1d": None if not previous else closes[index] / previous - 1,
            "volatility30": (
                std30 / sma30
                if sma30 is not None and sma30 != 0 and std30 is not None
                else None
            ),
        })


def _item_series_rows(
    conn: sqlite3.Connection,
    dataset: str,
    name: str,
    *,
    metrics: tuple[str, ...],
    limit: int,
) -> list[sqlite3.Row]:
    ids = item_ids(conn, name)
    if not ids:
        return []
    marks = _marks(ids)
    metric_marks = _marks(list(metrics))
    return conn.execute(
        f"""SELECT item_id, platform, metric, close, ts
            FROM series INDEXED BY idx_series_dataset_item
            WHERE source='siteb' AND dataset=?
              AND item_id IN ({marks}) AND metric IN ({metric_marks})
            ORDER BY ts DESC LIMIT ?""",
        [dataset, *ids, *metrics, limit],
    ).fetchall()


def _item_kline_points(
    conn: sqlite3.Connection,
    name: str,
    style: str,
    limit: int,
) -> list[dict[str, Any]]:
    ids = item_ids(conn, name)
    if not ids:
        return []
    marks = _marks(ids)
    rows = conn.execute(
        f"""SELECT item_id, ts, open, high, low, close, volume, turnover
            FROM series INDEXED BY idx_series_dataset_item
            WHERE dataset='item_kline' AND source='siteb'
              AND item_id IN ({marks}) AND metric='ohlc' AND platform=?
            ORDER BY ts DESC LIMIT ?""",
        [*ids, style.lower(), limit * max(1, len(ids))],
    ).fetchall()
    by_ts: dict[str, dict[str, Any]] = {}
    for row in rows:
        point = dict(row)
        ts = str(point["ts"])
        previous = by_ts.get(ts)
        if previous is None:
            by_ts[ts] = point
            continue
        current_score = sum(point.get(field) is not None for field in ("open", "high", "low", "close", "volume"))
        previous_score = sum(previous.get(field) is not None for field in ("open", "high", "low", "close", "volume"))
        if current_score > previous_score:
            by_ts[ts] = point
    points = sorted(by_ts.values(), key=lambda point: str(point["ts"]))
    return points[-limit:]


def _item_site_history_points(
    conn: sqlite3.Connection,
    name: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Actual listed-price observations from the site's all-time trend."""
    ids = numeric_ids(conn, name)
    if not ids:
        return []
    marks = _marks(ids)
    rows = conn.execute(
        f"""SELECT ts, close FROM series INDEXED BY idx_series_dataset_item
            WHERE source='siteb' AND dataset='item_history'
              AND item_id IN ({marks}) AND platform='all'
              AND metric='sell_price' AND close IS NOT NULL
            ORDER BY ts DESC LIMIT ?""",
        [*ids, limit * max(1, len(ids))],
    ).fetchall()
    by_ts = {str(row["ts"]): {"ts": row["ts"], "close": row["close"]}
             for row in rows}
    return [by_ts[ts] for ts in sorted(by_ts)][-limit:]


def _prioritize_viewed_site_page(name: str) -> None:
    """Place a viewed item ahead of the sitemap backlog for page capture."""
    if DB.resolve().parent != CONTROL_DB.resolve().parent or not CONTROL_DB.exists():
        return
    url = "https://www.siteb.com/cs2/" + urllib.parse.quote(name, safe="-._~")
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    try:
        with closing(sqlite3.connect(CONTROL_DB, timeout=5)) as connection:
            with connection:
                connection.execute(
                    """INSERT OR IGNORE INTO public_pages
                        (source,url,status,priority,discovered_from)
                        VALUES ('siteb',?,'ready',-20,'viewed_item')""",
                    (url,),
                )
                connection.execute(
                    """UPDATE public_pages SET priority=MIN(priority,-20),
                        status=CASE WHEN status='done' AND
                            (last_checked IS NULL OR last_checked<=?)
                            THEN 'ready' ELSE status END,
                        next_due=CASE WHEN status='done' AND
                            (last_checked IS NULL OR last_checked<=?)
                            THEN NULL ELSE next_due END
                        WHERE source='siteb' AND url=? AND status<>'running'""",
                    (cutoff, cutoff, url),
                )
    except sqlite3.Error as exc:
        log.warning("Unable to prioritize viewed 行情站B page %s: %s", name, exc)


def _item_platform_chart_points(
    conn: sqlite3.Connection,
    name: str,
    platform: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Return real platform snapshots as a price line, never as synthetic OHLC."""
    ids = item_ids(conn, name)
    if not ids:
        return []
    marks = _marks(ids)
    rows = conn.execute(
        f"""SELECT ts, metric, close
            FROM series INDEXED BY idx_series_dataset_item
            WHERE dataset='platform_snapshot' AND source='siteb'
              AND item_id IN ({marks}) AND platform=?
              AND metric IN ('sell_price', 'sell_num')
            ORDER BY ts DESC LIMIT ?""",
        [*ids, platform.lower(), limit * 4],
    ).fetchall()
    by_ts: dict[str, dict[str, Any]] = {}
    for row in rows:
        ts = str(row["ts"])
        point = by_ts.setdefault(ts, {"ts": ts})
        metric = str(row["metric"])
        if metric == "sell_price" and point.get("close") is None:
            point["close"] = row["close"]
        elif metric == "sell_num" and point.get("listing_count") is None:
            point["listing_count"] = row["close"]
    points = [
        point for point in sorted(by_ts.values(), key=lambda item: str(item["ts"]))
        if point.get("close") is not None
    ]
    return points[-limit:]


def latest_per_platform(rows: list[sqlite3.Row]) -> dict[str, float]:
    result: dict[str, float] = {}
    for row in rows:
        platform = str(row["platform"] or "")
        if platform not in result:
            result[platform] = row["close"]
    return result


def role_of(platform: str) -> tuple[str, str]:
    return PLATFORM_ROLES.get(platform, ("", "other"))

# 模板全局：平台画像（label, css-class），供 tracker 等模板渲染角色徽章
templates.env.globals["platform_role"] = role_of


def wear_label(name: str) -> str:
    """提取饰品名末尾的磨损/版本括号，用于磨损变体导航卡。"""
    match = re.search(r"\(([^()]*)\)\s*$", str(name or ""))
    return match.group(1).strip() if match and match.group(1).strip() else str(name or "")


templates.env.filters["wear"] = wear_label


def ts_label(value: Any) -> str:
    """ISO 时间戳 -> '10-10 14:00'（北京时间）；无效输入原样返回。"""
    text = str(value or "").strip()
    if not text:
        return "-"
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    local = parsed.astimezone()
    return local.strftime("%m-%d %H:%M")


templates.env.filters["tslabel"] = ts_label


def metric_number(value: Any, decimals: int = 0) -> str:
    """Format dashboard counts and amounts without changing stored values."""
    if value is None or value == "":
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    places = max(0, int(decimals))
    return f"{number:,.{places}f}"


templates.env.filters["metric"] = metric_number


# --------------------------------------------------------------------------
# 聚合数据（进程内缓存，供页面与 API 共用）
# --------------------------------------------------------------------------

def _latest_by(conn, dataset: str, metric: str) -> list[sqlite3.Row]:
    """每个 item/platform 最新一条 close（走 idx_series_snap_latest 索引）。"""
    return conn.execute(
        """SELECT s.item_id, s.platform, s.close, s.ts FROM series s
            JOIN (SELECT item_id, platform, MAX(ts) ts FROM series
                  WHERE dataset=? AND source='siteb' AND metric=?
                    AND item_id LIKE 'open:%'
                  GROUP BY item_id, platform) m
              ON s.item_id=m.item_id AND s.platform=m.platform AND s.ts=m.ts
            WHERE s.dataset=? AND s.source='siteb' AND s.metric=?
              AND s.item_id LIKE 'open:%'""",
        (dataset, metric, dataset, metric),
    ).fetchall()


def _names_map(conn, item_ids: set[str]) -> dict[str, str]:
    if not item_ids:
        return {}
    marks = ",".join("?" for _ in item_ids)
    rows = conn.execute(
        f"""SELECT item_id, market_hash_name FROM items WHERE source='siteb'
            AND item_id IN ({marks})""", list(item_ids)
    ).fetchall()
    return {str(r["item_id"]): str(r["market_hash_name"]) for r in rows}


def _display_names_map(conn, item_ids: set[str]) -> dict[str, str]:
    """Resolve both open catalog ids and numeric detail ids to display names."""
    if not item_ids:
        return {}
    marks = ",".join("?" for _ in item_ids)
    rows = conn.execute(
        f"""SELECT item_id,
                   COALESCE(NULLIF(market_hash_name, ''), NULLIF(name, ''), item_id) display_name
            FROM items
            WHERE source='siteb' AND item_id IN ({marks})""",
        list(item_ids),
    ).fetchall()
    return {str(row["item_id"]): str(row["display_name"]) for row in rows}


def _hot_board() -> dict[str, Any]:
    """导购热度榜 + 涨跌幅榜（基于最近快照窗口，120s 缓存）。

    只取最近 12 万条 platform_snapshot（约覆盖最近 2-4 小时活跃饰品）
    与最近 4 万条 platform_avg，避免全量分组扫描（实测 11s -> 2s 内）。
    """
    with db() as conn:
        snap_rows = conn.execute(
            """SELECT s.item_id, s.platform, s.metric, s.close, s.ts FROM series s
               WHERE s.dataset='platform_snapshot' AND s.source='siteb'
                 AND s.metric IN ('sell_price','sell_num')
               ORDER BY s.ts DESC LIMIT 120000"""
        ).fetchall()
        avg_rows = conn.execute(
            """SELECT s.item_id, s.platform, s.close FROM series s
               WHERE s.dataset='platform_avg' AND s.source='siteb'
                 AND s.metric='avg_price_7d'
               ORDER BY s.ts DESC LIMIT 40000"""
        ).fetchall()
    sell: dict[str, dict[str, float]] = {}
    price: dict[str, dict[str, float]] = {}
    price_ts: dict[str, dict[str, str]] = {}
    for r in snap_rows:
        iid = str(r["item_id"])
        p = str(r["platform"])
        m = str(r["metric"])
        if m == "sell_num":
            sell.setdefault(iid, {}).setdefault(p, float(r["close"] or 0))
        else:
            price.setdefault(iid, {}).setdefault(p, float(r["close"] or 0))
            price_ts.setdefault(iid, {}).setdefault(p, str(r["ts"]))
    avg7: dict[str, dict[str, float]] = {}
    for r in avg_rows:
        avg7.setdefault(str(r["item_id"]), {}).setdefault(str(r["platform"]), float(r["close"] or 0))

    board: list[dict[str, Any]] = []
    for iid in sell:
        s = sell[iid]
        broker = sum(s.get(p, 0.0) for p in BROKER_PLATFORMS)
        if broker <= 0:
            continue
        pr = price.get(iid, {})
        av = avg7.get(iid, {})
        diff: dict[str, float] = {}
        for p in BROKER_PLATFORMS + ("buff",):
            if pr.get(p, 0) > 0 and av.get(p, 0) > 0:
                diff[p] = (pr[p] - av[p]) / av[p] * 100
        board.append({
            "id": iid,
            "broker_sell": broker,
            "price": {p: pr.get(p) for p in BROKER_PLATFORMS + ("buff",)},
            "avg7": {p: av.get(p) for p in BROKER_PLATFORMS + ("buff",)},
            "diff": diff,
            "ts": max(price_ts.get(iid, {}).values(), default=""),
        })
    board.sort(key=lambda x: -x["broker_sell"])
    hot = board[:40]
    gain = sorted((x for x in board if (x["diff"].get("buff") or 0) > 0.5),
                  key=lambda x: -(x["diff"].get("buff") or 0))[:30]
    loss = sorted((x for x in board if (x["diff"].get("buff") or 0) < -0.5),
                  key=lambda x: (x["diff"].get("buff") or 0))[:30]
    # 榜单 id 并集（<=100）统一查名称，避免大集合 IN 参数超限
    need = {x["id"] for x in hot + gain + loss}
    names = _names_map_shared(need)
    for x in hot + gain + loss:
        x["name"] = names.get(x["id"], x["id"])
    profiles = _profiles_map_shared({x["name"] for x in hot + gain + loss})
    for entry in hot + gain + loss:
        entry["profile"] = profiles.get(entry["name"], {})
    return {"hot": hot, "gain": gain, "loss": loss}


def _names_map_shared(item_ids: set[str]) -> dict[str, str]:
    """带小缓存的名称映射（避免榜单高频重建时反复查 items）。"""
    key = "names:" + ",".join(sorted(item_ids)[:200])
    return cached(key, 300, lambda: _names_map_real(item_ids))


def _names_map_real(item_ids: set[str]) -> dict[str, str]:
    with db() as conn:
        return _names_map(conn, item_ids)


def _profiles_map_shared(names: set[str]) -> dict[str, dict[str, Any]]:
    key = "profiles:" + ",".join(sorted(names)[:200])
    return cached(key, 300, lambda: _profiles_map_real(names))


def _profiles_map_real(names: set[str]) -> dict[str, dict[str, Any]]:
    with db() as conn:
        return item_profiles(conn, list(names))


def _change_board(limit: int = 60) -> list[dict[str, Any]]:
    """Build the radar from captured change events, with a snapshot fallback."""
    with db() as conn:
        event_rows = conn.execute(
            """SELECT item_id, metric, close, ts
               FROM series INDEXED BY idx_series_dataset_ts
               WHERE dataset='item_change_event' AND source='siteb'
               ORDER BY ts DESC LIMIT ?""",
            (max(12000, limit * 240),),
        ).fetchall()
        names = _display_names_map(
            conn, {str(row["item_id"]) for row in event_rows}
        )

    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in event_rows:
        metric = str(row["metric"] or "")
        if "." not in metric:
            continue
        platform, field = metric.split(".", 1)
        key = (str(row["item_id"]), platform, str(row["ts"]))
        bucket = grouped.setdefault(
            key,
            {"id": str(row["item_id"]), "platform": platform, "ts": str(row["ts"])},
        )
        bucket[field] = row["close"]

    events: list[dict[str, Any]] = []
    for bucket in grouped.values():
        pairs = (
            ("sell_price", "before_sell_price", 3.0),
            ("sell_num", "before_sell_num", 40.0),
            ("buy_price", "before_buy_price", 3.0),
            ("buy_num", "before_buy_num", 40.0),
        )
        for field, before_field, threshold in pairs:
            old_value = bucket.get(before_field)
            new_value = bucket.get(field)
            if old_value is None or new_value is None:
                continue
            try:
                old = float(old_value)
                new = float(new_value)
            except (TypeError, ValueError):
                continue
            if old <= 0:
                continue
            ratio = (new - old) / old * 100
            if abs(ratio) < threshold:
                continue
            events.append({
                "id": bucket["id"],
                "platform": bucket["platform"],
                "metric": field,
                "old": old,
                "new": new,
                "ratio": ratio,
                "ts": bucket["ts"],
                "name": names.get(bucket["id"], bucket["id"]),
                "target": bucket.get("change_target"),
            })

    # Older captures may only contain platform snapshots. Keep the radar useful
    # while the event stream is being refreshed.
    if not events:
        with db() as conn:
            rows = conn.execute(
                """SELECT s.item_id, s.platform, s.metric, s.close, s.ts FROM series s
                   WHERE s.dataset='platform_snapshot' AND s.source='siteb'
                     AND s.metric IN ('sell_price','sell_num')
                   ORDER BY s.ts DESC LIMIT ?""",
                (max(12000, limit * 160),),
            ).fetchall()
            fallback_names = _display_names_map(
                conn, {str(row["item_id"]) for row in rows}
            )
        recent: dict[tuple[str, str, str], list[tuple[float, str]]] = {}
        for row in rows:
            key = (str(row["item_id"]), str(row["platform"]), str(row["metric"]))
            values = recent.setdefault(key, [])
            if len(values) < 2:
                values.append((float(row["close"] or 0), str(row["ts"])))
        for (item_id, platform, metric), values in recent.items():
            if len(values) < 2 or values[1][0] <= 0:
                continue
            old, new = values[1][0], values[0][0]
            ratio = (new - old) / old * 100
            threshold = 3.0 if metric == "sell_price" else 40.0
            if abs(ratio) < threshold:
                continue
            events.append({
                "id": item_id,
                "platform": platform,
                "metric": metric,
                "old": old,
                "new": new,
                "ratio": ratio,
                "ts": values[0][1],
                "name": fallback_names.get(item_id, item_id),
            })

    focus = [e for e in events if e["platform"] in BROKER_PLATFORMS]
    focus.sort(key=lambda e: -abs(e["ratio"]))
    others = [e for e in events if e["platform"] not in BROKER_PLATFORMS]
    others.sort(key=lambda e: -abs(e["ratio"]))
    top = (focus + others)[:limit]
    profiles = _profiles_map_shared({e["name"] for e in top})
    for event in top:
        event["profile"] = profiles.get(event["name"], {})
        identity = "|".join(str(event.get(field, "")) for field in (
            "id", "platform", "metric", "ts", "old", "new",
        ))
        event["key"] = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return top


def _market_summary() -> dict[str, float]:
    with db() as conn:
        snap: dict[str, float] = {}
        for row in conn.execute(
            """SELECT metric, close FROM series WHERE dataset='market_snapshot' AND source='siteb'
                ORDER BY ts DESC LIMIT 400"""
        ):
            metric = str(row["metric"])
            if metric not in snap:
                snap[metric] = row["close"]
    return snap


def _market_kline_points():
    points, _, _ = _siteb_market_full_history(None, "1day")
    return points


def _market_index_points():
    """Display the newest *hourly* 行情站B index stream, not the old broad ID.

    Site summary/trend captures use the empty item ID; Open API captures use
    "broad". Use whichever has the latest actual observation and show only
    the 48 hours preceding that observation.
    """
    with db() as conn:
        latest = conn.execute(
            """SELECT item_id, MAX(ts) latest FROM series
               WHERE dataset='market_index' AND source='siteb'
                 AND item_id IN ('', 'broad') AND metric='market_index'
                 AND close IS NOT NULL
               GROUP BY item_id ORDER BY latest DESC LIMIT 1"""
        ).fetchone()
        if latest is None:
            return []
        cutoff = (
            datetime.fromisoformat(str(latest["latest"]).replace("Z", "+00:00"))
            - timedelta(hours=48)
        ).isoformat()
        rows = conn.execute(
            """SELECT ts, close FROM series WHERE dataset='market_index' AND source='siteb'
                AND item_id=? AND metric='market_index' AND ts>=?
                AND close IS NOT NULL ORDER BY ts DESC LIMIT 200""",
            (latest["item_id"], cutoff),
        ).fetchall()
    return [dict(r) for r in reversed(rows)]


def _latest_metric_map(
    conn: sqlite3.Connection,
    source: str,
    dataset: str,
    item_id: str,
) -> tuple[dict[str, float], dict[str, str]]:
    if source == "siteb" and dataset == "market_snapshot":
        rows = conn.execute(
            """SELECT metric, close, ts FROM series
                WHERE source=? AND dataset=? AND item_id IN (?, '')
                ORDER BY ts DESC LIMIT 1000""",
            (source, dataset, item_id),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT metric, close, ts FROM series
                WHERE source=? AND dataset=? AND item_id=?
                ORDER BY ts DESC LIMIT 1000""",
            (source, dataset, item_id),
        ).fetchall()
    values: dict[str, float] = {}
    timestamps: dict[str, str] = {}
    for row in rows:
        metric = str(row["metric"])
        if metric in values or row["close"] is None:
            continue
        values[metric] = float(row["close"])
        timestamps[metric] = str(row["ts"])
    return values, timestamps


def _market_source_summaries() -> dict[str, dict[str, Any]]:
    with db() as conn:
        steam, steam_ts = _latest_metric_map(
            conn, "siteb", "market_snapshot", "broad",
        )
        sitea, sitea_ts = _latest_metric_map(
            conn, "sitea", "market_index_snapshot", "1",
        )
        sitea_candle = conn.execute(
            """SELECT ts, open, high, low, close FROM series
                WHERE source='sitea' AND dataset='sub_kline' AND item_id='1'
                  AND metric='ohlc' AND platform='1day'
                ORDER BY ts DESC LIMIT 1"""
        ).fetchone()
        steam_candles = [
            conn.execute(
                """SELECT ts,open,high,low,close FROM series INDEXED BY idx_series_dataset_item
                   WHERE source='siteb' AND dataset=? AND item_id=?
                     AND metric=? AND open IS NOT NULL AND high IS NOT NULL
                     AND low IS NOT NULL AND close IS NOT NULL"""
                + (" AND platform='all.k2'" if dataset == "sector_kline" else "")
                + " ORDER BY ts DESC LIMIT 1",
                (dataset, item_id, metric),
            ).fetchone()
            for dataset, item_id, metric in (
                ("market_kline", "broad", "ohlc"),
                ("sector_kline", "BROAD:0:root", "market_index"),
            )
        ]

    steam_candle = max(
        ((row, position) for position, row in enumerate(steam_candles) if row),
        key=lambda entry: (str(entry[0]["ts"])[:10], entry[1], str(entry[0]["ts"])),
        default=(None, 0),
    )[0]
    steam_latest = steam.get("broadMarketIndex")
    steam_as_of = steam_ts.get("broadMarketIndex")
    steam_uses_candle = bool(
        steam_candle and (steam_latest is None or
                         str(steam_candle["ts"]) > str(steam_as_of or ""))
    )
    if steam_candle is not None and steam_uses_candle:
        steam_latest = steam_candle["close"]
        steam_as_of = str(steam_candle["ts"])

    sitea_latest = sitea.get("now")
    sitea_as_of = sitea_ts.get("now")
    sitea_uses_candle = bool(
        sitea_candle and (sitea_latest is None or
                         str(sitea_candle["ts"]) > str(sitea_as_of or ""))
    )
    if sitea_candle is not None and sitea_uses_candle:
        sitea_latest = sitea_candle["close"]
        sitea_as_of = str(sitea_candle["ts"])

    return {
        "siteb": {
            "source": "siteb",
            "name": "行情站B 饰品指数",
            "short_name": "行情站B",
            "value": steam_latest,
            "change": None if steam_uses_candle else steam.get("diffYesterday"),
            "change_rate": None if steam_uses_candle else steam.get("diffYesterdayRatio"),
            "open": steam_candle["open"] if steam_candle else None,
            "high": steam_candle["high"] if steam_candle else None,
            "low": steam_candle["low"] if steam_candle else None,
            "candle_close": steam_candle["close"] if steam_candle else None,
            "candle_as_of": str(steam_candle["ts"]) if steam_candle else None,
            "trade_count": steam.get("todayStatistics.tradeNum"),
            "turnover": steam.get("todayStatistics.turnover"),
            "supply": steam.get("surviveNum"),
            "holders": steam.get("holdersNum"),
            "as_of": steam_as_of,
            "status": freshness_status(steam_as_of, "ready" if steam_as_of else "collecting", 6),
            "flow_as_of": steam_ts.get("todayStatistics.tradeNum"),
            "supported_periods": ["1day", "7day", "15day", "30day"],
        },
        "sitea": {
            "source": "sitea",
            "name": "行情站A 饰品指数",
            "short_name": "行情站A",
            "value": sitea_latest,
            "change": None if sitea_uses_candle else sitea.get("amplitude"),
            "change_rate": None if sitea_uses_candle else sitea.get("rate"),
            "open": sitea_candle["open"] if sitea_candle else None,
            "high": sitea_candle["high"] if sitea_candle else None,
            "low": sitea_candle["low"] if sitea_candle else None,
            "candle_close": sitea_candle["close"] if sitea_candle else None,
            "candle_as_of": str(sitea_candle["ts"]) if sitea_candle else None,
            "trade_count": None,
            "turnover": None,
            "supply": None,
            "holders": None,
            "as_of": sitea_as_of,
            "status": freshness_status(sitea_as_of, "ready" if sitea_as_of else "collecting", 6),
            "flow_as_of": None,
            "supported_periods": ["1hour", "4hour", "1day", "7day", "15day", "30day"],
        },
    }


def _siteb_market_analysis(limit: int | None) -> list[dict[str, Any]]:
    limit_clause = " LIMIT ?" if limit is not None else ""
    limit_params = (limit * 3,) if limit is not None else ()
    with db() as conn:
        rows = conn.execute(
            """SELECT ts, open, high, low, close, volume, turnover FROM series
                WHERE source='siteb' AND dataset='market_kline'
                  AND item_id='broad' AND metric='ohlc'
                ORDER BY ts DESC""" + limit_clause,
            limit_params,
        ).fetchall()
        site_rows = conn.execute(
            """SELECT ts,open,high,low,close FROM series INDEXED BY idx_series_dataset_item
               WHERE source='siteb' AND dataset='sector_kline'
                 AND item_id='BROAD:0:root' AND platform='all.k2'
                 AND metric='market_index' AND open IS NOT NULL
                 AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
               ORDER BY ts DESC""" + limit_clause,
            limit_params,
        ).fetchall()
        flow_rows = conn.execute(
            """SELECT ts, metric, close FROM series
                WHERE source='siteb' AND dataset='market_snapshot'
                  AND metric IN (
                    'todayStatistics.tradeNum',
                    'todayStatistics.turnover',
                    'yesterdayStatistics.tradeNum',
                    'yesterdayStatistics.turnover'
                  )
                ORDER BY ts"""
        ).fetchall()

    # The open endpoint can publish several updates for the current daily candle.
    # Keep the latest observation per calendar day so zoom ranges stay intuitive.
    by_day: dict[str, dict[str, Any]] = {}
    for row in reversed(rows):
        point = dict(row)
        by_day[str(point["ts"])[:10]] = point
    for row in reversed(site_rows):
        point = {**dict(row), "sample": "站点日 K"}
        by_day[str(point["ts"])[:10]] = point

    daily_flow: dict[str, dict[str, float]] = {}
    for row in flow_rows:
        day = str(row["ts"])[:10]
        metric = str(row["metric"])
        value = row["close"]
        if value is None:
            continue
        daily_flow.setdefault(day, {})[metric] = float(value)
        if metric.startswith("yesterdayStatistics."):
            try:
                previous_day = (datetime.fromisoformat(day) - timedelta(days=1)).date().isoformat()
            except ValueError:
                continue
            previous_metric = metric.replace("yesterdayStatistics.", "todayStatistics.", 1)
            daily_flow.setdefault(previous_day, {}).setdefault(previous_metric, float(value))

    points = sorted(by_day.values(), key=lambda point: str(point["ts"]))
    if limit is not None:
        points = points[-limit:]
    for point in points:
        flow = daily_flow.get(str(point["ts"])[:10], {})
        point["volume"] = flow.get("todayStatistics.tradeNum")
        point["turnover"] = flow.get("todayStatistics.turnover")
    return points


def _siteb_market_full_history(
    limit: int | None, period: str = "1day",
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, Any]]:
    """Native broad candles: weekly outside daily coverage, daily inside it.

    Newer hourly index observations can form an explicitly labelled observed
    range; these are not official daily K candles and may miss hours.
    """
    with db() as conn:
        weekly = conn.execute(
            """SELECT ts,open,high,low,close FROM series INDEXED BY idx_series_dataset_item
               WHERE source='siteb' AND dataset='sector_kline'
                 AND item_id='BROAD:0:root' AND platform='all.k3'
                 AND metric='market_index' AND open IS NOT NULL
                 AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
               ORDER BY ts""",
        ).fetchall()
        daily_site = conn.execute(
            """SELECT ts,open,high,low,close FROM series INDEXED BY idx_series_dataset_item
               WHERE source='siteb' AND dataset='sector_kline'
                 AND item_id='BROAD:0:root' AND platform='all.k2'
                 AND metric='market_index' AND open IS NOT NULL
                 AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
               ORDER BY ts""",
        ).fetchall()
        daily_open = conn.execute(
            """SELECT ts,open,high,low,close FROM series INDEXED BY idx_series_dataset_item
               WHERE source='siteb' AND dataset='market_kline'
                 AND item_id='broad' AND metric='ohlc'
                 AND open IS NOT NULL AND high IS NOT NULL
                 AND low IS NOT NULL AND close IS NOT NULL
               ORDER BY ts""",
        ).fetchall()
        hourly = conn.execute(
            """SELECT ts,close FROM series INDEXED BY idx_series_dataset_item
               WHERE source='siteb' AND dataset='market_index'
                 AND item_id='' AND metric='market_index' AND close IS NOT NULL
               ORDER BY ts""",
        ).fetchall()

    def dates(rows: list[sqlite3.Row], label: str) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            result[str(row["ts"])[:10]] = {**dict(row), "sample": label}
        return result

    weekly_by_day = dates(weekly, "站点周 K")
    daily_by_day = dates(daily_open, "公开日 K")
    daily_by_day.update(dates(daily_site, "站点日 K"))
    if period == "7day" or not daily_by_day:
        candles = weekly_by_day
    else:
        first, last = min(daily_by_day), max(daily_by_day)
        candles = {day: row for day, row in weekly_by_day.items()
                   if day < first or day > last}
        candles.update(daily_by_day)
    points = [candles[day] for day in sorted(candles)]
    newest_native_day = max(candles) if candles else ""
    hourly_by_day: dict[str, list[sqlite3.Row]] = {}
    for row in hourly:
        day = str(row["ts"])[:10]
        if day > newest_native_day:
            hourly_by_day.setdefault(day, []).append(row)
    for day, observations in sorted(hourly_by_day.items()):
        latest = observations[-1]
        if len(observations) < 2:
            points.append({
                "ts": latest["ts"], "close": latest["close"],
                "open": None, "high": None, "low": None,
                "sample": "指数小时观测单点（无 K 线）", "observations": 1,
                "latest_quote": latest is hourly[-1],
            })
            continue
        prices = [float(row["close"]) for row in observations]
        hours = {str(row["ts"])[11:13] for row in observations}
        complete = len(hours) == 24
        points.append({
            "ts": latest["ts"], "open": prices[0], "high": max(prices),
            "low": min(prices), "close": prices[-1],
            "sample": "小时指数观测聚合（24小时）" if complete
                      else "小时指数观测聚合（非完整日）",
            "observations": len(observations),
            "latest_quote": latest is hourly[-1],
        })
    missing_days: list[str] = []
    if daily_by_day:
        gap_day: date | None
        last_day: date | None
        try:
            gap_day = date.fromisoformat(min(daily_by_day))
            last_day = date.fromisoformat(max(daily_by_day))
        except ValueError:
            # ts 列理论上可能有非 ISO 前缀（动态类型列）；对齐 _period_bucket 的降级策略
            gap_day = last_day = None
        while gap_day is not None and last_day is not None and gap_day <= last_day:
            if gap_day.isoformat() not in daily_by_day:
                missing_days.append(gap_day.isoformat())
            gap_day += timedelta(days=1)
    available_count = len(points)
    history = {
        "available_count": available_count,
        "start": points[0]["ts"] if points else None,
        "end": points[-1]["ts"] if points else None,
        "daily_count": len(daily_by_day),
        "daily_start": min(daily_by_day) if daily_by_day else None,
        "daily_end": max(daily_by_day) if daily_by_day else None,
        "missing_daily_count": len(missing_days),
        "missing_daily_dates": missing_days,
    }
    if limit is not None:
        points = points[-limit:]
    history["displayed_count"] = len(points)
    history["truncated"] = len(points) < available_count
    return points, {
        "weekly": sum(point["sample"] == "站点周 K" for point in points),
        "daily": sum(point["sample"] in ("公开日 K", "站点日 K") for point in points),
        "observed": sum(point["sample"].startswith("小时指数观测聚合") for point in points),
        "latest": sum(point["sample"].startswith("指数小时观测单点") for point in points),
    }, history


def _sitea_market_analysis(period: str, limit: int | None) -> list[dict[str, Any]]:
    limit_clause = " LIMIT ?" if limit is not None else ""
    params = (period, limit) if limit is not None else (period,)
    with db() as conn:
        rows = conn.execute(
            """SELECT ts, open, high, low, close, volume, turnover FROM series
                WHERE source='sitea' AND dataset='sub_kline' AND item_id='1'
                  AND metric='ohlc' AND platform=?
                ORDER BY ts DESC""" + limit_clause,
            params,
        ).fetchall()
    points = [dict(row) for row in reversed(rows)]
    # 行情站A currently emits v=0 for every index candle. Treat that sentinel as
    # unavailable history instead of drawing a misleading flat zero series.
    if points and not any(abs(float(point["volume"] or 0)) > 0 for point in points):
        for point in points:
            point["volume"] = None
    return points


def _market_analysis(source: str, period: str, limit: int | None,
                     view: str = "full") -> dict[str, Any]:
    if source == "siteb" and view == "full":
        period = period if period in ("1day", "7day") else "1day"
        points, samples, history = _siteb_market_full_history(limit, period)
        _attach_indicators(points)
        candle_count = samples["weekly"] + samples["daily"]
        observed_count = samples["observed"]
        note = (
            f"站点原始周 K {samples['weekly']} 根"
            if period == "7day" else
            f"原始日 K {samples['daily']} 根；区间外接续站点周 K {samples['weekly']} 根"
        )
        if history["daily_count"] and period == "1day":
            note += f"；日 K 历史 {history['daily_start']} 至 {history['daily_end']}"
            if history["missing_daily_count"]:
                note += f"；区间内缺少 {history['missing_daily_count']} 个日期，未补造数据"
            else:
                note += "；日 K 日期连续"
        if history["truncated"]:
            note += (f"；请求限制仅显示最近 {history['displayed_count']} / "
                     f"{history['available_count']} 个时间点")
        if observed_count:
            note += (f"；最近 {observed_count} 根由已采集的小时指数观测聚合，"
                     "部分日期未覆盖全天，开高低收仅代表已观测样本，缺失日期不补造；"
                     "原始 K 线仍停在 "
                     + max((str(point["ts"])[:10] for point in points
                            if point["sample"] in ("站点周 K", "公开日 K", "站点日 K")),
                           default="未知"))
        if samples["latest"]:
            note += f"；{samples['latest']} 个只有单点指数报价的日期以金色标记显示"
        note += "；跨周期均线仅供参考"
        return {
            "source": "siteb", "period": period, "view": "full",
            "mode": "candlestick", "price_label": "公开大盘指数",
            "supported_periods": ["1day", "7day"],
            "points": points, "samples": samples, "history": history,
            "as_of": history["end"],
            "status": freshness_status(
                history["end"], "ready" if points else "collecting",
                192 if period == "7day" else 48,
            ),
            "message": note,
            "range": {
                "start": points[0]["ts"] if points else None,
                "end": points[-1]["ts"] if points else None,
                "count": len(points),
            },
            "coverage": {
                "ohlc": {"available": candle_count > 0,
                         "count": candle_count,
                         "native": candle_count, "observed": observed_count},
                "volume": {"available": False, "count": 0,
                           "note": "大盘周 K 与日 K 没有统一成交笔数"},
                "turnover": {"available": False, "count": 0,
                             "note": "大盘周 K 与日 K 没有统一成交额"},
            },
        }
    if source == "sitea":
        supported = ["1hour", "4hour", "1day", "7day", "15day", "30day"]
        period = period if period in supported else "1day"
        base_period = "1day" if period in PERIOD_DAYS else period
        multiplier = PERIOD_DAYS.get(period, 1)
        points = _sitea_market_analysis(
            base_period, limit * multiplier if limit is not None else None,
        )
        if period in PERIOD_DAYS:
            points = _aggregate_ohlc(points, period)
        volume_note = "行情站A 指数 K 线当前返回零值占位，未作为真实成交量展示"
        turnover_note = "行情站A 指数接口当前未提供历史成交金额"
    else:
        source = "siteb"
        supported = ["1day", "7day", "15day", "30day"]
        period = period if period in supported else "1day"
        multiplier = PERIOD_DAYS.get(period, 1)
        points = _siteb_market_analysis(
            limit * multiplier if limit is not None else None,
        )
        points = _aggregate_ohlc(points, period)
        volume_note = "仅在已采集到市场统计的交易日提供成交笔数"
        turnover_note = "仅在已采集到市场统计的交易日提供成交金额"

    if limit is not None:
        points = points[-limit:]
    _attach_indicators(points)
    volume_count = sum(point.get("volume") is not None for point in points)
    turnover_count = sum(point.get("turnover") is not None for point in points)
    return {
        "source": source,
        "period": period,
        "supported_periods": supported,
        "points": points,
        "as_of": points[-1]["ts"] if points else None,
        "status": freshness_status(
            points[-1]["ts"] if points else None,
            "ready" if points else "collecting",
            {"1hour": 6, "4hour": 12}.get(period, (PERIOD_DAYS.get(period, 1) + 1) * 24),
        ),
        "range": {
            "start": points[0]["ts"] if points else None,
            "end": points[-1]["ts"] if points else None,
            "count": len(points),
        },
        "coverage": {
            "ohlc": {"available": bool(points), "count": len(points)},
            "volume": {
                "available": volume_count > 0,
                "sparse": volume_count > 0 and volume_count < max(3, math.ceil(len(points) * 0.05)),
                "count": volume_count,
                "label": "成交笔数",
                "note": volume_note,
            },
            "turnover": {
                "available": turnover_count > 0,
                "sparse": turnover_count > 0 and turnover_count < max(3, math.ceil(len(points) * 0.05)),
                "count": turnover_count,
                "label": "成交金额",
                "note": turnover_note,
            },
        },
    }


def _period_bucket(ts: Any, days: int) -> int:
    """Return a stable calendar bucket for daily-to-period aggregation."""
    raw = str(ts or "").strip().replace("Z", "+00:00")
    try:
        value = datetime.fromisoformat(raw)
    except ValueError:
        try:
            value = datetime.strptime(raw[:10], "%Y-%m-%d")
        except ValueError:
            return 0
    return value.date().toordinal() // max(1, days)


def _aggregate_ohlc(points: list[dict[str, Any]], period: str) -> list[dict[str, Any]]:
    """Aggregate daily candles into the long periods used by section workspaces."""
    days = PERIOD_DAYS.get(period, 1)
    ordered = sorted(
        (dict(point) for point in points if point.get("close") is not None),
        key=lambda point: str(point.get("ts") or ""),
    )
    if days <= 1 or not ordered:
        return ordered

    grouped: dict[int, dict[str, Any]] = {}
    for point in ordered:
        bucket = _period_bucket(point.get("ts"), days)
        current = grouped.get(bucket)
        if current is None:
            current = {
                "ts": point.get("ts"),
                "open": point.get("open"),
                "high": point.get("high"),
                "low": point.get("low"),
                "close": point.get("close"),
                "volume": point.get("volume"),
                "turnover": point.get("turnover"),
                "_has_volume": point.get("volume") is not None,
                "_has_turnover": point.get("turnover") is not None,
            }
            grouped[bucket] = current
            continue
        if current.get("open") is None and point.get("open") is not None:
            current["open"] = point["open"]
        if point.get("high") is not None:
            current["high"] = max(float(current.get("high") or point["high"]), float(point["high"]))
        if point.get("low") is not None:
            current["low"] = min(float(current.get("low") or point["low"]), float(point["low"]))
        if point.get("close") is not None:
            current["close"] = point["close"]
            current["ts"] = point.get("ts")
        if point.get("volume") is not None:
            current["volume"] = float(current.get("volume") or 0) + float(point["volume"])
            current["_has_volume"] = True
        if point.get("turnover") is not None:
            current["turnover"] = float(current.get("turnover") or 0) + float(point["turnover"])
            current["_has_turnover"] = True

    result = []
    for point in sorted(grouped.values(), key=lambda item: str(item.get("ts") or "")):
        if not point.pop("_has_volume", False):
            point["volume"] = None
        if not point.pop("_has_turnover", False):
            point["turnover"] = None
        result.append(point)
    return result


def _sector_rows(conn: sqlite3.Connection, path: str) -> tuple[list[dict[str, Any]], str | None]:
    """Read the latest 行情站B section snapshot and normalize its list buckets."""
    latest_row = conn.execute(
        """SELECT MAX(ts) ts FROM series
           WHERE source='siteb' AND dataset='sector_snapshot'
             AND metric LIKE ?""",
        (path + ".%",),
    ).fetchone()
    latest_ts = str(latest_row["ts"]) if latest_row and latest_row["ts"] else None
    if not latest_ts:
        return [], None
    rows = conn.execute(
        """SELECT item_id,metric,platform,ts,close,extra_json
           FROM series
           WHERE source='siteb' AND dataset='sector_snapshot'
             AND metric LIKE ? AND ts=?
           ORDER BY metric,item_id""",
        (path + ".%", latest_ts),
    ).fetchall()
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        metric = str(row["metric"] or "")
        suffix = metric[len(path) + 1:] if metric.startswith(path + ".") else metric
        if "." not in suffix:
            continue
        bucket, field = suffix.split(".", 1)
        item_id = str(row["item_id"])
        entry = grouped.setdefault(
            item_id,
            {
                "id": item_id,
                "bucket": bucket,
                "ts": str(row["ts"]),
                "index": None,
                "change": None,
                "change_rate": None,
                "level": None,
            },
        )
        entry[field] = row["close"]
        if bucket != "defaultList" and entry.get("bucket") == "defaultList":
            entry["bucket"] = bucket

    names = _display_names_map(conn, set(grouped))
    result: list[dict[str, Any]] = []
    for item_id, entry in grouped.items():
        display_name = names.get(item_id, item_id)
        if path == "itemTypeLevel1":
            display_name = ITEM_TYPE_LABELS.get(item_id, display_name.replace("CSGO_", "").replace("_", " "))
        elif path == "itemTypeLevel2" and ":" in display_name:
            display_name = display_name.split(":", 1)[-1].replace("_", " ").replace("lootlist", "").strip()
        bucket_order = {"topList": 0, "defaultList": 1, "bottomList": 2}
        result.append({
            **entry,
            "name": display_name,
            "bucket_order": bucket_order.get(str(entry.get("bucket")), 1),
            "linkable": path == "itemTypeLevel3" and " | " in display_name,
        })
    result.sort(
        key=lambda row: (
            int(row.get("bucket_order") or 1),
            -(abs(float(row.get("change_rate") or 0))),
            -float(row.get("index") or 0),
        )
    )
    return result, latest_ts


def _section_snapshot_points(
    conn: sqlite3.Connection,
    section_type: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Build a section index timeline from its dated constituent snapshots."""
    path = SECTION_PATHS.get(str(section_type or "").upper())
    if not path or limit <= 0:
        return []

    timestamp_rows = conn.execute(
        """SELECT ts
           FROM series
           WHERE source='siteb' AND dataset='sector_snapshot'
             AND metric LIKE ? AND close IS NOT NULL
           GROUP BY ts
           ORDER BY ts DESC
           LIMIT ?""",
        (path + ".%.index", int(limit)),
    ).fetchall()
    timestamps = [str(row["ts"]) for row in timestamp_rows if row["ts"]]
    if not timestamps:
        return []

    marks = _marks(timestamps)
    rows = conn.execute(
        f"""SELECT ts, close
            FROM series
            WHERE source='siteb' AND dataset='sector_snapshot'
              AND metric LIKE ? AND ts IN ({marks}) AND close IS NOT NULL
            ORDER BY ts""",
        [path + ".%.index", *timestamps],
    ).fetchall()
    values_by_ts: dict[str, list[float]] = {}
    for row in rows:
        try:
            value = float(row["close"])
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value):
            continue
        values_by_ts.setdefault(str(row["ts"]), []).append(value)

    points: list[dict[str, Any]] = []
    previous: float | None = None
    for ts in sorted(values_by_ts):
        values = values_by_ts[ts]
        if not values:
            continue
        close = sum(values) / len(values)
        open_value = close if previous is None else previous
        points.append({
            "ts": ts,
            "open": open_value,
            "high": max(open_value, close),
            "low": min(open_value, close),
            "close": close,
            "volume": len(values),
            "turnover": None,
            "constituent_count": len(values),
        })
        previous = close
    return points


def _section_analysis(
    section_type: str = "BROAD",
    source: str = "siteb",
    period: str = "1day",
    limit: int | None = None,
) -> dict[str, Any]:
    """Return a chart timeline scoped to the selected section."""
    normalized = str(section_type or "BROAD").upper()
    if normalized not in SECTION_TYPE_ORDER:
        normalized = "BROAD"
    source = source if source in ("siteb", "sitea") else "siteb"
    supported = ["1day", "7day", "15day", "30day"]
    selected_period = period if period in supported else "1day"

    if normalized == "BROAD":
        # Longer aggregate periods require actual OHLC samples. The full view
        # supports only observed daily/mixed and native weekly index closes.
        view = "candles" if selected_period in ("15day", "30day") else "full"
        result = _market_analysis(source, selected_period, limit, view)
        result["section_type"] = normalized
        result["scope"] = "broad"
        return result

    if source != "siteb":
        return {
            "section_type": normalized,
            "scope": "section_snapshot",
            "source": source,
            "period": selected_period,
            "supported_periods": supported,
            "points": [],
            "range": {"start": None, "end": None, "count": 0},
            "mode": "line",
            "price_label": "板块指数均值",
            "activity_label": "成分观察数",
            "message": "行情站A 当前没有该板块层级的独立历史",
            "coverage": {
                "ohlc": {"available": False, "count": 0},
                "volume": {
                    "available": False,
                    "sparse": False,
                    "count": 0,
                    "label": "成分观察数",
                    "note": "该来源未提供板块成分时间轴",
                },
                "turnover": {
                    "available": False,
                    "sparse": False,
                    "count": 0,
                    "label": "成交金额",
                    "note": "该来源未提供板块历史成交金额",
                },
            },
        }

    limit = limit if limit is not None else 4000
    multiplier = PERIOD_DAYS[selected_period]
    with db() as conn:
        points = _section_snapshot_points(
            conn,
            normalized,
            min(4000, max(60, int(limit) * multiplier)),
        )
    if selected_period in PERIOD_DAYS:
        points = _aggregate_ohlc(points, selected_period)
    _attach_indicators(points)

    observation_count = sum(
        point.get("volume") is not None for point in points
    )
    return {
        "section_type": normalized,
        "scope": "section_snapshot",
        "source": "siteb",
        "period": selected_period,
        "supported_periods": supported,
        "points": points,
        "range": {
            "start": points[0]["ts"] if points else None,
            "end": points[-1]["ts"] if points else None,
            "count": len(points),
        },
        "mode": "line",
        "price_label": "板块指数均值",
        "activity_label": "成分观察数",
        "message": (
            "板块成分指数均值时间轴"
            if points else "当前板块暂无独立历史快照"
        ),
        "coverage": {
            "ohlc": {
                "available": bool(points),
                "count": len(points),
                "note": "板块历史按成分指数均值计算",
            },
            "volume": {
                "available": observation_count > 0,
                "sparse": False,
                "count": observation_count,
                "label": "成分观察数",
                "note": "副图表示每个快照参与计算的成分数量",
            },
            "turnover": {
                "available": False,
                "sparse": False,
                "count": 0,
                "label": "成交金额",
                "note": "板块快照未提供历史成交金额",
            },
        },
    }


def _section_workspace(section_type: str = "BROAD", period: str = "1day") -> dict[str, Any]:
    """Prepare a 行情站B-style section workspace from real local snapshots."""
    normalized = str(section_type or "BROAD").upper()
    if normalized not in SECTION_TYPE_ORDER:
        normalized = "BROAD"
    selected_period = period if period in PERIOD_DAYS else "1day"
    config = SECTION_TYPE_CONFIG[normalized]
    summaries = cached("market_source_summaries", 60, _market_source_summaries)
    board = cached("hot_board", 120, _hot_board)

    with db() as conn:
        snapshot_rows: list[dict[str, Any]] = []
        snapshot_ts: str | None = None
        if normalized == "BROAD":
            snapshot_rows, snapshot_ts = _sector_rows(conn, "itemTypeLevel1")
        else:
            path = SECTION_PATHS.get(normalized)
            if path:
                snapshot_rows, snapshot_ts = _sector_rows(conn, path)
        history_row = None
        if normalized != "BROAD":
            history_path = SECTION_PATHS.get(normalized)
            history_row = conn.execute(
                """SELECT COUNT(DISTINCT ts) n FROM series
                   WHERE source='siteb' AND dataset='sector_snapshot'
                     AND metric LIKE ? AND close IS NOT NULL""",
                (history_path + ".%.index",),
            ).fetchone() if history_path else None
        history_points = int(history_row["n"] or 0) if history_row else 0

    if normalized == "BROAD":
        rows = snapshot_rows
        snap = dict(summaries.get("siteb", {}))
        history = cached(
            f"section_broad_history:{selected_period}", 60,
            lambda: _section_analysis("BROAD", "siteb", selected_period, None),
        )
        history_points = history["range"]["count"]
        history_note = history.get("message") or "已采集的真实 OHLC 聚合 K 线"
    else:
        rows = snapshot_rows
        numeric = [float(row["index"]) for row in rows if row.get("index") is not None]
        changes = [float(row["change"]) for row in rows if row.get("change") is not None]
        rates = [float(row["change_rate"]) for row in rows if row.get("change_rate") is not None]
        snap = {
            "value": sum(numeric) / len(numeric) if numeric else None,
            "change": sum(changes) / len(changes) if changes else None,
            "change_rate": sum(rates) / len(rates) if rates else None,
            "as_of": snapshot_ts,
        }
        history_note = (
            "板块快照已采集；图表使用该层级成分指数均值时间轴"
            if snapshot_ts else "当前没有板块快照，先展示可用市场标的参考"
        )

    period_labels = dict(PERIOD_LABELS)
    if normalized == "BROAD":
        period_labels.update({"1day": "全部公开 K 线", "7day": "周 K 观测"})

    breadth = {"up": 0, "flat": 0, "down": 0}
    for row in rows:
        rate = row.get("change_rate")
        if rate is None:
            breadth["flat"] += 1
        elif float(rate) > 0.5:
            breadth["up"] += 1
        elif float(rate) < -0.5:
            breadth["down"] += 1
        else:
            breadth["flat"] += 1

    if not rows:
        fallback = []
        for row in board.get("hot", [])[:20]:
            fallback.append({
                "id": row.get("id"),
                "name": row.get("name"),
                "index": None,
                "change": None,
                "change_rate": row.get("diff", {}).get("buff"),
                "bucket": "defaultList",
                "bucket_order": 1,
                "ts": row.get("ts"),
                "linkable": True,
            })
        rows = fallback
        for row in rows:
            rate = row.get("change_rate")
            if rate is None or abs(float(rate)) <= 0.5:
                breadth["flat"] += 1
            elif float(rate) > 0:
                breadth["up"] += 1
            else:
                breadth["down"] += 1

    hot = sorted(
        rows,
        key=lambda row: (
            int(row.get("bucket_order") or 1),
            -(abs(float(row.get("change_rate") or 0))),
            -float(row.get("index") or 0),
        ),
    )[:20]
    return {
        "section_type": normalized,
        "period": selected_period,
        "period_label": period_labels[selected_period],
        "periods": [
            {"key": key, "label": label}
            for key, label in period_labels.items()
        ],
        "config": config,
        "sources": summaries,
        "snap": snap,
        "rows": rows,
        "hot": hot,
        "breadth": breadth,
        "coverage": {
            "snapshot_available": bool(snapshot_ts),
            "snapshot_at": snapshot_ts,
            "constituent_count": len(rows),
            "history_note": history_note,
            "history_points": history_points,
        },
        "generated_at": datetime.now().astimezone().isoformat(),
    }


def _changes_workspace(
    query: str = "",
    platform: str = "",
    metric: str = "",
    limit: int = 120,
) -> dict[str, Any]:
    events = cached("change_board", 60, _change_board)
    normalized_query = str(query or "").strip().lower()
    normalized_platform = str(platform or "").strip().lower()
    normalized_metric = str(metric or "").strip().lower()
    filtered = []
    for event in events:
        if normalized_query and normalized_query not in str(event.get("name", "")).lower():
            continue
        if normalized_platform and str(event.get("platform", "")).lower() != normalized_platform:
            continue
        if normalized_metric and str(event.get("metric", "")).lower() != normalized_metric:
            continue
        filtered.append(event)
    filtered = filtered[: max(1, min(int(limit), 300))]
    focus = [event for event in filtered if event.get("platform") in BROKER_PLATFORMS]
    return {
        "query": str(query or ""),
        "platform": normalized_platform,
        "metric": normalized_metric,
        "events": filtered,
        "summary": {
            "total": len(filtered),
            "focus": len(focus),
            "price": sum(event.get("metric") == "sell_price" for event in filtered),
            "listing": sum(event.get("metric") in ("sell_num", "buy_num") for event in filtered),
        },
        "generated_at": datetime.now().astimezone().isoformat(),
    }


def _hanging_workspace(limit: int = 80) -> dict[str, Any]:
    board = cached("hot_board", 120, _hot_board)
    opportunities: list[dict[str, Any]] = []
    buy_platforms = ("youpin", "c5", "buff")
    for row in board.get("hot", []):
        prices = row.get("price") or {}
        candidates = [
            (platform, float(prices[platform]))
            for platform in buy_platforms
            if prices.get(platform) not in (None, 0)
        ]
        if not candidates:
            continue
        buy_platform, buy_price = min(candidates, key=lambda item: item[1])
        sell_platform = "steam" if prices.get("steam") not in (None, 0) else "buff"
        sell_price = float(prices.get(sell_platform) or 0)
        if sell_price <= 0:
            continue
        gross = (sell_price - buy_price) / buy_price * 100
        net = (sell_price * 0.87 - buy_price) / buy_price * 100
        opportunities.append({
            "id": row.get("id"),
            "name": row.get("name"),
            "profile": row.get("profile") or {},
            "buy_platform": buy_platform,
            "buy_platform_label": PLATFORM_LABELS.get(buy_platform, buy_platform.upper()),
            "buy_price": buy_price,
            "sell_platform": sell_platform,
            "sell_platform_label": PLATFORM_LABELS.get(sell_platform, sell_platform.upper()),
            "sell_price": sell_price,
            "gross_rate": gross,
            "net_rate": net,
            "supply": row.get("broker_sell"),
            "ts": row.get("ts"),
        })
    opportunities.sort(key=lambda row: (-float(row["net_rate"]), -float(row.get("supply") or 0)))
    rows = opportunities[: max(1, min(int(limit), 200))]
    return {
        "rows": rows,
        "summary": {
            "total": len(rows),
            "positive": sum(row["net_rate"] > 0 for row in rows),
            "best": rows[0] if rows else None,
        },
        "fee_rate": 0.13,
        "generated_at": datetime.now().astimezone().isoformat(),
    }


def _updates_workspace() -> dict[str, Any]:
    """Expose the update-record route and its local acquisition coverage."""
    with db() as conn:
        coverage_table = _control_table(conn, "coverage")
        endpoint = conn.execute(
            """SELECT method,path,successful,bytes,last_seen,last_error
               FROM endpoint_inventory
               WHERE source='siteb' AND lower(path) LIKE '%cs2-update%'
               ORDER BY last_seen DESC LIMIT 1"""
        ).fetchone()
        coverage = conn.execute(
            f"""SELECT dataset,checked_at,note FROM {coverage_table}
               WHERE source='siteb' AND
                 (dataset LIKE '%update%' OR dataset LIKE '%record%')
               ORDER BY checked_at DESC LIMIT 10"""
        ).fetchall()
        # 必须在连接关闭前查询：原来该语句写在 with 块外，
        # ProgrammingError 被 except 吞掉导致更新记录恒为空。
        records: list[dict[str, Any]] = []
        try:
            rows = conn.execute(
                """SELECT item_id, ts, extra_json FROM series
                   WHERE dataset='update_record' ORDER BY ts DESC LIMIT 60"""
            ).fetchall()
        except sqlite3.Error:
            rows = []
        for row in rows:
            try:
                extra = json.loads(row["extra_json"] or "{}")
            except json.JSONDecodeError:
                extra = {}
            records.append({
                "id": row["item_id"], "ts": row["ts"],
                "title": extra.get("title") or "(无标题)",
                "type": extra.get("type"),
            })
    return {
        "records": records,
        "endpoint": dict(endpoint) if endpoint else None,
        "coverage": [dict(row) for row in coverage],
        "status": "ready" if (endpoint or records) else "pending",
        "message": (
            f"已入库 {len(records)} 条更新记录"
            if records else
            "已发现更新记录接口，历史 4905 条原始捕获可通过 rebuild 回填"
            if endpoint else "更新记录接口尚未形成本地快照"
        ),
        "generated_at": datetime.now().astimezone().isoformat(),
    }


def _collect_workspace() -> dict[str, Any]:
    """Provide a public collection workspace while keeping auth state explicit."""
    board = cached("hot_board", 120, _hot_board)
    with db() as conn:
        endpoint_rows = conn.execute(
            """SELECT method,path,successful,last_seen,last_error
               FROM endpoint_inventory
               WHERE source='siteb' AND path LIKE '%collect%'
               ORDER BY last_seen DESC LIMIT 4"""
        ).fetchall()
    references = [
        {
            "name": row.get("name"),
            "price": (row.get("price") or {}).get("buff"),
            "diff": (row.get("diff") or {}).get("buff"),
            "profile": row.get("profile") or {},
            "ts": row.get("ts"),
        }
        for row in board.get("hot", [])[:12]
    ]
    return {
        "folders": [],
        "references": references,
        "auth_required": True,
        "endpoint_rows": [dict(row) for row in endpoint_rows],
        "message": "收藏夹需要 行情站B 登录态；公开市场参考仍可直接使用。",
        "generated_at": datetime.now().astimezone().isoformat(),
    }


TREND_CATEGORY_LABELS = {
    "knife": "匕首",
    "hands": "手套",
    "rifle": "步枪",
    "pistol": "手枪",
    "smg": "微型冲锋枪",
    "shotgun": "霰弹枪",
    "other": "其它",
}
TREND_CATEGORY_ORDER = ("knife", "hands", "rifle", "pistol", "smg", "shotgun", "other")


def _trend_category(name: str, profile: dict[str, Any]) -> str:
    """Map the local catalog taxonomy to 行情站B-style trend tabs."""
    label = str(profile.get("item_type_label") or "")
    raw = str(profile.get("item_type") or "")
    text = f"{label} {raw} {name}".lower()
    if "刀" in text or "knife" in text or "dagger" in text:
        return "knife"
    if "手套" in text or "hands" in text or "glove" in text:
        return "hands"
    if "狙击" in text:
        return "rifle"
    if "步枪" in text or "rifle" in text or "ak-" in text or "m4a" in text or "aug" in text:
        return "rifle"
    if "手枪" in text or "pistol" in text or "usp" in text or "glock" in text or "deagle" in text:
        return "pistol"
    if "冲锋" in text or "smg" in text or "mp7" in text or "mp9" in text or "p90" in text:
        return "smg"
    if "霰弹" in text or "shotgun" in text or "nova" in text or "xm1014" in text:
        return "shotgun"
    return "other"


def _trend_daily_prices(conn, item_ids, platform):
    """Fetch recent sell prices for a bounded set of item IDs."""
    marks = ",".join("?" for _ in item_ids)
    rows = conn.execute(
        f"""SELECT item_id, ts, close
            FROM series INDEXED BY idx_series_dataset_item
            WHERE source='siteb' AND dataset='platform_snapshot'
              AND item_id IN ({marks}) AND platform=? AND metric='sell_price'
              AND ts>=?
            ORDER BY ts DESC LIMIT 20000""",
        [*item_ids, platform.lower(), (datetime.now() - timedelta(days=8)).date().isoformat()],
    ).fetchall()
    result = {}
    for row in rows:
        item_id = str(row["item_id"])
        day = str(row["ts"])[:10]
        price = row["close"]
        if price is None:
            continue
        bucket = result.setdefault(item_id, {})
        if day not in bucket:
            bucket[day] = float(price)
    return result


def _trend_board(category: str = "", platform: str = "buff", limit: int = 80) -> dict[str, Any]:
    """Build a compact category trend board from the already cached market board."""
    board = cached("hot_board", 120, _hot_board)
    all_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for bucket in ("hot", "gain", "loss"):
        for raw in board.get(bucket, []):
            item_id = str(raw.get("id") or "")
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            profile = raw.get("profile") or {}
            key = _trend_category(str(raw.get("name") or ""), profile)
            prices = raw.get("price") or {}
            averages = raw.get("avg7") or {}
            selected_platform = platform if platform in prices else "buff"
            current = prices.get(selected_platform)
            average = averages.get(selected_platform)
            rate = raw.get("diff", {}).get(selected_platform)
            if rate is None and current is not None and average:
                rate = (float(current) - float(average)) / float(average) * 100
            all_rows.append({
                **raw,
                "category": key,
                "category_label": TREND_CATEGORY_LABELS[key],
                "selected_platform": selected_platform,
                "selected_price": current,
                "selected_average": average,
                "trend_rate": rate,
            })

    records = [
        row for row in all_rows
        if not category or category == row["category"]
    ]
    if category:
        records.sort(
            key=lambda row: (
                -(abs(float(row["trend_rate"])) if row.get("trend_rate") is not None else -1),
                -float(row.get("broker_sell") or 0),
            )
        )
    else:
        records.sort(key=lambda row: -float(row.get("broker_sell") or 0))
    records = records[:limit]

    categories = []
    for key in TREND_CATEGORY_ORDER:
        values = [
            float(row["trend_rate"]) for row in all_rows
            if row["category"] == key and row["trend_rate"] is not None
        ]
        categories.append({
            "key": key,
            "label": TREND_CATEGORY_LABELS[key],
            "count": sum(row["category"] == key for row in all_rows),
            "change": sum(values) / len(values) if values else None,
        })
    ids = [r["id"] for r in records]
    with db() as conn:
        daily_prices = _trend_daily_prices(conn, ids, platform) if ids else {}
    for row in records:
        row["daily_prices"] = daily_prices.get(row["id"], {})
    return {
        "category": category or "all",
        "platform": platform if platform in ("buff", "youpin", "c5", "steam", "haloskins") else "buff",
        "categories": categories,
        "rows": records,
        "all_rows": all_rows,
        "total_count": len(all_rows),
        "daily_prices": daily_prices,
    }


def _inventory_workspace(query: str = "", view: str = "estimate") -> dict[str, Any]:
    """Return the inventory shell plus local market references.

    Inventory ownership is user-specific; the local database only contains
    public market observations. Keeping that distinction in the response lets
    the UI show a useful empty state without inventing holdings.
    """
    board = cached("hot_board", 120, _hot_board)
    references = []
    for row in board.get("hot", [])[:12]:
        references.append({
            "name": row.get("name"),
            "price": (row.get("price") or {}).get("buff"),
            "supply": row.get("broker_sell"),
            "profile": row.get("profile") or {},
            "ts": row.get("ts"),
        })
    normalized = str(query or "").strip()
    view_key = view if view in ("estimate", "changes", "history") else "estimate"
    view_meta = {
        "estimate": {
            "eyebrow": "Inventory workspace",
            "title": "库存覆盖工作区",
            "description": "输入 Steam ID 或库存链接，检查当前本地数据是否有对应的库存快照；下方行情仅是公开市场参考。",
            "empty_title": "当前没有可用的个人库存快照",
            "empty_pending": "当前没有该库存的快照",
            "reference_title": "市场参考价",
            "reference_subtitle": "可直接进入单饰品详情",
        },
        "changes": {
            "eyebrow": "Inventory changes",
            "title": "库存涨跌观察",
            "description": "库存快照接入后可按持仓观察价格变化。目前仅展示公开市场参考。",
            "empty_title": "尚无库存涨跌数据",
            "empty_pending": "当前没有该库存的涨跌记录",
            "reference_title": "涨跌参考价",
            "reference_subtitle": "先用公开行情确认变化范围",
        },
        "history": {
            "eyebrow": "Inventory history",
            "title": "历史持仓工作区",
            "description": "按时间回看持仓变化需要真实历史快照。目前仅展示公开市场参考。",
            "empty_title": "尚无历史持仓记录",
            "empty_pending": "当前没有该库存的历史快照",
            "reference_title": "历史参考价",
            "reference_subtitle": "可从市场快照进入单饰品历史",
        },
    }[view_key]
    return {
        "query": normalized,
        "view": view_key,
        "references": references,
        "status": "missing_inventory_snapshot",
        "view_meta": view_meta,
        "message": (
            "个人库存数据链尚未接入。可以浏览下方公开市场报价与单品行情。"
            if not normalized
            else "已收到查询参数，但本地没有对应库存快照，也未创建同步任务；下方价格仅供市场参考。"
        ),
    }


def _diy_workspace(name: str = "") -> dict[str, Any]:
    board = cached("hot_board", 120, _hot_board)
    choices = [
        {
            "name": row.get("name"),
            "price": (row.get("price") or {}).get("buff"),
            "supply": row.get("broker_sell"),
            "profile": row.get("profile") or {},
        }
        for row in board.get("hot", [])[:12]
        if row.get("name")
    ]
    selected_name = str(name or "").strip()
    if not selected_name and choices:
        selected_name = str(choices[0]["name"])
    profile: dict[str, Any] = {}
    selected: dict[str, Any] = {}
    if selected_name:
        with db() as conn:
            profile = item_profiles(conn, [selected_name]).get(selected_name, {})
        selected = next(
            (choice for choice in choices if choice["name"] == selected_name),
            {"name": selected_name, "price": None, "supply": None, "profile": profile},
        )
    return {
        "selected_name": selected_name,
        "profile": profile,
        "selected": selected,
        "choices": choices,
    }


# --------------------------------------------------------------------------
# 页面
# --------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
@app.get("/home", include_in_schema=False)
def index(request: Request):
    board = cached("hot_board", 120, _hot_board)
    sources = cached("market_source_summaries", 60, _market_source_summaries)
    snap = cached("market_summary", 60, _market_summary)
    events = cached("change_board", 60, _change_board)
    return templates.TemplateResponse(request, "index.html", {
        "request": request, "active": "index",
        "nav_group": "overview",
        "snap": snap,
        "sources": sources,
        "hot": board["hot"][:20],
        "gain": board["gain"][:8],
        "loss": board["loss"][:8],
        "events": events[:8],
    })


@app.get("/mkt", include_in_schema=False)
@app.get("/selection", include_in_schema=False)
@app.get("/items")
@app.get("/market/items")
@_ttl_cached(120)
def items_page(
    request: Request,
    q: str = "",
    item_type: str = "",
    rarity: str = "",
    exterior: str = "",
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
    sort: str = "name",
):
    order_by = {
        "name": "i.market_hash_name ASC",
        "updated": "i.updated_at DESC",
    }.get(sort, "i.market_hash_name ASC")
    with db() as conn:
        where = ""
        args: list[Any] = []
        clauses: list[str] = []
        if q:
            clauses.append("(i.market_hash_name LIKE ? OR i.name LIKE ?)")
            args.extend([f"%{q}%", f"%{q}%"])
        for field, value in (
            ("itemType", item_type),
            ("rarityName", rarity),
            ("exteriorName", exterior),
        ):
            if value:
                clauses.append(
                    f"""EXISTS (
                        SELECT 1 FROM items meta
                        WHERE meta.source='siteb'
                          AND meta.market_hash_name=i.market_hash_name
                          AND json_extract(meta.raw_json, '$.{field}')=?
                    )"""
                )
                args.append(value)
        if clauses:
            where = "AND " + " AND ".join(clauses)
        total = conn.execute(
            f"""SELECT COUNT(*) n FROM items i
                WHERE i.source='siteb' AND i.item_id LIKE 'open:%' {where}""",
            args,
        ).fetchone()["n"]
        rows = conn.execute(
            f"""SELECT i.item_id, i.name, i.market_hash_name, i.updated_at FROM items i
                WHERE i.source='siteb' AND i.item_id LIKE 'open:%' {where}
                ORDER BY {order_by} LIMIT ? OFFSET ?""",
            [*args, size, (page - 1) * size],
        ).fetchall()
        metadata = _items_browser_metadata()
        style_names = set(metadata["style_names"])
        type_options = metadata["type_options"]
        rarity_options = metadata["rarity_options"]
        exterior_options = metadata["exterior_options"]
        profiles = item_profiles(conn, [str(row["market_hash_name"]) for row in rows])
    items = []
    for r in rows:
        row = dict(r)
        row["has_styles"] = str(row["market_hash_name"]) in style_names
        row["profile"] = profiles.get(str(row["market_hash_name"]), {})
        items.append(row)
    return templates.TemplateResponse(request, "items.html", {
        "request": request, "active": "items", "nav_group": "market",
        "items": items, "total": total, "q": q, "page": page, "size": size,
        "sort": sort, "item_type": item_type, "rarity": rarity, "exterior": exterior,
        "type_options": type_options, "rarity_options": rarity_options,
        "exterior_options": exterior_options, "type_labels": ITEM_TYPE_LABELS,
        "pages": max(1, (total + size - 1) // size),
    })


@app.get("/item/{name:path}")
@app.get("/market/items/{name:path}")
def item_page(request: Request, name: str):
    oid = open_id(name)
    return_to = safe_return_to(request.query_params.get("from"), "/market/items")
    return_path = urllib.parse.urlsplit(str(return_to)).path
    parent_context = {
        "/": ("市场总览", "总览"),
        "/mkt": ("饰品市场", "饰品市场"),
        "/selection": ("饰品市场", "筛选结果"),
        "/market": ("市场指数", "指数"),
        "/items": ("标的筛选", "筛选结果"),
        "/trend": ("品类走势", "品类走势"),
        "/section": ("板块指数", "板块指数"),
        "/rank": ("机会榜单", "榜单"),
        "/ladders": ("机会榜单", "榜单"),
        "/tracker": ("异动雷达", "异动"),
        "/cs2/tracker": ("饰品追踪", "追踪"),
        "/inventory": ("库存估价", "库存"),
        "/case": ("供给观察", "开箱观察"),
        "/market/items": ("饰品目录", "筛选结果"),
        "/market/compare": ("跨平台比较", "比较结果"),
        "/research/index": ("大盘指数", "大盘"),
        "/research/sections": ("板块指数", "板块"),
        "/research/trends": ("品类走势", "品类"),
        "/research/rankings": ("排行榜", "榜单"),
        "/research/breadth": ("市场宽度", "样本明细"),
        "/monitor/radar": ("异动雷达", "雷达"),
        "/monitor/events": ("事件列表", "事件"),
        "/monitor/item-tracking": ("单品追踪", "追踪结果"),
        "/assets/inventory": ("库存", "库存"),
    }.get(
        return_path,
        ("事件详情", "事件证据") if return_path.startswith("/monitor/events/")
        else ("库存查询", "市场参考") if return_path.startswith("/assets/inventory/")
        else ("饰品目录", "筛选结果"),
    )
    with db() as conn:
        item = conn.execute(
            "SELECT item_id, name, market_hash_name, raw_json FROM items WHERE source='siteb' AND item_id=?",
            (oid,),
        ).fetchone()
        styles: list[str] = []
        all_ids = item_ids(conn, name)
        numeric = numeric_ids(conn, name)
        if numeric:
            marks = ",".join("?" for _ in numeric)
            styles = [r[0] for r in conn.execute(
                f"""SELECT DISTINCT platform FROM series INDEXED BY idx_series_dataset_item WHERE dataset='item_history' AND source='siteb'
                    AND item_id IN ({marks}) AND platform NOT IN ('all','')
                    AND platform<>'' ORDER BY platform""", numeric
            )]
        id_marks = _marks(all_ids)
        for row in conn.execute(
            f"""SELECT DISTINCT platform FROM series INDEXED BY idx_series_dataset_item
                WHERE dataset='item_kline' AND source='siteb'
                  AND item_id IN ({id_marks}) AND metric='ohlc'
                  AND platform NOT IN ('all','ALL')""",
            all_ids,
        ):
            if row[0] not in styles:
                styles.append(row[0])
        styles.sort()
        id_marks = _marks(all_ids)
        prices = conn.execute(
            f"""SELECT item_id, platform, metric, close, ts
                FROM series INDEXED BY idx_series_dataset_item
                WHERE dataset='platform_snapshot' AND source='siteb'
                  AND item_id IN ({id_marks}) ORDER BY ts DESC LIMIT 800""",
            all_ids,
        ).fetchall()
        platforms: dict[str, dict[str, Any]] = {}
        for row in prices:
            p = str(row["platform"] or "")
            slot = platforms.setdefault(p, {"updated_at": row["ts"]})
            if str(row["metric"]) not in slot:
                slot[str(row["metric"])] = row["close"]
            if str(row["ts"]) > str(slot["updated_at"]):
                slot["updated_at"] = row["ts"]
        avg_rows = conn.execute(
            f"""SELECT item_id, platform, close, ts
                FROM series INDEXED BY idx_series_dataset_item
                WHERE dataset='platform_avg' AND source='siteb'
                  AND item_id IN ({id_marks}) AND metric='avg_price_7d'
                ORDER BY ts DESC LIMIT 200""",
            all_ids,
        ).fetchall()
        anon_ids = [r[0] for r in conn.execute(
            """SELECT item_id FROM items WHERE source='siteb' AND market_hash_name=?
                AND item_id NOT LIKE 'open:%'""", (name,)
        )]
        history = []
        if anon_ids:
            marks = ",".join("?" for _ in anon_ids)
            history = conn.execute(
                f"""SELECT ts, close FROM series INDEXED BY idx_series_dataset_item WHERE dataset='item_history'
                    AND source='siteb' AND metric='sell_price' AND platform='all'
                    AND item_id IN ({marks}) ORDER BY ts DESC LIMIT 500""", anon_ids
            ).fetchall()
        kline_last = conn.execute(
            f"""SELECT ts, open, high, low, close
                FROM series INDEXED BY idx_series_dataset_item
                WHERE dataset='item_kline' AND source='siteb'
                  AND item_id IN ({id_marks}) AND metric='ohlc' AND platform='all'
                ORDER BY ts DESC LIMIT 1""",
            all_ids,
        ).fetchone()
        profile = item_profiles(conn, [name]).get(name, {})
        siblings = related_items(conn, name, profile)
        if not profile.get("image_url"):
            for sibling in siblings:
                sibling_image = sibling.get("profile", {}).get("image_url")
                if sibling_image:
                    profile = dict(profile)
                    profile["image_url"] = sibling_image
                    break
    if item is None:
        return templates.TemplateResponse(request, "item.html", {
            "request": request, "active": "item", "nav_group": "market",
            "missing": name, "item": None,
            "return_to": return_to,
            "parent_title": parent_context[0],
            "return_label": parent_context[1],
        })
    return templates.TemplateResponse(request, "item.html", {
        "request": request, "active": "item", "nav_group": "market",
        "item": dict(item), "styles": styles,
        "profile": profile,
        "prices": prices,
        "platforms": platforms,
        "avg": latest_per_platform(avg_rows),
        "history": [dict(r) for r in history],
        "kline_last": dict(kline_last) if kline_last else None,
        "siblings": siblings,
        "item_context": {
            "platformLinks": profile.get("platform_links", {}),
            "marketShortName": profile.get("market_short_name", ""),
        },
        "return_to": return_to,
        "parent_title": parent_context[0],
        "return_label": parent_context[1],
    })


@app.get("/market")
@app.get("/research/index")
def market_page(
    request: Request,
    source: str = Query("siteb", pattern="^(siteb|sitea)$"),
    period: str = Query("1day", pattern="^(1hour|4hour|1day|7day|15day|30day)$"),
):
    summaries = cached("market_source_summaries", 60, _market_source_summaries)
    source_info = summaries.get(source, summaries.get("siteb", {}))
    supported = source_info.get("supported_periods") or ["1day"]
    selected_period = period if period in supported else supported[0]
    return templates.TemplateResponse(request, "market.html", {
        "request": request, "active": "market", "nav_group": "market",
        "snap": summaries["siteb"],
        "sources": summaries,
        "initial_source": source,
        "initial_period": selected_period,
    })


@app.get("/research/features")
def research_features_page(request: Request):
    """特征仪表盘：L1/L2 特征截面的 UI 出口（消费 /api/features/grid）。"""
    return templates.TemplateResponse(request, "features.html", {
        "request": request, "active": "research", "nav_group": "research",
    })


@app.get("/research/intelligence")
def research_intelligence_page(request: Request):
    """市场情报一体化仪表盘：情绪周期/供给耗竭/事件日历/执行指标 + 晨报。

    页面骨架由模板直出，五段面板数据由前端并行请求
    /api/intelligence/*（web/routes/intelligence.py）填充。
    """
    return templates.TemplateResponse(request, "intelligence.html", {
        "request": request, "active": "intelligence", "nav_group": "research",
    })


@app.get("/ladders", include_in_schema=False)
@app.get("/rank")
@app.get("/research/rankings")
def rank_page(
    request: Request,
    kind: str = Query("hot", pattern="^(hot|gain|loss)$"),
):
    board = cached("hot_board", 120, _hot_board)
    return templates.TemplateResponse(request, "rank.html", {
        "request": request, "active": "rank", "nav_group": "rank",
        "hot": board["hot"][:50], "gain": board["gain"], "loss": board["loss"],
        "initial_rank": kind,
    })


@app.get("/cs2/tracker", include_in_schema=False)
@app.get("/tracker")
@app.get("/monitor/radar")
def tracker_page(request: Request):
    events = cached("change_board", 60, _change_board)
    return templates.TemplateResponse(request, "tracker.html", {
        "request": request, "active": "tracker", "nav_group": "tracker",
        "events": events[:60],
    })


@app.get("/trend")
@app.get("/research/trends")
def trend_page(
    request: Request,
    category: str = "",
    platform: str = "buff",
    sort: str = "default",
):
    category = category if category in ("", *TREND_CATEGORY_ORDER) else ""
    platform = platform if platform in ("buff", "youpin", "c5", "steam", "haloskins") else "buff"
    sort = sort if sort in ("default", "change", "price", "supply") else "default"
    board = _trend_board(category=category, platform=platform, limit=80)
    return templates.TemplateResponse(request, "trend.html", {
        "request": request,
        "active": "trend",
        "nav_group": "market",
        "trend": board,
        "trend_sort": sort,
    })


@app.get("/section")
@app.get("/research/sections")
def section_page(
    request: Request,
    section_type: str = Query("BROAD", alias="type"),
    period: str = Query("1day", pattern="^(1day|7day|15day|30day)$"),
):
    normalized_type = section_type.upper() if section_type else "BROAD"
    if normalized_type not in SECTION_TYPE_ORDER:
        normalized_type = "BROAD"
    workspace = _section_workspace(normalized_type, period)
    return templates.TemplateResponse(request, "section.html", {
        "request": request,
        "active": "section",
        "nav_group": "market",
        "section_type": normalized_type,
        "workspace": workspace,
    })


@app.get("/inventory")
@app.get("/assets/inventory")
def inventory_page(
    request: Request,
    q: str = "",
    view: str = "estimate",
):
    workspace = _inventory_workspace(q, view)
    return templates.TemplateResponse(request, "inventory.html", {
        "request": request,
        "active": "inventory",
        "nav_group": "inventory",
        "inventory_view": workspace["view"],
        "workspace": workspace,
    })


@app.get("/inventory/estimate", include_in_schema=False)
def inventory_estimate_page(request: Request, q: str = ""):
    return inventory_page(request, q=q, view="estimate")


@app.get("/inventory/changes", include_in_schema=False)
@app.get("/assets/inventory/changes")
def inventory_changes_page(request: Request, q: str = ""):
    return inventory_page(request, q=q, view="changes")


@app.get("/inventory/history", include_in_schema=False)
@app.get("/assets/inventory/history")
def inventory_history_page(request: Request, q: str = ""):
    return inventory_page(request, q=q, view="history")


@app.get("/inventory/{inventory_id:path}")
@app.get("/assets/inventory/{inventory_id:path}")
def inventory_detail_page(request: Request, inventory_id: str):
    workspace = _inventory_workspace(inventory_id, "estimate")
    return templates.TemplateResponse(request, "inventory_detail.html", {
        "request": request,
        "active": "inventory",
        "nav_group": "inventory",
        "inventory_view": "estimate",
        "inventory_id": inventory_id,
        "workspace": workspace,
    })


@app.get("/my/collect")
@app.get("/assets/collection")
def collect_page(request: Request):
    workspace = _collect_workspace()
    return templates.TemplateResponse(request, "collect.html", {
        "request": request,
        "active": "collect",
        "nav_group": "inventory",
        "workspace": workspace,
    })


@app.get("/hanging")
@app.get("/assets/arbitrage")
def hanging_page(request: Request):
    workspace = _hanging_workspace()
    return templates.TemplateResponse(request, "hanging.html", {
        "request": request,
        "active": "hanging",
        "nav_group": "more",
        "workspace": workspace,
    })


@app.get("/changes")
@app.get("/monitor/events")
def changes_page(
    request: Request,
    q: str = "",
    platform: str = "",
    metric: str = "",
):
    workspace = _changes_workspace(q, platform, metric)
    return templates.TemplateResponse(request, "changes.html", {
        "request": request,
        "active": "changes",
        "nav_group": "more",
        "workspace": workspace,
    })


@app.get("/cs2-updates")
@app.get("/data/updates")
def cs2_updates_page(request: Request):
    workspace = _updates_workspace()
    return templates.TemplateResponse(request, "updates.html", {
        "request": request,
        "active": "updates",
        "nav_group": "more",
        "workspace": workspace,
    })


@app.get("/acquisition/progress")
@app.get("/data/coverage")
def acquisition_page(request: Request):
    with db() as conn:
        workspace = _acquisition_progress(conn)
    return templates.TemplateResponse(request, "acquisition.html", {
        "request": request,
        "active": "acquisition",
        "nav_group": "more",
        "workspace": workspace,
    })


@app.get("/skindiy")
@app.get("/assets/skin-diy")
def skindiy_page(request: Request, name: str = ""):
    workspace = _diy_workspace(name)
    return templates.TemplateResponse(request, "skindiy.html", {
        "request": request,
        "active": "skindiy",
        "nav_group": "more",
        "workspace": workspace,
    })


@app.get("/cs2/{name:path}")
def cs2_item_page(request: Request, name: str):
    """行情站B-compatible detail path that shares the canonical item view."""
    return item_page(request, name)


def _case_page_data() -> dict:
    with db() as conn:
        rows = conn.execute(
            """SELECT item_id, close, ts FROM series WHERE dataset='case_snapshot' AND source='sitea'
                ORDER BY ts DESC LIMIT 400"""
        ).fetchall()
        names: dict[str, str] = {}
        for row in conn.execute(
            """SELECT item_id, name FROM items WHERE source='sitea' AND item_id IN
                (SELECT DISTINCT item_id FROM series WHERE dataset='case_snapshot' AND source='sitea')"""
        ):
            names[str(row["item_id"])] = str(row["name"])
        latest: dict[str, tuple[float, str]] = {}
        for row in rows:
            iid = str(row["item_id"])
            # close 为 NULL 的占位快照不能进榜单：模板 "%.2f"|format(None) 会 500
            if iid in latest or row["close"] is None:
                continue
            latest[iid] = (float(row["close"]), str(row["ts"]))
        container = conn.execute(
            "SELECT COUNT(DISTINCT item_id) n FROM series WHERE dataset='container_item_snapshot' AND source='sitea'"
        ).fetchone()["n"]
    cases = [{"id": iid, "name": names.get(iid, iid), "value": v[0], "ts": v[1]}
             for iid, v in latest.items()]
    return {
        "cases": cases,
        "container": container,
        "max_case_value": max((value for value, _ in latest.values()), default=1),
    }


@app.get("/case")
@app.get("/assets/supply")
def case_page(request: Request):
    # 数据按日更新：整体缓存 300s，省掉每次 200ms+ 的容器计数扫描。
    data = cached("case_page_data", 300, _case_page_data)
    return templates.TemplateResponse(request, "case.html", {
        "request": request, "active": "case", "nav_group": "more", **data,
    })


@app.get("/market/compare")
def compare_page(
    request: Request,
    q: str = "",
    platform: str = "",
    min_spread: float = Query(0, ge=0, le=1000),
    min_price: float = Query(1.0, ge=0, le=100000),
):
    board = cached("hot_board", 120, _hot_board)
    rows = []
    query = q.strip()[:120]
    platform = platform.lower() if platform.lower() in PLATFORM_ROLES else ""
    for record in board.get("hot", []):
        prices = [(platform, float(price)) for platform, price in
                  (record.get("price") or {}).items() if price is not None and float(price) > 0]
        if len(prices) < 2:
            continue
        cheapest = min(prices, key=lambda entry: entry[1])
        highest = max(prices, key=lambda entry: entry[1])
        rows.append({
            **record, "lowest": cheapest, "highest": highest,
            "difference": highest[1] - cheapest[1],
            "spread": (highest[1] - cheapest[1]) / cheapest[1] * 100,
            "source_count": len(prices),
        })
    available_count = len(rows)
    # 地板价过滤（默认 ¥1）：0.02→0.20 这类地板组合会以 +900% 刷屏，
    # 绝对价差不足一分钱且无成交意义，默认排除。
    rows = [
        row for row in rows
        if (not query or query.casefold() in str(row.get("name") or "").casefold())
        and (not platform or platform in dict((row.get("price") or {})))
        and row["spread"] >= min_spread
        and row["lowest"][1] >= min_price
    ]
    rows.sort(key=lambda record: (-record["difference"], -record["spread"]))
    return templates.TemplateResponse(request, "compare.html", {
        "request": request, "active": "compare", "rows": rows,
        "available_count": available_count, "query": query, "platform": platform,
        "min_spread": min_spread, "min_price": min_price,
    })


@app.get("/research/breadth")
def breadth_page(request: Request):
    board = cached("hot_board", 120, _hot_board)
    unique = {str(row["id"]): row for bucket in ("hot", "gain", "loss")
              for row in board.get(bucket, []) if row.get("id")}
    rows = list(unique.values())
    up = sum(float(row.get("diff", {}).get("buff") or 0) > 0.5 for row in rows)
    down = sum(float(row.get("diff", {}).get("buff") or 0) < -0.5 for row in rows)
    missing = sum(row.get("diff", {}).get("buff") is None for row in rows)
    flat = len(rows) - up - down - missing
    return templates.TemplateResponse(request, "breadth.html", {
        "request": request, "active": "breadth", "rows": rows,
        "counts": {"up": up, "flat": flat, "down": down, "missing": missing,
                   "total": len(rows), "comparable": len(rows) - missing},
    })


@app.get("/monitor/events/{event_key}")
def event_page(request: Request, event_key: str):
    # The live radar caches 60 rows. A separate key is required for the
    # extended detail window; otherwise a warm radar cache hides older links.
    events = cached("change_board:300", 60, lambda: _change_board(300))
    event = next((row for row in events if row.get("key") == event_key), None)
    return_to = safe_return_to(request.query_params.get("from"), "/monitor/events")
    return_path = urllib.parse.urlsplit(return_to).path
    if not (return_path == "/monitor/events" or return_path == "/monitor/radar" or
            return_path == "/monitor/item-tracking"):
        return_to = "/monitor/events"
    if event is None:
        return templates.TemplateResponse(request, "event.html", {
            "request": request, "active": "event", "event": None, "return_to": return_to,
        }, status_code=404)
    return templates.TemplateResponse(request, "event.html", {
        "request": request, "active": "event", "event": event, "return_to": return_to,
    })


@app.get("/monitor/item-tracking")
def item_tracking_page(request: Request, q: str = ""):
    query = q.strip()[:120]
    workspace = _changes_workspace(query=query, limit=60) if query else None
    return templates.TemplateResponse(request, "item_tracking.html", {
        "request": request, "active": "tracking",
        "query": query, "workspace": workspace,
    })


@app.get("/data/sources")
def sources_page(request: Request):
    return templates.TemplateResponse(request, "sources.html", {
        "request": request, "active": "sources",
        "sources": cached("market_source_summaries", 60, _market_source_summaries),
    })


# --------------------------------------------------------------------------
# API
# --------------------------------------------------------------------------

@app.get("/api/search")
def api_search(q: str = "", limit: int = Query(10, le=50)):
    if not q:
        return {"items": []}
    def _fn():
        with db() as conn:
            rows = conn.execute(
                """SELECT market_hash_name, name FROM items WHERE source='siteb'
                    AND item_id LIKE 'open:%'
                    AND (market_hash_name LIKE ? OR name LIKE ?)
                    ORDER BY CASE WHEN market_hash_name LIKE ? THEN 0 ELSE 1 END,
                             market_hash_name
                    LIMIT ?""",
                (f"%{q}%", f"%{q}%", f"{q}%", limit),
            ).fetchall()
        return {"items": [dict(r) for r in rows]}
    return cached(f"search:{q}:{limit}", 30, _fn)


@app.get("/api/acquisition/progress")
def api_acquisition_progress():
    def _fn():
        with db() as conn:
            return _acquisition_progress(conn)
    return cached("acquisition_progress", 10, _fn)


@app.get("/api/runtime/status")
def api_runtime_status():
    def _read():
        with db() as conn:
            return _runtime_status(conn)

    return cached(
        "runtime_status",
        10,
        _read,
    )


@app.get("/api/health", include_in_schema=False)
def api_health():
    try:
        with db() as conn:
            conn.execute("SELECT 1").fetchone()
        database = "ready"
    except sqlite3.Error:
        database = "unavailable"
    return {
        "status": "ok" if database == "ready" else "degraded",
        "database": database,
        "generated_at": datetime.now().astimezone().isoformat(),
    }


@app.get("/api/changes")
def api_changes(
    q: str = "",
    platform: str = "",
    metric: str = "",
    limit: int = Query(120, ge=1, le=300),
):
    cache_key = f"api:changes:{q}:{platform}:{metric}:{limit}"
    return cached(
        cache_key,
        30,
        lambda: _changes_workspace(q, platform, metric, limit),
    )


@app.get("/api/collect")
def api_collect():
    return cached("api:collect", 60, _collect_workspace)


@app.get("/api/item/availability")
def api_item_availability(name: str):
    with db() as conn:
        return _item_availability(conn, name)


@app.get("/api/image")
def api_image(url: str):
    """Serve trusted item artwork from the same origin.

    The source host returns valid PNGs, but some Chromium environments block
    cross-origin image responses because the object is marked as a download.
    """
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in IMAGE_PROXY_HOSTS:
        return Response(status_code=400)

    def _fetch():
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "image/avif,image/webp,image/apng,image/png,image/*;q=0.8",
                "User-Agent": "CS2-Market-Desk/1.0",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=8) as response:
                content_type = response.headers.get("Content-Type", "image/png").split(";")[0]
                if not content_type.startswith("image/"):
                    return None
                body = response.read(2_000_001)
                if len(body) > 2_000_000:
                    return None
                return content_type, body
        except (urllib.error.URLError, TimeoutError, ValueError, OSError):
            return None

    result = _fetch()
    if not result:
        return Response(
            content=IMAGE_PLACEHOLDER,
            media_type="image/svg+xml",
            headers={
                "Cache-Control": "public, max-age=300",
                "X-Content-Type-Options": "nosniff",
            },
        )
    content_type, body = result
    return Response(
        content=body,
        media_type=content_type,
        headers={
            "Cache-Control": "public, max-age=86400",
            "X-Content-Type-Options": "nosniff",
        },
    )


@app.get("/favicon.ico")
def favicon():
    return Response(content=FAVICON, media_type="image/svg+xml", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/api/market/summary")
def api_market_summary():
    return {"snap": cached("market_summary", 60, _market_summary)}


@app.get("/api/market/sources")
def api_market_sources():
    return {"sources": cached("market_source_summaries", 60, _market_source_summaries)}


@app.get("/api/market/analysis")
def api_market_analysis(
    source: str = Query("siteb", pattern="^(siteb|sitea)$"),
    period: str = Query("1day", pattern="^(1hour|4hour|1day|7day|15day|30day)$"),
    limit: int | None = Query(None, ge=60, le=4000),
    view: str = Query("full", pattern="^(full|candles)$"),
):
    cache_key = f"api:market_analysis:{source}:{period}:{limit}:{view}"
    return {
        **cached(cache_key, 60, lambda: _market_analysis(source, period, limit, view)),
        "sources": cached("market_source_summaries", 60, _market_source_summaries),
    }


@app.get("/api/section/analysis")
def api_section_analysis(
    section_type: str = Query(
        "BROAD",
        alias="type",
        pattern="^(BROAD|HOT|LEVEL1|LEVEL2|LEVEL3)$",
    ),
    source: str = Query("siteb", pattern="^(siteb|sitea)$"),
    period: str = Query("1day", pattern="^(1day|7day|15day|30day)$"),
    limit: int | None = Query(None, ge=60, le=4000),
):
    normalized = section_type.upper()
    cache_key = f"api:section_analysis:{normalized}:{source}:{period}:{limit}"
    return cached(
        cache_key,
        60,
        lambda: _section_analysis(normalized, source, period, limit),
    )


@app.get("/api/market/kline")
def api_market_kline():
    return {"points": cached("api:market_kline", 60, _market_kline_points)}


@app.get("/api/market/index")
def api_market_index():
    points = cached("api:market_index", 60, _market_index_points)
    as_of = points[-1]["ts"] if points else None
    return {
        "points": points,
        "as_of": as_of,
        "status": freshness_status(as_of, "ready", max_age_hours=6) if as_of else "missing",
    }


@app.get("/api/hot")
def api_hot():
    board = cached("hot_board", 120, _hot_board)
    return board


@app.get("/api/rank-live")
def api_rank_live(kind: str = "hot", limit: int = Query(30, le=100)):
    board = cached("hot_board", 120, _hot_board)
    return {"items": board.get(kind, [])[:limit]}


@app.get("/api/tracker-live")
def api_tracker_live(limit: int = Query(60, le=200)):
    return {"events": cached("change_board", 60, _change_board)[:limit]}


@app.get("/api/items/quote")
def api_items_quote(ids: str = ""):
    id_list = [i for i in ids.split(",") if i]
    if not id_list or len(id_list) > 200:
        return {"quotes": {}}
    marks = ",".join("?" for _ in id_list)
    def _fn():
        with db() as conn:
            rows = conn.execute(
                f"""SELECT s.item_id, s.platform, s.close, s.ts FROM series s
                    JOIN (SELECT item_id, platform, MAX(ts) ts FROM series
                          WHERE dataset='platform_snapshot' AND source='siteb'
                            AND metric='sell_price' AND item_id IN ({marks})
                          GROUP BY item_id, platform) m
                      ON s.item_id=m.item_id AND s.platform=m.platform AND s.ts=m.ts
                    WHERE s.dataset='platform_snapshot' AND s.source='siteb'
                      AND s.metric='sell_price' AND s.item_id IN ({marks})""",
                [*id_list, *id_list],
            ).fetchall()
            num_rows = conn.execute(
                f"""SELECT s.item_id, s.platform, s.close, s.ts FROM series s
                    JOIN (SELECT item_id, platform, MAX(ts) ts FROM series
                          WHERE dataset='platform_snapshot' AND source='siteb'
                            AND metric='sell_num' AND item_id IN ({marks})
                          GROUP BY item_id, platform) m
                      ON s.item_id=m.item_id AND s.platform=m.platform AND s.ts=m.ts
                    WHERE s.dataset='platform_snapshot' AND s.source='siteb'
                      AND s.metric='sell_num' AND s.item_id IN ({marks})""",
                [*id_list, *id_list],
            ).fetchall()
        quotes: dict[str, dict[str, Any]] = {}
        for r in rows:
            quote = quotes.setdefault(str(r["item_id"]), {})
            quote[f"{r['platform']}.price"] = r["close"]
            quote["ts"] = max(str(quote.get("ts") or ""), str(r["ts"] or ""))
        for r in num_rows:
            quote = quotes.setdefault(str(r["item_id"]), {})
            quote[f"{r['platform']}.num"] = r["close"]
            quote["ts"] = max(str(quote.get("ts") or ""), str(r["ts"] or ""))
        out = {}
        for iid in id_list:
            q = quotes.get(iid, {})
            broker_values = [q.get(f"{p}.num") for p in BROKER_PLATFORMS]
            price = q.get("buff.price")
            buff_price = float(price) if price is not None and float(price) > 0 else None
            broker = (sum(float(value or 0) for value in broker_values)
                      if any(value is not None for value in broker_values) else None)
            out[iid] = {
                "buff_price": buff_price,
                "broker_sell": broker,
                "ts": q.get("ts"),
                "available": buff_price is not None or broker is not None,
            }
        return {"quotes": out}
    return cached(f"quote:{','.join(id_list)}", 60, _fn)


@app.get("/api/item/kline")
def api_kline(name: str, style: str = "all", limit: int = Query(10000, ge=1, le=20000)):
    with db() as conn:
        points = _item_kline_points(conn, name, style, limit)
    return {"points": points}


@app.get("/api/item/analysis")
def api_item_analysis(
    name: str,
    style: str = "all",
    source: str = "all",
    limit: int = Query(10000, ge=30, le=20000),
):
    """Return price, volume and indicators on one shared timeline."""
    with db() as conn:
        if source.lower() == "site_history":
            points = _item_site_history_points(conn, name, limit)
            mode = "line"
            source_label = "行情站B 站点在售价"
            activity_label = "成交量"
        elif source.lower() == "all":
            points = _item_kline_points(conn, name, style, limit)
            mode = "candlestick"
            source_label = "总览 K 线"
            activity_label = "成交量"
        else:
            points = _item_platform_chart_points(conn, name, source, limit)
            mode = "line"
            source_label = source.upper()
            activity_label = "在售量"
    _attach_indicators(points)
    activity_available = source.lower() != "site_history" and any(
        point.get("volume") is not None if mode == "candlestick"
        else point.get("listing_count") is not None
        for point in points
    )
    return {
        "name": name,
        "style": style.lower(),
        "source": source.lower(),
        "source_label": source_label,
        "mode": mode,
        "activity_label": activity_label,
        "activity_available": activity_available,
        "points": points,
        "range": {
            "start": points[0]["ts"] if points else None,
            "end": points[-1]["ts"] if points else None,
            "count": len(points),
        },
    }


@app.get("/api/item/chart-sources")
def api_item_chart_sources(name: str):
    """Describe which sources have enough real history to replace the main chart."""
    with db() as conn:
        known_item = conn.execute(
            """SELECT 1 FROM items WHERE source='siteb' AND market_hash_name=?
               LIMIT 1""", (name,),
        ).fetchone()
        ids = item_ids(conn, name)
        if not ids:
            return {"sources": []}
        marks = _marks(ids)
        total_row = conn.execute(
            f"""SELECT COUNT(DISTINCT ts) n, COUNT(volume) volume_n
                FROM series INDEXED BY idx_series_dataset_item
                WHERE dataset='item_kline' AND source='siteb'
                  AND item_id IN ({marks}) AND metric='ohlc' AND platform='all'""",
            ids,
        ).fetchone()
        total = int(total_row["n"] or 0)
        total_volume = int(total_row["volume_n"] or 0)
        numeric = numeric_ids(conn, name)
        site_history = None
        if numeric:
            history_marks = _marks(numeric)
            site_history = conn.execute(
                f"""SELECT COUNT(DISTINCT ts) n, MIN(ts) lo, MAX(ts) hi
                    FROM series INDEXED BY idx_series_dataset_item
                    WHERE dataset='item_history' AND source='siteb'
                      AND item_id IN ({history_marks})
                      AND platform='all' AND metric='sell_price'
                      AND close IS NOT NULL""",
                numeric,
            ).fetchone()
        kline_range = conn.execute(
            f"""SELECT MIN(ts) lo, MAX(ts) hi
                FROM series INDEXED BY idx_series_dataset_item
                WHERE dataset='item_kline' AND source='siteb'
                  AND item_id IN ({marks}) AND metric='ohlc' AND platform='all'""",
            ids,
        ).fetchone()
        rows = conn.execute(
            f"""SELECT platform, COUNT(DISTINCT ts) n, MIN(ts) lo, MAX(ts) hi
                FROM series INDEXED BY idx_series_dataset_item
                WHERE dataset='platform_snapshot' AND source='siteb'
                  AND item_id IN ({marks}) AND metric='sell_price'
                GROUP BY platform ORDER BY n DESC, platform""",
            ids,
        ).fetchall()
    sources = [{
        "key": "all",
        "label": "总览",
        "description": "真实 OHLC" + (" 与成交量" if total_volume else "，成交量暂缺"),
        "mode": "candlestick",
        "count": total,
        "available": bool(total),
        "start": kline_range["lo"],
        "end": kline_range["hi"],
    }]
    if site_history and int(site_history["n"] or 0) >= 3:
        sources.append({
            "key": "site_history",
            "label": "行情站B 全部价格历史",
            "description": "站点在售价走势；仅收盘/报价，无 OHLC",
            "mode": "line",
            "count": int(site_history["n"]),
            "available": True,
            "start": site_history["lo"],
            "end": site_history["hi"],
        })
    for row in rows:
        count = int(row["n"] or 0)
        sources.append({
            "key": str(row["platform"]),
            "label": str(row["platform"]).upper(),
            "description": "平台卖价与在售量历史" if count >= 3 else "仅有零散快照",
            "mode": "line",
            "count": count,
            "available": count >= 3,
            "start": row["lo"],
            "end": row["hi"],
        })
    def before(value: Any, cutoff: datetime) -> bool:
        if not value:
            return True
        try:
            observed = datetime.fromisoformat(str(value))
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=timezone.utc)
            return observed < cutoff
        except ValueError:
            return True

    now = datetime.now(timezone.utc)
    if known_item and (site_history is None or
                       not before(site_history["lo"], now - timedelta(days=540)) or
                       before(site_history["hi"], now - timedelta(hours=48))):
        _prioritize_viewed_site_page(name)
    return {"sources": sources}


@app.get("/api/item/styles")
def api_styles(name: str):
    oid = open_id(name)
    with db() as conn:
        rows: list[sqlite3.Row] = []
        seen: set[str] = set()
        numeric = numeric_ids(conn, name)
        if numeric:
            marks = ",".join("?" for _ in numeric)
            for row in conn.execute(
                f"""SELECT platform, COUNT(*) n, MIN(ts) lo, MAX(ts) hi, 'history' src
                    FROM series INDEXED BY idx_series_dataset_item WHERE dataset='item_history' AND source='siteb' AND item_id IN ({marks})
                    AND platform NOT IN ('all','') AND platform<>''
                    GROUP BY platform ORDER BY platform""", numeric
            ):
                rows.append(row)
                seen.add(str(row["platform"]))
        for row in conn.execute(
            """SELECT platform, COUNT(*) n, MIN(ts) lo, MAX(ts) hi, 'kline' src
                FROM series INDEXED BY idx_series_dataset_item WHERE dataset='item_kline' AND source='siteb' AND item_id=? AND metric='ohlc'
                AND platform NOT IN ('all','ALL') GROUP BY platform""", (oid,)
        ):
            if str(row["platform"]) not in seen:
                rows.append(row)
    return {"styles": [dict(r) for r in rows]}


@app.get("/api/item/style-history")
def api_style_history(name: str, style: str):
    with db() as conn:
        numeric = numeric_ids(conn, name)
        if not numeric:
            return {"series": {}}
        marks = ",".join("?" for _ in numeric)
        rows = conn.execute(
            f"""SELECT ts, metric, close FROM series INDEXED BY idx_series_dataset_item WHERE dataset='item_history' AND source='siteb'
                AND item_id IN ({marks}) AND platform=?
                ORDER BY ts DESC""", [*numeric, style.lower()],
        ).fetchall()
    points: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        points.setdefault(str(row["metric"]), []).append({"ts": row["ts"], "close": row["close"]})
    return {"series": {k: v[::-1] for k, v in points.items()}}


@app.get("/api/item/price")
def api_price(name: str):
    with db() as conn:
        rows = _item_series_rows(
            conn,
            "platform_snapshot",
            name,
            metrics=("sell_price", "sell_num", "buy_price", "buy_num"),
            limit=800,
        )
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        p = str(row["platform"])
        if p not in latest:
            latest[p] = {"updated_at": row["ts"]}
        if str(row["metric"]) not in latest[p]:
            latest[p][str(row["metric"])] = row["close"]
        if str(row["ts"]) > str(latest[p]["updated_at"]):
            latest[p]["updated_at"] = row["ts"]
    return {"platforms": latest}


@app.get("/api/item/avg")
def api_avg(name: str):
    with db() as conn:
        rows = _item_series_rows(conn, "platform_avg", name, metrics=("avg_price_7d",), limit=200)
    return {"avg": latest_per_platform(rows)}


@app.get("/api/item/history")
def api_history(name: str):
    with db() as conn:
        numeric = numeric_ids(conn, name)
        rows: list[sqlite3.Row] = []
        if numeric:
            marks = _marks(numeric)
            rows = conn.execute(
                f"""SELECT ts, metric, close FROM series INDEXED BY idx_series_dataset_item
                    WHERE dataset='item_history' AND source='siteb'
                      AND platform='all' AND item_id IN ({marks})
                    ORDER BY ts DESC""",
                numeric,
            ).fetchall()
        availability = _item_availability(conn, name)
        fallback_points = [] if rows else _item_kline_points(conn, name, "all", 10000)
    points: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        points.setdefault(str(row["metric"]), []).append({"ts": row["ts"], "close": row["close"]})
    if points:
        return {
            "series": {key: value[::-1] for key, value in points.items()},
            "status": "ready",
            "source": "siteb_site_history",
            "source_label": "行情站B 站点买卖历史",
            "fallback": False,
            "message": "站点买卖历史已采集",
            "availability": availability,
        }
    if fallback_points:
        return {
            "series": {
                "sell_price": [
                    {"ts": point["ts"], "close": point["close"]}
                    for point in fallback_points if point.get("close") is not None
                ],
            },
            "status": "partial",
            "source": "siteb_open_kline",
            "source_label": "行情站B 公开日 K",
            "fallback": True,
            "message": "站点买卖历史仍在采集，暂以公开日 K 收盘价展示价格趋势",
            "availability": availability,
        }
    has_quotes = availability["platforms"]["available"]
    return {
        "series": {},
        "status": "collecting" if availability["catalog"]["available"] else "missing",
        "source": None,
        "source_label": "",
        "fallback": False,
        "message": (
            "已有平台当前报价，历史序列仍在采集"
            if has_quotes else availability["site_history"]["reason"]
        ),
        "availability": availability,
    }


@app.get("/api/item/indicators")
def api_indicators(name: str):
    with db() as conn:
        ids = item_ids(conn, name)
        if not ids:
            return {"points": []}
        marks = _marks(ids)
        rows = conn.execute(
            f"""SELECT item_id, ts, sma7, sma30, ema12, ema26, macd, macd_signal, rsi14,
                bollinger_mid, bollinger_upper, bollinger_lower, return_1d, volatility30
                FROM indicators WHERE source='siteb' AND item_id IN ({marks}) ORDER BY ts""",
            ids,
        ).fetchall()
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        latest[str(row["ts"])[:10]] = dict(row)
    if latest:
        return {"points": list(latest.values())}
    # indicators 表为遗留批量物化（仅历史 sitea 数据）；无命中时回退到
    # 与详情页一致的实时计算口径，保证端点不为空。
    analysis = api_item_analysis(name, limit=5000)
    points = analysis.get("points") or []
    computed: list[dict[str, Any]] = []
    for point in points:
        row = {"ts": point.get("ts")}
        for key in ("sma7", "sma30", "ema12", "ema26", "macd",
                    "macd_signal", "rsi14", "bollinger_mid",
                    "bollinger_upper", "bollinger_lower"):
            if point.get(key) is not None:
                row[key] = point[key]
        computed.append(row)
    return {"points": computed, "source": "live"}


@app.get("/api/item/change")
def api_item_change(name: str):
    """单饰品异动：各平台最新 vs 上一快照的价格/挂单变化。"""
    with db() as conn:
        rows = _item_series_rows(
            conn,
            "platform_snapshot",
            name,
            metrics=("sell_price", "sell_num"),
            limit=800,
        )
    recent: dict[tuple[str, str], list[tuple[float, str]]] = {}
    for r in rows:
        key = (str(r["platform"]), str(r["metric"]))
        lst = recent.setdefault(key, [])
        point = (float(r["close"] or 0), str(r["ts"]))
        if not any(existing[1] == point[1] for existing in lst) and len(lst) < 2:
            lst.append(point)
    changes: list[dict[str, Any]] = []
    for (platform, metric), lst in recent.items():
        if len(lst) < 2 or lst[1][0] <= 0:
            continue
        old, new = lst[1][0], lst[0][0]
        changes.append({
            "platform": platform, "metric": metric,
            "old": old, "new": new,
            "ratio": (new - old) / old * 100,
            "ts": lst[0][1],
        })
    changes.sort(key=lambda c: -abs(c["ratio"]))
    return {"changes": changes}


def _cache_warmer() -> None:
    """后台预热：榜单/异动缓存过期前 30% 提前重建，避免首请求卡顿。"""
    jobs = (
        ("hot_board", 120, _hot_board),
        ("change_board", 60, _change_board),
        ("market_source_summaries", 60, _market_source_summaries),
        ("market_summary", 60, _market_summary),
    )
    while True:
        _time.sleep(20)
        try:
            for key, ttl, fn in jobs:
                with _CACHE_LOCK:
                    hit = _CACHE.get(key)
                    fresh = hit is not None and hit[0] - _time.monotonic() > ttl * 0.3
                if not fresh:
                    cached(key, ttl, fn, refresh=True)
        except Exception:
            pass


_threading = __import__("threading")
_threading.Thread(target=_cache_warmer, daemon=True, name="cache-warmer").start()


@app.get("/data/exports", include_in_schema=False)
@app.get("/exports", include_in_schema=False)
def exports_page(request: Request):
    return templates.TemplateResponse(request, "exports.html", {
        "request": request, "active": "exports",
        "files": _list_export_files(),
    })


# ===========================================================================
# 实时数据层：SSE 推流 + 快照/增量 REST + 实时监控页（API 端点在 web/routes/realtime.py）
# ===========================================================================
from web.realtime import realtime_snapshot  # noqa: E402


@app.get("/realtime", include_in_schema=False)
def realtime_page(request: Request):
    with db() as conn:
        snapshot = realtime_snapshot(conn)
    return templates.TemplateResponse(request, "realtime.html", {
        "request": request,
        "active": "realtime",
        "snapshot_json": json.dumps(snapshot, ensure_ascii=False, default=str),
    })


from web.routes import dashboard as dashboard_routes  # noqa: E402
from web.routes import exports as exports_routes  # noqa: E402
from web.routes import intelligence as intelligence_routes  # noqa: E402
from web.routes import realtime as realtime_routes  # noqa: E402
from web.routes.exports import _list_export_files  # noqa: E402

app.include_router(realtime_routes.router)
app.include_router(dashboard_routes.router)
app.include_router(exports_routes.router)
app.include_router(intelligence_routes.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8800, log_level="warning")
