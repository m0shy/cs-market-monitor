"""Shared navigation and display-level data freshness classification."""

from datetime import datetime, timedelta

CHANNELS = (
    {
        "key": "overview", "label": "监测台", "icon": "layout-dashboard", "href": "/",
        "pages": ("index",), "links": (),
    },
    {
        "key": "market", "label": "饰品市场", "icon": "scan-search",
        "href": "/market/items", "pages": ("items", "item", "compare"),
        "links": (
            {"page": "items", "label": "饰品目录", "href": "/market/items"},
            {"page": "compare", "label": "跨平台比较", "href": "/market/compare"},
        ),
    },
    {
        "key": "research", "label": "市场研究", "icon": "chart-candlestick",
        "href": "/research/index", "pages": ("market", "section", "trend", "rank", "breadth", "features", "intelligence"),
        "links": (
            {"page": "market", "label": "大盘指数", "href": "/research/index"},
            {"page": "intelligence", "label": "市场情报", "href": "/research/intelligence"},
            {"page": "features", "label": "特征仪表盘", "href": "/research/features"},
            {"page": "section", "label": "板块指数", "href": "/research/sections"},
            {"page": "trend", "label": "品类走势", "href": "/research/trends"},
            {"page": "rank", "label": "排行榜", "href": "/research/rankings"},
            {"page": "breadth", "label": "市场宽度", "href": "/research/breadth"},
        ),
    },
    {
        "key": "monitor", "label": "监控中心", "icon": "radar",
        "href": "/monitor/radar",
        "pages": ("tracker", "changes", "event", "tracking", "realtime"),
        "links": (
            {"page": "realtime", "label": "实时监控", "href": "/realtime"},
            {"page": "tracker", "label": "异动雷达", "href": "/monitor/radar"},
            {"page": "changes", "label": "事件列表", "href": "/monitor/events"},
            {"page": "tracking", "label": "单品追踪", "href": "/monitor/item-tracking"},
        ),
    },
    {
        "key": "assets", "label": "资产工具", "icon": "wallet-cards",
        "href": "/assets/inventory",
        "pages": ("inventory", "collect", "hanging", "case", "skindiy"),
        "links": (
            {"page": "inventory", "label": "库存", "href": "/assets/inventory"},
            {"page": "hanging", "label": "挂刀比较", "href": "/assets/arbitrage"},
            {"page": "case", "label": "供给观察", "href": "/assets/supply"},
            {"page": "skindiy", "label": "饰品 DIY", "href": "/assets/skin-diy"},
            {"page": "collect", "label": "收藏", "href": "/assets/collection"},
        ),
    },
    {
        "key": "data", "label": "数据中心", "icon": "database",
        "href": "/data/coverage", "pages": ("acquisition", "exports", "updates", "sources"),
        "links": (
            {"page": "exports", "label": "导出中心", "href": "/data/exports"},
            {"page": "acquisition", "label": "采集覆盖", "href": "/data/coverage"},
            {"page": "sources", "label": "数据来源", "href": "/data/sources"},
            {"page": "updates", "label": "更新记录", "href": "/data/updates"},
        ),
    },
)


def channel_for(page: str) -> dict:
    return next((channel for channel in CHANNELS if page in channel["pages"]), CHANNELS[0])


def freshness_status(as_of: str | None, status: str, max_age_hours: int = 24) -> str:
    if not as_of or status not in ("ready", "partial"):
        return status
    try:
        value = datetime.fromisoformat(str(as_of).replace("Z", "+00:00"))
        now = datetime.now().astimezone()
        if value.tzinfo is None:
            value = value.astimezone()
        return "stale" if now - value > timedelta(hours=max_age_hours) else status
    except (ValueError, TypeError, OverflowError):
        return status
