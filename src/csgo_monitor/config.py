from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Settings:
    project_dir: Path
    output_dir: Path
    database: Path
    control_database: Path
    sitea_base: str
    sitea_token: str
    siteb_open_base: str
    siteb_api_key: str
    siteb_api_keys: tuple[str, ...]
    siteb_open_delay: float
    timeout: float
    days: int
    request_delay: float
    browser_headless: bool
    max_browser_pages: int
    proxy_url: str
    hot_interval_hours: int
    cold_interval_hours: int
    hot_tx_threshold: float
    broker_platforms: tuple[str, ...]
    hot_broker_sell_threshold: float
    hot_broker_single_threshold: float
    hot_buff_sell_threshold: float
    rate_per_minute: int
    fetch_avg_for_hot: bool
    siteb_site_page_size: int
    siteb_site_delay: float
    runtime_routes: dict[str, tuple[str, ...]]
    route_probe_timeout: float
    route_failure_threshold: int
    route_cooldown_seconds: int
    route_cache_seconds: int
    supervisor_max_parallel: int
    quiet_hours_enabled: bool
    quiet_start_minute: int
    quiet_end_minute: int
    quiet_interval_multiplier: float
    quiet_max_parallel: int
    supervisor_heartbeat_seconds: int
    supervisor_lease_seconds: int
    supervisor_lane_limits: dict[str, int]


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _route_values(value: Any, fallback: tuple[str, ...]) -> tuple[str, ...]:
    if value is None:
        values = list(fallback)
    elif isinstance(value, str):
        values = [value]
    else:
        values = [str(item) for item in value]
    normalized = []
    for item in values:
        route = str(item).strip() or "direct"
        if route not in normalized:
            normalized.append(route)
    return tuple(normalized or ("direct",))


def load_settings(config_path: str | Path = "config.yaml") -> Settings:
    path = Path(config_path).resolve()
    project_dir = path.parent
    raw: dict[str, Any] = {}
    if path.exists():
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    api = raw.get("api", {})
    siteb = raw.get("siteb_open", {})
    siteb_site = raw.get("siteb_site", {})
    crawler = raw.get("crawler", {})
    proxy = raw.get("proxy", {})
    incremental = raw.get("incremental", {})
    runtime = raw.get("runtime", {})
    route_config = runtime.get("routes", {})
    health_config = runtime.get("health_check", {})
    supervisor_config = runtime.get("supervisor", {})
    lane_config = supervisor_config.get("lanes", {})
    quiet_config = runtime.get("quiet_hours", {})

    def _minute(value: Any, default: int) -> int:
        try:
            parts = str(value).split(":")
            return int(parts[0]) * 60 + int(parts[1])
        except (ValueError, IndexError, TypeError):
            return default

    quiet_hours_enabled = bool(quiet_config.get("enabled", False))
    quiet_start_minute = _minute(quiet_config.get("start", "08:00"), 480)
    quiet_end_minute = _minute(quiet_config.get("end", "23:00"), 1380)
    configured_proxy = os.getenv(
        "CSGO_MONITOR_PROXY",
        proxy.get("url", ""),
    ).strip()
    default_fallbacks = ["direct"]
    if configured_proxy:
        default_fallbacks.append(configured_proxy)
    fallbacks = _route_values(
        route_config.get("fallbacks"),
        tuple(default_fallbacks),
    )
    runtime_routes = {
        "siteb_open_api": _route_values(
            route_config.get("siteb_open_api"),
            ("direct",),
        ),
        "siteb_site_api": _route_values(
            route_config.get("siteb_site_api"),
            fallbacks,
        ),
        "siteb_browser": _route_values(
            route_config.get("siteb_browser"),
            fallbacks,
        ),
        "sitea_public": _route_values(
            route_config.get("sitea_public"),
            fallbacks,
        ),
    }
    output_value = os.getenv(
        "CSGO_MONITOR_OUTPUT_DIR",
        crawler.get("output_dir", "../csgo-monitor-data/crawler"),
    )
    output_dir = (project_dir / output_value).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    return Settings(
        project_dir=project_dir,
        output_dir=output_dir,
        database=output_dir / "market.sqlite3",
        control_database=output_dir / "control.sqlite3",
        sitea_base=os.getenv("SITEA_BASE_URL", api.get("base_url", "https://api.sitea.com")).rstrip("/"),
        sitea_token=os.getenv("SITEA_API_TOKEN", api.get("token", "")),
        siteb_open_base=os.getenv(
            "SITEB_API_BASE", siteb.get("base_url", "https://open.siteb.com")
        ).rstrip("/"),
        siteb_api_key=os.getenv("SITEB_API_KEY", siteb.get("api_key", "")),
        siteb_api_keys=tuple(
            str(key).strip() for key in (siteb.get("api_keys") or [])
            if str(key).strip()
        ) or (os.getenv("SITEB_API_KEY", siteb.get("api_key", "")).strip(),),
        siteb_open_delay=float(
            os.getenv("SITEB_OPEN_DELAY", siteb.get("delay", crawler.get("request_delay", 1.2)))
        ),
        timeout=float(os.getenv("CSGO_MONITOR_TIMEOUT", api.get("timeout", 30))),
        days=int(os.getenv("CSGO_MONITOR_DAYS", crawler.get("days", 365))),
        request_delay=float(os.getenv("CSGO_MONITOR_DELAY", crawler.get("request_delay", 1.2))),
        browser_headless=_as_bool(os.getenv("CSGO_MONITOR_HEADLESS", crawler.get("browser_headless")), True),
        max_browser_pages=int(os.getenv("CSGO_MONITOR_BROWSER_PAGES", crawler.get("max_browser_pages", 20))),
        proxy_url=configured_proxy,
        hot_interval_hours=int(os.getenv("CSGO_MONITOR_HOT_HOURS", incremental.get("hot_interval_hours", 1))),
        cold_interval_hours=int(os.getenv("CSGO_MONITOR_COLD_HOURS", incremental.get("cold_interval_hours", 5))),
        hot_tx_threshold=float(os.getenv("CSGO_MONITOR_HOT_TX", incremental.get("hot_tx_threshold", 5))),
        broker_platforms=tuple(
            str(item).strip().lower()
            for item in incremental.get("broker_platforms") or ("youpin", "c5", "steam", "haloskins")
            if str(item).strip()
        ),
        hot_broker_sell_threshold=float(os.getenv("CSGO_MONITOR_HOT_BROKER_SELL", incremental.get("hot_broker_sell_threshold", 700))),
        hot_broker_single_threshold=float(os.getenv("CSGO_MONITOR_HOT_BROKER_SINGLE", incremental.get("hot_broker_single_threshold", 400))),
        hot_buff_sell_threshold=float(os.getenv("CSGO_MONITOR_HOT_BUFF_SELL", incremental.get("hot_buff_sell_threshold", 800))),
        rate_per_minute=int(os.getenv("CSGO_MONITOR_RATE", incremental.get("rate_per_minute", 250))),
        fetch_avg_for_hot=_as_bool(os.getenv("CSGO_MONITOR_FETCH_AVG", incremental.get("fetch_avg_for_hot")), True),
        siteb_site_page_size=max(
            50,
            min(
                500,
                int(os.getenv(
                    "SITEB_SITE_PAGE_SIZE",
                    siteb_site.get("page_size", 500),
                )),
            ),
        ),
        siteb_site_delay=max(
            0.05,
            float(os.getenv(
                "SITEB_SITE_DELAY",
                siteb_site.get("delay", min(1.0, max(0.5, crawler.get("request_delay", 1.2)))),
            )),
        ),
        runtime_routes=runtime_routes,
        route_probe_timeout=max(
            1.0,
            float(health_config.get("timeout_seconds", 5)),
        ),
        route_failure_threshold=max(
            1,
            int(health_config.get("failure_threshold", 2)),
        ),
        route_cooldown_seconds=max(
            30,
            int(health_config.get("cooldown_seconds", 600)),
        ),
        route_cache_seconds=max(
            15,
            int(health_config.get("cache_seconds", 300)),
        ),
        quiet_hours_enabled=quiet_hours_enabled,
        quiet_start_minute=quiet_start_minute,
        quiet_end_minute=quiet_end_minute,
        quiet_interval_multiplier=max(
            1.0, float(quiet_config.get("interval_multiplier", 3))),
        quiet_max_parallel=max(1, int(quiet_config.get("max_parallel", 3))),
        supervisor_max_parallel=max(
            1,
            int(supervisor_config.get("max_parallel", 2)),
        ),
        supervisor_heartbeat_seconds=max(
            5,
            int(supervisor_config.get("heartbeat_seconds", 15)),
        ),
        supervisor_lease_seconds=max(
            30,
            int(supervisor_config.get("lease_seconds", 90)),
        ),
        supervisor_lane_limits={
            lane: max(1, int(lane_config.get(lane, default)))
            for lane, default in {
                "siteb_site_fast": 1,
                "siteb_site_bulk": 1,
                "siteb_open": 1,
                "sitea": 1,
                "public_pages": 1,
                "maintenance": 1,
                "local": 1,
            }.items()
        },
    )
