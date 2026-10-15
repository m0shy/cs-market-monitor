from __future__ import annotations
from typing import Any

import argparse
import json
import logging
import os
import sys
import gzip
import math
import platform
import sqlite3
import urllib.request
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from .browser import (BrowserDiscovery, SiteABrowserHistory, SiteBBrowserHistory,
                      collect_siteb_catalog_from_sitemap,
                      collect_sitea_catalog, collect_siteb_catalog, recover_sitea_catalog)
from .config import load_settings
from .contracts import (
    EXIT_OK,
    EXIT_PARTIAL,
    EXIT_TEMPORARY,
    aggregate_status,
    configure_utf8_streams,
    emit_task_result,
)
from .sitea_api import SiteAApiCollector
from .indicators import calculate_all, derive_daily_klines
from .pipelines import build_pipeline_status
from .report import build_report
from .reconcile import reconcile_control_state
from .rebuild import rebuild_from_captures
from .recover import repair_siteb_kline_captures
from .sources import SiteASource, SiteBSource
from .siteb_open import SiteBOpenSource
from .siteb_site import SiteBSiteCollector
from .storage import Storage



def _pid_alive(pid: int) -> bool:
    """Return True if a Windows process with the given PID is still running."""
    import ctypes
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    kernel32 = ctypes.windll.kernel32
    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        return False
    try:
        exit_code = ctypes.c_ulong()
        ok = kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
        return bool(ok) and exit_code.value == 259  # STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def _maybe_start_sitea_sidecar(settings) -> None:
    """Start the 行情站A detail collector as a child of this process.

    Triggered by the marker file ``output_dir/sitea_sidecar.flag``. The
    backfill loop spawns fresh ``crawl`` processes that inherit an
    unrestricted token (Chromium can launch), which a restricted shell
    cannot. Each batch checks the PID file and only spawns the collector
    once; the child keeps running after ``crawl`` exits.
    """
    import os
    marker = settings.output_dir / "sitea_sidecar.flag"
    if not marker.exists():
        return
    pid_file = settings.output_dir / "sitea_sidecar.pid"
    if pid_file.exists():
        try:
            if _pid_alive(int(pid_file.read_text(encoding="utf-8").strip())):
                return
        except (OSError, ValueError):
            pass
    log_file = settings.output_dir / "sitea_sidecar.log"
    err_file = settings.output_dir / "sitea_sidecar.log.err"
    config_path = settings.project_dir / "config.yaml"
    with open(log_file, "a", encoding="utf-8") as out, open(err_file, "a", encoding="utf-8") as err:
        proc = subprocess.Popen(
            [sys.executable, "-m", "csgo_monitor.cli", "--config", str(config_path),
             "sitea-api", "--tasks", "detail", "--max-items", "0"],
            cwd=settings.project_dir, stdout=out, stderr=err,
            creationflags=0x08000000 if os.name == "nt" else 0,  # CREATE_NO_WINDOW
        )
    pid_file.write_text(str(proc.pid), encoding="utf-8")
    logging.getLogger(__name__).info("sitea sidecar started pid=%s", proc.pid)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="csgo-monitor",
        description="CS2 market data framework (supervisor/storage/indicators/web; site adapters stubbed)",
    )
    root.add_argument("--config", default="config.yaml", help="配置文件路径")
    root.add_argument("--verbose", action="store_true")
    root.add_argument("--proxy", default=None,
                    help="proxy URL (overrides config proxy.url / CSGO_MONITOR_PROXY); empty = direct")
    sub = root.add_subparsers(dest="command", required=True)

    discover = sub.add_parser("discover", help="用浏览器遍历公开页面并记录 API 响应")
    discover.add_argument("--source", choices=["sitea", "siteb", "all"], default="all")
    discover.add_argument("--headed", action="store_true", help="显示浏览器窗口")
    discover.add_argument("--max-pages", type=int, default=None, help="0 表示遍历全部已发现的公开页面")
    discover.add_argument("--human-mode", action="store_true", help="按公开 UI 节奏访问页面，不执行批量页面请求")

    crawl = sub.add_parser("crawl", help="抓取快照、目录、近一年历史并计算指标")
    crawl.add_argument("--source", choices=["sitea", "siteb", "all"], default="all")
    crawl.add_argument("--max-items", type=int, default=0, help="从当前断点处理的批次数；0 表示处理到末尾")
    crawl.add_argument("--catalog-pages", type=int, default=0, help="0 表示全部")
    crawl.add_argument("--fresh", action="store_true", help="忽略历史抓取断点")
    crawl.add_argument("--discover", action="store_true", help="抓取前先运行浏览器发现")
    crawl.add_argument("--with-indicators", action="store_true", help="批次结束后重算全部日 K 和技术指标")
    crawl.add_argument("--skip-snapshots", action="store_true", help="续跑时跳过大盘实时快照")
    crawl.add_argument("--human-mode", action="store_true",
                       help="仅由公开页面交互触发数据请求，不调用直接快照/Token 历史接口")
    crawl.add_argument("--hist-mode", choices=["all", "kline", "price", "avg"], default="all",
                       help="siteb open-api history mode: all/kline/price/avg (default all)")
    crawl.add_argument("--refresh-kline-hours", type=int, default=0,
                       help="在日 K 切片完成指定小时后重新抓取；0 表示仅回填缺口")
    crawl.add_argument("--retry-failures", action="store_true",
                       help="只处理当前可重试失败对象，不推进主目录游标")
    crawl.add_argument("--browser-only", action="store_true",
                       help="行情站B 强制走公开页面采集扩展数据，即使已配置 Open API")
    crawl.add_argument("--force-browser-visits", action="store_true",
                       help="行情站B 页面采集不因核心历史已齐全而跳过条目")

    backfill = sub.add_parser("backfill", help="分批、断点续跑全部公开目录并自动重试失败对象")
    backfill.add_argument("--source", choices=["sitea", "siteb", "all"], default="all")
    backfill.add_argument("--batch-size", type=int, default=20, help="每次浏览器会话处理的目录位置数")
    backfill.add_argument("--max-batches", type=int, default=0, help="0 表示持续到目录走完且无可重试对象")
    backfill.add_argument("--delay", type=float, default=5.0, help="批次间等待秒数")
    backfill.add_argument("--human-mode", action="store_true", help="只通过公开 UI 交互触发数据")
    backfill.add_argument("--resume-blocked", action="store_true",
                          help="重新尝试已被安全验证阻挡的来源")
    backfill.add_argument("--blocked-retry-minutes", type=float, default=60.0,
                          help="安全验证来源的自动探测间隔；0 表示本次运行不重试")
    backfill.add_argument("--no-indicators", action="store_true", help="结束后不重算日 K 和技术指标")

    catalog = sub.add_parser("catalog", help="滚动公开饰品列表并建立完整目录")
    catalog.add_argument("--source", choices=["sitea", "siteb", "all"], default="all")
    catalog.add_argument("--max-pages", type=int, default=0, help="0 表示滚动到末尾")
    catalog.add_argument("--refresh", action="store_true", help="忽略已保存目录并重新滚动")


    api = sub.add_parser("sitea-api", help="通过站点自身请求管线采集 行情站A 公开数据")
    api.add_argument(
        "--tasks", default="all",
        help="逗号分隔任务: public,catalog,kline,index,vol,case,roi,container,exchange,detail (默认 all)",
    )
    api.add_argument("--max-items", type=int, default=0, help="每个任务最多处理的条目数；0 表示全部")
    api.add_argument("--fresh", action="store_true", help="忽略续跑断点，从任务开头重新开始")
    api.add_argument("--retry-failures", action="store_true",
                     help="仅重试当前任务的失败项，不推进主游标")
    api.add_argument("--headed", action="store_true", help="显示浏览器窗口")
    api.add_argument("--human-mode", action="store_true", help="在请求间加入滚动与鼠标动作，贴近人工浏览节奏")

    api2 = sub.add_parser("siteb-api", help="通过站点 JSON 接口直连采集 行情站B 条目级数据（快照/历史/K线/平台对比）")
    api2.add_argument("--max-items", type=int, default=0, help="本次最多处理的条目数；0 表示全部")
    api2.add_argument("--delay", type=float, default=1.2, help="请求间隔秒数（含轮换探活）")
    api2.add_argument("--rotate-interval", type=int, default=350, help="每处理多少条主动轮换出口节点（0 关闭）")
    api2.add_argument("--retry-failures", action="store_true", help="只重试历史失败条目，不推进主游标")
    api2.add_argument("--controller", default="http://127.0.0.1:9097",
                    help="mihomo controller base URL for node rotation")
    api2.add_argument("--job-suffix", default="", help="job cursor suffix for multi-worker partitioning")
    api2.add_argument("--partition", type=int, default=0, help="multi-worker partition id (with --partition-total)")
    api2.add_argument("--partition-total", type=int, default=1, help="worker count; >1 splits catalog by index modulo")
    api2.add_argument("--impersonate", default="",
                    help="TLS fingerprint impersonation (e.g. chrome); empty = plain requests")
    api2.add_argument("--styles-only", action="store_true",
                    help="只采集特殊款式(多普勒/伽玛/绯红和服)的分款式K线")
    api2.add_argument("--extended-only", action="store_true",
                    help="只采集公开详情页扩展数据（磨损/成交/武器箱/变动）")
    api2.add_argument("--trade-day", default="",
                    help="按日期采集成交数据，格式 YYYY-MM-DD；yesterday 表示昨天")
    api2.add_argument("--repair-trade-captures", action="store_true",
                    help="离线重建已保存的成交响应；--max-items 指定单次最多处理数")
    api2.add_argument("--full-history", action="store_true", default=True,
                    help="全量历史：type-trend typeDay=7 + K线翻页至 API 耗尽（默认开启）")
    api2.add_argument("--no-full-history", action="store_false", dest="full_history",
                    help="退回一年窗口模式（节省请求量）")
    api2.add_argument("--refresh-history-hours", type=int, default=24,
                    help="全部历史与最新 K 线的刷新间隔（小时），默认 24")
    site = sub.add_parser(
        "siteb-site",
        help="采集 行情站B 全站公开目录、榜单、板块、走势、更新和库存快照",
    )
    site.add_argument(
        "--tasks",
        default="all",
        help="逗号分隔任务：home,recent_add,catalog,ranking,hanging,changes,sectors,trends,updates,inventory,route_health；默认 all",
    )
    site.add_argument("--catalog-pages", type=int, default=0,
                      help="市场目录本次最多请求页数；0 表示完整扫描")
    site.add_argument("--list-pages", type=int, default=0,
                      help="排行、异动、更新列表每项最多请求页数；0 表示完整扫描")
    site.add_argument("--page-size", type=int, default=0,
                      help="市场目录每页条数，范围 50-500；0 使用配置值")
    site.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True,
                      help="从各任务保存的游标继续执行")
    site.add_argument("--inventory-id", action="append", default=[],
                      help="公开库存 Steam ID；可重复传入或使用逗号分隔")
    site.add_argument("--update-detail-limit", type=int, default=0,
                      help="更新任务本次最多拉取详情数；0 表示列表中的全部详情")
    site.add_argument("--route-health", action="store_true",
                      help="额外执行公开路由状态检查")
    site.add_argument("--sector-nodes", type=int, default=0,
                      help="板块树本次最多处理节点数；0 表示完整扫描")
    site.add_argument("--delay", type=float, default=0.0,
                      help="公开请求间隔秒数；0 使用配置值")
    inc = sub.add_parser("incremental",
                       help="持续增量检测：热门 1h / 冷门 5h 轮询 open API 快照")
    inc.add_argument("--max-items", type=int, default=0, help="本次最多刷新条数；0=无限循环")
    inc.add_argument("--hot-interval", type=int, default=0, help="热门间隔秒数（默认取配置）")
    inc.add_argument("--cold-interval", type=int, default=0, help="冷门间隔秒数（默认取配置）")
    security = sub.add_parser("security-check", help="detect fingerprint/WebRTC/egress-IP leaks")
    security.add_argument("--headed", action="store_true", help="show the browser window")
    security.add_argument("--timeout", type=float, default=15.0, help="network probe timeout in seconds")
    ws = sub.add_parser(
        "siteb-ws",
        help="行情站B 实时推送通道（SockJS+STOMP，匿名 JWT）常驻采集",
    )
    ws.add_argument(
        "--max-minutes", type=float, default=0,
        help="本次最长运行分钟数；0 表示常驻（监督器 service 模式）",
    )
    ws.add_argument(
        "--destination", default="/exchange/steam-stock.broadcast/zh",
        help="STOMP 订阅目的地",
    )
    export = sub.add_parser(
        "export",
        help="导出 items/序列为 Parquet 或 CSV，并输出新鲜度与连续性摘要",
    )
    export.add_argument(
        "--datasets", default="",
        help="逗号分隔的数据集组：kline,history,index,kline_ws,listing,rank,snapshot,indicators；默认全部",
    )
    export.add_argument("--format", choices=("parquet", "csv"), default="parquet")
    export.add_argument("--out", default="", help="输出目录（默认 crawler/exports）")
    skin = sub.add_parser(
        "skin-indicators",
        help="计算 CS 饰品市场专属指标（供给/跨平台/挂单簿/动量）",
    )
    skin.add_argument("--lookback-hours", type=int, default=48,
                      help="小时级指标的回看窗口")
    feat = sub.add_parser(
        "features",
        help="L1/L2 特征引擎（最终架构：写入 feature_store，费用口径统一）",
    )
    feat.add_argument("--max-items", type=int, default=0,
                      help="只算最活跃的 N 件饰品（0=全量）")
    sub.add_parser(
        "sentiment-cycle",
        help="情绪周期状态机（狂热/恐慌/出清/冷漠/萌芽判定）",
    )
    sub.add_parser(
        "supply-tracker",
        help="供给耗竭追踪（品类级 exhaustion_score + 供需缺口）",
    )
    sub.add_parser(
        "tag-items",
        help="为全部饰品建立自动标签（品类/磨损/稀有度/规则暴露度）",
    )
    sub.add_parser(
        "pit-snapshot",
        help="目录 point-in-time 快照（回测防幸生者偏差，日频）",
    )
    val = sub.add_parser(
        "validate-features",
        help="验证脚手架：首批特征 IC/DM/分段/功效检验（最终架构 §5）",
    )
    val.add_argument("--sample", type=int, default=1500,
                     help="抽样饰品数（按活跃度）")
    val.add_argument("--months", type=int, default=18,
                     help="回放月数")
    val.add_argument("--cold", action="store_true",
                     help="冷门观察通道采样（操纵者画像段，验证 zero_vol_rise 用）")
    kline_bf = sub.add_parser(
        "kline-backfill",
        help="Open API kline 端点批量回填饰品K线（一次拿全历史）",
    )
    kline_bf.add_argument("--batch-size", type=int, default=200,
                          help="本轮处理的饰品数")
    page_hv = sub.add_parser(
        "page-harvest",
        help="Playwright 页面级批量采集：打开 行情站B 详情页截获 7 个核心端点（type-trend 全历史等）",
    )
    page_hv.add_argument("--batch-size", type=int, default=40,
                         help="本轮打开的详情页数")
    sub.add_parser(
        "cross-validate",
        help="三方数据交叉验证（行情站B vs 行情站A vs Skinport）",
    )
    sub.add_parser(
        "market-monitor",
        help="监控信号：大商建仓/热门萌发/大盘状态",
    )
    sub.add_parser(
        "siteb-open-extra",
        help="Open API 补充端点：每日全量基础目录（跨平台ID映射）+ 官方大盘指数/K线",
    )
    export.add_argument("--no-items", action="store_true", help="不导出饰品目录")
    export.add_argument("--since", default="", help="时间范围下界（ISO 日期或时间戳）")
    export.add_argument("--until", default="", help="时间范围上界（ISO 日期或时间戳）")
    route_check = sub.add_parser("route-check", help="探测运行时线路并输出结构化结果")
    route_check.add_argument(
        "--classes",
        default="all",
        help="逗号分隔线路类别；默认 all",
    )
    route_check.add_argument(
        "--force",
        action="store_true",
        help="忽略现有冷却状态并立即复测",
    )
    route_check.add_argument(
        "--output",
        default="",
        help="结果 JSON 路径；默认写入运行目录",
    )
    supervisor = sub.add_parser("supervisor", help="运行分组采集任务监督器")
    supervisor.add_argument(
        "--once",
        action="store_true",
        help="每个选中任务执行一轮后退出",
    )
    supervisor.add_argument(
        "--tasks",
        default="all",
        help="逗号分隔任务 ID；默认 all",
    )
    supervisor.add_argument(
        "--python",
        default="",
        help="子任务 Python 路径；默认当前解释器",
    )
    web = sub.add_parser("web", help="运行本地 Web 看板")
    web.add_argument("--host", default="127.0.0.1")
    web.add_argument("--port", type=int, default=8800)
    maintenance = sub.add_parser(
        "maintenance",
        help="执行控制库分类、租约回收与 WAL 检查点",
    )
    maintenance.add_argument(
        "--reclassify-limit",
        type=int,
        default=5000,
        help="本轮最多重分类的历史失败票据；0 表示全部",
    )
    maintenance.add_argument(
        "--repair-kline-captures",
        type=int,
        default=0,
        help="仅离线重放指定数量已保存的 行情站B K 线抓包（按断点继续）",
    )
    maintenance.add_argument(
        "--checkpoint",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="对业务库与控制库执行被动 WAL 检查点",
    )
    maintenance.add_argument(
        "--reconcile",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="同步旧成功证据、模块切片与完成度快照",
    )

    sub.add_parser("indicators", help="从已存价格序列重算技术指标")
    sub.add_parser("rebuild", help="从 gzip 原始响应重建规范化数据和指标")
    reconcile = sub.add_parser(
        "reconcile",
        help="核对成功证据并同步采集控制状态",
    )
    reconcile.add_argument(
        "--apply",
        action="store_true",
        help="写入核对后的模块切片、游标与覆盖状态",
    )
    reconcile.add_argument(
        "--sections",
        default="all",
        help="逗号分隔范围：sitea,siteb-history,siteb-extended,siteb-open",
    )
    report = sub.add_parser(
        "report",
        help="生成 JSON/Markdown 覆盖率报告",
    )
    report.add_argument(
        "--deep",
        action="store_true",
        help="刷新主库完整计数与严格历史覆盖缓存",
    )
    sub.add_parser("inventory", help="重建公开端点清单（不读取原始响应正文）")
    doctor = sub.add_parser(
        "doctor",
        help="检查运行环境、站点、数据库和采集进度",
    )
    doctor.add_argument(
        "--deep",
        action="store_true",
        help="执行主库完整计数与历史覆盖扫描",
    )
    api.add_argument("--min-delay", type=float, default=0.15,
                     help="minimum pause between public request batches")
    api.add_argument("--max-delay", type=float, default=0.35,
                     help="maximum pause between public request batches")
    api.add_argument("--job-suffix", default="",
                     help="job cursor suffix for partitioned detail workers")
    api.add_argument("--partition", type=int, default=0,
                     help="detail worker partition id")
    api.add_argument("--partition-total", type=int, default=1,
                     help="detail worker partition count")
    api2.add_argument("--extended-concurrency", type=int, default=4,
                      help="parallel public detail modules per item (1-8)")
    return root


def _storage(settings) -> Storage:
    return Storage(
        settings.database,
        settings.output_dir / "raw",
        settings.control_database,
    )


def _blocked_retry_seconds(storage: Storage, source: str, resume_blocked: bool,
                           retry_minutes: float, now: datetime | None = None) -> float | None:
    """Return seconds until a blocked source may be probed, or None when it may run now."""
    if resume_blocked:
        return None
    with storage.control_connect() as conn:
        row = conn.execute(
            "SELECT status,checked_at FROM coverage WHERE source=? AND dataset='item_browser_history'",
            (source,),
        ).fetchone()
    if not row or row["status"] != "blocked":
        return None
    if retry_minutes <= 0:
        return math.inf
    try:
        checked_at = datetime.fromisoformat(str(row["checked_at"]))
        if checked_at.tzinfo is None:
            checked_at = checked_at.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        remaining = retry_minutes * 60 - (current - checked_at).total_seconds()
        return max(0.0, remaining) or None
    except (TypeError, ValueError):
        return math.inf


def _backfill(args: argparse.Namespace, settings, storage: Storage) -> int:
    if args.batch_size <= 0:
        raise ValueError("--batch-size 必须大于 0")
    proxy = args.proxy or settings.proxy_url
    selected = ["sitea", "siteb"] if args.source == "all" else [args.source]
    if "sitea" in selected:
        recovered = recover_sitea_catalog(storage)
        if not recovered:
            collect_sitea_catalog(storage, settings.browser_headless, max_pages=0, proxy=proxy)
    if "siteb" in selected:
        open_source = SiteBOpenSource(settings, storage)
        if open_source.available():
            if not storage.get_catalog_items("siteb"):
                open_source.catalog()
        else:
            collect_siteb_catalog(storage, settings.browser_headless, max_pages=1, proxy=proxy)
    batches = 0
    consecutive_errors = 0

    def source_blocked(source: str) -> bool:
        return _blocked_retry_seconds(
            storage, source, args.resume_blocked, args.blocked_retry_minutes
        ) is not None

    for source in selected:
        if args.max_batches > 0 and batches >= args.max_batches:
            break
        if source_blocked(source) or not storage.unresolved_failures(f"{source}_browser_histories"):
            continue
        logging.info("Backfill startup retry source=%s", source)
        command = [sys.executable, "-m", "csgo_monitor", "--config", str(args.config),
                   "crawl", "--source", source, "--max-items", str(args.batch_size),
                   "--skip-snapshots", "--retry-failures"]
        if args.proxy:
            command += ["--proxy", args.proxy]
        if args.human_mode:
            command.append("--human-mode")
        result = subprocess.run(command, cwd=settings.project_dir, check=False)
        batches += 1
        consecutive_errors = (
            1
            if result.returncode not in {
                EXIT_OK,
                EXIT_PARTIAL,
                EXIT_TEMPORARY,
            }
            else 0
        )
        if args.delay > 0:
            time.sleep(args.delay)
    while args.max_batches <= 0 or batches < args.max_batches:
        pending_sources = []
        blocked_waits = []
        for source in selected:
            total = storage.catalog_count(source)
            job = f"{source}_browser_histories"
            cursor = int(storage.get_state(job) or 0)
            retryable = len(storage.unresolved_failures(job))
            if cursor >= total and not retryable:
                continue
            retry_after = _blocked_retry_seconds(
                storage, source, args.resume_blocked, args.blocked_retry_minutes
            )
            if retry_after is not None:
                if math.isfinite(retry_after):
                    blocked_waits.append((source, retry_after))
                continue
            pending_sources.append((source, cursor, total, retryable))
        if not pending_sources:
            if blocked_waits:
                source, retry_after = min(blocked_waits, key=lambda value: value[1])
                sleep_seconds = min(60.0, max(1.0, retry_after))
                logging.info("All unfinished sources are blocked; next probe source=%s in %.0fs",
                             source, retry_after)
                time.sleep(sleep_seconds)
                continue
            break
        for source, cursor, total, retryable in pending_sources:
            if args.max_batches > 0 and batches >= args.max_batches:
                break
            logging.info("Backfill batch source=%s cursor=%s/%s retryable=%s",
                         source, cursor, total, retryable)
            command = [sys.executable, "-m", "csgo_monitor", "--config", str(args.config),
                       "crawl", "--source", source, "--max-items", str(args.batch_size),
                       "--skip-snapshots"]
            if args.proxy:
                command += ["--proxy", args.proxy]
            if args.human_mode:
                command.append("--human-mode")
            result = subprocess.run(command, cwd=settings.project_dir, check=False)
            batches += 1
            if result.returncode not in {
                EXIT_OK,
                EXIT_PARTIAL,
                EXIT_TEMPORARY,
            }:
                consecutive_errors += 1
                logging.error("Backfill child process failed source=%s exit=%s",
                              source, result.returncode)
                if consecutive_errors >= 3:
                    logging.error("Backfill stopped after three consecutive child-process failures")
                    return result.returncode
            else:
                consecutive_errors = 0
            if args.delay > 0:
                time.sleep(args.delay)
    if not args.no_indicators:
        print(f"日 K 线写入：{derive_daily_klines(storage)} 行")
        print(f"技术指标写入：{calculate_all(storage)} 行")
    print(json.dumps({
        "batches": batches,
        "progress": {
            source: {
                "cursor": int(storage.get_state(f"{source}_browser_histories") or 0),
                "catalog": storage.catalog_count(source),
                "retryable": len(storage.unresolved_failures(f"{source}_browser_histories")),
                "failures": storage.failure_summary(f"{source}_browser_histories")[0],
                "exhausted": storage.failure_summary(f"{source}_browser_histories")[1],
            } for source in selected
        },
    }, ensure_ascii=False, indent=2))
    return 0


def _doctor(
    settings,
    storage: Storage,
    *,
    deep: bool = False,
    proxy: str | None = None,
) -> int:
    from .http import CHROME_UA

    checks: list[tuple[str, str, str]] = [
        ("Python", "PASS", platform.python_version())
    ]
    raw_samples: list[sqlite3.Row] = []
    states: list[sqlite3.Row] = []
    failure_map: dict[str, dict[str, int]] = {}
    database_metrics: dict[str, object] = {"deep": deep}
    for module in ("requests", "yaml", "playwright"):
        try:
            __import__(module)
            checks.append((module, "PASS", "import ok"))
        except ImportError as exc:
            checks.append((module, "FAIL", str(exc)))
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            browser.close()
        checks.append(("Chromium", "PASS", "launch ok"))
    except Exception as exc:
        checks.append(("Chromium", "FAIL", str(exc)))
    try:
        with storage.connect() as conn:
            tables = {
                str(row[0])
                for row in conn.execute(
                    """SELECT name FROM sqlite_master
                    WHERE type='table'"""
                )
            }
            required = {"captures", "items", "series", "indicators"}
            missing = sorted(required - tables)
            integrity = (
                conn.execute("PRAGMA quick_check").fetchone()[0]
                if deep
                else "schema readable"
            )
            captures = int(conn.execute(
                "SELECT COALESCE(MAX(id),0) FROM captures"
            ).fetchone()[0])
            items = int(conn.execute(
                "SELECT COUNT(*) FROM items"
            ).fetchone()[0])
            series = int(conn.execute(
                "SELECT COALESCE(MAX(rowid),0) FROM series"
            ).fetchone()[0])
            indicators = int(conn.execute(
                "SELECT COALESCE(MAX(rowid),0) FROM indicators"
            ).fetchone()[0])
            if deep:
                captures = int(conn.execute(
                    "SELECT COUNT(*) FROM captures"
                ).fetchone()[0])
                series = int(conn.execute(
                    "SELECT COUNT(*) FROM series"
                ).fetchone()[0])
                indicators = int(conn.execute(
                    "SELECT COUNT(*) FROM indicators"
                ).fetchone()[0])
            raw_samples = conn.execute(
                """SELECT raw_path FROM captures
                WHERE raw_path IS NOT NULL ORDER BY id DESC LIMIT ?""",
                (1000 if deep else 100,),
            ).fetchall()
            page_count = int(conn.execute(
                "PRAGMA page_count"
            ).fetchone()[0])
            page_size = int(conn.execute(
                "PRAGMA page_size"
            ).fetchone()[0])
            freelist = int(conn.execute(
                "PRAGMA freelist_count"
            ).fetchone()[0])
        with storage.control_connect() as conn:
            control_integrity = conn.execute(
                "PRAGMA quick_check"
            ).fetchone()[0]
            states = conn.execute(
                """SELECT job,cursor,updated_at FROM crawl_state
                ORDER BY job"""
            ).fetchall()
            for row in conn.execute(
                """SELECT job,
                SUM(CASE WHEN resolved=0 THEN 1 ELSE 0 END) unresolved,
                SUM(CASE WHEN resolved=0 AND failure_status IN (
                    'retryable','deferred'
                ) THEN 1 ELSE 0 END) pending,
                SUM(CASE WHEN resolved=0 AND failure_status IN (
                    'blocked','not_found','protocol_ticket','exhausted'
                ) THEN 1 ELSE 0 END) terminal
                FROM crawl_failures GROUP BY job"""
            ):
                failure_map[str(row["job"])] = {
                    "unresolved": int(row["unresolved"] or 0),
                    "pending": int(row["pending"] or 0),
                    "terminal": int(row["terminal"] or 0),
                }
        database_metrics.update({
            "captures": captures,
            "items": items,
            "series_rowid_high_water": series,
            "indicators_rowid_high_water": indicators,
            "database_bytes": page_count * page_size,
            "freelist_pages": freelist,
        })
        market_status = (
            "FAIL"
            if missing or (deep and integrity != "ok")
            else "PASS"
        )
        checks.append((
            "Market SQLite",
            market_status,
            (
                f"{integrity}; missing={','.join(missing) or 'none'}; "
                f"items={items}; captures={captures}; "
                f"series_high_water={series}; indicators_high_water={indicators}"
            ),
        ))
        checks.append((
            "Control SQLite",
            "PASS" if control_integrity == "ok" else "FAIL",
            str(control_integrity),
        ))
    except sqlite3.Error as exc:
        checks.append(("SQLite", "FAIL", str(exc)))
    bad = 0
    checked = 0
    for row in raw_samples:
        path = storage.database.parent / str(row["raw_path"])
        try:
            with gzip.open(path, "rb") as handle:
                handle.read(1)
            checked += 1
        except (OSError, EOFError):
            bad += 1
    checks.append((
        "Raw gzip sample",
        "PASS" if bad == 0 else "FAIL",
        f"checked={checked + bad}; corrupt={bad}",
    ))
    checks.append((
        "行情站A token",
        "PASS" if settings.sitea_token else "WARN",
        "configured" if settings.sitea_token else "not configured",
    ))
    checks.append((
        "行情站B API key",
        "PASS" if settings.siteb_api_key else "WARN",
        "configured" if settings.siteb_api_key else "not configured",
    ))
    for label, url in (
        ("行情站A home", "https://sitea.com/home"),
        ("行情站B home", "https://www.siteb.com/"),
    ):
        try:
            # 连通性检查也走配置代理：诊断不应成为绕过代理的直连泄露点。
            request = urllib.request.Request(
                url,
                headers={"User-Agent": CHROME_UA},
            )
            handler = (
                urllib.request.ProxyHandler(
                    {"http": proxy, "https": proxy}
                )
                if proxy
                else urllib.request.ProxyHandler({})
            )
            opener = urllib.request.build_opener(handler)
            with opener.open(request, timeout=15) as response:
                response_status = response.status
            checks.append((
                label,
                "PASS" if 200 <= response_status < 400 else "WARN",
                f"HTTP {response_status}",
            ))
        except Exception as exc:
            checks.append((label, "FAIL", str(exc)))

    pipeline_snapshot = build_pipeline_status(storage)
    pipelines = pipeline_snapshot["pipelines"]
    progress_jobs = {
        source: tuple(
            job
            for row in pipelines.values()
            if row["source"] == source
            for job in row["jobs"]
        )
        for source in ("sitea", "siteb")
    }
    deep_span: dict[str, int] = {}
    if deep:
        with storage.connect() as conn:
            for source in ("sitea", "siteb"):
                deep_span[source] = int(conn.execute(
                    """SELECT COUNT(*) FROM (
                    SELECT item_id FROM series
                    WHERE source=? AND dataset IN (
                        'item_history','item_kline','platform_history'
                    ) AND item_id<>''
                    GROUP BY item_id HAVING
                    julianday(MAX(substr(ts,1,10)))-
                    julianday(MIN(substr(ts,1,10)))>=363
                    )""",
                    (source,),
                ).fetchone()[0])
    for key, row in pipelines.items():
        failures = row["failures"]
        detail = (
            f"catalog={row['catalog']}; attempted={row['attempted']}; "
            f"completed={row['completed']}; remaining={row['remaining']}; "
            f"actionable={failures['actionable']}; "
            f"terminal={failures['terminal']}; "
            f"deferred_slices={row['deferred']}; "
            f"modules={json.dumps(row['module_done'], ensure_ascii=False)}"
        )
        if deep and key in {"sitea_detail", "siteb_site_history"}:
            detail += f"; span_365d={deep_span.get(row['source'], 0)}"
        check_status = (
            "PASS"
            if row["catalog"] > 0
            and row["status"] in {"complete", "limited"}
            and failures["actionable"] == 0
            else "WARN"
        )
        checks.append((row["label"], check_status, detail))
    quota = pipeline_snapshot["quota"]
    checks.append((
        "行情站B shared quota",
        "WARN" if quota["active"] else "PASS",
        (
            f"active={quota['active']}; "
            f"resume_at={quota['resume_at'] or ''}"
        ),
    ))

    print("\n".join(
        f"[{value}] {name}: {detail}"
        for name, value, detail in checks
    ))
    if states:
        selected_jobs = {
            job for jobs in progress_jobs.values() for job in jobs
        }
        print("crawl_state:")
        for row in states:
            if deep or str(row["job"]) in selected_jobs:
                print(
                    f"  {row['job']}={row['cursor']} "
                    f"({row['updated_at']})"
                )
    status = (
        "failed"
        if any(value == "FAIL" for _, value, _ in checks)
        else "partial"
        if any(value == "WARN" for _, value, _ in checks)
        else "ok"
    )
    return emit_task_result(
        "doctor",
        status,
        error_class="environment" if status == "failed" else "",
        metrics={
            "checks": len(checks),
            "passed": sum(value == "PASS" for _, value, _ in checks),
            "warnings": sum(value == "WARN" for _, value, _ in checks),
            "failed": sum(value == "FAIL" for _, value, _ in checks),
            "crawl_states": len(states),
            "database": database_metrics,
        },
        details={
            "checks": [
                {"name": name, "status": value, "detail": detail}
                for name, value, detail in checks
            ],
            "pipelines": pipelines,
            "quota": quota,
        },
    )


def _reconcile(
    args: argparse.Namespace,
    settings,
    storage: Storage,
) -> int:
    sections = [
        value.strip()
        for value in str(args.sections or "all").split(",")
        if value.strip()
    ]
    result = reconcile_control_state(
        storage,
        settings,
        apply=bool(args.apply),
        sections=sections,
    )
    pipelines = result.get("pipelines", {})
    incomplete = sum(
        int(row.get("remaining") or 0)
        for row in pipelines.values()
        if isinstance(row, dict)
    )
    status = "partial" if incomplete else "ok"
    return emit_task_result(
        "reconcile",
        status,
        metrics={
            "applied": bool(args.apply),
            "sections": len(result.get("sections", [])),
            "pipelines": len(pipelines),
            "remaining": incomplete,
        },
        details=result,
    )


def _maintenance(
    args: argparse.Namespace,
    settings,
    storage: Storage,
) -> int:
    classified = storage.reclassify_legacy_failures(
        max(0, args.reclassify_limit)
    )
    repaired_kline = (
        repair_siteb_kline_captures(
            storage, max(1, args.repair_kline_captures)
        )
        if args.repair_kline_captures > 0
        else None
    )
    expired_leases = [dict(row) for row in storage.expired_task_leases()]
    reset_slices = storage.reset_expired_task_slices()
    reconciliation = (
        reconcile_control_state(
            storage,
            settings,
            apply=True,
            sections=("all",),
        )
        if args.reconcile
        else None
    )
    # ---- control 每日备份（SQLite backup API；断点/票据是不可再生资产）----
    backup_info: dict[str, Any] = {}
    try:
        backups_dir = settings.output_dir / "backups"
        backups_dir.mkdir(parents=True, exist_ok=True)
        today = datetime.now().strftime("%Y%m%d")
        target = backups_dir / f"control.daily.{today}.sqlite3"
        if not target.exists():
            src_conn = sqlite3.connect(str(settings.control_database), timeout=30)
            dst_conn = sqlite3.connect(str(target))
            with dst_conn:
                src_conn.backup(dst_conn)
            src_conn.close()
            dst_conn.close()
        dailies = sorted(backups_dir.glob("control.daily.*.sqlite3"))
        for old_backup in dailies[:-7]:
            old_backup.unlink(missing_ok=True)
        # 完整性验证：没验证过的备份等于没备份
        check_conn = sqlite3.connect(str(target))
        integrity = check_conn.execute("PRAGMA quick_check").fetchone()[0]
        check_conn.close()
        backup_info = {
            "path": str(target), "kept": len(dailies[-7:]),
            "integrity": str(integrity),
        }
    except Exception as exc:
        backup_info = {"error": f"{type(exc).__name__}: {exc}"[:200]}

    # ---- 磁盘余量（<80GB 警告，<40GB 严重）----
    disk_info: dict[str, Any] = {}
    try:
        import shutil as _shutil
        usage = _shutil.disk_usage(str(settings.output_dir))
        free_gb = usage.free / (1024 ** 3)
        disk_info = {
            "free_gb": round(free_gb, 1),
            "level": "ok" if free_gb > 80 else ("warn" if free_gb > 40 else "critical"),
        }
    except Exception as exc:
        disk_info = {"error": str(exc)[:100]}

    checkpoints: dict[str, tuple[int, int, int]] = {}
    if args.checkpoint:
        with storage.connect() as conn:
            # sqlite.org 建议：大库 + 数据快速变化时，用 analysis_limit 限制
            # 的近似 ANALYZE 维持查询计划统计新鲜（启发式，多数时候为空操作）。
            try:
                conn.execute("PRAGMA analysis_limit=400")
                conn.execute("PRAGMA optimize")
            except sqlite3.Error:
                pass
            # RESTART 检查点：PASSIVE 在读者持续时永远做不完（实测 WAL
            # 大幅膨胀引发写者互相饿死）；RESTART 会等待读者后完成回卷。
            checkpoints["market"] = tuple(
                int(value)
                for value in conn.execute(
                    "PRAGMA wal_checkpoint(RESTART)"
                ).fetchone()
            )
        if storage.control_database != storage.database:
            with storage.control_connect() as conn:
                checkpoints["control"] = tuple(
                    int(value)
                    for value in conn.execute(
                        "PRAGMA wal_checkpoint(PASSIVE)"
                    ).fetchone()
                )
    return emit_task_result(
        "maintenance",
        "partial" if repaired_kline and repaired_kline["more"] else "ok",
        metrics={
            "classified": classified,
            "repaired_kline": repaired_kline,
            "expired_leases": len(expired_leases),
            "reset_slices": reset_slices,
            "checkpoints": checkpoints,
            "control_backup": backup_info,
            "disk": disk_info,
            "reconciled": bool(reconciliation),
        },
        details={
            "expired": expired_leases[:20],
            "reconciliation": reconciliation,
        },
    )


def main(argv: list[str] | None = None) -> int:
    configure_utf8_streams()
    args = parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = load_settings(args.config)
    # explicit --proxy "" means direct; otherwise CLI flag > config
    if args.proxy is None:
        proxy = settings.proxy_url
    elif args.proxy in ("", "direct"):
        proxy = ""
    else:
        proxy = args.proxy
    storage = _storage(settings)

    if args.command == "web":
        os.environ["CSGO_MONITOR_DATABASE"] = str(settings.database)
        os.environ["CSGO_MONITOR_CONTROL_DATABASE"] = str(
            settings.control_database
        )
        import uvicorn
        from web.app import app

        uvicorn.run(
            app,
            host=args.host,
            port=max(1, min(65535, args.port)),
            log_level="warning",
        )
        return emit_task_result("web", "ok")

    if args.command == "maintenance":
        return _maintenance(args, settings, storage)

    if args.command == "reconcile":
        return _reconcile(args, settings, storage)

    if args.command == "siteb-api" and not os.getenv(
        "CSGO_MONITOR_SUPERVISED"
    ):
        # keep worker output visible in per-worker files even when the
        # process is spawned without shell redirection
        err_path = settings.output_dir / f"siteb_api{args.job_suffix}.log.err"
        sys.stderr = open(err_path, "a", encoding="utf-8", buffering=1)
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setStream(sys.stderr)

    if args.command == "route-check":
        from .routing import RouteSelector

        route_classes = [
            value.strip()
            for value in args.classes.split(",")
            if value.strip()
        ]
        if route_classes == ["all"]:
            route_classes = list(settings.runtime_routes)
        unknown = set(route_classes) - set(settings.runtime_routes)
        if unknown:
            logging.error(
                "未知线路类别：%s",
                ", ".join(sorted(unknown)),
            )
            return emit_task_result(
                "route-check",
                "failed",
                error_class="protocol",
                details={"unknown_route_classes": sorted(unknown)},
                exit_code=2,
            )
        selector = RouteSelector(settings, storage)
        report = selector.probe_all(route_classes, force=args.force)
        output = Path(args.output).resolve() if args.output else None
        report_path = selector.write_report(report, output)
        payload = {
            "status": (
                "ok"
                if all(value["selected"] for value in report.values())
                else "unavailable"
            ),
            "report": str(report_path),
            "routes": report,
        }
        return emit_task_result(
            "route-check",
            "ok" if payload["status"] == "ok" else "deferred",
            error_class="" if payload["status"] == "ok" else "transport",
            metrics={"route_classes": len(report)},
            details=payload,
        )

    if args.command == "supervisor":
        from .supervisor import TaskSupervisor

        selected_tasks = [
            value.strip()
            for value in args.tasks.split(",")
            if value.strip()
        ]
        if selected_tasks == ["all"]:
            selected_tasks = []
        try:
            exit_code = TaskSupervisor(
                settings,
                storage,
                Path(args.config),
                python=args.python or sys.executable,
            ).run(
                once=args.once,
                selected_tasks=selected_tasks,
            )
            return emit_task_result(
                "supervisor",
                "ok" if exit_code == 0 else "failed",
                error_class="runtime" if exit_code else "",
                exit_code=exit_code,
            )
        except (RuntimeError, ValueError) as exc:
            logging.error("%s", exc)
            return emit_task_result(
                "supervisor",
                "failed",
                error_class="protocol",
                details={"error": str(exc)},
                exit_code=2,
            )

    if args.command == "discover":
        sources = None if args.source == "all" else [args.source]
        result = BrowserDiscovery(
            storage, headless=not args.headed,
            max_pages=settings.max_browser_pages if args.max_pages is None else args.max_pages,
            human_mode=args.human_mode,
            proxy=proxy,
        ).run(sources)
        payload = {
            key: vars(value)
            for key, value in result.items()
        }
        page_queue = {
            source: storage.public_page_summary(source)
            for source in result
        }
        return emit_task_result(
            "discover",
            (
                "deferred"
                if any(
                    value.quota_paused
                    or value.upstream_paused
                    or value.access_paused
                    for value in result.values()
                )
                else
                "partial"
                if (
                    any(value.failures for value in result.values())
                    or any(
                        any(queue.get(state, 0) for state in
                            ("ready", "deferred", "running"))
                        for queue in page_queue.values()
                    )
                )
                else "ok"
            ),
            error_class=(
                next((value.error_class for value in result.values()
                      if (
                          value.quota_paused
                          or value.upstream_paused
                          or value.access_paused
                      )), "")
            ),
            metrics={
                "sources": payload,
                "page_queue": page_queue,
            },
            details={"selected": sources or ["sitea", "siteb"]},
        )

    elif args.command == "backfill":
        exit_code = _backfill(args, settings, storage)
        json_path, md_path = build_report(storage, settings.output_dir)
        status = (
            "ok"
            if exit_code == EXIT_OK
            else "partial"
            if exit_code == EXIT_PARTIAL
            else "deferred"
            if exit_code == EXIT_TEMPORARY
            else "failed"
        )
        return emit_task_result(
            "backfill",
            status,
            error_class="runtime" if status == "failed" else "",
            details={
                "coverage_json": str(json_path),
                "coverage_markdown": str(md_path),
            },
            exit_code=exit_code,
        )

    elif args.command == "crawl":
        selected = ["sitea", "siteb"] if args.source == "all" else [args.source]
        crawl_results: list[dict[str, object]] = []
        if "sitea" in selected:
            _maybe_start_sitea_sidecar(settings)
        if args.discover:
            BrowserDiscovery(storage, settings.browser_headless, settings.max_browser_pages,
                             human_mode=args.human_mode, proxy=proxy).run(selected)
        if "sitea" in selected:
            sitea = SiteASource(settings, storage)
            if not args.skip_snapshots and not args.human_mode:
                sitea.snapshots()
            ids = recover_sitea_catalog(storage) or storage.get_catalog_items("sitea")
            if not ids:
                # Upgrade databases created before catalog_items existed.
                ids = sitea.stored_item_ids()
                if ids:
                    storage.upsert_catalog_items("sitea", ids)
            if not ids:
                ids = sitea.catalog(max_pages=args.catalog_pages)
            # Browser history is the authorized fallback when no API token is configured.
            browser_mode = args.human_mode or not settings.sitea_token
            if browser_mode and not ids:
                ids = collect_sitea_catalog(
                    storage, settings.browser_headless, max_pages=args.catalog_pages,
                    proxy=proxy
                ) or storage.get_catalog_items("sitea") or sitea.stored_item_ids()
            if browser_mode:
                stored = SiteABrowserHistory(storage, settings.browser_headless, args.human_mode,
                                                proxy=proxy).run(
                    ids, max_items=args.max_items, resume=not args.fresh,
                    retry_only=args.retry_failures,
                ) if ids else 0
            else:
                stored = sitea.histories(ids, max_items=args.max_items, resume=not args.fresh) if ids else 0
            logging.info("行情站A historical points stored: %s", stored)
            crawl_results.append({
                "source": "sitea",
                "status": "ok",
                "stored": stored,
            })
        if "siteb" in selected:
            open_source = SiteBOpenSource(settings, storage)
            if open_source.available() and not args.browser_only:
                if not args.skip_snapshots:
                    open_source.snapshots()
                if args.fresh or not storage.get_catalog_items("siteb"):
                    open_source.catalog()
                siteb_rows = open_source.histories(
                    max_items=args.max_items, resume=not args.fresh,
                    mode=args.hist_mode,
                    refresh_kline_hours=max(0, args.refresh_kline_hours),
                )
                crawl_results.append({
                    "source": "siteb",
                    **open_source.last_result,
                })
            else:
                if not args.skip_snapshots and not args.human_mode:
                    SiteBSource(settings, storage).snapshots()
                if not storage.get_catalog_items("siteb"):
                    collect_siteb_catalog(storage, settings.browser_headless, args.catalog_pages,
                                               proxy=proxy)
                siteb_rows = SiteBBrowserHistory(storage, settings.browser_headless, args.human_mode,
                                                        proxy=proxy).run(
                    max_items=args.max_items, resume=not args.fresh,
                    retry_only=args.retry_failures,
                    force_visit=args.force_browser_visits,
                )
                failure_counts = storage.failure_counts(
                    "siteb_browser_histories"
                )
                crawl_results.append({
                    "source": "siteb",
                    "status": (
                        "ok"
                        if not failure_counts["retryable"]
                        and not failure_counts["deferred"]
                        else "partial"
                    ),
                    "error_class": "",
                    "metrics": {
                        "stored": siteb_rows,
                        "failures": failure_counts,
                    },
                    "details": {
                        "job": "siteb_browser_histories",
                    },
                })
            logging.info("行情站B historical points stored: %s", siteb_rows)
        if args.with_indicators:
            klines = derive_daily_klines(storage)
            logging.info("Derived daily K-line rows: %s", klines)
            count = calculate_all(storage)
            logging.info("Technical indicator rows calculated: %s", count)
        else:
            logging.info("Indicator rebuild deferred; run `python -m csgo_monitor indicators` after collection")
        status = aggregate_status(
            result.get("status")
            for result in crawl_results
        )
        error_class = next(
            (
                str(result.get("error_class") or "")
                for result in crawl_results
                if result.get("error_class")
            ),
            "",
        )
        return emit_task_result(
            "crawl",
            status,
            error_class=error_class,
            metrics={"sources": crawl_results},
            details={
                "selected": selected,
                "max_items": args.max_items,
                "history_mode": args.hist_mode,
            },
        )

    elif args.command == "incremental":
        from .incremental import IncrementalCollector
        collector = IncrementalCollector(
            storage, settings,
            hot_interval=args.hot_interval or settings.hot_interval_hours * 3600,
            cold_interval=args.cold_interval or settings.cold_interval_hours * 3600,
            hot_tx_threshold=settings.hot_tx_threshold,
            broker_platforms=settings.broker_platforms,
            hot_broker_sell_threshold=settings.hot_broker_sell_threshold,
            hot_broker_single_threshold=settings.hot_broker_single_threshold,
            hot_buff_sell_threshold=settings.hot_buff_sell_threshold,
            rate_per_minute=settings.rate_per_minute,
            fetch_avg_for_hot=settings.fetch_avg_for_hot,
            api_keys=settings.siteb_api_keys,
        )
        processed = collector.run(max_items=args.max_items)
        run_result = collector.last_result
        return emit_task_result(
            "incremental",
            str(run_result.get("status") or "ok"),
            error_class=str(run_result.get("error_class") or ""),
            metrics=dict(run_result.get("metrics") or {
                "processed": processed,
            }),
            details=dict(run_result.get("details") or {}),
        )
    elif args.command == "siteb-site":
        requested_tasks = [
            item.strip() for item in args.tasks.split(",") if item.strip()
        ]
        try:
            tasks = SiteBSiteCollector.parse_tasks(requested_tasks)
        except ValueError as exc:
            logging.error("%s", exc)
            return emit_task_result(
                "siteb-site",
                "failed",
                error_class="protocol",
                details={"error": str(exc)},
                exit_code=2,
            )
        inventory_ids = [
            item.strip()
            for value in args.inventory_id
            for item in value.split(",")
            if item.strip()
        ]
        collector = SiteBSiteCollector(
            settings,
            storage,
            delay=max(0.05, args.delay or settings.siteb_site_delay),
            # 保留空串语义：supervisor 选中 direct 时传 ""，这里必须原样
            # 传下去；`proxy or None` 会把显式直连吞成 None，让 collector
            # 回落到 config 里的代理地址（历史上一次代理故障的根因）。
            proxy=proxy,
        )
        result = collector.run(
            tasks,
            catalog_pages=max(0, args.catalog_pages),
            page_size=max(50, args.page_size or settings.siteb_site_page_size),
            list_pages=max(0, args.list_pages),
            update_detail_limit=max(0, args.update_detail_limit),
            resume=args.resume,
            inventory_ids=inventory_ids,
            include_route_health=args.route_health,
            sector_nodes=max(0, args.sector_nodes),
        )
        payload = result.as_dict()
        statuses = [
            (
                "ok"
                if str(value.get("status") or "") == "skipped"
                else str(value.get("status") or "failed")
            )
            for value in payload.values()
        ]
        status = aggregate_status(statuses)
        error_class = next(
            (
                str(value.get("error_class") or "")
                for value in payload.values()
                if value.get("error_class")
            ),
            "",
        )
        return emit_task_result(
            "siteb-site",
            status,
            error_class=error_class,
            metrics={"tasks": payload},
            details={"selected_tasks": tasks},
        )

    elif args.command == "siteb-api":
        from .siteb_api import SiteBApiCollector
        from datetime import date as _date, timedelta as _timedelta
        collector = SiteBApiCollector(storage, delay=args.delay, proxy=proxy,
                                       impersonate=args.impersonate or None,
                                        rotate_interval=args.rotate_interval,
                                        controller=args.controller,
                                        job_suffix=args.job_suffix,
                                        partition=args.partition,
                                        partition_total=args.partition_total,
                                        full_history=args.full_history,
                                        refresh_history_hours=args.refresh_history_hours,
                                        extended_concurrency=args.extended_concurrency)
        import time as _time
        if args.repair_trade_captures:
            result = collector.repair_trade_captures(
                max_captures=args.max_items if args.max_items > 0 else 500
            )
            return emit_task_result(
                "siteb-api",
                "partial" if result["cursor"] < result["high_water"]
                else "ok",
                metrics=result,
                details={"job": "siteb_api_trade_capture_repair:v1"},
            )
        if args.trade_day:
            trade_day = (
                _date.today() - _timedelta(days=1)
                if args.trade_day == "yesterday"
                else _date.fromisoformat(args.trade_day)
            )
            processed = collector.run_trade_day(
                trade_day, max_items=args.max_items,
            )
            run_result = collector.last_result
            return emit_task_result(
                "siteb-api", str(run_result.get("status") or "partial"),
                error_class=str(run_result.get("error_class") or ""),
                metrics=dict(run_result.get("metrics") or {}),
                details=dict(run_result.get("details") or {}),
            )
        while True:
            if args.styles_only:
                processed = collector.collect_styles(max_items=args.max_items, resume=True)
                logging.info("行情站B style collector processed %s items", processed)
            elif args.extended_only:
                processed = collector.run_extended(
                    max_items=args.max_items, resume=True,
                    retry_only=args.retry_failures,
                )
                logging.info("行情站B extended collector processed %s items", processed)
            else:
                processed = collector.run(max_items=args.max_items, resume=True,
                                          retry_only=args.retry_failures)
                logging.info("行情站B api collector processed %s items", processed)
            if processed < 0:
                if args.max_items > 0 or args.retry_failures:
                    run_result = collector.last_result
                    return emit_task_result(
                        "siteb-api",
                        str(run_result.get("status") or "deferred"),
                        error_class=str(
                            run_result.get("error_class") or "quota"
                        ),
                        metrics=dict(run_result.get("metrics") or {}),
                        details=dict(run_result.get("details") or {}),
                    )
                remaining = max(collector.rotator.earliest_unblocked(),
                                collector.rotator.network_fuse_until,
                                collector.pause_until) - _time.time()
                logging.info("proxy pool blocked (quota/network); sleeping %.0fs", max(0, remaining))
                _time.sleep(max(60.0, min(remaining, 8 * 3600)))
                continue
            if args.max_items > 0 or args.retry_failures:
                run_result = collector.last_result
                status = str(run_result.get("status") or "ok")
                return emit_task_result(
                    "siteb-api",
                    status,
                    error_class=str(
                        run_result.get("error_class") or ""
                    ),
                    metrics=dict(run_result.get("metrics") or {
                        "processed": processed,
                        "inserted": collector.inserted,
                        "errors": collector.errors,
                    }),
                    details=dict(run_result.get("details") or {
                        "job": collector.job,
                    }),
                )
            _time.sleep(600)
    elif args.command == "sitea-api":
        tasks = [item.strip() for item in args.tasks.split(",") if item.strip()]
        if tasks == ["all"]:
            tasks = list(SiteAApiCollector.TASKS)
        collector = SiteAApiCollector(
            storage, headless=not args.headed,
            human_mode=args.human_mode, proxy=proxy,
            min_delay=args.min_delay, max_delay=args.max_delay,
            job_suffix=args.job_suffix, partition=args.partition,
            partition_total=args.partition_total,
        )
        stored = collector.run(
            tasks,
            max_items=args.max_items,
            resume=not args.fresh,
            retry_only=args.retry_failures,
        )
        logging.info("行情站A api pipeline stored %s series rows", stored)
        task_results = collector.task_results
        status = aggregate_status(
            result.get("status")
            for result in task_results.values()
        )
        error_class = next(
            (
                str(result.get("error_class") or "")
                for result in task_results.values()
                if result.get("error_class")
            ),
            "",
        )
        return emit_task_result(
            "sitea-api",
            status,
            error_class=error_class,
            metrics={
                "stored": stored,
                "tasks": task_results,
            },
            details={"errors": collector.last_task_errors},
        )

    elif args.command == "indicators":
        klines = derive_daily_klines(storage)
        indicators = calculate_all(storage)
        print(f"日 K 线写入：{klines} 行")
        print(f"技术指标写入：{indicators} 行")
        return emit_task_result(
            "indicators",
            "ok",
            metrics={"daily_klines": klines, "indicators": indicators},
        )

    elif args.command == "rebuild":
        metrics = rebuild_from_captures(storage)
        return emit_task_result(
            "rebuild",
            "ok",
            metrics=metrics,
        )

    elif args.command == "inventory":
        count = storage.rebuild_endpoint_inventory()
        return emit_task_result(
            "inventory",
            "ok",
            metrics={"endpoint_inventory": count},
        )

    elif args.command == "catalog":
        results = {}
        if args.source in {"sitea", "all"}:
            ids = collect_sitea_catalog(storage, settings.browser_headless, args.max_pages,
                                            proxy=proxy) if args.refresh \
                else storage.get_catalog_items("sitea")
            if not ids:
                ids = SiteASource(settings, storage).stored_item_ids()
                storage.upsert_catalog_items("sitea", ids)
            if not ids:
                ids = collect_sitea_catalog(storage, settings.browser_headless, args.max_pages,
                                                proxy=proxy)
            ids = recover_sitea_catalog(storage) or ids
            results["sitea"] = len(ids)
        if args.source in {"siteb", "all"}:
            open_source = SiteBOpenSource(settings, storage)
            if open_source.available() and (args.refresh or not storage.get_catalog_items("siteb")):
                ids = open_source.catalog()
            else:
                ids = storage.get_catalog_items("siteb") if not args.refresh else []
                if not ids:
                    ids = collect_siteb_catalog_from_sitemap(storage, settings.browser_headless,
                                                                  proxy=proxy)
                if not ids:
                    ids = collect_siteb_catalog(storage, settings.browser_headless, args.max_pages,
                                                      proxy=proxy)
            results["siteb"] = len(ids)
        return emit_task_result(
            "catalog",
            "ok" if all(results.values()) else "partial",
            error_class="" if all(results.values()) else "data_gap",
            metrics={"catalogs": results},
            details={
                "source": args.source,
                "refresh": bool(args.refresh),
                "max_pages": args.max_pages,
            },
        )

    elif args.command == "security-check":
        from .security_check import run_security_check
        result = run_security_check(proxy=proxy, headless=not args.headed, timeout=args.timeout)
        return emit_task_result(
            "security-check",
            "failed" if result.get("leaks") else "ok",
            error_class="security" if result.get("leaks") else "",
            details=result,
        )

    elif args.command == "siteb-ws":
        from .siteb_ws import SiteBWebsocketCollector
        collector = SiteBWebsocketCollector(
            storage,
            proxy=proxy or None,
            destination=args.destination,
            max_minutes=max(0.0, args.max_minutes),
        )
        code = collector.run()
        payload = collector.last_result
        return emit_task_result(
            "siteb-ws",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
            details=dict(payload.get("details") or {}),
            exit_code=code,
        )

    elif args.command == "cross-validate":
        from .cross_validate import run_cross_validate
        payload = run_cross_validate(storage)
        return emit_task_result(
            "cross-validate",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
        )

    elif args.command == "kline-backfill":
        from .siteb_open_extra import run_kline_backfill
        payload = run_kline_backfill(storage, settings,
                                     batch_size=max(10, args.batch_size))
        return emit_task_result(
            "kline-backfill",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
        )

    elif args.command == "page-harvest":
        from .page_harvest import run_page_harvest
        payload = run_page_harvest(storage, batch_size=max(1, args.batch_size))
        # "rate_limited" 不是合法任务状态；映射为 deferred + rate_limit，
        # 让监督器按既有节奏延后重排（页面封锁不绕过）。
        status = str(payload.get("status") or "ok")
        emit_status = "deferred" if status == "rate_limited" else (
            "failed" if status == "error" else "ok")
        return emit_task_result(
            "page-harvest",
            emit_status,
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
            details=dict(payload.get("details") or {}),
        )

    elif args.command == "market-monitor":
        from .market_monitor import compute_signals_v3
        payload = compute_signals_v3(storage)
        return emit_task_result(
            "market-monitor",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
            details=dict(payload.get("details") or {}),
        )

    elif args.command == "skin-indicators":
        from .skin_indicators import run_skin_indicators
        payload = run_skin_indicators(
            storage, lookback_hours=max(2, args.lookback_hours))
        return emit_task_result(
            "skin-indicators",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
            details=dict(payload.get("details") or {}),
        )

    elif args.command == "features":
        from .features import run_features
        payload = run_features(storage, max_items=max(0, args.max_items))
        return emit_task_result(
            "features",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
            details=dict(payload.get("details") or {}),
        )

    elif args.command == "sentiment-cycle":
        from .sentiment_cycle import run_sentiment_cycle
        payload = run_sentiment_cycle(storage)
        return emit_task_result(
            "sentiment-cycle",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
            details=dict(payload.get("details") or {}),
        )

    elif args.command == "supply-tracker":
        from .supply_tracker import run_supply_tracker
        payload = run_supply_tracker(storage)
        return emit_task_result(
            "supply-tracker",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
            details=dict(payload.get("details") or {}),
        )

    elif args.command == "tag-items":
        from .item_tagger import run_tag_items
        payload = run_tag_items(storage)
        return emit_task_result(
            "tag-items",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
            details=dict(payload.get("details") or {}),
        )

    elif args.command == "pit-snapshot":
        from .pit_snapshot import run_pit_snapshot
        payload = run_pit_snapshot(storage)
        return emit_task_result(
            "pit-snapshot",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
            details=dict(payload.get("details") or {}),
        )

    elif args.command == "validate-features":
        from .validation import run_validation
        payload = run_validation(storage, sample=max(100, args.sample),
                                 months=max(3, args.months),
                                 mode="cold" if args.cold else "top")
        out_dir = settings.output_dir / "validation"
        out_dir.mkdir(parents=True, exist_ok=True)
        mode_tag = "cold" if args.cold else "top"
        out_path = out_dir / f"validation-report-{mode_tag}.json"
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                            encoding="utf-8")
        logging.getLogger(__name__).info("validation report -> %s", out_path)
        return emit_task_result(
            "validate-features",
            "ok",
            error_class="",
            metrics={
                "sample": payload.get("sample_items"),
                "months": payload.get("months"),
                "elapsed_s": payload.get("elapsed_s"),
                "report_path": str(out_path),
            },
            details={},
        )

    elif args.command == "siteb-open-extra":
        from .siteb_open_extra import run_open_extra
        payload = run_open_extra(settings, storage)
        return emit_task_result(
            "siteb-open-extra",
            str(payload.get("status") or "ok"),
            error_class=str(payload.get("error_class") or ""),
            metrics=dict(payload.get("metrics") or {}),
            details=dict(payload.get("details") or {}),
            exit_code=0 if payload.get("status") == "ok" else 1,
        )

    elif args.command == "export":
        from .exporter import run_export
        groups = tuple(
            item.strip() for item in args.datasets.split(",") if item.strip()
        )
        out_dir = Path(args.out) if args.out else settings.output_dir / "exports"
        payload = run_export(
            storage,
            out_dir,
            datasets=groups,
            include_items=not args.no_items,
            fmt=args.format,
            since=args.since.strip(),
            until=args.until.strip(),
        )
        return emit_task_result(
            "export",
            "ok",
            metrics={
                "files": len(payload["files"]),
                "rows": sum(f["rows"] for f in payload["files"]),
            },
            details={
                "files": payload["files"],
                "summaries": payload["summaries"],
                "stale_datasets": payload["stale_datasets"],
            },
        )

    elif args.command == "doctor":
        return _doctor(
            settings,
            storage,
            deep=bool(args.deep),
            proxy=proxy or None,
        )

    elif args.command == "report":
        json_path, md_path = build_report(
            storage,
            settings.output_dir,
            deep=bool(args.deep),
        )
        print(f"数据库：{settings.database}")
        print(f"覆盖率报告：{md_path}")
        print(f"机器可读报告：{json_path}")
        return emit_task_result(
            "report",
            "ok",
            details={
                "database": str(settings.database),
                "coverage_json": str(json_path),
                "coverage_markdown": str(md_path),
            },
        )

    return emit_task_result(
        str(args.command or "unknown"),
        "failed",
        error_class="protocol",
        details={"error": "unhandled command"},
        exit_code=2,
    )


if __name__ == "__main__":
    sys.exit(main())
