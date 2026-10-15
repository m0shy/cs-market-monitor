from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from typing import Any, Iterable
import uuid

from .config import Settings
from .contracts import (
    EXIT_FAILED,
    EXIT_OK,
    EXIT_PARTIAL,
    EXIT_TEMPORARY,
    read_task_result,
)
from .errors import classify_error
from .pipelines import SITEB_SITE_QUOTA_JOB
from .routing import RouteSelector
from .storage import Storage, utc_now


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    route_class: str
    arguments: tuple[str, ...]
    interval_seconds: int
    startup_delay: int = 0
    catchup_interval: int = 60
    lane: str = "bulk"
    timeout_seconds: int = 6 * 3600
    mode: str = "scheduled"
    restart_delay: int = 30
    worker_slot: bool = True
    priority: int = 50
    failure_jobs: tuple[str, ...] = ()
    process_markers: tuple[str, ...] = ()

    def definition(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["arguments"] = list(self.arguments)
        payload["failure_jobs"] = list(self.failure_jobs)
        payload["process_markers"] = list(self.process_markers)
        return payload


@dataclass
class ActiveTask:
    spec: TaskSpec
    process: subprocess.Popen[Any]
    run_id: str
    route: str
    started_at: str
    started_mono: float
    lease_until: str
    stdout_handle: Any
    stderr_handle: Any
    stdout_path: Path
    stderr_path: Path
    result_path: Path
    timed_out: bool = False


def default_task_specs() -> tuple[TaskSpec, ...]:
    site_common = ("siteb-site", "--delay", "0.5")
    siteb_api_common = (
        "siteb-api",
        "--delay",
        "0.65",
        "--rotate-interval",
        "0",
        "--impersonate",
        "chrome",
        "--full-history",
    )
    sitea_detail_common = (
        "sitea-api",
        "--tasks",
        "detail",
        "--min-delay",
        "0.15",
        "--max-delay",
        "0.35",
        "--max-items",
        "100",
    )
    return (
        TaskSpec(
            "web",
            "local",
            ("web", "--host", "127.0.0.1", "--port", "8800"),
            0,
            mode="service",
            timeout_seconds=0,
            worker_slot=False,
            lane="local",
            priority=100,
            process_markers=(" web ",),
        ),
        TaskSpec(
            "siteb-incremental",
            "siteb_open_api",
            ("incremental", "--max-items", "0"),
            0,
            startup_delay=2,
            mode="service",
            timeout_seconds=0,
            worker_slot=False,
            lane="siteb_open",
            priority=95,
            process_markers=(" incremental ",),
            failure_jobs=("siteb_open_incremental",),
        ),
        TaskSpec(
            # 监控信号：大商建仓/热门萌发/大盘状态，每小时
            "market-monitor",
            "local",
            ("market-monitor",),
            3600,
            startup_delay=400,
            catchup_interval=600,
            lane="maintenance",
            worker_slot=False,
            priority=28,
            timeout_seconds=1800,
            process_markers=("market-monitor",),
        ),
        TaskSpec(
            # CS 饰品市场专属指标（供给/跨平台/挂单簿/动量）：
            # 增量水位计算，每小时一轮。
            "skin-indicators",
            "local",
            ("skin-indicators",),
            3600,
            startup_delay=300,
            catchup_interval=600,
            lane="maintenance",
            worker_slot=False,
            priority=30,
            timeout_seconds=1800,
            process_markers=("skin-indicators",),
        ),
        TaskSpec(
            # L1/L2 特征引擎（最终架构 P0）：双侧净得口径的 20 基础特征
            # + 8 组合特征，写 feature_store（与 series 分离保 PIT）。
            # 全量首批跑完后可降频到日级；先按活跃度前 5000 冒烟。
            "market-features",
            "local",
            ("features", "--max-items", "5000"),
            3600,
            startup_delay=420,
            catchup_interval=900,
            lane="maintenance",
            worker_slot=False,
            priority=32,
            timeout_seconds=3600,
            process_markers=("features",),
        ),
        TaskSpec(
            # 情绪周期状态机（架构 v3 L1 核心引擎）：
            # 判定市场当前处于狂热/恐慌/出清/冷漠/萌芽哪个阶段
            "sentiment-cycle",
            "local",
            ("sentiment-cycle",),
            3600,
            startup_delay=360,
            catchup_interval=600,
            lane="maintenance",
            worker_slot=False,
            priority=35,
            timeout_seconds=600,
            process_markers=("sentiment-cycle",),
        ),
        TaskSpec(
            # 供给耗竭追踪（架构 v3 L2 约束层）：
            # 品类级 supply exhaustion_score + 供需缺口
            "supply-tracker",
            "local",
            ("supply-tracker",),
            3600,
            startup_delay=390,
            catchup_interval=600,
            lane="maintenance",
            worker_slot=False,
            priority=34,
            timeout_seconds=600,
            process_markers=("supply-tracker",),
        ),
        TaskSpec(
            # 目录 PIT 快照（架构 §5.7）：每日一份全量目录 JSONL，
            # 未来回测按当日目录构造宇宙，防幸存者偏差。
            "pit-snapshot",
            "local",
            ("pit-snapshot",),
            86400,
            startup_delay=600,
            catchup_interval=3600,
            lane="maintenance",
            worker_slot=False,
            priority=28,
            timeout_seconds=900,
            process_markers=("pit-snapshot",),
        ),
        TaskSpec(
            # 三方数据交叉验证：行情站B vs 行情站A vs Skinport
            # 检测价格偏离超 10%/30% 的异常饰品
            "cross-validate",
            "local",
            ("cross-validate",),
            3600,
            startup_delay=600,
            catchup_interval=600,
            lane="maintenance",
            worker_slot=False,
            priority=25,
            timeout_seconds=1800,
            process_markers=("cross-validate",),
        ),
        TaskSpec(
            # Open API kline 批量回填：一次请求拿全历史（替代站点 API 逐日查询）
            # 约4.4万饰品 × 1 次 = 6.1h @120/min，大幅快于站点 API 配额方式
            "backfill-main",
            "siteb_open_api",
            ("kline-backfill", "--batch-size", "300"),
            7200,
            startup_delay=120,
            catchup_interval=300,
            lane="siteb_open",
            priority=92,
            timeout_seconds=7200,
            process_markers=("kline-backfill",),
        ),
        TaskSpec(
            # 页面级批量采集：Playwright 打开详情页，截获页面自动请求的
            # 核心端点（type-trend 全历史 / 磨损价格 / 平台对比 / 武器追踪）。
            # 2026-10-11 实测：页面内嵌请求与直接站点 API 配额池相互独立，
            # 直接 API 配额耗尽（err 108）期间页面仍正常返回——因此
            # route_class=local，不随站点配额暂停；页面间 2~4s 随机间隔，
            # 页面级封锁连续 3 次即中止本轮并按 rate_limit 回报，不绕过。
            "siteb-page-harvest",
            "local",
            ("page-harvest", "--batch-size", "40"),
            300,
            startup_delay=180,
            catchup_interval=120,
            lane="page_harvest",
            priority=88,
            timeout_seconds=1800,
            process_markers=("page-harvest",),
        ),
        TaskSpec(
            # Open API 补充端点：官方每日全量目录（跨平台ID映射，内部按日
            # 守卫）+ 大盘指数/K线。每小时一轮，仅 4 个请求。
            "siteb-open-extra",
            "siteb_open_api",
            ("siteb-open-extra",),
            3600,
            startup_delay=90,
            catchup_interval=600,
            lane="siteb_open",
            priority=50,
            timeout_seconds=900,
            process_markers=("siteb-open-extra",),
        ),
        TaskSpec(
            # 行情站B 实时推送通道：SockJS+STOMP 匿名 JWT 订阅广播，
            # 连接后为推送性质，不消耗站点请求配额——因此 route_class 用
            # local：不受站点额度暂停影响（此前被 siteb_site_api 的
            # 共享暂停推迟到午夜，实时数据停摆数小时）。
            "siteb-realtime-ws",
            "local",
            ("siteb-ws", "--max-minutes", "0"),
            0,
            startup_delay=45,
            mode="service",
            timeout_seconds=0,
            worker_slot=False,
            lane="local",
            priority=90,
            process_markers=("siteb-ws",),
        ),
        TaskSpec(
            "siteb-site-fast",
            "siteb_site_api",
            site_common + ("--tasks", "home,recent_add,hanging"),
            600,
            startup_delay=5,
            catchup_interval=60,
            lane="siteb_site_fast",
            priority=80,
            process_markers=(
                "siteb-site",
                "--tasks home,recent_add,hanging",
            ),
        ),
        TaskSpec(
            "siteb-site-changes",
            "siteb_site_api",
            site_common + ("--tasks", "changes", "--list-pages", "10"),
            600,
            startup_delay=10,
            catchup_interval=60,
            lane="siteb_site_fast",
            priority=80,
            process_markers=("siteb-site", "--tasks changes"),
        ),
        TaskSpec(
            "siteb-site-ranking",
            "siteb_site_api",
            site_common + ("--tasks", "ranking", "--list-pages", "10"),
            1800,
            startup_delay=15,
            catchup_interval=90,
            lane="siteb_site_fast",
            priority=75,
            process_markers=("siteb-site", "--tasks ranking"),
        ),
        TaskSpec(
            "siteb-site-trends-updates",
            "siteb_site_api",
            site_common
            + (
                "--tasks",
                "trends,updates",
                "--list-pages",
                "2",
                "--update-detail-limit",
                "100",
            ),
            3600,
            startup_delay=20,
            catchup_interval=120,
            lane="siteb_site_fast",
            priority=75,
            process_markers=(
                "siteb-site",
                "--tasks trends,updates",
            ),
        ),
        TaskSpec(
            "sitea-public",
            "sitea_public",
            (
                "sitea-api",
                "--tasks",
                "public",
                "--max-items",
                "10",
                "--min-delay",
                "0.25",
                "--max-delay",
                "0.6",
            ),
            3600,
            startup_delay=25,
            catchup_interval=300,
            lane="sitea",
            priority=70,
            process_markers=("sitea-api", "--tasks public"),
        ),
        TaskSpec(
            "siteb-history-b",
            "siteb_site_api",
            siteb_api_common
            + (
                "--partition",
                "0",
                "--partition-total",
                "3",
                "--job-suffix",
                "_b",
                "--max-items",
                "500",
            ),
            1800,
            startup_delay=30,
            catchup_interval=30,
            lane="siteb_site_bulk",
            priority=65,
            failure_jobs=("siteb_api_histories_b",),
            process_markers=("siteb-api", "--job-suffix _b"),
        ),
        TaskSpec(
            "siteb-history-c",
            "siteb_site_api",
            siteb_api_common
            + (
                "--partition",
                "1",
                "--partition-total",
                "3",
                "--job-suffix",
                "_c",
                "--max-items",
                "500",
            ),
            1800,
            startup_delay=35,
            catchup_interval=30,
            lane="siteb_site_bulk",
            priority=65,
            failure_jobs=("siteb_api_histories_c",),
            process_markers=("siteb-api", "--job-suffix _c"),
        ),
        TaskSpec(
            "siteb-history-d",
            "siteb_site_api",
            siteb_api_common
            + (
                "--partition",
                "2",
                "--partition-total",
                "3",
                "--job-suffix",
                "_d",
                "--max-items",
                "500",
            ),
            1800,
            startup_delay=40,
            catchup_interval=30,
            lane="siteb_site_bulk",
            priority=65,
            failure_jobs=("siteb_api_histories_d",),
            process_markers=("siteb-api", "--job-suffix _d"),
        ),
        TaskSpec(
            "siteb-kline",
            "siteb_open_api",
            (
                "crawl",
                "--source",
                "siteb",
                "--hist-mode",
                "kline",
                "--refresh-kline-hours",
                "24",
                "--skip-snapshots",
                "--max-items",
                "500",
            ),
            21600,
            startup_delay=45,
            catchup_interval=60,
            lane="siteb_open",
            # K线全量已由 Open API kline-backfill 完成（约4.7万件 > 目录
            # 约4.4万）。此任务只追剩余周级补片，边际价值低但写库压力大，
            # 降频到 6h、降优先级，把写锁让给页面级采集。
            priority=30,
            failure_jobs=("siteb_open_histories_kline",),
            process_markers=("crawl", "--hist-mode kline"),
        ),
        TaskSpec(
            "extended-scan",
            "siteb_site_api",
            siteb_api_common
            + (
                "--extended-only",
                "--extended-concurrency",
                "2",
                "--max-items",
                "500",
            ),
            3600,
            startup_delay=50,
            catchup_interval=60,
            lane="siteb_site_bulk",
            priority=60,
            failure_jobs=("siteb_api_extended",),
            process_markers=("siteb-api", "--extended-only"),
        ),
        TaskSpec(
            "siteb-styles",
            "siteb_site_api",
            siteb_api_common
            + (
                "--styles-only",
                "--job-suffix",
                "_styles",
                "--max-items",
                "100",
            ),
            6 * 3600,
            startup_delay=55,
            catchup_interval=300,
            lane="siteb_site_bulk",
            priority=55,
            failure_jobs=("siteb_site_styles_styles",),
            process_markers=("siteb-api", "--styles-only"),
        ),
        TaskSpec(
            "sitea-detail-p0",
            "sitea_public",
            sitea_detail_common
            + (
                "--job-suffix",
                "_p0",
                "--partition",
                "0",
                "--partition-total",
                "2",
            ),
            1800,
            startup_delay=60,
            catchup_interval=60,
            lane="sitea",
            priority=75,
            failure_jobs=("sitea_api_detail_p0",),
            process_markers=("sitea-api", "--job-suffix _p0"),
        ),
        TaskSpec(
            "sitea-detail-p1",
            "sitea_public",
            sitea_detail_common
            + (
                "--job-suffix",
                "_p1",
                "--partition",
                "1",
                "--partition-total",
                "2",
            ),
            1800,
            startup_delay=65,
            catchup_interval=60,
            lane="sitea",
            priority=75,
            failure_jobs=("sitea_api_detail_p1",),
            process_markers=("sitea-api", "--job-suffix _p1"),
        ),
        TaskSpec(
            "sitea-detail-p0-retry",
            "sitea_public",
            sitea_detail_common
            + (
                "--job-suffix",
                "_p0",
                "--partition",
                "0",
                "--partition-total",
                "2",
                "--retry-failures",
            ),
            3600,
            startup_delay=300,
            catchup_interval=300,
            lane="sitea",
            priority=70,
            failure_jobs=("sitea_api_detail_p0",),
            process_markers=(
                "sitea-api",
                "--job-suffix _p0",
                "--retry-failures",
            ),
        ),
        TaskSpec(
            "sitea-detail-p1-retry",
            "sitea_public",
            sitea_detail_common
            + (
                "--job-suffix",
                "_p1",
                "--partition",
                "1",
                "--partition-total",
                "2",
                "--retry-failures",
            ),
            3600,
            startup_delay=330,
            catchup_interval=300,
            lane="sitea",
            priority=70,
            failure_jobs=("sitea_api_detail_p1",),
            process_markers=(
                "sitea-api",
                "--job-suffix _p1",
                "--retry-failures",
            ),
        ),
        TaskSpec(
            "siteb-site-catalog",
            "siteb_site_api",
            site_common
            + (
                "--tasks",
                "catalog",
                "--catalog-pages",
                "20",
                "--page-size",
                "500",
            ),
            24 * 3600,
            startup_delay=70,
            catchup_interval=20,
            lane="maintenance",
            priority=35,
            process_markers=("siteb-site", "--tasks catalog"),
        ),
        TaskSpec(
            "siteb-site-sectors",
            "siteb_site_api",
            site_common
            + ("--tasks", "sectors", "--sector-nodes", "100"),
            24 * 3600,
            startup_delay=75,
            catchup_interval=30,
            lane="maintenance",
            priority=30,
            process_markers=("siteb-site", "--tasks sectors"),
        ),
        TaskSpec(
            "runtime-report",
            "local",
            ("report",),
            30 * 60,
            startup_delay=90,
            catchup_interval=300,
            lane="local",
            worker_slot=False,
            priority=100,
            timeout_seconds=300,
            process_markers=(" report",),
        ),
        TaskSpec(
            # 每日隐私自检：指纹/WebRTC/出口 IP（含 IPv6）泄露检测，结果进
            # 运行报告，出现泄露时任务标 failed 提醒处理。
            "security-audit",
            "local",
            ("security-check",),
            24 * 3600,
            startup_delay=600,
            catchup_interval=1800,
            lane="local",
            worker_slot=False,
            priority=15,
            timeout_seconds=600,
            process_markers=("security-check",),
        ),
        TaskSpec(
            # 每日数据导出（Parquet）+ 新鲜度/连续性摘要，供清洗与预测使用。
            "daily-export",
            "local",
            ("export",),
            24 * 3600,
            startup_delay=1200,
            catchup_interval=1800,
            lane="local",
            worker_slot=False,
            priority=14,
            timeout_seconds=3600,
            process_markers=(" export",),
        ),
        TaskSpec(
            "daily-refresh",
            "sitea_public",
            (
                "sitea-api",
                "--tasks",
                "catalog,kline,index,vol,case,roi,container,exchange",
                "--min-delay",
                "0.25",
                "--max-delay",
                "0.6",
            ),
            24 * 3600,
            startup_delay=120,
            catchup_interval=600,
            lane="sitea",
            priority=40,
            process_markers=(
                "sitea-api",
                "--tasks catalog,kline,index,vol,case,roi,container,exchange",
            ),
        ),
        TaskSpec(
            "siteb-public-pages",
            "siteb_browser",
            ("discover", "--source", "siteb", "--max-pages", "120"),
            900,
            startup_delay=180,
            catchup_interval=120,
            lane="public_pages",
            worker_slot=False,
            priority=40,
            timeout_seconds=1800,
            process_markers=("discover", "--source siteb"),
        ),
        TaskSpec(
            "siteb-public-item-pages",
            "siteb_browser",
            (
                "crawl",
                "--source",
                "siteb",
                "--browser-only",
                "--human-mode",
                "--skip-snapshots",
                "--max-items",
                "10",
                "--force-browser-visits",
            ),
            1800,
            startup_delay=240,
            catchup_interval=300,
            lane="public_pages",
            worker_slot=False,
            priority=45,
            timeout_seconds=3600,
            failure_jobs=("siteb_browser_histories",),
            process_markers=(
                "crawl",
                "--source siteb",
                "--browser-only",
            ),
        ),
        TaskSpec(
            "sitea-public-pages",
            "sitea_public",
            ("discover", "--source", "sitea", "--max-pages", "100"),
            1800,
            startup_delay=210,
            catchup_interval=120,
            lane="public_pages",
            worker_slot=False,
            priority=30,
            timeout_seconds=1500,
            process_markers=("discover", "--source sitea"),
        ),
        TaskSpec(
            "siteb-kline-recovery",
            "local",
            (
                "maintenance",
                "--reclassify-limit",
                "0",
                "--repair-kline-captures",
                "1000",
                "--no-checkpoint",
            ),
            1800,
            startup_delay=240,
            catchup_interval=120,
            lane="local",
            priority=65,
            timeout_seconds=1800,
            process_markers=("maintenance", "--repair-kline-captures"),
        ),
        TaskSpec(
            "siteb-route-health",
            "siteb_site_api",
            site_common + ("--tasks", "route_health"),
            24 * 3600,
            startup_delay=150,
            catchup_interval=600,
            lane="maintenance",
            priority=20,
            process_markers=("siteb-site", "--tasks route_health"),
        ),
        TaskSpec(
            "runtime-maintenance",
            "local",
            (
                "maintenance",
                "--reclassify-limit",
                "5000",
                "--checkpoint",
                "--reconcile",
            ),
            24 * 3600,
            startup_delay=180,
            catchup_interval=1800,
            lane="local",
            worker_slot=False,
            priority=90,
            timeout_seconds=3600,
            process_markers=(" maintenance ",),
        ),
    )


class TaskSupervisor:
    def __init__(
        self,
        settings: Settings,
        storage: Storage,
        config_path: Path,
        *,
        python: str | None = None,
        tasks: Iterable[TaskSpec] | None = None,
    ):
        self.settings = settings
        self.storage = storage
        self.config_path = config_path.resolve()
        self.python = python or sys.executable
        self.tasks = tuple(tasks or default_task_specs())
        self.selector = RouteSelector(settings, storage)
        self.runtime_dir = settings.output_dir / "runtime"
        self.log_dir = settings.output_dir / "logs"
        self.result_dir = self.runtime_dir / "results"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.result_dir.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.runtime_dir / "supervisor.pid"
        self.stop_path = self.runtime_dir / "supervisor.stop"
        self.lock_acquired = False
        self.owner_id = (
            f"supervisor:{os.getpid()}:{socket.gethostname()}:"
            f"{uuid.uuid4().hex[:12]}"
        )
        self.active: dict[str, ActiveTask] = {}
        self.blocked_since: dict[str, float] = {}
        self.blocked_reasons: dict[str, str] = {}
        now = time.monotonic()
        self.next_due = {
            spec.task_id: now + spec.startup_delay for spec in self.tasks
        }
        # 反饥饿（aging）：记录每个任务最近一次被派发的单调时间。排序时
        # 久未运行的任务获得优先级加成——共享每日配额的池子里，低优先级
        # bulk 任务（extended/styles 等）不再被高优先级任务每天抢先烧完
        # 配额而永远轮空。重启时从 task_runs 恢复，避免重置老化。
        self.last_dispatch: dict[str, float] = {
            spec.task_id: now for spec in self.tasks
        }
        self._seed_last_dispatch()
        self.last_heartbeat = 0.0
        self.last_reap = 0.0
        self.stop_requested = False
        self.failures = 0
        self.partial_runs = 0
        self.deferred_runs = 0
        self.run_once = False

    def _seed_last_dispatch(self) -> None:
        """用最近一次 task_runs 的完成时间初始化老化时钟（单调时间）。"""
        try:
            rows = self.storage.recent_task_finishes(
                [spec.task_id for spec in self.tasks], limit=len(self.tasks)
            )
        except Exception:
            return
        if not rows:
            return
        now = time.time()
        for row in rows:
            task_id = str(row["task_id"])
            if task_id not in self.last_dispatch:
                continue
            try:
                finished = datetime.fromisoformat(
                    str(row["finished_at"]).replace("Z", "+00:00")
                )
            except (TypeError, ValueError):
                continue
            age = max(0.0, now - finished.timestamp())
            # 转成"等效单调时间"：last_dispatch = monotonic_now - age
            self.last_dispatch[task_id] = time.monotonic() - age

    def _effective_priority(self, spec: TaskSpec, now: float) -> float:
        """优先级 + 老化加成：每小时饥饿 +2，封顶 +120（60 小时）。

        结构不变量（M1 红队审查结论）：封顶必须不低于任一车道内最高
        调度基线与最低任务基线之差，否则低优先级任务老化后仍被永久
        压制。local 车道存在 priority=100 的调度任务（runtime-report）
        与 priority=14 的 daily-export，差值 86——封顶 +120 保证任何
        任务充分老化后（60h）都能超过最高基线，不再饿死。若未来加入
        基线差超过 120 的车道拓扑，需同步提高本封顶或改为无上限老化。
        """
        last = self.last_dispatch.get(spec.task_id, now)
        age_hours = max(0.0, (now - last) / 3600.0)
        return spec.priority + min(60.0, age_hours) * 2.0

    def run(
        self,
        *,
        once: bool = False,
        selected_tasks: Iterable[str] | None = None,
    ) -> int:
        selected = {
            str(value).strip()
            for value in (selected_tasks or ())
            if str(value).strip()
        }
        known = {spec.task_id for spec in self.tasks}
        unknown = selected - known
        if unknown:
            raise ValueError(
                "unknown supervisor tasks: " + ", ".join(sorted(unknown))
            )
        if once and selected:
            services = {
                spec.task_id
                for spec in self.tasks
                if spec.task_id in selected and spec.mode == "service"
            }
            if services:
                raise ValueError(
                    "service tasks cannot be used with --once: "
                    + ", ".join(sorted(services))
                )
        specs = tuple(
            spec
            for spec in self.tasks
            if (not selected or spec.task_id in selected)
            and (not once or spec.mode != "service")
        )
        if not specs:
            raise ValueError("no supervisor tasks selected")

        self.run_once = once
        if selected or once:
            now = time.monotonic()
            for spec in specs:
                self.next_due[spec.task_id] = now

        supervisor_started = utc_now()
        heartbeat_started = False
        self._acquire_lock()
        try:
            self._clear_stop_file()
            self.storage.register_task_definitions(
                spec.definition() for spec in self.tasks
            )
            self._recover_runtime(specs, include_legacy=True)
            self.storage.update_heartbeat(
                "supervisor",
                "supervisor",
                pid=os.getpid(),
                status="running",
                route="local",
                started_at=supervisor_started,
                owner_id=self.owner_id,
                detail={"tasks": [spec.task_id for spec in specs]},
            )
            heartbeat_started = True
            loop_backoff = 1.0
            try:
                while not self.stop_requested:
                    if self.stop_path.exists():
                        self.stop_requested = True
                        break
                    try:
                        self._poll_active()
                        self._heartbeat(supervisor_started, specs)
                        self._recover_runtime(specs)
                        self._launch_due(specs)
                        loop_backoff = 1.0
                    except KeyboardInterrupt:
                        raise
                    except Exception as exc:
                        # 主循环异常保护（全量检修 H1）：瞬时 SQLite 锁/
                        # 磁盘满/OS 错误不再击穿整个进程树——记录后退避
                        # 重试，而不是 finally 杀掉全部子任务后退出
                        log.exception(
                            "supervisor loop error (backoff %.0fs): %s",
                            loop_backoff, exc)
                        time.sleep(loop_backoff)
                        loop_backoff = min(300.0, loop_backoff * 2)
                    if once and not self.active and all(
                        self.next_due[spec.task_id] == float("inf")
                        for spec in specs
                    ):
                        break
                    time.sleep(1.0)
            except KeyboardInterrupt:
                log.info("supervisor stop requested")
        finally:
            try:
                self._stop_children()
                if heartbeat_started:
                    self.storage.update_heartbeat(
                        "supervisor",
                        "supervisor",
                        pid=os.getpid(),
                        status="stopped",
                        route="local",
                        started_at=supervisor_started,
                        owner_id=self.owner_id,
                        detail={
                            "failures": self.failures,
                            "partial_runs": self.partial_runs,
                            "deferred_runs": self.deferred_runs,
                        },
                    )
                self._write_heartbeat_file(specs, status="stopped")
            finally:
                self._clear_stop_file()
                self._release_lock()
        return EXIT_FAILED if self.failures else EXIT_OK

    def _launch_due(self, specs: tuple[TaskSpec, ...]) -> None:
        now = time.monotonic()
        due = sorted(
            (
                spec
                for spec in specs
                if spec.task_id not in self.active
                and self.next_due[spec.task_id] <= now
            ),
            key=lambda spec: (
                0 if spec.mode == "service" else 1,
                -self._effective_priority(spec, now),
                self.next_due[spec.task_id],
                spec.task_id,
            ),
        )
        for spec in due:
            reason = self._capacity_reason(spec)
            if reason:
                self.blocked_since.setdefault(spec.task_id, now)
                self.blocked_reasons[spec.task_id] = reason
                continue
            self.blocked_since.pop(spec.task_id, None)
            self.blocked_reasons.pop(spec.task_id, None)
            self._launch(spec)

    def _capacity_available(self, spec: TaskSpec) -> bool:
        return self._capacity_reason(spec) == ""

    def _capacity_reason(self, spec: TaskSpec) -> str:
        if spec.worker_slot:
            active_slots = sum(
                active.spec.worker_slot for active in self.active.values()
            )
            # 日间降速档：同时运行的采集进程数也下调
            limit = self.settings.supervisor_max_parallel
            if (spec.mode == "scheduled" and self._in_quiet_hours()):
                limit = min(limit, getattr(
                    self.settings, "quiet_max_parallel", 3))
            if active_slots >= limit:
                return "global_capacity"
        if spec.mode != "scheduled":
            return ""
        lane_limit = self.settings.supervisor_lane_limits.get(spec.lane, 1)
        active_lane = sum(
            active.spec.mode == "scheduled"
            and active.spec.lane == spec.lane
            for active in self.active.values()
        )
        if active_lane >= lane_limit:
            return f"lane:{spec.lane}"
        return ""

    def _select_route(self, spec: TaskSpec) -> str | None:
        if spec.route_class == "local":
            return "local"
        result = self.selector.select(spec.route_class)
        return result.route if result else None

    def _launch(self, spec: TaskSpec) -> None:
        pause_delay, pause_class = self._shared_source_pause(spec)
        if pause_delay:
            self.deferred_runs += 1
            self._schedule_next(spec, "deferred", pause_class)
            return
        route = self._select_route(spec)
        if route is None:
            run_id = self.storage.start_task_run(
                spec.task_id,
                "",
                {"reason": "no healthy route"},
            )
            self.storage.finish_task_run(
                run_id,
                "deferred",
                exit_code=EXIT_TEMPORARY,
                error_class="transport",
                result={"reason": "no healthy route"},
            )
            self.deferred_runs += 1
            self._schedule_next(spec, "deferred", "transport")
            log.warning("task=%s deferred: no healthy route", spec.task_id)
            return

        started_at = utc_now()
        command = [
            self.python,
            "-u",
            "-m",
            "csgo_monitor.cli",
            "--config",
            str(self.config_path),
        ]
        if route != "local":
            command.extend(("--proxy", route))
        command.extend(spec.arguments)
        run_id = self.storage.start_task_run(
            spec.task_id,
            route,
            {"command": command, "owner_id": self.owner_id},
        )
        if not self.storage.claim_task_lease(
            spec.task_id,
            self.owner_id,
            run_id,
            lease_seconds=self.settings.supervisor_lease_seconds,
            detail={"command": command},
        ):
            self.storage.finish_task_run(
                run_id,
                "deferred",
                exit_code=EXIT_TEMPORARY,
                error_class="lease",
                result={"reason": "task lease is held by another owner"},
            )
            self.deferred_runs += 1
            self._schedule_next(spec, "deferred", "lease")
            return

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        task_log_dir = self.log_dir / spec.task_id
        task_result_dir = self.result_dir / spec.task_id
        task_log_dir.mkdir(parents=True, exist_ok=True)
        task_result_dir.mkdir(parents=True, exist_ok=True)
        base_name = f"{stamp}-{run_id}"
        stdout_path = task_log_dir / f"{base_name}.out.log"
        stderr_path = task_log_dir / f"{base_name}.err.log"
        result_path = task_result_dir / f"{base_name}.json"
        stdout_handle = stdout_path.open("a", encoding="utf-8", buffering=1)
        stderr_handle = stderr_path.open("a", encoding="utf-8", buffering=1)
        environment = dict(os.environ)
        environment.update(
            {
                "CSGO_MONITOR_SUPERVISED": "1",
                "CSGO_MONITOR_RUN_ID": run_id,
                "CSGO_MONITOR_RESULT_PATH": str(result_path),
                "CSGO_MONITOR_TASK_ID": spec.task_id,
                "PYTHONUTF8": "1",
                "PYTHONIOENCODING": "utf-8",
            }
        )
        creationflags = 0x08000000 if os.name == "nt" else 0
        try:
            process = subprocess.Popen(
                command,
                cwd=self.settings.project_dir,
                stdout=stdout_handle,
                stderr=stderr_handle,
                env=environment,
                creationflags=creationflags,
            )
        except Exception as exc:
            stdout_handle.close()
            stderr_handle.close()
            # 启动失败不重置老化：该任务下次仍有高优先级重试
            error_class = classify_error(exc)
            self.storage.release_task_lease(
                spec.task_id,
                self.owner_id,
                status="failed",
            )
            self.storage.finish_task_run(
                run_id,
                "failed",
                exit_code=EXIT_FAILED,
                error_class=error_class,
                result={"error": str(exc), "command": command},
            )
            self.failures += 1
            self._schedule_next(spec, "failed", error_class)
            log.exception("task=%s failed to start", spec.task_id)
            return

        lease_until = self.storage.renew_task_lease(
            spec.task_id,
            self.owner_id,
            lease_seconds=self.settings.supervisor_lease_seconds,
            pid=process.pid,
            detail={
                "run_id": run_id,
                "result_path": str(result_path),
            },
        )
        if lease_until is None:
            self._terminate_tree(process.pid)
            stdout_handle.close()
            stderr_handle.close()
            self.storage.finish_task_run(
                run_id,
                "failed",
                exit_code=EXIT_FAILED,
                error_class="lease",
                result={"reason": "lease renewal failed after launch"},
            )
            self.failures += 1
            self._schedule_next(spec, "failed", "lease")
            return

        active = ActiveTask(
            spec=spec,
            process=process,
            run_id=run_id,
            route=route,
            started_at=started_at,
            started_mono=time.monotonic(),
            lease_until=lease_until,
            stdout_handle=stdout_handle,
            stderr_handle=stderr_handle,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            result_path=result_path,
        )
        self.active[spec.task_id] = active
        # 实际派发成功才重置老化时钟
        self.last_dispatch[spec.task_id] = time.monotonic()
        if self.run_once:
            self.next_due[spec.task_id] = float("inf")
        self.storage.update_heartbeat(
            spec.task_id,
            spec.task_id,
            pid=process.pid,
            status="running",
            route=route,
            started_at=started_at,
            owner_id=self.owner_id,
            lease_until=lease_until,
            detail={
                "run_id": run_id,
                "lane": spec.lane,
                "mode": spec.mode,
                "stdout": str(stdout_path),
                "stderr": str(stderr_path),
                "result": str(result_path),
            },
        )
        log.info(
            "task=%s started pid=%s route=%s run=%s",
            spec.task_id,
            process.pid,
            route,
            run_id,
        )

    def _poll_active(self) -> None:
        for task_id, active in list(self.active.items()):
            elapsed = time.monotonic() - active.started_mono
            return_code = active.process.poll()
            if (
                return_code is None
                and active.spec.timeout_seconds > 0
                and elapsed > active.spec.timeout_seconds
            ):
                active.timed_out = True
                self._terminate_tree(active.process.pid)
                try:
                    active.process.wait(timeout=5)
                except Exception:
                    pass
                return_code = EXIT_TEMPORARY
            if return_code is None:
                continue
            self._finish_active(active, int(return_code), elapsed)
            del self.active[task_id]

    def _finish_active(
        self,
        active: ActiveTask,
        return_code: int,
        elapsed: float,
    ) -> None:
        active.stdout_handle.close()
        active.stderr_handle.close()
        error_text = self._tail(active.stderr_path)
        payload = read_task_result(active.result_path)
        status, error_class = self._classify_result(
            return_code,
            payload,
            error_text,
            timed_out=active.timed_out,
        )
        if status == "failed":
            self.failures += 1
        elif status == "partial":
            self.partial_runs += 1
        elif status == "deferred":
            self.deferred_runs += 1
        result = {
            "task": active.spec.task_id,
            "route": active.route,
            "exit_code": return_code,
            "elapsed_seconds": round(elapsed, 3),
            "stdout_path": str(active.stdout_path),
            "stderr_path": str(active.stderr_path),
            "result_path": str(active.result_path),
            "structured_result": payload,
            "error_sample": error_text[-1000:],
            "timed_out": active.timed_out,
        }
        self.storage.finish_task_run(
            active.run_id,
            status,
            exit_code=return_code,
            elapsed_seconds=elapsed,
            error_class=error_class,
            result=result,
        )
        self.storage.release_task_lease(
            active.spec.task_id,
            self.owner_id,
            status=status,
        )
        self.storage.remove_heartbeat(active.spec.task_id)
        if (
            status in {"deferred", "failed"}
            and error_class == "transport"
            and active.spec.route_class != "local"
        ):
            self.selector.invalidate(active.spec.route_class)
        delay = self._schedule_next(active.spec, status, error_class)
        log.info(
            "task=%s finished status=%s exit=%s elapsed=%.1fs next=%ss",
            active.spec.task_id,
            status,
            return_code,
            elapsed,
            delay,
        )

    @staticmethod
    def _classify_result(
        return_code: int,
        payload: dict[str, Any] | None,
        error_text: str,
        *,
        timed_out: bool,
    ) -> tuple[str, str]:
        if timed_out:
            return "deferred", "timeout"
        if payload:
            status = str(payload.get("status") or "failed")
            error_class = str(payload.get("error_class") or "")
            if status == "running":
                status = "failed"
            elif status == "stopped":
                status = "deferred"
            if return_code in {EXIT_FAILED, 2} and status == "ok":
                status = "failed"
            if status == "failed" and not error_class:
                error_class = classify_error(error_text)
            return status, error_class
        if return_code == EXIT_OK:
            return "ok", ""
        if return_code == EXIT_PARTIAL:
            return "partial", classify_error(error_text)
        if return_code == EXIT_TEMPORARY:
            return "deferred", classify_error(error_text)
        return "failed", classify_error(error_text)

    def _schedule_next(
        self,
        spec: TaskSpec,
        status: str,
        error_class: str,
    ) -> int:
        delay = self._next_delay(spec, status, error_class)
        self.next_due[spec.task_id] = (
            float("inf")
            if self.run_once
            else time.monotonic() + delay
        )
        self.blocked_since.pop(spec.task_id, None)
        self.blocked_reasons.pop(spec.task_id, None)
        return delay

    def _backfill_complete(self) -> bool:
        """检测全量回填是否完成（所有分区 ≥99%）。"""
        # 上下文管理器修复（全量检修II）：control_connect 是 @contextmanager，
        # 直接调用返回 _GeneratorContextManager 而非 Connection——原版
        # con.execute() 会 AttributeError 被静默吞掉，函数永远返回 False
        try:
            with self.storage.control_connect() as con:
                for job in ("siteb_api_histories_b", "siteb_api_histories_c",
                            "siteb_api_histories_d", "siteb_api_extended"):
                    row = con.execute(
                        f"SELECT COUNT(*), SUM(CASE WHEN status='done' AND outcome IN ('ok','empty_ok','legacy_data') THEN 1 ELSE 0 END) FROM task_slices WHERE task_id='{job}'"
                    ).fetchone()
                    if row and row[0] and row[1] is not None:
                        if row[1] / row[0] < 0.99:
                            return False
                return True
        except Exception:
            return False

    def _in_quiet_hours(self) -> bool:
        """日间降速窗口（日间时段）。支持跨午夜窗口。"""
        if not getattr(self.settings, "quiet_hours_enabled", False):
            return False
        now = datetime.now()
        minute = now.hour * 60 + now.minute
        start, end = (self.settings.quiet_start_minute,
                      self.settings.quiet_end_minute)
        if start <= end:
            return start <= minute < end
        return minute >= start or minute < end

    def _next_delay(
        self,
        spec: TaskSpec,
        status: str,
        error_class: str,
    ) -> int:
        pause_delay, _ = self._shared_source_pause(spec)
        if pause_delay:
            return max(5, pause_delay)
        if spec.mode == "service":
            if status == "deferred" and error_class == "quota":
                return 3600
            if status == "deferred" and error_class == "audit":
                return 1800
            if status == "deferred" and error_class == "rate_limit":
                return 600
            return max(5, spec.restart_delay)
        # ---- 调度任务：日间降速档（拉长下次运行间隔）----
        quiet_scale = (self._in_quiet_hours() and
                       getattr(self.settings, "quiet_interval_multiplier", 1.0)) or 1.0
        # ---- 全量回填完成后：历史类任务自动放宽到 1 小时（省磁盘）----
        if (spec.task_id.startswith("siteb-histor")
                or spec.task_id.startswith("extended-scan")
                or spec.task_id.startswith("siteb-styles")):
            if self._backfill_complete():
                return max(3600, spec.interval_seconds)
        if status == "partial":
            return int(max(1, spec.catchup_interval) * quiet_scale)
        if status == "deferred":
            base = {
                "quota": 3600,
                "audit": 1800,
                "rate_limit": 600,
                "authorization": 3600,
                "lease": 30,
                "timeout": max(60, spec.catchup_interval),
            }.get(error_class, 60)
            if spec.failure_jobs:
                waits = [
                    self.storage.next_failure_delay(job, default=base)
                    for job in spec.failure_jobs
                ]
                if waits:
                    return int(max(1, min(max(base, wait) for wait in waits)) * quiet_scale)
            return int(base * quiet_scale)
        if status == "failed":
            return int(min(900, max(60, spec.catchup_interval * 2)) * quiet_scale)
        return int(max(1, spec.interval_seconds) * quiet_scale)

    def _shared_source_pause(self, spec: TaskSpec) -> tuple[int, str]:
        if spec.route_class in {"siteb_site_api", "siteb_browser"}:
            job = SITEB_SITE_QUOTA_JOB
        elif spec.route_class == "sitea_public":
            job = "sitea_public_pause_until"
        else:
            return 0, ""
        try:
            pause_until = float(self.storage.get_state(job) or 0)
        except (TypeError, ValueError):
            return 0, ""
        if pause_until <= time.time():
            return 0, ""
        detail = self.storage.get_state_detail(job) or {}
        error_class = str(detail.get("error_class") or "quota")
        return max(1, int(pause_until - time.time()) + 1), error_class

    def _heartbeat(
        self,
        supervisor_started: str,
        specs: tuple[TaskSpec, ...],
    ) -> None:
        now = time.monotonic()
        if (
            now - self.last_heartbeat
            < self.settings.supervisor_heartbeat_seconds
        ):
            return
        self.last_heartbeat = now
        self.storage.update_heartbeat(
            "supervisor",
            "supervisor",
            pid=os.getpid(),
            status="running",
            route="local",
            started_at=supervisor_started,
            owner_id=self.owner_id,
            detail={
                "active": sorted(self.active),
                "blocked": {
                    task_id: {
                        "reason": self.blocked_reasons.get(task_id, ""),
                        "seconds": round(
                            max(0.0, now - blocked_at),
                            1,
                        ),
                    }
                    for task_id, blocked_at in self.blocked_since.items()
                },
                "failures": self.failures,
                "partial_runs": self.partial_runs,
                "deferred_runs": self.deferred_runs,
            },
        )
        lost_leases: list[str] = []
        for active in self.active.values():
            lease_until = self.storage.renew_task_lease(
                active.spec.task_id,
                self.owner_id,
                lease_seconds=self.settings.supervisor_lease_seconds,
                pid=active.process.pid,
                detail={
                    "run_id": active.run_id,
                    "elapsed_seconds": round(
                        time.monotonic() - active.started_mono,
                        1,
                    ),
                    "result_path": str(active.result_path),
                },
            )
            if lease_until is None:
                lost_leases.append(active.spec.task_id)
                continue
            active.lease_until = lease_until
            self.storage.update_heartbeat(
                active.spec.task_id,
                active.spec.task_id,
                pid=active.process.pid,
                status="running",
                route=active.route,
                started_at=active.started_at,
                owner_id=self.owner_id,
                lease_until=lease_until,
                detail={
                    "run_id": active.run_id,
                    "lane": active.spec.lane,
                    "mode": active.spec.mode,
                    "elapsed_seconds": round(
                        time.monotonic() - active.started_mono,
                        1,
                    ),
                    "stdout": str(active.stdout_path),
                    "stderr": str(active.stderr_path),
                    "result": str(active.result_path),
                },
            )
        for task_id in lost_leases:
            active = self.active.get(task_id)
            if active is not None:
                log.error("task=%s lost lease; terminating", task_id)
                active.timed_out = True
                self._terminate_tree(active.process.pid)
        self._write_heartbeat_file(specs, status="running")

    def _write_heartbeat_file(
        self,
        specs: tuple[TaskSpec, ...],
        *,
        status: str,
    ) -> None:
        now = time.monotonic()
        payload = {
            "status": status,
            "pid": os.getpid(),
            "owner_id": self.owner_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "active": {
                task_id: {
                    "pid": active.process.pid,
                    "route": active.route,
                    "run_id": active.run_id,
                    "lane": active.spec.lane,
                    "mode": active.spec.mode,
                    "lease_until": active.lease_until,
                    "started_at": active.started_at,
                    "stdout": str(active.stdout_path),
                    "stderr": str(active.stderr_path),
                    "result": str(active.result_path),
                }
                for task_id, active in self.active.items()
            },
            "next_due_seconds": {
                spec.task_id: (
                    None
                    if self.next_due[spec.task_id] == float("inf")
                    else round(
                        max(0.0, self.next_due[spec.task_id] - now),
                        1,
                    )
                )
                for spec in specs
            },
            "blocked": {
                task_id: {
                    "reason": self.blocked_reasons.get(task_id, ""),
                    "seconds": round(max(0.0, now - blocked_at), 1),
                }
                for task_id, blocked_at in self.blocked_since.items()
            },
            "failures": self.failures,
            "partial_runs": self.partial_runs,
            "deferred_runs": self.deferred_runs,
        }
        target = self.runtime_dir / "supervisor-heartbeat.json"
        temporary = target.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(target)

    def _recover_runtime(
        self,
        specs: tuple[TaskSpec, ...],
        *,
        include_legacy: bool = False,
    ) -> None:
        now = time.monotonic()
        if not include_legacy and now - self.last_reap < 30:
            return
        self.last_reap = now
        expired = self.storage.expired_task_leases()
        processes = (
            self._process_snapshot()
            if include_legacy or expired
            else {}
        )
        for row in expired:
            task_id = str(row["task_id"])
            owner_id = str(row["owner_id"])
            if owner_id == self.owner_id and task_id in self.active:
                continue
            pid = int(row["pid"] or 0)
            command = processes.get(pid, "")
            if pid and self._managed_command(command, specs):
                self._terminate_tree(pid)
            self.storage.remove_heartbeat(task_id)
        if include_legacy:
            live_lease_pids = {
                int(row["pid"])
                for row in self.storage.task_leases(status="running")
                if row["pid"] and self._lease_owner_alive(str(row["owner_id"]))
            }
            for pid, command in processes.items():
                if pid == os.getpid() or pid in live_lease_pids:
                    continue
                if self._managed_command(command, specs):
                    log.warning("terminating unmanaged collector pid=%s", pid)
                    self._terminate_tree(pid)
        reset = self.storage.reset_expired_task_slices()
        if expired or reset:
            log.info(
                "runtime recovery expired_leases=%s reset_slices=%s",
                len(expired),
                reset,
            )

    def _lease_owner_alive(self, owner_id: str) -> bool:
        parts = owner_id.split(":", 3)
        if len(parts) < 2 or parts[0] != "supervisor":
            return False
        try:
            pid = int(parts[1])
        except ValueError:
            return False
        return self._pid_alive(pid)

    def _managed_command(
        self,
        command: str,
        specs: tuple[TaskSpec, ...],
    ) -> bool:
        normalized = " ".join(str(command or "").lower().split())
        if not normalized:
            return False
        if "web\\app.py" in normalized or "web/app.py" in normalized:
            return True
        if (
            "web.app:app" in normalized
            and "--port 8800" in normalized
        ):
            return True
        project = str(self.settings.project_dir).lower()
        if project not in normalized and "csgo_monitor.cli" not in normalized:
            return False
        for spec in specs:
            markers = tuple(
                " ".join(marker.lower().split())
                for marker in spec.process_markers
                if marker.strip()
            )
            if markers and all(marker in normalized for marker in markers):
                return True
        return False

    @staticmethod
    def _process_snapshot() -> dict[int, str]:
        if os.name == "nt":
            script = (
                "$ErrorActionPreference='SilentlyContinue';"
                "@(Get-CimInstance Win32_Process | "
                "Where-Object {$_.Name -match '^(python|pythonw)\\.exe$'} | "
                "Select-Object ProcessId,CommandLine) | "
                "ConvertTo-Json -Compress"
            )
            try:
                result = subprocess.run(
                    [
                        "powershell.exe",
                        "-NoProfile",
                        "-Command",
                        script,
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=20,
                )
                payload = json.loads(result.stdout or "[]")
            except (
                OSError,
                subprocess.SubprocessError,
                json.JSONDecodeError,
            ):
                return {}
            rows = payload if isinstance(payload, list) else [payload]
            return {
                int(row.get("ProcessId") or 0): str(
                    row.get("CommandLine") or ""
                )
                for row in rows
                if isinstance(row, dict) and row.get("ProcessId")
            }
        try:
            result = subprocess.run(
                ["ps", "-eo", "pid=,args="],
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (OSError, subprocess.SubprocessError):
            return {}
        processes: dict[int, str] = {}
        for line in result.stdout.splitlines():
            value = line.strip().split(maxsplit=1)
            if len(value) != 2:
                continue
            try:
                processes[int(value[0])] = value[1]
            except ValueError:
                continue
        return processes

    def _stop_children(self) -> None:
        for active in list(self.active.values()):
            self._terminate_tree(active.process.pid)
            try:
                active.process.wait(timeout=5)
            except Exception:
                pass
            active.stdout_handle.close()
            active.stderr_handle.close()
            self.storage.finish_task_run(
                active.run_id,
                "stopped",
                exit_code=EXIT_TEMPORARY,
                elapsed_seconds=time.monotonic() - active.started_mono,
                result={
                    "reason": "supervisor stopped",
                    "stdout_path": str(active.stdout_path),
                    "stderr_path": str(active.stderr_path),
                    "result_path": str(active.result_path),
                },
            )
            self.storage.release_task_lease(
                active.spec.task_id,
                self.owner_id,
                status="stopped",
            )
            self.storage.remove_heartbeat(active.spec.task_id)
        self.active.clear()

    @staticmethod
    def _tail(path: Path, limit: int = 8192) -> str:
        try:
            with path.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                handle.seek(max(0, size - limit))
                return handle.read().decode("utf-8", errors="replace")
        except OSError:
            return ""

    @staticmethod
    def _terminate_tree(pid: int) -> None:
        if pid <= 0:
            return
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        try:
            os.kill(pid, 15)
        except OSError:
            pass

    def _acquire_lock(self) -> None:
        if self.lock_path.exists():
            try:
                existing_pid = int(
                    self.lock_path.read_text(encoding="utf-8").strip()
                )
            except (OSError, ValueError):
                existing_pid = 0
            if existing_pid and self._pid_alive(existing_pid):
                raise RuntimeError(
                    f"supervisor already running with pid={existing_pid}"
                )
            try:
                self.lock_path.unlink()
            except OSError:
                pass
        descriptor = os.open(
            self.lock_path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(str(os.getpid()))
        self.lock_acquired = True

    def _release_lock(self) -> None:
        if not self.lock_acquired:
            return
        try:
            current = self.lock_path.read_text(encoding="utf-8").strip()
            if current == str(os.getpid()):
                self.lock_path.unlink()
        except OSError:
            pass
        self.lock_acquired = False

    def _clear_stop_file(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            log.warning("unable to remove stop file: %s", self.stop_path)

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        if os.name == "nt":
            import ctypes

            process_query_limited_information = 0x1000
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(
                process_query_limited_information,
                False,
                pid,
            )
            if not handle:
                return False
            try:
                exit_code = ctypes.c_ulong()
                ok = kernel32.GetExitCodeProcess(
                    handle,
                    ctypes.byref(exit_code),
                )
                return bool(ok) and exit_code.value == 259
            finally:
                kernel32.CloseHandle(handle)
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
