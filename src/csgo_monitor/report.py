from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from .pipelines import refresh_pipeline_status
from .storage import Storage


REPORT_CACHE_JOB = "report:deep_metrics:v1"


def _endpoint_source_counts(
    endpoint_inventory: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    totals: dict[str, dict[str, Any]] = {}
    for row in endpoint_inventory:
        source = str(row["source"])
        current = totals.setdefault(
            source,
            {
                "source": source,
                "captures": 0,
                "endpoints": 0,
                "successful": 0,
            },
        )
        current["captures"] += int(row.get("captures") or 0)
        current["endpoints"] += 1
        current["successful"] += int(row.get("successful") or 0)
    return [totals[source] for source in sorted(totals)]


def _coverage_metrics(
    coverage: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "source": row["source"],
            "dataset": row["dataset"],
            "metric": "",
            "platform": "",
            "items": None,
            "points": int(row.get("record_count") or 0),
            "earliest": row.get("earliest"),
            "latest": row.get("latest"),
            "mode": "coverage_snapshot",
        }
        for row in coverage
    ]


def _deep_metric_groups(conn) -> list[dict[str, Any]]:
    return [
        {
            **dict(row),
            "mode": "exact",
        }
        for row in conn.execute(
            """SELECT source,dataset,metric,platform,
            COUNT(DISTINCT item_id) AS items,
            COUNT(*) AS points,MIN(ts) AS earliest,MAX(ts) AS latest
            FROM series GROUP BY source,dataset,metric,platform
            ORDER BY source,dataset,metric,platform"""
        )
    ]


def _deep_progress(conn, source: str) -> dict[str, int]:
    history_items = int(conn.execute(
        """SELECT COUNT(DISTINCT item_id)
        FROM series INDEXED BY idx_series_dataset_item
        WHERE source=? AND dataset IN ('item_history','item_kline',
        'platform_history') AND item_id<>''""",
        (source,),
    ).fetchone()[0])
    year_items = int(conn.execute(
        """SELECT COUNT(*) FROM (
        SELECT item_id
        FROM series INDEXED BY idx_series_dataset_item
        WHERE source=? AND dataset IN ('item_history','item_kline',
        'platform_history') AND item_id<>''
        GROUP BY item_id
        HAVING julianday(MAX(substr(ts,1,10)))-
        julianday(MIN(substr(ts,1,10)))>=363)""",
        (source,),
    ).fetchone()[0])
    if source == "sitea":
        complete_items = int(conn.execute(
            """SELECT COUNT(*) FROM (
            SELECT item_id
            FROM series INDEXED BY idx_series_dataset_item
            WHERE source='sitea' AND dataset='item_history'
              AND item_id<>''
            GROUP BY item_id
            HAVING COUNT(DISTINCT metric||'|'||platform)>=13
              AND julianday(MAX(substr(ts,1,10)))-
                  julianday(MIN(substr(ts,1,10)))>=363
            ) h JOIN (
            SELECT item_id
            FROM series INDEXED BY idx_series_dataset_item
            WHERE source='sitea'
              AND dataset IN ('item_kline','derived_kline_1d')
              AND item_id<>''
            GROUP BY item_id
            HAVING COUNT(*)>=360
              AND julianday(MAX(substr(ts,1,10)))-
                  julianday(MIN(substr(ts,1,10)))>=363
            ) k ON k.item_id=h.item_id"""
        ).fetchone()[0])
    else:
        complete_items = int(conn.execute(
            """SELECT COUNT(*) FROM (
            SELECT item_id
            FROM series INDEXED BY idx_series_dataset_item
            WHERE source='siteb' AND dataset='item_history'
              AND item_id<>''
            GROUP BY item_id
            HAVING COUNT(DISTINCT metric)>=5
              AND julianday(MAX(substr(ts,1,10)))-
                  julianday(MIN(substr(ts,1,10)))>=363
            ) h JOIN (
            SELECT item_id
            FROM series INDEXED BY idx_series_dataset_item
            WHERE source='siteb' AND dataset='item_kline'
              AND item_id<>''
            GROUP BY item_id
            HAVING COUNT(*)>=360
              AND julianday(MAX(substr(ts,1,10)))-
                  julianday(MIN(substr(ts,1,10)))>=363
            ) k ON k.item_id=h.item_id JOIN (
            SELECT item_id
            FROM series INDEXED BY idx_series_dataset_item
            WHERE source='siteb' AND dataset='item_snapshot'
            GROUP BY item_id
            HAVING COUNT(DISTINCT metric)>=4
            ) i ON i.item_id=h.item_id JOIN (
            SELECT item_id
            FROM series INDEXED BY idx_series_dataset_item
            WHERE source='siteb' AND dataset='platform_snapshot'
            GROUP BY item_id
            HAVING COUNT(DISTINCT metric)>=4
            ) p ON p.item_id=h.item_id"""
        ).fetchone()[0])
    return {
        "items_with_history": history_items,
        "items_with_one_year_history": year_items,
        "items_with_complete_one_year_metrics": complete_items,
    }


def _cursor_value(states: dict[str, str], jobs: tuple[str, ...]) -> int:
    total = 0
    found = False
    for job in jobs:
        value = states.get(job)
        if value is None:
            continue
        try:
            total += max(0, int(value))
            found = True
        except (TypeError, ValueError):
            continue
    return total if found else 0


def _failure_totals(
    failures: dict[str, dict[str, int]],
    jobs: tuple[str, ...],
) -> tuple[int, int]:
    unresolved = 0
    exhausted = 0
    for job in jobs:
        row = failures.get(job, {})
        unresolved += int(row.get("unresolved") or 0)
        exhausted += int(row.get("exhausted") or 0)
    return unresolved, exhausted


def _write_atomic(path: Path, text: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def _display(value: Any) -> str:
    return "" if value is None else str(value)


def build_report(
    storage: Storage,
    output_dir: Path,
    *,
    deep: bool = False,
) -> tuple[Path, Path]:
    generated_at = datetime.now().astimezone().isoformat()
    with storage.control_connect() as conn:
        coverage = [dict(row) for row in conn.execute(
            """SELECT source,dataset,status,record_count,earliest,latest,
            checked_at,note FROM coverage ORDER BY source,dataset"""
        )]
        cached_row = conn.execute(
            "SELECT detail_json FROM crawl_state WHERE job=?",
            (REPORT_CACHE_JOB,),
        ).fetchone()
    cached: dict[str, Any] = {}
    if cached_row and cached_row[0]:
        try:
            loaded = json.loads(str(cached_row[0]))
            if isinstance(loaded, dict) and loaded.get("schema") == 1:
                cached = loaded
        except json.JSONDecodeError:
            cached = {}

    with storage.connect() as conn:
        if deep:
            counts = {
                "captures": int(conn.execute(
                    "SELECT COUNT(*) FROM captures"
                ).fetchone()[0]),
                "items": int(conn.execute(
                    "SELECT COUNT(*) FROM items"
                ).fetchone()[0]),
                "series": int(conn.execute(
                    "SELECT COUNT(*) FROM series"
                ).fetchone()[0]),
                "indicators": int(conn.execute(
                    "SELECT COUNT(*) FROM indicators"
                ).fetchone()[0]),
            }
        else:
            counts = {
                "captures": int(conn.execute(
                    "SELECT COALESCE(MAX(id),0) FROM captures"
                ).fetchone()[0]),
                "items": int(conn.execute(
                    "SELECT COUNT(*) FROM items"
                ).fetchone()[0]),
                "series": int(conn.execute(
                    "SELECT COALESCE(MAX(rowid),0) FROM series"
                ).fetchone()[0]),
                "indicators": int(conn.execute(
                    "SELECT COALESCE(MAX(rowid),0) FROM indicators"
                ).fetchone()[0]),
            }
        endpoint_inventory = [dict(row) for row in conn.execute(
            """SELECT source,method,path,first_seen,last_seen,captures,
            successful,bytes,request_keys_json,response_schema_json,
            last_raw_path,last_error
            FROM endpoint_inventory ORDER BY source,path,method"""
        )]
        item_counts = {
            str(row["source"]): int(row["count"])
            for row in conn.execute(
                """SELECT source,COUNT(*) count
                FROM items GROUP BY source"""
            )
        }
        catalog_counts = {
            (str(row["source"]), str(row["universe"])): int(row["count"])
            for row in conn.execute(
                """SELECT source,universe,COUNT(*) count
                FROM catalog_memberships
                WHERE status='active'
                GROUP BY source,universe"""
            )
        }
        if deep:
            metric_groups = _deep_metric_groups(conn)
            strict_progress = {
                source: _deep_progress(conn, source)
                for source in ("sitea", "siteb")
            }
        else:
            cached_metrics = cached.get("metric_groups")
            metric_groups = (
                list(cached_metrics)
                if isinstance(cached_metrics, list)
                else _coverage_metrics(coverage)
            )
            cached_progress = cached.get("progress")
            strict_progress = (
                dict(cached_progress)
                if isinstance(cached_progress, dict)
                else {}
            )

    if deep:
        cache_payload = {
            "schema": 1,
            "generated_at": generated_at,
            "metric_groups": metric_groups,
            "progress": strict_progress,
        }
        storage.set_state(REPORT_CACHE_JOB, "1", cache_payload)
        cached = cache_payload

    coverage_index = {
        (str(row["source"]), str(row["dataset"])): row
        for row in coverage
    }
    pipeline_snapshot = refresh_pipeline_status(storage)
    pipelines = pipeline_snapshot["pipelines"]
    public_pages = {
        source: storage.public_page_summary(source)
        for source in ("sitea", "siteb")
    }
    primary_pipelines = {
        "sitea": "sitea_detail",
        "siteb": "siteb_site_history",
    }
    catalog_specs = {
        "sitea": ("sitea_public", "item_catalog_browser"),
        "siteb": ("siteb_open_api", "item_catalog_open_api"),
    }
    progress: dict[str, dict[str, Any]] = {}
    for source in ("sitea", "siteb"):
        universe, catalog_dataset = catalog_specs[source]
        catalog = int(catalog_counts.get((source, universe), 0))
        discovered = int(item_counts.get(source, 0))
        catalog_row = coverage_index.get((source, catalog_dataset), {})
        catalog_status = str(catalog_row.get("status") or "unknown")
        catalog_note = str(catalog_row.get("note") or "")
        expected_match = re.search(r"market_api_total=(\d+)", catalog_note)
        expected = int(expected_match.group(1)) if expected_match else None
        if (
            source == "siteb"
            and int(catalog_row.get("record_count") or 0) > 0
        ):
            expected = int(catalog_row["record_count"])
        pipeline = pipelines[primary_pipelines[source]]
        catalog = max(catalog, int(pipeline["catalog"]))
        denominator = expected or catalog or discovered
        attempted = int(pipeline["attempted"])
        if denominator:
            attempted = min(attempted, denominator)
        failure_counts = pipeline["failures"]
        actionable = int(failure_counts["actionable"])
        terminal = int(failure_counts["terminal"])
        unresolved = int(failure_counts["unresolved"])
        exact = strict_progress.get(source, {})
        history_items = exact.get("items_with_history")
        year_items = exact.get("items_with_one_year_history")
        complete_items = exact.get(
            "items_with_complete_one_year_metrics"
        )
        completion_percent = (
            round(int(complete_items) * 100 / denominator, 4)
            if complete_items is not None and denominator
            else None
        )
        progress[source] = {
            "catalog_items": catalog,
            "expected_catalog_items": expected,
            "catalog_status": catalog_status,
            "catalog_gap": (
                max(0, expected - catalog)
                if expected is not None
                else None
            ),
            "discovered_items": discovered,
            "items_with_history": history_items,
            "items_with_one_year_history": year_items,
            "items_with_complete_one_year_metrics": complete_items,
            "completion_percent": completion_percent,
            "attempted_items": attempted,
            "attempt_percent": (
                round(attempted * 100 / denominator, 4)
                if denominator
                else 0
            ),
            "pipeline_completed_items": int(pipeline["completed"]),
            "pipeline_remaining_items": int(pipeline["remaining"]),
            "actionable_failures": actionable,
            "terminal_failures": terminal,
            "unresolved_failures": unresolved,
            "exhausted_failures": terminal,
        }

    endpoints = [
        {
            "source": row["source"],
            "path": row["path"],
            "captures": int(row["captures"] or 0),
            "successful": int(row["successful"] or 0),
            "raw_responses": (
                int(row["captures"] or 0)
                if row.get("last_raw_path")
                else 0
            ),
            "latest": row["last_seen"],
            "last_error": row.get("last_error") or "",
        }
        for row in endpoint_inventory
    ]
    metric_mode = (
        "exact"
        if deep
        else "cached_deep"
        if cached.get("metric_groups")
        else "coverage_snapshot"
    )
    payload = {
        "generated_at": generated_at,
        "database": str(storage.database),
        "statistics_mode": "deep" if deep else "snapshot",
        "count_mode": "exact" if deep else "rowid_high_water",
        "metric_mode": metric_mode,
        "deep_metrics_checked_at": cached.get("generated_at"),
        "counts": counts,
        "sources": _endpoint_source_counts(endpoint_inventory),
        "endpoints": endpoints,
        "endpoint_inventory": endpoint_inventory,
        "progress": progress,
        "pipelines": pipelines,
        "public_pages": public_pages,
        "quota": pipeline_snapshot["quota"],
        "metrics": metric_groups,
        "coverage": coverage,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "coverage.json"
    md_path = output_dir / "coverage.md"
    _write_atomic(
        json_path,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )

    count_label = "精确计数" if deep else "索引高水位"
    lines = [
        "# 数据覆盖率报告",
        "",
        f"生成时间：{payload['generated_at']}",
        f"统计模式：{'深度统计' if deep else '快照统计'}",
        f"主表计数口径：{count_label}",
        "",
        f"- 原始响应：{counts['captures']}",
        f"- 饰品对象：{counts['items']}",
        f"- 时间序列点：{counts['series']}",
        f"- 技术指标点：{counts['indicators']}",
        "",
        "## 采集管线进度",
        "",
        "| 管线 | 状态 | 目录 | 已尝试 | 已完成 | 剩余 | 待刷新 | 空响应 | 可处理失败 | 数据限制 | 完成度 |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in pipelines.values():
        lines.append(
            f"| {row['label']} | {row['status']} | "
            f"{row['catalog']} | {row['attempted']} | "
            f"{row['completed']} | {row['remaining']} | "
            f"{row.get('refresh_due', 0)} | "
            f"{row.get('unverified_empty', 0)} | "
            f"{row['failures']['actionable']} | "
            f"{row['failures']['terminal']} | "
            f"{row['percent']}% |"
        )
    lines.extend([
        "",
        "## 公开页面队列",
        "",
        "| 来源 | 待访问 | 已保存 | 待重试 | 运行中 |",
        "|---|---:|---:|---:|---:|",
    ])
    for source, page_counts in public_pages.items():
        lines.append(
            f"| {source} | {page_counts.get('ready', 0)} | "
            f"{page_counts.get('done', 0)} | "
            f"{page_counts.get('deferred', 0)} | "
            f"{page_counts.get('running', 0)} |"
        )
    lines.extend([
        "",
        "## 单品历史完整度",
        "",
        "| 来源 | 目录状态 | 目录条目 | 站点总数 | 目录缺口 | 已尝试 | 管线完成 | 可处理失败 | 数据限制 | 有历史 | 满一年 | 严格完整 | 完成度 |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for source, row in progress.items():
        completion = (
            f"{row['completion_percent']}%"
            if row["completion_percent"] is not None
            else ""
        )
        lines.append(
            f"| {source} | {row['catalog_status']} | "
            f"{row['catalog_items']} | "
            f"{_display(row['expected_catalog_items'])} | "
            f"{_display(row['catalog_gap'])} | "
            f"{row['attempted_items']} ({row['attempt_percent']}%) | "
            f"{row['pipeline_completed_items']} | "
            f"{row['actionable_failures']} | "
            f"{row['terminal_failures']} | "
            f"{_display(row['items_with_history'])} | "
            f"{_display(row['items_with_one_year_history'])} | "
            f"{_display(row['items_with_complete_one_year_metrics'])} | "
            f"{completion} |"
        )
    lines.extend([
        "",
        "## 已规范化指标",
        "",
        "| 来源 | 数据集 | 指标 | 平台 | 饰品数 | 点数 | 最早 | 最新 |",
        "|---|---|---|---|---:|---:|---|---|",
    ])
    for row in metric_groups:
        lines.append(
            f"| {row['source']} | {row['dataset']} | "
            f"{row.get('metric') or ''} | "
            f"{row.get('platform') or ''} | "
            f"{_display(row.get('items'))} | "
            f"{row.get('points') or 0} | "
            f"{row.get('earliest') or ''} | "
            f"{row.get('latest') or ''} |"
        )
    lines.extend([
        "",
        "## 覆盖状态",
        "",
        "| 来源 | 数据集 | 状态 | 记录数 | 最早 | 最新 | 备注 |",
        "|---|---|---:|---:|---|---|---|",
    ])
    for row in coverage:
        note = str(row.get("note") or "").replace("|", "\\|").replace(
            "\n", " "
        )
        lines.append(
            f"| {row['source']} | {row['dataset']} | "
            f"{row['status']} | {row['record_count']} | "
            f"{row.get('earliest') or ''} | "
            f"{row.get('latest') or ''} | {note} |"
        )
    lines.extend([
        "",
        "## 端点审计",
        "",
        "| 来源 | 公开路径 | 抓取 | 成功 | 原始响应 | 最后抓取 | 最后错误 |",
        "|---|---|---:|---:|---:|---|---|",
    ])
    for row in endpoints:
        error = str(row["last_error"] or "").replace("|", "\\|").replace(
            "\n", " "
        )
        lines.append(
            f"| {row['source']} | {row['path']} | "
            f"{row['captures']} | {row['successful']} | "
            f"{row['raw_responses']} | {row['latest'] or ''} | "
            f"{error} |"
        )
    lines.extend([
        "",
        "## 说明",
        "",
        "默认快照统计读取控制库、端点清单和主表索引高水位，适合采集运行期间快速生成。",
        "使用 `report --deep` 刷新完整分组与严格历史完成度；后续快照报告复用该深度结果。",
        "满一年表示该饰品最早和最新数据日期相差至少 363 天；严格完整还要求核心历史指标和 K 线达到最低覆盖标准。",
    ])
    _write_atomic(md_path, "\n".join(lines) + "\n")
    return json_path, md_path
