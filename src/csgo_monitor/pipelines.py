from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

from .storage import Storage


SITEA_DETAIL_MODULES = ("good", "statistic")
SITEB_HISTORY_MODULES = (
    "lookup",
    "history",
    "kline",
    "comparison",
)
SITEB_EXTENDED_MODULES = (
    "sale_wear",
    "weapon_trace",
    "item_changes",
    "wear_rank",
    "trade_statistics",
    "trade_today",
    "trade_overview",
    "trade_list",
)
PIPELINE_STATUS_JOB = "runtime:pipeline_status:v1"
SITEB_SITE_QUOTA_JOB = "siteb_site_quota_until"


def _sitea_catalog(storage: Storage) -> list[str]:
    with storage.connect() as conn:
        rows = conn.execute(
            """SELECT item_id FROM items
            WHERE source='sitea' ORDER BY item_id"""
        ).fetchall()
    return [str(row["item_id"]) for row in rows]


def _siteb_catalog(
    storage: Storage,
) -> tuple[list[str], dict[str, str]]:
    with storage.connect() as conn:
        rows = conn.execute(
            """SELECT c.item_id,i.market_hash_name
            FROM catalog_memberships c
            JOIN items i
              ON i.source=c.source AND i.item_id=c.item_id
            WHERE c.source='siteb'
              AND c.universe='siteb_open_api'
              AND c.status='active'
              AND i.market_hash_name IS NOT NULL
              AND i.market_hash_name<>''
            ORDER BY c.position,c.item_id"""
        ).fetchall()
        if not rows:
            rows = conn.execute(
                """SELECT item_id,market_hash_name FROM items
                WHERE source='siteb'
                  AND market_hash_name IS NOT NULL
                  AND market_hash_name<>''
                ORDER BY item_id"""
            ).fetchall()
        numeric_rows = conn.execute(
            """SELECT item_id,market_hash_name FROM items
            WHERE source='siteb'
              AND item_id<>''
              AND item_id NOT GLOB '*[^0-9]*'
              AND market_hash_name IS NOT NULL
              AND market_hash_name<>''"""
        ).fetchall()
    catalog: list[str] = []
    parent_by_name: dict[str, str] = {}
    for row in rows:
        market_name = str(row["market_hash_name"])
        if market_name in parent_by_name:
            continue
        parent_id = str(row["item_id"])
        parent_by_name[market_name] = parent_id
        catalog.append(parent_id)
    aliases = {
        str(row["item_id"]): parent_by_name[market_name]
        for row in numeric_rows
        if (market_name := str(row["market_hash_name"])) in parent_by_name
    }
    return catalog, aliases


def _failure_counts(
    storage: Storage,
    jobs: tuple[str, ...],
) -> dict[str, int]:
    counts = {
        "unresolved": 0,
        "retryable": 0,
        "deferred": 0,
        "terminal": 0,
        "due": 0,
    }
    for job in jobs:
        current = storage.failure_counts(job)
        for key in counts:
            counts[key] += int(current.get(key) or 0)
    counts["actionable"] = counts["retryable"] + counts["deferred"]
    return counts


def _updated_at(
    storage: Storage,
    jobs: tuple[str, ...],
) -> str | None:
    marks = ",".join("?" for _ in jobs)
    with storage.control_connect() as conn:
        state_row = conn.execute(
            f"""SELECT MAX(updated_at) FROM crawl_state
            WHERE job IN ({marks})""",
            jobs,
        ).fetchone()
        slice_row = conn.execute(
            f"""SELECT MAX(updated_at) FROM task_slices
            WHERE task_id IN ({marks})""",
            jobs,
        ).fetchone()
    values = [
        str(value)
        for value in (
            state_row[0] if state_row else None,
            slice_row[0] if slice_row else None,
        )
        if value
    ]
    return max(values) if values else None


def shared_quota_status(storage: Storage) -> dict[str, Any]:
    with storage.control_connect() as conn:
        row = conn.execute(
            """SELECT cursor,updated_at,detail_json FROM crawl_state
            WHERE job=?""",
            (SITEB_SITE_QUOTA_JOB,),
        ).fetchone()
    try:
        resume_timestamp = float(row["cursor"] or 0) if row else 0.0
    except (TypeError, ValueError):
        resume_timestamp = 0.0
    active = resume_timestamp > time.time()
    return {
        "active": active,
        "resume_timestamp": int(resume_timestamp) if resume_timestamp else 0,
        "resume_at": (
            datetime.fromtimestamp(resume_timestamp).astimezone().isoformat()
            if resume_timestamp
            else None
        ),
        "updated_at": str(row["updated_at"]) if row else None,
    }


def build_pipeline_status(storage: Storage) -> dict[str, Any]:
    sitea_catalog = _sitea_catalog(storage)
    siteb_catalog, siteb_aliases = _siteb_catalog(storage)
    definitions = {
        "sitea_detail": {
            "source": "sitea",
            "label": "行情站A 全量详情",
            "jobs": (
                "sitea_api_detail_p0",
                "sitea_api_detail_p1",
            ),
            "modules": SITEA_DETAIL_MODULES,
            "catalog": sitea_catalog,
            "aliases": {},
        },
        "siteb_open_kline": {
            "source": "siteb",
            "label": "行情站B 公开日 K",
            "jobs": ("siteb_open_histories_kline",),
            "modules": ("kline",),
            "catalog": siteb_catalog,
            "aliases": {},
        },
        "siteb_site_history": {
            "source": "siteb",
            "label": "行情站B 站点历史",
            "jobs": (
                "siteb_api_histories_b",
                "siteb_api_histories_c",
                "siteb_api_histories_d",
            ),
            "modules": SITEB_HISTORY_MODULES,
            "catalog": siteb_catalog,
            "aliases": siteb_aliases,
        },
        "siteb_extended": {
            "source": "siteb",
            "label": "行情站B 扩展详情",
            "jobs": ("siteb_api_extended",),
            "modules": SITEB_EXTENDED_MODULES,
            "catalog": siteb_catalog,
            "aliases": siteb_aliases,
        },
    }
    quota = shared_quota_status(storage)
    pipelines: dict[str, dict[str, Any]] = {}
    for key, definition in definitions.items():
        jobs = tuple(definition["jobs"])
        modules = tuple(definition["modules"])
        catalog = list(definition["catalog"])
        summary = storage.task_pipeline_summary(
            jobs,
            modules,
            catalog_ids=catalog,
            item_aliases=definition["aliases"],
        )
        failures = _failure_counts(storage, jobs)
        completed = int(summary["completed"])
        remaining = max(0, len(catalog) - completed)
        refresh_due = 0
        if key == "siteb_open_kline":
            cutoff = (
                datetime.now(timezone.utc) - timedelta(hours=24)
            ).isoformat()
            catalog_set = set(catalog)
            with storage.control_connect() as conn:
                due_rows = conn.execute(
                    """SELECT slice_id FROM task_slices
                    WHERE task_id=? AND status='done'
                      AND slice_id LIKE '%:kline'
                      AND (outcome='legacy_data' OR updated_at<=?)""",
                    (jobs[0], cutoff),
                )
                refresh_due = sum(
                    str(row["slice_id"]).removesuffix(":kline") in catalog_set
                    for row in due_rows
                )
        quota_paused = (
            quota["active"]
            and key in {"siteb_site_history", "siteb_extended"}
            and remaining > 0
        )
        if len(catalog) > 0 and remaining == 0 and not failures["actionable"]:
            status = "stale" if refresh_due else "complete"
        elif (
            remaining > 0
            and failures["actionable"] == 0
            and failures["terminal"] >= remaining
            and int(summary["attempted"]) >= len(catalog)
        ):
            status = "limited"
        elif quota_paused:
            status = "deferred"
        elif int(summary["attempted"]) > 0:
            status = "running"
        else:
            status = "pending"
        pipelines[key] = {
            "source": definition["source"],
            "label": definition["label"],
            "jobs": list(jobs),
            "modules": list(modules),
            "catalog": len(catalog),
            "remaining": remaining,
            "refresh_due": refresh_due,
            "percent": (
                round(completed * 100 / len(catalog), 4)
                if catalog
                else 0
            ),
            "status": status,
            "updated_at": _updated_at(storage, jobs),
            "quota_paused": quota_paused,
            "failures": failures,
            **summary,
        }
    return {
        "schema": 1,
        "generated_at": datetime.now().astimezone().isoformat(),
        "quota": quota,
        "pipelines": pipelines,
    }


def refresh_pipeline_status(storage: Storage) -> dict[str, Any]:
    snapshot = build_pipeline_status(storage)
    storage.set_state(
        PIPELINE_STATUS_JOB,
        "1",
        snapshot,
    )
    return snapshot
