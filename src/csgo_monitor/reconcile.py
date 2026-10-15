from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlsplit

from .pipelines import (
    SITEA_DETAIL_MODULES,
    SITEB_EXTENDED_MODULES,
    SITEB_HISTORY_MODULES,
    build_pipeline_status,
    refresh_pipeline_status,
)
from .siteb_api import (
    SiteBApiCollector,
)
from .siteb_open import SiteBOpenSource
from .storage import Storage


SITEA_DETAIL_RECONCILE_SCHEMA = "sitea_detail_evidence:v2"
SITEA_DETAIL_RECONCILE_JOB = "reconcile:sitea_detail_evidence"
# 空 200 evidence 连续观察到多少轮才允许确认 done（D10：空载荷可能是
# 代理限流页等瞬时软故障，单次观察直接定终态会让模块被永久跳过）
SITEA_EMPTY_CONFIRM_ROUNDS = 3


def _payload_outcome(path: Path) -> str | None:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if isinstance(payload, dict):
        if payload.get("success") is False:
            return None
        code = payload.get("code")
        if code not in (None, 0, 200, "0", "200"):
            return None
        data = payload.get("data", payload)
    else:
        data = payload
    return "empty_ok" if data in (None, "", [], {}) else "legacy_data"


def _sitea_capture_evidence(
    storage: Storage,
    endpoint: str,
    *,
    after_id: int = 0,
    known_item_ids: Iterable[str] = (),
) -> dict[str, str]:
    base_url = "https://sitea.com" + endpoint
    evidence: dict[str, str] = {}
    known = {str(item_id) for item_id in known_item_ids}
    with storage.connect() as conn:
        rows = conn.execute(
            """SELECT id,url,request_json,raw_path FROM captures
            INDEXED BY idx_captures_source_url
            WHERE source='sitea' AND status BETWEEN 200 AND 299
              AND raw_path IS NOT NULL
              AND url>=? AND url<? AND id>?
            ORDER BY id DESC""",
            (base_url, base_url + "\uffff", after_id),
        )
        for row in rows:
            request_url = str(row["url"] or "")
            parsed_url = urlsplit(request_url)
            if parsed_url.path != endpoint:
                continue
            try:
                request = json.loads(str(row["request_json"] or "{}"))
            except (TypeError, json.JSONDecodeError):
                request = {}
            item_id = ""
            if isinstance(request, dict):
                candidates = [request]
                candidates.extend(
                    value
                    for value in request.values()
                    if isinstance(value, dict)
                )
                for candidate in candidates:
                    item_id = str(
                        candidate.get("id")
                        or candidate.get("itemId")
                        or ""
                    )
                    if item_id:
                        break
            if not item_id:
                query = parse_qs(parsed_url.query)
                item_id = str(
                    (query.get("id") or query.get("itemId") or [""])[0]
                )
            if not item_id or item_id in known or item_id in evidence:
                continue
            raw_path = str(row["raw_path"] or "")
            target = (storage.database.parent / raw_path).resolve()
            root = storage.database.parent.resolve()
            if target != root and root not in target.parents:
                continue
            outcome = _payload_outcome(target)
            if outcome:
                evidence[item_id] = outcome
    return evidence


def _sitea_series_evidence(storage: Storage) -> dict[str, dict[str, str]]:
    evidence = {module: {} for module in SITEA_DETAIL_MODULES}
    dataset_modules = {
        "item_snapshot": "good",
        "good_statistic": "statistic",
    }
    with storage.connect() as conn:
        for dataset, module in dataset_modules.items():
            rows = conn.execute(
                """SELECT i.item_id FROM items i
                WHERE i.source='sitea'
                  AND EXISTS (
                    SELECT 1 FROM series s
                    INDEXED BY idx_series_dataset_item
                    WHERE s.dataset=?
                      AND s.item_id=i.item_id
                      AND s.source='sitea'
                    LIMIT 1
                  )
                ORDER BY i.item_id""",
                (dataset,),
            ).fetchall()
            evidence[module].update(
                (str(row["item_id"]), "legacy_data")
                for row in rows
            )
    return evidence


def _sitea_catalog(storage: Storage) -> list[str]:
    with storage.connect() as conn:
        rows = conn.execute(
            """SELECT item_id FROM items
            WHERE source='sitea' ORDER BY item_id"""
        ).fetchall()
    return [str(row["item_id"]) for row in rows]


def _apply_sitea_detail(storage: Storage) -> dict[str, Any]:
    catalog = _sitea_catalog(storage)
    marker = storage.get_state(SITEA_DETAIL_RECONCILE_JOB)
    marker_detail = (
        storage.get_state_detail(SITEA_DETAIL_RECONCILE_JOB)
        if marker == SITEA_DETAIL_RECONCILE_SCHEMA
        else {}
    )
    seed_legacy = marker != SITEA_DETAIL_RECONCILE_SCHEMA
    try:
        capture_after = int(
            (marker_detail or {}).get("capture_high_water") or 0
        )
    except (TypeError, ValueError):
        capture_after = 0
    # 跨轮次持久化的"空 200 evidence"观察计数（slice_id -> 轮数）
    empty_seen: dict[str, int] = {}
    raw_empty_seen = (marker_detail or {}).get("empty_seen")
    if isinstance(raw_empty_seen, dict):
        for key, value in raw_empty_seen.items():
            try:
                empty_seen[str(key)] = int(value)
            except (TypeError, ValueError):
                continue
    with storage.connect() as conn:
        capture_high_water = int(conn.execute(
            """SELECT COALESCE(MAX(id),0) FROM captures
            WHERE source='sitea'"""
        ).fetchone()[0])
    evidence = (
        _sitea_series_evidence(storage)
        if seed_legacy
        else {module: {} for module in SITEA_DETAIL_MODULES}
    )
    if capture_high_water > capture_after:
        for endpoint, module in (
            ("/proxies/api/v1/info/good", "good"),
            ("/proxies/api/v1/info/good/statistic", "statistic"),
        ):
            captures = _sitea_capture_evidence(
                storage,
                endpoint,
                after_id=capture_after,
                known_item_ids=evidence[module],
            )
            for item_id, outcome in captures.items():
                evidence[module].setdefault(item_id, outcome)
    result: dict[str, Any] = {}
    for partition in range(2):
        job = f"sitea_api_detail_p{partition}"
        partition_ids = [
            item_id
            for index, item_id in enumerate(catalog)
            if index % 2 == partition
        ]
        confirmable: list[tuple[str, str]] = []
        pending_empty: list[tuple[str, str, str]] = []
        for item_id in partition_ids:
            for module in SITEA_DETAIL_MODULES:
                outcome = evidence[module].get(item_id)
                if not outcome:
                    continue
                slice_id = f"{item_id}:{module}"
                if outcome == "empty_ok":
                    pending_empty.append((slice_id, item_id, module))
                else:
                    confirmable.append((slice_id, outcome))
        promoted = storage.seed_task_slices(
            job,
            confirmable,
            result={"source": "normalized_series_and_captures"},
            promote_existing=True,
        )
        # D10：200+空载荷可能是瞬时软故障（代理限流页/站点抖动），一次
        # 空 evidence 直接升 done 会让该饰品模块被采集端永久跳过。空
        # evidence 只把模块切片挂起为 deferred（采集端按缺模块重试），
        # 仅当跨多轮 reconcile 反复为空（empty_seen 达到阈值）才确认
        # done；采集端自己补到真实数据时会以 done/ok 覆盖，计数作废。
        empty_deferred = 0
        resolved_aliases = [
            "".join(slice_id.rsplit(":", 1))
            for slice_id, _ in confirmable
        ]
        for slice_id, item_id, module in pending_empty:
            existing = storage.task_slice_status(job, slice_id)
            if existing is not None and str(existing["status"]) == "done":
                # 已由采集端落定（真实数据或终态），不再动它
                empty_seen.pop(slice_id, None)
                continue
            rounds = int(empty_seen.get(slice_id, 0)) + 1
            empty_seen[slice_id] = rounds
            if rounds >= SITEA_EMPTY_CONFIRM_ROUNDS:
                promoted += storage.seed_task_slices(
                    job,
                    ((slice_id, "empty_ok"),),
                    result={"source": "capture_empty_confirmed"},
                    promote_existing=True,
                )
                resolved_aliases.append(f"{item_id}{module}")
                empty_seen.pop(slice_id, None)
            else:
                empty_deferred += 1
                storage.update_task_slice(
                    job,
                    slice_id,
                    "deferred",
                    outcome="empty_ok",
                    result={
                        "source": "capture_empty_pending_confirm",
                        "empty_rounds": rounds,
                        "confirm_rounds": SITEA_EMPTY_CONFIRM_ROUNDS,
                    },
                )
        done = storage.done_task_slice_ids(job)
        completed = [
            item_id
            for item_id in partition_ids
            if all(
                f"{item_id}:{module}" in done
                for module in SITEA_DETAIL_MODULES
            )
        ]
        parents = storage.seed_task_slices(
            job,
            ((item_id, "legacy_data") for item_id in completed),
            result={"source": "completed_sitea_detail_modules"},
            promote_existing=True,
        )
        resolved_aliases.extend(completed)
        storage.resolve_failures(job, resolved_aliases)
        summary = storage.task_pipeline_summary(
            (job,),
            SITEA_DETAIL_MODULES,
            catalog_ids=partition_ids,
        )
        status = (
            "ok"
            if int(summary["completed"]) >= len(partition_ids)
            else "partial"
        )
        storage.set_state(
            job,
            str(summary["completed"]),
            {
                "queue_mode": "missing_modules",
                "completed": summary["completed"],
                "remaining": (
                    len(partition_ids) - int(summary["completed"])
                ),
                "catalog": len(partition_ids),
            },
        )
        storage.set_coverage(
            "sitea",
            f"api.detail_p{partition}",
            status,
            int(summary["completed"]),
            note=(
                f"completed={summary['completed']}/{len(partition_ids)}; "
                f"attempted={summary['attempted']}; reconciled={promoted}; "
                f"job={job}"
            ),
        )
        result[job] = {
            **summary,
            "catalog": len(partition_ids),
            "promoted": promoted,
            "parent_promoted": parents,
            "evidence": len(confirmable) + len(pending_empty),
            "empty_deferred": empty_deferred,
            "complete_candidates": len(completed),
        }
    storage.set_state(
        SITEA_DETAIL_RECONCILE_JOB,
        SITEA_DETAIL_RECONCILE_SCHEMA,
        {
            "catalog": len(catalog),
            "module_evidence": {
                module: len(evidence[module])
                for module in SITEA_DETAIL_MODULES
            },
            "capture_after": capture_after,
            "capture_high_water": capture_high_water,
            "empty_seen": empty_seen,
        },
    )
    return result


def _siteb_catalog_ids(
    items: Iterable[dict[str, str]],
) -> list[str]:
    return [str(row["item_id"]) for row in items]


def _apply_siteb_history(
    storage: Storage,
    collector: SiteBApiCollector,
) -> dict[str, Any]:
    catalog = collector.catalog_items()
    known_ids = collector.known_numeric_ids()
    result: dict[str, Any] = {}
    for partition in range(3):
        items = [
            row for index, row in enumerate(catalog)
            if index % 3 == partition
        ]
        job = f"siteb_api_histories_{chr(ord('b') + partition)}"
        promoted = collector._seed_existing_history_slices(
            job,
            items,
            known_ids,
        )
        parents = collector._promote_history_parent_slices(
            job,
            items,
            known_ids,
        )
        summary = storage.task_pipeline_summary(
            (job,),
            SITEB_HISTORY_MODULES,
            catalog_ids=_siteb_catalog_ids(items),
            item_aliases=collector._item_aliases(items, known_ids),
        )
        storage.set_state(
            job,
            str(summary["completed"]),
            {
                "queue_mode": "missing_modules",
                "completed": summary["completed"],
                "remaining": len(items) - int(summary["completed"]),
                "catalog": len(items),
            },
        )
        storage.set_coverage(
            "siteb",
            f"item_site_history_{chr(ord('b') + partition)}",
            (
                "ok"
                if int(summary["completed"]) >= len(items)
                else "partial"
            ),
            int(summary["completed"]),
            note=(
                f"completed={summary['completed']}/{len(items)}; "
                f"attempted={summary['attempted']}; reconciled={promoted}; "
                f"job={job}"
            ),
        )
        result[job] = {
            **summary,
            "catalog": len(items),
            "promoted": promoted,
            "parent_promoted": parents,
        }
    return result


def _apply_siteb_extended(
    storage: Storage,
    collector: SiteBApiCollector,
) -> dict[str, Any]:
    items = collector.catalog_items()
    known_ids = collector.known_numeric_ids()
    seeded = collector._seed_existing_extended_slices(
        "siteb_api_extended",
        known_ids,
    )
    parents = collector._promote_extended_parent_slices(
        "siteb_api_extended",
        items,
        known_ids,
    )
    summary = storage.task_pipeline_summary(
        ("siteb_api_extended",),
        SITEB_EXTENDED_MODULES,
        catalog_ids=_siteb_catalog_ids(items),
        item_aliases=collector._item_aliases(items, known_ids),
    )
    storage.set_state(
        "siteb_api_extended",
        str(summary["completed"]),
        {
            "queue_mode": "missing_modules",
            "completed": summary["completed"],
            "remaining": len(items) - int(summary["completed"]),
            "catalog": len(items),
        },
    )
    storage.set_coverage(
        "siteb",
        "item_extended_data",
        "ok" if int(summary["completed"]) >= len(items) else "partial",
        int(summary["completed"]),
        note=(
            f"completed={summary['completed']}/{len(items)}; "
            f"attempted={summary['attempted']}; "
            f"module_reconciled={seeded}; parent_reconciled={parents}"
        ),
    )
    return {
        **summary,
        "catalog": len(items),
        "module_reconciled": seeded,
        "parent_reconciled": parents,
    }


def _apply_siteb_open(
    storage: Storage,
    source: SiteBOpenSource,
) -> dict[str, Any]:
    job = "siteb_open_histories_kline"
    seeded = source._seed_existing_slices(job, "kline")
    with storage.connect() as conn:
        rows = conn.execute(
            """SELECT c.item_id FROM catalog_memberships c
            WHERE c.source='siteb'
              AND c.universe='siteb_open_api'
              AND c.status='active'
            ORDER BY c.position,c.item_id"""
        ).fetchall()
    catalog_ids = [str(row["item_id"]) for row in rows]
    done = storage.done_task_slice_ids(job)
    parents = [
        item_id
        for item_id in catalog_ids
        if f"{item_id}:kline" in done
    ]
    promoted = storage.seed_task_slices(
        job,
        ((item_id, "legacy_data") for item_id in parents),
        result={"source": "completed_open_kline_modules"},
        promote_existing=True,
    )
    storage.resolve_failures(job, parents)
    summary = storage.task_pipeline_summary(
        (job,),
        ("kline",),
        catalog_ids=catalog_ids,
    )
    storage.set_state(
        job,
        str(summary["completed"]),
        {
            "queue_mode": "missing_modules",
            "completed": summary["completed"],
            "remaining": len(catalog_ids) - int(summary["completed"]),
            "catalog": len(catalog_ids),
        },
    )
    storage.set_coverage(
        "siteb",
        "item_open_api_history_kline",
        (
            "ok"
            if int(summary["completed"]) >= len(catalog_ids)
            else "partial"
        ),
        int(summary["completed"]),
        note=(
            f"completed={summary['completed']}/{len(catalog_ids)}; "
            f"attempted={summary['attempted']}; "
            f"module_reconciled={seeded}; parent_reconciled={promoted}"
        ),
    )
    return {
        **summary,
        "catalog": len(catalog_ids),
        "module_reconciled": seeded,
        "parent_reconciled": promoted,
    }


def reconcile_control_state(
    storage: Storage,
    settings: Any,
    *,
    apply: bool = False,
    sections: Iterable[str] = ("all",),
) -> dict[str, Any]:
    selected = {
        str(section).strip().lower()
        for section in sections
        if str(section).strip()
    }
    if not selected or "all" in selected:
        selected = {"sitea", "siteb-history", "siteb-extended", "siteb-open"}
    allowed = {"sitea", "siteb-history", "siteb-extended", "siteb-open"}
    unknown = selected - allowed
    if unknown:
        raise ValueError(
            "unknown reconcile sections: " + ", ".join(sorted(unknown))
        )
    result: dict[str, Any] = {
        "applied": bool(apply),
        "sections": sorted(selected),
    }
    if not apply:
        snapshot = build_pipeline_status(storage)
        result["pipelines"] = {
            key: value
            for key, value in snapshot["pipelines"].items()
            if (
                key == "sitea_detail"
                and "sitea" in selected
                or key == "siteb_site_history"
                and "siteb-history" in selected
                or key == "siteb_extended"
                and "siteb-extended" in selected
                or key == "siteb_open_kline"
                and "siteb-open" in selected
            )
        }
        result["quota"] = snapshot["quota"]
        return result
    collector = None
    if selected & {"siteb-history", "siteb-extended"}:
        collector = SiteBApiCollector(
            storage,
            delay=getattr(settings, "request_delay", 1.2),
            proxy=None,
            rotate_interval=0,
            full_history=True,
            extended_concurrency=1,
        )
    if "sitea" in selected:
        result["sitea"] = _apply_sitea_detail(storage)
    if "siteb-history" in selected:
        assert collector is not None
        result["siteb_history"] = _apply_siteb_history(
            storage,
            collector,
        )
    if "siteb-extended" in selected:
        assert collector is not None
        result["siteb_extended"] = _apply_siteb_extended(
            storage,
            collector,
        )
    if "siteb-open" in selected:
        result["siteb_open"] = _apply_siteb_open(
            storage,
            SiteBOpenSource(settings, storage),
        )
    snapshot = refresh_pipeline_status(storage)
    result["pipelines"] = snapshot["pipelines"]
    result["quota"] = snapshot["quota"]
    storage.set_state(
        "reconcile:control_state:v1",
        "1",
        result,
    )
    return result
