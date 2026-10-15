from __future__ import annotations

import gzip
import json
from pathlib import PurePosixPath

from .indicators import calculate_all, derive_daily_klines
from .ingest import ingest_payload
from .storage import Storage


def _usable_capture_payload(source: str, payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    success = payload.get("success")
    if success is False or (
        isinstance(success, str) and success.strip().lower() == "false"
    ):
        return False
    if source == "sitea" and payload.get("code") is not None:
        code = str(payload.get("code")).strip()
        if code and code not in {"0", "200"}:
            return False
    error_code = payload.get("errorCode")
    if error_code not in (None, "", 0, "0"):
        return False
    return True


def rebuild_from_captures(storage: Storage) -> dict[str, int]:
    """Re-normalize stored raw responses after parser changes."""
    with storage.connect() as conn:
        captures = conn.execute(
            "SELECT source,url,request_json,raw_path FROM captures WHERE status BETWEEN 200 AND 299 AND raw_path IS NOT NULL"
        ).fetchall()
        normalized_datasets = conn.execute(
            "SELECT DISTINCT source,dataset FROM series"
        ).fetchall()
        # Catalog and crawl-state coverage cannot be reconstructed from response bodies.
        conn.execute("DELETE FROM series")
        conn.execute("DELETE FROM indicators")
    with storage.control_connect() as conn:
        conn.executemany(
            "DELETE FROM coverage WHERE source=? AND dataset=?",
            ((row["source"], row["dataset"]) for row in normalized_datasets),
        )
        conn.execute("DELETE FROM coverage WHERE dataset LIKE 'item_history.%'")
    item_count = 0
    series_count = 0
    seen_captures: set[tuple[str, str, str, str]] = set()
    for capture in captures:
        key = (
            capture["source"], capture["raw_path"], capture["url"],
            capture["request_json"] or "",
        )
        if key in seen_captures:
            continue
        seen_captures.add(key)
        raw_path = storage.database.parent / PurePosixPath(capture["raw_path"].replace("\\", "/"))
        try:
            with gzip.open(raw_path, "rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not _usable_capture_payload(capture["source"], payload):
            continue
        try:
            request_data = (
                json.loads(capture["request_json"])
                if capture["request_json"]
                else {}
            )
        except (TypeError, json.JSONDecodeError):
            request_data = {}
        items, series = ingest_payload(storage, capture["source"], capture["url"], payload, request_data)
        item_count += items
        series_count += series
    with storage.connect() as conn:
        groups = conn.execute(
            """SELECT source,dataset,COUNT(*) count,MIN(ts) earliest,MAX(ts) latest
            FROM series GROUP BY source,dataset"""
        ).fetchall()
    for row in groups:
        storage.set_coverage(row["source"], row["dataset"], "captured", row["count"],
                             row["earliest"], row["latest"], "rebuilt from raw responses")
    klines = derive_daily_klines(storage)
    indicators = calculate_all(storage)
    return {"items": item_count, "series": series_count, "klines": klines, "indicators": indicators}
