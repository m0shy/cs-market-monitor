from __future__ import annotations

import gzip
import json
from pathlib import Path

from .ingest import ingest_payload
from .siteb_api import _validate_kline_page
from .storage import Storage


def repair_siteb_kline_captures(
    storage: Storage, max_captures: int = 500,
) -> dict[str, int]:
    """Incrementally re-ingest saved site Kline pages without rebuilding the DB."""
    job = "repair:siteb_site_kline"
    cursor = int(storage.get_state(job) or 0)
    limit = max(1, max_captures)
    with storage.connect() as conn:
        captures = conn.execute(
            """SELECT id,url,request_json,raw_path FROM captures
            WHERE id>? AND source='siteb'
              AND url LIKE '%/api/user/steam/category/v1/kline%'
              AND status BETWEEN 200 AND 299 AND raw_path IS NOT NULL
            ORDER BY id LIMIT ?""",
            (cursor, limit),
        ).fetchall()

    result = {"examined": 0, "pages": 0, "rows_reprocessed": 0,
              "skipped": 0, "errors": 0, "cursor": cursor}
    root = storage.database.parent.resolve()
    for capture in captures:
        capture_id = int(capture["id"])
        try:
            target = (root / Path(str(capture["raw_path"]))).resolve()
            if root not in target.parents:
                raise ValueError("capture path outside data directory")
            with gzip.open(target, "rt", encoding="utf-8") as handle:
                payload = json.load(handle)
            if (
                not isinstance(payload, dict)
                or payload.get("success") is False
                or payload.get("errorCode") not in (None, "", 0, "0")
            ):
                result["skipped"] += 1
            else:
                points, _, _, error = _validate_kline_page(payload.get("data"))
                if error or not points:
                    result["skipped"] += 1
                else:
                    try:
                        request_data = json.loads(
                            capture["request_json"] or "{}"
                        )
                    except (TypeError, json.JSONDecodeError):
                        request_data = {}
                    normalized = dict(payload)
                    normalized["data"] = points
                    _, count = ingest_payload(
                        storage, "siteb", str(capture["url"]),
                        normalized, request_data,
                    )
                    result["pages"] += 1
                    result["rows_reprocessed"] += count
            storage.resolve_failure(job, str(capture_id))
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            result["errors"] += 1
            storage.record_failure(job, str(capture_id), str(exc))
        result["examined"] += 1
        result["cursor"] = capture_id
        storage.set_state(job, str(capture_id), result)

    result["more"] = int(len(captures) == limit)
    return result
