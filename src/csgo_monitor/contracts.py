from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_PARTIAL = 3
EXIT_TEMPORARY = 75

TASK_STATUSES = {
    "ok",
    "partial",
    "deferred",
    "failed",
    "running",
    "stopped",
}

STATUS_PRIORITY = {
    "ok": 0,
    "running": 0,
    "stopped": 1,
    "partial": 2,
    "deferred": 3,
    "failed": 4,
}


def configure_utf8_streams() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="backslashreplace")
            except (OSError, ValueError):
                pass


def exit_code_for_status(status: str) -> int:
    normalized = str(status or "").lower()
    if normalized == "ok":
        return EXIT_OK
    if normalized == "partial":
        return EXIT_PARTIAL
    if normalized == "deferred":
        return EXIT_TEMPORARY
    return EXIT_FAILED


def aggregate_status(statuses: Any) -> str:
    normalized = [
        str(status or "failed").lower()
        for status in statuses
    ]
    if not normalized:
        return "ok"
    return max(
        normalized,
        key=lambda status: STATUS_PRIORITY.get(status, STATUS_PRIORITY["failed"]),
    )


def emit_task_result(
    task: str,
    status: str,
    *,
    error_class: str = "",
    metrics: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
    exit_code: int | None = None,
) -> int:
    normalized = str(status or "failed").lower()
    if normalized not in TASK_STATUSES:
        normalized = "failed"
    code = exit_code_for_status(normalized) if exit_code is None else int(exit_code)
    payload = {
        "schema_version": 1,
        "task": str(task),
        "run_id": os.environ.get("CSGO_MONITOR_RUN_ID", ""),
        "status": normalized,
        "exit_code": code,
        "error_class": str(error_class or ""),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "metrics": metrics or {},
        "details": details or {},
    }
    result_path = os.environ.get("CSGO_MONITOR_RESULT_PATH", "").strip()
    if result_path:
        target = Path(result_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(target)
    print(json.dumps(payload, ensure_ascii=False))
    return code


def read_task_result(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("schema_version") != 1:
        return None
    if str(payload.get("status") or "") not in TASK_STATUSES:
        return None
    return payload
