# -*- coding: utf-8 -*-
"""导出中心 API：文件清单 / 安全下载 / 删除 / 异步触发（纯搬运自 web/app.py）.

EXPORTS_DIR/PROJECT 来自 web.state；页面路由（/data/exports、/exports）仍留在
web/app.py。
"""
from __future__ import annotations
import time as _time

import json
import threading as _threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter
from fastapi.responses import FileResponse, JSONResponse, Response

from web.state import EXPORTS_DIR, PROJECT

router = APIRouter()


# ===========================================================================
# 导出中心：文件清单 / 安全下载 / 删除 / 异步触发
# ===========================================================================

import sys as _sys  # noqa: E402
_EXPORT_PYTHON = _sys.executable
_EXPORT_TASKS: dict[str, dict[str, Any]] = {}
_EXPORT_TASK_LOCK = _threading.Lock()
_EXPORT_RUNNING = _threading.Event()


def _exports_manifest() -> dict[str, Any]:
    path = EXPORTS_DIR / "manifest.json"
    if not path.exists():
        return {"files": {}, "updated_at": None}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"files": {}, "updated_at": None}


def _list_export_files() -> list[dict[str, Any]]:
    manifest = _exports_manifest()
    files = []
    if EXPORTS_DIR.exists():
        for path in sorted(EXPORTS_DIR.iterdir(), reverse=True):
            if not path.is_file() or path.name == "manifest.json":
                continue
            stat = path.stat()
            info = manifest.get("files", {}).get(path.name, {})
            files.append({
                "name": path.name,
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(
                    stat.st_mtime, timezone.utc
                ).isoformat(),
                "rows": info.get("rows"),
                "dataset": path.name.split("_", 1)[0],
            })
    return files


def _safe_export_path(name: str) -> Path | None:
    """只允许 exports 目录内的直接文件名（防路径穿越）。"""
    clean = Path(name).name
    if not clean or clean in (".", "..", "manifest.json"):
        return None
    target = (EXPORTS_DIR / clean).resolve()
    if target.parent != EXPORTS_DIR.resolve() or not target.is_file():
        return None
    return target


@router.get("/api/exports/list")
def api_exports_list():
    return {"files": _list_export_files()}


@router.get("/api/exports/download/{name}")
def api_exports_download(name: str):
    target = _safe_export_path(name)
    if target is None:
        return Response(status_code=404)
    media = "text/csv" if target.suffix == ".csv" else "application/octet-stream"
    return FileResponse(target, media_type=media, filename=target.name)


@router.delete("/api/exports/{name}")
def api_exports_delete(name: str):
    target = _safe_export_path(name)
    if target is None:
        return Response(status_code=404)
    try:
        target.unlink()
    except OSError:
        return Response(status_code=500)
    return {"deleted": name}


@router.post("/api/exports/run")
def api_exports_run(datasets: str = ""):
    """异步触发导出：子进程执行 CLI（进程隔离，避开与采集的库锁竞争）。"""
    if _EXPORT_RUNNING.is_set():
        # 裸元组 (dict, 409) 会被 FastAPI 编成 200 + JSON 数组，客户端无法判冲突
        return JSONResponse({"error": "export already running"}, status_code=409)
    import subprocess
    task_id = f"export-{int(_time.time())}"
    groups = [g for g in datasets.split(",") if g.strip()]
    command = [_EXPORT_PYTHON, "-m", "csgo_monitor", "export"]
    if groups:
        command += ["--datasets", ",".join(groups)]
    _EXPORT_TASKS[task_id] = {
        "status": "running", "command": command,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    _EXPORT_RUNNING.set()

    def _run() -> None:
        try:
            proc = subprocess.run(
                command, cwd=str(PROJECT), capture_output=True, timeout=3600,
            )
            _EXPORT_TASKS[task_id].update({
                "status": "ok" if proc.returncode == 0 else "failed",
                "exit_code": proc.returncode,
                "stderr_tail": proc.stderr.decode("utf-8", "replace")[-500:],
                "finished_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as exc:  # noqa: BLE001
            _EXPORT_TASKS[task_id].update({
                "status": "failed", "error": f"{type(exc).__name__}: {exc}",
                "finished_at": datetime.now(timezone.utc).isoformat(),
            })
        finally:
            _EXPORT_RUNNING.clear()

    _threading.Thread(target=_run, daemon=True).start()
    return {"task_id": task_id}


@router.get("/api/exports/tasks/{task_id}")
def api_exports_task(task_id: str):
    task = _EXPORT_TASKS.get(task_id)
    if task is None:
        return Response(status_code=404)
    return task
