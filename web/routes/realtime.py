# -*- coding: utf-8 -*-
"""实时数据层 API：SSE 推流 + 快照/增量 REST（纯搬运自 web/app.py）.

注意：`db`/`DB`/`CONTROL_DB` 从 web.app 引入——测试通过 patch web.app.DB
切换测试库，连接工厂必须继续读取 web.app 的模块全局。
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import time as _time

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse

from web.app import DB, CONTROL_DB, db
from web.cache import cached
from web.realtime import realtime_events, realtime_snapshot

router = APIRouter()


def _open_realtime_db():
    """为推流线程打开独立的只读连接（含 control 附加库）。"""
    conn = sqlite3.connect(
        DB.resolve().as_uri() + "?mode=ro", uri=True, timeout=15,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    control = CONTROL_DB.resolve()
    if control != DB.resolve() and control.exists():
        conn.execute(
            "ATTACH DATABASE ? AS control",
            (control.as_uri() + "?mode=ro",),
        )
    return conn


@router.get("/api/realtime/latest")
def api_realtime_latest():
    def _read():
        with db() as conn:
            return realtime_snapshot(conn)

    return cached("realtime_snapshot", 2, _read)


@router.get("/api/realtime/events")
def api_realtime_events(since: str = Query(default="")):
    with db() as conn:
        return realtime_events(conn, since)


@router.get("/api/realtime/stream")
async def api_realtime_stream(request: Request):
    """SSE 推流：snapshot(初始) -> delta(新数据) / status(运行状态) / ping。"""
    loop = asyncio.get_running_loop()

    def _snapshot_sync():
        conn = _open_realtime_db()
        try:
            return realtime_snapshot(conn)
        finally:
            conn.close()

    def _events_sync(cursor: str):
        conn = _open_realtime_db()
        try:
            return realtime_events(conn, cursor)
        finally:
            conn.close()

    async def generate():
        snapshot = await loop.run_in_executor(None, _snapshot_sync)
        cursor = str(snapshot.get("data_cursor") or "")
        yield (
            "event: snapshot\ndata: "
            + json.dumps(snapshot, ensure_ascii=False, default=str)
            + "\n\n"
        )
        last_status = 0.0
        last_ping = _time.monotonic()
        while True:
            if await request.is_disconnected():
                break
            await asyncio.sleep(2.0)
            try:
                delta = await loop.run_in_executor(None, _events_sync, cursor)
            except sqlite3.Error:
                delta = {"cursor": cursor, "events": []}
            if delta.get("events"):
                cursor = str(delta.get("cursor") or cursor)
                yield (
                    "event: delta\ndata: "
                    + json.dumps(delta, ensure_ascii=False, default=str)
                    + "\n\n"
                )
            now = _time.monotonic()
            if now - last_status >= 10:
                last_status = now
                try:
                    status = await loop.run_in_executor(None, _snapshot_sync)
                except sqlite3.Error:
                    continue
                yield (
                    "event: status\ndata: "
                    + json.dumps(
                        {
                            "ws_collector": status.get("ws_collector"),
                            "supervisor": status.get("supervisor"),
                            "pauses": status.get("pauses"),
                            "server_time": status.get("server_time"),
                            "fresh_seconds": status.get("fresh_seconds"),
                        },
                        ensure_ascii=False, default=str,
                    )
                    + "\n\n"
                )
            if now - last_ping >= 15:
                last_ping = now
                yield "event: ping\ndata: {}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
