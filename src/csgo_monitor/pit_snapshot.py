# -*- coding: utf-8 -*-
"""目录 point-in-time 快照（最终架构 §5.7：为未来回测防幸存者偏差）。

回测宇宙必须用"当时的目录"而非"现在的目录"——绝版/退市品若被
事后剔除，收益系统性虚高（知识库7 §2.5）。每日把全量目录落一份
压缩 JSONL，保留 item_id↔market_hash_name↔source 映射。

运行：python -m csgo_monitor pit-snapshot（supervisor 日频调度）
"""
from __future__ import annotations

import gzip
import json
import logging
from datetime import datetime, timezone
from typing import Any

from .storage import Storage

log = logging.getLogger(__name__)


def run_pit_snapshot(storage: Storage) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    day = now.strftime("%Y-%m-%d")
    out_dir = storage.raw_dir.parent / "backups" / "pit"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"catalog-{day}.jsonl.gz"

    # 每日顺手的 WAL 维护：PASSIVE checkpoint（不阻塞读者，能推进多少
    # 推多少——长读连接会饿死 checkpoint，大型 WAL 巡检发现后的日检）
    wal_note = ""
    try:
        with storage.connect() as conn:
            r = conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            wal_note = f"ckpt busy={r[0]} log={r[1]} done={r[2]}"
    except Exception as exc:
        wal_note = f"ckpt failed: {type(exc).__name__}"

    n = 0
    if not out_path.exists():
        with storage.read_connect() as conn:
            cursor = conn.execute(
                """SELECT source, item_id, market_hash_name, updated_at
                FROM items
                WHERE market_hash_name IS NOT NULL AND market_hash_name != ''"""
            )
            with gzip.open(out_path, "wt", encoding="utf-8") as fh:
                for row in cursor:
                    fh.write(json.dumps({
                        "source": row[0], "item_id": row[1],
                        "market_hash_name": row[2], "updated_at": row[3],
                        "snapshot_at": now.isoformat(),
                    }, ensure_ascii=False) + "\n")
                    n += 1
        log.info("pit snapshot: %s items -> %s", n, out_path.name)
    else:
        # 当日已存在则跳过（幂等）
        with gzip.open(out_path, "rt", encoding="utf-8") as fh:
            n = sum(1 for _ in fh)
    return {
        "status": "ok",
        "error_class": "",
        "metrics": {"items": n, "day": day},
        "details": {"path": str(out_path), "wal": wal_note},
    }
