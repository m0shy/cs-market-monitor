from __future__ import annotations

import gzip
import hashlib
import json
import logging
import math
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping
from urllib.parse import urlsplit

from .errors import classify_error, failure_status as classify_failure_status
from .errors import next_retry_at as calculate_next_retry_at


log = logging.getLogger(__name__)


def _with_sqlite_retries(
    operation: Callable[[], Any],
    *,
    attempts: int = 12,
) -> Any:
    """Retry short SQLite writes when another worker briefly holds the WAL."""
    for attempt in range(max(1, attempts)):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise
            if attempt + 1 >= max(1, attempts):
                raise
            time.sleep(min(5.0, 0.08 * (2 ** attempt)))
    raise RuntimeError("SQLite operation did not return")


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS captures (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  method TEXT NOT NULL,
  url TEXT NOT NULL,
  status INTEGER,
  captured_at TEXT NOT NULL,
  request_json TEXT,
  content_type TEXT,
  sha256 TEXT,
  raw_path TEXT,
  error TEXT
);
CREATE INDEX IF NOT EXISTS idx_captures_source_url ON captures(source, url);
DROP INDEX IF EXISTS idx_captures_dedupe;
CREATE INDEX IF NOT EXISTS idx_captures_fingerprint ON captures(source, method, url, sha256);
CREATE TABLE IF NOT EXISTS items (
  source TEXT NOT NULL,
  item_id TEXT NOT NULL,
  name TEXT,
  market_hash_name TEXT,
  raw_json TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(source, item_id)
);
CREATE INDEX IF NOT EXISTS idx_items_source_market_name
  ON items(source, market_hash_name);
CREATE TABLE IF NOT EXISTS series (
  source TEXT NOT NULL,
  dataset TEXT NOT NULL,
  item_id TEXT NOT NULL DEFAULT '',
  metric TEXT NOT NULL,
  platform TEXT NOT NULL DEFAULT '',
  ts TEXT NOT NULL,
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  volume REAL,
  turnover REAL,
  extra_json TEXT,
  PRIMARY KEY(source, dataset, item_id, metric, platform, ts)
);
CREATE TABLE IF NOT EXISTS feature_store (
  item_id TEXT NOT NULL,
  freq TEXT NOT NULL,
  name TEXT NOT NULL,
  ts TEXT NOT NULL,
  value REAL,
  version INTEGER NOT NULL DEFAULT 1,
  computed_at TEXT NOT NULL,
  PRIMARY KEY(item_id, freq, name, ts, version)
);
CREATE INDEX IF NOT EXISTS idx_feature_store_lookup
  ON feature_store(freq, name, ts);
CREATE INDEX IF NOT EXISTS idx_feature_store_item
  ON feature_store(item_id, freq, ts);
CREATE INDEX IF NOT EXISTS idx_series_lookup ON series(source, item_id, metric, ts);
CREATE INDEX IF NOT EXISTS idx_series_dataset_item
  ON series(dataset, item_id, metric, platform, ts);
CREATE INDEX IF NOT EXISTS idx_series_dataset_ts
  ON series(dataset, ts);
CREATE INDEX IF NOT EXISTS idx_series_snap_latest
  ON series(dataset, source, metric, platform, ts);
CREATE TABLE IF NOT EXISTS indicators (
  source TEXT NOT NULL,
  item_id TEXT NOT NULL,
  metric TEXT NOT NULL,
  platform TEXT NOT NULL DEFAULT '',
  ts TEXT NOT NULL,
  sma7 REAL, sma30 REAL, ema12 REAL, ema26 REAL,
  macd REAL, macd_signal REAL, rsi14 REAL,
  bollinger_mid REAL, bollinger_upper REAL, bollinger_lower REAL,
  return_1d REAL, volatility30 REAL,
  PRIMARY KEY(source, item_id, metric, platform, ts)
);
CREATE TABLE IF NOT EXISTS coverage (
  source TEXT NOT NULL,
  dataset TEXT NOT NULL,
  status TEXT NOT NULL,
  record_count INTEGER NOT NULL DEFAULT 0,
  earliest TEXT,
  latest TEXT,
  checked_at TEXT NOT NULL,
  note TEXT,
  PRIMARY KEY(source, dataset)
);
CREATE INDEX IF NOT EXISTS idx_coverage_source_status
  ON coverage(source, status, checked_at);
CREATE TABLE IF NOT EXISTS coverage_snapshots (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  dataset TEXT NOT NULL,
  status TEXT NOT NULL,
  record_count INTEGER NOT NULL DEFAULT 0,
  earliest TEXT,
  latest TEXT,
  checked_at TEXT NOT NULL,
  note TEXT,
  run_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_coverage_snapshots_lookup
  ON coverage_snapshots(source, dataset, checked_at);
CREATE TABLE IF NOT EXISTS crawl_state (
  job TEXT PRIMARY KEY,
  cursor TEXT,
  updated_at TEXT NOT NULL,
  detail_json TEXT
);
CREATE TABLE IF NOT EXISTS catalog_items (
  source TEXT NOT NULL,
  item_id TEXT NOT NULL,
  position INTEGER,
  discovered_at TEXT NOT NULL,
  PRIMARY KEY(source, item_id)
);
CREATE INDEX IF NOT EXISTS idx_catalog_items_source_position ON catalog_items(source, position);
CREATE TABLE IF NOT EXISTS catalog_memberships (
  source TEXT NOT NULL,
  universe TEXT NOT NULL,
  item_id TEXT NOT NULL,
  position INTEGER,
  discovered_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  PRIMARY KEY(source, universe, item_id)
);
CREATE INDEX IF NOT EXISTS idx_catalog_memberships_universe_position
  ON catalog_memberships(source, universe, position, item_id);
CREATE TABLE IF NOT EXISTS crawl_failures (
  job TEXT NOT NULL,
  item_id TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  error_class TEXT NOT NULL DEFAULT 'unknown',
  next_retry_at TEXT,
  failure_status TEXT NOT NULL DEFAULT 'retryable',
  first_seen TEXT,
  updated_at TEXT NOT NULL,
  resolved INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(job, item_id)
);
CREATE INDEX IF NOT EXISTS idx_crawl_failures_retry
  ON crawl_failures(job, resolved, attempts, updated_at);
CREATE TABLE IF NOT EXISTS site_metadata (
  source TEXT NOT NULL,
  key TEXT NOT NULL,
  payload TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(source, key)
);
CREATE TABLE IF NOT EXISTS endpoint_inventory (
  source TEXT NOT NULL,
  method TEXT NOT NULL,
  path TEXT NOT NULL,
  first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  captures INTEGER NOT NULL DEFAULT 0,
  successful INTEGER NOT NULL DEFAULT 0,
  bytes INTEGER NOT NULL DEFAULT 0,
  request_keys_json TEXT,
  response_schema_json TEXT,
  last_raw_path TEXT,
  last_error TEXT,
  PRIMARY KEY(source, method, path)
);
CREATE INDEX IF NOT EXISTS idx_endpoint_inventory_source_path
  ON endpoint_inventory(source, path);
CREATE TABLE IF NOT EXISTS route_health_state (
  route_class TEXT NOT NULL,
  route TEXT NOT NULL,
  status TEXT NOT NULL,
  latency_ms REAL,
  error_class TEXT,
  note TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  checked_at TEXT NOT NULL,
  cooldown_until TEXT,
  PRIMARY KEY(route_class, route)
);
CREATE INDEX IF NOT EXISTS idx_route_health_status
  ON route_health_state(route_class, status, checked_at);
CREATE TABLE IF NOT EXISTS task_runs (
  run_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  route TEXT,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  exit_code INTEGER,
  elapsed_seconds REAL,
  error_class TEXT,
  result_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_task_runs_task_started
  ON task_runs(task_id, started_at);
CREATE TABLE IF NOT EXISTS worker_heartbeats (
  worker_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  pid INTEGER,
  status TEXT NOT NULL,
  route TEXT,
  started_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  detail_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_task
  ON worker_heartbeats(task_id, heartbeat_at);
"""


CONTROL_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS control_meta (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS coverage (
  source TEXT NOT NULL,
  dataset TEXT NOT NULL,
  status TEXT NOT NULL,
  record_count INTEGER NOT NULL DEFAULT 0,
  earliest TEXT,
  latest TEXT,
  checked_at TEXT NOT NULL,
  note TEXT,
  PRIMARY KEY(source, dataset)
);
CREATE INDEX IF NOT EXISTS idx_coverage_source_status
  ON coverage(source, status, checked_at);
CREATE TABLE IF NOT EXISTS coverage_snapshots (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  dataset TEXT NOT NULL,
  status TEXT NOT NULL,
  record_count INTEGER NOT NULL DEFAULT 0,
  earliest TEXT,
  latest TEXT,
  checked_at TEXT NOT NULL,
  note TEXT,
  run_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_coverage_snapshots_lookup
  ON coverage_snapshots(source, dataset, checked_at);
CREATE TABLE IF NOT EXISTS crawl_state (
  job TEXT PRIMARY KEY,
  cursor TEXT,
  updated_at TEXT NOT NULL,
  detail_json TEXT
);
CREATE TABLE IF NOT EXISTS crawl_failures (
  job TEXT NOT NULL,
  item_id TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  error_class TEXT NOT NULL DEFAULT 'unknown',
  error_code TEXT,
  endpoint TEXT,
  sample_capture TEXT,
  next_retry_at TEXT,
  failure_status TEXT NOT NULL DEFAULT 'retryable',
  first_seen TEXT,
  updated_at TEXT NOT NULL,
  resolved INTEGER NOT NULL DEFAULT 0,
  resolution_note TEXT,
  PRIMARY KEY(job, item_id)
);
CREATE INDEX IF NOT EXISTS idx_crawl_failures_retry
  ON crawl_failures(job, resolved, attempts, updated_at);
CREATE INDEX IF NOT EXISTS idx_crawl_failures_due
  ON crawl_failures(
    job,resolved,failure_status,next_retry_at,attempts
  );
CREATE TABLE IF NOT EXISTS route_health_state (
  route_class TEXT NOT NULL,
  route TEXT NOT NULL,
  status TEXT NOT NULL,
  latency_ms REAL,
  error_class TEXT,
  note TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  checked_at TEXT NOT NULL,
  cooldown_until TEXT,
  PRIMARY KEY(route_class, route)
);
CREATE INDEX IF NOT EXISTS idx_route_health_status
  ON route_health_state(route_class, status, checked_at);
CREATE TABLE IF NOT EXISTS task_definitions (
  task_id TEXT PRIMARY KEY,
  route_class TEXT NOT NULL,
  lane TEXT NOT NULL,
  mode TEXT NOT NULL,
  interval_seconds INTEGER NOT NULL,
  definition_json TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS task_runs (
  run_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  route TEXT,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  exit_code INTEGER,
  elapsed_seconds REAL,
  error_class TEXT,
  result_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_task_runs_task_started
  ON task_runs(task_id, started_at);
CREATE TABLE IF NOT EXISTS task_leases (
  task_id TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  run_id TEXT,
  pid INTEGER,
  status TEXT NOT NULL,
  acquired_at TEXT NOT NULL,
  lease_until TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  detail_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_task_leases_until
  ON task_leases(lease_until, status);
CREATE TABLE IF NOT EXISTS task_slices (
  task_id TEXT NOT NULL,
  slice_id TEXT NOT NULL,
  status TEXT NOT NULL,
  outcome TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT,
  lease_until TEXT,
  first_seen TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  result_json TEXT,
  PRIMARY KEY(task_id, slice_id)
);
CREATE INDEX IF NOT EXISTS idx_task_slices_status
  ON task_slices(task_id, status, updated_at);
CREATE TABLE IF NOT EXISTS worker_heartbeats (
  worker_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  pid INTEGER,
  status TEXT NOT NULL,
  route TEXT,
  owner_id TEXT,
  lease_until TEXT,
  started_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL,
  detail_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_task
  ON worker_heartbeats(task_id, heartbeat_at);
CREATE TABLE IF NOT EXISTS endpoint_contracts (
  source TEXT NOT NULL,
  method TEXT NOT NULL,
  path TEXT NOT NULL,
  schema_hash TEXT NOT NULL,
  schema_json TEXT NOT NULL,
  status TEXT NOT NULL,
  first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL,
  changes INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(source, method, path)
);
CREATE TABLE IF NOT EXISTS data_limits (
  source TEXT NOT NULL,
  task_id TEXT NOT NULL,
  item_id TEXT NOT NULL,
  status TEXT NOT NULL,
  reason TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(source, task_id, item_id)
);
CREATE TABLE IF NOT EXISTS public_pages (
  source TEXT NOT NULL,
  url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ready',
  priority INTEGER NOT NULL DEFAULT 10,
  discovered_from TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_checked TEXT,
  next_due TEXT,
  last_error TEXT,
  PRIMARY KEY(source, url)
);
CREATE INDEX IF NOT EXISTS idx_public_pages_due
  ON public_pages(source,status,next_due,priority);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_shape(value: Any, depth: int = 0) -> Any:
    """Return a small structural description without retaining response data."""
    if depth >= 2:
        return type(value).__name__
    if isinstance(value, dict):
        return {
            str(key): _json_shape(child, depth + 1)
            for key, child in list(value.items())[:80]
        }
    if isinstance(value, list):
        return {
            "type": "list",
            "len": len(value),
            "sample": _json_shape(value[0], depth + 1) if value else None,
        }
    return type(value).__name__


def _response_schema(body: bytes | None, content_type: str) -> str | None:
    if not body or ("json" not in content_type.lower()
                    and body.lstrip()[:1] not in (b"{", b"[")):
        return None
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return json.dumps(_json_shape(payload), ensure_ascii=False, separators=(",", ":"))


def _merge_schema_shape(previous: Any, current: Any) -> Any:
    if isinstance(previous, dict) and isinstance(current, dict):
        merged = dict(previous)
        for key, value in current.items():
            merged[key] = (
                _merge_schema_shape(merged[key], value)
                if key in merged else value
            )
        return merged
    return current


def _merge_response_schemas(
    previous_schema: str | None, current_schema: str | None
) -> str | None:
    if not previous_schema:
        return current_schema
    if not current_schema:
        return previous_schema
    try:
        previous = json.loads(previous_schema)
        current = json.loads(current_schema)
    except (TypeError, json.JSONDecodeError):
        return current_schema
    return json.dumps(
        _merge_schema_shape(previous, current),
        ensure_ascii=False,
        separators=(",", ":"),
    )


class Storage:
    CONTROL_TABLES = (
        "coverage",
        "coverage_snapshots",
        "crawl_state",
        "crawl_failures",
        "route_health_state",
        "task_definitions",
        "task_runs",
        "task_leases",
        "task_slices",
        "worker_heartbeats",
        "endpoint_contracts",
        "data_limits",
        "public_pages",
    )

    def __init__(
        self,
        database: Path,
        raw_dir: Path,
        control_database: Path | None = None,
    ):
        self.database = database
        self.control_database = control_database or database
        self.raw_dir = raw_dir
        self.raw_compresslevel = max(
            0, min(9, int(os.environ.get("CSGO_MONITOR_GZIP_LEVEL", "1")))
        )
        self._coverage_written: dict[tuple[str, str], float] = {}
        self._coverage_snapshot_written: dict[tuple[str, str], float] = {}
        self._coverage_lock = threading.Lock()
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.control_database.parent.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        def _init_main() -> None:
            with self.connect() as conn:
                conn.executescript(SCHEMA)
                if self.control_database == self.database:
                    conn.executescript(CONTROL_SCHEMA)
                self._migrate_control_schema(conn)
                self._bootstrap_catalog_memberships(conn)

        def _init_control() -> None:
            with self.control_connect() as conn:
                conn.executescript(CONTROL_SCHEMA)
                self._migrate_control_schema(conn)
            self._bootstrap_control_database()

        # 多个采集进程并发启动时，schema 初始化可能与其它写者撞锁；
        # 按短退避重试而不是让进程直接崩溃（监督器会按 local 类快速重排，
        # 但能自愈更省一轮调度）。
        _with_sqlite_retries(_init_main, attempts=10)
        if self.control_database != self.database:
            _with_sqlite_retries(_init_control, attempts=10)

    CONTROL_SCHEMA_VERSION = "2026-10-10.1"

    @staticmethod
    def _migrate_control_schema(conn: sqlite3.Connection) -> None:
        # 快速通道（纯读）：schema 已是最新版本时零写事务直接返回。
        # 此前每个采集进程启动都要执行下面的 ALTER/UPDATE 迁移写事务，
        # 多写者并发时新进程会在控制库写锁上饿死（"database is locked"
        # 直接把进程打死在出生时）。
        try:
            current = conn.execute(
                "SELECT value FROM control_meta WHERE key='control_schema_version'"
            ).fetchone()
        except sqlite3.Error:
            current = None
        if current and str(current[0]) == Storage.CONTROL_SCHEMA_VERSION:
            return

        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(crawl_failures)")
        }
        additions = {
            "error_class": "TEXT NOT NULL DEFAULT 'unknown'",
            "error_code": "TEXT",
            "endpoint": "TEXT",
            "sample_capture": "TEXT",
            "next_retry_at": "TEXT",
            "failure_status": "TEXT NOT NULL DEFAULT 'retryable'",
            "first_seen": "TEXT",
            "resolution_note": "TEXT",
        }
        for name, definition in additions.items():
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE crawl_failures ADD COLUMN {name} {definition}"
                )
        conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_crawl_failures_due
            ON crawl_failures(
                job,resolved,failure_status,next_retry_at,attempts
            )"""
        )
        heartbeat_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(worker_heartbeats)")
        }
        for name, definition in {
            "owner_id": "TEXT",
            "lease_until": "TEXT",
        }.items():
            if name not in heartbeat_columns:
                conn.execute(
                    f"ALTER TABLE worker_heartbeats ADD COLUMN {name} {definition}"
                )
        conn.execute(
            """UPDATE crawl_failures
            SET failure_status='deferred',
                next_retry_at=COALESCE(next_retry_at, updated_at),
                resolution_note='restored deferred retry state'
            WHERE resolved=0
              AND error_class IN ('transport','rate_limit','quota','audit')
              AND failure_status IN ('exhausted','protocol_ticket')"""
        )
        limits_exists = conn.execute(
            """SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='data_limits'"""
        ).fetchone()
        if limits_exists:
            conn.execute(
                """DELETE FROM data_limits
                WHERE status='exhausted'
                  AND EXISTS(
                    SELECT 1 FROM crawl_failures
                    WHERE crawl_failures.job=data_limits.task_id
                      AND crawl_failures.item_id=data_limits.item_id
                      AND crawl_failures.resolved=0
                      AND crawl_failures.failure_status='deferred'
                  )"""
            )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS control_meta(
            key TEXT PRIMARY KEY, value TEXT, updated_at TEXT NOT NULL)"""
        )
        conn.execute(
            "INSERT OR REPLACE INTO control_meta(key, value, updated_at) "
            "VALUES('control_schema_version', ?, ?)",
            (Storage.CONTROL_SCHEMA_VERSION, utc_now()),
        )

    def _bootstrap_control_database(self) -> None:
        legacy = sqlite3.connect(self.database, timeout=30)
        legacy.row_factory = sqlite3.Row
        try:
            with self.control_connect() as control:
                marker = control.execute(
                    "SELECT value FROM control_meta WHERE key='legacy_bootstrap_v2'"
                ).fetchone()
                if marker:
                    return
                control.execute("BEGIN IMMEDIATE")
                for table in self.CONTROL_TABLES:
                    exists = legacy.execute(
                        """SELECT 1 FROM sqlite_master
                        WHERE type='table' AND name=?""",
                        (table,),
                    ).fetchone()
                    if not exists:
                        continue
                    if control.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0]:
                        continue
                    source_columns = [
                        str(row["name"])
                        for row in legacy.execute(
                            f"PRAGMA table_info({table})"
                        )
                    ]
                    target_columns = {
                        str(row["name"])
                        for row in control.execute(
                            f"PRAGMA table_info({table})"
                        )
                    }
                    columns = [
                        name for name in source_columns
                        if name in target_columns
                    ]
                    if not columns:
                        continue
                    names = ",".join(columns)
                    marks = ",".join("?" for _ in columns)
                    rows = legacy.execute(
                        f"SELECT {names} FROM {table}"
                    ).fetchall()
                    if rows:
                        control.executemany(
                            f"INSERT OR IGNORE INTO {table}({names}) "
                            f"VALUES({marks})",
                            (tuple(row[name] for name in columns) for row in rows),
                        )
                control.execute(
                    """INSERT INTO control_meta(key,value,updated_at)
                    VALUES('legacy_bootstrap_v2',?,?)
                    ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,updated_at=excluded.updated_at""",
                    (str(self.database), utc_now()),
                )
        finally:
            legacy.close()

    @staticmethod
    def _bootstrap_catalog_memberships(conn: sqlite3.Connection) -> None:
        if conn.execute(
            "SELECT COUNT(*) FROM catalog_memberships"
        ).fetchone()[0]:
            return
        now = utc_now()
        conn.execute(
            """INSERT OR IGNORE INTO catalog_memberships(
                source,universe,item_id,position,discovered_at,
                last_seen_at,status
            )
            SELECT source,
              CASE
                WHEN source='siteb' AND item_id LIKE 'open:%'
                  THEN 'siteb_open_api'
                WHEN source='siteb' AND item_id LIKE 'sitemap:%'
                  THEN 'siteb_sitemap'
                WHEN source='siteb' THEN 'siteb_site_market'
                WHEN source='sitea' THEN 'sitea_public'
                ELSE 'legacy'
              END,
              item_id,position,discovered_at,?,'active'
            FROM catalog_items""",
            (now,),
        )
        open_count = conn.execute(
            """SELECT COUNT(*) FROM catalog_memberships
            WHERE source='siteb' AND universe='siteb_open_api'"""
        ).fetchone()[0]
        if not open_count:
            conn.execute(
                """INSERT OR IGNORE INTO catalog_memberships(
                    source,universe,item_id,position,discovered_at,
                    last_seen_at,status
                )
                SELECT 'siteb','siteb_open_api',item_id,
                  ROW_NUMBER() OVER (ORDER BY item_id)-1,
                  updated_at,?,'active'
                FROM items
                WHERE source='siteb' AND item_id LIKE 'open:%'""",
                (now,),
            )
            conn.execute(
                """INSERT OR IGNORE INTO catalog_items(
                    source,item_id,position,discovered_at
                )
                SELECT source,item_id,position,discovered_at
                FROM catalog_memberships
                WHERE source='siteb'
                  AND universe='siteb_open_api'"""
            )

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.database, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA busy_timeout=60000")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA cache_size=-32768")
            conn.execute("PRAGMA mmap_size=268435456")
            conn.execute("PRAGMA wal_autocheckpoint=4000")
            conn.execute("PRAGMA journal_size_limit=67108864")
            yield conn
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def control_connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.control_database, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA busy_timeout=60000")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA cache_size=-16384")
            conn.execute("PRAGMA wal_autocheckpoint=2000")
            conn.execute("PRAGMA journal_size_limit=67108864")
            yield conn
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def read_connect(self) -> Iterator[sqlite3.Connection]:
        with self.connect() as conn:
            if self.control_database != self.database:
                conn.execute(
                    "ATTACH DATABASE ? AS control",
                    (str(self.control_database),),
                )
            yield conn

    def control_table(self, name: str) -> str:
        if not name or not name.replace("_", "").isalnum():
            raise ValueError(f"invalid control table name: {name}")
        return (
            f"control.{name}"
            if self.control_database != self.database
            else name
        )

    def save_capture(
        self,
        source: str,
        method: str,
        url: str,
        status: int | None,
        body: bytes | None,
        content_type: str = "",
        request_data: Any = None,
        error: str | None = None,
    ) -> int | None:
        digest = hashlib.sha256(body).hexdigest() if body else None
        raw_path = None
        if body and digest:
            day = datetime.now().strftime("%Y-%m-%d")
            target = self.raw_dir / source / day / f"{digest}.json.gz"
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                with gzip.open(
                    target, "wb", compresslevel=self.raw_compresslevel
                ) as handle:
                    handle.write(body)
            raw_path = str(target.relative_to(self.database.parent))
        request_json = json.dumps(request_data, ensure_ascii=False, default=str) if request_data is not None else None
        def write_capture() -> tuple[int, str | None]:
            with self.connect() as conn:
                cur = conn.execute(
                    """INSERT INTO captures(source,method,url,status,captured_at,request_json,content_type,sha256,raw_path,error)
                    VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    (source, method, url, status, utc_now(), request_json, content_type, digest, raw_path, error),
                )
                path = urlsplit(url).path or "/"
                request_keys = set(request_data) if isinstance(request_data, dict) else set()
                previous = conn.execute(
                    """SELECT request_keys_json,response_schema_json,captures
                    FROM endpoint_inventory
                    WHERE source=? AND method=? AND path=?""",
                    (source, method, path),
                ).fetchone()
                if previous and previous[0]:
                    try:
                        request_keys.update(json.loads(previous[0]))
                    except (TypeError, json.JSONDecodeError):
                        pass
                previous_schema = str(previous[1]) if previous and previous[1] else None
                previous_captures = int(previous[2]) if previous else 0
                refresh_schema = (
                    not previous_schema
                    or previous_captures % 100 == 0
                    or status is None
                    or not 200 <= status < 300
                )
                current_schema = (
                    _response_schema(body, content_type)
                    if refresh_schema
                    else None
                )
                schema = _merge_response_schemas(
                    previous_schema,
                    current_schema,
                )
                conn.execute(
                    """INSERT INTO endpoint_inventory(
                        source,method,path,first_seen,last_seen,captures,successful,bytes,
                        request_keys_json,response_schema_json,last_raw_path,last_error
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(source,method,path) DO UPDATE SET
                        last_seen=excluded.last_seen,
                        captures=endpoint_inventory.captures+1,
                        successful=endpoint_inventory.successful+excluded.successful,
                        bytes=endpoint_inventory.bytes+excluded.bytes,
                        request_keys_json=excluded.request_keys_json,
                        response_schema_json=COALESCE(excluded.response_schema_json,
                            endpoint_inventory.response_schema_json),
                        last_raw_path=COALESCE(excluded.last_raw_path, endpoint_inventory.last_raw_path),
                        last_error=excluded.last_error""",
                    (
                        source, method, path, utc_now(), utc_now(), 1,
                        int(status is not None and 200 <= status < 300),
                        len(body or b""), json.dumps(sorted(request_keys), ensure_ascii=False),
                        schema, raw_path, error,
                    ),
                )
                return int(cur.lastrowid), current_schema

        capture_id, current_schema = _with_sqlite_retries(write_capture)
        if current_schema:
            try:
                _with_sqlite_retries(
                    lambda: self.record_endpoint_contract(
                        source,
                        method,
                        urlsplit(url).path or "/",
                        current_schema,
                    )
                )
            except sqlite3.OperationalError:
                log.warning(
                    "Unable to update endpoint contract after capture: %s %s",
                    method,
                    url,
                    exc_info=True,
                )
        return capture_id

    def upsert_items(
        self, source: str, items: Iterable[dict[str, Any]],
        *, chunk_size: int = 2000,
    ) -> int:
        """分块 upsert：数万行单事务会在写者密集时长时间持有主库写锁
        （其他采集进程因此锁超时），按块提交并在块间让出锁。"""
        rows = self._item_values(source, items)
        if not rows:
            return 0
        written = 0
        for start in range(0, len(rows), max(1, chunk_size)):
            chunk = rows[start:start + max(1, chunk_size)]
            def _write(chunk=chunk):
                with self.connect() as conn:
                    self._upsert_item_values(conn, chunk)
                return len(chunk)
            _with_sqlite_retries(_write)
            written += len(chunk)
        return written

    @staticmethod
    def _item_values(source: str, items: Iterable[dict[str, Any]]) -> list[tuple[Any, ...]]:
        rows = []
        now = utc_now()
        for item in items:
            item_id = item.get("good_id") or item.get("itemId") or item.get("item_id") or item.get("id")
            if item_id is None:
                continue
            name = item.get("name") or item.get("good_name") or item.get("shortName")
            market_name = item.get("marketHashName") or item.get("market_hash_name")
            rows.append((
                source, str(item_id), name, market_name,
                json.dumps(item, ensure_ascii=False, default=str), now,
            ))
        return rows

    @staticmethod
    def _upsert_item_values(conn: sqlite3.Connection, rows: list[tuple[Any, ...]]) -> None:
        conn.executemany(
            """INSERT INTO items(source,item_id,name,market_hash_name,raw_json,updated_at)
            VALUES(?,?,?,?,?,?) ON CONFLICT(source,item_id) DO UPDATE SET
            name=COALESCE(excluded.name, items.name),
            market_hash_name=COALESCE(excluded.market_hash_name, items.market_hash_name),
            raw_json=CASE WHEN excluded.raw_json IS NULL OR excluded.raw_json='{}'
                          THEN items.raw_json ELSE excluded.raw_json END,
            updated_at=excluded.updated_at""",
            rows,
        )

    def upsert_catalog_items(
        self,
        source: str,
        item_ids: Iterable[str],
        *,
        start_position: int = 0,
        discovered_at: str | None = None,
        universe: str = "default",
    ) -> int:
        item_ids = list(item_ids)
        values = self._catalog_values(
            source,
            item_ids,
            start_position=start_position,
            discovered_at=discovered_at,
        )
        if not values:
            return 0
        memberships = self._membership_values(
            source,
            universe,
            item_ids,
            start_position=start_position,
            discovered_at=discovered_at,
        )
        with self.connect() as conn:
            self._upsert_catalog_values(conn, values)
            self._upsert_membership_values(conn, memberships)
        return len(values)

    @staticmethod
    def _catalog_values(
        source: str,
        item_ids: Iterable[str],
        *,
        start_position: int = 0,
        discovered_at: str | None = None,
    ) -> list[tuple[str, str, int, str]]:
        unique = list(dict.fromkeys(str(value) for value in item_ids if value))
        position = max(0, int(start_position))
        seen_at = discovered_at or utc_now()
        return [
            (source, item_id, position + index, seen_at)
            for index, item_id in enumerate(unique)
        ]

    @staticmethod
    def _upsert_catalog_values(
        conn: sqlite3.Connection, values: list[tuple[str, str, int, str]]
    ) -> None:
        conn.executemany(
            """INSERT INTO catalog_items(source,item_id,position,discovered_at) VALUES(?,?,?,?)
            ON CONFLICT(source,item_id) DO UPDATE SET position=excluded.position,
            discovered_at=excluded.discovered_at""",
            values,
        )

    @staticmethod
    def _membership_values(
        source: str,
        universe: str,
        item_ids: Iterable[str],
        *,
        start_position: int = 0,
        discovered_at: str | None = None,
    ) -> list[tuple[str, str, str, int, str, str, str]]:
        unique = list(dict.fromkeys(str(value) for value in item_ids if value))
        position = max(0, int(start_position))
        seen_at = discovered_at or utc_now()
        last_seen = utc_now()
        return [
            (
                source,
                universe,
                item_id,
                position + index,
                seen_at,
                last_seen,
                "active",
            )
            for index, item_id in enumerate(unique)
        ]

    @staticmethod
    def _upsert_membership_values(
        conn: sqlite3.Connection,
        values: list[tuple[str, str, str, int, str, str, str]],
    ) -> None:
        if not values:
            return
        conn.executemany(
            """INSERT INTO catalog_memberships(
                source,universe,item_id,position,discovered_at,
                last_seen_at,status
            ) VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(source,universe,item_id) DO UPDATE SET
                position=excluded.position,
                discovered_at=excluded.discovered_at,
                last_seen_at=excluded.last_seen_at,
                status='active'""",
            values,
        )

    def prune_catalog_items_for_sweep(
        self,
        source: str,
        sweep_id: str,
        *,
        universe: str = "default",
    ) -> int:
        """Remove catalog rows absent from a successfully completed full sweep."""
        if not sweep_id:
            return 0
        with self.connect() as conn:
            cur = conn.execute(
                """DELETE FROM catalog_memberships
                WHERE source=? AND universe=? AND discovered_at<>?""",
                (source, universe, sweep_id),
            )
            conn.execute(
                """DELETE FROM catalog_items
                WHERE source=? AND NOT EXISTS (
                  SELECT 1 FROM catalog_memberships membership
                  WHERE membership.source=catalog_items.source
                    AND membership.item_id=catalog_items.item_id
                )""",
                (source,),
            )
            return int(cur.rowcount)

    def get_catalog_items(
        self,
        source: str,
        universe: str | None = None,
    ) -> list[str]:
        with self.connect() as conn:
            if universe:
                rows = conn.execute(
                    """SELECT item_id FROM catalog_memberships
                    WHERE source=? AND universe=? AND status='active'
                    ORDER BY position,item_id""",
                    (source, universe),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT item_id FROM catalog_items
                    WHERE source=? ORDER BY position,item_id""",
                    (source,),
                ).fetchall()
        return [str(row[0]) for row in rows]

    def catalog_count(
        self,
        source: str,
        universe: str | None = None,
    ) -> int:
        with self.connect() as conn:
            if universe:
                return int(conn.execute(
                    """SELECT COUNT(*) FROM catalog_memberships
                    WHERE source=? AND universe=? AND status='active'""",
                    (source, universe),
                ).fetchone()[0])
            return int(conn.execute(
                "SELECT COUNT(*) FROM catalog_items WHERE source=?",
                (source,),
            ).fetchone()[0])

    def replace_catalog_items(
        self,
        source: str,
        item_ids: Iterable[str],
        *,
        universe: str = "default",
    ) -> int:
        unique = list(dict.fromkeys(str(value) for value in item_ids if value))
        now = utc_now()
        catalog_values = self._catalog_values(
            source,
            unique,
            discovered_at=now,
        )
        memberships = self._membership_values(
            source,
            universe,
            unique,
            discovered_at=now,
        )
        with self.connect() as conn:
            conn.execute(
                """DELETE FROM catalog_memberships
                WHERE source=? AND universe=?""",
                (source, universe),
            )
            self._upsert_catalog_values(conn, catalog_values)
            self._upsert_membership_values(conn, memberships)
            conn.execute(
                """DELETE FROM catalog_items
                WHERE source=? AND NOT EXISTS (
                  SELECT 1 FROM catalog_memberships membership
                  WHERE membership.source=catalog_items.source
                    AND membership.item_id=catalog_items.item_id
                )""",
                (source,),
            )
        return len(unique)

    def upsert_series(
        self, rows: Iterable[dict[str, Any]], *, chunk_size: int = 2000
    ) -> int:
        """分块提交：单条历史响应可达 8760 行（一次 type-trend 全量），
        大事务长期持有主库写锁会让 WS 实时帧、captures 等全部饿死
        （实测 WAL 大幅膨胀、写锁 22s+ 不可得）。按块提交让出锁。"""
        values = self._series_values(rows)
        if not values:
            return 0
        size = max(1, chunk_size)
        written = 0
        for start in range(0, len(values), size):
            chunk = values[start:start + size]
            def _write(chunk=chunk):
                with self.connect() as conn:
                    self._upsert_series_values(conn, chunk)
                return len(chunk)
            _with_sqlite_retries(_write)
            written += len(chunk)
        return written

    @staticmethod
    def _series_values(rows: Iterable[dict[str, Any]]) -> list[tuple[Any, ...]]:
        values = []
        for row in rows:
            values.append((
                row["source"], row["dataset"], str(row.get("item_id", "")), row["metric"],
                str(row.get("platform", "")), str(row["ts"]), row.get("open"), row.get("high"),
                row.get("low"), row.get("close"), row.get("volume"), row.get("turnover"),
                json.dumps(row.get("extra", {}), ensure_ascii=False, default=str),
            ))
        return values

    # ------------------------------------------------------------------
    # feature_store（最终架构 §7：与 series 原始观测分离，保 PIT 可回放）
    # ------------------------------------------------------------------

    def upsert_features(
        self, rows: Iterable[dict[str, Any]], *, chunk_size: int = 2000
    ) -> int:
        """写特征（append-only 版本化：同键冲突时 version+1 追加新行）。

        行字段：item_id / freq('1h'|'1d'|'1w') / name / ts / value。
        分块提交与 upsert_series 同理（写锁友好）。
        """
        now = utc_now()
        prepared: dict[tuple[str, str, str, str], tuple[Any, ...]] = {}
        for row in rows:
            key = (str(row.get("item_id", "")), str(row.get("freq", "1d")),
                   str(row["name"]), str(row["ts"]))
            prepared[key] = (
                key[0], key[1], key[2], key[3], row.get("value"), now,
            )
        if not prepared:
            return 0
        values = list(prepared.values())
        size = max(1, chunk_size)
        written = 0
        for start in range(0, len(values), size):
            chunk = values[start:start + size]

            def _write(chunk=chunk):
                with self.connect() as conn:
                    conn.executemany(
                        """INSERT INTO feature_store(
                            item_id, freq, name, ts, value, version, computed_at)
                        SELECT ?,?,?,?,?, 
                            COALESCE((SELECT MAX(version) FROM feature_store f2
                                WHERE f2.item_id=? AND f2.freq=? AND f2.name=?
                                  AND f2.ts=?), 0) + 1, ?
                        """,
                        [(v[0], v[1], v[2], v[3], v[4],
                          v[0], v[1], v[2], v[3], v[5]) for v in chunk],
                    )
                return len(chunk)

            _with_sqlite_retries(_write)
            written += len(chunk)
        return written

    def read_feature_frame(
        self, names: list[str], *, freq: str = "1d",
        ts_from: str = "", ts_to: str = "",
    ) -> dict[tuple[str, str], dict[str, float | None]]:
        """读取特征矩形：{(item_id, ts): {name: value}}（最新版本）。

        供验证脚手架与相关矩阵工具使用。
        """
        if not names:
            return {}
        placeholders = ",".join("?" * len(names))
        sql_args: list[Any] = [freq, *names, freq]
        where_extra = ""
        if ts_from:
            where_extra += " AND f.ts >= ?"
            sql_args.append(ts_from)
        if ts_to:
            where_extra += " AND f.ts <= ?"
            sql_args.append(ts_to)
        out: dict[tuple[str, str], dict[str, float | None]] = {}
        with self.read_connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""SELECT f.item_id, f.ts, f.name, f.value
                FROM feature_store f
                JOIN (
                    SELECT item_id, name, ts, MAX(version) AS mv
                    FROM feature_store
                    WHERE freq=? AND name IN ({placeholders})
                    GROUP BY item_id, name, ts
                ) latest ON latest.item_id=f.item_id AND latest.name=f.name
                    AND latest.ts=f.ts AND latest.mv=f.version
                WHERE f.freq=?{where_extra}""",
                sql_args,
            ).fetchall()
            for r in rows:
                out.setdefault((str(r["item_id"]), str(r["ts"])), {})[
                    str(r["name"])] = r["value"]
        return out


    @staticmethod
    def _upsert_series_values(conn: sqlite3.Connection, values: list[tuple[Any, ...]]) -> None:
        conn.executemany(
            """INSERT INTO series(
                source,dataset,item_id,metric,platform,ts,open,high,low,close,
                volume,turnover,extra_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO UPDATE SET
            open=excluded.open,high=excluded.high,low=excluded.low,
            close=excluded.close,volume=excluded.volume,
            turnover=excluded.turnover,extra_json=excluded.extra_json""",
            values,
        )

    def upsert_ingest(
        self,
        source: str,
        dataset: str,
        items: Iterable[dict[str, Any]],
        rows: Iterable[dict[str, Any]],
        earliest: str | None = None,
        latest: str | None = None,
        coverage_interval: float = 5.0,
        catalog_item_ids: Iterable[str] | None = None,
        catalog_start_position: int = 0,
        catalog_discovered_at: str | None = None,
        catalog_universe: str = "default",
    ) -> tuple[int, int]:
        """Persist normalized items, series, and coverage in one transaction."""
        item_values = self._item_values(source, items)
        series_values = self._series_values(rows)
        catalog_ids = list(catalog_item_ids or ())
        catalog_values = self._catalog_values(
            source,
            catalog_ids,
            start_position=catalog_start_position,
            discovered_at=catalog_discovered_at,
        )
        membership_values = self._membership_values(
            source,
            catalog_universe,
            catalog_ids,
            start_position=catalog_start_position,
            discovered_at=catalog_discovered_at,
        )
        if not item_values and not series_values and not catalog_values:
            return 0, 0

        coverage_key = (source, dataset)
        now_mono = time.monotonic()
        write_coverage = bool(series_values)
        if write_coverage and coverage_interval > 0:
            with self._coverage_lock:
                previous = self._coverage_written.get(coverage_key, 0.0)
                write_coverage = now_mono - previous >= coverage_interval
                if write_coverage:
                    self._coverage_written[coverage_key] = now_mono

        with self.connect() as conn:
            if item_values:
                self._upsert_item_values(conn, item_values)
            if catalog_values:
                self._upsert_catalog_values(conn, catalog_values)
                self._upsert_membership_values(conn, membership_values)
            if series_values:
                self._upsert_series_values(conn, series_values)
            if write_coverage and self.control_database == self.database:
                conn.execute(
                    """INSERT INTO coverage(
                        source,dataset,status,record_count,earliest,latest,checked_at,note
                    ) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(source,dataset) DO UPDATE SET
                    status=excluded.status,record_count=excluded.record_count,
                    earliest=excluded.earliest,latest=excluded.latest,
                    checked_at=excluded.checked_at,note=excluded.note""",
                    (
                        source, dataset, "ok", len(series_values),
                        earliest, latest, utc_now(), "",
                    ),
                )
        if write_coverage and self.control_database != self.database:
            self.set_coverage(
                source,
                dataset,
                "ok",
                len(series_values),
                earliest,
                latest,
            )
        return len(item_values), len(series_values)

    def set_coverage(self, source: str, dataset: str, status: str, count: int = 0,
                     earliest: str | None = None, latest: str | None = None, note: str = "",
                     min_interval: float = 0.0, run_id: str | None = None) -> None:
        key = (source, dataset)
        now_mono = time.monotonic()
        if min_interval > 0:
            with self._coverage_lock:
                if now_mono - self._coverage_written.get(key, 0.0) < min_interval:
                    return
                self._coverage_written[key] = now_mono
        with self._coverage_lock:
            snapshot_due = (
                now_mono - self._coverage_snapshot_written.get(key, 0.0) >= 60.0
            )
            if snapshot_due:
                self._coverage_snapshot_written[key] = now_mono
        checked_at = utc_now()
        with self.control_connect() as conn:
            conn.execute(
                """INSERT INTO coverage(source,dataset,status,record_count,earliest,latest,checked_at,note)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(source,dataset) DO UPDATE SET status=excluded.status,
                record_count=excluded.record_count,earliest=excluded.earliest,latest=excluded.latest,
                checked_at=excluded.checked_at,note=excluded.note""",
                (source, dataset, status, count, earliest, latest, checked_at, note),
            )
            if snapshot_due:
                conn.execute(
                    """INSERT INTO coverage_snapshots(
                        source,dataset,status,record_count,earliest,latest,
                        checked_at,note,run_id
                    ) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (
                        source,
                        dataset,
                        status,
                        count,
                        earliest,
                        latest,
                        checked_at,
                        note,
                        run_id,
                    ),
                )
        if min_interval <= 0:
            with self._coverage_lock:
                self._coverage_written[key] = now_mono

    def state_suffixes(self, prefixes: Iterable[str]) -> dict[str, set[str]]:
        """Load large families of completion flags with one SQLite query."""
        normalized = tuple(dict.fromkeys(prefix for prefix in prefixes if prefix))
        result = {prefix: set() for prefix in normalized}
        if not normalized:
            return result
        clauses = " OR ".join("job LIKE ?" for _ in normalized)
        params = [prefix + ":%" for prefix in normalized]
        with self.control_connect() as conn:
            rows = conn.execute(
                f"SELECT job FROM crawl_state WHERE {clauses}", params
            ).fetchall()
        for row in rows:
            job = str(row[0])
            for prefix in normalized:
                marker = prefix + ":"
                if job.startswith(marker):
                    result[prefix].add(job[len(marker):])
                    break
        return result

    def get_state(self, job: str) -> str | None:
        with self.control_connect() as conn:
            row = conn.execute("SELECT cursor FROM crawl_state WHERE job=?", (job,)).fetchone()
            return str(row[0]) if row and row[0] is not None else None

    def get_state_detail(self, job: str) -> Any | None:
        with self.control_connect() as conn:
            row = conn.execute(
                "SELECT detail_json FROM crawl_state WHERE job=?", (job,)
            ).fetchone()
        if not row or not row[0]:
            return None
        try:
            return json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            return None

    def set_state(self, job: str, cursor: str, detail: Any = None) -> None:
        with self.control_connect() as conn:
            conn.execute(
                """INSERT INTO crawl_state(job,cursor,updated_at,detail_json) VALUES(?,?,?,?)
                ON CONFLICT(job) DO UPDATE SET cursor=excluded.cursor,updated_at=excluded.updated_at,detail_json=excluded.detail_json""",
                (job, cursor, utc_now(), json.dumps(detail, ensure_ascii=False, default=str)),
            )

    def extend_pause(self, job: str, deadline: float, detail: Any = None) -> float:
        deadline = float(deadline)
        if not math.isfinite(deadline) or deadline < 0:
            raise ValueError("pause deadline must be a finite nonnegative timestamp")
        with self.control_connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT cursor FROM crawl_state WHERE job=?", (job,)
            ).fetchone()
            try:
                previous = float(row["cursor"]) if row else 0.0
            except (TypeError, ValueError):
                previous = 0.0
            if not math.isfinite(previous):
                previous = 0.0
            if deadline > previous:
                conn.execute(
                    """INSERT INTO crawl_state(job,cursor,updated_at,detail_json)
                    VALUES(?,?,?,?) ON CONFLICT(job) DO UPDATE SET
                    cursor=excluded.cursor,updated_at=excluded.updated_at,
                    detail_json=excluded.detail_json""",
                    (job, str(deadline), utc_now(),
                     json.dumps(detail, ensure_ascii=False, default=str)),
                )
            return max(previous, deadline)

    def expire_pause(self, job: str, now: float | None = None) -> float:
        now = time.time() if now is None else float(now)
        with self.control_connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT cursor FROM crawl_state WHERE job=?", (job,)
            ).fetchone()
            try:
                deadline = float(row["cursor"]) if row else 0.0
            except (TypeError, ValueError):
                deadline = 0.0
            if not math.isfinite(deadline):
                deadline = 0.0
            if deadline > now:
                return deadline
            if row and deadline > 0:
                conn.execute(
                    """UPDATE crawl_state SET cursor='0',updated_at=?,detail_json=?
                    WHERE job=?""",
                    (utc_now(), json.dumps({
                        "resumed_at": int(now), "previous_resume_at": int(deadline),
                    }), job),
                )
            return 0.0

    def reserve_request_slot(
        self, scope: str, interval_seconds: float, slots: int = 1
    ) -> float:
        """Reserve consecutive request times across collectors using the control DB."""
        interval = max(0.01, float(interval_seconds))
        count = max(1, int(slots))
        job = f"rate_slot:{scope}"
        with self.control_connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = time.time()
            row = conn.execute(
                "SELECT cursor FROM crawl_state WHERE job=?", (job,)
            ).fetchone()
            try:
                previous = float(row["cursor"]) if row else 0.0
            except (TypeError, ValueError):
                previous = 0.0
            start = max(now, previous)
            conn.execute(
                """INSERT INTO crawl_state(job,cursor,updated_at,detail_json)
                VALUES(?,?,?,NULL)
                ON CONFLICT(job) DO UPDATE SET
                cursor=excluded.cursor,updated_at=excluded.updated_at""",
                (job, str(start + count * interval), utc_now()),
            )
        return max(0.0, start - time.time())

    def record_failure(
        self,
        job: str,
        item_id: str,
        error: str,
        reset_attempts: bool = False,
        *,
        error_class: str | None = None,
        error_code: str = "",
        endpoint: str = "",
        sample_capture: str = "",
        next_retry_at: str | None = None,
        failure_status: str | None = None,
    ) -> int:
        now = utc_now()
        normalized_class = error_class or classify_error(error)
        with self.control_connect() as conn:
            previous = conn.execute(
                """SELECT attempts,first_seen FROM crawl_failures
                WHERE job=? AND item_id=?""",
                (job, str(item_id)),
            ).fetchone()
            attempts = (
                1
                if reset_attempts or previous is None
                else int(previous["attempts"] or 0) + 1
            )
            status_value = failure_status or classify_failure_status(
                normalized_class,
                attempts,
            )
            retry_value = next_retry_at
            if status_value in {
                "blocked",
                "not_found",
                "protocol_ticket",
                "exhausted",
                "resolved",
            }:
                retry_value = None
            elif retry_value is None:
                retry_value = calculate_next_retry_at(
                    normalized_class,
                    attempts,
                )
            first_seen = (
                str(previous["first_seen"])
                if previous is not None and previous["first_seen"]
                else now
            )
            conn.execute(
                """INSERT INTO crawl_failures(
                    job,item_id,attempts,last_error,error_class,error_code,
                    endpoint,sample_capture,next_retry_at,failure_status,
                    first_seen,updated_at,resolved,resolution_note
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,NULL)
                ON CONFLICT(job,item_id) DO UPDATE SET
                attempts=excluded.attempts,
                last_error=excluded.last_error,
                error_class=excluded.error_class,
                error_code=excluded.error_code,
                endpoint=excluded.endpoint,
                sample_capture=excluded.sample_capture,
                next_retry_at=excluded.next_retry_at,
                failure_status=excluded.failure_status,
                first_seen=COALESCE(crawl_failures.first_seen, excluded.first_seen),
                updated_at=excluded.updated_at,
                resolved=0,
                resolution_note=NULL""",
                (
                    job,
                    str(item_id),
                    attempts,
                    error,
                    normalized_class,
                    error_code or None,
                    endpoint or None,
                    sample_capture or None,
                    retry_value,
                    status_value,
                    first_seen,
                    now,
                ),
            )
            source = (
                "siteb"
                if job.startswith("siteb")
                else "sitea"
                if job.startswith("sitea")
                else "runtime"
            )
            if status_value in {
                "blocked",
                "not_found",
                "protocol_ticket",
                "exhausted",
            }:
                conn.execute(
                    """INSERT INTO data_limits(
                        source,task_id,item_id,status,reason,updated_at
                    ) VALUES(?,?,?,?,?,?)
                    ON CONFLICT(source,task_id,item_id) DO UPDATE SET
                        status=excluded.status,
                        reason=excluded.reason,
                        updated_at=excluded.updated_at""",
                    (
                        source,
                        job,
                        str(item_id),
                        status_value,
                        error[:1000],
                        now,
                    ),
                )
            else:
                conn.execute(
                    "DELETE FROM data_limits WHERE task_id=? AND item_id=?",
                    (job, str(item_id)),
                )
            return attempts

    def resolve_failure(self, job: str, item_id: str) -> None:
        with self.control_connect() as conn:
            conn.execute(
                """UPDATE crawl_failures SET resolved=1,failure_status='resolved',
                next_retry_at=NULL,updated_at=?,resolution_note=?
                WHERE job=? AND item_id=?""",
                (utc_now(), "completed successfully", job, str(item_id)),
            )
            conn.execute(
                "DELETE FROM data_limits WHERE task_id=? AND item_id=?",
                (job, str(item_id)),
            )

    def resolve_failures(self, job: str, item_ids: Iterable[str]) -> int:
        """Resolve a known set of transient failures in bounded SQLite batches."""
        values = list(dict.fromkeys(str(item_id) for item_id in item_ids if item_id))
        if not values:
            return 0
        resolved = 0
        with self.control_connect() as conn:
            for offset in range(0, len(values), 900):
                batch = values[offset:offset + 900]
                marks = ",".join("?" for _ in batch)
                cur = conn.execute(
                    f"UPDATE crawl_failures SET resolved=1,failure_status='resolved',"
                    f"next_retry_at=NULL,updated_at=?,resolution_note=? "
                    f"WHERE job=? "
                    f"AND resolved=0 AND item_id IN ({marks})",
                    [utc_now(), "completed successfully", job, *batch],
                )
                resolved += cur.rowcount
                conn.execute(
                    f"DELETE FROM data_limits WHERE task_id=? "
                    f"AND item_id IN ({marks})",
                    [job, *batch],
                )
        return resolved

    def remap_failure_ids(
        self,
        job: str,
        normalizer: Callable[[str], str],
    ) -> int:
        """Merge legacy failure identifiers into their canonical keys."""
        terminal_statuses = {
            "blocked",
            "not_found",
            "protocol_ticket",
            "exhausted",
        }
        with self.control_connect() as conn:
            rows = conn.execute(
                """SELECT job,item_id,attempts,last_error,error_class,error_code,
                endpoint,sample_capture,next_retry_at,failure_status,first_seen,
                updated_at,resolved,resolution_note
                FROM crawl_failures WHERE job=?""",
                (job,),
            ).fetchall()
            groups: dict[str, list[sqlite3.Row]] = {}
            moved = 0
            for row in rows:
                original = str(row["item_id"])
                target = str(normalizer(original) or original)
                groups.setdefault(target, []).append(row)
                if target != original:
                    moved += 1
            for item_id, members in groups.items():
                originals = {str(row["item_id"]) for row in members}
                if len(originals) == 1 and item_id in originals:
                    continue
                active = [
                    row for row in members
                    if int(row["resolved"] or 0) == 0
                ]
                selected = max(
                    active or members,
                    key=lambda row: (
                        str(row["updated_at"] or ""),
                        int(row["attempts"] or 0),
                    ),
                )
                first_seen = min(
                    (
                        str(row["first_seen"])
                        for row in members
                        if row["first_seen"]
                    ),
                    default=str(selected["updated_at"] or utc_now()),
                )
                resolved = 0 if active else 1
                attempts = max(
                    int(row["attempts"] or 0)
                    for row in members
                )
                marks = ",".join("?" for _ in originals)
                conn.execute(
                    f"""DELETE FROM crawl_failures
                    WHERE job=? AND item_id IN ({marks})""",
                    (job, *sorted(originals)),
                )
                conn.execute(
                    """INSERT INTO crawl_failures(
                        job,item_id,attempts,last_error,error_class,error_code,
                        endpoint,sample_capture,next_retry_at,failure_status,
                        first_seen,updated_at,resolved,resolution_note
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        job,
                        item_id,
                        attempts,
                        selected["last_error"],
                        selected["error_class"],
                        selected["error_code"],
                        selected["endpoint"],
                        selected["sample_capture"],
                        selected["next_retry_at"],
                        selected["failure_status"],
                        first_seen,
                        selected["updated_at"],
                        resolved,
                        selected["resolution_note"],
                    ),
                )
                conn.execute(
                    f"""DELETE FROM data_limits
                    WHERE task_id=? AND item_id IN ({marks})""",
                    (job, *sorted(originals)),
                )
                status = str(selected["failure_status"] or "")
                if resolved == 0 and status in terminal_statuses:
                    source = (
                        "siteb"
                        if job.startswith("siteb")
                        else "sitea"
                        if job.startswith("sitea")
                        else "runtime"
                    )
                    conn.execute(
                        """INSERT INTO data_limits(
                            source,task_id,item_id,status,reason,updated_at
                        ) VALUES(?,?,?,?,?,?)""",
                        (
                            source,
                            job,
                            item_id,
                            status,
                            str(selected["last_error"] or "")[:1000],
                            selected["updated_at"] or utc_now(),
                        ),
                    )
        return moved

    def unresolved_failures(
        self,
        job: str,
        max_attempts: int = 3,
        *,
        due_only: bool = False,
    ) -> list[sqlite3.Row]:
        """Return retryable failures in oldest-first order."""
        due_clause = (
            "AND (next_retry_at IS NULL OR next_retry_at<=?)"
            if due_only
            else ""
        )
        params: list[Any] = [job, max_attempts]
        if due_only:
            params.append(utc_now())
        with self.control_connect() as conn:
            return conn.execute(
                f"""SELECT item_id,attempts,last_error,error_class,next_retry_at,
                failure_status,updated_at FROM crawl_failures
                WHERE job=? AND resolved=0
                AND (failure_status='deferred' OR attempts<?)
                AND failure_status IN ('retryable','deferred')
                {due_clause}
                ORDER BY COALESCE(next_retry_at,updated_at),item_id""",
                params,
            ).fetchall()

    def failure_summary(self, job: str) -> tuple[int, int]:
        """Return unresolved and exhausted failure counts for a crawl job."""
        with self.control_connect() as conn:
            row = conn.execute(
                """SELECT SUM(CASE WHEN resolved=0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN resolved=0 AND failure_status IN (
                    'blocked','not_found','protocol_ticket','exhausted'
                ) THEN 1 ELSE 0 END)
                FROM crawl_failures WHERE job=?""",
                (job,),
            ).fetchone()
        return int(row[0] or 0), int(row[1] or 0)

    def failure_counts(self, job: str) -> dict[str, int]:
        """Return control-plane failure counts without scanning business data."""
        now = utc_now()
        with self.control_connect() as conn:
            row = conn.execute(
                """SELECT
                SUM(CASE WHEN resolved=0 THEN 1 ELSE 0 END) unresolved,
                SUM(CASE WHEN resolved=0 AND failure_status='retryable'
                    THEN 1 ELSE 0 END) retryable,
                SUM(CASE WHEN resolved=0 AND failure_status='deferred'
                    THEN 1 ELSE 0 END) deferred,
                SUM(CASE WHEN resolved=0 AND failure_status IN (
                    'blocked','not_found','protocol_ticket','exhausted'
                ) THEN 1 ELSE 0 END) terminal,
                SUM(CASE WHEN resolved=0
                    AND failure_status IN ('retryable','deferred')
                    AND (next_retry_at IS NULL OR next_retry_at<=?)
                    THEN 1 ELSE 0 END) due
                FROM crawl_failures WHERE job=?""",
                (now, job),
            ).fetchone()
        return {
            "unresolved": int(row["unresolved"] or 0),
            "retryable": int(row["retryable"] or 0),
            "deferred": int(row["deferred"] or 0),
            "terminal": int(row["terminal"] or 0),
            "due": int(row["due"] or 0),
        }

    def terminal_failure_ids(self, job: str) -> set[str]:
        """Return unresolved objects with a terminal control-plane status."""
        with self.control_connect() as conn:
            rows = conn.execute(
                """SELECT item_id FROM crawl_failures
                WHERE job=? AND resolved=0
                  AND failure_status IN (
                    'blocked','not_found','protocol_ticket','exhausted'
                  )""",
                (job,),
            ).fetchall()
        return {str(row["item_id"]) for row in rows}

    def mark_failure_terminal(
        self,
        job: str,
        item_id: str,
        status: str,
        error: str,
        *,
        error_class: str | None = None,
    ) -> int:
        return self.record_failure(
            job,
            item_id,
            error,
            error_class=error_class,
            failure_status=status,
            next_retry_at=None,
        )

    def set_data_limit(
        self,
        source: str,
        task_id: str,
        item_id: str,
        status: str,
        reason: str = "",
    ) -> None:
        with self.control_connect() as conn:
            conn.execute(
                """INSERT INTO data_limits(
                    source,task_id,item_id,status,reason,updated_at
                ) VALUES(?,?,?,?,?,?)
                ON CONFLICT(source,task_id,item_id) DO UPDATE SET
                    status=excluded.status,
                    reason=excluded.reason,
                    updated_at=excluded.updated_at""",
                (
                    source,
                    task_id,
                    str(item_id),
                    status,
                    reason[:1000],
                    utc_now(),
                ),
            )

    def record_route_health(
        self,
        route_class: str,
        route: str,
        status: str,
        *,
        latency_ms: float | None = None,
        error_class: str = "",
        note: str = "",
        failure_threshold: int = 2,
        cooldown_seconds: int = 600,
    ) -> dict[str, Any]:
        checked_at = utc_now()
        with self.control_connect() as conn:
            previous = conn.execute(
                """SELECT consecutive_failures FROM route_health_state
                WHERE route_class=? AND route=?""",
                (route_class, route),
            ).fetchone()
            if status == "ok":
                consecutive = 0
                cooldown_until = None
            else:
                consecutive = int(previous[0] or 0) + 1 if previous else 1
                cooldown_until = None
                if consecutive >= max(1, failure_threshold):
                    cooldown_until = (
                        datetime.now(timezone.utc)
                        + timedelta(seconds=max(1, cooldown_seconds))
                    ).isoformat()
            conn.execute(
                """INSERT INTO route_health_state(
                    route_class,route,status,latency_ms,error_class,note,
                    consecutive_failures,checked_at,cooldown_until
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(route_class,route) DO UPDATE SET
                    status=excluded.status,
                    latency_ms=excluded.latency_ms,
                    error_class=excluded.error_class,
                    note=excluded.note,
                    consecutive_failures=excluded.consecutive_failures,
                    checked_at=excluded.checked_at,
                    cooldown_until=excluded.cooldown_until""",
                (
                    route_class,
                    route,
                    status,
                    latency_ms,
                    error_class,
                    note,
                    consecutive,
                    checked_at,
                    cooldown_until,
                ),
            )
        return {
            "route_class": route_class,
            "route": route,
            "status": status,
            "latency_ms": latency_ms,
            "error_class": error_class,
            "note": note,
            "consecutive_failures": consecutive,
            "checked_at": checked_at,
            "cooldown_until": cooldown_until,
        }

    def get_route_health(
        self,
        route_class: str,
        route: str | None = None,
    ) -> list[sqlite3.Row]:
        with self.control_connect() as conn:
            if route is not None:
                return conn.execute(
                    """SELECT * FROM route_health_state
                    WHERE route_class=? AND route=?""",
                    (route_class, route),
                ).fetchall()
            return conn.execute(
                """SELECT * FROM route_health_state WHERE route_class=?
                ORDER BY CASE status WHEN 'ok' THEN 0 ELSE 1 END,checked_at DESC""",
                (route_class,),
            ).fetchall()

    def start_task_run(
        self,
        task_id: str,
        route: str,
        result: Any = None,
    ) -> str:
        run_id = uuid.uuid4().hex
        with self.control_connect() as conn:
            conn.execute(
                """INSERT INTO task_runs(
                    run_id,task_id,route,status,started_at,result_json
                ) VALUES(?,?,?,?,?,?)""",
                (
                    run_id,
                    task_id,
                    route,
                    "running",
                    utc_now(),
                    json.dumps(result, ensure_ascii=False, default=str)
                    if result is not None
                    else None,
                ),
            )
        return run_id

    def finish_task_run(
        self,
        run_id: str,
        status: str,
        *,
        exit_code: int | None = None,
        elapsed_seconds: float | None = None,
        error_class: str = "",
        result: Any = None,
    ) -> None:
        with self.control_connect() as conn:
            conn.execute(
                """UPDATE task_runs SET status=?,finished_at=?,exit_code=?,
                elapsed_seconds=?,error_class=?,result_json=?
                WHERE run_id=?""",
                (
                    status,
                    utc_now(),
                    exit_code,
                    elapsed_seconds,
                    error_class,
                    json.dumps(result, ensure_ascii=False, default=str)
                    if result is not None
                    else None,
                    run_id,
                ),
            )

    def series_watermark(
        self, source: str, dataset: str, item_id: str, platform: str,
    ) -> str:
        """该 (source,dataset,item_id,platform) 组合的最新 ts（无行则空串）。

        供 ingest 层做增量水位：全量响应只写比水位更新的行。

        2026-10-11 事故：库涨到百GB级后统计失真，planner 曾选
        idx_series_dataset_ts 只按 dataset 过滤 → 全分区扫描数亿行，
        单次水位查询实测 723s，直接拖死页面采集。INDEXED BY 强制走
        (dataset,item_id,metric,platform,ts) 前缀，每个 metric 一次
        定位取尾，与表规模无关；ANALYZE 之后也不依赖统计保持稳定。
        """
        try:
            with self.connect() as conn:
                row = conn.execute(
                    """SELECT MAX(ts) FROM series
                    INDEXED BY idx_series_dataset_item
                    WHERE source=? AND dataset=? AND item_id=? AND platform=?""",
                    (source, dataset, str(item_id), platform),
                ).fetchone()
            return str(row[0]) if row and row[0] else ""
        except sqlite3.Error:
            return ""

    def recent_task_finishes(
        self, task_ids: list[str], *, limit: int = 100
    ) -> list[sqlite3.Row]:
        """每个任务最近一次运行完成时间（供监督器初始化反饥饿时钟）。"""
        values = [str(t) for t in dict.fromkeys(task_ids) if str(t)]
        if not values:
            return []
        with self.control_connect() as conn:
            placeholders = ",".join("?" * len(values))
            return conn.execute(
                f"""SELECT task_id, MAX(finished_at) AS finished_at
                FROM task_runs
                WHERE task_id IN ({placeholders}) AND finished_at IS NOT NULL
                GROUP BY task_id LIMIT ?""",
                [*values, max(1, int(limit))],
            ).fetchall()

    def update_heartbeat(
        self,
        worker_id: str,
        task_id: str,
        *,
        pid: int | None,
        status: str,
        route: str,
        started_at: str,
        owner_id: str = "",
        lease_until: str = "",
        detail: Any = None,
    ) -> None:
        now = utc_now()
        with self.control_connect() as conn:
            conn.execute(
                """INSERT INTO worker_heartbeats(
                    worker_id,task_id,pid,status,route,owner_id,lease_until,
                    started_at,heartbeat_at,detail_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    task_id=excluded.task_id,
                    pid=excluded.pid,
                    status=excluded.status,
                    route=excluded.route,
                    owner_id=excluded.owner_id,
                    lease_until=excluded.lease_until,
                    started_at=CASE
                      WHEN worker_heartbeats.pid<>excluded.pid
                        OR COALESCE(worker_heartbeats.owner_id,'')
                           <>COALESCE(excluded.owner_id,'')
                      THEN excluded.started_at
                      ELSE worker_heartbeats.started_at
                    END,
                    heartbeat_at=excluded.heartbeat_at,
                    detail_json=excluded.detail_json""",
                (
                    worker_id,
                    task_id,
                    pid,
                    status,
                    route,
                    owner_id or None,
                    lease_until or None,
                    started_at,
                    now,
                    json.dumps(detail, ensure_ascii=False, default=str)
                    if detail is not None
                    else None,
                ),
            )

    def remove_heartbeat(self, worker_id: str) -> None:
        with self.control_connect() as conn:
            conn.execute(
                "DELETE FROM worker_heartbeats WHERE worker_id=?",
                (worker_id,),
            )

    def register_task_definitions(
        self,
        definitions: Iterable[dict[str, Any]],
    ) -> None:
        now = utc_now()
        rows = []
        for definition in definitions:
            task_id = str(definition.get("task_id") or "").strip()
            if not task_id:
                continue
            rows.append(
                (
                    task_id,
                    str(definition.get("route_class") or "local"),
                    str(definition.get("lane") or "bulk"),
                    str(definition.get("mode") or "scheduled"),
                    int(definition.get("interval_seconds") or 0),
                    json.dumps(
                        definition,
                        ensure_ascii=False,
                        default=str,
                    ),
                    now,
                )
            )
        if not rows:
            return
        with self.control_connect() as conn:
            conn.executemany(
                """INSERT INTO task_definitions(
                    task_id,route_class,lane,mode,interval_seconds,
                    definition_json,updated_at
                ) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    route_class=excluded.route_class,
                    lane=excluded.lane,
                    mode=excluded.mode,
                    interval_seconds=excluded.interval_seconds,
                    definition_json=excluded.definition_json,
                    updated_at=excluded.updated_at""",
                rows,
            )

    def claim_task_lease(
        self,
        task_id: str,
        owner_id: str,
        run_id: str,
        *,
        lease_seconds: int,
        pid: int | None = None,
        detail: Any = None,
    ) -> bool:
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        lease_until = (
            now_dt + timedelta(seconds=max(30, lease_seconds))
        ).isoformat()
        with self.control_connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT owner_id,lease_until FROM task_leases
                WHERE task_id=?""",
                (task_id,),
            ).fetchone()
            if row and str(row["owner_id"]) != owner_id:
                try:
                    existing_until = datetime.fromisoformat(
                        str(row["lease_until"])
                    )
                    if existing_until.tzinfo is None:
                        existing_until = existing_until.replace(
                            tzinfo=timezone.utc
                        )
                except (TypeError, ValueError):
                    existing_until = now_dt
                if existing_until > now_dt:
                    return False
            conn.execute(
                """INSERT INTO task_leases(
                    task_id,owner_id,run_id,pid,status,acquired_at,
                    lease_until,updated_at,detail_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id) DO UPDATE SET
                    owner_id=excluded.owner_id,
                    run_id=excluded.run_id,
                    pid=excluded.pid,
                    status=excluded.status,
                    acquired_at=excluded.acquired_at,
                    lease_until=excluded.lease_until,
                    updated_at=excluded.updated_at,
                    detail_json=excluded.detail_json""",
                (
                    task_id,
                    owner_id,
                    run_id,
                    pid,
                    "running",
                    now,
                    lease_until,
                    now,
                    json.dumps(detail, ensure_ascii=False, default=str)
                    if detail is not None
                    else None,
                ),
            )
        return True

    def renew_task_lease(
        self,
        task_id: str,
        owner_id: str,
        *,
        lease_seconds: int,
        pid: int | None = None,
        detail: Any = None,
    ) -> str | None:
        now_dt = datetime.now(timezone.utc)
        lease_until = (
            now_dt + timedelta(seconds=max(30, lease_seconds))
        ).isoformat()
        with self.control_connect() as conn:
            cursor = conn.execute(
                """UPDATE task_leases SET pid=COALESCE(?,pid),
                status='running',lease_until=?,updated_at=?,detail_json=?
                WHERE task_id=? AND owner_id=?""",
                (
                    pid,
                    lease_until,
                    now_dt.isoformat(),
                    json.dumps(detail, ensure_ascii=False, default=str)
                    if detail is not None
                    else None,
                    task_id,
                    owner_id,
                ),
            )
        return lease_until if cursor.rowcount else None

    def release_task_lease(
        self,
        task_id: str,
        owner_id: str,
        *,
        status: str = "released",
    ) -> None:
        with self.control_connect() as conn:
            conn.execute(
                """UPDATE task_leases SET status=?,lease_until=?,
                updated_at=? WHERE task_id=? AND owner_id=?""",
                (status, utc_now(), utc_now(), task_id, owner_id),
            )

    def task_leases(self, *, status: str | None = None) -> list[sqlite3.Row]:
        with self.control_connect() as conn:
            if status:
                return conn.execute(
                    """SELECT * FROM task_leases
                    WHERE status=? ORDER BY task_id""",
                    (status,),
                ).fetchall()
            return conn.execute(
                "SELECT * FROM task_leases ORDER BY task_id"
            ).fetchall()

    def expired_task_leases(self) -> list[sqlite3.Row]:
        now = utc_now()
        with self.control_connect() as conn:
            rows = conn.execute(
                """SELECT * FROM task_leases
                WHERE status='running' AND lease_until<=?
                ORDER BY lease_until""",
                (now,),
            ).fetchall()
            if rows:
                conn.executemany(
                    """UPDATE task_leases SET status='expired',updated_at=?
                    WHERE task_id=? AND owner_id=?""",
                    (
                        (now, row["task_id"], row["owner_id"])
                        for row in rows
                    ),
                )
        return rows

    def reset_expired_task_slices(self) -> int:
        now = utc_now()
        with self.control_connect() as conn:
            cursor = conn.execute(
                """UPDATE task_slices SET status='ready',lease_owner=NULL,
                lease_until=NULL,updated_at=?,
                result_json=COALESCE(result_json,'{}')
                WHERE status='running' AND lease_until IS NOT NULL
                  AND lease_until<=?""",
                (now, now),
            )
            return int(cursor.rowcount)

    def update_task_slice(
        self,
        task_id: str,
        slice_id: str,
        status: str,
        *,
        outcome: str = "",
        owner_id: str = "",
        lease_seconds: int = 0,
        result: Any = None,
        increment_attempt: bool = False,
    ) -> bool:
        if status not in {"ready", "running", "done", "deferred"}:
            raise ValueError(f"invalid task slice status: {status}")
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        lease_until = (
            now_dt + timedelta(seconds=max(30, lease_seconds))
        ).isoformat() if lease_seconds else None
        with self.control_connect() as conn:
            cursor = conn.execute(
                """INSERT INTO task_slices(
                    task_id,slice_id,status,outcome,attempts,lease_owner,
                    lease_until,first_seen,updated_at,result_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(task_id,slice_id) DO UPDATE SET
                    status=excluded.status,
                    outcome=excluded.outcome,
                    attempts=task_slices.attempts+?,
                    lease_owner=excluded.lease_owner,
                    lease_until=excluded.lease_until,
                    updated_at=excluded.updated_at,
                    result_json=excluded.result_json
                WHERE (
                    excluded.lease_owner IS NULL
                    OR task_slices.lease_owner IS NULL
                    OR task_slices.lease_owner=excluded.lease_owner
                    OR task_slices.status<>'running'
                    OR task_slices.lease_until IS NULL
                    OR task_slices.lease_until<=excluded.updated_at
                )
                AND (
                    excluded.status<>'running'
                    OR task_slices.status<>'done'
                )""",
                (
                    task_id,
                    slice_id,
                    status,
                    outcome or None,
                    1 if increment_attempt else 0,
                    owner_id or None,
                    lease_until,
                    now,
                    now,
                    json.dumps(result, ensure_ascii=False, default=str)
                    if result is not None
                    else None,
                    1 if increment_attempt else 0,
                ),
            )
        return bool(cursor.rowcount)

    def seed_task_slices(
        self,
        task_id: str,
        slices: Iterable[tuple[str, str]],
        *,
        result: Any = None,
        promote_existing: bool = False,
    ) -> int:
        values = list(dict.fromkeys(
            (
                str(slice_id),
                str(outcome or "legacy_data"),
            )
            for slice_id, outcome in slices
            if str(slice_id)
        ))
        if not values:
            return 0
        now = utc_now()
        result_json = (
            json.dumps(result, ensure_ascii=False, default=str)
            if result is not None
            else None
        )
        inserted = 0
        with self.control_connect() as conn:
            for offset in range(0, len(values), 5000):
                batch = values[offset:offset + 5000]
                before = conn.total_changes
                if promote_existing:
                    conn.executemany(
                        """INSERT INTO task_slices(
                            task_id,slice_id,status,outcome,attempts,
                            lease_owner,lease_until,first_seen,updated_at,
                            result_json
                        ) VALUES(?,?,'done',?,0,NULL,NULL,?,?,?)
                        ON CONFLICT(task_id,slice_id) DO UPDATE SET
                            status='done',
                            outcome=excluded.outcome,
                            lease_owner=NULL,
                            lease_until=NULL,
                            updated_at=excluded.updated_at,
                            result_json=excluded.result_json
                        WHERE task_slices.status IN ('ready','deferred')""",
                        (
                            (
                                task_id,
                                slice_id,
                                outcome,
                                now,
                                now,
                                result_json,
                            )
                            for slice_id, outcome in batch
                        ),
                    )
                else:
                    conn.executemany(
                        """INSERT OR IGNORE INTO task_slices(
                            task_id,slice_id,status,outcome,attempts,
                            lease_owner,lease_until,first_seen,updated_at,
                            result_json
                        ) VALUES(?,?,'done',?,0,NULL,NULL,?,?,?)""",
                        (
                            (
                                task_id,
                                slice_id,
                                outcome,
                                now,
                                now,
                                result_json,
                            )
                            for slice_id, outcome in batch
                        ),
                    )
                inserted += conn.total_changes - before
        return inserted

    def task_slice_status(
        self,
        task_id: str,
        slice_id: str,
    ) -> sqlite3.Row | None:
        with self.control_connect() as conn:
            return conn.execute(
                """SELECT * FROM task_slices
                WHERE task_id=? AND slice_id=?""",
                (task_id, slice_id),
            ).fetchone()

    def task_slice_summary(self, task_id: str) -> dict[str, int]:
        with self.control_connect() as conn:
            rows = conn.execute(
                """SELECT status,COALESCE(outcome,'') outcome,COUNT(*) count
                FROM task_slices WHERE task_id=?
                GROUP BY status,COALESCE(outcome,'')""",
                (task_id,),
            ).fetchall()
        summary: dict[str, int] = {}
        for row in rows:
            key = str(row["status"])
            outcome = str(row["outcome"] or "")
            if outcome:
                key += "." + outcome
            summary[key] = int(row["count"])
        return summary

    def done_task_slice_ids(self, task_id: str) -> set[str]:
        """Return completed slice identifiers for a task."""
        with self.control_connect() as conn:
            rows = conn.execute(
                """SELECT slice_id FROM task_slices
                WHERE task_id=? AND status='done'""",
                (task_id,),
            ).fetchall()
        return {str(row["slice_id"]) for row in rows}

    def task_pipeline_summary(
        self,
        task_ids: Iterable[str],
        required_modules: Iterable[str],
        *,
        catalog_ids: Iterable[str] | None = None,
        item_aliases: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Summarize item completion from parent and module-level slices."""
        tasks = tuple(dict.fromkeys(
            str(task_id) for task_id in task_ids if str(task_id)
        ))
        modules = tuple(dict.fromkeys(
            str(module) for module in required_modules if str(module)
        ))
        catalog = (
            {str(item_id) for item_id in catalog_ids}
            if catalog_ids is not None
            else None
        )
        aliases = {
            str(item_id): str(parent_id)
            for item_id, parent_id in (item_aliases or {}).items()
            if str(item_id) and str(parent_id)
        }
        if not tasks:
            return {
                "attempted": 0,
                "completed": 0,
                "deferred": 0,
                "module_done": {},
                "outcomes": {},
            }
        marks = ",".join("?" for _ in tasks)
        with self.control_connect() as conn:
            rows = conn.execute(
                f"""SELECT task_id,slice_id,status,COALESCE(outcome,'') outcome
                FROM task_slices WHERE task_id IN ({marks})""",
                tasks,
            ).fetchall()
        parent_attempted: set[str] = set()
        parent_done: set[str] = set()
        parent_deferred: set[str] = set()
        object_modules: dict[str, set[str]] = {}
        deferred_modules: set[str] = set()
        module_objects = {module: set() for module in modules}
        outcomes: dict[str, int] = {}
        successful_outcomes = {"ok", "empty_ok", "legacy_data"}
        unverified_empty: set[str] = set()
        for row in rows:
            slice_id = str(row["slice_id"])
            status = str(row["status"])
            outcome = str(row["outcome"] or "")
            suffix = slice_id.rsplit(":", 1)[-1]
            if suffix in modules:
                raw_item_id = slice_id[:-(len(suffix) + 1)]
                item_id = aliases.get(raw_item_id, raw_item_id)
                if catalog is not None and item_id not in catalog:
                    continue
                parent_attempted.add(item_id)
                if status == "done":
                    if outcome in successful_outcomes:
                        object_modules.setdefault(item_id, set()).add(suffix)
                        module_objects[suffix].add(item_id)
                    if outcome == "empty_ok":
                        unverified_empty.add(item_id)
                    outcomes[outcome or "unknown"] = outcomes.get(
                        outcome or "unknown", 0
                    ) + 1
                elif status == "deferred":
                    deferred_modules.add(item_id)
                continue
            item_id = aliases.get(slice_id, slice_id)
            if catalog is not None and item_id not in catalog:
                continue
            parent_attempted.add(item_id)
            if status == "done" and outcome in successful_outcomes:
                parent_done.add(item_id)
            elif status == "deferred":
                parent_deferred.add(item_id)
        module_complete = {
            item_id
            for item_id, completed_modules in object_modules.items()
            if all(module in completed_modules for module in modules)
        }
        completed = module_complete if modules else parent_done | module_complete
        if catalog is not None:
            completed &= catalog
            parent_attempted &= catalog
            parent_deferred &= catalog
        return {
            "attempted": len(parent_attempted),
            "completed": len(completed),
            "deferred": len(
                (parent_deferred | deferred_modules) - completed
            ),
            "module_done": {
                module: len(module_objects[module])
                for module in modules
            },
            "outcomes": outcomes,
            "unverified_empty": len(unverified_empty),
        }

    def next_failure_delay(self, job: str, default: int = 60) -> int:
        now = datetime.now(timezone.utc)
        with self.control_connect() as conn:
            row = conn.execute(
                """SELECT MIN(next_retry_at) FROM crawl_failures
                WHERE job=? AND resolved=0
                AND failure_status IN ('retryable','deferred')""",
                (job,),
            ).fetchone()
        if not row or not row[0]:
            return max(1, default)
        try:
            retry_at = datetime.fromisoformat(str(row[0]))
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
        except ValueError:
            return max(1, default)
        return max(1, int((retry_at - now).total_seconds()))

    def reclassify_legacy_failures(self, limit: int = 0) -> dict[str, int]:
        with self.control_connect() as conn:
            sql = """SELECT job,item_id,attempts,last_error,error_class
                FROM crawl_failures
                WHERE resolved=0 AND (
                    (error_class='audit' AND (
                        last_error LIKE '%history_metrics=%'
                        OR last_error LIKE 'full=%'
                        OR last_error LIKE '%HTTP 200%'
                        OR last_error LIKE '%HTTP None%'
                        OR last_error LIKE '%ERR_SSL_PROTOCOL_ERROR%'
                        OR last_error LIKE '%ERR_CONNECTION_CLOSED%'
                        OR last_error LIKE '%ERR_CONNECTION_RESET%'
                        OR last_error LIKE '%ERR_TIMED_OUT%'
                        OR last_error LIKE '%ERR_TUNNEL_CONNECTION_FAILED%'
                        OR last_error LIKE '%ERR_NAME_NOT_RESOLVED%'
                        OR last_error LIKE '%timed out%'
                        OR last_error LIKE '%timeout%'
                        OR last_error LIKE '%metric/platform groups%'
                        OR last_error LIKE '%one-year span%'
                        OR last_error LIKE '%一年跨度%'
                        OR last_error LIKE '%指标/平台%'
                    ))
                      OR (error_class='protocol' AND (
                          last_error LIKE '%initial kline head is empty%'
                          OR last_error LIKE '%initial kline page is empty%'
                      ))
                      OR (error_class='unknown' AND (
                          last_error LIKE 'full=%'
                          OR last_error LIKE '%ResponseError: http error%'
                      ))
                )
                ORDER BY updated_at,job,item_id"""
            if limit > 0:
                sql += " LIMIT ?"
                rows = conn.execute(sql, (limit,)).fetchall()
            else:
                rows = conn.execute(sql).fetchall()
            counts: dict[str, int] = {}
            for row in rows:
                message = str(row["last_error"] or "")
                if message.strip().lower().startswith("full=") or (
                    "history_metrics=" in message
                    and classify_error(message) == "data_gap"
                ) or any(marker in message for marker in (
                    "initial kline head is empty",
                    "initial kline page is empty",
                )):
                    error_class = "data_gap"
                    status = "retryable"
                    attempts = min(2, max(1, int(row["attempts"] or 0)))
                    retry_at = calculate_next_retry_at(
                        error_class,
                        attempts,
                    )
                else:
                    error_class = classify_error(message)
                    if error_class == "audit":
                        # The response body is not available in some old
                        # failure rows.  Preserve an explicit audit marker;
                        # only legacy rows selected by the non-audit patterns
                        # below are eligible for migration.
                        continue
                    if error_class == "unknown" and any(
                        marker in message.lower()
                        for marker in (
                            "http none",
                            "err_timed_out",
                            "timed out",
                            "timeout",
                        )
                    ):
                        error_class = "transport"
                    attempts = max(1, int(row["attempts"] or 0))
                    status = classify_failure_status(
                        error_class,
                        attempts,
                    )
                    retry_at = calculate_next_retry_at(
                        error_class,
                        attempts,
                    )
                conn.execute(
                    """UPDATE crawl_failures SET attempts=?,error_class=?,
                    failure_status=?,next_retry_at=?,resolution_note=?,
                    updated_at=? WHERE job=? AND item_id=?""",
                    (
                        attempts,
                        error_class,
                        status,
                        retry_at,
                        "classified during control-plane migration",
                        utc_now(),
                        row["job"],
                        row["item_id"],
                    ),
                )
                if status not in {
                    "blocked", "not_found", "protocol_ticket", "exhausted",
                }:
                    conn.execute(
                        "DELETE FROM data_limits WHERE task_id=? AND item_id=?",
                        (row["job"], row["item_id"]),
                    )
                counts[error_class] = counts.get(error_class, 0) + 1
        return counts

    def repair_terminal_slices(self) -> int:
        """把历史终态失败切片从 done 纠正为 failed。

        _slice_finish 曾把 authorization/protocol/not_found 终态写成
        status='done'，导致 done_task_slice_ids 把它们计入完成度并把
        对象永久排除在队列外。数据本身仍由 crawl_failures 的终态票据
        管控，不会被重复抓取；这里只纠正完成度统计口径。
        """
        with self.control_connect() as conn:
            cursor = conn.execute(
                """UPDATE task_slices SET status='failed', updated_at=?
                WHERE status='done'
                AND COALESCE(outcome,'') IN (
                    'authorization','not_found'
                )""",
                (utc_now(),),
            )
            return cursor.rowcount

    def enqueue_public_pages(
        self, source: str, pages: Iterable[tuple[str, int, str]],
    ) -> int:
        """Add discovered page URLs without resetting existing checkpoints."""
        rows = list(dict.fromkeys(
            (url, priority, parent)
            for url, priority, parent in pages if url
        ))
        if not rows:
            return 0
        def enqueue() -> int:
            with self.control_connect() as conn:
                before = conn.total_changes
                conn.executemany(
                    """INSERT OR IGNORE INTO public_pages(
                        source,url,status,priority,discovered_from
                    ) VALUES(?,?,'ready',?,?)""",
                    ((source, url, priority, parent) for url, priority, parent in rows),
                )
                return conn.total_changes - before
        return _with_sqlite_retries(enqueue)

    def canonicalize_public_pages(
        self, source: str, canonicalize: Callable[[str], str],
    ) -> int:
        """Merge previously queued URLs differing only by URL encoding."""
        def canonicalize_rows() -> int:
            changed = 0
            with self.control_connect() as conn:
                rows = conn.execute(
                    """SELECT url,status,priority,discovered_from,attempts,
                        last_checked,next_due,last_error FROM public_pages
                    WHERE source=?""",
                    (source,),
                ).fetchall()
                for row in rows:
                    old_url = str(row["url"])
                    new_url = canonicalize(old_url)
                    if new_url == old_url:
                        continue
                    conn.execute(
                        """INSERT OR IGNORE INTO public_pages(
                            source,url,status,priority,discovered_from,attempts,
                            last_checked,next_due,last_error
                        ) VALUES(?,?,?,?,?,?,?,?,?)""",
                        (
                            source, new_url, row["status"], row["priority"],
                            row["discovered_from"], row["attempts"],
                            row["last_checked"], row["next_due"], row["last_error"],
                        ),
                    )
                    if row["status"] == "done":
                        conn.execute(
                            """UPDATE public_pages SET status='done',
                                next_due=?,last_checked=?,last_error=NULL
                            WHERE source=? AND url=? AND status='ready'""",
                            (
                                row["next_due"], row["last_checked"],
                                source, new_url,
                            ),
                        )
                    conn.execute(
                        "DELETE FROM public_pages WHERE source=? AND url=?",
                        (source, old_url),
                    )
                    changed += 1
            return changed
        return _with_sqlite_retries(canonicalize_rows)

    def recover_expired_public_pages(self, source: str) -> int:
        """Return expired page leases to the retry queue before a new run."""
        now = datetime.now(timezone.utc).isoformat()

        def recover() -> int:
            with self.control_connect() as conn:
                cursor = conn.execute(
                    """UPDATE public_pages SET status='deferred',
                        next_due=?,last_error=COALESCE(
                            last_error,'page lease expired before completion')
                    WHERE source=? AND status='running' AND next_due<=?""",
                    (now, source, now),
                )
                return cursor.rowcount

        return _with_sqlite_retries(recover)

    def claim_public_page(self, source: str, *, lease_seconds: int = 1800) -> str | None:
        """Atomically lease the next due page; crashed leases can be reclaimed."""
        now = datetime.now(timezone.utc)
        until = (now + timedelta(seconds=lease_seconds)).isoformat()
        def claim() -> str | None:
            with self.control_connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """SELECT url FROM public_pages
                    WHERE source=? AND (
                        status='ready'
                        OR (status IN ('done','deferred','running')
                            AND next_due<=?)
                    )
                      ORDER BY CASE
                          WHEN priority<=0 THEN 0
                          WHEN status='ready' THEN 1
                          WHEN status IN ('deferred','running') THEN 2
                          ELSE 3 END,
                          priority, COALESCE(next_due,'') ASC, url
                    LIMIT 1""",
                    (source, now.isoformat()),
                ).fetchone()
                if row is None:
                    return None
                url = str(row["url"])
                conn.execute(
                    """UPDATE public_pages SET status='running',
                        attempts=attempts+1,next_due=?
                    WHERE source=? AND url=?""",
                    (until, source, url),
                )
                return url
        return _with_sqlite_retries(claim)

    def renew_public_page(
        self,
        source: str,
        url: str,
        *,
        lease_seconds: int = 1800,
    ) -> bool:
        """Extend an active page lease while pagination is still running."""
        until = (
            datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        ).isoformat()

        def renew() -> bool:
            with self.control_connect() as conn:
                cursor = conn.execute(
                    """UPDATE public_pages SET next_due=?
                    WHERE source=? AND url=? AND status='running'""",
                    (until, source, url),
                )
                return bool(cursor.rowcount)

        return _with_sqlite_retries(renew)

    def finish_public_page(
        self, source: str, url: str, *, error: str = "",
        refresh_hours: int = 24,
        retry_at: str | None = None,
    ) -> None:
        def finish() -> None:
            now = datetime.now(timezone.utc)
            with self.control_connect() as conn:
                row = conn.execute(
                    "SELECT attempts FROM public_pages WHERE source=? AND url=?",
                    (source, url),
                ).fetchone()
                attempts = int(row["attempts"]) if row else 1
                delay = (
                    min(3600, 60 * (2 ** min(attempts - 1, 6)))
                    if error else max(1, refresh_hours) * 3600
                )
                conn.execute(
                    """UPDATE public_pages
                    SET status=?,next_due=?,last_checked=?,last_error=?
                    WHERE source=? AND url=?""",
                    (
                        "deferred" if error else "done",
                        retry_at or (now + timedelta(seconds=delay)).isoformat(),
                        now.isoformat(),
                        error[:500] if error else None,
                        source,
                        url,
                    ),
                )
        _with_sqlite_retries(finish)

    def public_page_summary(self, source: str) -> dict[str, int]:
        with self.control_connect() as conn:
            rows = conn.execute(
                """SELECT status,COUNT(*) AS count FROM public_pages
                WHERE source=? GROUP BY status""",
                (source,),
            ).fetchall()
        return {str(row["status"]): int(row["count"]) for row in rows}

    def record_endpoint_contract(
        self,
        source: str,
        method: str,
        path: str,
        schema_json: str,
    ) -> str:
        schema_hash = hashlib.sha256(
            schema_json.encode("utf-8")
        ).hexdigest()
        now = utc_now()
        with self.control_connect() as conn:
            row = conn.execute(
                """SELECT schema_hash,changes FROM endpoint_contracts
                WHERE source=? AND method=? AND path=?""",
                (source, method, path),
            ).fetchone()
            changed = bool(row and str(row["schema_hash"]) != schema_hash)
            status = "changed" if changed else "stable"
            changes = int(row["changes"] or 0) + int(changed) if row else 0
            conn.execute(
                """INSERT INTO endpoint_contracts(
                    source,method,path,schema_hash,schema_json,status,
                    first_seen,last_seen,changes
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source,method,path) DO UPDATE SET
                    schema_hash=excluded.schema_hash,
                    schema_json=excluded.schema_json,
                    status=excluded.status,
                    last_seen=excluded.last_seen,
                    changes=excluded.changes""",
                (
                    source,
                    method,
                    path,
                    schema_hash,
                    schema_json,
                    status,
                    now,
                    now,
                    changes,
                ),
            )
        return status

    def upsert_metadata(self, source: str, key: str, payload: Any) -> None:
        """Store non-time-series site metadata (taxonomies, config maps) keyed by source."""
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO site_metadata(source,key,payload,updated_at) VALUES(?,?,?,?)
                ON CONFLICT(source,key) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at""",
                (source, key, json.dumps(payload, ensure_ascii=False, default=str), utc_now()),
            )

    def get_metadata(self, source: str, key: str) -> Any | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT payload FROM site_metadata WHERE source=? AND key=?", (source, key)
            ).fetchone()
        if not row:
            return None
        try:
            return json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            return None

    def rebuild_endpoint_inventory(self) -> int:
        """Backfill endpoint counts from captures without rereading raw bodies."""
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT source,method,url,status,captured_at,raw_path,error
                FROM captures ORDER BY id"""
            ).fetchall()
            conn.execute("DELETE FROM endpoint_inventory")
            aggregates: dict[tuple[str, str, str], dict[str, Any]] = {}
            for row in rows:
                path = urlsplit(str(row["url"])).path or "/"
                key = (str(row["source"]), str(row["method"]), path)
                entry = aggregates.setdefault(key, {
                    "first_seen": row["captured_at"], "last_seen": row["captured_at"],
                    "captures": 0, "successful": 0, "bytes": 0,
                    "last_raw_path": row["raw_path"], "last_error": row["error"],
                })
                entry["last_seen"] = row["captured_at"]
                entry["captures"] += 1
                entry["successful"] += int(
                    row["status"] is not None and 200 <= int(row["status"]) < 300
                )
                if row["raw_path"]:
                    entry["last_raw_path"] = row["raw_path"]
                if row["error"]:
                    entry["last_error"] = row["error"]
            conn.executemany(
                """INSERT INTO endpoint_inventory(
                    source,method,path,first_seen,last_seen,captures,successful,bytes,
                    request_keys_json,response_schema_json,last_raw_path,last_error
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    (
                        source, method, path, value["first_seen"], value["last_seen"],
                        value["captures"], value["successful"], value["bytes"], "[]",
                        None, value["last_raw_path"], value["last_error"],
                    )
                    for (source, method, path), value in aggregates.items()
                ),
            )
            return len(aggregates)
