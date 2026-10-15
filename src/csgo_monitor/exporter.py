# -*- coding: utf-8 -*-
"""为清洗/预测导出数据：items / kline / history / index / listing 等。

导出到 Parquet（pandas+pyarrow 可用时）或 CSV，并输出各数据集的
新鲜度与连续性摘要，作为数据质量的第一道检查。

    python -m csgo_monitor export --datasets kline,index --format parquet
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .storage import Storage

log = logging.getLogger(__name__)

# 每个导出数据集：(series.dataset, 排序列说明)
EXPORT_DATASETS: dict[str, tuple[str, ...]] = {
    "kline": ("item_kline",),
    "history": ("item_history",),
    "index": ("market_index", "market_index_history", "market_index_snapshot",
              "ws_market_index", "ws_market_statistics"),
    "ws_kline": ("ws_market_kline_h", "ws_market_kline_d", "ws_market_kline_w"),
    "listing": ("ws_new_listing",),
    "rank": ("item_rank_snapshot",),
    "snapshot": ("item_snapshot",),
    "indicators": ("derived_kline_1d",),
}

BATCH = 50_000


@dataclass
class DatasetSummary:
    dataset: str
    rows: int
    items: int
    earliest: str
    latest: str
    freshness_hours: float
    # 日粒度数据集的连续性：条目平均缺失天数（相对其自身时间范围）
    avg_missing_days: float | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "rows": self.rows,
            "items": self.items,
            "earliest": self.earliest,
            "latest": self.latest,
            "freshness_hours": round(self.freshness_hours, 1),
            "avg_missing_days": (
                round(self.avg_missing_days, 2)
                if self.avg_missing_days is not None else None
            ),
        }


def _normalize_ts(value: Any) -> str:
    """统一导出时间戳为 UTC ISO8601。

    库内三种格式并存（iso+tz / 无时区 iso / 纯日期），pandas 混用必出错。
    无时区按 UTC 解释（采集管线以 UTC 写入）；纯日期补 T00:00:00+00:00。
    """
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) == 10:  # date-only
        return text + "T00:00:00+00:00"
    if "+" not in text and not text.endswith("Z"):
        return text + "+00:00"
    return text


def _bound_date(value: str) -> str:
    """把 since/until 边界值统一成 YYYY-MM-DD（UTC）。

    接受 ISO 日期/日期时间或 epoch 秒/毫秒时间戳。库内 ts 格式混杂
    （iso+tz / 无时区 iso / 纯日期），规范化字符串与原始列做字典序比较
    会在边界日丢行或混入——因此过滤统一在日期级别（substr(ts,1,10)）
    进行，纯日期精度对本用途足够。
    """
    text = str(value or "").strip()
    if not text:
        return ""
    if text.isdigit() and len(text) >= 10:
        number = float(text)
        if number > 10_000_000_000:
            number /= 1000
        try:
            return datetime.fromtimestamp(
                number, timezone.utc).date().isoformat()
        except (OverflowError, OSError, ValueError):
            return ""
    return text[:10]


def _iter_series(
    storage: Storage, datasets: tuple[str, ...],
    since: str = "", until: str = "",
) -> Iterator[dict]:
    clauses = [f"dataset IN ({','.join('?' * len(datasets))})"]
    args: list[Any] = list(datasets)
    since_date = _bound_date(since)
    if since_date:
        clauses.append("substr(ts,1,10) >= ?")
        args.append(since_date)
    until_date = _bound_date(until)
    if until_date:
        clauses.append("substr(ts,1,10) <= ?")
        args.append(until_date)
    with storage.read_connect() as conn:
        conn.row_factory = None
        cursor = conn.execute(
            """SELECT source,dataset,item_id,metric,platform,ts,
                      open,high,low,close,volume,turnover,extra_json
            FROM series WHERE """ + " AND ".join(clauses) + """
            ORDER BY dataset,item_id,ts""",
            args,
        )
        for row in cursor:
            yield {
                "source": row[0], "dataset": row[1], "item_id": row[2],
                "metric": row[3], "platform": row[4],
                "ts": _normalize_ts(row[5]),
                "open": row[6], "high": row[7], "low": row[8], "close": row[9],
                "volume": row[10], "turnover": row[11], "extra": row[12],
            }


def _iter_items(storage: Storage) -> Iterator[dict]:
    with storage.read_connect() as conn:
        conn.row_factory = None
        cursor = conn.execute(
            """SELECT source,item_id,name,market_hash_name,updated_at
            FROM items ORDER BY source,item_id"""
        )
        for row in cursor:
            yield {
                "source": row[0], "item_id": row[1], "name": row[2],
                "market_hash_name": row[3], "updated_at": row[4],
            }


def _write_parquet(path: Path, rows: Iterator[dict]) -> int:
    count = 0
    writer = None
    buffer: list[dict] = []
    for row in rows:
        buffer.append(row)
        if len(buffer) < BATCH:
            continue
        writer, written = _flush_parquet(writer, path, buffer)
        count += written
        buffer = []
    if buffer:
        writer, written = _flush_parquet(writer, path, buffer)
        count += written
    if writer is not None:
        writer.close()
    return count


SERIES_ARROW_SCHEMA = None


def _series_arrow_schema():
    """显式 schema：全 None 的列（如快照数据的 OHLC）不会被推断成 null 类型，
    避免分块写入时 Table schema does not match 报错。"""
    global SERIES_ARROW_SCHEMA
    if SERIES_ARROW_SCHEMA is None:
        import pyarrow as pa

        SERIES_ARROW_SCHEMA = pa.schema([
            ("source", pa.string()), ("dataset", pa.string()),
            ("item_id", pa.string()), ("metric", pa.string()),
            ("platform", pa.string()),
            ("ts", pa.timestamp("s", tz="UTC")),
            ("open", pa.float64()), ("high", pa.float64()),
            ("low", pa.float64()), ("close", pa.float64()),
            ("volume", pa.float64()), ("turnover", pa.float64()),
            ("extra", pa.string()),
        ])
    return SERIES_ARROW_SCHEMA


def _flush_parquet(writer: Any, path: Path, chunk: list[dict]) -> tuple[Any, int]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    # ts 字符串 -> datetime（timestamp("s",UTC) schema 需要 datetime 对象）
    prepared = []
    for row in chunk:
        ts = row.get("ts")
        if isinstance(ts, str) and ts:
            try:
                ts = datetime.fromisoformat(ts)
            except ValueError:
                ts = None
        prepared.append({**row, "ts": ts})
    table = pa.Table.from_pylist(prepared, schema=_series_arrow_schema())
    if writer is None:
        writer = pq.ParquetWriter(path, table.schema, compression="zstd")
    writer.write_table(table)
    return writer, len(chunk)


def _write_csv(path: Path, rows: Iterator[dict]) -> int:
    count = 0
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "source", "dataset", "item_id", "metric", "platform", "ts",
            "open", "high", "low", "close", "volume", "turnover", "extra",
        ])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def dataset_summaries(storage: Storage, datasets: tuple[str, ...]) -> list[DatasetSummary]:
    now = datetime.now(timezone.utc)
    summaries: list[DatasetSummary] = []
    with storage.read_connect() as conn:
        for dataset in datasets:
            row = conn.execute(
                """SELECT COUNT(*) rows, COUNT(DISTINCT item_id) items,
                          MIN(ts) earliest, MAX(ts) latest
                FROM series WHERE dataset=?""",
                (dataset,),
            ).fetchone()
            rows, items, earliest, latest = row
            if not rows:
                continue
            try:
                latest_dt = datetime.fromisoformat(str(latest))
                if latest_dt.tzinfo is None:
                    latest_dt = latest_dt.replace(tzinfo=timezone.utc)
                freshness = (now - latest_dt).total_seconds() / 3600
            except ValueError:
                freshness = -1.0
            summaries.append(DatasetSummary(
                dataset=dataset, rows=rows, items=items,
                earliest=str(earliest), latest=str(latest),
                freshness_hours=max(0.0, freshness),
                avg_missing_days=_continuity_gap(conn, dataset),
            ))
    return summaries


def _continuity_gap(conn: Any, dataset: str, limit: int = 500) -> float | None:
    """抽样估计日粒度数据集的平均缺失天数（条目内日期空洞）。"""
    sample = conn.execute(
        """SELECT item_id, COUNT(DISTINCT substr(ts,1,10)) days,
                  MIN(substr(ts,1,10)) first_day, MAX(substr(ts,1,10)) last_day
        FROM series WHERE dataset=? AND item_id<>'' GROUP BY item_id LIMIT ?""",
        (dataset, limit),
    ).fetchall()
    if not sample:
        return None
    gaps: list[float] = []
    for item_id, days, first_day, last_day in sample:
        try:
            span = (
                datetime.strptime(str(last_day), "%Y-%m-%d")
                - datetime.strptime(str(first_day), "%Y-%m-%d")
            ).days + 1
        except ValueError:
            continue
        if span > days > 0:
            gaps.append((span - days) / span)
    return sum(gaps) / len(gaps) * 100 if gaps else 0.0


def run_export(
    storage: Storage,
    out_dir: Path,
    *,
    datasets: tuple[str, ...] = (),
    include_items: bool = True,
    fmt: str = "parquet",
    since: str = "",
    until: str = "",
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    selected = {
        key: values for key, values in EXPORT_DATASETS.items()
        if not datasets or key in datasets
    }
    flat = tuple(sorted({d for values in selected.values() for d in values}))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results: dict[str, Any] = {"files": [], "summaries": []}

    if include_items:
        path = out_dir / f"items_{stamp}.csv"
        count = _write_csv_rows(
            path, _iter_items(storage),
            ["source", "item_id", "name", "market_hash_name", "updated_at"],
        )
        results["files"].append({"path": str(path), "rows": count})
        log.info("exported items: %s rows -> %s", count, path)

    for key, values in selected.items():
        if not values:
            continue
        rows = _iter_series(storage, values, since=since, until=until)
        suffix = ".parquet" if fmt == "parquet" else ".csv"
        path = out_dir / f"{key}_{stamp}{suffix}"
        if fmt == "parquet":
            count = _write_parquet(path, rows)
        else:
            count = _write_csv(path, rows)
        results["files"].append({"path": str(path), "rows": count})
        log.info("exported %s: %s rows -> %s", key, count, path)

    results["summaries"] = [
        summary.as_dict() for summary in dataset_summaries(storage, flat)
    ]
    stale = [
        s["dataset"] for s in results["summaries"]
        if s["freshness_hours"] > 48
    ]
    results["stale_datasets"] = stale
    _write_manifest(out_dir, results)
    _write_data_dictionary(storage, out_dir, flat)
    results["retention"] = _apply_retention(out_dir, keep_per_dataset=2)
    return results


_DICTIONARY_NOTES = {
    "kline": "日K OHLC。volume/turnover 约 89% 为 None（站点多数点不提供）。"
             "extra 与列重复且时间键 t 为字符串毫秒。",
    "history": "站点小时级买卖挂单价量（metric: sell_price/buy_price/sell_num/buy_num 等）。",
    "index": "大盘指数。注意 market_index 数据集的 ts 无时区后缀（按 UTC 解释）。",
    "ws_kline": "推送通道大盘 K 线（h/d/w），extra 恒为空对象。",
    "listing": "实时新上架流。核心载荷在 extra（assetId/floatWear/fingerprint），"
               "OHLC 列仅 close 有值；item_id 为 classinfoKey。",
    "rank": "行情站A 排名快照。",
    "snapshot": "饰品快照。",
    "indicators": "派生日 K。ts 为纯日期。",
}

_USAGE_PITFALLS = [
    "ts 列已统一为 UTC ISO8601 / parquet timestamp(s,UTC)；库内历史格式可能混杂，导出层已规范化。",
    "大文件（kline 千万行级）请用 pd.read_parquet(f, columns=[...]) 选列加载，全量加载约需十 GB 级内存。",
    "extra 为 JSON 字符串，需 json.loads + pandas.json_normalize 展开；各数据集结构见上。",
    "dropna 会清空 listing（OHLC 全 None）并砍掉 kline 约 89% 行——按需选列。",
    "siteb 的 item_id 是 19 位雪花 ID（字符串）；请勿经 Excel/float 转换以免精度丢失。",
    "items.csv 的 name 多为中文名，market_hash_name 为英文市场名；分组/Join 请用 market_hash_name。",
    "导出文件名前缀即数据集组名；ws_kline 前缀避免与 kline 混淆（glob 'kline_*' 不会误选推送数据）。",
]


def _write_data_dictionary(storage: Storage, out_dir: Path, datasets: tuple[str, ...]) -> None:
    """数据字典：各数据集的 metric/platform 枚举 + 使用要点（随 manifest 更新）。"""
    dictionary: dict[str, Any] = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "usage_pitfalls": _USAGE_PITFALLS,
        "datasets": {},
    }
    try:
        with storage.read_connect() as conn:
            marks = ",".join("?" * len(datasets))
            for ds, metric, platform, n in conn.execute(
                f"""SELECT dataset, metric, platform, COUNT(*) FROM series
                WHERE dataset IN ({marks}) GROUP BY dataset, metric, platform
                ORDER BY dataset, COUNT(*) DESC""",
                list(datasets),
            ):
                entry = dictionary["datasets"].setdefault(ds, {
                    "metrics": {}, "platforms": set(), "note": "",
                })
                entry["metrics"][str(metric)] = int(n)
                entry["platforms"].add(str(platform or ""))
        for key, entry in dictionary["datasets"].items():
            entry["platforms"] = sorted(entry["platforms"])
            entry["note"] = _DICTIONARY_NOTES.get(key, "")
        for key, group in EXPORT_DATASETS.items():
            for ds in group:
                if ds in dictionary["datasets"]:
                    dictionary["datasets"][ds]["group"] = key
    except Exception:
        return
    path = out_dir / "data_dictionary.json"
    path.write_text(
        json.dumps(dictionary, ensure_ascii=False, indent=1), encoding="utf-8")


def _write_manifest(out_dir: Path, results: dict[str, Any]) -> None:
    """导出侧车清单：文件名→行数/数据集，供导出中心页展示。"""
    manifest_path = out_dir / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            manifest = {}
    entry_list = manifest.get("files")
    manifest["files"] = entry_list if isinstance(entry_list, dict) else {}
    manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
    for record in results.get("files", []):
        manifest["files"][Path(record["path"]).name] = {
            "rows": record.get("rows"),
            "exported_at": manifest["updated_at"],
        }
    # 只保留仍存在的文件条目
    existing = {f.name for f in out_dir.iterdir() if f.is_file()}
    manifest["files"] = {
        name: info for name, info in manifest["files"].items()
        if name in existing
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")


def _dataset_prefix(filename: str) -> str:
    """items_YYYYMMDDTHHMMSSZ.csv -> items（按第一个 _ 前的词分组）。"""
    return filename.split("_", 1)[0] if "_" in filename else filename


def _apply_retention(out_dir: Path, *, keep_per_dataset: int = 4) -> dict[str, int]:
    """按数据集保留最近 N 份导出；manifest.json 豁免。返回 {prefix: 删除数}。"""
    removed: dict[str, int] = {}
    files = sorted(
        (f for f in out_dir.iterdir() if f.is_file() and f.name != "manifest.json"),
        key=lambda f: f.name, reverse=True,
    )
    counts: dict[str, int] = {}
    for path in files:
        prefix = _dataset_prefix(path.name)
        counts[prefix] = counts.get(prefix, 0) + 1
        if counts[prefix] > keep_per_dataset:
            try:
                path.unlink()
                removed[prefix] = removed.get(prefix, 0) + 1
            except OSError:
                pass
    return removed


def _write_csv_rows(path: Path, rows: Iterator[dict], fieldnames: list[str]) -> int:
    count = 0
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count
