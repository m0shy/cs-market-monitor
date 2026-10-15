from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Iterable


TIME_KEYS = ("ts", "timestamp", "time", "date", "day", "datetime", "d", "t", "createTime", "updateTime")
PRICE_KEYS = ("close", "c", "price", "value", "sellPrice", "sell_price", "index", "marketIndex")
VOLUME_KEYS = ("volume", "vol", "v", "count", "transactionCount", "turnoverNumber", "turnover_number")
TURNOVER_KEYS = ("turnover", "amount", "transactionAmount", "tradeAmount")

# 纯数字文本（含小数形式，如 "1700000000123.0"）走数值解析路径
_NUMERIC_TEXT_RE = re.compile(r"\d+(?:\.\d+)?")
# ISO 基本形态：YYYY-MM-DD 或 YYYY-MM-DD[T ]HH:MM[:SS[.fff]]，可带
# Z / ±HH:MM / ±HHMM 时区后缀。零填充、合法日历值缺一不可。
_TIME_TEXT_RE = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})"
    r"(?:[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?"
    r"(Z|[+-]\d{2}:?\d{2})?)?"
)


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_time(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) or (
        isinstance(value, str) and _NUMERIC_TEXT_RE.fullmatch(value)
    ):
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000
        if number > 1_000_000_000:
            try:
                return datetime.fromtimestamp(number, timezone.utc).isoformat()
            except (OverflowError, OSError, ValueError):
                return None
        digits = str(value).split(".", 1)[0]
        if len(digits) == 8 and digits.startswith(("19", "20")):
            try:
                return datetime.strptime(digits, "%Y%m%d").date().isoformat()
            except ValueError:
                return None
        return None
    text = str(value).strip()
    if not text:
        return None
    # 非数字文本必须形如 ISO 日期/日期时间：库内 ts 的排序、去重、水位
    # 与导出过滤全部依赖字典序，"2024/01/01"、非补齐 "2024-1-1"、垃圾值
    # 一旦原样入库会让这些比较失真。不匹配则返回 None，调用方跳过该行
    # ——比存一个垃圾时间戳好。
    match = _TIME_TEXT_RE.fullmatch(text)
    if match is None:
        return None
    year, month, day, hour, minute, second = match.group(1, 2, 3, 4, 5, 6)
    try:
        datetime(int(year), int(month), int(day))
    except ValueError:
        return None
    if hour is not None and (
        int(hour) > 23
        or int(minute) > 59
        or (second is not None and int(second) > 59)
    ):
        return None
    # 空格分隔符统一为 T，保证字典序口径一致
    return text.replace(" ", "T", 1)


def find_items(payload: Any) -> list[dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            item_id = node.get("good_id") or node.get("itemId") or node.get("item_id")
            if item_id is None and node.get("id") is not None and any(
                key in node for key in ("sell_price", "yyyp_sell_price", "marketHashName", "exterior_localized_name")
            ):
                item_id = node.get("id")
            has_name = any(node.get(key) for key in ("name", "good_name", "marketHashName", "market_hash_name"))
            if item_id is not None and has_name:
                found[str(item_id)] = node
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(payload)
    return list(found.values())


def extract_series(payload: Any, *, source: str, dataset: str, item_id: str = "",
                   metric: str = "value", platform: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def parse_point(point: Any) -> dict[str, Any] | None:
        if isinstance(point, (list, tuple)) and len(point) >= 2:
            ts = normalize_time(point[0])
            close = _number(point[1])
            if ts and close is not None:
                return {"ts": ts, "close": close, "extra": {"raw": list(point)}}
        if not isinstance(point, dict):
            return None
        ts_value = next((point.get(key) for key in TIME_KEYS if point.get(key) is not None), None)
        ts = normalize_time(ts_value)
        if not ts:
            return None
        close = next((_number(point.get(key)) for key in PRICE_KEYS if _number(point.get(key)) is not None), None)
        open_value = _number(point.get("open", point.get("o")))
        high = _number(point.get("high", point.get("h")))
        low = _number(point.get("low", point.get("l")))
        volume = next((_number(point.get(key)) for key in VOLUME_KEYS if _number(point.get(key)) is not None), None)
        turnover = next((_number(point.get(key)) for key in TURNOVER_KEYS if _number(point.get(key)) is not None), None)
        if close is None and all(value is None for value in (open_value, high, low, volume, turnover)):
            return None
        return {
            "ts": ts, "open": open_value, "high": high, "low": low, "close": close,
            "volume": volume, "turnover": turnover, "extra": point,
        }

    def walk(node: Any) -> None:
        if isinstance(node, dict) and isinstance(node.get("timestamp"), list):
            timestamps = node["timestamp"]
            main = node.get("main_data") or node.get("mainData") or node.get("price") or []
            volume = node.get("num_data") or node.get("numData") or []
            parallel = []
            for index, value in enumerate(timestamps):
                ts = normalize_time(value)
                main_value = main[index] if index < len(main) else None
                if isinstance(main_value, (list, tuple)):
                    prices = [number for item in main_value if (number := _number(item)) is not None]
                    open_value = prices[0] if prices else None
                    high = max(prices) if prices else None
                    low = min(prices) if prices else None
                    close = prices[-1] if prices else None
                else:
                    open_value = high = low = None
                    close = _number(main_value)
                count = _number(volume[index]) if index < len(volume) else None
                if ts and (close is not None or count is not None):
                    parallel.append({"ts": ts, "open": open_value, "high": high, "low": low,
                                     "close": close, "volume": count, "extra": {"index": index}})
            if len(parallel) >= 2:
                rows.extend(parallel)
                return
        if isinstance(node, list):
            parsed = [parse_point(point) for point in node]
            good = [point for point in parsed if point]
            if len(good) >= 2:
                rows.extend(good)
                return
            for value in node:
                walk(value)
        elif isinstance(node, dict):
            for value in node.values():
                walk(value)

    walk(payload)
    dedup: dict[str, dict[str, Any]] = {}
    for row in rows:
        row.update({"source": source, "dataset": dataset, "item_id": item_id, "metric": metric, "platform": platform})
        dedup[row["ts"]] = row
    return list(dedup.values())


def date_bounds(rows: Iterable[dict[str, Any]]) -> tuple[str | None, str | None]:
    earliest = None
    latest = None
    for row in rows:
        value = row.get("ts")
        if not value:
            continue
        stamp = str(value)
        if earliest is None or stamp < earliest:
            earliest = stamp
        if latest is None or stamp > latest:
            latest = stamp
    return earliest, latest
