from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

from .normalize import date_bounds, extract_series, find_items, normalize_time
from .storage import Storage


ITEM_METRICS = {
    "addNum": "new_supply",
    "surviveNum": "survive_num",
    "holdersNum": "holders_num",
    "transactionCount": "transaction_count",
    "turnoverRate": "turnover_rate",
    "volumeRatio": "volume_ratio",
    "sellPrice": "sell_price",
    "buyPrice": "buy_price",
    "sellNum": "sell_num",
    "buyNum": "buy_num",
    "buff_sell_price": "sell_price",
    "buff_buy_price": "buy_price",
    "buff_sell_num": "sell_num",
    "buff_buy_num": "buy_num",
    "yyyp_sell_price": "sell_price",
    "yyyp_buy_price": "buy_price",
    "yyyp_sell_num": "sell_num",
    "yyyp_buy_num": "buy_num",
    "steam_sell_price": "sell_price",
    "steam_buy_price": "buy_price",
    "steam_sell_num": "sell_num",
    "steam_buy_num": "buy_num",
    "turnover_number": "transaction_count",
    "turnover_avg_price": "transaction_avg_price",
}


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _data(payload: Any) -> Any:
    return payload.get("data") if isinstance(payload, dict) and "data" in payload else payload


def _item_id(request_data: Any, url: str) -> str:
    if isinstance(request_data, dict):
        value = (request_data.get("good_id") or request_data.get("itemId")
                 or request_data.get("item_id") or request_data.get("vol_id")
                 or request_data.get("case_id") or request_data.get("id"))
        if value is not None:
            return str(value)
    query = parse_qs(urlparse(url).query)
    return str((query.get("id") or query.get("itemId") or [""])[0])


def _store_payload(
    storage: Storage,
    source: str,
    dataset: str,
    items: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    *,
    catalog_item_ids: list[str] | None = None,
    catalog_start_position: int = 0,
    catalog_discovered_at: str | None = None,
    catalog_universe: str = "default",
) -> tuple[int, int]:
    earliest, latest = date_bounds(rows) if rows else (None, None)
    return storage.upsert_ingest(
        source,
        dataset,
        items,
        rows,
        earliest,
        latest,
        coverage_interval=5.0,
        catalog_item_ids=catalog_item_ids,
        catalog_start_position=catalog_start_position,
        catalog_discovered_at=catalog_discovered_at,
        catalog_universe=catalog_universe,
    )


def _snapshot_rows(source: str, dataset: str, item_id: str, node: dict[str, Any],
                   platform: str = "", ts: Any = None) -> list[dict[str, Any]]:
    stamp = normalize_time(ts or node.get("updateTime") or node.get("period_at")) \
        or datetime.now(timezone.utc).isoformat()
    rows = []
    for field, metric in ITEM_METRICS.items():
        value = _number(node.get(field))
        if value is None:
            continue
        metric_platform = platform
        if field.startswith("buff_"):
            metric_platform = "buff"
        elif field.startswith("yyyp_"):
            metric_platform = "youpin"
        elif field.startswith("steam_"):
            metric_platform = "steam"
        rows.append({"source": source, "dataset": dataset, "item_id": item_id, "metric": metric,
                     "platform": metric_platform, "ts": stamp, "close": value,
                     "extra": {"field": field, "snapshot": True}})
    return rows


def _numeric_metric_rows(source: str, dataset: str, item_id: str, node: dict[str, Any],
                         ts: Any, prefix: str = "") -> list[dict[str, Any]]:
    stamp = normalize_time(ts) or datetime.now(timezone.utc).isoformat()
    rows = []
    for field, raw_value in node.items():
        value = _number(raw_value)
        if value is None:
            continue
        rows.append({"source": source, "dataset": dataset, "item_id": item_id,
                     "metric": f"{prefix}{field}", "platform": "", "ts": stamp,
                     "close": value, "extra": {"field": field}})
    return rows


def _ohlcv_rows(source: str, dataset: str, item_id: str, points: Any, *,
                metric: str, platform: str = "", extra: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    rows = []
    for point in points if isinstance(points, list) else []:
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            continue
        ts = normalize_time(point[0])
        close = _number(point[2] if len(point) > 2 else point[1])
        if not ts or close is None:
            continue
        rows.append({
            "source": source,
            "dataset": dataset,
            "item_id": item_id,
            "metric": metric,
            "platform": platform,
            "ts": ts,
            "open": _number(point[1]) if len(point) > 1 else None,
            "high": _number(point[3]) if len(point) > 3 else None,
            "low": _number(point[4]) if len(point) > 4 else None,
            "close": close,
            "volume": _number(point[5]) if len(point) > 5 else None,
            "turnover": _number(point[6]) if len(point) > 6 else None,
            "extra": {**(extra or {}), "raw": list(point)},
        })
    return rows


def _sector_id(node: dict[str, Any], request_data: Any = None) -> str:
    request = request_data if isinstance(request_data, dict) else {}
    node_type = node.get("type") or request.get("type") or "BROAD"
    level = node.get("level")
    if level is None:
        level = request.get("level", 0)
    type_value = node.get("typeVal")
    if type_value in (None, ""):
        type_value = request.get("typeVal") or "root"
    return f"{node_type}:{level}:{type_value}"


def ingest_payload(
    storage: Storage,
    source: str,
    url: str,
    payload: Any,
    request_data: Any = None,
    *,
    catalog_start_position: int | None = None,
    catalog_discovered_at: str | None = None,
    catalog_universe: str = "default",
) -> tuple[int, int]:
    """Normalize known public-page responses while raw captures retain every field."""
    path = urlparse(url).path
    dataset = path.strip("/").replace("/", ".") or "homepage"
    item_id = _item_id(request_data, url)
    rows: list[dict[str, Any]] = []
    data = _data(payload)
    if (
        source == "siteb"
        and path.endswith("/skin/market/v3/page")
        and isinstance(data, dict)
    ):
        discovered_items = [
            item for item in data.get("list") or [] if isinstance(item, dict)
        ]
    else:
        discovered_items = list(find_items(payload))
    catalog_item_ids: list[str] | None = None

    if source == "siteb" and path.endswith("/open/cs2/v1/price/single") and isinstance(data, list):
        stamp = datetime.now(timezone.utc).isoformat()
        for market in data:
            if not isinstance(market, dict):
                continue
            rows.extend(_snapshot_rows(source, "platform_snapshot", item_id, {
                "sellPrice": market.get("sellPrice"),
                "buyPrice": market.get("biddingPrice"),
                "sellNum": market.get("sellCount"),
                "buyNum": market.get("biddingCount"),
            }, str(market.get("platform") or "").lower(), market.get("updateTime") or stamp))
    elif source == "siteb" and path.endswith("/open/cs2/v1/price/avg") and isinstance(data, dict):
        stamp = datetime.now(timezone.utc).isoformat()
        overall = _number(data.get("avgPrice"))
        for market in data.get("dataList") or []:
            if not isinstance(market, dict):
                continue
            rows.append({"source": source, "dataset": "platform_avg", "item_id": item_id,
                         "metric": "avg_price_7d",
                         "platform": str(market.get("platform") or "").lower(),
                         "ts": stamp, "close": _number(market.get("avgPrice")),
                         "extra": {"overallAvg": overall, "official_open_api": True}})
        if overall is not None:
            rows.append({"source": source, "dataset": "platform_avg", "item_id": item_id,
                         "metric": "avg_price_7d", "platform": "all", "ts": stamp,
                         "close": overall, "extra": {"official_open_api": True}})
    elif source == "siteb" and path.endswith("/open/cs2/item/v1/kline") and isinstance(data, list):
        platform = str(request_data.get("specialStyle") or request_data.get("platform") or "all").lower() \
            if isinstance(request_data, dict) else "all"
        for point in data:
            if not isinstance(point, list) or len(point) < 5:
                continue
            ts = normalize_time(point[0])
            if not ts:
                continue
            # Open API tuple order is [ts, open, high, low, close] (no volume).
            rows.append({"source": source, "dataset": "item_kline", "item_id": item_id,
                         "metric": "ohlc", "platform": platform.lower(), "ts": ts,
                         "open": _number(point[1]), "high": _number(point[2]),
                         "low": _number(point[3]), "close": _number(point[4]),
                         "extra": {"raw": point, "official_open_api": True}})
    elif source == "siteb" and path.endswith("/open/cs2/broad/v1/kline") and isinstance(data, list):
        for point in data:
            if not isinstance(point, list) or len(point) < 5:
                continue
            ts = normalize_time(point[0])
            if ts:
                rows.append({"source": source, "dataset": "market_kline", "item_id": "broad",
                             "metric": "ohlc", "platform": "", "ts": ts,
                             "open": _number(point[1]), "high": _number(point[2]),
                             "low": _number(point[3]), "close": _number(point[4]),
                             "extra": {"raw": point, "official_open_api": True}})
    elif source == "siteb" and path.endswith("/open/cs2/broad/v1/index") and isinstance(data, dict):
        stamp = datetime.now(timezone.utc).isoformat()
        rows = _numeric_metric_rows(source, "market_snapshot", "broad", {
            "broadMarketIndex": data.get("broadMarketIndex"),
            "diffYesterday": data.get("diffYesterday"),
            "diffYesterdayRatio": data.get("diffYesterdayRatio"),
        }, data.get("updateTime") or stamp)
        rows.extend(extract_series(data.get("historyMarketIndexList") or [], source=source,
                                   dataset="market_index", item_id="broad",
                                   metric="market_index"))

    elif source == "sitea" and path.endswith("/processEnv.json") and isinstance(data, dict):
        storage.upsert_metadata(source, "process_env", data)
    elif source == "sitea" and path.endswith("/currentUser") and isinstance(data, dict):
        # The public response contains site version/help-group information, not
        # an authenticated user profile when the page is logged out.
        storage.upsert_metadata(source, "current_user_public", data)
    elif source == "sitea" and path.endswith("/feed/get_feed_list") and isinstance(data, list):
        storage.upsert_metadata(source, "feed.latest", data)
        for index, entry in enumerate(data):
            if isinstance(entry, dict):
                storage.upsert_metadata(source, f"feed.{index}", entry)
        rows = _numeric_metric_rows(
            source, "feed_snapshot", "", {"count": len(data)},
            datetime.now(timezone.utc).isoformat(),
        )
    elif source == "sitea" and path.endswith("/info/get_page_list") and isinstance(data, dict):
        page_number = str(data.get("current_page") or "0")
        storage.upsert_metadata(source, f"catalog_page.{page_number}", data)
        page_items = data.get("data") or []
        for entry in page_items:
            if isinstance(entry, dict):
                actual_id = str(entry.get("id") or entry.get("good_id") or "")
                rows.extend(_snapshot_rows(
                    source, "catalog_snapshot", actual_id, entry,
                    ts=entry.get("updated_at") or entry.get("created_at"),
                ))
    elif source == "sitea" and path.endswith("/v1/current_data") and isinstance(data, dict):
        stamp = datetime.now(timezone.utc).isoformat()
        for index in data.get("sub_index_data") or []:
            if not isinstance(index, dict):
                continue
            actual_id = str(index.get("name_key") or index.get("id") or "")
            ts = normalize_time(index.get("updated_at")) or stamp
            market_value = _number(index.get("market_index"))
            if market_value is not None:
                rows.append({"source": source, "dataset": "market_index", "item_id": actual_id,
                             "metric": "market_index", "platform": "", "ts": ts,
                             "open": _number(index.get("open")), "high": _number(index.get("high")),
                             "low": _number(index.get("low")), "close": market_value,
                             "extra": {"name": index.get("name"), "id": index.get("id")}})
            rows.extend(_numeric_metric_rows(source, "market_index_snapshot", actual_id, {
                "change": index.get("chg_num"), "change_rate": index.get("chg_rate"),
            }, ts))
        for group, id_field in (("chg_type_data", "name"), ("chg_price_data", "xKey")):
            for node in data.get(group) or []:
                if isinstance(node, dict):
                    rows.extend(_numeric_metric_rows(
                        source, "market_category_snapshot", str(node.get(id_field) or ""), node, stamp,
                        prefix=f"{group}.",
                    ))
        if isinstance(data.get("rate_data"), dict):
            rows.extend(_numeric_metric_rows(source, "market_breadth_snapshot", "",
                                              data["rate_data"], stamp))
        if isinstance(data.get("online_number"), dict):
            rows.extend(_numeric_metric_rows(source, "player_snapshot", "",
                                              data["online_number"],
                                              data["online_number"].get("created_at") or stamp))
        for point in data.get("online_chart") or []:
            if isinstance(point, dict):
                ts, value = normalize_time(point.get("d")), _number(point.get("m"))
                if ts and value is not None:
                    rows.append({"source": source, "dataset": "player_history", "item_id": "",
                                 "metric": "online_players", "platform": "", "ts": ts,
                                 "close": value, "extra": {}})
        rows.extend(extract_series(data.get("greedy") or [], source=source,
                                   dataset="sentiment_history", metric="greedy_index"))
        for node in data.get("alteration") or []:
            if isinstance(node, dict):
                rows.extend(_numeric_metric_rows(source, "item_alteration_snapshot",
                                                  str(node.get("good_id") or ""), node,
                                                  node.get("created_at") or stamp))
        for node in data.get("view_count") or []:
            if isinstance(node, dict):
                rows.extend(_numeric_metric_rows(source, "item_rank_snapshot",
                                                  str(node.get("id") or ""), node, stamp))
        for point in data.get("card_price") or []:
            if isinstance(point, dict):
                ts, value = normalize_time(point.get("created_at")), _number(point.get("card_price"))
                if ts and value is not None:
                    rows.append({"source": source, "dataset": "card_price_history", "item_id": "",
                                 "metric": "card_price", "platform": "", "ts": ts,
                                 "close": value, "extra": {}})
    elif source == "sitea" and path.endswith("/v1/sub_data") and isinstance(data, dict):
        query = parse_qs(urlparse(url).query)
        actual_id = str((query.get("id") or [item_id])[0])
        timestamps = data.get("timestamp") or []
        values = data.get("main_data") or []
        for raw_ts, point in zip(timestamps, values):
            ts = normalize_time(raw_ts)
            if not ts or not isinstance(point, list):
                continue
            for index, metric in ((0, "market_index"), (1, "change"), (2, "change_rate")):
                value = _number(point[index]) if index < len(point) else None
                if value is not None:
                    rows.append({"source": source, "dataset": "market_index_history",
                                 "item_id": actual_id, "metric": metric, "platform": "",
                                 "ts": ts, "close": value, "extra": {"column": index}})
        if isinstance(data.get("count"), dict):
            rows.extend(_numeric_metric_rows(source, "market_index_snapshot", actual_id,
                                              data["count"], datetime.now(timezone.utc).isoformat()))
    elif source == "sitea" and path.endswith("/info/exchange_detail") and isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            actual_id = str(item.get("id") or item.get("good_id") or "")
            rows.extend(_snapshot_rows(source, "exchange_snapshot", actual_id, item))
            rows.extend(_numeric_metric_rows(source, "exchange_snapshot", actual_id, {
                "max_price": item.get("max_price"),
            }, datetime.now(timezone.utc).isoformat()))
    elif source == "sitea" and path.endswith("/info/get_rank_list") and isinstance(data, dict):
        stamp = datetime.now(timezone.utc).isoformat()
        for item in data.get("data") or []:
            if not isinstance(item, dict):
                continue
            actual_id = str(item.get("id") or item.get("good_id") or "")
            rows.extend(_snapshot_rows(source, "item_rank_snapshot", actual_id, item,
                                        ts=item.get("created_at") or stamp))
            rows.extend(_numeric_metric_rows(source, "item_rank_snapshot", actual_id, {
                key: value for key, value in item.items()
                if key.startswith(("sell_price_", "sell_price_rate_", "rank_num"))
            }, item.get("created_at") or stamp))
    elif source == "sitea" and path.endswith("/info/get_series_list") and isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            actual_id = str(item.get("id") or item.get("key") or "")
            rows.extend(_numeric_metric_rows(source, "series_snapshot", actual_id, {
                key: value for key, value in item.items()
                if key.startswith("sell_price_") or key in {"amount", "total_value"}
            }, item.get("created_at")))
    elif source == "sitea" and path.endswith("/info/roi") and isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                rows.extend(_numeric_metric_rows(source, "roi_snapshot",
                                                  str(item.get("good_id") or item.get("id") or ""), {
                                                      "num": item.get("num"), "price": item.get("price"),
                                                      "roi": item.get("roi"), "income": item.get("income"),
                                                  }, item.get("updated_at") or item.get("created_at")))
    elif source == "sitea" and path.endswith("/info/vol_data_info") and isinstance(data, list):
        # /vol page: per-case trading volume/amount summary (成交额、成交量).
        stamp = datetime.now(timezone.utc).isoformat()
        for entry in data:
            if not isinstance(entry, dict):
                continue
            actual_id = str(entry.get("good_id") or entry.get("id") or "")
            rows.extend(_numeric_metric_rows(source, "vol_snapshot", actual_id, {
                "statistic": entry.get("statistic"),
                "avg_price": entry.get("avg_price"),
                "sum_price": entry.get("sum_price"),
                "special": entry.get("special"),
            }, entry.get("updated_at") or stamp))
    elif source == "sitea" and path.endswith("/monitor/get_task_stat") and isinstance(data, dict):
        rows = _numeric_metric_rows(source, "monitor_snapshot", "", data,
                                    datetime.now(timezone.utc).isoformat())
    elif source == "sitea" and path.endswith("/monitor/get_task_trends") and isinstance(data, list):
        stamp = datetime.now(timezone.utc).isoformat()
        for group in data:
            for event in group if isinstance(group, list) else []:
                if not isinstance(event, dict):
                    continue
                info = event.get("info") if isinstance(event.get("info"), dict) else {}
                actual_id = str(info.get("good_id") or "")
                rows.extend(_numeric_metric_rows(source, "monitor_event", actual_id, {
                    "count": event.get("count"), "type": event.get("type"),
                    "price_sum": event.get("price_sum"), "num": info.get("num"),
                    "sell_price": info.get("yyyp_sell_price"),
                }, info.get("created_at") or stamp))
    elif source == "sitea" and path.endswith("/monitor/rank") and isinstance(data, list):
        stamp = datetime.now(timezone.utc).isoformat()
        for owner in data:
            if isinstance(owner, dict):
                rows.extend(_numeric_metric_rows(source, "monitor_owner_snapshot",
                                                  str(owner.get("steam_id") or owner.get("id") or ""),
                                                  {"num": owner.get("num")}, stamp))
    elif source == "sitea" and path.endswith("/info/chart") and isinstance(request_data, dict):
        metric = str(request_data.get("key") or "value")
        platform = str(request_data.get("platform") or "")
        rows = extract_series(payload, source=source, dataset="item_history", item_id=item_id,
                              metric=metric, platform=platform)
    elif source == "sitea" and path.endswith("/info/simple/chartAll"):
        platform = str(request_data.get("plat") or "") if isinstance(request_data, dict) else ""
        rows = extract_series(payload, source=source, dataset="item_kline", item_id=item_id,
                              metric="ohlc", platform=platform)
    elif source == "sitea" and path.endswith("/info/good") and isinstance(data, dict):
        info = data.get("goods_info") or {}
        actual_id = str(info.get("id") or item_id)
        rows = _snapshot_rows(source, "item_snapshot", actual_id, info)
        storage.upsert_metadata(source, f"good.{actual_id}", {
            "goods_info": info,
            "button_list": data.get("button_list") or [],
            "statistic_list": data.get("statistic_list") or [],
            "dpl": data.get("dpl") or [],
            "container": data.get("container") or [],
        })
        rows.extend(extract_series(
            info.get("trendList") or [], source=source,
            dataset="item_snapshot_history", item_id=actual_id,
            metric="sell_price",
        ))

    elif source == "sitea" and path.endswith("/v1/sub/kline") and isinstance(data, list):
        # Sub-index K-line (index board OHLCV). item_id = sub index id, platform = period.
        query = parse_qs(urlparse(url).query)
        actual_id = str((query.get("id") or [item_id])[0])
        period = str((query.get("type") or ["1day"])[0])
        rows = extract_series(payload, source=source, dataset="sub_kline", item_id=actual_id,
                              metric="ohlc", platform=period)
    elif source == "sitea" and path.endswith("/info/vol_data_detail") and isinstance(data, dict):
        # Per-case daily trading volume / average price / turnover history.
        stamp = datetime.now(timezone.utc).isoformat()
        for point in data.get("chart") or []:
            if not isinstance(point, dict):
                continue
            ts = normalize_time(point.get("updated_at")) or stamp
            for metric, field in (("statistic", "statistic"), ("avg_price", "avg_price"),
                                  ("sum_price", "sum_price")):
                value = _number(point.get(field))
                if value is not None:
                    rows.append({"source": source, "dataset": "vol_history", "item_id": item_id,
                                 "metric": metric, "platform": "", "ts": ts, "close": value,
                                 "extra": {"field": field, "vol_id": item_id}})
    elif source == "sitea" and path.endswith("/v1/stat/case") and isinstance(data, list):
        # Case open-count summary (today / week / month / all-time).
        stamp = datetime.now(timezone.utc).isoformat()
        for entry in data:
            if not isinstance(entry, dict):
                continue
            case_id = str(entry.get("case_id") or "")
            ts = normalize_time(entry.get("created_at")) or stamp
            for metric in ("daily", "weekly", "monthly", "total"):
                value = _number(entry.get(metric))
                if value is not None:
                    rows.append({"source": source, "dataset": "case_snapshot", "item_id": case_id,
                                 "metric": metric, "platform": "", "ts": ts, "close": value,
                                 "extra": {"type": entry.get("type"), "good_id": entry.get("good_id")}})
            storage.upsert_metadata(source, "case." + case_id, {
                key: entry.get(key) for key in ("case_id", "cn_name", "img", "type",
                                                "good_id", "ground_at", "created_at")
            })
    elif source == "sitea" and path.endswith("/v1/stat/case/chart") and isinstance(data, list):
        # Daily open-count history; the endpoint may repeat dates per group, keep every point.
        for seq, point in enumerate(data):
            if not isinstance(point, dict):
                continue
            ts = normalize_time(point.get("date"))
            value = _number(point.get("daily"))
            if ts and value is not None:
                rows.append({"source": source, "dataset": "case_history", "item_id": item_id,
                             "metric": "daily", "platform": "seq%d" % seq, "ts": ts,
                             "close": value, "extra": {"index": seq}})
    elif source == "sitea" and path.endswith("/info/good/statistic") and isinstance(data, list):
        # Item survival count (item count in existence) over ~180 days.
        query = parse_qs(urlparse(url).query)
        actual_id = str((query.get("id") or [item_id])[0])
        for point in data:
            if not isinstance(point, dict):
                continue
            ts = normalize_time(point.get("created_at"))
            value = _number(point.get("statistic"))
            if ts and value is not None:
                rows.append({"source": source, "dataset": "good_statistic", "item_id": actual_id,
                             "metric": "statistic", "platform": "", "ts": ts, "close": value,
                             "extra": {}})
    elif source == "sitea" and path.endswith("/info/roi_detail") and isinstance(data, list):
        # Per-case expected income / ROI time series.
        query = parse_qs(urlparse(url).query)
        actual_id = str((query.get("id") or [item_id])[0])
        for point in data:
            if not isinstance(point, dict):
                continue
            ts = normalize_time(point.get("date"))
            if not ts:
                continue
            for metric, field in (("income", "income"), ("roi", "roi")):
                value = _number(point.get(field))
                if value is not None:
                    rows.append({"source": source, "dataset": "roi_history", "item_id": actual_id,
                                 "metric": metric, "platform": "", "ts": ts, "close": value,
                                 "extra": {}})
    elif source == "sitea" and path.endswith("/info/container_data_info") and isinstance(data, list):
        # Weapon cases / collections catalogue; payload kept as metadata.
        stamp = datetime.now(timezone.utc).isoformat()
        for entry in data:
            if not isinstance(entry, dict):
                continue
            container_id = str(entry.get("id") or "")
            storage.upsert_metadata(source, "container." + container_id, {
                key: entry.get(key) for key in ("id", "img", "name", "comment", "created_at")
            })
            rows.append({"source": source, "dataset": "container_snapshot", "item_id": container_id,
                         "metric": "listed", "platform": "",
                         "ts": normalize_time(entry.get("created_at")) or stamp,
                         "close": 1.0, "extra": {"comment": entry.get("comment")}})
    elif source == "sitea" and path.endswith("/info/good/container_detail") and isinstance(data, list):
        # Items contained in a case / collection, with current price.
        query = parse_qs(urlparse(url).query)
        container_id = str((query.get("id") or [""])[0])
        stamp = datetime.now(timezone.utc).isoformat()
        for entry in data:
            if not isinstance(entry, dict):
                continue
            good_id = str(entry.get("id") or "")
            rows.append({"source": source, "dataset": "container_item_snapshot", "item_id": good_id,
                         "metric": "price", "platform": "container_" + container_id, "ts": stamp,
                         "close": _number(entry.get("price")),
                         "extra": {"container_id": container_id, "rln": entry.get("rln"),
                                   "qln": entry.get("qln"), "short_name": entry.get("short_name")}})
            discovered_items.append({
                "id": entry.get("id"), "name": entry.get("short_name"),
                "market_hash_name": entry.get("market_hash_name"),
            })
    elif source == "sitea" and path.endswith("/info/get_good_id") and isinstance(data, dict):
        # Paginated good_id catalogue; ids are ingested by find_items at the top.
        mapping = data.get("data") or {}
        if isinstance(mapping, dict):
            discovered_items.extend([
                node for node in mapping.values() if isinstance(node, dict)
            ])
        storage.upsert_metadata(source, "good_id_catalog", {
            "total": data.get("total"), "page_index": data.get("page_index"),
            "page_size": data.get("page_size"),
        })
    elif source == "sitea" and path.endswith("/search/suggest") and isinstance(data, list):
        discovered_items.extend([
            {"id": entry.get("id"), "name": entry.get("value")}
            for entry in data if isinstance(entry, dict) and entry.get("id") is not None and entry.get("value")
        ])
    elif source == "siteb" and path.endswith("/steam/type-trend/v2/item/details") and isinstance(data, list):
        mapping = ((1, "sell_price"), (2, "sell_num"), (3, "buy_price"), (4, "buy_num"),
                   (5, "transaction_avg_price"), (6, "transaction_count"), (7, "survive_num"))
        style = ""
        if isinstance(request_data, dict) and request_data.get("specialStyle"):
            style = str(request_data["specialStyle"]).lower()
        request_data = request_data if isinstance(request_data, dict) else {}
        platform = style or str(request_data.get("platform") or "all").lower()
        # 增量水位（SSD 省损核心）：type-trend 每次返回全量 ~8760 行，其中
        # 99% 与库内重复；SQLite 对同值 UPDATE 仍写页。已有 ≥1 点的对象只
        # 写比现有最新点更新的行，单次刷新写入量降两个数量级。
        watermark = storage.series_watermark(
            source, "item_history", item_id, platform,
        ) if storage is not None else ""
        for point in data:
            if not isinstance(point, list) or not point:
                continue
            ts = normalize_time(point[0])
            if watermark and ts and str(ts) <= watermark:
                continue
            for index, metric in mapping:
                value = _number(point[index]) if index < len(point) else None
                if ts and value is not None:
                    rows.append({"source": source, "dataset": "item_history", "item_id": item_id,
                                 "metric": metric, "platform": platform, "ts": ts, "close": value,
                                 "extra": {"column": index, "style": style}})
    elif source == "siteb" and path.endswith("/steam/category/v1/kline") and isinstance(data, list):
        query = parse_qs(urlparse(url).query)
        request_data = request_data if isinstance(request_data, dict) else {}
        actual_id = str(request_data.get("typeVal") or (query.get("typeVal") or [item_id])[0])
        platform = str(request_data.get("platform") or (query.get("platform") or [""])[0]).lower()
        # 增量水位（SSD 省损）：仅对"刷新"请求（无 maxTime=向后翻页）生效；
        # 回填请求带 maxTime、页内点全部早于头部，不能过滤。
        is_backfill = bool(request_data.get("maxTime") or query.get("maxTime"))
        kline_watermark = "" if is_backfill else (
            storage.series_watermark(source, "item_kline", actual_id, platform)
            if storage is not None else ""
        )
        for point in data:
            if not isinstance(point, list) or len(point) < 5:
                continue
            ts = normalize_time(point[0])
            if kline_watermark and ts and str(ts) <= kline_watermark:
                continue
            if not ts:
                continue
            rows.append({"source": source, "dataset": "item_kline", "item_id": actual_id,
                         "metric": "ohlc", "platform": platform, "ts": ts,
                         "open": _number(point[1]), "close": _number(point[2]),
                         "high": _number(point[3]), "low": _number(point[4]),
                         "volume": _number(point[5]) if len(point) > 5 else None,
                         "turnover": _number(point[6]) if len(point) > 6 else None,
                         "extra": {"raw": point}})
    elif source == "siteb" and path.endswith("/skin/v1/market-comparsion") and isinstance(data, dict):
        query = parse_qs(urlparse(url).query)
        request_data = request_data if isinstance(request_data, dict) else {}
        actual_id = str((query.get("itemId") or [None])[0] or request_data.get("itemId") or item_id)
        for market in data.get("platformMarket") or []:
            platform = str(market.get("platform") or "")
            for point in market.get("trendList") or []:
                if not isinstance(point, list) or len(point) < 2:
                    continue
                ts = normalize_time(point[0])
                for index, metric in ((1, "sell_price"), (2, "buy_price")):
                    value = _number(point[index]) if index < len(point) else None
                    if ts and value is not None:
                        rows.append({"source": source, "dataset": "platform_history", "item_id": actual_id,
                                     "metric": metric, "platform": platform, "ts": ts, "close": value,
                                     "extra": {}})
            rows.extend(_snapshot_rows(source, "platform_snapshot", actual_id, {
                "sellPrice": market.get("sellPrice"), "buyPrice": market.get("biddingPrice"),
                "sellNum": market.get("sellCount"), "buyNum": market.get("biddingCount"),
            }, platform, market.get("updateTime")))
    elif source == "siteb" and path.endswith("/skin/v1/item") and isinstance(data, dict):
        actual_id = str(data.get("itemId") or data.get("id") or item_id)
        rows = _snapshot_rows(source, "item_snapshot", actual_id, data)
        rows.extend(extract_series(
            data.get("trendList") or [], source=source,
            dataset="item_snapshot_history", item_id=actual_id,
            metric="sell_price",
        ))
        for market in data.get("sellingPriceList") or []:
            value = _number(market.get("price"))
            ts = normalize_time(market.get("updateTime"))
            if value is not None and ts:
                rows.append({"source": source, "dataset": "platform_snapshot", "item_id": actual_id,
                             "metric": "sell_price", "platform": str(market.get("platform") or ""),
                             "ts": ts, "close": value, "extra": {}})
    elif source == "siteb" and path.endswith("/index/statistics/v1/summary") and isinstance(data, dict):
        rows = extract_series(data.get("historyMarketIndexList") or [], source=source,
                              dataset="market_index", metric="market_index")
        stamp = normalize_time(data.get("updateTime")) or datetime.now(timezone.utc).isoformat()
        rows.extend(_numeric_metric_rows(source, "market_snapshot", "", {
            field: data.get(field) for field in (
                "broadMarketIndex", "diffYesterday", "diffYesterdayRatio", "surviveNum",
                "holdersNum", "riseFallDays",
            )
        }, stamp))
        for label in ("todayStatistics", "yesterdayStatistics"):
            if isinstance(data.get(label), dict):
                rows.extend(_numeric_metric_rows(source, "market_snapshot", "", data[label], stamp,
                                                  prefix=f"{label}."))
        trend = data.get("transPerformanceTrend")
        if isinstance(trend, dict):
            rows.extend(_numeric_metric_rows(source, "market_performance_snapshot", "", trend, stamp))
            rows.extend(extract_series(trend.get("history") or [], source=source,
                                       dataset="market_performance_history", metric="performance_score"))
    elif source == "siteb" and path.endswith("/index/players/v1/statistics") and isinstance(data, dict):
        stamp = normalize_time(data.get("updateTime")) or datetime.now(timezone.utc).isoformat()
        snapshot = {key: value for key, value in data.items()
                    if key not in {"history", "allHistory", "updateTime"}}
        rows = _numeric_metric_rows(source, "player_snapshot", "", snapshot, stamp)
        rows.extend(extract_series(data.get("history") or [], source=source,
                                   dataset="player_history", metric="online_players"))
        for point in data.get("allHistory") or []:
            if not isinstance(point, list) or not point:
                continue
            ts = normalize_time(point[0])
            for index, metric in ((1, "daily_average"), (2, "daily_peak")):
                value = _number(point[index]) if index < len(point) else None
                if ts and value is not None:
                    rows.append({"source": source, "dataset": "player_history", "item_id": "",
                                 "metric": metric, "platform": "", "ts": ts,
                                 "close": value, "extra": {"column": index}})
    elif source == "siteb" and path.endswith("/index/players/trend/v1/all") and isinstance(data, list):
        for point in data:
            if not isinstance(point, list) or not point:
                continue
            ts = normalize_time(point[0])
            for index, metric in ((1, "daily_average"), (2, "daily_peak")):
                value = _number(point[index]) if index < len(point) else None
                if ts and value is not None:
                    rows.append({"source": source, "dataset": "player_history", "item_id": "",
                                 "metric": metric, "platform": "", "ts": ts,
                                 "close": value, "extra": {"column": index}})
    elif source == "siteb" and path.endswith("/index/trend/v1/summary") and isinstance(data, dict):
        stamp = datetime.now(timezone.utc).isoformat()
        storage.upsert_metadata(source, "market_trend_summary", data)
        market = data.get("marketIndexTrend")
        if isinstance(market, dict):
            rows.extend(_numeric_metric_rows(
                source, "market_trend_snapshot", "", {
                    key: value for key, value in market.items()
                    if key != "historyMarketIndexList"
                }, stamp,
            ))
            rows.extend(extract_series(
                market.get("historyMarketIndexList") or [],
                source=source, dataset="market_index", metric="market_index",
            ))
        performance = data.get("transPerformanceTrend")
        if isinstance(performance, dict):
            rows.extend(_numeric_metric_rows(
                source, "market_performance_snapshot", "", {
                    key: value for key, value in performance.items()
                    if key != "history"
                }, stamp,
            ))
            rows.extend(extract_series(
                performance.get("history") or [],
                source=source, dataset="market_performance_history",
                metric="performance_score",
            ))
        players = data.get("onlinePlayerTrend")
        if isinstance(players, dict):
            rows.extend(_numeric_metric_rows(
                source, "player_snapshot", "", {
                    key: value for key, value in players.items()
                    if key not in {"history", "allHistory", "updateTime"}
                }, players.get("updateTime") or stamp,
            ))
            rows.extend(extract_series(
                players.get("history") or [],
                source=source, dataset="player_history", metric="online_players",
            ))
    elif source == "siteb" and path.endswith("/index/transaction/trend/v1/all") and isinstance(data, list):
        for point in data:
            if not isinstance(point, (list, tuple)) or not point:
                continue
            ts = normalize_time(point[0])
            if not ts:
                continue
            for index, metric in ((1, "transaction_amount"), (2, "transaction_count")):
                value = _number(point[index]) if index < len(point) else None
                if value is not None:
                    rows.append({
                        "source": source,
                        "dataset": "market_transaction_history",
                        "item_id": "",
                        "metric": metric,
                        "platform": "",
                        "ts": ts,
                        "close": value,
                        "extra": {"column": index},
                    })
    elif source == "siteb" and path.endswith("/statistics/v2/chart") and isinstance(data, list):
        request = request_data if isinstance(request_data, dict) else {}
        rows.extend(extract_series(
            data,
            source=source,
            dataset="market_chart",
            metric=f"type_{request.get('type', 'unknown')}",
            platform=str(request.get("dateType") or ""),
        ))
    elif source == "siteb" and path.endswith("/item/block/v1/summary") and isinstance(data, dict):
        actual_id = _sector_id(data, request_data)
        stamp = data.get("updateTime") or datetime.now(timezone.utc).isoformat()
        rows.extend(_numeric_metric_rows(
            source,
            "sector_summary",
            actual_id,
            {
                key: value for key, value in data.items()
                if key not in {"platforms", "blockLevel", "updateTime"}
            },
            stamp,
        ))
        for platform in data.get("platforms") or []:
            if not isinstance(platform, dict):
                continue
            rows.extend(_numeric_metric_rows(
                source,
                "sector_platform_snapshot",
                actual_id,
                platform,
                stamp,
                prefix=f"{str(platform.get('platform') or 'all').lower()}.",
            ))
        storage.upsert_metadata(source, f"sector_summary.{actual_id}", data)
    elif source == "siteb" and path.endswith("/item/block/v1/next-level") and isinstance(data, list):
        request = request_data if isinstance(request_data, dict) else {}
        parent_id = _sector_id({}, request)
        storage.upsert_metadata(source, f"sector_children.{parent_id}", data)
        for node in data:
            if not isinstance(node, dict):
                continue
            actual_id = _sector_id(node, request)
            stamp = datetime.now(timezone.utc).isoformat()
            rows.extend(_numeric_metric_rows(
                source,
                "sector_node_snapshot",
                actual_id,
                {
                    "index": node.get("index"),
                    "riseFallDiff": node.get("riseFallDiff"),
                    "riseFallRate": node.get("riseFallRate"),
                    "level": node.get("level"),
                },
                stamp,
            ))
            rows.extend(extract_series(
                node.get("trendList") or [],
                source=source,
                dataset="sector_index_history",
                item_id=actual_id,
                metric="market_index",
                platform=str(node.get("platform") or request.get("platform") or "ALL").lower(),
            ))
    elif source == "siteb" and path.endswith("/item/block/v1/relation") and isinstance(data, list):
        request = request_data if isinstance(request_data, dict) else {}
        parent_id = _sector_id({}, request)
        storage.upsert_metadata(source, f"sector_relation.{parent_id}", data)
        for node in data:
            if not isinstance(node, dict):
                continue
            actual_id = _sector_id(node, request)
            stamp = datetime.now(timezone.utc).isoformat()
            rows.extend(_numeric_metric_rows(
                source,
                "sector_relation_snapshot",
                actual_id,
                {
                    "index": node.get("index"),
                    "riseFallDiff": node.get("riseFallDiff"),
                    "riseFallRate": node.get("riseFallRate"),
                    "level": node.get("level"),
                },
                stamp,
            ))
            rows.extend(extract_series(
                node.get("trendList") or [],
                source=source,
                dataset="sector_relation_history",
                item_id=actual_id,
                metric="market_index",
                platform=str(request.get("platform") or "ALL").lower(),
            ))
    elif source == "siteb" and path.endswith("/item/block/v1/kline") and isinstance(data, list):
        request = request_data if isinstance(request_data, dict) else {}
        actual_id = _sector_id({}, request)
        rows.extend(_ohlcv_rows(
            source,
            "sector_kline",
            actual_id,
            data,
            metric="market_index",
            platform=f"{str(request.get('platform') or 'ALL').lower()}.k{request.get('klineType', '')}",
            extra={
                "type": request.get("type"),
                "level": request.get("level"),
                "typeVal": request.get("typeVal"),
                "klineType": request.get("klineType"),
            },
        ))
    elif source == "siteb" and path.endswith("/index/item-block/v1/summary") and isinstance(data, dict):
        stamp = datetime.now(timezone.utc).isoformat()
        for group_name, group in data.items():
            if not isinstance(group, dict):
                continue
            for list_name, entries in group.items():
                for entry in entries if isinstance(entries, list) else []:
                    if not isinstance(entry, dict):
                        continue
                    actual_id = str(entry.get("typeVal") or "")
                    rows.extend(_numeric_metric_rows(source, "sector_snapshot", actual_id, {
                        "index": entry.get("index"), "change_rate": entry.get("riseFallRate"),
                        "change": entry.get("riseFallDiff"), "level": entry.get("level"),
                    }, stamp, prefix=f"{group_name}.{list_name}."))
    elif source == "siteb" and (
        path.endswith("/index/item/change/v1/list")
        or path.endswith("/index/item/change/v1/lately")
    ) and isinstance(data, dict):
        for item in data.get("list") or []:
            if not isinstance(item, dict):
                continue
            actual_id = str(item.get("itemId") or "")
            rows.extend(_numeric_metric_rows(source, "item_change_event", actual_id, {
                "before_sell_price": item.get("beforeSellPrice"), "sell_price": item.get("sellPrice"),
                "before_sell_num": item.get("beforeSellCount"), "sell_num": item.get("sellCount"),
                "before_buy_price": item.get("beforePurchasePrice"), "buy_price": item.get("purchasePrice"),
                "before_buy_num": item.get("beforePurchaseCount"), "buy_num": item.get("purchaseCount"),
                "change_type": item.get("changeType"), "change_target": item.get("changeTarget"),
                "before_change_target": item.get("beforeChangeTarget"),
            }, item.get("changeTime"), prefix=f"{str(item.get('platform') or '').lower()}.") )
    elif source == "siteb" and path.endswith("/index/skin-folder/v1/hot") and isinstance(data, list):
        for folder in data:
            if not isinstance(folder, dict):
                continue
            folder_id = str(folder.get("folderId") or "")
            trend = folder.get("trend") if isinstance(folder.get("trend"), dict) else {}
            stamp = normalize_time(folder.get("modifyTime")) or datetime.now(timezone.utc).isoformat()
            rows.extend(_numeric_metric_rows(source, "folder_snapshot", folder_id, {
                "folder_count": folder.get("folderCount"), "hot_count": folder.get("hotCount"),
                "market_index": trend.get("marketIndex"), "change_rate": trend.get("riseFallRate"),
                "change": trend.get("riseFallDiff"),
            }, stamp))
            rows.extend(extract_series(trend.get("trendList") or [], source=source,
                                       dataset="folder_index_history", item_id=folder_id,
                                       metric="market_index"))
    elif source == "siteb" and path.endswith("/item/trade/statistics/v1/date") and isinstance(data, list):
        rows = extract_series(data, source=source, dataset="transaction_history",
                              item_id=item_id, metric="transaction_count")
    elif source == "siteb" and path.endswith("/item/trade/v1/list") and isinstance(data, dict):
        # The endpoint has returned both "list" and "records" envelopes.
        # Distinct executions may share the same platform and second.
        trades = next(
            (data[key] for key in ("list", "records", "items", "rows")
             if isinstance(data.get(key), list)),
            [],
        )
        for trade in trades:
            if not isinstance(trade, dict):
                continue
            ts = normalize_time(trade.get("tradeTime"))
            value = _number(trade.get("tradePrice"))
            if ts and value is not None:
                identity = next(
                    (str(trade[key]) for key in ("tradeId", "trade_id", "id", "orderId")
                     if trade.get(key) not in (None, "")),
                    None,
                )
                if identity is None:
                    # Stable across pagination retries, including when no ID is exposed.
                    identity = hashlib.sha256(
                        json.dumps(trade, sort_keys=True, ensure_ascii=False,
                                   default=str).encode("utf-8")
                    ).hexdigest()
                rows.append({"source": source, "dataset": "trade_event", "item_id": item_id,
                             "metric": f"trade_price:{identity}",
                             "platform": str(trade.get("platform") or ""),
                             "ts": ts, "close": value, "volume": _number(trade.get("tradeNum")),
                             "extra": trade})
    elif source == "siteb" and path.endswith("/item/trade/v1/overview") and isinstance(data, dict):
        stamp = datetime.now(timezone.utc).isoformat()
        for platform, node in data.items():
            if isinstance(node, dict):
                rows.extend(_numeric_metric_rows(source, "trade_overview_snapshot", item_id,
                                                  node, stamp, prefix=f"{platform}."))
    elif source == "siteb" and path.endswith("/item/trade/v1/overview/today") and isinstance(data, dict):
        stamp = datetime.now(timezone.utc).isoformat()
        for platform in ("overview", "steamOverview"):
            node = data.get(platform)
            if isinstance(node, dict):
                rows.extend(_numeric_metric_rows(source, "trade_today_snapshot", item_id,
                                                  node, stamp, prefix=f"{platform}."))
        if isinstance(data.get("specialStyles"), list):
            storage.upsert_metadata(
                source, f"trade_special_styles.{item_id}", data["specialStyles"]
            )
    elif source == "siteb" and path.endswith("/skin/market/v3/page") and isinstance(data, dict):
        if catalog_start_position is not None:
            catalog_item_ids = [
                str(item.get("id") or item.get("itemId") or "")
                for item in data.get("list") or []
                if isinstance(item, dict)
            ]
        for item in data.get("list") or []:
            if not isinstance(item, dict):
                continue
            actual_id = str(item.get("id") or item.get("itemId") or "")
            stamp = data.get("systemTime") or datetime.now(timezone.utc).isoformat()
            rows.extend(_numeric_metric_rows(source, "catalog_item_snapshot", actual_id, {
                "sell_num": item.get("sellNum"), "increase_price": item.get("increasePrice"),
            }, stamp))
            suspension = item.get("suspension")
            if isinstance(suspension, dict):
                rows.extend(_numeric_metric_rows(source, "catalog_item_snapshot", actual_id,
                                                  suspension, stamp, prefix="suspension."))
            rows.extend(extract_series(item.get("trendList") or [], source=source,
                                       dataset="catalog_price_history", item_id=actual_id,
                                       metric="sell_price"))
            for market in item.get("sellingPriceList") or []:
                if not isinstance(market, dict):
                    continue
                value = _number(market.get("price"))
                ts = normalize_time(market.get("lastUpdate"))
                if value is not None and ts:
                    rows.append({"source": source, "dataset": "platform_snapshot",
                                 "item_id": actual_id, "metric": "sell_price",
                                 "platform": str(market.get("platform") or ""), "ts": ts,
                                 "close": value, "extra": {"catalog": True}})
    elif source == "siteb" and path.endswith("/user/ranking/v1/page") and isinstance(data, dict):
        for rank in data.get("list") or []:
            if not isinstance(rank, dict):
                continue
            info = rank.get("itemInfoVO") if isinstance(rank.get("itemInfoVO"), dict) else {}
            actual_id = str(info.get("itemId") or "")
            stamp = rank.get("updateTime") or data.get("systemTime")
            rows.extend(_numeric_metric_rows(source, "item_ranking_snapshot", actual_id,
                                              {"survive_num": rank.get("surviveNum")}, stamp))
            for block, prefix in (("sellPriceInfoVO", "sell_price."),
                                  ("sellNumsInfoVO", "sell_num."),
                                  ("transactionCountInfoVO", "transaction_count."),
                                  ("transactionAmountInfoVO", "transaction_amount."),
                                  ("hotVO", "hot.")):
                node = rank.get(block)
                if isinstance(node, dict):
                    rows.extend(_numeric_metric_rows(source, "item_ranking_snapshot", actual_id,
                                                      node, stamp, prefix=prefix))
            for market in rank.get("platformInfoList") or []:
                if not isinstance(market, dict):
                    continue
                value = _number(market.get("price"))
                ts = normalize_time(market.get("updateTime"))
                if value is not None and ts:
                    rows.append({"source": source, "dataset": "platform_snapshot",
                                 "item_id": actual_id, "metric": "sell_price",
                                 "platform": str(market.get("platformEnum") or "").lower(),
                                 "ts": ts, "close": value, "extra": {"ranking": True}})
    elif source == "siteb" and path.endswith("/user/ranking/v1/hanging-knife") and isinstance(data, list):
        request = request_data if isinstance(request_data, dict) else {}
        mode = str(request.get("wantToGet") or "all").lower()
        dataset_name = f"hanging_snapshot.{mode}"
        for entry in data:
            if not isinstance(entry, dict):
                continue
            actual_id = str(entry.get("itemId") or entry.get("marketHashName") or entry.get("name") or "")
            stamp = entry.get("updateTime") or entry.get("platformUpdateTime") \
                or datetime.now(timezone.utc).isoformat()
            platform = str(entry.get("platformEnum") or "").lower()
            rows.extend(_numeric_metric_rows(
                source,
                dataset_name,
                actual_id,
                {
                    "platform_price": entry.get("platformPrice"),
                    "steam_price": entry.get("steamPrice"),
                    "profit_amount": entry.get("profitAmount"),
                    "transaction_count": entry.get("transactionCount"),
                    "sell_rate": entry.get("sellRate"),
                    "purchase_rate": entry.get("purchaseRate"),
                },
                stamp,
                prefix=f"{platform}.",
            ))
    elif source == "siteb" and path.endswith("/skin/v2/sale-wear-detail") and isinstance(data, dict):
        for group in ("relatedList", "categoryList", "all"):
            for item in data.get(group) or []:
                if not isinstance(item, dict):
                    continue
                actual_id = str(item.get("itemId") or "")
                rows.extend(_snapshot_rows(source, "wear_variant_snapshot", actual_id, item,
                                           platform=group, ts=item.get("updateTime")))
    elif source == "siteb" and path.endswith("/csgo-case/weapon-trace/list") and isinstance(data, list):
        for case in data:
            if isinstance(case, dict):
                rows.extend(_numeric_metric_rows(source, "case_trace_snapshot",
                                                  str(case.get("itemId") or item_id), {
                                                      "case_type": case.get("caseType"),
                                                      "price": case.get("price"),
                                                      "first_sale_date": case.get("firstSaleDate"),
                                                  }, case.get("firstSaleDate")))
    elif source == "siteb" and path.endswith("/skin/v1/recent-add") and isinstance(data, list):
        stamp = datetime.now(timezone.utc).isoformat()
        for record in data:
            asset = record.get("asset") if isinstance(record, dict) else None
            info = asset.get("assetInfo") if isinstance(asset, dict) and isinstance(asset.get("assetInfo"), dict) else {}
            actual_id = str(info.get("itemId") or "")
            rows.extend(_numeric_metric_rows(source, "asset_snapshot", actual_id, {
                "evaluate_price": info.get("evaluatePrice"), "float_wear": info.get("floatWear"),
                "paint_index": info.get("paintIndex"), "paint_seed": info.get("paintSeed"),
            }, stamp, prefix="recent_add."))
    elif source == "siteb" and path.endswith("/cs2-update-record/v1/page") and isinstance(data, dict):
        stamp = datetime.now(timezone.utc).isoformat()
        for entry in data.get("list") or []:
            if not isinstance(entry, dict):
                continue
            update_id = str(entry.get("id") or "")
            if update_id:
                storage.upsert_metadata(source, f"cs2_update.{update_id}", entry)
            rows.append({"source": source, "dataset": "update_record",
                         "item_id": update_id, "metric": "update",
                         "platform": "",
                         "ts": normalize_time(entry.get("updateTime") or entry.get("createTime")) or stamp,
                         "extra": {"title": entry.get("title"), "type": entry.get("type")}})
        storage.upsert_metadata(source, "cs2_update_catalog", {
            "pageNum": data.get("pageNum"),
            "pageSize": data.get("pageSize"),
            "total": data.get("total"),
            "ids": [
                str(entry.get("id"))
                for entry in data.get("list") or []
                if isinstance(entry, dict) and entry.get("id") is not None
            ],
        })
    elif source == "siteb" and "/cs2-update-record/v1/detail/" in path and isinstance(data, dict):
        update_id = path.rstrip("/").rsplit("/", 1)[-1]
        storage.upsert_metadata(source, f"cs2_update.{update_id}", data)
        rows.append({"source": source, "dataset": "update_record",
                     "item_id": update_id, "metric": "update",
                     "platform": "",
                     "ts": normalize_time(data.get("updateTime")) or datetime.now(timezone.utc).isoformat(),
                     "extra": {"title": data.get("title"), "type": data.get("type"),
                               "content": str(data.get("content") or "")[:500]}})
    elif source == "siteb" and path.endswith("/index/upgrade/v1/list") and isinstance(data, list):
        storage.upsert_metadata(source, "site_upgrade_records", data)
    elif source == "siteb" and path.endswith("/steam/inventory/v3") and isinstance(data, dict):
        account = data.get("steamAccount") if isinstance(data.get("steamAccount"), dict) else {}
        inventory = data.get("inventory") if isinstance(data.get("inventory"), dict) else {}
        summary = inventory.get("summary") if isinstance(inventory.get("summary"), dict) else {}
        request = request_data if isinstance(request_data, dict) else {}
        steam_id = str(
            summary.get("steamId")
            or account.get("steamId")
            or request.get("steam_id")
            or ""
        )
        stamp = summary.get("lastFetchTime") or datetime.now(timezone.utc).isoformat()
        rows.extend(_numeric_metric_rows(source, "public_inventory_summary", steam_id, {
            "total": summary.get("total"),
            "total_price": summary.get("totalPrice"),
            "total_tradable_count": summary.get("totalTradableCount"),
            "cool_down_count": summary.get("coolDownCount"),
        }, stamp))
        classinfos = inventory.get("classinfos") if isinstance(inventory.get("classinfos"), dict) else {}
        for asset in inventory.get("assets") or []:
            if not isinstance(asset, dict):
                continue
            actual_id = str(asset.get("itemId") or "")
            if not actual_id:
                continue
            classinfo = classinfos.get(str(asset.get("classinfoKey") or ""), {})
            if not isinstance(classinfo, dict):
                classinfo = {}
            discovered_items.append({
                "itemId": actual_id,
                "name": classinfo.get("name") or classinfo.get("hashName") or actual_id,
                "marketHashName": classinfo.get("marketHashName") or classinfo.get("hashName"),
                "inventoryAssetId": asset.get("assetId"),
            })
            asset_id = str(asset.get("assetId") or actual_id)
            asset_rows = _numeric_metric_rows(
                source,
                "public_inventory_asset",
                actual_id,
                {
                    "min_price": asset.get("minPrice"),
                    "evaluate_price": asset.get("evaluatePrice"),
                    "float_wear": asset.get("floatWear"),
                    "paint_index": asset.get("paintIndex"),
                    "paint_seed": asset.get("paintSeed"),
                },
                stamp,
            )
            asset_platform = f"inventory:{steam_id}:{asset_id}"
            for row in asset_rows:
                row["platform"] = asset_platform
            rows.extend(asset_rows)
        storage.upsert_metadata(source, f"public_inventory_account.{steam_id}", account)
    elif source == "siteb" and "/steam/inventory-snapshot/v1/recent/" in path and isinstance(data, list):
        for record in data:
            if not isinstance(record, dict):
                continue
            steam_id = str(record.get("steamId") or "")
            rows.extend(_numeric_metric_rows(
                source,
                "public_inventory_history",
                steam_id,
                {
                    "total_count": record.get("totalCount"),
                    "total_price": record.get("totalPrice"),
                },
                record.get("saveTime") or record.get("date"),
            ))
    elif source == "siteb" and path.endswith("/skin/v2/skin-info") and isinstance(data, list):
        stamp = datetime.now(timezone.utc).isoformat()
        for record in data:
            detail = record.get("assetDetail") if isinstance(record, dict) else None
            info = detail.get("assetInfo") if isinstance(detail, dict) and isinstance(detail.get("assetInfo"), dict) else {}
            actual_id = str(info.get("itemId") or "")
            owner = record.get("owner") if isinstance(record, dict) and isinstance(record.get("owner"), dict) else {}
            rows.extend(_numeric_metric_rows(source, "asset_snapshot", actual_id, {
                "evaluate_price": info.get("evaluatePrice"), "float_wear": info.get("floatWear"),
                "paint_index": info.get("paintIndex"), "paint_seed": info.get("paintSeed"),
                "trade_count": record.get("tradeCount"), "first_owned_time": owner.get("firstOwnedTime"),
            }, stamp, prefix="inventory."))
    elif source == "siteb" and path.endswith("/skin/v2/asset/wear-rank") and isinstance(data, dict):
        stamp = datetime.now(timezone.utc).isoformat()
        request_page = request_data if isinstance(request_data, dict) else {}
        rank_start = (
            max(0, int(request_page.get("pageNum") or 1) - 1)
            * max(1, int(request_page.get("pageSize") or 100))
        )
        for rank, asset in enumerate(data.get("list") or [], start=rank_start + 1):
            if not isinstance(asset, dict):
                continue
            actual_id = str(asset.get("itemId") or item_id)
            rows.extend(_numeric_metric_rows(source, "wear_rank_snapshot", actual_id, {
                "rank": rank, "float_wear": asset.get("floatWear") or asset.get("wearFlot"),
                "price": asset.get("price") or asset.get("evaluatePrice"),
                    }, asset.get("updateTime") or stamp, prefix=f"rank.{rank}."))
    elif source == "siteb" and path.endswith("/system-config/v1/default-config") and isinstance(data, dict):
        storage.upsert_metadata(source, "default_config", data)
    elif source == "siteb" and path.endswith("/websiteConfig/v1/list") and isinstance(data, list):
        request_data = request_data if isinstance(request_data, dict) else {}
        key = str(request_data.get("key") or "latest")
        storage.upsert_metadata(source, f"website_config.{key}", data)
    elif source == "siteb" and path.endswith("/common/v1/ws/jwt/anonymous") and isinstance(payload, dict):
        # Do not persist the anonymous JWT itself as metadata. Raw captures keep
        # the response for audit, while the inventory table records its schema.
        storage.upsert_metadata(source, "anonymous_ws_status", {
            "success": payload.get("success"),
            "has_data": bool(payload.get("data")),
            "error_code": payload.get("errorCode"),
        })
    elif source == "siteb" and path.endswith("/message/info") and isinstance(payload, dict):
        storage.upsert_metadata(source, "message_info", payload)
    elif source == "siteb" and "/ranking/v1/attribute/" in path and isinstance(data, list):
        # Item taxonomy (type -> weapon -> finish/rarity filters) used by the ranking UI.
        storage.upsert_metadata(source, "ranking_attributes",
                                {"appid": path.rsplit("/", 1)[-1], "taxonomy": data})
        return _store_payload(storage, source, dataset, discovered_items, rows)
    elif source == "siteb" and path.endswith("/system-config/v1/config") and isinstance(data, dict):
        # Style/filter definition map referenced by every page (wear phases, tiers, ranks).
        storage.upsert_metadata(source, "system_config", data)
        return _store_payload(storage, source, dataset, discovered_items, rows)
    else:
        rows = extract_series(payload, source=source, dataset=dataset, item_id=item_id)
    return _store_payload(
        storage,
        source,
        dataset,
        discovered_items,
        rows,
        catalog_item_ids=catalog_item_ids,
        catalog_start_position=catalog_start_position or 0,
        catalog_discovered_at=catalog_discovered_at,
        catalog_universe=catalog_universe,
    )
