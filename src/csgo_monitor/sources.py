from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .config import Settings
from .http import PoliteClient
from .ingest import ingest_payload
from .normalize import date_bounds, extract_series, find_items
from .storage import Storage

log = logging.getLogger(__name__)


def _payload_ok(status: int | None, payload: Any) -> bool:
    if status != 200 or payload is None:
        return False
    if isinstance(payload, dict):
        if payload.get("success") is False:
            return False
        if payload.get("code") not in (None, 0, 200):
            return False
    return True


def _payload_failure_detail(path: str, status: int | None, payload: Any) -> str:
    """Keep the small error fields needed to classify a rejected response.

    Some upstream errors use HTTP 200 and put the real reason in JSON.  Saving
    only the status loses the distinction between an audit response, a quota
    response, and an unknown protocol response during later maintenance.
    """
    detail = f"{path}: HTTP {status}"
    if not isinstance(payload, dict):
        return detail
    code = payload.get("errorCode", payload.get("code"))
    message = payload.get("errorMsg") or payload.get("message") or payload.get("msg")
    fields = []
    if code not in (None, ""):
        fields.append(f"code={str(code)[:80]}")
    if message not in (None, ""):
        fields.append("message=" + " ".join(str(message).split())[:240])
    return detail + (": " + "; ".join(fields) if fields else "")


class SiteASource:
    METRICS = ("sell_price", "buy_price", "sell_num", "buy_num", "turnover_number")
    PLATFORMS = (1, 2, 3)

    def __init__(self, settings: Settings, storage: Storage):
        headers = {"ApiToken": settings.sitea_token} if settings.sitea_token else {}
        self.client = PoliteClient("sitea", settings.sitea_base, storage, settings.request_delay,
                                   settings.timeout, headers, proxy=settings.proxy_url)
        self.storage = storage
        self.days = settings.days
        self.full_history = settings.days <= 0

    def snapshots(self) -> None:
        status, payload = self.client.get("/api/v1/current_data", {"type": "init"})
        if not _payload_ok(status, payload):
            self.storage.set_coverage("sitea", "market_snapshot", "blocked", note=f"HTTP {status}")
            return
        _, count = ingest_payload(self.storage, "sitea", "/api/v1/current_data", payload, {"type": "init"})
        self.storage.set_coverage("sitea", "market_snapshot", "ok", count)

    def catalog(self, page_size: int = 500, max_pages: int = 0) -> list[str]:
        page = 1
        item_ids: list[str] = []
        while max_pages <= 0 or page <= max_pages:
            status, payload = self.client.post(
                "/api/v1/goods/get_all_goods_id", {"page_index": page, "page_size": page_size}
            )
            if not _payload_ok(status, payload):
                state = "blocked" if status in {401, 403} else "failed"
                self.storage.set_coverage("sitea", "item_catalog", state, len(item_ids),
                                          note=f"HTTP {status}; token may be expired or not authorized")
                break
            items = find_items(payload)
            if not items:
                break
            self.storage.upsert_items("sitea", items)
            new_ids = [str(item.get("good_id") or item.get("id")) for item in items]
            item_ids.extend(value for value in new_ids if value and value != "None")
            if len(items) < page_size:
                break
            page += 1
            self.storage.set_state("sitea_catalog", str(page))
        if item_ids:
            self.storage.upsert_catalog_items(
                "sitea",
                item_ids,
                universe="sitea_official",
            )
            self.storage.set_coverage("sitea", "item_catalog", "ok", len(set(item_ids)))
        return list(dict.fromkeys(item_ids))

    def stored_item_ids(self) -> list[str]:
        catalog = self.storage.get_catalog_items("sitea")
        if catalog:
            return catalog
        with self.storage.connect() as conn:
            return [row[0] for row in conn.execute("SELECT item_id FROM items WHERE source='sitea' ORDER BY item_id")]

    def histories(self, item_ids: list[str], max_items: int = 0, resume: bool = True) -> int:
        start_at = int(self.storage.get_state("sitea_histories") or 0) if resume else 0
        if start_at >= len(item_ids):
            start_at = 0
        stop_at = min(len(item_ids), start_at + max_items) if max_items > 0 else len(item_ids)
        req_period = str(self.days if not self.full_history else 1095)
        cutoff = None
        if not self.full_history:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=self.days)).date().isoformat()
        inserted = 0
        interrupted = False
        for index, item_id in enumerate(item_ids):
            if index < start_at or index >= stop_at:
                continue
            for platform in self.PLATFORMS:
                for metric in self.METRICS:
                    status, payload = self.client.post("/api/v1/info/chart", {
                        "good_id": int(item_id), "key": metric, "period": req_period,
                        "platform": platform, "style": "all_style",
                    })
                    dataset = f"item_history.{metric}.p{platform}"
                    failure_id = f"{item_id}:{platform}:{metric}"
                    if not _payload_ok(status, payload):
                        state = "blocked" if status in {401, 403} else "failed"
                        self.storage.set_coverage("sitea", dataset, state, note=f"HTTP {status}")
                        self.storage.record_failure(
                            "sitea_histories", failure_id, f"HTTP {status}",
                            error_code=str(status or ""),
                            endpoint="/api/v1/info/chart",
                        )
                        interrupted = True
                        break
                    rows = extract_series(payload, source="sitea", dataset=dataset,
                                          item_id=item_id, metric=metric, platform=str(platform))
                    if cutoff is not None:
                        rows = [row for row in rows if str(row["ts"]) >= cutoff]
                    inserted += self.storage.upsert_series(rows)
                    earliest, latest = date_bounds(rows)
                    self.storage.set_coverage("sitea", dataset, "ok" if rows else "empty",
                                              len(rows), earliest, latest)
                    self.storage.resolve_failure("sitea_histories", failure_id)
                if interrupted:
                    break
            if interrupted:
                break
            self.storage.set_state("sitea_histories", str(index + 1), {"item_id": item_id})
        return inserted


class SiteBSource:
    ENDPOINTS = (
        ("statistics", "GET", "/api/index/statistics/v1/summary", None),
        ("player_trend", "GET", "/api/index/players/trend/v1/all", None),
        ("item_changes", "POST", "/api/index/item/change/v1/list", {"page_num": 1, "page_size": 100}),
        ("market_page", "POST", "/api/skin/market/v3/page", {"dataField": "pvNums", "dataRange": "", "sortType": "desc", "nextId": "", "queryName": "", "pageSize": 20}),
    )

    def __init__(self, settings: Settings, storage: Storage):
        self.client = PoliteClient("siteb", "https://www.siteb.com", storage,
                                   settings.request_delay, settings.timeout,
                                   {"Referer": "https://www.siteb.com/"},
                                   proxy=settings.proxy_url)
        self.storage = storage

    def snapshots(self) -> None:
        for name, method, path, data in self.ENDPOINTS:
            status, payload = self.client.get(path) if method == "GET" else self.client.post(path, data)
            if not _payload_ok(status, payload):
                self.storage.set_coverage("siteb", name, "failed", note=f"HTTP {status}")
                continue
            _, count = ingest_payload(self.storage, "siteb", path, payload, data)
            self.storage.set_coverage("siteb", name, "ok", count,
                                      note="Snapshot endpoint; browser discovery captures additional public datasets")
