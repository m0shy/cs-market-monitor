"""持续增量检测器（热门 1h / 冷门 5h）。

按平台画像加权轮询 行情站B Open API 快照：
- 导购/倒余额平台（youpin=悠悠有品、c5、steam、haloskins）挂单活跃 → 热门
- buff 仅作高门槛辅助参考价源，避免单一平台热度淹没导购信号
- 热门（1h 一轮）：附带 7 日均价（price/avg）
- 冷门（5h 一轮）：仅平台价格快照（price/single）

数据写入 platform_snapshot / platform_avg，与 site API 数据同表，
Web 详情页自动展示最新价格。调度状态由 series 表最新快照时间推导，
进程重启后自动恢复节奏，无需额外状态表。
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any

from .errors import classify_error
from .http import PoliteClient
from .ingest import ingest_payload
from .sources import _payload_failure_detail, _payload_ok
from .storage import Storage

log = logging.getLogger(__name__)

DEFAULT_BROKER_PLATFORMS = ("youpin", "c5", "steam", "haloskins")
RATE_STATE_JOB = "siteb_open_adaptive_rate"
"""导购/倒余额重点平台：悠悠有品(UU)、C5、Steam 市场、HaloSkins。"""


def _epoch(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _rate_limited(status: int | None, payload: Any) -> bool:
    """识别两种限流：HTTP 429 与 JSON errorCode 4005（200 包装的配额上限）。"""
    if status == 429:
        return True
    if isinstance(payload, dict) and payload.get("success") is False:
        return payload.get("errorCode") in (4005, 4006, 429)
    return False


class IncrementalCollector:
    def __init__(self, storage: Storage, settings, hot_interval: int = 3600,
                 cold_interval: int = 18000, hot_tx_threshold: float = 5.0,
                 hot_broker_sell_threshold: float = 700.0,
                 hot_broker_single_threshold: float = 400.0,
                 hot_buff_sell_threshold: float = 800.0,
                 broker_platforms: tuple[str, ...] = DEFAULT_BROKER_PLATFORMS,
                 rate_per_minute: int = 250, fetch_avg_for_hot: bool = True,
                 api_keys: tuple[str, ...] | None = None):
        api_keys = tuple(
            key
            for key in dict.fromkeys(
                str(value).strip()
                for value in (api_keys or (settings.siteb_api_key,))
            )
            if key
        )
        if not api_keys:
            api_keys = ("",)
        headers = {"Authorization": f"Bearer {api_keys[0]}"}
        self.client = PoliteClient("siteb", settings.siteb_open_base, storage,
                                   delay=0.0, timeout=35, headers=headers, retries=2)
        self.api_keys = api_keys
        self._key_index = 0
        self.storage = storage
        self.hot_interval = hot_interval
        self.cold_interval = cold_interval
        self.hot_tx_threshold = hot_tx_threshold
        self.hot_broker_sell_threshold = hot_broker_sell_threshold
        self.hot_broker_single_threshold = hot_broker_single_threshold
        self.hot_buff_sell_threshold = hot_buff_sell_threshold
        self.broker_platforms = tuple(broker_platforms) or DEFAULT_BROKER_PLATFORMS
        # 单个个人 key 实测安全速率约 60/min（JSON 4005 限流）；多 key 轮换
        # 时按 key 数放大上限，自适应降速仍会按 4005 回落。此前硬编码 60
        # 会让配置多 key 也无法提升吞吐。
        # 2026-10-11 实测：price/single 在 426/min 下 60 连发零失败。
        # 保守设 240/min（实测上限的 ~56%），自适应降速仍在 4005 时回落。
        per_key_rate = 240
        active_keys = len(api_keys) if api_keys != ("",) else 1
        self.configured_rate = min(
            per_key_rate * max(1, active_keys),
            max(1, rate_per_minute),
        )
        try:
            learned_rate = int(storage.get_state(RATE_STATE_JOB) or 0)
        except (TypeError, ValueError):
            learned_rate = 0
        self.rate_per_minute = (
            min(self.configured_rate, max(1, learned_rate))
            if learned_rate else self.configured_rate
        )
        self._last_rate_ok = time.time()
        self._last_rate_limit = (
            time.time() if self.rate_per_minute < self.configured_rate else 0.0
        )
        self.fetch_avg_for_hot = fetch_avg_for_hot
        self.items: list[dict] = []
        self.hot_count = 0
        self.cold_count = 0
        self._last_classify = 0.0
        self._backoff_until = 0.0
        # 连续失败计数：首次瞬时失败只计内存不落票据（此前 2 小时可产生
        # 3000+ 张"立即消解"的票据，控制库写放大严重）；连续第 2 次起落库。
        self._consecutive_failures: dict[str, int] = {}
        self.last_error_class = ""
        self.last_error = ""
        self.last_result: dict[str, Any] = {
            "status": "running",
            "error_class": "",
            "metrics": {},
            "details": {},
        }

    # ------------------------------------------------------------------
    # 热度信号
    # ------------------------------------------------------------------
    @staticmethod
    def _latest_sell_nums(conn) -> dict[str, dict[str, float]]:
        """每饰品最新快照的各平台挂单量，按 market_hash_name 聚合。

        历史数据混用 open:哈希（open API）与数字 id（site API）两种格式，
        通过 items 表统一桥接到名称后再合并，避免信号丢失。
        """
        rows = conn.execute(
            """SELECT i.market_hash_name, s.platform, s.close FROM series s
               JOIN items i ON i.source='siteb' AND i.item_id=s.item_id
               JOIN (SELECT item_id, platform, MAX(ts) ts FROM series
                     WHERE dataset='platform_snapshot' AND source='siteb'
                       AND metric='sell_num' GROUP BY item_id, platform) m
                 ON s.item_id=m.item_id AND s.platform=m.platform AND s.ts=m.ts
               WHERE s.dataset='platform_snapshot' AND s.source='siteb'
                 AND s.metric='sell_num'"""
        ).fetchall()
        out: dict[str, dict[str, float]] = {}
        for row in rows:
            name = str(row["market_hash_name"] or "")
            if not name:
                continue
            out.setdefault(name, {})[str(row["platform"] or "")] = float(row["close"] or 0)
        return out

    @staticmethod
    def _latest_tx(conn) -> dict[str, float]:
        """近 24h 成交数（transaction_count），按 market_hash_name 聚合。"""
        rows = conn.execute(
            """SELECT i.market_hash_name, MAX(s.close) v FROM series s
                JOIN items i ON i.source='siteb' AND i.item_id=s.item_id
                JOIN (
                    SELECT i2.market_hash_name, MAX(s2.ts) latest_ts
                    FROM series s2
                    JOIN items i2
                      ON i2.source='siteb' AND i2.item_id=s2.item_id
                    WHERE s2.dataset='item_snapshot' AND s2.source='siteb'
                      AND s2.metric='transaction_count'
                    GROUP BY i2.market_hash_name
                ) latest
                  ON latest.market_hash_name=i.market_hash_name
                 AND latest.latest_ts=s.ts
                WHERE s.dataset='item_snapshot' AND s.source='siteb'
                  AND s.metric='transaction_count'
                GROUP BY i.market_hash_name"""
        ).fetchall()
        return {str(r["market_hash_name"] or ""): float(r["v"] or 0) for r in rows}

    def broker_score(self, sells: dict[str, float]) -> float:
        """导购/倒余额平台挂单合计（buff 不参与加权）。"""
        return sum(sells.get(platform, 0.0) for platform in self.broker_platforms)

    def _is_hot(self, tx: float, sells: dict[str, float]) -> bool:
        if tx >= self.hot_tx_threshold:
            return True
        if self.broker_score(sells) >= self.hot_broker_sell_threshold:
            return True
        if any(sells.get(platform, 0.0) >= self.hot_broker_single_threshold
               for platform in self.broker_platforms):
            return True
        if (self.hot_buff_sell_threshold > 0
                and sells.get("buff", 0.0) >= self.hot_buff_sell_threshold):
            return True
        return False

    # ------------------------------------------------------------------
    # 载入与热度
    # ------------------------------------------------------------------
    def load_items(self) -> None:
        with self.storage.connect() as conn:
            rows = conn.execute(
                """SELECT c.item_id, i.market_hash_name, c.position
                   FROM catalog_memberships c
                   JOIN items i ON i.source=c.source AND i.item_id=c.item_id
                   WHERE c.source='siteb'
                     AND c.universe='siteb_open_api'
                     AND c.status='active'
                     AND i.market_hash_name IS NOT NULL
                     AND i.market_hash_name<>'' ORDER BY c.position"""
            ).fetchall()
            last_by_name = {
                str(r["market_hash_name"]): r["last_ts"] for r in conn.execute(
                    """SELECT i.market_hash_name, MAX(s.ts) last_ts FROM series s
                       JOIN items i ON i.source='siteb' AND i.item_id=s.item_id
                       WHERE s.dataset='platform_snapshot' AND s.source='siteb'
                         AND s.metric='sell_price'
                       GROUP BY i.market_hash_name""")
            }
            tx_by_name = self._latest_tx(conn)
            sell_by_name = self._latest_sell_nums(conn)
        now = time.time()
        items: list[dict] = []
        for row in rows:
            item_id = str(row["item_id"])
            name = str(row["market_hash_name"])
            sells = sell_by_name.get(name, {})
            tx = tx_by_name.get(name, 0.0)
            last_epoch = _epoch(last_by_name.get(name))
            hot = self._is_hot(tx, sells)
            interval = self.hot_interval if hot else self.cold_interval
            if last_epoch and now - last_epoch < interval:
                # 已有近期快照：按原节奏续期
                due = last_epoch + interval
            else:
                # 首轮错峰：把初始批次均匀铺满整个窗口
                spread = max(1, interval // 60)
                due = now + ((int(row["position"] or 0) % spread) * 60)
            items.append({
                "item_id": item_id,
                "name": name,
                "hot": hot,
                "broker_sell": self.broker_score(sells),
                "due": due,
                "due_interval": interval,
                "last": last_epoch,
            })
        self.items = items
        self._count()
        log.info("incremental loaded %s items (hot=%s cold=%s, "
                 "broker_platforms=%s, rate=%s/%s min)",
                 len(items), self.hot_count, self.cold_count,
                 ",".join(self.broker_platforms),
                 self.rate_per_minute, self.configured_rate)

    def _count(self) -> None:
        self.hot_count = sum(1 for item in self.items if item["hot"])
        self.cold_count = len(self.items) - self.hot_count

    def refresh_hotness(self) -> None:
        """每 30 分钟按最新成交/挂单数据重新分级（热门会变化）。"""
        if time.time() - self._last_classify < 1800:
            return
        self._last_classify = time.time()
        with self.storage.connect() as conn:
            tx_by_name = self._latest_tx(conn)
            sell_by_name = self._latest_sell_nums(conn)
        now = time.time()
        changed = 0
        for item in self.items:
            sells = sell_by_name.get(item["name"], {})
            tx = tx_by_name.get(item["name"], 0.0)
            hot = self._is_hot(tx, sells)
            item["broker_sell"] = self.broker_score(sells)
            if hot != item["hot"]:
                item["hot"] = hot
                interval = self.hot_interval if hot else self.cold_interval
                item["due_interval"] = interval
                item["due"] = min(item["due"], now + interval)
                changed += 1
        self._count()
        if changed:
            log.info("hotness refreshed: %s reclassified (hot=%s cold=%s)",
                     changed, self.hot_count, self.cold_count)

    # ------------------------------------------------------------------
    # 单饰品刷新
    # ------------------------------------------------------------------
    def _rotate_key(self) -> None:
        """轮换 API key（个人 key 各自有独立配额，多 key 可叠加吞吐）。"""
        if len(self.api_keys) <= 1:
            return
        self._key_index = (self._key_index + 1) % len(self.api_keys)
        self.client.session.headers["Authorization"] = \
            f"Bearer {self.api_keys[self._key_index]}"

    def _persist_rate(self, reason: str) -> None:
        self.storage.set_state(
            RATE_STATE_JOB,
            str(self.rate_per_minute),
            {
                "configured_rate": self.configured_rate,
                "reason": reason,
            },
        )

    def _on_rate_limited(self) -> None:
        now = time.time()
        self._backoff_until = max(self._backoff_until, now + 120)
        self._last_rate_limit = now
        self.rate_per_minute = max(1, self.rate_per_minute - 15)
        self._persist_rate("rate-limited")
        log.warning("open API rate-limited (HTTP 429 / errorCode 4005); "
                    "backing off 120s, rate -> %s/min", self.rate_per_minute)

    def refresh_item(self, item: dict) -> bool:
        name = item["name"]
        requests: list[tuple[str, str, dict]] = [
            ("GET", "/open/cs2/v1/price/single", {"marketHashName": name}),
        ]
        if item["hot"] and self.fetch_avg_for_hot:
            requests.append(("GET", "/open/cs2/v1/price/avg", {"marketHashName": name}))
        self._rotate_key()
        ok = True
        self.last_error_class = ""
        self.last_error = ""
        job = "siteb_open_incremental"
        item_id = str(item["item_id"])
        self.storage.update_task_slice(
            job,
            item_id,
            "running",
            owner_id=os.environ.get("CSGO_MONITOR_RUN_ID", ""),
            lease_seconds=600,
            increment_attempt=True,
            result={"market_hash_name": name, "hot": bool(item["hot"])},
        )
        for method, path, data in requests:
            status, payload = (self.client.get(path, data)
                               if method == "GET" else self.client.post(path, data))
            if _rate_limited(status, payload):
                self._on_rate_limited()
                self.last_error_class = "rate_limit"
                self.last_error = f"{path}: rate limited"
                ok = False
                break
            self._last_rate_ok = time.time()
            if not _payload_ok(status, payload):
                detail = _payload_failure_detail(path, status, payload)
                self.last_error_class = classify_error(
                    detail,
                    status=status,
                    payload=payload,
                )
                self.last_error = detail
                if self.last_error_class == "audit":
                    self._backoff_until = max(
                        self._backoff_until,
                        time.time() + 1800,
                    )
                ok = False
                break
            ingest_payload(self.storage, "siteb", path, payload,
                           {"marketHashName": name, "itemId": item["item_id"]})
        if ok:
            self._consecutive_failures.pop(item_id, None)
            self.storage.resolve_failure(job, item_id)
            self.storage.update_task_slice(
                job,
                item_id,
                "done",
                outcome="ok",
                owner_id=os.environ.get("CSGO_MONITOR_RUN_ID", ""),
            )
        else:
            streak = self._consecutive_failures.get(item_id, 0) + 1
            self._consecutive_failures[item_id] = streak
            if streak >= 2:
                self.storage.record_failure(
                    job,
                    item_id,
                    self.last_error or "incremental request failed",
                    error_class=self.last_error_class or None,
                )
            self.storage.update_task_slice(
                job,
                item_id,
                "deferred",
                outcome=self.last_error_class or "unknown",
                owner_id=os.environ.get("CSGO_MONITOR_RUN_ID", ""),
                result={"error": self.last_error},
            )
        return ok

    # ------------------------------------------------------------------
    # 主循环
    # ------------------------------------------------------------------
    def _quiet_rate_cap(self, now: float) -> int:
        """日间（默认 08:00-23:00）返回限速值，夜间返回 0（不限）。"""
        local = time.localtime(now)
        minute = local.tm_hour * 60 + local.tm_min
        start, end = 8 * 60, 23 * 60
        if start <= minute < end:
            return 60  # 实测远高于此，保守限制
        return 0

    def run(self, max_items: int = 0) -> int:
        self.load_items()
        if not self.items:
            log.warning("incremental: no catalog items to monitor")
            self.last_result = {
                "status": "failed",
                "error_class": "data_gap",
                "metrics": {"catalog": 0},
                "details": {"reason": "no open API catalog items"},
            }
            return 0
        if not self.client:
            return 0
        processed = 0
        failed = 0
        attempted = 0
        last_report = time.time()
        last_rate_raise = time.time()
        while True:
            now = time.time()
            if now < self._backoff_until:
                time.sleep(min(10.0, self._backoff_until - now))
                continue
            # 日间降速档（08:00-23:00）：价格轮询上限压到 quiet_rate，
            # 夜间恢复全速。持续采集不断流，只降速率。
            quiet_cap = self._quiet_rate_cap(now)
            if quiet_cap and self.rate_per_minute > quiet_cap:
                self.rate_per_minute = quiet_cap
            # 连续健康 30 分钟后再小幅升速，避免在滚动配额边界来回震荡。
            if (self.rate_per_minute < self.configured_rate
                    and now - last_rate_raise >= 600
                    and now - self._last_rate_limit >= 1800
                    and now - self._last_rate_ok < 120):
                self.rate_per_minute = min(
                    self.configured_rate if not quiet_cap else quiet_cap,
                    self.rate_per_minute + 5)
                last_rate_raise = now
                self._persist_rate("healthy-window")
                log.info("open API healthy; rate -> %s/min", self.rate_per_minute)
            self.refresh_hotness()
            due = (self.items if max_items > 0
                   else [item for item in self.items if item["due"] <= now])
            if not due:
                next_due = min(item["due"] for item in self.items)
                time.sleep(min(30.0, max(0.5, next_due - now)))
                continue
            # 排序：导购信号强优先 -> 严重超期者优先（防饿死）
            def due_key(item: dict) -> tuple:
                return (0 if now - item["due"] > item["due_interval"] * 0.5 else 1,
                        -item.get("broker_sell", 0.0), item["due"])
            hot_due = sorted((item for item in due if item["hot"]), key=due_key)
            cold_due = sorted((item for item in due if not item["hot"]), key=due_key)
            # 3 热门 : 1 冷门 交错，保证冷门不被热门长期占满队列而饿死
            due = []
            hi = ci = 0
            while hi < len(hot_due) or ci < len(cold_due):
                for _ in range(3):
                    if hi >= len(hot_due):
                        break
                    due.append(hot_due[hi])
                    hi += 1
                if ci < len(cold_due):
                    due.append(cold_due[ci])
                    ci += 1
            for item in due:
                now = time.time()
                if item["due"] > now or now < self._backoff_until:
                    break
                ok = self.refresh_item(item)
                attempted += 1
                interval = self.hot_interval if item["hot"] else self.cold_interval
                if ok:
                    item["last"] = now
                    item["due"] = now + interval
                    processed += 1
                else:
                    item["due"] = now + 300  # 5 分钟后重试
                    failed += 1
                if max_items and attempted >= max_items:
                    log.info("incremental test done: %s ok %s failed", processed, failed)
                    failure_counts = self.storage.failure_counts(
                        "siteb_open_incremental"
                    )
                    self.last_result = {
                        "status": (
                            "deferred"
                            if self.last_error_class in {
                                "rate_limit",
                                "quota",
                                "transport",
                                "audit",
                            }
                            else "partial"
                            if failed
                            else "ok"
                        ),
                        "error_class": self.last_error_class,
                        "metrics": {
                            "attempted": attempted,
                            "processed": processed,
                            "failed": failed,
                            "catalog": len(self.items),
                            "hot": self.hot_count,
                            "cold": self.cold_count,
                            "rate_per_minute": self.rate_per_minute,
                            "failures": failure_counts,
                        },
                        "details": {"error": self.last_error},
                    }
                    return processed
                # 按请求数节流：热门 1 条 = single+avg 两个请求
                req_per_item = 2 if (item["hot"] and self.fetch_avg_for_hot) else 1
                time.sleep(60.0 / self.rate_per_minute * req_per_item)
            if time.time() - last_report >= 300:
                last_report = time.time()
                self.storage.set_coverage(
                    "siteb",
                    "open_incremental",
                    "running",
                    processed,
                    note=(
                        f"attempted={attempted}; failed={failed}; "
                        f"hot={self.hot_count}; cold={self.cold_count}; "
                        f"rate={self.rate_per_minute}"
                    ),
                )
                log.info("incremental progress: %s ok, %s failed (hot=%s cold=%s, next=%s)",
                         processed, failed, self.hot_count, self.cold_count,
                         time.strftime("%H:%M:%S", time.localtime(min(item["due"] for item in self.items))))
