from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import uuid
from typing import Any, Iterable

import requests

from .config import Settings
from .errors import classify_error
from .http import CHROME_UA
from .storage import Storage


@dataclass(frozen=True)
class RouteProbeResult:
    route_class: str
    route: str
    status: str
    latency_ms: float | None
    http_status: int | None
    error_class: str
    note: str
    checked_at: str
    cooldown_until: str | None = None

    @property
    def healthy(self) -> bool:
        return self.status == "ok"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_route(route: str | None) -> str:
    value = str(route or "").strip()
    return "direct" if value.lower() in {"", "direct", "none"} else value


def route_proxy(route: str | None) -> str | None:
    value = normalize_route(route)
    return None if value == "direct" else value


class RouteSelector:
    def __init__(self, settings: Settings, storage: Storage):
        self.settings = settings
        self.storage = storage
        self._selected_cache: dict[
            str, tuple[float, RouteProbeResult]
        ] = {}

    def candidates(self, route_class: str) -> tuple[str, ...]:
        values = self.settings.runtime_routes.get(route_class, ("direct",))
        return tuple(dict.fromkeys(normalize_route(value) for value in values))

    def invalidate(self, route_class: str) -> None:
        self._selected_cache.pop(route_class, None)

    def select(
        self,
        route_class: str,
        *,
        force: bool = False,
    ) -> RouteProbeResult | None:
        cached = self._selected_cache.get(route_class)
        if not force and cached and cached[0] > time.monotonic():
            return cached[1]
        for route in self.candidates(route_class):
            result = self.probe(route_class, route, force=force)
            if result.healthy:
                self._selected_cache[route_class] = (
                    time.monotonic() + self.settings.route_cache_seconds,
                    result,
                )
                return result
        self._selected_cache.pop(route_class, None)
        return None

    def probe_all(
        self,
        route_classes: Iterable[str] | None = None,
        *,
        force: bool = False,
    ) -> dict[str, dict[str, Any]]:
        selected: dict[str, dict[str, Any]] = {}
        classes = tuple(route_classes or self.settings.runtime_routes)
        for route_class in classes:
            probes = [
                self.probe(route_class, route, force=force)
                for route in self.candidates(route_class)
            ]
            healthy = next((probe for probe in probes if probe.healthy), None)
            if healthy is not None:
                self._selected_cache[route_class] = (
                    time.monotonic() + self.settings.route_cache_seconds,
                    healthy,
                )
            selected[route_class] = {
                "selected": healthy.route if healthy else None,
                "status": "ok" if healthy else "unavailable",
                "probes": [probe.as_dict() for probe in probes],
            }
        return selected

    def probe(
        self,
        route_class: str,
        route: str,
        *,
        force: bool = False,
    ) -> RouteProbeResult:
        normalized = normalize_route(route)
        if not force:
            cooldown = self._cooldown_until(route_class, normalized)
            if cooldown:
                return RouteProbeResult(
                    route_class=route_class,
                    route=normalized,
                    status="cooldown",
                    latency_ms=None,
                    http_status=None,
                    error_class="transport",
                    note="route cooling down after recent failures",
                    checked_at=datetime.now(timezone.utc).isoformat(),
                    cooldown_until=cooldown,
                )

        started = time.monotonic()
        http_status: int | None = None
        note = ""
        error_class = ""
        status = "failed"
        try:
            http_status, payload = self._request_probe(route_class, normalized)
            if self._response_ok(route_class, http_status, payload):
                status = "ok"
                note = f"HTTP {http_status}"
            else:
                note = self._response_note(http_status, payload)
                error_class = classify_error(
                    note,
                    status=http_status,
                    payload=payload,
                )
        except Exception as exc:
            note = f"{type(exc).__name__}: {exc}"[:500]
            error_class = classify_error(exc)
        latency_ms = round((time.monotonic() - started) * 1000, 2)
        stored = self.storage.record_route_health(
            route_class,
            normalized,
            status,
            latency_ms=latency_ms,
            error_class=error_class,
            note=note,
            failure_threshold=self.settings.route_failure_threshold,
            cooldown_seconds=self.settings.route_cooldown_seconds,
        )
        return RouteProbeResult(
            route_class=route_class,
            route=normalized,
            status=status,
            latency_ms=latency_ms,
            http_status=http_status,
            error_class=error_class,
            note=note,
            checked_at=str(stored["checked_at"]),
            cooldown_until=stored["cooldown_until"],
        )

    def write_report(
        self,
        report: dict[str, Any],
        path: Path | None = None,
    ) -> Path:
        target = path or self.settings.output_dir / "runtime" / "route_health.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".tmp")
        temporary.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(target)
        return target

    def _cooldown_until(self, route_class: str, route: str) -> str | None:
        rows = self.storage.get_route_health(route_class, route)
        if not rows or not rows[0]["cooldown_until"]:
            return None
        value = str(rows[0]["cooldown_until"])
        try:
            until = datetime.fromisoformat(value)
            if until.tzinfo is None:
                until = until.replace(tzinfo=timezone.utc)
            if until > datetime.now(timezone.utc):
                return value
        except ValueError:
            return None
        return None

    def _request_probe(
        self,
        route_class: str,
        route: str,
    ) -> tuple[int, Any]:
        # 安全（全面优化审计）：探测也走 Chrome TLS 指纹——平时采集是
        # chrome131 指纹、探测却是 python-requests 会自曝自动化客户端
        from .http import curl_requests, DEFAULT_IMPERSONATE
        session = (
            curl_requests.Session(impersonate=DEFAULT_IMPERSONATE)
            if curl_requests is not None else requests.Session()
        )
        session.trust_env = False
        proxy = route_proxy(route)
        if proxy:
            session.proxies.update({"http": proxy, "https": proxy})
        session.headers.update(
            {
                "User-Agent": CHROME_UA,
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
            }
        )
        timeout = (
            min(3.0, self.settings.route_probe_timeout),
            self.settings.route_probe_timeout,
        )
        if route_class == "siteb_site_api":
            session.get(
                "https://www.siteb.com/",
                timeout=timeout,
            ).raise_for_status()
            device_id = session.cookies.get("SDT_DeviceId") or str(uuid.uuid4())
            session.headers.update(
                {
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.siteb.com/",
                    "language": "zh_CN",
                    "x-device": "1",
                    "x-device-id": device_id,
                    "x-currency": "CNY",
                    "x-app-version": "1.0.0",
                }
            )
            response = session.get(
                "https://www.siteb.com/api/index/statistics/v1/summary",
                timeout=timeout,
            )
            return response.status_code, self._json_or_text(response)
        if route_class == "siteb_open_api":
            response = session.get(
                self.settings.siteb_open_base + "/",
                timeout=timeout,
            )
            return response.status_code, self._json_or_text(response)
        if route_class == "siteb_browser":
            response = session.get(
                "https://www.siteb.com/",
                timeout=timeout,
            )
            return response.status_code, self._json_or_text(response)
        if route_class == "sitea_public":
            response = session.get(
                "https://sitea.com/home",
                timeout=timeout,
            )
            return response.status_code, self._json_or_text(response)
        raise ValueError(f"unknown route class: {route_class}")

    @staticmethod
    def _json_or_text(response: requests.Response) -> Any:
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return response.text[:500]

    @staticmethod
    def _response_ok(
        route_class: str,
        status: int,
        payload: Any,
    ) -> bool:
        if route_class == "siteb_open_api":
            return 200 <= status < 500
        if not 200 <= status < 400:
            return False
        if route_class == "siteb_site_api":
            if not isinstance(payload, dict):
                return False
            if payload.get("success") is False:
                return False
            code = payload.get("errorCode", payload.get("code"))
            return code in (None, 0, "0", 200, "200")
        return True

    @staticmethod
    def _response_note(status: int, payload: Any) -> str:
        if isinstance(payload, dict):
            code = payload.get("errorCode", payload.get("code"))
            message = payload.get("errorMsg") or payload.get("message") or ""
            return f"HTTP {status}; code={code}; {message}"[:500]
        return f"HTTP {status}; {str(payload)[:300]}"
