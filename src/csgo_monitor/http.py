from __future__ import annotations

import hashlib
import json
import logging
import random
import threading
import time
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable
from urllib.parse import urlsplit

import requests
try:
    from curl_cffi import requests as curl_requests
except Exception:  # curl_cffi optional; falls back to requests
    curl_requests = None

from .errors import classify_error
from .storage import Storage

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 采集端一致性基线：UA / Client-Hints / TLS 指纹三者对齐到同一 Chrome 版本。
# 目标是让请求看起来与真实浏览器一致（而非暴露出 Python requests 的默认
# 特征），配合代理形成基本的隐私基线；不做验证码绕过或指纹对抗。
# ---------------------------------------------------------------------------
DEFAULT_IMPERSONATE = "chrome131"

CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

CHROME_CLIENT_HINTS = {
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}

# www.siteb.com 站点接口的全局跨进程限速间隔。supervisor 有 5 条站点车道
# 并行，若各进程只做本地 delay，聚合请求会以数倍速率打向站点并快速触发
# err106/err107；该槽位让所有进程共享同一节流预算。
SITEB_SITE_SHARED_INTERVAL = 0.75

# transport_error 熔丝的自愈时长：命中终态传输错误后短路这么久，
# 之后的请求允许重试探测，避免一次网络抖动把常驻服务永久打停。
TRANSPORT_FUSE_RESET_SECONDS = 300.0


def make_timestamp() -> str:
    """行情站B anti-bot timestamp (13 digits).

    The last digit is a checksum of the first 12 digits (sum mod 10).
    Plain ``Date.now()`` values are rejected by www.siteb.com with
    ``errorCode=108``, so every site API request must carry a checksum-valid
    timestamp (query string for GET, query string + body for POST). The
    algorithm was confirmed against two independent GitHub crawler projects
    and against every browser-driven capture stored in the database.
    """
    prefix = str(int(time.time() * 1000))[:12]
    checksum = sum(int(ch) for ch in prefix) % 10
    return prefix + str(checksum)


class PoliteClient:
    def __init__(self, source: str, base_url: str, storage: Storage, delay: float = 1.2,
                 timeout: float | tuple[float, float] = 30,
                 headers: dict[str, str] | None = None,
                 proxy: str | None = None,
                 timestamp_fn: Callable[[], str] | None = None,
                 impersonate: str | None = None,
                 retries: int = 4,
                 jitter: float = 0.05):
        self.source = source
        self.base_url = base_url.rstrip("/")
        self.storage = storage
        self.delay = delay
        if isinstance(timeout, tuple):
            self.timeout = timeout
        else:
            total = max(1.0, float(timeout))
            self.timeout = (min(8.0, total), total)
        self.retries = max(1, retries)
        self.jitter = max(0.0, float(jitter))
        self.timestamp_fn = timestamp_fn or make_timestamp
        self.last_request = 0.0
        self.last_error = ""
        self.transport_error = ""
        self._transport_error_at = 0.0
        self.last_response_status: int | None = None
        self.last_response_body: bytes | None = None
        self.last_response_error = ""
        self.impersonate = impersonate or (
            DEFAULT_IMPERSONATE if curl_requests is not None else None
        )
        self._siteb_bootstrap_lock = threading.Lock()
        self._request_lock = threading.Lock()
        self._siteb_session_ready = False
        self._siteb_bootstrap_retry_at = 0.0
        if self.impersonate and curl_requests is not None:
            self.session = curl_requests.Session(impersonate=self.impersonate)
        elif curl_requests is None and DEFAULT_IMPERSONATE:
            # 安全（全面优化审计）：curl_cffi 不可用时禁止静默回退裸
            # requests——python-requests 的 TLS 指纹/头序会暴露自动化
            # 客户端。宁可显式失败也不带错误指纹出门。
            raise RuntimeError(
                "curl_cffi unavailable: refusing to fall back to "
                "python-requests fingerprint")
        else:
            self.session = requests.Session()
        if hasattr(self.session, "trust_env"):
            self.session.trust_env = False
        self.session.headers.update({
            "User-Agent": CHROME_UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
            **CHROME_CLIENT_HINTS,
        })
        if headers:
            self.session.headers.update(headers)
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})
        if self._is_siteb_site_url(self.base_url):
            self.session.headers.update({
                "language": "zh_CN",
                "x-device": "1",
                "x-currency": "CNY",
                "x-app-version": "1.0.0",
            })
            self.session.headers.setdefault("Referer", f"{self.base_url}/")

    def _is_siteb_site_url(self, url: str) -> bool:
        return (
            self.source == "siteb"
            and urlsplit(url).netloc.lower() == "www.siteb.com"
        )

    def _siteb_device_id(self) -> str:
        cookies = self.session.cookies
        try:
            value = cookies.get("SDT_DeviceId")
        except Exception:
            value = None
        if value:
            return str(value)
        for cookie in cookies:
            if isinstance(cookie, str):
                if cookie != "SDT_DeviceId":
                    continue
                try:
                    value = cookies.get(cookie)
                except Exception:
                    value = None
                if value:
                    return str(value)
                continue
            if getattr(cookie, "name", "") == "SDT_DeviceId":
                value = getattr(cookie, "value", "")
                if value:
                    return str(value)
        return ""

    def _prepare_siteb_site_session(self, url: str) -> None:
        parsed = urlsplit(url)
        if not self._is_siteb_site_url(url) or not parsed.path.startswith("/api/"):
            return
        with self._siteb_bootstrap_lock:
            now = time.monotonic()
            if (
                not self._siteb_session_ready
                and now >= self._siteb_bootstrap_retry_at
            ):
                try:
                    response = self.session.request(
                        "GET",
                        f"{self.base_url}/",
                        timeout=self.timeout,
                    )
                    self.last_request = time.monotonic()
                    if 200 <= int(getattr(response, "status_code", 0)) < 500:
                        self._siteb_session_ready = True
                        self._siteb_bootstrap_retry_at = 0.0
                    else:
                        self._siteb_bootstrap_retry_at = (
                            time.monotonic() + 15.0
                        )
                except Exception as exc:
                    self.last_error = str(exc)
                    if self._terminal_transport_error(exc):
                        self.transport_error = self.last_error
                        self._transport_error_at = time.monotonic()
                    else:
                        self._siteb_bootstrap_retry_at = (
                            time.monotonic() + 5.0
                        )
                    log.debug("行情站B session preparation failed: %s", exc)
            if self.transport_error:
                return
            device_id = self._siteb_device_id() or str(uuid.uuid4())
            self.session.headers["x-device-id"] = device_id

    def _wait_for_request_slot(self) -> None:
        elapsed = time.monotonic() - self.last_request
        if elapsed < self.delay:
            time.sleep(
                # 拟人化节奏：对数正态长尾抖动（median=jitter、偶发数倍
                # 停顿）替代均匀 0-50ms——固定窄带间隔是可识别的机器特征
                self.delay - elapsed + self._human_jitter()
            )

    @staticmethod
    def _human_jitter() -> float:
        """对数正态抖动：median≈0.1s，p95≈0.31s，p99≈0.6s，截顶 2s。
        数学验证：lognormvariate(-2.3, 0.77) → exp(-2.3)=0.100 中位，
        p99=exp(-2.3+2.33*0.77)≈0.60（审查B校正：原 σ=1.2 的 p99 实为
        1.64s，礼貌预算偏重 2.7 倍）。"""
        return min(2.0, random.lognormvariate(-2.3, 0.77))

    @staticmethod
    def _retry_delay(response: Any, attempt: int) -> float:
        fallback = min(60.0, (2 ** attempt) * 3 + random.random())
        headers = getattr(response, "headers", {}) or {}
        value = str(headers.get("retry-after", "")).strip()
        if not value:
            return fallback
        try:
            return min(300.0, max(0.0, float(value)))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                return min(
                    300.0,
                    max(
                        0.0,
                        (retry_at - datetime.now(timezone.utc)).total_seconds(),
                    ),
                )
            except (TypeError, ValueError, IndexError):
                return fallback

    def _terminal_transport_error(self, exc: Exception) -> bool:
        if isinstance(exc, requests.exceptions.ProxyError):
            return True
        text = str(exc).lower()
        strong_markers = (
            "proxyerror",
            "unable to connect to proxy",
            "proxy connection",
            "wrong_version_number",
            "wrong version number",
            "unexpected_eof",
            "unexpected eof",
            "eof occurred",
            "connection reset",
            "connection aborted",
            "remote disconnected",
            "connection closed abruptly",
            "ssl_error_syscall",
            "tls connect error",
            "ssl_connect",
        )
        return classify_error(exc) == "transport" and any(
            marker in text for marker in strong_markers
        )

    def _siteb_timestamped(self, url: str, params: dict[str, Any] | None,
                             json_data: Any) -> tuple[dict[str, Any] | None, Any]:
        """Attach the checksum timestamp to www.siteb.com API requests.

        Mirrors the site frontend: POST bodies and parameterised requests
        carry the same checksum timestamp. The authorized Open API host
        (open.siteb.com), plain GETs without parameters, and other sources
        are left untouched (they are already served without it).
        """
        host = urlsplit(url).netloc
        if self.source != "siteb" or host != "www.siteb.com":
            return params, json_data
        ts = self.timestamp_fn()
        if isinstance(json_data, dict) or params is not None:
            params = dict(params) if params is not None else {}
            params["timestamp"] = ts
        if isinstance(json_data, dict):
            json_data = dict(json_data)
            json_data["timestamp"] = ts
        return params, json_data

    def _fuse_open(self) -> bool:
        """True while the transport fuse should short-circuit requests.

        The fuse used to latch forever, which turned one transient network
        blip into a permanently dead client until process restart. It now
        self-heals after TRANSPORT_FUSE_RESET_SECONDS and is cleared by any
        successful response.
        """
        if not self.transport_error:
            return False
        if time.monotonic() - self._transport_error_at >= TRANSPORT_FUSE_RESET_SECONDS:
            self.transport_error = ""
            self._transport_error_at = 0.0
            return False
        return True

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None,
                json_data: Any = None) -> tuple[int | None, Any | None]:
        url = path if path.startswith("http") else self.base_url + path
        if self._fuse_open():
            self.last_error = self.transport_error
            return None, None
        self._prepare_siteb_site_session(url)
        if self._fuse_open():
            self.last_error = self.transport_error
            return None, None
        base_params, base_json = params, json_data
        last_error = None
        for attempt in range(self.retries):
            # 每次尝试重新生成校验 timestamp：携带陈旧 timestamp 的重试会被
            # 站点以 errorCode=108 拒绝，瞬时限流就被升级成终态 protocol 票据。
            req_params, req_json = self._siteb_timestamped(url, base_params, base_json)
            try:
                with self._request_lock:
                    self._wait_for_request_slot()
                    if self.source == "sitea":
                        wait = self.storage.reserve_request_slot("sitea", 1.1)
                        if wait > 0:
                            time.sleep(wait)
                    elif self.source == "siteb":
                        url_path = urlsplit(url).path
                        if url_path.startswith("/open/"):
                            endpoint = url_path
                            interval = {
                                "/open/cs2/v1/price/single": 0.25,
                                "/open/cs2/v1/price/avg": 0.25,
                                "/open/cs2/item/v1/kline": 0.55,
                                "/open/cs2/v1/price/batch": 60.0,
                            }.get(endpoint)
                            if interval is not None:
                                # 限速槽按 key 分离：每个 key 有独立配额，
                                # 共享一个槽会让多 key 轮换失去意义。
                                auth = str(
                                    self.session.headers.get("Authorization", "")
                                    or ""
                                )
                                key_scope = (
                                    hashlib.md5(auth.encode("utf-8")).hexdigest()[:8]
                                    if auth
                                    else "anon"
                                )
                                wait = self.storage.reserve_request_slot(
                                    "siteb_open:" + endpoint + ":" + key_scope,
                                    interval,
                                )
                                if wait > 0:
                                    time.sleep(wait)
                        elif (
                            self._is_siteb_site_url(url)
                            and url_path.startswith("/api/")
                        ):
                            # 跨进程共享槽：多条站点车道并行时仍保持礼貌的聚合速率
                            wait = self.storage.reserve_request_slot(
                                "siteb_site", SITEB_SITE_SHARED_INTERVAL
                            )
                            if wait > 0:
                                time.sleep(wait)
                    response = self.session.request(
                        method,
                        url,
                        params=req_params,
                        json=req_json,
                        timeout=self.timeout,
                    )
                    self.last_request = time.monotonic()
                self.transport_error = ""
                self._transport_error_at = 0.0
                self.last_error = ""
                body = response.content
                self.last_response_status = response.status_code
                self.last_response_body = body
                self.last_response_error = ""
                capture_error = (
                    "HTTP 555: 上游公开页面暂时拒绝访问（内容审计命中风险规则）"
                    if response.status_code == 555
                    else None
                )
                request_data = (
                    req_json if req_json is not None else req_params
                )
                self.storage.save_capture(
                    self.source, method.upper(), response.url, response.status_code, body,
                    response.headers.get("content-type", ""),
                    request_data,
                    error=capture_error,
                )
                if response.status_code in {408, 425, 429, 500, 502, 503, 504}:
                    if attempt + 1 < self.retries:
                        time.sleep(self._retry_delay(response, attempt))
                        continue
                try:
                    return response.status_code, response.json()
                except (json.JSONDecodeError, requests.exceptions.JSONDecodeError):
                    self.last_response_error = (
                        capture_error
                        or f"HTTP {response.status_code}: non-JSON response"
                    )
                    if capture_error is None:
                        self.storage.save_capture(
                            self.source,
                            method.upper(),
                            response.url,
                            response.status_code,
                            body,
                            response.headers.get("content-type", ""),
                            request_data,
                            error=self.last_response_error,
                        )
                    return response.status_code, None
            except Exception as exc:
                if isinstance(exc, requests.RequestException) or \
                        (curl_requests is not None and isinstance(exc, curl_requests.RequestsError)):
                    last_error = str(exc)
                    if self._terminal_transport_error(exc):
                        self.transport_error = last_error
                        self._transport_error_at = time.monotonic()
                        break
                else:
                    raise
                if attempt + 1 < self.retries:
                    time.sleep(min(30, (2 ** attempt) * 2 + random.random()))
        self.last_error = last_error or "request failed"
        request_data = req_json if req_json is not None else req_params
        self.storage.save_capture(
            self.source,
            method.upper(),
            url,
            None,
            None,
            request_data=request_data,
            error=self.last_error,
        )
        log.error("%s %s failed: %s", method, url, last_error)
        return None, None

    def get(self, path: str, params: dict[str, Any] | None = None) -> tuple[int | None, Any | None]:
        return self.request("GET", path, params=params)

    def post(self, path: str, data: Any = None) -> tuple[int | None, Any | None]:
        return self.request("POST", path, json_data=data)
