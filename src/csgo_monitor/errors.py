from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import math
import re
from typing import Any


ERROR_CLASSES = {
    "transport",
    "rate_limit",
    "quota",
    "protocol",
    "authorization",
    "not_found",
    "parse",
    "data_gap",
    "audit",
    "local",
    "unknown",
}


# data_gap 连续重试这么多次后进入 data_limits 观察清单（exhausted）。
# 站点确实没有该数据的条目不该每 30 分钟被无限重试；reconcile/人工仍可复活。
DATA_GAP_MAX_ATTEMPTS = 8


def _payload_code(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    value = payload.get("errorCode", payload.get("code", ""))
    return str(value).strip().lower()


def _payload_message(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("errorMsg") or payload.get("message") or payload.get("msg") or "")


def classify_error(
    error: Any = "",
    *,
    status: int | None = None,
    payload: Any = None,
) -> str:
    explicit = getattr(error, "error_class", "")
    if explicit in ERROR_CLASSES:
        return explicit
    if status is None:
        status = getattr(error, "status", None)
    code = _payload_code(payload)
    text = " ".join(
        (
            type(error).__name__,
            str(error or ""),
            _payload_message(payload),
            f"http {status}" if status is not None else "",
            f"code {code}" if code else "",
        )
    ).lower()
    if status is None:
        match = re.search(
            r"\b(?:http[\s:=]*|code[\s=]+|status[\s:=]*)([45]\d{2})\b",
            text,
        )
        if match:
            status = int(match.group(1))

    if status == 555 or code == "555" or any(
        marker in text
        for marker in (
            "内容审计",
            "风险规则",
            "内容安全",
            "内容审核",
            "content audit",
            "risk rule",
            "risk control",
            "content review",
        )
    ):
        return "audit"

    # 行情站B 站点风控：err401“当前访问环境异常”绑定出口 IP 环境，换节点即可恢复，
    # 不是凭据问题，必须可重试；判定要先于 authorization，避免被归为永久 blocked。
    if "环境异常" in text or "err401" in text:
        return "transport"
    if status in {401, 403} or code in {"401", "403"} or any(
        marker in text
        for marker in (
            "http401",
            "http403",
            "unauthorized",
            "forbidden",
            "authorization",
            "authentication",
            "permission denied",
            "security verification",
            "captcha",
            "安全验证",
            "请先登录",
            "需要登录",
            "无权限",
        )
    ):
        return "authorization"
    if status == 404 or code == "404" or any(
        marker in text
        for marker in (
            "http404",
            "not found",
            "does not exist",
            "no itemid",
            "missing object",
            "数据不存在",
        )
    ):
        return "not_found"
    if status == 429 or code in {"107", "429", "4005"} or any(
        marker in text
        for marker in (
            "http429",
            "rate limit",
            "rate-limit",
            "rate_limit pause",
            "too many requests",
            "请求过于频繁",
            "访问过于频繁",
            "1 分钟后再试",
            "限流",
            "err107",
        )
    ):
        return "rate_limit"
    if code == "106" or any(
        marker in text
        for marker in (
            "quota-exhausted",
            "kline-quota",
            "quota pause",
            "daily quota",
            "err106",
            "访问次数已达上限",
            "配额",
        )
    ):
        return "quota"
    if any(
        marker in text
        for marker in (
            "database is locked",
            "database is busy",
            "database table is locked",
            "disk i/o error",
        )
    ):
        # 本地 SQLite 写竞争不是网络故障：不要当成 transport 让监督器
        # 按网络退避延迟整个任务，短间隔重试即可。
        return "local"
    if any(
        marker in text
        for marker in (
            "initial kline head is empty",
            "initial kline page is empty",
            "metric/platform groups",
            "one-year span",
            "一年跨度",
            "指标/平台",
        )
    ):
        return "data_gap"
    if code == "108" or any(
        marker in text
        for marker in (
            "invalid timestamp",
            "checksum",
            "repeated nextid",
            "data is not an object",
            "non-object payload",
            "protocol:",
            "protocol_ticket",
            "pipeline did not become ready",
        )
    ):
        return "protocol"
    if any(
        marker in text
        for marker in (
            "jsondecode",
            "decode error",
            "parse error",
            "unexpected payload",
            "schema mismatch",
        )
    ):
        return "parse"
    if status == 408 or (status is not None and 500 <= status < 600) or code in {
        "408", "500", "502", "503", "504",
    } or any(
        f"http{value}" in text for value in (408, 500, 502, 503, 504)
    ):
        return "transport"
    if any(
        marker in text
        for marker in (
            "proxyerror",
            "unable to connect to proxy",
            "transport unavailable",
            "connection refused",
            "proxy connection",
            "err_ssl_protocol_error",
            "err_proxy_connection_failed",
            "err_connection_reset",
            "err_connection_closed",
            "err_timed_out",
            "err_tunnel_connection_failed",
            "err_name_not_resolved",
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
            "connection timed out",
            "connect timeout",
            "read timed out",
            "timed out",
            "failed to establish a new connection",
            "failed to fetch",
            "name resolution",
            "temporary failure in name resolution",
            "network-fuse",
            "network",
            "http none",
            "responseerror: http error",
            "no-node",
        )
    ):
        return "transport"
    if "exhausted" in text:
        # 请求池/重试预算耗尽是瞬态过载，不是数据终态；
        # “quota-exhausted”“kline-quota”已在上方 quota 分支先行命中。
        return "transport"
    if "history_metrics=" in text or text.strip().startswith("str full="):
        return "data_gap"
    return "unknown"


def retry_after_seconds(value: Any, *, now: datetime | None = None) -> float:
    current = now or datetime.now(timezone.utc)
    try:
        seconds = float(value)
        return max(0.0, seconds) if math.isfinite(seconds) else 0.0
    except (TypeError, ValueError):
        try:
            target = parsedate_to_datetime(str(value))
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            return max(0.0, (target - current).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0.0


def failure_status(error_class: str, attempts: int) -> str:
    normalized = error_class if error_class in ERROR_CLASSES else "unknown"
    if normalized == "audit":
        return "deferred"
    if normalized == "authorization":
        return "blocked"
    if normalized == "not_found":
        return "not_found"
    if normalized == "protocol":
        return "protocol_ticket"
    if normalized == "data_gap":
        # 站点确实缺数据的条目在有限重试后进入 data_limits 观察清单，
        # 不再无限 churn；后续 reconcile 或站点补数据时可复活。
        return "exhausted" if attempts >= DATA_GAP_MAX_ATTEMPTS else "retryable"
    if normalized in {"transport", "rate_limit", "quota", "audit", "local"}:
        return "deferred"
    if attempts >= 3:
        return "exhausted"
    return "retryable"


def next_retry_at(
    error_class: str,
    attempts: int,
    *,
    now: datetime | None = None,
) -> str | None:
    normalized = error_class if error_class in ERROR_CLASSES else "unknown"
    if failure_status(normalized, attempts) in {
        "blocked",
        "not_found",
        "protocol_ticket",
        "exhausted",
    }:
        return None
    delays = {
        "transport": min(600, 60 * (2 ** max(0, attempts - 1))),
        "rate_limit": min(3600, 300 * (2 ** max(0, attempts - 1))),
        "quota": 3600,
        "audit": min(21600, 1800 * (2 ** max(0, attempts - 1))),
        "parse": min(1800, 300 * max(1, attempts)),
        "data_gap": min(1800, 300 * max(1, attempts)),
        "local": 15,
        "unknown": min(900, 60 * max(1, attempts)),
    }
    current = now or datetime.now(timezone.utc)
    return (current + timedelta(seconds=delays.get(normalized, 300))).isoformat()
