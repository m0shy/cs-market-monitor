# -*- coding: utf-8 -*-
"""Security self-check for the collectors.

Verifies that the stealth browser context does not leak identifiable
information: ``navigator.webdriver``, languages/plugins spoofing, WebRTC
local/public IP candidates and the egress IP (direct vs proxy).

Run from the CLI:  python -m csgo_monitor security-check
"""

from __future__ import annotations

import logging
import re
import urllib.request
from typing import Any

from .browser import _stealth_context

log = logging.getLogger(__name__)

_IPV4_RE = re.compile(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b")
_PRIVATE_RE = re.compile(
    r"^(?:10\.|127\.|192\.168\.|169\.254\.|172\.(?:1[6-9]|2[0-9]|3[01])\.)"
)

FINGERPRINT_JS = """() => ({
    webdriver: navigator.webdriver,
    languages: navigator.languages,
    plugins: Array.from(navigator.plugins || []).map(p => p.name),
    hardwareConcurrency: navigator.hardwareConcurrency,
    deviceMemory: navigator.deviceMemory,
    platform: navigator.platform,
    userAgent: navigator.userAgent,
})"""

WEBRTC_JS = """() => new Promise((resolve) => {
    const results = [];
    let done = false;
    const finish = () => { if (!done) { done = true; resolve(results); } };
    try {
        const pc = new RTCPeerConnection({iceServers: [
            {urls: ['stun:stun.l.google.com:19302']},
            {urls: ['stun:stun1.l.google.com:19302']}
        ]});
        pc.onicecandidate = (event) => {
            if (event.candidate) {
                results.push({
                    type: event.candidate.type,
                    protocol: event.candidate.protocol,
                    address: event.candidate.address,
                    candidate: event.candidate.candidate
                });
            } else {
                finish();
            }
        };
        pc.oniceconnectionstatechange = () => {
            if (pc.iceConnectionState === 'failed') { finish(); }
        };
        pc.createDataChannel('probe');
        pc.createOffer().then((offer) => pc.setLocalDescription(offer)).catch(finish);
        setTimeout(finish, 8000);
    } catch (error) {
        results.push({error: String(error)});
        finish();
    }
})"""


def _http_get(url: str, proxy: str | None, timeout: float) -> str:
    """Fetch a URL with a browser-like TLS stack.

    公共出口探测端点（ipify/cloudflare）会按 TLS 指纹拒绝 Python 默认
    客户端；curl_cffi 模拟 Chrome 后稳定可用，回退到 urllib。
    """
    try:
        from curl_cffi import requests as curl_requests
        session = curl_requests.Session(impersonate="chrome131")
        kwargs = {"proxies": {"http": proxy, "https": proxy}} if proxy else {}
        response = session.get(url, timeout=timeout, **kwargs)
        return response.text
    except ImportError:
        handler = (
            urllib.request.ProxyHandler({"http": proxy, "https": proxy})
            if proxy else urllib.request.ProxyHandler({})
        )
        opener = urllib.request.build_opener(handler)
        request = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        with opener.open(request, timeout=timeout) as response:
            return response.read().decode("utf-8", errors="replace")


def _egress_ip(proxy: str | None, timeout: float) -> str | None:
    """Return the egress IPv4 seen by a public endpoint (or None on failure)."""
    probes = (
        ("ipify", "https://api.ipify.org"),
        ("cloudflare", "https://www.cloudflare.com/cdn-cgi/trace"),
    )
    for name, url in probes:
        try:
            body = _http_get(url, proxy, timeout)
            if name == "ipify":
                value = body.strip()
            else:
                match = re.search(r"(?m)^ip=(\S+)", body)
                value = match.group(1) if match else ""
            if value:
                return value
        except Exception as exc:
            log.debug("egress probe failed (%s, proxy=%s): %s", url, bool(proxy), exc)
    return None


def _egress_ipv6(proxy: str | None, timeout: float) -> str | None:
    """Return the egress IPv6 seen by a public endpoint.

    双栈环境下 IPv6 常常绕过只配置了 IPv4 的代理路径；没有 IPv6 路由时
    返回 None（正常）。
    """
    try:
        body = _http_get("https://api6.ipify.org", proxy, timeout)
        return body.strip() or None
    except Exception as exc:
        log.debug("ipv6 egress probe failed (proxy=%s): %s", bool(proxy), exc)
        return None


def _impersonation_posture() -> tuple[str, str]:
    """静态确认 PoliteClient 具备浏览器 TLS 模拟能力（curl_cffi 可用且
    默认启用）。返回 (status, detail)。"""
    try:
        from . import http as http_module
    except Exception as exc:
        return "error", f"http module import failed: {exc}"
    try:
        import curl_cffi  # noqa: F401
    except Exception:
        return ("fail",
                "curl_cffi not installed; requests fall back to the Python TLS "
                "fingerprint (clearly non-browser)")
    if not getattr(http_module, "DEFAULT_IMPERSONATE", ""):
        return "fail", "DEFAULT_IMPERSONATE is empty"
    return ("pass",
            f"curl_cffi present; PoliteClient defaults to "
            f"{http_module.DEFAULT_IMPERSONATE} with matching UA/Client-Hints")


def _classify_address(address: str, proxy: str | None) -> tuple[bool, str]:
    """Return (is_leak, note) for a WebRTC ICE candidate address."""
    if address.endswith(".local"):
        return False, "mDNS-obfuscated hostname (safe)"
    if not _IPV4_RE.match(address):
        return False, f"non-IPv4 address {address!r} (safe)"
    if _PRIVATE_RE.match(address):
        return True, f"private/local IPv4 exposed: {address}"
    if not proxy:
        return True, f"public IPv4 exposed without proxy: {address}"
    return False, f"public IPv4 via proxy (proxy egress): {address}"


def run_security_check(proxy: str | None = None, headless: bool = True,
                       timeout: float = 15.0) -> dict[str, Any]:
    """Run the full security self-check and return a machine-readable report."""
    checks: list[dict[str, str]] = []
    leaks: list[str] = []

    fingerprint: dict[str, Any] = {}
    webrtc: list[dict[str, str]] = []
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as playwright:
            ctx = _stealth_context(playwright, headless, proxy)
            page = ctx.new_page()
            fingerprint = page.evaluate(FINGERPRINT_JS)
            webrtc = page.evaluate(WEBRTC_JS)
            ctx.close()
    except Exception as exc:
        checks.append({"name": "browser_launch", "status": "error",
                       "detail": f"unable to launch stealth context: {exc}"})
        return {"proxy": proxy or "(direct)", "fingerprint": {}, "webrtc": [],
                "checks": checks, "leaks": leaks}

    # --- fingerprint checks -------------------------------------------------
    fp_issues: list[str] = []
    # 隐藏后 webdriver 应为 undefined（Python 侧 None）或 False；true = 暴露自动化
    if fingerprint.get("webdriver") not in (None, False):
        fp_issues.append("navigator.webdriver is exposed (true)")
    languages = fingerprint.get("languages")
    if not isinstance(languages, list) or not languages:
        fp_issues.append("navigator.languages is missing/empty")
    plugins = fingerprint.get("plugins")
    if not isinstance(plugins, list) or len(plugins) < 3:
        fp_issues.append("navigator.plugins looks empty (headless tell)")
    if fingerprint.get("hardwareConcurrency") != 8:
        fp_issues.append(f"hardwareConcurrency={fingerprint.get('hardwareConcurrency')} (real hardware exposed)")
    if fingerprint.get("deviceMemory") != 8:
        fp_issues.append(f"deviceMemory={fingerprint.get('deviceMemory')} (real memory exposed)")
    ua = str(fingerprint.get("userAgent") or "")
    if "HeadlessChrome" in ua:
        fp_issues.append("UA contains 'HeadlessChrome' (headless tell)")
    if fp_issues:
        leaks.append("fingerprint: " + "; ".join(fp_issues))
        checks.append({"name": "fingerprint", "status": "fail", "detail": "; ".join(fp_issues)})
    else:
        checks.append({"name": "fingerprint", "status": "pass",
                       "detail": "webdriver hidden; languages/plugins/concurrency/memory "
                                 "masked to ordinary desktop values; UA is plain Chrome"})

    # --- WebRTC checks -------------------------------------------------------
    candidates = [c for c in webrtc if "error" not in c]
    for candidate in candidates:
        address = str(candidate.get("address") or "")
        is_leak, note = _classify_address(address, proxy)
        if is_leak:
            leaks.append("webrtc: " + note)
        else:
            log.debug("webrtc candidate: %s", note)
    if any("error" in c for c in webrtc):
        checks.append({"name": "webrtc", "status": "error",
                       "detail": "RTCPeerConnection raised: " + str(webrtc[0].get("error"))})
    elif not candidates:
        checks.append({"name": "webrtc", "status": "pass",
                       "detail": "no ICE candidates gathered (STUN unreachable); nothing exposed"})
    elif leaks and any(l.startswith("webrtc:") for l in leaks):
        checks.append({"name": "webrtc", "status": "fail",
                       "detail": f"{len([l for l in leaks if l.startswith('webrtc:')])} candidate(s) exposed real IPs"})
    else:
        checks.append({"name": "webrtc", "status": "pass",
                       "detail": f"{len(candidates)} candidate(s), no real IPs exposed"})

    # --- egress checks --------------------------------------------------------
    direct_ip = _egress_ip(None, timeout)
    proxy_ip = _egress_ip(proxy, timeout) if proxy else None
    egress: dict[str, Any] = {"direct": direct_ip}
    if proxy:
        egress["proxy"] = proxy_ip
    if proxy:
        if direct_ip is None or proxy_ip is None:
            checks.append({"name": "egress_proxy", "status": "error",
                           "detail": "could not compare egress IPs (network restricted?)"})
        elif direct_ip == proxy_ip:
            leaks.append(f"egress: proxy does not change the public IP ({direct_ip})")
            checks.append({"name": "egress_proxy", "status": "fail",
                           "detail": f"direct == proxy == {direct_ip}"})
        else:
            checks.append({"name": "egress_proxy", "status": "pass",
                           "detail": f"direct={direct_ip} proxy={proxy_ip}"})
    else:
        checks.append({"name": "egress_direct", "status": "pass" if direct_ip else "error",
                       "detail": f"direct egress IP: {direct_ip}" if direct_ip
                       else "direct egress probe failed (network restricted?)"})

    # --- IPv6 egress ----------------------------------------------------------
    # 直连模式下 IPv6 出口同样是真实地址；代理模式下若本机有 IPv6 路由而
    # 代理不转发 IPv6，浏览器侧可能出现绕行。这里只记录观测值并提示。
    ipv6_direct = _egress_ipv6(None, timeout)
    egress["direct_v6"] = ipv6_direct
    if ipv6_direct and not proxy:
        checks.append({"name": "egress_ipv6", "status": "pass",
                       "detail": f"direct IPv6 egress observed: {ipv6_direct} "
                                  "(no proxy configured; shown for the record)"})
    elif ipv6_direct and proxy:
        checks.append({"name": "egress_ipv6", "status": "warn",
                       "detail": f"host has IPv6 route ({ipv6_direct}); ensure the "
                                  "browser/proxy path does not bypass the proxy for IPv6"})
    else:
        checks.append({"name": "egress_ipv6", "status": "pass",
                       "detail": "no IPv6 route observed"})

    # --- TLS/指纹一致性态势 -----------------------------------------------------
    status, detail = _impersonation_posture()
    checks.append({"name": "tls_impersonation", "status": status, "detail": detail})
    if status == "fail":
        leaks.append("tls: " + detail)

    return {
        "proxy": proxy or "(direct)",
        "fingerprint": fingerprint,
        "webrtc_candidates": webrtc,
        "egress": egress,
        "checks": checks,
        "leaks": leaks,
    }