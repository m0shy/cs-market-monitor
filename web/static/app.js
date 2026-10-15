// CS2 饰品监控 - 前端脚本 v3（深色金融终端 · 对标 行情站B 交互）
// 全屏搜索 / ticker / K线周期 / 价格卡片 / 市场宽度 / 相对时间 / 双层榜单
(function () {
  "use strict";

  function el(id) { return document.getElementById(id); }
  function setText(id, v) { var x = el(id); if (x) x.textContent = (v == null || v === "") ? "-" : v; }
  var REQUESTS = {};
  function cancelRequest(key) {
    var request = key && REQUESTS[key];
    if (!request) return;
    if (request.controller) request.controller.abort();
    delete REQUESTS[key];
  }
  function json(url, cb, fail, key) {
    var requestKey = key || "";
    var previous = requestKey && REQUESTS[requestKey];
    if (previous && previous.controller) previous.controller.abort();

    var controller = window.AbortController ? new AbortController() : null;
    var timedOut = false;
    var request = { controller: controller };
    if (requestKey) REQUESTS[requestKey] = request;
    var timer = window.setTimeout(function () {
      timedOut = true;
      if (controller) controller.abort();
    }, 15000);
    function isCurrent() {
      return !requestKey || REQUESTS[requestKey] === request;
    }
    function finish() {
      window.clearTimeout(timer);
      if (requestKey && REQUESTS[requestKey] === request) delete REQUESTS[requestKey];
    }
    fetch(url, {
      headers: { "Accept": "application/json" },
      cache: "no-store",
      credentials: "same-origin",
      signal: controller ? controller.signal : undefined
    })
      .then(function (r) {
        if (!r.ok) throw new Error("HTTP " + r.status);
        return r.json();
      })
      .then(function (data) {
        if (isCurrent()) cb(data);
      })
      .catch(function (error) {
        if (!isCurrent()) return;
        if (error && error.name === "AbortError" && !timedOut) return;
        if (fail) fail(error);
        else console.error("Request failed:", url, error);
      })
      .then(finish, finish);
    return request;
  }
  function bindImageFallback(root) {
    (root || document).querySelectorAll("img").forEach(function (img) {
      if (img.dataset.fallbackBound === "true") return;
      img.dataset.fallbackBound = "true";
      function replaceBrokenImage() {
        if (!img.parentNode) return;
        var icon = document.createElement("i");
        icon.setAttribute("data-lucide", "package");
        icon.setAttribute("aria-hidden", "true");
        icon.className = "image-fallback-icon";
        img.replaceWith(icon);
        if (window.lucide && typeof window.lucide.createIcons === "function") {
          window.lucide.createIcons({
            attrs: { "aria-hidden": "true" },
            root: icon.parentNode
          });
        }
      }
      img.addEventListener("error", replaceBrokenImage, { once: true });
      if (img.complete && img.naturalWidth === 0) replaceBrokenImage();
    });
  }
  function refreshIcons(root) {
    bindImageFallback(root);
    if (window.lucide && typeof window.lucide.createIcons === "function") {
      window.lucide.createIcons({ attrs: { "aria-hidden": "true" }, root: root || document });
    }
  }

  var REDUCED = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  // ---------- 工具 ----------
  function esc(s) {
    return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }
  function fmt(v, d) {
    if (v == null || isNaN(v)) return "-";
    return Number(v).toFixed(d == null ? 2 : d);
  }
  function fmtI(v) {
    if (v == null || isNaN(v)) return "-";
    return Math.round(v).toLocaleString();
  }
  function fmtMetric(v, decimals) {
    if (v == null || v === "" || isNaN(v)) return "-";
    var places = decimals == null ? 0 : decimals;
    return Number(v).toLocaleString("en-US", {
      minimumFractionDigits: places,
      maximumFractionDigits: places
    });
  }
  function diffCls(v) { return v == null || isNaN(v) ? "" : (v >= 0 ? "up" : "down"); }
  function diffTxt(v) {
    if (v == null || isNaN(v)) return "-";
    return (v >= 0 ? "+" : "") + Number(v).toFixed(2) + "%";
  }
  function chip(v) {
    if (v == null || isNaN(v)) return "-";
    return "<span class='chip-num " + diffCls(v) + "'>" + diffTxt(v) + "</span>";
  }
  function itemLink(name) {
    return "<a class='item-name' href='/market/items/" + encodeURIComponent(name) + "'>" + esc(name) + "</a>";
  }
  function imageUrl(profile) {
    var url = profile && profile.image_url;
    return url ? "/api/image?url=" + encodeURIComponent(url) : "";
  }
  function itemCell(record, from, tiny) {
    var name = record && record.name ? record.name : "";
    var profile = record && record.profile ? record.profile : {};
    var img = imageUrl(profile);
    var thumb = img
      ? "<img src='" + esc(img) + "' alt='' loading='lazy'>"
      : "<i data-lucide='package' aria-hidden='true'></i>";
    var href = "/market/items/" + encodeURIComponent(name) + (from ? "?from=" + encodeURIComponent(from) : "");
    return "<a class='item-cell' href='" + href + "'>"
      + "<span class='item-thumb " + (tiny ? "tiny" : "") + "'>" + thumb + "</span>"
      + "<span><strong>" + esc(name) + "</strong><small>" + esc(profile.rarity || "CS2 饰品") + "</small></span></a>";
  }
  function rankNum(i) {
    var cls = i < 3 ? "r" + (i + 1) : "";
    return "<span class='rank-num " + cls + "'>" + (i + 1) + "</span>";
  }
  function touch(id) {
    var x = el(id);
    if (!x) return;
    var n = new Date();
    x.textContent = "更新于 " + n.getHours() + ":" + String(n.getMinutes()).padStart(2, "0")
      + ":" + String(n.getSeconds()).padStart(2, "0");
  }
  function parseTs(ts) {
    var s = String(ts == null ? "" : ts).trim();
    if (!s) return null;
    var iso = s.indexOf("T") === -1 ? s.replace(" ", "T") : s;
    return new Date(iso);
  }
  var MARKET_TIME_FORMAT = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", hourCycle: "h23"
  });
  function marketTimestamp(value) {
    if (typeof value === "number") return value;
    var timestamp = String(value == null ? "" : value).trim().replace(" ", "T");
    if (/^\d{4}-\d{2}-\d{2}$/.test(timestamp)) timestamp += "T00:00:00+08:00";
    else if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/.test(timestamp)) {
      timestamp += "+08:00";
    }
    return Date.parse(timestamp);
  }
  function marketDateLabel(value, includeTime) {
    if (value == null || value === "") return "-";
    var timestamp = String(value).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(timestamp)) return timestamp;
    var instant = new Date(marketTimestamp(value));
    if (isNaN(instant.getTime())) return timestamp;
    var parts = {};
    MARKET_TIME_FORMAT.formatToParts(instant).forEach(function (part) {
      parts[part.type] = part.value;
    });
    var label = parts.year + "-" + parts.month + "-" + parts.day;
    return includeTime ? label + " " + parts.hour + ":" + parts.minute : label;
  }
  function reltime(ts) {
    var t = parseTs(ts);
    if (!t || isNaN(t.getTime())) return ts ? String(ts).slice(0, 16) : "-";
    var diff = (Date.now() - t.getTime()) / 1000;
    if (diff < 60) return "刚刚";
    if (diff < 3600) return Math.floor(diff / 60) + " 分钟前";
    if (diff < 86400) return Math.floor(diff / 3600) + " 小时前";
    /* 超过 48 小时改用绝对时间：相对跨度太大失去参考意义 */
    if (diff < 86400 * 2) return Math.floor(diff / 86400) + " 天前";
    var pad = function (n) { return (n < 10 ? "0" : "") + n; };
    return t.getFullYear() + "-" + pad(t.getMonth() + 1) + "-" + pad(t.getDate())
      + " " + pad(t.getHours()) + ":" + pad(t.getMinutes());
  }
  function processReltime(root) {
    (root || document).querySelectorAll(".reltime[data-ts]").forEach(function (x) {
      var ts = x.getAttribute("data-ts");
      x.textContent = reltime(ts);
      x.title = ts ? String(ts).slice(0, 16) : "";
    });
  }
  function readViewState(key, fallback) {
    try {
      var raw = localStorage.getItem("cs2m.view." + key);
      if (!raw) return fallback || {};
      var value = JSON.parse(raw);
      return value && typeof value === "object" ? value : (fallback || {});
    } catch (e) {
      return fallback || {};
    }
  }
  function writeViewState(key, value) {
    try {
      localStorage.setItem("cs2m.view." + key, JSON.stringify(value || {}));
    } catch (e) {}
  }
  function validIndicators(value, allowed) {
    return Array.isArray(value)
      ? value.filter(function (item, index, list) {
        return allowed.indexOf(item) !== -1 && list.indexOf(item) === index;
      })
      : [];
  }
  function syncDataInputs(selector, dataKey, values) {
    var selected = values || [];
    document.querySelectorAll(selector).forEach(function (input) {
      input.checked = selected.indexOf(input.dataset[dataKey]) !== -1;
    });


  }

  // ---------- 顶部导航 ----------
  var navToggle = el("nav-toggle");
  var navBackdrop = el("nav-backdrop");
  var navMore = el("nav-more");
  function closeNavMore() {
    if (navMore) navMore.removeAttribute("open");
  }
  function closeMobileNav() {
    document.body.classList.remove("nav-open");
    closeNavMore();
    if (navToggle) {
      navToggle.setAttribute("aria-expanded", "false");
      navToggle.setAttribute("aria-label", "展开导航");
    }
  }
  if (navToggle) {
    navToggle.addEventListener("click", function () {
      var open = document.body.classList.toggle("nav-open");
      navToggle.setAttribute("aria-expanded", open ? "true" : "false");
      navToggle.setAttribute("aria-label", open ? "收起导航" : "展开导航");
    });
    document.querySelectorAll("#main-nav a").forEach(function (link) {
      link.addEventListener("click", function () {
        closeMobileNav();
      });
    });
  }
  if (navBackdrop) navBackdrop.addEventListener("click", closeMobileNav);
  document.addEventListener("click", function (event) {
    if (navMore && !event.target.closest("#nav-more")) closeNavMore();
  });
  refreshIcons();

  // ---------- 平台画像 ----------
  var ROLE_MAP = {
    buff: ["参考价源", "secondary"],
    youpin: ["导购 + 租赁", "focus"],
    c5: ["导购 + 租赁", "focus"],
    steam: ["倒余额 + 导购", "focus"],
    haloskins: ["海外导购", "focus"],
    waxpeer: ["海外", "other"], skinport: ["海外", "other"],
    dmarket: ["海外", "other"], csmoney: ["海外", "other"], all: ["全站", "other"]
  };
  function roleOf(p) { return ROLE_MAP[p] || [p, "other"]; }
  function roleBadge(p) {
    var r = roleOf(p);
    return "<span class='badge badge-" + r[1] + "'>" + esc(p) + " · " + esc(r[0]) + "</span>";
  }
  function metricTag(m) {
    var labels = {
      sell_price: "在售价", buy_price: "求购价",
      sell_num: "在售量", buy_num: "求购量"
    };
    return "<span class='metric-tag'>" + esc(labels[m] || m || "未知指标") + "</span>";
  }
  function eventValue(metric, value) {
    return metric === "sell_price" || metric === "buy_price" ? fmt(value) : fmtI(value);
  }

  // ---------- 图表 ----------
  var CHART_INSTANCES = {};
  function resizeChart(domId) {
    var chart = CHART_INSTANCES[domId];
    if (!chart) return;
    window.requestAnimationFrame(function () {
      try { chart.resize(); } catch (e) {}
    });
  }
  function disposeChart(domId, clearDom) {
    var dom = el(domId);
    var chart = CHART_INSTANCES[domId]
      || (dom && window.echarts && echarts.getInstanceByDom(dom));
    if (dom && dom.__chartResizeObserver) {
      dom.__chartResizeObserver.disconnect();
      dom.__chartResizeObserver = null;
    }
    if (chart && (!chart.isDisposed || !chart.isDisposed())) {
      try { chart.dispose(); } catch (e) {}
    }
    delete CHART_INSTANCES[domId];
    if (dom && clearDom) dom.innerHTML = "";
  }
  function initChart(domId) {
    var dom = el(domId);
    if (!dom) return null;
    disposeChart(domId, true);
    var chart = echarts.init(dom, "dark");
    CHART_INSTANCES[domId] = chart;
    return chart;
  }
  function bindChartResize(dom, chart) {
    if (!dom || !chart) return;
    if (dom.__chartResizeObserver) dom.__chartResizeObserver.disconnect();
    if (window.ResizeObserver) {
      dom.__chartResizeObserver = new ResizeObserver(function () { chart.resize(); });
      dom.__chartResizeObserver.observe(dom);
    } else {
      window.addEventListener("resize", function () { chart.resize(); }, { once: true });
    }
  }
  function chartDone(container) {
    var loading = container ? container.querySelector(".chart-loading") : null;
    if (loading) {
      loading.classList.add("hidden");
      loading.classList.remove("error");
      loading.innerHTML = "";
    }
  }
  function chartState(container, message, retry) {
    var loading = container ? container.querySelector(".chart-loading") : null;
    if (!loading) return;
    loading.classList.remove("hidden");
    loading.classList.toggle("error", !!retry);
    loading.innerHTML = "<span>" + esc(message || "数据加载失败") + "</span>";
    if (retry) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "btn sm chart-retry";
      button.textContent = "重试";
      button.addEventListener("click", retry);
      loading.appendChild(button);
    }
  }

  function chartActionTarget(button) {
    return button.getAttribute("data-chart-target") || "";
  }

  function resetChartZoom(targetId) {
    var chart = CHART_INSTANCES[targetId];
    if (!chart) return;
    CHART_ZOOM[targetId] = null;
    chart.dispatchAction({ type: "dataZoom", start: 0, end: 100 });
  }

  function exportChartImage(targetId) {
    var chart = CHART_INSTANCES[targetId];
    if (!chart) return;
    var link = document.createElement("a");
    link.download = "cs2-market-" + targetId + ".png";
    link.href = chart.getDataURL({
      type: "png",
      pixelRatio: 2,
      backgroundColor: "#141922"
    });
    link.click();
  }

  function toggleChartFullscreen(targetId) {
    var target = el(targetId);
    if (!target) return;
    if (document.fullscreenElement === target) {
      if (document.exitFullscreen) {
        var exit = document.exitFullscreen();
        if (exit && exit.catch) exit.catch(function () {});
      }
      return;
    }
    if (target.requestFullscreen) {
      var request = target.requestFullscreen();
      if (request && request.catch) request.catch(function () {});
    }
  }

  function resetPopoverLayout(panel) {
    if (!panel) return;
    panel.classList.remove("popover-up", "popover-align-right");
    panel.style.removeProperty("max-height");
    panel.style.removeProperty("overflow-y");
  }

  function fitPopover(panel, trigger) {
    if (!panel || !trigger || panel.classList.contains("hidden")) return;
    resetPopoverLayout(panel);
    if (window.innerWidth <= 720) return;

    var edge = 12;
    var triggerRect = trigger.getBoundingClientRect();
    var panelRect = panel.getBoundingClientRect();
    var naturalHeight = Math.max(panel.scrollHeight || 0, panelRect.height || 0);
    if (panelRect.right > window.innerWidth - edge) {
      panel.classList.add("popover-align-right");
      panelRect = panel.getBoundingClientRect();
    }

    var below = Math.max(0, window.innerHeight - triggerRect.bottom - edge);
    var above = Math.max(0, triggerRect.top - edge);
    var sticky = document.querySelector(".item-summary-bar");
    var stickyRect = sticky ? sticky.getBoundingClientRect() : null;
    if (stickyRect && stickyRect.bottom > edge
      && stickyRect.top < triggerRect.top && stickyRect.bottom < triggerRect.top) {
      above = Math.max(0, triggerRect.top - stickyRect.bottom - 7);
    }
    var openAbove = above > below && naturalHeight > below;
    var available = openAbove ? above : below;
    if (!available && above) {
      openAbove = true;
      available = above;
    }
    if (openAbove) {
      panel.classList.add("popover-up");
    }
    panel.style.maxHeight = Math.max(1, Math.floor(available - 8)) + "px";
    panel.style.overflowY = "auto";
  }

  function setChartPopover(panel, trigger, open) {
    if (!panel || !trigger) return;
    panel.__popoverTrigger = trigger;
    panel.classList.toggle("hidden", !open);
    trigger.setAttribute("aria-expanded", open ? "true" : "false");
    if (!open) {
      resetPopoverLayout(panel);
      return;
    }
    window.requestAnimationFrame(function () {
      fitPopover(panel, trigger);
      var focusable = panel.querySelector(
        "button:not([disabled]), input:not([disabled]), [href], [tabindex]:not([tabindex='-1'])"
      );
      if (focusable) focusable.focus({ preventScroll: true });
    });
  }
  // Every chart workspace has the same exclusive menu, outside-click and
  // Escape behavior. Keep the page-specific controls in their own workspaces.
  function bindChartMenus(pairs, scope, closeButtonId) {
    function close(except) {
      pairs.forEach(function (pair) {
        if (pair[0] !== except) setChartPopover(el(pair[0]), el(pair[1]), false);
      });
    }
    pairs.forEach(function (pair) {
      var trigger = el(pair[1]);
      if (!trigger) return;
      trigger.addEventListener("click", function () {
        var panel = el(pair[0]);
        var open = panel && panel.classList.contains("hidden");
        close(pair[0]);
        setChartPopover(panel, trigger, open);
      });
    });
    var closeButton = el(closeButtonId);
    if (closeButton) closeButton.addEventListener("click", function () {
      var pair = pairs.find(function (entry) { return entry[0].indexOf("indicator") !== -1; });
      if (!pair) return;
      setChartPopover(el(pair[0]), el(pair[1]), false);
      el(pair[1]).focus({ preventScroll: true });
    });
    document.addEventListener("click", function (event) {
      if (!event.target.closest(scope + " .chart-menu-wrap")) close();
    });
    document.addEventListener("keydown", function (event) {
      if (event.key !== "Escape") return;
      var active = pairs.find(function (pair) {
        var panel = el(pair[0]);
        return panel && !panel.classList.contains("hidden");
      });
      close();
      if (active && el(active[1])) el(active[1]).focus({ preventScroll: true });
    });
    return close;
  }
  var popoverFitFrame = 0;
  function refitOpenPopovers() {
    if (popoverFitFrame) return;
    popoverFitFrame = window.requestAnimationFrame(function () {
      popoverFitFrame = 0;
      document.querySelectorAll(".chart-popover:not(.hidden)").forEach(function (panel) {
        fitPopover(panel, panel.__popoverTrigger);
      });
    });
  }
  window.addEventListener("resize", refitOpenPopovers);
  window.addEventListener("scroll", refitOpenPopovers, { passive: true });

  function activityCount(points, label) {
    return (points || []).filter(function (point) {
      var value = label === "在售量" ? point.listing_count : point.volume;
      return value != null && isFinite(Number(value)) && Number(value) > 0;
    }).length;
  }

  function usableSubIndicators(points, requested, label) {
    var count = activityCount(points, label);
    var total = (points || []).length;
    return (requested || []).filter(function (indicator) {
      if (indicator === "volume") {
        // Sparse snapshots are kept in coverage metadata, but do not create a
        // misleading empty panel in the chart viewport.
        return count > 0 && (total <= 60 || count >= Math.max(3, Math.ceil(total * 0.05)));
      }
      if (indicator === "rsi") return total >= 15;
      if (indicator === "volatility") return total >= 30;
      return total >= 2;
    });
  }
  function indicatorReady(points, indicator, label) {
    var total = (points || []).length;
    if (!total) return true;
    if (indicator === "ma") return total >= 5;
    if (indicator === "ema" || indicator === "macd") return total >= 2;
    if (indicator === "boll" || indicator === "volatility") return total >= 30;
    if (indicator === "rsi") return total >= 15;
    if (indicator === "volume") return activityCount(points, label) > 0;
    return true;
  }
  function syncIndicatorInputGroup(selector, dataKey, values, points, label) {
    var selected = values || [];
    document.querySelectorAll(selector).forEach(function (input) {
      var ready = indicatorReady(points, input.dataset[dataKey], label);
      input.checked = selected.indexOf(input.dataset[dataKey]) !== -1;
      input.disabled = !ready;
      var wrapper = input.closest("label");
      if (wrapper) wrapper.classList.toggle("disabled", !ready);
    });
  }

  var CHART_ZOOM = {};

  function chartIndicators(points) {
    var closes = points.map(function (p) { return Number(p.close); });
    function ma(period) {
      return closes.map(function (_, index) {
        if (index < period - 1) return null;
        var total = 0;
        for (var i = index - period + 1; i <= index; i++) total += closes[i];
        return +(total / period).toFixed(4);
      });
    }
    function ema(values, period) {
      var alpha = 2 / (period + 1);
      var current = null;
      return values.map(function (value) {
        current = current == null ? value : alpha * value + (1 - alpha) * current;
        return +current.toFixed(4);
      });
    }
    var ema12 = ema(closes, 12);
    var ema26 = ema(closes, 26);
    var dif = closes.map(function (_, i) { return +(ema12[i] - ema26[i]).toFixed(4); });
    var dea = ema(dif, 9);
    var macd = dif.map(function (value, i) { return +((value - dea[i]) * 2).toFixed(4); });
    var bollMid = ma(30);
    var bollUpper = [], bollLower = [], volatility = [];
    closes.forEach(function (_, index) {
      if (index < 29) {
        bollUpper.push(null);
        bollLower.push(null);
        volatility.push(null);
        return;
      }
      var windowValues = closes.slice(index - 29, index + 1);
      var mean = bollMid[index];
      var variance = windowValues.reduce(function (sum, value) {
        return sum + Math.pow(value - mean, 2);
      }, 0) / windowValues.length;
      var std = Math.sqrt(variance);
      bollUpper.push(+(mean + 2 * std).toFixed(4));
      bollLower.push(+(mean - 2 * std).toFixed(4));
      volatility.push(mean ? +(std / mean * 100).toFixed(3) : null);
    });
    var rsi = closes.map(function (_, index) {
      if (index < 14) return null;
      var gains = 0, losses = 0;
      for (var i = index - 13; i <= index; i++) {
        var change = closes[i] - closes[i - 1];
        if (change >= 0) gains += change;
        else losses -= change;
      }
      if (!losses) return 100;
      return +(100 - 100 / (1 + gains / losses)).toFixed(3);
    });
    return {
      ma5: ma(5), ma10: ma(10), ma20: ma(20),
      ema12: ema12, ema26: ema26,
      bollMid: bollMid, bollUpper: bollUpper, bollLower: bollLower,
      dif: dif, dea: dea, macd: macd, rsi: rsi, volatility: volatility
    };
  }

  function chartTooltip(params, timestamp) {
    if (!params || !params.length) return "";
    var stamp = timestamp == null ? params[0].axisValue : timestamp;
    if (stamp == null) stamp = params[0].axisValueLabel || params[0].name || "";
    var lines = ["<strong>" + esc(marketDateLabel(stamp, true)) + "（北京时间）</strong>"];
    params.forEach(function (param) {
      var value = param.value;
      if (value == null || value === "-" || (Array.isArray(value) && !value.length)) return;
      if (param.seriesType === "candlestick") {
        var offset = value.length === 5 ? 1 : 0;
        lines.push(param.marker + esc(param.seriesName)
          + "<span class='tip-values'>开 " + fmt(value[offset]) + "　收 " + fmt(value[offset + 1])
          + "　低 " + fmt(value[offset + 2]) + "　高 " + fmt(value[offset + 3]) + "</span>");
        return;
      }
      if (typeof value === "object" && value.value != null) value = value.value;
      if (Array.isArray(value)) value = value[1];
      if (value == null || isNaN(value)) return;
      var integer = /成交量|在售量/.test(param.seriesName);
      lines.push(param.marker + esc(param.seriesName)
        + "<span class='tip-values'>" + (integer ? fmtI(value) : fmt(value, Math.abs(value) < 10 ? 3 : 2)) + "</span>");
    });
    return lines.join("<br>");
  }

  // 多面板行情图：主图指标叠加，副图共享时间轴。
  function klineChart(domId, points, opts) {
    opts = opts || {};
    var dom = el(domId);
    if (!dom || !points.length) return null;
    var chart = initChart(domId);
    var legacyMa = opts.ma == null ? [5, 10, 20] : opts.ma;
    var mainIndicators = opts.mainIndicators || (legacyMa.length ? ["ma"] : []);
    var mode = opts.mode || "candlestick";
    var priceUnit = opts.priceUnit == null ? "¥" : String(opts.priceUnit);
    var calendarScale = !!opts.calendarScale;
    var activityLabel = opts.activityLabel || "成交量";
    var requestedSubIndicators = opts.subIndicators || (opts.volume === false ? [] : ["volume"]);
    var subIndicators = usableSubIndicators(points, requestedSubIndicators, activityLabel);
    var dates = points.map(function (p) { return String(p.ts); });
    var ohlc = points.map(function (p) {
      return p.open == null || p.high == null || p.low == null
        ? "-" : [p.open, p.close, p.low, p.high];
    });
    var closes = points.map(function (p) { return Number(p.close); });
    var vols = points.map(function (p, index) {
      var value = activityLabel === "在售量" ? p.listing_count : p.volume;
      var rising = mode === "line"
        ? index === 0 || p.close >= points[index - 1].close
        : p.close >= p.open;
      return { value: value == null ? 0 : value,
        itemStyle: { color: rising ? "rgba(255,91,112,.58)" : "rgba(36,201,139,.58)" } };
    });
    var calculated = chartIndicators(points);
    var hasObservedRanges = mode === "candlestick" && points.some(function (p) {
      return p.sample && p.sample.indexOf("小时指数观测聚合") === 0;
    });
    var mainLegend = [mode === "candlestick"
      ? (hasObservedRanges ? "站点 K 线" : (opts.priceLabel || "价格")) : (opts.priceLabel || "平台卖价")];
    var series = [];
    if (mode === "candlestick") {
      series.push({ name: hasObservedRanges ? "站点 K 线" : (opts.priceLabel || "价格"),
        type: "candlestick",
        data: hasObservedRanges ? ohlc.map(function (value, index) {
          return points[index].sample && points[index].sample.indexOf("小时指数观测聚合") === 0
            ? "-" : value;
        }) : ohlc,
        itemStyle: { color: "#ff3b3b", color0: "#00b063", borderColor: "#ff3b3b", borderColor0: "#00b063" } });
      if (hasObservedRanges) {
        mainLegend.push("小时指数观测区间");
        series.push({
          name: "小时指数观测区间", type: "candlestick",
          data: ohlc.map(function (value, index) {
            return points[index].sample && points[index].sample.indexOf("小时指数观测聚合") === 0
              ? value : "-";
          }),
          itemStyle: {
            color: "rgba(255,173,89,.78)", color0: "rgba(114,170,255,.78)",
            borderColor: "#ffad59", borderColor0: "#72aaff", borderWidth: 1.3
          }
        });
      }
      if (points.some(function (p) { return p.sample && p.open == null; })) {
        mainLegend.push("单点指数观测");
        series.push({
          name: "单点指数观测", type: "scatter", symbolSize: 10, z: 7,
          data: points.map(function (p) { return p.sample && p.open == null ? p.close : "-"; }),
          itemStyle: { color: "#ffcf66", borderColor: "#f2f5f8", borderWidth: 1 }
        });
      }
    } else {
      series.push({ name: opts.priceLabel || "平台卖价", type: "line", data: closes, showSymbol: false,
        connectNulls: true, lineStyle: { width: 1.8, color: "#5aa7ff" },
        areaStyle: { opacity: .08, color: "#5aa7ff" } });
    }
    function addMainLine(name, data, color, dashed) {
      mainLegend.push(name);
      series.push({ name: name, type: "line", data: data, showSymbol: false, connectNulls: true,
        smooth: false, lineStyle: { width: 1.15, color: color, type: dashed ? "dashed" : "solid" } });
    }
    if (mainIndicators.indexOf("ma") !== -1) {
      addMainLine("MA5", calculated.ma5, "#f2c96d");
      addMainLine("MA10", calculated.ma10, "#5aa7ff");
      addMainLine("MA20", calculated.ma20, "#5aa7ff");
    }
    if (mainIndicators.indexOf("ema") !== -1) {
      addMainLine("EMA12", calculated.ema12, "#ff9f43");
      addMainLine("EMA26", calculated.ema26, "#55d6be");
    }
    if (mainIndicators.indexOf("boll") !== -1) {
      addMainLine("BOLL上轨", calculated.bollUpper, "#8591a3", true);
      addMainLine("BOLL中轨", calculated.bollMid, "#d8dee8", false);
      addMainLine("BOLL下轨", calculated.bollLower, "#8591a3", true);
    }

    var height = Math.max(320, dom.clientHeight || 520);
    var plotTop = 54, bottomSpace = 44, gap = 34;
    var usable = height - plotTop - bottomSpace;
    var subHeight = subIndicators.length
      ? Math.max(88, Math.min(118, Math.floor(usable * .42 / subIndicators.length)))
      : 0;
    var mainHeight = usable - subIndicators.length * (subHeight + gap);
    if (mainHeight < 180) {
      subHeight = Math.max(78, subHeight - (180 - mainHeight) / Math.max(1, subIndicators.length));
      mainHeight = usable - subIndicators.length * (subHeight + gap);
    }
    var grids = [{ left: 58, right: 62, top: plotTop, height: mainHeight }];
    var xAxes = [];
    var yAxes = [];
    var panelLegends = [{
      data: mainLegend, top: 5, left: 52, right: 12, type: "scroll",
      pageIconColor: "#aab4c0", pageTextStyle: { color: "#8b98ab" },
      textStyle: { color: "#aab4c0", fontSize: 11 }
    }];
    var titles = [];
    var totalPanels = subIndicators.length + 1;
    function makeXAxis(index) {
      return {
        type: calendarScale ? "time" : "category", gridIndex: index,
        data: calendarScale ? undefined : dates, boundaryGap: !calendarScale,
        axisLine: { lineStyle: { color: "#343e4c" } },
        axisTick: { show: false },
        axisLabel: {
          show: index === totalPanels - 1, color: "#8b98ab", hideOverlap: true, fontSize: 11,
          formatter: function (value) {
            return marketDateLabel(calendarScale ? Number(value) : value);
          }
        },
        splitLine: { show: false },
        axisPointer: { label: {
          show: index === totalPanels - 1,
          formatter: function (params) {
            return marketDateLabel(calendarScale ? Number(params.value) : params.value, true);
          }
        } }
      };
    }
    function makeYAxis(index, formatter) {
      return {
        gridIndex: index, scale: true, position: "right",
        splitNumber: index === 0 ? 5 : 3,
        splitLine: { lineStyle: { color: "rgba(52,62,76,.28)", type: "dashed" } },
        axisLine: { show: false }, axisTick: { show: false },
        axisLabel: { color: "#8b98ab", fontSize: 11, formatter: formatter || function (v) { return fmt(v); } }
      };
    }
    xAxes.push(makeXAxis(0));
    yAxes.push(makeYAxis(0, function (v) { return priceUnit + fmt(v); }));

    subIndicators.forEach(function (indicator, subIndex) {
      var axisIndex = subIndex + 1;
      var gridTop = plotTop + mainHeight + gap + subIndex * (subHeight + gap);
      grids.push({ left: 58, right: 62, top: gridTop, height: subHeight });
      xAxes.push(makeXAxis(axisIndex));
      var legendData = [];
      var title = "";
      if (indicator === "volume") {
        title = activityLabel;
        legendData = [activityLabel];
        series.push({ name: activityLabel, type: "bar", xAxisIndex: axisIndex, yAxisIndex: axisIndex,
          data: vols, barWidth: "62%", large: points.length > 500 });
        yAxes.push(makeYAxis(axisIndex, function (v) { return fmtI(v); }));
      } else if (indicator === "macd") {
        title = "MACD (12, 26, 9)";
        legendData = ["DIF", "DEA", "MACD"];
        series.push({ name: "MACD", type: "bar", xAxisIndex: axisIndex, yAxisIndex: axisIndex,
          data: calculated.macd.map(function (value) {
            return { value: value, itemStyle: { color: value >= 0 ? "rgba(255,91,112,.64)" : "rgba(36,201,139,.64)" } };
          }), barWidth: "58%" });
        series.push({ name: "DIF", type: "line", xAxisIndex: axisIndex, yAxisIndex: axisIndex,
          data: calculated.dif, showSymbol: false, lineStyle: { width: 1.15, color: "#f2c96d" } });
        series.push({ name: "DEA", type: "line", xAxisIndex: axisIndex, yAxisIndex: axisIndex,
          data: calculated.dea, showSymbol: false, lineStyle: { width: 1.15, color: "#5aa7ff" } });
        yAxes.push(makeYAxis(axisIndex));
      } else if (indicator === "rsi") {
        title = "RSI (14)";
        legendData = ["RSI14"];
        series.push({ name: "RSI14", type: "line", xAxisIndex: axisIndex, yAxisIndex: axisIndex,
          data: calculated.rsi, showSymbol: false, lineStyle: { width: 1.25, color: "#5aa7ff" },
          markLine: { symbol: "none", silent: true, label: { show: false },
            lineStyle: { color: "rgba(116,128,145,.55)", type: "dashed" },
            data: [{ yAxis: 30 }, { yAxis: 70 }] } });
        yAxes.push({
          gridIndex: axisIndex, min: 0, max: 100, interval: 50, position: "right",
          splitLine: { lineStyle: { color: "rgba(52,62,76,.24)", type: "dashed" } },
          axisLine: { show: false }, axisTick: { show: false },
          axisLabel: { color: "#8b98ab", fontSize: 11 }
        });
      } else if (indicator === "volatility") {
        title = "30 日波动率";
        legendData = ["波动率"];
        series.push({ name: "波动率", type: "line", xAxisIndex: axisIndex, yAxisIndex: axisIndex,
          data: calculated.volatility, showSymbol: false, connectNulls: true,
          lineStyle: { width: 1.25, color: "#ff9f43" },
          areaStyle: { opacity: .08, color: "#ff9f43" } });
        yAxes.push(makeYAxis(axisIndex, function (v) { return fmt(v, 1) + "%"; }));
      }
      titles.push({
        text: title, left: 58, top: gridTop - 27,
        textStyle: { color: "#aab4c0", fontSize: 10.5, fontWeight: 600 }
      });
      panelLegends.push({
        data: legendData, left: 160, top: gridTop - 30,
        textStyle: { color: "#8b98ab", fontSize: 10.5 },
        itemWidth: 12, itemHeight: 6
      });
    });
    var axisIndexes = xAxes.map(function (_, index) { return index; });
    var zoom = CHART_ZOOM[domId] || {};
    if (calendarScale) {
      var pointByTime = {};
      points.forEach(function (point) { pointByTime[marketTimestamp(point.ts)] = point; });
      series.forEach(function (item) {
        item.data = item.data.map(function (datum, index) {
          if (datum === "-" || datum == null) return null;
          var stamp = marketTimestamp(points[index].ts);
          if (Array.isArray(datum)) return [stamp].concat(datum);
          if (typeof datum === "object" && datum.value != null) {
            return Object.assign({}, datum, { value: [stamp, datum.value] });
          }
          return [stamp, datum];
        }).filter(function (datum) { return datum != null; });
        item.encode = item.type === "candlestick"
          ? { x: 0, y: [1, 2, 3, 4] } : { x: 0, y: 1 };
        if (item.type === "candlestick") item.barWidth = 4;
      });
    }
    chart.setOption({
      backgroundColor: "transparent",
      animation: !REDUCED,
      axisPointer: { link: [{ xAxisIndex: "all" }] },
      tooltip: {
        trigger: "axis", confine: true, formatter: function (params) {
          var index = params && params.length ? params[0].dataIndex : -1;
          var sample = calendarScale && params && params.length
            ? (pointByTime[Number(params[0].axisValue)] || {}).sample
            : (points[index] && points[index].sample);
          var point = calendarScale && params && params.length
            ? pointByTime[Number(params[0].axisValue)] : points[index];
          var text = chartTooltip(params, point && point.ts);
          return sample ? text + "<br><span class='tip-values'>来源：" + esc(sample)
            + (point && point.observations ? " · " + point.observations + " 个小时样本" : "")
            + "</span>" : text;
        },
        axisPointer: { type: "cross", label: { backgroundColor: "#1d2430" } },
        backgroundColor: "rgba(13,16,21,.96)", borderColor: "#343e4c",
        padding: [8, 10], textStyle: { color: "#f2f5f8", fontSize: 11 }
      },
      title: titles,
      legend: panelLegends,
      grid: grids,
      xAxis: xAxes,
      yAxis: yAxes,
      dataZoom: [
        { type: "inside", xAxisIndex: axisIndexes,
          start: zoom.start == null ? (opts.fullRange ? 0
            : Math.max(0, 100 - 80 / Math.max(1, points.length) * 100)) : zoom.start,
          end: zoom.end == null ? 100 : zoom.end },
        { type: "slider", xAxisIndex: axisIndexes, bottom: 4, height: 16, borderColor: "#252c36",
          backgroundColor: "#0d1015", fillerColor: "rgba(90,167,255,.14)",
          handleStyle: { color: "#5aa7ff", borderColor: "#5aa7ff" },
          textStyle: { color: "#8b98ab", fontSize: 10 } }
      ],
      series: series
    });
    chart.on("dataZoom", function (event) {
      var data = event.batch && event.batch[0] ? event.batch[0] : event;
      CHART_ZOOM[domId] = { start: data.start, end: data.end };
    });
    chart.__chartMeta = {
      dates: dates,
      timeAxis: calendarScale,
      mainGridIndex: 0,
      subIndicators: subIndicators.slice(),
      activityCount: activityCount(points, activityLabel)
    };
    bindChartResize(dom, chart);
    return chart;
  }

  var DRAWING_CONTROLLERS = {};

  function drawingController(domId, overlayId, ui) {
    if (DRAWING_CONTROLLERS[domId]) return DRAWING_CONTROLLERS[domId];
    var dom = el(domId);
    var overlay = el(overlayId);
    if (!dom || !overlay) return null;
    ui = ui || {};
    var ns = "http://www.w3.org/2000/svg";
    var controller = {
      chart: null,
      dates: [],
      key: "",
      drawings: [],
      mode: null,
      pending: null,
      pointer: null
    };
    function storageKey() { return "cs2m.chart.drawings." + controller.key; }
    function read() {
      try { return JSON.parse(localStorage.getItem(storageKey())) || []; }
      catch (e) { return []; }
    }
    function write() {
      try { localStorage.setItem(storageKey(), JSON.stringify(controller.drawings.slice(-80))); }
      catch (e) {}
    }
    function node(tag, attrs, text) {
      var item = document.createElementNS(ns, tag);
      Object.keys(attrs || {}).forEach(function (key) { item.setAttribute(key, attrs[key]); });
      if (text != null) item.textContent = text;
      overlay.appendChild(item);
      return item;
    }
    function mainRect() {
      if (!controller.chart) return null;
      var component = controller.chart.getModel().getComponent("grid", 0);
      return component && component.coordinateSystem ? component.coordinateSystem.getRect() : null;
    }
    function pixel(point) {
      if (!controller.chart || !point) return null;
      var index = controller.dates.indexOf(point.date);
      if (index < 0) return null;
      var result = controller.chart.convertToPixel(
        { xAxisIndex: 0, yAxisIndex: 0 },
        [controller.timeAxis ? marketTimestamp(point.date) : index, point.value]
      );
      return result && isFinite(result[0]) && isFinite(result[1]) ? result : null;
    }
    function dataPoint(x, y) {
      var rect = mainRect();
      if (!rect || x < rect.x || x > rect.x + rect.width || y < rect.y || y > rect.y + rect.height) return null;
      var result = controller.chart.convertFromPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [x, y]);
      if (!result) return null;
      var rawIndex = Number(result[0]);
      var index = isFinite(rawIndex) ? Math.round(rawIndex) : controller.dates.indexOf(String(result[0]));
      if (controller.timeAxis && isFinite(rawIndex)) {
        index = controller.dates.reduce(function (best, date, current) {
          return Math.abs(marketTimestamp(date) - rawIndex)
            < Math.abs(marketTimestamp(controller.dates[best]) - rawIndex) ? current : best;
        }, 0);
      }
      if (index < 0) return null;
      index = Math.max(0, Math.min(controller.dates.length - 1, index));
      return { date: controller.dates[index], value: +Number(result[1]).toFixed(4) };
    }
    function drawLine(x1, y1, x2, y2, extra) {
      node("line", Object.assign({
        x1: x1, y1: y1, x2: x2, y2: y2,
        stroke: "#f2c96d", "stroke-width": 1.4, "vector-effect": "non-scaling-stroke"
      }, extra || {}));
    }
    function renderOne(drawing) {
      var rect = mainRect();
      if (!rect) return;
      var a = pixel(drawing.a);
      var b = drawing.b ? pixel(drawing.b) : null;
      if (drawing.type === "trend" && a && b) {
        drawLine(a[0], a[1], b[0], b[1]);
        node("circle", { cx: a[0], cy: a[1], r: 2.5, fill: "#f2c96d" });
        node("circle", { cx: b[0], cy: b[1], r: 2.5, fill: "#f2c96d" });
      } else if (drawing.type === "horizontal" && a) {
        drawLine(rect.x, a[1], rect.x + rect.width, a[1], { "stroke-dasharray": "5 4" });
        node("text", {
          x: rect.x + rect.width - 4, y: a[1] - 5, fill: "#f2c96d",
          "font-size": 10, "text-anchor": "end"
        }, "¥" + fmt(drawing.a.value));
      } else if (drawing.type === "vertical" && a) {
        drawLine(a[0], rect.y, a[0], rect.y + rect.height, { "stroke-dasharray": "5 4", stroke: "#5aa7ff" });
        node("text", {
          x: a[0] + 4, y: rect.y + 12, fill: "#8fc2ff", "font-size": 10
        }, drawing.a.date);
      } else if (drawing.type === "fib" && a && b) {
        var levels = [0, .236, .382, .5, .618, .786, 1];
        var startX = Math.min(a[0], b[0]);
        var endX = rect.x + rect.width;
        levels.forEach(function (level) {
          var y = a[1] + (b[1] - a[1]) * level;
          drawLine(startX, y, endX, y, {
            stroke: level === .5 ? "#ff9f43" : "#8fc2ff",
            "stroke-width": level === .5 ? 1.35 : 1,
            "stroke-dasharray": "4 4"
          });
          node("text", {
            x: startX + 4, y: y - 4, fill: level === .5 ? "#ffb563" : "#8fc2ff", "font-size": 9
          }, String(level));
        });
      }
    }
    controller.render = function () {
      overlay.setAttribute("viewBox", "0 0 " + dom.clientWidth + " " + dom.clientHeight);
      overlay.setAttribute("width", dom.clientWidth);
      overlay.setAttribute("height", dom.clientHeight);
      while (overlay.firstChild) overlay.removeChild(overlay.firstChild);
      controller.drawings.forEach(renderOne);
      if (controller.pending && controller.pointer && (controller.mode === "trend" || controller.mode === "fib")) {
        var a = pixel(controller.pending);
        if (a) drawLine(a[0], a[1], controller.pointer[0], controller.pointer[1], {
          stroke: "#aab4c0", "stroke-dasharray": "4 4"
        });
      }
    };
    controller.bind = function (chart, key, dates) {
      controller.chart = chart;
      controller.dates = dates || [];
      controller.timeAxis = !!(chart.__chartMeta && chart.__chartMeta.timeAxis);
      if (controller.key !== key) {
        controller.key = key;
        controller.drawings = read();
        controller.pending = null;
      }
      chart.on("dataZoom", controller.render);
      chart.on("finished", controller.render);
      controller.render();
    };
    controller.detach = function () {
      controller.chart = null;
      controller.dates = [];
      controller.pending = null;
      controller.pointer = null;
      controller.render();
    };
    controller.setMode = function (mode) {
      controller.mode = controller.mode === mode ? null : mode;
      controller.pending = null;
      controller.pointer = null;
      overlay.classList.toggle("active", !!controller.mode);
      overlay.style.pointerEvents = controller.mode ? "auto" : "none";
      document.querySelectorAll(ui.toolSelector || "[data-draw-tool]").forEach(function (button) {
        var value = button.dataset.drawTool
          || button.dataset.marketDrawTool
          || button.dataset.homeDrawTool
          || button.dataset.sectionDrawTool;
        button.classList.toggle("active", value === controller.mode);
      });
      var hint = ui.hintId ? el(ui.hintId) : null;
      var status = el(ui.statusId || "draw-tool-status");
      var labels = { trend: "趋势线", horizontal: "水平线", vertical: "垂直线", fib: "斐波那契回撤" };
      if (hint) {
        hint.classList.toggle("hidden", !controller.mode);
        hint.textContent = controller.mode ? "在主图选择第一个点" : "";
      }
      if (status) status.textContent = controller.mode ? labels[controller.mode] + "已启用" : "选择工具后在主图点击";
      controller.render();
    };
    controller.undo = function () {
      controller.drawings.pop();
      write();
      controller.render();
    };
    controller.clear = function () {
      controller.drawings = [];
      controller.pending = null;
      write();
      controller.render();
    };
    overlay.addEventListener("click", function (event) {
      if (!controller.mode || !controller.chart) return;
      var bounds = overlay.getBoundingClientRect();
      var point = dataPoint(event.clientX - bounds.left, event.clientY - bounds.top);
      if (!point) return;
      if (controller.mode === "horizontal" || controller.mode === "vertical") {
        controller.drawings.push({ type: controller.mode, a: point });
        write();
      } else if (!controller.pending) {
        controller.pending = point;
        var hint = ui.hintId ? el(ui.hintId) : null;
        if (hint) hint.textContent = "选择第二个点";
      } else {
        controller.drawings.push({ type: controller.mode, a: controller.pending, b: point });
        controller.pending = null;
        write();
        var nextHint = ui.hintId ? el(ui.hintId) : null;
        if (nextHint) nextHint.textContent = "在主图选择第一个点";
      }
      controller.render();
    });
    overlay.addEventListener("mousemove", function (event) {
      if (!controller.pending) return;
      var bounds = overlay.getBoundingClientRect();
      controller.pointer = [event.clientX - bounds.left, event.clientY - bounds.top];
      controller.render();
    });
    overlay.addEventListener("mouseleave", function () {
      controller.pointer = null;
      controller.render();
    });
    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && controller.mode) controller.setMode(null);
    });
    DRAWING_CONTROLLERS[domId] = controller;
    return controller;
  }

  function clearChartView(domId, drawing) {
    if (drawing && drawing.detach) drawing.detach();
    if (drawing && drawing.setMode) drawing.setMode(null);
    disposeChart(domId, true);
  }

  // 折线图（渐变面积）
  function lineChartTime(value) {
    return marketTimestamp(value);
  }
  function lineChart(domId, series, opts) {
    var dom = el(domId);
    if (!dom) return null;
    var chart = initChart(domId);
    var s = [];
    (Array.isArray(series) ? series : [series]).forEach(function (item, idx) {
      var data = (item.data || []).filter(function (p) {
        return p && p.close != null;
      }).map(function (p) {
        return [lineChartTime(p.ts), Number(p.close)];
      }).filter(function (point) {
        return Number.isFinite(point[0]) && Number.isFinite(point[1]);
      });
      s.push({
        name: item.name || "line", type: "line", showSymbol: false, smooth: false, data: data,
        lineStyle: { width: 1.55 },
        areaStyle: idx === 0 ? { opacity: .06, color: "#5aa7ff" } : undefined
      });
    });
    chart.setOption({
      backgroundColor: "transparent",
      animation: !REDUCED,
      tooltip: {
        trigger: "axis", confine: true, axisPointer: { type: "cross" },
        formatter: function (params) { return chartTooltip(params); },
        backgroundColor: "rgba(13,16,21,.96)", borderColor: "#343e4c",
        textStyle: { color: "#f2f5f8", fontSize: 11 }
      },
      legend: { type: "scroll", textStyle: { color: "#aab4c0", fontSize: 11 }, top: 0 },
      grid: { left: 58, right: 62, top: 38, bottom: 42 },
      xAxis: {
        type: "time", axisLine: { lineStyle: { color: "#343e4c" } }, axisTick: { show: false },
        axisLabel: {
          color: "#8b98ab", hideOverlap: true, fontSize: 11,
          formatter: function (value) { return marketDateLabel(Number(value), true); }
        },
        axisPointer: { label: {
          formatter: function (params) { return marketDateLabel(Number(params.value), true); }
        } },
        splitLine: { show: false }
      },
      yAxis: {
        scale: true, position: "right", axisLine: { show: false }, axisTick: { show: false },
        splitLine: { lineStyle: { color: "rgba(52,62,76,.28)", type: "dashed" } },
        axisLabel: { color: "#8b98ab", fontSize: 11 }
      },
      dataZoom: [
        { type: "inside" },
        { type: "slider", bottom: 4, height: 15, borderColor: "#252c36",
          backgroundColor: "#0d1015", fillerColor: "rgba(90,167,255,.14)",
          textStyle: { color: "#8b98ab", fontSize: 10 } }
      ],
      series: s
    });
    bindChartResize(dom, chart);
    return chart;
  }

  // 迷你走势（卡片内 sparkline）
  function sparkChart(domId, values, opts) {
    var dom = el(domId);
    if (!dom || !values || !values.length) return null;
    var chart = initChart(domId);
    var color = (opts && opts.color) || "#4d8dff";
    chart.setOption({
      backgroundColor: "transparent",
      animation: !REDUCED,
      grid: { left: 1, right: 1, top: 2, bottom: 2 },
      xAxis: { type: "category", show: false, data: values.map(function (v, i) { return i; }) },
      yAxis: { type: "value", show: false, scale: true },
      tooltip: { show: false },
      series: [{ type: "line", data: values, showSymbol: false, smooth: true,
        lineStyle: { width: 1.4, color: color },
        areaStyle: { opacity: .18, color: color } }]
    });
    bindChartResize(dom, chart);
    return chart;
  }

  function trendColor(values) {
    if (!values || values.length < 2) return "#4d8dff";
    return values[values.length - 1] >= values[0] ? "#f6465d" : "#0ecb81";
  }
  // ---------- 全屏搜索浮层（行情站B 搜索模式） ----------
  var overlayEl = el("search-overlay"), triggerEl = el("search-trigger"),
      searchInput = el("search-input"), searchResults = el("search-results"),
      searchTabs = el("search-tabs");
  var SEARCH_KEY = "cs2m.search.history";
  function getHistory() {
    try { return JSON.parse(localStorage.getItem(SEARCH_KEY)) || []; }
    catch (e) { return []; }
  }
  function setHistory(arr) {
    try { localStorage.setItem(SEARCH_KEY, JSON.stringify(arr.slice(0, 8))); } catch (e) {}
  }
  function pushHistory(q) {
    var h = getHistory().filter(function (x) { return x !== q; });
    h.unshift(q);
    setHistory(h);
    renderHistoryChips();
  }
  function removeHistory(q) {
    setHistory(getHistory().filter(function (x) { return x !== q; }));
    renderHistoryChips();
  }
  function renderHistoryChips() {
    var box = el("search-history");
    if (!box) return;
    Array.prototype.forEach.call(box.querySelectorAll(".chip"), function (c) { c.remove(); });
    getHistory().forEach(function (q) {
      var chipNode = document.createElement("span");
      chipNode.className = "chip";
      chipNode.appendChild(document.createTextNode(q));
      var x = document.createElement("span");
      x.className = "x";
      x.textContent = "×";
      x.addEventListener("click", function (e) { e.stopPropagation(); removeHistory(q); });
      chipNode.appendChild(x);
      chipNode.addEventListener("click", function () {
        searchInput.value = q;
        doSuggest(q);
      });
      box.appendChild(chipNode);
    });
  }
  function currentType() {
    var act = searchTabs ? searchTabs.querySelector("button.active") : null;
    return (act && act.dataset.type) || "all";
  }
  function syncSearchTabs(activeType) {
    if (!searchTabs) return;
    searchTabs.querySelectorAll("button").forEach(function (button) {
      var active = button.dataset.type === activeType;
      button.classList.toggle("active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
  }
  function openSearch() {
    if (!overlayEl) return;
    overlayEl.classList.remove("hidden");
    if (triggerEl) triggerEl.setAttribute("aria-expanded", "true");
    document.body.style.overflow = "hidden";
    if (searchInput) { searchInput.focus(); searchInput.select(); }
  }
  function closeSearch() {
    if (!overlayEl) return;
    overlayEl.classList.add("hidden");
    if (triggerEl) {
      triggerEl.setAttribute("aria-expanded", "false");
      triggerEl.focus({ preventScroll: true });
    }
    document.body.style.overflow = "";
    cancelRequest("search-suggest");
    clearTimeout(suggestTimer);
    if (searchResults) {
      searchResults.classList.add("hidden");
      searchResults.dataset.query = "";
    }
  }
  function goSearch(q) {
    if (!q) return;
    pushHistory(q);
    var t = currentType();
    if (t === "tracker") { location.href = "/monitor/radar?q=" + encodeURIComponent(q); return; }
    if (t === "items") { location.href = "/market/items?q=" + encodeURIComponent(q); return; }
    var first = searchResults && searchResults.dataset.query === q
      ? (searchResults.querySelector("a.sel[data-name]") || searchResults.querySelector("a[data-name]"))
      : null;
    location.href = first
      ? "/market/items/" + encodeURIComponent(first.getAttribute("data-name"))
      : "/market/items?q=" + encodeURIComponent(q);
  }
  var suggestTimer = null;
  function doSuggest(q) {
    if (!searchResults) return;
    clearTimeout(suggestTimer);
    searchResults.dataset.query = "";
    if (currentType() === "tracker" || !q) {
      cancelRequest("search-suggest");
      searchResults.classList.add("hidden");
      return;
    }
    searchResults.classList.add("hidden");
    suggestTimer = setTimeout(function () {
      json("/api/search?q=" + encodeURIComponent(q) + "&limit=8", function (d) {
        var items = d.items || [];
        searchResults.innerHTML = items.length ? items.map(function (it, index) {
          return "<a href='/market/items/" + encodeURIComponent(it.market_hash_name) + "' data-name='" + esc(it.market_hash_name) + "' role='option' aria-selected='" + (index === 0 ? "true" : "false") + "'>"
            + "<span class='nm'>" + esc(it.market_hash_name) + "</span>"
            + "<span class='go'>" + (it.name && it.name !== it.market_hash_name ? esc(it.name) : "打开") + "</span></a>";
        }).join("") : "<div class='search-no-result'><strong>目录中没有匹配项</strong><span>可尝试英文市场名、武器名或更短的关键词</span></div>";
        searchResults.dataset.query = q;
        searchResults.classList.remove("hidden");
        var first = searchResults.querySelector("a");
        if (first) first.classList.add("sel");
      }, function () {
        searchResults.dataset.query = q;
        searchResults.innerHTML = "<div class='search-no-result'><strong>搜索暂时不可用</strong><span>请稍后重试，或直接打开饰品市场筛选。</span></div>";
        searchResults.classList.remove("hidden");
      }, "search-suggest");
    }, 180);
  }
  if (overlayEl) {
    if (triggerEl) triggerEl.addEventListener("click", openSearch);
    var closeBtn = el("search-close");
    if (closeBtn) closeBtn.addEventListener("click", closeSearch);
    var searchGo = el("search-go");
    if (searchGo) searchGo.addEventListener("click", function () {
      goSearch(searchInput ? searchInput.value.trim() : "");
    });
    overlayEl.addEventListener("click", function (e) { if (e.target === overlayEl) closeSearch(); });
    if (searchInput) {
      searchInput.addEventListener("input", function () { doSuggest(searchInput.value.trim()); });
      searchInput.addEventListener("keydown", function (e) {
        if (e.key === "Escape") { closeSearch(); return; }
        if (e.key === "ArrowDown" || e.key === "ArrowUp") {
          var links = Array.prototype.slice.call(searchResults.querySelectorAll("a"));
          if (!links.length) return;
          e.preventDefault();
          var cur = links.indexOf(searchResults.querySelector("a.sel"));
          var next = e.key === "ArrowDown"
            ? (cur + 1) % links.length
            : (cur - 1 + links.length) % links.length;
          links.forEach(function (l) {
            l.classList.remove("sel");
            l.setAttribute("aria-selected", "false");
          });
          links[next].classList.add("sel");
          links[next].setAttribute("aria-selected", "true");
          links[next].scrollIntoView({ block: "nearest" });
        } else if (e.key === "Enter") {
          e.preventDefault();
          goSearch(searchInput.value.trim());
        }
      });
    }
    if (searchTabs) {
      Array.prototype.forEach.call(searchTabs.querySelectorAll("button"), function (b) {
        b.addEventListener("click", function () {
          syncSearchTabs(b.dataset.type || "all");
          doSuggest(searchInput ? searchInput.value.trim() : "");
        });
      });
    }
    if (searchResults) {
      searchResults.addEventListener("click", function (e) {
        var a = e.target.closest ? e.target.closest("a[data-name]") : null;
        if (a) pushHistory(a.getAttribute("data-name"));
      });
    }
    renderHistoryChips();
  }
  document.addEventListener("keydown", function (e) {
    var t = e.target;
    var typing = t && (t.tagName === "INPUT" || t.tagName === "TEXTAREA" || t.tagName === "SELECT" || t.isContentEditable);
    if (e.key === "/" && !typing) { e.preventDefault(); openSearch(); }
    if (e.key === "Escape" && overlayEl && !overlayEl.classList.contains("hidden")) closeSearch();
    if (e.key === "Escape" && document.body.classList.contains("nav-open")) {
      closeMobileNav();
    }
  });

  // ---------- 品类走势页：大盘指数走势图 ----------
  function initTrendChart() {
    var node = el("trend-index-chart");
    if (!node || typeof echarts === "undefined") return;
    var chart = echarts.init(node);
    var current = "7day";
    function load(period) {
      current = period;
      json("/api/market/analysis?source=siteb&period=" + period, function (d) {
        var points = d.points || [];
        if (!points.length) {
          node.innerHTML = "<div class='chart-empty'>暂无该周期数据</div>";
          return;
        }
        chart.setOption({
          animation: false,
          grid: { left: 60, right: 18, top: 18, bottom: 30 },
          tooltip: { trigger: "axis" },
          xAxis: {
            type: "category", boundaryGap: false,
            data: points.map(function (p) { return String(p.ts).slice(5, 16); }),
            axisLabel: { color: "#7d8aa0", hideOverlap: true },
          },
          yAxis: { scale: true, axisLabel: { color: "#7d8aa0" },
                   splitLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } } },
          series: [
            { name: "指数", type: "line", showSymbol: false, smooth: true,
              data: points.map(function (p) { return p.close; }),
              lineStyle: { color: "#4d8dff", width: 2 } },
            { name: "SMA7", type: "line", showSymbol: false,
              data: points.map(function (p) { return p.sma7 == null ? null : p.sma7; }),
              lineStyle: { color: "#3fd0a4", width: 1, type: "dashed" } },
          ],
        }, true);
        var sub = el("trend-chart-sub");
        if (sub && d.range && d.range.start) {
          sub.textContent = points.length + " 点 · " + d.range.start.slice(0, 10)
            + " 至 " + String(d.range.end || points[points.length - 1].ts).slice(0, 10);
        }
      }, function () {
        node.innerHTML = "<div class='chart-empty'>走势数据暂不可用</div>";
      }, "trend-index-" + period);
    }
    var seg = el("trend-period");
    if (seg) {
      seg.addEventListener("click", function (event) {
        var btn = event.target.closest("button[data-period]");
        if (!btn) return;
        seg.querySelectorAll("button").forEach(function (b) { b.classList.remove("active"); });
        btn.classList.add("active");
        load(btn.getAttribute("data-period"));
      });
    }
    window.addEventListener("resize", function () { chart.resize(); });
    load(current);
  }

  // ---------- 实时滚动条（金融 ticker） ----------
  function initTicker() {
    var track = el("ticker-track");
    if (!track) return;
    json("/api/hot", function (d) {
      var hot = (d.hot || []).slice(0, 12);
      var items = hot.map(function (r) {
        var diff = r.diff && r.diff.buff;
        var cls = diffCls(diff);
        return "<a class='ticker-item' href='/market/items/" + encodeURIComponent(r.name) + "'>"
          + "<span class='nm'>" + esc(r.name) + "</span>"
          + "<span class='px " + cls + "'>" + fmt(r.price.buff) + "</span>"
          + "<span class='px " + cls + "'>" + diffTxt(diff) + "</span></a>";
      }).join("");
      if (!items.length) {
        items = "<span class='ticker-item'><span class='nm'>暂无热度数据，等待采集…</span></span>";
      }
      track.innerHTML = items + items;
    }, function () {
      track.innerHTML = "<span class='ticker-item'><span class='nm'>热度数据暂时不可用</span></span>";
    }, null, "ticker-hot");
  }
  if (el("ticker")) initTicker();
  if (el("trend-index-chart")) initTrendChart();

  // ---------- 首页大盘图表工作区 ----------
  var homeChart = el("market-kline-chart");
  if (homeChart) {
    var savedHomeState = readViewState("home-chart", {});
    var homeSource = savedHomeState.source === "sitea" ? "sitea" : "siteb";
    var homePeriod = savedHomeState.period === "7day" ? "7day" : "1day";
    var homePoints = [];
    var homeChartMode = "candlestick";
    var homeChartMessage = "";
    var homeSamples = null;
    var homeMainIndicators = validIndicators(savedHomeState.main, ["ma", "ema", "boll"]);
    var homeSubIndicators = validIndicators(savedHomeState.sub, ["volume", "macd", "rsi", "volatility"]);
    if (!homeMainIndicators.length) homeMainIndicators = ["ma"];
    if (!homeSubIndicators.length) homeSubIndicators = ["volume"];
    var homeDrawing = null;
    var homeVolumeAvailable = true;
    var homeBox = el("home-chart-box") || homeChart.closest(".chart-box");
    function saveHomeState() {
      writeViewState("home-chart", {
        source: homeSource,
        period: homePeriod,
        main: homeMainIndicators,
        sub: homeSubIndicators
      });
    }

    function syncHomeControls() {
      document.querySelectorAll("[data-home-source]").forEach(function (button) {
        var active = button.dataset.homeSource === homeSource;
        button.classList.toggle("active", active);
        button.setAttribute("aria-pressed", active ? "true" : "false");
      });
      var supported = ["1day", "7day"];
      if (supported.indexOf(homePeriod) === -1) homePeriod = "1day";
      document.querySelectorAll("[data-home-period]").forEach(function (button) {
        var enabled = supported.indexOf(button.dataset.homePeriod) !== -1;
        button.disabled = !enabled;
        button.textContent = homeSource === "siteb"
          ? (button.dataset.homePeriod === "7day" ? "周 K 观测" : "全部公开 K 线")
          : (button.dataset.homePeriod === "7day" ? "周线" : "日 K");
        button.classList.toggle("active", button.dataset.homePeriod === homePeriod);
        button.setAttribute("aria-selected", button.dataset.homePeriod === homePeriod ? "true" : "false");
      });
      setText("home-source-label", homeSource === "sitea" ? "行情站A" : "行情站B");
    }
    function syncHomeIndicatorInputs() {
      syncIndicatorInputGroup("[data-home-main]", "homeMain", homeMainIndicators,
        homePoints, "成交量");
      syncIndicatorInputGroup("[data-home-sub]", "homeSub", homeSubIndicators,
        homePoints, "成交量");
    }

    function renderHomeChart() {
      if (!homePoints.length) return;
      var compact = window.innerWidth <= 720;
      var visibleSub = usableSubIndicators(homePoints, homeSubIndicators, "成交量");
      var visibleMain = homeMainIndicators.filter(function (indicator) {
        return indicatorReady(homePoints, indicator, "成交量");
      });
      homeChart.style.height = (compact ? 300 : 405)
        + visibleSub.length * (compact ? 104 : 118) + "px";
      var chart = klineChart("market-kline-chart", homePoints, {
        mode: homeChartMode,
        priceLabel: "公开大盘指数",
        priceUnit: "",
        calendarScale: homeSource === "siteb",
        fullRange: homeSource === "siteb" || homeChartMode === "line",
        mainIndicators: visibleMain,
        subIndicators: visibleSub,
        activityLabel: "成交量"
      });
      homeDrawing = drawingController("market-kline-chart", "home-drawing-layer", {
        statusId: "home-draw-status",
        toolSelector: "[data-home-draw-tool]"
      });
      if (homeDrawing && chart) {
        homeDrawing.bind(chart, "home." + homeSource + "." + homePeriod,
          homePoints.map(function (point) { return String(point.ts); }));
      }
      var labels = { ma: "MA", ema: "EMA", boll: "BOLL", volume: "成交量",
        macd: "MACD", rsi: "RSI", volatility: "波动率" };
      var active = visibleMain.concat(visibleSub);
      setText("home-indicator-count", active.length);
      setText("home-activity-name", "成交量");
      syncHomeIndicatorInputs();
      var holder = el("home-active-indicators");
      if (holder) holder.innerHTML = active.map(function (key) {
        return "<span>" + esc(labels[key] || key) + "</span>";
      }).join("");
      var periodLabel = homeSource === "siteb"
        ? (homePeriod === "7day" ? "站点原始周 K" : "原始日 K 历史")
        : (homePeriod === "7day" ? "周线" : "日线");
      var homeNative = homeSamples
        ? homeSamples.weekly + homeSamples.daily
        : homePoints.filter(function (p) { return p.open != null; }).length;
      var homeObserved = homeSamples ? homeSamples.observed : 0;
      setText("home-chart-source-meta",
        (homeSource === "sitea" ? "行情站A 指数" : "行情站B 总览")
        + " · " + periodLabel + " · "
        + marketDateLabel(homePoints[0].ts) + " 至 "
        + marketDateLabel(homePoints[homePoints.length - 1].ts)
        + " · 原始 K 线 " + homeNative + " 根"
        + (homeObserved ? " · 小时观测区间 " + homeObserved + " 根" : "")
        + (homeSamples && homeSamples.latest ? " · 单点 " + homeSamples.latest + " 个" : ""));
      setText("home-chart-sub",
        (homeSource === "sitea" ? "行情站A 指数" : "行情站B 大盘")
        + " · " + periodLabel + " · 主图 " + homeMainIndicators.join("/") 
        + " · 副图 " + (visibleSub.length ? visibleSub.join("/") : "无")
        + (homeChartMessage ? " · " + homeChartMessage : ""));
      saveHomeState();
      touch("kline-refresh");
    }

    function loadHomeChart() {
      homePoints = [];
      clearChartView("market-kline-chart", homeDrawing);
      syncHomeControls();
      homeBox.classList.remove("hidden");
      var loading = homeBox.querySelector(".chart-loading");
      if (loading) {
        loading.classList.remove("hidden");
        loading.classList.remove("error");
        loading.textContent = "大盘行情加载中…";
      }
      json("/api/market/analysis?source=" + homeSource + "&period=" + homePeriod, function (data) {
        homeSource = data.source || homeSource;
        homePeriod = data.period || homePeriod;
        homePoints = data.points || [];
        homeChartMode = data.mode || "candlestick";
        homeChartMessage = data.message || "";
        homeSamples = homeSource === "siteb" && data.view === "full"
          ? data.samples : null;
        homeVolumeAvailable = !!(data.coverage && data.coverage.volume && data.coverage.volume.available);
        if (!homeVolumeAvailable && homeSubIndicators.indexOf("volume") !== -1) {
          homeSubIndicators = homeSubIndicators.filter(function (key) { return key !== "volume"; });
          if (!homeSubIndicators.length) homeSubIndicators = ["macd"];
          setText("home-indicator-note", "当前来源缺少成交样本，已切换为 MACD");
        }
        syncHomeControls();
        if (homePoints.length) {
          renderHomeChart();
          chartDone(homeBox);
        } else {
          chartState(homeBox, "当前来源暂无可用行情", loadHomeChart);
        }
      }, function () {
        chartState(homeBox, "大盘行情加载失败，请重试", loadHomeChart);
      }, "home-analysis");
    }

    var closeHomePopovers = bindChartMenus([
      ["home-source-menu", "home-source-trigger"],
      ["home-indicator-panel", "home-indicator-trigger"],
      ["home-draw-menu", "home-draw-trigger"]
    ], ".home-chart-commandbar", "home-indicator-close");

    document.querySelectorAll("[data-home-source]").forEach(function (button) {
      button.addEventListener("click", function () {
        homeSource = button.dataset.homeSource || "siteb";
        homePeriod = homeSource === "siteb" ? "1day" : homePeriod;
        saveHomeState();
        CHART_ZOOM["market-kline-chart"] = null;
        if (homeDrawing) homeDrawing.setMode(null);
        closeHomePopovers();
        loadHomeChart();
      });
    });
    document.querySelectorAll("[data-home-period]").forEach(function (button) {
      button.addEventListener("click", function () {
        if (button.disabled) return;
        homePeriod = button.dataset.homePeriod || "1day";
        saveHomeState();
        CHART_ZOOM["market-kline-chart"] = null;
        if (homeDrawing) homeDrawing.setMode(null);
        syncHomeControls();
        loadHomeChart();
      });
    });
    document.querySelectorAll("[data-home-main]").forEach(function (input) {
      input.addEventListener("change", function () {
        homeMainIndicators = Array.prototype.map.call(
          document.querySelectorAll("[data-home-main]:checked"),
          function (item) { return item.dataset.homeMain; }
        );
        saveHomeState();
        renderHomeChart();
      });
    });
    document.querySelectorAll("[data-home-sub]").forEach(function (input) {
      input.addEventListener("change", function () {
        var checked = Array.prototype.slice.call(document.querySelectorAll("[data-home-sub]:checked"));
        if (checked.length > 2) {
          input.checked = false;
          setText("home-indicator-note", "副图最多同时显示 2 个");
          return;
        }
        homeSubIndicators = checked.map(function (item) { return item.dataset.homeSub; });
        if (!homeSubIndicators.length) homeSubIndicators = [];
        saveHomeState();
        setText("home-indicator-note", homeSubIndicators.length === 2
          ? "已达到 2 个副图上限" : "默认组合适合日常观察");
        renderHomeChart();
      });
    });
    var homeIndicatorReset = el("home-indicator-reset");
    if (homeIndicatorReset) homeIndicatorReset.addEventListener("click", function () {
      homeMainIndicators = ["ma"];
      homeSubIndicators = homeVolumeAvailable ? ["volume"] : ["macd"];
      saveHomeState();
      document.querySelectorAll("[data-home-main]").forEach(function (input) {
        input.checked = homeMainIndicators.indexOf(input.dataset.homeMain) !== -1;
      });
      document.querySelectorAll("[data-home-sub]").forEach(function (input) {
        input.checked = homeSubIndicators.indexOf(input.dataset.homeSub) !== -1;
      });
      setText("home-indicator-note", "已恢复默认组合");
      renderHomeChart();
    });
    document.querySelectorAll("[data-home-draw-tool]").forEach(function (button) {
      button.addEventListener("click", function () {
        if (homeDrawing) homeDrawing.setMode(button.dataset.homeDrawTool);
        setChartPopover(el("home-draw-menu"), el("home-draw-trigger"), false);
      });
    });
    var homeDrawUndo = el("home-draw-undo");
    if (homeDrawUndo) homeDrawUndo.addEventListener("click", function () { if (homeDrawing) homeDrawing.undo(); });
    var homeDrawClear = el("home-draw-clear");
    if (homeDrawClear) homeDrawClear.addEventListener("click", function () { if (homeDrawing) homeDrawing.clear(); });
    syncHomeControls();
    loadHomeChart();
  }
  if (el("index-line-chart")) {
    var ilBox = el("index-line-chart").closest(".chart-box");
    var indexSignature = "";
    function refreshIndexLine() {
      if (document.hidden) return;
      json("/api/market/index", function (d) {
        var points = d.points || [];
        var last = points[points.length - 1];
        var note = el("index-line-freshness");
        if (!last) {
          clearChartView("index-line-chart");
          indexSignature = "";
          if (note) { note.textContent = "暂无已采集的小时指数；请查看数据覆盖情况。"; note.className = "data-source-note index-line-freshness stale"; }
          chartState(ilBox, "暂无小时指数数据", null);
          return;
        }
        var signature = [points.length, points[0].ts, last.ts, last.close].join("|");
        if (signature !== indexSignature) {
          lineChart("index-line-chart", { name: "饰品指数", data: points });
          indexSignature = signature;
        }
        chartDone(ilBox);
        var dateLabel = marketDateLabel(d.as_of || last.ts, true);
        if (note) {
          note.textContent = "行情站B 小时指数 · 最近采集 " + dateLabel + "（北京时间）"
            + " · " + points.length + " 个点"
            + (d.status === "stale" ? " · 数据已过期" : "")
            + "；时间间隔按采集点显示";
          note.className = "data-source-note index-line-freshness "
            + (d.status === "stale" ? "stale" : "ready");
        }
      }, function () {
        var note = el("index-line-freshness");
        if (indexSignature) {
          if (note) { note.textContent = "获取更新失败，当前图表保留上次采集数据。"; note.className = "data-source-note index-line-freshness stale"; }
        } else {
          if (note) note.textContent = "小时指数读取失败。";
          chartState(ilBox, "指数走势加载失败，请重试", refreshIndexLine);
        }
      }, "index-line");
    }
    refreshIndexLine();
    window.setInterval(refreshIndexLine, 60000);
  }
  if (el("market-index-chart")) {
    var miBox = el("market-index-chart").closest(".chart-box");
    json("/api/market/index", function (d) {
      lineChart("market-index-chart", { name: "指数", data: d.points || [] });
      chartDone(miBox);
    }, function () {
      chartState(miBox, "指数走势加载失败，请重试", function () {
        location.reload();
      });
    }, "market-index");
  }

  // ---------- 双来源市场指数工作台 ----------
  var marketAnalysisChart = el("market-analysis-chart");
  if (marketAnalysisChart) {
    var savedMarketState = readViewState("market-chart", {});
    var marketSource = savedMarketState.source === "sitea" ? "sitea" : "siteb";
    var marketPeriod = String(savedMarketState.period || "1day");
    // An old saved "candles" preference must not silently reopen only the
    // 2025-2026 subset when the user navigates to the bare market page.
    var marketView = "full";
    var marketChartMode = "candlestick";
    var marketPoints = [];
    var marketLoadedKey = "";
    var marketSignature = "";
    var marketSources = {};
    var marketMainIndicators = validIndicators(savedMarketState.main, ["ma", "ema", "boll"]);
    var marketSubIndicators = validIndicators(savedMarketState.sub, ["volume", "macd", "rsi", "volatility"]);
    if (!marketMainIndicators.length) marketMainIndicators = ["ma"];
    if (!marketSubIndicators.length) marketSubIndicators = ["volume"];
    var marketDrawing = null;
    var marketVolumeAvailable = true;
    var sourceDataNode = el("market-sources-data");
    try { marketSources = JSON.parse(sourceDataNode ? sourceDataNode.textContent : "{}"); }
    catch (e) { marketSources = {}; }
    if (sourceDataNode && sourceDataNode.dataset) {
      var marketQuery = new URLSearchParams(window.location.search);
      var hasExplicitMarketQuery = marketQuery.has("source") || marketQuery.has("period");
      if (marketQuery.has("view")) marketView = marketQuery.get("view") === "candles" ? "candles" : "full";
      if (hasExplicitMarketQuery || !savedMarketState.source) {
        marketSource = sourceDataNode.dataset.initialSource || marketSource;
        marketPeriod = sourceDataNode.dataset.initialPeriod || marketPeriod;
      }
    }
    function saveMarketState() {
      writeViewState("market-chart", {
        source: marketSource,
        period: marketPeriod,
        view: marketView,
        main: marketMainIndicators,
        sub: marketSubIndicators
      });
    }
    function syncMarketUrl() {
      if (!window.history || !window.history.replaceState) return;
      var url = new URL(window.location.href);
      if (marketSource === "siteb") url.searchParams.delete("source");
      else url.searchParams.set("source", marketSource);
      if (marketPeriod === "1day") url.searchParams.delete("period");
      else url.searchParams.set("period", marketPeriod);
      if (marketView === "candles" && marketSource === "siteb") url.searchParams.set("view", "candles");
      else url.searchParams.delete("view");
      window.history.replaceState({}, "", url.pathname + url.search + url.hash);
    }

    function marketTimeLabel(value) {
      return marketDateLabel(value, true);
    }
    function setMarketSummary(source) {
      var s = marketSources[source] || {};
      setText("market-source-name", s.name);
      setText("market-source-value", s.value == null ? null : fmt(s.value));
      setText("market-source-badge", String(source || "").toUpperCase());
      setText("market-source-asof", marketTimeLabel(s.as_of)
        + (s.status === "stale" ? " · 数据已过期" : ""));
      setText("market-open", s.open == null ? null : fmt(s.open));
      setText("market-high", s.high == null ? null : fmt(s.high));
      setText("market-low", s.low == null ? null : fmt(s.low));
      setText("market-close", s.candle_close == null ? null : fmt(s.candle_close));
      setText("market-candle-asof", marketTimeLabel(s.candle_as_of));
      setText("market-trade-count", s.trade_count == null ? null : fmtI(s.trade_count));
      setText("market-turnover", s.turnover == null ? null : fmtI(s.turnover));
      setText("market-supply", s.supply == null ? null : fmtI(s.supply));
      setText("market-holders", s.holders == null ? null : fmtI(s.holders));
      var provenance = el("market-provenance");
      if (provenance) {
        var status = s.status || (s.as_of ? "ready" : "collecting");
        provenance.dataset.status = status;
        var provenanceSource = provenance.querySelector(".provenance-source");
        var provenanceState = provenance.querySelector(".provenance-state");
        var provenanceTime = provenance.querySelector("time");
        if (provenanceSource) provenanceSource.textContent = s.name || source.toUpperCase();
        if (provenanceState) provenanceState.textContent = {
          ready: "可用", stale: "数据过期", collecting: "采集中", missing: "尚无数据",
          partial: "部分覆盖", blocked: "来源受限", error: "读取失败"
        }[status] || status;
        if (provenanceTime) {
          provenanceTime.dateTime = s.as_of || "";
          provenanceTime.textContent = s.as_of
            ? "更新 " + marketTimeLabel(s.as_of) + "（北京时间）" : "无更新时间";
        }
      }
      var change = el("market-source-change");
      if (change) {
        var rate = s.change_rate == null ? null : Number(s.change_rate);
        change.className = "stat-diff " + diffCls(rate);
        change.innerHTML = "<span class='pill'>" + (rate == null ? "-" : diffTxt(rate)) + "</span><span>"
          + (s.change == null ? "-" : (Number(s.change) >= 0 ? "+" : "") + fmt(s.change)) + "</span>";
      }
      document.querySelectorAll(".source-compare [data-market-source]").forEach(function (button) {
        var summary = marketSources[button.dataset.marketSource] || {};
        var timeNode = button.querySelector(".compare-asof");
        var valueNode = button.querySelector(".compare-value b");
        var rateNode = button.querySelector(".compare-value small");
        if (timeNode) timeNode.textContent = marketTimeLabel(summary.as_of)
          + (summary.status === "stale" ? " · 已过期" : "");
        if (valueNode) valueNode.textContent = fmt(summary.value);
        if (rateNode) {
          rateNode.textContent = summary.change_rate == null ? "-" : diffTxt(summary.change_rate);
          rateNode.className = diffCls(summary.change_rate);
        }
      });
    }

    function syncMarketControls() {
      var viewBar = el("market-view");
      if (viewBar) {
        viewBar.classList.toggle("hidden", marketSource !== "siteb");
        viewBar.querySelectorAll("[data-market-view]").forEach(function (button) {
          var active = button.dataset.marketView === marketView;
          button.classList.toggle("active", active);
          button.setAttribute("aria-selected", active ? "true" : "false");
        });
      }
      document.querySelectorAll("[data-market-source]").forEach(function (button) {
        var active = button.dataset.marketSource === marketSource;
        button.classList.toggle("active", active);
        button.setAttribute("aria-pressed", active ? "true" : "false");
        if (button.getAttribute("role") === "tab") {
          button.setAttribute("aria-selected", active ? "true" : "false");
        }
      });
      var supported = marketSource === "siteb" && marketView === "full"
        ? ["1day", "7day"]
        : ((marketSources[marketSource] && marketSources[marketSource].supported_periods) || ["1day"]);
      if (supported.indexOf(marketPeriod) === -1) marketPeriod = supported[0] || "1day";
      document.querySelectorAll("[data-market-period]").forEach(function (button) {
        var enabled = supported.indexOf(button.dataset.marketPeriod) !== -1;
        button.disabled = !enabled;
        if (button.dataset.marketPeriod === "1day") {
          button.textContent = marketSource === "siteb" && marketView === "full" ? "日 K 历史" : "日线";
        } else if (button.dataset.marketPeriod === "7day") {
          button.textContent = marketSource === "siteb" && marketView === "full" ? "周 K 观测" : "7 天";
        }
        button.classList.toggle("active", button.dataset.marketPeriod === marketPeriod);
        button.setAttribute("aria-selected", button.dataset.marketPeriod === marketPeriod ? "true" : "false");
      });
      setMarketSummary(marketSource);
      syncMarketUrl();
    }
    function syncMarketIndicatorInputs() {
      syncIndicatorInputGroup("[data-market-main]", "marketMain", marketMainIndicators,
        marketPoints, "成交量");
      syncIndicatorInputGroup("[data-market-sub]", "marketSub", marketSubIndicators,
        marketPoints, "成交量");
    }

    function renderMarketWorkspace() {
      if (!marketPoints.length) return;
      var compact = window.innerWidth <= 720;
      var visibleSubIndicators = usableSubIndicators(marketPoints, marketSubIndicators, "成交量");
      var visibleMainIndicators = marketMainIndicators.filter(function (indicator) {
        return indicatorReady(marketPoints, indicator, "成交量");
      });
      marketAnalysisChart.style.height = (compact ? 300 : 405)
        + visibleSubIndicators.length * (compact ? 104 : 118) + "px";
      var chart = klineChart("market-analysis-chart", marketPoints, {
        mode: marketChartMode,
        priceLabel: marketSource === "siteb" ? "公开大盘指数" : "行情站A 指数",
        priceUnit: "",
        calendarScale: marketSource === "siteb" && marketView === "full",
        fullRange: marketSource === "siteb" && marketView === "full",
        mainIndicators: visibleMainIndicators,
        subIndicators: visibleSubIndicators,
        activityLabel: "成交量"
      });
      marketDrawing = drawingController("market-analysis-chart", "market-drawing-layer", {
        statusId: "market-draw-status",
        toolSelector: "[data-market-draw-tool]"
      });
      if (marketDrawing && chart) {
        marketDrawing.bind(chart, "market." + marketSource + "." + marketPeriod,
          marketPoints.map(function (point) { return String(point.ts); }));
      }
      var labels = { ma: "MA", ema: "EMA", boll: "BOLL", volume: "成交量",
        macd: "MACD", rsi: "RSI", volatility: "波动率" };
      var active = visibleMainIndicators.concat(visibleSubIndicators);
      setText("market-indicator-count", active.length);
      var holder = el("market-active-indicators");
      if (holder) holder.innerHTML = active.map(function (key) {
        return "<span>" + esc(labels[key] || key) + "</span>";
      }).join("");
      syncMarketIndicatorInputs();
      saveMarketState();
    }

    function coverageState(id, info) {
      var node = el(id);
      if (!node) return;
      var available = info && info.available;
      node.textContent = available ? fmtI(info.count || 0) + " 点" : "暂无";
      node.className = available
        ? (info.sparse ? "available sparse" : "available")
        : "missing";
    }

    function setMarketFreshness(message, status) {
      var note = el("market-chart-freshness");
      if (!note) return;
      note.textContent = message;
      note.className = "data-source-note index-line-freshness " + (status || "ready");
    }
    function loadMarketWorkspace(preserve) {
      if (preserve === true && document.hidden) return;
      var requestedKey = [marketSource, marketPeriod, marketView].join("|");
      var keepChart = preserve === true && marketPoints.length && marketLoadedKey === requestedKey;
      if (!keepChart) {
        marketPoints = [];
        marketSignature = "";
        marketLoadedKey = "";
        clearChartView("market-analysis-chart", marketDrawing);
      }
      var loading = el("market-analysis-loading");
      if (loading && !keepChart) {
        loading.classList.remove("hidden");
        loading.classList.remove("error");
        loading.textContent = "指数行情加载中…";
      }
      json("/api/market/analysis?source=" + marketSource + "&period=" + marketPeriod
        + "&view=" + marketView, function (d) {
        if (d.sources) marketSources = d.sources;
        var incomingPoints = d.points || [];
        if (keepChart && !incomingPoints.length) {
          setMarketSummary(marketSource);
          setMarketFreshness("本次更新没有返回行情，保留上次成功图表；不会填造缺失数据。", "stale");
          return;
        }
        var signature = JSON.stringify([d.mode || "candlestick", incomingPoints]);
        var changed = signature !== marketSignature;
        marketPeriod = d.period || marketPeriod;
        marketPoints = incomingPoints;
        marketChartMode = d.mode || "candlestick";
        syncMarketControls();
        setText("market-chart-title", (marketSources[marketSource] && marketSources[marketSource].short_name || marketSource.toUpperCase())
          + " · " + (marketSource === "siteb" && marketView === "full"
            ? (marketPeriod === "7day" ? "站点原始周 K + 近期指数观测"
              : "原始日 K 历史 + 近期指数观测")
            : ({ "1hour": "1 小时", "4hour": "4 小时", "1day": "日线", "7day": "7 天", "15day": "15 天", "30day": "30 天" }[marketPeriod] || marketPeriod)));
        var range = d.range || {};
        setText("market-chart-range", range.count
          ? marketDateLabel(range.start) + " 至 " + marketDateLabel(range.end) + " · " + fmtI(range.count)
            + (marketSource === "siteb" && marketView === "full"
              ? " 个时间点（原始 K 线 " + fmtI(d.coverage.ohlc.count)
                + " 根 · 小时观测区间 " + fmtI(d.samples.observed)
                + " 根 · 单点 " + fmtI(d.samples.latest) + " 个）"
              : (marketChartMode === "line" ? " 个观测" : " 根"))
          : "当前周期暂无行情");
        coverageState("coverage-ohlc", d.coverage && d.coverage.ohlc);
        coverageState("coverage-volume", d.coverage && d.coverage.volume);
        coverageState("coverage-turnover", d.coverage && d.coverage.turnover);
        marketVolumeAvailable = usableSubIndicators(marketPoints, ["volume"], "成交量").length > 0;
        var marketVolumeInput = document.querySelector("[data-market-sub='volume']");
        if (marketVolumeInput) {
          marketVolumeInput.disabled = !marketVolumeAvailable;
          var volumeLabel = marketVolumeInput.closest("label");
          if (volumeLabel) volumeLabel.classList.toggle("disabled", !marketVolumeAvailable);
        }
        if (!marketVolumeAvailable && marketSubIndicators.indexOf("volume") !== -1) {
          marketSubIndicators = marketSubIndicators.filter(function (item) { return item !== "volume"; });
          if (!marketSubIndicators.length) marketSubIndicators = ["macd"];
          document.querySelectorAll("[data-market-sub]").forEach(function (input) {
            input.checked = marketSubIndicators.indexOf(input.dataset.marketSub) !== -1;
          });
          var volumeCoverage = d.coverage && d.coverage.volume;
          setText("market-indicator-note", volumeCoverage && volumeCoverage.available
            ? "成交量样本较稀疏，已切换为 MACD"
            : "该来源缺少成交量，已切换为 MACD");
        }
        var notes = [];
        if (d.message) notes.push(d.message);
        if (d.coverage && d.coverage.volume && d.coverage.volume.note) notes.push(d.coverage.volume.note);
        if (d.coverage && d.coverage.turnover && d.coverage.turnover.note) notes.push(d.coverage.turnover.note);
        setText("coverage-note", notes.join("；"));
        if (marketPoints.length) {
          if (changed) renderMarketWorkspace();
          marketSignature = signature;
          marketLoadedKey = [marketSource, marketPeriod, marketView].join("|");
          chartDone(marketAnalysisChart.closest(".chart-box"));
          setMarketFreshness(
            (marketSource === "siteb" ? "行情站B" : "行情站A")
            + " · 最新行情 " + marketTimeLabel(d.as_of || range.end) + "（北京时间）"
            + (d.status === "stale" ? " · 数据已过期" : "")
            + " · 每分钟读取已采集更新；保留缩放与画线",
            d.status === "stale" ? "stale" : "ready"
          );
        } else if (loading) {
          chartState(marketAnalysisChart.closest(".chart-box"), "当前来源与周期暂无行情", loadMarketWorkspace);
          setMarketFreshness("当前来源与周期暂无已采集行情。", "stale");
        }
      }, function () {
        if (keepChart) {
          setMarketFreshness("获取更新失败，当前图表保留上次成功数据；稍后自动重试，也可手动刷新。", "stale");
        } else {
          chartState(marketAnalysisChart.closest(".chart-box"), "指数行情加载失败，请重试", loadMarketWorkspace);
          setMarketFreshness("指数行情读取失败，请重试。", "stale");
        }
      }, "market-analysis");
    }

    document.querySelectorAll("[data-market-view]").forEach(function (button) {
      button.addEventListener("click", function () {
        marketView = button.dataset.marketView;
        syncMarketControls();
        CHART_ZOOM["market-analysis-chart"] = null;
        saveMarketState();
        loadMarketWorkspace();
      });
    });

    document.querySelectorAll("[data-market-source]").forEach(function (button) {
      button.addEventListener("click", function () {
        marketSource = button.dataset.marketSource === "sitea" ? "sitea" : "siteb";
        saveMarketState();
        CHART_ZOOM["market-analysis-chart"] = null;
        if (marketDrawing) marketDrawing.setMode(null);
        syncMarketControls();
        loadMarketWorkspace();
      });
    });
    document.querySelectorAll("[data-market-period]").forEach(function (button) {
      button.addEventListener("click", function () {
        if (button.disabled) return;
        marketPeriod = button.dataset.marketPeriod;
        saveMarketState();
        CHART_ZOOM["market-analysis-chart"] = null;
        if (marketDrawing) marketDrawing.setMode(null);
        syncMarketControls();
        loadMarketWorkspace();
      });
    });

    function setMarketPopover(panelId, triggerId, open) {
      var panel = el(panelId), trigger = el(triggerId);
      setChartPopover(panel, trigger, open);
    }
    var closeMarketPopovers = bindChartMenus([
      ["market-indicator-panel", "market-indicator-trigger"],
      ["market-draw-menu", "market-draw-trigger"]
    ], ".market-chart-commandbar", "market-indicator-close");
    document.querySelectorAll("[data-market-main]").forEach(function (input) {
      input.addEventListener("change", function () {
        marketMainIndicators = Array.prototype.map.call(
          document.querySelectorAll("[data-market-main]:checked"),
          function (item) { return item.dataset.marketMain; }
        );
        saveMarketState();
        renderMarketWorkspace();
      });
    });
    document.querySelectorAll("[data-market-sub]").forEach(function (input) {
      input.addEventListener("change", function () {
        var checked = Array.prototype.slice.call(document.querySelectorAll("[data-market-sub]:checked"));
        if (checked.length > 2) {
          input.checked = false;
          setText("market-indicator-note", "副图最多同时显示 2 个");
          return;
        }
        marketSubIndicators = checked.map(function (item) { return item.dataset.marketSub; });
        saveMarketState();
        setText("market-indicator-note", marketSubIndicators.length === 2 ? "已达到 2 个副图上限" : "默认组合适合大盘观察");
        renderMarketWorkspace();
      });
    });
    var marketIndicatorReset = el("market-indicator-reset");
    if (marketIndicatorReset) marketIndicatorReset.addEventListener("click", function () {
      marketMainIndicators = ["ma"];
      marketSubIndicators = marketVolumeAvailable ? ["volume"] : ["macd"];
      saveMarketState();
      document.querySelectorAll("[data-market-main]").forEach(function (input) {
        input.checked = marketMainIndicators.indexOf(input.dataset.marketMain) !== -1;
      });
      document.querySelectorAll("[data-market-sub]").forEach(function (input) {
        input.checked = marketSubIndicators.indexOf(input.dataset.marketSub) !== -1;
      });
      renderMarketWorkspace();
    });
    document.querySelectorAll("[data-market-draw-tool]").forEach(function (button) {
      button.addEventListener("click", function () {
        if (marketDrawing) marketDrawing.setMode(button.dataset.marketDrawTool);
        setMarketPopover("market-draw-menu", "market-draw-trigger", false);
      });
    });
    var marketDrawUndo = el("market-draw-undo");
    if (marketDrawUndo) marketDrawUndo.addEventListener("click", function () { if (marketDrawing) marketDrawing.undo(); });
    var marketDrawClear = el("market-draw-clear");
    if (marketDrawClear) marketDrawClear.addEventListener("click", function () { if (marketDrawing) marketDrawing.clear(); });
    syncMarketControls();
    loadMarketWorkspace();
    window.setInterval(function () { loadMarketWorkspace(true); }, 60000);
    document.addEventListener("visibilitychange", function () {
      if (!document.hidden) loadMarketWorkspace(true);
    });
    var marketRefresh = el("market-refresh");
    if (marketRefresh) marketRefresh.addEventListener("click", function () {
      loadMarketWorkspace(true);
    });
  }
  if (el("index-spark")) {
    json("/api/market/index", function (d) {
      var values = (d.points || []).map(function (p) { return p.close; });
      sparkChart("index-spark", values.slice(-80), { color: trendColor(values) });
      var sk = el("index-spark").querySelector(".skeleton");
      if (sk) sk.remove();
    }, function () {
      var sk = el("index-spark").querySelector(".skeleton");
      if (sk) sk.classList.add("quote-missing");
      setText("kline-refresh", "走势暂不可用");
    }, "index-spark");
  }

  // ---------- 市场宽度（大盘页） ----------
  function initBreadth() {
    var bar = el("breadth-bar");
    if (!bar) return;
    var counts = { up: 0, down: 0, flat: 0 };
    var pending = 2;
    function finish() {
      var total = counts.up + counts.down + counts.flat || 1;
      var html = "";
      ["up", "down", "flat"].forEach(function (k) {
        if (counts[k]) html += "<i class='" + k + "' style='width:" + (counts[k] / total * 100) + "%'></i>";
      });
      bar.innerHTML = html || "<i class='flat' style='width:100%'></i>";
      setText("b-up", counts.up);
      setText("b-down", counts.down);
      setText("b-flat", counts.flat);
      setText("breadth-ratio", counts.up + "涨 / " + counts.down + "跌");
      touch("breadth-refresh");
    }
    function count(items) {
      (items || []).forEach(function (r) {
        var d = r.diff && r.diff.buff;
        if (d == null || isNaN(d) || Math.abs(d) < 0.5) counts.flat++;
        else if (d >= 0) counts.up++;
        else counts.down++;
      });
    }
    function settle(items) {
      if (items) count(items);
      if (--pending === 0) finish();
    }
    json("/api/rank-live?kind=gain&limit=100", function (d) { settle(d.items); }, function () { settle(); }, "breadth-gain");
    json("/api/rank-live?kind=loss&limit=100", function (d) { settle(d.items); }, function () { settle(); }, "breadth-loss");
  }
  if (el("breadth-bar")) initBreadth();

  // ---------- 首页摘要卡刷新（60s） ----------
  function renderSummary(s) {
    s = s || {};
    function setT(id, v) { setText(id, v); }
    setT("m-index", s.broadMarketIndex != null ? Number(s.broadMarketIndex).toFixed(2) : null);
    var r = s.diffYesterdayRatio;
    var dd = el("m-index-diff");
    if (dd) {
      dd.className = "stat-diff " + ((r || 0) >= 0 ? "up" : "down");
      var pill = dd.querySelector(".pill");
      if (pill) pill.textContent = r != null ? (r >= 0 ? "+" : "") + Number(r).toFixed(2) + "%" : "-";
    }
    setT("m-index-abs", s.diffYesterday != null ? (s.diffYesterday >= 0 ? "+" : "") + Number(s.diffYesterday).toFixed(2) : null);
    setT("m-holders", fmtMetric(s.holdersNum));
    setT("m-survive", fmtMetric(s.surviveNum));
    setT("m-risefall", fmtMetric(s.riseFallDays));
    setT("m-trade", fmtMetric(s["todayStatistics.tradeNum"]));
    setT("m-trade-turnover", fmtMetric(s["todayStatistics.turnover"], 2));
    setT("m-trade-add", fmtMetric(s["todayStatistics.addNum"]));
    setT("m-trade-valuation", fmtMetric(s["todayStatistics.addValuation"], 2));
    setT("m-yday", fmtMetric(s["yesterdayStatistics.tradeNum"]));
    setT("m-yday-turnover", fmtMetric(s["yesterdayStatistics.turnover"], 2));
    setT("m-yday-add", fmtMetric(s["yesterdayStatistics.addNum"]));
    setT("m-yday-valuation", fmtMetric(s["yesterdayStatistics.addValuation"], 2));
  }
  if (el("m-index")) {
    setInterval(function () {
      json("/api/market/summary", function (d) { renderSummary(d.snap); }, null, "market-summary");
    }, 60000);
  }

  // ---------- 榜单行 ----------
  function hotRow(r, i, full) {
    var c = r.diff || {};
    var d = c.buff;
    return "<tr><td>" + rankNum(i) + "</td>"
      + "<td>" + itemCell(r, full ? "/research/rankings" : "/", !full) + "</td>"
      + "<td class='num strong'>" + fmtI(r.broker_sell) + "</td>"
      + (full
        ? "<td class='num'>" + fmt(r.price.youpin) + "</td>"
          + "<td class='num'>" + fmt(r.price.c5) + "</td>"
          + "<td class='num'>" + fmt(r.price.steam) + "</td>"
          + "<td class='num'>" + fmt(r.price.haloskins) + "</td>"
        : "")
      + "<td class='num'>" + fmt(r.price.buff) + "</td>"
      + "<td class='num'>" + fmt(r.avg7.buff) + "</td>"
      + "<td class='num'>" + chip(d) + "</td>"
      + (full ? "<td class='muted'><span class='reltime' data-ts='" + esc(r.ts) + "'>" + reltime(r.ts) + "</span></td>" : "")
      + "</tr>";
  }
  function hotCompactRow(r, i) {
    var d = r.diff && r.diff.buff;
    return "<tr><td>" + rankNum(i) + "</td>"
      + "<td>" + itemCell(r, "/", true) + "</td>"
      + "<td class='num strong'>" + fmtI(r.broker_sell) + "</td>"
      + "<td class='num'>" + chip(d) + "</td></tr>";
  }
  function gainRow(r, i) {
    var c = r.diff || {};
    var d = c.buff;
    return "<tr><td>" + rankNum(i) + "</td>"
      + "<td>" + itemCell(r, "/research/rankings", false) + "</td>"
      + "<td class='num'>" + fmt(r.price.buff) + "</td>"
      + "<td class='num'>" + fmt(r.avg7.buff) + "</td>"
      + "<td class='num'>" + chip(d) + "</td>"
      + "<td class='num strong'>" + fmtI(r.broker_sell) + "</td>"
      + "<td class='muted'><span class='reltime' data-ts='" + esc(r.ts) + "'>" + reltime(r.ts) + "</span></td></tr>";
  }
  function loadHot() {
    var body = el("hot-body");
    if (!body) return;
    json("/api/hot", function (d) {
      var hot = (d.hot || []).slice(0, 20);
      body.innerHTML = hot.length
        ? hot.map(function (r, i) { return hotRow(r, i, true); }).join("")
        : "<tr><td colspan='11' class='empty'>暂无热度数据</td></tr>";
      var top = el("hot-top-body");
      if (top) {
        top.innerHTML = hot.slice(0, 10).map(hotCompactRow).join("")
          || "<tr><td colspan='4' class='empty'>暂无热度数据</td></tr>";
      }
      processReltime(body);
      refreshIcons(body);
      if (top) refreshIcons(top);
      touch("hot-refresh");
    }, null, "hot-live");
  }
  if (el("hot-body")) { loadHot(); setInterval(loadHot, 90000); }

  // ---------- 排行榜（一级 seg + 二级胶囊排序） ----------
  var RANK_ROWS = {}, RANK_SORT = {};
  function rankSortValue(pane, r) {
    var s = RANK_SORT[pane] || "default";
    if (s === "price") return (r.price && r.price.buff != null) ? -r.price.buff : null;
    if (s === "diff") {
      var d = r.diff && r.diff.buff;
      if (d == null || isNaN(d)) return null;
      return pane === "loss" ? d : -d;
    }
    if (s === "broker") return -(r.broker_sell || 0);
    return null;
  }
  function renderRankPane(kind) {
    var body = document.querySelector("tbody[data-kind='" + kind + "']");
    if (!body) return;
    var rows = (RANK_ROWS[kind] || []).slice();
    if (RANK_SORT[kind] && RANK_SORT[kind] !== "default") {
      rows.sort(function (a, b) {
        var av = rankSortValue(kind, a), bv = rankSortValue(kind, b);
        if (av == null && bv == null) return 0;
        if (av == null) return 1;
        if (bv == null) return -1;
        return av - bv;
      });
    }
    body.innerHTML = rows.length
      ? rows.map(kind === "hot"
          ? function (r, i) { return hotRow(r, i, true); }
          : gainRow).join("")
      : "<tr><td colspan='" + (kind === "hot" ? 11 : 7) + "' class='empty'>暂无数据</td></tr>";
    processReltime(body);
    refreshIcons(body);
    touch("rank-refresh");
  }
  function loadRank(kind) {
    json("/api/rank-live?kind=" + kind + "&limit=50", function (d) {
      RANK_ROWS[kind] = d.items || [];
      renderRankPane(kind);
    }, function () {
      if (!RANK_ROWS[kind]) RANK_ROWS[kind] = [];
      renderRankPane(kind);
    }, "rank-live-" + kind);
  }
  var rankBodies = document.querySelectorAll("tbody[data-kind]");
  if (rankBodies.length) {
    function selectRankPane(kind, updateUrl) {
      var selected = ["hot", "gain", "loss"].indexOf(kind) !== -1 ? kind : "hot";
      document.querySelectorAll(".seg button[data-rank]").forEach(function (button) {
        var active = button.dataset.rank === selected;
        button.classList.toggle("active", active);
        button.setAttribute("aria-selected", active ? "true" : "false");
        button.setAttribute("tabindex", active ? "0" : "-1");
      });
      document.querySelectorAll(".rank-pane").forEach(function (pane) {
        var active = pane.id === "rank-" + selected;
        pane.classList.toggle("active", active);
        pane.setAttribute("aria-hidden", active ? "false" : "true");
      });
      if (updateUrl && window.history && window.history.replaceState) {
        var url = new URL(window.location.href);
        if (selected === "hot") url.searchParams.delete("kind");
        else url.searchParams.set("kind", selected);
        window.history.replaceState({}, "", url.pathname + url.search + url.hash);
      }
    }
    rankBodies.forEach(function (tb) { loadRank(tb.getAttribute("data-kind")); });
    setInterval(function () {
      rankBodies.forEach(function (tb) { loadRank(tb.getAttribute("data-kind")); });
    }, 120000);
    document.querySelectorAll(".seg button[data-rank]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        selectRankPane(btn.dataset.rank, true);
      });
      btn.addEventListener("keydown", function (event) {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
        var buttons = Array.prototype.slice.call(document.querySelectorAll(".seg button[data-rank]"));
        var current = buttons.indexOf(btn);
        var next = event.key === "ArrowRight"
          ? (current + 1) % buttons.length
          : (current - 1 + buttons.length) % buttons.length;
        event.preventDefault();
        selectRankPane(buttons[next].dataset.rank, true);
        buttons[next].focus();
      });
    });
    document.querySelectorAll("[data-sort-pane] button[data-sort]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var pane = btn.closest("[data-sort-pane]").getAttribute("data-sort-pane");
        Array.prototype.forEach.call(btn.parentElement.querySelectorAll("button"), function (b) {
          var active = b === btn;
          b.classList.toggle("active", active);
          b.setAttribute("aria-pressed", active ? "true" : "false");
        });
        RANK_SORT[pane] = btn.dataset.sort;
        renderRankPane(pane);
      });
    });
    var initialRank = document.querySelector(".seg button[data-rank].active");
    selectRankPane(initialRank ? initialRank.dataset.rank : "hot", false);
  }

  // ---------- 异动 ----------
  var ALL_EVENTS = [];
  function radarReturnPath() { return "/monitor/radar" + location.search; }
  function trackerRow(e) {
    return "<tr data-role='" + roleOf(e.platform)[1] + "'>"
      + "<td class='muted'><span class='reltime' data-ts='" + esc(e.ts) + "'>" + reltime(e.ts) + "</span></td>"
      + "<td>" + roleBadge(e.platform) + "</td>"
      + "<td>" + itemCell(e, radarReturnPath(), true) + "</td>"
      + "<td>" + metricTag(e.metric) + "</td>"
      + "<td class='num'>" + eventValue(e.metric, e.old) + "</td>"
      + "<td class='num'>" + eventValue(e.metric, e.new) + "</td>"
      + "<td class='num'><a class='chip-num " + diffCls(e.ratio) + "' href='/monitor/events/" + encodeURIComponent(e.key || "")
      + "?from=" + encodeURIComponent(radarReturnPath()) + "' title='查看触发证据'>" + diffTxt(e.ratio) + "</a></td></tr>";
  }
  var trackerRoleFilter = "all";
  function saveRadarState() {
    var url = new URL(location.href);
    var field = el("tracker-q");
    var q = field ? field.value.trim() : "";
    if (q) url.searchParams.set("q", q);
    else url.searchParams.delete("q");
    if (trackerRoleFilter !== "all") url.searchParams.set("role", trackerRoleFilter);
    else url.searchParams.delete("role");
    window.history.replaceState({}, "", url.pathname + url.search);
  }
  function renderTracker() {
    var body = el("tracker-body");
    if (!body) return;
    var q = "";
    var qInput = el("tracker-q");
    if (qInput) q = qInput.value.trim().toLowerCase();
    var evs = ALL_EVENTS.filter(function (e) {
      var role = roleOf(e.platform)[1];
      if (trackerRoleFilter === "focus" && role !== "focus") return false;
      if (trackerRoleFilter === "other" && role === "focus") return false;
      if (q && String(e.name).toLowerCase().indexOf(q) === -1) return false;
      return true;
    });
    body.innerHTML = evs.map(trackerRow).join("")
      || "<tr><td colspan='7' class='empty'>暂无异动数据</td></tr>";
    processReltime(body);
    refreshIcons(body);
    if (el("t-count")) el("t-count").textContent = ALL_EVENTS.length;
    if (el("t-focus")) el("t-focus").textContent = ALL_EVENTS.filter(function (e) { return roleOf(e.platform)[1] === "focus"; }).length;
    if (el("t-max")) {
      var mx = ALL_EVENTS.reduce(function (a, b) { return Math.abs(b.ratio) > Math.abs(a.ratio) ? b : a; }, { ratio: 0 });
      el("t-max").textContent = diffTxt(mx.ratio);
    }
  }
  function loadTracker() {
    json("/api/tracker-live?limit=60", function (d) {
      ALL_EVENTS = (d.events || []).slice().sort(function (a, b) {
        return String(b.ts).localeCompare(String(a.ts));
      });
      renderTracker();
      var mini = el("tracker-mini-body");
      if (mini) {
        mini.innerHTML = ALL_EVENTS.slice(0, 6).map(function (e) {
          return "<tr><td>" + itemCell(e, radarReturnPath(), true) + "</td><td>" + roleBadge(e.platform) + "</td>"
            + "<td class='num'>" + chip(e.ratio) + "</td></tr>";
        }).join("") || "<tr><td colspan='3' class='empty'>暂无异动</td></tr>";
        processReltime(mini);
        refreshIcons(mini);
      }
      touch("tracker-refresh");
    }, function () {
      if (!ALL_EVENTS.length) renderTracker();
      touch("tracker-refresh");
    }, "tracker-live");
  }
  if (el("tracker-body")) {
    var radarParams = new URLSearchParams(location.search);
    trackerRoleFilter = ["focus", "other"].indexOf(radarParams.get("role")) !== -1
      ? radarParams.get("role") : "all";
    document.querySelectorAll(".seg button[data-filter]").forEach(function (button) {
      button.classList.toggle("active", button.dataset.filter === trackerRoleFilter);
    });
    var qInput = el("tracker-q");
    if (qInput) {
      qInput.value = radarParams.get("q") || (location.hash.indexOf("#q=") === 0
        ? decodeURIComponent(location.hash.slice(3)) : "");
      if (location.hash.indexOf("#q=") === 0) {
        window.history.replaceState({}, "", location.pathname + "?q=" + encodeURIComponent(qInput.value));
      }
      qInput.addEventListener("input", function () { saveRadarState(); renderTracker(); });
    }
    loadTracker(); setInterval(loadTracker, 60000);
    document.querySelectorAll(".seg button[data-filter]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        document.querySelectorAll(".seg button[data-filter]").forEach(function (b) { b.classList.remove("active"); });
        btn.classList.add("active");
        trackerRoleFilter = btn.dataset.filter;
        saveRadarState();
        renderTracker();
      });
    });
  } else if (el("tracker-mini-body")) {
    loadTracker();
    setInterval(loadTracker, 60000);
  }

  // ---------- 批量报价 ----------
  function loadQuotes() {
    var rows = document.querySelectorAll("tr[data-id]");
    if (!rows.length) return;
    var ids = Array.prototype.map.call(rows, function (tr) { return tr.getAttribute("data-id"); });
    Array.prototype.forEach.call(rows, function (tr) {
      tr.classList.add("quote-refreshing");
      tr.setAttribute("aria-busy", "true");
    });
    json("/api/items/quote?ids=" + encodeURIComponent(ids.join(",")), function (d) {
      var qs = d.quotes || {};
      var latestQuote = "";
      Array.prototype.forEach.call(rows, function (tr) {
        var q = qs[tr.getAttribute("data-id")];
        if (q && q.ts && String(q.ts) > latestQuote) latestQuote = String(q.ts);
        var b = tr.querySelector(".q-buff"), k = tr.querySelector(".q-broker");
        var available = !!(q && q.available);
        if (b) {
          b.textContent = q && q.buff_price != null ? fmt(q.buff_price) : "—";
          b.classList.toggle("quote-missing", !available || q.buff_price == null);
        }
        if (k) {
          k.textContent = q && q.broker_sell != null ? fmtI(q.broker_sell) : "—";
          k.classList.toggle("quote-missing", !available || q.broker_sell == null);
        }
        tr.classList.remove("quote-refreshing");
        tr.removeAttribute("aria-busy");
      });
      setText("quote-refresh", latestQuote
        ? "快照 " + latestQuote.slice(0, 16)
        : "当前页没有可用报价");
    }, function () {
      Array.prototype.forEach.call(rows, function (tr) {
        var b = tr.querySelector(".q-buff"), k = tr.querySelector(".q-broker");
        if (b) { b.textContent = "—"; b.classList.add("quote-missing"); }
        if (k) { k.textContent = "—"; k.classList.add("quote-missing"); }
        tr.classList.remove("quote-refreshing");
        tr.removeAttribute("aria-busy");
      });
      setText("quote-refresh", "报价请求失败，请稍后刷新");
    }, "items-quotes");
  }
  if (document.querySelector("tr[data-id]")) { loadQuotes(); setInterval(loadQuotes, 120000); }

  // ---------- 饰品详情 ----------
  var itemChart = el("item-kline-chart");
  if (itemChart) {
    var itemPath = location.pathname;
    var itemPrefix = itemPath.indexOf("/market/items/") === 0 ? "/market/items/"
      : itemPath.indexOf("/cs2/") === 0 ? "/cs2/" : "/item/";
    var name = decodeURIComponent(itemPath.slice(itemPrefix.length).replace(/\/+$/, ""));
    var savedItemState = readViewState("item." + encodeURIComponent(name), {});
    var hints = { "p1": "相位一", "p2": "相位二", "p3": "相位三", "p4": "相位四",
      "ruby": "红宝石", "emerald": "绿宝石", "sapphire": "蓝宝石", "blackpearl": "黑珍珠",
      "sun": "太阳", "singleblue": "单面全蓝", "t1": "一档", "t2": "二档", "t3": "三档", "t4": "四档" };
    var PTS = [];
    var PERIOD = ["day", "week", "hour"].indexOf(savedItemState.period) !== -1
      ? savedItemState.period : "day";
    var CHART_SOURCE = typeof savedItemState.source === "string" && savedItemState.source
      ? savedItemState.source : "all";
    var sourceChosen = savedItemState.sourceChosen === true;
    var CHART_SOURCE_LABEL = "总览";
    var CHART_MODE = "candlestick";
    var ACTIVITY_LABEL = "成交量";
    var ACTIVITY_AVAILABLE = true;
    var CHART_RANGE = {};
    var ITEM_AVAILABILITY = null;
    var MAIN_INDICATORS = validIndicators(savedItemState.main, ["ma", "ema", "boll"]);
    var SUB_INDICATORS = validIndicators(savedItemState.sub, ["volume", "macd", "rsi", "volatility"]);
    if (!MAIN_INDICATORS.length) MAIN_INDICATORS = ["ma"];
    if (!SUB_INDICATORS.length) SUB_INDICATORS = ["volume"];
    var VISIBLE_SUB_INDICATORS = [];
    var ITEM_DRAWING = null;
    var historyLoaded = false;
    var historyLoading = false;
    var platformLoaded = false;
    var platformLoading = false;
    var changeLoaded = false;
    var changeLoading = false;
    var styleLoaded = {};
    var styleLoadingKey = "";
    var itemSourceMeta = {};
    var itemSourcesReady = false;
    function saveItemState() {
      writeViewState("item." + encodeURIComponent(name), {
        source: CHART_SOURCE,
        sourceChosen: sourceChosen,
        period: PERIOD,
        main: MAIN_INDICATORS,
        sub: SUB_INDICATORS
      });
    }

    // Tab 切换（含吸顶摘要条快捷跳转）
    function switchTab(tab) {
      document.querySelectorAll(".tab[data-tab]").forEach(function (b) {
        var active = b.dataset.tab === tab;
        b.classList.toggle("active", active);
        b.setAttribute("aria-selected", active ? "true" : "false");
        b.setAttribute("tabindex", active ? "0" : "-1");
      });
      document.querySelectorAll(".tab-pane").forEach(function (p) {
        var active = p.id === "tab-" + tab;
        p.classList.toggle("active", active);
        p.setAttribute("aria-hidden", active ? "false" : "true");
      });
      var btn = document.querySelector(".tab[data-tab='" + tab + "']");
      var pane = el("tab-" + tab);
      if (pane && tab === "history") {
        window.requestAnimationFrame(function () {
          resizeChart("item-history-chart");
        });
      }
      if (tab === "styles") loadStyle();
      if (tab === "change") loadChange();
      if (tab === "platform") loadPlatformFresh();
      if (tab === "history" && !historyLoaded && !historyLoading) {
        loadHistoryChart();
      }
      if (tab === "kline") renderKline();
      var bar = el("item-summary-bar");
      if (bar) window.scrollTo({ top: Math.max(bar.offsetTop - 80, 0), behavior: REDUCED ? "auto" : "smooth" });
    }
    document.querySelectorAll(".tab[data-tab]").forEach(function (btn) {
      btn.addEventListener("click", function () { switchTab(btn.dataset.tab); });
      btn.addEventListener("keydown", function (event) {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
        var buttons = Array.prototype.slice.call(document.querySelectorAll(".tab[data-tab]"));
        var current = buttons.indexOf(btn);
        var next = event.key === "ArrowRight"
          ? (current + 1) % buttons.length
          : (current - 1 + buttons.length) % buttons.length;
        event.preventDefault();
        switchTab(buttons[next].dataset.tab);
        buttons[next].focus();
      });
    });
    document.querySelectorAll("[data-tab-jump]").forEach(function (btn) {
      btn.addEventListener("click", function () { switchTab(btn.dataset.tabJump); });
    });

    // 周K 聚合（日K 数据折算，诚实地标注来源）
    function weekKey(d) {
      if (!d || isNaN(d.getTime())) return "";
      var t = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
      var day = t.getUTCDay() || 7;
      t.setUTCDate(t.getUTCDate() + 4 - day);
      var y = t.getUTCFullYear();
      var start = new Date(Date.UTC(y, 0, 1));
      var w = Math.ceil((((t - start) / 86400000) + 1) / 7);
      return y + "-W" + w;
    }
    function aggregateWeek(points) {
      var out = [];
      points.forEach(function (p) {
        var key = weekKey(parseTs(p.ts));
        if (!key) return;
        var last = out[out.length - 1];
        var open = p.open == null ? p.close : p.open;
        var high = p.high == null ? p.close : p.high;
        var low = p.low == null ? p.close : p.low;
        if (!last || last.key !== key) {
          out.push({ key: key, ts: String(p.ts).slice(0, 10), open: open, high: high,
            low: low, close: p.close, volume: p.volume || 0, listing_count: p.listing_count });
        } else {
          last.high = Math.max(last.high, high);
          last.low = Math.min(last.low, low);
          last.close = p.close;
          last.volume += p.volume || 0;
          if (p.listing_count != null) last.listing_count = p.listing_count;
          last.ts = String(p.ts).slice(0, 10);
        }
      });
      return out;
    }

    function syncIndicatorSummary() {
      var labels = { ma: "MA", ema: "EMA", boll: "BOLL", volume: ACTIVITY_LABEL,
        macd: "MACD", rsi: "RSI", volatility: "波动率" };
      var requirements = { ma: 5, ema: 2, boll: 30, volume: 1, macd: 2, rsi: 15, volatility: 30 };
      var shownSubIndicators = VISIBLE_SUB_INDICATORS.length
        ? VISIBLE_SUB_INDICATORS : SUB_INDICATORS;
      var active = MAIN_INDICATORS.concat(shownSubIndicators);
      var holder = el("active-indicators");
      if (holder) holder.innerHTML = active.map(function (key) {
        return "<span>" + esc(labels[key] || key) + "</span>";
      }).join("");
      setText("indicator-count", active.length);
      setText("activity-indicator-name", ACTIVITY_LABEL);
      setText("activity-indicator-desc", ACTIVITY_LABEL === "在售量" ? "平台快照中的在售数量" : "每日成交量");
      document.querySelectorAll("[data-main-indicator]").forEach(function (input) {
        input.checked = MAIN_INDICATORS.indexOf(input.dataset.mainIndicator) !== -1;
        var available = (requirements[input.dataset.mainIndicator] || 1) <= PTS.length;
        input.disabled = !available;
        input.closest("label").classList.toggle("disabled", !available);
      });
      document.querySelectorAll("[data-sub-indicator]").forEach(function (input) {
        input.checked = SUB_INDICATORS.indexOf(input.dataset.subIndicator) !== -1;
        var available = (requirements[input.dataset.subIndicator] || 1) <= PTS.length;
        if (input.dataset.subIndicator === "volume") {
          available = available && ACTIVITY_AVAILABLE
            && usableSubIndicators(PTS, ["volume"], ACTIVITY_LABEL).length > 0;
        }
        input.disabled = !available;
        input.closest("label").classList.toggle("disabled", !available);
      });
      saveItemState();
    }
    function enforceIndicatorAvailability() {
      var removed = [];
      if (PTS.length < 30 && MAIN_INDICATORS.indexOf("boll") !== -1) removed.push("BOLL");
      if (PTS.length < 15 && SUB_INDICATORS.indexOf("rsi") !== -1) removed.push("RSI");
      if (PTS.length < 30 && SUB_INDICATORS.indexOf("volatility") !== -1) removed.push("波动率");
      if (ACTIVITY_AVAILABLE
        && SUB_INDICATORS.indexOf("volume") !== -1
        && !usableSubIndicators(PTS, ["volume"], ACTIVITY_LABEL).length) {
        removed.push(ACTIVITY_LABEL + "样本");
      }
      MAIN_INDICATORS = MAIN_INDICATORS.filter(function (key) { return key !== "boll" || PTS.length >= 30; });
      SUB_INDICATORS = SUB_INDICATORS.filter(function (key) {
        if (key === "rsi") return PTS.length >= 15;
        if (key === "volatility") return PTS.length >= 30;
        if (key === "volume") {
          return ACTIVITY_AVAILABLE && usableSubIndicators(PTS, ["volume"], ACTIVITY_LABEL).length;
        }
        return true;
      });
      if (!SUB_INDICATORS.length && PTS.length >= 2) SUB_INDICATORS = ["macd"];
      if (removed.length) setText("indicator-limit-note", removed.join("、") + "需要更多样本，已暂时隐藏");
    }

    function renderKline() {
      var box = el("item-kline-chart").closest(".chart-box");
      var emptyEl = el("kline-empty");
      if (PERIOD === "hour") {
        box.classList.add("hidden");
        emptyEl.classList.remove("hidden");
        emptyEl.textContent = "时K 粒度暂未采集 · 可切换 日K / 周K 查看";
        return;
      }
      box.classList.remove("hidden");
      emptyEl.classList.add("hidden");
      if (!PTS.length) {
        box.classList.add("hidden");
        emptyEl.classList.remove("hidden");
        emptyEl.textContent = ITEM_AVAILABILITY && ITEM_AVAILABILITY.catalog.available
          ? "该数据源仍在采集，可先查看平台报价或切换其他来源"
          : "当前数据源暂无可用历史";
        return;
      }
      var pts = PERIOD === "week" ? aggregateWeek(PTS) : PTS;
      var chartDom = el("item-kline-chart");
      var compact = window.innerWidth <= 720;
      VISIBLE_SUB_INDICATORS = usableSubIndicators(pts, SUB_INDICATORS, ACTIVITY_LABEL);
      var visibleMainIndicators = MAIN_INDICATORS.filter(function (indicator) {
        return indicatorReady(pts, indicator, ACTIVITY_LABEL);
      });
      chartDom.style.height = (compact ? 300 : 405)
        + VISIBLE_SUB_INDICATORS.length * (compact ? 104 : 118) + "px";
      var chart = klineChart("item-kline-chart", pts, {
        mode: CHART_MODE,
        priceLabel: CHART_SOURCE === "site_history" ? "站点在售价"
          : (CHART_MODE === "line" ? CHART_SOURCE_LABEL + " 卖价" : "价格"),
        fullRange: CHART_SOURCE === "site_history",
        activityLabel: ACTIVITY_LABEL,
        mainIndicators: visibleMainIndicators,
        subIndicators: VISIBLE_SUB_INDICATORS
      });
      ITEM_DRAWING = drawingController("item-kline-chart", "item-drawing-layer", {
        hintId: "drawing-hint",
        statusId: "draw-tool-status",
        toolSelector: "[data-draw-tool]"
      });
      if (ITEM_DRAWING && chart) {
        ITEM_DRAWING.bind(chart, encodeURIComponent(name) + "." + CHART_SOURCE + "." + PERIOD,
          pts.map(function (point) { return String(point.ts); }));
      }
      var periodLabel = PERIOD === "week" ? "周线（日线折算）" : "日线";
      var modeLabel = CHART_MODE === "line" ? "价格快照折线" : "OHLC K 线";
      setText("chart-source-meta", CHART_SOURCE_LABEL + " · " + periodLabel + " · "
        + modeLabel + " · " + String(pts[0].ts).slice(0, 10) + " 至 "
        + String(pts[pts.length - 1].ts).slice(0, 10) + " · " + pts.length + " 点");
      el("kline-hint").textContent = CHART_SOURCE_LABEL + " · " + pts.length
        + (PERIOD === "week" ? " 根周线" : (CHART_MODE === "line" ? " 个价格快照" : " 根日 K"));
      syncIndicatorSummary();
    }
    var periodBar = el("kline-period");
    function syncItemPeriodControl() {
      if (!periodBar) return;
      periodBar.querySelectorAll("button").forEach(function (button) {
        var active = button.dataset.period === PERIOD;
        button.classList.toggle("active", active);
        button.setAttribute("aria-selected", active ? "true" : "false");
      });
    }
    if (periodBar) {
      Array.prototype.forEach.call(periodBar.querySelectorAll("button"), function (btn) {
        btn.addEventListener("click", function () {
          Array.prototype.forEach.call(periodBar.querySelectorAll("button"), function (b) { b.classList.remove("active"); });
          btn.classList.add("active");
          PERIOD = btn.dataset.period;
          saveItemState();
          syncItemPeriodControl();
          renderKline();
        });
      });
      syncItemPeriodControl();
    }

    function setPopover(panelId, triggerId, open) {
      var panel = el(panelId);
      var trigger = el(triggerId);
      setChartPopover(panel, trigger, open);
    }
    var closeChartPopovers = bindChartMenus([
      ["chart-source-menu", "chart-source-trigger"],
      ["chart-indicator-panel", "chart-indicator-trigger"],
      ["chart-draw-menu", "chart-draw-trigger"]
    ], ".chart-commandbar", "indicator-close");

    document.querySelectorAll("[data-main-indicator]").forEach(function (input) {
      input.addEventListener("change", function () {
        MAIN_INDICATORS = Array.prototype.map.call(
          document.querySelectorAll("[data-main-indicator]:checked"),
          function (item) { return item.dataset.mainIndicator; }
        );
        enforceIndicatorAvailability();
        saveItemState();
        renderKline();
      });
    });
    document.querySelectorAll("[data-sub-indicator]").forEach(function (input) {
      input.addEventListener("change", function () {
        var checked = Array.prototype.slice.call(document.querySelectorAll("[data-sub-indicator]:checked"));
        if (checked.length > 2) {
          input.checked = false;
          setText("indicator-limit-note", "副图最多同时显示 2 个，已保留原组合");
          return;
        }
        SUB_INDICATORS = checked.map(function (item) { return item.dataset.subIndicator; });
        enforceIndicatorAvailability();
        saveItemState();
        setText("indicator-limit-note", SUB_INDICATORS.length === 2
          ? "已达到 2 个副图上限"
          : "默认组合适合日常观察");
        renderKline();
      });
    });
    var indicatorReset = el("indicator-reset");
    if (indicatorReset) indicatorReset.addEventListener("click", function () {
      MAIN_INDICATORS = ["ma"];
      SUB_INDICATORS = ["volume"];
      enforceIndicatorAvailability();
      saveItemState();
      setText("indicator-limit-note", "已恢复默认组合");
      renderKline();
    });
    var summaryIndicator = el("summary-indicator-trigger");
    if (summaryIndicator) summaryIndicator.addEventListener("click", function () {
      setTimeout(function () {
        closeChartPopovers("chart-indicator-panel");
        setPopover("chart-indicator-panel", "chart-indicator-trigger", true);
      }, REDUCED ? 0 : 220);
    });

    document.querySelectorAll("[data-draw-tool]").forEach(function (button) {
      button.addEventListener("click", function () {
        if (ITEM_DRAWING) ITEM_DRAWING.setMode(button.dataset.drawTool);
        setPopover("chart-draw-menu", "chart-draw-trigger", false);
      });
    });
    var drawUndo = el("draw-undo");
    if (drawUndo) drawUndo.addEventListener("click", function () { if (ITEM_DRAWING) ITEM_DRAWING.undo(); });
    var drawClear = el("draw-clear");
    if (drawClear) drawClear.addEventListener("click", function () { if (ITEM_DRAWING) ITEM_DRAWING.clear(); });

    // 价格变化卡片（今日 / 本周 / 本月）+ 吸顶摘要条
    function pct(cur, prev) { return prev ? (cur - prev) / prev * 100 : null; }
    function setCard(id, chgId, cur, prev) {
      setText(id, cur == null ? "-" : fmt(cur));
      var c = el(chgId);
      if (!c) return;
      var v = pct(cur, prev);
      if (v == null) { c.textContent = "-"; c.className = "chg"; return; }
      c.textContent = (v >= 0 ? "+" : "") + v.toFixed(2) + "%";
      c.className = "chg " + diffCls(v);
    }
    function renderPriceCards(updateHero) {
      if (!PTS.length) return;
      var last = PTS[PTS.length - 1].close;
      var prev1 = PTS.length > 1 ? PTS[PTS.length - 2].close : null;
      var prev7 = PTS.length > 7 ? PTS[PTS.length - 8].close : null;
      var prev30 = PTS.length > 30 ? PTS[PTS.length - 31].close : null;
      setCard("pc-day", "pc-day-chg", last, prev1);
      setCard("pc-week", "pc-week-chg", last, prev7);
      setCard("pc-month", "pc-month-chg", last, prev30);
      if (!updateHero) return;
      setText("sbar-price", fmt(last));
      var d = pct(last, prev1);
      var sd = el("sbar-diff");
      if (sd && d != null) {
        sd.textContent = (d >= 0 ? "+" : "") + d.toFixed(2) + "%";
        sd.className = "v " + diffCls(d);
      }
      setText("hero-price", fmt(last));
      setText("hero-diff-value", d == null ? null : diffTxt(d));
      var heroDiff = el("hero-diff");
      if (heroDiff && d != null) heroDiff.className = "quote-change " + diffCls(d);
      setText("sbar-time", String(PTS[PTS.length - 1].ts || "").slice(0, 10));
    }

    function loadChartSource(source) {
      CHART_SOURCE = source || "all";
      VISIBLE_SUB_INDICATORS = [];
      PTS = [];
      clearChartView("item-kline-chart", ITEM_DRAWING);
      var chartBox = el("item-kline-chart").closest(".chart-box");
      chartBox.classList.remove("hidden");
      var emptyEl = el("kline-empty");
      if (emptyEl) emptyEl.classList.add("hidden");
      var loading = el("kline-loading");
      if (loading) {
        loading.classList.remove("hidden");
        loading.classList.remove("error");
        loading.textContent = "行情加载中…";
      }
      json("/api/item/analysis?name=" + encodeURIComponent(name)
        + "&source=" + encodeURIComponent(CHART_SOURCE) + "&style=all", function (d) {
        PTS = d.points || [];
        CHART_SOURCE = d.source || CHART_SOURCE;
        CHART_MODE = d.mode || "candlestick";
        CHART_SOURCE_LABEL = d.source_label || (CHART_SOURCE === "all" ? "总览" : CHART_SOURCE.toUpperCase());
        ACTIVITY_LABEL = d.activity_label || (CHART_MODE === "line" ? "在售量" : "成交量");
        ACTIVITY_AVAILABLE = d.activity_available !== false;
        if (!ACTIVITY_AVAILABLE && SUB_INDICATORS.indexOf("volume") !== -1) {
          SUB_INDICATORS = SUB_INDICATORS.filter(function (item) { return item !== "volume"; });
          if (!SUB_INDICATORS.length) SUB_INDICATORS = ["macd"];
          setText("indicator-limit-note", ACTIVITY_LABEL + "数据暂缺，已切换为 MACD");
        }
        enforceIndicatorAvailability();
        CHART_RANGE = d.range || {};
        setText("chart-source-label", CHART_SOURCE === "all" ? "总览" : CHART_SOURCE_LABEL);
        document.querySelectorAll("[data-chart-source]").forEach(function (button) {
          button.classList.toggle("active", button.dataset.chartSource === CHART_SOURCE);
        });
        syncItemSourceSelection();
        saveItemState();
        chartDone(el("item-kline-chart").closest(".chart-box"));
        renderKline();
        renderPriceCards(CHART_SOURCE === "all");
      }, function () {
        chartState(el("item-kline-chart").closest(".chart-box"), "行情加载失败，请重试",
          function () { loadChartSource(CHART_SOURCE); });
      }, "item-analysis");
    }
    function syncItemSourceSelection() {
      document.querySelectorAll("[data-chart-source-trigger]").forEach(function (node) {
        var active = node.dataset.chartSourceTrigger === CHART_SOURCE;
        node.classList.toggle("is-chart-source", active && node.classList.contains("platform-card"));
        node.classList.toggle("active", active && !node.classList.contains("platform-card"));
        node.setAttribute("aria-pressed", active ? "true" : "false");
      });
      var status = el("platform-source-strip-label");
      if (status) {
        status.textContent = "当前图表：" + (CHART_SOURCE === "all" ? "总览" : CHART_SOURCE.toUpperCase());
      }
    }
    function chooseItemSource(source) {
      if (!source) return;
      if (!itemSourcesReady) {
        setText("platform-source-strip-label", "图表来源仍在读取");
        return;
      }
      if (!itemSourceMeta[source] || !itemSourceMeta[source].available) {
        var count = itemSourceMeta[source] ? itemSourceMeta[source].count : 0;
        setText("platform-source-strip-label",
          source.toUpperCase() + " 当前仅有 " + fmtI(count) + " 个快照，历史点不足");
        return;
      }
      switchTab("kline");
      if (source === CHART_SOURCE) {
        syncItemSourceSelection();
        return;
      }
      CHART_ZOOM["item-kline-chart"] = null;
      if (ITEM_DRAWING) ITEM_DRAWING.setMode(null);
      sourceChosen = true;
      loadChartSource(source);
      setPopover("chart-source-menu", "chart-source-trigger", false);
    }
    function syncItemSourceAvailability() {
      document.querySelectorAll("[data-chart-source-trigger]").forEach(function (node) {
        var source = node.dataset.chartSourceTrigger;
        var meta = itemSourceMeta[source];
        var available = !!(meta && meta.available);
        node.classList.toggle("chart-source-unavailable", !available);
        node.setAttribute("aria-disabled", available ? "false" : "true");
        if (!available) {
          node.title = source === "all"
            ? "总览历史仍在采集"
            : "当前仅有报价快照，历史点不足";
        }
      });
    }
    function loadChartSources(done) {
      json("/api/item/chart-sources?name=" + encodeURIComponent(name), function (d) {
        var sources = d.sources || [];
        itemSourceMeta = {};
        sources.forEach(function (source) {
          itemSourceMeta[source.key] = source;
        });
        itemSourcesReady = true;
        var holder = el("chart-source-options");
        if (!holder) {
          if (done) done();
          return;
        }
        function sourceButton(source) {
          var mode = source.mode === "line" ? "价格快照折线" : "OHLC K 线";
          var count = source.available ? fmtI(source.count) + " 点" : "快照";
          var range = source.start && source.end
            ? String(source.start).slice(0, 10) + " 至 " + String(source.end).slice(0, 10)
            : "时间范围待采集";
          return "<button type='button' data-chart-source='" + esc(source.key) + "'"
            + (source.available ? "" : " disabled")
            + (source.key === CHART_SOURCE ? " class='active'" : "") + ">"
            + "<span><b>" + esc(source.label) + "</b><small>" + esc(mode) + " · "
            + esc(range) + " · " + esc(source.description) + "</small></span>"
            + "<em>" + count + "</em></button>";
        }
        var overview = sources.filter(function (source) {
          return source.key === "all" || source.key === "site_history";
        });
        var platforms = sources.filter(function (source) {
          return source.key !== "all" && source.key !== "site_history";
        });
        holder.innerHTML = (overview.length
          ? "<div class='source-group-label'>综合视图</div>" + overview.map(sourceButton).join("")
          : "")
          + (platforms.length
            ? "<div class='source-group-label'>单平台历史</div>" + platforms.map(sourceButton).join("")
            : "");
        holder.querySelectorAll("[data-chart-source]").forEach(function (button) {
          button.addEventListener("click", function () {
            if (button.disabled || button.dataset.chartSource === CHART_SOURCE) {
              setPopover("chart-source-menu", "chart-source-trigger", false);
              return;
            }
            CHART_ZOOM["item-kline-chart"] = null;
            if (ITEM_DRAWING) ITEM_DRAWING.setMode(null);
            sourceChosen = true;
            loadChartSource(button.dataset.chartSource);
            saveItemState();
            setPopover("chart-source-menu", "chart-source-trigger", false);
          });
        });
        if (!itemSourceMeta[CHART_SOURCE] || !itemSourceMeta[CHART_SOURCE].available) {
          var fallbackSource = sources.find(function (source) {
            return source.available;
          });
          CHART_SOURCE = fallbackSource ? fallbackSource.key : "all";
        }
        var site = itemSourceMeta.site_history;
        var candle = itemSourceMeta.all;
        if (!sourceChosen && site && site.available && (
            !candle || !candle.available
            || Date.parse(site.start) + 30 * 86400000 < Date.parse(candle.start)
            || Date.parse(site.end) > Date.parse(candle.end) + 7 * 86400000)) {
          CHART_SOURCE = "site_history";
        }
        syncItemSourceAvailability();
        syncItemSourceSelection();
        if (done) done();
      }, function () {
        itemSourcesReady = true;
        itemSourceMeta = { all: { key: "all", available: true, count: 0 } };
        var holder = el("chart-source-options");
        if (holder) {
          holder.innerHTML = "<div class='popover-empty'>数据源读取失败，请稍后重试</div>";
        }
        if (CHART_SOURCE !== "all") CHART_SOURCE = "all";
        syncItemSourceAvailability();
        if (done) done();
      }, "item-chart-sources");
    }
    document.querySelectorAll("[data-chart-source-trigger]").forEach(function (node) {
      function activateSource(event) {
        if (event.type === "keydown" && event.key !== "Enter" && event.key !== " ") return;
        if (event.type === "keydown") event.preventDefault();
        chooseItemSource(node.dataset.chartSourceTrigger);
      }
      node.addEventListener("click", activateSource);
      node.addEventListener("keydown", activateSource);
    });
    syncIndicatorSummary();
    loadChartSources(function () {
      loadChartSource(CHART_SOURCE || "all");
    });

    function setHealth(id, state, detail) {
      var node = el(id);
      if (!node) return;
      node.className = "data-health-item " + state;
      var small = node.querySelector("small");
      if (small) small.textContent = detail;
    }
    function loadAvailability() {
      json("/api/item/availability?name=" + encodeURIComponent(name), function (d) {
        ITEM_AVAILABILITY = d;
        var kline = d.kline || {};
        var platforms = d.platforms || {};
        var history = d.site_history || {};
        setHealth("health-kline", kline.available ? "ready" : "collecting",
          kline.available ? fmtI(kline.points) + " 点" : "采集中");
        setHealth("health-platform", platforms.available ? "ready" : "collecting",
          platforms.available ? fmtI(platforms.count) + " 个平台" : "采集中");
        setHealth("health-history", history.available ? "ready" : "collecting",
          history.available ? fmtI(history.points) + " 点" : "回填中");
        var ready = [kline.available, platforms.available, history.available].filter(Boolean).length;
        setText("data-health-summary", ready === 3
          ? "当前标的三类核心数据均可用"
          : "当前标的 " + ready + "/3 类核心数据可用，缺失项会自动降级展示");
        if (!PTS.length) renderKline();
      }, function () {
        setText("data-health-summary", "数据可用性暂时无法读取，图表仍可继续使用");
      }, "item-availability");
    }
    loadAvailability();

    // 平台行情卡：补相对时间 + 用最新快照刷新数值
    function updatePlatformCard(p, m) {
      var card = document.querySelector(".platform-card[data-platform='" + p + "']");
      if (card && m.sell_price != null) {
        var price = card.querySelector(".price");
        if (price) price.textContent = "¥" + fmt(m.sell_price);
      }
      var depth = card && card.querySelector(".platform-depth");
      if (depth) {
        depth.innerHTML = "<span>在售<b>" + (m.sell_num != null ? fmtI(m.sell_num) : "-") + "</b></span>"
          + "<span>求购价<b>" + (m.buy_price != null ? fmt(m.buy_price) : "-") + "</b></span>"
          + "<span>求购量<b>" + (m.buy_num != null ? fmtI(m.buy_num) : "-") + "</b></span>";
      }
      var rt = card && card.querySelector(".reltime[data-ts]");
      if (rt && m.updated_at) {
        rt.setAttribute("data-ts", m.updated_at);
        rt.textContent = reltime(m.updated_at);
      }
      var summary = document.querySelector("[data-platform-summary='" + p + "']");
      if (summary) {
        var right = summary.lastElementChild;
        if (right) {
          right.innerHTML = "<b>¥" + (m.sell_price != null ? fmt(m.sell_price) : "-") + "</b>"
            + "<small>在售 " + (m.sell_num != null ? fmtI(m.sell_num) : "-") + "</small>";
        }
      }
    }
    function loadPlatformFresh() {
      if (platformLoaded || platformLoading) return;
      platformLoading = true;
      json("/api/item/price?name=" + encodeURIComponent(name), function (d) {
        platformLoading = false;
        platformLoaded = true;
        var platforms = d.platforms || {};
        Object.keys(platforms).forEach(function (p) { updatePlatformCard(p, platforms[p]); });
      }, function () {
        platformLoading = false;
      }, "item-price");
    }

    function loadHistoryChart() {
      if (historyLoading) return;
      historyLoading = true;
      var box = el("item-history-chart").closest(".chart-box");
      var chartNode = el("item-history-chart");
      var emptyNode = el("history-empty");
      var loading = box.querySelector(".chart-loading");
      if (loading) {
        loading.classList.remove("hidden", "error");
        loading.textContent = "历史价格加载中…";
      }
      if (emptyNode) emptyNode.classList.add("hidden");
      if (chartNode) chartNode.style.display = "";
      json("/api/item/history?name=" + encodeURIComponent(name), function (d) {
        historyLoading = false;
        historyLoaded = true;
        var series = d.series || {};
        var wanted = ["sell_price", "buy_price"];
        var items = [];
        wanted.forEach(function (k) {
          if (series[k] && series[k].length) {
            items.push({
              name: d.fallback && k === "sell_price" ? "日 K 收盘价" : (k === "sell_price" ? "在售价" : "求购价"),
              data: series[k]
            });
          }
        });
        var sourceNote = el("history-source-note");
        if (sourceNote) {
          var timeline = series.sell_price || series.buy_price || [];
          sourceNote.textContent = (d.message || "")
            + (timeline.length ? " · " + String(timeline[0].ts).slice(0, 10)
              + " 至 " + String(timeline[timeline.length - 1].ts).slice(0, 10)
              + " · " + timeline.length + " 点" : "");
          sourceNote.className = "data-source-note " + (d.fallback ? "partial" : "ready")
            + (d.message ? "" : " hidden");
        }
        if (items.length) {
          chartNode.style.display = "";
          lineChart("item-history-chart", items);
          emptyNode.classList.add("hidden");
          window.requestAnimationFrame(function () {
            resizeChart("item-history-chart");
          });
        } else {
          emptyNode.textContent = d.message || "历史价格仍在采集中";
          emptyNode.classList.remove("hidden");
          clearChartView("item-history-chart");
          chartNode.style.display = "none";
        }
        chartDone(box);
      }, function () {
        historyLoading = false;
        historyLoaded = false;
        clearChartView("item-history-chart");
        chartNode.style.display = "none";
        chartState(box, "历史价格加载失败，请重试", loadHistoryChart);
      }, "item-history");
    }

    function loadChange() {
      if (changeLoaded || changeLoading) return;
      changeLoading = true;
      json("/api/item/change?name=" + encodeURIComponent(name), function (d) {
        changeLoading = false;
        changeLoaded = true;
        var changes = d.changes || [];
        el("change-body").innerHTML = changes.map(function (c) {
          return "<tr><td>" + roleBadge(c.platform) + "</td>"
            + "<td>" + metricTag(c.metric) + "</td>"
            + "<td class='num'>" + eventValue(c.metric, c.old) + "</td>"
            + "<td class='num'>" + eventValue(c.metric, c.new) + "</td>"
            + "<td class='num'>" + chip(c.ratio) + "</td>"
            + "<td class='muted'><span class='reltime' data-ts='" + esc(c.ts) + "'>" + reltime(c.ts) + "</span></td></tr>";
          }).join("") || "<tr><td colspan='6' class='empty'>近两轮快照无变化</td></tr>";
        processReltime(el("change-body"));
      }, function () {
        changeLoading = false;
        el("change-body").innerHTML = "<tr><td colspan='6' class='empty'>异动数据加载失败，请稍后重试</td></tr>";
      }, "item-change");
    }

    function renderStyle(payload) {
      var box = el("style-kline-chart").closest(".chart-box");
      var chart = el("style-kline-chart");
      clearChartView("style-kline-chart");
      chart.style.display = "";
      chart.innerHTML = "";
      if (payload.kind === "kline") {
        klineChart("style-kline-chart", payload.points);
      } else if (payload.kind === "history") {
        lineChart("style-kline-chart", payload.items);
      } else {
        chart.innerHTML = "<div class='empty'>该款式暂无行情数据</div>";
      }
      chartDone(box);
    }
    function loadStyle() {
      var sel = el("style-select");
      if (!sel) return;
      var style = sel.value || (sel.options[0] && sel.options[0].value);
      if (!style) return;
      if (styleLoaded[style]) {
        renderStyle(styleLoaded[style]);
        return;
      }
      if (styleLoadingKey === style) return;
      cancelRequest("item-style-kline");
      cancelRequest("item-style-history");
      styleLoadingKey = style;
      var label = hints[style] || style;
      var box = el("style-kline-chart").closest(".chart-box");
      clearChartView("style-kline-chart");
      var styleChart = el("style-kline-chart");
      if (styleChart) styleChart.innerHTML = "";
      var loading = box.querySelector(".chart-loading");
      if (loading) {
        loading.classList.remove("hidden", "error");
        loading.textContent = "款式行情加载中…";
      }
      json("/api/item/kline?name=" + encodeURIComponent(name) + "&style=" + encodeURIComponent(style), function (d) {
        if (styleLoadingKey !== style) return;
        if (d.points && d.points.length) {
          styleLoaded[style] = { kind: "kline", points: d.points };
          styleLoadingKey = "";
          renderStyle(styleLoaded[style]);
          return;
        }
        json("/api/item/style-history?name=" + encodeURIComponent(name) + "&style=" + encodeURIComponent(style), function (h) {
          if (styleLoadingKey !== style) return;
          var series = h.series || {};
          var items = [];
          if (series.sell_price) items.push({ name: "卖价 (" + label + ")", data: series.sell_price });
          if (series.buy_price) items.push({ name: "求购价 (" + label + ")", data: series.buy_price });
          styleLoaded[style] = items.length
            ? { kind: "history", items: items }
            : { kind: "empty" };
          styleLoadingKey = "";
          renderStyle(styleLoaded[style]);
        }, function () {
          if (styleLoadingKey !== style) return;
          styleLoadingKey = "";
          chartState(box, "款式历史加载失败，请重试", loadStyle);
        }, "item-style-history");
      }, function () {
        if (styleLoadingKey !== style) return;
        styleLoadingKey = "";
        chartState(box, "款式行情加载失败，请重试", loadStyle);
      }, "item-style-kline");
    }
    var sel = el("style-select");
    if (sel) sel.addEventListener("change", loadStyle);
  }

  // ---------- 品类走势工作区 ----------
  var trendDataNode = el("trend-data");
  if (trendDataNode) {
    var trendData = {};
    try { trendData = JSON.parse(trendDataNode.textContent || "{}"); }
    catch (e) { trendData = { rows: [], categories: [] }; }
    var trendRows = trendData.all_rows || trendData.rows || [];
    var trendCalendarData = trendData;
    var trendCategory = trendData.category || "all";
    var trendPlatform = trendData.platform || "buff";
    var trendSort = (el("trend-sort") && el("trend-sort").value) || "default";
    var trendCategoryLabels = { all: "全部" };
    (trendData.categories || []).forEach(function (item) {
      trendCategoryLabels[item.key] = item.label;
    });
    function syncTrendUrl() {
      if (!window.history || !window.history.replaceState) return;
      var url = new URL(window.location.href);
      if (trendCategory === "all") url.searchParams.delete("category");
      else url.searchParams.set("category", trendCategory);
      if (trendPlatform === "buff") url.searchParams.delete("platform");
      else url.searchParams.set("platform", trendPlatform);
      if (trendSort === "default") url.searchParams.delete("sort");
      else url.searchParams.set("sort", trendSort);
      window.history.replaceState({}, "", url.pathname + url.search + url.hash);
    }
    function trendReturnPath() {
      return window.location.pathname + window.location.search;
    }

    function trendValue(row, key) {
      var map = row && row[key] || {};
      var value = map[trendPlatform];
      if (value == null && trendPlatform !== "buff") value = map.buff;
      return value;
    }
    function trendRate(row) {
      var direct = trendValue(row, "diff");
      if (direct != null && isFinite(Number(direct))) return Number(direct);
      var price = trendValue(row, "price");
      var average = trendValue(row, "avg7");
      return price != null && average ? (Number(price) - Number(average)) / Number(average) * 100 : null;
    }
    function trendRowsForView() {
      var rows = trendRows.filter(function (row) {
        return trendCategory === "all" || row.category === trendCategory;
      }).map(function (row) {
        return { row: row, rate: trendRate(row), price: trendValue(row, "price"), average: trendValue(row, "avg7") };
      });
      rows.sort(function (a, b) {
        if (trendSort === "change") return Math.abs(b.rate || 0) - Math.abs(a.rate || 0);
        if (trendSort === "price") return (b.price || -Infinity) - (a.price || -Infinity);
        if (trendSort === "supply") return (b.row.broker_sell || 0) - (a.row.broker_sell || 0);
        return (b.row.broker_sell || 0) - (a.row.broker_sell || 0);
      });
      return rows;
    }
    function trendRowHtml(entry, index) {
      var row = entry.row;
      var profile = row.profile || {};
      var image = imageUrl(profile);
      var thumb = image
        ? "<img src='" + esc(image) + "' alt='' loading='lazy'>"
        : "<i data-lucide='package' aria-hidden='true'></i>";
      var href = "/market/items/" + encodeURIComponent(row.name || "")
        + "?from=" + encodeURIComponent(trendReturnPath());
      var rate = entry.rate;
      return "<tr>"
        + "<td>" + (index + 1) + "</td>"
        + "<td><a class='item-cell' href='" + href + "'><span class='item-thumb'>" + thumb + "</span>"
        + "<span><strong>" + esc(row.name || "-") + "</strong><small>" + esc(profile.rarity || "CS2 饰品") + "</small></span></a></td>"
        + "<td><span class='badge badge-secondary'>" + esc(trendCategoryLabels[row.category] || row.category || "其它") + "</span></td>"
        + "<td class='num strong'>" + fmt(entry.price) + "</td>"
        + "<td class='num'>" + fmt(entry.average) + "</td>"
        + "<td class='num'>" + chip(rate) + "</td>"
        + "<td class='num'>" + fmtI(row.broker_sell) + "</td>"
        + "<td class='muted'><span class='reltime' data-ts='" + esc(row.ts || "") + "'>" + reltime(row.ts) + "</span></td>"
        + "<td><a class='icon-btn row-action' href='" + href + "' aria-label='查看详情' title='查看详情'><i data-lucide='chevron-right' aria-hidden='true'></i></a></td>"
        + "</tr>";
    }
    function syncTrendSummary() {
      document.querySelectorAll(".trend-summary-card[data-trend-category]").forEach(function (card) {
        var key = card.dataset.trendCategory || "all";
        var rows = trendRows.filter(function (row) {
          return key === "all" || row.category === key;
        });
        var rates = rows.map(trendRate).filter(function (value) {
          return value != null && isFinite(Number(value));
        });
        var count = card.querySelector("strong");
        if (count) count.textContent = fmtI(rows.length);
        var change = card.querySelector("small");
        if (!change || key === "all") return;
        if (!rates.length) {
          change.textContent = "暂无变化";
          change.className = "";
          return;
        }
        var average = rates.reduce(function (sum, value) { return sum + Number(value); }, 0) / rates.length;
        change.textContent = diffTxt(average);
        change.className = diffCls(average);
      });
    }
    function renderTrendWorkspace() {
      var body = el("trend-body");
      if (!body) return;
      var rows = trendRowsForView();
      body.innerHTML = rows.length
        ? rows.map(trendRowHtml).join("")
        : "<tr><td colspan='9' class='empty'>当前筛选暂无可用行情</td></tr>";
      setText("trend-result-note", (trendCategoryLabels[trendCategory] || "全部")
        + " · " + rows.length + " 件 · " + (trendPlatform || "BUFF").toUpperCase());
      syncTrendSummary();
      processReltime(body);
      refreshIcons(body);
      document.querySelectorAll("[data-trend-category]").forEach(function (button) {
        var active = (button.dataset.trendCategory || "all") === trendCategory;
        button.classList.toggle("active", active);
        if (button.getAttribute("role") === "tab") {
          button.setAttribute("aria-selected", active ? "true" : "false");
          button.setAttribute("tabindex", active ? "0" : "-1");
        } else {
          button.setAttribute("aria-pressed", active ? "true" : "false");
        }
      });
      document.querySelectorAll("[data-trend-platform] button").forEach(function (button) {
        var active = button.dataset.platform === trendPlatform;
        button.classList.toggle("active", active);
        button.setAttribute("aria-selected", active ? "true" : "false");
        button.setAttribute("tabindex", active ? "0" : "-1");
      });
      var refresh = el("trend-refresh");
      if (refresh) refresh.textContent = "已加载 " + new Date().toLocaleTimeString();
    }
    document.querySelectorAll("[data-trend-category]").forEach(function (button) {
      button.addEventListener("click", function () {
        trendCategory = button.dataset.trendCategory || "all";
        syncTrendUrl();
        renderTrendWorkspace();
        renderTrendCalendar();
      });
    });
    document.querySelectorAll("[data-trend-platform] button").forEach(function (button) {
      button.addEventListener("click", function () {
        trendPlatform = button.dataset.platform || "buff";
        syncTrendUrl();
        renderTrendWorkspace();
        renderTrendCalendar();
      });
    });
    var trendSortNode = el("trend-sort");
    if (trendSortNode) trendSortNode.addEventListener("change", function () {
      trendSort = trendSortNode.value || "default";
      syncTrendUrl();
      renderTrendWorkspace();
    });
    renderTrendWorkspace();

    renderTrendCalendar();

    function renderTrendCalendar() {
      var panel = el("trend-calendar-panel");
      var wrap = el("trend-calendar-wrap");
      if (!panel || !wrap) return;
      var rows = (trendCalendarData && trendCalendarData.rows) || [];
      if (!rows.length) { panel.style.display = "none"; return; }
      panel.style.display = "";
      var platform = trendPlatform || "buff";
      var catLabel = trendCategoryLabels[trendCategory] || "All";
      var headerHtml = '<div class="trend-cal-header"><span class="trend-cal-name"></span>';
      var days = [];
      var today = new Date();
      for (var d = 6; d >= 0; d--) {
        var dt = new Date(today);
        dt.setDate(dt.getDate() - d);
        var ds = dt.getFullYear() + "-" + String(dt.getMonth()+1).padStart(2,"0") + "-" + String(dt.getDate()).padStart(2,"0");
        var labels = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
        headerHtml += '<div class="trend-cal-col"><div class="trend-cal-date">' + ds.slice(5) + '</div><div class="trend-cal-dow">' + labels[dt.getDay()] + '</div></div>';
        days.push(ds);
      }
      headerHtml += "</div>";
      var prevPrices = {};
      var rowsHtml = "";
      for (var ri = 0; ri < rows.length; ri++) {
        var row = rows[ri];
        var dp = row.daily_prices || {};
        var itemId = row.name || "";
        var prevPrice = null;
        var cellsHtml = "";
        for (var di = 0; di < days.length; di++) {
          var day = days[di];
          var price = dp[day];
          var cls = "trend-cal-cell";
          var bg = "";
          var chg = null;
          if (price != null) {
            if (prevPrice != null && prevPrice > 0) {
              chg = (price - prevPrice) / prevPrice * 100;
              if (chg > 2) { bg = "var(--up-soft)"; cls += " up"; }
              else if (chg < -2) { bg = "var(--down-soft)"; cls += " down"; }
              else { bg = "rgba(128,128,128,.08)"; }
            } else if (prevPrice == null) {
              bg = "rgba(128,128,128,.06)";
            }
          }
          var val = price != null ? parseFloat(price).toFixed(2) : "-";
          var tip = day + (price != null ? ": " + fmt(price) : " (no data)");
          cellsHtml += '<div class="' + cls + '" style="background:' + bg + '" title="' + esc(tip) + '"><span class="trend-cal-val">' + val + '</span></div>';
          if (price != null) prevPrice = price;
        }
        rowsHtml += '<div class="trend-cal-row"><div class="trend-cal-name">' + esc(itemId) + '</div>' + cellsHtml + '</div>';
      }
      wrap.innerHTML = headerHtml + rowsHtml;
    }

  }
  // ---------- 板块指数工作区 ----------
  var sectionChartDom = el("section-index-chart");
  if (sectionChartDom) {
    var sectionCommandbar = document.querySelector(".section-chart-commandbar");
    var sectionType = sectionCommandbar && sectionCommandbar.dataset
      ? (sectionCommandbar.dataset.sectionType || "BROAD") : "BROAD";
    var savedSectionState = readViewState("section." + sectionType, {});
    var sectionSource = savedSectionState.source === "sitea" ? "sitea" : "siteb";
    var sectionPeriod = String(savedSectionState.period || "1day");
    var sectionPoints = [];
    var sectionMode = "candlestick";
    var sectionPriceLabel = "板块指数";
    var sectionActivityLabel = "成交量";
    var sectionMessage = "";
    var sectionMainIndicators = validIndicators(savedSectionState.main, ["ma", "ema", "boll"]);
    var sectionSubIndicators = validIndicators(savedSectionState.sub, ["volume", "macd", "rsi", "volatility"]);
    if (!sectionMainIndicators.length) sectionMainIndicators = ["ma"];
    if (!sectionSubIndicators.length) sectionSubIndicators = ["volume"];
    var sectionVolumeAvailable = true;
    var sectionDrawing = null;
    var sectionBox = el("section-chart-box") || sectionChartDom.closest(".chart-box");
    if (sectionCommandbar && sectionCommandbar.dataset) {
      var sectionQuery = new URLSearchParams(window.location.search);
      if (sectionQuery.has("period") || !savedSectionState.period) {
        sectionPeriod = sectionCommandbar.dataset.sectionPeriod || sectionPeriod;
      }
    }
    function saveSectionState() {
      writeViewState("section." + sectionType, {
        source: sectionSource,
        period: sectionPeriod,
        main: sectionMainIndicators,
        sub: sectionSubIndicators
      });
    }

    function syncSectionControls() {
      document.querySelectorAll("[data-section-source]").forEach(function (button) {
        var active = button.dataset.sectionSource === sectionSource;
        button.classList.toggle("active", active);
        button.setAttribute("aria-pressed", active ? "true" : "false");
      });
      var supported = ["1day", "7day", "15day", "30day"];
      if (supported.indexOf(sectionPeriod) === -1) sectionPeriod = "1day";
      document.querySelectorAll("button[data-section-period]").forEach(function (button) {
        var enabled = supported.indexOf(button.dataset.sectionPeriod) !== -1;
        button.disabled = !enabled;
        if (button.dataset.sectionPeriod === "1day") {
          button.textContent = sectionType === "BROAD" && sectionSource === "siteb"
            ? "全部公开 K 线" : "日线";
        } else if (button.dataset.sectionPeriod === "7day") {
          button.textContent = sectionType === "BROAD" && sectionSource === "siteb"
            ? "周 K 观测" : "7 天";
        }
        button.classList.toggle("active", button.dataset.sectionPeriod === sectionPeriod);
        button.setAttribute("aria-selected", button.dataset.sectionPeriod === sectionPeriod ? "true" : "false");
      });
      setText("section-source-label", sectionSource === "sitea" ? "行情站A" : "行情站B");
    }
    function syncSectionIndicatorInputs() {
      syncIndicatorInputGroup("[data-section-main]", "sectionMain", sectionMainIndicators,
        sectionPoints, sectionActivityLabel);
      syncIndicatorInputGroup("[data-section-sub]", "sectionSub", sectionSubIndicators,
        sectionPoints, sectionActivityLabel);
      setText("section-activity-name", sectionActivityLabel);
    }

    function renderSectionChart() {
      if (!sectionPoints.length) return;
      var compact = window.innerWidth <= 720;
      var visibleSub = usableSubIndicators(sectionPoints, sectionSubIndicators, sectionActivityLabel);
      var visibleMain = sectionMainIndicators.filter(function (indicator) {
        return indicatorReady(sectionPoints, indicator, sectionActivityLabel);
      });
      sectionChartDom.style.height = (compact ? 300 : 405)
        + visibleSub.length * (compact ? 104 : 118) + "px";
      var chart = klineChart("section-index-chart", sectionPoints, {
        mode: sectionMode,
        calendarScale: sectionType === "BROAD" && sectionSource === "siteb"
          && (sectionPeriod === "1day" || sectionPeriod === "7day"),
        fullRange: sectionType === "BROAD" && sectionSource === "siteb"
          && (sectionPeriod === "1day" || sectionPeriod === "7day"),
        priceLabel: sectionPriceLabel,
        priceUnit: "",
        mainIndicators: visibleMain,
        subIndicators: visibleSub,
        activityLabel: sectionActivityLabel
      });
      sectionDrawing = drawingController("section-index-chart", "section-drawing-layer", {
        statusId: "section-draw-status",
        toolSelector: "[data-section-draw-tool]"
      });
      if (sectionDrawing && chart) {
        sectionDrawing.bind(chart, "section." + sectionSource + "." + sectionPeriod,
          sectionPoints.map(function (point) { return String(point.ts); }));
      }
      var labels = { ma: "MA", ema: "EMA", boll: "BOLL", volume: sectionActivityLabel,
        macd: "MACD", rsi: "RSI", volatility: "波动率" };
      var active = visibleMain.concat(visibleSub);
      setText("section-indicator-count", active.length);
      var holder = el("section-active-indicators");
      if (holder) holder.innerHTML = active.map(function (key) {
        return "<span>" + esc(labels[key] || key) + "</span>";
      }).join("");
      var periodLabel = ({
        "1day": "日线",
        "7day": "7 天",
        "15day": "15 天",
        "30day": "30 天"
      }[sectionPeriod] || sectionPeriod);
      var sourceLabel = sectionSource === "sitea" ? "行情站A" : "行情站B";
      var broadIndex = sectionType === "BROAD" && sectionSource === "siteb"
        && (sectionPeriod === "1day" || sectionPeriod === "7day");
      if (broadIndex) periodLabel = sectionPeriod === "7day" ? "周 K 观测" : "全部公开 K 线";
      var modeLabel = broadIndex
        ? (sectionPeriod === "7day" ? "站点原始周 K" : "原始日 K 历史")
        : (sectionMode === "line" ? "指数均值折线" : "OHLC K 线");
      var dateRange = marketDateLabel(sectionPoints[0].ts) + " 至 "
        + marketDateLabel(sectionPoints[sectionPoints.length - 1].ts);
      setText("section-chart-meta", sourceLabel + " · " + periodLabel + " · "
        + modeLabel + " · " + dateRange + " · "
        + (broadIndex
          ? "原始 K 线 " + (sectionPoints.filter(function (p) {
            return p.open != null && !(p.sample || "").startsWith("小时指数观测");
          }).length) + " 根 · 小时观测区间 " + sectionPoints.filter(function (p) {
            return (p.sample || "").startsWith("小时指数观测聚合");
          }).length + " 根"
          : sectionPoints.length + " 点"));
      setText("section-chart-note", sourceLabel + " " + periodLabel + " · " + sectionPoints.length
        + (broadIndex ? " 个时间点" : (sectionMode === "line" ? " 个观测" : " 根")) + " · 主图 "
        + (visibleMain.length ? visibleMain.join("/") : "无") + " · 副图 "
        + (visibleSub.length ? visibleSub.join("/") : "无")
        + (sectionMessage ? " · " + sectionMessage : ""));
      syncSectionIndicatorInputs();
      saveSectionState();
    }
    function syncSectionChartMeta(count, note) {
      var periodLabel = ({
        "1day": "日线",
        "7day": "7 天",
        "15day": "15 天",
        "30day": "30 天"
      }[sectionPeriod] || sectionPeriod);
      var sourceLabel = sectionSource === "sitea" ? "行情站A" : "行情站B";
      var modeLabel = sectionMode === "line" && sectionType === "BROAD"
        && sectionSource === "siteb" ? "公开大盘指数折线" :
        (sectionMode === "line" ? "指数均值折线" : "OHLC K 线");
      setText("section-chart-meta", sourceLabel + " · " + periodLabel + " · "
        + modeLabel + " · " + (count ? count + " 点" : "暂无数据"));
      setText("section-chart-note", note || (sourceLabel + " " + periodLabel
        + " · " + (count ? count + " 根" : "当前来源暂无独立历史")));
    }

    function loadSectionChart() {
      sectionPoints = [];
      clearChartView("section-index-chart", sectionDrawing);
      sectionChartDom.style.height = (window.innerWidth <= 720 ? 300 : 405) + "px";
      syncSectionControls();
      var loading = sectionBox.querySelector(".chart-loading");
      if (loading) {
        loading.classList.remove("hidden");
        loading.classList.remove("error");
        loading.textContent = "板块行情加载中…";
      }
      json("/api/section/analysis?type=" + encodeURIComponent(sectionType)
        + "&source=" + sectionSource + "&period=" + sectionPeriod, function (data) {
        sectionSource = data.source || sectionSource;
        sectionPeriod = data.period || sectionPeriod;
        sectionPoints = data.points || [];
        sectionMode = data.mode || "candlestick";
        sectionPriceLabel = data.price_label || "板块指数";
        sectionActivityLabel = data.activity_label || "成交量";
        sectionMessage = data.message || "";
        sectionVolumeAvailable = !!(data.coverage && data.coverage.volume && data.coverage.volume.available);
        setText("section-activity-name", sectionActivityLabel);
        if (!sectionVolumeAvailable && sectionSubIndicators.indexOf("volume") !== -1) {
          sectionSubIndicators = sectionSubIndicators.filter(function (key) { return key !== "volume"; });
          if (!sectionSubIndicators.length) sectionSubIndicators = ["macd"];
          setText("section-indicator-note", "当前来源缺少成交样本，已切换为 MACD");
        }
        syncSectionControls();
        if (sectionPoints.length) {
          renderSectionChart();
          chartDone(sectionBox);
        } else {
          syncSectionChartMeta(0, data.message || "当前来源暂无独立板块历史");
          chartState(sectionBox, data.message || "当前暂无板块指数历史", loadSectionChart);
        }
      }, function () {
        chartState(sectionBox, "板块指数加载失败，请重试", loadSectionChart);
      }, "section-analysis");
    }

    var closeSectionPopovers = bindChartMenus([
      ["section-source-menu", "section-source-trigger"],
      ["section-indicator-panel", "section-indicator-trigger"],
      ["section-draw-menu", "section-draw-trigger"]
    ], ".section-chart-commandbar", "section-indicator-close");

    document.querySelectorAll("[data-section-source]").forEach(function (button) {
      button.addEventListener("click", function () {
        if (button.disabled) return;
        sectionSource = button.dataset.sectionSource || "siteb";
        sectionPeriod = sectionSource === "siteb" ? "1day" : sectionPeriod;
        saveSectionState();
        CHART_ZOOM["section-index-chart"] = null;
        if (sectionDrawing) sectionDrawing.setMode(null);
        closeSectionPopovers();
        loadSectionChart();
      });
    });
    document.querySelectorAll("button[data-section-period]").forEach(function (button) {
      button.addEventListener("click", function () {
        if (button.disabled) return;
        sectionPeriod = button.dataset.sectionPeriod || "1day";
        saveSectionState();
        CHART_ZOOM["section-index-chart"] = null;
        if (sectionDrawing) sectionDrawing.setMode(null);
        syncSectionControls();
        loadSectionChart();
      });
    });
    document.querySelectorAll("[data-section-main]").forEach(function (input) {
      input.addEventListener("change", function () {
        sectionMainIndicators = Array.prototype.map.call(
          document.querySelectorAll("[data-section-main]:checked"),
          function (item) { return item.dataset.sectionMain; }
        );
        syncSectionIndicatorInputs();
        saveSectionState();
        renderSectionChart();
      });
    });
    document.querySelectorAll("[data-section-sub]").forEach(function (input) {
      input.addEventListener("change", function () {
        var checked = Array.prototype.slice.call(document.querySelectorAll("[data-section-sub]:checked"));
        if (checked.length > 2) {
          input.checked = false;
          setText("section-indicator-note", "副图最多同时显示 2 个");
          return;
        }
        sectionSubIndicators = checked.map(function (item) { return item.dataset.sectionSub; });
        syncSectionIndicatorInputs();
        saveSectionState();
        setText("section-indicator-note", sectionSubIndicators.length === 2
          ? "已达到 2 个副图上限" : "默认组合适合板块观察");
        renderSectionChart();
      });
    });
    var sectionIndicatorReset = el("section-indicator-reset");
    if (sectionIndicatorReset) sectionIndicatorReset.addEventListener("click", function () {
      sectionMainIndicators = ["ma"];
      sectionSubIndicators = sectionVolumeAvailable ? ["volume"] : ["macd"];
      saveSectionState();
      document.querySelectorAll("[data-section-main]").forEach(function (input) {
        input.checked = sectionMainIndicators.indexOf(input.dataset.sectionMain) !== -1;
      });
      document.querySelectorAll("[data-section-sub]").forEach(function (input) {
        input.checked = sectionSubIndicators.indexOf(input.dataset.sectionSub) !== -1;
      });
      syncSectionIndicatorInputs();
      setText("section-indicator-note", "已恢复默认组合");
      renderSectionChart();
    });
    document.querySelectorAll("[data-section-draw-tool]").forEach(function (button) {
      button.addEventListener("click", function () {
        if (sectionDrawing) sectionDrawing.setMode(button.dataset.sectionDrawTool);
        setChartPopover(el("section-draw-menu"), el("section-draw-trigger"), false);
      });
    });
    var sectionDrawUndo = el("section-draw-undo");
    if (sectionDrawUndo) sectionDrawUndo.addEventListener("click", function () { if (sectionDrawing) sectionDrawing.undo(); });
    var sectionDrawClear = el("section-draw-clear");
    if (sectionDrawClear) sectionDrawClear.addEventListener("click", function () { if (sectionDrawing) sectionDrawing.clear(); });
    syncSectionControls();
    syncSectionIndicatorInputs();
    loadSectionChart();
  }

  // ---------- 饰品 DIY 工作区 ----------
  var diyDataNode = el("diy-data");
  if (diyDataNode) {
    var diyData = {};
    try { diyData = JSON.parse(diyDataNode.textContent || "{}"); }
    catch (e) { diyData = {}; }
    var diyWear = el("diy-wear");
    var diyPattern = el("diy-pattern");
    var diyFilter = el("diy-choice-filter");
    function renderDiyConfig() {
      var wear = diyWear ? diyWear.value : "崭新出厂";
      var pattern = diyPattern ? diyPattern.value : "0";
      var selectedSlots = document.querySelectorAll("[data-sticker-slot]:checked").length;
      setText("diy-preview-config", "磨损：" + wear + " · 图案：" + (pattern || "0")
        + " · 贴纸：" + selectedSlots);
      setText("diy-addon-price", "—");
      var base = diyData.selected && diyData.selected.price;
      setText("diy-total-price", base == null ? null : fmt(base));
    }
    function filterDiyChoices() {
      var query = diyFilter ? diyFilter.value.trim().toLowerCase() : "";
      document.querySelectorAll("[data-diy-choice]").forEach(function (choice) {
        choice.style.display = !query || choice.dataset.diyChoice.toLowerCase().indexOf(query) !== -1 ? "" : "none";
      });
    }
    if (diyWear) diyWear.addEventListener("change", renderDiyConfig);
    if (diyPattern) diyPattern.addEventListener("input", renderDiyConfig);
    document.querySelectorAll("[data-sticker-slot]").forEach(function (input) {
      input.addEventListener("change", renderDiyConfig);
    });
    if (diyFilter) diyFilter.addEventListener("input", filterDiyChoices);
    var diyReset = el("diy-reset");
    if (diyReset) diyReset.addEventListener("click", function () {
      if (diyWear) diyWear.value = "崭新出厂";
      if (diyPattern) diyPattern.value = "0";
      document.querySelectorAll("[data-sticker-slot]").forEach(function (input) { input.checked = false; });
      renderDiyConfig();
    });
    renderDiyConfig();
  }

  document.querySelectorAll("[data-chart-action]").forEach(function (button) {
    button.addEventListener("click", function () {
      var action = button.getAttribute("data-chart-action");
      var targetId = chartActionTarget(button);
      if (action === "reset") resetChartZoom(targetId);
      if (action === "export") exportChartImage(targetId);
      if (action === "fullscreen") toggleChartFullscreen(targetId);
    });
  });

  // ---------- 相对时间维护 ----------
  processReltime();
  refreshIcons();
  setInterval(processReltime, 60000);
})();
