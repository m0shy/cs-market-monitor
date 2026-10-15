/* 实时数据层（SSE）：
 * - 所有页面：顶栏“实时”徽标反映推送连接与采集器状态。
 * - /realtime 页：统计卡片、指数/K线图、新上架流实时更新。
 *
 * 连接：EventSource(/api/realtime/stream)，服务器事件：
 *   snapshot(初始全量) / delta(增量数据) / status(运行状态) / ping(心跳)
 */
(function () {
  "use strict";

  function escapeHtml(v) {
    return String(v == null ? "" : v).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }


  function fmtNumber(value) {
    if (value === null || value === undefined || value === "") return "—";
    var num = Number(value);
    if (!isFinite(num)) return "—";
    if (Math.abs(num) >= 100000000) return (num / 100000000).toFixed(2) + " 亿";
    if (Math.abs(num) >= 10000) return (num / 10000).toFixed(2) + " 万";
    return num.toLocaleString("zh-CN", { maximumFractionDigits: 2 });
  }

  function fmtTime(ts) {
    if (!ts) return "—";
    var date = new Date(ts);
    if (isNaN(date.getTime())) return String(ts).slice(11, 19);
    function p(n) { return (n < 10 ? "0" : "") + n; }
    return p(date.getHours()) + ":" + p(date.getMinutes()) + ":" + p(date.getSeconds());
  }

  
    /* 顶栏迷你指数更新 */
    function updateNavIndex(latest, diffPct) {
        var el = document.getElementById("nav-index");
        if (!el) return;
        var valEl = el.querySelector(".nav-index-val");
        var diffEl = el.querySelector(".nav-index-diff");
        if (valEl && latest != null) {
            valEl.textContent = Number(latest).toFixed(2);
        }
        if (diffEl && diffPct != null) {
            var pct = (diffPct * 100).toFixed(2) + "%";
            diffEl.textContent = (diffPct >= 0 ? "+" : "") + pct;
            diffEl.className = "nav-index-diff " + (diffPct >= 0 ? "up" : "down");
        }
    }

  function agoText(seconds) {
    if (seconds === null || seconds === undefined) return "—";
    if (seconds < 60) return Math.max(0, Math.round(seconds)) + " 秒前";
    if (seconds < 3600) return Math.round(seconds / 60) + " 分钟前";
    return (seconds / 3600).toFixed(1) + " 小时前";
  }

  /* ------------------------------------------------------------------ *
   * 全站：顶栏实时徽标
   * ------------------------------------------------------------------ */
  var topDot = document.getElementById("rt-live-dot");
  var topLabel = document.getElementById("rt-live-label");
  var lastStatus = null;

  function setTopState(state, text) {
    if (topDot) topDot.setAttribute("data-state", state);
    if (topLabel && text) topLabel.textContent = text;
  }

  function applyStatus(status) {
    lastStatus = status;
    var wsOk = status && status.ws_collector && status.ws_collector.running;
    var supOk = status && status.supervisor && status.supervisor.running;
    var paused = status && status.pauses && Object.keys(status.pauses).length > 0;
    if (paused) {
      setTopState("paused", "暂停");
    } else if (wsOk && supOk) {
      setTopState("on", "实时");
    } else {
      setTopState("warn", wsOk ? "采集待命" : "等待数据");
    }
    var alertDot = document.getElementById("rt-data-alert");
    if (alertDot) alertDot.style.display = (paused || !wsOk) ? "inline-block" : "none";
    var root = document.getElementById("realtime-root");
    if (root && root._page) root._page.onStatus(status);
  }

  /* ------------------------------------------------------------------ *
   * /realtime 页面逻辑
   * ------------------------------------------------------------------ */
  function RealtimePage(root) {
    this.root = root;
    this.indexChart = null;
    this.klineChart = null;
    this.indexPoints = [];
    this.klinePoints = [];
    this.listings = [];
    this._bind();
  }

  RealtimePage.prototype._bind = function () {
    var self = this;
    this.root._page = this;
    if (window.echarts) {
      this.indexChart = echarts.init(document.getElementById("rt-index-chart"));
      this.klineChart = echarts.init(document.getElementById("rt-kline-chart"));
      window.addEventListener("resize", function () {
        if (self.indexChart) self.indexChart.resize();
        if (self.klineChart) self.klineChart.resize();
      });
    }
  };

  RealtimePage.prototype.onSnapshot = function (snapshot) {
    var self = this;
    this.applyStats(snapshot.statistics || {});
    this.indexPoints = (snapshot.index && snapshot.index.series || []).map(function (p) {
      return [p.ts, p.value];
    });
    this.renderIndex();
    var kline = (snapshot.kline && snapshot.kline.h) || [];
    this.klinePoints = kline.map(function (k) {
      return [k.ts, k.open, k.close, k.low, k.high];
    });
    this.renderKline();
    this.listings = (snapshot.listings || []).slice().reverse();
    this.renderListings();
    this.onStatus({
      ws_collector: snapshot.ws_collector,
      supervisor: snapshot.supervisor,
      pauses: snapshot.pauses,
      fresh_seconds: snapshot.fresh_seconds,
    });
    var idx = snapshot.index && snapshot.index.latest;
    var el = document.getElementById("rt-index-value");
        updateNavIndex(idx && idx.value, idx && idx.change);
    if (el && idx) el.textContent = fmtNumber(idx.value);
  };

  RealtimePage.prototype.applyStats = function (statistics) {
    var cards = this.root.querySelectorAll(".rt-stat-card");
    cards.forEach(function (card) {
      var metric = card.getAttribute("data-metric");
      var item = statistics[metric];
      var valueEl = card.querySelector(".rt-stat-value");
      var tsEl = card.querySelector(".rt-stat-ts");
      if (!item) return; /* 增量批次只更新出现的指标，不清空其它卡片 */
      if (valueEl && valueEl.textContent !== fmtNumber(item.value)) {
        valueEl.textContent = fmtNumber(item.value);
        card.classList.remove("flash");
        void card.offsetWidth; /* 重启动画 */
        card.classList.add("flash");
      }
      if (tsEl) tsEl.textContent = fmtTime(item.ts);
    });
  };

  RealtimePage.prototype.renderIndex = function () {
    if (!this.indexChart || !this.indexPoints.length) return;
    var values = this.indexPoints.map(function (p) { return p[1]; });
    var min = Math.min.apply(null, values);
    var max = Math.max.apply(null, values);
    var pad = (max - min) * 0.1 || 1;
    this.indexChart.setOption({
      animation: false,
      grid: { left: 56, right: 16, top: 16, bottom: 28 },
      xAxis: {
        type: "category",
        boundaryGap: false,
        data: this.indexPoints.map(function (p) { return fmtTime(p[0]); }),
        axisLabel: { color: "#7d8aa0", hideOverlap: true, fontSize: 10 },
      },
      yAxis: {
        type: "value",
        min: Math.floor(min - pad),
        max: Math.ceil(max + pad),
        axisLabel: { color: "#7d8aa0" },
        splitLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } },
      },
      series: [{
        type: "line",
        showSymbol: false,
        data: values,
        lineStyle: { color: "#3fd0a4", width: 2 },
        areaStyle: {
          color: {
            type: "linear", x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: "rgba(63,208,164,0.25)" },
              { offset: 1, color: "rgba(63,208,164,0)" },
            ],
          },
        },
      }],
    }, true);
  };

  RealtimePage.prototype.renderKline = function () {
    var hint = document.getElementById("rt-kline-hint");
    if (hint) {
      hint.textContent = this.klinePoints.length < 5
        ? "推送样本 " + this.klinePoints.length + " 根（每小时新增 1 根，稍后成型）"
        : "";
    }
    if (!this.klineChart || !this.klinePoints.length) return;
    this.klineChart.setOption({
      animation: false,
      grid: { left: 56, right: 16, top: 16, bottom: 28 },
      xAxis: {
        type: "category",
        data: this.klinePoints.map(function (p) { return p[0].slice(5, 16); }),
        axisLabel: { color: "#7d8aa0" },
      },
      yAxis: { scale: true, axisLabel: { color: "#7d8aa0" },
               splitLine: { lineStyle: { color: "rgba(255,255,255,0.06)" } } },
      series: [{
        type: "candlestick",
        data: this.klinePoints.map(function (p) { return [p[1], p[2], p[3], p[4]]; }),
        itemStyle: { color: "#3fd0a4", color0: "#e0566b",
                     borderColor: "#3fd0a4", borderColor0: "#e0566b" },
      }],
    }, true);
  };

  RealtimePage.prototype.renderListings = function () {
    var body = document.getElementById("rt-listing-body");
    var empty = document.getElementById("rt-listing-empty");
    var count = document.getElementById("rt-listing-count");
    if (!body) return;
    if (empty) empty.classList.toggle("hidden", this.listings.length > 0);
    if (count) count.textContent = this.listings.length + " 条";
    var rows = this.listings.slice(0, 50).map(function (item) {
      var wear = item.float_wear === null || item.float_wear === undefined
        ? "—" : Number(item.float_wear).toFixed(6);
      return "<tr><td>" + fmtTime(item.ts) + "</td><td class=\"mono\" title=\""
        + String(item.item_id || "") + "\">" + String(item.item_id || "—").slice(0, 24)
        + "</td><td>" + fmtNumber(item.price) + "</td><td>" + wear + "</td></tr>";
    });
    body.innerHTML = rows.join("");
  };

  RealtimePage.prototype.onDelta = function (delta) {
    var self = this;
    var stats = {};
    var indexChanged = false;
    var klineChanged = false;
    (delta.events || []).forEach(function (event) {
      var payload = event.payload || {};
      if (event.kind === "statistics") {
        stats[payload.metric] = { value: payload.value, ts: payload.ts };
      } else if (event.kind === "index") {
        self.indexPoints.push([payload.ts, payload.value]);
        if (self.indexPoints.length > 720) self.indexPoints.shift();
        indexChanged = true;
        var el = document.getElementById("rt-index-value");
        updateNavIndex(idx && idx.value, idx && idx.change);
        if (el) el.textContent = fmtNumber(payload.value);
      } else if (event.kind === "kline_h") {
        self.klinePoints.push([payload.ts, payload.open, payload.close,
                               payload.low, payload.high]);
        if (self.klinePoints.length > 240) self.klinePoints.shift();
        klineChanged = true;
      } else if (event.kind === "listing") {
        var extra = payload.extra || {};
        self.listings.unshift({
          ts: payload.ts,
          item_id: payload.item_id,
          price: payload.value,
          float_wear: extra.floatWear,
          asset_id: extra.assetId,
        });
        if (self.listings.length > 200) self.listings.pop();
      }
    });
    if (Object.keys(stats).length) this.applyStats(stats);
    if (indexChanged) this.renderIndex();
    if (klineChanged) this.renderKline();
    this.renderListings();
  };

  RealtimePage.prototype.onStatus = function (status) {
    if (!status) return;
    var wsEl = document.getElementById("rt-ws-collector");
    if (wsEl) {
      var ws = status.ws_collector || {};
      wsEl.querySelector(".rt-pill-value").textContent = ws.running
        ? "运行中 · " + agoText(ws.seconds_since_message)
        : "未运行";
      wsEl.setAttribute("data-state", ws.running ? "on" : "off");
    }
    var supEl = document.getElementById("rt-supervisor");
    if (supEl) {
      var sup = status.supervisor || {};
      supEl.querySelector(".rt-pill-value").textContent = sup.running
        ? "运行中" : "离线";
      supEl.setAttribute("data-state", sup.running ? "on" : "off");
    }
    var taskEl = document.getElementById("rt-active-tasks");
    if (taskEl && status.supervisor && status.supervisor.active) {
      taskEl.querySelector(".rt-pill-value").textContent =
        status.supervisor.active.length + " 个";
    }
    var pauseEl = document.getElementById("rt-pauses");
    if (pauseEl) {
      var pauses = status.pauses || {};
      var names = Object.keys(pauses);
      pauseEl.querySelector(".rt-pill-value").textContent = names.length
        ? names.map(function (n) {
            var info = pauses[n] || {};
            return n + (info.error_class ? "(" + info.error_class + ")" : "");
          }).join(" ")
        : "无";
      pauseEl.setAttribute("data-state", names.length ? "warn" : "on");
    }
    var freshEl = document.getElementById("rt-freshness");
    if (freshEl) {
      freshEl.querySelector(".rt-pill-value").textContent =
        agoText(status.fresh_seconds);
    }
    var timeEl = document.getElementById("rt-server-time");
    if (timeEl && status.server_time) {
      timeEl.textContent = "服务器时间 " + fmtTime(status.server_time);
    }
  };

  /* ------------------------------------------------------------------ *
   * SSE 连接管理（EventSource 自带重连；此处补充状态反馈）
   * ------------------------------------------------------------------ */
  /* 首页：大盘指数卡片实时刷新（值更新 + 闪动标记） */
  function HomePage() { this.indexEl = document.getElementById("m-index"); }
  HomePage.prototype.onDelta = function (delta) {
    var self = this;
    if (!this.indexEl) return;
    (delta.events || []).forEach(function (event) {
      if (event.kind !== "index" || !event.payload) return;
      var value = event.payload.value;
      if (value === null || value === undefined) return;
      self.indexEl.textContent = fmtNumber(Number(value));
      self.indexEl.classList.remove("rt-flash-text");
      void self.indexEl.offsetWidth;
      self.indexEl.classList.add("rt-flash-text");
    });
  };

  var pageRoot = document.getElementById("realtime-root");
  var page = null;
  if (pageRoot && document.body.classList.contains("page-realtime")) {
    page = new RealtimePage(pageRoot);
    try {
      var embedded = JSON.parse(pageRoot.getAttribute("data-snapshot") || "{}");
      if (embedded && embedded.statistics) page.onSnapshot(embedded);
    } catch (err) { /* 初始快照解析失败时等待 SSE snapshot */ }
  }

  var connDot = document.getElementById("rt-conn-dot");
  var connText = document.getElementById("rt-conn-text");

  function setConn(state, text) {
    if (connDot) connDot.setAttribute("data-state", state);
    if (connText) connText.textContent = text;
  }

  // ---------- 市场看盘台 ----------
  function loadDashboard() {
    fetch("/api/dashboard/market").then(function (r) { return r.json(); }).then(function (d) {
      var spreads = d.spreads || [];
      var spreadEl = document.getElementById("dash-spreads-body");
      if (spreadEl) {
        spreadEl.innerHTML = spreads.slice(0, 10).map(function (s) {
          var net = s.net_spread_pct;
          var netColor = net > 0 ? "var(--up, #f6465d)" : "var(--text-3)";
          var img = s.image_url ? "<img src='/api/image?url=" + encodeURIComponent(s.image_url) + "' style='width:28px;height:20px;object-fit:contain' onerror='this.style.visibility=\"hidden\"'>" : "";
          return "<tr><td style='display:flex;align-items:center;gap:4px'>" + img + "<span title='" + s.item_id + "'>" + (s.name || "").slice(0, 16) + "</span></td>" +
            "<td>" + s.min_platform + " ¥" + s.min_price + "</td>" +
            "<td>" + s.max_platform + " ¥" + s.max_price + "</td>" +
            "<td>" + s.spread_pct + "%</td>" +
            "<td style='color:" + netColor + "'>" + (net > 0 ? "+" : "") + net + "%</td></tr>";
        }).join("") || "<tr><td colspan='5' class='muted'>暂无≥5%价差</td></tr>";
      }
      var hot = d.hot_items || [];
      var hotEl = document.getElementById("dash-hot-body");
      if (hotEl) {
        hotEl.innerHTML = hot.slice(0, 20).map(function (h) {
          var img = h.image_url ? "<img src='/api/image?url=" + encodeURIComponent(h.image_url) + "' style='width:28px;height:20px;object-fit:contain' onerror='this.style.visibility=\"hidden\"'>" : "";
          return "<tr><td style='display:flex;align-items:center;gap:4px'>" + img + "<span title='" + h.item_id + "'>" + (h.name || "").slice(0, 20) + "</span></td>" +
            "<td class='num'>" + h.tx_count.toLocaleString() + "</td></tr>";
        }).join("") || "<tr><td colspan='2' class='muted'>暂无数据</td></tr>";
      }
      var m = d.market || {};
      var bar = document.getElementById("dash-market-bar");
      if (bar) {
        bar.innerHTML =
          (m.index ? "大盘指数 " + m.index : "") +
          (m.today_tx ? " · 今日成交 " + m.today_tx.toLocaleString() + " 笔" : "") +
          (m.today_turnover ? " · 成交额 ¥" + (m.today_turnover / 10000).toFixed(0) + " 万" : "") +
          (m.survive ? " · 存世 " + (m.survive / 1e8).toFixed(2) + " 亿" : "");
      }
    }).catch(function () {});
  }
  loadDashboard();
  setInterval(loadDashboard, 3600000);

  var homePage = null;
  if (document.body.classList.contains("page-index")) {
    homePage = new HomePage();
  }

  function connect() {
    if (!("EventSource" in window)) {
      setConn("off", "浏览器不支持 SSE");
      return;
    }
    var source = new EventSource("/api/realtime/stream");
    source.addEventListener("open", function () {
      setConn("on", "已连接");
      setTopState(lastStatus && lastStatus.ws_collector &&
                  lastStatus.ws_collector.running ? "on" : "warn", "实时");
    });
    source.addEventListener("snapshot", function (event) {
      var data = JSON.parse(event.data);
      if (page) page.onSnapshot(data);
      applyStatus({
        ws_collector: data.ws_collector,
        supervisor: data.supervisor,
        pauses: data.pauses,
        fresh_seconds: data.fresh_seconds,
      });
    });
    source.addEventListener("delta", function (event) {
      var data = JSON.parse(event.data);
      if (page) page.onDelta(data);
      if (homePage) homePage.onDelta(data);
    });
    source.addEventListener("status", function (event) {
      applyStatus(JSON.parse(event.data));
      fetchDashboard();
    });
    /* 断线感知（检修缺失项）：EventSource 自动重连，但徽标必须如实反映 */
    source.onopen = function () {
      setTopState(lastStatus ? (lastStatus.pauses && Object.keys(lastStatus.pauses).length ? "paused" : "on") : "on",
                  lastStatus && lastStatus.pauses && Object.keys(lastStatus.pauses).length ? "暂停" : "实时");
    };
    source.onerror = function () {
      setTopState("off", "重连中…");
    };
    /* watchdog：90 秒无任何事件（含 ping）→ 数据过期横幅 */
    var lastEventAt = Date.now();
    ["snapshot", "delta", "status", "ping"].forEach(function (name) {
      source.addEventListener(name, function () { lastEventAt = Date.now(); });
    });
    setInterval(function () {
      var stale = Date.now() - lastEventAt > 90000;
      var banner = document.getElementById("rt-stale-banner");
      if (banner) banner.style.display = stale ? "block" : "none";
      if (stale) setTopState("off", "数据过期");
    }, 15000);

    
function fetchDashboard() {
      Promise.all([
        fetch("/api/dashboard/supply-trend?limit=10").then(function(r){return r.ok?r.json():{items:[]};}).catch(function(){return {items:[]};}),
        fetch("/api/dashboard/volume-flow?limit=10").then(function(r){return r.ok?r.json():{items:[]};}).catch(function(){return {items:[]};}),
        fetch("/api/dashboard/spread?limit=10").then(function(r){return r.ok?r.json():{items:[]};}).catch(function(){return {items:[]};})
      ]).then(function(results) {
        var body = document.getElementById("rt-signals-body");
        if (!body) return;
        var html = "";
        var sections = [
          {label: "📦 在售量↓（吸筹前兆）", items: results[0].items, key: "ratio", fmt: function(v){return v ? v.toFixed(3) : "-";}},
          {label: "⚡ 成交活跃↑", items: results[1].items, key: "multiple", fmt: function(v){return v ? v.toFixed(1)+"x" : "-";}},
          {label: "💰 跨平台价差", items: results[2].items, key: "max_p", fmt: function(v,item){
            if (!v || !item.min_p) return "-";
            return ((item.max_p-item.min_p)/((item.max_p+item.min_p)/2)*100).toFixed(1)+"%";
          }}
        ];
        sections.forEach(function(sec) {
          html += "<tr><td colspan='3' style='color:var(--text-3);font-size:12px;padding:4px 0'>"+sec.label+"</td></tr>";
          (sec.items||[]).forEach(function(s) {
            var img = s.image_url ? "<img src='/api/image?url="+encodeURIComponent(s.image_url)+"' style='width:24px;height:18px;object-fit:contain;border-radius:2px'>" : "";
            html += "<tr><td style='display:flex;align-items:center;gap:4px;font-size:13px'>"+img+"<span>"+(s.item_name||"?").slice(0,16)+"</span></td>"+
              "<td class='num'>"+sec.fmt(s[sec.key],s)+"</td>"+
              "<td class='muted' style='font-size:11px'>¥"+(s.min_p||s.sn_recent||0).toFixed(0)+"</td></tr>";
          });
        });
        body.innerHTML = html || "<tr><td colspan='3' class='muted'>加载中…</td></tr>";
      });
    }
    fetchDashboard();
    setInterval(fetchDashboard, 60000);
  }  /* connect() 闭合（界面审计发现的语法错误：全站实时 JS 因此瘫痪） */

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", connect);
  } else {
    connect();
  }
})();
