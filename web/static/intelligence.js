/* 市场情报仪表盘：单次 fetch 渲染全部面板（原生 JS 零构建）。 */
(function () {
  "use strict";

  function $(id) { return document.getElementById(id); }

  function esc(v) {
    return String(v == null ? "" : v).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  var STAGE_COLORS = {
    "狂热": "#f6465d", "恐慌": "#b3223a", "出清": "#e8963a",
    "冷漠": "#8d99ae", "萌芽": "#0ecb81"
  };
  var CYCLE_ORDER = ["狂热", "恐慌", "出清", "冷漠", "萌芽"];

  function pct(v, d) {
    if (v == null || !isFinite(v)) return "—";
    return (v * 100).toFixed(d === undefined ? 1 : d) + "%";
  }

  function render(data) {
    var s = data.sentiment || {};
    var sup = data.supply || {};
    var m = sup.market || {};
    var ev = data.events || {};
    var ex = data.execution || {};
    var sm = data.summary || {};

    /* ---- 顶部状态条 ---- */
    var stage = s.stage || "unknown";
    var color = STAGE_COLORS[stage] || "#8d99ae";
    var stageEl = $("intel-stage");
    if (stageEl) {
      stageEl.textContent = stage === "unknown" ? "等待数据" : stage;
      stageEl.style.color = color;
      stageEl.style.borderColor = color + "66";
      stageEl.style.background = color + "1a";
    }
    if ($("intel-stage-conf")) {
      $("intel-stage-conf").textContent = s.confidence != null
        ? "置信度 " + pct(s.confidence, 0) : "";
    }
    if ($("intel-supply-score")) {
      $("intel-supply-score").textContent = m.exhaustion_score != null
        ? m.exhaustion_score.toFixed(2) : "—";
      var gapMap = { supply_tight: "供给收紧", balanced: "供需均衡", oversupplied: "供给过剩" };
      $("intel-supply-gap").textContent = gapMap[m.gap] || m.gap || "—";
    }
    if ($("intel-action")) {
      var action = sm.action || "";
      if (!action) {
        if (stage === "狂热") action = "左侧减仓";
        else if (stage === "恐慌") action = "观望等待";
        else if (stage === "冷漠") action = "可分批建仓";
        else if (stage === "萌芽") action = "逐步加仓";
        else action = "观察等待";
      }
      $("intel-action").textContent = action;
    }
    if ($("intel-time")) {
      $("intel-time").textContent = "更新于 " + new Date().toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" });
    }

    /* ---- 风险警告 ---- */
    var riskEl = $("intel-risk");
    if (riskEl) {
      if (m.risk_flag === "dead_demand") {
        riskEl.textContent = "⚠ 供给收紧但需求也在萎缩——有价无市风险（参考 Beanie Babies 教训）";
        riskEl.classList.remove("hidden");
      } else {
        riskEl.classList.add("hidden");
      }
    }

    /* ---- 情绪周期条 ---- */
    CYCLE_ORDER.forEach(function (name) {
      var el = document.querySelector('[data-stage="' + name + '"]');
      if (el) {
        el.classList.remove("active");
        if (name === stage) el.classList.add("active");
      }
    });

    /* ---- 情绪指标行 ---- */
    var indRows = $("intel-ind-rows");
    if (indRows) {
      var inds = s.indicators || {};
      var html = "";
      var labelMap = {
        tx_volume: "成交量", breadth_active: "涨跌比", market_index: "大盘",
        listing_rate: "挂单流速", spd_fb: "价差", survive: "存世量"
      };
      Object.keys(inds).forEach(function (key) {
        var ind = inds[key];
        if (!ind || typeof ind !== "object") return;
        var label = labelMap[key] || key;
        var val = ind.display || "—";
        var status = ind.met === true ? "✓" : ind.met === false ? "✗" : "?";
        var statusCls = ind.met === true ? "met" : ind.met === false ? "not-met" : "unknown";
        var quality = ind.quality || "";
        if (quality === "insufficient") {
          html += '<div class="intel-ind-row muted"><span class="ind-label">' + esc(label) +
            '</span><span class="ind-val">数据不足</span><span class="ind-status unknown">?</span></div>';
        } else {
          html += '<div class="intel-ind-row"><span class="ind-label">' + esc(label) +
            '</span><span class="ind-val">' + esc(val) +
            '</span><span class="ind-status ' + statusCls + '">' + status + '</span></div>';
        }
      });
      indRows.innerHTML = html || '<p class="empty">等待情绪周期引擎运行</p>';
    }

    /* ---- 转移提示（折叠） ---- */
    var hintsWrap = $("intel-hints-wrap");
    var hintsEl = $("intel-hints");
    if (hintsWrap && hintsEl) {
      var hints = s.transition_hints || [];
      if (hints.length > 0) {
        hintsWrap.style.display = "";
        hintsEl.innerHTML = hints.map(function (h) {
          return "<li>" + esc(h) + "</li>";
        }).join("");
      } else {
        hintsWrap.style.display = "none";
      }
    }

    /* ---- 供给仪表 ---- */
    var gauge = $("intel-gauge-fill");
    if (gauge && m.exhaustion_score != null) {
      var score = Math.max(0, Math.min(1, m.exhaustion_score));
      gauge.style.width = (score * 100).toFixed(1) + "%";
      gauge.style.background = score > 0.6 ? "#e8963a" : score > 0.35 ? "#e9b965" : "#0ecb81";
    }
    var kvs = $("intel-supp-kvs");
    if (kvs) {
      var demandMap = { "+": "↑ 回升", "-": "↓ 萎缩", "0": "→ 持平" };
      var rows = [
        ["月消耗率", m.burn_rate != null ? pct(m.burn_rate, 1) : "—"],
        ["在售/存世", m.listing_ratio != null ? pct(m.listing_ratio, 1) : "—"],
        ["连续收缩", m.absorption_days != null ? m.absorption_days + " 天" : "—"],
        ["需求方向", demandMap[m.demand_trend] || m.demand_trend || "—"],
      ];
      kvs.innerHTML = rows.map(function (r) {
        return '<div class="intel-supp-kv"><span>' + r[0] + '</span><strong>' + esc(r[1]) + '</strong></div>';
      }).join("");
    }

    /* ---- 事件列表 ---- */
    var evEl = $("intel-events");
    if (evEl) {
      var recent = (ev.recent || []).slice(0, 5);
      var upcoming = ev.upcoming || [];
      var html = "";
      if (upcoming.length > 0) {
        html += '<div class="intel-ev-section-title">即将发生</div>';
        upcoming.slice(0, 3).forEach(function (e) {
          html += '<div class="intel-ev-item upcoming"><span class="ev-date">' + esc((e.date || "").slice(5)) +
            '</span><span class="ev-title">' + esc(e.title || "") + '</span><span class="ev-type">' + esc(e.type || "") + '</span></div>';
        });
      }
      html += '<div class="intel-ev-section-title">近期</div>';
      if (recent.length === 0) {
        html += '<p class="empty">暂无事件记录</p>';
      } else {
        recent.forEach(function (e) {
          html += '<div class="intel-ev-item"><span class="ev-date">' + esc((e.date || "").slice(5)) +
            '</span><span class="ev-title">' + esc(e.title || "") + '</span><span class="ev-type">' + esc(e.type || "") + '</span></div>';
        });
      }
      evEl.innerHTML = html;
    }

    /* ---- 执行参考 ---- */
    var execCards = $("intel-exec-cards");
    if (execCards) {
      var ami = ex.amihud || {};
      var spd = ex.spd_fb || {};
      var amiMedian = ami.median;
      var amiLevel = "—";
      if (amiMedian != null && isFinite(amiMedian)) {
        if (amiMedian < 1e-5) amiLevel = "低（易交易）";
        else if (amiMedian < 1e-4) amiLevel = "中";
        else amiLevel = "高（难交易）";
      }
      var spdMedian = spd.median;
      var spdLevel = "—";
      if (spdMedian != null && isFinite(spdMedian)) {
        spdLevel = pct(spdMedian, 1);
      }
      var topTurnover = (ex.turnover || {}).top || [];
      var topHtml = topTurnover.slice(0, 3).map(function (t) {
        return '<span class="intel-tag">' + esc((t.name || "?").slice(0, 14)) + '</span>';
      }).join("") || '<span class="muted">—</span>';

      execCards.innerHTML =
        '<div class="intel-exec-card"><span class="exec-icon">💧</span><strong>流动性</strong>' +
        '<span class="exec-val">' + esc(amiLevel) + '</span>' +
        '<span class="sub">Amihud ' + (amiMedian != null && isFinite(amiMedian) ? amiMedian.toExponential(1) : "—") + '</span></div>' +
        '<div class="intel-exec-card"><span class="exec-icon">⇄</span><strong>跨平台价差</strong>' +
        '<span class="exec-val">' + esc(spdLevel) + '</span>' +
        '<span class="sub">' + (spd.trend != null ? (spd.trend > 0 ? "走扩 ↑" : spd.trend < 0 ? "收窄 ↓" : "持平 →") : "") + '</span></div>' +
        '<div class="intel-exec-card"><span class="exec-icon">🔄</span><strong>换手活跃</strong>' +
        '<span class="exec-tags">' + topHtml + '</span></div>';
    }
  }

  /* ---- 情绪周期历史图（阶段色带 + 置信度 + 大盘指数） ---- */
  var STAGE_NAMES = ["狂热", "恐慌", "出清", "冷漠", "萌芽"];
  function renderHistory(data) {
    var dom = $("intel-history-chart");
    var note = $("intel-history-note");
    if (!dom || typeof echarts === "undefined") return;
    var pts = (data && data.points) || [];
    if (note) {
      note.textContent = pts.length
        ? "阶段色带 + 置信度曲线 + 大盘指数（近 90 天 · " + pts.length + " 个判定点）"
        : "情绪周期引擎尚未积累历史（每次运行写入一个判定点）";
    }
    if (!pts.length) { dom.style.display = "none"; return; }
    if (pts.length < 5) {
      /* 历史太短：画出来近似空白，改为明确的积累提示 */
      dom.style.display = "none";
      if (note) {
        note.innerHTML = "📈 情绪周期引擎已运行 <b>" + pts.length + "</b> 天，历史轨迹将在积累约 1 周后开始展示" +
          '<span class="intel-tag" style="margin-left:6px">当前阶段：' + esc(STAGE_NAMES[pts[pts.length - 1].stage_idx] || "?") + "</span>";
      }
      return;
    }
    dom.style.display = "";
    var chart = dom.__echarts || echarts.init(dom);
    dom.__echarts = chart;
    var xs = [], stage = [], conf = [], mkt = [];
    var mkt0 = null;
    pts.forEach(function (p) {
      xs.push(p.ts.slice(5, 10));
      stage.push(p.stage_idx == null ? null : p.stage_idx);
      conf.push(p.confidence == null ? null : +(p.confidence * 100).toFixed(1));
      if (p.market_index != null) {
        if (mkt0 == null) mkt0 = p.market_index;
        mkt.push(+(((p.market_index / mkt0) - 1) * 100).toFixed(2));
      } else mkt.push(null);
    });
    chart.setOption({
      backgroundColor: "transparent", animation: false,
      tooltip: { trigger: "axis", confine: true,
        backgroundColor: "rgba(13,16,21,.96)", borderColor: "#2d3848",
        textStyle: { color: "#f2f5f8", fontSize: 11 },
        formatter: function (ps) {
          var i = ps[0].dataIndex;
          return esc(pts[i].ts.slice(0, 10)) + "<br>阶段：<b>" +
            (pts[i].stage_idx != null ? STAGE_NAMES[pts[i].stage_idx] || "?" : "—") + "</b><br>置信度：" +
            (pts[i].confidence != null ? (pts[i].confidence * 100).toFixed(0) + "%" : "—");
        } },
      grid: { left: 44, right: 44, top: 14, bottom: 24 },
      xAxis: { type: "category", data: xs, boundaryGap: false,
        axisLine: { lineStyle: { color: "#2d3848" } },
        axisLabel: { color: "#8b98ab", fontSize: 10, hideOverlap: true },
        axisTick: { show: false } },
      yAxis: [
        { type: "value", min: -0.5, max: 4.5, interval: 1,
          axisLabel: { color: "#8b98ab", fontSize: 10,
            formatter: function (v) { return STAGE_NAMES[v] || ""; } },
          splitLine: { show: false } },
        { type: "value", axisLabel: { color: "#8b98ab", fontSize: 10,
            formatter: "{value}%" },
          splitLine: { lineStyle: { color: "rgba(52,62,76,.24)", type: "dashed" } } }
      ],
      series: [
        { name: "阶段", type: "line", step: "middle", data: stage,
          lineStyle: { width: 0 }, symbol: "none", silent: true,
          areaStyle: { opacity: .28 },
          markLine: { silent: true, symbol: "none", label: { show: false },
            lineStyle: { color: "rgba(139,148,166,.25)", type: "dashed" },
            data: [{ yAxis: 4 }, { yAxis: 3 }, { yAxis: 2 }, { yAxis: 1 }, { yAxis: 0 }] } },
        { name: "置信度", type: "line", yAxisIndex: 1, data: conf, symbol: "none",
          lineStyle: { width: 1.55, color: "#3d7eff" },
          areaStyle: { opacity: .06, color: "#3d7eff" } },
        { name: "大盘", type: "line", yAxisIndex: 1, data: mkt, symbol: "none",
          lineStyle: { width: 1.25, color: "#8b94a6", type: "dashed" } }
      ],
      visualMap: { show: false, dimension: 1, seriesIndex: 0,
        pieces: [
          { value: 0, color: "#f6465d" }, { value: 1, color: "#b3223a" },
          { value: 2, color: "#e8963a" }, { value: 3, color: "#8d99ae" },
          { value: 4, color: "#0ecb81" }
        ] }
    }, true);
  }
  function loadHistory() {
    fetch("/api/intelligence/sentiment-history?days=90")
      .then(function (r) { return r.ok ? r.json() : { points: [] }; })
      .then(renderHistory)
      .catch(function () { renderHistory({ points: [] }); });
  }

  function load() {
    fetch("/api/intelligence/all")
      .then(function (r) { return r.ok ? r.json() : Promise.reject(new Error("HTTP " + r.status)); })
      .then(render)
      .catch(function (err) {
        var el = $("intel-stage");
        if (el) { el.textContent = "加载失败"; el.style.color = "#f6465d"; }
        console.error("intelligence load error:", err);
      });
  }

  document.getElementById("intel-refresh").addEventListener("click", function () { load(); loadHistory(); });
  load();
  loadHistory();
  setInterval(load, 60000);
})();
