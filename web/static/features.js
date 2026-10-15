/* 特征仪表盘：消费 /api/features/grid（特征体系 UI 闭环，原生 JS 零构建）。 */
(function () {
  "use strict";

  function escapeHtml(v) {
    return String(v == null ? "" : v).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function fmt(v, digits) {
    if (v === null || v === undefined) return "—";
    if (!isFinite(v)) return "—";
    var abs = Math.abs(v);
    if (abs >= 1000) return v.toFixed(0);
    if (abs >= 1) return v.toFixed(digits === undefined ? 2 : digits);
    if (abs === 0) return "0";
    if (abs < 0.0001) return v.toExponential(1);
    return v.toFixed(digits === undefined ? 3 : digits + 1);
  }

  function fmtAmihud(v) {
    if (v === null || v === undefined || !isFinite(v)) return "—";
    return v.toExponential(1);
  }

  function cell(v, cls) {
    return '<td class="num ' + (cls || "") + '">' + v + "</td>";
  }

  function signedClass(v) {
    if (v === null || v === undefined || !isFinite(v)) return "";
    return v > 0 ? "up" : v < 0 ? "down" : "";
  }

  function renderRow(item) {
    var f = item.features || {};
    var name = escapeHtml(item.name || item.item_id);
    var zvr = f.zero_vol_rise === 1;
    return "<tr>" +
      '<td><a href="/market/items/' + encodeURIComponent(item.name) + '">' + name + "</a></td>" +
      cell(fmt(f.ano_score, 1), f.ano_score > 5 ? "up" : "") +
      cell(fmt(f.ret_7d, 3), signedClass(f.ret_7d)) +
      cell(fmt(f.dsr_z90, 1), signedClass(f.dsr_z90)) +
      cell(fmtAmihud(f.amihud_7d)) +
      cell(fmt(f.liq_contraction, 2), signedClass(f.liq_contraction)) +
      cell(fmt(f.crash_pressure, 2), signedClass(f.crash_pressure)) +
      cell(f.bottom_seq === undefined ? "—" : f.bottom_seq, f.bottom_seq >= 2 ? "up" : "") +
      cell(zvr ? "⚠" : "—", zvr ? "up" : "") +
      cell(fmt(f.miss_rate_7d, 2), f.miss_rate_7d > 0.5 ? "down" : "") +
      "</tr>";
  }

  function load() {
    var state = document.getElementById("feat-state");
    var sort = document.getElementById("feat-sort").value;
    var miss = document.getElementById("feat-miss").value;
    state.textContent = "加载中…";
    var url = "/api/features/grid?limit=100&sort=" + encodeURIComponent(sort) +
      "&max_miss=" + encodeURIComponent(miss);
    fetch(url).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function (d) {
      var body = document.getElementById("feat-body");
      var items = d.items || [];
      body.innerHTML = items.length
        ? items.map(renderRow).join("")
        : '<tr><td colspan="10" class="empty">无数据（特征管线可能未运行或 miss 过滤过严）</td></tr>';
      document.getElementById("feat-total").textContent =
        "（截面 " + (d.total || 0) + " 件，显示前 " + items.length + "）";
      document.getElementById("feat-asof").textContent =
        d.as_of ? "特征计算于 " + String(d.as_of).slice(0, 19).replace("T", " ") : "";
      state.textContent = "就绪";
    }).catch(function (err) {
      document.getElementById("feat-body").innerHTML =
        '<tr><td colspan="10" class="empty">加载失败：' + escapeHtml(String(err.message || err)) +
        ' <button class="btn sm" onclick="location.reload()">重试</button></td></tr>';
      state.textContent = "错误";
    });
  }

  document.getElementById("feat-refresh").addEventListener("click", load);
  document.getElementById("feat-sort").addEventListener("change", load);
  document.getElementById("feat-miss").addEventListener("change", load);
  load();
  setInterval(load, 300000); /* 5 分钟自动刷新（API 自带 120s 缓存） */
})();
