(function () {
  "use strict";
  var search = document.getElementById("search-overlay");
  var trigger = document.getElementById("search-trigger");
  var close = document.getElementById("search-close");
  if (search && trigger && close) {
    search.addEventListener("keydown", function (event) {
      if (event.key !== "Tab") return;
      var controls = Array.prototype.slice.call(search.querySelectorAll(
        "button:not([disabled]), input:not([disabled]), a[href]"
      )).filter(function (node) { return node.getClientRects().length; });
      if (!controls.length) return;
      if (event.shiftKey && document.activeElement === controls[0]) {
        event.preventDefault();
        controls[controls.length - 1].focus();
      } else if (!event.shiftKey && document.activeElement === controls[controls.length - 1]) {
        event.preventDefault();
        controls[0].focus();
      }
    });
  }
})();
