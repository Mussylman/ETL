// Портал: тема, копирование команд, оглавление со scroll-spy, поиск по разделам. Без библиотек.
(function () {
  var root = document.documentElement;
  try { var t = localStorage.getItem("theme"); if (t) root.setAttribute("data-theme", t); } catch (e) {}
  document.addEventListener("DOMContentLoaded", function () {
    var tb = document.getElementById("theme");
    if (tb) tb.addEventListener("click", function () {
      var light = root.getAttribute("data-theme") === "light";
      root.setAttribute("data-theme", light ? "dark" : "light");
      try { localStorage.setItem("theme", light ? "dark" : "light"); } catch (e) {}
    });
    var mb = document.getElementById("menu");
    if (mb) mb.addEventListener("click", function () { document.body.classList.toggle("nav-open"); });

    document.querySelectorAll(".copy").forEach(function (b) {
      b.addEventListener("click", function () {
        var pre = b.closest(".cli").querySelector("pre");
        var text = pre.innerText.replace(/^\$ /gm, "");
        var done = function () { b.textContent = "скопировано"; setTimeout(function () { b.textContent = "копировать"; }, 1400); };
        if (navigator.clipboard && window.isSecureContext) navigator.clipboard.writeText(text).then(done);
        else { var ta = document.createElement("textarea"); ta.value = text; document.body.appendChild(ta); ta.select();
               try { document.execCommand("copy"); done(); } catch (e) {} document.body.removeChild(ta); }
      });
    });

    // оглавление «На этой странице» + подсветка текущего раздела
    var toc = document.getElementById("toc-list"), heads = document.querySelectorAll("main h2[id]");
    heads.forEach(function (h) { var a = document.createElement("a"); a.href = "#" + h.id; a.textContent = h.dataset.toc || h.textContent; toc && toc.appendChild(a); });
    var links = toc ? toc.querySelectorAll("a") : [];
    if ("IntersectionObserver" in window && links.length) {
      var obs = new IntersectionObserver(function (es) {
        es.forEach(function (e) { if (e.isIntersecting) links.forEach(function (l) { l.classList.toggle("on", l.getAttribute("href") === "#" + e.target.id); }); });
      }, { rootMargin: "-70px 0px -70% 0px" });
      heads.forEach(function (h) { obs.observe(h); });
    }

    // поиск по заголовкам всех трёх страниц (индекс встроен при сборке)
    var q = document.getElementById("q"), box = document.getElementById("q-res"), idx = window.SEARCH_INDEX || [];
    if (q) q.addEventListener("input", function () {
      var v = q.value.trim().toLowerCase(); box.innerHTML = "";
      if (v.length < 2) { box.style.display = "none"; return; }
      var hits = idx.filter(function (x) { return (x.t + " " + x.k).toLowerCase().indexOf(v) >= 0; }).slice(0, 12);
      hits.forEach(function (x) { var a = document.createElement("a"); a.href = x.u; a.innerHTML = ""; a.textContent = x.t;
        var s = document.createElement("small"); s.textContent = x.p; a.appendChild(s); box.appendChild(a); });
      if (!hits.length) { var n = document.createElement("a"); n.textContent = "ничего не найдено"; box.appendChild(n); }
      box.style.display = "block";
    });
  });
})();
