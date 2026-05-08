(function () {
  var year = document.getElementById("shellYear");
  if (year) {
    year.textContent = new Date().getFullYear();
  }

  var btn = document.querySelector(".art-info-bar-btn");
  var bar = document.querySelector(".art-info-bar");
  var curtain = document.getElementById("shellMobileCurtain");
  if (!btn || !bar) return;

  function setOpen(open) {
    bar.classList.toggle("art-active", open);
    btn.classList.toggle("art-active", open);
    btn.setAttribute("aria-expanded", open ? "true" : "false");
    if (curtain) {
      curtain.classList.toggle("is-open", open);
      curtain.setAttribute("aria-hidden", open ? "false" : "true");
    }
  }

  btn.addEventListener("click", function () {
    setOpen(!bar.classList.contains("art-active"));
  });

  if (curtain) {
    curtain.addEventListener("click", function () {
      setOpen(false);
    });
  }
})();

(function () {
  var form = document.getElementById("fs-form");
  if (!form) return;
  var errEl = document.getElementById("fs-error");
  var resultEl = document.getElementById("fs-result");
  var submitBtn = document.getElementById("fs-submit");
  var sampleBtn = document.getElementById("fs-sample-btn");
  var copyCliBtn = document.getElementById("fs-copy-cli");
  if (!errEl || !resultEl || !submitBtn) return;

  if (copyCliBtn) {
    copyCliBtn.addEventListener("click", function () {
      var code = document.querySelector("#fs-cli-default code");
      var text = code ? code.textContent.trim() : "";
      if (!text) return;
      function done(ok) {
        var t = copyCliBtn.textContent;
        copyCliBtn.textContent = ok ? "Copied" : "Copy";
        if (ok) setTimeout(function () { copyCliBtn.textContent = t; }, 1600);
      }
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(function () { done(true); }).catch(function () { done(false); });
      } else {
        done(false);
      }
    });
  }

  function showError(msg) {
    errEl.textContent = msg;
    errEl.classList.remove("fs-hidden");
    resultEl.classList.add("fs-hidden");
  }
  function clearError() {
    errEl.textContent = "";
    errEl.classList.add("fs-hidden");
  }

  function buildTable(rows) {
    if (!rows || !rows.length) return '<p class="fs-muted">No rows</p>';
    var keys = Object.keys(rows[0]);
    var h = '<table class="fs-data-table"><thead><tr>';
    keys.forEach(function (k) { h += "<th>" + escapeHtml(k) + "</th>"; });
    h += "</tr></thead><tbody>";
    rows.forEach(function (row) {
      h += "<tr>";
      keys.forEach(function (k) {
        var v = row[k];
        h += "<td>" + escapeHtml(v === null || v === undefined ? "" : String(v)) + "</td>";
      });
      h += "</tr>";
    });
    h += "</tbody></table>";
    return h;
  }
  function escapeHtml(s) {
    var d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function apiOrigin() {
    var meta = document.querySelector('meta[name="fs-api-origin"]');
    var fromMeta = meta && meta.getAttribute("content") && meta.getAttribute("content").trim();
    if (fromMeta) return fromMeta.replace(/\/$/, "");
    if (window.location.protocol === "file:") return "http://127.0.0.1:8000";
    try {
      return new URL("..", window.location.href).href.replace(/\/$/, "");
    } catch (e) {
      return "";
    }
  }

  function apiUrl(pathWithOptionalQuery) {
    var path = pathWithOptionalQuery.charAt(0) === "/" ? pathWithOptionalQuery.slice(1) : pathWithOptionalQuery;
    var origin = apiOrigin();
    if (!origin) return path;
    return origin + "/" + path;
  }

  function setRunLoading(on, spinner) {
    submitBtn.disabled = on;
    if (sampleBtn) sampleBtn.disabled = on;
    submitBtn.classList.remove("is-loading");
    if (sampleBtn) sampleBtn.classList.remove("is-loading");
    if (on) {
      if (spinner === "sample" && sampleBtn) sampleBtn.classList.add("is-loading");
      else submitBtn.classList.add("is-loading");
    }
    form.setAttribute("aria-busy", on ? "true" : "false");
  }

  function parseFetchResponse(res) {
    var ct = (res.headers.get("content-type") || "").toLowerCase();
    if (ct.indexOf("application/json") !== -1) {
      return res.json().then(function (data) {
        return { ok: res.ok, status: res.status, data: data, raw: null };
      });
    }
    return res.text().then(function (text) {
      return { ok: res.ok, status: res.status, data: null, raw: text };
    });
  }

  function postTransform(file, opts) {
    opts = opts || {};
    var spinner = opts.spinner === "sample" ? "sample" : "submit";
    var strict = document.getElementById("fs-strict").checked;
    var url = apiUrl("/demo/transform" + (strict ? "?strict=true" : ""));
    var fd = new FormData();
    fd.append("file", file);

    setRunLoading(true, spinner);
    fetch(url, { method: "POST", body: fd })
      .then(parseFetchResponse)
      .then(function (r) {
        setRunLoading(false);
        if (r.data === null && r.raw !== null) {
          var snippet = (r.raw || "").slice(0, 400);
          showError("Expected JSON from API but got another response (" + r.status + ").\n\n" + (snippet ? snippet + (r.raw.length > 400 ? "…" : "") : "(empty body)"));
          return;
        }
        if (!r.ok) {
          var d = r.data;
          var msg;
          if (r.status === 422 && d && d.detail && typeof d.detail === "object") {
            msg = JSON.stringify(d.detail, null, 2);
          } else if (d && d.detail !== undefined) {
            msg = typeof d.detail === "string" ? d.detail : JSON.stringify(d.detail, null, 2);
          } else {
            msg = d ? JSON.stringify(d, null, 2) : String(r.raw || "");
          }
          showError("Request failed (" + r.status + "):\n" + msg);
          return;
        }
        var data = r.data;
        document.getElementById("fs-meta").textContent = JSON.stringify(data.meta, null, 2);
        document.getElementById("fs-input").textContent = JSON.stringify(data.input, null, 2);
        document.getElementById("fs-output").textContent = JSON.stringify({
          row_count: data.output.row_count,
          column_count: data.output.column_count,
          id_column: data.output.id_column,
          feature_columns: data.output.feature_columns
        }, null, 2);
        document.getElementById("fs-validation-summary").textContent = JSON.stringify(data.validation.summary, null, 2);
        document.getElementById("fs-feature-cols").textContent = (data.output.feature_columns || []).join(", ");
        var rows = data.preview && data.preview.rows;
        document.getElementById("fs-preview-table").innerHTML = buildTable(rows);
        var cap = document.getElementById("fs-preview-caption");
        if (cap) {
          var n = rows && rows.length ? rows.length : 0;
          var lim = data.preview && data.preview.limit != null ? data.preview.limit : "";
          var cols = data.output && data.output.column_count != null ? data.output.column_count : "";
          cap.textContent = n
            ? "Showing " + n + " row" + (n === 1 ? "" : "s") + (lim !== "" ? " (preview cap " + lim + ")" : "") + (cols !== "" ? " · " + cols + " columns in output" : "") + "."
            : "";
        }

        var badge = document.getElementById("fs-val-badge");
        var allOk = data.validation && data.validation.summary && data.validation.summary.all_ok;
        badge.innerHTML = allOk
          ? '<span class="fs-badge ok">all_ok</span>'
          : '<span class="fs-badge fail">issues</span>';

        resultEl.classList.remove("fs-hidden");
        var reduceMotion = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
        try {
          resultEl.scrollIntoView({ behavior: reduceMotion ? "auto" : "smooth", block: "start" });
        } catch (e) {
          resultEl.scrollIntoView(true);
        }
      })
      .catch(function (err) {
        setRunLoading(false);
        var msg = err.message || String(err);
        if (msg === "Failed to fetch" || (err && err.name === "TypeError")) {
          msg += "\n\nCheck that the API is running and that this page’s fs-api-origin meta points to it (or open the UI from the same host as the API under /layout-shell/).";
        }
        showError(msg);
      });
  }

  if (sampleBtn) {
    sampleBtn.addEventListener("click", function () {
      clearError();
      resultEl.classList.add("fs-hidden");
      var cap = document.getElementById("fs-preview-caption");
      if (cap) cap.textContent = "";
      setRunLoading(true, "sample");
      fetch(apiUrl("/demo/sample-raw.csv"))
        .then(function (res) {
          if (!res.ok) {
            setRunLoading(false);
            showError("Could not load bundled sample (" + res.status + "). Is data/raw/sample_raw.csv deployed with the app?");
            return null;
          }
          return res.blob();
        })
        .then(function (blob) {
          if (!blob) return;
          var file = new File([blob], "sample_raw.csv", { type: "text/csv" });
          postTransform(file, { spinner: "sample" });
        })
        .catch(function (err) {
          setRunLoading(false);
          var msg = err.message || String(err);
          if (msg === "Failed to fetch" || (err && err.name === "TypeError")) {
            msg += "\n\nEnsure the API serves GET /demo/sample-raw.csv and allows requests from this origin (CORS), or open the demo from the API host.";
          }
          showError(msg);
        });
    });
  }

  form.addEventListener("submit", function (e) {
    e.preventDefault();
    clearError();
    resultEl.classList.add("fs-hidden");

    var fileInput = document.getElementById("fs-file");
    if (!fileInput.files || !fileInput.files.length) {
      showError('Choose a CSV file, or use “Try bundled sample”.');
      return;
    }

    postTransform(fileInput.files[0]);
  });
})();
