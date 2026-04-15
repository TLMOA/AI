(function () {
  const config = window.APP_CONFIG || {};
  const state = {
    treeRoots: [],
    selectedPath: "",
    selectedFileId: "",
    selectedFileMeta: null,
    fileOffset: 0,
    fileLimit: 20,
    fileTotal: 0,
    treeNodeMap: {},
    treeChildren: {},
    treeRootPath: "",
    expandedDirs: {},
  };

  function getRelativeApiBase() {
    try {
      const url = new URL("./api/v1/", window.location.href);
      return url.pathname.replace(/\/$/, "");
    } catch (_) {
      return "/api/v1";
    }
  }

  function getApiBaseCandidates() {
    const configured = String(config.API_BASE || "").trim();
    const normalizedConfigured = configured
      ? (configured.startsWith("/") ? configured : `/${configured}`)
      : "";
    const pageRelative = getRelativeApiBase();
    const localHosts = new Set(["localhost", "127.0.0.1", "::1"]);
    const isLocalPage = localHosts.has(String(window.location.hostname || "").toLowerCase());
    const candidates = [
      "/api/v1",
      pageRelative,
      normalizedConfigured,
    ];
    if (isLocalPage) {
      candidates.push("http://127.0.0.1:8081/api/v1", "http://localhost:8081/api/v1");
    }
    return [...new Set(candidates.filter(Boolean))];
  }

  function isValidApiBody(body) {
    return !!(body && typeof body === "object" && Object.prototype.hasOwnProperty.call(body, "code"));
  }

  async function callApi(path, options) {
    const suffix = path.startsWith("/") ? path : `/${path}`;
    let lastErr = "";
    const errors = [];
    for (const base of getApiBaseCandidates()) {
      try {
        const prefix = base.endsWith("/") ? base.slice(0, -1) : base;
        const res = await fetch(`${prefix}${suffix}`, options || {});
        const text = await res.text();
        let body = {};
        try {
          body = text ? JSON.parse(text) : {};
        } catch (_) {
          body = { raw: text };
        }
        if (!res.ok) {
          lastErr = body.detail || body.message || `HTTP ${res.status}`;
          errors.push(`${base}: ${lastErr}`);
          continue;
        }
        if (!isValidApiBody(body)) {
          lastErr = "invalid api response";
          errors.push(`${base}: ${lastErr}`);
          continue;
        }
        return body;
      } catch (e) {
        lastErr = e && e.message ? e.message : String(e);
        errors.push(`${base}: ${lastErr}`);
      }
    }
    const merged = errors.slice(0, 3).join(" | ");
    return { code: -1, message: merged || lastErr || "request failed", data: null };
  }

  function inferFactoryId() {
    const sel = document.getElementById("factorySelect");
    const v = sel && sel.value ? String(sel.value).replace(/\\/g, "/") : "";
    if (!v) return "factory-001";
    const marker = "/in_data/";
    const idx = v.indexOf(marker);
    if (idx >= 0) {
      const part = v.slice(idx + marker.length).split("/")[0];
      if (part) return part;
    }
    const tokens = v.split("/").filter(Boolean);
    return tokens[tokens.length - 1] || "factory-001";
  }

  function setStatus(msg, isError) {
    const el = document.getElementById("statusMsg");
    if (!el) return;
    el.textContent = msg || "";
    el.style.color = isError ? "#b91c1c" : "#64748b";
  }

  function renderTree(roots) {
    const box = document.getElementById("treeBox");
    if (!box) return;
    box.innerHTML = "";
    if (!state.treeRootPath || !state.treeNodeMap[state.treeRootPath]) {
      box.textContent = "当前没有可展示目录";
      return;
    }

    const renderNode = (dirPath, depth) => {
      const row = document.createElement("div");
      row.className = "tree-row";
      row.style.marginLeft = `${depth * 12}px`;
      const children = state.treeChildren[dirPath] || [];
      const hasChildren = children.length > 0;
      const isExpanded = state.expandedDirs[dirPath] !== false;

      const toggle = document.createElement("button");
      toggle.className = "tree-toggle";
      toggle.textContent = hasChildren ? (isExpanded ? "▼" : "▶") : "•";
      toggle.disabled = !hasChildren;
      if (hasChildren) {
        toggle.addEventListener("click", () => {
          state.expandedDirs[dirPath] = !isExpanded;
          renderTree(state.treeRoots);
        });
      }

      const btn = document.createElement("button");
      btn.className = "tree-dir-btn";
      const node = state.treeNodeMap[dirPath] || {};
      btn.textContent = node.label || node.name || (dirPath.split("/").pop() || dirPath);
      if (state.selectedPath === dirPath) {
        btn.classList.add("current");
      }
      btn.title = dirPath;
      btn.addEventListener("click", async () => {
        state.selectedPath = dirPath;
        state.fileOffset = 0;
        renderTree(state.treeRoots);
        await loadAssets();
      });
      btn.addEventListener("dblclick", async () => {
        state.selectedPath = dirPath;
        state.fileOffset = 0;
        renderTree(state.treeRoots);
        await loadAssets();
      });
      btn.addEventListener("contextmenu", (ev) => {
        ev.preventDefault();
        const choice = window.prompt(`操作: 1=刷新目录,2=复制路径\n输入序号:`);
        if (choice === "1") {
          loadAssets();
        } else if (choice === "2") {
          try {
            if (navigator && navigator.clipboard && navigator.clipboard.writeText) {
              navigator.clipboard.writeText(dirPath);
              setStatus("路径已复制到剪贴板");
            } else {
              window.prompt("请手动复制路径:", dirPath);
            }
          } catch (e) {
            window.prompt("请手动复制路径:", dirPath);
          }
        }
      });

      row.appendChild(toggle);
      row.appendChild(btn);
      box.appendChild(row);
      if (isExpanded) {
        children.forEach((childPath) => renderNode(childPath, depth + 1));
      }
    };

    renderNode(state.treeRootPath, 0);
  }

  function buildTreeState(roots) {
    const nodeMap = {};
    const children = {};
    const allNodes = [];
    const walk = (node, parentPath) => {
      if (!node || !node.path) return;
      const path = String(node.path);
      nodeMap[path] = node;
      allNodes.push(path);
      if (!children[path]) children[path] = [];
      if (parentPath) {
        if (!children[parentPath]) children[parentPath] = [];
        children[parentPath].push(path);
      }
      if (Array.isArray(node.children)) {
        node.children.forEach((child) => walk(child, path));
      }
    };

    (roots || []).forEach((r) => walk(r, ""));
    Object.keys(children).forEach((k) => {
      children[k] = Array.from(new Set(children[k])).sort((a, b) => a.localeCompare(b));
    });

    state.treeNodeMap = nodeMap;
    state.treeChildren = children;
    state.treeRootPath = (roots && roots[0] && roots[0].path) ? roots[0].path : "";

    allNodes.forEach((p) => {
      if (typeof state.expandedDirs[p] !== "boolean") {
        state.expandedDirs[p] = true;
      }
    });
  }

  function getFactoryNodes(roots) {
    const list = Array.isArray(roots) ? roots : [];
    if (!list.length) return [];
    if (list.length === 1) {
      const root = list[0];
      if (Array.isArray(root.children) && root.children.length) {
        return root.children.filter((x) => x && x.path);
      }
      return [root].filter((x) => x && x.path);
    }
    return list.filter((x) => x && x.path);
  }

  function renderFactorySelect(roots) {
    const sel = document.getElementById("factorySelect");
    sel.innerHTML = "";
    const factories = getFactoryNodes(roots);
    factories.forEach((f) => {
      const op = document.createElement("option");
      op.value = f.path;
      op.textContent = f.label || f.name || f.path;
      sel.appendChild(op);
    });

    const validValues = factories.map((f) => f.path);
    if (!state.selectedPath || !validValues.includes(state.selectedPath)) {
      state.selectedPath = validValues[0] || "";
    }
    if (state.selectedPath) {
      sel.value = state.selectedPath;
    } else if (factories.length > 0) {
      sel.selectedIndex = 0;
      state.selectedPath = sel.value;
    }

    sel.onchange = async () => {
      state.selectedPath = sel.value;
      state.fileOffset = 0;
      renderTree(state.treeRoots);
      await loadAssets();
    };
  }

  function humanSize(n) {
    const v = Number(n || 0);
    if (v < 1024) return `${v} B`;
    if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB`;
    return `${(v / (1024 * 1024)).toFixed(1)} MB`;
  }

  function renderFiles(items) {
    const body = document.getElementById("fileTable") || document.getElementById("fileBody");
    body.innerHTML = "";
    const apiBase = getApiBaseCandidates()[0] || "/api/v1";
    items.forEach((f) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${f.fileId || "-"}</td>
        <td>${f.fileName || "-"}</td>
        <td>${(f.fileFormat || "-").toUpperCase()}</td>
        <td>${humanSize(f.fileSize)}</td>
        <td>
          <button class="secondary" data-preview="${f.fileId || ""}">预览</button>
          ${f.fileId ? `<a href="${apiBase}/files/${f.fileId}/download" target="_blank" rel="noopener">下载</a>` : ""}
        </td>
      `;
      const btn = tr.querySelector("button[data-preview]");
      if (btn) {
        btn.addEventListener("click", () => {
          if (!f.fileId) return;
          state.selectedFileId = f.fileId;
          state.selectedFileMeta = f;
          previewFile(f.fileId);
        });
      }
      body.appendChild(tr);
    });
  }

  function renderPreviewTable(data) {
    const head = document.getElementById("previewHead");
    const body = document.getElementById("previewBody");
    const raw = document.getElementById("previewRaw");
    head.innerHTML = "";
    body.innerHTML = "";

    if (!data || !Array.isArray(data.columns) || !Array.isArray(data.rows)) {
      document.getElementById("previewHead").innerHTML = "";
      document.getElementById("previewBody").innerHTML = "";
      raw.style.display = "block";
      raw.textContent = JSON.stringify(data || {}, null, 2);
      return;
    }

    const trh = document.createElement("tr");
    data.columns.forEach((c) => {
      const th = document.createElement("th");
      th.textContent = c;
      trh.appendChild(th);
    });
    head.appendChild(trh);

    data.rows.forEach((row) => {
      const tr = document.createElement("tr");
      row.forEach((cell) => {
        const td = document.createElement("td");
        td.textContent = cell == null ? "" : String(cell);
        tr.appendChild(td);
      });
      body.appendChild(tr);
    });

    raw.style.display = "none";
    raw.textContent = JSON.stringify({ total: data.total || data.rows.length }, null, 2);
  }

  async function manualSync() {
    const btn = document.getElementById("syncBtn");
    if (!btn) return;
    try {
      btn.disabled = true;
      setStatus("正在刷新同步 nifi_data...", false);
      const payload = { factory_id: inferFactoryId() };
      let res = await callApi("/internal/factory-tree/refresh", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res || res.code !== 0) {
        // backward compatibility: older backend may not provide refresh endpoint
        res = await callApi("/internal/factory-tree/fetch", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
      }
      if (!res || res.code !== 0) {
        // fallback with explicit source path for legacy behavior
        res = await callApi("/internal/factory-tree/fetch", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ factory_id: payload.factory_id, path: "/home/yhz/nifi-data" }),
        });
      }
      if (!res || res.code !== 0) {
        setStatus(`刷新同步失败: ${res && res.message ? res.message : "unknown"}`, true);
        return;
      }
      setStatus("刷新同步完成，正在刷新目录...", false);
      await loadTree();
      await loadAssets();
      setStatus("刷新同步完成并刷新目录", false);
    } catch (e) {
      setStatus(`刷新同步异常: ${e && e.message ? e.message : String(e)}`, true);
    } finally {
      const btn2 = document.getElementById("syncBtn");
      if (btn2) btn2.disabled = false;
    }
  }

  async function loadTree() {
    setStatus("加载目录树中...");
    const res = await callApi("/internal/factory-tree?depth=6");
    if (res.code !== 0) {
      setStatus(`目录树加载失败: ${res.message || "unknown"}`, true);
      return false;
    }
    const roots = (res.data && Array.isArray(res.data.roots)) ? res.data.roots : [];
    if (!roots.length) {
      setStatus("目录树为空", true);
      state.treeRoots = [];
      renderFactorySelect([]);
      renderTree([]);
      return false;
    }
    state.treeRoots = roots;
    buildTreeState(roots);
    renderFactorySelect(roots);
    if (!state.selectedPath) {
      const firstFactory = getFactoryNodes(roots)[0];
      state.selectedPath = firstFactory ? firstFactory.path : (roots[0] && roots[0].path) || "";
    }
    renderTree(roots);
    setStatus("目录树加载成功");
    return true;
  }

  async function loadAssets() {
    if (!state.selectedPath) return;
    const q = new URLSearchParams({ path: state.selectedPath, offset: String(state.fileOffset || 0), limit: String(state.fileLimit || 20) });
    document.getElementById("currentPath").textContent = `当前路径: ${state.selectedPath}`;
    setStatus("加载文件中...");
    const res = await callApi(`/internal/factory-assets?${q.toString()}`);
    if (res.code !== 0) {
      setStatus(`文件加载失败: ${res.message || "unknown"}`, true);
      renderFiles([]);
      return;
    }
    const items = (res.data && res.data.items) || [];
    state.fileTotal = (res.data && typeof res.data.total === 'number') ? res.data.total : (items.length + (state.fileOffset || 0));
    renderFiles(items.filter((x) => x.type !== "dir"));
    updateFilePaginationInfo(items.length);
    if (!items.length) {
      setStatus("当前路径暂无文件（in_data 目录为空或尚未同步）", false);
    } else {
      setStatus(`文件加载完成: ${items.length} 条`);
    }
  }

  function updateFilePaginationInfo(pageCount) {
    const info = document.getElementById('filePageInfo');
    const start = state.fileOffset + 1;
    const end = Math.min(state.fileOffset + pageCount, state.fileTotal || (state.fileOffset + pageCount));
    info.textContent = `显示 ${start}-${end} / ${state.fileTotal || '未知'}`;
  }

  async function previewFile(fileId) {
    const offset = Number(document.getElementById("previewOffset").value || "0");
    const limit = Number(document.getElementById("previewLimit").value || "20");
    const delimiter = encodeURIComponent(document.getElementById('delimiterSelect').value || ',');
    const encoding = encodeURIComponent(document.getElementById('encodingSelect').value || 'utf-8');
    const formatHint = (state.selectedFileMeta && state.selectedFileMeta.fileFormat) ? state.selectedFileMeta.fileFormat : '';
    const qs = `offset=${offset}&limit=${limit}&delimiter=${delimiter}&encoding=${encoding}&format=${encodeURIComponent(formatHint)}`;
    const res = await callApi(`/files/${fileId}/preview?${qs}`);
    if (res.code !== 0) {
      document.getElementById("previewRaw").textContent = `预览失败: ${res.message || "unknown"}`;
      document.getElementById("previewHead").innerHTML = "";
      document.getElementById("previewBody").innerHTML = "";
      document.getElementById('previewRaw').style.display = 'block';
      return;
    }
    renderPreviewTable(res.data || {});
  }

  function bindEvents() {
    const refreshBtn = document.getElementById("refreshBtn");
    const reloadPreviewBtn = document.getElementById("reloadPreviewBtn");
    const syncBtn = document.getElementById("syncBtn");
    const prevPage = document.getElementById('prevPage');
    const nextPage = document.getElementById('nextPage');
    const pageSizeSelect = document.getElementById('pageSizeSelect');
    const downloadFullBtn = document.getElementById('downloadFullBtn');
    const delimiterSelect = document.getElementById('delimiterSelect');
    const encodingSelect = document.getElementById('encodingSelect');

    refreshBtn.addEventListener("click", async () => {
      const ok = await loadTree();
      if (ok) await loadAssets();
    });

    if (syncBtn) {
      syncBtn.addEventListener("click", async () => {
        await manualSync();
      });
    }

    reloadPreviewBtn.addEventListener("click", () => {
      if (state.selectedFileId) previewFile(state.selectedFileId);
    });

    if (prevPage) {
      prevPage.addEventListener('click', async () => {
        state.fileOffset = Math.max(0, (state.fileOffset || 0) - (state.fileLimit || 20));
        await loadAssets();
      });
    }
    if (nextPage) {
      nextPage.addEventListener('click', async () => {
        // allow next even if total unknown
        state.fileOffset = (state.fileOffset || 0) + (state.fileLimit || 20);
        await loadAssets();
      });
    }
    if (pageSizeSelect) {
      pageSizeSelect.addEventListener('change', async (e) => {
        state.fileLimit = Number(e.target.value || 20);
        state.fileOffset = 0;
        await loadAssets();
      });
    }
    if (downloadFullBtn) {
      downloadFullBtn.addEventListener('click', () => {
        if (!state.selectedFileId) return setStatus('请先选择要下载的文件', true);
        const apiBase = getApiBaseCandidates()[0] || '/api/v1';
        window.open(`${apiBase}/files/${state.selectedFileId}/download`, '_blank');
      });
    }
    if (delimiterSelect) {
      delimiterSelect.addEventListener('change', () => {
        // keep selection for preview
      });
    }
    if (encodingSelect) {
      encodingSelect.addEventListener('change', () => {
        // keep selection for preview
      });
    }
  }

  async function init() {
    bindEvents();
    const ok = await loadTree();
    if (ok) await loadAssets();
  }

  init();
})();
