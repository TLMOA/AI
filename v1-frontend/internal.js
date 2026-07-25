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
    backendMode: "",
    currentUsername: "",       // v4: 当前选中的用户
    currentUserMode: "public",  // v4: 当前用户的部署模式
  };

  const BACKEND_MODE_STORAGE_KEY = "iot.backend.mode";

  function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = text == null ? "" : String(text);
    return div.innerHTML;
  }

  function getRelativeApiBase() {
    try {
      const url = new URL("./api/v1/", window.location.href);
      return url.pathname.replace(/\/$/, "");
    } catch (_) {
      return "/api/v1";
    }
  }

  function getConfiguredBackendMode() {
    const fromStorage = window.localStorage.getItem(BACKEND_MODE_STORAGE_KEY);
    const fallback = String(config.DEFAULT_BACKEND_MODE || "local").toLowerCase();
    const normalized = String(fromStorage || fallback || "local").toLowerCase();
    return normalized === "nifi" ? "nifi" : "local";
  }

  function getBackendModeMeta(mode) {
    const modes = config.BACKEND_MODES || {};
    const normalized = mode === "nifi" ? "nifi" : "local";
    return modes[normalized] || { label: normalized === "nifi" ? "NiFi" : "Local", apiBase: config.API_BASE || "api/v1" };
  }

  function getApiBaseCandidates() {
    const configuredMode = getConfiguredBackendMode();
    const backendMeta = getBackendModeMeta(configuredMode);
    const configured = String(backendMeta.apiBase || config.API_BASE || "").trim();
    const normalizedConfigured = configured
      ? (configured.startsWith("/") ? configured : `/${configured}`)
      : "";
    const pageRelative = getRelativeApiBase();
    const localHosts = new Set(["localhost", "127.0.0.1", "::1"]);
    const isLocalPage = localHosts.has(String(window.location.hostname || "").toLowerCase());
    const candidates = [
      normalizedConfigured,
      pageRelative,
      "/api/v1",
      "/api",
    ];
    if (isLocalPage) {
      candidates.push("http://127.0.0.1:8081/api/v1", "http://localhost:8081/api/v1",
                      "http://127.0.0.1:8081/api", "http://localhost:8081/api");
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
          // v4: 401/403 自动跳登录（除登录/登出/me 自身外）
          const skipPaths = ["/api/v1/auth/login", "/api/v1/auth/register", "/api/v1/auth/logout", "/api/v1/auth/me"];
          if ((res.status === 401 || res.status === 403) && !skipPaths.some((p) => suffix.startsWith(p))) {
            const next = encodeURIComponent(window.location.pathname + window.location.search);
            window.location.href = "/login.html?next=" + next;
            return { code: res.status, message: lastErr, data: null };
          }
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

  // v4: 从 userSelect 获取当前选中的用户名
  function getSelectedUsername() {
    const sel = document.getElementById("userSelect");
    return (sel && sel.value) || "";
  }

  function inferUsername() {
    // v4: 优先使用 userSelect 的值
    const sel = getSelectedUsername();
    if (sel) return sel;
    // 回退到当前登录用户
    try {
      const u = (window.currentUser && window.currentUser.username) || (window.CURRENT_USER && window.CURRENT_USER.username);
      if (u) return u;
    } catch (e) {}
    return "user";
  }

  function setStatus(msg, isError) {
    const el = document.getElementById("statusMsg");
    if (!el) return;
    el.textContent = msg || "";
    el.style.color = isError ? "#b91c1c" : "#64748b";
  }

  function updateBackendToggleUI() {
    const localBtn = document.getElementById("backendLocalBtn");
    const nifiBtn = document.getElementById("backendNifiBtn");
    const mode = getConfiguredBackendMode();
    state.backendMode = mode;
    if (localBtn) localBtn.classList.toggle("active", mode === "local");
    if (nifiBtn) nifiBtn.classList.toggle("active", mode === "nifi");
    const meta = getBackendModeMeta(mode);
    setStatus(`当前后端模式：${meta.label}`);
  }

  function setBackendMode(mode) {
    const normalized = mode === "nifi" ? "nifi" : "local";
    window.localStorage.setItem(BACKEND_MODE_STORAGE_KEY, normalized);
    updateBackendToggleUI();
    setStatus(`已切换到 ${getBackendModeMeta(normalized).label}，其余页面无需修改`, false);
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

    // v4: 同时渲染 nifi-data / real_nifi_data 等多个根目录，而不是只显示第一个
    (roots || state.treeRoots || []).forEach((root) => {
      if (root && root.path) {
        renderNode(root.path, 0);
      }
    });
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

  // v4: 加载用户下拉列表
  async function loadUserDropdown() {
    const sel = document.getElementById("userSelect");
    if (!sel) return;
    const res = await callApi("/internal/all-users");
    const users = (res.code === 0 && res.data && res.data.users) ? res.data.users : [];
    sel.innerHTML = '<option value="">请选择用户</option>';
    users.forEach((u) => {
      const modeLabel = u.deployment_mode === "private" ? "私有化" : "公有化";
      const op = document.createElement("option");
      op.value = u.username;
      op.textContent = `${u.username} (${modeLabel})`;
      op.dataset.mode = u.deployment_mode || "public";
      op.dataset.ceph = u.ceph_endpoint || "";
      sel.appendChild(op);
    });
    // 恢复上次选择或默认选第一个
    if (state.currentUsername && sel.querySelector(`option[value="${state.currentUsername}"]`)) {
      sel.value = state.currentUsername;
    } else if (users.length > 0) {
      sel.value = users[0].username;
      state.currentUsername = users[0].username;
    }
    updateUserInfo();
  }

  function updateUserInfo() {
    const sel = document.getElementById("userSelect");
    const info = document.getElementById("userInfo");
    const pullBtn = document.getElementById("pullBtn");
    if (!sel || !info) return;
    const opt = sel.selectedOptions[0];
    if (!opt || !opt.value) {
      info.textContent = "";
      state.currentUsername = "";
      state.currentUserMode = "public";
      if (pullBtn) pullBtn.style.display = "none";
      return;
    }
    state.currentUsername = opt.value;
    state.currentUserMode = opt.dataset.mode || "public";
    const ceph = opt.dataset.ceph || "";
    info.textContent = `当前用户: ${opt.value} | 部署模式: ${state.currentUserMode === "private" ? "私有化" : "公有化"}` + (ceph ? ` | Ceph: ${ceph}` : "");
    if (pullBtn) {
      pullBtn.style.display = state.currentUserMode === "private" ? "inline-block" : "none";
    }
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
          const xattrInput = document.getElementById("xattrSelectedFileId");
          if (xattrInput) xattrInput.value = f.fileId;
        });
      }
      body.appendChild(tr);
    });
  }

  function renderPreviewTable(data, { append = false } = {}) {
    const head = document.getElementById("previewHead");
    const body = document.getElementById("previewBody");
    const raw = document.getElementById("previewRaw");
    if (!append) {
      head.innerHTML = "";
      body.innerHTML = "";
    }

    if (!data || !Array.isArray(data.columns) || !Array.isArray(data.rows)) {
      if (!append) {
        head.innerHTML = "";
        body.innerHTML = "";
        raw.style.display = "block";
        raw.textContent = JSON.stringify(data || {}, null, 2);
      }
      return;
    }

    // 表格数据：<pre> 仅展示前 20 条 JSON 行；下方表格完整渲染并支持 "下一批"
    const PREVIEW_JSON_LIMIT = 20;
    if (!append) {
      raw.style.display = "block";
      const sliced = {
        columns: data.columns,
        rows: data.rows.slice(0, PREVIEW_JSON_LIMIT),
        total: (typeof data.total === "number") ? data.total : data.rows.length,
        meta: data.meta,
        _previewNote: `（仅展示前 ${PREVIEW_JSON_LIMIT} 条；下方表格为完整预览，点 "下一批" 加载更多）`,
      };
      raw.textContent = JSON.stringify(sliced, null, 2);
    } else {
      raw.style.display = "none";
    }

    if (!append || head.children.length === 0) {
      const trh = document.createElement("tr");
      data.columns.forEach((c) => {
        const th = document.createElement("th");
        th.textContent = c;
        trh.appendChild(th);
      });
      head.appendChild(trh);
    }

    data.rows.forEach((row) => {
      const tr = document.createElement("tr");
      row.forEach((cell) => {
        const td = document.createElement("td");
        td.textContent = cell == null ? "" : String(cell);
        tr.appendChild(td);
      });
      body.appendChild(tr);
    });
  }

  async function manualSync() {
    const btn = document.getElementById("syncBtn");
    if (!btn) return;
    try {
      btn.disabled = true;
      setStatus("正在刷新同步 nifi_data...", false);
      const payload = { username: inferUsername() };
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
          body: JSON.stringify({ username: payload.username, path: "/home/yhz/nifi-data" }),
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

  async function loadSilentExportStatus() {
    const tenant = inferFactoryId();
    const res = await callApi(`/internal/tenants/${encodeURIComponent(tenant)}/silent-export`);
    const toggle = document.getElementById('silentExportToggle');
    const triggerBtn = document.getElementById('silentExportTrigger');
    if (!toggle || !triggerBtn) return;
    if (!res || res.code !== 0) {
      toggle.checked = false;
      toggle.disabled = true;
      triggerBtn.disabled = true;
      console.warn('[silent-export] status load failed:', JSON.stringify(res));
      return;
    }
    const cfg = res.data || {};
    toggle.checked = !!cfg.enabled;
    toggle.disabled = false;
    triggerBtn.disabled = !cfg.enabled;
  }

  async function setSilentExport(enabled) {
    const tenant = inferFactoryId();
    const payload = { enabled };
    const res = await callApi(`/internal/tenants/${encodeURIComponent(tenant)}/silent-export`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res || res.code !== 0) {
      setStatus(`操作失败: ${res && res.message ? res.message : 'unknown'}`, true);
      return false;
    }
    setStatus(`Silent Export 已 ${enabled ? '启用' : '禁用'}`, false);
    return true;
  }

  async function listSilentExportManifest() {
    const tenant = inferFactoryId();
    const panel = document.getElementById('silentExportManifestPanel');
    if (panel) panel.style.display = 'block';
    setStatus('正在查询已注册表...', false);
    const res = await callApi(`/internal/tenants/${encodeURIComponent(tenant)}/silent-export/manifest`);
    if (!res || res.code !== 0) {
      setStatus(`查询失败: ${res && res.message ? res.message : 'unknown'}`, true);
      if (panel) panel.textContent = '查询失败';
      return;
    }
    const data = res.data || {};
    const list = Array.isArray(data.registered_tables) ? data.registered_tables : [];
    if (panel) {
      if (list.length === 0) {
        panel.innerHTML = `<div style="color:#b91c1c;">该租户 <code>${tenant}</code> 暂无已注册表。` +
          `请先在「数据库导出」中成功导出一次，系统会自动把表注册到静默导出清单中，之后立即触发才能真正生成文件。</div>`;
      } else {
        panel.innerHTML = `<div>租户 <code>${tenant}</code> 已注册 ${list.length} 张表，落盘目录：<code>${data.output_dir}</code></div>` +
          '<ul style="margin:6px 0 0 18px;">' +
          list.map(t => `<li><code>${t.db}.${t.table}</code> <span class="muted">注册于 ${t.registered_at || '-'}</span></li>`).join('') +
          '</ul>';
      }
    }
    setStatus(`已注册表: ${list.length} 张`, list.length === 0);
  }

  async function triggerSilentExport() {
    const confirmOk = window.confirm('确认要对当前工厂立即触发一次静默导出吗？此操作仅限管理员用途。');
    if (!confirmOk) return;
    const tenant = inferFactoryId();
    setStatus('正在触发静默导出...', false);
    const res = await callApi(`/internal/tenants/${encodeURIComponent(tenant)}/silent-export/trigger`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ operator: 'web-admin' }),
    });
    if (!res || res.code !== 0) {
      setStatus(`触发失败: ${res && res.message ? res.message : 'unknown'}`, true);
      return;
    }
    const data = res.data || {};
    const regCount = data.registered_count || 0;
    const regList = Array.isArray(data.registered_tables) ? data.registered_tables : [];
    let msg = `静默导出已触发（后台执行）。已注册表: ${regCount} 张`;
    if (regList.length > 0) {
      msg += `（${regList.map(x => `${x.db}.${x.table}`).join(', ')}）`;
    }
    if (data.hint) msg += ` ｜ ${data.hint}`;
    setStatus(msg, regCount === 0);
    // 静默导出落盘目录提示
    setStatus(`${msg} | 落盘目录: 用户 silent_exports/${tenant}/<db_key>/`, regCount === 0);
  }

  async function loadTree() {
    setStatus("加载目录树中...");
    const uname = inferUsername();
    if (!uname) {
      setStatus("请先选择用户", true);
      state.treeRoots = [];
      renderTree([]);
      return false;
    }
    const res = await callApi(`/internal/factory-tree?depth=6&username=${encodeURIComponent(uname)}`);
    if (res.code !== 0) {
      setStatus(`目录树加载失败: ${res.message || "unknown"}`, true);
      return false;
    }
    const roots = (res.data && Array.isArray(res.data.roots)) ? res.data.roots : [];
    if (!roots.length) {
      setStatus("目录树为空", true);
      state.treeRoots = [];
      renderTree([]);
      return false;
    }
    state.treeRoots = roots;
    buildTreeState(roots);
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
    const q = new URLSearchParams({
      path: state.selectedPath,
      username: inferUsername(),
      offset: String(state.fileOffset || 0),
      limit: String(state.fileLimit || 20),
    });
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
      setStatus("当前路径暂无文件（用户目录为空或尚未同步）", false);
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
    return _loadPreview(fileId, { append: false });
  }

  async function loadMorePreview() {
    if (!state.selectedFileId) return;
    return _loadPreview(state.selectedFileId, { append: true });
  }

  async function _loadPreview(fileId, { append = false } = {}) {
    const limit = Number(document.getElementById("previewLimit").value || "100");
    const offset = append
      ? (state.previewOffset || 0) + limit
      : Number(document.getElementById("previewOffset").value || "0");
    state.previewOffset = offset;
    const delimiter = encodeURIComponent(document.getElementById('delimiterSelect').value || ',');
    const encoding = encodeURIComponent(document.getElementById('encodingSelect').value || 'utf-8');
    const formatHint = (state.selectedFileMeta && state.selectedFileMeta.fileFormat) ? state.selectedFileMeta.fileFormat : '';
    const qs = `offset=${offset}&limit=${limit}&delimiter=${delimiter}&encoding=${encoding}&format=${encodeURIComponent(formatHint)}`;
    const res = await callApi(`/files/${fileId}/preview?${qs}`);
    const head = document.getElementById("previewHead");
    const body = document.getElementById("previewBody");
    const raw = document.getElementById("previewRaw");
    const progressEl = document.getElementById("previewProgress");
    const loadMoreBtn = document.getElementById("loadMorePreviewBtn");
    if (res.code !== 0) {
      raw.textContent = `预览失败: ${res.message || "unknown"}`;
      head.innerHTML = "";
      body.innerHTML = "";
      raw.style.display = "block";
      if (progressEl) progressEl.textContent = "";
      if (loadMoreBtn) loadMoreBtn.style.display = "none";
      return;
    }
    renderPreviewTable(res.data || {}, { append });
    // 更新进度
    const data = res.data || {};
    const isTabular = Array.isArray(data.columns) && Array.isArray(data.rows);
    if (isTabular) {
      const total = (typeof data.total === "number") ? data.total : 0;
      const loaded = offset + data.rows.length;
      if (progressEl) progressEl.textContent = `已加载 ${loaded} / ${total}`;
      if (loadMoreBtn) loadMoreBtn.style.display = (total > 0 && loaded < total) ? "" : "none";
    } else {
      if (progressEl) progressEl.textContent = "";
      if (loadMoreBtn) loadMoreBtn.style.display = "none";
    }
  }

  async function loadXattrMeta() {
    const fileId = document.getElementById("xattrSelectedFileId")?.value || state.selectedFileId;
    const contentEl = document.getElementById("xattrMetaContent");
    const statusEl = document.getElementById("xattrStatus");
    if (!fileId) {
      if (contentEl) contentEl.innerHTML = '<div class="muted">请先在文件列表点击“预览”选择一个文件。</div>';
      return;
    }
    if (contentEl) contentEl.innerHTML = '<div class="muted">加载中...</div>';
    if (statusEl) statusEl.textContent = "";
    try {
      const res = await callApi(`/files/${fileId}/xattr`);
      if (res.code !== 0) {
        if (contentEl) contentEl.innerHTML = `<div class="error">加载失败: ${escapeHtml(res.message || '未知错误')}</div>`;
        return;
      }
      const meta = res.data && res.data.meta ? res.data.meta : {};
      const xattrEnabled = res.data && res.data.xattrEnabled;
      const html = `
        <div class="small" style="margin-bottom:8px;">
          <strong>xattr 状态:</strong> ${xattrEnabled ? '<span style="color:#16a34a;">已启用</span>' : '<span style="color:#dc2626;">未启用</span>'}
          &nbsp;|&nbsp; <strong>存储路径:</strong> ${escapeHtml(res.data.storagePath || '')}
        </div>
        <pre style="white-space:pre-wrap;word-break:break-word;">${escapeHtml(JSON.stringify(meta, null, 2))}</pre>
      `;
      if (contentEl) contentEl.innerHTML = html;
      if (statusEl) statusEl.textContent = `已读取扩展属性 user.meta（${Object.keys(meta).length} 个字段）`;
    } catch (e) {
      if (contentEl) contentEl.innerHTML = `<div class="error">请求异常: ${escapeHtml(String(e))}</div>`;
    }
  }

  // === v4: 用户管理 ===
  async function renderUserManagement() {
    const status = document.getElementById("userMgmtStatus");
    const body = document.getElementById("userMgmtBody");
    if (!body) return;
    status.textContent = "加载中...";
    const res = await callApi("/internal/users");
    const users = (res.code === 0 && res.data && res.data.users) ? res.data.users : [];
    if (!users.length) {
      body.innerHTML = '<tr><td colspan="6" style="text-align:center;">暂无用户</td></tr>';
      status.textContent = "";
      return;
    }
    body.innerHTML = users.map((u) => {
      const modeLabel = u.deployment_mode === "private" ? '<span style="color:#d97706;">私有化</span>' : '<span style="color:#059669;">公有化</span>';
      return `<tr>
        <td>${u.username}</td>
        <td>${u.is_admin ? "✓" : ""}</td>
        <td>${modeLabel}</td>
        <td>${u.ceph_endpoint || "-"}</td>
        <td>${u.created_at ? u.created_at.slice(0, 10) : "-"}</td>
        <td>
          <button class="secondary" data-edit-username="${u.username}" data-edit-mode="${u.deployment_mode || 'public'}" data-edit-ceph="${u.ceph_endpoint || ''}">编辑</button>
          ${u.username !== 'admin' ? `<button class="secondary" data-delete-username="${u.username}" style="color:#b91c1c;">删除</button>` : ''}
        </td>
      </tr>`;
    }).join("");
    status.textContent = "共 " + users.length + " 个用户";
    body.querySelectorAll('[data-edit-username]').forEach((btn) => {
      btn.addEventListener('click', () => openEditDeployment(btn.dataset.editUsername, btn.dataset.editMode, btn.dataset.editCeph));
    });
    body.querySelectorAll('[data-delete-username]').forEach((btn) => {
      btn.addEventListener('click', () => deleteUser(btn.dataset.deleteUsername));
    });
  }

  function openEditDeployment(username, mode, ceph) {
    const modal = document.getElementById("editDeploymentModal");
    const uname = document.getElementById("editDeployUsername");
    const saveBtn = document.getElementById("editDeploySave");
    const cancelBtn = document.getElementById("editDeployCancel");
    const err = document.getElementById("editDeployError");
    if (!modal) return;
    uname.value = username;
    document.querySelector('input[name="editDeployMode"][value="' + mode + '"]').checked = true;
    document.getElementById("editCephEndpoint").value = ceph || "";
    document.getElementById("editCephGroup").style.display = mode === "private" ? "block" : "none";
    modal.style.display = "flex";
    err.style.display = "none";
    document.querySelectorAll('input[name="editDeployMode"]').forEach((el) => {
      el.onchange = () => {
        document.getElementById("editCephGroup").style.display = el.value === "private" ? "block" : "none";
      };
    });
    saveBtn.onclick = async () => {
      const newMode = document.querySelector('input[name="editDeployMode"]:checked').value;
      const newCeph = document.getElementById("editCephEndpoint").value.trim();
      if (newMode === "private" && !newCeph) {
        err.textContent = "私有化部署必须填写 Ceph 路径";
        err.style.display = "block";
        return;
      }
      const res = await callApi("/internal/users/" + username + "/deployment", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ deployment_mode: newMode, ceph_endpoint: newCeph }),
      });
      if (res.code !== 0) {
        err.textContent = res.message || "修改失败";
        err.style.display = "block";
        return;
      }
      modal.style.display = "none";
      setStatus("用户 " + username + " 部署模式已更新", false);
      await loadUserDropdown();
      await renderUserManagement();
    };
    cancelBtn.onclick = () => { modal.style.display = "none"; };
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.style.display = "none"; });
  }

  async function deleteUser(username) {
    if (!confirm("确定删除用户 " + username + "？\n此操作将同时删除该用户的用户目录和所有关联数据。")) return;
    const res = await callApi("/internal/users/" + username, {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ purge_data: true }),
    });
    if (res.code !== 0) {
      setStatus("删除失败: " + (res.message || "unknown"), true);
      return;
    }
    setStatus("用户 " + username + " 已删除" + (res.data && res.data.data_purged ? "（含目录）" : ""), false);
    await loadUserDropdown();
    await renderUserManagement();
  }

  // v4: 拉取私有化用户数据
  async function pullUserData() {
    const uname = getSelectedUsername();
    if (!uname) { setStatus("请先选择用户", true); return; }
    setStatus("正在拉取 " + uname + " 的数据...");
    const res = await callApi("/internal/private-users/" + uname + "/pull", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    if (res.code !== 0) {
      setStatus("拉取失败: " + (res.message || "unknown"), true);
      return;
    }
    setStatus(uname + " 数据拉取成功", false);
    await loadTree();
    await loadAssets();
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

    // v4: 用户下拉切换
    const userSelect = document.getElementById("userSelect");
    if (userSelect) {
      userSelect.addEventListener("change", async () => {
        updateUserInfo();
        state.selectedPath = "";
        state.fileOffset = 0;
        const ok = await loadTree();
        if (ok) await loadAssets();
        try { await loadSilentExportStatus(); } catch (e) {}
      });
    }

    // v4: 拉取按钮
    const pullBtn = document.getElementById("pullBtn");
    if (pullBtn) {
      pullBtn.addEventListener("click", () => pullUserData());
    }

    // v4: 用户管理
    const userMgmtRefreshBtn = document.getElementById("userMgmtRefreshBtn");
    if (userMgmtRefreshBtn) {
      userMgmtRefreshBtn.addEventListener("click", () => renderUserManagement());
    }

    // v4: 退出登录
    const logoutBtn = document.getElementById("logoutBtn");
    if (logoutBtn) logoutBtn.addEventListener("click", doLogout);

    if (syncBtn) {
      syncBtn.addEventListener("click", async () => {
        await manualSync();
      });
    }

    const silentToggle = document.getElementById('silentExportToggle');
    const silentTrigger = document.getElementById('silentExportTrigger');
    if (silentToggle) {
      silentToggle.addEventListener('change', async (e) => {
        const checked = !!e.target.checked;
        silentToggle.disabled = true;
        const ok = await setSilentExport(checked);
        if (!ok) {
          // revert
          silentToggle.checked = !checked;
        } else {
          if (silentTrigger) silentTrigger.disabled = !checked;
        }
        silentToggle.disabled = false;
      });
    }
    if (silentTrigger) {
      silentTrigger.addEventListener('click', async () => {
        await triggerSilentExport();
      });
    }

    const listBtn = document.getElementById('silentExportListBtn');
    if (listBtn) {
      listBtn.addEventListener('click', async () => {
        await listSilentExportManifest();
      });
    }

    reloadPreviewBtn.addEventListener("click", () => {
      if (state.selectedFileId) previewFile(state.selectedFileId);
    });

    const loadMorePreviewBtn = document.getElementById("loadMorePreviewBtn");
    if (loadMorePreviewBtn) {
      loadMorePreviewBtn.addEventListener("click", () => {
        if (state.selectedFileId) loadMorePreview();
      });
    }

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
      encodingSelect.addEventListener("change", () => {
        // keep selection for preview
      });
    }

    // v4: 文件元数据（xattr）
    const xattrLoadBtn = document.getElementById("xattrLoadBtn");
    if (xattrLoadBtn) {
      xattrLoadBtn.addEventListener("click", () => loadXattrMeta());
    }
  }

// === v4: 登录相关辅助 ===
async function doLogout() {
  if (!confirm("确定退出登录？")) return;
  try {
    await fetch("/api/v1/auth/logout", { method: "POST", credentials: "include" });
  } catch (e) { /* ignore */ }
  try { localStorage.removeItem(BACKEND_MODE_STORAGE_KEY); } catch (_) {}
  window.location.href = "/login.html";
}
window.doLogout = doLogout;

function renderCurrentUserLabel() {
  const el = document.getElementById("currentUserLabel");
  if (!el) return;
  const u = window.currentUser || (state && state.currentUser);
  if (u && u.username) {
    const adminTag = u.is_admin ? " (管理员)" : "";
    el.textContent = "当前用户：" + u.username + adminTag;
  } else {
    el.textContent = "";
  }
}

// v4: 会话过期提醒（过期前 5 分钟弹窗）
let _sessionWarningTimer = null;
let _sessionWarningShown = false;

function setupSessionExpiryWarning() {
  if (_sessionWarningTimer) {
    clearTimeout(_sessionWarningTimer);
    _sessionWarningTimer = null;
  }
  _sessionWarningShown = false;
  const expiresAt = (state && state.sessionExpiresAt);
  if (!expiresAt) return;
  const nowSec = Math.floor(Date.now() / 1000);
  const remainingSec = expiresAt - nowSec;
  const WARNING_BEFORE_SEC = 5 * 60;
  if (remainingSec <= WARNING_BEFORE_SEC) {
    showSessionExpiryWarning();
    return;
  }
  const delayMs = (remainingSec - WARNING_BEFORE_SEC) * 1000;
  _sessionWarningTimer = setTimeout(() => {
    showSessionExpiryWarning();
  }, delayMs);
}

function showSessionExpiryWarning() {
  if (_sessionWarningShown) return;
  _sessionWarningShown = true;
  const ok = confirm("会话即将过期（5 分钟内），是否延长会话？\n\n点击「确定」延长 15 分钟，点击「取消」将跳转到登录页。");
  if (ok) {
    refreshSession();
  } else {
    window.location.href = "/login.html";
  }
}

async function refreshSession() {
  try {
    const resp = await fetch("/api/v1/auth/refresh", { method: "POST", credentials: "include" });
    if (resp.ok) {
      const j = await resp.json().catch(() => null);
      if (j && j.expires_at) {
        state.sessionExpiresAt = j.expires_at;
      }
      _sessionWarningShown = false;
      setupSessionExpiryWarning();
    } else {
      window.location.href = "/login.html";
    }
  } catch (e) {
    window.location.href = "/login.html";
  }
}

  async function init() {
    // v4: 通过 /api/v1/auth/me 验证登录态；access_token 是 HttpOnly cookie，
    // JavaScript 无法读取 document.cookie，必须依赖后端接口判断。
    try {
      const me = await fetch("/api/v1/auth/me", { credentials: "include" });
      if (!me.ok) {
        window.location.href = "/login.html?next=" + encodeURIComponent(window.location.pathname + window.location.search);
        return;
      }
      const meJson = await me.json().catch(() => null);
      if (meJson && (meJson.user || meJson.username)) {
        window.currentUser = meJson.user || { username: meJson.username, is_admin: !!meJson.is_admin };
        state.currentUser = window.currentUser;
        // v4: 存储 token 过期时间，用于会话过期提醒
        if (meJson.user && meJson.user.expires_at) {
          state.sessionExpiresAt = meJson.user.expires_at;
        }
        try { renderCurrentUserLabel(); } catch (e) {}
      } else {
        window.location.href = "/login.html?next=" + encodeURIComponent(window.location.pathname + window.location.search);
        return;
      }
    } catch (e) {
      window.location.href = "/login.html?next=" + encodeURIComponent(window.location.pathname + window.location.search);
      return;
    }

    // v4: 设置会话过期提醒
    setupSessionExpiryWarning();

    bindEvents();
    // v4: 先加载用户下拉，再加载树
    await loadUserDropdown();
    const ok = await loadTree();
    if (ok) await loadAssets();
    // load silent export status for selected tenant
    try { await loadSilentExportStatus(); } catch (e) { /* ignore */ }
    // v4: 加载用户管理列表
    try { await renderUserManagement(); } catch (e) { /* ignore */ }
  }

  init();
})();
