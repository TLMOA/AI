const config = window.APP_CONFIG;
const DEFAULT_FACTORY_ID = config.FACTORY_ID || "user";  // deprecated — v4 统一使用 username
const DEFAULT_USERNAME = config.DEFAULT_USERNAME || "admin";

function _currentUsername() {
  // v4: 优先取当前登录用户
  try {
    const u = (state && state.currentUser && state.currentUser.username) || (window.currentUser && window.currentUser.username);
    if (u) return u;
  } catch (e) {}
  return DEFAULT_USERNAME;
}

// v4: 已知子目录名（相对路径），不再硬编码绝对路径
const KNOWN_NIFI_SUBDIRS = [
  "output_csv", "output_json", "output_tsv",
  "inbox_csv", "inbox_json", "inbox_tsv",
  "csv_to_json", "json_to_csv", "tsv_to_json",
  "json_to_tsv", "csv_to_tsv", "tsv_to_csv",
  "tagged_output", "exports",
];

const state = {
  allFiles: [],
  files: [],
  rootDir: "",            // 兼容单根场景
  rootDirs: [],           // v4: 多用户根目录列表
  currentDir: "",
  dirChildren: {},
  expandedDirs: {},
  selectedFileId: null,
  previewColumns: [],
  previewRows: [],
  previewOffset: 0,
  tagRules: [],
  currentUser: null,
  backendMode: "local",
  trainingFiles: [],
  trainingTaskPollTimer: null,
  currentTrainingTaskId: null,
};

function escapeHtml(text) {
  const div = document.createElement("div");
  div.textContent = text == null ? "" : String(text);
  return div.innerHTML;
}

const BACKEND_MODE_STORAGE_KEY = "iot.backend.mode";

const ERROR_HINTS = {
  NIFI_AUTH_ERROR: "NiFi 鉴权失败，请检查用户名/密码或权限配置。",
  NIFI_NETWORK_ERROR: "NiFi 网络连接异常，请检查地址、DNS 与端口连通性。",
  NIFI_FLOW_NOT_FOUND: "NiFi 流程不存在，请检查流程映射 ID 是否正确。",
  NIFI_FLOW_UNMAPPED: "任务类型未配置流程映射，请先完善后端映射文件。",
  NIFI_EXEC_ERROR: "NiFi 执行异常，请查看任务详情中的错误信息。",
};

function toErrorHint(errorCode) {
  if (!errorCode) return "";
  return ERROR_HINTS[errorCode] || `未知错误码: ${errorCode}`;
}

function getBackendModeConfig(mode) {
  const backendModes = config.BACKEND_MODES || {};
  return backendModes[mode] || backendModes[config.DEFAULT_BACKEND_MODE] || { label: mode, apiBase: config.API_BASE };
}

function isAbsoluteUrl(value) {
  return /^https?:\/\//i.test(String(value || "").trim());
}

function getBackendApiBase(mode = state.backendMode || config.DEFAULT_BACKEND_MODE || "local") {
  const cfg = getBackendModeConfig(mode);
  const apiBase = String(cfg.apiBase || config.API_BASE || "/api/v1").trim();
  if (!apiBase) return "/api/v1";
  if (isAbsoluteUrl(apiBase)) return apiBase.replace(/\/$/, "");
  return apiBase.startsWith("/") ? apiBase : `/${apiBase}`;
}

async function setBackendMode(mode) {
  const nextMode = mode === "nifi" ? "nifi" : "local";
  const res = await setBackendModeRemote(nextMode);
  if (res && res.code === 0) {
    state.backendMode = res.data?.mode === "nifi" ? "nifi" : nextMode;
    try {
      localStorage.setItem(BACKEND_MODE_STORAGE_KEY, state.backendMode);
    } catch (_) {}
    updateBackendToggleUI();
    return;
  }
  state.backendMode = nextMode;
  try {
    localStorage.setItem(BACKEND_MODE_STORAGE_KEY, state.backendMode);
  } catch (_) {}
  updateBackendToggleUI();
}

function updateBackendToggleUI() {
  const label = document.getElementById("backendModeLabel");
  const localBtn = document.getElementById("backendLocalBtn");
  const nifiBtn = document.getElementById("backendNifiBtn");
  const current = state.backendMode || config.DEFAULT_BACKEND_MODE || "local";
  if (label) {
    const cfg = getBackendModeConfig(current);
    label.textContent = `当前后端：${cfg.label || current}`;
  }
  if (localBtn) {
    localBtn.classList.toggle("primary", current === "local");
    localBtn.classList.toggle("secondary", current !== "local");
  }
  if (nifiBtn) {
    nifiBtn.classList.toggle("primary", current === "nifi");
    nifiBtn.classList.toggle("secondary", current !== "nifi");
  }
}

async function getBackendModeState() {
  if (config.USE_MOCK_API) {
    return { code: 0, data: { username: _currentUsername(), factory_id: DEFAULT_FACTORY_ID, mode: state.backendMode || config.DEFAULT_BACKEND_MODE || "local", updatedAt: new Date().toISOString(), updatedBy: "mock" } };
  }
  return await requestJson("/internal/backend-mode", {}, "local");
}

async function setBackendModeRemote(mode) {
  if (config.USE_MOCK_API) {
    return { code: 0, data: { username: _currentUsername(), factory_id: DEFAULT_FACTORY_ID, mode } };
  }
  const payload = { username: _currentUsername(), factory_id: DEFAULT_FACTORY_ID, mode, operator: (state.currentUser && state.currentUser.username) || "system" };
  return await requestJson("/internal/backend-mode", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  }, "local");
}

function api(path, options = {}) {
  return requestJson(path, options);
}

function _normalizeBase(base) {
  const b = String(base || "").trim();
  if (!b) return "";
  if (isAbsoluteUrl(b)) return b.replace(/\/$/, "");
  return b.startsWith("/") ? b : `/${b}`;
}

async function requestJson(path, options = {}, mode = state.backendMode || config.DEFAULT_BACKEND_MODE || "local") {
  if (config.USE_MOCK_API) {
    return mockApi(path, options);
  }
  const fetchOptions = Object.assign({ credentials: 'same-origin' }, options || {});
  const requestPath = String(path || "").trim();
  const requestUrl = isAbsoluteUrl(requestPath)
    ? requestPath
    : `${getBackendApiBase(mode).replace(/\/$/, "")}${requestPath.startsWith("/") ? requestPath : `/${requestPath}`}`;
  const r = await fetch(requestUrl, fetchOptions);
  const text = await r.text();
  let body = {};
  try {
    body = text ? JSON.parse(text) : {};
  } catch (_) {
    body = { raw: text };
  }
  if (!r.ok) {
    // v4: 401/403 自动跳登录（除登录/注册/登出自身外）
    const skipPaths = ["/api/v1/auth/login", "/api/v1/auth/register", "/api/v1/auth/logout", "/api/v1/auth/me"];
    if ((r.status === 401 || r.status === 403) && !skipPaths.some((p) => requestPath.startsWith(p))) {
      const next = encodeURIComponent(window.location.pathname + window.location.search);
      window.location.href = "/login.html?next=" + next;
    }
    return { code: r.status, message: body?.detail || body?.message || body?.raw || `HTTP ${r.status}`, data: body?.data ?? null };
  }
  return body;
}

async function exportJobsApi(path, options = {}) {
  if (config.USE_MOCK_API) {
    return mockApi(path, options);
  }
  try {
    return await api(path, options);
  } catch (e) {
    return { code: -1, message: `request failed: ${e?.message || String(e)}`, data: null };
  }
}

function mockApi(path, options = {}) {
  if (path.startsWith("/files")) {
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: { total: 0, pageNo: 1, pageSize: 20, rows: [] } });
  }
  if (path.startsWith("/tags/rules")) {
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: [{ ruleId: "NIFI_RULE_ID_V5", ruleName: "Mock rule", ruleVersion: "v1", enabled: true }] });
  }
  if (path.startsWith("/training/files")) {
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: { total: 0, page: 1, size: 20, categories: [], files: [] } });
  }
  if (path.startsWith("/training/submit")) {
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: { taskId: "task_mock_001", totalFiles: 0, acceptedFiles: [], rejectedFiles: [] } });
  }
  if (path.startsWith("/training/tasks/")) {
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: { taskId: "task_mock_001", status: "completed", progress: 100, totalFiles: 0, processedFiles: 0, errors: [] } });
  }
  return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: null });
}

function renderFiles() {
  const currentDirLabel = document.getElementById("currentDirLabel");
  if (currentDirLabel) {
    const norm = (p) => (p || "").replace(/\\/g, "/");
    const userDir = state.rootDirs.find((r) => norm(state.currentDir).startsWith(norm(r + "/"))) || "";
    const displayDir = userDir ? state.currentDir.replace(userDir, userDir.split("/").pop()) : state.currentDir;
    currentDirLabel.textContent = `当前目录: ${displayDir || state.currentDir} （文件 ${state.files.length || 0} 个）`;
  }
  const tbody = document.getElementById("fileTable");
  tbody.innerHTML = "";
  state.files.forEach((file) => {
    const tr = document.createElement("tr");
    if (file.fileId === state.selectedFileId) {
      tr.classList.add("file-row-selected");
    }
    tr.innerHTML = `
      <td data-file-id="${file.fileId}">${file.fileId}</td>
      <td>${file.fileName}</td>
      <td>${file.fileFormat}</td>
      <td>${file.fileSize}</td>
      <td>
        <button class="secondary" data-preview="${file.fileId}">预览</button>
        <a href="${getBackendApiBase()}/files/${file.fileId}/download" target="_blank">下载</a>
        <div class="small">路径: ${file.storagePath || ''}</div>
      </td>
    `;
    tr.querySelector("button").addEventListener("click", () => {
      state.selectedFileId = file.fileId;
      renderFiles();
      previewFile(file.fileId);
    });
    tbody.appendChild(tr);
  });
}

function buildDirectoryState(files) {
  const children = {};
  const norm = (p) => (p || "").replace(/\\/g, "/");

  // v4: 从文件路径中收集所有用户根目录（nifi-data / real_nifi_data）
  const rootDirs = new Set();
  files.forEach((f) => {
    const path = norm(f.storagePath);
    const idx = path.indexOf("/nifi-data/");
    const idx2 = path.indexOf("/real_nifi_data/");
    if (idx >= 0) rootDirs.add(path.slice(0, idx + "/nifi-data".length));
    if (idx2 >= 0) rootDirs.add(path.slice(0, idx2 + "/real_nifi_data".length));
  });

  // v4: 始终为当前用户兜底添加 nifi-data / real_nifi_data 两个根目录，
  // 确保 Local/NiFi 两种模式对应的目录头都能在文件中心看到，而不仅是有文件时才出现。
  const un = _currentUsername();
  if (un) {
    rootDirs.add(norm(`/home/yhz/${un}/nifi-data`));
    rootDirs.add(norm(`/home/yhz/${un}/real_nifi_data`));
  } else if (rootDirs.size === 0) {
    rootDirs.add(norm("/home/yhz/admin/nifi-data"));
    rootDirs.add(norm("/home/yhz/admin/real_nifi_data"));
  }

  const sortedRoots = Array.from(rootDirs).sort();
  state.rootDirs = sortedRoots;
  if (sortedRoots.length > 0) {
    state.rootDir = sortedRoots[0];
  }

  const dirs = new Set(sortedRoots);

  // 预置已知子目录（在每个根下）
  sortedRoots.forEach((root) => {
    KNOWN_NIFI_SUBDIRS.forEach((subdir) => {
      dirs.add(norm(root + "/" + subdir));
    });
  });

  // 从实际文件路径收集所有中间目录
  files.forEach((f) => {
    const path = norm(f.storagePath);
    const root = sortedRoots.find((r) => path.startsWith(r + "/"));
    if (!root) return;
    let dir = path.slice(0, path.lastIndexOf("/"));
    while (dir.startsWith(root)) {
      dirs.add(dir);
      if (dir === root) break;
      const idx = dir.lastIndexOf("/");
      if (idx < root.length) {
        dir = root;
      } else {
        dir = dir.slice(0, idx);
      }
    }
  });

  dirs.forEach((d) => {
    children[d] = [];
  });
  dirs.forEach((d) => {
    const root = sortedRoots.find((r) => d.startsWith(r + "/") || d === r);
    if (!root || d === root) {
      // 根目录的父目录：用用户目录（如 /home/yhz/admin）作为父级
      const parts = d.split("/");
      if (parts.length >= 2) {
        const parent = parts.slice(0, -1).join("/");
        if (parent) {
          if (!children[parent]) children[parent] = [];
          children[parent].push(d);
        }
      }
      return;
    }
    const parent = d.slice(0, d.lastIndexOf("/"));
    if (children[parent]) children[parent].push(d);
  });
  Object.keys(children).forEach((k) => children[k].sort());

  state.dirChildren = children;
}

function renderDirectoryTree() {
  const root = document.getElementById("fileTree");
  if (!root) return;
  root.innerHTML = "";

  const norm = (p) => (p || "").replace(/\\/g, "/");

  const renderNode = (dir, depth) => {
    const row = document.createElement("div");
    row.className = "tree-row";
    row.style.marginLeft = `${depth * 16}px`;
    const children = state.dirChildren[dir] || [];
    const hasChildren = children.length > 0;
    const isExpanded = state.expandedDirs[dir] !== false;

    const toggle = document.createElement("button");
    toggle.className = "tree-toggle";
    toggle.textContent = hasChildren ? (isExpanded ? "▼" : "▶") : "•";
    toggle.disabled = !hasChildren;
    toggle.title = hasChildren ? (isExpanded ? "折叠" : "展开") : "无子目录";
    if (hasChildren) {
      toggle.addEventListener("click", (ev) => {
        ev.stopPropagation();
        state.expandedDirs[dir] = !isExpanded;
        renderDirectoryTree();
      });
    }

    const btn = document.createElement("button");
    btn.className = "tree-dir-btn";
    // 生成更友好的显示名称
    let name = dir.split("/").pop();
    const isRootDir = state.rootDirs.some((r) => r === dir);
    const isUserDir = state.rootDirs.some((r) => norm(r).startsWith(norm(dir + "/")));
    if (isRootDir) {
      name = dir.endsWith("real_nifi_data") ? "real_nifi_data" : "nifi-data";
    } else if (isUserDir) {
      name = dir.split("/").pop();
    }
    btn.textContent = name;
    btn.title = dir;
    if (dir === state.currentDir) {
      btn.classList.add("current");
    }
    btn.addEventListener("click", () => {
      state.currentDir = dir;
      // 点击目录名同时切换展开/折叠，方便查看细分目录
      if (hasChildren) {
        state.expandedDirs[dir] = !isExpanded;
      }
      filterFilesByCurrentDir();
      renderDirectoryTree();
    });
    row.appendChild(toggle);
    row.appendChild(btn);
    root.appendChild(row);
    if (isExpanded) {
      children.forEach((child) => renderNode(child, depth + 1));
    }
  };

  // v4: 主控制台是个人工作台，只展示当前用户目录。
  // 如果 rootDirs 都属于同一个用户，直接把 nifi-data / real_nifi_data 作为顶层节点；
  // 只有内部管理页等多用户场景才按用户名分组。
  const userDirs = new Set();
  state.rootDirs.forEach((r) => {
    const parent = r.slice(0, r.lastIndexOf("/"));
    if (parent) userDirs.add(parent);
  });

  if (userDirs.size <= 1) {
    state.rootDirs.forEach((root) => renderNode(root, 0));
  } else {
    Array.from(userDirs).sort().forEach((userDir) => {
      renderNode(userDir, 0);
    });
  }
}

function expandAllDirs() {
  Object.keys(state.dirChildren).forEach((dir) => {
    if ((state.dirChildren[dir] || []).length > 0) {
      state.expandedDirs[dir] = true;
    }
  });
  renderDirectoryTree();
}

function collapseAllDirs() {
  Object.keys(state.dirChildren).forEach((dir) => {
    state.expandedDirs[dir] = false;
  });
  renderDirectoryTree();
}

function filterFilesByCurrentDir() {
  const fmt = document.getElementById("fileFormatFilter")?.value || "";
  const norm = (p) => (p || "").replace(/\\/g, "/");
  state.files = state.allFiles.filter((f) => {
    const path = norm(f.storagePath);
    // include files directly under currentDir or in any subdirectory of currentDir
    if (!path.startsWith(state.currentDir)) return false;
    if (fmt && f.fileFormat !== fmt.toUpperCase()) return false;
    return true;
  });
  renderFiles();
}

async function loadFiles() {
  // v4: 后台触发 NiFi 根目录重扫描，不阻塞文件列表加载
  const refreshPromise = api("/internal/factory-tree/refresh", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username: _currentUsername(), factory_id: DEFAULT_FACTORY_ID }),
  }).catch((e) => {
    console.warn("refresh scan failed:", e);
  });

  const pageSize = 500;
  let pageNo = 1;
  const allRows = [];
  while (true) {
    const currentUsername = _currentUsername();
    const userParam = currentUsername ? `&username=${encodeURIComponent(currentUsername)}` : "";
    const res = await api(`/files?nifiOnly=true&pageNo=${pageNo}&pageSize=${pageSize}${userParam}`);
    if (res.code !== 0) return;
    const rows = res.data.rows || [];
    allRows.push(...rows);
    if (rows.length < pageSize) break;
    pageNo += 1;
  }
  state.allFiles = allRows;
  buildDirectoryState(state.allFiles);
  if (!state.currentDir || !state.dirChildren[state.currentDir]) {
    // 默认选中第一个用户目录（包含 nifi-data / real_nifi_data）
    const userDirs = Object.keys(state.dirChildren).filter((d) => state.rootDirs.some((r) => r.startsWith(d + "/")));
    state.currentDir = userDirs[0] || state.rootDirs[0] || state.rootDir || "";
  }
  renderDirectoryTree();
  filterFilesByCurrentDir();
}

async function purgeMissingFiles() {
  const btn = document.getElementById("purgeMissingBtn");
  if (btn) btn.disabled = true;
  setFilesMessage("正在清理失效文件…");
  try {
    const res = await api("/files/purge-missing", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });
    if (res.code !== 0) {
      setFilesMessage(`清理失败：${res.message || res.code}`);
      return;
    }
    const removed = (res.data && res.data.removed) || 0;
    const removedMeta = (res.data && res.data.removedMeta) || 0;
    setFilesMessage(`清理完成：移除失效文件 ${removed} 条，孤儿 meta ${removedMeta} 个`);
    // 立即重新加载文件列表，让前端看到最新状态
    await loadFiles();
  } catch (e) {
    setFilesMessage(`清理异常：${e && e.message ? e.message : e}`);
  } finally {
    if (btn) btn.disabled = false;
  }
}

function setFilesMessage(text) {
  // 复用现有 setScheduleMessage 提示容器（无副作用，仅显示）
  if (typeof setScheduleMessage === "function") {
    setScheduleMessage(text);
  } else {
    console.log("[file-center]", text);
  }
}

async function previewFile(fileId) {
  return _loadPreview(fileId, { append: false });
}

async function loadMorePreview() {
  if (!state.selectedFileId) return;
  return _loadPreview(state.selectedFileId, { append: true });
}

async function _loadPreview(fileId, { append = false } = {}) {
  state.selectedFileId = fileId;
  const limit = Number(document.getElementById("previewLimit").value || "100");
  // 追加模式从 state.previewOffset 推进，否则按用户输入的 offset（默认 0）
  const offset = append
    ? (state.previewOffset || 0) + limit
    : Number(document.getElementById("previewOffset").value || "0");
  state.previewOffset = offset;
  const res = await api(`/files/${fileId}/preview?offset=${offset}&limit=${limit}`);
  const preview = document.getElementById("filePreview");
  if (res.code !== 0) {
    preview.textContent = `预览失败: ${res.message}`;
    return;
  }
  if (!append) {
    // 表格数据：仅在 <pre> 中展示前 20 条 JSON 行（避免下方表格内容重复展示全量）
    // 非表格数据：直接 JSON 预览整段内容
    const PREVIEW_JSON_LIMIT = 20;
    const isTabular = res.data && res.data.columns && Array.isArray(res.data.rows);
    if (isTabular) {
      const sliced = {
        columns: res.data.columns,
        rows: res.data.rows.slice(0, PREVIEW_JSON_LIMIT),
        total: (typeof res.data.total === "number") ? res.data.total : res.data.rows.length,
        meta: res.data.meta,
        _previewNote: `（仅展示前 ${PREVIEW_JSON_LIMIT} 条；下方表格可编辑，点 "下一批" 加载更多）`,
      };
      preview.textContent = JSON.stringify(sliced, null, 2);
    } else {
      preview.textContent = JSON.stringify(res.data, null, 2);
    }
  }
  // 显示元数据摘要
  const metaInfoBar = document.getElementById("metaInfoBar");
  const metaInfoContent = document.getElementById("metaInfoContent");
  if (metaInfoBar && metaInfoContent && res.data && res.data.meta) {
    const m = res.data.meta;
    const hasTag = m.hasTag ? "是" : "否";
    const parts = [`hasTag: ${hasTag}`];
    if (m.tagColumn) parts.push(`tagColumn: ${m.tagColumn}`);
    if (m.tagName) parts.push(`tagName: ${m.tagName}`);
    if (m.tagRange && Array.isArray(m.tagRange)) parts.push(`tagRange: [${m.tagRange.join(", ")}]`);
    if (m.failedTag) parts.push(`failedTag: ${m.failedTag}`);
    metaInfoContent.textContent = parts.join(" | ");
    metaInfoBar.style.display = "";
  } else if (metaInfoBar) {
    metaInfoBar.style.display = "none";
  }
  // 控制「自动打标」按钮显示
  const autoTagBtn = document.getElementById("autoTagFromPreviewBtn");
  if (autoTagBtn && res.data && res.data.meta) {
    autoTagBtn.style.display = "";
    autoTagBtn.onclick = () => {
      const atf = document.getElementById("autoTagFileId");
      if (atf) { atf.value = fileId; }
      const atn = document.getElementById("autoTagName");
      if (atn && res.data.meta.tagName) { atn.value = res.data.meta.tagName; }
      const tagSection = document.getElementById("tagCenterTitle");
      if (tagSection) {
        tagSection.scrollIntoView({ behavior: "smooth", block: "start" });
        // 短暂高亮标签中心标题，提示用户已跳转
        tagSection.style.transition = "background 0.2s";
        const original = tagSection.style.background;
        tagSection.style.background = "#fff3bf";
        setTimeout(() => { tagSection.style.background = original || ""; }, 1200);
      }
    };
  } else if (autoTagBtn) {
    autoTagBtn.style.display = "none";
  }
  // 若为表格格式（columns/rows），展示可编辑表格
  const editable = document.getElementById("editablePreview");
  const wrapper = document.getElementById("editableTableWrapper");
  const progressEl = document.getElementById("previewProgress");
  const loadMoreBtn = document.getElementById("loadMorePreviewBtn");
  if (!append) {
    wrapper.innerHTML = "";
    state.previewColumns = [];
    state.previewRows = [];
    document.getElementById("saveTagsResult").textContent = "";
  }
  if (res.data && res.data.columns && Array.isArray(res.data.rows)) {
    let table = wrapper.querySelector("table.editable");
    let thead, headRow, tbody;
    if (!append || !table) {
      editable.style.display = "block";
      table = document.createElement("table");
      table.className = "editable";
      thead = document.createElement("thead");
      headRow = document.createElement("tr");
      res.data.columns.forEach((c, colIdx) => {
        const th = document.createElement("th");
        th.contentEditable = true;
        th.className = "editable-header";
        th.dataset.original = c;
        th.dataset.colIndex = String(colIdx);
        th.textContent = c;
        headRow.appendChild(th);
      });
      thead.appendChild(headRow);
      table.appendChild(thead);
      tbody = document.createElement("tbody");
      table.appendChild(tbody);
      wrapper.appendChild(table);
      // 重新初始化 state 列
      state.previewColumns = [...res.data.columns];
      state.previewRows = [];
    } else {
      thead = table.querySelector("thead");
      headRow = thead ? thead.querySelector("tr") : null;
      tbody = table.querySelector("tbody");
    }

    res.data.rows.forEach((r, idx) => {
      const tr = document.createElement("tr");
      r.forEach((cell, colIdx) => {
        const td = document.createElement("td");
        td.contentEditable = true;
        td.className = "editable-cell";
        td.dataset.rowId = String(offset + idx + 1);
        td.dataset.column = res.data.columns[colIdx] || `col_${colIdx + 1}`;
        td.dataset.original = String(cell ?? "");
        td.textContent = cell;
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
      state.previewRows.push([...r]);
    });

    // 「已加载 X / Y」进度
    const total = (typeof res.data.total === "number") ? res.data.total : (offset + res.data.rows.length);
    const loaded = offset + res.data.rows.length;
    if (progressEl) progressEl.textContent = `已加载 ${loaded} / ${total}`;
    if (loadMoreBtn) loadMoreBtn.style.display = (loaded < total) ? "" : "none";

    // 「新增字段」按钮（仅在首次加载时挂接）
    const addFieldBtn = document.getElementById("addFieldBtn");
    if (addFieldBtn && !append) {
      const hasTagColumn = state.previewColumns.includes("tag");
      addFieldBtn.disabled = hasTagColumn;
      addFieldBtn.onclick = () => {
        if (state.previewColumns.includes("tag")) {
          return;
        }
        const newColName = "tag";
        state.previewColumns.push(newColName);
        const newTh = document.createElement("th");
        newTh.contentEditable = true;
        newTh.className = "editable-header";
        newTh.dataset.original = newColName;
        newTh.dataset.colIndex = String(headRow.children.length);
        newTh.textContent = newColName;
        headRow.appendChild(newTh);
        if (tbody) {
          Array.from(tbody.querySelectorAll("tr")).forEach((tr, rowIdx) => {
            const td = document.createElement("td");
            td.contentEditable = true;
            td.className = "editable-cell";
            td.dataset.rowId = String(state.previewOffset + rowIdx + 1);
            td.dataset.column = newColName;
            td.dataset.original = "";
            td.textContent = "";
            tr.appendChild(td);
          });
        }
        addFieldBtn.disabled = true;
      };
    }
  } else {
    if (!append) {
      editable.style.display = "none";
      state.previewColumns = [];
      state.previewRows = [];
      if (progressEl) progressEl.textContent = "";
      if (loadMoreBtn) loadMoreBtn.style.display = "none";
    }
  }
}

  // 刷新当前预览
  document.getElementById("previewReloadBtn").addEventListener("click", () => {
    if (state.selectedFileId) previewFile(state.selectedFileId);
  });

  // 下一批：基于当前 offset 继续追加
  const loadMoreBtn = document.getElementById("loadMorePreviewBtn");
  if (loadMoreBtn) {
    loadMoreBtn.addEventListener("click", () => {
      if (!state.selectedFileId) return;
      loadMorePreview();
    });
  }

  const openModalBtn = document.getElementById("openEditorModalBtn");
  const closeModalBtn = document.getElementById("closeEditorModalBtn");
  const closePreviewModalBtn = document.getElementById("closePreviewModalBtn");
  const previewContainer = document.getElementById("filePreviewContainer");
  const modalBar = document.getElementById("editorModalBar");
  const modalBackdrop = document.getElementById("editorModalBackdrop");
  const previewModal = document.getElementById("previewModal");
  const previewModalMount = document.getElementById("previewModalMount");
  const previewContainerPlaceholder = document.getElementById("previewContainerPlaceholder");
  const previewModalBackdrop = document.getElementById("previewModalBackdrop");

  const closeEditorModal = () => {
    if (previewContainerPlaceholder && previewContainer && previewContainer.parentElement === previewModalMount) {
      previewContainerPlaceholder.appendChild(previewContainer);
    }
    if (previewModal) previewModal.style.display = "none";
    previewContainer.classList.remove("preview-modal");
    document.body.classList.remove("modal-open");
    modalBar.style.display = "none";
    if (modalBackdrop) modalBackdrop.style.display = "none";
  };

  if (openModalBtn && previewContainer && modalBar && previewModal && previewModalMount && previewContainerPlaceholder) {
    openModalBtn.addEventListener("click", () => {
      previewModal.style.display = "block";
      previewModalMount.appendChild(previewContainer);
      previewContainer.classList.add("preview-modal");
      document.body.classList.add("modal-open");
      modalBar.style.display = "flex";
      if (modalBackdrop) modalBackdrop.style.display = "block";
    });
    if (closeModalBtn) closeModalBtn.addEventListener("click", closeEditorModal);
    if (closePreviewModalBtn) closePreviewModalBtn.addEventListener("click", closeEditorModal);
    if (modalBackdrop) {
      modalBackdrop.addEventListener("click", closeEditorModal);
    }
    if (previewModalBackdrop) {
      previewModalBackdrop.addEventListener("click", closeEditorModal);
    }
    document.addEventListener("keydown", (evt) => {
      if (evt.key === "Escape") {
        closeEditorModal();
      }
    });
  }

  // 保存编辑：收集有变化的单元格并调用后端 /tags/manual-table
  document.getElementById("saveTagsBtn").addEventListener("click", async () => {
    const wrapper = document.getElementById("editableTableWrapper");
    const editableCells = wrapper.querySelectorAll(".editable-cell");
    const editableHeaders = wrapper.querySelectorAll(".editable-header");
    const changes = [];
    const renameColumns = [];

    editableHeaders.forEach((el) => {
      const oldName = (el.dataset.original || "").trim();
      const newName = (el.textContent || "").trim();
      if (!oldName || !newName || oldName === newName) return;
      renameColumns.push({ old: oldName, new: newName });
      const colIndex = Number(el.dataset.colIndex || "-1");
      if (colIndex >= 0) {
        wrapper.querySelectorAll(`.editable-cell[data-column="${oldName}"]`).forEach((cell) => {
          cell.dataset.column = newName;
        });
      }
    });

    editableCells.forEach((el) => {
      const value = (el.textContent || "").trim();
      const original = (el.dataset.original || "").trim();
      const rowId = el.dataset.rowId;
      const column = el.dataset.column;
      if (value !== original) {
        changes.push({ rowId, column, value });
      }
    });
    const result = document.getElementById("saveTagsResult");
    if (!state.selectedFileId) {
      result.textContent = "请先选择并预览文件";
      return;
    }
    if (changes.length === 0 && renameColumns.length === 0) {
      result.textContent = "没有发现需要保存的改动";
      return;
    }
    result.textContent = "保存中...";
    const payload = { fileId: state.selectedFileId, operator: (state.currentUser && state.currentUser.username) || "user-001", changes, renameColumns };
    const res = await api("/tags/manual-table", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (res.code !== 0) {
      result.textContent = `保存失败：${res.message}`;
      return;
    }
    const file = res.data?.file;
    if (file) {
      const renameInfo = Number(res.data.renamedColumns || 0) > 0 ? `，重命名 ${res.data.renamedColumns} 个字段` : "";
      result.textContent = `保存成功，更新 ${res.data.updatedCells} 个单元格${renameInfo}，并生成 ${file.fileName}（${file.storagePath}）`;
    } else {
      result.textContent = `保存成功，更新 ${res.data.updatedCells} 个单元格`;
    }
    await loadFiles();
  });

function renderTagRules() {
  const select = document.getElementById("tagRuleSelect");
  if (!select) return;
  select.innerHTML = "";
  state.tagRules.forEach((rule) => {
    const op = document.createElement("option");
    op.value = rule.ruleId;
    op.textContent = `${rule.ruleName} (${rule.ruleVersion})`;
    select.appendChild(op);
  });
}

async function loadTagRules() {
  const res = await api("/tags/rules");
  if (res.code !== 0) return;
  state.tagRules = res.data.filter((x) => x.enabled !== false);
  renderTagRules();
}

function collectTagRuleFromUI() {
  const ruleName = (document.getElementById("autoTagRuleName")?.value || "").trim();
  const inputsStr = (document.getElementById("autoTagInputs")?.value || "").trim();
  const defaultTag = (document.getElementById("autoTagDefault")?.value || "未知").trim();
  const inputs = inputsStr ? inputsStr.split(",").map(s => s.trim()).filter(Boolean) : [];
  const mapping = [];
  const rows = document.querySelectorAll("#tagRuleMappingContainer .tag-rule-row");
  rows.forEach(row => {
    const whenInputs = row.querySelectorAll(".rule-when-input");
    const tagInput = row.querySelector(".rule-tag-input");
    const when = {};
    whenInputs.forEach(wi => {
      const col = wi.dataset.column || "";
      const raw = wi.value.trim();
      if (!col || !raw) return;
      // 支持速记: "value>15" / "status==200" / "code in 200,201,204"
      const m = raw.match(/^\s*([a-zA-Z_][\w]*)\s*(==|!=|>=|<=|>|=|<)\s*(.+)$/);
      if (m && m[1] === col) {
        when[col] = { op: m[2], value: m[3].trim() };
      } else {
        // 回退为相等
        when[col] = raw;
      }
    });
    const tag = tagInput ? tagInput.value.trim() : "";
    if (Object.keys(when).length > 0 && tag) {
      mapping.push({ when, tag });
    }
  });
  if (!ruleName || inputs.length === 0 || mapping.length === 0) return null;
  return { ruleName, inputs, mapping, defaultTag };
}

function addTagRuleConditionRow() {
  const container = document.getElementById("tagRuleMappingContainer");
  if (!container) return;
  const inputsStr = (document.getElementById("autoTagInputs")?.value || "").trim();
  const inputs = inputsStr ? inputsStr.split(",").map(s => s.trim()).filter(Boolean) : [];
  if (inputs.length === 0) {
    // 提示先填写参与打标的列
    const tagResult = document.getElementById("tagResult");
    if (tagResult) tagResult.textContent = "请先填写「参与打标的列」";
    return;
  }
  const row = document.createElement("div");
  row.className = "row tag-rule-row";
  row.style.cssText = "margin-top:4px; align-items:center;";
  inputs.forEach(col => {
    const label = document.createElement("label");
    label.textContent = col;
    label.style.cssText = "font-size:0.85em;";
    row.appendChild(label);
    const input = document.createElement("input");
    input.type = "text";
    input.className = "rule-when-input";
    input.dataset.column = col;
    input.placeholder = `值 / 表达式: 5 或 ${col}>15`;
    input.style.cssText = "width:100px;";
    row.appendChild(input);
  });
  const tagLabel = document.createElement("label");
  tagLabel.textContent = "→ 标签";
  tagLabel.style.cssText = "font-size:0.85em; margin-left:8px;";
  row.appendChild(tagLabel);
  const tagInput = document.createElement("input");
  tagInput.type = "text";
  tagInput.className = "rule-tag-input";
  tagInput.placeholder = "如 故障";
  tagInput.style.cssText = "width:80px;";
  row.appendChild(tagInput);
  const delBtn = document.createElement("button");
  delBtn.textContent = "×";
  delBtn.className = "secondary";
  delBtn.type = "button";
  delBtn.style.cssText = "margin-left:4px; padding:2px 6px; font-size:0.85em;";
  delBtn.onclick = () => row.remove();
  row.appendChild(delBtn);
  container.appendChild(row);
}

async function triggerAutoTag() {
  const fileId = document.getElementById("autoTagFileId").value.trim();
  const msg = document.getElementById("tagResult");
  if (!fileId) {
    msg.textContent = "请先输入 fileId";
    return;
  }
  const tagRule = collectTagRuleFromUI();
  const tagName = (document.getElementById("autoTagName")?.value || "").trim();
  const payload = {
    fileId,
    outputFormat: "CSV",
    operator: (state.currentUser && state.currentUser.username) || "user-001",
  };
  if (tagRule) payload.tagRule = tagRule;
  if (tagName) payload.tagName = tagName;
  const res = await api("/tags/auto", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (res.code !== 0) {
    msg.textContent = `自动标签失败: ${res.message}`;
    return;
  }
  const data = res.data || {};
  if (data.file) {
    const newFile = data.file;
    const newFileId = newFile.fileId || "";
    const newFileName = newFile.fileName || "";
    const newStoragePath = newFile.storagePath || "";
    // 重要：自动打标会在 tagged_output/ 下生成新文件（新的 fileId），原文件不会被就地修改
    msg.innerHTML = "";
    msg.appendChild(document.createTextNode("自动标签完成（已生成新文件，原文件未修改）："));
    const wrap = document.createElement("div");
    wrap.style.cssText = "margin-top:6px; padding:8px 10px; background:#ecfdf5; border:1px solid #6ee7b7; border-radius:6px; display:inline-block;";
    const nameEl = document.createElement("div");
    nameEl.innerHTML = `<strong>${newFileName}</strong> <span class="muted">fileId:</span> <code>${newFileId}</code>`;
    wrap.appendChild(nameEl);
    if (newStoragePath) {
      const pathEl = document.createElement("div");
      pathEl.className = "small";
      pathEl.style.cssText = "margin-top:4px; color:#065f46; word-break:break-all;";
      pathEl.innerHTML = `<span class="muted">存放路径:</span> <code>${escapeHtml(newStoragePath)}</code>`;
      wrap.appendChild(pathEl);
    }
    const btnRow = document.createElement("div");
    btnRow.style.cssText = "margin-top:6px; display:flex; gap:6px;";
    const previewBtn = document.createElement("button");
    previewBtn.textContent = "预览新文件";
    previewBtn.className = "secondary";
    previewBtn.onclick = async () => {
      // 先确保文件列表已包含新文件
      try { await loadFiles(); } catch (_) {}
      // 搜索/定位文件中心新行
      const fileIdCell = document.querySelector(`[data-file-id="${newFileId}"]`);
      if (fileIdCell) {
        fileIdCell.scrollIntoView({ behavior: "smooth", block: "center" });
        const row = fileIdCell.closest("tr");
        if (row) {
          row.style.transition = "background 0.2s";
          const original = row.style.background;
          row.style.background = "#fef08a";
          setTimeout(() => { row.style.background = original || ""; }, 1500);
        }
      }
      if (typeof previewFile === "function") {
        await previewFile(newFileId);
      }
    };
    const fillBtn = document.createElement("button");
    fillBtn.textContent = "把 fileId 填回上方（再次打标）";
    fillBtn.className = "secondary";
    fillBtn.onclick = () => {
      const inp = document.getElementById("autoTagFileId");
      if (inp) inp.value = newFileId;
    };
    btnRow.appendChild(previewBtn);
    btnRow.appendChild(fillBtn);
    wrap.appendChild(btnRow);
    msg.appendChild(wrap);
  } else {
    msg.textContent = `自动标签任务已创建: ${data.jobId || "-"}`;
  }
  await loadFiles();
}

// ---------- 训练中心 ----------

function renderTrainingCategories(categories) {
  const select = document.getElementById("trainingCategoryFilter");
  if (!select) return;
  const current = select.value || "";
  select.innerHTML = '<option value="">全部</option>';
  (categories || []).forEach((cat) => {
    const op = document.createElement("option");
    op.value = cat.categoryId || "";
    op.textContent = `${cat.categoryName || cat.categoryId || "未分类"} (${cat.fileCount || 0})`;
    select.appendChild(op);
  });
  select.value = current;
}

function renderTrainingFiles(files) {
  const tbody = document.getElementById("trainingFilesTable");
  if (!tbody) return;
  tbody.innerHTML = "";
  state.trainingFiles = files || [];
  if (!files || files.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="empty">暂无训练文件，请先在文件中心上传/打标</td></tr>';
    return;
  }
  files.forEach((f) => {
    const tr = document.createElement("tr");
    const tags = Array.isArray(f.tags) ? f.tags.join(", ") : (f.tags || "");
    tr.innerHTML = `
      <td><input type="checkbox" class="training-checkbox" data-file-id="${f.fileId}" /></td>
      <td>${f.fileId}</td>
      <td>${f.fileName}</td>
      <td>${(f.fileType || "").toUpperCase()}</td>
      <td>${f.fileSizeHuman || f.fileSize || "-"}</td>
      <td>${tags}</td>
      <td>${f.labelColumn || "-"}</td>
    `;
    tbody.appendChild(tr);
  });
}

function getTrainingFilterParams() {
  const categoryId = document.getElementById("trainingCategoryFilter")?.value || "";
  const tag = document.getElementById("trainingTagFilter")?.value.trim() || "";
  const keyword = document.getElementById("trainingKeywordFilter")?.value.trim() || "";
  const params = new URLSearchParams();
  params.set("page", "1");
  params.set("size", "100");
  if (categoryId) params.set("categoryId", categoryId);
  if (tag) params.set("tag", tag);
  if (keyword) params.set("keyword", keyword);
  return params;
}

async function loadTrainingFiles() {
  const status = document.getElementById("trainingStatus");
  if (status) status.textContent = "加载中…";
  const params = getTrainingFilterParams();
  const res = await api(`/training/files?${params.toString()}`);
  if (res.code !== 0) {
    if (status) status.textContent = `加载失败：${res.message || res.code}`;
    return;
  }
  renderTrainingCategories(res.data?.categories || []);
  renderTrainingFiles(res.data?.files || []);
  if (status) status.textContent = `共 ${res.data?.total || 0} 个文件`;
}

function getSelectedTrainingFileIds() {
  const ids = [];
  document.querySelectorAll("#trainingFilesTable .training-checkbox:checked").forEach((cb) => {
    const fid = cb.dataset.fileId;
    if (fid) ids.push(fid);
  });
  return ids;
}

function updateTrainingTaskPanel(task) {
  const panel = document.getElementById("trainingTaskPanel");
  const taskIdEl = document.getElementById("trainingTaskId");
  const statusEl = document.getElementById("trainingTaskStatus");
  const progressEl = document.getElementById("trainingTaskProgress");
  const resultEl = document.getElementById("trainingTaskResult");
  if (!panel || !taskIdEl || !statusEl || !progressEl) return;
  panel.style.display = "block";
  taskIdEl.textContent = task.taskId || "-";
  statusEl.textContent = task.status || "-";
  progressEl.textContent = task.progress || 0;
  if (task.status === "completed" && task.result) {
    resultEl.textContent = `模型版本: ${task.result.modelVersion || "-"}, 指标: ${JSON.stringify(task.result.metrics || {})}`;
  } else if ((task.errors || []).length > 0) {
    resultEl.textContent = `错误: ${task.errors.join("; ")}`;
  } else {
    resultEl.textContent = `已处理 ${task.processedFiles || 0} / ${task.totalFiles || 0} 个文件`;
  }
}

async function pollTrainingTask(taskId) {
  state.currentTrainingTaskId = taskId;
  if (state.trainingTaskPollTimer) {
    clearInterval(state.trainingTaskPollTimer);
    state.trainingTaskPollTimer = null;
  }
  const doPoll = async () => {
    const res = await api(`/training/tasks/${encodeURIComponent(taskId)}`);
    if (res.code !== 0) {
      updateTrainingTaskPanel({ taskId, status: "error", progress: 0, errors: [res.message || "查询失败"], totalFiles: 0, processedFiles: 0 });
      stopTrainingPoll();
      return;
    }
    updateTrainingTaskPanel(res.data);
    const status = res.data?.status;
    if (status === "completed" || status === "failed" || status === "error") {
      stopTrainingPoll();
    }
  };
  await doPoll();
  state.trainingTaskPollTimer = setInterval(doPoll, 1000);
}

function stopTrainingPoll() {
  if (state.trainingTaskPollTimer) {
    clearInterval(state.trainingTaskPollTimer);
    state.trainingTaskPollTimer = null;
  }
}

async function submitTraining() {
  const ids = getSelectedTrainingFileIds();
  const status = document.getElementById("trainingStatus");
  if (ids.length === 0) {
    if (status) status.textContent = "请先选择训练文件";
    return;
  }
  const modelName = document.getElementById("trainingModelName")?.value.trim() || "";
  const paramsStr = document.getElementById("trainingParams")?.value.trim() || "";
  let trainingParams = {};
  if (paramsStr) {
    try {
      trainingParams = JSON.parse(paramsStr);
    } catch (e) {
      if (status) status.textContent = `训练参数 JSON 格式错误: ${e.message}`;
      return;
    }
  }
  if (status) status.textContent = "提交中…";
  const payload = {
    selectedFileIds: ids,
    trainingConfig: Object.assign({ modelName: modelName || undefined }, trainingParams),
  };
  const res = await api("/training/submit", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (res.code !== 0) {
    if (status) status.textContent = `提交失败：${res.message || res.code}`;
    return;
  }
  if (status) status.textContent = `训练任务已创建: ${res.data?.taskId}, 接受 ${(res.data?.acceptedFiles || []).length} 个文件`;
  if (res.data?.taskId) {
    await pollTrainingTask(res.data.taskId);
  }
}

function getScheduleSpecFromUi() {
  const freq = document.getElementById("dbScheduleFreq")?.value || "5m";
  const cron = document.getElementById("dbScheduleCron")?.value.trim() || "";
  if (freq === "cron") {
    return cron;
  }
  return freq;
}

function collectDbConfigForSchedule() {
  const dbTypeEl = document.getElementById("dbType");
  const dsnEl = document.getElementById("dbDSN");
  const db_type = (dbTypeEl && String(dbTypeEl.value).trim()) || "mysql";
  const dsn = dsnEl && String(dsnEl.value).trim();
  const DEFAULT_PORTS = { mysql: 3306, postgresql: 5432, sqlserver: 1433, oracle: 1521 };
  // add common Hadoop-related defaults: Hive thrift, HBase thrift, WebHDFS
  // HIVE: 10000 (Thrift/Beeswax), HBASE: 19090 (host-mapped Thrift), HDFS(WebHDFS): 9870/50070 (use 9870 modern)
  DEFAULT_PORTS.hive = 10000;
  DEFAULT_PORTS.hbase = 19090;
  DEFAULT_PORTS.hdfs = 9870;
  const conf = {
    db_type: db_type,
    host: document.getElementById("dbHost")?.value.trim() || "192.168.1.181",
    port: Number(document.getElementById("dbPort")?.value || DEFAULT_PORTS[db_type] || 3306),
    user: document.getElementById("dbUser")?.value.trim() || "root",
    password: document.getElementById("dbPassword")?.value || "",
    database: document.getElementById("dbName")?.value.trim() || "",
    path: document.getElementById("dbPath")?.value.trim() || "",
    table: getSelectedTableForSchedule(),
    row_key_prefix: document.getElementById("hbaseRowKeyPrefix")?.value.trim() || "",
  };
  if (dsn) {
    conf.dsn = dsn;
  }
  return conf;
}

const DB_TYPE_DEFAULTS = {
  mysql: { host: '202.113.76.55', port: '3306', user: 'root', database: 'nifi', password: 'root' },
  postgresql: { host: '202.113.76.55', port: '5432', user: 'postgres', database: 'postgres', password: 'difyai123456' },
  sqlserver: { host: '202.113.76.55', port: '1433', user: 'sa', database: 'master', password: 'Your_password123' },
  oracle: { host: '202.113.76.55', port: '1521', user: 'system', database: 'FREEPDB1', password: 'Oracle123456' },
  sqlite: { host: '', port: '', user: '', database: '', password: '' },
  hive: { host: '202.113.76.55', port: '10000', user: 'hive', database: 'default', password: '' },
  hdfs: { host: '202.113.76.55', port: '9870', user: 'hadoop', database: '/', password: '', path: '/' },
  hbase: { host: '202.113.76.55', port: '19090', user: 'root', database: 'default', password: '' },
};

function applyDbTypeDefaults() {
  const dbType = (document.getElementById("dbType")?.value || "mysql").toLowerCase();
  const defaults = DB_TYPE_DEFAULTS[dbType] || DB_TYPE_DEFAULTS.mysql;
  const previousType = window.__lastDbType || '';
  const previousDefaults = DB_TYPE_DEFAULTS[previousType] || {};
  const allKnownValues = (key) => Array.from(new Set(Object.values(DB_TYPE_DEFAULTS).map((item) => item[key]).filter((v) => v !== undefined && v !== null).map(String)));
  const maybeSet = (id, key) => {
    const el = document.getElementById(id);
    if (!el || defaults[key] === undefined) return;
    const current = String(el.value || '').trim();
    const previous = previousDefaults[key] !== undefined ? String(previousDefaults[key]) : '';
    const known = allKnownValues(key);
    if (!current || current === previous || known.includes(current)) {
      el.value = defaults[key];
    }
  };
  maybeSet('dbHost', 'host');
  maybeSet('dbPort', 'port');
  maybeSet('dbUser', 'user');
  maybeSet('dbPassword', 'password');
  maybeSet('dbName', 'database');
  if (dbType === 'hdfs') maybeSet('dbPath', 'path');
  window.__lastDbType = dbType;
}

// Render DB-specific fields and DSN-priority behavior
function renderDbFields() {
  applyDbTypeDefaults();
  const dbType = (document.getElementById("dbType")?.value || "mysql").toLowerCase();
  const dsn = (document.getElementById("dbDSN")?.value || "").trim();
  const DEFAULT_PORTS = Object.fromEntries(Object.entries(DB_TYPE_DEFAULTS).map(([key, value]) => [key, value.port]));
  const dbPathRow = document.getElementById("dbPathRow");
  // Render fields per dbType
  if (dbType === "sqlite") {
    if (dbPathRow) dbPathRow.style.display = '';
    try {
      ["dbHost", "dbPort"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = true; const row = el.closest('.row'); if (row) row.style.display = 'none'; } });
      ["dbName", "dbUser", "dbPassword"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = true; const row = el.closest('.row'); if (row) row.style.display = 'none'; } });
      // SQLite also supports table listing via sqlite_master
      ["dbTableSelect", "dbTableInput", "dbWhere"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = false; const row = el.closest('.row'); if (row) row.style.display = ''; } });
    } catch (e) {
      ["dbHost", "dbPort", "dbName", "dbUser", "dbPassword"].forEach((id) => { const el = document.getElementById(id); if (el) el.disabled = true; });
    }
    // set dbPath label/placeholder for sqlite
    try {
      const dbPathLabel = document.getElementById('dbPathLabel');
      const dbPathInput = document.getElementById('dbPath');
      if (dbPathLabel) dbPathLabel.textContent = 'SQLite 文件路径';
      if (dbPathInput) dbPathInput.placeholder = '例如 /data/db/mydb.sqlite';
    } catch (e) {}
  } else if (dbType === 'hdfs') {
    // HDFS: need host/port, user and a path; table/where are not applicable
    if (dbPathRow) dbPathRow.style.display = '';
    try {
      // show host/port/user rows
      ["dbHost", "dbPort", "dbUser"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = false; const row = el.closest('.row'); if (row) row.style.display = ''; el.style.display = ''; const lbl = el.previousElementSibling; if (lbl && lbl.tagName && lbl.tagName.toLowerCase() === 'label') lbl.style.display = ''; } });
      const hostEl = document.getElementById('dbHost');
      const portEl = document.getElementById('dbPort');
      const userEl = document.getElementById('dbUser');
      if (hostEl && (!hostEl.value || ['202.113.76.55', '192.168.1.181', '127.0.0.1', 'localhost'].includes(String(hostEl.value).trim()))) hostEl.value = '202.113.76.55';
      if (portEl && (!portEl.value || ['3306', '5432', '1433', '1521', '10000', '9090', '9870', '19090'].includes(String(portEl.value).trim()))) portEl.value = '9870';
      if (userEl && (!userEl.value || ['root', 'hive', 'admin'].includes(String(userEl.value).trim()))) userEl.value = 'hadoop';
      // HDFS 通常不需要密码字段，隐藏 password input 与其 label（仅隐藏，不移除 DOM）
      const pwdEl = document.getElementById('dbPassword');
      if (pwdEl) {
        pwdEl.disabled = true;
        pwdEl.style.display = 'none';
        const prev = pwdEl.previousElementSibling;
        if (prev && prev.tagName && prev.tagName.toLowerCase() === 'label') prev.style.display = 'none';
      }
      // hide DB name row (database not applicable)
      const dbNameEl = document.getElementById('dbName'); if (dbNameEl) { dbNameEl.disabled = true; const row = dbNameEl.closest('.row'); if (row) row.style.display = 'none'; }
      // hide table/list controls
      ["dbTableSelect", "dbTableInput", "dbWhere"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = true; const row = el.closest('.row'); if (row) row.style.display = 'none'; } });
      // update dbPath label/placeholder/default for HDFS
      const dbPathLabel = document.getElementById('dbPathLabel');
      const dbPathInput = document.getElementById('dbPath');
      if (dbPathLabel) dbPathLabel.textContent = 'HDFS 路径';
      if (dbPathInput) {
        dbPathInput.placeholder = '例如 / 或 /user/data/csv/';
        const currentPath = String(dbPathInput.value || '').trim();
        if (!currentPath || currentPath === '/nifi') dbPathInput.value = '/';
      }
    } catch (e) {
      // best-effort
    }
  } else if (dbType === 'hbase') {
    // HBase: use host/port (thrift), table selection required; dbName not used
    if (dbPathRow) dbPathRow.style.display = 'none';
    try {
      ["dbHost", "dbPort", "dbUser", "dbPassword"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = false; const row = el.closest('.row'); if (row) row.style.display = ''; } });
      const dbNameEl = document.getElementById('dbName'); if (dbNameEl) { dbNameEl.disabled = true; const row = dbNameEl.closest('.row'); if (row) row.style.display = 'none'; }
      // show table selector/input
      ["dbTableSelect", "dbTableInput"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = false; const row = el.closest('.row'); if (row) row.style.display = ''; } });
      // keep where as scan filter (show)
      const whereEl = document.getElementById('dbWhere'); if (whereEl) { whereEl.disabled = false; const row = whereEl.closest('.row'); if (row) row.style.display = ''; }
      // show row key prefix input for HBase (use wrap)
      const rkWrap = document.getElementById('hbaseRowKeyWrap');
      const rk = document.getElementById('hbaseRowKeyPrefix');
      if (rkWrap) rkWrap.style.display = '';
      if (rk) rk.disabled = false;
    } catch (e) {}
  } else {
    // default relational types (mysql, postgres, hive, mssql, oracle)
    if (dbPathRow) dbPathRow.style.display = 'none';
    try {
      ["dbHost", "dbPort"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = false; const row = el.closest('.row'); if (row) row.style.display = ''; } });
      ["dbName", "dbUser", "dbPassword"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = false; const row = el.closest('.row'); if (row) row.style.display = ''; el.style.display = ''; const lbl = el.previousElementSibling; if (lbl && lbl.tagName && lbl.tagName.toLowerCase() === 'label') lbl.style.display = ''; } });
      ["dbTableSelect", "dbTableInput", "dbWhere"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = false; const row = el.closest('.row'); if (row) row.style.display = ''; } });
      // hide hbase row key prefix for non-HBase types
      const rkWrap = document.getElementById('hbaseRowKeyWrap');
      const rk = document.getElementById('hbaseRowKeyPrefix');
      if (rkWrap) rkWrap.style.display = 'none';
      if (rk) rk.disabled = true;
    } catch (e) {
      ["dbHost", "dbPort", "dbName", "dbUser", "dbPassword"].forEach((id) => { const el = document.getElementById(id); if (el) el.disabled = false; });
    }
  }

  // DSN priority: if DSN present, disable other connection fields
  if (dsn) {
    ["dbHost", "dbPort", "dbUser", "dbPassword", "dbName", "dbPath"].forEach((id) => { const el = document.getElementById(id); if (el) el.disabled = true; });
    const hint = document.getElementById("dbConnStatus"); if (hint) hint.textContent = "已检测到 DSN，其他连接字段已禁用（DSN 优先）";
  } else {
    ["dbHost", "dbPort", "dbUser", "dbPassword", "dbName", "dbPath"].forEach((id) => { const el = document.getElementById(id); if (el) el.disabled = false; });
    const hint = document.getElementById("dbConnStatus"); if (hint) hint.textContent = "";
  }

  // update port placeholder based on dbType when port input exists
  try {
    const portEl = document.getElementById('dbPort');
    if (portEl) {
      const def = DEFAULT_PORTS[dbType] !== undefined ? DEFAULT_PORTS[dbType] : '';
      // set placeholder, but do not overwrite explicit user value
      portEl.placeholder = def ? String(def) : '';
    }
  } catch (e) {
    // ignore
  }
}

// When dbType changes, if port is empty or currently equals another known default,
// update it to the default for the selected dbType so UI reflects expected port.
function syncPortWithDbType() {
  try {
    const dbType = (document.getElementById('dbType')?.value || 'mysql').toLowerCase();
    const portEl = document.getElementById('dbPort');
    if (!portEl) return;
    const cur = (String(portEl.value || '').trim());
    const known = Object.values(DB_TYPE_DEFAULTS).map((item) => String(item.port || '')).filter(Boolean);
    const def = DB_TYPE_DEFAULTS[dbType]?.port ? String(DB_TYPE_DEFAULTS[dbType].port) : '';
    if (cur === '' || known.includes(cur)) {
      if (def) portEl.value = def; else portEl.value = '';
    }
  } catch (e) {
    // ignore
  }
}

// bind dynamic behavior: bind immediately and perform initial render
{
  const dbTypeEl = document.getElementById('dbType');
  const dsnEl = document.getElementById('dbDSN');
  if (dbTypeEl) dbTypeEl.addEventListener('change', renderDbFields);
  if (dsnEl) dsnEl.addEventListener('input', renderDbFields);
  if (dbTypeEl) dbTypeEl.addEventListener('change', syncPortWithDbType);
  // initial render
  try {
    renderDbFields();
  } catch (e) {
    // swallow errors during initial render to avoid blocking page
    console.warn('renderDbFields initial render failed', e);
  }
}
// sync port when dbType changes (call once during initial bind)
try { syncPortWithDbType(); } catch (e) { /* ignore */ }

function getSelectedTableForSchedule() {
  const select = document.getElementById("dbTableSelect");
  const input = document.getElementById("dbTableInput");
  return (select && select.value) || (input && input.value.trim()) || "";
}

function setScheduleMessage(text, isError = false) {
  const el = document.getElementById("dbScheduleResult");
  if (!el) return;
  el.style.color = isError ? "#b91c1c" : "#0f766e";
  el.textContent = text;
}

function formatScheduleTime(ts) {
  if (!ts) return "-";
  try {
    let normalized = String(ts).trim();
    if (/^\d{4}-\d{2}-\d{2}T/.test(normalized) && !/(Z|[+-]\d{2}:?\d{2})$/.test(normalized)) {
      normalized = `${normalized}Z`;
    }
    const d = new Date(normalized);
    if (Number.isNaN(d.getTime())) return String(ts);
    return d.toLocaleString("zh-CN", { hour12: false });
  } catch (_) {
    return String(ts);
  }
}

function buildSchedulePayload() {
  const table = getSelectedTableForSchedule();
  const format = (document.getElementById("dbExportFormat")?.value || "CSV").toLowerCase();
  const spec = getScheduleSpecFromUi();
  if (!table) {
    throw new Error("请先选择或输入表名");
  }
  if (!spec) {
    throw new Error("请选择频率或填写 cron 表达式");
  }
  const payload = { table };
  // 添加标签配置参数
  const hasTag = document.getElementById("dbHasTag")?.value;
  if (hasTag) {
    payload.hasTag = hasTag === "true";
    if (hasTag === "true") {
      const tc = (document.getElementById("dbTagColumn")?.value || "").trim();
      const tn = (document.getElementById("dbTagName")?.value || "").trim();
      const tr = (document.getElementById("dbTagRangeWithLabel")?.value || "").trim();
      const ft = (document.getElementById("dbFailedTagWithLabel")?.value || "").trim();
      if (tc) payload.tagColumn = tc;
      if (tn) payload.tagName = tn;
      if (tr) payload.tagRange = tr;
      if (ft) payload.failedTag = ft;
    } else {
      const tr = (document.getElementById("dbTagRange")?.value || "").trim();
      const ft = (document.getElementById("dbFailedTag")?.value || "").trim();
      if (tr) payload.tagRange = tr;
      if (ft) payload.failedTag = ft;
    }
  }
  const cid = (document.getElementById("dbCategoryId")?.value || "").trim();
  const cname = (document.getElementById("dbCategoryName")?.value || "").trim();
  const desc = (document.getElementById("dbDescription")?.value || "").trim();
  const dsName = (document.getElementById("dbExportDatasetName")?.value || "").trim();
  if (cid) payload.categoryId = cid;
  if (cname) payload.categoryName = cname;
  if (dsName) payload.datasetName = dsName;
  if (desc) payload.description = desc;
  return {
    job_name: `db_export_${table}_${Date.now()}`,
    username: _currentUsername(),
    factory_id: DEFAULT_FACTORY_ID,
    owner_id: (state.currentUser && state.currentUser.username) || "user-001",
    schedule: spec,
    file_format: format,
    enabled: true,
    mode: "visible",
    destination: { type: "local", path: "" },
    db_config: collectDbConfigForSchedule(),
    payload,
  };
}

async function createScheduledExportFromDb() {
  try {
    const payload = buildSchedulePayload();
    setScheduleMessage("创建定时任务中...");
    const res = await exportJobsApi("/export-jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (res.code !== 0) {
      setScheduleMessage(`创建失败: ${res.message || "未知错误"}`, true);
      return;
    }
    setScheduleMessage(`创建成功，任务ID: ${res.data?.id}`);
    await loadExportJobsForModal();
  } catch (e) {
    setScheduleMessage(`创建失败: ${e.message || e}`, true);
  }
}

function openScheduleModal() {
  const modal = document.getElementById("scheduleModal");
  if (!modal) return;
  modal.style.display = "block";
  document.body.classList.add("modal-open");
  loadExportJobsForModal().catch(() => {});
}

function closeScheduleModal() {
  const modal = document.getElementById("scheduleModal");
  if (!modal) return;
  modal.style.display = "none";
  document.body.classList.remove("modal-open");
}

function renderExportJobsRows(list) {
  const tbody = document.getElementById("scheduleTableBody");
  if (!tbody) return;
  tbody.innerHTML = "";
  list.forEach((job) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${job.id ?? "-"}</td>
      <td>${job.job_name || "-"}</td>
      <td>${job.username || job.factory_id || DEFAULT_USERNAME}</td>
      <td>${job.schedule || "-"}</td>
      <td>${(job.file_format || "-").toUpperCase()}</td>
      <td>${job.enabled ? "是" : "否"}</td>
      <td>${formatScheduleTime(job.created_at)}</td>
      <td>
        <div class="schedule-actions">
          <button class="tiny-btn" data-action="toggle" data-id="${job.id}" data-enabled="${job.enabled ? "1" : "0"}">${job.enabled ? "禁用" : "启用"}</button>
          <button class="tiny-btn" data-action="trigger" data-id="${job.id}">触发</button>
          <button class="tiny-btn secondary" data-action="delete" data-id="${job.id}">删除</button>
        </div>
      </td>
    `;
    tbody.appendChild(tr);
  });

  tbody.querySelectorAll("button[data-action]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = btn.dataset.id;
      const action = btn.dataset.action;
      if (!id || !action) return;
      if (action === "toggle") {
        const enabled = btn.dataset.enabled !== "1";
        await patchExportJob(id, { enabled });
      } else if (action === "trigger") {
        await triggerExportJob(id);
      } else if (action === "delete") {
        await deleteExportJob(id);
      }
      await loadExportJobsForModal();
    });
  });
}

async function loadExportJobsForModal() {
  const q = new URLSearchParams({ username: _currentUsername(), factory_id: DEFAULT_FACTORY_ID });
  const res = await exportJobsApi(`/export-jobs?${q.toString()}`, { method: "GET" });
  if (res.code !== 0) {
    setScheduleMessage(`加载任务失败: ${res.message || "未知错误"}`, true);
    return;
  }
  renderExportJobsRows(res.data || []);
}

async function patchExportJob(id, patch) {
  const res = await exportJobsApi(`/export-jobs/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  if (res.code !== 0) {
    setScheduleMessage(`更新失败: ${res.message || "未知错误"}`, true);
    return;
  }
  setScheduleMessage(`任务 ${id} 已更新`);
}

async function deleteExportJob(id) {
  const res = await exportJobsApi(`/export-jobs/${id}`, { method: "DELETE" });
  if (res.code !== 0) {
    setScheduleMessage(`删除失败: ${res.message || "未知错误"}`, true);
    return;
  }
  setScheduleMessage(`任务 ${id} 已删除`);
}

async function triggerExportJob(id) {
  const detail = await exportJobsApi(`/export-jobs/${id}`, { method: "GET" });
  if (detail.code !== 0 || !detail.data) {
    setScheduleMessage(`获取任务详情失败: ${detail.message || "未知错误"}`, true);
    return;
  }
  const body = {
    id: detail.data.id,
    job_name: detail.data.job_name,
    factory_id: detail.data.factory_id || detail.data.username || DEFAULT_USERNAME,
    owner_id: detail.data.owner_id,
    db_config: detail.data.db_config,
    file_format: detail.data.file_format,
    destination: detail.data.destination,
    payload: detail.data.payload || { table: getSelectedTableForSchedule() },
  };
  const res = await exportJobsApi("/export-jobs/trigger", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.code !== 0) {
    setScheduleMessage(`触发失败: ${res.message || "未知错误"}`, true);
    return;
  }
  setScheduleMessage(`任务 ${id} 触发成功`);
}

function bindEvents() {
  document.getElementById("loadFilesBtn").addEventListener("click", loadFiles);
  const purgeMissingBtn = document.getElementById("purgeMissingBtn");
  if (purgeMissingBtn) purgeMissingBtn.addEventListener("click", purgeMissingFiles);
  const expandAllDirsBtn = document.getElementById("expandAllDirsBtn");
  const collapseAllDirsBtn = document.getElementById("collapseAllDirsBtn");
  if (expandAllDirsBtn) expandAllDirsBtn.addEventListener("click", expandAllDirs);
  if (collapseAllDirsBtn) collapseAllDirsBtn.addEventListener("click", collapseAllDirs);
  document.getElementById("loadRulesBtn").addEventListener("click", loadTagRules);
  document.getElementById("autoTagBtn").addEventListener("click", triggerAutoTag);
  const addTagRuleBtn = document.getElementById("addTagRuleBtn");
  if (addTagRuleBtn) addTagRuleBtn.addEventListener("click", addTagRuleConditionRow);
  const backendLocalBtn = document.getElementById("backendLocalBtn");
  const backendNifiBtn = document.getElementById("backendNifiBtn");
  if (backendLocalBtn) backendLocalBtn.addEventListener("click", () => setBackendMode("local"));
  if (backendNifiBtn) backendNifiBtn.addEventListener("click", () => setBackendMode("nifi"));

  // 训练中心
  const loadTrainingFilesBtn = document.getElementById("loadTrainingFilesBtn");
  const submitTrainingBtn = document.getElementById("submitTrainingBtn");
  const selectAllTraining = document.getElementById("selectAllTraining");
  if (loadTrainingFilesBtn) loadTrainingFilesBtn.addEventListener("click", loadTrainingFiles);
  if (submitTrainingBtn) submitTrainingBtn.addEventListener("click", submitTraining);
  if (selectAllTraining) {
    selectAllTraining.addEventListener("change", () => {
      document.querySelectorAll("#trainingFilesTable .training-checkbox").forEach((cb) => {
        cb.checked = selectAllTraining.checked;
      });
    });
  }
  ["trainingCategoryFilter", "trainingTagFilter", "trainingKeywordFilter"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) {
      el.addEventListener("change", loadTrainingFiles);
      el.addEventListener("keyup", (e) => { if (e.key === "Enter") loadTrainingFiles(); });
    }
  });

  // v4: 退出登录
  const logoutBtn = document.getElementById("logoutBtn");
  if (logoutBtn) logoutBtn.addEventListener("click", doLogout);

  // v4: 显示当前用户
  try { renderCurrentUserLabel(); } catch (e) {}

  // DB export UI
  const dbTestBtn = document.getElementById("dbTestBtn");
  const dbListBtn = document.getElementById("dbListBtn");
  const dbExportBtn = document.getElementById("dbExportBtn");
  const dbCreateScheduleBtn = document.getElementById("dbCreateScheduleBtn");
  const dbManageSchedulesBtn = document.getElementById("dbManageSchedulesBtn");
  const scheduleCloseBtn = document.getElementById("scheduleModalCloseBtn");
  const scheduleBackdrop = document.getElementById("scheduleModalBackdrop");
  const scheduleRefreshBtn = document.getElementById("scheduleRefreshBtn");
  if (dbTestBtn) dbTestBtn.addEventListener("click", testDbConnection);
  if (dbListBtn) dbListBtn.addEventListener("click", listTables);
  if (dbExportBtn) dbExportBtn.addEventListener("click", exportFromDb);
  if (dbCreateScheduleBtn) dbCreateScheduleBtn.addEventListener("click", createScheduledExportFromDb);
  if (dbManageSchedulesBtn) dbManageSchedulesBtn.addEventListener("click", openScheduleModal);
  if (scheduleCloseBtn) scheduleCloseBtn.addEventListener("click", closeScheduleModal);
  if (scheduleBackdrop) scheduleBackdrop.addEventListener("click", closeScheduleModal);
  if (scheduleRefreshBtn) scheduleRefreshBtn.addEventListener("click", () => {
    loadExportJobsForModal().catch(() => {});
  });

  // 标签配置 UI 联动
  const dbHasTag = document.getElementById("dbHasTag");
  if (dbHasTag) {
    dbHasTag.addEventListener("change", () => {
      const val = dbHasTag.value;
      const withLabel = document.getElementById("dbTagWithLabel");
      const withoutLabel = document.getElementById("dbTagWithoutLabel");
      if (withLabel) withLabel.style.display = val === "true" ? "" : "none";
      if (withoutLabel) withoutLabel.style.display = val === "false" ? "" : "none";
    });
  }
  const dbTagRange = document.getElementById("dbTagRange");
  if (dbTagRange) {
    dbTagRange.addEventListener("input", () => {
      // 故障标签改为手动输入，不再自动填充选项
    });
  }

  const uploadHasTag = document.getElementById("uploadHasTag");
  if (uploadHasTag) {
    uploadHasTag.addEventListener("change", () => {
      const val = uploadHasTag.value;
      const withLabel = document.getElementById("uploadTagWithLabel");
      const withoutLabel = document.getElementById("uploadTagWithoutLabel");
      if (withLabel) withLabel.style.display = val === "true" ? "" : "none";
      if (withoutLabel) withoutLabel.style.display = val === "false" ? "" : "none";
    });
  }
  const uploadTagRange = document.getElementById("uploadTagRange");
  if (uploadTagRange) {
    uploadTagRange.addEventListener("input", () => {
      // 故障标签改为手动输入，不再自动填充选项
    });
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
      // 存储用户信息到 localStorage 供后续使用
      try { localStorage.setItem('currentUser', JSON.stringify(window.currentUser)); } catch (e) {}
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

  // v4: 不读取 localStorage，直接从后端获取当前用户的模式配置
  // 避免 localStorage 中持久化的旧值导致 UI 闪现错误的模式
  try {
    const res = await getBackendModeState();
    if (res && res.code === 0 && res.data && res.data.mode) {
      state.backendMode = res.data.mode === "nifi" ? "nifi" : "local";
      try { localStorage.setItem(BACKEND_MODE_STORAGE_KEY, state.backendMode); } catch (_) {}
    }
  } catch (_) {}
  updateBackendToggleUI();
  bindEvents();
  loadFiles();
  loadTagRules();

  // 显示内部管理链接（仅管理员）
  if (state.currentUser && state.currentUser.is_admin) {
    const a = document.getElementById('internalLink');
    if(a) a.style.display = '';
  }
}

async function testDbConnection() {
  const dbType = (document.getElementById("dbType")?.value || "mysql").toLowerCase();
  const defaults = DB_TYPE_DEFAULTS[dbType] || DB_TYPE_DEFAULTS.mysql;
  const host = document.getElementById("dbHost").value.trim() || defaults.host;
  const port = Number(document.getElementById("dbPort").value || defaults.port || 3306);
  const database = (dbType === 'hdfs' || dbType === 'sqlite')
    ? (document.getElementById("dbPath")?.value.trim() || (dbType === 'hdfs' ? '/' : ''))
    : (document.getElementById("dbName").value.trim() || defaults.database || 'default');
  const username = document.getElementById("dbUser").value.trim() || defaults.user;
  const password = document.getElementById("dbPassword").value;
  const status = document.getElementById("dbConnStatus");
  status.textContent = "测试中...";
  const payload = { db_type: dbType, host, port, username, password, database };
  const res = await api("/db/test-connection", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (res.code === 0) {
    status.textContent = "连接成功";
  } else {
    status.textContent = `连接失败: ${res.message} ${res.detail || ""}`;
  }
}

async function listTables() {
  const dbType = (document.getElementById("dbType")?.value || "mysql").toLowerCase();
  const defaults = DB_TYPE_DEFAULTS[dbType] || DB_TYPE_DEFAULTS.mysql;
  const host = document.getElementById("dbHost").value.trim() || defaults.host;
  const port = Number(document.getElementById("dbPort").value || defaults.port || 3306);
  const database = (dbType === 'hdfs' || dbType === 'sqlite')
    ? (document.getElementById("dbPath")?.value.trim() || (dbType === 'hdfs' ? '/' : ''))
    : (document.getElementById("dbName").value.trim() || defaults.database || 'default');
  const username = document.getElementById("dbUser").value.trim() || defaults.user;
  const password = document.getElementById("dbPassword").value;
  const status = document.getElementById("dbConnStatus");
  status.textContent = "列出表中...";
  const payload = { db_type: dbType, host, port, username, password, database };
  const res = await api("/db/list-tables", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (res.code === 0) {
    const select = document.getElementById("dbTableSelect");
    select.innerHTML = "";
    (res.data || []).forEach((t) => {
      const op = document.createElement("option");
      op.value = t;
      op.textContent = t;
      select.appendChild(op);
    });
    status.textContent = `找到 ${res.data?.length || 0} 个表`;
  } else {
    status.textContent = `列出表失败: ${res.message} ${res.detail || ""}`;
  }
}

async function exportFromDb() {
  const conf = collectDbConfigForSchedule();
  const where = document.getElementById("dbWhere")?.value.trim() || "";
  const table = conf.table || getSelectedTableForSchedule();
  const format = document.getElementById("dbExportFormat").value || "CSV";
  const append_to_latest = !!document.getElementById("dbAppendLatest").checked;
  const result = document.getElementById("dbExportResult");
  const exportBtn = document.getElementById("dbExportBtn");
  if (!table) {
    result.textContent = "请先选择或输入表名";
    return;
  }
  result.textContent = "导出中...";
  if (exportBtn) exportBtn.disabled = true;
  try {
    const payload = {
      db_config: {
        db_type: conf.db_type,
        user: conf.user,
        password: conf.password,
        host: conf.host,
        port: conf.port,
        database: conf.database,
        path: conf.path,
        dsn: conf.dsn,
        table: table,
        row_key_prefix: conf.row_key_prefix,
      },
      format: format,
        append_to_latest: append_to_latest,
        owner_id: (state.currentUser && state.currentUser.username) || "user-001",
      where: where,
    };

    // 添加标签配置参数
    const hasTag = document.getElementById("dbHasTag")?.value;
    if (hasTag) {
      payload.hasTag = hasTag === "true";
      if (hasTag === "true") {
        const tc = (document.getElementById("dbTagColumn")?.value || "").trim();
        const tn = (document.getElementById("dbTagName")?.value || "").trim();
        const tr = (document.getElementById("dbTagRangeWithLabel")?.value || "").trim();
        const ft = (document.getElementById("dbFailedTagWithLabel")?.value || "").trim();
        if (tc) payload.tagColumn = tc;
        if (tn) payload.tagName = tn;
        if (tr) payload.tagRange = tr;
        if (ft) payload.failedTag = ft;
      } else {
        const tr = (document.getElementById("dbTagRange")?.value || "").trim();
        const ft = (document.getElementById("dbFailedTag")?.value || "").trim();
        if (tr) payload.tagRange = tr;
        if (ft) payload.failedTag = ft;
      }
    }
    // 补充信息
    const cid = (document.getElementById("dbCategoryId")?.value || "").trim();
    const cname = (document.getElementById("dbCategoryName")?.value || "").trim();
    const desc = (document.getElementById("dbDescription")?.value || "").trim();
    if (cid) payload.categoryId = cid;
    if (cname) payload.categoryName = cname;
    if (desc) payload.description = desc;

    const res = await api(`/export`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (res && res.code === 0) {
      // prefer returned file meta
      if (res.data && res.data.file) {
        result.textContent = `导出成功: ${res.data.file.fileName}，路径: ${res.data.file.storagePath}`;
      } else if (res.data && res.data.path) {
        result.textContent = `导出成功，路径: ${res.data.path}`;
      } else {
        result.textContent = `导出成功`;
      }
      // 文件列表刷新放到下一轮事件循环，避免页面长时间停留在“导出中...”
      setTimeout(() => {
        loadFiles().catch(() => {});
      }, 0);
    } else {
      const message = res?.message || "未知导出错误";
      const detail = res?.data?.detail || res?.detail || "";
      result.textContent = `导出失败: ${message}${detail ? ` ${detail}` : ""}`;
    }
  } catch (error) {
    result.textContent = `导出异常: ${error?.message || error}`;
  } finally {
    if (exportBtn) exportBtn.disabled = false;
  }
}

init();

// 文件上传逻辑
const fileInput = document.getElementById("fileInput");
const uploadFormat = document.getElementById("uploadFormat");
const uploadColumns = document.getElementById("uploadColumns");
const uploadBtn = document.getElementById("uploadBtn");
const uploadResult = document.getElementById("uploadResult");
if (fileInput && uploadBtn && uploadResult) {
  uploadBtn.onclick = async () => {
    if (!fileInput.files || fileInput.files.length === 0) {
      uploadResult.textContent = "请先选择文件";
      return;
    }
    const file = fileInput.files[0];
    const formData = new FormData();
    formData.append("file", file);
    const sourceType = uploadFormat ? uploadFormat.value : "CSV";
    const username = (state.currentUser && state.currentUser.username) || "user";
    const columns = uploadColumns ? uploadColumns.value.trim() : "";
    const uploadMapping = {
      CSV: { endpoint: "/upload/inbox_csv", convertType: "csv_to_json" },
      JSON: { endpoint: "/upload/inbox_json", convertType: "json_to_csv" },
      CSV_TO_TSV: { endpoint: "/upload/inbox_csv", convertType: "csv_to_tsv" },
      TSV_TO_CSV: { endpoint: "/upload/inbox_tsv", convertType: "tsv_to_csv" },
      JSON_TO_TSV: { endpoint: "/upload/inbox_json", convertType: "json_to_tsv" },
      TSV_TO_JSON: { endpoint: "/upload/inbox_tsv", convertType: "tsv_to_json" },
    };
    const selected = uploadMapping[sourceType] || uploadMapping.CSV;
    uploadResult.textContent = "上传中...";
    try {
      // CSV/TSV may be headerless; require explicit columns when first line does not look like header names.
      if ((sourceType === "CSV" || sourceType === "CSV_TO_TSV" || sourceType === "TSV_TO_CSV" || sourceType === "TSV_TO_JSON") && !columns) {
        const text = await file.text();
        const delimiter = sourceType.startsWith("TSV") ? "\t" : ",";
        const firstLine = (text.split(/\r?\n/).find((ln) => ln.trim().length > 0) || "").trim();
        const headerCells = firstLine ? firstLine.split(delimiter).map((x) => x.trim()) : [];
        const looksLikeHeader = headerCells.length >= 2 && headerCells.every((cell) => /[A-Za-z_\u4e00-\u9fa5]/.test(cell) && !/^\d+(\.\d+)?$/.test(cell));
        if (!looksLikeHeader) {
          uploadResult.textContent = "检测到文件可能无表头，请在“无表头时列名”中填写逗号分隔列名后再上传。";
          return;
        }
      }
      const query = new URLSearchParams({
        username,
        convertType: selected.convertType,
      });
      if (columns) {
        query.set("columns", columns);
      }

      // 添加标签配置参数
      const upHasTag = (document.getElementById("uploadHasTag")?.value || "").trim();
      if (upHasTag) {
        query.set("hasTag", upHasTag);
        if (upHasTag === "true") {
          const tc = (document.getElementById("uploadTagColumn")?.value || "").trim();
          const tn = (document.getElementById("uploadTagName")?.value || "").trim();
          const tr = (document.getElementById("uploadTagRangeWithLabel")?.value || "").trim();
          const ft = (document.getElementById("uploadFailedTagWithLabel")?.value || "").trim();
          if (tc) query.set("tagColumn", tc);
          if (tn) query.set("tagName", tn);
          if (tr) query.set("tagRange", tr);
          if (ft) query.set("failedTag", ft);
        } else {
          const tr = (document.getElementById("uploadTagRange")?.value || "").trim();
          const ft = (document.getElementById("uploadFailedTag")?.value || "").trim();
          if (tr) query.set("tagRange", tr);
          if (ft) query.set("failedTag", ft);
        }
      }
      // 补充信息
      const upCid = (document.getElementById("uploadCategoryId")?.value || "").trim();
      const upCname = (document.getElementById("uploadCategoryName")?.value || "").trim();
      const upDesc = (document.getElementById("uploadDescription")?.value || "").trim();
      const upDs = (document.getElementById("uploadDatasetName")?.value || "").trim();
      if (upCid) query.set("categoryId", upCid);
      if (upCname) query.set("categoryName", upCname);
      if (upDs) query.set("datasetName", upDs);
      if (upDesc) query.set("description", upDesc);

      const data = await requestJson(`${selected.endpoint}?${query.toString()}`, {
        method: "POST",
        body: formData,
      });
      if (data.code === 0) {
        const sourcePath = data?.data?.sourcePath || data?.data?.storagePath || "";
        const targetPath = data?.data?.targetPath || "";
        const convertedFileId = data?.data?.convertedFileId || "";
        const isNifi = data?.data?.status === "PENDING" || data?.data?.mode === "nifi";
        const convertType = selected.convertType || "csv_to_json";
        const msgLines = [
          `上传成功，已执行 ${convertType}，原文件 fileId: ${data.data.fileId}`,
          sourcePath ? `上传文件路径: ${sourcePath}` : "上传文件路径: （后端未返回）",
          targetPath
            ? `转换文件路径: ${targetPath}${convertedFileId ? ` ｜ 新 fileId: ${convertedFileId}` : ""}`
            : (isNifi
                ? `转换状态: 已提交到 NiFi 容器异步处理，约 30 秒后完成。产物在 tagged_output/${convertType.split("_")[0]}_to_*/ 目录下。`
                : (sourcePath
                  ? `转换文件路径: （转换未生成目标文件，请检查源内容是否为空/格式错误）`
                  : "转换文件路径: （后端未返回）")),
        ];
        uploadResult.innerHTML = msgLines.join("<br>");
        await loadFiles();
      } else {
        uploadResult.textContent = `上传失败：${data.message}${data.detail ? `（${data.detail}）` : ""}`;
      }
    } catch (e) {
      uploadResult.textContent = `上传异常：${e}`;
    }
  };
}

// === v4: 登录相关辅助 ===
async function doLogout() {
  if (!confirm("确定退出登录？")) return;
  try {
    await fetch("/api/v1/auth/logout", { method: "POST", credentials: "include" });
  } catch (e) {
    // ignore
  }
  try { localStorage.removeItem(BACKEND_MODE_STORAGE_KEY); } catch (_) {}
  window.location.href = "/login.html";
}

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
  const WARNING_BEFORE_SEC = 5 * 60; // 5 分钟
  if (remainingSec <= WARNING_BEFORE_SEC) {
    // 已经快过期了，直接提示
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