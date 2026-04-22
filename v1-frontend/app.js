const config = window.APP_CONFIG;
const DEFAULT_FACTORY_ID = config.FACTORY_ID || "factory-001";

const KNOWN_NIFI_DIRS = [
  "/home/yhz/nifi-data/output_csv",
  "/home/yhz/nifi-data/output_json",
  "/home/yhz/nifi-data/output_tsv",
  "/home/yhz/nifi-data/inbox_csv",
  "/home/yhz/nifi-data/inbox_json",
  "/home/yhz/nifi-data/inbox_tsv",
  "/home/yhz/nifi-data/csv_to_json",
  "/home/yhz/nifi-data/json_to_csv",
  "/home/yhz/nifi-data/tsv_to_json",
  "/home/yhz/nifi-data/json_to_tsv",
  "/home/yhz/nifi-data/csv_to_tsv",
  "/home/yhz/nifi-data/tsv_to_csv",
  "/home/yhz/nifi-data/tagged_output",
];

const state = {
  jobs: [],
  currentJobId: null,
  pollTimer: null,
  allFiles: [],
  files: [],
  rootDir: "/home/yhz/nifi-data",
  currentDir: "/home/yhz/nifi-data",
  dirChildren: {},
  expandedDirs: { "/home/yhz/nifi-data": true },
  selectedFileId: null,
  previewColumns: [],
  previewRows: [],
  previewOffset: 0,
  tagRules: [],
  currentUser: null,
};

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

function api(path, options = {}) {
  if (config.USE_MOCK_API) {
    return mockApi(path, options);
  }
  const fetchOptions = Object.assign({ credentials: 'same-origin' }, options || {});
  return fetch(`${config.API_BASE}${path}`, fetchOptions).then((r) => r.json());
}

function _normalizeBase(base) {
  const b = String(base || "").trim();
  if (!b) return "";
  return b.startsWith("/") ? b : `/${b}`;
}

async function exportJobsApi(path, options = {}) {
  const apiBaseNormalized = _normalizeBase(config.API_BASE || "");
  const apiBaseNoV1 = apiBaseNormalized.replace(/\/v1\/?$/, "");
  const apiBaseRelative = apiBaseNormalized.replace(/^\//, "");
  const apiBaseNoV1Relative = apiBaseNoV1.replace(/^\//, "");
  const candidates = [];

  const absoluteOrigins = [
    // Current backend default in this project.
    "http://127.0.0.1:8081",
    "http://localhost:8081",
  ];

  // Prefer /api/v1 first because frontend proxy only forwards /api/v1 POST.
  ["/api/v1", "api/v1", apiBaseNormalized, apiBaseRelative, "/api", "api", apiBaseNoV1, apiBaseNoV1Relative].forEach((base) => {
    if (!base) return;
    if (!candidates.includes(base)) {
      candidates.push(base);
    }
  });

  // Also try absolute API endpoints to avoid hitting static web server routes (405 HTML).
  absoluteOrigins.forEach((origin) => {
    ["/api", "/api/v1", apiBaseNoV1 || "/api", apiBaseNormalized || "/api/v1"].forEach((base) => {
      const merged = `${origin}${base.startsWith("/") ? base : `/${base}`}`;
      if (!candidates.includes(merged)) {
        candidates.push(merged);
      }
    });
  });

  let lastError = null;
  for (const base of candidates) {
    try {
      const prefix = base.endsWith("/") ? base.slice(0, -1) : base;
      const r = await fetch(`${prefix}${path}`, options);
      const text = await r.text();
      let body = {};
      try {
        body = text ? JSON.parse(text) : {};
      } catch (_) {
        body = { raw: text };
      }

      if (!r.ok) {
        const msg = body?.detail || body?.message || body?.raw || `HTTP ${r.status}`;
        const contentType = (r.headers.get("content-type") || "").toLowerCase();
        const rawText = String(body?.raw || "");
        const looksLikeHtmlError = contentType.includes("text/html") || /<html|<!doctype/i.test(rawText);

        // HTML 404/405/501 usually means request hit static server/proxy mismatch; try next candidate.
        if (r.status === 404 || ((r.status === 405 || r.status === 501) && looksLikeHtmlError)) {
          lastError = { code: r.status, message: msg };
          continue;
        }
        return { code: r.status, message: msg, data: body?.data ?? null };
      }
      return body;
    } catch (e) {
      lastError = { code: -1, message: e?.message || String(e) };
    }
  }

  return {
    code: lastError?.code || -1,
    message: `request failed: ${lastError?.message || "no reachable export-jobs endpoint"}`,
    data: null,
  };
}

function mockApi(path, options = {}) {
  if (path === "/jobs" && options.method === "POST") {
    const id = `job_mock_${Date.now()}`;
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: { jobId: id, status: "PENDING", createdAt: new Date().toISOString() } });
  }
  if (path.startsWith("/jobs/") && path.endsWith("/outputs")) {
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: [] });
  }
  if (path.startsWith("/jobs/")) {
    const jobId = path.split("/")[2];
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: { jobId, jobType: "CONVERT", status: "RUNNING", progress: 60, source: {}, target: {}, createdAt: new Date().toISOString() } });
  }
  if (path.startsWith("/jobs")) {
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: { total: 0, pageNo: 1, pageSize: 20, rows: [] } });
  }
  if (path.startsWith("/files")) {
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: { total: 0, pageNo: 1, pageSize: 20, rows: [] } });
  }
  if (path.startsWith("/tags/rules")) {
    return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: [{ ruleId: "NIFI_RULE_ID_V5", ruleName: "Mock rule", ruleVersion: "v1", enabled: true }] });
  }
  return Promise.resolve({ code: 0, message: "OK", traceId: "mock", data: null });
}

function renderJobs() {
  const tbody = document.getElementById("jobTable");
  tbody.innerHTML = "";
  state.jobs.forEach((job) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${job.jobId}</td>
      <td>${job.jobType}</td>
      <td><span class="status ${job.status}">${job.status}</span></td>
      <td>${job.progress ?? 0}%</td>
      <td><button class="secondary" data-id="${job.jobId}">查看</button></td>
    `;
    tr.querySelector("button").addEventListener("click", () => loadJobDetail(job.jobId));
    tbody.appendChild(tr);
  });
}

async function loadJobs() {
  const res = await api("/jobs");
  if (res.code !== 0) return;
  state.jobs = res.data.rows;
  renderJobs();
}

async function createJob() {
  const jobType = document.getElementById("jobType").value;
  const format = document.getElementById("format").value;
  const payload = {
    jobType,
    source: { sourceType: "MYSQL_TABLE", tableName: "sensor_data" },
    target: { format, outputDir: "/data/jobs" },
    tagConfig: { mode: "AUTO", ruleId: "NIFI_RULE_ID_V5" },
    copyFormats: [],
    cron: "",
    runBy: (state.currentUser && state.currentUser.username) || "user-001",
    remark: "created from v1 frontend",
  };

  const res = await api("/jobs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const msg = document.getElementById("createResult");
  if (res.code !== 0) {
    msg.textContent = `创建失败: ${res.message}`;
    return;
  }
  msg.textContent = `创建成功: ${res.data.jobId}`;
  await loadJobs();
  await loadJobDetail(res.data.jobId);
}

async function loadJobDetail(jobId) {
  state.currentJobId = jobId;
  const res = await api(`/jobs/${jobId}`);
  if (res.code !== 0) return;
  const job = res.data;
  let detail = `${job.jobId} | ${job.status} | ${job.progress}%`;
  if (job.nifiFlowId) {
    detail += `\nflowId: ${job.nifiFlowId}`;
  }
  if (typeof job.nifiRetryCount === "number") {
    detail += `\nretry: ${job.nifiRetryCount}`;
  }
  if (job.status === "FAILED") {
    const hint = toErrorHint(job.errorCode);
    if (job.errorCode) {
      detail += `\nerrorCode: ${job.errorCode}`;
    }
    if (hint) {
      detail += `\n提示: ${hint}`;
    }
    if (job.errorMessage) {
      detail += `\nerror: ${job.errorMessage}`;
    }
  }
  document.getElementById("jobDetail").textContent = detail;
  await loadOutputs(jobId);

  if (state.pollTimer) clearTimeout(state.pollTimer);
  if (["PENDING", "RUNNING"].includes(job.status)) {
    state.pollTimer = setTimeout(() => loadJobDetail(jobId), 3000);
  }
}

async function loadOutputs(jobId) {
  const res = await api(`/jobs/${jobId}/outputs`);
  const ul = document.getElementById("outputs");
  ul.innerHTML = "";
  if (res.code !== 0) return;
  res.data.forEach((f) => {
    const li = document.createElement("li");
    li.innerHTML = `${f.fileName} (${f.fileFormat}) - <a href="${config.API_BASE}/files/${f.fileId}/download" target="_blank">下载</a> <span class="small">路径: ${f.storagePath || ''}</span>`;
    ul.appendChild(li);
  });
}

function renderFiles() {
  const currentDirLabel = document.getElementById("currentDirLabel");
  if (currentDirLabel) {
    currentDirLabel.textContent = `当前目录: ${state.currentDir} （文件 ${state.files.length || 0} 个）`;
  }
  const tbody = document.getElementById("fileTable");
  tbody.innerHTML = "";
  state.files.forEach((file) => {
    const tr = document.createElement("tr");
    if (file.fileId === state.selectedFileId) {
      tr.classList.add("file-row-selected");
    }
    tr.innerHTML = `
      <td>${file.fileId}</td>
      <td>${file.fileName}</td>
      <td>${file.fileFormat}</td>
      <td>${file.fileSize}</td>
      <td>
        <button class="secondary" data-preview="${file.fileId}">预览</button>
        <a href="${config.API_BASE}/files/${file.fileId}/download" target="_blank">下载</a>
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
  const dirs = new Set([state.rootDir]);
  const norm = (p) => (p || "").replace(/\\/g, "/");

  KNOWN_NIFI_DIRS.forEach((dir) => {
    if (dir.startsWith(state.rootDir)) dirs.add(norm(dir));
  });

  files.forEach((f) => {
    const path = norm(f.storagePath);
    if (!path.startsWith(state.rootDir + "/")) return;
    let dir = path.slice(0, path.lastIndexOf("/"));
    while (dir.startsWith(state.rootDir)) {
      dirs.add(dir);
      if (dir === state.rootDir) break;
      const idx = dir.lastIndexOf("/");
      if (idx < state.rootDir.length) {
        dir = state.rootDir;
      } else {
        dir = dir.slice(0, idx);
      }
    }
  });

  dirs.forEach((d) => {
    children[d] = [];
  });
  dirs.forEach((d) => {
    if (d === state.rootDir) return;
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

  const renderNode = (dir, depth) => {
    const row = document.createElement("div");
    row.className = "tree-row";
    row.style.marginLeft = `${depth * 12}px`;
    const children = state.dirChildren[dir] || [];
    const hasChildren = children.length > 0;
    const isExpanded = state.expandedDirs[dir] !== false;

    const toggle = document.createElement("button");
    toggle.className = "tree-toggle";
    toggle.textContent = hasChildren ? (isExpanded ? "▼" : "▶") : "•";
    toggle.disabled = !hasChildren;
    if (hasChildren) {
      toggle.addEventListener("click", () => {
        state.expandedDirs[dir] = !isExpanded;
        renderDirectoryTree();
      });
    }

    const btn = document.createElement("button");
    btn.className = "tree-dir-btn";
    const name = dir === state.rootDir ? "nifi-data" : dir.split("/").pop();
    btn.textContent = name;
    if (dir === state.currentDir) {
      btn.classList.add("current");
    }
    btn.addEventListener("click", () => {
      state.currentDir = dir;
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

  renderNode(state.rootDir, 0);
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
  // Ensure backend rescans NiFi roots so deletions/additions are reflected
  try {
    await api("/internal/factory-tree/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ factory_id: DEFAULT_FACTORY_ID }),
    });
  } catch (e) {
    // ignore refresh errors — we'll still try to load files
    console.warn("refresh scan failed:", e);
  }
  const pageSize = 500;
  let pageNo = 1;
  const allRows = [];
  while (true) {
    const res = await api(`/files?nifiOnly=true&pageNo=${pageNo}&pageSize=${pageSize}`);
    if (res.code !== 0) return;
    const rows = res.data.rows || [];
    allRows.push(...rows);
    if (rows.length < pageSize) break;
    pageNo += 1;
  }
  state.allFiles = allRows;
  buildDirectoryState(state.allFiles);
  if (!state.currentDir || !state.dirChildren[state.currentDir]) {
    state.currentDir = state.rootDir;
  }
  renderDirectoryTree();
  filterFilesByCurrentDir();
}

async function previewFile(fileId) {
  state.selectedFileId = fileId;
  const offset = Number(document.getElementById("previewOffset").value || "0");
  const limit = Number(document.getElementById("previewLimit").value || "20");
  state.previewOffset = offset;
  const res = await api(`/files/${fileId}/preview?offset=${offset}&limit=${limit}`);
  const preview = document.getElementById("filePreview");
  if (res.code !== 0) {
    preview.textContent = `预览失败: ${res.message}`;
    return;
  }
    preview.textContent = JSON.stringify(res.data, null, 2);
    // 若为表格格式（columns/rows），展示可编辑表格（整表可编辑）
    const editable = document.getElementById("editablePreview");
    const wrapper = document.getElementById("editableTableWrapper");
    wrapper.innerHTML = "";
    document.getElementById("saveTagsResult").textContent = "";
    if (res.data && res.data.columns && Array.isArray(res.data.rows)) {
      state.previewColumns = [...res.data.columns];
      state.previewRows = res.data.rows.map((r) => [...r]);
      editable.style.display = "block";
      const table = document.createElement("table");
      table.className = "editable";
      const thead = document.createElement("thead");
      const headRow = document.createElement("tr");
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

      const tbody = document.createElement("tbody");
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
      });
      table.appendChild(tbody);
      wrapper.appendChild(table);
      // add '新增字段' handler: create new column at end
      const addFieldBtn = document.getElementById('addFieldBtn');
      if (addFieldBtn) {
        addFieldBtn.disabled = false;
        addFieldBtn.onclick = () => {
          const newColName = `new_field_${Date.now()}`;
          // update state and header
          state.previewColumns.push(newColName);
          const newTh = document.createElement('th');
          newTh.contentEditable = true;
          newTh.className = 'editable-header';
          // Keep the generated name as original so rename logic can remap cell column keys on save.
          newTh.dataset.original = newColName;
          newTh.dataset.colIndex = String(headRow.children.length);
          newTh.textContent = newColName;
          headRow.appendChild(newTh);
          // add empty cells to each existing row
          const tbody = table.querySelector('tbody');
          if (tbody) {
            Array.from(tbody.querySelectorAll('tr')).forEach((tr, rowIdx) => {
              const td = document.createElement('td');
              td.contentEditable = true;
              td.className = 'editable-cell';
              td.dataset.rowId = String(offset + rowIdx + 1);
              td.dataset.column = newColName;
              td.dataset.original = '';
              td.textContent = '';
              tr.appendChild(td);
            });
          }
        };
      }
    } else {
      editable.style.display = "none";
      state.previewColumns = [];
      state.previewRows = [];
    }
}

  // 刷新当前预览
  document.getElementById("previewReloadBtn").addEventListener("click", () => {
    if (state.selectedFileId) previewFile(state.selectedFileId);
  });

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

async function triggerAutoTag() {
  const fileId = document.getElementById("autoTagFileId").value.trim();
  const ruleId = document.getElementById("tagRuleSelect").value;
  const msg = document.getElementById("tagResult");
  if (!fileId) {
    msg.textContent = "请先输入 fileId";
    return;
  }
  const payload = { fileId, ruleId, outputFormat: "CSV", operator: (state.currentUser && state.currentUser.username) || "user-001" };
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
    msg.textContent = `自动标签完成: ${data.file.fileName}，路径: ${data.file.storagePath}，fileId: ${data.file.fileId}`;
  } else {
    msg.textContent = `自动标签任务已创建: ${data.jobId || "-"}`;
  }
  await loadJobs();
  if (data.jobId) {
    await loadJobDetail(data.jobId);
  }
  await loadFiles();
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
  // HIVE: 10000 (Thrift/Beeswax), HBASE: 9090 (Thrift), HDFS(WebHDFS): 9870/50070 (use 9870 modern)
  DEFAULT_PORTS.hive = 10000;
  DEFAULT_PORTS.hbase = 9090;
  DEFAULT_PORTS.hdfs = 9870;
  const conf = {
    db_type: db_type,
    host: document.getElementById("dbHost")?.value.trim() || "127.0.0.1",
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

// Render DB-specific fields and DSN-priority behavior
function renderDbFields() {
  const dbType = (document.getElementById("dbType")?.value || "mysql").toLowerCase();
  const dsn = (document.getElementById("dbDSN")?.value || "").trim();
  const DEFAULT_PORTS = { mysql: 3306, postgresql: 5432, sqlserver: 1433, oracle: 1521, sqlite: '', hive: 10000, hbase: 9090, hdfs: 9870 };
  const dbPathRow = document.getElementById("dbPathRow");
  const hostInputs = ["dbHost", "dbPort", "dbUser", "dbPassword", "dbName", "dbTableSelect", "dbTableInput", "dbWhere", "dbExportFormat", "dbAppendLatest", "dbTestBtn", "dbListBtn"];
  // Render fields per dbType
  if (dbType === "sqlite") {
    if (dbPathRow) dbPathRow.style.display = '';
    try {
      ["dbHost", "dbPort"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = true; const row = el.closest('.row'); if (row) row.style.display = 'none'; } });
      ["dbName", "dbUser", "dbPassword"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = true; const row = el.closest('.row'); if (row) row.style.display = 'none'; } });
      // table selector/input also not applicable for raw sqlite path mode
      ["dbTableSelect", "dbTableInput", "dbWhere"].forEach((id) => { const el = document.getElementById(id); if (el) { el.disabled = true; const row = el.closest('.row'); if (row) row.style.display = 'none'; } });
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
      // update dbPath label/placeholder for HDFS
      const dbPathLabel = document.getElementById('dbPathLabel');
      const dbPathInput = document.getElementById('dbPath');
      if (dbPathLabel) dbPathLabel.textContent = 'HDFS 路径';
      if (dbPathInput) dbPathInput.placeholder = '例如 /user/data/*.parquet 或 /user/data/csv/';
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
    const DEFAULT_PORTS = { mysql: 3306, postgresql: 5432, sqlserver: 1433, oracle: 1521, hive: 10000, hbase: 9090, hdfs: 9870 };
    const dbType = (document.getElementById('dbType')?.value || 'mysql').toLowerCase();
    const portEl = document.getElementById('dbPort');
    if (!portEl) return;
    const cur = (String(portEl.value || '').trim());
    const known = Object.values(DEFAULT_PORTS).map(String);
    const def = DEFAULT_PORTS[dbType] ? String(DEFAULT_PORTS[dbType]) : '';
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
  return {
    job_name: `db_export_${table}_${Date.now()}`,
    factory_id: DEFAULT_FACTORY_ID,
    owner_id: (state.currentUser && state.currentUser.username) || "user-001",
    schedule: spec,
    file_format: format,
    enabled: true,
    mode: "visible",
    destination: { type: "local", path: "nifi_data/exports" },
    db_config: collectDbConfigForSchedule(),
    payload: { table },
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
      <td>${job.factory_id || DEFAULT_FACTORY_ID}</td>
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
  const q = new URLSearchParams({ factory_id: DEFAULT_FACTORY_ID });
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
    factory_id: detail.data.factory_id || DEFAULT_FACTORY_ID,
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
  document.getElementById("createBtn").addEventListener("click", createJob);
  document.getElementById("refreshBtn").addEventListener("click", loadJobs);
  document.getElementById("loadFilesBtn").addEventListener("click", loadFiles);
  document.getElementById("loadRulesBtn").addEventListener("click", loadTagRules);
  document.getElementById("autoTagBtn").addEventListener("click", triggerAutoTag);

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
}

function init() {
  bindEvents();
  loadJobs();
  loadFiles();
  loadTagRules();
    bindNiFiButtons(); // Add NiFi trigger button handler implementation

  // Check current user to decide whether to show internal management link
  (async function checkCurrentUser(){
    try{
      const res = await api('/auth/me');
      // normalize responses from different backend shapes
      const user = (res && res.user) || (res && res.data && res.data.user) || (res && res.data) || null;
      if (user) {
        state.currentUser = user;
        try { localStorage.setItem('currentUser', JSON.stringify(user)); } catch (e) {}
        if (user.is_admin) {
          const a = document.getElementById('internalLink');
          if(a) a.style.display = '';
        }
      } else {
        // fallback: try from localStorage
        try {
          const raw = localStorage.getItem('currentUser');
          if (raw) state.currentUser = JSON.parse(raw);
        } catch (e) {}
      }
    }catch(e){
      // not logged in or error: clear cached user
      try { localStorage.removeItem('currentUser'); } catch (e) {}
      state.currentUser = null;
    }
  })();
  
    // NiFi quick trigger helpers
    function bindNiFiButtons() {
      document.querySelectorAll(".nifi-btn").forEach((btn) => {
        btn.addEventListener("click", async () => {
          const jobType = btn.dataset.type;
            const payload = {
            jobType,
            source: { sourceType: "MYSQL_TABLE" },
            target: { format: "CSV", outputDir: "/data/jobs" },
            tagConfig: { mode: jobType === "TAG_MANUAL" ? "MANUAL" : "AUTO", ruleId: "NIFI_RULE_ID_V5" },
            runBy: (state.currentUser && state.currentUser.username) || "user-001",
          };
          const res = await api("/jobs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          });
          if (res.code !== 0) {
            alert(`触发失败: ${res.message}`);
            return;
          }
          alert(`已创建任务 ${res.data.jobId}，开始执行`);
          await loadJobs();
          await loadJobDetail(res.data.jobId);
        });
      });
    }
}

async function testDbConnection() {
  const host = document.getElementById("dbHost").value.trim();
  const port = Number(document.getElementById("dbPort").value || 3306);
  const database = document.getElementById("dbName").value.trim();
  const username = document.getElementById("dbUser").value.trim();
  const password = document.getElementById("dbPassword").value;
  const status = document.getElementById("dbConnStatus");
  status.textContent = "测试中...";
  const dbType = document.getElementById("dbType")?.value || "mysql";
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
  const host = document.getElementById("dbHost").value.trim();
  const port = Number(document.getElementById("dbPort").value || 3306);
  const database = document.getElementById("dbName").value.trim();
  const username = document.getElementById("dbUser").value.trim();
  const password = document.getElementById("dbPassword").value;
  const status = document.getElementById("dbConnStatus");
  status.textContent = "列出表中...";
  const dbType = document.getElementById("dbType")?.value || "mysql";
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
const uploadUserName = document.getElementById("uploadUserName");
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
    const username = uploadUserName ? uploadUserName.value.trim() || "user" : "user";
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
      const resp = await fetch(
        `${config.API_BASE}${selected.endpoint}?${query.toString()}`,
        {
        method: "POST",
        body: formData,
      });
      const data = await resp.json();
      if (data.code === 0) {
        const sourcePath = data?.data?.sourcePath || data?.data?.storagePath || "";
        const targetPath = data?.data?.targetPath || "";
        const msgLines = [
          `上传成功，已执行 ${selected.convertType}，原文件 fileId: ${data.data.fileId}`,
          sourcePath ? `上传文件路径: ${sourcePath}` : "上传文件路径: （后端未返回）",
          targetPath ? `转换文件路径: ${targetPath}` : "转换文件路径: （后端未返回）",
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
