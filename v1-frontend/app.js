const config = window.APP_CONFIG;

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
  return fetch(`${config.API_BASE}${path}`, options).then((r) => r.json());
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
    runBy: "user-001",
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

  // --- DB export handlers: immediate export and schedule creation ---
  async function dbTestConn() {
    const host = document.getElementById('dbHost').value;
    const port = Number(document.getElementById('dbPort').value || 3306);
    const database = document.getElementById('dbName').value;
    const user = document.getElementById('dbUser').value;
    const password = document.getElementById('dbPassword').value;
    const cfg = { db_type: 'mysql', driver: 'pymysql', host, port, user, password, database };
    try {
      const res = await fetch(`${config.API_BASE}/api/db/test`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg)
      });
      const j = await res.json();
      document.getElementById('dbConnStatus').textContent = j.message || JSON.stringify(j);
    } catch (e) {
      document.getElementById('dbConnStatus').textContent = '连接失败: ' + e.message;
    }
  }

  async function dbListTables() {
    const host = document.getElementById('dbHost').value;
    const port = Number(document.getElementById('dbPort').value || 3306);
    const database = document.getElementById('dbName').value;
    const user = document.getElementById('dbUser').value;
    const password = document.getElementById('dbPassword').value;
    const cfg = { db_type: 'mysql', driver: 'pymysql', host, port, user, password, database };
    try {
      const res = await fetch(`${config.API_BASE}/api/db/list-tables`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(cfg)
      });
      const j = await res.json();
      const sel = document.getElementById('dbTableSelect');
      sel.innerHTML = '<option value="">-- 先选择 --</option>';
      if (j && j.tables) {
        j.tables.forEach(t => {
          const opt = document.createElement('option'); opt.value = t; opt.textContent = t; sel.appendChild(opt);
        });
      }
    } catch (e) {
      console.error(e);
    }
  }

  async function dbExportImmediate() {
    const table = document.getElementById('dbTableInput').value || document.getElementById('dbTableSelect').value;
    if (!table) { document.getElementById('dbExportResult').textContent = '请先选择或输入表名'; return; }
    const host = document.getElementById('dbHost').value;
    const port = Number(document.getElementById('dbPort').value || 3306);
    const database = document.getElementById('dbName').value;
    const user = document.getElementById('dbUser').value;
    const password = document.getElementById('dbPassword').value;
    const format = document.getElementById('dbExportFormat').value || 'CSV';
    const saveLocation = document.getElementById('dbSaveLocation').value || '';
    const db_conf = { db_type: 'mysql', driver: 'pymysql', host, port, user, password, database };
    const job = { owner_id: 'web-user', payload: { table }, db_config: db_conf, file_format: format, destination: { outputDir: saveLocation } };
    try {
      const res = await fetch(`${config.API_BASE}/api/export-jobs/trigger`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(job)
      });
      const j = await res.json();
      if (j.ok || j.code === 0) {
        document.getElementById('dbExportResult').textContent = '导出成功: ' + JSON.stringify(j.data || j);
      } else {
        document.getElementById('dbExportResult').textContent = '导出返回: ' + JSON.stringify(j);
      }
    } catch (e) {
      document.getElementById('dbExportResult').textContent = '导出失败: ' + e.message;
    }
  }

  async function dbCreateSchedule() {
    const table = document.getElementById('dbTableInput').value || document.getElementById('dbTableSelect').value;
    if (!table) { document.getElementById('dbExportResult').textContent = '请先选择或输入表名'; return; }
    const host = document.getElementById('dbHost').value;
    const port = Number(document.getElementById('dbPort').value || 3306);
    const database = document.getElementById('dbName').value;
    const user = document.getElementById('dbUser').value;
    const password = document.getElementById('dbPassword').value;
    const format = document.getElementById('dbExportFormat').value || 'CSV';
    const freq = document.getElementById('dbFrequencySelect').value || '';
    const saveLocation = document.getElementById('dbSaveLocation').value || '';
    if (!freq) { document.getElementById('dbExportResult').textContent = '请选择频率后创建定时任务'; return; }
    const db_conf = { db_type: 'mysql', driver: 'pymysql', host, port, user, password, database };
    const payload = { table };
    const body = {
      job_name: `export_${database}_${table}`,
      owner_id: 'web-user',
      schedule: freq,
      file_format: format,
      destination: { outputDir: saveLocation },
      mode: 'visible',
      enabled: true,
      db_config: db_conf,
      payload,
    };
    try {
      const res = await fetch(`${config.API_BASE}/api/export-jobs`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
      });
      const j = await res.json();
      if (j.code === 0 || j.ok) {
        document.getElementById('dbExportResult').textContent = '创建定时任务成功: ' + JSON.stringify(j.data || j);
      } else {
        document.getElementById('dbExportResult').textContent = '创建任务返回: ' + JSON.stringify(j);
      }
    } catch (e) {
      document.getElementById('dbExportResult').textContent = '创建任务失败: ' + e.message;
    }
  }

  // attach handlers when DOM ready
  window.addEventListener('load', () => {
    const dbTestBtn = document.getElementById('dbTestBtn'); if (dbTestBtn) dbTestBtn.addEventListener('click', dbTestConn);
    const dbListBtn = document.getElementById('dbListBtn'); if (dbListBtn) dbListBtn.addEventListener('click', dbListTables);
    const dbExportBtn = document.getElementById('dbExportBtn'); if (dbExportBtn) dbExportBtn.addEventListener('click', dbExportImmediate);
    const dbScheduleBtn = document.getElementById('dbScheduleBtn'); if (dbScheduleBtn) dbScheduleBtn.addEventListener('click', dbCreateSchedule);
  });
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
    const payload = { fileId: state.selectedFileId, operator: "user-001", changes, renameColumns };
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
  const payload = { fileId, ruleId, outputFormat: "CSV", operator: "user-001" };
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
  if (dbTestBtn) dbTestBtn.addEventListener("click", testDbConnection);
  if (dbListBtn) dbListBtn.addEventListener("click", listTables);
  if (dbExportBtn) dbExportBtn.addEventListener("click", exportFromDb);
}

function init() {
  bindEvents();
  loadJobs();
  loadFiles();
  loadTagRules();
    bindNiFiButtons(); // Add NiFi trigger button handler implementation
  
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
            runBy: "user-001",
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
  const payload = { db_type: "mysql", host, port, username, password, database };
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
  const payload = { db_type: "mysql", host, port, username, password, database };
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
  const host = document.getElementById("dbHost").value.trim();
  const port = Number(document.getElementById("dbPort").value || 3306);
  const database = document.getElementById("dbName").value.trim();
  const username = document.getElementById("dbUser").value.trim();
  const password = document.getElementById("dbPassword").value;
  const where = document.getElementById("dbWhere")?.value.trim() || "";
  const select = document.getElementById("dbTableSelect");
  const input = document.getElementById("dbTableInput");
  const table = (select && select.value) || (input && input.value.trim());
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
    const payload = { host, port, user: username, password, db: database, table, format, append_to_latest, where };
    const res = await api(`/export/mysql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (res && res.code === 0) {
      result.textContent = `导出成功: ${res.data.file.fileName}，路径: ${res.data.file.storagePath}`;
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
