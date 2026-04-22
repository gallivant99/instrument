const state = {
  me: null,
  lookups: {
    suppliers: [],
    departments: [],
    patients: [],
    devices: [],
    procurements: [],
    recalls: [],
    scraps: [],
    maintenance_types: [],
    risk_levels: [],
    trace_modes: [],
    categories: [],
    recall_severities: [],
  },
  dashboard: null,
  alerts: null,
  devices: [],
  inventory: [],
  maintenance: [],
  procurements: [],
  recalls: [],
  scraps: [],
  audits: [],
  reportSummary: null,
};

const statusText = {
  requested: "待审批",
  approved: "已审批",
  completed: "已完成",
  open: "进行中",
  closed: "已关闭",
  disposed: "已处置",
  registered: "已登记",
  in_stock: "在库",
  distributed: "已发放",
  in_use: "使用中",
  used: "已使用",
  recalled: "召回中",
  scrapped: "已报废",
};

const roleText = {
  admin: "系统管理员",
  warehouse: "库房管理员",
  clinician: "临床护士",
  engineer: "设备工程师",
  manager: "管理人员",
};

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    credentials: "same-origin",
    ...options,
  });
  const contentType = response.headers.get("Content-Type") || "";
  const data = contentType.includes("application/json") ? await response.json() : null;
  if (!response.ok) {
    throw new Error(data?.error || "请求失败");
  }
  return data;
}

function hasPermission(permission) {
  return state.me?.permissions?.includes(permission);
}

function showMessage(message, isError = false) {
  const bar = document.getElementById("messageBar");
  bar.textContent = message;
  bar.classList.remove("hidden", "error");
  if (isError) {
    bar.classList.add("error");
  }
  window.clearTimeout(showMessage.timer);
  showMessage.timer = window.setTimeout(() => {
    bar.classList.add("hidden");
  }, 3600);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function formToObject(formElement) {
  const formData = new FormData(formElement);
  return Object.fromEntries(formData.entries());
}

function fillSelect(selectId, items, valueKey, labelBuilder, placeholder = "") {
  const select = document.getElementById(selectId);
  if (!select) return;
  const options = items.map((item) => `<option value="${item[valueKey]}">${labelBuilder(item)}</option>`);
  if (placeholder) {
    options.unshift(`<option value="">${placeholder}</option>`);
  }
  select.innerHTML = options.join("");
}

function renderRows(containerId, items, rowBuilder, emptyText, colspan) {
  const container = document.getElementById(containerId);
  if (!container) return;
  if (!items.length) {
    container.innerHTML = `<tr><td colspan="${colspan}">${emptyText}</td></tr>`;
    return;
  }
  container.innerHTML = items.map(rowBuilder).join("");
}

function renderList(containerId, items, builder, emptyText = "暂无数据。") {
  const container = document.getElementById(containerId);
  if (!container) return;
  if (!items.length) {
    container.innerHTML = `<div class="muted-box">${emptyText}</div>`;
    return;
  }
  container.innerHTML = items.map(builder).join("");
}

function renderNoPermission(elementId, text = "当前角色无权限查看该内容。") {
  const element = document.getElementById(elementId);
  if (element) {
    element.innerHTML = `<div class="muted-box">${text}</div>`;
  }
}

function normalizeStatus(status) {
  return statusText[status] || status || "-";
}

function normalizeRole(role) {
  return roleText[role] || role || "-";
}

function applyPermissionLocks() {
  document.querySelectorAll("[data-permission]").forEach((element) => {
    const permission = element.dataset.permission;
    const enabled = hasPermission(permission);
    element.classList.toggle("is-disabled", !enabled);
    element.querySelectorAll("input, select, textarea, button").forEach((field) => {
      field.disabled = !enabled;
    });
    if (element.tagName === "BUTTON") {
      element.disabled = !enabled;
    }
  });
}

function activateTabs() {
  document.querySelectorAll(".tab-btn").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".tab-btn").forEach((node) => node.classList.remove("active"));
      document.querySelectorAll(".content-panel").forEach((node) => node.classList.remove("active"));
      button.classList.add("active");
      document.getElementById(button.dataset.panel).classList.add("active");
    });
  });
}

async function loadCurrentUser() {
  try {
    const payload = await fetchJson("/api/me");
    state.me = payload.user;
    document.getElementById("loginGate").classList.add("hidden");
    document.getElementById("currentUserText").textContent = state.me.display_name;
    document.getElementById("currentRoleText").textContent = normalizeRole(state.me.role);
    applyPermissionLocks();
    return true;
  } catch (error) {
    state.me = null;
    document.getElementById("loginGate").classList.remove("hidden");
    document.getElementById("currentUserText").textContent = "未登录";
    document.getElementById("currentRoleText").textContent = "-";
    return false;
  }
}

async function loadLookups() {
  if (!state.me) return;
  state.lookups = await fetchJson("/api/lookups");
  fillSelect("supplierSelect", state.lookups.suppliers, "id", (item) => item.name);
  fillSelect("procurementSupplierSelect", state.lookups.suppliers, "id", (item) => item.name);
  fillSelect("outboundDepartmentSelect", state.lookups.departments, "id", (item) => `${item.name} (${item.type})`);
  fillSelect("clinicalDepartmentSelect", state.lookups.departments, "id", (item) => `${item.name} (${item.type})`);
  fillSelect("patientSelect", state.lookups.patients, "id", (item) => `${item.patient_no} - ${item.name}`);
  fillSelect("maintenanceTypeSelect", state.lookups.maintenance_types.map((name) => ({ id: name, name })), "id", (item) => item.name);
  fillSelect("riskLevelSelect", state.lookups.risk_levels.map((name) => ({ id: name, name })), "id", (item) => item.name);
  fillSelect("traceModeSelect", state.lookups.trace_modes.map((name) => ({ id: name, name })), "id", (item) => item.name);
  fillSelect("categorySelect", state.lookups.categories.map((name) => ({ id: name, name })), "id", (item) => item.name);
  fillSelect("recallSeveritySelect", state.lookups.recall_severities.map((name) => ({ id: name, name })), "id", (item) => item.name);
  syncActionSelects();
}

async function refreshDashboard() {
  if (!hasPermission("dashboard:view")) return;
  state.dashboard = await fetchJson("/api/dashboard");
  const cards = [
    ["器械总数", state.dashboard.total_devices],
    ["效期预警", state.dashboard.expiring_soon],
    ["维护到期", state.dashboard.maintenance_due],
    ["服务患者", state.dashboard.served_patients],
    ["低库存", state.dashboard.low_stock],
    ["进行中召回", state.dashboard.open_recalls],
    ["超期未校准", state.dashboard.overdue_calibration],
    ["超期未盘点", state.dashboard.stale_stocktake],
    ["待关闭采购/报废", state.dashboard.pending_procurements + state.dashboard.pending_scraps],
  ];
  document.getElementById("kpiGrid").innerHTML = cards
    .map(
      ([label, value]) => `
        <article class="kpi-card">
          <span>${label}</span>
          <strong>${value}</strong>
        </article>
      `
    )
    .join("");
}

async function refreshAlerts() {
  if (!hasPermission("alerts:view")) {
    ["expiryAlerts", "stockAlerts", "maintenanceAlerts", "overdueCalibrationAlerts", "recallAlerts", "staleStocktakeAlerts"].forEach((id) =>
      renderNoPermission(id)
    );
    return;
  }
  state.alerts = await fetchJson("/api/alerts");
  renderList(
    "expiryAlerts",
    state.alerts.expiry_alerts,
    (item) => `
      <div class="warning-item">
        <strong>${escapeHtml(item.device_name)}</strong>
        <span>批次 ${escapeHtml(item.batch_no)}，效期 ${escapeHtml(item.expiry_date)}</span>
      </div>
    `,
    "暂无效期预警。"
  );
  renderList(
    "stockAlerts",
    state.alerts.stock_alerts,
    (item) => `
      <div class="warning-item">
        <strong>${escapeHtml(item.device_name)}</strong>
        <span>库存 ${item.stock_qty}，阈值 ${item.reorder_threshold}</span>
      </div>
    `,
    "暂无低库存预警。"
  );
  renderList(
    "maintenanceAlerts",
    state.alerts.maintenance_alerts,
    (item) => `
      <div class="warning-item">
        <strong>${escapeHtml(item.device_name)}</strong>
        <span>${escapeHtml(item.record_type)} 到期日 ${escapeHtml(item.next_due_date)}</span>
      </div>
    `,
    "暂无维护到期预警。"
  );
  renderList(
    "overdueCalibrationAlerts",
    state.alerts.overdue_calibration_alerts,
    (item) => `
      <div class="warning-item">
        <strong>${escapeHtml(item.device_name)}</strong>
        <span>校准已超期，原到期日 ${escapeHtml(item.next_due_date)}</span>
      </div>
    `,
    "暂无超期校准。"
  );
  renderList(
    "recallAlerts",
    state.alerts.recall_alerts,
    (item) => `
      <div class="warning-item">
        <strong>${escapeHtml(item.recall_no)}</strong>
        <span>${escapeHtml(item.device_name)} / ${escapeHtml(item.batch_no || "-")} / ${escapeHtml(item.severity)}级</span>
      </div>
    `,
    "暂无召回预警。"
  );
  renderList(
    "staleStocktakeAlerts",
    state.alerts.stale_stocktake_alerts,
    (item) => `
      <div class="warning-item">
        <strong>${escapeHtml(item.device_name)}</strong>
        <span>最近盘点 ${escapeHtml(item.last_stocktake_at || "-")}</span>
      </div>
    `,
    "暂无超期未盘点。"
  );
}

async function refreshDevices() {
  if (!hasPermission("devices:view")) {
    renderRows("deviceTableBody", [], () => "", "当前角色无权限查看器械清单。", 7);
    return;
  }
  state.devices = await fetchJson("/api/devices");
  fillSelect(
    "procurementDeviceSelect",
    state.devices,
    "id",
    (item) => `${item.device_name} (${item.udi_code || item.batch_no || item.id})`
  );
  renderRows(
    "deviceTableBody",
    state.devices,
    (item) => `
      <tr>
        <td>${escapeHtml(item.device_name)}</td>
        <td>${escapeHtml(item.category)}</td>
        <td>${escapeHtml(item.udi_code || "-")}</td>
        <td>${escapeHtml(item.batch_no || "-")}</td>
        <td>${item.stock_qty}</td>
        <td>${escapeHtml(normalizeStatus(item.status))}</td>
        <td>${escapeHtml(item.current_location || "-")}</td>
      </tr>
    `,
    "暂无器械数据。",
    7
  );
  syncProcurementSupplier();
}

async function refreshInventory() {
  if (!hasPermission("devices:view")) {
    renderRows("inventoryTableBody", [], () => "", "当前角色无权限查看库存。", 7);
    return;
  }
  state.inventory = await fetchJson("/api/inventory");
  renderRows(
    "inventoryTableBody",
    state.inventory,
    (item) => `
      <tr>
        <td>${escapeHtml(item.device_name)}</td>
        <td>${escapeHtml(item.batch_no || "-")}</td>
        <td>${item.stock_qty}</td>
        <td>${item.reorder_threshold}</td>
        <td>${escapeHtml(item.expiry_date || "-")}</td>
        <td>${escapeHtml(item.current_location || "-")}</td>
        <td>${escapeHtml(normalizeStatus(item.status))}</td>
      </tr>
    `,
    "暂无库存数据。",
    7
  );
}

async function refreshMaintenance() {
  if (!hasPermission("devices:view")) {
    renderRows("maintenanceTableBody", [], () => "", "当前角色无权限查看维护记录。", 6);
    return;
  }
  state.maintenance = await fetchJson("/api/maintenance");
  renderRows(
    "maintenanceTableBody",
    state.maintenance,
    (item) => `
      <tr>
        <td>${escapeHtml(item.device_name)}</td>
        <td>${escapeHtml(item.record_type)}</td>
        <td>${escapeHtml(item.completed_at || item.plan_date || "-")}</td>
        <td>${escapeHtml(item.result || "-")}</td>
        <td>${escapeHtml(item.next_due_date || "-")}</td>
        <td>${escapeHtml(item.current_location || "-")}</td>
      </tr>
    `,
    "暂无维护记录。",
    6
  );
}

async function refreshProcurements() {
  if (!hasPermission("procurement:view")) {
    renderRows("procurementTableBody", [], () => "", "当前角色无权限查看采购单。", 7);
    fillSelect("procurementApproveSelect", [], "id", () => "", "无权限");
    fillSelect("procurementReceiveSelect", [], "id", () => "", "无权限");
    return;
  }
  state.procurements = await fetchJson("/api/procurements");
  renderRows(
    "procurementTableBody",
    state.procurements,
    (item) => `
      <tr>
        <td>${escapeHtml(item.request_no)}</td>
        <td>${escapeHtml(item.device_name)}</td>
        <td>${escapeHtml(item.supplier_name)}</td>
        <td>${item.quantity}</td>
        <td>${escapeHtml(normalizeStatus(item.status))}</td>
        <td>${escapeHtml(item.requested_at)}</td>
        <td>${escapeHtml(item.inbound_completed_at || "-")}</td>
      </tr>
    `,
    "暂无采购单。",
    7
  );
  syncActionSelects();
}

async function refreshRecalls() {
  if (!hasPermission("recall:view")) {
    renderRows("recallTableBody", [], () => "", "当前角色无权限查看召回单。", 6);
    fillSelect("recallCloseSelect", [], "id", () => "", "无权限");
    return;
  }
  state.recalls = await fetchJson("/api/recalls");
  renderRows(
    "recallTableBody",
    state.recalls,
    (item) => `
      <tr>
        <td>${escapeHtml(item.recall_no)}</td>
        <td>${escapeHtml(item.device_name || item.batch_no || "-")}</td>
        <td>${escapeHtml(item.severity)}</td>
        <td>${escapeHtml(normalizeStatus(item.status))}</td>
        <td>${item.affected_device_count}</td>
        <td>${item.affected_patient_count}</td>
      </tr>
    `,
    "暂无召回单。",
    6
  );
  syncActionSelects();
}

async function refreshScraps() {
  if (!hasPermission("scrap:view")) {
    renderRows("scrapTableBody", [], () => "", "当前角色无权限查看报废单。", 6);
    fillSelect("scrapApproveSelect", [], "id", () => "", "无权限");
    fillSelect("scrapDisposeSelect", [], "id", () => "", "无权限");
    return;
  }
  state.scraps = await fetchJson("/api/scraps");
  renderRows(
    "scrapTableBody",
    state.scraps,
    (item) => `
      <tr>
        <td>${escapeHtml(item.scrap_no)}</td>
        <td>${escapeHtml(item.device_name)}</td>
        <td>${item.quantity}</td>
        <td>${escapeHtml(normalizeStatus(item.status))}</td>
        <td>${escapeHtml(item.requested_by)}</td>
        <td>${escapeHtml(item.disposed_at || "-")}</td>
      </tr>
    `,
    "暂无报废单。",
    6
  );
  syncActionSelects();
}

async function refreshAuditLogs() {
  if (!hasPermission("audit:view")) {
    renderRows("auditTableBody", [], () => "", "当前角色无权限查看审计日志。", 5);
    return;
  }
  state.audits = await fetchJson("/api/audit-logs?limit=120");
  renderRows(
    "auditTableBody",
    state.audits,
    (item) => `
      <tr>
        <td>${escapeHtml(item.created_at)}</td>
        <td>${escapeHtml(item.username)} / ${escapeHtml(normalizeRole(item.role))}</td>
        <td>${escapeHtml(item.action)}</td>
        <td>${escapeHtml(item.target_name || item.target_type)}</td>
        <td>${escapeHtml(item.detail || "-")}</td>
      </tr>
    `,
    "暂无审计日志。",
    5
  );
}

async function refreshReportSummary() {
  if (!hasPermission("reports:view")) {
    renderNoPermission("reportSummary");
    return;
  }
  state.reportSummary = await fetchJson("/api/reports/summary");
  const summary = state.reportSummary;
  document.getElementById("reportSummary").innerHTML = `
    <div class="list-row">
      <div><strong>近 30 天采购申请</strong><span>${summary.recent_procurements}</span></div>
      <div><strong>近 30 天临床使用</strong><span>${summary.recent_usages}</span></div>
    </div>
    <div class="list-row">
      <div><strong>近 30 天维护记录</strong><span>${summary.recent_maintenances}</span></div>
      <div><strong>近 30 天审计动作</strong><span>${summary.recent_audits}</span></div>
    </div>
    <div class="list-row">
      <div><strong>当前进行中召回</strong><span>${summary.dashboard.open_recalls}</span></div>
      <div><strong>待处理采购/报废</strong><span>${summary.dashboard.pending_procurements + summary.dashboard.pending_scraps}</span></div>
    </div>
  `;
}

function syncActionSelects() {
  fillSelect(
    "procurementApproveSelect",
    state.procurements.filter((item) => item.status === "requested"),
    "id",
    (item) => `${item.request_no} - ${item.device_name}`,
    "暂无待审批采购单"
  );
  fillSelect(
    "procurementReceiveSelect",
    state.procurements.filter((item) => item.status === "approved"),
    "id",
    (item) => `${item.request_no} - ${item.device_name}`,
    "暂无待到货采购单"
  );
  fillSelect(
    "recallCloseSelect",
    state.recalls.filter((item) => item.status === "open"),
    "id",
    (item) => `${item.recall_no} - ${item.device_name || item.batch_no || "-"}`,
    "暂无待关闭召回单"
  );
  fillSelect(
    "scrapApproveSelect",
    state.scraps.filter((item) => item.status === "requested"),
    "id",
    (item) => `${item.scrap_no} - ${item.device_name}`,
    "暂无待审批报废单"
  );
  fillSelect(
    "scrapDisposeSelect",
    state.scraps.filter((item) => item.status === "approved"),
    "id",
    (item) => `${item.scrap_no} - ${item.device_name}`,
    "暂无待处置报废单"
  );
}

function syncProcurementSupplier() {
  const deviceSelect = document.getElementById("procurementDeviceSelect");
  const supplierSelect = document.getElementById("procurementSupplierSelect");
  if (!deviceSelect || !supplierSelect) return;
  const selectedDevice = state.devices.find((item) => String(item.id) === deviceSelect.value);
  if (selectedDevice?.supplier_id) {
    supplierSelect.value = String(selectedDevice.supplier_id);
  }
}

function renderTraceability(result) {
  const summaryEl = document.getElementById("traceSummary");
  const mappingsEl = document.getElementById("traceMappings");
  const patientsEl = document.getElementById("tracePatients");
  const forwardEl = document.getElementById("traceForward");
  const reverseEl = document.getElementById("traceReverse");
  const timelineEl = document.getElementById("traceTimeline");
  const devicesEl = document.getElementById("traceDevices");
  const graphEl = document.getElementById("traceGraph");

  if (result.mode === "none") {
    summaryEl.innerHTML = "未查询到匹配结果。";
    mappingsEl.innerHTML = "";
    patientsEl.innerHTML = "暂无数据。";
    forwardEl.innerHTML = '<div class="muted-box">暂无数据。</div>';
    reverseEl.innerHTML = "暂无数据。";
    timelineEl.innerHTML = '<div class="muted-box">暂无时间轴。</div>';
    devicesEl.innerHTML = "暂无数据。";
    graphEl.innerHTML = "暂无图谱。";
    return;
  }

  if (result.mode === "device") {
    summaryEl.innerHTML = `
      <div class="list-row">
        <div><strong>${escapeHtml(result.summary.device_name)}</strong><span>${escapeHtml(result.summary.category)} / ${escapeHtml(normalizeStatus(result.summary.status))}</span></div>
        <div><strong>库存</strong><span>${result.summary.stock_qty}${escapeHtml(result.summary.unit)}</span></div>
      </div>
      <div class="list-row">
        <div><strong>批次号</strong><span>${escapeHtml(result.summary.batch_no || "-")}</span></div>
        <div><strong>当前位置</strong><span>${escapeHtml(result.summary.current_location || "-")}</span></div>
      </div>
    `;
  } else if (result.mode === "batch") {
    summaryEl.innerHTML = `
      <div class="list-row">
        <div><strong>批次号</strong><span>${escapeHtml(result.summary.batch_no)}</span></div>
        <div><strong>器械条目</strong><span>${result.summary.device_count}</span></div>
      </div>
      <div class="list-row">
        <div><strong>受影响患者</strong><span>${result.summary.impacted_patient_count}</span></div>
      </div>
    `;
  } else {
    summaryEl.innerHTML = `
      <div class="list-row">
        <div><strong>${escapeHtml(result.summary.name)}</strong><span>${escapeHtml(result.summary.patient_no)} / ${escapeHtml(result.summary.gender || "-")}</span></div>
        <div><strong>使用记录</strong><span>${result.summary.usage_count}</span></div>
      </div>
    `;
  }

  mappingsEl.innerHTML = (result.mappings || [])
    .map((item) => `<span class="chip">${escapeHtml(item.code_type)}: ${escapeHtml(item.code_value)}</span>`)
    .join("");

  patientsEl.innerHTML = result.impacted_patients?.length
    ? result.impacted_patients
        .map(
          (item) => `
            <div class="list-row">
              <div><strong>${escapeHtml(item.name || item.patient_no)}</strong><span>${escapeHtml(item.patient_no || "-")}</span></div>
              <div><span>${escapeHtml(item.operation_name || item.device_name || "-")}</span></div>
            </div>
          `
        )
        .join("")
    : "暂无关联对象。";

  forwardEl.innerHTML = result.forward_path?.length
    ? result.forward_path
        .map(
          (item) => `
            <div class="timeline-item">
              <strong>${escapeHtml(item.event_title)}</strong>
              <p>${escapeHtml(item.event_desc || "")}</p>
              <div class="timeline-meta">
                <span>${escapeHtml(item.occurred_at || "-")}</span>
                <span>${escapeHtml(item.location || "-")}</span>
                <span>${escapeHtml(item.actor || "-")}</span>
              </div>
            </div>
          `
        )
        .join("")
    : '<div class="muted-box">暂无正向路径。</div>';

  const reverseSections = [];
  Object.entries(result.reverse_relations || {}).forEach(([key, items]) => {
    if (!items?.length) return;
    reverseSections.push(`
      <div class="list-row">
        <div><strong>${escapeHtml(key)}</strong><span>${items.length} 条</span></div>
      </div>
      ${items
        .slice(0, 4)
        .map((item) => `<div class="list-row"><div><span>${escapeHtml(item.name || item.device_name || item.recall_no || item.scrap_no || item.request_no || item.patient_no || item.batch_no || "-")}</span></div></div>`)
        .join("")}
    `);
  });
  reverseEl.innerHTML = reverseSections.length ? reverseSections.join("") : "暂无逆向影响。";

  timelineEl.innerHTML = result.timeline?.length
    ? result.timeline
        .map(
          (item) => `
            <div class="timeline-item">
              <strong>${escapeHtml(item.event_title)}</strong>
              <p>${escapeHtml(item.event_desc || "")}</p>
              <div class="timeline-meta">
                <span>${escapeHtml(item.occurred_at || "-")}</span>
                <span>${escapeHtml(item.location || "-")}</span>
                <span>${escapeHtml(item.actor || "-")}</span>
                <span>${escapeHtml(item.device_name || "")}</span>
              </div>
            </div>
          `
        )
        .join("")
    : '<div class="muted-box">暂无时间轴。</div>';

  devicesEl.innerHTML = result.related_devices?.length
    ? result.related_devices
        .map(
          (item) => `
            <div class="list-row">
              <div><strong>${escapeHtml(item.device_name)}</strong><span>${escapeHtml(item.batch_no || "-")} / ${escapeHtml(item.serial_no || "-")}</span></div>
              <div><span>${escapeHtml(item.current_location || "-")}</span></div>
            </div>
          `
        )
        .join("")
    : "暂无关联器械。";

  graphEl.innerHTML = result.graph?.nodes?.length
    ? `
      <div class="graph-list">
        <div class="graph-chip-row">
          ${result.graph.nodes.map((node) => `<span class="graph-chip">${escapeHtml(node.type)}: ${escapeHtml(node.label)}</span>`).join("")}
        </div>
        <div class="graph-chip-row">
          ${result.graph.edges.map((edge) => `<span class="graph-chip">${escapeHtml(edge.label)}: ${escapeHtml(edge.from)} -> ${escapeHtml(edge.to)}</span>`).join("")}
        </div>
      </div>
    `
    : "暂无图谱。";
}

async function refreshAll() {
  if (!state.me) return;
  const jobs = [refreshDashboard(), refreshAlerts(), refreshDevices(), refreshInventory(), refreshMaintenance()];
  if (hasPermission("procurement:view")) jobs.push(refreshProcurements());
  if (hasPermission("recall:view")) jobs.push(refreshRecalls());
  if (hasPermission("scrap:view")) jobs.push(refreshScraps());
  if (hasPermission("audit:view")) jobs.push(refreshAuditLogs());
  if (hasPermission("reports:view")) jobs.push(refreshReportSummary());
  await Promise.all(jobs);
  syncActionSelects();
}

function wireForms() {
  document.getElementById("loginForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    try {
      const result = await fetchJson("/api/login", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      state.me = result.user;
      await loadCurrentUser();
      await loadLookups();
      await refreshAll();
      showMessage(`已登录，当前角色：${normalizeRole(state.me.role)}。`);
      form.reset();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("logoutBtn").addEventListener("click", async () => {
    await fetchJson("/api/logout", { method: "POST", body: JSON.stringify({}) });
    state.me = null;
    document.getElementById("loginGate").classList.remove("hidden");
    showMessage("已退出登录。");
  });

  document.getElementById("refreshAllBtn").addEventListener("click", async () => {
    try {
      await refreshAll();
      showMessage("数据已刷新。");
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("procurementDeviceSelect").addEventListener("change", syncProcurementSupplier);

  document.getElementById("deviceForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    try {
      await fetchJson("/api/devices", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      document.querySelector("#deviceForm [name='unit']").value = "件";
      document.querySelector("#deviceForm [name='reorder_threshold']").value = "0";
      showMessage("器械主数据已保存。");
      await loadLookups();
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("procurementForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.device_id = Number(payload.device_id);
    if (payload.supplier_id) payload.supplier_id = Number(payload.supplier_id);
    payload.quantity = Number(payload.quantity);
    payload.unit_price = Number(payload.unit_price);
    try {
      await fetchJson("/api/procurements", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      document.querySelector("#procurementForm [name='quantity']").value = "1";
      document.querySelector("#procurementForm [name='unit_price']").value = "0";
      showMessage("采购申请已提交。");
      await loadLookups();
      await refreshProcurements();
      await refreshDashboard();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("procurementApproveForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.procurement_id = Number(payload.procurement_id);
    if (payload.unit_price) payload.unit_price = Number(payload.unit_price);
    try {
      await fetchJson("/api/procurements/approve", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      showMessage("采购单已审批。");
      await loadLookups();
      await refreshProcurements();
      await refreshDashboard();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("procurementReceiveForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.procurement_id = Number(payload.procurement_id);
    try {
      await fetchJson("/api/procurements/receive", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      document.querySelector("#procurementReceiveForm [name='warehouse']").value = "中央库房";
      showMessage("采购到货已入库。");
      await loadLookups();
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("inboundForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.quantity = Number(payload.quantity);
    try {
      await fetchJson("/api/warehouse/inbound", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      document.querySelector("#inboundForm [name='warehouse']").value = "中央库房";
      showMessage("入库完成。");
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("outboundForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.quantity = Number(payload.quantity);
    payload.department_id = Number(payload.department_id);
    try {
      await fetchJson("/api/warehouse/outbound", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      document.querySelector("#outboundForm [name='quantity']").value = "1";
      showMessage("出库完成。");
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("stocktakeForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.codes = (payload.codes || "")
      .split(/\r?\n/)
      .map((item) => item.trim())
      .filter(Boolean);
    try {
      const result = await fetchJson("/api/warehouse/stocktake", { method: "POST", body: JSON.stringify(payload) });
      document.getElementById("stocktakeResult").innerHTML = result.count
        ? result.recognized
            .map(
              (item) => `
                <div class="list-row">
                  <div><strong>${escapeHtml(item.device_name)}</strong><span>${escapeHtml(item.current_location || "-")}</span></div>
                  <div><span>最近识别：${escapeHtml(item.last_seen_at || "-")}</span></div>
                </div>
              `
            )
            .join("")
        : "未识别到有效器械。";
      showMessage(`盘点完成，识别 ${result.count} 条器械记录。`);
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("clinicalForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.patient_id = Number(payload.patient_id);
    payload.department_id = Number(payload.department_id);
    payload.quantity = Number(payload.quantity);
    try {
      await fetchJson("/api/clinical-uses", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      document.querySelector("#clinicalForm [name='quantity']").value = "1";
      document.querySelector("#clinicalForm [name='operator_name']").value = "临床护士";
      showMessage("临床使用记录已登记。");
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("maintenanceForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    if (payload.completed_at) {
      payload.completed_at = payload.completed_at.replace("T", " ") + ":00";
    }
    try {
      await fetchJson("/api/maintenance", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      document.querySelector("#maintenanceForm [name='result']").value = "通过";
      document.querySelector("#maintenanceForm [name='operator_name']").value = "设备工程师";
      showMessage("维护记录已登记。");
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("recallForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    try {
      await fetchJson("/api/recalls", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      showMessage("召回单已创建。");
      await loadLookups();
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("recallCloseForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.recall_case_id = Number(payload.recall_case_id);
    try {
      await fetchJson("/api/recalls/close", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      showMessage("召回流程已关闭。");
      await loadLookups();
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("scrapForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.quantity = Number(payload.quantity);
    try {
      await fetchJson("/api/scraps", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      document.querySelector("#scrapForm [name='quantity']").value = "1";
      showMessage("报废申请已提交。");
      await loadLookups();
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("scrapApproveForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.scrap_request_id = Number(payload.scrap_request_id);
    try {
      await fetchJson("/api/scraps/approve", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      showMessage("报废申请已审批。");
      await loadLookups();
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("scrapDisposeForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const payload = formToObject(form);
    payload.scrap_request_id = Number(payload.scrap_request_id);
    try {
      await fetchJson("/api/scraps/dispose", { method: "POST", body: JSON.stringify(payload) });
      form.reset();
      showMessage("报废处置已完成。");
      await loadLookups();
      await refreshAll();
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("traceForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const keyword = document.getElementById("traceKeyword").value.trim();
    if (!keyword) {
      showMessage("请输入查询关键字。", true);
      return;
    }
    try {
      const result = await fetchJson(`/api/traceability?keyword=${encodeURIComponent(keyword)}`);
      renderTraceability(result);
    } catch (error) {
      showMessage(error.message, true);
    }
  });

  document.getElementById("exportReportBtn").addEventListener("click", async () => {
    try {
      const response = await fetch("/api/reports/export", { credentials: "same-origin" });
      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "导出失败");
      }
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = "traceability-report.xlsx";
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
      showMessage("Excel 报表已导出。");
      if (hasPermission("audit:view")) {
        await refreshAuditLogs();
      }
    } catch (error) {
      showMessage(error.message, true);
    }
  });
}

async function bootstrap() {
  activateTabs();
  wireForms();
  const loggedIn = await loadCurrentUser();
  if (loggedIn) {
    await loadLookups();
    await refreshAll();
  }
}

bootstrap();
