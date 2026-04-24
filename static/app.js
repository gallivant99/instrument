const root = document.querySelector("#app");

const state = {
  user: null,
  active: "overview",
  loading: false,
  message: null,
  messageType: "info",
  lookups: {},
  dashboard: {},
  alerts: {},
  trace: null,
  data: {
    devices: [],
    inventory: [],
    suppliers: [],
    users: [],
    purchaseStaff: [],
    purchasePlans: [],
    inboundOrders: [],
    deviceRequests: [],
    qualityReports: [],
    transfers: [],
    maintenance: [],
    procurements: [],
    recalls: [],
    scraps: [],
    auditLogs: [],
    reportSummary: {},
  },
};

const ROLE_LABELS = {
  admin: "系统管理员",
  manager: "管理人员",
  purchaser: "采购员",
  warehouse: "库房管理员",
  clinician: "医院工作人员",
  engineer: "设备工程师",
};

const STATUS_LABELS = {
  submitted: "待审核",
  requested: "待审核",
  approved: "已通过",
  rejected: "已驳回",
  purchased: "已采购",
  received: "已入库",
  completed: "已完成",
  issued: "已发放",
  disposed: "已处置",
  processing: "处理中",
  resolved: "已解决",
  open: "进行中",
  closed: "已关闭",
  in_stock: "在库",
  in_use: "使用中",
  used: "已使用",
  registered: "已建档",
  distributed: "已发放",
  recalled: "召回中",
  scrapped: "已报废",
};

const NAV_ITEMS = [
  { id: "overview", label: "工作台", title: "业务工作台", desc: "按角色聚合待处理事项、库存预警和追溯风险。" },
  { id: "devices", label: "器械管理", title: "医疗器械管理", desc: "维护器械基础信息、编码、库存阈值和当前状态。", any: ["devices:view"] },
  { id: "people", label: "用户与采购员", title: "用户与采购员管理", desc: "维护系统用户、医院工作人员账号和采购人员档案。", any: ["users:view", "purchase_staff:manage"] },
  { id: "procurement", label: "采购业务", title: "采购与供应商管理", desc: "供应商建档、采购计划提交、管理员审核、到货入库单审核。", any: ["purchase_plan:view", "procurement:view", "suppliers:view", "inbound_order:view"] },
  { id: "warehouse", label: "出入库与调拨", title: "出入库与科室调拨", desc: "处理申领发放、采购入库、库存预警和科室之间的器械调拨。", any: ["request:view", "transfer:view", "warehouse:write", "devices:view"] },
  { id: "hospital", label: "医院工作人员", title: "申领、使用与质量上报", desc: "提交器械申领单，登记患者使用记录，上报质量问题。", any: ["request:create", "clinical:write", "quality:write"] },
  { id: "risk", label: "维护召回报废", title: "器械维护、召回与报废", desc: "记录维护校准，处理召回、报废和质量风险闭环。", any: ["maintenance:write", "recall:view", "scrap:view", "quality:view"] },
  { id: "trace", label: "追溯查询", title: "追溯查询", desc: "按 UDI/RFID/院内码/批号/患者编号查询采购、入库、使用和处理过程。", any: ["trace:view"] },
  { id: "reports", label: "报表审计", title: "报表与审计", desc: "查看综合统计、审计日志，并导出演示报表。", any: ["reports:view", "audit:view"] },
];

const DATA_LOADERS = [
  { key: "dashboard", url: "/api/dashboard", root: true, fallback: {} },
  { key: "lookups", url: "/api/lookups", root: true, fallback: {} },
  { key: "alerts", url: "/api/alerts", root: true, fallback: {} },
  { key: "devices", url: "/api/devices", perm: "devices:view" },
  { key: "inventory", url: "/api/inventory", perm: "devices:view" },
  { key: "suppliers", url: "/api/suppliers", perm: "suppliers:view" },
  { key: "users", url: "/api/users", perm: "users:view" },
  { key: "purchaseStaff", url: "/api/purchase-staff", perm: "purchase_staff:manage" },
  { key: "purchasePlans", url: "/api/purchase-plans", perm: "purchase_plan:view" },
  { key: "inboundOrders", url: "/api/inbound-orders", perm: "inbound_order:view" },
  { key: "deviceRequests", url: "/api/device-requests", perm: "request:view" },
  { key: "qualityReports", url: "/api/quality-reports", perm: "quality:view" },
  { key: "transfers", url: "/api/transfers", perm: "transfer:view" },
  { key: "maintenance", url: "/api/maintenance", perm: "devices:view" },
  { key: "procurements", url: "/api/procurements", perm: "procurement:view" },
  { key: "recalls", url: "/api/recalls", perm: "recall:view" },
  { key: "scraps", url: "/api/scraps", perm: "scrap:view" },
  { key: "auditLogs", url: "/api/audit-logs?limit=120", perm: "audit:view" },
  { key: "reportSummary", url: "/api/reports/summary", perm: "reports:view", fallback: {} },
];

document.addEventListener("submit", handleSubmit);
document.addEventListener("click", handleClick);
document.addEventListener("input", handleInput);

init();

async function init() {
  await loadCurrentUser();
  if (state.user) {
    await loadAll();
  }
  render();
}

async function loadCurrentUser() {
  try {
    const result = await apiGet("/api/me");
    state.user = result.user;
  } catch {
    state.user = null;
  }
}

async function loadAll() {
  state.loading = true;
  renderShellOnly();
  await Promise.all(
    DATA_LOADERS.map(async (loader) => {
      if (loader.perm && !can(loader.perm)) {
        assignLoaderResult(loader, loader.fallback ?? []);
        return;
      }
      try {
        assignLoaderResult(loader, await apiGet(loader.url));
      } catch (error) {
        assignLoaderResult(loader, loader.fallback ?? []);
      }
    }),
  );
  state.loading = false;
}

function assignLoaderResult(loader, value) {
  if (loader.root) {
    state[loader.key] = value;
  } else {
    state.data[loader.key] = value;
  }
}

async function apiGet(url) {
  return api(url);
}

async function apiPost(url, payload) {
  return api(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function api(url, options = {}) {
  const response = await fetch(url, { credentials: "same-origin", ...options });
  const contentType = response.headers.get("content-type") || "";
  const body = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    throw new Error(typeof body === "string" ? body : body.error || "请求失败");
  }
  return body;
}

function can(permission) {
  return Boolean(state.user?.permissions?.includes(permission));
}

function canAny(permissions = []) {
  return permissions.length === 0 || permissions.some(can);
}

function render() {
  if (!state.user) {
    renderLogin();
    return;
  }
  renderShell();
}

function renderShellOnly() {
  if (state.user && document.querySelector(".app-shell")) {
    const badge = document.querySelector("#loadingBadge");
    if (badge) badge.textContent = state.loading ? "正在同步数据..." : "数据已同步";
  }
}

function renderLogin() {
  root.innerHTML = `
    <main class="login-screen">
      <section class="login-card">
        <div class="login-copy">
          <span class="eyebrow">Traceability Control Center</span>
          <h1>医疗器械全生命周期追溯管理系统</h1>
          <p>覆盖器械建档、采购计划、采购入库、申领审批、临床使用、质量上报、召回报废和追溯查询，适合毕业设计演示。</p>
        </div>
        <div class="login-panel">
          <form class="smart-form" data-form="login">
            ${field("用户名", "username", "text", "admin", "required")}
            ${field("密码", "password", "password", "admin123", "required")}
            <div class="form-actions"><button class="primary" type="submit">登录系统</button></div>
          </form>
          <div class="account-grid">
            ${account("admin / admin123", "系统管理员")}
            ${account("buyer / buyer123", "采购员")}
            ${account("warehouse / warehouse123", "库房管理员")}
            ${account("nurse / nurse123", "医院工作人员")}
            ${account("doctor / doctor123", "临床医生")}
            ${account("manager / manager123", "管理人员")}
          </div>
          ${state.message ? `<p class="message error">${esc(state.message)}</p>` : ""}
        </div>
      </section>
    </main>
  `;
}

function account(login, role) {
  return `<div class="account-item"><strong>${esc(login)}</strong><span>${esc(role)}</span></div>`;
}

function renderShell() {
  const activeItem = NAV_ITEMS.find((item) => item.id === state.active) || NAV_ITEMS[0];
  root.innerHTML = `
    <div class="app-shell">
      <aside class="sidebar">
        <div class="brand">
          <strong>医疗器械<br />追溯管理系统</strong>
          <span>Lifecycle Traceability</span>
        </div>
        <nav class="nav">
          ${NAV_ITEMS.filter((item) => canAny(item.any)).map((item) => `
            <button type="button" class="${state.active === item.id ? "active" : ""}" data-route="${item.id}">
              ${esc(item.label)}
            </button>
          `).join("")}
        </nav>
        <div class="sidebar-card">
          <span>当前用户</span>
          <strong>${esc(state.user.display_name)} · ${esc(ROLE_LABELS[state.user.role] || state.user.role)}</strong>
        </div>
      </aside>
      <main class="main">
        <header class="topbar">
          <div class="page-title">
            <span class="eyebrow">Hospital Device Operations</span>
            <h1>${esc(activeItem.title)}</h1>
            <p>${esc(activeItem.desc)}</p>
          </div>
          <div class="top-actions">
            <button class="ghost" type="button" data-action="refresh">刷新数据</button>
            <button class="ghost" type="button" data-action="logout">退出登录</button>
            <div class="utility-card">
              <span id="loadingBadge">${state.loading ? "正在同步数据..." : "数据已同步"}</span>
              <strong>${formatDateTime(new Date())}</strong>
            </div>
          </div>
        </header>
        ${state.message ? `<div class="message ${state.messageType === "error" ? "error" : ""}">${esc(state.message)}</div>` : ""}
        <section id="pageContent">${renderPage()}</section>
      </main>
    </div>
  `;
}

function renderPage() {
  switch (state.active) {
    case "devices":
      return renderDevicesPage();
    case "people":
      return renderPeoplePage();
    case "procurement":
      return renderProcurementPage();
    case "warehouse":
      return renderWarehousePage();
    case "hospital":
      return renderHospitalPage();
    case "risk":
      return renderRiskPage();
    case "trace":
      return renderTracePage();
    case "reports":
      return renderReportsPage();
    default:
      return renderOverviewPage();
  }
}

function renderOverviewPage() {
  const d = state.dashboard || {};
  return `
    <div class="overview-shell">
      <section class="metric-groups">
        ${metricGroup("业务待办", "需要管理员或库房继续处理", "待", "warn", [
          ["待审采购计划", d.pending_purchase_plans, "采购员提交后等待审核"],
          ["待审申领单", d.pending_device_requests, "医院工作人员提交后等待审批"],
          ["待审入库单", d.pending_inbound_orders, "到货后等待验收入库"],
        ])}
        ${metricGroup("库存风险", "关注低库存、效期和盘点", "库", "warn", [
          ["低库存预警", d.low_stock, "库存量已低于安全阈值"],
          ["临期器械", d.expiring_soon, "30 天内到期"],
          ["盘点超期", d.stale_stocktake, "超过 30 天未盘点"],
        ])}
        ${metricGroup("质量风险", "质量、召回、报废集中处理", "质", "danger", [
          ["质量问题", d.open_quality_reports, "待受理或处理中"],
          ["召回中", d.open_recalls, "仍在核查流向"],
          ["待处理报废", d.pending_scraps, "报废流程未完成"],
        ])}
        ${metricGroup("追溯概览", "系统主数据与临床使用规模", "溯", "", [
          ["器械档案", d.total_devices, "已建档器械主数据"],
          ["已服务患者", d.served_patients, "已绑定患者使用记录"],
          ["维护到期", d.maintenance_due, "15 天内需维护校准"],
        ])}
      </section>
      <section class="grid three">
        ${flowCard("采购闭环", "采购员提交计划，管理员审核，通过后采购到货并提交入库单，管理员验收后库存增加。")}
        ${flowCard("临床闭环", "医院工作人员提交申领单，管理员审批，库房发放后库存扣减，使用时绑定患者。")}
        ${flowCard("质量闭环", "临床发现问题后上报质量报告，管理员处理，可关联召回、报废和追溯查询。")}
      </section>
      <section class="grid two">
        ${alertCard("低库存器械", state.alerts.stock_alerts, ["device_name", "stock_qty", "reorder_threshold", "current_location"])}
        ${alertCard("召回与效期", [...(state.alerts.recall_alerts || []), ...(state.alerts.expiry_alerts || [])], ["device_name", "batch_no", "severity", "expiry_date"])}
      </section>
    </div>
  `;
}

function metricGroup(title, desc, icon, tone = "", items = []) {
  return `
    <article class="metric-group ${tone}">
      <div class="metric-group-header">
        <div>
          <h3>${esc(title)}</h3>
          <p>${esc(desc)}</p>
        </div>
        <span class="metric-group-icon">${esc(icon)}</span>
      </div>
      <div class="metric-list">
        ${items.map(([label, value, helper]) => kpi(label, value, tone, helper)).join("")}
      </div>
    </article>
  `;
}

function kpi(label, value = 0, tone = "", helper = "") {
  return `
    <article class="kpi ${tone}">
      <span>${esc(label)}</span>
      <strong>${Number(value || 0)}</strong>
      ${helper ? `<small>${esc(helper)}</small>` : ""}
    </article>
  `;
}

function flowCard(title, text) {
  return `
    <article class="flow-card">
      <h3>${esc(title)}</h3>
      <p class="subtle">${esc(text)}</p>
    </article>
  `;
}

function alertCard(title, rows = [], keys = []) {
  const body = rows.length
    ? rows.slice(0, 8).map((row) => {
        const titleValue = row.device_name || row.recall_no || row.batch_no || "-";
        const meta = keys
          .filter((key) => key !== "device_name")
          .map((key) => row[key])
          .filter((value) => value !== undefined && value !== null && value !== "")
          .join(" · ");
        return `
          <div class="risk-row">
            <div>
              <strong>${esc(titleValue)}</strong>
              <span>${esc(meta || "暂无补充信息")}</span>
            </div>
            ${row.severity ? severityBadge(row.severity) : row.stock_qty !== undefined ? `<span class="badge warn">库存 ${esc(row.stock_qty)}</span>` : `<span class="badge neutral">关注</span>`}
          </div>
        `;
      }).join("")
    : emptyState("暂无预警", "当前没有需要立即处理的库存或质量风险。", "稳");
  return `
    <article class="insight-card">
      <div class="card-head">
        <div>
          <h3>${esc(title)}</h3>
          <p>展示当前最需要关注的风险项</p>
        </div>
        <span class="badge ${rows.length ? "warn" : "ok"}">${rows.length}</span>
      </div>
      <div class="risk-list">${body}</div>
    </article>
  `;
}

function renderDevicesPage() {
  return `
    <div class="grid">
      ${can("devices:write") ? `
        <section class="form-card">
          <h3>新增医疗器械档案</h3>
          <form class="smart-form compact" data-form="create-device">
            ${field("器械名称", "device_name", "text", "一次性压力传感器", "required")}
            ${selectField("分类", "category", textOptions(state.lookups.categories || ["高值耗材", "普耗", "设备"]), "", "required")}
            ${selectField("追溯方式", "trace_mode", textOptions(state.lookups.trace_modes || ["RFID", "QR"]), "", "required")}
            ${selectField("风险等级", "risk_level", textOptions(state.lookups.risk_levels || ["I类", "II类", "III类"]), "", "required")}
            ${selectField("供应商", "supplier_id", supplierOptions(), "", "required")}
            ${field("UDI 编码", "udi_code", "text", "UDI-MD-DEMO-20260001", "required")}
            ${field("规格型号", "model", "text", "MODEL-A")}
            ${field("规格说明", "specification", "text", "成人型")}
            ${field("生产厂家", "manufacturer", "text", "示例医疗")}
            ${field("单位", "unit", "text", "套")}
            ${field("批号", "batch_no", "text", "BATCH-DEMO-202604")}
            ${field("序列号", "serial_no", "text", "SN-DEMO-001")}
            ${field("生产日期", "production_date", "date")}
            ${field("有效期", "expiry_date", "date")}
            ${field("库存阈值", "reorder_threshold", "number", "5")}
            ${field("院内码", "internal_code", "text", "HOSP-DEMO-001")}
            ${field("RFID", "rfid_code", "text", "RFID-DEMO-001")}
            ${field("二维码", "qr_code", "text", "QR-DEMO-001")}
            <div class="form-actions"><button class="primary" type="submit">保存器械档案</button></div>
          </form>
        </section>
      ` : ""}
      ${dataTable("devices", "器械基础信息", state.data.devices, [
        col("device_name", "器械名称"),
        col("category", "分类"),
        col("risk_level", "风险"),
        col("udi_code", "UDI"),
        col("batch_no", "批号"),
        col("stock_qty", "库存"),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
        col("current_location", "位置"),
        col("supplier_name", "供应商"),
      ])}
    </div>
  `;
}

function renderPeoplePage() {
  return `
    <div class="grid">
      ${can("users:manage") ? `
        <section class="form-card">
          <h3>新增系统用户</h3>
          <form class="smart-form compact" data-form="create-user">
            ${field("用户名", "username", "text", "new_user", "required")}
            ${field("初始密码", "password", "text", "123456")}
            ${field("姓名", "display_name", "text", "新用户", "required")}
            ${selectField("角色", "role", textOptions(state.lookups.roles || Object.keys(ROLE_LABELS)), "", "required", ROLE_LABELS)}
            ${field("工号", "job_no", "text", "CLN-099")}
            ${field("电话", "phone", "text", "18802990099")}
            ${field("所属部门", "department", "text", "心内科")}
            <div class="form-actions"><button class="primary" type="submit">创建用户</button></div>
          </form>
        </section>
      ` : ""}
      ${can("purchase_staff:manage") ? `
        <section class="form-card">
          <h3>新增采购人员档案</h3>
          <form class="smart-form compact" data-form="create-purchase-staff">
            ${field("工号", "staff_no", "text", "PUR-009", "required")}
            ${field("姓名", "name", "text", "采购员姓名", "required")}
            ${field("电话", "phone", "text", "18802990009")}
            ${field("部门", "department", "text", "采购办")}
            ${field("岗位", "position", "text", "采购专员")}
            ${textareaField("资质说明", "qualification", "供应商准入审核培训合格")}
            <div class="form-actions"><button class="primary" type="submit">保存采购员</button></div>
          </form>
        </section>
      ` : ""}
      ${dataTable("users", "用户基础信息", state.data.users, [
        col("username", "账号"),
        col("display_name", "姓名"),
        htmlCol("role", "角色", (row) => `<span class="badge">${esc(ROLE_LABELS[row.role] || row.role)}</span>`),
        col("job_no", "工号"),
        col("phone", "电话"),
        col("department", "部门"),
      ])}
      ${dataTable("purchaseStaff", "采购人员信息", state.data.purchaseStaff, [
        col("staff_no", "工号"),
        col("name", "姓名"),
        col("phone", "电话"),
        col("department", "部门"),
        col("position", "岗位"),
        col("qualification", "资质"),
        htmlCol("status", "状态", (row) => statusBadge(row.status || "active")),
      ])}
    </div>
  `;
}

function renderProcurementPage() {
  return `
    <div class="grid">
      ${can("suppliers:write") ? `
        <section class="form-card">
          <h3>新增供应商</h3>
          <form class="smart-form compact" data-form="create-supplier">
            ${field("供应商名称", "name", "text", "陕西示例医疗供应链有限公司", "required")}
            ${field("许可证号", "license_no", "text", "SX-UDI-2026-009")}
            ${field("联系人", "contact_person", "text", "联系人")}
            ${field("联系电话", "phone", "text", "029-80000000")}
            ${textareaField("经营范围", "business_scope", "高值耗材、普耗、设备维保")}
            ${textareaField("供应商资质", "qualification", "医疗器械经营许可证；厂家授权书")}
            <div class="form-actions"><button class="primary" type="submit">保存供应商</button></div>
          </form>
        </section>
      ` : ""}
      ${can("purchase_plan:create") ? `
        <section class="form-card">
          <h3>提交采购计划</h3>
          <form class="smart-form compact" data-form="create-purchase-plan">
            ${selectField("采购器械", "device_id", deviceOptions(), "", "required")}
            ${selectField("供应商", "supplier_id", supplierOptions())}
            ${field("数量", "quantity", "number", "10", "required min=\"1\"")}
            ${field("预估单价", "estimated_unit_price", "number", "100")}
            ${selectField("来源", "source", textOptions(["库存预警", "科室需求", "月度计划", "手工提交"]))}
            ${textareaField("采购原因", "reason", "库存低于安全阈值，需要补货。", "required")}
            <div class="form-actions"><button class="primary" type="submit">提交给管理员审核</button></div>
          </form>
        </section>
      ` : ""}
      ${can("inbound_order:create") ? `
        <section class="form-card">
          <h3>提交采购入库单</h3>
          <form class="smart-form compact" data-form="create-inbound-order">
            ${selectField("关联采购计划", "plan_id", planOptions())}
            ${selectField("到货器械", "device_id", deviceOptions(), "", "required")}
            ${field("到货数量", "quantity", "number", "5", "required min=\"1\"")}
            ${field("入库位置", "warehouse", "text", "中央库房", "required")}
            ${textareaField("到货说明", "remark", "已到货，票据和批号待管理员审核。")}
            <div class="form-actions"><button class="primary" type="submit">提交入库审核</button></div>
          </form>
        </section>
      ` : ""}
      ${dataTable("suppliers", "供应商管理", state.data.suppliers, [
        col("name", "名称"),
        col("business_scope", "经营范围"),
        col("contact_person", "联系人"),
        col("phone", "电话"),
        col("qualification", "资质"),
      ])}
      ${dataTable("purchasePlans", "采购计划审核状态", state.data.purchasePlans, [
        col("plan_no", "计划号"),
        col("device_name", "器械"),
        col("quantity", "数量"),
        col("source", "来源"),
        col("reason", "原因"),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
        col("submitted_by", "提交人"),
        col("reviewed_by", "审核人"),
        htmlCol("actions", "操作", (row) => planActions(row)),
      ])}
      ${dataTable("inboundOrders", "采购入库单", state.data.inboundOrders, [
        col("order_no", "入库单"),
        col("plan_no", "采购计划"),
        col("device_name", "器械"),
        col("quantity", "数量"),
        col("warehouse", "位置"),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
        col("submitted_by", "提交人"),
        col("reviewed_by", "审核人"),
        htmlCol("actions", "操作", (row) => inboundActions(row)),
      ])}
      ${dataTable("procurements", "采购单审批", state.data.procurements, [
        col("request_no", "采购单"),
        col("device_name", "器械"),
        col("supplier_name", "供应商"),
        col("quantity", "数量"),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
        col("requested_by", "申请人"),
        col("approved_by", "审批人"),
        col("received_by", "收货人"),
        htmlCol("actions", "操作", (row) => procurementActions(row)),
      ])}
    </div>
  `;
}

function renderWarehousePage() {
  return `
    <div class="grid">
      ${can("warehouse:write") ? `
        <section class="grid two">
          <article class="form-card">
            <h3>快速入库</h3>
            <form class="smart-form" data-form="warehouse-inbound">
              ${selectField("器械编码", "identifier", deviceCodeOptions(), "", "required")}
              ${field("数量", "quantity", "number", "1", "required min=\"1\"")}
              ${field("库位", "warehouse", "text", "中央库房", "required")}
              ${textareaField("备注", "remark", "手工扫码入库")}
              <div class="form-actions"><button class="primary" type="submit">确认入库</button></div>
            </form>
          </article>
          <article class="form-card">
            <h3>快速出库</h3>
            <form class="smart-form" data-form="warehouse-outbound">
              ${selectField("器械编码", "identifier", deviceCodeOptions(), "", "required")}
              ${field("数量", "quantity", "number", "1", "required min=\"1\"")}
              ${selectField("出库科室", "department_id", departmentOptions(), "", "required")}
              ${textareaField("备注", "remark", "科室领用出库")}
              <div class="form-actions"><button class="primary" type="submit">确认出库</button></div>
            </form>
          </article>
        </section>
      ` : ""}
      ${can("transfer:create") ? `
        <section class="form-card">
          <h3>科室器械调拨</h3>
          <form class="smart-form compact" data-form="create-transfer">
            ${selectField("调拨器械", "device_id", deviceOptions(), "", "required")}
            ${field("数量", "quantity", "number", "1", "required min=\"1\"")}
            ${selectField("调出科室", "from_department_id", departmentOptions(), "", "required")}
            ${selectField("调入科室", "to_department_id", departmentOptions(), "", "required")}
            ${textareaField("调拨原因", "reason", "科室二级库库存调拨", "required")}
            <div class="form-actions"><button class="primary" type="submit">提交调拨</button></div>
          </form>
        </section>
      ` : ""}
      ${dataTable("inventory", "库存查询与低库存查看", state.data.inventory, [
        col("device_name", "器械"),
        col("category", "分类"),
        col("batch_no", "批号"),
        col("stock_qty", "库存"),
        col("reorder_threshold", "阈值"),
        col("expiry_date", "效期"),
        col("current_location", "位置"),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
      ])}
      ${dataTable("deviceRequests", "申领单审批与发放", state.data.deviceRequests, [
        col("request_no", "申领单"),
        col("requester_name", "申请人"),
        col("department_name", "科室"),
        col("device_name", "器械"),
        col("quantity", "数量"),
        col("purpose", "用途"),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
        htmlCol("actions", "操作", (row) => requestActions(row)),
      ])}
      ${dataTable("transfers", "科室调拨记录", state.data.transfers, [
        col("transfer_no", "调拨单"),
        col("device_name", "器械"),
        col("quantity", "数量"),
        col("from_department", "调出"),
        col("to_department", "调入"),
        col("operator_name", "操作人"),
        col("reason", "原因"),
        col("transferred_at", "时间"),
      ])}
    </div>
  `;
}

function renderHospitalPage() {
  return `
    <div class="grid">
      ${can("request:create") ? `
        <section class="form-card">
          <h3>提交器械申领单</h3>
          <form class="smart-form compact" data-form="create-device-request">
            ${selectField("申领器械", "device_id", deviceOptions(), "", "required")}
            ${selectField("申领科室", "department_id", departmentOptions(), "", "required")}
            ${field("数量", "quantity", "number", "1", "required min=\"1\"")}
            ${textareaField("用途", "purpose", "请说明用于什么治疗、手术或科室备用。", "required")}
            <div class="form-actions"><button class="primary" type="submit">提交管理员审批</button></div>
          </form>
        </section>
      ` : ""}
      ${can("clinical:write") ? `
        <section class="form-card">
          <h3>器械使用登记</h3>
          <form class="smart-form compact" data-form="clinical-use">
            ${selectField("器械编码", "identifier", deviceCodeOptions(), "", "required")}
            ${selectField("使用患者", "patient_id", patientOptions(), "", "required")}
            ${selectField("使用科室", "department_id", departmentOptions(), "", "required")}
            ${field("使用数量", "quantity", "number", "1", "required min=\"1\"")}
            ${field("治疗/手术名称", "operation_name", "text", "冠脉介入手术", "required")}
            ${textareaField("备注", "remark", "与病历号、手术间或使用说明关联")}
            <div class="form-actions"><button class="primary" type="submit">登记使用</button></div>
          </form>
        </section>
      ` : ""}
      ${can("quality:write") ? `
        <section class="form-card">
          <h3>器械质量问题上报</h3>
          <form class="smart-form compact" data-form="quality-report">
            ${selectField("问题器械", "device_id", deviceOptions(), "", "required")}
            ${selectField("上报科室", "department_id", departmentOptions(), "", "required")}
            ${selectField("关联患者", "patient_id", patientOptions())}
            ${field("问题类型", "problem_type", "text", "包装破损", "required")}
            ${selectField("严重程度", "severity", textOptions(["高", "中", "低"]), "", "required")}
            ${textareaField("问题描述", "description", "描述发现时间、现象、是否已用于患者、现场处置。", "required")}
            <div class="form-actions"><button class="primary" type="submit">上报给管理员</button></div>
          </form>
        </section>
      ` : ""}
      ${dataTable("myRequests", "申领单状态查询", state.data.deviceRequests, [
        col("request_no", "申领单"),
        col("department_name", "科室"),
        col("device_name", "器械"),
        col("quantity", "数量"),
        col("purpose", "用途"),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
        col("review_note", "审批意见"),
      ])}
      ${dataTable("qualityReports", "质量问题处理状态", state.data.qualityReports, [
        col("report_no", "报告号"),
        col("department_name", "科室"),
        col("device_name", "器械"),
        col("problem_type", "问题"),
        htmlCol("severity", "严重程度", (row) => severityBadge(row.severity)),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
        col("handling_result", "处理结果"),
      ])}
    </div>
  `;
}

function renderRiskPage() {
  return `
    <div class="grid">
      ${can("maintenance:write") ? `
        <section class="form-card">
          <h3>维护/校准登记</h3>
          <form class="smart-form compact" data-form="maintenance">
            ${selectField("器械编码", "identifier", deviceCodeOptions(), "", "required")}
            ${selectField("维护类型", "record_type", textOptions(state.lookups.maintenance_types || ["保养", "维修", "校准"]), "", "required")}
            ${field("计划日期", "plan_date", "date")}
            ${field("完成时间", "completed_at", "datetime-local")}
            ${field("结果", "result", "text", "通过")}
            ${field("服务单位", "vendor_name", "text", "设备维保单位")}
            ${field("下次到期", "next_due_date", "date")}
            ${textareaField("备注", "remark", "校准/维修结果说明")}
            <div class="form-actions"><button class="primary" type="submit">保存维护记录</button></div>
          </form>
        </section>
      ` : ""}
      ${can("recall:manage") ? `
        <section class="form-card">
          <h3>创建召回单</h3>
          <form class="smart-form compact" data-form="recall">
            ${field("批号", "batch_no", "text", "BATCH-DG-202604")}
            ${selectField("或器械编码", "identifier", deviceCodeOptions())}
            ${selectField("召回等级", "severity", textOptions(["高", "中", "低"]), "", "required")}
            ${textareaField("召回原因", "reason", "供应商通报同批次存在质量风险，需要暂停使用并核查流向。", "required")}
            <div class="form-actions"><button class="primary" type="submit">创建召回</button></div>
          </form>
        </section>
      ` : ""}
      ${can("scrap:create") ? `
        <section class="form-card">
          <h3>提交报废申请</h3>
          <form class="smart-form compact" data-form="scrap">
            ${selectField("器械编码", "identifier", deviceCodeOptions(), "", "required")}
            ${field("数量", "quantity", "number", "1", "required min=\"1\"")}
            ${textareaField("报废原因", "reason", "包装破损、污染、过期或设备不可修复。", "required")}
            ${textareaField("备注", "remark", "附照片或处置证明编号")}
            <div class="form-actions"><button class="primary" type="submit">提交报废审批</button></div>
          </form>
        </section>
      ` : ""}
      ${dataTable("maintenance", "维护校准记录", state.data.maintenance, [
        col("device_name", "器械"),
        col("record_type", "类型"),
        col("completed_at", "完成时间"),
        col("result", "结果"),
        col("operator_name", "执行人"),
        col("next_due_date", "下次到期"),
        col("remark", "备注"),
      ])}
      ${dataTable("recalls", "召回管理", state.data.recalls, [
        col("recall_no", "召回单"),
        col("device_name", "器械"),
        col("batch_no", "批号"),
        htmlCol("severity", "等级", (row) => severityBadge(row.severity)),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
        col("affected_patient_count", "影响患者"),
        htmlCol("actions", "操作", (row) => recallActions(row)),
      ])}
      ${dataTable("scraps", "报废管理", state.data.scraps, [
        col("scrap_no", "报废单"),
        col("device_name", "器械"),
        col("quantity", "数量"),
        col("reason", "原因"),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
        htmlCol("actions", "操作", (row) => scrapActions(row)),
      ])}
      ${dataTable("qualityAdmin", "质量问题处理", state.data.qualityReports, [
        col("report_no", "报告号"),
        col("department_name", "科室"),
        col("device_name", "器械"),
        col("problem_type", "类型"),
        htmlCol("severity", "严重程度", (row) => severityBadge(row.severity)),
        htmlCol("status", "状态", (row) => statusBadge(row.status)),
        col("description", "描述"),
        htmlCol("actions", "操作", (row) => qualityActions(row)),
      ])}
    </div>
  `;
}

function renderTracePage() {
  const trace = state.trace;
  return `
    <div class="trace-layout">
      <section class="form-card">
        <h3>追溯查询</h3>
        <form class="smart-form" data-form="trace">
          ${field("UDI / RFID / 院内码 / 批号 / 患者编号", "keyword", "text", "UDI-MD-GJ-20260002", "required")}
          <div class="form-actions"><button class="primary" type="submit">查询追溯链</button></div>
        </form>
        <p class="subtle">可查询采购时间、入库时间、申领发放、临床使用患者、召回、报废和质量问题处理记录。</p>
      </section>
      <section class="card">
        ${trace ? renderTraceResult(trace) : emptyState("等待追溯查询", "请输入 UDI、RFID、院内码、批号或患者编号，系统会展示采购、入库、使用和质量处理链路。", "查")}
      </section>
    </div>
  `;
}

function renderTraceResult(trace) {
  if (trace.mode === "none") {
    return emptyState("未找到追溯记录", `没有找到与 ${trace.query} 相关的器械、批号或患者记录。`, "空");
  }
  const summaryRows = Object.entries(trace.summary || {}).slice(0, 10);
  return `
    <h3>查询结果：${esc(trace.query)} · ${esc(trace.mode)}</h3>
    <div class="grid two">
      <article>
        <h3>摘要</h3>
        <table><tbody>${summaryRows.map(([key, value]) => `<tr><th>${esc(key)}</th><td>${esc(value)}</td></tr>`).join("")}</tbody></table>
      </article>
      <article>
        <h3>关联患者</h3>
        ${(trace.impacted_patients || []).length ? `<table><tbody>${trace.impacted_patients.slice(0, 8).map((row) => `<tr><td>${esc(row.patient_no || "-")}</td><td>${esc(row.name || "-")}</td><td>${esc(row.operation_name || "-")}</td></tr>`).join("")}</tbody></table>` : emptyState("暂无患者使用记录", "该器械或批次暂未登记临床使用患者，后续使用登记后会自动出现在这里。", "患")}
      </article>
    </div>
    <h3>时间线</h3>
    <div class="timeline">
      ${(trace.timeline || []).map((item) => `
        <div class="timeline-item">
          <strong>${esc(item.event_title)}</strong>
          <p class="subtle">${esc(item.event_desc || "")}</p>
          <small>${esc(item.occurred_at)} · ${esc(item.actor || "-")} · ${esc(item.location || "-")}</small>
        </div>
      `).join("") || emptyState("暂无时间线", "当前对象还没有采购、入库、使用、维护或质量处理事件。", "线")}
    </div>
  `;
}

function emptyState(title, text, icon = "空") {
  return `
    <div class="empty">
      <div class="empty-state">
        <span class="empty-icon">${esc(icon)}</span>
        <strong class="empty-title">${esc(title)}</strong>
        <span class="empty-text">${esc(text)}</span>
      </div>
    </div>
  `;
}

function renderReportsPage() {
  return `
    <div class="grid">
      <section class="grid three">
        ${kpi("近 30 天采购", state.data.reportSummary.recent_procurements)}
        ${kpi("近 30 天使用", state.data.reportSummary.recent_usages)}
        ${kpi("近 30 天维护", state.data.reportSummary.recent_maintenances)}
      </section>
      ${can("reports:view") ? `<button class="primary" type="button" data-action="export-report">导出综合报表 Excel</button>` : ""}
      ${dataTable("auditLogs", "审计日志", state.data.auditLogs, [
        col("username", "用户"),
        col("role", "角色"),
        col("action", "动作"),
        col("target_type", "对象"),
        col("target_name", "名称"),
        col("detail", "详情"),
        col("created_at", "时间"),
      ])}
    </div>
  `;
}

function dataTable(id, title, rows = [], columns = []) {
  const normalizedRows = Array.isArray(rows) ? rows : [];
  const body = normalizedRows.length
    ? normalizedRows.map((row) => {
        const searchable = JSON.stringify(row).toLowerCase();
        return `
          <tr data-search="${escAttr(searchable)}">
            ${columns.map((column) => {
              const value = column.html ? column.render(row) : esc(column.render ? column.render(row) : row[column.key] ?? "-");
              return `<td>${value || "-"}</td>`;
            }).join("")}
          </tr>
        `;
      }).join("")
    : `<tr><td colspan="${columns.length}">暂无数据</td></tr>`;
  return `
    <section class="table-card">
      <div class="table-toolbar">
        <div>
          <h3>${esc(title)} <span class="badge neutral">${normalizedRows.length}</span></h3>
        </div>
        <input type="search" data-table-filter="${escAttr(id)}" placeholder="查询 ${escAttr(title)}" />
      </div>
      <div class="table-wrap">
        <table class="data-table">
          <thead><tr>${columns.map((column) => `<th>${esc(column.label)}</th>`).join("")}</tr></thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    </section>
  `;
}

function col(key, label, render) {
  return { key, label, render };
}

function htmlCol(key, label, render) {
  return { key, label, render, html: true };
}

function planActions(row) {
  if (!can("purchase_plan:approve") || row.status !== "submitted") return "-";
  return actionRow([
    ["approve-plan", row.id, "通过"],
    ["reject-plan", row.id, "驳回", "danger"],
  ]);
}

function inboundActions(row) {
  if (!can("inbound_order:approve") || !["submitted", "approved"].includes(row.status)) return "-";
  return actionRow([
    ["receive-inbound", row.id, "验收入库"],
    ["reject-inbound", row.id, "驳回", "danger"],
  ]);
}

function procurementActions(row) {
  const actions = [];
  if (can("procurement:approve") && row.status === "requested") actions.push(["approve-procurement", row.id, "审批"]);
  if (can("procurement:receive") && row.status === "approved") actions.push(["receive-procurement", row.id, "收货"]);
  return actions.length ? actionRow(actions) : "-";
}

function requestActions(row) {
  const actions = [];
  if (can("request:approve") && row.status === "submitted") {
    actions.push(["approve-request", row.id, "通过"]);
    actions.push(["reject-request", row.id, "驳回", "danger"]);
  }
  if (can("request:issue") && row.status === "approved") actions.push(["issue-request", row.id, "发放"]);
  return actions.length ? actionRow(actions) : "-";
}

function qualityActions(row) {
  if (!can("quality:manage") || ["resolved", "rejected"].includes(row.status)) return "-";
  return actionRow([
    ["process-quality", row.id, "受理"],
    ["resolve-quality", row.id, "解决"],
  ]);
}

function recallActions(row) {
  if (!can("recall:manage") || row.status === "closed") return "-";
  return actionRow([["close-recall", row.id, "关闭召回"]]);
}

function scrapActions(row) {
  const actions = [];
  if (can("scrap:approve") && row.status === "requested") actions.push(["approve-scrap", row.id, "审批"]);
  if (can("scrap:dispose") && row.status === "approved") actions.push(["dispose-scrap", row.id, "处置"]);
  return actions.length ? actionRow(actions) : "-";
}

function actionRow(actions) {
  return `<div class="action-row">${actions.map(([action, id, label, tone]) => `<button class="small-btn ${tone || ""}" type="button" data-action="${action}" data-id="${id}">${esc(label)}</button>`).join("")}</div>`;
}

async function handleSubmit(event) {
  const form = event.target.closest("form[data-form]");
  if (!form) return;
  event.preventDefault();
  const formName = form.dataset.form;
  const payload = cleanPayload(Object.fromEntries(new FormData(form)));
  try {
    if (formName === "login") {
      const result = await apiPost("/api/login", payload);
      state.user = result.user;
      state.message = null;
      await loadAll();
      render();
      return;
    }
    if (formName === "trace") {
      state.trace = await apiGet(`/api/traceability?keyword=${encodeURIComponent(payload.keyword)}`);
      render();
      return;
    }
    const endpoint = formEndpoint(formName);
    await apiPost(endpoint, payload);
    setMessage("操作已完成，数据已刷新。");
    await loadAll();
    render();
  } catch (error) {
    setMessage(error.message, "error");
    render();
  }
}

function formEndpoint(formName) {
  const map = {
    "create-device": "/api/devices",
    "create-user": "/api/users",
    "create-supplier": "/api/suppliers",
    "create-purchase-staff": "/api/purchase-staff",
    "create-purchase-plan": "/api/purchase-plans",
    "create-inbound-order": "/api/inbound-orders",
    "create-device-request": "/api/device-requests",
    "clinical-use": "/api/clinical-uses",
    "quality-report": "/api/quality-reports",
    "create-transfer": "/api/transfers",
    "warehouse-inbound": "/api/warehouse/inbound",
    "warehouse-outbound": "/api/warehouse/outbound",
    maintenance: "/api/maintenance",
    recall: "/api/recalls",
    scrap: "/api/scraps",
  };
  if (!map[formName]) throw new Error("未配置表单接口。");
  return map[formName];
}

async function handleClick(event) {
  const routeButton = event.target.closest("[data-route]");
  if (routeButton) {
    state.active = routeButton.dataset.route;
    state.message = null;
    render();
    return;
  }
  const button = event.target.closest("[data-action]");
  if (!button) return;
  const action = button.dataset.action;
  if (action === "logout") {
    await apiPost("/api/logout", {});
    state.user = null;
    state.message = null;
    render();
    return;
  }
  if (action === "refresh") {
    await loadAll();
    setMessage("数据已刷新。");
    render();
    return;
  }
  if (action === "export-report") {
    window.location.href = "/api/reports/export";
    return;
  }
  await runRowAction(action, Number(button.dataset.id));
}

async function runRowAction(action, id) {
  try {
    switch (action) {
      case "approve-plan":
        await apiPost("/api/purchase-plans/approve", { plan_id: id, decision: "approved", review_note: "管理员审核通过。" });
        break;
      case "reject-plan":
        await apiPost("/api/purchase-plans/approve", { plan_id: id, decision: "rejected", review_note: promptText("驳回原因", "库存已有在途订单，暂缓采购。") });
        break;
      case "receive-inbound":
        await apiPost("/api/inbound-orders/approve", { order_id: id, decision: "received", review_note: "票据、批号、数量核对一致，准予入库。" });
        break;
      case "reject-inbound":
        await apiPost("/api/inbound-orders/approve", { order_id: id, decision: "rejected", review_note: promptText("驳回原因", "到货资料不完整，退回采购员补充。") });
        break;
      case "approve-procurement":
        await apiPost("/api/procurements/approve", { procurement_id: id, remark: "管理员审批通过。" });
        break;
      case "receive-procurement":
        await apiPost("/api/procurements/receive", { procurement_id: id, warehouse: promptText("入库位置", "中央库房") });
        break;
      case "approve-request":
        await apiPost("/api/device-requests/approve", { request_id: id, decision: "approved", review_note: "同意申领，请库房按批号发放。" });
        break;
      case "reject-request":
        await apiPost("/api/device-requests/approve", { request_id: id, decision: "rejected", review_note: promptText("驳回原因", "用途说明不完整，请补充后重新提交。") });
        break;
      case "issue-request":
        await apiPost("/api/device-requests/issue", { request_id: id, remark: "库房已发放并扣减库存。" });
        break;
      case "process-quality":
        await apiPost("/api/quality-reports/handle", { report_id: id, status: "processing", handling_result: "管理员已受理，正在核查批号与流向。" });
        break;
      case "resolve-quality":
        await apiPost("/api/quality-reports/handle", { report_id: id, status: "resolved", handling_result: promptText("处理结果", "已完成核查并归档处理结果。") });
        break;
      case "close-recall":
        await apiPost("/api/recalls/close", { recall_case_id: id, disposal_note: promptText("召回关闭说明", "库存和患者流向核查完成，处置结果已归档。") });
        break;
      case "approve-scrap":
        await apiPost("/api/scraps/approve", { scrap_request_id: id, remark: "同意报废，待完成处置证明。" });
        break;
      case "dispose-scrap":
        await apiPost("/api/scraps/dispose", { scrap_request_id: id, remark: promptText("处置说明", "已完成销毁并上传证明。") });
        break;
      default:
        throw new Error("未知操作。");
    }
    setMessage("操作已完成，数据已刷新。");
    await loadAll();
    render();
  } catch (error) {
    setMessage(error.message, "error");
    render();
  }
}

function handleInput(event) {
  const input = event.target.closest("[data-table-filter]");
  if (!input) return;
  const card = input.closest(".table-card");
  const keyword = input.value.trim().toLowerCase();
  card?.querySelectorAll("tbody tr[data-search]").forEach((row) => {
    row.classList.toggle("hidden", keyword && !row.dataset.search.includes(keyword));
  });
}

function promptText(title, fallback) {
  const value = window.prompt(title, fallback);
  return value === null ? fallback : value.trim() || fallback;
}

function cleanPayload(payload) {
  const result = {};
  Object.entries(payload).forEach(([key, value]) => {
    if (value !== "") result[key] = value;
  });
  return result;
}

function setMessage(message, type = "info") {
  state.message = message;
  state.messageType = type;
}

function statusBadge(status) {
  const label = STATUS_LABELS[status] || status || "-";
  const danger = ["rejected", "open", "recalled", "scrapped"].includes(status);
  const warn = ["submitted", "requested", "processing", "approved", "purchased"].includes(status);
  const ok = ["completed", "received", "issued", "disposed", "resolved", "closed", "in_stock", "registered", "active"].includes(status);
  const tone = danger ? "danger" : warn ? "warn" : ok ? "ok" : "neutral";
  return `<span class="badge ${tone}">${esc(label)}</span>`;
}

function severityBadge(value) {
  const tone = value === "高" ? "danger" : value === "中" ? "warn" : value ? "ok" : "neutral";
  return `<span class="badge ${tone}">${esc(value || "-")}</span>`;
}

function field(label, name, type = "text", placeholder = "", attrs = "") {
  return `
    <label>
      ${esc(label)}
      <input type="${escAttr(type)}" name="${escAttr(name)}" placeholder="${escAttr(placeholder)}" ${attrs} />
    </label>
  `;
}

function textareaField(label, name, placeholder = "", attrs = "") {
  return `
    <label class="wide">
      ${esc(label)}
      <textarea name="${escAttr(name)}" placeholder="${escAttr(placeholder)}" ${attrs}></textarea>
    </label>
  `;
}

function selectField(label, name, options = [], value = "", attrs = "", labelMap = null) {
  const optionHtml = [`<option value="">请选择</option>`]
    .concat(options.map((option) => {
      const optionValue = typeof option === "object" ? option.value : option;
      const optionLabel = typeof option === "object" ? option.label : labelMap?.[option] || option;
      return `<option value="${escAttr(optionValue)}" ${String(optionValue) === String(value) ? "selected" : ""}>${esc(optionLabel)}</option>`;
    }))
    .join("");
  return `
    <label>
      ${esc(label)}
      <select name="${escAttr(name)}" ${attrs}>${optionHtml}</select>
    </label>
  `;
}

function textOptions(values = []) {
  return values.map((value) => ({ value, label: value }));
}

function supplierOptions() {
  return (state.lookups.suppliers || state.data.suppliers || []).map((item) => ({ value: item.id, label: item.name }));
}

function departmentOptions() {
  return (state.lookups.departments || []).map((item) => ({ value: item.id, label: `${item.name} · ${item.type}` }));
}

function patientOptions() {
  return (state.lookups.patients || []).map((item) => ({ value: item.id, label: `${item.patient_no} · ${item.name}` }));
}

function deviceOptions() {
  return (state.lookups.devices || state.data.devices || []).map((item) => ({
    value: item.id,
    label: `${item.device_name}${item.stock_qty !== undefined ? ` · 库存 ${item.stock_qty}` : ""}${item.batch_no ? ` · ${item.batch_no}` : ""}`,
  }));
}

function deviceCodeOptions() {
  return (state.lookups.devices || state.data.devices || []).map((item) => ({
    value: item.udi_code || item.batch_no || item.device_name,
    label: `${item.device_name} · ${item.udi_code || item.batch_no || "无编码"}`,
  }));
}

function planOptions() {
  return (state.lookups.purchase_plans || state.data.purchasePlans || []).map((item) => ({ value: item.id, label: `${item.plan_no} · ${STATUS_LABELS[item.status] || item.status}` }));
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  })[char]);
}

function escAttr(value) {
  return esc(value).replace(/`/g, "&#096;");
}

function formatDateTime(value) {
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(value);
}
