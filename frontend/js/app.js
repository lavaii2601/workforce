const state = {
  token: localStorage.getItem("wm_token"),
  currentUser: null,
  shifts: [],
  branches: [],
  employeeBranches: [],
  sidebarCollapsed: localStorage.getItem("wm_sidebar_collapsed") !== "0",
  profileAvatarDataUrl: "",
  oneTimeScan: null,
};

const ceoBranchState = {
  query: "",
  page: 1,
  pageSize: 5,
  totalPages: 1,
};

const ceoBranchAuditState = {
  page: 1,
  pageSize: 6,
  totalPages: 1,
  branchId: null,
};

const ceoExportState = {
  branchId: "",
};

const managerScheduleUiState = {
  activeTab: "preferences",
};

const managerEmployeeUiState = {
  keyword: "",
};

const authUiState = {
  screen: "login",
};

const WEEK_DAYS = [1, 2, 3, 4, 5, 6, 7];
const managerStaffingRules = new Map();

const ROUTES = {
  employee: [
    { key: "employee-attendance", title: "Chấm công" },
    { key: "employee-shifts", title: "Chọn ca làm trong tuần" },
    { key: "employee-assigned", title: "Lịch đã sắp xếp" },
    { key: "employee-issues", title: "Báo cáo vấn đề" },
  ],
  manager: [
    { key: "manager-attendance", title: "Chấm công" },
    { key: "manager-self-shifts", title: "Chọn ca của quản lý" },
    { key: "manager-schedule", title: "Sắp xếp ca nhân viên" },
    { key: "manager-issues", title: "Tiếp nhận và báo cáo" },
    { key: "manager-employees", title: "Quản lí nhân sự" },
    { key: "manager-export", title: "Xuất CSV giờ làm" },
  ],
  ceo: [
    { key: "ceo-chat", title: "Chat tong hop" },
    { key: "ceo-issues", title: "Báo cáo cấp cao" },
    { key: "ceo-export", title: "Xuất CSV toàn hệ thống" },
    { key: "ceo-branches", title: "Quản lý chi nhánh" },
    { key: "ceo-users", title: "Tạo tài khoản nhân sự" },
  ],
};

const $ = (selector) => document.querySelector(selector);

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function mondayOfCurrentWeekISO() {
  const now = new Date();
  const day = now.getDay();
  const diff = day === 0 ? -6 : 1 - day;
  now.setDate(now.getDate() + diff);
  return now.toISOString().slice(0, 10);
}

function formatDateTimeDisplay(raw) {
  const text = String(raw || "").trim();
  const matched = text.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::\d{2})?$/);
  if (!matched) {
    return text || "-";
  }
  const [, yyyy, mm, dd, hh, min] = matched;
  return `${dd}/${mm}/${yyyy} ${hh}:${min}`;
}

function showToast(message, isError = false) {
  const toast = $("#toast");
  toast.textContent = message;
  toast.classList.remove("hidden", "error");
  if (isError) {
    toast.classList.add("error");
  }
  setTimeout(() => toast.classList.add("hidden"), 2500);
}

async function api(path, options = {}) {
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };
  if (state.token) {
    headers.Authorization = `Bearer ${state.token}`;
  }

  const res = await fetch(path, { ...options, headers });
  const payload = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(payload.error || `Request failed: ${res.status}`);
  }
  return payload;
}

async function fetchCsv(path) {
  const headers = {};
  if (state.token) {
    headers.Authorization = `Bearer ${state.token}`;
  }
  const res = await fetch(path, { headers });
  if (!res.ok) {
    const payload = await res.json().catch(() => ({}));
    throw new Error(payload.error || `Export failed: ${res.status}`);
  }
  const blob = await res.blob();
  const disposition = res.headers.get("Content-Disposition") || "";
  const m = disposition.match(/filename=([^;]+)/i);
  const filename = m ? m[1].trim() : "payroll.csv";
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function fetchCsvText(path) {
  const headers = {};
  if (state.token) {
    headers.Authorization = `Bearer ${state.token}`;
  }
  const res = await fetch(path, { headers });
  if (!res.ok) {
    const payload = await res.json().catch(() => ({}));
    throw new Error(payload.error || `Preview failed: ${res.status}`);
  }
  return res.text();
}

function parseCsvLine(line) {
  const out = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      const next = line[i + 1];
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(current);
      current = "";
      continue;
    }
    current += ch;
  }
  out.push(current);
  return out;
}

function parseCsv(text) {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (!lines.length) {
    return { headers: [], rows: [] };
  }

  const headers = parseCsvLine(lines[0]);
  const rows = lines.slice(1).map((line) => parseCsvLine(line));
  return { headers, rows };
}

async function previewCsv(path, containerSelector) {
  const root = $(containerSelector);
  root.classList.remove("hidden");
  root.innerHTML = "<p class='muted'>Đang tải dữ liệu xem trước...</p>";

  const text = await fetchCsvText(path);
  const parsed = parseCsv(text);
  if (!parsed.headers.length) {
    root.innerHTML = "<p class='muted'>Không có dữ liệu để xem trước.</p>";
    return;
  }

  const maxRows = 12;
  const previewRows = parsed.rows.slice(0, maxRows);
  const table = document.createElement("table");
  table.className = "csv-preview-table";

  const headHtml = parsed.headers.map((h) => `<th>${h}</th>`).join("");
  const bodyHtml = previewRows
    .map((row) => `<tr>${parsed.headers.map((_, i) => `<td>${row[i] || ""}</td>`).join("")}</tr>`)
    .join("");

  table.innerHTML = `<thead><tr>${headHtml}</tr></thead><tbody>${bodyHtml}</tbody>`;

  const note = document.createElement("p");
  note.className = "muted csv-preview-note";
  note.textContent =
    parsed.rows.length > maxRows
      ? `Đang hiển thị ${maxRows}/${parsed.rows.length} dòng. Bấm Tải CSV để lấy đầy đủ.`
      : `Đang hiển thị ${parsed.rows.length} dòng.`;

  root.innerHTML = "";
  root.appendChild(note);
  root.appendChild(table);
}

function roleLabel(role) {
  if (role === "employee") return "Nhân viên";
  if (role === "manager") return "Quản lý";
  if (role === "ceo") return "CEO";
  return role;
}

function setShellByAuth() {
  const loggedIn = !!state.currentUser;
  const loginView = $("#login-view");
  const changePasswordView = $("#change-password-view");
  loginView.classList.toggle("hidden", loggedIn || authUiState.screen !== "login");
  changePasswordView.classList.toggle("hidden", loggedIn || authUiState.screen !== "change-password");
  $("#app-view").classList.toggle("hidden", !loggedIn);

  if (!loggedIn) {
    toggleProfileRequiredModal(false);
    document.body.classList.remove("no-scroll");
    return;
  }

  $("#sidebar-role").textContent = roleLabel(state.currentUser.role);
  $("#sidebar-user").textContent = `${state.currentUser.display_name} (${state.currentUser.username})`;
  const avatarText = (state.currentUser.display_name || state.currentUser.username || "U")
    .split(" ")
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase() || "")
    .join("");
  const avatarNode = $("#sidebar-avatar");
  avatarNode.textContent = avatarText || "U";
  const avatarImage = state.currentUser.profile?.avatar_data_url || "";
  if (avatarImage) {
    avatarNode.style.backgroundImage = `url(${avatarImage})`;
    avatarNode.style.backgroundSize = "cover";
    avatarNode.style.backgroundPosition = "center";
    avatarNode.style.color = "transparent";
  } else {
    avatarNode.style.backgroundImage = "";
    avatarNode.style.color = "#13412f";
  }
  applySidebarState();
  toggleProfileRequiredModal(shouldRequireProfileCompletion());
  renderNav();
}

function switchAuthScreen(screen) {
  authUiState.screen = screen === "change-password" ? "change-password" : "login";
  if (!state.currentUser) {
    setShellByAuth();
  }
}

function shouldRequireProfileCompletion() {
  return !!(state.currentUser && state.currentUser.needs_profile_completion);
}

function setProfileAvatarPreview(dataUrl) {
  const preview = $("#profile-avatar-preview");
  const fallback = $("#profile-avatar-fallback");
  if (!preview || !fallback) return;

  if (dataUrl) {
    preview.src = dataUrl;
    preview.classList.remove("hidden");
    fallback.classList.add("hidden");
  } else {
    preview.src = "";
    preview.classList.add("hidden");
    fallback.classList.remove("hidden");
  }
}

function toggleProfileRequiredModal(show) {
  const modal = $("#profile-required-modal");
  if (!modal) return;
  modal.classList.toggle("hidden", !show);
}

async function loadProfileRequiredForm() {
  if (!state.currentUser) return;
  const payload = await api("/api/profile/me");
  const profile = payload.profile || {};
  $("#profile-full-name").value = profile.full_name || "";
  $("#profile-dob").value = profile.date_of_birth || "";
  $("#profile-phone").value = profile.phone_number || "";
  $("#profile-address").value = profile.address || "";

  state.profileAvatarDataUrl = profile.avatar_data_url || "";
  setProfileAvatarPreview(state.profileAvatarDataUrl);
}

async function submitRequiredProfile() {
  const full_name = $("#profile-full-name").value.trim();
  const date_of_birth = $("#profile-dob").value;
  const phone_number = $("#profile-phone").value.trim();
  const address = $("#profile-address").value.trim();
  const avatar_data_url = state.profileAvatarDataUrl;

  const payload = await api("/api/profile/me", {
    method: "PUT",
    body: JSON.stringify({ full_name, date_of_birth, phone_number, address, avatar_data_url }),
  });

  state.currentUser = payload.user;
  persistSession();
  setShellByAuth();
  toggleProfileRequiredModal(false);
  await renderRoute();
  showToast("Da cap nhat ho so ca nhan");
}

function persistSession() {
  if (state.token && state.currentUser) {
    localStorage.setItem("wm_token", state.token);
    localStorage.setItem("wm_current_user", JSON.stringify(state.currentUser));
  } else {
    localStorage.removeItem("wm_token");
    localStorage.removeItem("wm_current_user");
  }
}

function currentWeek() {
  return $("#week-start").value;
}

function activeRouteKey() {
  const hash = window.location.hash || "";
  const key = hash.replace("#/", "").trim();
  return key;
}

function defaultRouteForRole(role) {
  return ROUTES[role][0].key;
}

function ensureValidRoute() {
  if (!state.currentUser) return;
  const allowed = new Set(ROUTES[state.currentUser.role].map((r) => r.key));
  const cur = activeRouteKey();
  if (!allowed.has(cur)) {
    window.location.hash = `#/${defaultRouteForRole(state.currentUser.role)}`;
  }
}

function renderNav() {
  const nav = $("#nav-links");
  nav.innerHTML = "";
  const cur = activeRouteKey();
  ROUTES[state.currentUser.role].forEach((route) => {
    const button = document.createElement("button");
    button.className = cur === route.key ? "nav-link active" : "nav-link ghost";
    button.innerHTML = `<span class="nav-link-dot"></span><span class="nav-link-title">${route.title}</span>`;
    button.addEventListener("click", () => {
      window.location.hash = `#/${route.key}`;
      setSidebarCollapsed(true);
    });
    nav.appendChild(button);
  });
}

async function renderRoute() {
  if (!state.currentUser) return;
  ensureValidRoute();
  const key = activeRouteKey();
  renderNav();

  document.querySelectorAll(".route-view").forEach((node) => node.classList.add("hidden"));
  const routeNode = $(`#route-${key}`);
  if (!routeNode) return;
  routeNode.classList.remove("hidden");

  const routeInfo = ROUTES[state.currentUser.role].find((r) => r.key === key);
  $("#page-title").textContent = routeInfo ? routeInfo.title : "Dashboard";

  if (key === "employee-attendance") await loadMyAttendance("employee");
  if (key === "employee-shifts") await loadEmployeeShifts();
  if (key === "employee-assigned") await loadEmployeeAssignedSchedule();
  if (key === "employee-issues") await loadMyIssues();

  if (key === "manager-attendance") {
    await loadMyAttendance("manager");
    await loadManagerShiftAttendanceToday();
    $("#manager-attendance-one-time-meta").textContent =
      "Nhấn Tạo QR cho ngày hôm nay. Mỗi lần nhân viên quét sẽ nhận random key one-time riêng.";
    try {
      await generateManagerOneTimeQr();
    } catch (error) {
      const oneTimeQrImage = $("#manager-attendance-one-time-qr-image");
      if (oneTimeQrImage) {
        oneTimeQrImage.classList.add("hidden");
        oneTimeQrImage.removeAttribute("src");
      }
      const message = error instanceof Error ? error.message : "Khong tai duoc QR hom nay";
      $("#manager-attendance-one-time-meta").textContent =
        `Khong tai duoc QR hom nay: ${message}. Vui long bam Tao QR lai.`;
    }
  }
  if (key === "manager-self-shifts") await loadManagerSelfShifts();
  if (key === "manager-schedule") await loadManagerSchedule();
  if (key === "manager-issues") await loadManagerIssues();
  if (key === "manager-employees") await loadManagerEmployees();

  if (key === "ceo-chat") await loadCeoChat();
  if (key === "ceo-issues") await loadCeoIssues();
  if (key === "ceo-export") {
    loadCeoExportBranchOptions();
  }
  if (key === "ceo-branches") {
    await Promise.all([loadCeoBranches(), loadBranchAuditLogs()]);
  }
  if (key === "ceo-users") {
    loadCeoUserBranchOptions();
    syncCeoUserRoleForm();
    await loadCeoUsers();
  }
}

function setManagerScheduleTab(tabKey) {
  managerScheduleUiState.activeTab = tabKey;
  document.querySelectorAll("#manager-schedule-tabs .schedule-tab").forEach((tab) => {
    const active = tab.dataset.scheduleTab === tabKey;
    tab.classList.toggle("active", active);
  });

  const paneMap = {
    preferences: "#manager-schedule-pane-preferences",
    assigned: "#manager-schedule-pane-assigned",
    staffing: "#manager-schedule-pane-staffing",
  };
  Object.entries(paneMap).forEach(([key, selector]) => {
    const pane = $(selector);
    if (!pane) return;
    pane.classList.toggle("hidden", key !== tabKey);
    pane.classList.toggle("active", key === tabKey);
  });
}

function initManagerScheduleTabs() {
  const tabsRoot = $("#manager-schedule-tabs");
  if (!tabsRoot || tabsRoot.dataset.bound === "true") {
    setManagerScheduleTab(managerScheduleUiState.activeTab);
    return;
  }

  tabsRoot.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLButtonElement)) return;
    const tabKey = target.dataset.scheduleTab;
    if (!tabKey) return;
    setManagerScheduleTab(tabKey);
  });
  tabsRoot.dataset.bound = "true";
  setManagerScheduleTab(managerScheduleUiState.activeTab);
}

function applySidebarState() {
  const appView = $("#app-view");
  const sidebar = $("#app-sidebar");
  const toggle = $("#btn-sidebar-toggle");
  const overlay = $("#sidebar-overlay");
  if (!appView || !sidebar || !toggle || !overlay) return;

  appView.classList.toggle("sidebar-collapsed", state.sidebarCollapsed);
  overlay.classList.toggle("hidden", state.sidebarCollapsed);
  document.body.classList.toggle("no-scroll", !state.sidebarCollapsed);
  toggle.setAttribute("aria-label", state.sidebarCollapsed ? "Mo menu" : "Dong menu");
}

function setSidebarCollapsed(collapsed) {
  state.sidebarCollapsed = collapsed;
  localStorage.setItem("wm_sidebar_collapsed", collapsed ? "1" : "0");
  applySidebarState();
}

function toggleSidebar() {
  setSidebarCollapsed(!state.sidebarCollapsed);
}

function fillBranchSelect(selectEl, branches, { includeAll = false, allLabel = "Tat ca" } = {}) {
  if (!selectEl) return;
  const previousValue = selectEl.value;
  selectEl.innerHTML = "";

  if (includeAll) {
    const allOption = document.createElement("option");
    allOption.value = "";
    allOption.textContent = allLabel;
    selectEl.appendChild(allOption);
  }

  branches.forEach((b) => {
    const op = document.createElement("option");
    op.value = String(b.id);
    op.textContent = b.name;
    selectEl.appendChild(op);
  });

  if (previousValue && [...selectEl.options].some((op) => op.value === previousValue)) {
    selectEl.value = previousValue;
  }
}

function ceoPayrollPath() {
  const params = new URLSearchParams({ week_start: currentWeek() });
  if (ceoExportState.branchId) {
    params.set("branch_id", ceoExportState.branchId);
  }
  return `/api/ceo/payroll-export.csv?${params.toString()}`;
}

function loadCeoExportBranchOptions() {
  const select = $("#ceo-export-branch-filter");
  if (!select) return;
  fillBranchSelect(select, state.branches, { includeAll: true, allLabel: "Tat ca chi nhanh" });
  select.value = ceoExportState.branchId;
}

function loadCeoUserBranchOptions() {
  const single = $("#ceo-new-user-branch-single");
  const multi = $("#ceo-new-user-branch-multi");
  fillBranchSelect(single, state.branches);
  fillBranchSelect(multi, state.branches);
  if (multi && multi.options.length) {
    multi.options[0].selected = true;
  }
}

function syncCeoUserRoleForm() {
  const role = $("#ceo-new-user-role")?.value || "employee";
  $("#ceo-new-user-branch-single-wrap")?.classList.toggle("hidden", role !== "manager");
  $("#ceo-new-user-branch-multi-wrap")?.classList.toggle("hidden", role !== "employee");
}

async function refreshBranchesMeta() {
  const meta = await api("/api/meta");
  state.branches = meta.branches || [];
}

async function loadEmployeeBranches() {
  state.employeeBranches = await api("/api/employee/branches");
  fillBranchSelect($("#employee-attendance-branch"), state.employeeBranches);
  fillBranchSelect($("#employee-issue-branch"), state.employeeBranches);
  fillBranchSelect($("#employee-assigned-branch-filter"), state.employeeBranches, {
    includeAll: true,
    allLabel: "Tat ca chi nhanh",
  });
  fillBranchSelect($("#employee-issue-branch-filter"), state.employeeBranches, {
    includeAll: true,
    allLabel: "Tat ca chi nhanh",
  });
}

async function loadMyAttendance(viewMode) {
  const data = await api(`/api/attendance/my-week?week_start=${encodeURIComponent(currentWeek())}`);
  const target = viewMode === "manager" ? "#manager-attendance-list" : "#employee-attendance-list";
  const list = $(target);
  list.innerHTML = "";
  const fragment = document.createDocumentFragment();
  data.items.forEach((item) => {
    const row = document.createElement("div");
    row.className = "list-item";
    row.innerHTML = `<span>${item.branch_name} | ${formatDateTimeDisplay(item.check_in_at)} -> ${item.check_out_at ? formatDateTimeDisplay(item.check_out_at) : "Đang làm"}</span><strong>${(item.minutes_worked / 60).toFixed(2)}h</strong>`;
    fragment.appendChild(row);
  });
  const sum = document.createElement("div");
  sum.className = "list-item";
  sum.innerHTML = `<span>Tổng giờ trong tuần</span><strong>${(data.total_minutes / 60).toFixed(2)}h</strong>`;
  fragment.appendChild(sum);
  list.appendChild(fragment);
}

async function generateManagerOneTimeQr() {
  const payload = await api("/api/manager/attendance-qr-one-time", {
    method: "POST",
  });
  const imageNode = $("#manager-attendance-one-time-qr-image");
  if (imageNode) {
    imageNode.src = payload.qr_image_data_url || "";
    imageNode.classList.toggle("hidden", !payload.qr_image_data_url);
  }
  $("#manager-attendance-one-time-meta").textContent =
    `Đã tạo QR theo ngày | Hết hạn lúc: ${formatDateTimeDisplay(payload.expires_at)}`;
  showToast("Da tao QR theo ngay");
}

function shiftAttendanceStatusLabel(status) {
  if (status === "present") return "Đã chấm công";
  if (status === "present_override") return "Đã đi làm (quản lý xác nhận)";
  if (status === "absent") return "Vắng";
  if (status === "late_unmarked") return "Quá giờ +15 phút (chưa xác nhận)";
  return "Chờ vào ca";
}

function shiftAttendanceStatusClass(status) {
  if (status === "present") return "is-present";
  if (status === "present_override") return "is-present-override";
  if (status === "absent") return "is-absent";
  if (status === "late_unmarked") return "is-late";
  return "is-pending";
}

async function loadManagerShiftAttendanceToday() {
  const payload = await api("/api/manager/attendance-shifts/today");
  const list = $("#manager-shift-attendance-list");
  if (!list) return;
  if (list.dataset.bound !== "true") {
    list.dataset.bound = "true";
    list.addEventListener("click", async (event) => {
      const btn = event.target.closest("button[data-override-shift]");
      if (!btn) return;

      const scheduleId = Number(btn.dataset.overrideShift);
      if (!scheduleId) return;
      const note = document.querySelector(`input[data-override-note='${scheduleId}']`)?.value?.trim() || "";
      await api("/api/manager/attendance-shifts/override", {
        method: "PUT",
        body: JSON.stringify({ schedule_id: scheduleId, note }),
      });
      showToast("Đã cập nhật trạng thái đi làm");
      await loadManagerShiftAttendanceToday();
    });
  }

  list.innerHTML = "";

  if (!(payload.items || []).length) {
    const empty = document.createElement("div");
    empty.className = "list-item";
    empty.innerHTML = "<span>Hôm nay chưa có ca được phân cho nhân viên.</span>";
    list.appendChild(empty);
    return;
  }

  const fragment = document.createDocumentFragment();
  (payload.items || []).forEach((item) => {
    const row = document.createElement("div");
    row.className = `list-item shift-attendance-item ${shiftAttendanceStatusClass(item.status)}`;
    const canOverride = item.status === "absent" || item.status === "late_unmarked";
    const safeEmployeeName = escapeHtml(item.employee_name);
    const safeShiftCode = escapeHtml(item.shift_code);
    const safeStatus = escapeHtml(shiftAttendanceStatusLabel(item.status));
    const safeNote = escapeHtml(item.note || "");
    row.innerHTML = `
      <div>
        <strong>${safeEmployeeName}</strong> - ${safeShiftCode}<br />
        <small>Bắt đầu ca: ${formatDateTimeDisplay(item.shift_start_at)} | Hạn check-in: ${formatDateTimeDisplay(item.late_deadline_at)}</small><br />
        <small>Trạng thái: <span class="shift-status-badge ${shiftAttendanceStatusClass(item.status)}">${safeStatus}</span></small>
      </div>
      <div class="row compact">
        <input type="text" data-override-note="${item.schedule_id}" placeholder="Ghi chú bằng chứng đi làm" value="${safeNote}" />
        ${canOverride ? `<button class="ghost" data-override-shift="${item.schedule_id}">Đánh đã đi làm</button>` : ""}
      </div>
    `;
    fragment.appendChild(row);
  });
  list.appendChild(fragment);
}

async function scanEmployeeOneTimeQr() {
  const payloadRaw = $("#employee-one-time-qr-payload").value.trim();
  if (!payloadRaw) {
    throw new Error("Vui long nhap du lieu QR one-time");
  }

  const payload = await api("/api/attendance/scan-qr-one-time", {
    method: "POST",
    body: JSON.stringify({ qr_payload: payloadRaw }),
  });

  state.oneTimeScan = {
    branchId: Number(payload.branch_id),
    qrToken: payload.qr_token,
    randomKey: payload.random_key,
  };
  $("#employee-one-time-random-key").value = payload.random_key || "";
  $("#employee-one-time-scan-result").textContent =
    `Random key nhận được: ${payload.random_key || "-"}. Vui lòng nhập key để xác nhận bắt đầu chấm công.`;
  showToast("Da quet QR va nhan random key");
}

async function checkInEmployeeOneTime() {
  const note = $("#attendance-note").value.trim();
  if (!state.oneTimeScan) {
    const payloadInput = $("#employee-one-time-qr-payload");
    const keyInput = $("#employee-one-time-random-key");
    const payloadText = (payloadInput?.value || "").trim();
    const keyText = (keyInput?.value || "").trim();

    if (!payloadText && keyText.toUpperCase().startsWith("WM1|")) {
      payloadInput.value = keyText;
    }

    if ((payloadInput?.value || "").trim()) {
      await scanEmployeeOneTimeQr();
    }

    if (!state.oneTimeScan) {
      throw new Error("Vui long quet QR de lay random key truoc khi check-in");
    }
  }

  const typedKey = $("#employee-one-time-random-key").value.trim().toUpperCase();
  if (!typedKey) {
    throw new Error("Vui long nhap random key one-time");
  }
  if (typedKey !== state.oneTimeScan.randomKey) {
    throw new Error("Random key khong khop voi key vua quet");
  }

  await api("/api/attendance/check-in-qr-one-time", {
    method: "POST",
    body: JSON.stringify({
      branch_id: state.oneTimeScan.branchId,
      qr_token: state.oneTimeScan.qrToken,
      one_time_code: typedKey,
      note,
    }),
  });
  showToast("Check-in one-time thành công");
  $("#employee-one-time-qr-payload").value = "";
  $("#employee-one-time-random-key").value = "";
  $("#employee-one-time-scan-result").textContent = "";
  state.oneTimeScan = null;
  await loadMyAttendance("employee");
}

async function checkOutEmployee() {
  await api("/api/attendance/check-out", { method: "POST" });
  showToast("Check-out thành công");
  await loadMyAttendance("employee");
}

async function checkInManager() {
  const note = $("#manager-attendance-note").value.trim();
  await api("/api/attendance/check-in", {
    method: "POST",
    body: JSON.stringify({ note }),
  });
  showToast("Check-in thành công");
  await loadMyAttendance("manager");
  await loadManagerShiftAttendanceToday();
}

async function checkOutManager() {
  await api("/api/attendance/check-out", { method: "POST" });
  showToast("Check-out thành công");
  await loadMyAttendance("manager");
  await loadManagerShiftAttendanceToday();
}

function shiftText(code) {
  const s = state.shifts.find((x) => x.code === code);
  return s ? `${s.code} (${s.start}-${s.end})` : code;
}

function normalizeDay(value) {
  const day = Number(value);
  if (day >= 1 && day <= 7) return day;
  return null;
}

function expandDays(value) {
  const day = normalizeDay(value);
  return day ? [day] : WEEK_DAYS;
}

function weekDaysMeta() {
  const labels = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"];
  const start = new Date(`${currentWeek()}T00:00:00`);
  return WEEK_DAYS.map((day, index) => {
    let dateLabel = "";
    if (!Number.isNaN(start.getTime())) {
      const d = new Date(start);
      d.setDate(start.getDate() + index);
      dateLabel = `${String(d.getDate()).padStart(2, "0")}/${String(d.getMonth() + 1).padStart(2, "0")}`;
    }
    return { day, label: labels[index], dateLabel };
  });
}

function weekHeaderCellsHtml() {
  return weekDaysMeta()
    .map((meta) => {
      if (!meta.dateLabel) {
        return `<th>${meta.label}</th>`;
      }
      return `<th>${meta.label}<br /><small>${meta.dateLabel}</small></th>`;
    })
    .join("");
}

async function loadEmployeeShifts() {
  const prefs = await api(`/api/employee/preferences?week_start=${encodeURIComponent(currentWeek())}`);
  const checked = new Set(
    prefs.flatMap((x) =>
      expandDays(x.day_of_week).map((day) => `${Number(x.branch_id)}|${x.shift_code}|${day}`)
    )
  );
  const list = $("#employee-shift-grid");
  list.innerHTML = "";

  const board = document.createElement("div");
  board.className = "week-table-wrap";
  board.innerHTML = `
    <table class="week-table">
      <thead>
        <tr>
          <th class="week-shift-col">Chi nhánh - Ca làm</th>
          ${weekHeaderCellsHtml()}
        </tr>
      </thead>
      <tbody id="employee-pref-week-body"></tbody>
    </table>
  `;
  const body = board.querySelector("#employee-pref-week-body");

  state.employeeBranches.forEach((branch) => {
    state.shifts.forEach((shift) => {
      const row = document.createElement("tr");
      const cells = weekDaysMeta()
        .map((meta) => {
          const key = `${branch.id}|${shift.code}|${meta.day}`;
          return `<td><label class="tt-checkbox"><input type="checkbox" data-branch="${branch.id}" data-shift="${shift.code}" data-day="${meta.day}" ${checked.has(key) ? "checked" : ""} /> Chọn</label></td>`;
        })
        .join("");

      row.innerHTML = `<th class="week-shift-col">${branch.name} - ${shiftText(shift.code)}</th>${cells}`;
      body.appendChild(row);
    });
  });

  list.appendChild(board);
}

async function saveEmployeeShifts() {
  const selections = [...document.querySelectorAll("#employee-shift-grid input:checked")].map((el) => ({
    branch_id: Number(el.dataset.branch),
    shift_code: el.dataset.shift,
    day_of_week: Number(el.dataset.day),
  }));
  await api("/api/employee/preferences", {
    method: "PUT",
    body: JSON.stringify({ week_start: currentWeek(), selections }),
  });
  showToast("Đã lưu ca làm");
}

async function loadEmployeeAssignedSchedule() {
  const allItems = await api(
    `/api/employee/assigned-schedule?week_start=${encodeURIComponent(currentWeek())}`
  );
  const selectedBranch = $("#employee-assigned-branch-filter")?.value || "";
  const items = selectedBranch
    ? allItems.filter((item) => String(item.branch_id) === selectedBranch)
    : allItems;
  const list = $("#employee-assigned-list");
  list.innerHTML = "";

  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "list-item";
    empty.innerHTML = "<span>Chưa có lịch được sắp xếp cho tuần này.</span>";
    list.appendChild(empty);
    return;
  }

  const byShift = new Map();
  items.forEach((item) => {
    expandDays(item.day_of_week).forEach((day) => {
      const key = `${item.shift_code}|${day}`;
      if (!byShift.has(key)) {
        byShift.set(key, []);
      }
      byShift.get(key).push(item);
    });
  });

  const board = document.createElement("div");
  board.className = "week-table-wrap";
  const headerCells = weekHeaderCellsHtml();
  board.innerHTML = `
    <table class="week-table">
      <thead>
        <tr>
          <th class="week-shift-col">Ca lam</th>
          ${headerCells}
        </tr>
      </thead>
      <tbody id="employee-assigned-week-body"></tbody>
    </table>
  `;
  const body = board.querySelector("#employee-assigned-week-body");

  state.shifts.forEach((shift) => {
    const row = document.createElement("tr");
    const cells = weekDaysMeta()
      .map((meta) => {
        const rows = byShift.get(`${shift.code}|${meta.day}`) || [];
        const content = rows.length
          ? rows
              .map(
                (item) =>
                  `<span class="tt-pill">${item.branch_name} - Xếp bởi: ${item.assigned_by_name}</span>`
              )
              .join("")
          : `<span class="tt-empty">Trống</span>`;
        return `<td><div class="tt-content">${content}</div></td>`;
      })
      .join("");

    row.innerHTML = `<th class="week-shift-col">${shiftText(shift.code)}</th>${cells}`;
    body.appendChild(row);
  });

  list.appendChild(board);
}

async function submitIssue() {
  const title = $("#employee-issue-title").value.trim();
  const details = $("#employee-issue-details").value.trim();
  const branch_id = Number($("#employee-issue-branch").value);
  await api("/api/issues", {
    method: "POST",
    body: JSON.stringify({ title, details, branch_id }),
  });
  $("#employee-issue-title").value = "";
  $("#employee-issue-details").value = "";
  showToast("Đã gửi báo cáo vấn đề");
  await loadMyIssues();
}

async function loadMyIssues() {
  const allItems = await api("/api/issues/my");
  const selectedBranch = $("#employee-issue-branch-filter")?.value || "";
  const items = selectedBranch
    ? allItems.filter((item) => String(item.branch_id) === selectedBranch)
    : allItems;
  const list = $("#employee-issues-list");
  list.innerHTML = "";
  items.forEach((item) => {
    const row = document.createElement("div");
    row.className = "list-item";
    row.innerHTML = `<span><strong>${item.title}</strong><br /><small>${item.branch_name} | ${item.status}</small></span><span>${item.created_at}</span>`;
    list.appendChild(row);
  });
}

async function loadManagerSelfShifts() {
  const prefs = await api(`/api/manager/self-preferences?week_start=${encodeURIComponent(currentWeek())}`);
  const checked = new Set(
    prefs.flatMap((x) => expandDays(x.day_of_week).map((day) => `${x.shift_code}|${day}`))
  );
  const root = $("#manager-self-shifts-list");
  root.innerHTML = "";

  const board = document.createElement("div");
  board.className = "week-table-wrap";
  board.innerHTML = `
    <table class="week-table">
      <thead>
        <tr>
          <th class="week-shift-col">Ca lam</th>
          ${weekHeaderCellsHtml()}
        </tr>
      </thead>
      <tbody id="manager-self-week-body"></tbody>
    </table>
  `;

  const body = board.querySelector("#manager-self-week-body");
  state.shifts.forEach((shift) => {
    const row = document.createElement("tr");
    const cells = weekDaysMeta()
      .map((meta) => {
        const key = `${shift.code}|${meta.day}`;
        return `<td><label class="tt-checkbox"><input type="checkbox" data-shift="${shift.code}" data-day="${meta.day}" ${checked.has(key) ? "checked" : ""} /> Chọn</label></td>`;
      })
      .join("");
    row.innerHTML = `<th class="week-shift-col">${shiftText(shift.code)}</th>${cells}`;
    body.appendChild(row);
  });

  root.appendChild(board);
}

async function saveManagerSelfShifts() {
  const selections = [...document.querySelectorAll("#manager-self-shifts-list input:checked")].map((el) => ({
    shift_code: el.dataset.shift,
    day_of_week: Number(el.dataset.day),
  }));
  await api("/api/manager/self-preferences", {
    method: "PUT",
    body: JSON.stringify({ week_start: currentWeek(), selections }),
  });
  showToast("Đã lưu ca của quản lý");
}

function groupByEmployee(records) {
  const map = new Map();
  records.forEach((r) => {
    if (!map.has(r.employee_id)) {
      map.set(r.employee_id, { employee_name: r.employee_name, shifts: [] });
    }
    map.get(r.employee_id).shifts.push(r.shift_code);
  });
  return [...map.values()];
}

async function loadManagerSchedule() {
  const [prefs, schedule] = await Promise.all([
    api(`/api/manager/preferences?week_start=${encodeURIComponent(currentWeek())}`),
    api(`/api/manager/schedule?week_start=${encodeURIComponent(currentWeek())}`),
  ]);

  await loadManagerStaffingRules();
  const assigned = new Set(
    schedule.flatMap((x) =>
      expandDays(x.day_of_week).map((day) => `${x.employee_id}|${x.shift_code}|${day}`)
    )
  );

  const prefBox = $("#manager-preferences");
  prefBox.innerHTML = "";

  const prefBoard = document.createElement("div");
  prefBoard.className = "week-table-wrap compact-week-table-wrap";
  const headerCells = weekHeaderCellsHtml();
  prefBoard.innerHTML = `
    <table class="week-table compact-week-table">
      <thead>
        <tr>
          <th class="week-shift-col">Ca lam</th>
          ${headerCells}
        </tr>
      </thead>
      <tbody id="manager-pref-week-body"></tbody>
    </table>
  `;
  const prefBody = prefBoard.querySelector("#manager-pref-week-body");

  state.shifts.forEach((shift, shiftIndex) => {
    const row = document.createElement("tr");
    row.className = `shift-row shift-idx-${shiftIndex % 6}`;
    const cells = weekDaysMeta()
      .map((meta) => {
        const rows = prefs.filter(
          (p) => p.shift_code === shift.code && expandDays(p.day_of_week).includes(meta.day)
        );
        const cellClass = rows.length ? "has-data" : "is-empty";
        let content = `<span class="tt-empty">Không có đăng ký</span>`;
        if (rows.length) {
          content = rows
            .map((p) => {
              const key = `${p.employee_id}|${p.shift_code}|${meta.day}`;
              const checked = assigned.has(key) ? "checked" : "";
              return `<label class="tt-checkbox"><input type="checkbox" data-eid="${p.employee_id}" data-shift="${p.shift_code}" data-day="${meta.day}" ${checked} /> ${p.employee_name}</label>`;
            })
            .join("");
        }
        return `<td class="day-cell day-${meta.day} ${cellClass}"><div class="tt-content">${content}</div></td>`;
      })
      .join("");

    row.innerHTML = `<th class="week-shift-col">${shiftText(shift.code)}</th>${cells}`;
    prefBody.appendChild(row);
  });
  prefBox.appendChild(prefBoard);

  const scheduleBox = $("#manager-schedule");
  scheduleBox.innerHTML = "";

  const scheduleBoard = document.createElement("div");
  scheduleBoard.className = "week-table-wrap compact-week-table-wrap";
  scheduleBoard.innerHTML = `
    <table class="week-table compact-week-table">
      <thead>
        <tr>
          <th class="week-shift-col">Ca lam</th>
          ${headerCells}
        </tr>
      </thead>
      <tbody id="manager-schedule-week-body"></tbody>
    </table>
  `;
  const scheduleBody = scheduleBoard.querySelector("#manager-schedule-week-body");

  state.shifts.forEach((shift, shiftIndex) => {
    const row = document.createElement("tr");
    row.className = `shift-row shift-idx-${shiftIndex % 6}`;
    const cells = weekDaysMeta()
      .map((meta) => {
        const rows = schedule.filter(
          (item) => item.shift_code === shift.code && expandDays(item.day_of_week).includes(meta.day)
        );
        const rule = managerStaffingRules.get(shift.code) || { min_staff: 3, max_staff: 4 };
        const count = rows.length;
        const outOfRange = count > 0 && (count < Number(rule.min_staff) || count > Number(rule.max_staff));
        const cellClass = count === 0 ? "is-empty" : outOfRange ? "is-out-range" : "is-in-range";
        const content = rows.length
          ? rows.map((item) => `<span class="tt-pill">${item.employee_name}</span>`).join("")
          : `<span class="tt-empty">Trống</span>`;
        return `<td class="day-cell day-${meta.day} ${cellClass}"><div class="tt-content">${content}</div></td>`;
      })
      .join("");

    row.innerHTML = `<th class="week-shift-col">${shiftText(shift.code)}</th>${cells}`;
    scheduleBody.appendChild(row);
  });
  scheduleBox.appendChild(scheduleBoard);
  initManagerScheduleTabs();
}

async function saveManagerSchedule() {
  const assignments = [...document.querySelectorAll("#manager-preferences input:checked")].map((el) => ({
    employee_id: Number(el.dataset.eid),
    shift_code: el.dataset.shift,
    day_of_week: Number(el.dataset.day),
  }));
  await api("/api/manager/schedule", {
    method: "PUT",
    body: JSON.stringify({ week_start: currentWeek(), assignments }),
  });
  showToast("Đã cập nhật lịch nhân viên");
  await loadManagerSchedule();
}

async function loadManagerStaffingRules() {
  const rows = await api("/api/manager/staffing-rules");
  managerStaffingRules.clear();
  rows.forEach((row) => {
    managerStaffingRules.set(row.shift_code, {
      min_staff: Number(row.min_staff),
      max_staff: Number(row.max_staff),
    });
  });

  const root = $("#manager-staffing-rules");
  if (!root) return;
  root.innerHTML = "";
  state.shifts.forEach((shift) => {
    const rule = managerStaffingRules.get(shift.code) || { min_staff: 3, max_staff: 4 };
    const row = document.createElement("div");
    row.className = "staffing-rule-row";
    row.innerHTML = `
      <strong>${shiftText(shift.code)}</strong>
      <label>Min <input type="number" min="0" max="20" data-rule-min="${shift.code}" value="${rule.min_staff}" /></label>
      <label>Max <input type="number" min="1" max="30" data-rule-max="${shift.code}" value="${rule.max_staff}" /></label>
    `;
    root.appendChild(row);
  });
}

async function saveManagerStaffingRules() {
  const rules = state.shifts.map((shift) => {
    const minInput = document.querySelector(`input[data-rule-min='${shift.code}']`);
    const maxInput = document.querySelector(`input[data-rule-max='${shift.code}']`);
    return {
      shift_code: shift.code,
      min_staff: Number(minInput?.value || 0),
      max_staff: Number(maxInput?.value || 0),
    };
  });

  await api("/api/manager/staffing-rules", {
    method: "PUT",
    body: JSON.stringify({ rules }),
  });
  showToast("Đã lưu định mức nhân sự theo ca");
  await loadManagerSchedule();
}

async function loadManagerIssues() {
  const items = await api("/api/manager/issues");
  const list = $("#manager-issues-list");
  list.innerHTML = "";

  items.forEach((item) => {
    const row = document.createElement("div");
    row.className = "list-item";
    row.innerHTML = `
      <div>
        <strong>${item.title}</strong><br />
        <small>${item.reporter_name} | ${item.branch_name} | ${item.status}</small>
      </div>
      <div class="row compact">
        <button class="ghost" data-issue-detail-toggle="${item.id}">Xem noi dung</button>
        <select data-status="${item.id}">
          <option value="open" ${item.status === "open" ? "selected" : ""}>open</option>
          <option value="in_review" ${item.status === "in_review" ? "selected" : ""}>in_review</option>
          <option value="resolved" ${item.status === "resolved" ? "selected" : ""}>resolved</option>
          <option value="escalated" ${item.status === "escalated" ? "selected" : ""}>escalated</option>
        </select>
        <label><input type="checkbox" data-escalate="${item.id}" ${item.escalated_to_ceo ? "checked" : ""}/> Escalate</label>
        <input type="text" data-note="${item.id}" placeholder="Ghi chú" value="${item.manager_note || ""}" />
        <button data-save-issue="${item.id}">Lưu</button>
      </div>
      <div class="issue-detail-box hidden" data-issue-detail="${item.id}">
        <p><strong>Noi dung bao cao:</strong> ${item.details || "-"}</p>
        <p><strong>Ghi chu quan ly:</strong> ${item.manager_note || "Chua co"}</p>
      </div>
    `;
    list.appendChild(row);
  });

  document.querySelectorAll("button[data-issue-detail-toggle]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = Number(btn.dataset.issueDetailToggle);
      const detail = document.querySelector(`div[data-issue-detail='${id}']`);
      if (!detail) return;
      detail.classList.toggle("hidden");
      btn.textContent = detail.classList.contains("hidden") ? "Xem noi dung" : "An noi dung";
    });
  });

  document.querySelectorAll("button[data-save-issue]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = Number(btn.dataset.saveIssue);
      const status = document.querySelector(`select[data-status='${id}']`).value;
      const escalate_to_ceo = document.querySelector(`input[data-escalate='${id}']`).checked;
      const manager_note = document.querySelector(`input[data-note='${id}']`).value;
      await api(`/api/manager/issues/${id}`, {
        method: "PUT",
        body: JSON.stringify({ status, escalate_to_ceo, manager_note }),
      });
      showToast("Da cap nhat bao cao");
      await loadManagerIssues();
    });
  });
}

async function loadManagerEmployees() {
  const searchInput = $("#manager-employee-search");
  if (searchInput) {
    searchInput.value = managerEmployeeUiState.keyword;
  }

  const params = new URLSearchParams();
  if (managerEmployeeUiState.keyword) {
    params.set("q", managerEmployeeUiState.keyword);
  }
  const path = params.toString() ? `/api/manager/employees?${params.toString()}` : "/api/manager/employees";
  const payload = await api(path);
  const branchSelect = $("#manager-new-branches");
  if (branchSelect) {
    fillBranchSelect(branchSelect, payload.branches || []);
    [...branchSelect.options].forEach((op) => {
      op.selected = payload.default_branch_ids?.includes(Number(op.value));
    });
  }

  const list = $("#manager-employees-list");
  list.innerHTML = "";
  if (!payload.employees.length) {
    const empty = document.createElement("div");
    empty.className = "list-item";
    empty.innerHTML = `<span>Không tìm thấy nhân viên phù hợp.</span>`;
    list.appendChild(empty);
    return;
  }

  payload.employees.forEach((e) => {
    const phone = String(e.phone_number || "").trim();
    const fullName = String(e.full_name || "").trim() || String(e.display_name || "");
    const address = String(e.address || "").trim();
    const position = String(e.job_position || "").trim();
    const birth = String(e.date_of_birth || "").trim();
    const branchNames = Array.isArray(e.branch_names) ? e.branch_names.join(", ") : "-";
    const row = document.createElement("div");
    row.className = "list-item manager-employee-item";
    row.innerHTML = `
      ${buildEmployeeAvatarHtml(e)}
      <div class="manager-employee-content">
        <div class="manager-employee-view">
          <strong>${escapeHtml(fullName)}</strong> (${escapeHtml(e.username)})<br />
          <small>Tên hiển thị: ${escapeHtml(e.display_name)}</small><br />
          <small>Vị trí: ${escapeHtml(position || "-")} | Ngày sinh: ${escapeHtml(birth || "-")}</small><br />
          <small>Số điện thoại: ${escapeHtml(phone || "-")}</small><br />
          <small>Địa chỉ: ${escapeHtml(address || "-")}</small><br />
          <small>Chi nhánh: ${escapeHtml(branchNames || "-")}</small>
        </div>
        <div class="manager-employee-edit hidden">
          <div class="field-grid manager-employee-edit-grid">
            <label>Tên hiển thị
              <input type="text" data-edit-display-name value="${escapeHtml(e.display_name || "")}" />
            </label>
            <label>Họ tên
              <input type="text" data-edit-full-name value="${escapeHtml(e.full_name || "")}" />
            </label>
            <label>Số điện thoại
              <input type="text" data-edit-phone value="${escapeHtml(phone)}" placeholder="VD: 0901234567" />
            </label>
            <label>Ngày sinh
              <input type="date" data-edit-dob value="${escapeHtml(birth)}" />
            </label>
            <label>Vị trí
              <input type="text" data-edit-position value="${escapeHtml(position)}" />
            </label>
            <label>Địa chỉ
              <input type="text" data-edit-address value="${escapeHtml(address)}" />
            </label>
          </div>
          <small class="muted">Bắt buộc: Tên hiển thị, Họ tên. Có thể cập nhật ngay để tránh sai sót.</small>
        </div>
      </div>
      <div class="row compact">
        <button class="ghost" data-edit-emp="${e.id}">Sửa</button>
        <button class="hidden" data-save-emp="${e.id}">Lưu</button>
        <button class="ghost hidden" data-cancel-edit-emp="${e.id}">Hủy</button>
        <button class="danger" data-del-emp="${e.id}">Xóa</button>
      </div>
    `;
    list.appendChild(row);
  });
}

function validateManagerEmployeeUpdate(payload) {
  if (!payload.display_name) {
    throw new Error("Tên hiển thị không được để trống");
  }
  if (!payload.full_name) {
    throw new Error("Họ tên không được để trống");
  }
  if (payload.phone_number && !/^\+?[0-9]{9,15}$/.test(payload.phone_number)) {
    throw new Error("Số điện thoại không hợp lệ");
  }
  if (payload.date_of_birth && !/^\d{4}-\d{2}-\d{2}$/.test(payload.date_of_birth)) {
    throw new Error("Ngày sinh phải theo định dạng YYYY-MM-DD");
  }
}

function bindManagerEmployeeListEvents() {
  const list = $("#manager-employees-list");
  if (!list || list.dataset.bound === "true") {
    return;
  }
  list.dataset.bound = "true";
  list.addEventListener("click", async (event) => {
    const row = event.target.closest(".manager-employee-item");
    if (!row) return;

    const editBtn = event.target.closest("button[data-edit-emp]");
    if (editBtn) {
      row.classList.add("is-editing");
      row.querySelector(".manager-employee-view")?.classList.add("hidden");
      row.querySelector(".manager-employee-edit")?.classList.remove("hidden");
      row.querySelector("button[data-edit-emp]")?.classList.add("hidden");
      row.querySelector("button[data-save-emp]")?.classList.remove("hidden");
      row.querySelector("button[data-cancel-edit-emp]")?.classList.remove("hidden");
      row.querySelector("input[data-edit-display-name]")?.focus();
      return;
    }

    const cancelBtn = event.target.closest("button[data-cancel-edit-emp]");
    if (cancelBtn) {
      row.classList.remove("is-editing");
      row.querySelector(".manager-employee-view")?.classList.remove("hidden");
      row.querySelector(".manager-employee-edit")?.classList.add("hidden");
      row.querySelector("button[data-edit-emp]")?.classList.remove("hidden");
      row.querySelector("button[data-save-emp]")?.classList.add("hidden");
      row.querySelector("button[data-cancel-edit-emp]")?.classList.add("hidden");
      return;
    }

    const saveBtn = event.target.closest("button[data-save-emp]");
    if (saveBtn) {
      const id = Number(saveBtn.dataset.saveEmp);
      if (!id) return;

      const payload = {
        display_name: String(row.querySelector("input[data-edit-display-name]")?.value || "").trim(),
        full_name: String(row.querySelector("input[data-edit-full-name]")?.value || "").trim(),
        phone_number: String(row.querySelector("input[data-edit-phone]")?.value || "").trim(),
        date_of_birth: String(row.querySelector("input[data-edit-dob]")?.value || "").trim(),
        job_position: String(row.querySelector("input[data-edit-position]")?.value || "").trim(),
        address: String(row.querySelector("input[data-edit-address]")?.value || "").trim(),
      };

      validateManagerEmployeeUpdate(payload);
      await api(`/api/manager/employees/${id}`, {
        method: "PUT",
        body: JSON.stringify(payload),
      });
      showToast("Đã cập nhật thông tin nhân viên");
      await loadManagerEmployees();
      return;
    }

    const btn = event.target.closest("button[data-del-emp]");
    if (!btn) return;

    const id = Number(btn.dataset.delEmp);
    if (!id) return;
    if (!window.confirm("Bạn có chắc muốn xóa tài khoản này?")) return;

    await api(`/api/manager/employees/${id}`, { method: "DELETE" });
    showToast("Đã xóa nhân viên");
    await loadManagerEmployees();
  });
}

async function createEmployee() {
  const username = $("#manager-new-username").value.trim();
  const display_name = $("#manager-new-name").value.trim();
  const password = $("#manager-new-password").value;
  const branch_ids = [...$("#manager-new-branches").selectedOptions].map((op) => Number(op.value));
  await api("/api/manager/employees", {
    method: "POST",
    body: JSON.stringify({ username, display_name, password, branch_ids }),
  });
  $("#manager-new-username").value = "";
  $("#manager-new-name").value = "";
  $("#manager-new-password").value = "";
  showToast("Đã tạo nhân viên mới");
  await loadManagerEmployees();
}

function setManagerEmployeeKeyword(value) {
  managerEmployeeUiState.keyword = (value || "").trim();
}

function buildEmployeeAvatarHtml(employee) {
  const fullName = String(employee.full_name || "").trim() || String(employee.display_name || "").trim();
  const initials = (fullName || String(employee.username || "NV"))
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase() || "")
    .join("")
    .slice(0, 2) || "NV";

  const avatarDataUrl = String(employee.avatar_data_url || "").trim();
  if (!avatarDataUrl) {
    return `<div class="manager-employee-avatar" aria-hidden="true">${escapeHtml(initials)}</div>`;
  }

  const safeDataUrl = avatarDataUrl.replace(/'/g, "%27");
  return `<div class="manager-employee-avatar has-image" style="background-image:url('${safeDataUrl}')" aria-hidden="true"></div>`;
}

async function loadCeoChat() {
  const items = await api("/api/ceo/chat");
  const log = $("#ceo-chat-log");
  log.innerHTML = "";
  items.forEach((m) => {
    const row = document.createElement("div");
    const typeClass = m.sender_type === "jarvis" ? "jarvis" : "user";
    const safeMessage = (m.message || "").trim().replaceAll("\n", "<br />");
    row.className = `chat-message ${typeClass}`;
    row.innerHTML = `
      <div class="chat-header"><span>${m.sender_name}</span><span>${m.created_at}</span></div>
      <div class="chat-body">${safeMessage}</div>
    `;
    log.appendChild(row);
  });
  log.scrollTop = log.scrollHeight;
}

async function sendCeoChat() {
  const message = $("#ceo-chat-input").value.trim();
  if (!message) return;
  await api("/api/ceo/chat", { method: "POST", body: JSON.stringify({ message }) });
  $("#ceo-chat-input").value = "";
  await loadCeoChat();
}

async function loadCeoIssues() {
  const items = await api("/api/ceo/issues");
  const list = $("#ceo-issues-list");
  list.innerHTML = "";
  items.forEach((item) => {
    const row = document.createElement("div");
    row.className = "list-item";
    row.innerHTML = `
      <span>
        <strong>${item.title}</strong><br /><small>${item.branch_name} | ${item.reporter_name}</small>
      </span>
      <div class="row compact">
        <span>${item.status}</span>
        <button class="ghost" data-ceo-issue-detail-toggle="${item.id}">Xem noi dung</button>
      </div>
      <div class="issue-detail-box hidden" data-ceo-issue-detail="${item.id}">
        <p><strong>Noi dung bao cao:</strong> ${item.details || "-"}</p>
        <p><strong>Ghi chu quan ly:</strong> ${item.manager_note || "Chua co"}</p>
      </div>
    `;
    list.appendChild(row);
  });

  document.querySelectorAll("button[data-ceo-issue-detail-toggle]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = Number(btn.dataset.ceoIssueDetailToggle);
      const detail = document.querySelector(`div[data-ceo-issue-detail='${id}']`);
      if (!detail) return;
      detail.classList.toggle("hidden");
      btn.textContent = detail.classList.contains("hidden") ? "Xem noi dung" : "An noi dung";
    });
  });
}

function renderBranchPagination() {
  const pageText = $("#ceo-branches-page");
  const prevBtn = $("#btn-ceo-branches-prev");
  const nextBtn = $("#btn-ceo-branches-next");

  pageText.textContent = `Trang ${ceoBranchState.page}/${Math.max(ceoBranchState.totalPages, 1)}`;
  prevBtn.disabled = ceoBranchState.page <= 1;
  nextBtn.disabled = ceoBranchState.page >= ceoBranchState.totalPages;
}

function renderAuditPagination() {
  const pageText = $("#ceo-audit-page");
  const prevBtn = $("#btn-ceo-audit-prev");
  const nextBtn = $("#btn-ceo-audit-next");

  pageText.textContent = `Trang ${ceoBranchAuditState.page}/${Math.max(ceoBranchAuditState.totalPages, 1)}`;
  prevBtn.disabled = ceoBranchAuditState.page <= 1;
  nextBtn.disabled = ceoBranchAuditState.page >= ceoBranchAuditState.totalPages;
}

async function loadBranchAuditLogs() {
  const params = new URLSearchParams({
    page: String(ceoBranchAuditState.page),
    page_size: String(ceoBranchAuditState.pageSize),
  });
  if (ceoBranchAuditState.branchId) {
    params.set("branch_id", String(ceoBranchAuditState.branchId));
  }

  const payload = await api(`/api/admin/branch-audit-logs?${params.toString()}`);
  const list = $("#ceo-branch-audit-list");
  list.innerHTML = "";

  (payload.items || []).forEach((item) => {
    const row = document.createElement("div");
    row.className = "list-item audit-item";
    row.innerHTML = `
      <div>
        <strong>${item.action}</strong><br />
        <small>${item.details || ""}</small>
      </div>
      <div>
        <small>${item.actor_username || "system"}<br />${item.created_at}</small>
      </div>
    `;
    list.appendChild(row);
  });

  ceoBranchAuditState.totalPages = Math.max(payload.pagination?.total_pages || 1, 1);
  renderAuditPagination();
}

async function loadCeoBranches() {
  const params = new URLSearchParams({
    page: String(ceoBranchState.page),
    page_size: String(ceoBranchState.pageSize),
  });
  if (ceoBranchState.query) {
    params.set("q", ceoBranchState.query);
  }

  const payload = await api(`/api/admin/branches?${params.toString()}`);
  const branches = payload.items || [];
  const list = $("#ceo-branches-list");
  list.innerHTML = "";

  branches.forEach((branch) => {
    const row = document.createElement("div");
    row.className = "list-item ceo-branch-item";
    row.innerHTML = `
      <div class="ceo-branch-content">
        <div class="ceo-branch-view">
          <strong>${branch.name}</strong><br />
          <small>Địa điểm: ${branch.location || "-"}</small><br />
          <small>IP router: ${branch.network_ip || "-"}</small><br />
          <small>Quản lý: ${branch.manager_count} | Nhân viên: ${branch.employee_count}</small>
        </div>
        <div class="ceo-branch-edit hidden">
          <div class="field-grid ceo-branch-edit-grid">
            <label>Tên chi nhánh
              <input type="text" data-branch-name-input value="${escapeHtml(branch.name || "")}" />
            </label>
            <label>Địa điểm
              <input type="text" data-branch-location-input value="${escapeHtml(branch.location || "")}" />
            </label>
            <label>IP router
              <input type="text" data-branch-ip-input value="${escapeHtml(branch.network_ip || "")}" placeholder="VD: 203.113.10.20" />
            </label>
          </div>
          <small class="muted">Có thể cập nhật trực tiếp để sửa sai thông tin chi nhánh.</small>
        </div>
      </div>
      <div class="row compact">
        <button class="ghost" data-branch-edit="${branch.id}">Sửa</button>
        <button class="hidden" data-branch-save="${branch.id}">Lưu</button>
        <button class="ghost hidden" data-branch-cancel="${branch.id}">Hủy</button>
        <button class="danger" data-branch-del="${branch.id}">Xóa</button>
        <button class="ghost" data-branch-view="${branch.id}">Xem nhân viên</button>
        <button class="ghost" data-branch-audit="${branch.id}">Audit</button>
      </div>
    `;
    list.appendChild(row);
  });

  ceoBranchState.totalPages = Math.max(payload.pagination?.total_pages || 1, 1);
  renderBranchPagination();

  if (!list.dataset.bound) {
    list.addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLButtonElement)) {
        return;
      }

      const row = target.closest(".ceo-branch-item");
      if (!row) {
        return;
      }

      if (target.dataset.branchEdit) {
        row.classList.add("is-editing");
        row.querySelector(".ceo-branch-view")?.classList.add("hidden");
        row.querySelector(".ceo-branch-edit")?.classList.remove("hidden");
        row.querySelector("button[data-branch-edit]")?.classList.add("hidden");
        row.querySelector("button[data-branch-save]")?.classList.remove("hidden");
        row.querySelector("button[data-branch-cancel]")?.classList.remove("hidden");
        row.querySelector("input[data-branch-name-input]")?.focus();
        return;
      }

      if (target.dataset.branchCancel) {
        row.classList.remove("is-editing");
        row.querySelector(".ceo-branch-view")?.classList.remove("hidden");
        row.querySelector(".ceo-branch-edit")?.classList.add("hidden");
        row.querySelector("button[data-branch-edit]")?.classList.remove("hidden");
        row.querySelector("button[data-branch-save]")?.classList.add("hidden");
        row.querySelector("button[data-branch-cancel]")?.classList.add("hidden");
        return;
      }

      if (target.dataset.branchSave) {
        const branchId = Number(target.dataset.branchSave);
        const name = String(row.querySelector("input[data-branch-name-input]")?.value || "").trim();
        const location = String(row.querySelector("input[data-branch-location-input]")?.value || "").trim();
        const network_ip = String(row.querySelector("input[data-branch-ip-input]")?.value || "").trim();
        if (!name) {
          showToast("Tên chi nhánh không được để trống", true);
          return;
        }
        if (network_ip && !/^\d{1,3}(\.\d{1,3}){3}$/.test(network_ip)) {
          showToast("IP router không hợp lệ", true);
          return;
        }
        await api(`/api/admin/branches/${branchId}`, {
          method: "PUT",
          body: JSON.stringify({ name, location, network_ip }),
        });
        showToast("Đã cập nhật chi nhánh");
        ceoBranchAuditState.branchId = branchId;
        ceoBranchAuditState.page = 1;
        await refreshCeoBranchDependentViews();
        return;
      }

      if (target.dataset.branchDel) {
        const branchId = Number(target.dataset.branchDel);
        if (!window.confirm("Xoa chi nhanh nay?")) {
          return;
        }
        await api(`/api/admin/branches/${branchId}`, { method: "DELETE" });
        showToast("Da xoa chi nhanh");
        ceoBranchAuditState.branchId = null;
        ceoBranchAuditState.page = 1;
        await refreshCeoBranchDependentViews();
        return;
      }

      if (target.dataset.branchAudit) {
        ceoBranchAuditState.branchId = Number(target.dataset.branchAudit);
        ceoBranchAuditState.page = 1;
        await loadBranchAuditLogs();
        showToast("Dang loc audit theo chi nhanh da chon");
        return;
      }

      if (target.dataset.branchView) {
        const branchId = Number(target.dataset.branchView);
        await loadBranchEmployeesByCeo(branchId);
        showToast("Da tai nhan vien thuoc chi nhanh");
      }
    });
    list.dataset.bound = "true";
  }
}

async function refreshCeoBranchDependentViews() {
  await refreshBranchesMeta();
  loadCeoExportBranchOptions();
  loadCeoUserBranchOptions();
  await Promise.all([loadCeoBranches(), loadBranchAuditLogs()]);
}

async function loadBranchEmployeesByCeo(branchId) {
  const payload = await api(`/api/admin/branches/${branchId}/employees`);
  const list = $("#ceo-branch-employees-list");
  list.innerHTML = "";

  const managerCount = payload.managers?.length || 0;
  const employeeCount = payload.employees?.length || 0;
  const summary = document.createElement("div");
  summary.className = "list-item";
  summary.innerHTML = `
    <span><strong>${payload.branch.name}</strong><br /><small>${payload.branch.location || "Khong co dia diem"}</small></span>
    <span>${managerCount} quan ly | ${employeeCount} nhan vien</span>
  `;
  list.appendChild(summary);

  const managerTitle = document.createElement("div");
  managerTitle.className = "list-item";
  managerTitle.innerHTML = "<span><strong>Quan ly chi nhanh</strong></span>";
  list.appendChild(managerTitle);

  if (!managerCount) {
    const emptyManagers = document.createElement("div");
    emptyManagers.className = "list-item";
    emptyManagers.innerHTML = "<span>Chi nhanh nay chua co quan ly.</span>";
    list.appendChild(emptyManagers);
  } else {
    payload.managers.forEach((manager) => {
      const row = document.createElement("div");
      row.className = "list-item";
      row.innerHTML = `
        <span>
          <strong>${manager.display_name}</strong><br />
          <small>User ID: ${manager.username}</small>
        </span>
        <span>${manager.is_active ? "active" : "inactive"}</span>
      `;
      list.appendChild(row);
    });
  }

  const employeeTitle = document.createElement("div");
  employeeTitle.className = "list-item";
  employeeTitle.innerHTML = "<span><strong>Nhan vien</strong></span>";
  list.appendChild(employeeTitle);

  if (!employeeCount) {
    const empty = document.createElement("div");
    empty.className = "list-item";
    empty.innerHTML = "<span>Chi nhanh nay chua co nhan vien.</span>";
    list.appendChild(empty);
    return;
  }

  payload.employees.forEach((employee) => {
    const row = document.createElement("div");
    row.className = "list-item";
    row.innerHTML = `
      <span>
        <strong>${employee.display_name}</strong><br />
        <small>User ID: ${employee.username}</small>
      </span>
      <span>${employee.is_active ? "active" : "inactive"}</span>
    `;
    list.appendChild(row);
  });
}

async function createUserByCeo() {
  const role = $("#ceo-new-user-role").value;
  const username = $("#ceo-new-user-username").value.trim();
  const display_name = $("#ceo-new-user-display-name").value.trim();
  const password = $("#ceo-new-user-password").value;

  const body = { role, username, display_name, password };
  if (role === "manager") {
    body.branch_id = Number($("#ceo-new-user-branch-single").value);
  } else {
    const selectedBranchIds = [...$("#ceo-new-user-branch-multi").selectedOptions].map((op) =>
      Number(op.value)
    );
    body.branch_ids = selectedBranchIds;
  }

  await api("/api/admin/users", {
    method: "POST",
    body: JSON.stringify(body),
  });

  $("#ceo-new-user-username").value = "";
  $("#ceo-new-user-display-name").value = "";
  $("#ceo-new-user-password").value = "";
  $("#ceo-new-user-role").value = "employee";
  syncCeoUserRoleForm();
  loadCeoUserBranchOptions();
  showToast("CEO da tao tai khoan nhan su moi");
  await Promise.all([loadCeoUsers(), loadCeoBranches(), loadBranchAuditLogs()]);
}

async function loadCeoUsers() {
  const payload = await api("/api/admin/users");
  const list = $("#ceo-users-list");
  if (!list) return;
  list.innerHTML = "";

  const users = (payload.users || []).filter((u) => u.role === "employee" || u.role === "manager");
  if (!users.length) {
    const empty = document.createElement("div");
    empty.className = "list-item";
    empty.innerHTML = "<span>Chua co tai khoan nhan su nao.</span>";
    list.appendChild(empty);
    return;
  }

  users.forEach((user) => {
    const row = document.createElement("div");
    row.className = "list-item";
    const roleText = user.role === "manager" ? "Quan ly" : "Nhan vien";
    row.innerHTML = `
      <span>
        <strong>${user.display_name}</strong><br />
        <small>User ID: ${user.username}</small><br />
        <small>Chuc vu: ${roleText} ${user.branch_name ? `| Chi nhanh: ${user.branch_name}` : ""}</small>
      </span>
      <div class="row compact">
        <span>${user.is_active ? "active" : "inactive"}</span>
        <button class="danger" data-ceo-del-user="${user.id}" data-ceo-del-username="${user.username}">Xoa</button>
      </div>
    `;
    list.appendChild(row);
  });

  if (!list.dataset.boundDelete) {
    list.addEventListener("click", async (event) => {
      const target = event.target;
      if (!(target instanceof HTMLButtonElement)) {
        return;
      }
      const userIdRaw = target.dataset.ceoDelUser;
      if (!userIdRaw) {
        return;
      }
      const username = target.dataset.ceoDelUsername || "";
      const userId = Number(userIdRaw);
      const confirmInput = window.prompt(
        `Nhap dung User ID de xac nhan xoa: ${username}`
      );
      if (confirmInput === null) {
        return;
      }
      if (confirmInput.trim() !== username) {
        showToast("User ID khong khop, da huy thao tac xoa", true);
        return;
      }

      await api(`/api/admin/users/${userId}`, { method: "DELETE" });
      showToast("Da xoa tai khoan nhan su");
      await Promise.all([loadCeoUsers(), loadCeoBranches(), loadBranchAuditLogs()]);
    });
    list.dataset.boundDelete = "true";
  }
}

async function createBranchByCeo() {
  const name = $("#ceo-branch-new-name").value.trim();
  const location = $("#ceo-branch-new-location").value.trim();
  const network_ip = $("#ceo-branch-new-network-ip").value.trim();
  await api("/api/admin/branches", {
    method: "POST",
    body: JSON.stringify({ name, location, network_ip }),
  });
  $("#ceo-branch-new-name").value = "";
  $("#ceo-branch-new-location").value = "";
  $("#ceo-branch-new-network-ip").value = "";
  showToast("Da them chi nhanh moi");
  ceoBranchState.page = 1;
  ceoBranchAuditState.page = 1;
  ceoBranchAuditState.branchId = null;
  await refreshCeoBranchDependentViews();
}

async function login() {
  const username = $("#login-username").value;
  const password = $("#login-password").value;
  const payload = await api("/api/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
  state.token = payload.token;
  state.currentUser = payload.user;
  if (state.currentUser.role === "manager") {
    const cur = await api("/api/current-user");
    state.currentUser = cur.user;
  }
  persistSession();
  setShellByAuth();
  if (shouldRequireProfileCompletion()) {
    await loadProfileRequiredForm();
  }
  window.location.hash = `#/${defaultRouteForRole(state.currentUser.role)}`;
  if (state.currentUser.role === "employee") await loadEmployeeBranches();
  await renderRoute();
  showToast("Dang nhap thanh cong");
}

async function changePasswordFromLogin() {
  const username = $("#change-password-username").value.trim();
  const currentPassword = $("#change-password-current").value;
  const newPassword = $("#change-password-new").value;
  const confirmPassword = $("#change-password-confirm").value;

  if (!username) {
    throw new Error("Vui lòng nhập tài khoản");
  }
  if (!currentPassword) {
    throw new Error("Vui lòng nhập mật khẩu hiện tại");
  }
  if (!newPassword) {
    throw new Error("Vui lòng nhập mật khẩu mới");
  }
  if (newPassword.length < 8) {
    throw new Error("Mật khẩu mới phải có ít nhất 8 ký tự");
  }
  if (newPassword !== confirmPassword) {
    throw new Error("Xác nhận mật khẩu mới không khớp");
  }

  await api("/api/change-password-login", {
    method: "POST",
    body: JSON.stringify({
      username,
      current_password: currentPassword,
      new_password: newPassword,
    }),
  });

  $("#change-password-current").value = "";
  $("#change-password-new").value = "";
  $("#change-password-confirm").value = "";
  $("#login-username").value = username;
  $("#login-password").value = "";
  switchAuthScreen("login");
  showToast("Đổi mật khẩu thành công, vui lòng đăng nhập lại");
}

async function logout() {
  try {
    if (state.token) await api("/api/logout", { method: "POST" });
  } catch {
    // ignore
  }
  state.token = null;
  state.currentUser = null;
  persistSession();
  setShellByAuth();
  window.location.hash = "";
}

async function tryRestoreSession() {
  const raw = localStorage.getItem("wm_current_user");
  if (!state.token || !raw) return;
  try {
    state.currentUser = JSON.parse(raw);
    const cur = await api("/api/current-user");
    state.currentUser = cur.user;
    if (shouldRequireProfileCompletion()) {
      await loadProfileRequiredForm();
    }
    if (state.currentUser.role === "employee") await loadEmployeeBranches();
  } catch {
    state.token = null;
    state.currentUser = null;
    persistSession();
  }
}

function attachEvents() {
  bindManagerEmployeeListEvents();

  $("#btn-login").addEventListener("click", () => login().catch((e) => showToast(e.message, true)));
  $("#login-username").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      login().catch((e) => showToast(e.message, true));
    }
  });
  $("#login-password").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      login().catch((e) => showToast(e.message, true));
    }
  });
  $("#btn-toggle-password").addEventListener("click", () => {
    const input = $("#login-password");
    const isHidden = input.type === "password";
    input.type = isHidden ? "text" : "password";
    $("#btn-toggle-password").textContent = isHidden ? "An" : "Hien";
    $("#btn-toggle-password").setAttribute(
      "aria-label",
      isHidden ? "An mat khau" : "Hien mat khau"
    );
    input.focus();
  });
  $("#btn-open-change-password").addEventListener("click", () => {
    $("#change-password-username").value = $("#login-username").value.trim();
    switchAuthScreen("change-password");
    $("#change-password-current").focus();
  });
  $("#btn-back-login").addEventListener("click", () => {
    switchAuthScreen("login");
    $("#login-password").focus();
  });
  $("#btn-change-password").addEventListener("click", () =>
    changePasswordFromLogin().catch((e) => showToast(e.message, true))
  );
  $("#change-password-confirm").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      changePasswordFromLogin().catch((e) => showToast(e.message, true));
    }
  });
  $("#btn-logout").addEventListener("click", () => logout().catch((e) => showToast(e.message, true)));
  $("#btn-save-profile-required").addEventListener("click", () =>
    submitRequiredProfile().catch((e) => showToast(e.message, true))
  );
  $("#profile-avatar-input").addEventListener("change", (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      state.profileAvatarDataUrl = String(reader.result || "");
      setProfileAvatarPreview(state.profileAvatarDataUrl);
    };
    reader.readAsDataURL(file);
  });
  $("#btn-week-refresh").addEventListener("click", () => renderRoute().catch((e) => showToast(e.message, true)));

  $("#btn-check-in-one-time").addEventListener("click", () =>
    checkInEmployeeOneTime().catch((e) => showToast(e.message, true))
  );
  $("#btn-scan-one-time-qr").addEventListener("click", () =>
    scanEmployeeOneTimeQr().catch((e) => showToast(e.message, true))
  );
  $("#btn-check-out").addEventListener("click", () => checkOutEmployee().catch((e) => showToast(e.message, true)));
  $("#btn-save-employee-shifts").addEventListener("click", () => saveEmployeeShifts().catch((e) => showToast(e.message, true)));
  $("#btn-submit-employee-issue").addEventListener("click", () => submitIssue().catch((e) => showToast(e.message, true)));
  $("#employee-assigned-branch-filter").addEventListener("change", () =>
    loadEmployeeAssignedSchedule().catch((e) => showToast(e.message, true))
  );
  $("#employee-issue-branch-filter").addEventListener("change", () =>
    loadMyIssues().catch((e) => showToast(e.message, true))
  );

  $("#btn-manager-check-in").addEventListener("click", () => checkInManager().catch((e) => showToast(e.message, true)));
  $("#btn-manager-check-out").addEventListener("click", () => checkOutManager().catch((e) => showToast(e.message, true)));
  $("#btn-manager-generate-one-time-qr").addEventListener("click", () =>
    generateManagerOneTimeQr().catch((e) => showToast(e.message, true))
  );
  $("#btn-save-manager-self-shifts").addEventListener("click", () => saveManagerSelfShifts().catch((e) => showToast(e.message, true)));
  $("#btn-save-manager-schedule").addEventListener("click", () => saveManagerSchedule().catch((e) => showToast(e.message, true)));
  $("#btn-save-manager-staffing-rules").addEventListener("click", () =>
    saveManagerStaffingRules().catch((e) => showToast(e.message, true))
  );
  $("#btn-create-employee").addEventListener("click", () => createEmployee().catch((e) => showToast(e.message, true)));
  $("#btn-manager-employee-search").addEventListener("click", () => {
    setManagerEmployeeKeyword($("#manager-employee-search").value);
    loadManagerEmployees().catch((e) => showToast(e.message, true));
  });
  $("#btn-manager-employee-reset").addEventListener("click", () => {
    setManagerEmployeeKeyword("");
    $("#manager-employee-search").value = "";
    loadManagerEmployees().catch((e) => showToast(e.message, true));
  });
  $("#manager-employee-search").addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    setManagerEmployeeKeyword($("#manager-employee-search").value);
    loadManagerEmployees().catch((e) => showToast(e.message, true));
  });
  $("#btn-manager-export-csv").addEventListener("click", () =>
    fetchCsv(`/api/manager/payroll-export.csv?week_start=${encodeURIComponent(currentWeek())}`).catch((e) =>
      showToast(e.message, true)
    )
  );
  $("#btn-manager-preview-csv").addEventListener("click", () =>
    previewCsv(
      `/api/manager/payroll-export.csv?week_start=${encodeURIComponent(currentWeek())}`,
      "#manager-export-preview"
    ).catch((e) => showToast(e.message, true))
  );

  $("#btn-ceo-chat-send").addEventListener("click", () => sendCeoChat().catch((e) => showToast(e.message, true)));
  $("#ceo-chat-input").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      sendCeoChat().catch((e) => showToast(e.message, true));
    }
  });
  $("#btn-ceo-export-csv").addEventListener("click", () =>
    fetchCsv(ceoPayrollPath()).catch((e) => showToast(e.message, true))
  );
  $("#btn-ceo-preview-csv").addEventListener("click", () =>
    previewCsv(ceoPayrollPath(), "#ceo-export-preview").catch((e) => showToast(e.message, true))
  );
  $("#ceo-export-branch-filter").addEventListener("change", (event) => {
    ceoExportState.branchId = event.target.value;
  });
  $("#btn-ceo-create-branch").addEventListener("click", () =>
    createBranchByCeo().catch((e) => showToast(e.message, true))
  );
  $("#btn-ceo-create-user").addEventListener("click", () =>
    createUserByCeo().catch((e) => showToast(e.message, true))
  );
  $("#ceo-new-user-role").addEventListener("change", () => syncCeoUserRoleForm());
  $("#btn-ceo-branch-search").addEventListener("click", () => {
    ceoBranchState.query = $("#ceo-branch-search").value.trim();
    ceoBranchState.page = 1;
    loadCeoBranches().catch((e) => showToast(e.message, true));
  });
  $("#btn-ceo-branches-prev").addEventListener("click", () => {
    if (ceoBranchState.page <= 1) return;
    ceoBranchState.page -= 1;
    loadCeoBranches().catch((e) => showToast(e.message, true));
  });
  $("#btn-ceo-branches-next").addEventListener("click", () => {
    if (ceoBranchState.page >= ceoBranchState.totalPages) return;
    ceoBranchState.page += 1;
    loadCeoBranches().catch((e) => showToast(e.message, true));
  });
  $("#btn-ceo-audit-prev").addEventListener("click", () => {
    if (ceoBranchAuditState.page <= 1) return;
    ceoBranchAuditState.page -= 1;
    loadBranchAuditLogs().catch((e) => showToast(e.message, true));
  });
  $("#btn-ceo-audit-next").addEventListener("click", () => {
    if (ceoBranchAuditState.page >= ceoBranchAuditState.totalPages) return;
    ceoBranchAuditState.page += 1;
    loadBranchAuditLogs().catch((e) => showToast(e.message, true));
  });

  document.querySelectorAll("button[data-chat-prompt]").forEach((button) => {
    button.addEventListener("click", () => {
      $("#ceo-chat-input").value = button.dataset.chatPrompt || "";
      $("#ceo-chat-input").focus();
    });
  });

  document.querySelectorAll("button[data-route-link]").forEach((button) => {
    button.addEventListener("click", () => {
      const route = button.dataset.routeLink;
      if (!route) return;
      window.location.hash = `#/${route}`;
    });
  });

  $("#btn-sidebar-toggle").addEventListener("click", toggleSidebar);
  $("#sidebar-overlay").addEventListener("click", () => setSidebarCollapsed(true));

  window.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setSidebarCollapsed(true);
    }
  });

  window.addEventListener("hashchange", () => renderRoute().catch((e) => showToast(e.message, true)));
}

async function bootstrap() {
  const meta = await api("/api/meta");
  state.shifts = meta.shifts;
  state.branches = meta.branches;

  $("#week-start").value = mondayOfCurrentWeekISO();

  await tryRestoreSession();
  setShellByAuth();
  if (state.currentUser && shouldRequireProfileCompletion()) {
    await loadProfileRequiredForm();
  }
  if (state.currentUser) {
    ensureValidRoute();
    await renderRoute();
  }
}

function registerServiceWorker() {
  if (!("serviceWorker" in navigator)) {
    return;
  }
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("/sw.js").catch(() => {
      // Silent fail to avoid blocking app usage when SW is unavailable.
    });
  });
}

attachEvents();
registerServiceWorker();
bootstrap().catch((e) => showToast(e.message, true));
