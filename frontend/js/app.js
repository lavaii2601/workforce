const state = {
  token: localStorage.getItem("wm_token"),
  currentUser: null,
  shifts: [],
  shiftByCode: new Map(),
  branches: [],
  employeeBranches: [],
  employeeFlexTimeByKey: {},
  employeeFlexEditorDismissed: (() => {
    const saved = localStorage.getItem("wm_employee_flex_editor_dismissed");
    return saved === "1";
  })(),
  managerSelfFlexTimeByKey: {},
  managerSelfFlexEditorDismissed: (() => {
    const saved = localStorage.getItem("wm_manager_flex_editor_dismissed");
    return saved === "1";
  })(),
  sidebarCollapsed: (() => {
    const saved = localStorage.getItem("wm_sidebar_collapsed");
    if (saved === null) {
      return window.innerWidth < 1200;
    }
    return saved !== "0";
  })(),
  profileAvatarDataUrl: "",
  oneTimeScan: null,
  employeeCanCheckInToday: false,
  managerDailyQr: null,
  managerEmployeeAvatarObjectUrls: {},
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
  selectedDay: 0,
  assignedSelectedDay: 0,
  scheduleRevision: "",
};

const managerEmployeeUiState = {
  keyword: "",
};

const employeePreferenceUiState = {
  locked: false,
};

const authUiState = {
  screen: "login",
};

let sidebarResizeRafId = null;
let employeeOneTimeCountdownTimerId = null;
let employeeQrCameraStream = null;
let employeeQrCameraLoopId = null;
let employeeQrDetectInProgress = false;

const WEEK_DAYS = [1, 2, 3, 4, 5, 6, 7];
const SMALL_SHIFT_CODES = ["S1", "S2", "S3", "S4"];
const managerStaffingRules = new Map();
const PROFILE_AVATAR_MAX_FILE_BYTES = 2 * 1024 * 1024;
const PROFILE_AVATAR_MAX_DATA_URL_LENGTH = 350000;
const PROFILE_AVATAR_MAX_DIMENSION = 640;
const API_MESSAGE_REPLACEMENTS = [
  ["Request failed", "Yêu cầu thất bại"],
  ["Missing access token", "Thiếu mã truy cập"],
  ["Invalid or expired session", "Phiên đăng nhập không hợp lệ hoặc đã hết hạn"],
  ["Please check-out current session before new check-in", "Vui lòng check-out ca hiện tại trước khi check-in mới"],
  ["No open attendance session to check-out", "Không có phiên chấm công đang mở để check-out"],
  ["No open attendance session to confirm", "Không có phiên chấm công đang mở để xác nhận"],
  ["Invalid, consumed, or expired one-time QR code", "Mã QR một lần không hợp lệ, đã dùng hoặc đã hết hạn"],
  ["Invalid branch for attendance", "Chi nhánh chấm công không hợp lệ"],
  ["No branch assigned for this employee", "Nhân viên chưa được gán chi nhánh"],
  ["Branch is not in employee access scope", "Chi nhánh không nằm trong phạm vi được phép"],
  ["User not found", "Không tìm thấy người dùng"],
  ["username already exists", "Tên đăng nhập đã tồn tại"],
];

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

function getShiftByCode(code) {
  const normalized = String(code || "").toUpperCase();
  return state.shiftByCode.get(normalized) || null;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function toSafeChatHtml(value) {
  return escapeHtml(value || "").replaceAll("\n", "<br />");
}

function issueStatusLabel(value) {
  const status = String(value || "").trim().toLowerCase();
  if (status === "open") return "Mới";
  if (status === "in_review") return "Đang xem xét";
  if (status === "resolved") return "Đã xử lý";
  if (status === "escalated") return "Đã chuyển cấp";
  return value || "-";
}

function localizeApiMessage(message) {
  let text = String(message || "").trim();
  API_MESSAGE_REPLACEMENTS.forEach(([from, to]) => {
    text = text.replaceAll(from, to);
  });
  return text;
}

function isSessionErrorMessage(message) {
  const text = String(message || "").toLowerCase();
  return (
    text.includes("invalid or expired session") ||
    text.includes("missing access token") ||
    text.includes("phiên đăng nhập không hợp lệ") ||
    text.includes("thiếu mã truy cập")
  );
}

function forceSessionReset() {
  state.token = null;
  state.currentUser = null;
  state.managerDailyQr = null;
  state.oneTimeScan = null;
  persistSession();
  setShellByAuth();
  window.location.hash = "";
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

function parseDbDateTimeToEpoch(raw) {
  const text = String(raw || "").trim();
  const matched = text.match(/^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?$/);
  if (!matched) return Number.NaN;
  return new Date(text.replace(" ", "T")).getTime();
}

function stopEmployeeOneTimeCountdown() {
  if (employeeOneTimeCountdownTimerId) {
    clearInterval(employeeOneTimeCountdownTimerId);
    employeeOneTimeCountdownTimerId = null;
  }
}

function renderEmployeeOneTimeCountdown() {
  const note = $("#employee-one-time-scan-result");
  if (!note || !state.oneTimeScan) return;
  const expiresAtEpoch = Number(state.oneTimeScan.expiresAtEpoch || 0);
  if (!Number.isFinite(expiresAtEpoch) || expiresAtEpoch <= 0) return;

  const now = Date.now();
  const remainSeconds = Math.max(0, Math.floor((expiresAtEpoch - now) / 1000));
  if (remainSeconds <= 0) {
    note.textContent = "Random key đã hết hạn. Vui lòng quét lại QR để nhận key mới.";
    stopEmployeeOneTimeCountdown();
    return;
  }

  note.textContent = `Random key nhận được: ${state.oneTimeScan.randomKey || "-"}. Con hieu luc ${remainSeconds}s.`;
}

function startEmployeeOneTimeCountdown() {
  stopEmployeeOneTimeCountdown();
  renderEmployeeOneTimeCountdown();
  employeeOneTimeCountdownTimerId = setInterval(renderEmployeeOneTimeCountdown, 1000);
}

function stopEmployeeQrCameraScanner() {
  if (employeeQrCameraLoopId) {
    clearInterval(employeeQrCameraLoopId);
    employeeQrCameraLoopId = null;
  }
  if (employeeQrCameraStream) {
    employeeQrCameraStream.getTracks().forEach((track) => track.stop());
    employeeQrCameraStream = null;
  }
  employeeQrDetectInProgress = false;
  const video = $("#employee-qr-camera-video");
  if (video) {
    video.srcObject = null;
  }
  const modal = $("#employee-qr-camera-modal");
  if (modal) {
    modal.classList.add("hidden");
  }
}

async function openEmployeeQrCameraScanner() {
  const modal = $("#employee-qr-camera-modal");
  const video = $("#employee-qr-camera-video");
  const status = $("#employee-qr-camera-status");
  if (!modal || !video || !status) return;

  if (!("mediaDevices" in navigator) || !navigator.mediaDevices.getUserMedia) {
    throw new Error("Trình duyệt không hỗ trợ camera. Vui lòng dán mã QR thủ công.");
  }
  if (!("BarcodeDetector" in window)) {
    throw new Error("Thiết bị chưa hỗ trợ quét QR tự động. Vui lòng dán mã QR thủ công.");
  }

  stopEmployeeQrCameraScanner();
  modal.classList.remove("hidden");
  status.textContent = "Đang mở camera...";

  try {
    employeeQrCameraStream = await navigator.mediaDevices.getUserMedia({
      video: { facingMode: { ideal: "environment" } },
      audio: false,
    });
    video.srcObject = employeeQrCameraStream;
    await video.play();

    const detector = new window.BarcodeDetector({ formats: ["qr_code"] });
    status.textContent = "Đưa mã QR vào khung camera để hệ thống tự nhận.";

    employeeQrCameraLoopId = setInterval(async () => {
      if (employeeQrDetectInProgress || !video.videoWidth || !video.videoHeight) {
        return;
      }
      employeeQrDetectInProgress = true;
      try {
        const codes = await detector.detect(video);
        const qrText = (codes && codes[0] && codes[0].rawValue) ? String(codes[0].rawValue).trim() : "";
        if (!qrText) {
          return;
        }
        status.textContent = "Đã nhận QR, đang lấy random key...";
        $("#employee-one-time-qr-payload").value = qrText;
        stopEmployeeQrCameraScanner();
        await scanEmployeeOneTimeQr();
      } catch {
        // Ignore transient detector errors and keep scanning.
      } finally {
        employeeQrDetectInProgress = false;
      }
    }, 250);
  } catch (error) {
    stopEmployeeQrCameraScanner();
    throw new Error(error?.message || "Không thể mở camera để quét QR");
  }
}

async function readQrPayloadFromImageFile(file) {
  if (!file) {
    throw new Error("Vui lòng chọn ảnh QR");
  }
  if (!("BarcodeDetector" in window)) {
    throw new Error("Trình duyệt chưa hỗ trợ quét QR từ ảnh. Vui lòng dùng camera trực tiếp hoặc dán mã thủ công.");
  }

  const bitmap = await createImageBitmap(file);
  try {
    const detector = new window.BarcodeDetector({ formats: ["qr_code"] });
    const codes = await detector.detect(bitmap);
    const qrText = (codes && codes[0] && codes[0].rawValue) ? String(codes[0].rawValue).trim() : "";
    if (!qrText) {
      throw new Error("Không nhận diện được QR trong ảnh. Vui lòng chọn ảnh rõ hơn.");
    }
    return qrText;
  } finally {
    bitmap.close();
  }
}

async function scanEmployeeOneTimeQrFromImage(event) {
  const file = event?.target?.files?.[0];
  if (!file) return;
  const fileInput = $("#employee-qr-image-input");
  try {
    const qrPayload = await readQrPayloadFromImageFile(file);
    $("#employee-one-time-qr-payload").value = qrPayload;
    await scanEmployeeOneTimeQr();
  } finally {
    if (fileInput) {
      fileInput.value = "";
    }
  }
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
    ...(options.headers || {}),
  };
  const method = String(options.method || "GET").toUpperCase();
  const hasBody = options.body !== undefined && options.body !== null;
  if (hasBody && !headers["Content-Type"] && !headers["content-type"]) {
    headers["Content-Type"] = "application/json";
  }
  if (state.token) {
    headers.Authorization = `Bearer ${state.token}`;
  }

  const res = await fetch(path, { ...options, method, headers });
  const payload = await res.json().catch(() => ({}));
  if (!res.ok) {
    const rawError = payload.error || `Yêu cầu thất bại: ${res.status}`;
    if (res.status === 401 && state.token && isSessionErrorMessage(rawError)) {
      forceSessionReset();
      throw new Error("Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.");
    }
    throw new Error(localizeApiMessage(rawError));
  }
  return payload;
}

async function withButtonLocks(buttonSelectors, action, { loadingText = "Đang lưu..." } = {}) {
  const selectors = Array.isArray(buttonSelectors) ? buttonSelectors : [buttonSelectors];
  const buttons = selectors
    .map((selector) => (selector instanceof HTMLElement ? selector : $(selector)))
    .filter((button) => button instanceof HTMLButtonElement);

  if (buttons.some((button) => button.dataset.loading === "1")) {
    return;
  }

  const snapshots = buttons.map((button) => ({
    button,
    text: button.textContent,
  }));

  snapshots.forEach(({ button }) => {
    button.dataset.loading = "1";
    button.disabled = true;
    button.textContent = loadingText;
  });

  try {
    await action();
  } finally {
    snapshots.forEach(({ button, text }) => {
      button.dataset.loading = "0";
      button.disabled = false;
      button.textContent = text;
    });
  }
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

function parseCsvSections(text) {
  const lines = text.split(/\r?\n/);
  const sections = [];
  let idx = 0;

  while (idx < lines.length) {
    while (idx < lines.length && !lines[idx].trim()) idx += 1;
    if (idx >= lines.length) break;

    const firstCells = parseCsvLine(lines[idx]);
    const secondLine = idx + 1 < lines.length ? lines[idx + 1] : "";
    const secondCells = parseCsvLine(secondLine);

    let title = "";
    let headers = [];

    // New format: title row + header row.
    if (firstCells.length === 1 && secondLine.trim() && secondCells.length > 1) {
      title = (firstCells[0] || "").trim();
      headers = secondCells;
      idx += 2;
    } else {
      // Backward-compatible format: plain single table.
      headers = firstCells;
      idx += 1;
    }

    const rows = [];
    while (idx < lines.length && lines[idx].trim()) {
      rows.push(parseCsvLine(lines[idx]));
      idx += 1;
    }

    if (headers.length) {
      sections.push({ title, headers, rows });
    }
  }

  return sections;
}

async function previewCsv(path, containerSelector) {
  const root = $(containerSelector);
  root.classList.remove("hidden");
  root.innerHTML = "<p class='muted'>Đang tải dữ liệu xem trước...</p>";

  const text = await fetchCsvText(path);
  const sections = parseCsvSections(text);
  if (!sections.length) {
    root.innerHTML = "<p class='muted'>Không có dữ liệu để xem trước.</p>";
    return;
  }

  root.innerHTML = "";
  const maxRowsPerSection = 12;
  const totalRows = sections.reduce((sum, section) => sum + section.rows.length, 0);
  const totalShown = sections.reduce(
    (sum, section) => sum + Math.min(section.rows.length, maxRowsPerSection),
    0
  );

  const note = document.createElement("p");
  note.className = "muted csv-preview-note";
  note.textContent =
    totalRows > totalShown
      ? `Đang hiển thị ${totalShown}/${totalRows} dòng. Bấm Tải CSV để lấy đầy đủ.`
      : `Đang hiển thị ${totalRows} dòng.`;
  root.appendChild(note);

  sections.forEach((section) => {
    const sectionBox = document.createElement("section");
    sectionBox.className = "csv-preview-section";

    if (section.title) {
      const title = document.createElement("h4");
      title.className = "csv-preview-title";
      title.textContent = section.title;
      sectionBox.appendChild(title);
    }

    const previewRows = section.rows.slice(0, maxRowsPerSection);
    const table = document.createElement("table");
    table.className = "csv-preview-table";
    table.style.minWidth = `${Math.max(760, section.headers.length * 130)}px`;

    const headHtml = section.headers.map((h) => `<th>${escapeHtml(h)}</th>`).join("");
    const bodyHtml = previewRows
      .map(
        (row) =>
          `<tr>${section.headers
            .map((_, i) => `<td>${escapeHtml(String(row[i] || ""))}</td>`)
            .join("")}</tr>`
      )
      .join("");

    table.innerHTML = `<thead><tr>${headHtml}</tr></thead><tbody>${bodyHtml}</tbody>`;

    const sectionMeta = document.createElement("p");
    sectionMeta.className = "muted csv-preview-meta";
    sectionMeta.textContent =
      section.rows.length > maxRowsPerSection
        ? `Hiển thị ${maxRowsPerSection}/${section.rows.length} dòng trong phần này.`
        : `Hiển thị ${section.rows.length} dòng trong phần này.`;

    const scrollWrap = document.createElement("div");
    scrollWrap.className = "csv-preview-scroll";
    scrollWrap.appendChild(table);

    sectionBox.appendChild(sectionMeta);
    sectionBox.appendChild(scrollWrap);
    root.appendChild(sectionBox);
  });
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
  $("#btn-open-profile")?.classList.toggle("hidden", !loggedIn);

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
  applyProfileModalMode();
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

function applyProfileModalMode() {
  const isRequired = shouldRequireProfileCompletion();
  const titleNode = $("#profile-modal-title");
  const descNode = $("#profile-modal-desc");
  const closeBtn = $("#btn-close-profile-modal");

  if (titleNode) {
    titleNode.textContent = isRequired ? "Hoàn thiện hồ sơ cá nhân" : "Cập nhật hồ sơ cá nhân";
  }
  if (descNode) {
    descNode.textContent = isRequired
      ? "Bạn cần cập nhật thông tin cá nhân trước khi sử dụng hệ thống."
      : "Bạn có thể cập nhật avatar và thông tin cá nhân bất kỳ lúc nào.";
  }
  if (closeBtn) {
    closeBtn.classList.toggle("hidden", isRequired);
  }
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

function resizeImageDataUrlToJpeg(dataUrl, maxDimension, quality = 0.78) {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => {
      const ratio = Math.min(1, maxDimension / Math.max(image.width, image.height));
      const width = Math.max(1, Math.round(image.width * ratio));
      const height = Math.max(1, Math.round(image.height * ratio));
      const canvas = document.createElement("canvas");
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext("2d");
      if (!ctx) {
        reject(new Error("Canvas not supported"));
        return;
      }
      ctx.drawImage(image, 0, 0, width, height);
      resolve(canvas.toDataURL("image/jpeg", quality));
    };
    image.onerror = () => reject(new Error("Cannot read image"));
    image.src = dataUrl;
  });
}

async function prepareProfileAvatarDataUrl(file) {
  if (!file.type.startsWith("image/")) {
    throw new Error("Chi ho tro tep anh");
  }
  if (file.size > PROFILE_AVATAR_MAX_FILE_BYTES) {
    throw new Error("Anh qua lon, vui long chon anh nho hon 2MB");
  }

  const originalDataUrl = await new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("Khong the doc tep anh"));
    reader.readAsDataURL(file);
  });

  if (originalDataUrl.length <= PROFILE_AVATAR_MAX_DATA_URL_LENGTH) {
    return originalDataUrl;
  }

  const compressed = await resizeImageDataUrlToJpeg(
    originalDataUrl,
    PROFILE_AVATAR_MAX_DIMENSION,
    0.75
  );
  if (compressed.length > PROFILE_AVATAR_MAX_DATA_URL_LENGTH) {
    throw new Error("Anh van qua lon sau khi nen, vui long chon anh nho hon");
  }
  return compressed;
}

function toggleProfileRequiredModal(show) {
  const modal = $("#profile-required-modal");
  if (!modal) return;
  modal.classList.toggle("hidden", !show);
}

async function openProfileModal() {
  if (!state.currentUser) return;
  await loadProfileRequiredForm();
  applyProfileModalMode();
  toggleProfileRequiredModal(true);
}

function closeProfileModal() {
  if (shouldRequireProfileCompletion()) return;
  toggleProfileRequiredModal(false);
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
  applyProfileModalMode();
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
  $("#page-title").textContent = routeInfo ? routeInfo.title : "Tổng quan";

  try {
    if (key === "employee-attendance") {
      await Promise.all([loadMyAttendance("employee"), loadCheckInAvailability("employee")]);
    }
    if (key === "employee-shifts") {
      await Promise.all([loadEmployeeShifts(), loadEmployeeRegistrationGroups()]);
    }
    if (key === "employee-assigned") await loadEmployeeAssignedSchedule();
    if (key === "employee-issues") await loadMyIssues();

    if (key === "manager-attendance") {
      await Promise.all([loadMyAttendance("manager"), loadManagerShiftAttendanceToday()]);
      await loadCheckInAvailability("manager");
      $("#manager-attendance-one-time-meta").textContent =
        "Nhấn Tạo QR cho ngày hôm nay. Mỗi lần nhân viên quét sẽ nhận mã xác thực một lần riêng.";

      if (isManagerDailyQrValid(state.managerDailyQr)) {
        applyManagerDailyQr(state.managerDailyQr, { showToastSuccess: false });
        return;
      }

      try {
        await generateManagerOneTimeQr({ showToastSuccess: false });
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
    if (key === "manager-schedule") {
      await Promise.all([loadManagerSchedule(), loadManagerRegistrationGroups()]);
    }
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
  } catch (error) {
    const message = error instanceof Error ? error.message : "Khong tai duoc du lieu";
    if (isSessionErrorMessage(message)) {
      forceSessionReset();
      showToast("Phien dang nhap da het han, vui long dang nhap lai", true);
      return;
    }
    showToast(localizeApiMessage(message), true);
  }
}

function isManagerDailyQrValid(payload) {
  if (!payload || !payload.qr_image_data_url || !payload.expires_at) return false;
  const expiresMs = parseDbDateTimeToEpoch(payload.expires_at);
  if (Number.isNaN(expiresMs)) return false;
  return expiresMs > Date.now() + 1000;
}

function applyManagerDailyQr(payload, { showToastSuccess = true } = {}) {
  const imageNode = $("#manager-attendance-one-time-qr-image");
  if (imageNode) {
    imageNode.src = payload.qr_image_data_url || "";
    imageNode.classList.toggle("hidden", !payload.qr_image_data_url);
  }
  $("#manager-attendance-one-time-meta").textContent =
    `Đã tạo QR theo ngày | Hết hạn lúc: ${formatDateTimeDisplay(payload.expires_at)}`;
  state.managerDailyQr = payload;
  if (showToastSuccess) {
    showToast("Da tao QR theo ngay");
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

  const desktopDocked = window.innerWidth >= 1200;

  appView.classList.toggle("sidebar-collapsed", state.sidebarCollapsed);
  appView.classList.toggle("desktop-docked", desktopDocked && !state.sidebarCollapsed);
  overlay.classList.toggle("hidden", state.sidebarCollapsed || desktopDocked);
  document.body.classList.toggle("no-scroll", !state.sidebarCollapsed && !desktopDocked);
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

function scheduleApplySidebarState() {
  if (sidebarResizeRafId != null) return;
  sidebarResizeRafId = window.requestAnimationFrame(() => {
    sidebarResizeRafId = null;
    applySidebarState();
  });
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

async function refreshBranchesMeta({ force = false } = {}) {
  if (!force && state.branches.length) {
    return;
  }
  const meta = await api("/api/meta");
  state.branches = meta.branches || [];
}

async function loadEmployeeBranches() {
  state.employeeBranches = await api("/api/employee/branches");
  fillBranchSelect($("#employee-attendance-branch"), state.employeeBranches);
  fillBranchSelect($("#employee-issue-branch"), state.employeeBranches);
  fillBranchSelect($("#employee-group-branch"), state.employeeBranches);
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
    const confirmedText = item.confirmed_at
      ? ` | Mốc tính công: ${formatDateTimeDisplay(item.confirmed_at)}`
      : "";
    const sourceText = attendanceSourceText(item);
    row.innerHTML = `<span>${item.branch_name} | ${formatDateTimeDisplay(item.check_in_at)} -> ${item.check_out_at ? formatDateTimeDisplay(item.check_out_at) : "Đang làm"}${confirmedText}<br /><small>${sourceText}</small></span><strong>${(item.minutes_worked / 60).toFixed(2)}h</strong>`;
    fragment.appendChild(row);
  });
  const sum = document.createElement("div");
  sum.className = "list-item";
  sum.innerHTML = `<span>Tổng giờ trong tuần</span><strong>${(data.total_minutes / 60).toFixed(2)}h</strong>`;
  fragment.appendChild(sum);
  list.appendChild(fragment);
}

async function loadCheckInAvailability(viewMode) {
  const payload = await api("/api/attendance/checkin-availability");
  const canCheckIn = !!payload.can_check_in;
  const reason = String(payload.reason || "").trim();

  if (viewMode === "employee") {
    state.employeeCanCheckInToday = canCheckIn;
    updateEmployeeOneTimeCheckInButtonState(reason);
    const note = $("#employee-one-time-scan-result");
    if (note && !canCheckIn) {
      note.textContent = reason || "Bạn chưa có ca làm hôm nay";
    }
  }

  if (viewMode === "manager") {
    const button = $("#btn-manager-check-in");
    if (button) {
      button.disabled = !canCheckIn;
      button.title = canCheckIn ? "" : (reason || "Bạn chưa có ca làm hôm nay");
    }
  }

  return payload;
}

function isEmployeeOneTimeKeyReady() {
  if (!state.oneTimeScan) return false;
  const typedKey = $("#employee-one-time-random-key")?.value?.trim()?.toUpperCase() || "";
  return !!typedKey && typedKey === String(state.oneTimeScan.randomKey || "").toUpperCase();
}

function updateEmployeeOneTimeCheckInButtonState(reason = "") {
  const button = $("#btn-check-in-one-time");
  if (!button) return;

  const keyReady = isEmployeeOneTimeKeyReady();
  button.disabled = !keyReady;

  if (!state.oneTimeScan) {
    button.title = "Vui lòng quét QR để lấy random key";
    return;
  }
  if (!keyReady) {
    button.title = "Vui lòng nhập đúng random key để xác nhận";
    return;
  }
  if (!state.employeeCanCheckInToday) {
    button.title = reason || "Nhân viên không có ca làm, không thể check-in";
    return;
  }
  button.title = "";
}

async function generateManagerOneTimeQr({ showToastSuccess = true } = {}) {
  const payload = await api("/api/manager/attendance-qr-one-time", {
    method: "POST",
  });
  applyManagerDailyQr(payload, { showToastSuccess });
}

function shiftAttendanceStatusLabel(status) {
  if (status === "present") return "Đã chấm công";
  if (status === "present_override") return "Đã đi làm (quản lý xác nhận thiếu nhân sự)";
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
      const row = btn.closest(".shift-attendance-item");
      const noteInput = row?.querySelector(`input[data-override-note='${scheduleId}']`);
      const note = noteInput?.value?.trim() || "";
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
    const safeShiftCode = escapeHtml(shiftText(item.shift_code));
    const safeStatus = escapeHtml(shiftAttendanceStatusLabel(item.status));
    const safeNote = escapeHtml(item.note || "");
    const lateMinutes = Number(item.late_minutes || 0);
    const shortageWarning = item.is_late_shortage_override
      ? `<span class="late-shortage-icon" title="Di tre ${lateMinutes}p, quan ly xac nhan do thieu nhan su">!</span>`
      : "";
    const flexInfo =
      item.shift_code === "FLEX" && item.flexible_start_at && item.flexible_end_at
        ? `<br /><small>Khung linh hoạt: ${escapeHtml(item.flexible_start_at)} - ${escapeHtml(item.flexible_end_at)}</small>`
        : "";
    row.innerHTML = `
      <div>
        <strong>${safeEmployeeName}</strong> - ${safeShiftCode}<br />
        <small>Bắt đầu ca: ${formatDateTimeDisplay(item.shift_start_at)} | Hạn check-in: ${formatDateTimeDisplay(item.late_deadline_at)}</small><br />
        ${flexInfo}
        <small>Trạng thái: <span class="shift-status-badge ${shiftAttendanceStatusClass(item.status)}">${safeStatus}</span>${shortageWarning}</small><br />
        <small>Tre: ${lateMinutes} phut</small>
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
    throw new Error("Vui lòng nhập dữ liệu QR một lần");
  }

  const payload = await api("/api/attendance/scan-qr-one-time", {
    method: "POST",
    body: JSON.stringify({ qr_payload: payloadRaw }),
  });

  state.oneTimeScan = {
    branchId: Number(payload.branch_id),
    qrToken: payload.qr_token,
    randomKey: payload.random_key,
    expiresAt: payload.expires_at || "",
    expiresAtEpoch: parseDbDateTimeToEpoch(payload.expires_at),
  };
  $("#employee-one-time-random-key").value = payload.random_key || "";
  updateEmployeeOneTimeCheckInButtonState();
  startEmployeeOneTimeCountdown();
  showToast("Da quet QR va nhan random key");
}

async function checkInEmployeeOneTime() {
  const availability = await loadCheckInAvailability("employee");
  if (!availability.can_check_in) {
    throw new Error(availability.reason || "Bạn chưa có ca làm hôm nay");
  }

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
    throw new Error("Vui lòng nhập mã xác thực một lần");
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
  showToast("Bắt đầu chấm công thành công");
  $("#employee-one-time-qr-payload").value = "";
  $("#employee-one-time-random-key").value = "";
  $("#employee-one-time-scan-result").textContent = "";
  state.oneTimeScan = null;
  stopEmployeeOneTimeCountdown();
  state.employeeCanCheckInToday = false;
  updateEmployeeOneTimeCheckInButtonState();
  await loadMyAttendance("employee");
}

async function checkOutEmployee() {
  await api("/api/attendance/check-out", { method: "POST" });
  showToast("Kết thúc ca thành công");
  await loadMyAttendance("employee");
}

async function checkInManager() {
  const availability = await loadCheckInAvailability("manager");
  if (!availability.can_check_in) {
    throw new Error(availability.reason || "Bạn chưa có ca làm hôm nay");
  }

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
  showToast("Kết thúc ca thành công");
  await loadMyAttendance("manager");
  await loadManagerShiftAttendanceToday();
}

async function confirmAttendanceEmployee() {
  const note = $("#attendance-note").value.trim();
  await api("/api/attendance/confirm-open", {
    method: "POST",
    body: JSON.stringify({ note }),
  });
  showToast("Đã xác nhận mốc tính công");
  await loadMyAttendance("employee");
}

async function confirmAttendanceManager() {
  const note = $("#manager-attendance-note").value.trim();
  await api("/api/attendance/confirm-open", {
    method: "POST",
    body: JSON.stringify({ note }),
  });
  showToast("Đã xác nhận mốc tính công");
  await loadMyAttendance("manager");
}

function shiftText(code) {
  const s = getShiftByCode(code);
  if (!s) return code;
  if (s.code === "FLEX") return `${s.code} (${s.name})`;
  return `${s.code} (${s.name}: ${s.start}-${s.end})`;
}

function shiftTimeRangeText(code) {
  const s = getShiftByCode(code);
  if (!s) return code;
  if (s.code === "FLEX") return "Linh hoạt";
  if (!s.start || !s.end) return s.name || code;
  return `${s.start}-${s.end}`;
}

function shiftRowHourLabel(code) {
  const s = getShiftByCode(code);
  if (!s) return code;
  if (s.code === "FLEX") return "Giờ linh hoạt";
  if (!s.start || !s.end) return s.name || code;
  return `${s.start}-${s.end}`;
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

function weekHeaderCellsHtmlFor(metaList) {
  return metaList
    .map((meta) => {
      if (!meta.dateLabel) {
        return `<th>${meta.label}</th>`;
      }
      return `<th>${meta.label}<br /><small>${meta.dateLabel}</small></th>`;
    })
    .join("");
}

function hhmmToMinutes(value) {
  const text = String(value || "").trim();
  const parts = text.split(":");
  if (parts.length !== 2) return Number.NaN;
  const hour = Number(parts[0]);
  const minute = Number(parts[1]);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return Number.NaN;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return Number.NaN;
  return hour * 60 + minute;
}

function shiftRangeFromHHMM(startText, endText) {
  const start = hhmmToMinutes(startText);
  const end = hhmmToMinutes(endText);
  if (Number.isNaN(start) || Number.isNaN(end) || end <= start) return null;
  return { start, end };
}

function shiftRangeByCode(shiftCode, record = null) {
  const normalized = String(shiftCode || "").toUpperCase();
  const rangeFromDefinition = (code) => {
    const shift = getShiftByCode(code);
    return shift ? shiftRangeFromHHMM(shift.start, shift.end) : null;
  };

  if (SMALL_SHIFT_CODES.includes(normalized)) {
    return rangeFromDefinition(normalized);
  }
  if (normalized === "M1") {
    const s1 = rangeFromDefinition("S1");
    const s2 = rangeFromDefinition("S2");
    if (s1 && s2) return { start: s1.start, end: s2.end };
    return rangeFromDefinition("M1");
  }
  if (normalized === "M2") {
    const s3 = rangeFromDefinition("S3");
    const s4 = rangeFromDefinition("S4");
    if (s3 && s4) return { start: s3.start, end: s4.end };
    return rangeFromDefinition("M2");
  }
  if (normalized === "FLEX") {
    return shiftRangeFromHHMM(record?.flexible_start_at, record?.flexible_end_at);
  }
  return rangeFromDefinition(normalized);
}

function rangesOverlap(a, b) {
  if (!a || !b) return false;
  return Math.max(a.start, b.start) < Math.min(a.end, b.end);
}

function validateNoShiftConflicts(selections, { includeBranch = false } = {}) {
  const groups = new Map();

  selections.forEach((item) => {
    const day = Number(item.day_of_week || 0);
    const branchPart = includeBranch ? String(Number(item.branch_id || 0)) : "GLOBAL";
    const key = `${branchPart}|${day}`;
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(item);
  });

  for (const [, records] of groups.entries()) {
    for (let i = 0; i < records.length; i += 1) {
      for (let j = i + 1; j < records.length; j += 1) {
        const a = records[i];
        const b = records[j];
        const rangeA = shiftRangeByCode(a.shift_code, a);
        const rangeB = shiftRangeByCode(b.shift_code, b);
        if (rangesOverlap(rangeA, rangeB)) {
          const dayText = weekDaysMeta().find((meta) => meta.day === Number(a.day_of_week))?.label || `Ngày ${a.day_of_week}`;
          throw new Error(`Xung đột ca trong ${dayText}: ${a.shift_code} trùng giờ với ${b.shift_code}`);
        }
      }
    }
  }
}

function attendanceSourceText(item) {
  if (item.attendance_source === "manager_override") {
    const managerName = String(item.checked_in_by_manager_name || "").trim();
    if (managerName && managerName !== "-") {
      return `Nguồn: Quản lý xác nhận (${managerName})`;
    }
    return "Nguồn: Quản lý xác nhận";
  }
  return "Nguồn: Nhân viên tự check-in";
}

function employeeInitials(nameText) {
  const name = String(nameText || "").trim();
  if (!name) return "--";
  const parts = name.split(/\s+/).filter(Boolean);
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return `${parts[0][0] || ""}${parts[parts.length - 1][0] || ""}`.toUpperCase();
}

function shiftDisplayTime(item) {
  if (item.shift_code === "FLEX") {
    return `${item.flexible_start_at || "--:--"}-${item.flexible_end_at || "--:--"}`;
  }
  const shift = getShiftByCode(item.shift_code);
  if (shift?.start && shift?.end) {
    return `${shift.start}-${shift.end}`;
  }
  return item.shift_code || "--";
}

function displayRowShiftCode(record) {
  const shiftCode = String(record?.shift_code || "").toUpperCase();
  if (SMALL_SHIFT_CODES.includes(shiftCode)) return shiftCode;
  if (shiftCode === "M1") return "S1";
  if (shiftCode === "M2") return "S3";
  if (shiftCode === "FLEX") {
    const startMin = hhmmToMinutes(record?.flexible_start_at);
    if (Number.isNaN(startMin)) return "S1";
    if (startMin < 11 * 60) return "S1";
    if (startMin < 15 * 60) return "S2";
    if (startMin < 19 * 60) return "S3";
    return "S4";
  }
  return "S1";
}

function groupScheduleRowsForDisplay(rows) {
  const groups = new Map();
  rows.forEach((item) => {
    const isLarge = item.shift_code === "M1" || item.shift_code === "M2" || item.shift_code === "FLEX";
    const isGroup = item.registration_type === "group" && !!item.group_code;
    let key = "";
    if (isGroup) {
      key = `${item.shift_code}|GROUP|${item.group_code}|${shiftDisplayTime(item)}`;
    } else {
      key = isLarge
        ? `${item.shift_code}|${shiftDisplayTime(item)}`
        : `${item.shift_code}|${item.employee_id}`;
    }
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(item);
  });
  return [...groups.values()];
}

function compactGroupedShiftTag(items) {
  const list = items || [];
  if (!list.length) return "";
  if (list.length === 1) return compactShiftTag(list[0]);

  const lead = list[0];
  const isGroup = lead.registration_type === "group" && !!lead.group_code;
  if (isGroup) {
    const teamCode = escapeHtml(lead.group_code);
    const timeText = escapeHtml(shiftDisplayTime(lead));
    return `<span class="tt-pill compact-shift-pill worker-shift-pill grouped-shift-pill" title="Team ${teamCode} | ${timeText}"><span class="worker-name-icon">${list.length}</span><span class="worker-shift-meta"><span class="worker-name-text">Team ${teamCode}</span><small>${timeText}</small></span></span>`;
  }

  const isMain = lead.shift_code === "M1" || lead.shift_code === "M2";
  const isFlex = lead.shift_code === "FLEX";
  const names = list
    .map((x) => escapeHtml(x.employee_name || "-"))
    .slice(0, 4)
    .join(", ");
  const suffix = list.length > 4 ? ` +${list.length - 4}` : "";
  const timeText = escapeHtml(shiftDisplayTime(lead));
  const extraClass = isFlex ? "is-flex" : isMain ? "is-main" : "";

  return `<span class="tt-pill compact-shift-pill worker-shift-pill grouped-shift-pill ${extraClass}" title="${names}${suffix} | ${timeText}"><span class="worker-name-icon">${list.length}</span><span class="worker-shift-meta"><span class="worker-name-text">${names}${suffix}</span><small>${timeText}</small></span></span>`;
}

function collectAnchoredRows(records, rowShiftCode, day) {
  return records.filter((item) => {
    const days = expandDays(item.day_of_week);
    if (!days.includes(day)) return false;
    return displayRowShiftCode(item) === rowShiftCode;
  });
}

function buildManagerAssignableRows(rows) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const groupedMap = new Map();
  const ordered = [];

  sourceRows.forEach((row) => {
    const isGroup = row.registration_type === "group" && !!row.group_code;
    if (!isGroup) {
      ordered.push({
        employee_id: Number(row.employee_id),
        shift_code: row.shift_code,
        employee_name: row.employee_name,
        flexible_start_at: row.flexible_start_at,
        flexible_end_at: row.flexible_end_at,
        is_group: false,
        group_code: null,
      });
      return;
    }

    const groupKey = [
      String(row.shift_code || ""),
      String(row.group_code || ""),
      String(row.flexible_start_at || ""),
      String(row.flexible_end_at || ""),
    ].join("|");

    if (!groupedMap.has(groupKey)) {
      const item = {
        employee_id: Number(row.employee_id),
        shift_code: row.shift_code,
        employee_name: `Team ${row.group_code}`,
        flexible_start_at: row.flexible_start_at,
        flexible_end_at: row.flexible_end_at,
        is_group: true,
        group_code: row.group_code,
      };
      groupedMap.set(groupKey, item);
      ordered.push(item);
      return;
    }

    const existing = groupedMap.get(groupKey);
    if (Number(row.employee_id) < Number(existing.employee_id)) {
      existing.employee_id = Number(row.employee_id);
    }
  });

  return ordered;
}

function compactShiftTag(item) {
  if (item.registration_type === "group" && item.group_code) {
    const teamCode = escapeHtml(item.group_code);
    const timeText = escapeHtml(shiftDisplayTime(item));
    return `<span class="tt-pill worker-shift-pill" title="Team ${teamCode} | ${timeText}"><span class="worker-name-icon">T</span><span class="worker-shift-meta"><span class="worker-name-text">Team ${teamCode}</span><small>${timeText}</small></span></span>`;
  }

  const name = escapeHtml(item.employee_name || "-");
  const initials = escapeHtml(employeeInitials(item.employee_name));
  const timeText = escapeHtml(shiftDisplayTime(item));
  if (item.shift_code === "FLEX") {
    return `<span class="tt-pill compact-shift-pill is-flex worker-shift-pill" title="${name} - FLEX ${timeText}"><span class="worker-name-icon">${initials}</span><span class="worker-shift-meta"><span class="worker-name-text">${name}</span><small class="flex-registered-time">${timeText}</small></span></span>`;
  }
  if (item.shift_code === "M1" || item.shift_code === "M2") {
    return `<span class="tt-pill compact-shift-pill is-main worker-shift-pill" title="${name} - ${escapeHtml(item.shift_code)} ${timeText}"><span class="worker-name-icon">${initials}</span><span class="worker-shift-meta"><span class="worker-name-text">${name}</span><small>${timeText}</small></span></span>`;
  }
  return `<span class="tt-pill worker-shift-pill" title="${name}"><span class="worker-name-icon">${initials}</span><span class="worker-shift-meta"><span class="worker-name-text">${name}</span></span></span>`;
}

function ensureManagerDayFilterOptions() {
  const options = [
    { value: "0", label: "Tất cả ngày" },
    ...weekDaysMeta().map((meta) => ({ value: String(meta.day), label: `${meta.label} (${meta.dateLabel || ""})` })),
  ];

  const bind = (selector, currentValue) => {
    const select = $(selector);
    if (!select) return;
    const oldValue = select.value || String(currentValue || 0);
    select.innerHTML = "";
    options.forEach((op) => {
      const node = document.createElement("option");
      node.value = op.value;
      node.textContent = op.label;
      select.appendChild(node);
    });
    if ([...select.options].some((op) => op.value === oldValue)) {
      select.value = oldValue;
    }
  };

  bind("#manager-schedule-day-filter", managerScheduleUiState.selectedDay);
  bind("#manager-assigned-day-filter", managerScheduleUiState.assignedSelectedDay);
}

function groupCapacityText(item) {
  const count = Number(item.member_count || 0);
  const max = item.max_members == null ? null : Number(item.max_members);
  if (!max) return `${count} thành viên`;
  return `${count}/${max} thành viên`;
}

function updateEmployeeRegistrationUiState() {
  const type = $("#employee-registration-type")?.value || "individual";
  const groupWrap = $("#employee-group-code-wrap");

  if (groupWrap) {
    groupWrap.classList.toggle("hidden", type !== "group");
  }
}

function employeeFlexKey(branchId, dayOfWeek) {
  return `${Number(branchId)}|FLEX|${Number(dayOfWeek)}`;
}

function parseEmployeeFlexKey(key) {
  const [branchRaw, _shift, dayRaw] = String(key || "").split("|");
  return {
    branchId: Number(branchRaw || 0),
    dayOfWeek: Number(dayRaw || 0),
  };
}

function selectedEmployeeFlexKeys() {
  return [...document.querySelectorAll("#employee-shift-grid input:checked[data-shift='FLEX']")].map((el) =>
    employeeFlexKey(el.dataset.branch, el.dataset.day)
  );
}

function currentFlexDraftStorageKey() {
  const userId = Number(state.currentUser?.id || 0);
  return `wm_flex_draft_${userId}_${currentWeek()}`;
}

function saveEmployeeFlexDraft() {
  localStorage.setItem(currentFlexDraftStorageKey(), JSON.stringify(state.employeeFlexTimeByKey || {}));
}

function loadEmployeeFlexDraft() {
  const raw = localStorage.getItem(currentFlexDraftStorageKey());
  if (!raw) return;
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") {
      Object.entries(parsed).forEach(([key, value]) => {
        const slot = value || {};
        const start = String(slot.start || "").trim();
        const end = String(slot.end || "").trim();
        if (start && end && !state.employeeFlexTimeByKey[key]) {
          state.employeeFlexTimeByKey[key] = { start, end };
        }
      });
    }
  } catch (_error) {
    // Ignore malformed local draft and continue with server values.
  }
}

function updateEmployeeShiftSummary() {
  const node = $("#employee-shifts-summary");
  if (!node) return;
  const checkedInputs = [...document.querySelectorAll("#employee-shift-grid input:checked[data-shift]")];
  const total = checkedInputs.length;
  const flexChecked = checkedInputs.filter((el) => el.dataset.shift === "FLEX").length;
  const flexCompleted = checkedInputs.filter((el) => {
    if (el.dataset.shift !== "FLEX") return false;
    const key = employeeFlexKey(el.dataset.branch, el.dataset.day);
    const slot = state.employeeFlexTimeByKey[key] || { start: "", end: "" };
    return !!slot.start && !!slot.end;
  }).length;
  node.innerHTML = `
    <span class="summary-pill">Đã chọn: <strong>${total}</strong> ô</span>
    <span class="summary-pill">FLEX: <strong>${flexChecked}</strong> ô</span>
    <span class="summary-pill ${flexChecked > flexCompleted ? "warn" : ""}">Đã nhập giờ FLEX: <strong>${flexCompleted}/${flexChecked}</strong></span>
  `;
}

function updateManagerSelfShiftSummary() {
  const node = $("#manager-self-shifts-summary");
  if (!node) return;
  const checkedInputs = [...document.querySelectorAll("#manager-self-shifts-list input:checked[data-shift]")];
  const total = checkedInputs.length;
  const flexChecked = checkedInputs.filter((el) => el.dataset.shift === "FLEX").length;
  const flexCompleted = checkedInputs.filter((el) => {
    if (el.dataset.shift !== "FLEX") return false;
    const key = managerSelfFlexKey(el.dataset.day);
    const slot = state.managerSelfFlexTimeByKey[key] || { start: "", end: "" };
    return !!slot.start && !!slot.end;
  }).length;
  const byShift = checkedInputs.reduce((acc, el) => {
    const shift = el.dataset.shift || "";
    acc[shift] = (acc[shift] || 0) + 1;
    return acc;
  }, {});
  const shiftTextLine = Object.entries(byShift)
    .slice(0, 4)
    .map(([code, count]) => `${code}: ${count}`)
    .join(" | ");
  node.innerHTML = `
    <span class="summary-pill">Tổng ô đã chọn: <strong>${total}</strong></span>
    <span class="summary-pill">Theo ca: <strong>${escapeHtml(shiftTextLine || "Chưa chọn")}</strong></span>
    <span class="summary-pill ${flexChecked > flexCompleted ? "warn" : ""}">Giờ FLEX: <strong>${flexCompleted}/${flexChecked}</strong></span>
  `;
}

function managerSelfFlexKey(dayOfWeek) {
  return `FLEX|${Number(dayOfWeek)}`;
}

function managerSelfFlexDraftStorageKey() {
  const userId = Number(state.currentUser?.id || 0);
  return `wm_manager_flex_draft_${userId}_${currentWeek()}`;
}

function saveEmployeeFlexEditorDismissed() {
  localStorage.setItem("wm_employee_flex_editor_dismissed", state.employeeFlexEditorDismissed ? "1" : "0");
}

function saveManagerFlexEditorDismissed() {
  localStorage.setItem("wm_manager_flex_editor_dismissed", state.managerSelfFlexEditorDismissed ? "1" : "0");
}

function saveManagerSelfFlexDraft() {
  localStorage.setItem(
    managerSelfFlexDraftStorageKey(),
    JSON.stringify(state.managerSelfFlexTimeByKey || {})
  );
}

function loadManagerSelfFlexDraft() {
  const raw = localStorage.getItem(managerSelfFlexDraftStorageKey());
  if (!raw) return;
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") {
      Object.entries(parsed).forEach(([key, value]) => {
        const slot = value || {};
        const start = String(slot.start || "").trim();
        const end = String(slot.end || "").trim();
        if (start && end && !state.managerSelfFlexTimeByKey[key]) {
          state.managerSelfFlexTimeByKey[key] = { start, end };
        }
      });
    }
  } catch (_error) {
    // Ignore malformed local draft and continue with server values.
  }
}

function parseManagerSelfFlexKey(key) {
  const [_shift, dayRaw] = String(key || "").split("|");
  return { dayOfWeek: Number(dayRaw || 0) };
}

function selectedManagerSelfFlexKeys() {
  return [...document.querySelectorAll("#manager-self-shifts-list input:checked[data-shift='FLEX']")].map((el) =>
    managerSelfFlexKey(el.dataset.day)
  );
}

function refreshManagerSelfFlexCells() {
  document.querySelectorAll("#manager-self-shifts-list input[data-shift='FLEX']").forEach((input) => {
    if (!(input instanceof HTMLInputElement)) return;
    const key = managerSelfFlexKey(input.dataset.day);
    const slot = state.managerSelfFlexTimeByKey[key] || { start: "", end: "" };
    const text = input.parentElement?.querySelector(".manager-self-flex-text");
    if (text instanceof HTMLElement) {
      text.textContent = slot.start && slot.end ? `${slot.start}-${slot.end}` : "--:---";
    }
  });
}

function renderManagerSelfFlexEditor() {
  const wrap = $("#manager-self-flex-editor-wrap");
  const list = $("#manager-self-flex-editor-list");
  const openButton = $("#btn-open-manager-self-flex-editor");
  const backdrop = $("#manager-self-flex-modal-backdrop");
  if (!wrap || !list) return;

  const keys = selectedManagerSelfFlexKeys();
  if (!keys.length) {
    state.managerSelfFlexEditorDismissed = false;
    wrap.classList.add("hidden");
    backdrop?.classList.add("hidden");
    openButton?.classList.add("hidden");
    list.innerHTML = "";
    updateManagerSelfShiftSummary();
    return;
  }

  const shouldHidePanel = state.managerSelfFlexEditorDismissed;
  wrap.classList.toggle("hidden", shouldHidePanel);
  backdrop?.classList.toggle("hidden", shouldHidePanel);
  openButton?.classList.toggle("hidden", !shouldHidePanel);

  const dayMetaByDay = new Map(weekDaysMeta().map((meta) => [meta.day, meta]));
  list.innerHTML = "";
  keys.forEach((key) => {
    const slot = state.managerSelfFlexTimeByKey[key] || { start: "", end: "" };
    const { dayOfWeek } = parseManagerSelfFlexKey(key);
    const dayMeta = dayMetaByDay.get(dayOfWeek);
    const dayLabel = dayMeta ? `${dayMeta.label} ${dayMeta.dateLabel ? `(${dayMeta.dateLabel})` : ""}` : `Ngày ${dayOfWeek}`;

    const row = document.createElement("div");
    row.className = "list-item flex-editor-row";
    row.innerHTML = `
      <span><strong>${escapeHtml(dayLabel)}</strong><br /><small>Ca linh hoạt của quản lý</small></span>
      <div class="row compact flex-editor-controls">
        <label>Giờ vào <input type="time" data-manager-flex-key="${key}" data-manager-flex-field="start" value="${escapeHtml(slot.start || "")}" /></label>
        <label>Giờ ra <input type="time" data-manager-flex-key="${key}" data-manager-flex-field="end" value="${escapeHtml(slot.end || "")}" /></label>
      </div>
    `;
    list.appendChild(row);
  });

  if (list.dataset.bound !== "true") {
    list.dataset.bound = "true";
    list.addEventListener("input", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement)) return;
      const key = target.dataset.managerFlexKey;
      const field = target.dataset.managerFlexField;
      if (!key || !field) return;
      const current = state.managerSelfFlexTimeByKey[key] || { start: "", end: "" };
      current[field] = target.value || "";
      state.managerSelfFlexTimeByKey[key] = current;
      refreshManagerSelfFlexCells();
      updateManagerSelfShiftSummary();
    });
  }

  refreshManagerSelfFlexCells();
  updateManagerSelfShiftSummary();
}

function updateManagerScheduleSummary() {
  const prefNode = $("#manager-preferences-summary");
  const assignedNode = $("#manager-assigned-summary");

  if (prefNode) {
    const selected = document.querySelectorAll("#manager-preferences input:checked").length;
    const all = document.querySelectorAll("#manager-preferences input[type='checkbox']").length;
    prefNode.innerHTML = `
      <span class="summary-pill">Đã phân công: <strong>${selected}/${all}</strong> lựa chọn</span>
    `;
  }

  if (assignedNode) {
    const inRange = document.querySelectorAll("#manager-schedule td.is-in-range").length;
    const outRange = document.querySelectorAll("#manager-schedule td.is-out-range").length;
    const empty = document.querySelectorAll("#manager-schedule td.is-empty").length;
    assignedNode.innerHTML = `
      <span class="summary-pill">Đủ định mức: <strong>${inRange}</strong></span>
      <span class="summary-pill ${outRange > 0 ? "warn" : ""}">Lệch định mức: <strong>${outRange}</strong></span>
      <span class="summary-pill">Ô trống: <strong>${empty}</strong></span>
    `;
  }
}

function renderEmployeeFlexEditor() {
  const wrap = $("#employee-flex-editor-wrap");
  const list = $("#employee-flex-editor-list");
  const openButton = $("#btn-open-flex-editor");
  const backdrop = $("#employee-flex-modal-backdrop");
  if (!wrap || !list) return;

  const keys = selectedEmployeeFlexKeys();
  if (!keys.length) {
    state.employeeFlexEditorDismissed = false;
    wrap.classList.add("hidden");
    backdrop?.classList.add("hidden");
    openButton?.classList.add("hidden");
    list.innerHTML = "";
    updateEmployeeShiftSummary();
    return;
  }

  const shouldHidePanel = state.employeeFlexEditorDismissed;
  wrap.classList.toggle("hidden", shouldHidePanel);
  backdrop?.classList.toggle("hidden", shouldHidePanel);
  openButton?.classList.toggle("hidden", !shouldHidePanel);

  const dayMetaByDay = new Map(weekDaysMeta().map((meta) => [meta.day, meta]));
  const branchNameById = new Map((state.employeeBranches || []).map((branch) => [Number(branch.id), branch.name]));

  list.innerHTML = "";
  keys.forEach((key) => {
    const slot = state.employeeFlexTimeByKey[key] || { start: "", end: "" };
    const { branchId, dayOfWeek } = parseEmployeeFlexKey(key);
    const dayMeta = dayMetaByDay.get(dayOfWeek);
    const branchName = branchNameById.get(branchId) || `Chi nhánh ${branchId}`;
    const dayLabel = dayMeta ? `${dayMeta.label} ${dayMeta.dateLabel ? `(${dayMeta.dateLabel})` : ""}` : `Ngày ${dayOfWeek}`;

    const row = document.createElement("div");
    row.className = "list-item flex-editor-row";
    row.innerHTML = `
      <span><strong>${escapeHtml(branchName)}</strong><br /><small>${escapeHtml(dayLabel)}</small></span>
      <div class="row compact flex-editor-controls">
        <label>Giờ vào <input type="time" data-flex-key="${key}" data-flex-field="start" value="${escapeHtml(slot.start || "")}" /></label>
        <label>Giờ ra <input type="time" data-flex-key="${key}" data-flex-field="end" value="${escapeHtml(slot.end || "")}" /></label>
      </div>
    `;
    list.appendChild(row);
  });

  if (list.dataset.bound !== "true") {
    list.dataset.bound = "true";
    list.addEventListener("input", (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement)) return;
      const key = target.dataset.flexKey;
      const field = target.dataset.flexField;
      if (!key || !field) return;
      const current = state.employeeFlexTimeByKey[key] || { start: "", end: "" };
      current[field] = target.value || "";
      state.employeeFlexTimeByKey[key] = current;
      updateEmployeeShiftSummary();
    });
  }

  updateEmployeeShiftSummary();
}

async function loadEmployeeRegistrationGroups() {
  const items = await api(`/api/employee/registration-groups?week_start=${encodeURIComponent(currentWeek())}`);
  const branchFilter = Number($("#employee-group-branch")?.value || 0);
  const filtered = branchFilter ? items.filter((item) => Number(item.branch_id) === branchFilter) : items;
  const list = $("#employee-registration-groups-list");
  if (!list) return;
  list.innerHTML = "";

  if (!filtered.length) {
    const empty = document.createElement("div");
    empty.className = "list-item";
    empty.innerHTML = "<span>Chưa có nhóm đăng ký cho tuần này.</span>";
    list.appendChild(empty);
    return;
  }

  filtered.forEach((item) => {
    const row = document.createElement("div");
    const count = Number(item.member_count || 0);
    const max = item.max_members == null ? null : Number(item.max_members);
    const isFull = !!max && count >= max;
    const membersText = String(item.members_text || "").trim() || "Chưa có thành viên";
    row.className = `list-item registration-group-item ${isFull ? "is-full" : ""}`;
    row.innerHTML = `
      <span>
        <strong>Mã nhóm: ${escapeHtml(item.group_code)}</strong><br />
        <small>Tên nhóm: ${escapeHtml(item.group_name || "-")}</small><br />
        <small>${escapeHtml(item.branch_name)} | ${groupCapacityText(item)}</small><br />
        <small>Thành viên: ${escapeHtml(membersText)}</small>
      </span>
      <button class="ghost" data-use-group-code="${escapeHtml(item.group_code)}" ${employeePreferenceUiState.locked ? "disabled" : ""}>Dùng mã nhóm</button>
    `;
    list.appendChild(row);
  });

  list.querySelectorAll("button[data-use-group-code]").forEach((button) => {
    button.addEventListener("click", () => {
      const code = button.dataset.useGroupCode || "";
      $("#employee-group-code").value = code;
      $("#employee-group-code-join").value = code;
      $("#employee-registration-type").value = "group";
      updateEmployeeRegistrationUiState();
      showToast("Đã chọn mã nhóm để đăng ký ca");
    });
  });
}

async function joinEmployeeGroup() {
  if (employeePreferenceUiState.locked) {
    throw new Error("Tuần này đã chốt ca, không thể chỉnh sửa thêm");
  }
  const branch_id = Number($("#employee-group-branch").value);
  const group_code = $("#employee-group-code-join").value.trim();
  if (!branch_id) throw new Error("Vui lòng chọn chi nhánh");
  if (!group_code) throw new Error("Vui lòng nhập mã nhóm");

  await api("/api/employee/registration-groups/join", {
    method: "POST",
    body: JSON.stringify({
      week_start: currentWeek(),
      branch_id,
      group_code,
    }),
  });

  $("#employee-group-code").value = group_code;
  $("#employee-registration-type").value = "group";
  updateEmployeeRegistrationUiState();
  showToast("Đã tham gia nhóm");
  await loadEmployeeRegistrationGroups();
}

async function loadEmployeeShifts() {
  const weekStart = currentWeek();
  const [prefs, lockInfo] = await Promise.all([
    api(`/api/employee/preferences?week_start=${encodeURIComponent(weekStart)}`),
    api(`/api/employee/preferences-lock?week_start=${encodeURIComponent(weekStart)}`),
  ]);
  employeePreferenceUiState.locked = !!lockInfo.locked;

  const prefMap = new Map();
  state.employeeFlexTimeByKey = {};
  prefs.forEach((item) => {
    expandDays(item.day_of_week).forEach((day) => {
      const key = `${Number(item.branch_id)}|${item.shift_code}|${day}`;
      prefMap.set(key, item);
      if (item.shift_code === "FLEX") {
        state.employeeFlexTimeByKey[key] = {
          start: item.flexible_start_at || "",
          end: item.flexible_end_at || "",
        };
      }
    });
  });
  loadEmployeeFlexDraft();

  if (prefs.length) {
    const first = prefs[0];
    if (first.registration_type) {
      $("#employee-registration-type").value = first.registration_type;
    }
    if (first.group_code) {
      $("#employee-group-code").value = first.group_code;
      $("#employee-group-code-join").value = first.group_code;
    }
  }

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
  updateEmployeeRegistrationUiState();
  const lockNote = $("#employee-shifts-lock-note");
  if (lockNote) {
    lockNote.textContent = employeePreferenceUiState.locked
      ? "Tuần này đã chốt đăng ký ca. Bạn chỉ có thể chỉnh sửa khi sang tuần mới."
      : "Bạn có thể đăng ký ca cho tuần hiện tại. Sau khi lưu lần đầu, hệ thống sẽ khóa đến tuần mới.";
  }

  const lockInputs = [
    "#employee-registration-type",
    "#employee-group-code",
    "#employee-group-code-join",
    "#employee-group-branch",
    "#btn-employee-join-group",
    "#btn-open-flex-editor",
    "#btn-save-flex-hours-draft",
    "#btn-save-employee-shifts",
  ];
  lockInputs.forEach((selector) => {
    const node = $(selector);
    if (node) {
      node.disabled = employeePreferenceUiState.locked;
    }
  });
  document.querySelectorAll("#employee-shift-grid input[type='checkbox']").forEach((checkbox) => {
    checkbox.disabled = employeePreferenceUiState.locked;
  });

  await loadEmployeeRegistrationGroups();
  renderEmployeeFlexEditor();
  updateEmployeeShiftSummary();
}

async function saveEmployeeShifts() {
  if (employeePreferenceUiState.locked) {
    throw new Error("Tuần này đã chốt đăng ký ca, không thể chỉnh sửa thêm");
  }
  const registrationType = $("#employee-registration-type").value;
  const groupCode = $("#employee-group-code").value.trim().toUpperCase();

  if (registrationType === "group" && !groupCode) {
    throw new Error("Vui lòng nhập mã nhóm khi đăng ký theo nhóm");
  }

  const selections = [...document.querySelectorAll("#employee-shift-grid input:checked")].map((el) => {
    const shiftCode = el.dataset.shift;
    const key = employeeFlexKey(el.dataset.branch, el.dataset.day);
    const flexSlot = state.employeeFlexTimeByKey[key] || { start: "", end: "" };
    const payload = {
      branch_id: Number(el.dataset.branch),
      shift_code: shiftCode,
      day_of_week: Number(el.dataset.day),
      registration_type: registrationType,
      group_code: registrationType === "group" ? groupCode : null,
    };
    if (shiftCode === "FLEX") {
      payload.flexible_start_at = flexSlot.start || null;
      payload.flexible_end_at = flexSlot.end || null;
    }
    return payload;
  });

  selections.forEach((item) => {
    if (item.shift_code !== "FLEX") return;
    if (!item.flexible_start_at || !item.flexible_end_at) {
      throw new Error("Vui lòng nhập giờ vào/ra cho mọi ô Ca linh hoạt đã chọn");
    }
    if (item.flexible_end_at <= item.flexible_start_at) {
      throw new Error("Giờ ra phải lớn hơn giờ vào ở Ca linh hoạt");
    }
  });

  validateNoShiftConflicts(selections, { includeBranch: true });

  await api("/api/employee/preferences", {
    method: "PUT",
    body: JSON.stringify({ week_start: currentWeek(), selections }),
  });
  employeePreferenceUiState.locked = true;
  saveEmployeeFlexDraft();
  showToast("Đã lưu ca làm");
  await loadEmployeeShifts();
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
  board.className = "week-table-wrap employee-assigned-board";
  const headerCells = weekHeaderCellsHtml();
  board.innerHTML = `
    <table class="week-table employee-assigned-table">
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
                (item) => {
                  const modeLabel = item.registration_type === "group"
                    ? `Nhóm: ${item.group_code || "-"}`
                    : "Cá nhân";
                  const teamLabel =
                    item.registration_type === "group"
                      ? ` | Team (${Number(item.team_size || 0)}): ${item.team_members_text || "-"}`
                      : "";
                  const flexLabel =
                    item.shift_code === "FLEX" && item.flexible_start_at && item.flexible_end_at
                      ? ` | Linh hoạt: ${item.flexible_start_at}-${item.flexible_end_at}`
                      : "";
                  return `<span class="tt-pill assigned-pill"><strong>${item.branch_name}</strong><small>Xếp bởi: ${item.assigned_by_name} | ${modeLabel}${teamLabel}${flexLabel}</small></span>`;
                }
              )
              .join("")
          : `<span class="tt-empty assigned-empty">Trống</span>`;
        return `<td class="assigned-cell day-${meta.day}"><div class="tt-content assigned-content">${content}</div></td>`;
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

async function renderMyIssueReplies(issueId) {
  const listNode = document.querySelector(`div[data-my-issue-replies-list='${issueId}']`);
  if (!listNode) return;
  const replies = await api(`/api/issues/my/${issueId}/replies`);
  listNode.innerHTML = "";
  if (!replies.length) {
    listNode.innerHTML = "<div class='list-item'><small>Chưa có phản hồi nào từ quản lý/CEO.</small></div>";
    return;
  }
  replies.forEach((reply) => {
    const row = document.createElement("div");
    row.className = "list-item";
    row.innerHTML = `<span><strong>${escapeHtml(reply.sender_name || "Không rõ")}</strong> (${escapeHtml(reply.sender_role || "-")})<br /><small>${escapeHtml(reply.message || "")}</small></span><small>${escapeHtml(reply.created_at || "")}</small>`;
    listNode.appendChild(row);
  });
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
    const safeTitle = escapeHtml(item.title || "-");
    const safeBranch = escapeHtml(item.branch_name || "-");
    const safeStatus = escapeHtml(issueStatusLabel(item.status));
    const safeDetails = escapeHtml(item.details || "-");
    const safeManagerNote = escapeHtml(item.manager_note || "");
    row.className = "list-item";
    row.innerHTML = `
      <div>
        <strong>${safeTitle}</strong><br /><small>${safeBranch} | ${safeStatus}</small>
      </div>
      <div class="row compact">
        <button class="ghost" data-my-issue-detail-toggle="${item.id}">Xem nội dung</button>
        <button class="ghost" data-my-issue-replies-toggle="${item.id}">Xem phản hồi</button>
      </div>
      <small>${escapeHtml(item.created_at || "")}</small>
      <div class="issue-detail-box hidden" data-my-issue-detail="${item.id}">
        <p><strong>Nội dung báo cáo:</strong> ${safeDetails}</p>
        <p><strong>Ghi chú quản lý:</strong> ${safeManagerNote || "Chưa có"}</p>
      </div>
      <div class="issue-detail-box hidden" data-my-issue-replies="${item.id}">
        <div class="list" data-my-issue-replies-list="${item.id}"></div>
      </div>
    `;
    list.appendChild(row);
  });

  document.querySelectorAll("button[data-my-issue-detail-toggle]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = Number(btn.dataset.myIssueDetailToggle);
      const detail = document.querySelector(`div[data-my-issue-detail='${id}']`);
      if (!detail) return;
      detail.classList.toggle("hidden");
      btn.textContent = detail.classList.contains("hidden") ? "Xem nội dung" : "Ẩn nội dung";
    });
  });

  document.querySelectorAll("button[data-my-issue-replies-toggle]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = Number(btn.dataset.myIssueRepliesToggle);
      const box = document.querySelector(`div[data-my-issue-replies='${id}']`);
      if (!box) return;
      box.classList.toggle("hidden");
      if (!box.classList.contains("hidden")) {
        await renderMyIssueReplies(id);
      }
    });
  });
}

async function loadManagerSelfShifts() {
  const prefs = await api(`/api/manager/self-preferences?week_start=${encodeURIComponent(currentWeek())}`);
  state.managerSelfFlexTimeByKey = {};
  const checked = new Set(
    prefs.flatMap((x) => expandDays(x.day_of_week).map((day) => `${x.shift_code}|${day}`))
  );
  prefs.forEach((item) => {
    if (item.shift_code !== "FLEX") return;
    expandDays(item.day_of_week).forEach((day) => {
      const key = managerSelfFlexKey(day);
      state.managerSelfFlexTimeByKey[key] = {
        start: item.flexible_start_at || "",
        end: item.flexible_end_at || "",
      };
    });
  });
  loadManagerSelfFlexDraft();
  const root = $("#manager-self-shifts-list");
  root.innerHTML = "";

  const board = document.createElement("div");
  board.className = "week-table-wrap";
  board.innerHTML = `
    <table class="week-table">
      <thead>
        <tr>
          <th class="week-shift-col">Khung giờ</th>
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
        if (shift.code !== "FLEX") {
          return `<td><label class="tt-checkbox"><input type="checkbox" data-shift="${shift.code}" data-day="${meta.day}" ${checked.has(key) ? "checked" : ""} /> Chọn</label></td>`;
        }
        const flexKey = managerSelfFlexKey(meta.day);
        const slot = state.managerSelfFlexTimeByKey[flexKey] || { start: "", end: "" };
        const labelTime = slot.start && slot.end ? `${slot.start}-${slot.end}` : "--:---";
        return `<td><label class="tt-checkbox manager-self-flex-checkbox"><input type="checkbox" data-shift="${shift.code}" data-day="${meta.day}" ${checked.has(key) ? "checked" : ""} /> Chọn <small class="manager-self-flex-text">${escapeHtml(labelTime)}</small></label></td>`;
      })
      .join("");
    row.innerHTML = `<th class="week-shift-col">${shiftRowHourLabel(shift.code)}</th>${cells}`;
    body.appendChild(row);
  });

  root.appendChild(board);
  renderManagerSelfFlexEditor();
  refreshManagerSelfFlexCells();
  updateManagerSelfShiftSummary();
}

async function saveManagerSelfShifts() {
  const selections = [...document.querySelectorAll("#manager-self-shifts-list input:checked")].map((el) => {
    const shiftCode = el.dataset.shift;
    const day = Number(el.dataset.day);
    const payload = {
      shift_code: shiftCode,
      day_of_week: day,
    };
    if (shiftCode === "FLEX") {
      const key = managerSelfFlexKey(day);
      const flexSlot = state.managerSelfFlexTimeByKey[key] || { start: "", end: "" };
      payload.flexible_start_at = flexSlot.start || null;
      payload.flexible_end_at = flexSlot.end || null;
    }
    return payload;
  });

  selections.forEach((item) => {
    if (item.shift_code !== "FLEX") return;
    if (!item.flexible_start_at || !item.flexible_end_at) {
      throw new Error("Vui lòng nhập giờ vào/ra cho mọi ô FLEX đã chọn của quản lý");
    }
    if (item.flexible_end_at <= item.flexible_start_at) {
      throw new Error("Giờ ra phải lớn hơn giờ vào ở FLEX của quản lý");
    }
  });

  validateNoShiftConflicts(selections);

  await api("/api/manager/self-preferences", {
    method: "PUT",
    body: JSON.stringify({ week_start: currentWeek(), selections }),
  });
  showToast("Đã lưu ca của quản lý");
  await loadManagerSelfShifts();
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

function registrationMetaText(item) {
  const mode = item.registration_type === "group"
    ? `Nhóm ${item.group_code || "-"}`
    : "Cá nhân";
  const flex = item.shift_code === "FLEX" && item.flexible_start_at && item.flexible_end_at
    ? ` | Linh hoạt ${item.flexible_start_at}-${item.flexible_end_at}`
    : "";
  return `${mode}${flex}`;
}

async function loadManagerRegistrationGroups() {
  const list = $("#manager-registration-groups-list");
  if (!list) return;
  const rows = await api(`/api/manager/registration-groups?week_start=${encodeURIComponent(currentWeek())}`);
  list.innerHTML = "";

  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "list-item";
    empty.innerHTML = "<span>Chưa có nhóm nào trong tuần này.</span>";
    list.appendChild(empty);
    return;
  }

  rows.forEach((item) => {
    const row = document.createElement("div");
    const count = Number(item.member_count || 0);
    const max = item.max_members == null ? null : Number(item.max_members);
    const isFull = !!max && count >= max;
    row.className = `list-item registration-group-item ${isFull ? "is-full" : ""}`;
    row.innerHTML = `
      <span>
        <strong>${escapeHtml(item.group_name)}</strong> (${escapeHtml(item.group_code)})<br />
        <small>${groupCapacityText(item)}</small>
      </span>
      <span class="muted">${escapeHtml(item.note || "")}</span>
    `;
    list.appendChild(row);
  });
}

async function createManagerRegistrationGroup() {
  const group_name = $("#manager-group-name").value.trim();
  const group_code = $("#manager-group-code").value.trim().toUpperCase();
  const note = $("#manager-group-note").value.trim();
  const max_members = Number($("#manager-group-max-members").value || 0);

  if (!group_name) throw new Error("Vui lòng nhập tên nhóm");
  if (!Number.isInteger(max_members) || max_members < 1) {
    throw new Error("Số lượng thành viên phải từ 1 trở lên");
  }

  const payload = await api("/api/manager/registration-groups", {
    method: "POST",
    body: JSON.stringify({
      week_start: currentWeek(),
      group_name,
      group_code: group_code || null,
      max_members,
      note: note || null,
    }),
  });

  $("#manager-group-name").value = "";
  $("#manager-group-note").value = "";
  if (!group_code) {
    $("#manager-group-code").value = payload.group_code || "";
  }
  showToast("Đã tạo nhóm đăng ký ca");
  await loadManagerRegistrationGroups();
}

async function loadManagerSchedule() {
  const [prefs, schedule, scheduleRevisionPayload, employeePayload] = await Promise.all([
    api(`/api/manager/preferences?week_start=${encodeURIComponent(currentWeek())}`),
    api(`/api/manager/schedule?week_start=${encodeURIComponent(currentWeek())}`),
    api(`/api/manager/schedule-revision?week_start=${encodeURIComponent(currentWeek())}`),
    api("/api/manager/employees"),
  ]);

  managerScheduleUiState.scheduleRevision = String(scheduleRevisionPayload?.schedule_revision || "");

  const _ = employeePayload; // keep payload request for future UI extensions

  ensureManagerDayFilterOptions();
  const selectedDay = Number($("#manager-schedule-day-filter")?.value || managerScheduleUiState.selectedDay || 0);
  const assignedSelectedDay = Number($("#manager-assigned-day-filter")?.value || managerScheduleUiState.assignedSelectedDay || 0);
  managerScheduleUiState.selectedDay = selectedDay;
  managerScheduleUiState.assignedSelectedDay = assignedSelectedDay;

  const prefDays = weekDaysMeta().filter((meta) => selectedDay === 0 || meta.day === selectedDay);
  const assignedDays = weekDaysMeta().filter((meta) => assignedSelectedDay === 0 || meta.day === assignedSelectedDay);

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
  const headerCells = weekHeaderCellsHtmlFor(prefDays);
  prefBoard.innerHTML = `
    <table class="week-table compact-week-table">
      <thead>
        <tr>
          <th class="week-shift-col">Khung giờ</th>
          ${headerCells}
        </tr>
      </thead>
      <tbody id="manager-pref-week-body"></tbody>
    </table>
  `;
  const prefBody = prefBoard.querySelector("#manager-pref-week-body");

  const smallShifts = state.shifts.filter((shift) => SMALL_SHIFT_CODES.includes(shift.code));

  smallShifts.forEach((shift, shiftIndex) => {
    const row = document.createElement("tr");
    row.className = `shift-row shift-idx-${shiftIndex % 6}`;
    const cells = prefDays
      .map((meta) => {
        const rows = collectAnchoredRows(prefs, shift.code, meta.day);
        const assignableRows = buildManagerAssignableRows(rows);
        const hasAssignableRows = assignableRows.length > 0;
        const cellClass = hasAssignableRows ? "has-data" : "is-empty";
        let content = `<span class="tt-empty">Không có nhân sự để phân công</span>`;
        if (hasAssignableRows) {
          content = assignableRows
            .map((p) => {
              const key = `${p.employee_id}|${p.shift_code}|${meta.day}`;
              const checked = assigned.has(key) ? "checked" : "";
              const initials = escapeHtml(employeeInitials(p.employee_name));
              const isLargeOrFlex = p.shift_code === "FLEX" || p.shift_code === "M1" || p.shift_code === "M2";
              const timeText = isLargeOrFlex ? escapeHtml(shiftDisplayTime(p)) : "";
              const tag =
                p.shift_code === "FLEX"
                  ? ` <small class="flex-registered-time">(${p.flexible_start_at || "--:--"}-${p.flexible_end_at || "--:--"})</small>`
                  : p.shift_code === "M1" || p.shift_code === "M2"
                    ? ` <small>(${p.shift_code})</small>`
                    : "";
              const groupBadge = p.is_group
                ? `<small class="worker-group-badge">Team ${escapeHtml(p.group_code || "-")}</small>`
                : "";
              return `<label class="tt-checkbox"><input type="checkbox" data-eid="${p.employee_id}" data-shift="${p.shift_code}" data-day="${meta.day}" ${checked} /><span class="worker-pref-chip"><span class="worker-name-icon">${initials}</span><span class="worker-shift-meta"><span class="worker-name-text">${escapeHtml(p.employee_name || "-")}</span>${groupBadge}${timeText ? `<small>${timeText}</small>` : ""}</span></span>${tag}</label>`;
            })
            .join("");
        }
        return `<td class="day-cell day-${meta.day} ${cellClass}"><div class="tt-content">${content}</div></td>`;
      })
      .join("");

    row.innerHTML = `<th class="week-shift-col">${shiftRowHourLabel(shift.code)}</th>${cells}`;
    prefBody.appendChild(row);
  });
  prefBox.appendChild(prefBoard);

  const scheduleBox = $("#manager-schedule");
  scheduleBox.innerHTML = "";

  const scheduleBoard = document.createElement("div");
  scheduleBoard.className = "week-table-wrap compact-week-table-wrap";
  const assignedHeaderCells = weekHeaderCellsHtmlFor(assignedDays);
  scheduleBoard.innerHTML = `
    <table class="week-table compact-week-table">
      <thead>
        <tr>
          <th class="week-shift-col">Khung giờ</th>
          ${assignedHeaderCells}
        </tr>
      </thead>
      <tbody id="manager-schedule-week-body"></tbody>
    </table>
  `;
  const scheduleBody = scheduleBoard.querySelector("#manager-schedule-week-body");

  smallShifts.forEach((shift, shiftIndex) => {
    const row = document.createElement("tr");
    row.className = `shift-row shift-idx-${shiftIndex % 6}`;
    const cells = assignedDays
      .map((meta) => {
        const rows = collectAnchoredRows(schedule, shift.code, meta.day);
        const rule = managerStaffingRules.get(shift.code) || { min_staff: 3, max_staff: 4 };
        const count = rows.length;
        const outOfRange = count > 0 && (count < Number(rule.min_staff) || count > Number(rule.max_staff));
        const cellClass = count === 0 ? "is-empty" : outOfRange ? "is-out-range" : "is-in-range";
        const groupedRows = groupScheduleRowsForDisplay(rows);
        const content = rows.length
          ? groupedRows.map((items) => compactGroupedShiftTag(items)).join("")
          : `<span class="tt-empty">Trống</span>`;
        return `<td class="day-cell day-${meta.day} ${cellClass}"><div class="tt-content">${content}</div><small class="muted">${count} người</small></td>`;
      })
      .join("");

    row.innerHTML = `<th class="week-shift-col">${shiftRowHourLabel(shift.code)}</th>${cells}`;
    scheduleBody.appendChild(row);
  });
  scheduleBox.appendChild(scheduleBoard);
  initManagerScheduleTabs();
  updateManagerScheduleSummary();
}

async function saveManagerSchedule() {
  const assignments = [...document.querySelectorAll("#manager-preferences input:checked")].map((el) => ({
    employee_id: Number(el.dataset.eid),
    shift_code: el.dataset.shift,
    day_of_week: Number(el.dataset.day),
  }));
  const payload = await api("/api/manager/schedule", {
    method: "PUT",
    body: JSON.stringify({
      week_start: currentWeek(),
      assignments,
      schedule_revision: managerScheduleUiState.scheduleRevision,
    }),
  });
  managerScheduleUiState.scheduleRevision = String(payload?.schedule_revision || managerScheduleUiState.scheduleRevision || "");
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

async function renderManagerIssueReplies(issueId) {
  const listNode = document.querySelector(`div[data-issue-replies-list='${issueId}']`);
  if (!listNode) return;
  const replies = await api(`/api/manager/issues/${issueId}/replies`);
  listNode.innerHTML = "";
  if (!replies.length) {
    listNode.innerHTML = "<div class='list-item'><small>Chua co phan hoi nao.</small></div>";
    return;
  }
  replies.forEach((reply) => {
    const row = document.createElement("div");
    row.className = "list-item";
    row.innerHTML = `<span><strong>${escapeHtml(reply.sender_name || "Không rõ")}</strong> (${escapeHtml(reply.sender_role || "-")})<br /><small>${escapeHtml(reply.message || "")}</small></span><small>${escapeHtml(reply.created_at || "")}</small>`;
    listNode.appendChild(row);
  });
}

async function renderCeoIssueReplies(issueId) {
  const listNode = document.querySelector(`div[data-ceo-issue-replies-list='${issueId}']`);
  if (!listNode) return;
  const replies = await api(`/api/ceo/issues/${issueId}/replies`);
  listNode.innerHTML = "";
  if (!replies.length) {
    listNode.innerHTML = "<div class='list-item'><small>Chua co phan hoi nao.</small></div>";
    return;
  }
  replies.forEach((reply) => {
    const row = document.createElement("div");
    row.className = "list-item";
    row.innerHTML = `<span><strong>${escapeHtml(reply.sender_name || "Không rõ")}</strong> (${escapeHtml(reply.sender_role || "-")})<br /><small>${escapeHtml(reply.message || "")}</small></span><small>${escapeHtml(reply.created_at || "")}</small>`;
    listNode.appendChild(row);
  });
}

async function loadManagerIssues() {
  const items = await api("/api/manager/issues");
  const list = $("#manager-issues-list");
  list.innerHTML = "";

  items.forEach((item) => {
    const safeTitle = escapeHtml(item.title || "-");
    const safeReporter = escapeHtml(item.reporter_name || "-");
    const safeBranch = escapeHtml(item.branch_name || "-");
    const safeStatus = escapeHtml(issueStatusLabel(item.status || "open"));
    const safeDetails = escapeHtml(item.details || "-");
    const safeManagerNote = escapeHtml(item.manager_note || "");
    const isManagerReport = item.reporter_role === "manager";
    const row = document.createElement("div");
    row.className = "list-item";
    row.innerHTML = `
      <div>
        <strong>${safeTitle}</strong><br />
        <small>${safeReporter} | ${safeBranch} | ${safeStatus}${isManagerReport ? " | Bao cao quan ly" : ""}</small>
      </div>
      <div class="row compact">
        <button class="ghost" data-issue-detail-toggle="${item.id}">Xem noi dung</button>
        <button class="ghost" data-issue-replies-toggle="${item.id}">Phan hoi</button>
        <select data-status="${item.id}">
          <option value="open" ${item.status === "open" ? "selected" : ""}>Mới</option>
          <option value="in_review" ${item.status === "in_review" ? "selected" : ""}>Đang xem xét</option>
          <option value="resolved" ${item.status === "resolved" ? "selected" : ""}>Đã xử lý</option>
          <option value="escalated" ${item.status === "escalated" ? "selected" : ""}>Đã chuyển cấp</option>
        </select>
        <label><input type="checkbox" data-escalate="${item.id}" ${item.escalated_to_ceo ? "checked" : ""}/> Chuyển CEO</label>
        <input type="text" data-note="${item.id}" placeholder="Ghi chú" value="${safeManagerNote}" />
        <button data-save-issue="${item.id}">Lưu</button>
      </div>
      <div class="issue-detail-box hidden" data-issue-detail="${item.id}">
        <p><strong>Noi dung bao cao:</strong> ${safeDetails}</p>
        <p><strong>Ghi chu quan ly:</strong> ${safeManagerNote || "Chua co"}</p>
      </div>
      <div class="issue-detail-box hidden" data-issue-replies="${item.id}">
        <div class="list" data-issue-replies-list="${item.id}"></div>
        <div class="row compact">
          <input type="text" data-issue-reply-input="${item.id}" placeholder="Nhap phan hoi gui CEO/quan ly" />
          <button class="ghost" data-issue-reply-send="${item.id}">Gui phan hoi</button>
        </div>
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
      const openDetailIds = [...document.querySelectorAll("div[data-issue-detail]:not(.hidden)")]
        .map((node) => Number(node.dataset.issueDetail))
        .filter((value) => Number.isFinite(value));
      const openReplyIds = [...document.querySelectorAll("div[data-issue-replies]:not(.hidden)")]
        .map((node) => Number(node.dataset.issueReplies))
        .filter((value) => Number.isFinite(value));
      await api(`/api/manager/issues/${id}`, {
        method: "PUT",
        body: JSON.stringify({ status, escalate_to_ceo, manager_note }),
      });
      showToast("Da cap nhat bao cao");
      await loadManagerIssues();
      openDetailIds.forEach((issueId) => {
        const detail = document.querySelector(`div[data-issue-detail='${issueId}']`);
        const toggle = document.querySelector(`button[data-issue-detail-toggle='${issueId}']`);
        if (detail && detail.classList.contains("hidden")) {
          detail.classList.remove("hidden");
        }
        if (toggle && detail) {
          toggle.textContent = detail.classList.contains("hidden") ? "Xem noi dung" : "An noi dung";
        }
      });
      for (const issueId of openReplyIds) {
        const box = document.querySelector(`div[data-issue-replies='${issueId}']`);
        if (box && box.classList.contains("hidden")) {
          box.classList.remove("hidden");
        }
        await renderManagerIssueReplies(issueId);
      }
    });
  });

  document.querySelectorAll("button[data-issue-replies-toggle]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = Number(btn.dataset.issueRepliesToggle);
      const box = document.querySelector(`div[data-issue-replies='${id}']`);
      if (!box) return;
      box.classList.toggle("hidden");
      if (!box.classList.contains("hidden")) {
        await renderManagerIssueReplies(id);
      }
    });
  });

  document.querySelectorAll("button[data-issue-reply-send]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = Number(btn.dataset.issueReplySend);
      const input = document.querySelector(`input[data-issue-reply-input='${id}']`);
      const message = input?.value?.trim() || "";
      if (!message) {
        showToast("Vui long nhap noi dung phan hoi", true);
        return;
      }
      await api(`/api/manager/issues/${id}/replies`, {
        method: "POST",
        body: JSON.stringify({ message }),
      });
      if (input) input.value = "";
      showToast("Da gui phan hoi");
      await renderManagerIssueReplies(id);
    });
  });
}

async function submitManagerReportToCeo() {
  const title = $("#manager-ceo-report-title").value.trim();
  const details = $("#manager-ceo-report-details").value.trim();
  await api("/api/manager/issues/report-ceo", {
    method: "POST",
    body: JSON.stringify({ title, details }),
  });
  $("#manager-ceo-report-title").value = "";
  $("#manager-ceo-report-details").value = "";
  showToast("Da gui bao cao len CEO");
  await loadManagerIssues();
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
  clearManagerEmployeeAvatarObjectUrls();
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
          <small>Chi nhánh: ${escapeHtml(branchNames || "-")}</small><br />
          <small>Avatar: yeu cau anh the 4x3</small>
        </div>
      </div>
      <div class="row compact">
        <button class="danger" data-del-emp="${e.id}">Xóa</button>
      </div>
    `;
    list.appendChild(row);
  });

  await hydrateManagerEmployeeAvatars();
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

function clearManagerEmployeeAvatarObjectUrls() {
  Object.values(state.managerEmployeeAvatarObjectUrls || {}).forEach((url) => {
    try {
      URL.revokeObjectURL(url);
    } catch {
      // Ignore revoke errors for already-collected object URLs.
    }
  });
  state.managerEmployeeAvatarObjectUrls = {};
}

async function hydrateManagerEmployeeAvatars() {
  const nodes = [...document.querySelectorAll(".manager-employee-avatar[data-avatar-url][data-employee-id]")];
  if (!nodes.length) return;

  await Promise.all(
    nodes.map(async (node) => {
      const avatarUrl = String(node.dataset.avatarUrl || "").trim();
      const employeeId = Number(node.dataset.employeeId || 0);
      if (!avatarUrl || !employeeId) return;

      const headers = {};
      if (state.token) {
        headers.Authorization = `Bearer ${state.token}`;
      }

      try {
        const response = await fetch(avatarUrl, {
          headers,
          cache: "no-store",
        });
        if (!response.ok) return;
        const blob = await response.blob();
        const objectUrl = URL.createObjectURL(blob);

        const previousUrl = state.managerEmployeeAvatarObjectUrls[employeeId];
        if (previousUrl && previousUrl !== objectUrl) {
          try {
            URL.revokeObjectURL(previousUrl);
          } catch {
            // Ignore revoke errors for stale object URLs.
          }
        }

        state.managerEmployeeAvatarObjectUrls[employeeId] = objectUrl;
        node.style.backgroundImage = `url(${objectUrl})`;
        node.classList.add("has-image");
        node.textContent = "";
      } catch {
        // Keep initials fallback when avatar cannot be loaded.
      }
    })
  );
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

  const avatarUrl = String(employee.avatar_url || "").trim();
  if (!avatarUrl) {
    return `<div class="manager-employee-avatar" aria-hidden="true">${escapeHtml(initials)}</div>`;
  }

  return `<div class="manager-employee-avatar" data-avatar-url="${escapeHtml(avatarUrl)}" data-employee-id="${Number(employee.id || 0)}" aria-hidden="true">${escapeHtml(initials)}</div>`;
}

async function loadCeoChat() {
  const items = await api("/api/ceo/chat");
  const log = $("#ceo-chat-log");
  log.innerHTML = "";
  items.forEach((m) => {
    const row = document.createElement("div");
    const typeClass = m.sender_type === "jarvis" ? "jarvis" : "user";
    const safeMessage = toSafeChatHtml((m.message || "").trim());
    const safeSenderName = escapeHtml(m.sender_name || "Không rõ");
    const safeCreatedAt = escapeHtml(m.created_at || "-");
    row.className = `chat-message ${typeClass}`;
    row.innerHTML = `
      <div class="chat-header"><span>${safeSenderName}</span><span>${safeCreatedAt}</span></div>
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
    const safeTitle = escapeHtml(item.title || "-");
    const safeBranch = escapeHtml(item.branch_name || "-");
    const safeReporter = escapeHtml(item.reporter_name || "-");
    const safeStatus = escapeHtml(issueStatusLabel(item.status || "-"));
    const safeDetails = escapeHtml(item.details || "-");
    const safeManagerNote = escapeHtml(item.manager_note || "");
    const row = document.createElement("div");
    row.className = "list-item";
    row.innerHTML = `
      <span>
        <strong>${safeTitle}</strong><br /><small>${safeBranch} | ${safeReporter} | ${safeStatus}</small>
      </span>
      <div class="row compact">
        <button class="ghost" data-ceo-issue-detail-toggle="${item.id}">Xem noi dung</button>
        <button class="ghost" data-ceo-issue-replies-toggle="${item.id}">Phan hoi</button>
      </div>
      <div class="issue-detail-box hidden" data-ceo-issue-detail="${item.id}">
        <p><strong>Noi dung bao cao:</strong> ${safeDetails}</p>
        <p><strong>Ghi chu quan ly:</strong> ${safeManagerNote || "Chua co"}</p>
      </div>
      <div class="issue-detail-box hidden" data-ceo-issue-replies="${item.id}">
        <div class="list" data-ceo-issue-replies-list="${item.id}"></div>
        <div class="row compact">
          <input type="text" data-ceo-issue-reply-input="${item.id}" placeholder="Nhap phan hoi cho quan ly" />
          <button class="ghost" data-ceo-issue-reply-send="${item.id}">Gui phan hoi</button>
        </div>
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

  document.querySelectorAll("button[data-ceo-issue-replies-toggle]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = Number(btn.dataset.ceoIssueRepliesToggle);
      const box = document.querySelector(`div[data-ceo-issue-replies='${id}']`);
      if (!box) return;
      box.classList.toggle("hidden");
      if (!box.classList.contains("hidden")) {
        await renderCeoIssueReplies(id);
      }
    });
  });

  document.querySelectorAll("button[data-ceo-issue-reply-send]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const id = Number(btn.dataset.ceoIssueReplySend);
      const input = document.querySelector(`input[data-ceo-issue-reply-input='${id}']`);
      const message = input?.value?.trim() || "";
      if (!message) {
        showToast("Vui long nhap noi dung phan hoi", true);
        return;
      }
      await api(`/api/ceo/issues/${id}/replies`, {
        method: "POST",
        body: JSON.stringify({ message }),
      });
      if (input) input.value = "";
      showToast("Da gui phan hoi");
      await renderCeoIssueReplies(id);
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

function auditActionLabel(action) {
  if (action === "branch.create") return "Tạo chi nhánh";
  if (action === "branch.update") return "Cập nhật chi nhánh";
  if (action === "branch.delete") return "Xóa chi nhánh";
  return action || "Thao tác";
}

function prettifyLegacyBranchAuditDetail(action, rawDetails) {
  const details = String(rawDetails || "").trim();
  if (!details) return "";

  if (action === "branch.create") {
    const created = details.match(/^Created branch:\s*(.*?)\s*\|\s*location:\s*(.*)$/i);
    if (created) {
      const name = created[1] || "chua cap nhat";
      const location = created[2] || "chua cap nhat";
      return `Da tao chi nhanh "${name}". Dia diem: ${location}.`;
    }
  }

  if (action === "branch.delete") {
    const deleted = details.match(/^Deleted branch:\s*(.*)$/i);
    if (deleted) {
      const name = deleted[1] || "chua cap nhat";
      return `Da xoa chi nhanh "${name}".`;
    }
  }

  if (action === "branch.update") {
    const updated = details.match(
      /^Updated branch from name='([^']*)',\s*location='([^']*)'\s*to name='([^']*)',\s*location='([^']*)',\s*network_ip='([^']*)'$/i,
    );
    if (updated) {
      const oldName = updated[1] || "chua cap nhat";
      const oldLocation = updated[2] || "chua cap nhat";
      const newName = updated[3] || "chua cap nhat";
      const newLocation = updated[4] || "chua cap nhat";
      return `Cap nhat chi nhanh "${newName}": doi ten tu "${oldName}" sang "${newName}". Cap nhat dia diem tu "${oldLocation}" sang "${newLocation}".`;
    }
  }

  return details;
}

function toAuditDetailHtml(details) {
  return escapeHtml(details || "").replaceAll("\n", "<br />");
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
    const readableDetails = prettifyLegacyBranchAuditDetail(item.action, item.details);
    const row = document.createElement("div");
    row.className = "list-item audit-item";
    row.innerHTML = `
      <div>
        <strong>${auditActionLabel(item.action)}</strong><br />
        <small>${toAuditDetailHtml(readableDetails)}</small>
      </div>
      <div>
        <small>${escapeHtml(item.actor_username || "system")}<br />${formatDateTimeDisplay(item.created_at)}</small>
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
        if (!name) {
          showToast("Tên chi nhánh không được để trống", true);
          return;
        }
        await api(`/api/admin/branches/${branchId}`, {
          method: "PUT",
          body: JSON.stringify({ name, location }),
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
  await refreshBranchesMeta({ force: true });
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
  await api("/api/admin/branches", {
    method: "POST",
    body: JSON.stringify({ name, location }),
  });
  $("#ceo-branch-new-name").value = "";
  $("#ceo-branch-new-location").value = "";
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
  clearManagerEmployeeAvatarObjectUrls();
  state.token = null;
  state.currentUser = null;
  state.managerDailyQr = null;
  stopEmployeeQrCameraScanner();
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
  $("#btn-open-profile").addEventListener("click", () =>
    openProfileModal().catch((e) => showToast(e.message, true))
  );
  $("#btn-save-profile-required").addEventListener("click", () =>
    submitRequiredProfile().catch((e) => showToast(e.message, true))
  );
  $("#btn-close-profile-modal").addEventListener("click", closeProfileModal);
  $("#profile-avatar-input").addEventListener("change", async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
      const preparedDataUrl = await prepareProfileAvatarDataUrl(file);
      state.profileAvatarDataUrl = preparedDataUrl;
      setProfileAvatarPreview(state.profileAvatarDataUrl);
    } catch (error) {
      event.target.value = "";
      showToast(error.message || "Khong the tai anh len", true);
    }
  });
  $("#btn-week-refresh").addEventListener("click", () => renderRoute().catch((e) => showToast(e.message, true)));

  $("#btn-check-in-one-time").addEventListener("click", () =>
    checkInEmployeeOneTime().catch((e) => showToast(e.message, true))
  );
  $("#btn-scan-one-time-qr").addEventListener("click", () =>
    scanEmployeeOneTimeQr().catch((e) => showToast(e.message, true))
  );
  $("#btn-open-employee-qr-camera").addEventListener("click", () =>
    openEmployeeQrCameraScanner().catch((e) => showToast(e.message, true))
  );
  $("#btn-scan-employee-qr-image").addEventListener("click", () => {
    $("#employee-qr-image-input").click();
  });
  $("#employee-qr-image-input").addEventListener("change", (event) => {
    scanEmployeeOneTimeQrFromImage(event).catch((e) => showToast(e.message, true));
  });
  $("#btn-close-employee-qr-camera").addEventListener("click", stopEmployeeQrCameraScanner);
  $("#employee-qr-camera-modal").addEventListener("click", (event) => {
    if (event.target?.id === "employee-qr-camera-modal") {
      stopEmployeeQrCameraScanner();
    }
  });
  $("#employee-one-time-random-key").addEventListener("input", () => {
    updateEmployeeOneTimeCheckInButtonState();
  });
  $("#employee-one-time-qr-payload").addEventListener("input", () => {
    if (!$("#employee-one-time-qr-payload").value.trim()) {
      state.oneTimeScan = null;
      stopEmployeeOneTimeCountdown();
    }
    updateEmployeeOneTimeCheckInButtonState();
  });
  $("#btn-check-out").addEventListener("click", () => checkOutEmployee().catch((e) => showToast(e.message, true)));
  $("#btn-confirm-attendance").addEventListener("click", () =>
    confirmAttendanceEmployee().catch((e) => showToast(e.message, true))
  );
  $("#btn-save-employee-shifts").addEventListener("click", () =>
    withButtonLocks("#btn-save-employee-shifts", () => saveEmployeeShifts(), { loadingText: "Đang lưu..." }).catch(
      (e) => showToast(e.message, true)
    )
  );
  $("#employee-registration-type").addEventListener("change", updateEmployeeRegistrationUiState);
  $("#btn-open-flex-editor").addEventListener("click", () => {
    state.employeeFlexEditorDismissed = false;
    saveEmployeeFlexEditorDismissed();
    renderEmployeeFlexEditor();
  });
  $("#btn-save-flex-hours-draft").addEventListener("click", () => {
    saveEmployeeFlexDraft();
    state.employeeFlexEditorDismissed = true;
    saveEmployeeFlexEditorDismissed();
    renderEmployeeFlexEditor();
    showToast("Đã lưu giờ FLEX tạm");
  });
  $("#btn-close-flex-editor").addEventListener("click", () => {
    state.employeeFlexEditorDismissed = true;
    saveEmployeeFlexEditorDismissed();
    renderEmployeeFlexEditor();
  });
  $("#employee-flex-modal-backdrop").addEventListener("click", () => {
    state.employeeFlexEditorDismissed = true;
    saveEmployeeFlexEditorDismissed();
    renderEmployeeFlexEditor();
  });
  $("#employee-shift-grid").addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.matches("input[type='checkbox'][data-shift]")) {
      if (target.dataset.shift === "FLEX" && target.checked) {
        state.employeeFlexEditorDismissed = false;
        saveEmployeeFlexEditorDismissed();
      }
      renderEmployeeFlexEditor();
      updateEmployeeShiftSummary();
    }
  });
  $("#manager-self-shifts-list").addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.matches("input[type='checkbox'][data-shift]")) {
      if (target.dataset.shift === "FLEX" && target.checked) {
        state.managerSelfFlexEditorDismissed = false;
        saveManagerFlexEditorDismissed();
      }
      renderManagerSelfFlexEditor();
      updateManagerSelfShiftSummary();
    }
  });
  $("#btn-open-manager-self-flex-editor").addEventListener("click", () => {
    state.managerSelfFlexEditorDismissed = false;
    saveManagerFlexEditorDismissed();
    renderManagerSelfFlexEditor();
  });
  $("#btn-save-manager-flex-hours-draft").addEventListener("click", () => {
    saveManagerSelfFlexDraft();
    state.managerSelfFlexEditorDismissed = true;
    saveManagerFlexEditorDismissed();
    renderManagerSelfFlexEditor();
    showToast("Đã lưu giờ FLEX tạm của quản lý");
  });
  $("#btn-close-manager-self-flex-editor").addEventListener("click", () => {
    state.managerSelfFlexEditorDismissed = true;
    saveManagerFlexEditorDismissed();
    renderManagerSelfFlexEditor();
  });
  $("#manager-self-flex-modal-backdrop").addEventListener("click", () => {
    state.managerSelfFlexEditorDismissed = true;
    saveManagerFlexEditorDismissed();
    renderManagerSelfFlexEditor();
  });
  $("#manager-preferences").addEventListener("change", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.matches("input[type='checkbox'][data-eid]")) {
      updateManagerScheduleSummary();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !$("#employee-flex-editor-wrap")?.classList.contains("hidden")) {
      state.employeeFlexEditorDismissed = true;
      saveEmployeeFlexEditorDismissed();
      renderEmployeeFlexEditor();
    }
    if (event.key === "Escape" && !$("#manager-self-flex-editor-wrap")?.classList.contains("hidden")) {
      state.managerSelfFlexEditorDismissed = true;
      saveManagerFlexEditorDismissed();
      renderManagerSelfFlexEditor();
    }
  });
  $("#btn-employee-join-group").addEventListener("click", () =>
    joinEmployeeGroup().catch((e) => showToast(e.message, true))
  );
  $("#employee-group-branch").addEventListener("change", () =>
    loadEmployeeRegistrationGroups().catch((e) => showToast(e.message, true))
  );
  $("#btn-submit-employee-issue").addEventListener("click", () => submitIssue().catch((e) => showToast(e.message, true)));
  $("#employee-assigned-branch-filter").addEventListener("change", () =>
    loadEmployeeAssignedSchedule().catch((e) => showToast(e.message, true))
  );
  $("#employee-issue-branch-filter").addEventListener("change", () =>
    loadMyIssues().catch((e) => showToast(e.message, true))
  );

  $("#btn-manager-check-in").addEventListener("click", () => checkInManager().catch((e) => showToast(e.message, true)));
  $("#btn-manager-confirm-attendance").addEventListener("click", () =>
    confirmAttendanceManager().catch((e) => showToast(e.message, true))
  );
  $("#btn-manager-check-out").addEventListener("click", () => checkOutManager().catch((e) => showToast(e.message, true)));
  $("#btn-manager-generate-one-time-qr").addEventListener("click", () =>
    generateManagerOneTimeQr().catch((e) => showToast(e.message, true))
  );
  $("#btn-save-manager-self-shifts").addEventListener("click", () =>
    withButtonLocks("#btn-save-manager-self-shifts", () => saveManagerSelfShifts(), {
      loadingText: "Đang lưu...",
    }).catch((e) => showToast(e.message, true))
  );
  $("#manager-schedule-day-filter").addEventListener("change", (event) => {
    managerScheduleUiState.selectedDay = Number(event.target.value || 0);
    loadManagerSchedule().catch((e) => showToast(e.message, true));
  });
  $("#manager-assigned-day-filter").addEventListener("change", (event) => {
    managerScheduleUiState.assignedSelectedDay = Number(event.target.value || 0);
    loadManagerSchedule().catch((e) => showToast(e.message, true));
  });
  const managerScheduleSaveHandler = () =>
    withButtonLocks(
      ["#btn-save-manager-schedule", "#btn-save-manager-schedule-inline"],
      () => saveManagerSchedule(),
      { loadingText: "Đang lưu..." }
    ).catch((e) => showToast(e.message, true));
  $("#btn-save-manager-schedule").addEventListener("click", managerScheduleSaveHandler);
  $("#btn-save-manager-schedule-inline").addEventListener("click", managerScheduleSaveHandler);
  $("#btn-save-manager-staffing-rules").addEventListener("click", () =>
    withButtonLocks("#btn-save-manager-staffing-rules", () => saveManagerStaffingRules(), {
      loadingText: "Đang lưu...",
    }).catch((e) => showToast(e.message, true))
  );
  $("#btn-manager-create-group").addEventListener("click", () =>
    createManagerRegistrationGroup().catch((e) => showToast(e.message, true))
  );
  $("#btn-submit-manager-ceo-report").addEventListener("click", () =>
    submitManagerReportToCeo().catch((e) => showToast(e.message, true))
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
  window.addEventListener("resize", scheduleApplySidebarState);
}

async function bootstrap() {
  const meta = await api("/api/meta");
  state.shifts = meta.shifts;
  state.shiftByCode = new Map((state.shifts || []).map((shift) => [String(shift.code || "").toUpperCase(), shift]));
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

function initPwaUiMode() {
  const inStandaloneMode =
    window.matchMedia("(display-mode: standalone)").matches ||
    window.matchMedia("(display-mode: fullscreen)").matches ||
    window.navigator.standalone === true;

  document.body.classList.toggle("pwa-mode", inStandaloneMode);

  window.matchMedia("(display-mode: standalone)").addEventListener?.("change", (event) => {
    document.body.classList.toggle("pwa-mode", event.matches);
  });
}

function clampCompactButtonPosition(btn, x, y) {
  const rect = btn.getBoundingClientRect();
  const width = Math.max(40, Math.round(rect.width || 48));
  const height = Math.max(40, Math.round(rect.height || 48));
  const margin = 8;
  const maxX = Math.max(margin, window.innerWidth - width - margin);
  const maxY = Math.max(margin, window.innerHeight - height - margin);
  return {
    x: Math.min(Math.max(margin, Math.round(x)), maxX),
    y: Math.min(Math.max(margin, Math.round(y)), maxY),
  };
}

function applyCompactButtonPosition(btn, position, { save = false } = {}) {
  if (!position || !Number.isFinite(position.x) || !Number.isFinite(position.y)) {
    btn.classList.remove("is-positioned");
    btn.style.left = "";
    btn.style.top = "";
    btn.style.right = "";
    btn.style.bottom = "";
    localStorage.removeItem("wm_compact_btn_pos");
    updateCompactButtonToastAvoidance(btn);
    return;
  }

  const clamped = clampCompactButtonPosition(btn, position.x, position.y);
  btn.classList.add("is-positioned");
  btn.style.left = `${clamped.x}px`;
  btn.style.top = `${clamped.y}px`;
  btn.style.right = "auto";
  btn.style.bottom = "auto";
  if (save) {
    localStorage.setItem("wm_compact_btn_pos", JSON.stringify(clamped));
  }
  updateCompactButtonToastAvoidance(btn);
}

function updateCompactButtonToastAvoidance(btn) {
  const rect = btn.getBoundingClientRect();
  const nearBottom = window.innerHeight - rect.bottom <= 96;
  const nearRight = window.innerWidth - rect.right <= 196;
  document.body.classList.toggle("fab-over-toast", nearBottom && nearRight);
}

function initCompactButtonDrag(btn) {
  const raw = localStorage.getItem("wm_compact_btn_pos");
  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      applyCompactButtonPosition(btn, parsed, { save: false });
    } catch {
      localStorage.removeItem("wm_compact_btn_pos");
    }
  } else {
    updateCompactButtonToastAvoidance(btn);
  }

  let pointerId = null;
  let startX = 0;
  let startY = 0;
  let originX = 0;
  let originY = 0;
  let moved = false;

  btn.addEventListener("pointerdown", (event) => {
    if (event.button !== 0) return;
    pointerId = event.pointerId;
    btn.setPointerCapture(pointerId);
    const rect = btn.getBoundingClientRect();
    startX = event.clientX;
    startY = event.clientY;
    originX = rect.left;
    originY = rect.top;
    moved = false;
    btn.classList.remove("dragging");
  });

  btn.addEventListener("pointermove", (event) => {
    if (pointerId == null || event.pointerId !== pointerId) return;
    const dx = event.clientX - startX;
    const dy = event.clientY - startY;
    if (!moved && Math.hypot(dx, dy) >= 8) {
      moved = true;
      btn.classList.add("dragging");
    }
    if (!moved) return;
    applyCompactButtonPosition(
      btn,
      {
        x: originX + dx,
        y: originY + dy,
      },
      { save: false }
    );
  });

  const endDrag = (event) => {
    if (pointerId == null || event.pointerId !== pointerId) return;
    if (btn.hasPointerCapture(pointerId)) {
      btn.releasePointerCapture(pointerId);
    }
    pointerId = null;
    btn.classList.remove("dragging");
    if (moved) {
      btn.dataset.dragMoved = "1";
      const rect = btn.getBoundingClientRect();
      applyCompactButtonPosition(btn, { x: rect.left, y: rect.top }, { save: true });
      window.setTimeout(() => {
        btn.dataset.dragMoved = "";
      }, 180);
    }
  };

  btn.addEventListener("pointerup", endDrag);
  btn.addEventListener("pointercancel", endDrag);

  btn.addEventListener("dblclick", () => {
    applyCompactButtonPosition(btn, null);
    showToast("Đã đưa nút về vị trí mặc định");
  });

  window.addEventListener("resize", () => {
    const rawPos = localStorage.getItem("wm_compact_btn_pos");
    if (!rawPos) {
      updateCompactButtonToastAvoidance(btn);
      return;
    }
    try {
      const parsed = JSON.parse(rawPos);
      applyCompactButtonPosition(btn, parsed, { save: true });
    } catch {
      localStorage.removeItem("wm_compact_btn_pos");
      updateCompactButtonToastAvoidance(btn);
    }
  });
}

// ===== COMPACT MODE TOGGLE =====
function initCompactMode() {
  const isCompactMode = localStorage.getItem("wm_compact_mode") === "1";
  if (isCompactMode) {
    document.body.classList.add("compact-mode");
  }

  const btn = $("#btn-compact-mode");
  if (!btn) return;

  initCompactButtonDrag(btn);

  btn.addEventListener("click", () => {
    if (btn.dataset.dragMoved === "1") {
      return;
    }
    document.body.classList.toggle("compact-mode");
    const isNowCompact = document.body.classList.contains("compact-mode");
    localStorage.setItem("wm_compact_mode", isNowCompact ? "1" : "0");
    btn.title = isNowCompact ? "Tắt chế độ tối giản" : "Bật chế độ tối giản";
  });

  btn.title = isCompactMode ? "Tắt chế độ tối giản" : "Bật chế độ tối giản";
}

attachEvents();
initCompactMode();
initPwaUiMode();
registerServiceWorker();
bootstrap().catch((e) => showToast(e.message, true));
