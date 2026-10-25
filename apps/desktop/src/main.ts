import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

type EngineEvent =
  | { kind: "uploaded"; key: string }
  | { kind: "downloaded"; key: string }
  | { kind: "pendingDelete"; key: string; originatedRemotely: boolean }
  | { kind: "quotaExceeded" | "accountSuspended" | "invalidCredentials" | "error"; message: string };

const onboardingScreen = document.querySelector<HTMLElement>("#onboarding")!;
const shell = document.querySelector<HTMLElement>("#shell")!;
const onboardingForm = document.querySelector<HTMLFormElement>("#onboarding-form")!;
const onboardingError = document.querySelector<HTMLElement>("#onboarding-error")!;
const activityLog = document.querySelector<HTMLUListElement>("#activity-log")!;
const pendingDeletesEl = document.querySelector<HTMLElement>("#pending-deletes")!;
const accountBanner = document.querySelector<HTMLElement>("#account-banner")!;
const statusDot = document.querySelector<HTMLElement>("#status-dot")!;
const metaFolder = document.querySelector<HTMLElement>("#meta-folder")!;
const metaVersion = document.querySelector<HTMLElement>("#meta-version")!;

const MAX_LOG_ENTRIES = 50;

/// Wraps a promise so it always settles within `ms`, even if whatever it's
/// waiting on (a Rust command, a system dialog) never resolves — a stuck
/// onboarding form with no visible error is worse than a timeout message.
function withTimeout<T>(promise: Promise<T>, ms: number, timeoutMessage: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(timeoutMessage)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err) => {
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}

function showScreen(screen: "onboarding" | "shell") {
  onboardingScreen.hidden = screen !== "onboarding";
  shell.hidden = screen !== "shell";
}

function setupTabs() {
  const tabs = document.querySelectorAll<HTMLButtonElement>(".tab");
  const panels: Record<string, HTMLElement> = {
    status: document.querySelector("#tab-status")!,
    help: document.querySelector("#tab-help")!,
  };
  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      const target = tab.dataset.tab!;
      tabs.forEach((t) => {
        t.classList.toggle("is-active", t === tab);
        t.setAttribute("aria-selected", String(t === tab));
      });
      Object.entries(panels).forEach(([name, el]) => {
        el.hidden = name !== target;
      });
    });
  });
}

function relativeTime(date: Date): string {
  const seconds = Math.round((Date.now() - date.getTime()) / 1000);
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  return `${hours}h ago`;
}

function logActivity(iconClass: string, glyph: string, text: string) {
  const empty = activityLog.querySelector(".empty-state");
  empty?.remove();

  const item = document.createElement("li");
  item.className = "activity-item";

  const icon = document.createElement("span");
  icon.className = `activity-icon ${iconClass}`;
  icon.textContent = glyph;

  const body = document.createElement("span");
  body.className = "activity-text";
  body.textContent = text;

  const time = document.createElement("span");
  time.className = "activity-time";
  time.textContent = relativeTime(new Date());

  item.append(icon, body, time);
  activityLog.prepend(item);
  while (activityLog.querySelectorAll(".activity-item").length > MAX_LOG_ENTRIES) {
    activityLog.querySelector(".activity-item:last-of-type")?.remove();
  }
}

function renderPendingDelete(key: string, originatedRemotely: boolean) {
  if (document.getElementById(`pending-${cssEscape(key)}`)) return;

  const row = document.createElement("div");
  row.id = `pending-${cssEscape(key)}`;
  row.className = "pending-delete";

  const label = document.createElement("span");
  label.style.flex = "1";
  label.textContent = originatedRemotely
    ? `"${key}" was deleted remotely — remove it locally too?`
    : `"${key}" was deleted locally — remove it from Buttercloud too?`;

  const confirmBtn = document.createElement("button");
  confirmBtn.className = "btn btn-primary";
  confirmBtn.textContent = "Confirm delete";
  confirmBtn.onclick = async () => {
    await invoke("confirm_delete", { key, originatedRemotely });
    row.remove();
    logActivity("deleted", "✕", `Deleted "${key}"`);
  };

  const cancelBtn = document.createElement("button");
  cancelBtn.textContent = "Keep it";
  cancelBtn.className = "btn btn-ghost";
  cancelBtn.onclick = async () => {
    await invoke("cancel_delete", { key });
    row.remove();
  };

  row.append(label, confirmBtn, cancelBtn);
  pendingDeletesEl.append(row);
}

function cssEscape(s: string): string {
  return s.replace(/[^a-zA-Z0-9_-]/g, "_");
}

function showBanner(kind: "warn" | "danger", title: string, body: string, withDashboardLink: boolean) {
  accountBanner.hidden = false;
  accountBanner.className = `banner is-${kind}`;
  accountBanner.innerHTML = "";

  const icon = document.createElement("span");
  icon.className = "banner-icon";
  icon.textContent = kind === "danger" ? "⛔" : "⚠️";

  const content = document.createElement("div");
  content.className = "banner-body";
  const strong = document.createElement("strong");
  strong.textContent = title;
  const p = document.createElement("div");
  p.textContent = body;
  content.append(strong, p);

  if (withDashboardLink) {
    const actions = document.createElement("div");
    actions.className = "banner-actions";
    const btn = document.createElement("button");
    btn.className = "btn btn-secondary";
    btn.textContent = "Open usage & billing ↗";
    btn.onclick = () => invoke("open_dashboard");
    actions.append(btn);
    content.append(actions);
  }

  accountBanner.append(icon, content);
}

function clearBanner() {
  accountBanner.hidden = true;
  accountBanner.innerHTML = "";
}

function handleEngineEvent(event: EngineEvent) {
  switch (event.kind) {
    case "uploaded":
      statusDot.classList.remove("is-error");
      logActivity("uploaded", "↑", `Uploaded "${event.key}"`);
      break;
    case "downloaded":
      statusDot.classList.remove("is-error");
      logActivity("downloaded", "↓", `Downloaded "${event.key}"`);
      break;
    case "pendingDelete":
      renderPendingDelete(event.key, event.originatedRemotely);
      break;
    case "quotaExceeded":
      statusDot.classList.add("is-error");
      showBanner("warn", "Storage quota exceeded", "Uploads are paused until you free up space or upgrade your plan.", true);
      logActivity("error", "!", "Storage quota exceeded");
      break;
    case "accountSuspended":
      statusDot.classList.add("is-error");
      showBanner("danger", "Account suspended", "An invoice is overdue. Reads and deletes still work, but new uploads are blocked until it's settled.", true);
      logActivity("error", "!", "Account suspended — invoice overdue");
      break;
    case "invalidCredentials":
      statusDot.classList.add("is-error");
      showBanner("danger", "Credentials rejected", "Sign out and reconnect with a valid access key and secret key.", false);
      logActivity("error", "!", "Credentials rejected");
      break;
    case "error":
      statusDot.classList.add("is-error");
      logActivity("error", "!", event.message);
      break;
  }
}

onboardingForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  onboardingError.hidden = true;

  const bucket = (document.querySelector<HTMLInputElement>("#bucket")!).value.trim();
  const accessKey = (document.querySelector<HTMLInputElement>("#access-key")!).value.trim();
  const secretKey = (document.querySelector<HTMLInputElement>("#secret-key")!).value;

  const submitBtn = onboardingForm.querySelector("button[type=submit]") as HTMLButtonElement;
  submitBtn.disabled = true;
  submitBtn.textContent = "Connecting…";

  try {
    await withTimeout(
      invoke("onboard", { bucket, accessKey, secretKey }),
      20000,
      "Connecting timed out — check your network and try again.",
    );
    showScreen("shell");
    clearBanner();
    logActivity("uploaded", "✓", "Connected to Buttercloud");
    await loadMeta();
  } catch (err) {
    onboardingError.textContent = String(err);
    onboardingError.hidden = false;
  } finally {
    submitBtn.disabled = false;
    submitBtn.textContent = "Connect";
  }
});

document.querySelector("#open-folder")!.addEventListener("click", () => invoke("open_sync_folder"));
document.querySelector("#open-dashboard")!.addEventListener("click", () => invoke("open_dashboard"));
document.querySelector("#open-dashboard-2")!.addEventListener("click", () => invoke("open_dashboard"));
document.querySelector("#repo-link")!.addEventListener("click", (e) => {
  e.preventDefault();
  invoke("open_external", { url: "https://github.com/Mantequilla-Soft/butterdrive" });
});

document.querySelector("#sign-out")!.addEventListener("click", async () => {
  await invoke("sign_out");
  activityLog.innerHTML = '<li class="empty-state">Nothing yet — drop a file in your Buttercloud folder to get started.</li>';
  pendingDeletesEl.innerHTML = "";
  clearBanner();
  showScreen("onboarding");
});

async function loadMeta() {
  try {
    metaFolder.textContent = await invoke<string>("sync_folder_path");
    metaVersion.textContent = `v${await invoke<string>("app_version")}`;
  } catch {
    // non-critical; leave the placeholders
  }
}

async function init() {
  setupTabs();
  await listen<EngineEvent>("engine-event", (e) => handleEngineEvent(e.payload));
  const hasAccount = await invoke<boolean>("has_account");
  showScreen(hasAccount ? "shell" : "onboarding");
  if (hasAccount) await loadMeta();
}

window.addEventListener("DOMContentLoaded", () => {
  init();
});
