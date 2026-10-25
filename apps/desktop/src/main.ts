import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

type EngineEvent =
  | { kind: "uploaded"; key: string }
  | { kind: "downloaded"; key: string }
  | { kind: "pendingDelete"; key: string; originatedRemotely: boolean }
  | { kind: "error"; message: string };

const onboardingScreen = document.querySelector<HTMLElement>("#onboarding")!;
const statusScreen = document.querySelector<HTMLElement>("#status")!;
const onboardingForm = document.querySelector<HTMLFormElement>("#onboarding-form")!;
const onboardingError = document.querySelector<HTMLElement>("#onboarding-error")!;
const activityLog = document.querySelector<HTMLUListElement>("#activity-log")!;
const pendingDeletesEl = document.querySelector<HTMLElement>("#pending-deletes")!;
const openFolderBtn = document.querySelector<HTMLButtonElement>("#open-folder")!;
const signOutBtn = document.querySelector<HTMLButtonElement>("#sign-out")!;

const MAX_LOG_ENTRIES = 50;

function showScreen(screen: "onboarding" | "status") {
  onboardingScreen.hidden = screen !== "onboarding";
  statusScreen.hidden = screen !== "status";
}

function logLine(text: string) {
  const item = document.createElement("li");
  item.textContent = `${new Date().toLocaleTimeString()} — ${text}`;
  activityLog.prepend(item);
  while (activityLog.childElementCount > MAX_LOG_ENTRIES) {
    activityLog.lastElementChild?.remove();
  }
}

function renderPendingDelete(key: string, originatedRemotely: boolean) {
  if (document.getElementById(`pending-${key}`)) return;

  const row = document.createElement("div");
  row.id = `pending-${key}`;
  row.className = "pending-delete";

  const label = document.createElement("span");
  label.textContent = originatedRemotely
    ? `"${key}" was deleted remotely — remove it locally too?`
    : `"${key}" was deleted locally — remove it from Buttercloud too?`;

  const confirmBtn = document.createElement("button");
  confirmBtn.textContent = "Confirm delete";
  confirmBtn.onclick = async () => {
    await invoke("confirm_delete", { key, originatedRemotely });
    row.remove();
    logLine(`Deleted "${key}"`);
  };

  const cancelBtn = document.createElement("button");
  cancelBtn.textContent = "Keep it";
  cancelBtn.className = "secondary";
  cancelBtn.onclick = async () => {
    await invoke("cancel_delete", { key });
    row.remove();
  };

  row.append(label, confirmBtn, cancelBtn);
  pendingDeletesEl.append(row);
}

function handleEngineEvent(event: EngineEvent) {
  switch (event.kind) {
    case "uploaded":
      logLine(`Uploaded "${event.key}"`);
      break;
    case "downloaded":
      logLine(`Downloaded "${event.key}"`);
      break;
    case "pendingDelete":
      renderPendingDelete(event.key, event.originatedRemotely);
      break;
    case "error":
      logLine(`Error: ${event.message}`);
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
    await invoke("onboard", { bucket, accessKey, secretKey });
    showScreen("status");
    logLine("Connected to Buttercloud");
  } catch (err) {
    onboardingError.textContent = String(err);
    onboardingError.hidden = false;
  } finally {
    submitBtn.disabled = false;
    submitBtn.textContent = "Connect";
  }
});

openFolderBtn.addEventListener("click", () => invoke("open_sync_folder"));

signOutBtn.addEventListener("click", async () => {
  await invoke("sign_out");
  activityLog.innerHTML = "";
  pendingDeletesEl.innerHTML = "";
  showScreen("onboarding");
});

async function init() {
  await listen<EngineEvent>("engine-event", (e) => handleEngineEvent(e.payload));
  const hasAccount = await invoke<boolean>("has_account");
  showScreen(hasAccount ? "status" : "onboarding");
}

window.addEventListener("DOMContentLoaded", () => {
  init();
});
