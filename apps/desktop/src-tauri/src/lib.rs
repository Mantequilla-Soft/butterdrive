use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use sync_core::client::ButtercloudClient;
use sync_core::config::AccountConfig;
use sync_core::creds;
use sync_core::engine::{EngineEvent, SyncEngine};
use sync_core::error::SyncError;
use sync_core::state::SyncState;

use tauri::menu::{MenuBuilder, MenuItemBuilder};
use tauri::tray::TrayIconBuilder;
use tauri::{AppHandle, Emitter, Manager, State};
use tauri_plugin_notification::NotificationExt;
use tauri_plugin_opener::OpenerExt;

/// How often the engine polls the remote bucket for changes it didn't make
/// itself (there's no webhook/push signal yet — handoff doc, issue #17).
const POLL_INTERVAL: Duration = Duration::from_secs(20);

struct AppState {
    engine: tokio::sync::Mutex<Option<Arc<SyncEngine>>>,
    // Set once from the single `creds::load()` call in `setup`, kept in sync
    // by `onboard`/`sign_out`. `has_account` reads this instead of hitting
    // the OS keychain a second time on every launch — each keychain read is
    // its own macOS authorization prompt on an unsigned dev build.
    has_account: std::sync::atomic::AtomicBool,
    // The currently-running engine's background tasks (its run() loop and
    // its event-forwarding loop). Replacing `engine` above does NOT stop
    // these on its own — an Arc clone is already captured inside each
    // spawned task, independent of what AppState.engine points to — so a
    // reconnect or sign-out must abort these explicitly or the old engine
    // keeps polling with stale credentials forever, its errors mixed into
    // the same event stream the new (or no) engine uses.
    engine_tasks: tokio::sync::Mutex<Vec<tauri::async_runtime::JoinHandle<()>>>,
}

/// Stops whatever engine is currently running (if any) so a fresh one can
/// start, or so nothing keeps running after sign-out.
async fn stop_engine(state: &AppState) {
    *state.engine.lock().await = None;
    for task in state.engine_tasks.lock().await.drain(..) {
        task.abort();
    }
}

fn sync_root(app: &AppHandle) -> PathBuf {
    let base = app
        .path()
        .document_dir()
        .unwrap_or_else(|_| std::env::temp_dir());
    base.join("Buttercloud")
}

fn state_db_path(app: &AppHandle) -> PathBuf {
    let base = app
        .path()
        .app_data_dir()
        .unwrap_or_else(|_| std::env::temp_dir());
    let _ = std::fs::create_dir_all(&base);
    base.join("sync-state.sqlite")
}

fn event_payload(event: &EngineEvent) -> serde_json::Value {
    match event {
        EngineEvent::Uploaded(key) => serde_json::json!({"kind": "uploaded", "key": key}),
        EngineEvent::Downloaded(key) => serde_json::json!({"kind": "downloaded", "key": key}),
        EngineEvent::PendingDelete {
            key,
            originated_remotely,
        } => serde_json::json!({
            "kind": "pendingDelete",
            "key": key,
            "originatedRemotely": originated_remotely,
        }),
        EngineEvent::Error { key, error } => {
            let kind = match error {
                SyncError::QuotaExceeded => "quotaExceeded",
                SyncError::AccountSuspended => "accountSuspended",
                SyncError::AccessKeyNotFound => "accessKeyNotFound",
                SyncError::SignatureMismatch => "signatureMismatch",
                SyncError::Other(_) => "error",
            };
            serde_json::json!({"kind": kind, "message": error.to_string(), "key": key})
        }
    }
}

fn handle_engine_event(app: &AppHandle, event: EngineEvent) {
    eprintln!("[engine-event] {event:?}");
    let _ = app.emit("engine-event", event_payload(&event));
    // Quota/suspension/transport errors and delete confirmations are the
    // moments the user needs to notice even if the status window isn't
    // focused — everything else (uploaded/downloaded) just updates the UI.
    match &event {
        EngineEvent::Error { key, error } => {
            let body = match key {
                Some(key) => format!("\"{key}\": {error}"),
                None => error.to_string(),
            };
            let _ = app
                .notification()
                .builder()
                .title("Buttercloud sync error")
                .body(body)
                .show();
        }
        EngineEvent::PendingDelete { key, .. } => {
            let _ = app
                .notification()
                .builder()
                .title("Buttercloud: delete needs confirmation")
                .body(format!("\"{key}\" was deleted — open Buttercloud to confirm."))
                .show();
        }
        _ => {}
    }
}

/// Starts (or restarts) the sync engine for a saved account and wires its
/// event stream into the UI. Safe to call again after `onboard` changes the
/// account: stops whatever engine was previously running first, so its
/// background tasks don't keep polling with stale credentials.
async fn start_engine(app: AppHandle, cfg: AccountConfig) -> Result<(), String> {
    let app_state: State<AppState> = app.state();
    stop_engine(&app_state).await;

    let root = sync_root(&app);
    std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;

    let client = ButtercloudClient::connect(&cfg).await;
    let state_db = SyncState::open(&state_db_path(&app)).map_err(|e| e.to_string())?;
    let (engine, mut events) = SyncEngine::new(client, state_db, root, POLL_INTERVAL);
    let engine = Arc::new(engine);

    *app_state.engine.lock().await = Some(engine.clone());

    let run_handle = app.clone();
    let run_task = tauri::async_runtime::spawn(async move {
        if let Err(e) = engine.run().await {
            let _ = run_handle.emit(
                "engine-event",
                event_payload(&EngineEvent::Error { key: None, error: e }),
            );
        }
    });

    let events_handle = app.clone();
    let events_task = tauri::async_runtime::spawn(async move {
        while let Some(event) = events.recv().await {
            handle_engine_event(&events_handle, event);
        }
    });

    *app_state.engine_tasks.lock().await = vec![run_task, events_task];

    Ok(())
}

#[tauri::command]
fn has_account(state: State<'_, AppState>) -> bool {
    state.has_account.load(std::sync::atomic::Ordering::Relaxed)
}

#[tauri::command]
async fn onboard(
    app: AppHandle,
    state: State<'_, AppState>,
    access_key: String,
    secret_key: String,
    bucket: String,
) -> Result<(), String> {
    let cfg = AccountConfig::production(bucket, access_key, secret_key);
    let client = ButtercloudClient::connect(&cfg).await;
    client.validate().await.map_err(|e| e.to_string())?;
    creds::save(&cfg).map_err(|e| e.to_string())?;
    state
        .has_account
        .store(true, std::sync::atomic::Ordering::Relaxed);
    start_engine(app, cfg).await
}

#[tauri::command]
async fn sign_out(state: State<'_, AppState>) -> Result<(), String> {
    stop_engine(&state).await;
    creds::clear().map_err(|e| e.to_string())?;
    state
        .has_account
        .store(false, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

#[tauri::command]
async fn confirm_delete(
    state: State<'_, AppState>,
    key: String,
    originated_remotely: bool,
) -> Result<(), String> {
    let engine = state
        .engine
        .lock()
        .await
        .clone()
        .ok_or_else(|| "sync not started".to_string())?;
    engine.confirm_delete(&key, originated_remotely).await;
    Ok(())
}

#[tauri::command]
async fn cancel_delete(state: State<'_, AppState>, key: String) -> Result<(), String> {
    let engine = state
        .engine
        .lock()
        .await
        .clone()
        .ok_or_else(|| "sync not started".to_string())?;
    engine.cancel_delete(&key).await;
    Ok(())
}

#[tauri::command]
fn open_sync_folder(app: AppHandle) -> Result<(), String> {
    let path = sync_root(&app);
    std::fs::create_dir_all(&path).map_err(|e| e.to_string())?;
    app.opener()
        .open_path(path.to_string_lossy().to_string(), None::<&str>)
        .map_err(|e| e.to_string())
}

/// Opens the account's web dashboard in the system browser — usage, billing
/// and invoice status all live there (the API for them is JWT-only, and
/// this client deliberately never touches JWT; see the handoff doc).
#[tauri::command]
async fn open_dashboard(app: AppHandle) -> Result<(), String> {
    let cfg = creds::load()
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "no account connected".to_string())?;
    app.opener()
        .open_url(cfg.endpoint, None::<&str>)
        .map_err(|e| e.to_string())
}

/// Opens an https URL in the system browser — used for the "source & issues"
/// link so it doesn't navigate the app's own webview.
#[tauri::command]
fn open_external(app: AppHandle, url: String) -> Result<(), String> {
    if !url.starts_with("https://") {
        return Err("only https URLs may be opened".to_string());
    }
    app.opener().open_url(url, None::<&str>).map_err(|e| e.to_string())
}

#[tauri::command]
fn sync_folder_path(app: AppHandle) -> String {
    sync_root(&app).to_string_lossy().to_string()
}

#[tauri::command]
fn app_version(app: AppHandle) -> String {
    app.package_info().version.to_string()
}

fn setup_tray(app: &tauri::App) -> tauri::Result<()> {
    let open_item = MenuItemBuilder::with_id("open_folder", "Open Buttercloud Folder").build(app)?;
    let dashboard_item = MenuItemBuilder::with_id("dashboard", "Usage & Billing (Web)").build(app)?;
    let quit_item = MenuItemBuilder::with_id("quit", "Quit").build(app)?;
    let menu = MenuBuilder::new(app)
        .item(&open_item)
        .item(&dashboard_item)
        .separator()
        .item(&quit_item)
        .build()?;

    let mut tray = TrayIconBuilder::new()
        .menu(&menu)
        .tooltip("Buttercloud")
        .on_menu_event(|app, event| match event.id().as_ref() {
            "open_folder" => {
                let _ = open_sync_folder(app.clone());
            }
            "dashboard" => {
                let app = app.clone();
                tauri::async_runtime::spawn(async move {
                    let _ = open_dashboard(app).await;
                });
            }
            "quit" => app.exit(0),
            _ => {}
        });
    if let Some(icon) = app.default_window_icon() {
        tray = tray.icon(icon.clone());
    }
    tray.build(app)?;
    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_notification::init())
        .manage(AppState {
            engine: tokio::sync::Mutex::new(None),
            has_account: std::sync::atomic::AtomicBool::new(false),
            engine_tasks: tokio::sync::Mutex::new(Vec::new()),
        })
        .invoke_handler(tauri::generate_handler![
            has_account,
            onboard,
            sign_out,
            confirm_delete,
            cancel_delete,
            open_sync_folder,
            open_dashboard,
            open_external,
            sync_folder_path,
            app_version
        ])
        .setup(|app| {
            setup_tray(app)?;
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                // The one keychain read per launch: every other place that
                // needs to know "is an account connected" reads
                // AppState::has_account instead.
                match creds::load() {
                    Ok(Some(cfg)) => {
                        eprintln!("[startup] found saved account, bucket={:?}; starting engine", cfg.bucket);
                        let state: State<AppState> = handle.state();
                        state
                            .has_account
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                        let result = start_engine(handle, cfg).await;
                        eprintln!("[startup] start_engine result: {result:?}");
                    }
                    Ok(None) => eprintln!("[startup] no saved account; showing onboarding"),
                    Err(e) => eprintln!("[startup] creds::load() FAILED: {e}"),
                }
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
