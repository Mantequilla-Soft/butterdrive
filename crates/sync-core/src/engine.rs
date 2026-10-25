use crate::client::ButtercloudClient;
use crate::error::SyncError;
use crate::state::{SyncState, SyncedObject};
use crate::watcher::LocalWatcher;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::time::interval;

/// What the UI should show. Deletes are never applied automatically — see
/// `PendingDelete` and `confirm_delete`/`cancel_delete` below — because
/// there's no recovery path until Buttercloud ships a recycle bin (#13).
#[derive(Debug, Clone)]
pub enum EngineEvent {
    Uploaded(String),
    Downloaded(String),
    /// A delete was observed (locally or remotely) and is waiting on
    /// `SyncEngine::confirm_delete` / `cancel_delete` before it propagates.
    PendingDelete { key: String, originated_remotely: bool },
    /// `key` is the object this error was about, when there is one (most
    /// errors are about a specific file; a few — like a failed remote
    /// listing — aren't about any one key).
    Error { key: Option<String>, error: SyncError },
}

pub struct SyncEngine {
    client: ButtercloudClient,
    state: SyncState,
    root: PathBuf,
    poll_interval: Duration,
    events_tx: mpsc::Sender<EngineEvent>,
    pending_deletes: Mutex<HashSet<String>>,
    // Keyed by object key, value is the local mtime that last failed to
    // upload. Lets a failing upload of the same unmodified file go quiet
    // after the first report instead of re-failing (and re-notifying) every
    // time the engine restarts and rescans — a new mtime (the user edited
    // the file again) clears the entry and allows a fresh attempt.
    failed_uploads: Mutex<HashMap<String, i64>>,
}

impl SyncEngine {
    pub fn new(
        client: ButtercloudClient,
        state: SyncState,
        root: PathBuf,
        poll_interval: Duration,
    ) -> (Self, mpsc::Receiver<EngineEvent>) {
        let (events_tx, events_rx) = mpsc::channel(256);
        // On macOS, `notify`'s FSEvents backend reports paths through their
        // canonicalized form (e.g. resolving the /var -> /private/var
        // symlink). If `root` isn't canonicalized too, `key_for` strips the
        // wrong prefix and silently drops every local-originated event.
        // Callers are expected to have already created this directory.
        let root = std::fs::canonicalize(&root).unwrap_or(root);
        (
            Self {
                client,
                state,
                root,
                poll_interval,
                events_tx,
                pending_deletes: Mutex::new(HashSet::new()),
                failed_uploads: Mutex::new(HashMap::new()),
            },
            events_rx,
        )
    }

    /// Runs forever: local fs events and the remote poll timer both feed the
    /// same reconciliation logic. Call from a dedicated tokio task.
    pub async fn run(&self) -> Result<(), SyncError> {
        let mut watcher = LocalWatcher::watch(&self.root)?;
        let mut ticker = interval(self.poll_interval);

        // Catch up on anything that happened before the watcher/poller were
        // running (first launch, or the app was quit for a while).
        self.scan_local().await;
        self.poll_remote().await;

        loop {
            tokio::select! {
                maybe_event = watcher.events.recv() => {
                    match maybe_event {
                        Some(event) => self.handle_local_event(event).await,
                        None => return Ok(()),
                    }
                }
                _ = ticker.tick() => {
                    self.poll_remote().await;
                }
            }
        }
    }

    /// A user (via the UI) agreed to apply a delete that was waiting for
    /// confirmation. `originated_remotely` must match what was reported in
    /// the `PendingDelete` event.
    pub async fn confirm_delete(&self, key: &str, originated_remotely: bool) {
        self.pending_deletes.lock().await.remove(key);
        let result = if originated_remotely {
            // The object is already gone upstream; bring the local copy in
            // line with it.
            std::fs::remove_file(self.local_path(key)).map_err(|e| SyncError::Other(e.to_string()))
        } else {
            self.client.delete_object(key).await
        };
        match result.and_then(|_| self.state.remove(key)) {
            Ok(()) => {}
            Err(e) => self.emit_error(Some(key.to_string()), e).await,
        }
    }

    pub async fn cancel_delete(&self, key: &str) {
        self.pending_deletes.lock().await.remove(key);
    }

    async fn scan_local(&self) {
        for entry in walkdir::WalkDir::new(&self.root)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path().to_path_buf();
            let Some(key) = self.key_for(&path) else { continue };
            let Some(mtime) = file_mtime(&path) else { continue };
            match self.state.get(&key) {
                Ok(Some(existing)) if existing.local_mtime == Some(mtime) => {
                    // Already synced at this mtime, nothing to do.
                }
                Ok(_) => self.upload_local(&path, &key, mtime).await,
                Err(e) => self.emit_error(Some(key), e).await,
            }
        }
    }

    async fn poll_remote(&self) {
        let remote_objects = match self.client.list_objects().await {
            Ok(objs) => objs,
            Err(e) => {
                self.emit_error(None, e).await;
                return;
            }
        };
        let mut remote_keys = HashSet::new();

        for obj in &remote_objects {
            remote_keys.insert(obj.key.clone());
            // Folder markers have nothing to download.
            if obj.key.ends_with('/') {
                continue;
            }
            let known = match self.state.get(&obj.key) {
                Ok(v) => v,
                Err(e) => {
                    self.emit_error(Some(obj.key.clone()), e).await;
                    continue;
                }
            };
            let changed_remotely = match &known {
                Some(existing) => existing.etag != obj.etag,
                None => true,
            };
            if changed_remotely {
                self.download_remote(obj).await;
            }
        }

        // Anything we previously synced that's no longer in the remote
        // listing was deleted upstream.
        let known = match self.state.all() {
            Ok(v) => v,
            Err(e) => {
                self.emit_error(None, e).await;
                return;
            }
        };
        for obj in known {
            if !remote_keys.contains(&obj.key) {
                self.queue_pending_delete(&obj.key, true).await;
            }
        }
    }

    async fn handle_local_event(&self, event: notify::Event) {
        use notify::EventKind;
        for path in &event.paths {
            let Some(key) = self.key_for(path) else { continue };
            match event.kind {
                EventKind::Create(_) | EventKind::Modify(_) => {
                    if path.is_file()
                        && let Some(mtime) = file_mtime(path) {
                            // A single write can fire several Create/Modify
                            // events for the same path; skip re-uploading
                            // when we already synced this exact mtime.
                            let already_synced = matches!(
                                self.state.get(&key),
                                Ok(Some(existing)) if existing.local_mtime == Some(mtime)
                            );
                            if !already_synced {
                                self.upload_local(path, &key, mtime).await;
                            }
                        }
                }
                EventKind::Remove(_) => {
                    // The path no longer exists to stat; only act if we
                    // actually knew about this key.
                    match self.state.get(&key) {
                        Ok(Some(_)) => self.queue_pending_delete(&key, false).await,
                        Ok(None) => {}
                        Err(e) => self.emit_error(Some(key), e).await,
                    }
                }
                _ => {}
            }
        }
    }

    async fn upload_local(&self, path: &Path, key: &str, mtime: i64) {
        if self.failed_uploads.lock().await.get(key) == Some(&mtime) {
            // Already reported this exact (key, mtime) failing; stay quiet
            // until the file changes again or the key clears on success.
            return;
        }

        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                self.emit_error(Some(key.to_string()), SyncError::Other(e.to_string())).await;
                return;
            }
        };
        let size = bytes.len() as i64;
        match self.client.put_object_bytes(key, bytes).await {
            Ok(()) => {
                self.failed_uploads.lock().await.remove(key);
                let etag = match self.client.head_object(key).await {
                    Ok(Some(obj)) => obj.etag,
                    _ => None,
                };
                let record = SyncedObject {
                    key: key.to_string(),
                    size,
                    etag,
                    local_mtime: Some(mtime),
                    synced_at: now(),
                };
                if let Err(e) = self.state.record(&record) {
                    self.emit_error(Some(key.to_string()), e).await;
                } else {
                    let _ = self
                        .events_tx
                        .send(EngineEvent::Uploaded(key.to_string()))
                        .await;
                }
            }
            Err(e) => {
                self.failed_uploads.lock().await.insert(key.to_string(), mtime);
                self.emit_error(Some(key.to_string()), e).await;
            }
        }
    }

    async fn download_remote(&self, obj: &crate::client::RemoteObject) {
        let bytes = match self.client.get_object_bytes(&obj.key).await {
            Ok(b) => b,
            Err(e) => {
                self.emit_error(Some(obj.key.clone()), e).await;
                return;
            }
        };
        let path = self.local_path(&obj.key);
        if let Some(parent) = path.parent()
            && let Err(e) = std::fs::create_dir_all(parent) {
                self.emit_error(Some(obj.key.clone()), SyncError::Other(e.to_string())).await;
                return;
            }
        if let Err(e) = std::fs::write(&path, &bytes) {
            self.emit_error(Some(obj.key.clone()), SyncError::Other(e.to_string())).await;
            return;
        }
        let mtime = file_mtime(&path);
        let record = SyncedObject {
            key: obj.key.clone(),
            size: obj.size,
            etag: obj.etag.clone(),
            local_mtime: mtime,
            synced_at: now(),
        };
        match self.state.record(&record) {
            Ok(()) => {
                let _ = self
                    .events_tx
                    .send(EngineEvent::Downloaded(obj.key.clone()))
                    .await;
            }
            Err(e) => self.emit_error(Some(obj.key.clone()), e).await,
        }
    }

    async fn queue_pending_delete(&self, key: &str, originated_remotely: bool) {
        let mut pending = self.pending_deletes.lock().await;
        if pending.insert(key.to_string()) {
            drop(pending);
            let _ = self
                .events_tx
                .send(EngineEvent::PendingDelete {
                    key: key.to_string(),
                    originated_remotely,
                })
                .await;
        }
    }

    async fn emit_error(&self, key: Option<String>, error: SyncError) {
        let _ = self.events_tx.send(EngineEvent::Error { key, error }).await;
    }

    fn key_for(&self, path: &Path) -> Option<String> {
        key_for_root(&self.root, path)
    }

    fn local_path(&self, key: &str) -> PathBuf {
        local_path_for_root(&self.root, key)
    }
}

/// The local-path <-> object-key mapping, factored out of `SyncEngine` so it
/// can be unit tested without a real client/state/watcher.
fn key_for_root(root: &Path, path: &Path) -> Option<String> {
    let rel = path.strip_prefix(root).ok()?;
    let key = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/");
    if key.is_empty() { None } else { Some(key) }
}

fn local_path_for_root(root: &Path, key: &str) -> PathBuf {
    let mut path = root.to_path_buf();
    for part in key.split('/') {
        path.push(part);
    }
    path
}

fn file_mtime(path: &Path) -> Option<i64> {
    let meta = std::fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    modified
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs() as i64)
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_for_root_strips_prefix_and_normalizes_separators() {
        let root = Path::new("/sync/root");
        let path = Path::new("/sync/root/docs/notes.txt");
        assert_eq!(key_for_root(root, path), Some("docs/notes.txt".to_string()));
    }

    #[test]
    fn key_for_root_rejects_paths_outside_root() {
        let root = Path::new("/sync/root");
        let path = Path::new("/elsewhere/notes.txt");
        assert_eq!(key_for_root(root, path), None);
    }

    #[test]
    fn key_for_root_rejects_the_root_itself() {
        let root = Path::new("/sync/root");
        assert_eq!(key_for_root(root, root), None);
    }

    #[test]
    fn local_path_for_root_rebuilds_nested_keys() {
        let root = Path::new("/sync/root");
        assert_eq!(
            local_path_for_root(root, "docs/notes.txt"),
            Path::new("/sync/root/docs/notes.txt")
        );
    }

    #[test]
    fn roundtrip_key_and_local_path() {
        let root = Path::new("/sync/root");
        let original = Path::new("/sync/root/a/b/c.txt");
        let key = key_for_root(root, original).unwrap();
        assert_eq!(local_path_for_root(root, &key), original);
    }
}
