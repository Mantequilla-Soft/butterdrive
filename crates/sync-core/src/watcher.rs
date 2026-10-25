use crate::error::SyncError;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::path::Path;
use tokio::sync::mpsc;

/// Bridges `notify`'s callback-based local filesystem watching into an async
/// channel the sync engine can select on alongside the remote poll timer.
pub struct LocalWatcher {
    _watcher: RecommendedWatcher,
    pub events: mpsc::Receiver<notify::Event>,
}

impl LocalWatcher {
    pub fn watch(root: &Path) -> Result<Self, SyncError> {
        let (tx, rx) = mpsc::channel(256);
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if let Ok(event) = res {
                // Runs on notify's own background thread, not inside the
                // tokio runtime, so blocking_send is the right call here.
                let _ = tx.blocking_send(event);
            }
        })
        .map_err(|e| SyncError::Other(e.to_string()))?;
        watcher
            .watch(root, RecursiveMode::Recursive)
            .map_err(|e| SyncError::Other(e.to_string()))?;
        Ok(Self {
            _watcher: watcher,
            events: rx,
        })
    }
}
