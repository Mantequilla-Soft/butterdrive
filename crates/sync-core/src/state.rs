use crate::error::SyncError;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;

/// What we knew about one synced object the last time it was reconciled,
/// used to tell "changed since last sync" apart from "we just haven't looked
/// yet" on both the local and remote side.
#[derive(Debug, Clone)]
pub struct SyncedObject {
    pub key: String,
    pub size: i64,
    pub etag: Option<String>,
    pub local_mtime: Option<i64>,
    pub synced_at: i64,
}

/// `rusqlite::Connection` is `Send` but not `Sync` (it caches prepared
/// statements internally via a `RefCell`), and `SyncEngine` is shared across
/// Tauri's command handlers as `Arc<SyncEngine>`, which requires every field
/// to be `Sync`. The mutex is what makes that legal — these are all quick
/// local queries, so lock contention isn't a real concern.
pub struct SyncState {
    conn: Mutex<Connection>,
}

fn lock_err(_: impl std::fmt::Debug) -> SyncError {
    SyncError::Other("sync-state database lock poisoned".to_string())
}

impl SyncState {
    pub fn open(path: &Path) -> Result<Self, SyncError> {
        let conn = Connection::open(path).map_err(|e| SyncError::Other(e.to_string()))?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS objects (
                key TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                etag TEXT,
                local_mtime INTEGER,
                synced_at INTEGER NOT NULL
            )",
            [],
        )
        .map_err(|e| SyncError::Other(e.to_string()))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn record(&self, obj: &SyncedObject) -> Result<(), SyncError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        conn.execute(
            "INSERT INTO objects (key, size, etag, local_mtime, synced_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(key) DO UPDATE SET
                size = excluded.size,
                etag = excluded.etag,
                local_mtime = excluded.local_mtime,
                synced_at = excluded.synced_at",
            params![obj.key, obj.size, obj.etag, obj.local_mtime, obj.synced_at],
        )
        .map_err(|e| SyncError::Other(e.to_string()))?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<SyncedObject>, SyncError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        conn.query_row(
            "SELECT key, size, etag, local_mtime, synced_at FROM objects WHERE key = ?1",
            params![key],
            |row| {
                Ok(SyncedObject {
                    key: row.get(0)?,
                    size: row.get(1)?,
                    etag: row.get(2)?,
                    local_mtime: row.get(3)?,
                    synced_at: row.get(4)?,
                })
            },
        )
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(SyncError::Other(other.to_string())),
        })
    }

    pub fn remove(&self, key: &str) -> Result<(), SyncError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        conn.execute("DELETE FROM objects WHERE key = ?1", params![key])
            .map_err(|e| SyncError::Other(e.to_string()))?;
        Ok(())
    }

    pub fn all(&self) -> Result<Vec<SyncedObject>, SyncError> {
        let conn = self.conn.lock().map_err(lock_err)?;
        let mut stmt = conn
            .prepare("SELECT key, size, etag, local_mtime, synced_at FROM objects")
            .map_err(|e| SyncError::Other(e.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(SyncedObject {
                    key: row.get(0)?,
                    size: row.get(1)?,
                    etag: row.get(2)?,
                    local_mtime: row.get(3)?,
                    synced_at: row.get(4)?,
                })
            })
            .map_err(|e| SyncError::Other(e.to_string()))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| SyncError::Other(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(key: &str) -> SyncedObject {
        SyncedObject {
            key: key.to_string(),
            size: 42,
            etag: Some("abc123".to_string()),
            local_mtime: Some(1_000),
            synced_at: 2_000,
        }
    }

    #[test]
    fn get_on_empty_state_returns_none() {
        let state = SyncState::open(Path::new(":memory:")).unwrap();
        assert!(state.get("missing.txt").unwrap().is_none());
    }

    #[test]
    fn record_then_get_roundtrips() {
        let state = SyncState::open(Path::new(":memory:")).unwrap();
        state.record(&obj("notes.txt")).unwrap();
        let fetched = state.get("notes.txt").unwrap().unwrap();
        assert_eq!(fetched.size, 42);
        assert_eq!(fetched.etag, Some("abc123".to_string()));
    }

    #[test]
    fn record_upserts_on_conflict() {
        let state = SyncState::open(Path::new(":memory:")).unwrap();
        state.record(&obj("notes.txt")).unwrap();
        let mut updated = obj("notes.txt");
        updated.size = 99;
        state.record(&updated).unwrap();
        assert_eq!(state.get("notes.txt").unwrap().unwrap().size, 99);
        assert_eq!(state.all().unwrap().len(), 1);
    }

    #[test]
    fn remove_deletes_the_row() {
        let state = SyncState::open(Path::new(":memory:")).unwrap();
        state.record(&obj("notes.txt")).unwrap();
        state.remove("notes.txt").unwrap();
        assert!(state.get("notes.txt").unwrap().is_none());
    }

    #[test]
    fn all_returns_every_record() {
        let state = SyncState::open(Path::new(":memory:")).unwrap();
        state.record(&obj("a.txt")).unwrap();
        state.record(&obj("b.txt")).unwrap();
        let mut keys: Vec<_> = state.all().unwrap().into_iter().map(|o| o.key).collect();
        keys.sort();
        assert_eq!(keys, vec!["a.txt".to_string(), "b.txt".to_string()]);
    }
}
