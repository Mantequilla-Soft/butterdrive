//! Phase 7 E2E: drives the real `SyncEngine` (watcher + poller + diff +
//! upload/download + delete confirmation) against a real account, not just
//! the raw client. Reads credentials from the environment — never hardcode
//! them here.
//!
//! BUTTERCLOUD_BUCKET / BUTTERCLOUD_ACCESS_KEY / BUTTERCLOUD_SECRET_KEY
//! [BUTTERCLOUD_ENDPOINT, defaults to production]

use std::time::Duration;
use sync_core::client::ButtercloudClient;
use sync_core::config::AccountConfig;
use sync_core::engine::{EngineEvent, SyncEngine};
use sync_core::state::SyncState;
use tokio::time::timeout;

const POLL_INTERVAL: Duration = Duration::from_secs(3);
const WAIT: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() {
    let bucket = env_var("BUTTERCLOUD_BUCKET");
    let access_key = env_var("BUTTERCLOUD_ACCESS_KEY");
    let secret_key = env_var("BUTTERCLOUD_SECRET_KEY");
    let endpoint =
        std::env::var("BUTTERCLOUD_ENDPOINT").unwrap_or_else(|_| "https://s3.3speak.tv".to_string());

    let mut cfg = AccountConfig::production(bucket.clone(), access_key, secret_key);
    cfg.endpoint = endpoint;

    let root = std::env::temp_dir().join(format!("butterdrive-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("create sync root");
    println!("sync root: {}", root.display());

    let db_path = root.with_extension("sqlite");
    let client = ButtercloudClient::connect(&cfg).await;
    let probe_client = ButtercloudClient::connect(&cfg).await; // a second, independent client to simulate "remote" actors
    let state = SyncState::open(&db_path).expect("open state db");

    let (engine, mut events) = SyncEngine::new(client, state, root.clone(), POLL_INTERVAL);
    let engine = std::sync::Arc::new(engine);
    let engine_task = {
        let engine = engine.clone();
        tokio::spawn(async move {
            let _ = engine.run().await;
        })
    };

    // give the engine's initial scan/poll a moment to settle before we start asserting
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut pass = true;

    // --- 1. local create -> upload ---
    println!("\n== test 1: local file create -> remote upload ==");
    let local_file = root.join("hello.txt");
    std::fs::write(&local_file, b"hello from the e2e harness").unwrap();
    pass &= expect_event(&mut events, |e| matches!(e, EngineEvent::Uploaded(k) if k == "hello.txt")).await;

    // --- 2. remote put (by a second client) -> download ---
    println!("\n== test 2: remote object (via second client) -> local download ==");
    probe_client
        .put_object_bytes("remote-origin.txt", b"written directly via a second client".to_vec())
        .await
        .expect("probe put");
    pass &= expect_event(&mut events, |e| matches!(e, EngineEvent::Downloaded(k) if k == "remote-origin.txt")).await;
    let downloaded_path = root.join("remote-origin.txt");
    pass &= check(downloaded_path.exists(), "downloaded file exists on disk");

    // --- 3. local delete -> pending delete (local-originated) -> confirm ---
    println!("\n== test 3: local delete -> PendingDelete(local) -> confirm_delete removes remotely ==");
    std::fs::remove_file(&local_file).unwrap();
    let key = pass_through(&mut events, |e| match e {
        EngineEvent::PendingDelete { key, originated_remotely: false } => Some(key.clone()),
        _ => None,
    })
    .await;
    pass &= check(key.as_deref() == Some("hello.txt"), "PendingDelete reported for hello.txt (local-originated)");
    engine.confirm_delete("hello.txt", false).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let still_remote = client_has_object(&probe_client, "hello.txt").await;
    pass &= check(!still_remote, "hello.txt removed from the bucket after confirm_delete");

    // --- 4. remote delete (by second client) -> pending delete (remote-originated) -> confirm ---
    println!("\n== test 4: remote delete (via second client) -> PendingDelete(remote) -> confirm_delete removes locally ==");
    probe_client.delete_object("remote-origin.txt").await.expect("probe delete");
    let key = pass_through(&mut events, |e| match e {
        EngineEvent::PendingDelete { key, originated_remotely: true } => Some(key.clone()),
        _ => None,
    })
    .await;
    pass &= check(key.as_deref() == Some("remote-origin.txt"), "PendingDelete reported for remote-origin.txt (remote-originated)");
    engine.confirm_delete("remote-origin.txt", true).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    pass &= check(!downloaded_path.exists(), "remote-origin.txt removed locally after confirm_delete");

    engine_task.abort();
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_file(&db_path);

    // belt-and-suspenders: make sure the bucket is clean even if an assertion above failed early
    let _ = probe_client.delete_object("hello.txt").await;
    let _ = probe_client.delete_object("remote-origin.txt").await;

    println!("\n== result ==");
    if pass {
        println!("ALL PASSED");
    } else {
        println!("SOME CHECKS FAILED (see [FAIL] above)");
        std::process::exit(1);
    }
}

async fn client_has_object(client: &ButtercloudClient, key: &str) -> bool {
    matches!(client.head_object(key).await, Ok(Some(_)))
}

async fn expect_event<F: Fn(&EngineEvent) -> bool>(
    rx: &mut tokio::sync::mpsc::Receiver<EngineEvent>,
    pred: F,
) -> bool {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            println!("[FAIL] timed out waiting for expected event");
            return false;
        }
        match timeout(remaining, rx.recv()).await {
            Ok(Some(event)) => {
                println!("  event: {event:?}");
                if pred(&event) {
                    println!("[PASS] matched expected event");
                    return true;
                }
            }
            Ok(None) => {
                println!("[FAIL] event channel closed");
                return false;
            }
            Err(_) => {
                println!("[FAIL] timed out waiting for expected event");
                return false;
            }
        }
    }
}

async fn pass_through<T, F: Fn(&EngineEvent) -> Option<T>>(
    rx: &mut tokio::sync::mpsc::Receiver<EngineEvent>,
    pred: F,
) -> Option<T> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            println!("[FAIL] timed out waiting for expected event");
            return None;
        }
        match timeout(remaining, rx.recv()).await {
            Ok(Some(event)) => {
                println!("  event: {event:?}");
                if let Some(v) = pred(&event) {
                    println!("[PASS] matched expected event");
                    return Some(v);
                }
            }
            Ok(None) | Err(_) => {
                println!("[FAIL] timed out or channel closed waiting for expected event");
                return None;
            }
        }
    }
}

fn check(cond: bool, label: &str) -> bool {
    if cond {
        println!("[PASS] {label}");
    } else {
        println!("[FAIL] {label}");
    }
    cond
}

fn env_var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required env var {name}"))
}
