//! Phase 2 spike: exercises `ButtercloudClient` against a real account to
//! confirm the things the upstream docs don't state (multipart lifecycle,
//! error body shapes, path-style addressing, region handling). Reads
//! credentials from the environment — never hardcode them here.
//!
//! BUTTERCLOUD_BUCKET / BUTTERCLOUD_ACCESS_KEY / BUTTERCLOUD_SECRET_KEY
//! [BUTTERCLOUD_ENDPOINT, defaults to production]

use sync_core::client::ButtercloudClient;
use sync_core::config::AccountConfig;

const TEST_KEY: &str = "spike-test/hello.txt";
const TEST_FOLDER_MARKER: &str = "spike-test/subfolder/";

#[tokio::main]
async fn main() {
    let bucket = env_var("BUTTERCLOUD_BUCKET");
    let access_key = env_var("BUTTERCLOUD_ACCESS_KEY");
    let secret_key = env_var("BUTTERCLOUD_SECRET_KEY");
    let endpoint =
        std::env::var("BUTTERCLOUD_ENDPOINT").unwrap_or_else(|_| "https://s3.3speak.tv".to_string());

    let mut cfg = AccountConfig::production(bucket, access_key, secret_key);
    cfg.endpoint = endpoint;

    let client = ButtercloudClient::connect(&cfg).await;

    step("validate (HEAD bucket)", client.validate().await);

    step(
        "list_objects (before)",
        client.list_objects().await.map(describe_objects),
    );

    step(
        "put_object_bytes",
        client
            .put_object_bytes(TEST_KEY, b"hello from the buttercloud spike".to_vec())
            .await,
    );

    step(
        "head_object",
        client.head_object(TEST_KEY).await.map(|o| {
            o.map(|o| format!("size={} etag={:?}", o.size, o.etag))
                .unwrap_or_else(|| "missing".to_string())
        }),
    );

    step(
        "get_object_bytes",
        client.get_object_bytes(TEST_KEY).await.map(|bytes| {
            format!("{} bytes = {:?}", bytes.len(), String::from_utf8_lossy(&bytes))
        }),
    );

    step(
        "create_folder_marker",
        client.create_folder_marker("spike-test/subfolder").await,
    );

    step(
        "list_objects (after)",
        client.list_objects().await.map(describe_objects),
    );

    println!("\n-- cleanup --");
    step("delete_object(test file)", client.delete_object(TEST_KEY).await);
    step(
        "delete_object(folder marker)",
        client.delete_object(TEST_FOLDER_MARKER).await,
    );

    step(
        "list_objects (final, should be empty of spike-test/*)",
        client.list_objects().await.map(describe_objects),
    );
}

fn describe_objects(objs: Vec<sync_core::client::RemoteObject>) -> String {
    format!(
        "{} objects: {:?}",
        objs.len(),
        objs.iter().map(|o| o.key.clone()).collect::<Vec<_>>()
    )
}

fn step<T: std::fmt::Debug>(name: &str, result: Result<T, sync_core::error::SyncError>) {
    match result {
        Ok(v) => println!("[OK]    {name}: {v:?}"),
        Err(e) => println!("[ERROR] {name}: {e} ({e:?})"),
    }
}

fn env_var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("missing required env var {name}"))
}
