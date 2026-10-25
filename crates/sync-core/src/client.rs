use crate::config::AccountConfig;
use crate::error::{ButtercloudErrorBody, SyncError};
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_runtime_api::http::Response as HttpResponse;

/// One object as it comes back from `ListObjectsV2`.
#[derive(Debug, Clone)]
pub struct RemoteObject {
    pub key: String,
    pub size: i64,
    pub etag: Option<String>,
    pub last_modified: Option<aws_sdk_s3::primitives::DateTime>,
}

/// Thin wrapper around `aws_sdk_s3::Client` pointed at a Buttercloud account.
/// Every call goes through SigV4 (the SDK's native auth) against the
/// account's single bucket — never JWT.
pub struct ButtercloudClient {
    s3: aws_sdk_s3::Client,
    pub bucket: String,
}

impl ButtercloudClient {
    pub async fn connect(cfg: &AccountConfig) -> Self {
        let creds = aws_sdk_s3::config::Credentials::new(
            &cfg.access_key,
            &cfg.secret_key,
            None,
            None,
            "buttercloud-static",
        );
        let s3_config = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .endpoint_url(&cfg.endpoint)
            // Buttercloud's docs only show path-style routes
            // (`/{bucket}/{key}`), not virtual-hosted-style.
            .force_path_style(true)
            // Confirmed via the Phase 2 spike: signing with a real AWS
            // region (e.g. "us-east-1") gets rejected with
            // SignatureDoesNotMatch. Buttercloud's own Node.js example signs
            // with the literal region "default", and the gateway's SigV4
            // validation apparently requires that exact string.
            .region(aws_sdk_s3::config::Region::new("default"))
            .credentials_provider(creds)
            .build();
        Self {
            s3: aws_sdk_s3::Client::from_conf(s3_config),
            bucket: cfg.bucket.clone(),
        }
    }

    /// Cheap call to confirm the credentials + bucket actually work, used by
    /// onboarding before credentials are saved to the keychain.
    ///
    /// Deliberately uses `ListObjectsV2` rather than `HEAD bucket`: HTTP HEAD
    /// responses never carry a body, so a HEAD failure gives
    /// `classify_sdk_error` nothing to read and it can't tell the real cause
    /// (quota, suspension, a bad key) from anything else.
    pub async fn validate(&self) -> Result<(), SyncError> {
        self.s3
            .list_objects_v2()
            .bucket(&self.bucket)
            .max_keys(1)
            .send()
            .await
            .map(|_| ())
            .map_err(classify_sdk_error)
    }

    pub async fn list_objects(&self) -> Result<Vec<RemoteObject>, SyncError> {
        let mut objects = Vec::new();
        let mut continuation_token: Option<String> = None;
        loop {
            let mut req = self.s3.list_objects_v2().bucket(&self.bucket);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(classify_sdk_error)?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    objects.push(RemoteObject {
                        key: key.to_string(),
                        size: obj.size().unwrap_or(0),
                        etag: obj.e_tag().map(str::to_string),
                        last_modified: obj.last_modified().cloned(),
                    });
                }
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation_token = resp.next_continuation_token().map(str::to_string);
            } else {
                break;
            }
        }
        Ok(objects)
    }

    pub async fn put_object_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<(), SyncError> {
        self.s3
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map(|_| ())
            .map_err(classify_sdk_error)
    }

    pub async fn get_object_bytes(&self, key: &str) -> Result<Vec<u8>, SyncError> {
        let resp = self
            .s3
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(classify_sdk_error)?;
        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| SyncError::Other(e.to_string()))?;
        Ok(data.into_bytes().to_vec())
    }

    pub async fn delete_object(&self, key: &str) -> Result<(), SyncError> {
        self.s3
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map(|_| ())
            .map_err(classify_sdk_error)
    }

    pub async fn head_object(&self, key: &str) -> Result<Option<RemoteObject>, SyncError> {
        match self
            .s3
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => Ok(Some(RemoteObject {
                key: key.to_string(),
                size: resp.content_length().unwrap_or(0),
                etag: resp.e_tag().map(str::to_string),
                last_modified: resp.last_modified().cloned(),
            })),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(classify_sdk_error(e)),
        }
    }

    /// Creates the zero-byte, trailing-slash marker object that represents an
    /// empty folder (Buttercloud's `mkdir` is a JWT-only portal route, so the
    /// client creates this convention itself over SigV4).
    pub async fn create_folder_marker(&self, prefix: &str) -> Result<(), SyncError> {
        let key = if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        };
        self.put_object_bytes(&key, Vec::new()).await
    }
}

fn is_not_found<E>(err: &aws_sdk_s3::error::SdkError<E, HttpResponse>) -> bool {
    matches!(
        err.raw_response().map(|r| r.status().as_u16()),
        Some(404)
    )
}

/// Pulls the raw response body off any SDK error and checks it against
/// Buttercloud's custom 403 bodies before falling back to a generic error.
/// A HEAD response never carries a body (HTTP spec), so a HEAD 403 always
/// falls through to `Other` — callers that need a specific reason should
/// prefer a GET/LIST-based call (see `validate`).
fn classify_sdk_error<E: std::fmt::Debug>(
    err: aws_sdk_s3::error::SdkError<E, HttpResponse>,
) -> SyncError {
    let status = err.raw_response().map(|r| r.status().as_u16());
    let body_bytes = err.raw_response().and_then(|r| r.body().bytes());

    if std::env::var("SYNC_CORE_DEBUG_ERRORS").is_ok() {
        eprintln!(
            "[debug] status={status:?} body={:?} sdk_err={err:?}",
            body_bytes.map(String::from_utf8_lossy)
        );
    }

    if status == Some(403) {
        if let Some(bytes) = body_bytes
            && let Ok(body) = serde_json::from_slice::<ButtercloudErrorBody>(bytes)
                && let Some(classified) = body.classify() {
                    return classified;
                }
        return SyncError::Other(format!("access denied (403): {err}"));
    }

    SyncError::Other(format!("{err}"))
}
