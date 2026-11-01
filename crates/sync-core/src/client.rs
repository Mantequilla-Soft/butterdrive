use crate::config::AccountConfig;
use crate::error::{ButtercloudErrorBody, SyncError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_smithy_runtime_api::http::Response as HttpResponse;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::task::JoinSet;

/// Files bigger than this go up as a multipart upload instead of one PUT.
/// `s3.3speak.tv` sits behind Cloudflare, which rejects any single request
/// body over 100 MB, so one big PUT fails outright no matter what the gateway
/// would accept. Kept well under that cap since each part is held in memory
/// while it's sent (`PART_SIZE * PART_CONCURRENCY` at most per upload).
pub const MULTIPART_THRESHOLD: u64 = PART_SIZE;
pub const PART_SIZE: u64 = 16 * 1024 * 1024;
const PART_CONCURRENCY: usize = 4;

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

            // Buttercloud has a confirmed bug where ListObjectsV2's response
            // body gets silently truncated server-side once it's long
            // enough, with `Content-Length` matching the truncated body (so
            // it's not a transport error) and `IsTruncated` wrongly left
            // `false` — the XML parser accepts what's there without erroring
            // and we'd otherwise return a partial listing as if it were
            // complete. That's dangerous here specifically: callers diff
            // this list against known state to detect remote deletes, so a
            // silently-short list looks exactly like files being deleted.
            // `KeyCount` is generated from the same (pre-truncation) object
            // count as the rest of the body, so a mismatch against what
            // actually parsed out is the signal this happened.
            if let Some(reported) = resp.key_count() {
                let page_len = resp.contents().len() as i32;
                if page_len != reported {
                    return Err(SyncError::Other(format!(
                        "remote listing looks incomplete (server reported {reported} objects, \
                         only {page_len} were in the response) — see github.com/Mantequilla-Soft/buttercloud/issues/21"
                    )));
                }
            }

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

    /// Uploads a local file, picking a single PUT or a multipart upload by
    /// size. Returns the number of bytes uploaded.
    pub async fn upload_file(&self, key: &str, path: &Path) -> Result<u64, SyncError> {
        let size = tokio::fs::metadata(path)
            .await
            .map_err(|e| SyncError::Other(e.to_string()))?
            .len();
        if size <= MULTIPART_THRESHOLD {
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|e| SyncError::Other(e.to_string()))?;
            let len = bytes.len() as u64;
            self.put_object_bytes(key, bytes).await?;
            return Ok(len);
        }
        self.upload_multipart(key, path, size).await?;
        Ok(size)
    }

    async fn upload_multipart(&self, key: &str, path: &Path, size: u64) -> Result<(), SyncError> {
        let upload_id = self
            .s3
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(classify_sdk_error)?
            .upload_id
            .ok_or_else(|| SyncError::Other("server returned no upload id".to_string()))?;

        let result = async {
            let parts = self.upload_parts(key, path, size, &upload_id).await?;
            self.s3
                .complete_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(&upload_id)
                .multipart_upload(CompletedMultipartUpload::builder().set_parts(Some(parts)).build())
                .send()
                .await
                .map(|_| ())
                .map_err(classify_sdk_error)
        }
        .await;

        if result.is_err() {
            // Best-effort, for a failure at any step after creation (parts or
            // completion): frees the parts already stored, which otherwise take
            // up space on the storage node without showing up anywhere.
            let _ = self
                .s3
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
        }
        result
    }

    /// Sends every part, up to `PART_CONCURRENCY` at a time, each read from
    /// disk on its own so a large file is never held in memory whole. Parts
    /// go up as in-memory bodies, same as `put_object_bytes`: a streamed file
    /// body makes the SDK switch to aws-chunked encoding, which the gateway
    /// isn't known to handle. Transient failures are retried by the SDK's own
    /// retry policy, per part.
    async fn upload_parts(
        &self,
        key: &str,
        path: &Path,
        size: u64,
        upload_id: &str,
    ) -> Result<Vec<CompletedPart>, SyncError> {
        let part_count = size.div_ceil(PART_SIZE);
        let mut parts = Vec::with_capacity(part_count as usize);
        let mut in_flight = JoinSet::new();
        let mut next: u64 = 1;
        let mut first_error: Option<SyncError> = None;

        loop {
            while first_error.is_none() && next <= part_count && in_flight.len() < PART_CONCURRENCY {
                let offset = (next - 1) * PART_SIZE;
                let len = PART_SIZE.min(size - offset);
                let (s3, bucket, key, upload_id, path) = (
                    self.s3.clone(),
                    self.bucket.clone(),
                    key.to_string(),
                    upload_id.to_string(),
                    path.to_path_buf(),
                );
                let part_number = next as i32;
                in_flight.spawn(async move {
                    let body = read_range(&path, offset, len).await?;
                    let resp = s3
                        .upload_part()
                        .bucket(bucket)
                        .key(key)
                        .upload_id(upload_id)
                        .part_number(part_number)
                        .body(ByteStream::from(body))
                        .send()
                        .await
                        .map_err(classify_sdk_error)?;
                    Ok::<_, SyncError>(
                        CompletedPart::builder()
                            .part_number(part_number)
                            .set_e_tag(resp.e_tag)
                            .build(),
                    )
                });
                next += 1;
            }

            // After a failure, stop starting parts but let the ones in flight
            // finish rather than cancelling them mid-request: a cancelled part
            // can still land server-side after the caller's abort, leaving an
            // orphan behind it.
            let err = match in_flight.join_next().await {
                None => break,
                Some(Ok(Ok(part))) => {
                    parts.push(part);
                    continue;
                }
                Some(Ok(Err(e))) => e,
                Some(Err(e)) => SyncError::Other(e.to_string()),
            };
            first_error.get_or_insert(err);
        }

        if let Some(e) = first_error {
            return Err(e);
        }
        parts.sort_by_key(|p| p.part_number);
        Ok(parts)
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

/// Reads exactly `len` bytes at `offset`. Errors (rather than sending a short
/// part) if the file shrank since its size was read.
async fn read_range(path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, SyncError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| SyncError::Other(e.to_string()))?;
    file.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|e| SyncError::Other(e.to_string()))?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)
        .await
        .map_err(|e| SyncError::Other(format!("file changed while uploading: {e}")))?;
    Ok(buf)
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
