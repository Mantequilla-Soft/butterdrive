use std::fmt;

/// Buttercloud's documented custom 403 codes, plus the generic S3 error
/// surface. Branch on the specific variants for user-facing messages; treat
/// `Other` as a normal transfer failure.
#[derive(Debug, Clone)]
pub enum SyncError {
    /// Plan storage limit hit (`Code: 'QuotaExceeded'`).
    QuotaExceeded,
    /// Invoice overdue past the grace period (`Code: 'AccountSuspended'`).
    /// Reads/deletes of the user's own data still work; new uploads don't.
    AccountSuspended,
    /// The access key itself isn't recognized (`Code: 'InvalidAccessKeyId'`)
    /// — deleted/revoked server-side, or mistyped. Distinct from
    /// `SignatureMismatch`: the gateway tells these apart, so we do too.
    AccessKeyNotFound,
    /// `Code: 'SignatureDoesNotMatch'`. Usually a wrong secret key — but this
    /// gateway is also confirmed (as of 2026-10-25, reported upstream) to
    /// reject correctly-signed requests whose object key needs
    /// percent-encoding (spaces, apostrophes, `+`, ...). If this happens for
    /// some files/folders but not others under the same account, it's that
    /// bug, not a bad key.
    SignatureMismatch,
    /// Anything else: transport failure, unexpected status, bad config, etc.
    Other(String),
}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncError::QuotaExceeded => write!(f, "storage quota exceeded"),
            SyncError::AccountSuspended => write!(f, "account suspended (overdue invoice)"),
            SyncError::AccessKeyNotFound => {
                write!(f, "access key not found — it may have been deleted or revoked")
            }
            SyncError::SignatureMismatch => write!(
                f,
                "signature rejected — check the secret key, or this item's name may use characters the server doesn't accept yet (spaces, quotes, etc.)"
            ),
            SyncError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for SyncError {}

/// Buttercloud's custom error body on its 403s, e.g.
/// `{"Code":"QuotaExceeded","Message":"..."}`.
#[derive(Debug, serde::Deserialize)]
pub struct ButtercloudErrorBody {
    #[serde(rename = "Code")]
    pub code: String,
    #[serde(rename = "Message", default)]
    pub message: Option<String>,
}

impl ButtercloudErrorBody {
    pub fn classify(&self) -> Option<SyncError> {
        match self.code.as_str() {
            "QuotaExceeded" => Some(SyncError::QuotaExceeded),
            "AccountSuspended" => Some(SyncError::AccountSuspended),
            "InvalidAccessKeyId" => Some(SyncError::AccessKeyNotFound),
            "SignatureDoesNotMatch" => Some(SyncError::SignatureMismatch),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_quota_exceeded() {
        let body: ButtercloudErrorBody =
            serde_json::from_str(r#"{"Code":"QuotaExceeded","Message":"over plan limit"}"#)
                .unwrap();
        assert!(matches!(body.classify(), Some(SyncError::QuotaExceeded)));
    }

    #[test]
    fn classifies_account_suspended() {
        let body: ButtercloudErrorBody =
            serde_json::from_str(r#"{"Code":"AccountSuspended"}"#).unwrap();
        assert!(matches!(body.classify(), Some(SyncError::AccountSuspended)));
    }

    #[test]
    fn classifies_invalid_access_key_id() {
        let body: ButtercloudErrorBody =
            serde_json::from_str(r#"{"Code":"InvalidAccessKeyId"}"#).unwrap();
        assert!(matches!(body.classify(), Some(SyncError::AccessKeyNotFound)));
    }

    #[test]
    fn classifies_signature_does_not_match() {
        let body: ButtercloudErrorBody =
            serde_json::from_str(r#"{"Code":"SignatureDoesNotMatch"}"#).unwrap();
        assert!(matches!(body.classify(), Some(SyncError::SignatureMismatch)));
    }

    #[test]
    fn unknown_code_is_not_classified() {
        let body: ButtercloudErrorBody =
            serde_json::from_str(r#"{"Code":"AccessDenied"}"#).unwrap();
        assert!(body.classify().is_none());
    }
}
