use std::fmt;

/// Buttercloud's two documented custom 403 codes, plus the generic S3 error
/// surface. Branch on `Quota`/`Suspended` for user-facing messages; treat
/// `Other` as a normal transfer failure.
#[derive(Debug, Clone)]
pub enum SyncError {
    /// Plan storage limit hit (`Code: 'QuotaExceeded'`).
    QuotaExceeded,
    /// Invoice overdue past the grace period (`Code: 'AccountSuspended'`).
    /// Reads/deletes of the user's own data still work; new uploads don't.
    AccountSuspended,
    /// Credentials rejected by the gateway.
    InvalidCredentials,
    /// Anything else: transport failure, unexpected status, bad config, etc.
    Other(String),
}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncError::QuotaExceeded => write!(f, "storage quota exceeded"),
            SyncError::AccountSuspended => write!(f, "account suspended (overdue invoice)"),
            SyncError::InvalidCredentials => write!(f, "invalid credentials"),
            SyncError::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for SyncError {}

/// Buttercloud's custom error body on its special 403s, e.g.
/// `{"Code":"QuotaExceeded","Message":"..."}`. Exact shape isn't documented
/// upstream — this is a best-effort reading of the handoff doc's `Code:
/// '...'` description, to be confirmed against a real account.
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
    fn unknown_code_is_not_classified() {
        let body: ButtercloudErrorBody =
            serde_json::from_str(r#"{"Code":"AccessDenied"}"#).unwrap();
        assert!(body.classify().is_none());
    }
}
