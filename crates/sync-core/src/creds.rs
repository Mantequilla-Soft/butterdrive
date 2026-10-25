use crate::config::AccountConfig;
use crate::error::SyncError;

const SERVICE: &str = "com.menosoft.butterdrive";
const SECRET_KEY_USER: &str = "secret-key";

/// The access key, bucket, and endpoint aren't secret on their own, but we
/// keep the whole account config behind the OS keychain anyway rather than
/// splitting it across a plaintext config file + keychain — simpler, and
/// nothing about this account belongs in a dotfile.
pub fn save(cfg: &AccountConfig) -> Result<(), SyncError> {
    let entry = keyring::Entry::new(SERVICE, SECRET_KEY_USER)
        .map_err(|e| SyncError::Other(e.to_string()))?;
    let payload = serde_json::json!({
        "endpoint": cfg.endpoint,
        "bucket": cfg.bucket,
        "access_key": cfg.access_key,
        "secret_key": cfg.secret_key,
    });
    entry
        .set_password(&payload.to_string())
        .map_err(|e| SyncError::Other(e.to_string()))
}

pub fn load() -> Result<Option<AccountConfig>, SyncError> {
    let entry = keyring::Entry::new(SERVICE, SECRET_KEY_USER)
        .map_err(|e| SyncError::Other(e.to_string()))?;
    match entry.get_password() {
        Ok(raw) => {
            let v: serde_json::Value =
                serde_json::from_str(&raw).map_err(|e| SyncError::Other(e.to_string()))?;
            Ok(Some(AccountConfig {
                endpoint: v["endpoint"].as_str().unwrap_or_default().to_string(),
                bucket: v["bucket"].as_str().unwrap_or_default().to_string(),
                access_key: v["access_key"].as_str().unwrap_or_default().to_string(),
                secret_key: v["secret_key"].as_str().unwrap_or_default().to_string(),
            }))
        }
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(SyncError::Other(e.to_string())),
    }
}

pub fn clear() -> Result<(), SyncError> {
    let entry = keyring::Entry::new(SERVICE, SECRET_KEY_USER)
        .map_err(|e| SyncError::Other(e.to_string()))?;
    match entry.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(e) => Err(SyncError::Other(e.to_string())),
    }
}
