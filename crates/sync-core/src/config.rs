/// Everything needed to talk to one Buttercloud account. The bucket name is
/// pasted in during onboarding rather than discovered via `GET
/// /api/v1/bucket`, because that route is JWT-only and this client is
/// SigV4-only end to end (see the handoff doc's "use SigV4, not JWT"
/// guidance).
#[derive(Debug, Clone)]
pub struct AccountConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
}

impl AccountConfig {
    pub fn production(bucket: String, access_key: String, secret_key: String) -> Self {
        Self {
            endpoint: "https://s3.3speak.tv".to_string(),
            bucket,
            access_key,
            secret_key,
        }
    }
}
