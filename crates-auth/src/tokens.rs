use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};

#[derive(Debug, Clone)]
pub struct TokenConfig {
    pub issuer: String,
    pub audience: String,
    pub access_ttl: Duration,
    pub refresh_ttl: Duration,
    /// HMAC secret (base64 or raw). Keep it long.
    pub jwt_hs256_secret: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: String,
    pub exp: i64,
    pub iat: i64,
}

pub fn now_ts() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

pub fn exp_ts(ttl: Duration) -> i64 {
    (OffsetDateTime::now_utc() + ttl).unix_timestamp()
}
