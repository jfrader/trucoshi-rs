use crate::tokens::{Claims, TokenConfig, exp_ts, now_ts};
use anyhow::Context;
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation, decode, encode,
};

pub fn issue_access_jwt(cfg: &TokenConfig, user_id: i64) -> anyhow::Result<String> {
    let claims = Claims {
        sub: user_id.to_string(),
        iss: cfg.issuer.clone(),
        aud: cfg.audience.clone(),
        iat: now_ts(),
        exp: exp_ts(cfg.access_ttl),
    };

    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(&cfg.jwt_hs256_secret),
    )
    .context("encode access jwt")
}

pub fn decode_access_jwt(cfg: &TokenConfig, token: &str) -> anyhow::Result<TokenData<Claims>> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[cfg.issuer.clone()]);
    validation.set_audience(&[cfg.audience.clone()]);

    decode::<Claims>(
        token,
        &DecodingKey::from_secret(&cfg.jwt_hs256_secret),
        &validation,
    )
    .context("decode access jwt")
}
