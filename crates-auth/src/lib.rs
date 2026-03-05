pub mod jwt;
pub mod tokens;

pub mod refresh {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use rand::{RngCore, rngs::OsRng};
    use sha2::{Digest, Sha256};

    pub fn new_refresh_token() -> String {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        URL_SAFE_NO_PAD.encode(bytes)
    }

    pub fn hash_refresh_token(token: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        let out = hasher.finalize();
        hex::encode(out)
    }
}

pub mod password {
    use argon2::{
        Argon2,
        password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    };
    use rand_core::OsRng;

    pub fn hash_password(password: &str) -> anyhow::Result<String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        let hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?
            .to_string();
        Ok(hash)
    }

    pub fn verify_password(hash: &str, password: &str) -> anyhow::Result<bool> {
        let parsed = PasswordHash::new(hash).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok())
    }
}
