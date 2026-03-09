use hmac::{Hmac, Mac};
use once_cell::sync::Lazy;
use rand::{Rng, rngs::OsRng};
use sha2::Sha256;

const DEFAULT_WORD_COUNT: usize = 5;

type HmacSha256 = Hmac<Sha256>;

static WORDS: Lazy<Vec<&'static str>> = Lazy::new(|| {
    let raw = include_str!("words/spanish.json");
    serde_json::from_str::<Vec<&'static str>>(raw).expect("invalid seed word list")
});

/// Generate a random seed phrase consisting of `word_count` spanish words.
/// Defaults to five words to match the legacy lightning-accounts behavior.
pub fn generate_seed_phrase(word_count: Option<usize>) -> String {
    let count = word_count.unwrap_or(DEFAULT_WORD_COUNT).max(1);
    (0..count)
        .map(|_| {
            let idx = OsRng.gen_range(0..WORDS.len());
            WORDS[idx]
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Hash a normalized, lower-case seed phrase with an application secret.
pub fn hash_seed_phrase(seed_phrase: &str, secret: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC can take key of any size");
    mac.update(seed_phrase.as_bytes());
    let out = mac.finalize().into_bytes();
    hex::encode(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_expected_word_count() {
        let phrase = generate_seed_phrase(Some(5));
        assert_eq!(phrase.split_whitespace().count(), 5);
    }

    #[test]
    fn hashes_is_case_sensitive_input() {
        let secret = b"secret";
        let lower = hash_seed_phrase("uno dos tres cuatro cinco", secret);
        let upper = hash_seed_phrase("UNO DOS TRES CUATRO CINCO", secret);
        assert_ne!(lower, upper);
        let normalized = hash_seed_phrase("uno dos tres cuatro cinco", secret);
        assert_eq!(lower, normalized);
    }
}
