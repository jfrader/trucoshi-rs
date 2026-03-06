//! Persistence schema helpers for game history.
//!
//! Tables are created by migration `0003_game_history.sql`.
//!
//! This is intentionally append-only to support analytics / ML training.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde()]
pub struct GameMatch {
    pub id: i64,
    pub created_at: OffsetDateTime,
    pub finished_at: Option<OffsetDateTime>,
    pub server_version: Option<String>,
    pub protocol_version: Option<i32>,
    pub rng_seed: Option<i64>,
    pub options: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde()]
pub struct GameMatchPlayer {
    pub id: i64,
    pub match_id: i64,
    pub seat_idx: i32,
    pub team_idx: i32,
    pub user_id: Option<i64>,
    pub display_name: String,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde()]
pub struct GameMatchEvent {
    pub id: i64,
    pub match_id: i64,
    pub seq: i64,
    pub created_at: OffsetDateTime,
    pub actor_seat_idx: Option<i32>,
    pub actor_user_id: Option<i64>,
    pub r#type: String,
    pub data: serde_json::Value,
}
