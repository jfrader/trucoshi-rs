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

impl crate::Store {
    /// Insert a new `game_matches` row and return the DB match id.
    pub async fn gh_create_match(
        &self,
        server_version: Option<&str>,
        protocol_version: Option<i32>,
        rng_seed: Option<i64>,
        options: &serde_json::Value,
    ) -> anyhow::Result<i64> {
        let id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO game_matches (server_version, protocol_version, rng_seed, options)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
        )
        .bind(server_version)
        .bind(protocol_version)
        .bind(rng_seed)
        .bind(options)
        .fetch_one(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn gh_add_player(
        &self,
        match_id: i64,
        seat_idx: i32,
        team_idx: i32,
        user_id: Option<i64>,
        display_name: &str,
    ) -> anyhow::Result<i64> {
        let id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO game_match_players (match_id, seat_idx, team_idx, user_id, display_name)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
        )
        .bind(match_id)
        .bind(seat_idx)
        .bind(team_idx)
        .bind(user_id)
        .bind(display_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(id)
    }

    pub async fn gh_append_event(
        &self,
        match_id: i64,
        seq: i64,
        actor_seat_idx: Option<i32>,
        actor_user_id: Option<i64>,
        r#type: &str,
        data: &serde_json::Value,
    ) -> anyhow::Result<i64> {
        let id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO game_match_events (match_id, seq, actor_seat_idx, actor_user_id, type, data)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
            "#,
        )
        .bind(match_id)
        .bind(seq)
        .bind(actor_seat_idx)
        .bind(actor_user_id)
        .bind(r#type)
        .bind(data)
        .fetch_one(&self.pool)
        .await?;

        Ok(id)
    }

    /// Mark a match finished (idempotent).
    pub async fn gh_finish_match(&self, match_id: i64) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE game_matches
            SET finished_at = now()
            WHERE id = $1
              AND finished_at IS NULL
            "#,
        )
        .bind(match_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
