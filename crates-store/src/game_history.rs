//! Persistence schema helpers for game history.
//!
//! Tables are created by migration `0003_game_history.sql`.
//!
//! This is intentionally append-only to support analytics / ML training.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
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

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
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

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
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

    pub async fn gh_get_match(&self, match_id: i64) -> anyhow::Result<Option<GameMatch>> {
        let rec = sqlx::query_as::<_, GameMatch>(
            r#"
            SELECT id, created_at, finished_at, server_version, protocol_version, rng_seed, options
            FROM game_matches
            WHERE id = $1
            "#,
        )
        .bind(match_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(rec)
    }

    pub async fn gh_list_players(&self, match_id: i64) -> anyhow::Result<Vec<GameMatchPlayer>> {
        let rows = sqlx::query_as::<_, GameMatchPlayer>(
            r#"
            SELECT id, match_id, seat_idx, team_idx, user_id, display_name, created_at
            FROM game_match_players
            WHERE match_id = $1
            ORDER BY seat_idx ASC
            "#,
        )
        .bind(match_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn gh_list_events(
        &self,
        match_id: i64,
        limit: i64,
        after_seq: Option<i64>,
    ) -> anyhow::Result<Vec<GameMatchEvent>> {
        let limit = limit.clamp(1, 1_000);

        let rows = sqlx::query_as::<_, GameMatchEvent>(
            r#"
            SELECT id, match_id, seq, created_at, actor_seat_idx, actor_user_id, type, data
            FROM game_match_events
            WHERE match_id = $1
              AND ($2::BIGINT IS NULL OR seq > $2)
            ORDER BY seq ASC
            LIMIT $3
            "#,
        )
        .bind(match_id)
        .bind(after_seq)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use crate::Store;
    use serde_json::json;

    #[sqlx::test(migrations = "../migrations")]
    async fn gh_read_helpers_return_rows(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = Store { pool };

        let match_id = store
            .gh_create_match(
                Some("0.1.0"),
                Some(2),
                Some(42),
                &json!({ "ws_match_id": "m1" }),
            )
            .await?;

        store.gh_add_player(match_id, 0, 0, None, "p1").await?;
        store.gh_add_player(match_id, 1, 1, None, "guest").await?;

        store
            .gh_append_event(
                match_id,
                0,
                Some(0),
                None,
                "match.create",
                &json!({ "ws_match_id": "m1" }),
            )
            .await?;
        store
            .gh_append_event(
                match_id,
                1,
                Some(1),
                None,
                "match.join",
                &json!({ "ws_match_id": "m1" }),
            )
            .await?;

        store.gh_finish_match(match_id).await?;

        let summary = store.gh_get_match(match_id).await?.expect("match row");
        assert_eq!(summary.server_version.as_deref(), Some("0.1.0"));
        assert!(summary.finished_at.is_some());

        let players = store.gh_list_players(match_id).await?;
        assert_eq!(players.len(), 2);
        assert_eq!(players[0].seat_idx, 0);
        assert_eq!(players[1].display_name, "guest");

        let events = store.gh_list_events(match_id, 10, None).await?;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].seq, 0);
        assert_eq!(events[1].seq, 1);

        Ok(())
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn gh_list_events_supports_after_seq(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = Store { pool };

        let match_id = store
            .gh_create_match(
                Some("0.1.0"),
                Some(2),
                Some(7),
                &json!({ "ws_match_id": "m2" }),
            )
            .await?;

        for seq in 0..3 {
            store
                .gh_append_event(
                    match_id,
                    seq,
                    None,
                    None,
                    "match.event",
                    &json!({ "seq": seq }),
                )
                .await?;
        }

        let first_page = store.gh_list_events(match_id, 2, None).await?;
        assert_eq!(first_page.len(), 2);
        assert_eq!(first_page[0].seq, 0);
        assert_eq!(first_page[1].seq, 1);

        let after = first_page.last().map(|e| e.seq).unwrap();
        let second_page = store.gh_list_events(match_id, 2, Some(after)).await?;
        assert_eq!(second_page.len(), 1);
        assert_eq!(second_page[0].seq, 2);

        Ok(())
    }
}
