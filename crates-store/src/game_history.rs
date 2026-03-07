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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PlayerMatchRow {
    pub match_id: i64,
    pub created_at: OffsetDateTime,
    pub finished_at: Option<OffsetDateTime>,
    pub options: serde_json::Value,
    pub seat_idx: i32,
    pub team_idx: i32,
    pub finish_data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, sqlx::FromRow)]
pub struct PlayerAggregateRow {
    pub matches_played: i64,
    pub matches_finished: i64,
    pub matches_won: i64,
    pub points_for: i64,
    pub points_against: i64,
    pub last_played_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LeaderboardRow {
    pub user_id: i64,
    pub name: String,
    pub avatar_url: Option<String>,
    pub twitter_handle: Option<String>,
    pub matches_played: i64,
    pub matches_finished: i64,
    pub matches_won: i64,
    pub points_for: i64,
    pub points_against: i64,
    pub last_played_at: Option<OffsetDateTime>,
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

    pub async fn gh_list_player_matches(
        &self,
        user_id: i64,
        limit: i64,
        offset: i64,
    ) -> anyhow::Result<Vec<PlayerMatchRow>> {
        let limit = limit.clamp(1, 200);
        let offset = offset.max(0);

        let rows = sqlx::query_as::<_, PlayerMatchRow>(
            r#"
            SELECT
                gm.id AS match_id,
                gm.created_at,
                gm.finished_at,
                gm.options,
                gmp.seat_idx,
                gmp.team_idx,
                finish_event.data AS finish_data
            FROM game_match_players gmp
            JOIN game_matches gm ON gm.id = gmp.match_id
            LEFT JOIN LATERAL (
                SELECT data
                FROM game_match_events
                WHERE match_id = gm.id
                  AND type = 'match.finish'
                ORDER BY seq DESC
                LIMIT 1
            ) AS finish_event ON TRUE
            WHERE gmp.user_id = $1
            ORDER BY gm.created_at DESC, gm.id DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(user_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn gh_player_aggregates(&self, user_id: i64) -> anyhow::Result<PlayerAggregateRow> {
        let row = sqlx::query_as::<_, PlayerAggregateRow>(
            r#"
            SELECT
                COUNT(*)::BIGINT AS matches_played,
                COUNT(*) FILTER (WHERE finish.player_points IS NOT NULL)::BIGINT AS matches_finished,
                COALESCE(SUM(CASE
                    WHEN finish.player_points IS NOT NULL
                         AND finish.winning_points IS NOT NULL
                         AND finish.player_points = finish.winning_points
                    THEN 1 ELSE 0 END), 0)::BIGINT AS matches_won,
                COALESCE(SUM(COALESCE(finish.player_points, 0)), 0)::BIGINT AS points_for,
                COALESCE(SUM(CASE
                    WHEN finish.player_points IS NULL OR finish.total_points IS NULL THEN 0
                    ELSE finish.total_points - finish.player_points
                END), 0)::BIGINT AS points_against,
                MAX(gm.created_at) AS last_played_at
            FROM game_match_players gmp
            JOIN game_matches gm ON gm.id = gmp.match_id
            LEFT JOIN LATERAL (
                SELECT
                    data,
                    CASE
                        WHEN data IS NOT NULL AND data ? 'team_points'
                        THEN ((data->'team_points'->(gmp.team_idx)::int)::text)::BIGINT
                        ELSE NULL
                    END AS player_points,
                    CASE
                        WHEN data IS NOT NULL AND data ? 'team_points'
                        THEN (
                            SELECT MAX((value)::BIGINT)
                            FROM jsonb_array_elements_text(data->'team_points') AS value
                        )
                        ELSE NULL
                    END AS winning_points,
                    CASE
                        WHEN data IS NOT NULL AND data ? 'team_points'
                        THEN (
                            SELECT SUM((value)::BIGINT)
                            FROM jsonb_array_elements_text(data->'team_points') AS value
                        )
                        ELSE NULL
                    END AS total_points
                FROM game_match_events
                WHERE match_id = gm.id
                  AND type = 'match.finish'
                ORDER BY seq DESC
                LIMIT 1
            ) AS finish ON TRUE
            WHERE gmp.user_id = $1
            "#,
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.unwrap_or_default())
    }

    pub async fn gh_leaderboard(
        &self,
        min_finished: i64,
        limit: i64,
        offset: i64,
    ) -> anyhow::Result<Vec<LeaderboardRow>> {
        let min_finished = min_finished.max(0);
        let limit = limit.clamp(1, 200);
        let offset = offset.max(0);

        let rows = sqlx::query_as::<_, LeaderboardRow>(
            r#"
            WITH player_matches AS (
                SELECT
                    gmp.user_id,
                    gm.id AS match_id,
                    gm.created_at,
                    CASE
                        WHEN finish.data IS NOT NULL AND finish.data ? 'team_points'
                        THEN ((finish.data->'team_points'->(gmp.team_idx)::int)::text)::BIGINT
                        ELSE NULL
                    END AS player_points,
                    CASE
                        WHEN finish.data IS NOT NULL AND finish.data ? 'team_points'
                        THEN (
                            SELECT MAX((value)::BIGINT)
                            FROM jsonb_array_elements_text(finish.data->'team_points') AS value
                        )
                        ELSE NULL
                    END AS winning_points,
                    CASE
                        WHEN finish.data IS NOT NULL AND finish.data ? 'team_points'
                        THEN (
                            SELECT SUM((value)::BIGINT)
                            FROM jsonb_array_elements_text(finish.data->'team_points') AS value
                        )
                        ELSE NULL
                    END AS total_points
                FROM game_match_players gmp
                JOIN game_matches gm ON gm.id = gmp.match_id
                LEFT JOIN LATERAL (
                    SELECT data
                    FROM game_match_events
                    WHERE match_id = gm.id
                      AND type = 'match.finish'
                    ORDER BY seq DESC
                    LIMIT 1
                ) AS finish ON TRUE
                WHERE gmp.user_id IS NOT NULL
            ), per_user AS (
                SELECT
                    pm.user_id,
                    COUNT(*)::BIGINT AS matches_played,
                    COUNT(*) FILTER (WHERE pm.player_points IS NOT NULL)::BIGINT AS matches_finished,
                    COALESCE(SUM(CASE
                        WHEN pm.player_points IS NOT NULL
                             AND pm.winning_points IS NOT NULL
                             AND pm.player_points = pm.winning_points
                        THEN 1 ELSE 0 END), 0)::BIGINT AS matches_won,
                    COALESCE(SUM(COALESCE(pm.player_points, 0)), 0)::BIGINT AS points_for,
                    COALESCE(SUM(CASE
                        WHEN pm.player_points IS NULL OR pm.total_points IS NULL THEN 0
                        ELSE pm.total_points - pm.player_points
                    END), 0)::BIGINT AS points_against,
                    MAX(pm.created_at) AS last_played_at
                FROM player_matches pm
                GROUP BY pm.user_id
            )
            SELECT
                u.id AS user_id,
                u.name,
                u.avatar_url,
                u.twitter_handle,
                per_user.matches_played,
                per_user.matches_finished,
                per_user.matches_won,
                per_user.points_for,
                per_user.points_against,
                per_user.last_played_at
            FROM per_user
            JOIN users u ON u.id = per_user.user_id
            WHERE per_user.matches_finished >= $1
            ORDER BY per_user.matches_won DESC,
                     per_user.matches_finished DESC,
                     per_user.last_played_at DESC,
                     u.id ASC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(min_finished)
        .bind(limit)
        .bind(offset)
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

    async fn insert_user(store: &Store, email: &str, name: &str) -> anyhow::Result<i64> {
        let user_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(email)
        .bind("hash")
        .bind(name)
        .fetch_one(&store.pool)
        .await?;

        Ok(user_id)
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn gh_player_aggregates_and_match_list(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = Store { pool };

        let alice_id = insert_user(&store, "alice@example.com", "Alice").await?;
        let bob_id = insert_user(&store, "bob@example.com", "Bob").await?;

        // Match A: Alice wins
        let match_a = store
            .gh_create_match(
                Some("0.2.0"),
                Some(2),
                Some(11),
                &json!({ "ws_match_id": "match-a" }),
            )
            .await?;
        store
            .gh_add_player(match_a, 0, 0, Some(alice_id), "Alice")
            .await?;
        store
            .gh_add_player(match_a, 1, 1, Some(bob_id), "Bob")
            .await?;
        store
            .gh_append_event(
                match_a,
                0,
                None,
                None,
                "match.finish",
                &json!({ "team_points": [30, 10], "reason": "score_reached" }),
            )
            .await?;
        store.gh_finish_match(match_a).await?;

        // Match B: Bob wins
        let match_b = store
            .gh_create_match(
                Some("0.2.0"),
                Some(2),
                Some(22),
                &json!({ "ws_match_id": "match-b" }),
            )
            .await?;
        store
            .gh_add_player(match_b, 0, 0, Some(alice_id), "Alice")
            .await?;
        store
            .gh_add_player(match_b, 1, 1, Some(bob_id), "Bob")
            .await?;
        store
            .gh_append_event(
                match_b,
                0,
                None,
                None,
                "match.finish",
                &json!({ "team_points": [12, 30], "reason": "score_reached" }),
            )
            .await?;
        store.gh_finish_match(match_b).await?;

        let totals = store.gh_player_aggregates(alice_id).await?;
        assert_eq!(totals.matches_played, 2);
        assert_eq!(totals.matches_finished, 2);
        assert_eq!(totals.matches_won, 1);
        assert_eq!(totals.points_for, 30 + 12);
        assert_eq!(totals.points_against, 10 + 30);
        assert!(totals.last_played_at.is_some());

        let matches = store.gh_list_player_matches(alice_id, 10, 0).await?;
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].options["ws_match_id"], json!("match-b"));
        assert_eq!(matches[1].options["ws_match_id"], json!("match-a"));

        Ok(())
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn gh_leaderboard_orders_by_wins(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = Store { pool };

        let alice_id = insert_user(&store, "alice@example.com", "Alice").await?;
        let bob_id = insert_user(&store, "bob@example.com", "Bob").await?;
        let carol_id = insert_user(&store, "carol@example.com", "Carol").await?;

        // Match 1: Alice beats Bob
        let match1 = store
            .gh_create_match(
                Some("0.2.0"),
                Some(2),
                Some(33),
                &json!({ "ws_match_id": "m1" }),
            )
            .await?;
        store
            .gh_add_player(match1, 0, 0, Some(alice_id), "Alice")
            .await?;
        store
            .gh_add_player(match1, 1, 1, Some(bob_id), "Bob")
            .await?;
        store
            .gh_append_event(
                match1,
                0,
                None,
                None,
                "match.finish",
                &json!({ "team_points": [30, 20], "reason": "score" }),
            )
            .await?;
        store.gh_finish_match(match1).await?;

        // Match 2: Bob beats Alice
        let match2 = store
            .gh_create_match(
                Some("0.2.0"),
                Some(2),
                Some(44),
                &json!({ "ws_match_id": "m2" }),
            )
            .await?;
        store
            .gh_add_player(match2, 0, 0, Some(alice_id), "Alice")
            .await?;
        store
            .gh_add_player(match2, 1, 1, Some(bob_id), "Bob")
            .await?;
        store
            .gh_append_event(
                match2,
                0,
                None,
                None,
                "match.finish",
                &json!({ "team_points": [10, 30], "reason": "score" }),
            )
            .await?;
        store.gh_finish_match(match2).await?;

        // Match 3: Bob beats Carol
        let match3 = store
            .gh_create_match(
                Some("0.2.0"),
                Some(2),
                Some(55),
                &json!({ "ws_match_id": "m3" }),
            )
            .await?;
        store
            .gh_add_player(match3, 0, 0, Some(bob_id), "Bob")
            .await?;
        store
            .gh_add_player(match3, 1, 1, Some(carol_id), "Carol")
            .await?;
        store
            .gh_append_event(
                match3,
                0,
                None,
                None,
                "match.finish",
                &json!({ "team_points": [30, 12], "reason": "score" }),
            )
            .await?;
        store.gh_finish_match(match3).await?;

        let rows = store.gh_leaderboard(1, 10, 0).await?;
        assert_eq!(rows[0].user_id, bob_id);
        assert_eq!(rows[1].user_id, alice_id);

        Ok(())
    }
}
