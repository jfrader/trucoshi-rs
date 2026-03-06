use super::types::{Tournament, TournamentEntry, TournamentStatus};
use sqlx::PgPool;
use time::OffsetDateTime;

#[derive(Clone)]
pub struct TournamentsRepo {
    pool: PgPool,
}

impl TournamentsRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_tournament(
        &self,
        owner_user_id: Option<i64>,
        name: &str,
        max_players: i32,
        starts_at: Option<OffsetDateTime>,
    ) -> anyhow::Result<Tournament> {
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            name: String,
            status: String,
            owner_user_id: Option<i64>,
            max_players: i32,
            starts_at: Option<OffsetDateTime>,
            created_at: OffsetDateTime,
            updated_at: OffsetDateTime,
        }

        let rec: Row = sqlx::query_as(
            r#"
            INSERT INTO tournaments (owner_user_id, name, max_players, starts_at)
            VALUES ($1, $2, $3, $4)
            RETURNING id, name, status, owner_user_id, max_players, starts_at, created_at, updated_at
            "#,
        )
        .bind(owner_user_id)
        .bind(name)
        .bind(max_players)
        .bind(starts_at)
        .fetch_one(&self.pool)
        .await?;

        Ok(Tournament {
            id: rec.id,
            name: rec.name,
            status: parse_status(&rec.status),
            owner_user_id: rec.owner_user_id,
            max_players: rec.max_players,
            starts_at: rec.starts_at,
            created_at: rec.created_at,
            updated_at: rec.updated_at,
        })
    }

    /// List tournaments visible to a viewer.
    ///
    /// - Everyone can see `OPEN` tournaments.
    /// - If `viewer_user_id` is present, they can also see their own `DRAFT` tournaments.
    pub async fn list_visible_tournaments(
        &self,
        limit: i64,
        viewer_user_id: Option<i64>,
    ) -> anyhow::Result<Vec<Tournament>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            name: String,
            status: String,
            owner_user_id: Option<i64>,
            max_players: i32,
            starts_at: Option<OffsetDateTime>,
            created_at: OffsetDateTime,
            updated_at: OffsetDateTime,
        }

        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT id, name, status, owner_user_id, max_players, starts_at, created_at, updated_at
            FROM tournaments
            WHERE status = 'OPEN'
               OR ($2 IS NOT NULL AND status = 'DRAFT' AND owner_user_id = $2)
            ORDER BY created_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .bind(viewer_user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|rec| Tournament {
                id: rec.id,
                name: rec.name,
                status: parse_status(&rec.status),
                owner_user_id: rec.owner_user_id,
                max_players: rec.max_players,
                starts_at: rec.starts_at,
                created_at: rec.created_at,
                updated_at: rec.updated_at,
            })
            .collect())
    }

    pub async fn get_tournament(&self, id: i64) -> anyhow::Result<Option<Tournament>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            name: String,
            status: String,
            owner_user_id: Option<i64>,
            max_players: i32,
            starts_at: Option<OffsetDateTime>,
            created_at: OffsetDateTime,
            updated_at: OffsetDateTime,
        }

        let rec: Option<Row> = sqlx::query_as(
            r#"
            SELECT id, name, status, owner_user_id, max_players, starts_at, created_at, updated_at
            FROM tournaments
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(rec.map(|rec| Tournament {
            id: rec.id,
            name: rec.name,
            status: parse_status(&rec.status),
            owner_user_id: rec.owner_user_id,
            max_players: rec.max_players,
            starts_at: rec.starts_at,
            created_at: rec.created_at,
            updated_at: rec.updated_at,
        }))
    }

    pub async fn update_tournament_status(
        &self,
        id: i64,
        status: TournamentStatus,
    ) -> anyhow::Result<Option<Tournament>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            name: String,
            status: String,
            owner_user_id: Option<i64>,
            max_players: i32,
            starts_at: Option<OffsetDateTime>,
            created_at: OffsetDateTime,
            updated_at: OffsetDateTime,
        }

        let rec: Option<Row> = sqlx::query_as(
            r#"
            UPDATE tournaments
            SET status = $2
            WHERE id = $1
            RETURNING id, name, status, owner_user_id, max_players, starts_at, created_at, updated_at
            "#,
        )
        .bind(id)
        .bind(status.as_str())
        .fetch_optional(&self.pool)
        .await?;

        Ok(rec.map(|rec| Tournament {
            id: rec.id,
            name: rec.name,
            status: parse_status(&rec.status),
            owner_user_id: rec.owner_user_id,
            max_players: rec.max_players,
            starts_at: rec.starts_at,
            created_at: rec.created_at,
            updated_at: rec.updated_at,
        }))
    }

    pub async fn count_entries(&self, tournament_id: i64) -> anyhow::Result<i64> {
        let (count,): (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(1)
            FROM tournament_entries
            WHERE tournament_id = $1
            "#,
        )
        .bind(tournament_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    pub async fn list_entries(
        &self,
        tournament_id: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<TournamentEntry>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tournament_id: i64,
            user_id: Option<i64>,
            display_name: String,
            created_at: OffsetDateTime,
        }

        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT id, tournament_id, user_id, display_name, created_at
            FROM tournament_entries
            WHERE tournament_id = $1
            ORDER BY created_at ASC
            LIMIT $2
            "#,
        )
        .bind(tournament_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|rec| TournamentEntry {
                id: rec.id,
                tournament_id: rec.tournament_id,
                user_id: rec.user_id,
                display_name: rec.display_name,
                created_at: rec.created_at,
            })
            .collect())
    }

    pub async fn add_entry(
        &self,
        tournament_id: i64,
        user_id: Option<i64>,
        display_name: &str,
    ) -> anyhow::Result<TournamentEntry> {
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tournament_id: i64,
            user_id: Option<i64>,
            display_name: String,
            created_at: OffsetDateTime,
        }

        let rec: Row = sqlx::query_as(
            r#"
            INSERT INTO tournament_entries (tournament_id, user_id, display_name)
            VALUES ($1, $2, $3)
            RETURNING id, tournament_id, user_id, display_name, created_at
            "#,
        )
        .bind(tournament_id)
        .bind(user_id)
        .bind(display_name)
        .fetch_one(&self.pool)
        .await?;

        Ok(TournamentEntry {
            id: rec.id,
            tournament_id: rec.tournament_id,
            user_id: rec.user_id,
            display_name: rec.display_name,
            created_at: rec.created_at,
        })
    }
}

fn parse_status(s: &str) -> TournamentStatus {
    match s {
        "DRAFT" => TournamentStatus::Draft,
        "OPEN" => TournamentStatus::Open,
        "STARTED" => TournamentStatus::Started,
        "FINISHED" => TournamentStatus::Finished,
        "CANCELLED" => TournamentStatus::Cancelled,
        _ => TournamentStatus::Draft,
    }
}
