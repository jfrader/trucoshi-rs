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

    pub async fn list_open_tournaments(&self, limit: i64) -> anyhow::Result<Vec<Tournament>> {
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
            WHERE status IN ('OPEN','DRAFT')
            ORDER BY created_at DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
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
