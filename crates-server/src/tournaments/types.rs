use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TournamentStatus {
    Draft,
    Open,
    Started,
    Finished,
    Cancelled,
}

impl TournamentStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TournamentStatus::Draft => "DRAFT",
            TournamentStatus::Open => "OPEN",
            TournamentStatus::Started => "STARTED",
            TournamentStatus::Finished => "FINISHED",
            TournamentStatus::Cancelled => "CANCELLED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tournament {
    pub id: i64,
    pub name: String,
    pub status: TournamentStatus,
    pub owner_user_id: Option<i64>,
    pub max_players: i32,
    pub starts_at: Option<OffsetDateTime>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TournamentEntry {
    pub id: i64,
    pub tournament_id: i64,
    pub user_id: Option<i64>,
    pub display_name: String,
    pub created_at: OffsetDateTime,
}
