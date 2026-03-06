use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryPlayer {
    pub seat_idx: u8,
    pub team_idx: u8,
    pub user_id: i64,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GameHistoryEvent {
    MatchCreated {
        match_id: String,
        server_version: String,
        protocol_version: i32,
        rng_seed: i64,
        options: Value,
        owner: HistoryPlayer,
    },
    PlayerJoined {
        match_id: String,
        seat_idx: u8,
        team_idx: u8,
        user_id: i64,
        display_name: String,
    },
    MatchStarted {
        match_id: String,
    },
    MatchFinished {
        match_id: String,
        team_points: [u8; 2],
        reason: String,
    },
}
