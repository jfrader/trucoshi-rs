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

    /// Generic append-only event for actions that occurred during gameplay.
    ///
    /// Stored as a row in `game_match_events` with `type=ty` and `data` as provided.
    GameAction {
        match_id: String,
        actor_seat_idx: u8,
        actor_team_idx: u8,
        actor_user_id: i64,
        ty: String,
        data: Value,
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
