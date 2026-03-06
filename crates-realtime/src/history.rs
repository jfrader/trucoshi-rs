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

    PlayerLeft {
        match_id: String,
        seat_idx: u8,
        team_idx: u8,
        user_id: i64,
        display_name: String,
        /// Best-effort reason string (e.g. "client_leave" or "disconnect").
        reason: String,
    },

    SpectatorJoined {
        match_id: String,
        user_id: i64,
    },

    SpectatorLeft {
        match_id: String,
        user_id: i64,
        /// Best-effort reason string (e.g. "client_leave" or "disconnect").
        reason: String,
    },

    /// Generic append-only event for actions that occurred during gameplay.
    ///
    /// Stored as a row in `game_match_events` with `type=ty` and `data` as provided.
    GameAction {
        match_id: String,
        actor_seat_idx: Option<u8>,
        actor_team_idx: Option<u8>,
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
