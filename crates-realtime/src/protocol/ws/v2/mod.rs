//! WebSocket protocol (v2).
//!
//! This module intentionally contains **only** the v2 schema. We do not keep legacy wire
//! compatibility in this crate; if/when we need a breaking change, we add a new versioned module
//! (v3, v4, ...).

mod envelope;
mod error;
pub mod messages;

#[cfg(test)]
mod tests;

#[cfg(feature = "json-schema")]
pub mod json_schema;

pub mod schema;

pub use envelope::{WS_PROTOCOL_VERSION, WsInMessage, WsOutMessage, WsVersion};
pub use error::ErrorPayload;
pub use messages::{
    ActiveMatchesSnapshotData, C2sMessage, ChatJoinData, ChatMessageData, ChatSayData,
    ChatSnapshotData, GamePlayCardData, GameSayData, GameSnapshotData, GameUpdateData, HelloData,
    LobbyMatchRemoveData, LobbyMatchUpsertData, LobbySnapshotData, LobbyStatsData, MatchCreateData,
    MatchJoinData, MatchLeftData, MatchPauseVoteData, MatchReadyData, MatchRefData,
    MatchSnapshotData, MatchUpdateData, PingData, PongData, S2cMessage,
};
