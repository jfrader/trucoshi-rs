use serde::{Deserialize, Serialize};

#[cfg(feature = "json-schema")]
use schemars::JsonSchema;

use super::ErrorPayload;
use super::schema::{
    GameCommand, LobbyMatch, MatchOptions, Maybe, PrivatePlayer, PublicChatMessage, PublicChatRoom,
    PublicGameState, PublicMatch, TeamIdx,
};

// ===== Client -> Server payloads =====

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PingData {
    pub client_time_ms: i64,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchCreateData {
    /// Display name for the creating player.
    pub name: String,

    /// Optional requested team (0 or 1). Server may override if full.
    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub team: Maybe<TeamIdx>,

    /// Match options.
    ///
    /// Optional: when omitted, the server uses defaults.
    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub options: Maybe<MatchOptions>,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchJoinData {
    pub match_id: String,
    pub name: String,

    /// Optional requested team (0 or 1). Server may override if full.
    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub team: Maybe<TeamIdx>,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchReadyData {
    pub match_id: String,
    pub ready: bool,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchRefData {
    pub match_id: String,
}

/// Join a match as a spectator (does not occupy a player seat).
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchWatchData {
    pub match_id: String,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatJoinData {
    pub room_id: String,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatSayData {
    pub room_id: String,
    pub content: String,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GamePlayCardData {
    pub match_id: String,

    /// `card_idx` references the caller's current hand ordering.
    ///
    /// Message type: `game.play_card`
    pub card_idx: u8,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GameSayData {
    pub match_id: String,
    pub command: GameCommand,
}

// ===== Server -> Client payloads =====

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PongData {
    pub server_time_ms: i64,
    pub client_time_ms: i64,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HelloData {
    pub session_id: String,
    pub server_version: String,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LobbySnapshotData {
    pub matches: Vec<LobbyMatch>,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LobbyMatchUpsertData {
    #[serde(rename = "match")]
    pub match_: LobbyMatch,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LobbyMatchRemoveData {
    /// Match id (same value as `PublicMatch.id`).
    pub match_id: String,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchSnapshotData {
    #[serde(rename = "match")]
    pub match_: PublicMatch,

    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub me: Maybe<PrivatePlayer>,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchUpdateData {
    #[serde(rename = "match")]
    pub match_: PublicMatch,

    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub me: Maybe<PrivatePlayer>,
}

/// Confirmation that the client has left a match.
///
/// This exists so `match.leave` does not need any legacy-style "ok" quirks.
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MatchLeftData {
    pub match_id: String,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GameSnapshotData {
    pub match_id: String,
    pub game: PublicGameState,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GameUpdateData {
    pub match_id: String,
    pub game: PublicGameState,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatSnapshotData {
    pub room: PublicChatRoom,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatMessageData {
    pub room_id: String,
    pub message: PublicChatMessage,
}

// ===== Messages =====

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", deny_unknown_fields)]
pub enum C2sMessage {
    #[serde(rename = "ping")]
    Ping(PingData),

    /// Request a lobby snapshot.
    #[serde(rename = "lobby.snapshot.get")]
    LobbySnapshotGet,

    #[serde(rename = "match.create")]
    MatchCreate(MatchCreateData),

    #[serde(rename = "match.join")]
    MatchJoin(MatchJoinData),

    /// Join a match as a spectator (hands hidden; no seat).
    #[serde(rename = "match.watch")]
    MatchWatch(MatchWatchData),

    #[serde(rename = "match.leave")]
    MatchLeave(MatchRefData),

    #[serde(rename = "match.ready")]
    MatchReady(MatchReadyData),

    /// Fetch the current match snapshot by id (useful after reconnects).
    #[serde(rename = "match.snapshot.get")]
    MatchSnapshotGet(MatchRefData),

    /// Fetch the current gameplay snapshot by match id.
    #[serde(rename = "game.snapshot.get")]
    GameSnapshotGet(MatchRefData),

    #[serde(rename = "match.start")]
    MatchStart(MatchRefData),

    #[serde(rename = "match.pause")]
    MatchPause(MatchRefData),

    #[serde(rename = "match.resume")]
    MatchResume(MatchRefData),

    #[serde(rename = "chat.join")]
    ChatJoin(ChatJoinData),

    #[serde(rename = "chat.say")]
    ChatSay(ChatSayData),

    /// Gameplay: play a card.
    #[serde(rename = "game.play_card")]
    GamePlayCard(GamePlayCardData),

    /// Gameplay: say a command (truco/envido/flor/etc).
    #[serde(rename = "game.say")]
    GameSay(GameSayData),
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", deny_unknown_fields)]
pub enum S2cMessage {
    #[serde(rename = "pong")]
    Pong(PongData),

    #[serde(rename = "hello")]
    Hello(HelloData),

    #[serde(rename = "lobby.snapshot")]
    LobbySnapshot(LobbySnapshotData),

    #[serde(rename = "lobby.match.upsert")]
    LobbyMatchUpsert(LobbyMatchUpsertData),

    #[serde(rename = "lobby.match.remove")]
    LobbyMatchRemove(LobbyMatchRemoveData),

    #[serde(rename = "match.snapshot")]
    MatchSnapshot(MatchSnapshotData),

    #[serde(rename = "match.update")]
    MatchUpdate(MatchUpdateData),

    #[serde(rename = "match.left")]
    MatchLeft(MatchLeftData),

    #[serde(rename = "game.snapshot")]
    GameSnapshot(GameSnapshotData),

    #[serde(rename = "game.update")]
    GameUpdate(GameUpdateData),

    #[serde(rename = "chat.snapshot")]
    ChatSnapshot(ChatSnapshotData),

    #[serde(rename = "chat.message")]
    ChatMessage(ChatMessageData),

    #[serde(rename = "error")]
    Error(ErrorPayload),
}
