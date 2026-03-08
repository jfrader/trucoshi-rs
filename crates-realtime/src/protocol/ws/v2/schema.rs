use serde::{Deserialize, Serialize};

#[cfg(feature = "json-schema")]
use schemars::JsonSchema;

use std::marker::PhantomData;

// ===== Strict small integers =====

/// Team index (protocol v2).
///
/// Strictly limited to `0` or `1` (no legacy "any u8" values).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct TeamIdx(u8);

impl TeamIdx {
    pub const TEAM_0: TeamIdx = TeamIdx(0);
    pub const TEAM_1: TeamIdx = TeamIdx(1);

    pub const fn as_u8(self) -> u8 {
        self.0
    }
}

#[cfg(feature = "json-schema")]
impl JsonSchema for TeamIdx {
    fn schema_name() -> String {
        "TeamIdx".to_string()
    }

    fn json_schema(r#gen: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        use schemars::schema::{InstanceType, Schema, SchemaObject, SingleOrVec};
        use serde_json::json;

        // Register the underlying schema to keep generators happy.
        let _ = r#gen.subschema_for::<u8>();

        let mut obj = SchemaObject {
            instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Integer))),
            format: Some("uint8".to_string()),
            ..Default::default()
        };

        // Prefer an explicit enum so downstream generators can produce `0 | 1` unions.
        obj.enum_values = Some(vec![json!(0), json!(1)]);

        // Keep numeric bounds for nicer tooling.
        obj.number = Some(Box::new(schemars::schema::NumberValidation {
            minimum: Some(0.0),
            maximum: Some(1.0),
            ..Default::default()
        }));

        Schema::Object(obj)
    }
}

impl From<TeamIdx> for u8 {
    fn from(v: TeamIdx) -> Self {
        v.0
    }
}

impl TryFrom<u8> for TeamIdx {
    type Error = String;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 | 1 => Ok(Self(v)),
            _ => Err(format!("invalid team idx {v}; expected 0 or 1")),
        }
    }
}

// ===== Optional-but-not-null fields =====

/// Protocol-level optional field wrapper.
///
/// In JSON, `Option<T>` traditionally accepts both *missing* and explicit `null`.
/// For the WS protocol we want a stricter contract:
///
/// - Missing field: allowed (treated as none)
/// - Explicit `null`: **rejected**
///
/// This keeps the generated JSON Schema / TS types cleaner (no `| null` unions) and prevents
/// legacy clients from relying on `null` payload quirks.
#[derive(Debug, Clone)]
pub struct Maybe<T>(pub Option<T>);

impl<T> Default for Maybe<T> {
    fn default() -> Self {
        Self(None)
    }
}

#[cfg(feature = "json-schema")]
impl<T: JsonSchema> JsonSchema for Maybe<T> {
    fn schema_name() -> String {
        // Transparent wrapper: avoid polluting downstream generated types with `Maybe*`.
        T::schema_name()
    }

    fn json_schema(r#gen: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        // IMPORTANT: we purposely do *not* emit a `null` union here.
        // The field optionality is represented by the parent struct's `required` list.
        T::json_schema(r#gen)
    }
}

impl<T> Maybe<T> {
    pub fn is_none(v: &Self) -> bool {
        v.0.is_none()
    }
}

impl<T> From<Option<T>> for Maybe<T> {
    fn from(v: Option<T>) -> Self {
        Self(v)
    }
}

impl<T> From<Maybe<T>> for Option<T> {
    fn from(v: Maybe<T>) -> Self {
        v.0
    }
}

impl<T: Serialize> Serialize for Maybe<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Maybe<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct V<T>(PhantomData<T>);

        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for V<T> {
            type Value = Maybe<T>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a non-null value")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Err(E::custom("null is not allowed for this field"))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Err(E::custom("null is not allowed for this field"))
            }

            fn visit_some<D>(self, d: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Ok(Maybe(Some(T::deserialize(d)?)))
            }
        }

        deserializer.deserialize_option(V(PhantomData))
    }
}

// ===== Core match / lobby =====

/// Match lifecycle phase (protocol v2).
///
/// Readiness is **not** encoded here; use `PublicMatch.players[].ready`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatchPhase {
    /// Match exists in the lobby and can accept ready/start actions.
    Lobby,

    Started,

    Paused,

    Finished,
}

#[cfg(feature = "json-schema")]
impl JsonSchema for MatchPhase {
    fn schema_name() -> String {
        "MatchPhase".to_string()
    }

    fn json_schema(_gen: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        use schemars::schema::{InstanceType, Schema, SchemaObject, SingleOrVec};
        use serde_json::json;

        // Keep this schema *simple* and stable: just a strict string enum.
        let mut obj = SchemaObject {
            instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::String))),
            ..Default::default()
        };

        obj.enum_values = Some(vec![
            json!("lobby"),
            json!("started"),
            json!("paused"),
            json!("finished"),
        ]);

        Schema::Object(obj)
    }
}

impl Default for MatchPhase {
    fn default() -> Self {
        Self::Lobby
    }
}

fn deserialize_match_max_players<'de, D>(d: D) -> Result<u8, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = u8::deserialize(d)?;
    match v {
        // Truco supports 1v1 (2), 2v2 (4), 3v3 (6).
        2 | 4 | 6 => Ok(v),
        _ => Err(serde::de::Error::custom(
            "max_players must be one of: 2, 4, 6",
        )),
    }
}

#[cfg(feature = "json-schema")]
fn schema_match_max_players(
    _gen: &mut schemars::r#gen::SchemaGenerator,
) -> schemars::schema::Schema {
    use schemars::schema::{InstanceType, Schema, SchemaObject, SingleOrVec};
    use serde_json::json;

    let mut obj = SchemaObject {
        instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Integer))),
        format: Some("uint8".to_string()),
        ..Default::default()
    };

    obj.enum_values = Some(vec![json!(2), json!(4), json!(6)]);

    Schema::Object(obj)
}

fn default_abandon_time_ms() -> i64 {
    120_000
}

fn default_reconnect_grace_ms() -> i64 {
    5_000
}

fn default_falta_envido() -> u8 {
    2
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MatchOptions {
    /// Maximum number of players allowed in the match.
    ///
    /// Protocol v2: enforced as an explicit enum (2/4/6), since Truco is always two teams
    /// with equal sizes.
    #[serde(deserialize_with = "deserialize_match_max_players")]
    #[cfg_attr(
        feature = "json-schema",
        schemars(schema_with = "schema_match_max_players")
    )]
    pub max_players: u8,

    /// Whether Flor is enabled.
    pub flor: bool,

    /// Points required to win the match.
    #[cfg_attr(feature = "json-schema", schemars(range(min = 1, max = 15)))]
    pub match_points: u8,

    /// Falta Envido scoring mode (`1` = two faltas / 2 × `match_points`, `2` = one falta / `match_points`).
    #[serde(default = "default_falta_envido")]
    #[cfg_attr(feature = "json-schema", schemars(range(min = 1, max = 2)))]
    pub falta_envido: u8,

    /// Turn timer in milliseconds.
    #[cfg_attr(feature = "json-schema", schemars(range(min = 1, max = 600_000)))]
    pub turn_time_ms: i64,

    /// Inactivity window before a disconnected player is removed.
    #[serde(default = "default_abandon_time_ms")]
    #[cfg_attr(feature = "json-schema", schemars(range(min = 1, max = 600_000)))]
    pub abandon_time_ms: i64,

    /// Minimum grace after a websocket drop before sweeps run.
    #[serde(default = "default_reconnect_grace_ms")]
    #[cfg_attr(feature = "json-schema", schemars(range(min = 1, max = 60_000)))]
    pub reconnect_grace_ms: i64,
}

impl MatchOptions {
    pub fn falta_envido_goal(&self) -> trucoshi_game::FaltaEnvidoGoal {
        match self.falta_envido {
            1 => trucoshi_game::FaltaEnvidoGoal::TwoFaltas,
            2 => trucoshi_game::FaltaEnvidoGoal::OneFalta,
            _ => trucoshi_game::FaltaEnvidoGoal::OneFalta,
        }
    }
}

impl Default for MatchOptions {
    fn default() -> Self {
        Self {
            max_players: 6,
            flor: true,
            match_points: 9,
            falta_envido: default_falta_envido(),
            turn_time_ms: 30_000,
            abandon_time_ms: default_abandon_time_ms(),
            reconnect_grace_ms: default_reconnect_grace_ms(),
        }
    }
}

/// Lobby match summary (protocol v2).
///
/// This is intentionally minimal and is the only match type used by lobby events.
/// It should stay stable and UI-friendly.
///
/// Note: we intentionally duplicate fields between `LobbyMatch` and `PublicMatch` instead of
/// using a `flatten`ed base struct. This keeps Rust-side ergonomics simple and avoids schema/
/// generator edge-cases.
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LobbyMatch {
    /// Match id (opaque).
    pub id: String,

    pub options: MatchOptions,
    pub phase: MatchPhase,

    /// Players ordered by seating/turn order (server-defined).
    pub players: Vec<PublicPlayer>,

    /// Seat index (in `players[]`) of the current match owner.
    pub owner_seat_idx: u8,

    /// Number of currently connected spectator (watch) sessions.
    pub spectator_count: u32,
}

/// Public match snapshot (protocol v2).
///
/// This is the authoritative match state used by match-scoped events.
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicMatch {
    /// Match id (opaque).
    pub id: String,

    pub options: MatchOptions,
    pub phase: MatchPhase,

    /// Players ordered by seating/turn order (server-defined).
    pub players: Vec<PublicPlayer>,

    /// Seat index (in `players[]`) of the current match owner.
    pub owner_seat_idx: u8,

    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub pause_request: Maybe<PublicPauseRequest>,

    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub pending_unpause: Maybe<PublicPendingUnpause>,

    /// Number of currently connected spectator (watch) sessions.
    pub spectator_count: u32,

    /// Current match points for teams 0 and 1.
    pub team_points: [u8; 2],
}

/// Pending pause request metadata exposed in `PublicMatch`.
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicPauseRequest {
    pub requested_by_team: TeamIdx,
    pub awaiting_team: TeamIdx,
    pub requested_by_seat_idx: u8,
    pub expires_at_ms: i64,
}

/// Pending unpause countdown metadata.
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicPendingUnpause {
    pub resume_at_ms: i64,
}

/// Public player view (protocol v2).
///
/// This is designed to be minimal and UI-friendly:
/// - The players array ordering is server-defined and stable for the lifetime of the match.
/// - `ready` is per-player (no separate match-level ready list)
/// - Match ownership is exposed at the match level via `owner_seat_idx`.
///
/// Note: player keys are server-internal (session-bound) and are intentionally not exposed.
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicPlayer {
    pub name: String,

    pub team: TeamIdx,

    pub ready: bool,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveMatchPlayer {
    pub seat_idx: u8,
    pub team: TeamIdx,
    pub ready: bool,
    pub is_owner: bool,
    pub last_active_ms: i64,
    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub disconnected_at_ms: Maybe<i64>,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveMatchSummary {
    #[serde(rename = "match")]
    pub match_: PublicMatch,
    pub me: ActiveMatchPlayer,
}

/// Recipient-only private view of the current player.
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivatePlayer {
    /// Seat index in the match's current `players` ordering.
    pub seat_idx: u8,

    pub hand: Vec<String>,
    pub used: Vec<String>,

    /// Commands the player is currently allowed to say.
    ///
    /// Protocol v2 is intentionally strict: this is a typed enum instead of free-form strings.
    pub commands: Vec<GameCommand>,

    pub has_flor: bool,
    pub envido_points: i32,
}

// ===== Gameplay =====

// Gameplay types are defined in `trucoshi-game` (engine).
//
// We re-export the stable enums + leaf structs, but we define a protocol-level `PublicGameState`
// wrapper so the WS schema can be stricter than a vanilla `Option<T>` (i.e. allow *missing* but
// reject explicit `null`).
pub use trucoshi_game::{GameCommand, HandState, PlayedCard};

/// Minimal public game state (protocol v2).
///
/// This mirrors `trucoshi_game::PublicGameState` but uses `Maybe<T>` for optional fields.
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicGameState {
    pub hand_state: HandState,
    pub forehand_seat_idx: u8,
    pub turn_seat_idx: u8,

    /// Played cards grouped by round/trick.
    pub rounds: Vec<Vec<PlayedCard>>,

    /// When set, the current hand is finished and the winner team is known.
    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub winner_team_idx: Maybe<TeamIdx>,
}

fn team_idx_from_u8(v: u8) -> Option<TeamIdx> {
    TeamIdx::try_from(v).ok()
}

impl From<trucoshi_game::PublicGameState> for PublicGameState {
    fn from(v: trucoshi_game::PublicGameState) -> Self {
        Self {
            hand_state: v.hand_state,
            forehand_seat_idx: v.forehand_seat_idx,
            turn_seat_idx: v.turn_seat_idx,
            rounds: v.rounds,
            winner_team_idx: Maybe(v.winner_team_idx.and_then(team_idx_from_u8)),
        }
    }
}

impl From<&trucoshi_game::PublicGameState> for PublicGameState {
    fn from(v: &trucoshi_game::PublicGameState) -> Self {
        Self {
            hand_state: v.hand_state,
            forehand_seat_idx: v.forehand_seat_idx,
            turn_seat_idx: v.turn_seat_idx,
            rounds: v.rounds.clone(),
            winner_team_idx: Maybe(v.winner_team_idx.and_then(team_idx_from_u8)),
        }
    }
}

// ===== Chat =====

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicChatUser {
    pub name: String,

    /// Optional match seat index, if the sender is currently in a match.
    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub seat_idx: Maybe<u8>,

    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub team: Maybe<TeamIdx>,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChatCommandOutcomeKind {
    None,
    PointsAwarded,
    HandEnded,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatCommandMetadataOutcome {
    pub kind: ChatCommandOutcomeKind,

    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub winner_team_idx: Maybe<TeamIdx>,

    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub points: Maybe<u8>,

    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub award_reason: Maybe<String>,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatMessageMetadata {
    CardPlayed {
        card: String,
    },

    Command {
        command: GameCommand,

        #[serde(default, skip_serializing_if = "Maybe::is_none")]
        outcome: Maybe<ChatCommandMetadataOutcome>,
    },
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicChatMessage {
    pub id: String,

    /// Epoch millis.
    pub date_ms: i64,

    pub user: PublicChatUser,

    /// Whether the message is system-generated.
    ///
    /// Protocol v2 is strict: this field is always present (use `false` for normal messages).
    pub system: bool,

    pub content: String,

    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub metadata: Maybe<ChatMessageMetadata>,
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PublicChatRoom {
    pub id: String,

    /// Recent chat messages for the room (may be empty, but is always present).
    pub messages: Vec<PublicChatMessage>,
}
