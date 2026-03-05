use serde::{Deserialize, Serialize};

#[cfg(feature = "json-schema")]
use schemars::JsonSchema;

/// Gameplay hand state hints.
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandState {
    WaitingPlay,
    WaitingForTrucoAnswer,
    WaitingEnvidoAnswer,
    WaitingEnvidoPointsAnswer,
    WaitingFlorAnswer,
    DisplayFlorBattle,
    DisplayPreviousHand,
    Finished,
}

/// Core gameplay commands (Truco argentino).
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GameCommand {
    // Truco escalation
    Truco,
    Retruco,
    ValeCuatro,

    // Envido
    Envido,
    RealEnvido,
    FaltaEnvido,

    // Flor
    Flor,
    ContraFlor,
    ContraFlorAlResto,

    // Answers / misc
    Quiero,
    NoQuiero,
    SonBuenas,
}

/// Minimal public game state.
///
/// This struct is part of the WS protocol surface (re-exported by `trucoshi-realtime`). Keep it
/// stable and avoid legacy-only convenience fields.
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PublicGameState {
    pub hand_state: HandState,
    pub forehand_seat_idx: u8,
    pub turn_seat_idx: u8,

    /// Played cards grouped by round/trick.
    pub rounds: Vec<Vec<PlayedCard>>,

    /// When set, the current hand is finished and the winner team is known.
    pub winner_team_idx: Option<u8>,
}

impl Default for PublicGameState {
    fn default() -> Self {
        Self {
            hand_state: HandState::WaitingPlay,
            forehand_seat_idx: 0,
            turn_seat_idx: 0,
            rounds: vec![],
            winner_team_idx: None,
        }
    }
}

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlayedCard {
    pub seat_idx: u8,
    pub card: String,
}
