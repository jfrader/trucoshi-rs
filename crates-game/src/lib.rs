//! Game engine primitives.
//!
//! During the migration from the original TypeScript server, we only need a small
//! subset of the full game logic to keep the realtime UI unblocked.
//!
//! This crate provides:
//! - a Spanish deck (40 cards)
//! - deterministic dealing
//! - a minimal, stateful turn-based engine for playing cards and advancing tricks/hands
//!
//! The realtime service can then expose per-player hands via the `me` field on `match.snapshot` / `match.update` (WS protocol v2).

use rand::rngs::StdRng;
use rand::{SeedableRng, seq::SliceRandom};
mod types;

use serde::{Deserialize, Serialize};

/// Card identifier.
///
/// Examples: "1e", "7o", "re".
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Card(pub String);

impl Card {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// All playable cards in the Spanish deck (40 cards).
pub const ALL_CARDS: [&str; 40] = [
    "1e", "1b", "7e", "7o", "3e", "3o", "3b", "3c", "2e", "2o", "2b", "2c", "1o", "1c", "re", "ro",
    "rb", "rc", "ce", "co", "cb", "cc", "pe", "po", "pb", "pc", "7b", "7c", "6e", "6o", "6b", "6c",
    "5e", "5o", "5b", "5c", "4e", "4o", "4b", "4c",
];

/// Deal `cards_per_player` cards to each player.
///
/// The returned vector is aligned to the player index (0..players_len).
pub fn deal_hands(players_len: usize, cards_per_player: usize, seed: u64) -> Vec<Vec<Card>> {
    let mut deck: Vec<Card> = ALL_CARDS.iter().map(|c| Card((*c).to_string())).collect();

    let mut rng = StdRng::seed_from_u64(seed);
    deck.shuffle(&mut rng);

    let mut hands = vec![Vec::<Card>::new(); players_len];

    for _ in 0..cards_per_player {
        for p in 0..players_len {
            if let Some(card) = deck.pop() {
                hands[p].push(card);
            }
        }
    }

    hands
}

/// Outcome of applying a card play.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayOutcome {
    /// Payload was invalid or could not be applied.
    Invalid,
    /// Turn advanced within the current trick.
    TurnAdvanced,
    /// Trick ended and a new trick began.
    TrickEnded,
    /// Hand ended; contains the winner team (0 or 1).
    HandEnded { winner_team_idx: u8 },
}

/// Outcome of applying a spoken command (truco/envido/etc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandOutcome {
    /// No immediate resolution; the hand continues.
    None,
    /// The current hand ended as a consequence of the command.
    HandEnded { winner_team_idx: u8 },
}

/// Minimal, migration-friendly in-memory game state.
#[derive(Debug, Clone)]
pub struct GameState {
    pub public: PublicGameState,

    /// Index (0..players_len) of the current player to act.
    pub turn_idx: u8,

    /// Epoch millis when the current turn times out.
    pub turn_expires_at_ms: i64,

    /// Per-player hand (only revealed to the owning session via the `me` field on match events).
    hands: Vec<Vec<String>>,

    /// Per-player used cards.
    used_hands: Vec<Vec<String>>,
}

impl GameState {
    /// Best-effort list of commands the current player can say, based on the public hand state.
    ///
    /// Note: this still does not enforce full Truco rules, but protocol v2 is strict about the
    /// wire format: we return typed `GameCommand` values rather than free-form strings.
    pub fn possible_commands_for_cards(&self, cards: &[String]) -> Vec<GameCommand> {
        use HandState::*;

        let hs = self.public.hand_state.clone();

        let has_flor = has_flor(cards);

        let cmds: Vec<GameCommand> = match hs {
            WaitingPlay => {
                let mut v = vec![
                    // Truco escalation
                    GameCommand::Truco,
                    GameCommand::Retruco,
                    GameCommand::ValeCuatro,
                    // Envido
                    GameCommand::Envido,
                    GameCommand::RealEnvido,
                    GameCommand::FaltaEnvido,
                ];

                // Flor (only if player has it).
                if has_flor {
                    v.extend([
                        GameCommand::Flor,
                        GameCommand::ContraFlor,
                        GameCommand::ContraFlorAlResto,
                    ]);
                }

                v
            }
            WaitingForTrucoAnswer | WaitingEnvidoAnswer | WaitingFlorAnswer => {
                vec![GameCommand::Quiero, GameCommand::NoQuiero]
            }
            WaitingEnvidoPointsAnswer => vec![GameCommand::SonBuenas],
            DisplayFlorBattle | DisplayPreviousHand | Finished => vec![],
        };

        cmds
    }

    pub fn new(players_len: usize, seed: u64, turn_time_ms: i64, now_ms: i64) -> Self {
        Self::new_with_forehand(players_len, seed, turn_time_ms, now_ms, 0)
    }

    pub fn new_with_forehand(
        players_len: usize,
        seed: u64,
        turn_time_ms: i64,
        now_ms: i64,
        forehand: u8,
    ) -> Self {
        let forehand = if players_len == 0 {
            0
        } else {
            forehand % (players_len as u8)
        };

        let hands = deal_hands(players_len, 3, seed)
            .into_iter()
            .map(|h| h.into_iter().map(|c| c.0).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let used_hands = vec![Vec::<String>::new(); players_len];

        let mut public = PublicGameState::default();
        public.hand_state = HandState::WaitingPlay;
        public.forehand_seat_idx = forehand;
        public.turn_seat_idx = forehand;
        public.rounds = vec![vec![]];
        public.winner_team_idx = None;

        Self {
            public,
            turn_idx: if players_len == 0 { 0 } else { forehand },
            turn_expires_at_ms: now_ms.saturating_add(turn_time_ms.max(0)),
            hands,
            used_hands,
        }
    }

    pub fn hands(&self) -> &Vec<Vec<String>> {
        &self.hands
    }

    pub fn used_hands(&self) -> &Vec<Vec<String>> {
        &self.used_hands
    }

    fn apply_hand_state_hint(&mut self, cmd: GameCommand) {
        if let Some(hs) = hand_state_for_command(cmd) {
            self.public.hand_state = hs;
        }
    }

    /// Apply a command (truco/envido/flor/etc) spoken by a player.
    ///
    /// This is intentionally a *minimal* ruleset for the migration:
    /// - it updates the public `hand_state` hint
    /// - it can end the hand in the specific case of declining truco
    pub fn apply_command(
        &mut self,
        cmd: GameCommand,
        from_player_idx: usize,
        teams_by_player_idx: &[u8],
    ) -> CommandOutcome {
        use HandState::*;

        let prev_state = self.public.hand_state.clone();
        self.apply_hand_state_hint(cmd);

        match (prev_state, cmd) {
            // Declining truco ends the hand; award the hand to the *requesting* team.
            // We don't yet track who requested, so we approximate that the requester is
            // the opponent of the answering player.
            (WaitingForTrucoAnswer, GameCommand::NoQuiero) => {
                let from_team = teams_by_player_idx.get(from_player_idx).copied();
                if let Some(t) = from_team {
                    let winner = if t == 0 { 1 } else { 0 };
                    self.public.winner_team_idx = Some(winner);
                    self.public.hand_state = Finished;
                    return CommandOutcome::HandEnded {
                        winner_team_idx: winner,
                    };
                }
                CommandOutcome::None
            }

            // Generic answers just resume play.
            (
                WaitingEnvidoAnswer | WaitingEnvidoPointsAnswer | WaitingFlorAnswer,
                GameCommand::Quiero | GameCommand::NoQuiero,
            ) => {
                self.public.hand_state = WaitingPlay;
                CommandOutcome::None
            }
            (WaitingForTrucoAnswer, GameCommand::Quiero) => {
                self.public.hand_state = WaitingPlay;
                CommandOutcome::None
            }

            // For everything else, the hand continues with whatever `hand_state_for_command`
            // decided.
            _ => CommandOutcome::None,
        }
    }

    /// Apply a card play for the current turn.
    ///
    /// - `card_idx` references the caller's current hand ordering.
    /// - `teams_by_player_idx` must be aligned to player indices.
    pub fn play_card(
        &mut self,
        card_idx: u8,
        teams_by_player_idx: &[u8],
        turn_time_ms: i64,
        now_ms: i64,
    ) -> PlayOutcome {
        let players_len = teams_by_player_idx.len();
        if players_len == 0 {
            return PlayOutcome::Invalid;
        }

        let turn_idx_usize = self.turn_idx as usize;

        let card = self
            .hands
            .get(turn_idx_usize)
            .and_then(|h| h.get(card_idx as usize))
            .cloned();

        let Some(card) = card else {
            return PlayOutcome::Invalid;
        };

        // Remove from hand if present.
        if let Some(hand) = self.hands.get_mut(turn_idx_usize) {
            if let Some(pos) = hand.iter().position(|c| c == &card) {
                let used = hand.remove(pos);
                if let Some(used_hand) = self.used_hands.get_mut(turn_idx_usize) {
                    used_hand.push(used);
                }
            }
        }

        if self.public.rounds.is_empty() {
            self.public.rounds.push(vec![]);
        }

        // note: we intentionally do not expose a "last card" helper in v2
        let (cur_round_len, cur_round_snapshot) = {
            let cur_round = self.public.rounds.last_mut().expect("round exists");

            cur_round.push(PlayedCard {
                seat_idx: self.turn_idx,
                card: card.to_string(),
            });

            (cur_round.len(), cur_round.clone())
        };

        // If everyone has played for this trick, compute a winner and advance the hand.
        if cur_round_len >= players_len {
            // Count completed tricks.
            let completed_rounds = self
                .public
                .rounds
                .iter()
                .filter(|r| r.len() >= players_len)
                .collect::<Vec<_>>();

            let mut trick_wins = [0u8, 0u8];
            for r in &completed_rounds {
                let (winner_team, _winner_player) = trick_winner(r, teams_by_player_idx);
                if let Some(t) = winner_team {
                    if t < 2 {
                        trick_wins[t as usize] = trick_wins[t as usize].saturating_add(1);
                    }
                }
            }

            let mut hand_winner: Option<u8> = None;
            if trick_wins[0] >= 2 {
                hand_winner = Some(0);
            } else if trick_wins[1] >= 2 {
                hand_winner = Some(1);
            } else if completed_rounds.len() >= 3 {
                // Best-effort resolution for a fully-played hand.
                hand_winner = match trick_wins[0].cmp(&trick_wins[1]) {
                    std::cmp::Ordering::Greater => Some(0),
                    std::cmp::Ordering::Less => Some(1),
                    std::cmp::Ordering::Equal => {
                        // Tie: award to forehand's team.
                        let forehand_idx = self.public.forehand_seat_idx as usize;
                        teams_by_player_idx.get(forehand_idx).copied()
                    }
                };
            }

            if let Some(winner_team_idx) = hand_winner {
                return PlayOutcome::HandEnded { winner_team_idx };
            }

            // Trick ended but the hand continues: start next trick, and set next turn to the trick winner.
            let (_winner_team, winner_player) =
                trick_winner(&cur_round_snapshot, teams_by_player_idx);

            if self.public.rounds.len() < 3 {
                self.public.rounds.push(vec![]);
            }

            let forehand_idx = self.public.forehand_seat_idx;
            let next_turn = winner_player.unwrap_or(forehand_idx);
            self.turn_idx = next_turn;
            self.public.turn_seat_idx = next_turn;
            self.turn_expires_at_ms = now_ms.saturating_add(turn_time_ms.max(0));

            return PlayOutcome::TrickEnded;
        }

        // Normal progression within a trick.
        self.turn_idx = (self.turn_idx + 1) % (players_len as u8);
        self.public.turn_seat_idx = self.turn_idx;
        self.turn_expires_at_ms = now_ms.saturating_add(turn_time_ms.max(0));
        PlayOutcome::TurnAdvanced
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SplitCard {
    value: i32,
    suit: char,
    card: String,
    envido_value: i32,
}

fn split_card(card: &str) -> SplitCard {
    // Wire card format: "1e", "7o", "pe" (p/c/r = 10).
    let mut value_str = card.chars().next().unwrap_or('0').to_string();
    let suit = card.chars().nth(1).unwrap_or('x');
    if matches!(value_str.as_str(), "p" | "c" | "r") {
        value_str = "10".into();
    }

    let value = value_str.parse::<i32>().unwrap_or(0);
    let envido_value = value % 10;

    SplitCard {
        value,
        suit,
        card: card.to_string(),
        envido_value,
    }
}

/// Whether the given cards constitute a flor (all same suit).
///
/// Note: we check across the provided slice, so callers can pass the current hand,
/// used cards, or both.
pub fn has_flor(cards: &[String]) -> bool {
    if cards.is_empty() {
        return false;
    }
    let first = split_card(&cards[0]);
    cards.iter().all(|c| split_card(c).suit == first.suit)
}

/// Calculate the best envido score for the given cards.
///
/// Returns the maximum score achievable with either:
/// - any same-suit pair: `a + b + 20`
/// - or (if no same-suit pair exists) the highest single card envido value.
pub fn calculate_envido_points(cards: &[String]) -> i32 {
    let hand = cards.iter().map(|c| split_card(c)).collect::<Vec<_>>();

    let mut best_pair: Option<i32> = None;
    for i in 0..hand.len() {
        for j in (i + 1)..hand.len() {
            if hand[i].suit == hand[j].suit {
                let v = hand[i].envido_value + hand[j].envido_value + 20;
                best_pair = Some(best_pair.map(|b| b.max(v)).unwrap_or(v));
            }
        }
    }

    if let Some(v) = best_pair {
        return v;
    }

    hand.iter().map(|c| c.envido_value).max().unwrap_or(0)
}

/// Typed mapping from gameplay commands to the next public hand-state hint.
pub use types::{GameCommand, HandState, PlayedCard, PublicGameState};

pub fn hand_state_for_command(cmd: GameCommand) -> Option<HandState> {
    use GameCommand::*;

    match cmd {
        // Truco escalation
        Truco | Retruco | ValeCuatro => Some(HandState::WaitingForTrucoAnswer),

        // Envido
        Envido | RealEnvido | FaltaEnvido => Some(HandState::WaitingEnvidoAnswer),

        // Envido points / misc
        SonBuenas => Some(HandState::WaitingEnvidoPointsAnswer),

        // Flor
        Flor | ContraFlor | ContraFlorAlResto => Some(HandState::WaitingFlorAnswer),

        // Answers / misc
        Quiero | NoQuiero => Some(HandState::WaitingPlay),
    }
}

pub fn card_power(card: &str) -> i32 {
    match card {
        "1e" => 13,
        "1b" => 12,
        "7e" => 11,
        "7o" => 10,
        "3e" | "3o" | "3b" | "3c" => 9,
        "2e" | "2o" | "2b" | "2c" => 8,
        "1o" | "1c" => 7,
        "re" | "ro" | "rb" | "rc" => 6,
        "ce" | "co" | "cb" | "cc" => 5,
        "pe" | "po" | "pb" | "pc" => 4,
        "7b" | "7c" => 3,
        "6e" | "6o" | "6b" | "6c" => 2,
        "5e" | "5o" | "5b" | "5c" => 1,
        "4e" | "4o" | "4b" | "4c" => 0,
        _ => -1,
    }
}

/// Returns (winner_team_idx, winner_player_idx).
///
/// If there is a tie for highest power, both are None.
pub fn trick_winner(played: &[PlayedCard], teams_by_player_idx: &[u8]) -> (Option<u8>, Option<u8>) {
    let mut best_power: i32 = -2;
    let mut best_player_idx: Option<u8> = None;
    let mut tie = false;

    for v in played {
        let card = v.card.as_str();
        let pidx = v.seat_idx;

        let pwr = card_power(card);
        if pwr > best_power {
            best_power = pwr;
            best_player_idx = Some(pidx);
            tie = false;
        } else if pwr == best_power {
            tie = true;
        }
    }

    if tie {
        return (None, None);
    }

    let Some(winner_player_idx) = best_player_idx else {
        return (None, None);
    };

    let winner_team_idx = teams_by_player_idx.get(winner_player_idx as usize).copied();

    (winner_team_idx, Some(winner_player_idx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deal_hands_is_deterministic_for_same_seed() {
        let h1 = deal_hands(4, 3, 123);
        let h2 = deal_hands(4, 3, 123);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 4);
        assert_eq!(h1[0].len(), 3);
    }

    #[test]
    fn trick_winner_detects_tie() {
        let played = vec![
            PlayedCard {
                seat_idx: 0,
                card: "3e".to_string(),
            },
            PlayedCard {
                seat_idx: 1,
                card: "3o".to_string(),
            },
        ];
        let teams = vec![0u8, 1u8];
        let (t, p) = trick_winner(&played, &teams);
        assert_eq!((t, p), (None, None));
    }

    #[test]
    fn play_card_advances_turn() {
        let now = 1000i64;
        let mut g = GameState::new_with_forehand(2, 42, 10_000, now, 0);
        let teams = vec![0u8, 1u8];
        let outcome = g.play_card(0, &teams, 10_000, now);
        assert_eq!(outcome, PlayOutcome::TurnAdvanced);
        assert_eq!(g.public.turn_seat_idx, 1);
    }

    #[test]
    fn calculates_envido_points_and_flor() {
        let cards = vec!["1e".to_string(), "7e".to_string(), "3e".to_string()];
        assert!(has_flor(&cards));

        // Best same-suit pair is 7e+3e => 7 + 3 + 20 = 30.
        assert_eq!(calculate_envido_points(&cards), 30);
    }

    #[test]
    fn no_quiero_during_truco_ends_hand_for_opponent() {
        let now = 1000i64;
        let mut g = GameState::new_with_forehand(2, 42, 10_000, now, 0);
        let teams = vec![0u8, 1u8];

        // Someone says TRUCO, we enter the answer state.
        g.apply_command(GameCommand::Truco, 0, &teams);
        assert_eq!(g.public.hand_state, HandState::WaitingForTrucoAnswer);

        // Opponent declines.
        let outcome = g.apply_command(GameCommand::NoQuiero, 1, &teams);
        assert_eq!(outcome, CommandOutcome::HandEnded { winner_team_idx: 0 });
        assert_eq!(g.public.winner_team_idx, Some(0));
        assert_eq!(g.public.hand_state, HandState::Finished);
    }
}
