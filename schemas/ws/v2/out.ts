/*
 * AUTO-GENERATED FILE. DO NOT EDIT.
 *
 * Generated from JSON Schema under schemas/ws/v2/*.json
 *
 * Regenerate with:
 *   npm run gen:ws:types
 */

export type S2CMessage =
  | {
      data: PongData;
      type: 'pong';
    }
  | {
      data: HelloData;
      type: 'hello';
    }
  | {
      data: ActiveMatchesSnapshotData;
      type: 'me.active_matches';
    }
  | {
      data: LobbySnapshotData;
      type: 'lobby.snapshot';
    }
  | {
      data: LobbyMatchUpsertData;
      type: 'lobby.match.upsert';
    }
  | {
      data: LobbyMatchRemoveData;
      type: 'lobby.match.remove';
    }
  | {
      data: MatchSnapshotData;
      type: 'match.snapshot';
    }
  | {
      data: MatchUpdateData;
      type: 'match.update';
    }
  | {
      data: MatchLeftData;
      type: 'match.left';
    }
  | {
      data: MatchKickedData;
      type: 'match.kicked';
    }
  | {
      data: GameSnapshotData;
      type: 'game.snapshot';
    }
  | {
      data: GameUpdateData;
      type: 'game.update';
    }
  | {
      data: ChatSnapshotData;
      type: 'chat.snapshot';
    }
  | {
      data: ChatMessageData;
      type: 'chat.message';
    }
  | {
      data: ErrorPayload;
      type: 'error';
    };
export type MatchPhase = 'lobby' | 'started' | 'paused' | 'finished';
export type TeamIdx = 0 | 1;
export type Int64 = number;
/**
 * Core gameplay commands (Truco argentino).
 */
export type GameCommand =
  | 'truco'
  | 'retruco'
  | 'vale_cuatro'
  | 'envido'
  | 'real_envido'
  | 'falta_envido'
  | 'flor'
  | 'contra_flor'
  | 'contra_flor_al_resto'
  | 'quiero'
  | 'no_quiero'
  | 'son_buenas';
/**
 * Gameplay hand state hints.
 */
export type HandState =
  | 'waiting_play'
  | 'waiting_for_truco_answer'
  | 'waiting_envido_answer'
  | 'waiting_envido_points_answer'
  | 'waiting_flor_answer'
  | 'display_flor_battle'
  | 'display_previous_hand'
  | 'finished';
export type Uint8 = number;
export type WsVersion = 2;

export interface WsOutMessage {
  /**
   * Optional correlation id (client sets; server echoes in responses).
   *
   * Missing: allowed. Explicit `null`: rejected.
   */
  id?: string;
  /**
   * Tagged payload.
   */
  msg: S2CMessage;
  /**
   * Protocol version (must be exactly 2).
   */
  v: WsVersion;
}
export interface PongData {
  client_time_ms: number;
  server_time_ms: number;
}
export interface HelloData {
  server_version: string;
  session_id: string;
}
export interface ActiveMatchesSnapshotData {
  matches: ActiveMatchSummary[];
}
export interface ActiveMatchSummary {
  match: PublicMatch;
  me: ActiveMatchPlayer;
}
/**
 * Public match snapshot (protocol v2).
 *
 * This is the authoritative match state used by match-scoped events.
 */
export interface PublicMatch {
  /**
   * Match id (opaque).
   */
  id: string;
  options: MatchOptions;
  /**
   * Seat index (in `players[]`) of the current match owner.
   */
  owner_seat_idx: number;
  phase: MatchPhase;
  /**
   * Players ordered by seating/turn order (server-defined).
   */
  players: PublicPlayer[];
  /**
   * Number of currently connected spectator (watch) sessions.
   */
  spectator_count: number;
  /**
   * Current match points for teams 0 and 1.
   *
   * @minItems 2
   * @maxItems 2
   */
  team_points: [number, number];
}
export interface MatchOptions {
  /**
   * Inactivity window before a disconnected player is removed.
   */
  abandon_time_ms?: number;
  /**
   * Falta Envido scoring mode (`1` = two faltas / 2 × `match_points`, `2` = one falta / `match_points`).
   */
  falta_envido?: number;
  /**
   * Whether Flor is enabled.
   */
  flor: boolean;
  /**
   * Points required to win the match.
   */
  match_points: number;
  /**
   * Maximum number of players allowed in the match.
   *
   * Protocol v2: enforced as an explicit enum (2/4/6), since Truco is always two teams with equal sizes.
   */
  max_players: 2 | 4 | 6;
  /**
   * Minimum grace after a websocket drop before sweeps run.
   */
  reconnect_grace_ms?: number;
  /**
   * Turn timer in milliseconds.
   */
  turn_time_ms: number;
}
/**
 * Public player view (protocol v2).
 *
 * This is designed to be minimal and UI-friendly: - The players array ordering is server-defined and stable for the lifetime of the match. - `ready` is per-player (no separate match-level ready list) - Match ownership is exposed at the match level via `owner_seat_idx`.
 *
 * Note: player keys are server-internal (session-bound) and are intentionally not exposed.
 */
export interface PublicPlayer {
  name: string;
  ready: boolean;
  team: TeamIdx;
}
export interface ActiveMatchPlayer {
  disconnected_at_ms?: Int64;
  is_owner: boolean;
  last_active_ms: number;
  ready: boolean;
  seat_idx: number;
  team: TeamIdx;
}
export interface LobbySnapshotData {
  matches: LobbyMatch[];
}
/**
 * Lobby match summary (protocol v2).
 *
 * This is intentionally minimal and is the only match type used by lobby events. It should stay stable and UI-friendly.
 *
 * Note: we intentionally duplicate fields between `LobbyMatch` and `PublicMatch` instead of using a `flatten`ed base struct. This keeps Rust-side ergonomics simple and avoids schema/ generator edge-cases.
 */
export interface LobbyMatch {
  /**
   * Match id (opaque).
   */
  id: string;
  options: MatchOptions;
  /**
   * Seat index (in `players[]`) of the current match owner.
   */
  owner_seat_idx: number;
  phase: MatchPhase;
  /**
   * Players ordered by seating/turn order (server-defined).
   */
  players: PublicPlayer[];
  /**
   * Number of currently connected spectator (watch) sessions.
   */
  spectator_count: number;
}
export interface LobbyMatchUpsertData {
  match: LobbyMatch;
}
export interface LobbyMatchRemoveData {
  /**
   * Match id (same value as `PublicMatch.id`).
   */
  match_id: string;
}
export interface MatchSnapshotData {
  match: PublicMatch;
  me?: PrivatePlayer;
}
/**
 * Recipient-only private view of the current player.
 */
export interface PrivatePlayer {
  /**
   * Commands the player is currently allowed to say.
   *
   * Protocol v2 is intentionally strict: this is a typed enum instead of free-form strings.
   */
  commands: GameCommand[];
  envido_points: number;
  hand: string[];
  has_flor: boolean;
  /**
   * Seat index in the match's current `players` ordering.
   */
  seat_idx: number;
  used: string[];
}
export interface MatchUpdateData {
  match: PublicMatch;
  me?: PrivatePlayer;
}
/**
 * Confirmation that the client has left a match.
 *
 * This exists so `match.leave` does not need any legacy-style "ok" quirks.
 */
export interface MatchLeftData {
  match_id: string;
}
export interface MatchKickedData {
  match_id: string;
  reason: string;
}
export interface GameSnapshotData {
  game: PublicGameState;
  match_id: string;
}
/**
 * Minimal public game state (protocol v2).
 *
 * This mirrors `trucoshi_game::PublicGameState` but uses `Maybe<T>` for optional fields.
 */
export interface PublicGameState {
  forehand_seat_idx: number;
  hand_state: HandState;
  /**
   * Played cards grouped by round/trick.
   */
  rounds: PlayedCard[][];
  turn_seat_idx: number;
  /**
   * When set, the current hand is finished and the winner team is known.
   */
  winner_team_idx?: TeamIdx;
}
export interface PlayedCard {
  card: string;
  seat_idx: number;
}
export interface GameUpdateData {
  game: PublicGameState;
  match_id: string;
}
export interface ChatSnapshotData {
  room: PublicChatRoom;
}
export interface PublicChatRoom {
  id: string;
  /**
   * Recent chat messages for the room (may be empty, but is always present).
   */
  messages: PublicChatMessage[];
}
export interface PublicChatMessage {
  content: string;
  /**
   * Epoch millis.
   */
  date_ms: number;
  id: string;
  /**
   * Whether the message is system-generated.
   *
   * Protocol v2 is strict: this field is always present (use `false` for normal messages).
   */
  system: boolean;
  user: PublicChatUser;
}
export interface PublicChatUser {
  name: string;
  /**
   * Optional match seat index, if the sender is currently in a match.
   */
  seat_idx?: Uint8;
  team?: TeamIdx;
}
export interface ChatMessageData {
  message: PublicChatMessage;
  room_id: string;
}
export interface ErrorPayload {
  /**
   * Machine-readable error code.
   */
  code: string;
  /**
   * Human-readable error message.
   *
   * Protocol v2 is strict: this field is always present.
   */
  message: string;
}
