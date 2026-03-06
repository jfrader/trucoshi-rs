/*
 * AUTO-GENERATED FILE. DO NOT EDIT.
 *
 * Generated from JSON Schema under schemas/ws/v2/*.json
 *
 * Regenerate with:
 *   npm run gen:ws:types
 */

export type C2SMessage =
  | {
      data: PingData;
      type: 'ping';
    }
  | {
      type: 'lobby.snapshot.get';
    }
  | {
      data: MatchCreateData;
      type: 'match.create';
    }
  | {
      data: MatchJoinData;
      type: 'match.join';
    }
  | {
      data: MatchRefData;
      type: 'match.leave';
    }
  | {
      data: MatchReadyData;
      type: 'match.ready';
    }
  | {
      data: MatchRefData;
      type: 'match.snapshot.get';
    }
  | {
      data: MatchRefData;
      type: 'game.snapshot.get';
    }
  | {
      data: MatchRefData;
      type: 'match.start';
    }
  | {
      data: MatchRefData;
      type: 'match.pause';
    }
  | {
      data: MatchRefData;
      type: 'match.resume';
    }
  | {
      data: ChatJoinData;
      type: 'chat.join';
    }
  | {
      data: ChatSayData;
      type: 'chat.say';
    }
  | {
      data: GamePlayCardData;
      type: 'game.play_card';
    }
  | {
      data: GameSayData;
      type: 'game.say';
    };
export type TeamIdx = 0 | 1;
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
export type WsVersion = 2;

export interface WsInMessage {
  /**
   * Optional correlation id (client sets; server echoes in responses).
   *
   * Missing: allowed. Explicit `null`: rejected.
   */
  id?: string;
  /**
   * Tagged payload.
   */
  msg: C2SMessage;
  /**
   * Protocol version (must be exactly 2).
   */
  v: WsVersion;
}
export interface PingData {
  client_time_ms: number;
}
export interface MatchCreateData {
  /**
   * Display name for the creating player.
   */
  name: string;
  /**
   * Match options.
   *
   * Optional: when omitted, the server uses defaults.
   */
  options?: MatchOptions;
  /**
   * Optional requested team (0 or 1). Server may override if full.
   */
  team?: TeamIdx;
}
export interface MatchOptions {
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
   * Protocol v2: this is intentionally constrained to the UI-supported range.
   */
  max_players: number;
  /**
   * Turn timer in milliseconds.
   */
  turn_time_ms: number;
}
export interface MatchJoinData {
  match_id: string;
  name: string;
  /**
   * Optional requested team (0 or 1). Server may override if full.
   */
  team?: TeamIdx;
}
export interface MatchRefData {
  match_id: string;
}
export interface MatchReadyData {
  match_id: string;
  ready: boolean;
}
export interface ChatJoinData {
  room_id: string;
}
export interface ChatSayData {
  content: string;
  room_id: string;
}
export interface GamePlayCardData {
  /**
   * `cardIdx` references the caller's current hand ordering.
   *
   * Message type: `game.play_card`
   */
  card_idx: number;
  match_id: string;
}
export interface GameSayData {
  command: GameCommand;
  match_id: string;
}
