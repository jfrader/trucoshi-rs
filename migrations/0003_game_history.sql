-- Append-only game history for analytics / ML training.
-- Goal: record every user-visible action + enough metadata to reconstruct games.

CREATE TABLE IF NOT EXISTS game_matches (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,

  -- Protocol / engine versioning
  server_version TEXT,
  protocol_version INT,

  -- Determinism hooks
  rng_seed BIGINT,

  -- Options at match creation
  options JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS game_matches_created_at_idx ON game_matches(created_at);

CREATE TABLE IF NOT EXISTS game_match_players (
  id BIGSERIAL PRIMARY KEY,
  match_id BIGINT NOT NULL REFERENCES game_matches(id) ON DELETE CASCADE,

  seat_idx INT NOT NULL,
  team_idx INT NOT NULL,

  user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  display_name TEXT NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (match_id, seat_idx)
);

CREATE INDEX IF NOT EXISTS game_match_players_match_id_idx ON game_match_players(match_id);
CREATE INDEX IF NOT EXISTS game_match_players_user_id_idx ON game_match_players(user_id);

-- Append-only event log.
-- `type` should be a stable string (e.g. "match.create", "game.play_card", "game.say", "match.phase", ...)
CREATE TABLE IF NOT EXISTS game_match_events (
  id BIGSERIAL PRIMARY KEY,
  match_id BIGINT NOT NULL REFERENCES game_matches(id) ON DELETE CASCADE,

  -- Total order within a match for easy replay.
  seq BIGINT NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Actor metadata
  actor_seat_idx INT,
  actor_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,

  type TEXT NOT NULL,
  data JSONB NOT NULL DEFAULT '{}'::jsonb,

  UNIQUE (match_id, seq)
);

CREATE INDEX IF NOT EXISTS game_match_events_match_seq_idx ON game_match_events(match_id, seq);
CREATE INDEX IF NOT EXISTS game_match_events_type_idx ON game_match_events(type);
CREATE INDEX IF NOT EXISTS game_match_events_created_at_idx ON game_match_events(created_at);

-- Optional periodic snapshots (kept sparse) to speed up replay.
CREATE TABLE IF NOT EXISTS game_match_snapshots (
  id BIGSERIAL PRIMARY KEY,
  match_id BIGINT NOT NULL REFERENCES game_matches(id) ON DELETE CASCADE,
  seq BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Public + private engine state as JSON for debugging/training. Keep sensitive info out.
  state JSONB NOT NULL,

  UNIQUE (match_id, seq)
);

CREATE INDEX IF NOT EXISTS game_match_snapshots_match_seq_idx ON game_match_snapshots(match_id, seq);
