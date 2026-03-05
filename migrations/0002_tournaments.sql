-- Minimal tournament schema (no wallet/bets).

CREATE TABLE IF NOT EXISTS tournaments (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'DRAFT', -- DRAFT | OPEN | STARTED | FINISHED | CANCELLED

  owner_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,

  max_players INT NOT NULL DEFAULT 16,
  starts_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS tournaments_status_idx ON tournaments(status);
CREATE INDEX IF NOT EXISTS tournaments_owner_idx ON tournaments(owner_user_id);

CREATE TABLE IF NOT EXISTS tournament_entries (
  id BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  tournament_id BIGINT NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
  user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,

  display_name TEXT NOT NULL,

  UNIQUE (tournament_id, user_id)
);

CREATE INDEX IF NOT EXISTS tournament_entries_tournament_idx ON tournament_entries(tournament_id);

DROP TRIGGER IF EXISTS tournaments_set_updated_at ON tournaments;
CREATE TRIGGER tournaments_set_updated_at
BEFORE UPDATE ON tournaments
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();
