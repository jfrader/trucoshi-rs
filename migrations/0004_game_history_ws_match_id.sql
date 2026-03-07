ALTER TABLE game_matches
    ADD COLUMN IF NOT EXISTS ws_match_id UUID;

UPDATE game_matches
SET ws_match_id = (options->>'ws_match_id')::uuid
WHERE ws_match_id IS NULL
  AND options ? 'ws_match_id'
  AND (options->>'ws_match_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

ALTER TABLE game_matches
    ALTER COLUMN ws_match_id SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS game_matches_ws_match_id_idx
    ON game_matches(ws_match_id);
