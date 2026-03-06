# Trucoshi WebSocket Protocol v2

This directory contains the **versioned, authoritative JSON Schemas** for the Trucoshi realtime WebSocket protocol.

## Files

- `in.json` — **Inbound frame envelope** (client → server)
- `out.json` — **Outbound frame envelope** (server → client)
- `c2s.json` — **Client → server payloads only** (`msg` values)
- `s2c.json` — **Server → client payloads only** (`msg` values)
- `in.ts` / `out.ts` — TypeScript types for the WS frame envelopes
- `c2s.ts` / `s2c.ts` — TypeScript types generated from the JSON Schemas

All schemas are pinned under the `v2/` path and include stable `$id` URLs:

- `https://trucoshi.dev/schemas/ws/v2/in.json`
- `https://trucoshi.dev/schemas/ws/v2/out.json`
- `https://trucoshi.dev/schemas/ws/v2/c2s.json`
- `https://trucoshi.dev/schemas/ws/v2/s2c.json`

## Envelope (wire format)

All WebSocket frames are JSON objects.

### Client → server

```jsonc
{
  "v": 2,
  "id": "optional-correlation-id",
  "msg": { "type": "ping", "data": { "client_time_ms": 123 } }
}
```

### Server → client

```jsonc
{
  "v": 2,
  "id": "optional-correlation-id",
  "msg": { "type": "pong", "data": { "server_time_ms": 456, "client_time_ms": 123 } }
}
```

### Notes

- `v` is **required** and must be exactly `2`.
- `id` is optional; when provided by the client, the server **echoes it back** on the relevant response(s).
- All message types are dot-separated namespaces using `snake_case` segments.
- Player readiness is exposed per-player via `LobbyMatch.players[].ready` / `PublicMatch.players[].ready` (no separate ready list).
- Player seating is implied by `players[]` ordering (no `PublicPlayer.seat_idx`).
- Match ownership is exposed per-match via `owner_seat_idx` (index into `players[]`), keeping `PublicPlayer` minimal.
- Match score (`team_points`) is only part of match-scoped snapshots/updates (`PublicMatch`), not lobby summaries.

## Core event namespaces

### Connection / utility

- `hello` (S2C) — sent immediately after connect
- `ping` (C2S) → `pong` (S2C)
- `error` (S2C)

### Lobby

- `lobby.snapshot.get` (C2S) → `lobby.snapshot` (S2C)
- `lobby.match.upsert` (S2C)
- `lobby.match.remove` (S2C)

### Match (lifecycle / seating)

- `match.create` (C2S) — creates a match and seats the creator (requires `data.name`; optional `data.team` + `data.options`)
- `match.join` (C2S)
- `match.watch` (C2S) — join a match as a spectator (hands hidden; no seat)
- `match.leave` (C2S) → `match.left` (S2C) — explicit confirmation
- `match.ready` (C2S)
- `match.start` (C2S)
- `match.pause` (C2S)
- `match.resume` (C2S)
- `match.rematch` (C2S) — restart a finished match into a fresh lobby with the same players/teams
- `match.snapshot.get` (C2S) → `match.snapshot` (S2C)
- `match.update` (S2C)

### Gameplay

- `game.snapshot.get` (C2S) → `game.snapshot` (S2C)
- `game.play_card` (C2S)
- `game.say` (C2S)
- `game.update` (S2C)

### Chat

- `chat.join` (C2S) → `chat.snapshot` (S2C)
- `chat.say` (C2S)
- `chat.message` (S2C)

## Regeneration

From the repo root:

- Regenerate JSON Schemas:
  - `npm run gen:ws:schema`
- Regenerate TypeScript types (envelopes + payloads):
  - `npm run gen:ws:types`
- Regenerate both:
  - `npm run gen:ws`
