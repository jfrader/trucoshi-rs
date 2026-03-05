# PROJECT.md — Trucoshi-rs context

This file exists so a new contributor (or a fresh AI session) can quickly understand what this repo is, what we’re building, and what “done” looks like.

## What this is

`trucoshi-rs` is the **Rust monorepo backend** for Trucoshi.

It is intended to replace the previous backend stack where the `trucoshi-client` relied on two separate services:

- `github.com/jfrader/trucoshi` (Node/TypeScript)
- `github.com/jfrader/lightning-accounts` (Node/TypeScript)

The goal is to converge on **one backend** in Rust.

## Scope / non-goals

- We keep the *authentication* approach/integration from the old stack as reference.
- **Non-goal:** importing Lightning wallet / deposit-withdraw / bets functionality into this repo.

## Current focus: WebSocket protocol v2 (no legacy compatibility)

We are designing and implementing a clean, strict **WS protocol v2** for realtime gameplay:

- Versioned wire envelope with `v: 2`.
- Versioned, authoritative JSON Schemas in `schemas/ws/v2/`.
- Generated client types (currently TypeScript) from those schemas.
- Core message namespaces for:
  - lobby
  - match lifecycle (create/join/leave/ready/start/pause/resume)
  - gameplay (snapshots + updates)
  - chat

See: `schemas/ws/v2/README.md`.

## Running (dev)

There is a Docker dev stack (Postgres + API) for deployments/testing:

```bash
docker-compose -f docker-compose.dev.yml up --build
```

Key endpoints:

- `GET /healthz`
- `GET /v2/ws` (WebSocket; requires `Authorization: Bearer <accessToken>`)

More detail: `DEPLOYING.md`.

## Repo layout (high level)

- `crates-realtime/` — WS v2 protocol types + realtime server
- `crates-server/bin-http/` — HTTP API + WS upgrade endpoint (`trucoshi-http`)
- `crates-store/` — DB access + migrations
- `migrations/` — SQL migrations
- `schemas/ws/v2/` — authoritative JSON Schemas + generated TS types

## Working agreements

- Prefer small, coherent commits.
- Keep protocol strictness explicit (reject invalid versions, avoid silent no-ops).
- Keep `cargo fmt` + `cargo test` passing.

## Next steps (typical)

- Keep tightening WS v2 schema constraints + mirror them server-side.
- Add protocol-level integration tests (full lobby → match → game flows).
- Improve deployability (env docs, healthchecks, prod compose/k8s later).
