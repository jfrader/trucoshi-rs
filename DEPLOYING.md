# Deploying trucoshi-rs (dev)

This repo contains a Rust HTTP + WebSocket server binary (`trucoshi-http`).

## Local dev via Docker Compose

From the repo root:

```bash
docker-compose -f docker-compose.dev.yml up --build
```

This starts:

- Postgres on `localhost:5432` (db `trucoshi`, user/pass `postgres`)
- API on `http://localhost:2992`

### Endpoints

- `GET /healthz`
- `GET /v2/ws` (WebSocket; requires `Authorization: Bearer <accessToken>`)
- Auth (work-in-progress but wired):
  - `POST /v1/auth/register`
  - `POST /v1/auth/login`
  - `GET /v1/auth/me`
  - `POST /v1/auth/refresh-tokens`
  - `POST /v1/auth/logout`

## Environment variables (api)

Required:

- `DATABASE_URL`
- `JWT_SECRET`
- `SEED_HASH_SECRET`

Optional:

- `APP_ADDR` (default `0.0.0.0:2992`)
- `APP_ENV` (`development`|`production`)
- `PUBLIC_BASE_URL`
- `JWT_ISSUER` / `JWT_AUDIENCE`

Notes:

- `JWT_SECRET` can be plain bytes or base64; the server will try base64 decode and fall back to raw.
- `SEED_HASH_SECRET` is any random string; keep it constant per environment so seed users can log back in.
- DB migrations are applied automatically at startup.
