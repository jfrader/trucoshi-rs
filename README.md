# trucoshi-rs

Rust monorepo backend for Trucoshi.

## Dev (Docker)

Requirements:

- `docker`
- `docker-compose` (v1)

Start Postgres + API:

```bash
docker-compose -f docker-compose.dev.yml up --build
```

API:

- Health: `GET http://localhost:2992/healthz`
- WebSocket v2: `GET ws://localhost:2992/v2/ws` (requires `Authorization: Bearer <accessToken>`)

WS v2 schemas + generated TS types:

- `schemas/ws/v2/*.json`
- `schemas/ws/v2/*.ts`

## Regenerate WS schemas/types

```bash
npm run gen:ws
```

## Notes

- DB migrations are applied automatically on startup.
- For now, Twitter OAuth env vars are placeholders.
- See `DEPLOYING.md` for more details.
