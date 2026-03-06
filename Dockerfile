# syntax=docker/dockerfile:1

# ===== Build stage =====
FROM rust:1.88-bookworm AS build

WORKDIR /app

# Build deps for common crates.
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates pkg-config \
  && rm -rf /var/lib/apt/lists/*

# Copy sources.
COPY Cargo.toml Cargo.lock ./
COPY crates-auth crates-auth
COPY crates-store crates-store
COPY crates-game crates-game
COPY crates-realtime crates-realtime
COPY crates-server crates-server
COPY crates-ws-schema-check crates-ws-schema-check
COPY migrations migrations

# Build the HTTP+WS server.
RUN cargo build -p trucoshi-http --release

# ===== Runtime stage =====
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 app

WORKDIR /app

COPY --from=build /app/target/release/trucoshi-http /app/trucoshi-http

ENV APP_ADDR=0.0.0.0:2992
EXPOSE 2992

USER app

CMD ["/app/trucoshi-http"]
