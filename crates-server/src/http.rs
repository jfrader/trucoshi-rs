use axum::{
    Json, Router,
    extract::{FromRequestParts, Path, Query, State, ws::WebSocketUpgrade},
    http::{HeaderMap, StatusCode, request::Parts},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use headers::{Authorization, HeaderMapExt, authorization::Bearer};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use time::{Duration, OffsetDateTime};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use trucoshi_auth::seed;

#[derive(Clone)]
struct AppState {
    store: trucoshi_store::Store,
    tokens: trucoshi_auth::tokens::TokenConfig,
    cookie: CookieConfig,
    twitter: TwitterConfig,
    seed_hash_secret: Vec<u8>,
    realtime: trucoshi_realtime::server::Realtime,
}

#[derive(Clone)]
struct TwitterConfig {
    public_base_url: String,
    client_id: String,
    client_secret: String,
}

#[derive(Clone)]
struct CookieConfig {
    refresh_cookie_name: String,
    refresh_cookie_path: String,
    secure: bool,
    same_site: SameSite,
}

#[derive(Clone, Copy)]
enum SameSite {
    Lax,
    None,
}

impl SameSite {
    fn as_attr(&self) -> &'static str {
        match self {
            SameSite::Lax => "SameSite=Lax",
            SameSite::None => "SameSite=None",
        }
    }
}

// (no FromRef impls here; keep state as Arc<AppState>)

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,trucoshi=debug,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let env = std::env::var("APP_ENV").unwrap_or_else(|_| "development".into());

    // Placeholders for Twitter OAuth2. Replace env vars in prod.
    let public_base_url = std::env::var("PUBLIC_BASE_URL").unwrap_or_else(|_| {
        if env == "production" {
            "https://trucoshi.com".into()
        } else {
            "http://localhost:2992".into()
        }
    });
    let twitter_client_id = std::env::var("TWITTER_CLIENT_ID").unwrap_or_else(|_| "TODO".into());
    let twitter_client_secret =
        std::env::var("TWITTER_CLIENT_SECRET").unwrap_or_else(|_| "TODO".into());

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");

    let jwt_secret_raw = std::env::var("JWT_SECRET").expect("JWT_SECRET is required");
    let jwt_secret = STANDARD
        .decode(jwt_secret_raw.as_bytes())
        .unwrap_or_else(|_| jwt_secret_raw.as_bytes().to_vec());

    let issuer = std::env::var("JWT_ISSUER").unwrap_or_else(|_| "trucoshi".into());
    let audience = std::env::var("JWT_AUDIENCE").unwrap_or_else(|_| "trucoshi".into());

    let store = trucoshi_store::Store::connect(&database_url).await?;
    trucoshi_store::migrate::run_migrations(&store.pool).await?;

    let cookie = CookieConfig {
        refresh_cookie_name: "trucoshi_refresh".into(),
        refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
        secure: env == "production",
        same_site: SameSite::Lax,
    };

    let seed_hash_secret = std::env::var("SEED_HASH_SECRET")
        .expect("SEED_HASH_SECRET is required")
        .into_bytes();

    let state = Arc::new(AppState {
        store,
        tokens: trucoshi_auth::tokens::TokenConfig {
            issuer,
            audience,
            access_ttl: Duration::minutes(15),
            refresh_ttl: Duration::days(60),
            jwt_hs256_secret: jwt_secret,
        },
        cookie,
        twitter: TwitterConfig {
            public_base_url,
            client_id: twitter_client_id,
            client_secret: twitter_client_secret,
        },
        seed_hash_secret,
        realtime: trucoshi_realtime::server::Realtime::new(),
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/v2/ws", get(ws_handler))
        .route("/v1/auth/register", post(register))
        .route("/v1/auth/login", post(login))
        .route("/v1/auth/register-seed", post(register_seed))
        .route("/v1/auth/login-seed", post(login_seed))
        .route("/v1/auth/me", get(me))
        .route("/v1/auth/refresh-tokens", post(refresh_tokens))
        .route("/v1/auth/logout", post(logout))
        .route("/v1/auth/twitter", get(twitter_start))
        .route("/v1/auth/twitter/callback", get(twitter_callback))
        // ===== Tournaments (no wallets/bets) =====
        .route(
            "/v1/tournaments",
            get(list_tournaments).post(create_tournament),
        )
        .route("/v1/tournaments/{id}/entries", post(join_tournament))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr: SocketAddr = std::env::var("APP_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:2992".to_string())
        .parse()?;

    tracing::info!(%addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn healthz() -> impl IntoResponse {
    StatusCode::OK
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // v2 WS: allow both authenticated and guest connections.
    //
    // If an access token is provided and valid, we use the real user id.
    // Otherwise we assign an ephemeral negative id to keep the rest of the
    // realtime layer simple (and keep auth endpoints unchanged).
    let bearer = headers
        .typed_get::<Authorization<Bearer>>()
        .map(|Authorization(b)| b);

    let user_id: i64 = if let Some(bearer) = bearer {
        let Ok(data) = trucoshi_auth::jwt::decode_access_jwt(&state.tokens, bearer.token()) else {
            return StatusCode::UNAUTHORIZED.into_response();
        };

        let Ok(user_id) = data.claims.sub.parse::<i64>() else {
            return StatusCode::UNAUTHORIZED.into_response();
        };

        user_id
    } else {
        use std::sync::atomic::{AtomicI64, Ordering};
        static GUEST_USER_ID: AtomicI64 = AtomicI64::new(-1);
        GUEST_USER_ID.fetch_sub(1, Ordering::Relaxed)
    };

    let realtime = state.realtime.clone();
    ws.on_upgrade(move |socket| async move {
        realtime.handle_socket(socket, user_id).await;
    })
}

// ===== Auth =====

#[derive(Debug, Deserialize)]
struct RegisterRequest {
    email: String,
    password: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    email: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct RegisterSeedRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
struct LoginSeedRequest {
    seed_phrase: String,
}

#[derive(Debug, Serialize)]
struct RegisterSeedResponse {
    seed_phrase: String,
    access_token: String,
    user: UserDto,
}

#[derive(Debug, Serialize)]
struct UserDto {
    id: i64,
    email: Option<String>,
    name: String,
    avatar_url: Option<String>,
    twitter_handle: Option<String>,
    is_email_verified: bool,
    has_seed: bool,
}

#[derive(Debug, Serialize)]
struct AuthResponse {
    access_token: String,
    user: UserDto,
}

#[derive(Debug, sqlx::FromRow)]
struct UserRow {
    id: i64,
    email: Option<String>,
    password_hash: Option<String>,
    name: String,
    avatar_url: Option<String>,
    twitter_handle: Option<String>,
    is_email_verified: bool,
    has_seed: bool,
}

#[derive(Debug, sqlx::FromRow)]
struct RefreshRow {
    id: i64,
    user_id: i64,
    expires_at: OffsetDateTime,
    revoked_at: Option<OffsetDateTime>,
}

#[derive(Debug)]
struct AuthedUser {
    user_id: i64,
}

impl FromRequestParts<Arc<AppState>> for AuthedUser {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let app = state;

        let Authorization(bearer) = parts
            .headers
            .typed_get::<Authorization<Bearer>>()
            .ok_or_else(|| ApiError::unauthorized("missing bearer token"))?;

        let token = bearer.token();
        let data = trucoshi_auth::jwt::decode_access_jwt(&app.tokens, token)
            .map_err(|_| ApiError::unauthorized("invalid token"))?;

        let user_id: i64 = data
            .claims
            .sub
            .parse()
            .map_err(|_| ApiError::unauthorized("invalid sub"))?;

        Ok(Self { user_id })
    }
}

async fn register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<Response, ApiError> {
    let password_hash = trucoshi_auth::password::hash_password(&req.password)
        .map_err(|_| ApiError::bad_request("invalid password"))?;

    let rec: UserRow = sqlx::query_as(
        r#"
        INSERT INTO users (email, password_hash, name)
        VALUES ($1, $2, $3)
        RETURNING id, email, password_hash, name, avatar_url, twitter_handle, is_email_verified, has_seed
        "#,
    )
    .bind(req.email)
    .bind(password_hash)
    .bind(req.name)
    .fetch_one(&state.store.pool)
    .await
    .map_err(|e| ApiError::from_sqlx(e, "email already taken"))?;

    issue_tokens_and_cookie(&state, rec.id).await
}

async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> Result<Response, ApiError> {
    let rec: UserRow = sqlx::query_as(
        r#"SELECT id, email, password_hash, name, avatar_url, twitter_handle, is_email_verified, has_seed FROM users WHERE email = $1"#,
    )
    .bind(req.email)
    .fetch_optional(&state.store.pool)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::unauthorized("invalid email or password"))?;

    let Some(hash) = rec.password_hash.clone() else {
        return Err(ApiError::unauthorized("invalid email or password"));
    };

    let ok = trucoshi_auth::password::verify_password(&hash, &req.password)
        .map_err(ApiError::internal)?;
    if !ok {
        return Err(ApiError::unauthorized("invalid email or password"));
    }

    issue_tokens_and_cookie(&state, rec.id).await
}

async fn register_seed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterSeedRequest>,
) -> Result<Response, ApiError> {
    let trimmed = req.name.trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request("name required"));
    }
    if trimmed.chars().count() > 40 {
        return Err(ApiError::bad_request("name too long"));
    }
    let name = trimmed.to_string();

    let seed_phrase = normalize_seed_phrase(&seed::generate_seed_phrase(None))?;
    let seed_hash = seed::hash_seed_phrase(&seed_phrase, state.seed_hash_secret.as_slice());

    let user_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO users (name, seed_hash, has_seed)
        VALUES ($1, $2, TRUE)
        RETURNING id
        "#,
    )
    .bind(&name)
    .bind(seed_hash)
    .fetch_one(&state.store.pool)
    .await
    .map_err(ApiError::internal)?;

    issue_tokens_and_cookie_with_body(&state, user_id, move |auth| RegisterSeedResponse {
        seed_phrase,
        access_token: auth.access_token,
        user: auth.user,
    })
}

async fn login_seed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginSeedRequest>,
) -> Result<Response, ApiError> {
    let normalized = normalize_seed_phrase(&req.seed_phrase)?;
    let seed_hash = seed::hash_seed_phrase(&normalized, state.seed_hash_secret.as_slice());

    let user_id: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM users WHERE seed_hash = $1 AND has_seed = TRUE"
    )
    .bind(seed_hash)
    .fetch_optional(&state.store.pool)
    .await
    .map_err(ApiError::internal)?;

    let user_id = user_id.ok_or_else(|| ApiError::unauthorized("invalid seed phrase"))?;
    issue_tokens_and_cookie(&state, user_id).await
}

async fn me(
    State(state): State<Arc<AppState>>,
    AuthedUser { user_id }: AuthedUser,
) -> Result<Json<UserDto>, ApiError> {
    let rec: UserRow = sqlx::query_as(
        r#"SELECT id, email, password_hash, name, avatar_url, twitter_handle, is_email_verified, has_seed FROM users WHERE id = $1"#,
    )
    .bind(user_id)
    .fetch_one(&state.store.pool)
    .await
    .map_err(ApiError::internal)?;

    Ok(Json(UserDto {
        id: rec.id,
        email: rec.email,
        name: rec.name,
        avatar_url: rec.avatar_url,
        twitter_handle: rec.twitter_handle,
        is_email_verified: rec.is_email_verified,
        has_seed: rec.has_seed,
    }))
}

#[derive(Debug, Deserialize)]
struct RefreshRequest {
    // body intentionally empty; cookie only
}

async fn refresh_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(_req): Json<RefreshRequest>,
) -> Result<Response, ApiError> {
    let refresh = get_cookie_value(&headers, &state.cookie.refresh_cookie_name)
        .ok_or_else(|| ApiError::unauthorized("missing refresh cookie"))?;

    let token_hash = trucoshi_auth::refresh::hash_refresh_token(&refresh);

    let now = OffsetDateTime::now_utc();

    let rec: RefreshRow = sqlx::query_as(
        r#"
        SELECT id, user_id, expires_at, revoked_at
        FROM refresh_tokens
        WHERE token_hash = $1
        "#,
    )
    .bind(token_hash)
    .fetch_optional(&state.store.pool)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::unauthorized("invalid refresh"))?;

    if rec.revoked_at.is_some() {
        return Err(ApiError::unauthorized("invalid refresh"));
    }

    if rec.expires_at <= now {
        return Err(ApiError::unauthorized("expired refresh"));
    }

    // Rotation: revoke old, issue new.
    sqlx::query(r#"UPDATE refresh_tokens SET revoked_at = now() WHERE id = $1"#)
        .bind(rec.id)
        .execute(&state.store.pool)
        .await
        .map_err(ApiError::internal)?;

    issue_tokens_and_cookie(&state, rec.user_id).await
}

async fn logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Some(refresh) = get_cookie_value(&headers, &state.cookie.refresh_cookie_name) {
        let token_hash = trucoshi_auth::refresh::hash_refresh_token(&refresh);
        let _ =
            sqlx::query(r#"UPDATE refresh_tokens SET revoked_at = now() WHERE token_hash = $1"#)
                .bind(token_hash)
                .execute(&state.store.pool)
                .await;
    }

    // Clear cookie
    let mut res = Response::new(().into_response().into_body());
    *res.status_mut() = StatusCode::NO_CONTENT;
    res.headers_mut().append(
        axum::http::header::SET_COOKIE,
        clear_cookie_header(
            &state.cookie.refresh_cookie_name,
            &state.cookie.refresh_cookie_path,
        )
        .parse()
        .map_err(ApiError::internal)?,
    );
    Ok(res)
}

async fn issue_tokens_and_cookie(
    state: &Arc<AppState>,
    user_id: i64,
) -> Result<Response, ApiError> {
    issue_tokens_and_cookie_with_body(state, user_id, |auth| auth).await
}

async fn issue_tokens_and_cookie_with_body<T, F>(
    state: &Arc<AppState>,
    user_id: i64,
    build: F,
) -> Result<Response, ApiError>
where
    T: Serialize,
    F: FnOnce(AuthResponse) -> T,
{
    let (auth, refresh) = issue_tokens_inner(state, user_id).await?;
    let body = build(auth);
    let mut res = (StatusCode::OK, Json(body)).into_response();
    res.headers_mut().append(
        axum::http::header::SET_COOKIE,
        build_refresh_cookie(state, &refresh)
            .parse()
            .map_err(ApiError::internal)?,
    );

    Ok(res)
}

async fn issue_tokens_inner(
    state: &Arc<AppState>,
    user_id: i64,
) -> Result<(AuthResponse, String), ApiError> {
    let access =
        trucoshi_auth::jwt::issue_access_jwt(&state.tokens, user_id).map_err(ApiError::internal)?;

    let refresh = trucoshi_auth::refresh::new_refresh_token();
    let token_hash = trucoshi_auth::refresh::hash_refresh_token(&refresh);

    let expires_at = OffsetDateTime::now_utc() + state.tokens.refresh_ttl;

    sqlx::query(
        r#"
        INSERT INTO refresh_tokens (user_id, expires_at, token_hash)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(user_id)
    .bind(expires_at)
    .bind(token_hash)
    .execute(&state.store.pool)
    .await
    .map_err(ApiError::internal)?;

    let rec: UserRow = sqlx::query_as(
        r#"SELECT id, email, password_hash, name, avatar_url, twitter_handle, is_email_verified, has_seed FROM users WHERE id = $1"#,
    )
    .bind(user_id)
    .fetch_one(&state.store.pool)
    .await
    .map_err(ApiError::internal)?;

    let auth = AuthResponse {
        access_token: access,
        user: UserDto {
            id: rec.id,
            email: rec.email,
            name: rec.name,
            avatar_url: rec.avatar_url,
            twitter_handle: rec.twitter_handle,
            is_email_verified: rec.is_email_verified,
            has_seed: rec.has_seed,
        },
    };

    Ok((auth, refresh))
}

fn build_refresh_cookie(state: &AppState, refresh: &str) -> String {
    // Cookie attributes: HttpOnly refresh; Secure in prod; SameSite Lax; strict path.
    // If SameSite=None, Secure must be true.
    let secure = state.cookie.secure || matches!(state.cookie.same_site, SameSite::None);

    let mut parts = vec![
        format!("{}={}", state.cookie.refresh_cookie_name, refresh),
        format!("Path={}", state.cookie.refresh_cookie_path),
        "HttpOnly".into(),
        state.cookie.same_site.as_attr().into(),
    ];

    if secure {
        parts.push("Secure".into());
    }

    parts.join("; ")
}

fn clear_cookie_header(name: &str, path: &str) -> String {
    format!(
        "{name}=; Path={path}; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; SameSite=Lax"
    )
}

fn get_cookie_value(headers: &HeaderMap, cookie_name: &str) -> Option<String> {
    let cookie = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    for part in cookie.split(';') {
        let part = part.trim();
        if let Some((k, v)) = part.split_once('=') {
            if k.trim() == cookie_name {
                return Some(v.trim().to_string());
            }
        }
    }
    None
}

fn normalize_seed_phrase(input: &str) -> Result<String, ApiError> {
    let words: Vec<String> = input
        .split_whitespace()
        .map(|w| w.trim().to_lowercase())
        .filter(|w| !w.is_empty())
        .collect();

    if words.len() != 5 {
        return Err(ApiError::bad_request("seed_phrase must contain exactly 5 words"));
    }

    Ok(words.join(" "))
}

// ===== Tournaments (minimal; no wallets/bets) =====

#[derive(Debug, Deserialize)]
struct ListTournamentsQuery {
    #[serde(default = "default_tournament_limit")]
    limit: i64,
}

fn default_tournament_limit() -> i64 {
    20
}

async fn list_tournaments(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ListTournamentsQuery>,
) -> Result<Json<Vec<crate::tournaments::types::Tournament>>, ApiError> {
    let repo = crate::tournaments::repo::TournamentsRepo::new(state.store.pool.clone());
    let limit = q.limit.clamp(1, 200);

    let viewer_user_id = if let Some(Authorization(bearer)) =
        headers.typed_get::<Authorization<Bearer>>()
    {
        let data = trucoshi_auth::jwt::decode_access_jwt(&state.tokens, bearer.token())
            .map_err(|_| ApiError::unauthorized("invalid token"))?;
        let user_id: i64 = data
            .claims
            .sub
            .parse()
            .map_err(|_| ApiError::unauthorized("invalid sub"))?;
        Some(user_id)
    } else {
        None
    };

    let list = repo
        .list_visible_tournaments(limit, viewer_user_id)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(list))
}

#[derive(Debug, Deserialize)]
struct CreateTournamentRequest {
    name: String,
    max_players: i32,
    starts_at: Option<OffsetDateTime>,
}

async fn create_tournament(
    State(state): State<Arc<AppState>>,
    AuthedUser { user_id }: AuthedUser,
    Json(req): Json<CreateTournamentRequest>,
) -> Result<(StatusCode, Json<crate::tournaments::types::Tournament>), ApiError> {
    let name = req.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("name required"));
    }
    if req.max_players < 2 {
        return Err(ApiError::bad_request("max_players must be >= 2"));
    }

    let repo = crate::tournaments::repo::TournamentsRepo::new(state.store.pool.clone());
    let t = repo
        .create_tournament(Some(user_id), name, req.max_players, req.starts_at)
        .await
        .map_err(ApiError::internal)?;

    Ok((StatusCode::CREATED, Json(t)))
}

#[derive(Debug, Deserialize)]
struct JoinTournamentRequest {
    display_name: String,
}

async fn join_tournament(
    State(state): State<Arc<AppState>>,
    AuthedUser { user_id }: AuthedUser,
    Path(id): Path<i64>,
    Json(req): Json<JoinTournamentRequest>,
) -> Result<(StatusCode, Json<crate::tournaments::types::TournamentEntry>), ApiError> {
    let display_name = req.display_name.trim();
    if display_name.is_empty() {
        return Err(ApiError::bad_request("display_name required"));
    }

    let repo = crate::tournaments::repo::TournamentsRepo::new(state.store.pool.clone());
    let entry = repo
        .add_entry(id, Some(user_id), display_name)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("duplicate") || msg.contains("UNIQUE") {
                ApiError::bad_request("already joined")
            } else {
                ApiError::internal(e)
            }
        })?;

    Ok((StatusCode::CREATED, Json(entry)))
}

// ===== Twitter (placeholders) =====

async fn twitter_start(State(state): State<Arc<AppState>>) -> Result<Response, ApiError> {
    // Placeholder: in real implementation, redirect to Twitter OAuth2 authorize URL.
    // We return 501 with instructions so the client can be built now.
    let msg = format!(
        "Twitter OAuth not configured. Set TWITTER_CLIENT_ID/TWITTER_CLIENT_SECRET. callback={}",
        format!("{}/v1/auth/twitter/callback", state.twitter.public_base_url)
    );
    Err(ApiError {
        status: StatusCode::NOT_IMPLEMENTED,
        message: msg,
    })
}

async fn twitter_callback(State(_state): State<Arc<AppState>>) -> Result<Response, ApiError> {
    // Placeholder: would verify state/code, fetch profile, upsert/link user, then issue tokens.
    Err(ApiError {
        status: StatusCode::NOT_IMPLEMENTED,
        message: "Twitter OAuth not implemented yet".into(),
    })
}

// ===== Errors =====

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(e: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: e.to_string(),
        }
    }

    fn from_sqlx(e: sqlx::Error, dup_message: &str) -> Self {
        // crude but good enough for now
        match &e {
            sqlx::Error::Database(db) if db.message().contains("duplicate") => {
                Self::bad_request(dup_message)
            }
            _ => Self::internal(e),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct ErrBody {
            error: String,
        }

        (
            self.status,
            Json(ErrBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_seed_phrase;

    #[test]
    fn normalizes_seed_phrase_trims_and_lowercases() {
        let normalized = normalize_seed_phrase("  Uno   DOS tres  Cuatro   CINCO " ).unwrap();
        assert_eq!(normalized, "uno dos tres cuatro cinco");
    }

    #[test]
    fn rejects_seed_phrase_with_wrong_word_count() {
        assert!(normalize_seed_phrase("uno dos tres").is_err());
    }
}
