use axum::{
    Json, Router,
    extract::{FromRequestParts, Path, Query, State, ws::WebSocketUpgrade},
    http::{HeaderMap, StatusCode, request::Parts},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use headers::{Authorization, HeaderMapExt, authorization::Bearer};
use mailer::Mailer;
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, net::SocketAddr, sync::Arc};
use time::{Duration, OffsetDateTime};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use trucoshi_auth::seed;
use trucoshi_realtime::protocol::ws::v2::schema::ActiveMatchSummary;
use trucoshi_realtime::server::Realtime;
use uuid::Uuid;

mod game_history;
mod mailer;

#[derive(Clone)]
struct AppState {
    store: trucoshi_store::Store,
    tokens: trucoshi_auth::tokens::TokenConfig,
    cookie: CookieConfig,
    twitter: TwitterConfig,
    public_base_url: String,
    mailer: Mailer,
    seed_hash_secret: Vec<u8>,
    realtime: Realtime,
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

    // Best-effort game history persistence.
    //
    // IMPORTANT: this must never interfere with gameplay; failures are logged and dropped.
    let (history_tx, history_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(game_history::run_game_history_worker(
        store.clone(),
        history_rx,
    ));

    let cookie = CookieConfig {
        refresh_cookie_name: "trucoshi_refresh".into(),
        refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
        secure: env == "production",
        same_site: SameSite::Lax,
    };

    let mailer = mailer::Mailer::from_env()?;

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
            public_base_url: public_base_url.clone(),
            client_id: twitter_client_id,
            client_secret: twitter_client_secret,
        },
        public_base_url,
        mailer,
        seed_hash_secret,
        realtime: Realtime::new_with_history(history_tx),
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
        .route(
            "/v1/auth/send-verification-email",
            post(send_verification_email),
        )
        .route("/v1/auth/verify-email", post(verify_email))
        .route("/v1/auth/forgot-password", post(forgot_password))
        .route("/v1/auth/reset-password", post(reset_password))
        // ===== Tournaments (no wallets/bets) =====
        .route(
            "/v1/tournaments",
            get(list_tournaments).post(create_tournament),
        )
        .route("/v1/tournaments/{id}", get(get_tournament))
        .route(
            "/v1/tournaments/{id}/entries",
            get(list_tournament_entries).post(join_tournament),
        )
        .route("/v1/tournaments/{id}/open", post(open_tournament))
        .route("/v1/tournaments/{id}/cancel", post(cancel_tournament))
        .route("/v1/history/matches/{id}", get(get_match_history))
        .route("/v1/matches/active", get(list_active_matches))
        .route("/v1/stats/players/{id}", get(get_player_profile))
        .route("/v1/stats/leaderboard", get(get_leaderboard))
        .route("/v1/auth/twitter", get(twitter_start))
        .route("/v1/auth/twitter/callback", get(twitter_callback))
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
    // Authenticated WS is preferred, but we allow guest connections to make
    // "open the app and see it" workflows easy (especially on web where WS
    // headers aren't supported).

    let bearer = headers
        .typed_get::<Authorization<Bearer>>()
        .map(|Authorization(b)| b);

    let user_id = if let Some(bearer) = bearer {
        let Ok(data) = trucoshi_auth::jwt::decode_access_jwt(&state.tokens, bearer.token()) else {
            return StatusCode::UNAUTHORIZED.into_response();
        };

        let Ok(user_id) = data.claims.sub.parse::<i64>() else {
            return StatusCode::UNAUTHORIZED.into_response();
        };

        user_id
    } else {
        // Guest ids are ephemeral and negative to avoid colliding with DB ids.
        // (This does not grant access to any HTTP endpoints that require auth.)
        let r: u64 = rand::random();
        -(r as i64).abs()
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

#[derive(Debug, Deserialize)]
struct ForgotPasswordRequest {
    email: String,
}

#[derive(Debug, Deserialize)]
struct ResetPasswordRequest {
    token: String,
    password: String,
}

#[derive(Debug, Deserialize)]
struct VerifyEmailRequest {
    token: String,
}

#[derive(Debug, sqlx::FromRow)]
struct UserTokenRow {
    id: i64,
    user_id: i64,
    expires_at: OffsetDateTime,
    consumed_at: Option<OffsetDateTime>,
}

const TOKEN_TYPE_VERIFY_EMAIL: &str = "verify_email";
const TOKEN_TYPE_RESET_PASSWORD: &str = "reset_password";

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
    .await
}

async fn login_seed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginSeedRequest>,
) -> Result<Response, ApiError> {
    let normalized = normalize_seed_phrase(&req.seed_phrase)?;
    let seed_hash = seed::hash_seed_phrase(&normalized, state.seed_hash_secret.as_slice());

    let user_id: Option<i64> =
        sqlx::query_scalar("SELECT id FROM users WHERE seed_hash = $1 AND has_seed = TRUE")
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
        has_seed: rec.has_seed,
        is_email_verified: rec.is_email_verified,
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

async fn send_verification_email(
    State(state): State<Arc<AppState>>,
    AuthedUser { user_id }: AuthedUser,
) -> Result<Response, ApiError> {
    let rec: UserRow = sqlx::query_as(
        r#"SELECT id, email, password_hash, name, avatar_url, twitter_handle, is_email_verified, has_seed FROM users WHERE id = $1"#,
    )
    .bind(user_id)
    .fetch_one(&state.store.pool)
    .await
    .map_err(ApiError::internal)?;

    let UserRow {
        id,
        email,
        name,
        is_email_verified,
        ..
    } = rec;

    if is_email_verified {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }

    let email = email.ok_or_else(|| ApiError::bad_request("user has no email configured"))?;

    let token = create_user_token(
        &state.store.pool,
        id,
        TOKEN_TYPE_VERIFY_EMAIL,
        Duration::minutes(30),
    )
    .await?;

    let link = public_link(
        &state.public_base_url,
        &format!("verify-email?token={token}"),
    );

    let subject = "Verificá tu email en Trucoshi";
    let body = format!(
        "Hola {},

Para verificar tu email en Trucoshi, abrí este link: {}
Si no creaste esta cuenta, ignorá este mensaje.
",
        name, link
    );

    state
        .mailer
        .send(&email, subject, &body)
        .await
        .map_err(ApiError::internal)?;

    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn verify_email(
    State(state): State<Arc<AppState>>,
    Json(req): Json<VerifyEmailRequest>,
) -> Result<Response, ApiError> {
    let token = req.token.trim();
    if token.is_empty() {
        return Err(ApiError::bad_request("token is required"));
    }

    let row = consume_user_token(&state.store.pool, TOKEN_TYPE_VERIFY_EMAIL, token).await?;

    sqlx::query(
        "UPDATE users SET is_email_verified = TRUE, email_verified_at = now() WHERE id = $1",
    )
    .bind(row.user_id)
    .execute(&state.store.pool)
    .await
    .map_err(ApiError::internal)?;

    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn forgot_password(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ForgotPasswordRequest>,
) -> Result<Response, ApiError> {
    let email = req.email.trim();
    if email.is_empty() {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }

    if let Some(rec) = sqlx::query_as::<_, UserRow>(
        r#"SELECT id, email, password_hash, name, avatar_url, twitter_handle, is_email_verified, has_seed
           FROM users WHERE email IS NOT NULL AND lower(email) = lower($1)"#,
    )
    .bind(email)
    .fetch_optional(&state.store.pool)
    .await
    .map_err(ApiError::internal)?
    {
        if rec.password_hash.is_some() {
            if let Some(rec_email) = rec.email.clone() {
                let token = create_user_token(
                    &state.store.pool,
                    rec.id,
                    TOKEN_TYPE_RESET_PASSWORD,
                    Duration::minutes(30),
                )
                .await?;

                let link = public_link(
                    &state.public_base_url,
                    &format!("reset-password?token={token}"),
                );

                let subject = "Resetear tu contraseña de Trucoshi";
                let body = format!(
                    "Hola {},

Para resetear tu contraseña, abrí este link: {}
Si no pediste el cambio, ignorá este mensaje.
",
                    rec.name, link
                );

                if let Err(err) = state.mailer.send(&rec_email, subject, &body).await {
                    tracing::error!(error = %err, "failed to send reset password email");
                }
            }
        }
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn reset_password(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ResetPasswordRequest>,
) -> Result<Response, ApiError> {
    let token = req.token.trim();
    if token.is_empty() {
        return Err(ApiError::bad_request("token is required"));
    }

    if req.password.len() < 8 {
        return Err(ApiError::bad_request(
            "password must be at least 8 characters",
        ));
    }

    let row = consume_user_token(&state.store.pool, TOKEN_TYPE_RESET_PASSWORD, token).await?;

    let password_hash = trucoshi_auth::password::hash_password(&req.password)
        .map_err(|_| ApiError::bad_request("invalid password"))?;

    let mut tx = state.store.pool.begin().await.map_err(ApiError::internal)?;

    sqlx::query("UPDATE users SET password_hash = $1 WHERE id = $2")
        .bind(&password_hash)
        .bind(row.user_id)
        .execute(&mut *tx)
        .await
        .map_err(ApiError::internal)?;

    sqlx::query("DELETE FROM refresh_tokens WHERE user_id = $1")
        .bind(row.user_id)
        .execute(&mut *tx)
        .await
        .map_err(ApiError::internal)?;

    tx.commit().await.map_err(ApiError::internal)?;

    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn create_user_token(
    pool: &sqlx::PgPool,
    user_id: i64,
    token_type: &str,
    ttl: Duration,
) -> Result<String, ApiError> {
    let token = trucoshi_auth::refresh::new_refresh_token();
    let token_hash = trucoshi_auth::refresh::hash_refresh_token(&token);
    let expires_at = OffsetDateTime::now_utc() + ttl;

    let mut tx = pool.begin().await.map_err(ApiError::internal)?;

    sqlx::query("DELETE FROM user_tokens WHERE user_id = $1 AND token_type = $2")
        .bind(user_id)
        .bind(token_type)
        .execute(&mut *tx)
        .await
        .map_err(ApiError::internal)?;

    sqlx::query(
        "INSERT INTO user_tokens (user_id, token_type, token_hash, expires_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(user_id)
    .bind(token_type)
    .bind(token_hash)
    .bind(expires_at)
    .execute(&mut *tx)
    .await
    .map_err(ApiError::internal)?;

    tx.commit().await.map_err(ApiError::internal)?;

    Ok(token)
}

async fn consume_user_token(
    pool: &sqlx::PgPool,
    token_type: &str,
    token: &str,
) -> Result<UserTokenRow, ApiError> {
    let token_hash = trucoshi_auth::refresh::hash_refresh_token(token);

    let row = sqlx::query_as::<_, UserTokenRow>(
        r#"SELECT id, user_id, expires_at, consumed_at FROM user_tokens WHERE token_type = $1 AND token_hash = $2"#,
    )
    .bind(token_type)
    .bind(token_hash)
    .fetch_optional(pool)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::unauthorized("invalid or expired token"))?;

    if row.consumed_at.is_some() || row.expires_at <= OffsetDateTime::now_utc() {
        return Err(ApiError::unauthorized("invalid or expired token"));
    }

    let updated = sqlx::query(
        "UPDATE user_tokens SET consumed_at = now() WHERE id = $1 AND consumed_at IS NULL",
    )
    .bind(row.id)
    .execute(pool)
    .await
    .map_err(ApiError::internal)?;

    if updated.rows_affected() == 0 {
        return Err(ApiError::unauthorized("invalid or expired token"));
    }

    Ok(row)
}

fn public_link(base: &str, path: &str) -> String {
    let trimmed_base = base.trim_end_matches('/');
    let trimmed_path = path.trim_start_matches('/');
    format!("{trimmed_base}/{trimmed_path}")
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
        return Err(ApiError::bad_request(
            "seed_phrase must contain exactly 5 words",
        ));
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

fn viewer_user_id_from_headers(
    headers: &HeaderMap,
    tokens: &trucoshi_auth::tokens::TokenConfig,
) -> Result<Option<i64>, ApiError> {
    // Public endpoint behavior: if a bearer token is provided, use it.
    // If the token is present but invalid, return 401 (don't silently ignore auth errors).
    if let Some(Authorization(bearer)) = headers.typed_get::<Authorization<Bearer>>() {
        let data = trucoshi_auth::jwt::decode_access_jwt(tokens, bearer.token())
            .map_err(|_| ApiError::unauthorized("invalid token"))?;
        let user_id: i64 = data
            .claims
            .sub
            .parse()
            .map_err(|_| ApiError::unauthorized("invalid sub"))?;
        Ok(Some(user_id))
    } else {
        Ok(None)
    }
}

async fn list_tournaments(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ListTournamentsQuery>,
) -> Result<Json<Vec<trucoshi_server::tournaments::types::Tournament>>, ApiError> {
    let repo = trucoshi_server::tournaments::repo::TournamentsRepo::new(state.store.pool.clone());
    let limit = q.limit.clamp(1, 200);

    let viewer_user_id = viewer_user_id_from_headers(&headers, &state.tokens)?;

    let list = repo
        .list_visible_tournaments(limit, viewer_user_id)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(list))
}

async fn get_tournament(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<trucoshi_server::tournaments::types::Tournament>, ApiError> {
    use trucoshi_server::tournaments::types::TournamentStatus;

    let repo = trucoshi_server::tournaments::repo::TournamentsRepo::new(state.store.pool.clone());
    let t = repo
        .get_tournament(id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("tournament not found"))?;

    let viewer_user_id = viewer_user_id_from_headers(&headers, &state.tokens)?;
    let is_owner = viewer_user_id.is_some() && t.owner_user_id == viewer_user_id;

    // Keep visibility rules simple and consistent:
    // - OPEN/STARTED/FINISHED are public
    // - DRAFT/CANCELLED are owner-only
    let visible = match t.status {
        TournamentStatus::Draft | TournamentStatus::Cancelled => is_owner,
        TournamentStatus::Open | TournamentStatus::Started | TournamentStatus::Finished => true,
    };

    if !visible {
        return Err(ApiError::not_found("tournament not found"));
    }

    Ok(Json(t))
}

#[derive(Debug, Deserialize)]
struct ListTournamentEntriesQuery {
    #[serde(default = "default_tournament_limit")]
    limit: i64,
}

async fn list_tournament_entries(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Query(q): Query<ListTournamentEntriesQuery>,
) -> Result<Json<Vec<trucoshi_server::tournaments::types::TournamentEntry>>, ApiError> {
    use trucoshi_server::tournaments::types::TournamentStatus;

    let repo = trucoshi_server::tournaments::repo::TournamentsRepo::new(state.store.pool.clone());

    let t = repo
        .get_tournament(id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("tournament not found"))?;

    let viewer_user_id = viewer_user_id_from_headers(&headers, &state.tokens)?;
    let is_owner = viewer_user_id.is_some() && t.owner_user_id == viewer_user_id;

    let visible = match t.status {
        TournamentStatus::Draft | TournamentStatus::Cancelled => is_owner,
        TournamentStatus::Open | TournamentStatus::Started | TournamentStatus::Finished => true,
    };

    if !visible {
        return Err(ApiError::not_found("tournament not found"));
    }

    let limit = q.limit.clamp(1, 500);
    let entries = repo
        .list_entries(id, limit)
        .await
        .map_err(ApiError::internal)?;

    Ok(Json(entries))
}

async fn open_tournament(
    State(state): State<Arc<AppState>>,
    AuthedUser { user_id }: AuthedUser,
    Path(id): Path<i64>,
) -> Result<Json<trucoshi_server::tournaments::types::Tournament>, ApiError> {
    use trucoshi_server::tournaments::types::TournamentStatus;

    let repo = trucoshi_server::tournaments::repo::TournamentsRepo::new(state.store.pool.clone());
    let t = repo
        .get_tournament(id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("tournament not found"))?;

    if t.owner_user_id != Some(user_id) {
        return Err(ApiError::forbidden("only the owner can open"));
    }

    match t.status {
        TournamentStatus::Draft | TournamentStatus::Open => {}
        TournamentStatus::Started | TournamentStatus::Finished => {
            return Err(ApiError::bad_request("tournament already started"));
        }
        TournamentStatus::Cancelled => {
            return Err(ApiError::bad_request("tournament cancelled"));
        }
    }

    let updated = repo
        .update_tournament_status(id, TournamentStatus::Open)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("tournament not found"))?;

    Ok(Json(updated))
}

async fn cancel_tournament(
    State(state): State<Arc<AppState>>,
    AuthedUser { user_id }: AuthedUser,
    Path(id): Path<i64>,
) -> Result<Json<trucoshi_server::tournaments::types::Tournament>, ApiError> {
    use trucoshi_server::tournaments::types::TournamentStatus;

    let repo = trucoshi_server::tournaments::repo::TournamentsRepo::new(state.store.pool.clone());
    let t = repo
        .get_tournament(id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("tournament not found"))?;

    if t.owner_user_id != Some(user_id) {
        return Err(ApiError::forbidden("only the owner can cancel"));
    }

    match t.status {
        TournamentStatus::Draft | TournamentStatus::Open => {}
        TournamentStatus::Cancelled => return Ok(Json(t)),
        TournamentStatus::Started | TournamentStatus::Finished => {
            return Err(ApiError::bad_request("tournament already started"));
        }
    }

    let updated = repo
        .update_tournament_status(id, TournamentStatus::Cancelled)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("tournament not found"))?;

    Ok(Json(updated))
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
) -> Result<
    (
        StatusCode,
        Json<trucoshi_server::tournaments::types::Tournament>,
    ),
    ApiError,
> {
    let name = req.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("name required"));
    }
    if req.max_players < 2 {
        return Err(ApiError::bad_request("max_players must be >= 2"));
    }

    let repo = trucoshi_server::tournaments::repo::TournamentsRepo::new(state.store.pool.clone());
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
) -> Result<
    (
        StatusCode,
        Json<trucoshi_server::tournaments::types::TournamentEntry>,
    ),
    ApiError,
> {
    let display_name = req.display_name.trim();
    if display_name.is_empty() {
        return Err(ApiError::bad_request("display_name required"));
    }

    use trucoshi_server::tournaments::types::TournamentStatus;

    let repo = trucoshi_server::tournaments::repo::TournamentsRepo::new(state.store.pool.clone());

    let t = repo
        .get_tournament(id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("tournament not found"))?;

    match t.status {
        TournamentStatus::Open => {}
        TournamentStatus::Draft => return Err(ApiError::bad_request("tournament not open")),
        TournamentStatus::Started | TournamentStatus::Finished => {
            return Err(ApiError::bad_request("tournament already started"));
        }
        TournamentStatus::Cancelled => return Err(ApiError::bad_request("tournament cancelled")),
    }

    let entry = repo
        .add_entry_checked_open(id, user_id, display_name)
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("duplicate") || msg.contains("UNIQUE") {
                ApiError::bad_request("already joined")
            } else if msg.contains("TOURNAMENT_FULL") {
                ApiError::bad_request("tournament full")
            } else if msg.contains("TOURNAMENT_NOT_OPEN") {
                ApiError::bad_request("tournament not open")
            } else {
                ApiError::internal(e)
            }
        })?;

    Ok((StatusCode::CREATED, Json(entry)))
}

// ===== Match history =====

#[derive(Debug, Deserialize)]
struct MatchHistoryQuery {
    limit: Option<i64>,
    after_seq: Option<i64>,
}

#[derive(Debug, Serialize)]
struct MatchHistoryPlayerDto {
    seat_idx: i32,
    team_idx: i32,
    user_id: Option<i64>,
    display_name: String,
    created_at: OffsetDateTime,
}

#[derive(Debug, Serialize)]
struct ListActiveMatchesResponse {
    matches: Vec<ActiveMatchSummary>,
}

async fn list_active_matches(
    State(state): State<Arc<AppState>>,
    AuthedUser { user_id }: AuthedUser,
) -> Result<Json<ListActiveMatchesResponse>, ApiError> {
    let matches = state.realtime.list_active_matches_for_user(user_id).await;
    Ok(Json(ListActiveMatchesResponse { matches }))
}

#[derive(Debug, Serialize)]
struct MatchHistoryEventDto {
    id: i64,
    seq: i64,
    created_at: OffsetDateTime,
    actor_seat_idx: Option<i32>,
    actor_user_id: Option<i64>,
    #[serde(rename = "type")]
    ty: String,
    data: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct MatchHistoryResponse {
    id: i64,
    ws_match_id: Uuid,
    created_at: OffsetDateTime,
    finished_at: Option<OffsetDateTime>,
    server_version: Option<String>,
    protocol_version: Option<i32>,
    rng_seed: Option<i64>,
    options: serde_json::Value,
    players: Vec<MatchHistoryPlayerDto>,
    events: Vec<MatchHistoryEventDto>,
    next_after_seq: Option<i64>,
}

async fn get_match_history(
    State(state): State<Arc<AppState>>,
    Path(raw_match_id): Path<String>,
    Query(q): Query<MatchHistoryQuery>,
) -> Result<Json<MatchHistoryResponse>, ApiError> {
    let summary = if let Ok(match_id) = raw_match_id.parse::<i64>() {
        state.store.gh_get_match(match_id).await
    } else {
        let ws_match_id = Uuid::parse_str(&raw_match_id)
            .map_err(|_| ApiError::bad_request("invalid match id (expected integer or UUID)"))?;
        state.store.gh_get_match_by_ws_id(ws_match_id).await
    }
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("match not found"))?;

    let match_db_id = summary.id;

    let players = state
        .store
        .gh_list_players(match_db_id)
        .await
        .map_err(ApiError::internal)?;

    let limit = q.limit.unwrap_or(500).clamp(1, 1_000);
    let events = state
        .store
        .gh_list_events(match_db_id, limit, q.after_seq)
        .await
        .map_err(ApiError::internal)?;

    let reached_limit = (events.len() as i64) == limit;
    let next_after_seq = if reached_limit {
        events.last().map(|e| e.seq)
    } else {
        None
    };

    Ok(Json(MatchHistoryResponse {
        id: summary.id,
        ws_match_id: summary.ws_match_id,
        created_at: summary.created_at,
        finished_at: summary.finished_at,
        server_version: summary.server_version,
        protocol_version: summary.protocol_version,
        rng_seed: summary.rng_seed,
        options: summary.options,
        players: players
            .into_iter()
            .map(|p| MatchHistoryPlayerDto {
                seat_idx: p.seat_idx,
                team_idx: p.team_idx,
                user_id: p.user_id,
                display_name: p.display_name,
                created_at: p.created_at,
            })
            .collect(),
        events: events
            .into_iter()
            .map(|e| MatchHistoryEventDto {
                id: e.id,
                seq: e.seq,
                created_at: e.created_at,
                actor_seat_idx: e.actor_seat_idx,
                actor_user_id: e.actor_user_id,
                ty: e.r#type,
                data: e.data,
            })
            .collect(),
        next_after_seq,
    }))
}

// ===== Player stats =====

#[derive(Debug, Deserialize)]
struct PlayerProfileQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Debug, Serialize)]
struct PlayerProfileUserDto {
    id: i64,
    name: String,
    avatar_url: Option<String>,
    twitter_handle: Option<String>,
    created_at: OffsetDateTime,
}

#[derive(Debug, Serialize)]
struct PlayerTotalsDto {
    matches_played: i64,
    matches_finished: i64,
    matches_won: i64,
    win_rate: f64,
    points_for: i64,
    points_against: i64,
    points_diff: i64,
    last_played_at: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize)]
struct PlayerMatchDto {
    match_id: i64,
    ws_match_id: Option<String>,
    created_at: OffsetDateTime,
    finished_at: Option<OffsetDateTime>,
    seat_idx: i32,
    team_idx: i32,
    match_options: Option<serde_json::Value>,
    team_points: Option<Vec<i64>>,
    points_for: Option<i64>,
    points_against: Option<i64>,
    finish_reason: Option<String>,
    outcome: PlayerMatchOutcome,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum PlayerMatchOutcome {
    Win,
    Loss,
    InProgress,
}

#[derive(Debug, Serialize)]
struct PlayerProfileResponse {
    user: PlayerProfileUserDto,
    totals: PlayerTotalsDto,
    recent_matches: Vec<PlayerMatchDto>,
    next_offset: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
struct PlayerProfileUserRow {
    id: i64,
    name: String,
    avatar_url: Option<String>,
    twitter_handle: Option<String>,
    created_at: OffsetDateTime,
}

async fn get_player_profile(
    State(state): State<Arc<AppState>>,
    Path(user_id): Path<i64>,
    Query(q): Query<PlayerProfileQuery>,
) -> Result<Json<PlayerProfileResponse>, ApiError> {
    let limit = q.limit.unwrap_or(20).clamp(1, 50);
    let offset = q.offset.unwrap_or(0).max(0);

    let profile = sqlx::query_as::<_, PlayerProfileUserRow>(
        r#"
        SELECT id, name, avatar_url, twitter_handle, created_at
        FROM users
        WHERE id = $1
        "#,
    )
    .bind(user_id)
    .fetch_optional(&state.store.pool)
    .await
    .map_err(ApiError::internal)?
    .ok_or_else(|| ApiError::not_found("user not found"))?;

    let totals = state
        .store
        .gh_player_aggregates(user_id)
        .await
        .map_err(ApiError::internal)?;

    let limit_plus_one = limit + 1;
    let mut matches = state
        .store
        .gh_list_player_matches(user_id, limit_plus_one, offset)
        .await
        .map_err(ApiError::internal)?;

    let has_more = (matches.len() as i64) > limit;
    if has_more {
        let keep = usize::try_from(limit).unwrap_or(matches.len());
        if matches.len() > keep {
            matches.truncate(keep);
        }
    }

    let recent_matches = matches
        .into_iter()
        .map(player_match_to_dto)
        .collect::<Vec<_>>();

    let next_offset = if has_more { Some(offset + limit) } else { None };

    let win_rate = if totals.matches_finished > 0 {
        (totals.matches_won as f64) / (totals.matches_finished as f64)
    } else {
        0.0
    };

    let totals_dto = PlayerTotalsDto {
        matches_played: totals.matches_played,
        matches_finished: totals.matches_finished,
        matches_won: totals.matches_won,
        win_rate,
        points_for: totals.points_for,
        points_against: totals.points_against,
        points_diff: totals.points_for - totals.points_against,
        last_played_at: totals.last_played_at,
    };

    Ok(Json(PlayerProfileResponse {
        user: PlayerProfileUserDto {
            id: profile.id,
            name: profile.name,
            avatar_url: profile.avatar_url,
            twitter_handle: profile.twitter_handle,
            created_at: profile.created_at,
        },
        totals: totals_dto,
        recent_matches,
        next_offset,
    }))
}

fn player_match_to_dto(row: trucoshi_store::game_history::PlayerMatchRow) -> PlayerMatchDto {
    let match_options = row.options.get("match_options").cloned();
    let ws_match_id = Some(row.ws_match_id.to_string());

    let team_points = row.finish_data.as_ref().and_then(parse_team_points);

    let finish_reason = row
        .finish_data
        .as_ref()
        .and_then(|data| data.get("reason"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let (points_for, points_against, outcome) = if let Some(ref points) = team_points {
        if let Some((pf, pa, outcome)) = points_summary(points, row.team_idx) {
            (Some(pf), Some(pa), outcome)
        } else {
            (None, None, PlayerMatchOutcome::InProgress)
        }
    } else {
        (None, None, PlayerMatchOutcome::InProgress)
    };

    PlayerMatchDto {
        match_id: row.match_id,
        ws_match_id,
        created_at: row.created_at,
        finished_at: row.finished_at,
        seat_idx: row.seat_idx,
        team_idx: row.team_idx,
        match_options,
        team_points,
        points_for,
        points_against,
        finish_reason,
        outcome,
    }
}

#[derive(Debug, Deserialize)]
struct LeaderboardQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    min_finished: Option<i64>,
}

#[derive(Debug, Serialize)]
struct LeaderboardEntryDto {
    rank: i64,
    user_id: i64,
    name: String,
    avatar_url: Option<String>,
    twitter_handle: Option<String>,
    matches_played: i64,
    matches_finished: i64,
    matches_won: i64,
    win_rate: f64,
    points_for: i64,
    points_against: i64,
    points_diff: i64,
    last_played_at: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize)]
struct LeaderboardResponse {
    entries: Vec<LeaderboardEntryDto>,
    next_offset: Option<i64>,
}

async fn get_leaderboard(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LeaderboardQuery>,
) -> Result<Json<LeaderboardResponse>, ApiError> {
    let limit = q.limit.unwrap_or(25).clamp(1, 100);
    let offset = q.offset.unwrap_or(0).max(0);
    let min_finished = q.min_finished.unwrap_or(5).max(0);

    let limit_plus_one = limit + 1;
    let mut rows = state
        .store
        .gh_leaderboard(min_finished, limit_plus_one, offset)
        .await
        .map_err(ApiError::internal)?;

    let has_more = (rows.len() as i64) > limit;
    if has_more {
        let keep = usize::try_from(limit).unwrap_or(rows.len());
        if rows.len() > keep {
            rows.truncate(keep);
        }
    }

    let entries = rows
        .into_iter()
        .enumerate()
        .map(|(idx, row)| {
            let win_rate = if row.matches_finished > 0 {
                (row.matches_won as f64) / (row.matches_finished as f64)
            } else {
                0.0
            };

            LeaderboardEntryDto {
                rank: offset + idx as i64 + 1,
                user_id: row.user_id,
                name: row.name,
                avatar_url: row.avatar_url,
                twitter_handle: row.twitter_handle,
                matches_played: row.matches_played,
                matches_finished: row.matches_finished,
                matches_won: row.matches_won,
                win_rate,
                points_for: row.points_for,
                points_against: row.points_against,
                points_diff: row.points_for - row.points_against,
                last_played_at: row.last_played_at,
            }
        })
        .collect::<Vec<_>>();

    let next_offset = if has_more { Some(offset + limit) } else { None };

    Ok(Json(LeaderboardResponse {
        entries,
        next_offset,
    }))
}

fn parse_team_points(value: &serde_json::Value) -> Option<Vec<i64>> {
    let arr = value.get("team_points")?.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for v in arr {
        out.push(v.as_i64()?);
    }
    Some(out)
}

fn points_summary(team_points: &[i64], team_idx: i32) -> Option<(i64, i64, PlayerMatchOutcome)> {
    let idx = usize::try_from(team_idx).ok()?;
    let points_for = *team_points.get(idx)?;
    let total: i64 = team_points.iter().sum();
    let points_against = total - points_for;
    let best = team_points.iter().copied().max().unwrap_or(points_for);
    let outcome = if points_for == best {
        PlayerMatchOutcome::Win
    } else {
        PlayerMatchOutcome::Loss
    };

    Some((points_for, points_against, outcome))
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

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
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
    use super::*;

    use axum::{
        Router,
        body::Body,
        extract::State,
        http::Request,
        routing::{get, post},
    };
    use http_body_util::BodyExt;
    use serde_json::json;
    use std::sync::Arc;
    use tower::util::ServiceExt;

    #[sqlx::test(migrations = "../../migrations")]
    async fn match_history_endpoint_pages_events(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = trucoshi_store::Store { pool };

        let ws_match_id = Uuid::new_v4();
        let ws_match_id_str = ws_match_id.to_string();

        let match_id = store
            .gh_create_match(
                &ws_match_id_str,
                Some("0.2.0"),
                Some(2),
                Some(99),
                &json!({ "ws_match_id": ws_match_id }),
            )
            .await?;

        store.gh_add_player(match_id, 0, 0, None, "p1").await?;
        store.gh_add_player(match_id, 1, 1, None, "guest").await?;

        store
            .gh_append_event(
                match_id,
                0,
                Some(0),
                None,
                "match.create",
                &json!({ "ws_match_id": ws_match_id }),
            )
            .await?;
        store
            .gh_append_event(
                match_id,
                1,
                Some(1),
                None,
                "match.join",
                &json!({ "ws_match_id": ws_match_id }),
            )
            .await?;

        store.gh_finish_match(match_id).await?;

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: b"test".to_vec(),
            realtime: Realtime::new(),
        });

        let app = Router::new()
            .route("/v1/history/matches/{id}", get(get_match_history))
            .with_state(state);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/history/matches/{match_id}?limit=1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["players"].as_array().unwrap().len(), 2);
        assert_eq!(payload["events"].as_array().unwrap().len(), 1);
        assert_eq!(payload["ws_match_id"], serde_json::json!(ws_match_id));
        assert_eq!(payload["next_after_seq"], serde_json::json!(0));

        // WS id lookup should return the same payload.
        let res_ws = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/history/matches/{ws_match_id}?limit=1"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res_ws.status(), StatusCode::OK);
        let body_ws = res_ws.into_body().collect().await.unwrap().to_bytes();
        let payload_ws: serde_json::Value = serde_json::from_slice(&body_ws).unwrap();
        assert_eq!(payload_ws, payload);

        Ok(())
    }

    #[sqlx::test(migrations = "../../migrations")]
    async fn active_matches_endpoint_returns_empty_list(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = trucoshi_store::Store { pool };

        let user_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("alice@example.com")
        .bind("hash")
        .bind("Alice")
        .fetch_one(&store.pool)
        .await?;

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: b"test".to_vec(),
            realtime: Realtime::new(),
        });

        let token = trucoshi_auth::jwt::issue_access_jwt(&state.tokens, user_id).unwrap();

        let app = Router::new()
            .route("/v1/matches/active", get(list_active_matches))
            .with_state(state);

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/v1/matches/active")
                    .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["matches"], serde_json::json!([]));

        Ok(())
    }

    #[tokio::test]
    async fn axum_route_params_match_brace_syntax() {
        async fn ok() -> StatusCode {
            StatusCode::OK
        }

        let app = Router::new().route("/t/{id}", get(ok));

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/t/123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
    }
    #[sqlx::test(migrations = "../../migrations")]
    async fn player_profile_endpoint_returns_stats(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = trucoshi_store::Store { pool };

        let alice_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("alice@example.com")
        .bind("hash")
        .bind("Alice")
        .fetch_one(&store.pool)
        .await?;

        let bob_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("bob@example.com")
        .bind("hash")
        .bind("Bob")
        .fetch_one(&store.pool)
        .await?;

        // Alice wins match A
        let match_a_ws = Uuid::new_v4();
        let match_a_ws_str = match_a_ws.to_string();

        let match_a = store
            .gh_create_match(
                &match_a_ws_str,
                Some("0.2.0"),
                Some(2),
                Some(11),
                &json!({ "ws_match_id": match_a_ws, "match_options": { "max_players": 4 } }),
            )
            .await?;
        store
            .gh_add_player(match_a, 0, 0, Some(alice_id), "Alice")
            .await?;
        store
            .gh_add_player(match_a, 1, 1, Some(bob_id), "Bob")
            .await?;
        store
            .gh_append_event(
                match_a,
                0,
                None,
                None,
                "match.finish",
                &json!({ "team_points": [30, 10], "reason": "score_reached" }),
            )
            .await?;
        store.gh_finish_match(match_a).await?;

        // Bob wins match B
        let match_b_ws = Uuid::new_v4();
        let match_b_ws_str = match_b_ws.to_string();

        let match_b = store
            .gh_create_match(
                &match_b_ws_str,
                Some("0.2.0"),
                Some(2),
                Some(22),
                &json!({ "ws_match_id": match_b_ws, "match_options": { "max_players": 4 } }),
            )
            .await?;
        store
            .gh_add_player(match_b, 0, 0, Some(alice_id), "Alice")
            .await?;
        store
            .gh_add_player(match_b, 1, 1, Some(bob_id), "Bob")
            .await?;
        store
            .gh_append_event(
                match_b,
                0,
                None,
                None,
                "match.finish",
                &json!({ "team_points": [12, 30], "reason": "score_reached" }),
            )
            .await?;
        store.gh_finish_match(match_b).await?;

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: b"test".to_vec(),
            realtime: Realtime::new(),
        });

        let app = Router::new()
            .route("/v1/stats/players/{id}", get(get_player_profile))
            .with_state(state);

        let res = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/stats/players/{}?limit=1", alice_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["user"]["name"], serde_json::json!("Alice"));
        assert_eq!(payload["totals"]["matches_played"], serde_json::json!(2));
        assert_eq!(payload["totals"]["matches_won"], serde_json::json!(1));
        assert_eq!(payload["recent_matches"].as_array().unwrap().len(), 1);
        assert_eq!(payload["next_offset"], serde_json::json!(1));
        assert_eq!(
            payload["recent_matches"][0]["ws_match_id"],
            serde_json::json!(match_b_ws)
        );
        assert_eq!(
            payload["recent_matches"][0]["outcome"],
            serde_json::json!("loss")
        );

        Ok(())
    }

    #[sqlx::test(migrations = "../../migrations")]
    async fn leaderboard_endpoint_orders_by_wins(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = trucoshi_store::Store { pool };

        let alice_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("alice@example.com")
        .bind("hash")
        .bind("Alice")
        .fetch_one(&store.pool)
        .await?;

        let bob_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("bob@example.com")
        .bind("hash")
        .bind("Bob")
        .fetch_one(&store.pool)
        .await?;

        let carol_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("carol@example.com")
        .bind("hash")
        .bind("Carol")
        .fetch_one(&store.pool)
        .await?;

        // Match 1: Alice beats Bob
        let match1_ws = Uuid::new_v4();
        let match1_ws_str = match1_ws.to_string();

        let match1 = store
            .gh_create_match(
                &match1_ws_str,
                Some("0.2.0"),
                Some(2),
                Some(33),
                &json!({ "ws_match_id": match1_ws }),
            )
            .await?;
        store
            .gh_add_player(match1, 0, 0, Some(alice_id), "Alice")
            .await?;
        store
            .gh_add_player(match1, 1, 1, Some(bob_id), "Bob")
            .await?;
        store
            .gh_append_event(
                match1,
                0,
                None,
                None,
                "match.finish",
                &json!({ "team_points": [30, 20], "reason": "score" }),
            )
            .await?;
        store.gh_finish_match(match1).await?;

        // Match 2: Bob beats Alice
        let match2_ws = Uuid::new_v4();
        let match2_ws_str = match2_ws.to_string();

        let match2 = store
            .gh_create_match(
                &match2_ws_str,
                Some("0.2.0"),
                Some(2),
                Some(44),
                &json!({ "ws_match_id": match2_ws }),
            )
            .await?;
        store
            .gh_add_player(match2, 0, 0, Some(alice_id), "Alice")
            .await?;
        store
            .gh_add_player(match2, 1, 1, Some(bob_id), "Bob")
            .await?;
        store
            .gh_append_event(
                match2,
                0,
                None,
                None,
                "match.finish",
                &json!({ "team_points": [10, 30], "reason": "score" }),
            )
            .await?;
        store.gh_finish_match(match2).await?;

        // Match 3: Bob beats Carol
        let match3_ws = Uuid::new_v4();
        let match3_ws_str = match3_ws.to_string();

        let match3 = store
            .gh_create_match(
                &match3_ws_str,
                Some("0.2.0"),
                Some(2),
                Some(55),
                &json!({ "ws_match_id": match3_ws }),
            )
            .await?;
        store
            .gh_add_player(match3, 0, 0, Some(bob_id), "Bob")
            .await?;
        store
            .gh_add_player(match3, 1, 1, Some(carol_id), "Carol")
            .await?;
        store
            .gh_append_event(
                match3,
                0,
                None,
                None,
                "match.finish",
                &json!({ "team_points": [30, 12], "reason": "score" }),
            )
            .await?;
        store.gh_finish_match(match3).await?;

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: b"test".to_vec(),
            realtime: Realtime::new(),
        });

        let app = Router::new()
            .route("/v1/stats/leaderboard", get(get_leaderboard))
            .with_state(state);

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/v1/stats/leaderboard?limit=2&min_finished=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let entries = payload["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["user_id"], serde_json::json!(bob_id));
        assert_eq!(entries[0]["rank"], serde_json::json!(1));
        assert_eq!(entries[1]["user_id"], serde_json::json!(alice_id));
        assert_eq!(payload["next_offset"], serde_json::json!(2));

        Ok(())
    }

    #[sqlx::test(migrations = "../../migrations")]
    async fn send_verification_email_creates_token(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = trucoshi_store::Store { pool };

        let user_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("verify@example.com")
        .bind("hash")
        .bind("Verify Me")
        .fetch_one(&store.pool)
        .await?;

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: b"test".to_vec(),
            realtime: Realtime::new(),
        });

        let res = send_verification_email(State(state.clone()), AuthedUser { user_id })
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let token_type: String =
            sqlx::query_scalar("SELECT token_type FROM user_tokens WHERE user_id = $1")
                .bind(user_id)
                .fetch_one(&state.store.pool)
                .await?;

        assert_eq!(token_type, TOKEN_TYPE_VERIFY_EMAIL);

        Ok(())
    }

    #[sqlx::test(migrations = "../../migrations")]
    async fn verify_email_marks_user_verified(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = trucoshi_store::Store { pool };

        let user_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("verify2@example.com")
        .bind("hash")
        .bind("Verify Later")
        .fetch_one(&store.pool)
        .await?;

        let token = trucoshi_auth::refresh::new_refresh_token();
        let token_hash = trucoshi_auth::refresh::hash_refresh_token(&token);
        let expires_at = OffsetDateTime::now_utc() + Duration::minutes(30);

        sqlx::query(
            "INSERT INTO user_tokens (user_id, token_type, token_hash, expires_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(user_id)
        .bind(TOKEN_TYPE_VERIFY_EMAIL)
        .bind(token_hash)
        .bind(expires_at)
        .execute(&store.pool)
        .await?;

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: b"test".to_vec(),
            realtime: Realtime::new(),
        });

        let res = verify_email(State(state.clone()), Json(VerifyEmailRequest { token }))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let verified: bool =
            sqlx::query_scalar("SELECT is_email_verified FROM users WHERE id = $1")
                .bind(user_id)
                .fetch_one(&state.store.pool)
                .await?;
        assert!(verified);

        Ok(())
    }

    #[sqlx::test(migrations = "../../migrations")]
    async fn reset_password_updates_hash_and_revokes_tokens(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        let store = trucoshi_store::Store { pool };

        let user_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("reset@example.com")
        .bind("old-hash")
        .bind("Reset Me")
        .fetch_one(&store.pool)
        .await?;

        sqlx::query(
            "INSERT INTO refresh_tokens (user_id, expires_at, token_hash) VALUES ($1, now() + interval '1 day', $2)",
        )
        .bind(user_id)
        .bind("refresh-hash")
        .execute(&store.pool)
        .await?;

        let token = trucoshi_auth::refresh::new_refresh_token();
        let token_hash = trucoshi_auth::refresh::hash_refresh_token(&token);
        let expires_at = OffsetDateTime::now_utc() + Duration::minutes(30);

        sqlx::query(
            "INSERT INTO user_tokens (user_id, token_type, token_hash, expires_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(user_id)
        .bind(TOKEN_TYPE_RESET_PASSWORD)
        .bind(token_hash)
        .bind(expires_at)
        .execute(&store.pool)
        .await?;

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: b"test".to_vec(),
            realtime: Realtime::new(),
        });

        let res = reset_password(
            State(state.clone()),
            Json(ResetPasswordRequest {
                token,
                password: "newpassword123".into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);

        let stored_hash: Option<String> =
            sqlx::query_scalar("SELECT password_hash FROM users WHERE id = $1")
                .bind(user_id)
                .fetch_one(&store.pool)
                .await?;
        assert_ne!(stored_hash.as_deref(), Some("old-hash"));

        let refresh_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM refresh_tokens WHERE user_id = $1")
                .bind(user_id)
                .fetch_one(&store.pool)
                .await?;
        assert_eq!(refresh_count, 0);

        Ok(())
    }

    #[sqlx::test(migrations = "../../migrations")]
    async fn register_seed_creates_seed_only_user(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = trucoshi_store::Store { pool };

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: b"seed-secret".to_vec(),
            realtime: Realtime::new(),
        });

        let app = Router::new()
            .route("/v1/auth/register-seed", post(register_seed))
            .with_state(state.clone());

        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/auth/register-seed")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{ "name": "Seed User" }"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let cookie = res
            .headers()
            .get(axum::http::header::SET_COOKIE)
            .expect("refresh cookie")
            .to_str()
            .unwrap();
        assert!(cookie.contains("refresh"));

        let body = res.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let seed_phrase = payload["seed_phrase"].as_str().unwrap();
        assert_eq!(seed_phrase.split_whitespace().count(), 5);

        let user = &payload["user"];
        assert_eq!(user["name"], serde_json::json!("Seed User"));
        assert!(user["has_seed"].as_bool().unwrap());
        let user_id = user["id"].as_i64().unwrap();

        let (email, password_hash, seed_hash, has_seed): (
            Option<String>,
            Option<String>,
            Option<String>,
            bool,
        ) = sqlx::query_as(
            "SELECT email, password_hash, seed_hash, has_seed FROM users WHERE id = $1",
        )
        .bind(user_id)
        .fetch_one(&store.pool)
        .await?;

        assert!(email.is_none());
        assert!(password_hash.is_none());
        assert!(seed_hash.is_some());
        assert!(has_seed);

        Ok(())
    }

    #[sqlx::test(migrations = "../../migrations")]
    async fn login_seed_normalizes_phrase(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let store = trucoshi_store::Store { pool };
        let seed_secret = b"seed-secret".to_vec();

        let seed_phrase = "tango mate truco flor falta";
        let seed_hash = trucoshi_auth::seed::hash_seed_phrase(seed_phrase, &seed_secret);
        let user_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (name, seed_hash, has_seed) VALUES ($1, $2, TRUE) RETURNING id",
        )
        .bind("Seed Login")
        .bind(seed_hash)
        .fetch_one(&store.pool)
        .await?;

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: seed_secret.clone(),
            realtime: Realtime::new(),
        });

        let app = Router::new()
            .route("/v1/auth/login-seed", post(login_seed))
            .with_state(state.clone());

        let res = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/auth/login-seed")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{ "seed_phrase": "  TANGO   MATE   TRUCO  FLOR   FALTA  " }"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["user"]["id"].as_i64().unwrap(), user_id);
        assert!(payload["user"]["has_seed"].as_bool().unwrap());

        Ok(())
    }

    #[sqlx::test(migrations = "../../migrations")]
    async fn list_tournaments_respects_visibility_and_limit(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        use trucoshi_server::tournaments::{repo::TournamentsRepo, types::TournamentStatus};

        let store = trucoshi_store::Store { pool };
        let repo = TournamentsRepo::new(store.pool.clone());

        let owner_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("owner@example.com")
        .bind("hash")
        .bind("Owner")
        .fetch_one(&store.pool)
        .await?;

        let other_owner_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("other@example.com")
        .bind("hash")
        .bind("Other Owner")
        .fetch_one(&store.pool)
        .await?;

        let open_old = repo
            .create_tournament(Some(other_owner_id), "Public Open", 16, None)
            .await?;
        repo.update_tournament_status(open_old.id, TournamentStatus::Open)
            .await?;

        let open_recent = repo
            .create_tournament(Some(owner_id), "Weekend Open", 8, None)
            .await?;
        repo.update_tournament_status(open_recent.id, TournamentStatus::Open)
            .await?;

        let owner_draft = repo
            .create_tournament(Some(owner_id), "Owner Draft", 12, None)
            .await?;

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: b"test".to_vec(),
            realtime: Realtime::new(),
        });

        let owner_token = trucoshi_auth::jwt::issue_access_jwt(&state.tokens, owner_id).unwrap();

        let app = Router::new()
            .route("/v1/tournaments", get(list_tournaments))
            .with_state(state);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/tournaments?limit=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let list = payload.as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["name"], serde_json::json!("Weekend Open"));

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/v1/tournaments?limit=10")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {owner_token}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let list = payload.as_array().unwrap();
        assert_eq!(list.len(), 3);
        let names: Vec<String> = list
            .iter()
            .map(|v| v.get("name").unwrap().as_str().unwrap().to_string())
            .collect();
        assert!(names.contains(&"Owner Draft".to_owned()));
        assert!(names.contains(&"Weekend Open".to_owned()));
        assert!(names.contains(&"Public Open".to_owned()));

        Ok(())
    }

    #[sqlx::test(migrations = "../../migrations")]
    async fn tournament_details_and_entries_enforce_visibility(
        pool: sqlx::PgPool,
    ) -> anyhow::Result<()> {
        use trucoshi_server::tournaments::{repo::TournamentsRepo, types::TournamentStatus};

        let store = trucoshi_store::Store { pool };
        let repo = TournamentsRepo::new(store.pool.clone());

        let owner_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("owner@example.com")
        .bind("hash")
        .bind("Owner")
        .fetch_one(&store.pool)
        .await?;

        let viewer_id: i64 = sqlx::query_scalar(
            "INSERT INTO users (email, password_hash, name) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind("viewer@example.com")
        .bind("hash")
        .bind("Viewer")
        .fetch_one(&store.pool)
        .await?;

        let tournament = repo
            .create_tournament(Some(owner_id), "Draft Cup", 8, None)
            .await?;

        sqlx::query(
            "INSERT INTO tournament_entries (tournament_id, user_id, display_name) VALUES ($1, $2, $3)",
        )
        .bind(tournament.id)
        .bind(Some(owner_id))
        .bind("Alpha")
        .execute(&store.pool)
        .await?;

        sqlx::query(
            "INSERT INTO tournament_entries (tournament_id, user_id, display_name) VALUES ($1, $2, $3)",
        )
        .bind(tournament.id)
        .bind(Some(viewer_id))
        .bind("Bravo")
        .execute(&store.pool)
        .await?;

        let state = Arc::new(AppState {
            store: store.clone(),
            tokens: trucoshi_auth::tokens::TokenConfig {
                issuer: "test".into(),
                audience: "test".into(),
                access_ttl: Duration::minutes(5),
                refresh_ttl: Duration::minutes(5),
                jwt_hs256_secret: vec![0; 32],
            },
            cookie: CookieConfig {
                refresh_cookie_name: "refresh".into(),
                refresh_cookie_path: "/v1/auth/refresh-tokens".into(),
                secure: false,
                same_site: SameSite::Lax,
            },
            twitter: TwitterConfig {
                public_base_url: "http://localhost".into(),
                client_id: "id".into(),
                client_secret: "secret".into(),
            },
            public_base_url: "http://localhost".into(),
            mailer: Mailer::disabled(),
            seed_hash_secret: b"test".to_vec(),
            realtime: Realtime::new(),
        });

        let owner_token = trucoshi_auth::jwt::issue_access_jwt(&state.tokens, owner_id).unwrap();

        let app = Router::new()
            .route("/v1/tournaments/{id}", get(get_tournament))
            .route("/v1/tournaments/{id}/entries", get(list_tournament_entries))
            .with_state(state.clone());

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/tournaments/{}", tournament.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/tournaments/{}", tournament.id))
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {owner_token}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let draft_payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(draft_payload["name"], serde_json::json!("Draft Cup"));
        assert_eq!(draft_payload["status"], serde_json::json!("DRAFT"));

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/tournaments/{}/entries?limit=0", tournament.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/tournaments/{}/entries?limit=0", tournament.id))
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {owner_token}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let entries_payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let entries = entries_payload.as_array().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["display_name"], serde_json::json!("Alpha"));

        repo.update_tournament_status(tournament.id, TournamentStatus::Open)
            .await?;

        let res = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/tournaments/{}/entries?limit=999",
                        tournament.id
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let entries_payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let entries = entries_payload.as_array().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["display_name"], serde_json::json!("Alpha"));
        assert_eq!(entries[1]["display_name"], serde_json::json!("Bravo"));

        Ok(())
    }
}
