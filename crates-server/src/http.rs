use axum::{
    Json, Router,
    extract::{FromRequestParts, State, ws::WebSocketUpgrade},
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

#[derive(Clone)]
struct AppState {
    store: trucoshi_store::Store,
    tokens: trucoshi_auth::tokens::TokenConfig,
    cookie: CookieConfig,
    twitter: TwitterConfig,
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
        realtime: trucoshi_realtime::server::Realtime::new(),
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/v2/ws", get(ws_handler))
        .route("/v1/auth/register", post(register))
        .route("/v1/auth/login", post(login))
        .route("/v1/auth/me", get(me))
        .route("/v1/auth/refresh-tokens", post(refresh_tokens))
        .route("/v1/auth/logout", post(logout))
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

#[derive(Debug, Serialize)]
struct UserDto {
    id: i64,
    email: Option<String>,
    name: String,
    avatar_url: Option<String>,
    twitter_handle: Option<String>,
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
        RETURNING id, email, password_hash, name, avatar_url, twitter_handle
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
        r#"SELECT id, email, password_hash, name, avatar_url, twitter_handle FROM users WHERE email = $1"#,
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

async fn me(
    State(state): State<Arc<AppState>>,
    AuthedUser { user_id }: AuthedUser,
) -> Result<Json<UserDto>, ApiError> {
    let rec: UserRow = sqlx::query_as(
        r#"SELECT id, email, password_hash, name, avatar_url, twitter_handle FROM users WHERE id = $1"#,
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
        r#"SELECT id, email, password_hash, name, avatar_url, twitter_handle FROM users WHERE id = $1"#,
    )
    .bind(user_id)
    .fetch_one(&state.store.pool)
    .await
    .map_err(ApiError::internal)?;

    let body = AuthResponse {
        access_token: access,
        user: UserDto {
            id: rec.id,
            email: rec.email,
            name: rec.name,
            avatar_url: rec.avatar_url,
            twitter_handle: rec.twitter_handle,
        },
    };

    let mut res = (StatusCode::OK, Json(body)).into_response();
    res.headers_mut().append(
        axum::http::header::SET_COOKIE,
        build_refresh_cookie(state, &refresh)
            .parse()
            .map_err(ApiError::internal)?,
    );

    Ok(res)
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
