use crate::history::{GameHistoryEvent, HistoryPlayer};
use crate::protocol::ws::v2::messages::{ActiveMatchesSnapshotData, MatchKickedData};
use crate::protocol::ws::v2::{
    C2sMessage, ChatMessageData, ChatSnapshotData, ErrorPayload, GameSnapshotData, GameUpdateData,
    HelloData, LobbyMatchRemoveData, LobbyMatchUpsertData, LobbySnapshotData, MatchLeftData,
    MatchSnapshotData, MatchUpdateData, PongData, S2cMessage, WsInMessage, WsOutMessage,
    schema::{
        ActiveMatchPlayer, ActiveMatchSummary, HandState, LobbyMatch, MatchOptions, MatchPhase,
        Maybe, PrivatePlayer, PublicChatMessage, PublicChatRoom, PublicChatUser, PublicMatch,
        PublicPauseRequest, PublicPendingUnpause, PublicPlayer, TeamIdx,
    },
};
use anyhow::Context;
use axum::extract::ws::{Message, WebSocket};
use futures_util::{sink::SinkExt, stream::StreamExt};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, sleep};
use tracing::{debug, warn};
use trucoshi_game::{CommandOutcome, PlayOutcome, PointsAwardReason};
use uuid::Uuid;

#[cfg(test)]
const PAUSE_REQUEST_TIMEOUT_MS: i64 = 1_000;
#[cfg(not(test))]
const PAUSE_REQUEST_TIMEOUT_MS: i64 = 30_000;

#[cfg(test)]
const UNPAUSE_COUNTDOWN_MS: i64 = 250;
#[cfg(not(test))]
const UNPAUSE_COUNTDOWN_MS: i64 = 10_000;

#[derive(Default)]
pub struct RealtimeState {
    pub connections: usize,
    pub sessions: HashMap<Uuid, SessionState>,
    pub matches: HashMap<String, MatchState>,
    pub rooms: HashMap<String, ChatRoomState>,
}

#[derive(Debug)]
pub struct SessionState {
    pub user_id: i64,
    pub tx: mpsc::UnboundedSender<WsOutMessage>,

    pub active_match_id: Option<String>,
    pub player_key: Option<String>,

    pub rooms: HashSet<String>,
    pub last_active_ms: i64,
}

#[derive(Debug, Clone)]
pub struct ChatRoomState {
    pub id: String,
    pub messages: Vec<PublicChatMessage>,
    pub participants: HashSet<Uuid>,
}

#[derive(Debug, Clone)]
struct ChatAppendResult {
    message: PublicChatMessage,
    history: Option<ChatHistoryContext>,
}

#[derive(Debug, Clone)]
struct ChatHistoryContext {
    match_id: String,
    actor_seat_idx: Option<u8>,
    actor_team_idx: Option<u8>,
    actor_user_id: i64,
}

#[derive(Debug, Clone)]
pub struct PlayerState {
    pub key: String,
    pub user_id: i64,
    pub name: String,
    pub team: TeamIdx,
    pub ready: bool,
    pub last_active_ms: i64,
    pub disconnected_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct PauseRequestState {
    pub token: Uuid,
    pub requested_by_seat_idx: u8,
    pub requested_by_user_id: i64,
    pub requested_by_team: TeamIdx,
    pub awaiting_team: TeamIdx,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct AutoUnpauseState {
    pub token: Uuid,
    pub trigger_at_ms: i64,
    pub paused_by_team: TeamIdx,
}

#[derive(Debug, Clone)]
pub enum PendingUnpauseTrigger {
    Manual {
        actor_user_id: i64,
        actor_seat_idx: u8,
        actor_team_idx: u8,
    },
    AutoTimeout,
}

#[derive(Debug, Clone)]
pub struct PendingUnpauseState {
    pub token: Uuid,
    pub resume_at_ms: i64,
    pub trigger: PendingUnpauseTrigger,
}

#[derive(Debug, Clone)]
pub struct MatchState {
    pub match_id: String,
    pub owner_key: String,
    pub options: MatchOptions,
    pub phase: MatchPhase,
    pub players: Vec<PlayerState>,
    pub participants: HashSet<Uuid>,

    /// Team points (match score) for teams 0 and 1.
    pub team_points: [u8; 2],

    /// Current hand number within the match (0-based).
    ///
    /// Used for deterministic dealing across reconnects + to derive per-hand RNG seeds.
    pub hand_no: u32,

    /// Current hand/game state.
    pub game: Option<trucoshi_game::GameState>,

    /// When a hand ends but the match is still ongoing, we stage the next hand here.
    ///
    /// This allows us to broadcast the *finished* hand state first (so clients can animate/show
    /// results) and then start the next hand with a second update.
    pub pending_game: Option<trucoshi_game::GameState>,

    pub pause_request: Option<PauseRequestState>,
    pub auto_unpause: Option<AutoUnpauseState>,
    pub pending_unpause: Option<PendingUnpauseState>,
}

impl MatchState {
    fn reset_pause_state(&mut self) {
        self.pause_request = None;
        self.auto_unpause = None;
        self.pending_unpause = None;
    }
}

fn opposing_team(team: TeamIdx) -> TeamIdx {
    if team == TeamIdx::TEAM_0 {
        TeamIdx::TEAM_1
    } else {
        TeamIdx::TEAM_0
    }
}

fn ms_to_duration(ms: i64) -> Duration {
    if ms <= 0 {
        Duration::from_millis(0)
    } else {
        Duration::from_millis(ms as u64)
    }
}

fn team_has_connected_player(m: &MatchState, team: TeamIdx) -> bool {
    m.players
        .iter()
        .filter(|p| p.team == team)
        .any(|p| p.disconnected_at_ms.is_none())
}

enum JoinResult {
    NewSeat,
    Reconnected,
}

#[derive(Clone)]
pub struct Realtime {
    state: Arc<Mutex<RealtimeState>>,
    history_tx: Option<mpsc::UnboundedSender<GameHistoryEvent>>,
}

impl Realtime {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(RealtimeState::default())),
            history_tx: None,
        }
    }

    pub fn new_with_history(history_tx: mpsc::UnboundedSender<GameHistoryEvent>) -> Self {
        Self {
            state: Arc::new(Mutex::new(RealtimeState::default())),
            history_tx: Some(history_tx),
        }
    }

    /// Return a stable snapshot of unfinished matches the given user is seated in.
    pub async fn list_active_matches_for_user(&self, user_id: i64) -> Vec<ActiveMatchSummary> {
        let mut entries = {
            let s = self.state.lock().await;
            let mut matches = Vec::new();

            for m in s.matches.values() {
                if m.phase == MatchPhase::Finished {
                    continue;
                }

                if let Some(idx) = m.players.iter().position(|p| p.user_id == user_id) {
                    let public = Self::public_match_for(m);
                    let player = m.players[idx].clone();

                    matches.push(ActiveMatchSummary {
                        match_: public,
                        me: ActiveMatchPlayer {
                            seat_idx: u8::try_from(idx).unwrap_or(0),
                            team: player.team,
                            ready: player.ready,
                            is_owner: player.key == m.owner_key,
                            last_active_ms: player.last_active_ms,
                            disconnected_at_ms: Maybe(player.disconnected_at_ms),
                        },
                    });
                }
            }

            matches
        };

        entries.sort_by(|a, b| {
            b.me.last_active_ms
                .cmp(&a.me.last_active_ms)
                .then_with(|| a.match_.id.cmp(&b.match_.id))
        });
        entries
    }

    fn emit_history(&self, ev: GameHistoryEvent) {
        if let Some(tx) = self.history_tx.as_ref() {
            let _ = tx.send(ev);
        }
    }

    pub async fn handle_socket(&self, socket: WebSocket, user_id: i64) {
        let session_id = Uuid::new_v4();
        let (mut socket_tx, mut socket_rx) = socket.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<WsOutMessage>();

        {
            let mut s = self.state.lock().await;
            s.connections += 1;
            s.sessions.insert(
                session_id,
                SessionState {
                    user_id,
                    tx: tx.clone(),
                    active_match_id: None,
                    player_key: None,
                    rooms: HashSet::new(),
                    last_active_ms: Self::now_ms(),
                },
            );
            debug!(connections = s.connections, %session_id, "ws connected");
        }

        // writer task
        let writer = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let txt = match serde_json::to_string(&msg).context("serialize ws message") {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "ws serialize failed");
                        continue;
                    }
                };

                if let Err(e) = socket_tx
                    .send(Message::Text(txt.into()))
                    .await
                    .context("ws send")
                {
                    warn!(error = %e, "ws send failed");
                    break;
                }
            }
        });

        // Hello + initial lobby snapshot.
        let _ = tx.send(WsOutMessage {
            v: Default::default(),
            msg: S2cMessage::Hello(HelloData {
                session_id: session_id.to_string(),
                server_version: env!("CARGO_PKG_VERSION").to_string(),
            }),
            id: Default::default(),
        });
        self.emit_lobby_snapshot_to(session_id, None).await;

        while let Some(Ok(msg)) = socket_rx.next().await {
            match msg {
                Message::Text(txt) => match serde_json::from_str::<WsInMessage>(&txt) {
                    Ok(in_msg) => {
                        // Version is validated during deserialization (WsVersion is strict).
                        self.handle_message(session_id, in_msg).await;
                    }
                    Err(e) => {
                        warn!(error = %e, "ws parse error");
                        let _ = tx.send(Self::err_out(None, "BAD_MESSAGE", "invalid json message"));
                    }
                },
                Message::Close(_) => break,
                _ => {}
            }
        }

        // cleanup
        let mut maybe_leave: Option<(String, Option<String>, i64)> = None;
        let mut rooms_to_leave: Vec<String> = Vec::new();
        {
            let mut s = self.state.lock().await;
            if let Some(sess) = s.sessions.remove(&session_id) {
                if let Some(msid) = sess.active_match_id {
                    maybe_leave = Some((msid, sess.player_key.clone(), sess.user_id));
                }
                rooms_to_leave.extend(sess.rooms.into_iter());
            }
            s.connections = s.connections.saturating_sub(1);
            debug!(connections = s.connections, %session_id, "ws disconnected");
        }

        for room_id in rooms_to_leave {
            self.leave_room_internal(session_id, &room_id).await;
        }

        if let Some((msid, pkey, user_id)) = maybe_leave {
            if let Some(pkey) = pkey.as_deref() {
                self.handle_player_disconnect(session_id, &msid, pkey).await;
            } else {
                // spectator: just detach from match participants / rooms
                self.leave_match_watch_internal(session_id, &msid, user_id, "disconnect")
                    .await;

                // Notify remaining participants and refresh lobby counts.
                self.emit_match_update_to_match(&msid, None).await;
                self.broadcast_lobby_match_upsert(&msid).await;
            }
        }

        writer.abort();
    }

    fn now_ms() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    fn err_out<M: Into<String>, C: Into<String>>(
        id: Option<String>,
        code: C,
        message: M,
    ) -> WsOutMessage {
        WsOutMessage {
            v: Default::default(),
            msg: S2cMessage::Error(ErrorPayload {
                code: code.into(),
                message: message.into(),
            }),
            id: id.into(),
        }
    }

    async fn handle_message(&self, session_id: Uuid, in_msg: WsInMessage) {
        let id: Option<String> = in_msg.id.into();

        self.mark_session_active(session_id).await;
        self.sweep_inactive_players().await;

        match in_msg.msg {
            C2sMessage::Ping(p) => {
                self.send_to(
                    session_id,
                    WsOutMessage {
                        v: Default::default(),
                        msg: S2cMessage::Pong(PongData {
                            server_time_ms: Self::now_ms(),
                            client_time_ms: p.client_time_ms,
                        }),
                        id: id.clone().into(),
                    },
                )
                .await;
            }

            C2sMessage::LobbySnapshotGet => {
                // Echo correlation id on the snapshot (v2 does not require a separate ack).
                self.emit_lobby_snapshot_to(session_id, id).await;
            }

            C2sMessage::MeActiveMatchesGet => {
                let user_id = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    sess.user_id
                };

                let matches = self.list_active_matches_for_user(user_id).await;
                self.send_to(
                    session_id,
                    WsOutMessage {
                        v: Default::default(),
                        msg: S2cMessage::MeActiveMatches(ActiveMatchesSnapshotData { matches }),
                        id: id.clone().into(),
                    },
                )
                .await;
            }

            C2sMessage::MatchSnapshotGet(d) => {
                self.emit_match_snapshot_to(session_id, &d.match_id, id)
                    .await;
            }

            C2sMessage::GameSnapshotGet(d) => {
                // If the match exists and has gameplay, this will emit a snapshot.
                self.emit_game_snapshot_to(session_id, &d.match_id, id)
                    .await;
            }

            C2sMessage::MatchCreate(d) => {
                // v2: do not allow creating a match while already in a match.
                let already_in_match = {
                    let s = self.state.lock().await;
                    s.sessions
                        .get(&session_id)
                        .map(|sess| sess.active_match_id.is_some())
                        .unwrap_or(false)
                };

                if already_in_match {
                    self.send_to(
                        session_id,
                        Self::err_out(id, "ALREADY_IN_MATCH", "already in a match"),
                    )
                    .await;
                    return;
                }

                let options = Option::<MatchOptions>::from(d.options).unwrap_or_default();
                let requested_team: Option<crate::protocol::ws::v2::schema::TeamIdx> =
                    d.team.into();
                let match_id = Uuid::new_v4().to_string();
                let owner_key = Uuid::new_v4().to_string();

                let team = requested_team.unwrap_or(TeamIdx::TEAM_0);

                let name = d.name.trim().to_string();
                if name.is_empty() {
                    self.send_to(
                        session_id,
                        Self::err_out(id, "BAD_REQUEST", "name required"),
                    )
                    .await;
                    return;
                }

                let owner_user_id = {
                    let s = self.state.lock().await;
                    s.sessions
                        .get(&session_id)
                        .map(|sess| sess.user_id)
                        .unwrap_or(0)
                };
                let now_ms = Self::now_ms();

                let player = PlayerState {
                    key: owner_key.clone(),
                    user_id: owner_user_id,
                    name,
                    team,
                    ready: false,
                    last_active_ms: now_ms,
                    disconnected_at_ms: None,
                };

                let m = MatchState {
                    match_id: match_id.clone(),
                    owner_key: owner_key.clone(),
                    options,
                    phase: MatchPhase::Lobby,
                    players: vec![player.clone()],
                    participants: HashSet::from([session_id]),
                    team_points: [0, 0],
                    hand_no: 0,
                    game: None,
                    pending_game: None,
                    pause_request: None,
                    auto_unpause: None,
                    pending_unpause: None,
                };

                {
                    let mut s = self.state.lock().await;
                    if let Some(sess) = s.sessions.get_mut(&session_id) {
                        sess.active_match_id = Some(match_id.clone());
                        sess.player_key = Some(owner_key.clone());
                    }
                    s.matches.insert(match_id.clone(), m);
                }

                // Best-effort: emit persistence events (does not affect gameplay if dropped).
                {
                    let match_options = serde_json::to_value(&options).unwrap_or_else(
                        |_| serde_json::json!({ "error": "failed_to_serialize_match_options" }),
                    );

                    self.emit_history(GameHistoryEvent::MatchCreated {
                        match_id: match_id.clone(),
                        server_version: env!("CARGO_PKG_VERSION").to_string(),
                        protocol_version: 2,
                        rng_seed: Self::seed_for_hand(&match_id, 0) as i64,
                        options: serde_json::json!({
                            "ws_match_id": match_id.clone(),
                            "match_options": match_options,
                        }),
                        owner: HistoryPlayer {
                            seat_idx: 0,
                            team_idx: team.as_u8(),
                            user_id: owner_user_id,
                            display_name: player.name.clone(),
                        },
                    });
                }

                // auto-join the match chat room (room id == match_id)
                self.join_room_internal(session_id, &match_id).await;

                // Send the creator an immediate match snapshot (echoing correlation id), then
                // broadcast the lobby update.
                self.emit_match_snapshot_to(session_id, &match_id, id).await;
                self.broadcast_lobby_match_upsert(&match_id).await;
            }

            C2sMessage::MatchJoin(d) => {
                let match_id = d.match_id;

                let (already_in_match, user_id) = {
                    let s = self.state.lock().await;
                    let maybe = s.sessions.get(&session_id);
                    (
                        maybe
                            .map(|sess| sess.active_match_id.is_some())
                            .unwrap_or(false),
                        maybe.map(|sess| sess.user_id).unwrap_or(0),
                    )
                };

                if already_in_match {
                    self.send_to(
                        session_id,
                        Self::err_out(id, "ALREADY_IN_MATCH", "already in a match"),
                    )
                    .await;
                    return;
                }

                let name = d.name.trim().to_string();
                if name.is_empty() {
                    self.send_to(
                        session_id,
                        Self::err_out(id, "BAD_REQUEST", "name required"),
                    )
                    .await;
                    return;
                }

                let team: Option<TeamIdx> = d.team.into();
                let res = self
                    .join_match_internal(session_id, &match_id, name, team, user_id)
                    .await;

                match res {
                    Ok(join_result) => {
                        self.join_room_internal(session_id, &match_id).await;

                        if matches!(join_result, JoinResult::NewSeat) {
                            // Best-effort: emit persistence events.
                            let maybe = {
                                let s = self.state.lock().await;
                                if let Some(sess) = s.sessions.get(&session_id) {
                                    if let Some(pkey) = sess.player_key.as_deref() {
                                        if let Some(m) = s.matches.get(&match_id) {
                                            if let Some(seat_idx) =
                                                m.players.iter().position(|p| p.key == pkey)
                                            {
                                                if let Some(p) = m.players.get(seat_idx) {
                                                    Some((
                                                        sess.user_id,
                                                        seat_idx,
                                                        p.team.as_u8(),
                                                        p.name.clone(),
                                                    ))
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            };

                            if let Some((user_id, seat_idx, team_idx, display_name)) = maybe {
                                self.emit_history(GameHistoryEvent::PlayerJoined {
                                    match_id: match_id.clone(),
                                    seat_idx: u8::try_from(seat_idx).unwrap_or(0),
                                    team_idx,
                                    user_id,
                                    display_name,
                                });
                            }
                        }

                        // Echo correlation id on the joiner's match snapshot.
                        self.emit_match_snapshot_to(session_id, &match_id, id).await;

                        // Notify others in the match.
                        self.emit_match_update_to_match(&match_id, None).await;
                        self.broadcast_lobby_match_upsert(&match_id).await;
                    }
                    Err((code, msg)) => {
                        self.send_to(session_id, Self::err_out(id, code, msg)).await;
                    }
                }
            }

            C2sMessage::MatchWatch(d) => {
                let match_id = d.match_id;

                // v2: do not allow watching a match while already in a match.
                let already_in_match = {
                    let s = self.state.lock().await;
                    s.sessions
                        .get(&session_id)
                        .map(|sess| sess.active_match_id.is_some())
                        .unwrap_or(false)
                };

                if already_in_match {
                    self.send_to(
                        session_id,
                        Self::err_out(id, "ALREADY_IN_MATCH", "already in a match"),
                    )
                    .await;
                    return;
                }

                // Add as a participant (but do not create a player seat).
                let (exists, inserted, actor_user_id) = {
                    let mut s = self.state.lock().await;
                    match s.matches.get_mut(&match_id) {
                        Some(m) => {
                            let inserted = m.participants.insert(session_id);

                            let actor_user_id = s
                                .sessions
                                .get(&session_id)
                                .map(|sess| sess.user_id)
                                .unwrap_or(0);

                            if let Some(sess) = s.sessions.get_mut(&session_id) {
                                sess.active_match_id = Some(match_id.clone());
                                sess.player_key = None;
                            }

                            (true, inserted, actor_user_id)
                        }
                        None => (false, false, 0),
                    }
                };

                if !exists {
                    self.send_to(
                        session_id,
                        Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
                    )
                    .await;
                    return;
                }

                if inserted {
                    self.emit_history(GameHistoryEvent::SpectatorJoined {
                        match_id: match_id.clone(),
                        user_id: actor_user_id,
                    });
                }

                // Auto-join the match chat room (room id == match_id)
                self.join_room_internal(session_id, &match_id).await;

                // Send immediate snapshots to spectator (hands hidden, me omitted).
                //
                // Note: `emit_match_snapshot_to` already includes a game snapshot when gameplay is
                // running, so we intentionally don't emit a separate `game.snapshot` here.
                self.emit_match_snapshot_to(session_id, &match_id, id).await;

                // Broadcast updated match/lobby state (e.g. spectator_count changes).
                self.emit_match_update_to_match(&match_id, None).await;
                self.broadcast_lobby_match_upsert(&match_id).await;
            }

            C2sMessage::MatchLeave(d) => {
                let (msid, pkey, user_id) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (
                        sess.active_match_id.clone(),
                        sess.player_key.clone(),
                        sess.user_id,
                    )
                };

                let msid = match msid {
                    Some(msid) => msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                if msid != d.match_id {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                }

                // Player vs spectator leave:
                // - players have a player_key and occupy a seat
                // - spectators have no player_key and should only be removed from participants
                let _removed = if let Some(pkey) = pkey.as_deref() {
                    let removed = self
                        .leave_match_internal(session_id, &msid, pkey, user_id, "client_leave")
                        .await;

                    if removed {
                        self.broadcast_lobby_match_remove(&msid).await;
                    } else {
                        self.broadcast_lobby_match_upsert(&msid).await;
                    }

                    removed
                } else {
                    // spectator: just detach
                    self.leave_match_watch_internal(session_id, &msid, user_id, "client_leave")
                        .await;

                    // Notify remaining participants and refresh lobby counts.
                    self.emit_match_update_to_match(&msid, None).await;
                    self.broadcast_lobby_match_upsert(&msid).await;

                    false
                };

                // Explicit confirmation (new v2, no legacy quirks).
                self.send_to(
                    session_id,
                    WsOutMessage {
                        v: Default::default(),
                        msg: S2cMessage::MatchLeft(MatchLeftData {
                            match_id: msid.clone(),
                        }),
                        id: id.into(),
                    },
                )
                .await;

                // Also refresh lobby for the caller (useful after leave + reconnect flows).
                self.emit_lobby_snapshot_to(session_id, None).await;
            }

            C2sMessage::MatchReady(d) => {
                let msid = d.match_id;
                let ready = d.ready;

                let (active_msid, pkey, actor_user_id) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (
                        sess.active_match_id.clone(),
                        sess.player_key.clone(),
                        sess.user_id,
                    )
                };

                let active_msid = match active_msid {
                    Some(active_msid) => active_msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                let pkey = match pkey {
                    Some(pkey) => pkey,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_A_PLAYER", "not a player (spectator)"),
                        )
                        .await;
                        return;
                    }
                };

                if active_msid != msid {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                }

                let now_ms = Self::now_ms();
                let mut err: Option<(String, String)> = None;
                let mut history_action: Option<GameHistoryEvent> = None;
                {
                    let mut s = self.state.lock().await;
                    let Some(m) = s.matches.get_mut(&msid) else {
                        // The match may have been removed (e.g. last player left).
                        self.send_to(
                            session_id,
                            Self::err_out(id.clone(), "MATCH_NOT_FOUND", "match not found"),
                        )
                        .await;
                        return;
                    };

                    // v2: readiness only applies in lobby phase.
                    if m.phase != MatchPhase::Lobby {
                        err = Some(("BAD_STATE".into(), "match not in lobby".into()));
                    } else if let Some(seat_idx) = m.players.iter().position(|p| p.key == pkey) {
                        if let Some(p) = m.players.get_mut(seat_idx) {
                            p.ready = ready;
                            history_action = Some(GameHistoryEvent::GameAction {
                                match_id: msid.clone(),
                                actor_seat_idx: Some(u8::try_from(seat_idx).unwrap_or(0)),
                                actor_team_idx: Some(p.team.as_u8()),
                                actor_user_id,
                                ty: "match.ready".into(),
                                data: serde_json::json!({
                                    "ready": ready,
                                    "server_time_ms": now_ms,
                                }),
                            });
                        }
                    }

                    // Readiness is tracked per-player; the match lifecycle phase remains `lobby`
                    // until the owner successfully starts the match.
                    //
                    // (We intentionally avoid encoding readiness into `PublicMatch.phase`.)
                }

                if let Some((code, msg)) = err {
                    self.send_to(session_id, Self::err_out(id.clone(), code, msg))
                        .await;
                    return;
                }

                if let Some(ev) = history_action.take() {
                    self.emit_history(ev);
                }

                let correlated = id.clone().map(|id| (session_id, id));
                self.emit_match_update_to_match(&msid, correlated).await;
                self.broadcast_lobby_match_upsert(&msid).await;
            }

            C2sMessage::MatchStart(d) => {
                let msid = d.match_id;

                let (active_msid, pkey) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (sess.active_match_id.clone(), sess.player_key.clone())
                };

                let active_msid = match active_msid {
                    Some(active_msid) => active_msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                let pkey = match pkey {
                    Some(pkey) => pkey,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_A_PLAYER", "not a player (spectator)"),
                        )
                        .await;
                        return;
                    }
                };

                if active_msid != msid {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                }

                let mut err: Option<(String, String)> = None;
                {
                    let mut s = self.state.lock().await;
                    let Some(m) = s.matches.get_mut(&msid) else {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
                        )
                        .await;
                        return;
                    };

                    if m.owner_key != pkey {
                        err = Some(("NOT_OWNER".into(), "only the owner can start".into()));
                    } else if m.phase != MatchPhase::Lobby {
                        err = Some(("BAD_STATE".into(), "match not in lobby".into()));
                    } else {
                        let players_len = m.players.len();
                        if players_len < 2 {
                            err = Some((
                                "NOT_ENOUGH_PLAYERS".into(),
                                "need at least 2 players".into(),
                            ));
                        } else {
                            let team_0 = m
                                .players
                                .iter()
                                .filter(|p| p.team == TeamIdx::TEAM_0)
                                .count();
                            let team_1 = m
                                .players
                                .iter()
                                .filter(|p| p.team == TeamIdx::TEAM_1)
                                .count();

                            if team_0 != team_1 {
                                err = Some((
                                    "UNBALANCED_TEAMS".into(),
                                    "teams must be balanced".into(),
                                ));
                            } else if !m.players.iter().all(|p| p.ready) {
                                err =
                                    Some(("NOT_READY".into(), "all players must be ready".into()));
                            } else {
                                m.phase = MatchPhase::Started;
                                if m.game.is_none() {
                                    let forehand = (m.hand_no as u8) % (players_len as u8);

                                    m.game = Some(trucoshi_game::GameState::new_with_forehand(
                                        players_len,
                                        Self::seed_for_hand(&msid, m.hand_no),
                                        m.options.turn_time_ms,
                                        Self::now_ms(),
                                        forehand,
                                    ));
                                }
                            }
                        }
                    }
                }

                if let Some((code, msg)) = err {
                    self.send_to(session_id, Self::err_out(id, code, msg)).await;
                    return;
                }

                self.emit_history(GameHistoryEvent::MatchStarted {
                    match_id: msid.clone(),
                });

                let correlated = id.clone().map(|id| (session_id, id));
                self.emit_match_snapshot_to_match(&msid, correlated).await;
                self.broadcast_lobby_match_upsert(&msid).await;
            }

            C2sMessage::MatchPause(d) => {
                let msid = d.match_id;

                let (active_msid, pkey, actor_user_id) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (
                        sess.active_match_id.clone(),
                        sess.player_key.clone(),
                        sess.user_id,
                    )
                };

                let active_msid = match active_msid {
                    Some(active_msid) => active_msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                let pkey = match pkey {
                    Some(pkey) => pkey,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_A_PLAYER", "not a player (spectator)"),
                        )
                        .await;
                        return;
                    }
                };

                if active_msid != msid {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                };

                let now_ms = Self::now_ms();
                let mut err: Option<(String, String)> = None;
                let mut history_action: Option<GameHistoryEvent> = None;
                let mut broadcast = false;
                let mut spawn_expiration: Option<(String, Uuid)> = None;

                {
                    let mut s = self.state.lock().await;
                    let Some(m) = s.matches.get_mut(&msid) else {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
                        )
                        .await;
                        return;
                    };

                    if m.owner_key != pkey {
                        err = Some(("NOT_OWNER".into(), "only the owner can pause".into()));
                    } else if m.phase != MatchPhase::Started {
                        err = Some(("BAD_STATE".into(), "match not started".into()));
                    } else if m.pause_request.is_some() {
                        err = Some(("PAUSE_PENDING".into(), "pause already pending".into()));
                    } else if m.pending_unpause.is_some() {
                        err = Some((
                            "UNPAUSE_PENDING".into(),
                            "unpause countdown in progress".into(),
                        ));
                    } else if let Some(seat_idx) = m.players.iter().position(|p| p.key == pkey) {
                        let player = m.players.get(seat_idx).cloned().expect("player missing");
                        let requested_by_team = player.team;
                        let requested_by_user_id = player.user_id;
                        let awaiting_team = opposing_team(requested_by_team);
                        let awaiting_connected = team_has_connected_player(m, awaiting_team);

                        if awaiting_connected {
                            let token = Uuid::new_v4();
                            let expires_at_ms = now_ms + PAUSE_REQUEST_TIMEOUT_MS;

                            m.pause_request = Some(PauseRequestState {
                                token,
                                requested_by_seat_idx: u8::try_from(seat_idx).unwrap_or(0),
                                requested_by_user_id,
                                requested_by_team,
                                awaiting_team,
                                expires_at_ms,
                            });
                            m.pending_unpause = None;
                            m.auto_unpause = None;
                            broadcast = true;
                            spawn_expiration = Some((msid.clone(), token));
                        } else {
                            m.phase = MatchPhase::Paused;
                            m.pause_request = None;
                            m.pending_unpause = None;
                            m.auto_unpause = None;
                            broadcast = true;

                            history_action = Some(GameHistoryEvent::GameAction {
                                match_id: msid.clone(),
                                actor_seat_idx: Some(u8::try_from(seat_idx).unwrap_or(0)),
                                actor_team_idx: Some(requested_by_team.as_u8()),
                                actor_user_id,
                                ty: "match.pause".into(),
                                data: serde_json::json!({
                                    "server_time_ms": now_ms,
                                    "requires_vote": false,
                                }),
                            });
                        }
                    }
                }

                if let Some((code, msg)) = err {
                    self.send_to(session_id, Self::err_out(id, code, msg)).await;
                    return;
                }

                if let Some(ev) = history_action.take() {
                    self.emit_history(ev);
                }

                if broadcast {
                    let correlated = id.clone().map(|id| (session_id, id));
                    self.emit_match_update_to_match(&msid, correlated).await;
                    self.broadcast_lobby_match_upsert(&msid).await;
                }

                if let Some((match_id, token)) = spawn_expiration {
                    let rt = self.clone();
                    tokio::spawn(async move {
                        rt.expire_pause_request(match_id, token).await;
                    });
                }
            }

            C2sMessage::MatchPauseVote(d) => {
                let msid = d.match_id;
                let accept = d.accept;

                let (active_msid, pkey) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (sess.active_match_id.clone(), sess.player_key.clone())
                };

                let active_msid = match active_msid {
                    Some(active_msid) => active_msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                let pkey = match pkey {
                    Some(pkey) => pkey,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_A_PLAYER", "not a player (spectator)"),
                        )
                        .await;
                        return;
                    }
                };

                if active_msid != msid {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                };

                let now_ms = Self::now_ms();
                let mut err: Option<(String, String)> = None;
                let mut history_action: Option<GameHistoryEvent> = None;
                let mut broadcast = false;
                let mut spawn_auto: Option<(String, Uuid, i64)> = None;

                {
                    let mut s = self.state.lock().await;
                    let Some(m) = s.matches.get_mut(&msid) else {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
                        )
                        .await;
                        return;
                    };

                    if let Some(req) = m.pause_request.clone() {
                        if let Some(seat_idx) = m.players.iter().position(|p| p.key == pkey) {
                            let player = m.players.get(seat_idx).cloned().expect("player missing");
                            if player.team != req.awaiting_team {
                                err = Some((
                                    "NOT_PENDING_TEAM".into(),
                                    "not the awaiting team".into(),
                                ));
                            } else if now_ms >= req.expires_at_ms {
                                m.pause_request = None;
                                broadcast = true;
                                err = Some((
                                    "PAUSE_REQUEST_EXPIRED".into(),
                                    "pause request expired".into(),
                                ));
                            } else if !accept {
                                m.pause_request = None;
                                broadcast = true;
                            } else {
                                m.pause_request = None;
                                m.pending_unpause = None;
                                m.phase = MatchPhase::Paused;
                                broadcast = true;

                                let delay_ms = m.options.abandon_time_ms.max(1);
                                let auto_token = Uuid::new_v4();
                                m.auto_unpause = Some(AutoUnpauseState {
                                    token: auto_token,
                                    trigger_at_ms: now_ms + delay_ms,
                                    paused_by_team: req.requested_by_team,
                                });
                                spawn_auto = Some((msid.clone(), auto_token, delay_ms));

                                let actor =
                                    m.players.get(req.requested_by_seat_idx as usize).cloned();

                                history_action = Some(GameHistoryEvent::GameAction {
                                    match_id: msid.clone(),
                                    actor_seat_idx: actor
                                        .as_ref()
                                        .map(|_| req.requested_by_seat_idx),
                                    actor_team_idx: actor.map(|p| p.team.as_u8()),
                                    actor_user_id: req.requested_by_user_id,
                                    ty: "match.pause".into(),
                                    data: serde_json::json!({
                                        "server_time_ms": now_ms,
                                        "requires_vote": true,
                                    }),
                                });
                            }
                        } else {
                            err = Some(("PLAYER_NOT_FOUND".into(), "player seat missing".into()));
                        }
                    } else {
                        err = Some(("NO_PAUSE_REQUEST".into(), "no pause request pending".into()));
                    }
                }

                if let Some((code, msg)) = err {
                    self.send_to(session_id, Self::err_out(id, code, msg)).await;
                    return;
                }

                if let Some(ev) = history_action.take() {
                    self.emit_history(ev);
                }

                if broadcast {
                    self.emit_match_update_to_match(&msid, id.clone().map(|id| (session_id, id)))
                        .await;
                    self.broadcast_lobby_match_upsert(&msid).await;
                }

                if let Some((match_id, token, delay_ms)) = spawn_auto {
                    let rt = self.clone();
                    tokio::spawn(async move {
                        rt.auto_unpause_after(match_id, token, delay_ms).await;
                    });
                }
            }
            C2sMessage::MatchResume(d) => {
                let msid = d.match_id;

                let (active_msid, pkey, actor_user_id) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (
                        sess.active_match_id.clone(),
                        sess.player_key.clone(),
                        sess.user_id,
                    )
                };

                let active_msid = match active_msid {
                    Some(active_msid) => active_msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                let pkey = match pkey {
                    Some(pkey) => pkey,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_A_PLAYER", "not a player (spectator)"),
                        )
                        .await;
                        return;
                    }
                };

                if active_msid != msid {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                };

                let now_ms = Self::now_ms();
                let mut err: Option<(String, String)> = None;
                let mut broadcast = false;
                let mut spawn_resume: Option<(String, Uuid, i64)> = None;
                let mut immediate_resume: Option<Uuid> = None;

                {
                    let mut s = self.state.lock().await;
                    let Some(m) = s.matches.get_mut(&msid) else {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
                        )
                        .await;
                        return;
                    };

                    if m.owner_key != pkey {
                        err = Some(("NOT_OWNER".into(), "only the owner can resume".into()));
                    } else if m.phase != MatchPhase::Paused {
                        err = Some(("BAD_STATE".into(), "match not paused".into()));
                    } else if m.pending_unpause.is_some() {
                        err = Some(("UNPAUSE_PENDING".into(), "unpause already pending".into()));
                    } else if let Some(seat_idx) = m.players.iter().position(|p| p.key == pkey) {
                        let player = m.players.get(seat_idx).cloned().expect("player missing");
                        let trigger = PendingUnpauseTrigger::Manual {
                            actor_user_id,
                            actor_seat_idx: u8::try_from(seat_idx).unwrap_or(0),
                            actor_team_idx: player.team.as_u8(),
                        };

                        if let Some((token, delay_ms)) =
                            Self::begin_pending_unpause_locked(m, player.team, trigger, now_ms)
                        {
                            m.auto_unpause = None;
                            broadcast = true;
                            if delay_ms == 0 {
                                immediate_resume = Some(token);
                            } else {
                                spawn_resume = Some((msid.clone(), token, delay_ms));
                            }
                        } else {
                            err =
                                Some(("UNPAUSE_PENDING".into(), "unpause already pending".into()));
                        }
                    } else {
                        err = Some(("PLAYER_NOT_FOUND".into(), "player seat missing".into()));
                    }
                }

                if let Some((code, msg)) = err {
                    self.send_to(session_id, Self::err_out(id, code, msg)).await;
                    return;
                }

                if broadcast {
                    let correlated = id.clone().map(|id| (session_id, id));
                    self.emit_match_update_to_match(&msid, correlated).await;
                    self.broadcast_lobby_match_upsert(&msid).await;
                }

                if let Some(token) = immediate_resume {
                    self.complete_pending_unpause(msid.clone(), token).await;
                }

                if let Some((match_id, token, delay_ms)) = spawn_resume {
                    let rt = self.clone();
                    tokio::spawn(async move {
                        rt.await_pending_unpause(match_id, token, delay_ms).await;
                    });
                }
            }

            C2sMessage::MatchOptionsSet(d) => {
                let msid = d.match_id;
                let new_options = d.options;

                let (active_msid, owner_key, actor_user_id) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (
                        sess.active_match_id.clone(),
                        sess.player_key.clone(),
                        sess.user_id,
                    )
                };

                let active_msid = match active_msid {
                    Some(active_msid) => active_msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                let owner_key = match owner_key {
                    Some(pkey) => pkey,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_A_PLAYER", "not a player (spectator)"),
                        )
                        .await;
                        return;
                    }
                };

                if active_msid != msid {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                }

                let now_ms = Self::now_ms();
                let mut err: Option<(String, String)> = None;
                let mut history_action: Option<GameHistoryEvent> = None;
                {
                    let mut s = self.state.lock().await;
                    let Some(m) = s.matches.get_mut(&msid) else {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
                        )
                        .await;
                        return;
                    };

                    if m.owner_key != owner_key {
                        err = Some((
                            "NOT_OWNER".into(),
                            "only the owner can update options".into(),
                        ));
                    } else if m.phase != MatchPhase::Lobby {
                        err = Some(("BAD_STATE".into(), "match not in lobby".into()));
                    } else {
                        let player_count = m.players.len();
                        if player_count > usize::from(new_options.max_players) {
                            err = Some((
                                "MAX_PLAYERS_TOO_LOW".into(),
                                "max_players cannot be lower than the current player count".into(),
                            ));
                        } else {
                            let team_capacity = usize::from(new_options.max_players / 2);
                            let team_0 = m
                                .players
                                .iter()
                                .filter(|p| p.team == TeamIdx::TEAM_0)
                                .count();
                            let team_1 = m
                                .players
                                .iter()
                                .filter(|p| p.team == TeamIdx::TEAM_1)
                                .count();

                            if team_0 > team_capacity || team_1 > team_capacity {
                                err = Some((
                                    "TEAM_TOO_LARGE".into(),
                                    "new max_players would evict an existing team".into(),
                                ));
                            } else if let Some(owner_idx) =
                                m.players.iter().position(|p| p.key == owner_key)
                            {
                                let owner_team_idx = m
                                    .players
                                    .get(owner_idx)
                                    .map(|p| p.team.as_u8())
                                    .unwrap_or(0);

                                let previous = m.options;
                                if previous != new_options {
                                    m.options = new_options;

                                    let previous_json = serde_json::to_value(previous)
                                        .unwrap_or_else(|_| {
                                            serde_json::json!({
                                                "error": "failed_to_serialize_previous_options"
                                            })
                                        });
                                    let updated_json = serde_json::to_value(new_options)
                                        .unwrap_or_else(|_| {
                                            serde_json::json!({
                                                "error": "failed_to_serialize_updated_options"
                                            })
                                        });

                                    history_action = Some(GameHistoryEvent::GameAction {
                                        match_id: msid.clone(),
                                        actor_seat_idx: Some(
                                            u8::try_from(owner_idx).unwrap_or_default(),
                                        ),
                                        actor_team_idx: Some(owner_team_idx),
                                        actor_user_id,
                                        ty: "match.options.set".into(),
                                        data: serde_json::json!({
                                            "previous": previous_json,
                                            "updated": updated_json,
                                            "server_time_ms": now_ms,
                                        }),
                                    });
                                }
                            } else {
                                err = Some(("OWNER_NOT_FOUND".into(), "owner seat missing".into()));
                            }
                        }
                    }
                }

                if let Some((code, msg)) = err {
                    self.send_to(session_id, Self::err_out(id, code, msg)).await;
                    return;
                }

                if let Some(ev) = history_action.take() {
                    self.emit_history(ev);
                }

                let correlated = id.clone().map(|id| (session_id, id));
                self.emit_match_update_to_match(&msid, correlated).await;
                self.broadcast_lobby_match_upsert(&msid).await;
            }

            C2sMessage::GamePlayCard(d) => {
                let msid = d.match_id;
                let card_idx = d.card_idx;

                let (active_msid, pkey, actor_user_id) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (
                        sess.active_match_id.clone(),
                        sess.player_key.clone(),
                        sess.user_id,
                    )
                };

                let active_msid = match active_msid {
                    Some(active_msid) => active_msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                let pkey = match pkey {
                    Some(pkey) => pkey,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_A_PLAYER", "not a player (spectator)"),
                        )
                        .await;
                        return;
                    }
                };

                if active_msid != msid {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                }

                // Apply move under lock.
                let mut err: Option<(String, String)> = None;
                let mut history_action: Option<GameHistoryEvent> = None;
                let mut history_finish: Option<([u8; 2], String)> = None;
                {
                    let mut s = self.state.lock().await;
                    let Some(m) = s.matches.get_mut(&msid) else {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
                        )
                        .await;
                        return;
                    };

                    if m.phase == MatchPhase::Paused {
                        err = Some(("MATCH_PAUSED".into(), "match paused".into()));
                    } else if m.phase != MatchPhase::Started {
                        err = Some(("MATCH_NOT_STARTED".into(), "match not started".into()));
                    } else {
                        let from_player_idx = match m.players.iter().position(|p| p.key == pkey) {
                            Some(i) => i,
                            None => {
                                err = Some(("NOT_IN_MATCH".into(), "not in a match".into()));
                                0
                            }
                        };

                        if err.is_none() {
                            if m.game.is_none() {
                                let players_len = m.players.len();
                                let forehand = if players_len == 0 {
                                    0
                                } else {
                                    (m.hand_no as u8) % (players_len as u8)
                                };

                                m.game = Some(trucoshi_game::GameState::new_with_forehand(
                                    players_len,
                                    Self::seed_for_hand(&msid, m.hand_no),
                                    m.options.turn_time_ms,
                                    Self::now_ms(),
                                    forehand,
                                ));
                            }
                            let g = m.game.as_mut().expect("game state initialized");

                            // Protocol v2 is strict: don't allow card plays while waiting on
                            // a truco/envido/flor answer.
                            if g.public.hand_state != HandState::WaitingPlay {
                                err = Some((
                                    "BAD_STATE".into(),
                                    "hand not accepting card plays".into(),
                                ));
                            }

                            // Protocol v2 is strict: the caller must be the current turn seat.
                            if err.is_none() && (g.public.turn_seat_idx as usize) != from_player_idx
                            {
                                err = Some(("NOT_YOUR_TURN".into(), "not your turn".into()));
                            } else if err.is_none() {
                                let teams_by_player_idx =
                                    m.players.iter().map(|p| p.team.as_u8()).collect::<Vec<_>>();

                                let actor_seat_idx =
                                    Some(u8::try_from(from_player_idx).unwrap_or(0));
                                let actor_team_idx =
                                    m.players.get(from_player_idx).map(|p| p.team.as_u8());
                                let hand_no_at_action = m.hand_no;

                                let outcome = g.play_card(
                                    card_idx,
                                    &teams_by_player_idx,
                                    m.options.turn_time_ms,
                                    Self::now_ms(),
                                );

                                match outcome {
                                    PlayOutcome::Invalid => {
                                        err = Some(("INVALID_CARD".into(), "invalid card".into()));
                                    }
                                    PlayOutcome::TurnAdvanced => {
                                        history_action = Some(GameHistoryEvent::GameAction {
                                            match_id: msid.clone(),
                                            actor_seat_idx,
                                            actor_team_idx,
                                            actor_user_id,
                                            ty: "game.play_card".into(),
                                            data: serde_json::json!({
                                                "hand_no": hand_no_at_action,
                                                "card_idx": card_idx,
                                                "outcome": "turn_advanced",
                                            }),
                                        });
                                    }
                                    PlayOutcome::TrickEnded => {
                                        history_action = Some(GameHistoryEvent::GameAction {
                                            match_id: msid.clone(),
                                            actor_seat_idx,
                                            actor_team_idx,
                                            actor_user_id,
                                            ty: "game.play_card".into(),
                                            data: serde_json::json!({
                                                "hand_no": hand_no_at_action,
                                                "card_idx": card_idx,
                                                "outcome": "trick_ended",
                                            }),
                                        });
                                    }
                                    PlayOutcome::HandEnded {
                                        winner_team_idx,
                                        points,
                                    } => {
                                        history_action = Some(GameHistoryEvent::GameAction {
                                            match_id: msid.clone(),
                                            actor_seat_idx,
                                            actor_team_idx,
                                            actor_user_id,
                                            ty: "game.play_card".into(),
                                            data: serde_json::json!({
                                                "hand_no": hand_no_at_action,
                                                "card_idx": card_idx,
                                                "outcome": "hand_ended",
                                                "winner_team_idx": winner_team_idx,
                                                "points": points,
                                            }),
                                        });

                                        if winner_team_idx < 2 {
                                            m.team_points[winner_team_idx as usize] = m.team_points
                                                [winner_team_idx as usize]
                                                .saturating_add(points);
                                        }

                                        let match_points = m.options.match_points.max(1);
                                        if winner_team_idx < 2
                                            && m.team_points[winner_team_idx as usize]
                                                >= match_points
                                        {
                                            m.phase = MatchPhase::Finished;
                                            history_finish =
                                                Some((m.team_points, "score_reached".into()));
                                            g.public.hand_state = HandState::Finished;
                                            g.public.winner_team_idx = Some(winner_team_idx);
                                            m.reset_pause_state();
                                        } else {
                                            // Match continues. First, expose the finished hand state to
                                            // clients, then stage the next hand so we can broadcast it as
                                            // a second update.
                                            g.public.hand_state = HandState::Finished;
                                            g.public.winner_team_idx = Some(winner_team_idx);

                                            let players_len = m.players.len();
                                            let next_forehand = if players_len == 0 {
                                                0
                                            } else {
                                                (g.public.forehand_seat_idx + 1)
                                                    % (players_len as u8)
                                            };

                                            m.hand_no = m.hand_no.saturating_add(1);
                                            m.pending_game =
                                                Some(trucoshi_game::GameState::new_with_forehand(
                                                    players_len,
                                                    Self::seed_for_hand(&msid, m.hand_no),
                                                    m.options.turn_time_ms,
                                                    Self::now_ms(),
                                                    next_forehand,
                                                ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some((code, msg)) = err {
                    self.send_to(session_id, Self::err_out(id, code, msg)).await;
                    return;
                }

                if let Some(ev) = history_action.take() {
                    self.emit_history(ev);
                }

                if let Some((team_points, reason)) = history_finish.take() {
                    self.emit_history(GameHistoryEvent::MatchFinished {
                        match_id: msid.clone(),
                        team_points,
                        reason,
                    });
                }

                // Broadcast updates outside the lock.
                //
                // Protocol v2: echo the caller correlation id on *all* state updates triggered by
                // the request (match + game), not just the gameplay update.
                let correlated = id.clone().map(|id| (session_id, id));
                self.emit_match_update_to_match(&msid, correlated.clone())
                    .await;
                self.broadcast_game_update_to_match(&msid, correlated.clone())
                    .await;
                self.broadcast_lobby_match_upsert(&msid).await;

                self.maybe_start_pending_hand(&msid, correlated).await;
            }

            C2sMessage::GameSay(d) => {
                let msid = d.match_id;
                let command = d.command;
                let command_for_history = command.clone();

                let (active_msid, pkey, actor_user_id) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (
                        sess.active_match_id.clone(),
                        sess.player_key.clone(),
                        sess.user_id,
                    )
                };

                let active_msid = match active_msid {
                    Some(active_msid) => active_msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                let pkey = match pkey {
                    Some(pkey) => pkey,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_A_PLAYER", "not a player (spectator)"),
                        )
                        .await;
                        return;
                    }
                };

                if active_msid != msid {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                }

                let mut err: Option<(String, String)> = None;
                let mut history_action: Option<GameHistoryEvent> = None;
                let mut history_finish: Option<([u8; 2], String)> = None;
                {
                    let mut s = self.state.lock().await;
                    let Some(m) = s.matches.get_mut(&msid) else {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
                        )
                        .await;
                        return;
                    };

                    if m.phase == MatchPhase::Paused {
                        err = Some(("MATCH_PAUSED".into(), "match paused".into()));
                    } else if m.phase != MatchPhase::Started {
                        err = Some(("MATCH_NOT_STARTED".into(), "match not started".into()));
                    } else {
                        if m.game.is_none() {
                            let players_len = m.players.len();
                            let forehand = if players_len == 0 {
                                0
                            } else {
                                (m.hand_no as u8) % (players_len as u8)
                            };

                            m.game = Some(trucoshi_game::GameState::new_with_forehand(
                                players_len,
                                Self::seed_for_hand(&msid, m.hand_no),
                                m.options.turn_time_ms,
                                Self::now_ms(),
                                forehand,
                            ));
                        }
                        let g = m.game.as_mut().expect("game state initialized");

                        // Determine caller player idx.
                        let from_player_idx = match m.players.iter().position(|p| p.key == pkey) {
                            Some(i) => i,
                            None => {
                                err = Some(("NOT_IN_MATCH".into(), "not in a match".into()));
                                0
                            }
                        };

                        if err.is_none() {
                            // Protocol v2 is strict: the caller must be the current turn seat.
                            if (g.public.turn_seat_idx as usize) != from_player_idx {
                                err = Some(("NOT_YOUR_TURN".into(), "not your turn".into()));
                            }

                            // Protocol v2 is strict: the command must be valid for the current
                            // public hand state.
                            if err.is_none() {
                                let allowed =
                                    g.possible_commands_for_player(from_player_idx, m.options.flor);
                                if !allowed.contains(&command) {
                                    err = Some((
                                        "COMMAND_NOT_ALLOWED".into(),
                                        "command not allowed".into(),
                                    ));
                                }
                            }

                            if err.is_none() {
                                let teams_by_player_idx =
                                    m.players.iter().map(|p| p.team.as_u8()).collect::<Vec<_>>();

                                let actor_seat_idx =
                                    Some(u8::try_from(from_player_idx).unwrap_or(0));
                                let actor_team_idx =
                                    m.players.get(from_player_idx).map(|p| p.team.as_u8());
                                let hand_no_at_action = m.hand_no;

                                let command_value = serde_json::to_value(&command_for_history)
                                    .unwrap_or_else(|_| {
                                        serde_json::json!("failed_to_serialize_command")
                                    });

                                let now_ms = Self::now_ms();
                                let out = g.apply_command(
                                    command,
                                    from_player_idx,
                                    &teams_by_player_idx,
                                    m.options.match_points,
                                    m.team_points,
                                    m.options.falta_envido_goal(),
                                    m.options.turn_time_ms,
                                    now_ms,
                                );

                                match out {
                                    CommandOutcome::None => {
                                        history_action = Some(GameHistoryEvent::GameAction {
                                            match_id: msid.clone(),
                                            actor_seat_idx,
                                            actor_team_idx,
                                            actor_user_id,
                                            ty: "game.say".into(),
                                            data: serde_json::json!({
                                                "hand_no": hand_no_at_action,
                                                "command": command_value,
                                                "outcome": "none",
                                                "server_time_ms": now_ms,
                                            }),
                                        });
                                    }

                                    CommandOutcome::PointsAwarded {
                                        winner_team_idx,
                                        points,
                                        reason,
                                    } => {
                                        let award_reason = match reason {
                                            PointsAwardReason::EnvidoAccepted => "envido_accepted",
                                            PointsAwardReason::EnvidoFaltaAccepted => {
                                                "envido_falta_accepted"
                                            }
                                            PointsAwardReason::EnvidoDeclined => "envido_declined",
                                        };

                                        history_action = Some(GameHistoryEvent::GameAction {
                                            match_id: msid.clone(),
                                            actor_seat_idx,
                                            actor_team_idx,
                                            actor_user_id,
                                            ty: "game.say".into(),
                                            data: serde_json::json!({
                                                "hand_no": hand_no_at_action,
                                                "command": command_value,
                                                "outcome": "points_awarded",
                                                "winner_team_idx": winner_team_idx,
                                                "points": points,
                                                "award_reason": award_reason,
                                                "server_time_ms": now_ms,
                                            }),
                                        });

                                        if winner_team_idx < 2 {
                                            m.team_points[winner_team_idx as usize] = m.team_points
                                                [winner_team_idx as usize]
                                                .saturating_add(points);
                                        }

                                        let match_points = m.options.match_points.max(1);
                                        if winner_team_idx < 2
                                            && m.team_points[winner_team_idx as usize]
                                                >= match_points
                                        {
                                            m.phase = MatchPhase::Finished;
                                            m.reset_pause_state();
                                            m.game = None;
                                            m.pending_game = None;
                                            history_finish =
                                                Some((m.team_points, "score_reached".into()));
                                        }
                                    }

                                    CommandOutcome::HandEnded {
                                        winner_team_idx,
                                        points,
                                    } => {
                                        history_action = Some(GameHistoryEvent::GameAction {
                                            match_id: msid.clone(),
                                            actor_seat_idx,
                                            actor_team_idx,
                                            actor_user_id,
                                            ty: "game.say".into(),
                                            data: serde_json::json!({
                                                "hand_no": hand_no_at_action,
                                                "command": command_value,
                                                "outcome": "hand_ended",
                                                "winner_team_idx": winner_team_idx,
                                                "points": points,
                                                "server_time_ms": now_ms,
                                            }),
                                        });

                                        if winner_team_idx < 2 {
                                            m.team_points[winner_team_idx as usize] = m.team_points
                                                [winner_team_idx as usize]
                                                .saturating_add(points);
                                        }

                                        let match_points = m.options.match_points.max(1);
                                        if winner_team_idx < 2
                                            && m.team_points[winner_team_idx as usize]
                                                >= match_points
                                        {
                                            m.phase = MatchPhase::Finished;
                                            history_finish =
                                                Some((m.team_points, "score_reached".into()));
                                            g.public.hand_state = HandState::Finished;
                                            g.public.winner_team_idx = Some(winner_team_idx);
                                            m.reset_pause_state();
                                        } else {
                                            // Match continues: broadcast the finished hand first, then
                                            // start the next hand with a follow-up update.
                                            g.public.hand_state = HandState::Finished;
                                            g.public.winner_team_idx = Some(winner_team_idx);

                                            let players_len = m.players.len();
                                            let next_forehand = if players_len == 0 {
                                                0
                                            } else {
                                                (g.public.forehand_seat_idx + 1)
                                                    % (players_len as u8)
                                            };

                                            m.hand_no = m.hand_no.saturating_add(1);
                                            m.pending_game =
                                                Some(trucoshi_game::GameState::new_with_forehand(
                                                    players_len,
                                                    Self::seed_for_hand(&msid, m.hand_no),
                                                    m.options.turn_time_ms,
                                                    Self::now_ms(),
                                                    next_forehand,
                                                ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some((code, msg)) = err {
                    self.send_to(session_id, Self::err_out(id, code, msg)).await;
                    return;
                }

                if let Some(ev) = history_action.take() {
                    self.emit_history(ev);
                }

                if let Some((team_points, reason)) = history_finish.take() {
                    self.emit_history(GameHistoryEvent::MatchFinished {
                        match_id: msid.clone(),
                        team_points,
                        reason,
                    });
                }

                {
                    // Protocol v2: echo correlation id on all state updates caused by this
                    // command (match + game).
                    let correlated = id.clone().map(|id| (session_id, id));
                    self.emit_match_update_to_match(&msid, correlated.clone())
                        .await;
                    self.broadcast_game_update_to_match(&msid, correlated.clone())
                        .await;
                    self.broadcast_lobby_match_upsert(&msid).await;

                    self.maybe_start_pending_hand(&msid, correlated).await;
                }
            }

            C2sMessage::MatchRematch(d) => {
                let msid = d.match_id;

                let (active_msid, pkey, actor_user_id) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (
                        sess.active_match_id.clone(),
                        sess.player_key.clone(),
                        sess.user_id,
                    )
                };

                let active_msid = match active_msid {
                    Some(active_msid) => active_msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                let pkey = match pkey {
                    Some(pkey) => pkey,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_A_PLAYER", "not a player (spectator)"),
                        )
                        .await;
                        return;
                    }
                };

                if active_msid != msid {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                }

                let now_ms = Self::now_ms();
                let mut err: Option<(String, String)> = None;

                struct RematchOutputs {
                    new_match_id: String,
                    room_moves: Vec<(Uuid, bool)>,
                    watcher_sessions: Vec<Uuid>,
                    history_events: Vec<GameHistoryEvent>,
                    rematch_event: GameHistoryEvent,
                }

                let mut outputs: Option<RematchOutputs> = None;

                {
                    let mut s = self.state.lock().await;
                    let Some(m) = s.matches.get(&msid) else {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
                        )
                        .await;
                        return;
                    };

                    if m.owner_key != pkey {
                        err = Some(("NOT_OWNER".into(), "only the owner can rematch".into()));
                    } else if m.phase != MatchPhase::Finished {
                        err = Some(("BAD_STATE".into(), "match not finished".into()));
                    } else {
                        struct PendingPlayer {
                            session_id: Uuid,
                            player: PlayerState,
                            is_owner: bool,
                        }

                        let participants = m.participants.clone();
                        let options = m.options.clone();
                        let players_snapshot = m.players.clone();
                        let owner_key = m.owner_key.clone();

                        let mut key_to_session: HashMap<String, Uuid> = HashMap::new();
                        for (sid, sess) in s.sessions.iter() {
                            if let Some(pk) = sess.player_key.as_deref() {
                                key_to_session.insert(pk.to_string(), *sid);
                            }
                        }

                        let mut pending_players: Vec<PendingPlayer> = Vec::new();
                        for player in &players_snapshot {
                            if let Some(&sid) = key_to_session.get(&player.key) {
                                let new_key = Uuid::new_v4().to_string();
                                pending_players.push(PendingPlayer {
                                    session_id: sid,
                                    player: PlayerState {
                                        key: new_key,
                                        user_id: player.user_id,
                                        name: player.name.clone(),
                                        team: player.team,
                                        ready: false,
                                        last_active_ms: now_ms,
                                        disconnected_at_ms: None,
                                    },
                                    is_owner: player.key == owner_key,
                                });
                            }
                        }

                        if pending_players.len() < 2 {
                            err = Some((
                                "NOT_ENOUGH_PLAYERS".into(),
                                "need at least 2 connected players".into(),
                            ));
                        } else if !pending_players.iter().any(|p| p.is_owner) {
                            err = Some((
                                "NOT_OWNER".into(),
                                "owner must be connected to rematch".into(),
                            ));
                        } else {
                            let new_match_id = Uuid::new_v4().to_string();
                            let new_players = pending_players
                                .iter()
                                .map(|p| p.player.clone())
                                .collect::<Vec<_>>();

                            let owner_player = pending_players
                                .iter()
                                .find(|p| p.is_owner)
                                .map(|p| p.player.clone())
                                .expect("owner player missing");

                            let owner_seat_idx_new = new_players
                                .iter()
                                .position(|p| p.key == owner_player.key)
                                .unwrap_or(0);

                            let mut participants_set = HashSet::new();
                            for pending in &pending_players {
                                participants_set.insert(pending.session_id);
                            }

                            let new_match_state = MatchState {
                                match_id: new_match_id.clone(),
                                owner_key: owner_player.key.clone(),
                                options: options.clone(),
                                phase: MatchPhase::Lobby,
                                players: new_players.clone(),
                                participants: participants_set,
                                team_points: [0, 0],
                                hand_no: 0,
                                game: None,
                                pending_game: None,
                                pause_request: None,
                                auto_unpause: None,
                                pending_unpause: None,
                            };

                            s.matches.remove(&msid);
                            s.matches.insert(new_match_id.clone(), new_match_state);

                            for pending in &pending_players {
                                if let Some(sess) = s.sessions.get_mut(&pending.session_id) {
                                    sess.active_match_id = Some(new_match_id.clone());
                                    sess.player_key = Some(pending.player.key.clone());
                                    sess.last_active_ms = now_ms;
                                }
                            }

                            let player_session_ids: HashSet<Uuid> =
                                pending_players.iter().map(|p| p.session_id).collect();

                            let mut watchers: Vec<Uuid> = Vec::new();
                            for sid in participants {
                                if !player_session_ids.contains(&sid) {
                                    if let Some(sess) = s.sessions.get_mut(&sid) {
                                        if sess.active_match_id.as_deref() == Some(msid.as_str()) {
                                            sess.active_match_id = None;
                                            sess.player_key = None;
                                        }
                                    }
                                    watchers.push(sid);
                                }
                            }

                            let mut room_moves = pending_players
                                .iter()
                                .map(|p| (p.session_id, true))
                                .collect::<Vec<_>>();
                            room_moves.extend(watchers.iter().map(|sid| (*sid, false)));

                            let owner_old_idx = players_snapshot
                                .iter()
                                .position(|p| p.key == owner_key)
                                .map(|idx| u8::try_from(idx).unwrap_or(0));

                            let owner_old_team = players_snapshot
                                .iter()
                                .find(|p| p.key == owner_key)
                                .map(|p| p.team.as_u8());

                            let rematch_event = GameHistoryEvent::GameAction {
                                match_id: msid.clone(),
                                actor_seat_idx: owner_old_idx,
                                actor_team_idx: owner_old_team,
                                actor_user_id,
                                ty: "match.rematch".into(),
                                data: serde_json::json!({
                                    "new_match_id": new_match_id.clone(),
                                    "player_count": pending_players.len(),
                                    "server_time_ms": now_ms,
                                }),
                            };

                            let match_options =
                                serde_json::to_value(&options).unwrap_or_else(|_| {
                                    serde_json::json!({
                                        "error": "failed_to_serialize_match_options"
                                    })
                                });

                            let mut history_events: Vec<GameHistoryEvent> = Vec::new();
                            history_events.push(GameHistoryEvent::MatchCreated {
                                match_id: new_match_id.clone(),
                                server_version: env!("CARGO_PKG_VERSION").to_string(),
                                protocol_version: 2,
                                rng_seed: Self::seed_for_hand(&new_match_id, 0) as i64,
                                options: serde_json::json!({
                                    "ws_match_id": new_match_id.clone(),
                                    "match_options": match_options,
                                }),
                                owner: HistoryPlayer {
                                    seat_idx: u8::try_from(owner_seat_idx_new).unwrap_or(0),
                                    team_idx: owner_player.team.as_u8(),
                                    user_id: owner_player.user_id,
                                    display_name: owner_player.name.clone(),
                                },
                            });

                            for (seat_idx, player) in new_players.iter().enumerate() {
                                if player.key == owner_player.key {
                                    continue;
                                }
                                history_events.push(GameHistoryEvent::PlayerJoined {
                                    match_id: new_match_id.clone(),
                                    seat_idx: u8::try_from(seat_idx).unwrap_or(0),
                                    team_idx: player.team.as_u8(),
                                    user_id: player.user_id,
                                    display_name: player.name.clone(),
                                });
                            }

                            outputs = Some(RematchOutputs {
                                new_match_id,
                                room_moves,
                                watcher_sessions: watchers,
                                history_events,
                                rematch_event,
                            });
                        }
                    }
                }

                if let Some((code, msg)) = err {
                    self.send_to(session_id, Self::err_out(id, code, msg)).await;
                    return;
                }

                let Some(outputs) = outputs else {
                    return;
                };

                for sid in &outputs.watcher_sessions {
                    self.send_to(
                        *sid,
                        WsOutMessage {
                            v: Default::default(),
                            msg: S2cMessage::MatchLeft(MatchLeftData {
                                match_id: msid.clone(),
                            }),
                            id: Default::default(),
                        },
                    )
                    .await;
                }

                for (sid, join_new) in &outputs.room_moves {
                    self.leave_room_internal(*sid, &msid).await;
                    if *join_new {
                        self.join_room_internal(*sid, &outputs.new_match_id).await;
                    }
                }

                self.emit_history(outputs.rematch_event);
                for ev in outputs.history_events {
                    self.emit_history(ev);
                }

                let correlated = id.clone().map(|id| (session_id, id));
                self.emit_match_snapshot_to_match(&outputs.new_match_id, correlated)
                    .await;

                self.broadcast_lobby_match_remove(&msid).await;
                self.broadcast_lobby_match_upsert(&outputs.new_match_id)
                    .await;
            }

            C2sMessage::MatchKick(d) => {
                let msid = d.match_id;
                let target_seat = usize::from(d.seat_idx);

                let (active_msid, owner_key) = {
                    let s = self.state.lock().await;
                    let Some(sess) = s.sessions.get(&session_id) else {
                        return;
                    };
                    (sess.active_match_id.clone(), sess.player_key.clone())
                };

                let active_msid = match active_msid {
                    Some(active_msid) => active_msid,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_IN_MATCH", "not in a match"),
                        )
                        .await;
                        return;
                    }
                };

                let owner_key = match owner_key {
                    Some(pkey) => pkey,
                    None => {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "NOT_A_PLAYER", "not a player (spectator)"),
                        )
                        .await;
                        return;
                    }
                };

                if active_msid != msid {
                    self.send_to(
                        session_id,
                        Self::err_out(
                            id,
                            "MATCH_MISMATCH",
                            "message match_id does not match your active match",
                        ),
                    )
                    .await;
                    return;
                }

                let mut owner_meta: Option<(u8, u8, i64)> = None;
                let mut target: Option<(String, i64, Option<Uuid>, u8, u8, String)> = None;
                let mut err: Option<(String, String)> = None;
                {
                    let s = self.state.lock().await;
                    let Some(m) = s.matches.get(&msid) else {
                        self.send_to(
                            session_id,
                            Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
                        )
                        .await;
                        return;
                    };

                    if m.owner_key != owner_key {
                        err = Some(("NOT_OWNER".into(), "only the owner can kick".into()));
                    } else if m.phase != MatchPhase::Lobby {
                        err = Some(("BAD_STATE".into(), "match not in lobby".into()));
                    } else if target_seat >= m.players.len() {
                        err = Some(("PLAYER_NOT_FOUND".into(), "seat not found".into()));
                    } else {
                        if let Some(owner_idx) = m.players.iter().position(|p| p.key == owner_key) {
                            let owner_player = &m.players[owner_idx];
                            owner_meta = Some((
                                u8::try_from(owner_idx).unwrap_or(0),
                                owner_player.team.as_u8(),
                                owner_player.user_id,
                            ));
                        } else {
                            err = Some(("OWNER_NOT_FOUND".into(), "owner seat missing".into()));
                        }

                        if err.is_none() {
                            let player = m.players[target_seat].clone();
                            if player.key == owner_key {
                                err = Some(("BAD_REQUEST".into(), "cannot kick yourself".into()));
                            } else {
                                let session_id = s.sessions.iter().find_map(|(sid, sess)| {
                                    (sess.player_key.as_deref() == Some(player.key.as_str()))
                                        .then_some(*sid)
                                });
                                target = Some((
                                    player.key.clone(),
                                    player.user_id,
                                    session_id,
                                    u8::try_from(target_seat).unwrap_or(0),
                                    player.team.as_u8(),
                                    player.name.clone(),
                                ));
                            }
                        }
                    }
                }

                if let Some((code, msg)) = err {
                    self.send_to(session_id, Self::err_out(id, code, msg)).await;
                    return;
                }

                let Some((
                    target_key,
                    target_user_id,
                    target_session,
                    target_seat_idx,
                    target_team_idx,
                    target_display_name,
                )) = target
                else {
                    self.send_to(
                        session_id,
                        Self::err_out(id, "PLAYER_NOT_FOUND", "seat not found"),
                    )
                    .await;
                    return;
                };

                let Some((owner_seat_idx, owner_team_idx, owner_user_id)) = owner_meta else {
                    self.send_to(
                        session_id,
                        Self::err_out(id, "OWNER_NOT_FOUND", "owner seat missing"),
                    )
                    .await;
                    return;
                };

                let now_ms = Self::now_ms();

                let sid = target_session.unwrap_or_else(Uuid::nil);
                let removed = self
                    .leave_match_internal(sid, &msid, &target_key, target_user_id, "owner_kick")
                    .await;
                if removed {
                    self.broadcast_lobby_match_remove(&msid).await;
                } else {
                    self.broadcast_lobby_match_upsert(&msid).await;
                }

                if let Some(target_sid) = target_session {
                    self.send_to(
                        target_sid,
                        WsOutMessage {
                            v: Default::default(),
                            msg: S2cMessage::MatchKicked(MatchKickedData {
                                match_id: msid.clone(),
                                reason: "owner_kick".into(),
                            }),
                            id: Default::default(),
                        },
                    )
                    .await;
                }

                self.emit_history(GameHistoryEvent::GameAction {
                    match_id: msid.clone(),
                    actor_seat_idx: Some(owner_seat_idx),
                    actor_team_idx: Some(owner_team_idx),
                    actor_user_id: owner_user_id,
                    ty: "match.kick".into(),
                    data: serde_json::json!({
                        "target_seat_idx": target_seat_idx,
                        "target_team_idx": target_team_idx,
                        "target_user_id": target_user_id,
                        "target_display_name": target_display_name,
                        "reason": "owner_kick",
                        "trigger": "owner_request",
                        "server_time_ms": now_ms,
                    }),
                });
            }

            C2sMessage::ChatJoin(d) => {
                self.join_room_internal(session_id, &d.room_id).await;
                self.emit_chat_snapshot_to(session_id, &d.room_id, id).await;
            }

            C2sMessage::ChatSay(d) => {
                let msg = self
                    .append_chat_message(&d.room_id, session_id, d.content)
                    .await;
                let Some(result) = msg else {
                    self.send_to(
                        session_id,
                        Self::err_out(id, "BAD_REQUEST", "unknown session"),
                    )
                    .await;
                    return;
                };

                if let Some(history) = result.history.clone() {
                    let message_json = serde_json::to_value(&result.message).unwrap_or_else(
                        |_| serde_json::json!({"error": "failed_to_serialize_chat_message" }),
                    );

                    self.emit_history(GameHistoryEvent::GameAction {
                        match_id: history.match_id,
                        actor_seat_idx: history.actor_seat_idx,
                        actor_team_idx: history.actor_team_idx,
                        actor_user_id: history.actor_user_id,
                        ty: "chat.message".into(),
                        data: serde_json::json!({
                            "room_id": d.room_id.clone(),
                            "message": message_json,
                        }),
                    });
                }

                let correlated = id.clone().map(|id| (session_id, id));
                self.broadcast_chat_message(&d.room_id, result.message, correlated)
                    .await;
            }
        }
    }

    fn seed_from_str(s: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        s.hash(&mut h);
        h.finish()
    }

    fn seed_for_hand(match_id: &str, hand_no: u32) -> u64 {
        if hand_no == 0 {
            Self::seed_from_str(match_id)
        } else {
            Self::seed_from_str(&format!("{match_id}:{hand_no}"))
        }
    }

    async fn send_to(&self, session_id: Uuid, msg: WsOutMessage) {
        let s = self.state.lock().await;
        if let Some(sess) = s.sessions.get(&session_id) {
            let _ = sess.tx.send(msg);
        }
    }

    async fn broadcast_to(&self, targets: &[Uuid], msg: &WsOutMessage) {
        let s = self.state.lock().await;
        for sid in targets {
            if let Some(sess) = s.sessions.get(sid) {
                let _ = sess.tx.send(msg.clone());
            }
        }
    }
    async fn emit_lobby_snapshot_to(&self, session_id: Uuid, id: Option<String>) {
        let matches = {
            let s = self.state.lock().await;
            s.matches
                .values()
                .map(Self::lobby_match_for)
                .collect::<Vec<_>>()
        };

        self.send_to(
            session_id,
            WsOutMessage {
                v: Default::default(),
                msg: S2cMessage::LobbySnapshot(LobbySnapshotData { matches }),
                id: id.into(),
            },
        )
        .await;
    }

    async fn broadcast_lobby_match_upsert(&self, match_id: &str) {
        let (targets, msg) = {
            let s = self.state.lock().await;
            let targets = s.sessions.keys().copied().collect::<Vec<_>>();
            let match_state = s.matches.get(match_id);

            let Some(match_state) = match_state else {
                // If the match disappeared, emit a remove instead.
                drop(s);
                return self.broadcast_lobby_match_remove(match_id).await;
            };

            let match_info = Self::lobby_match_for(match_state);

            (
                targets,
                WsOutMessage {
                    v: Default::default(),
                    msg: S2cMessage::LobbyMatchUpsert(LobbyMatchUpsertData { match_: match_info }),
                    id: Default::default(),
                },
            )
        };

        self.broadcast_to(&targets, &msg).await;
    }

    async fn broadcast_lobby_match_remove(&self, match_id: &str) {
        let (targets, msg) = {
            let s = self.state.lock().await;
            let targets = s.sessions.keys().copied().collect::<Vec<_>>();
            (
                targets,
                WsOutMessage {
                    v: Default::default(),
                    msg: S2cMessage::LobbyMatchRemove(LobbyMatchRemoveData {
                        match_id: match_id.to_string(),
                    }),
                    id: Default::default(),
                },
            )
        };

        self.broadcast_to(&targets, &msg).await;
    }

    fn lobby_match_for(m: &MatchState) -> LobbyMatch {
        let players = m
            .players
            .iter()
            .map(|p| PublicPlayer {
                name: p.name.clone(),
                team: p.team,
                ready: p.ready,
            })
            .collect::<Vec<_>>();

        let owner_seat_idx = m
            .players
            .iter()
            .position(|p| p.key == m.owner_key)
            .unwrap_or(0);

        let spectator_count = (m.participants.len()).saturating_sub(m.players.len()) as u32;

        LobbyMatch {
            id: m.match_id.clone(),
            options: m.options.clone(),
            phase: m.phase,
            players,
            owner_seat_idx: u8::try_from(owner_seat_idx).unwrap_or(0),
            spectator_count,
        }
    }

    fn public_match_for(m: &MatchState) -> PublicMatch {
        let players = m
            .players
            .iter()
            .map(|p| PublicPlayer {
                name: p.name.clone(),
                team: p.team,
                ready: p.ready,
            })
            .collect::<Vec<_>>();

        let owner_seat_idx = m
            .players
            .iter()
            .position(|p| p.key == m.owner_key)
            .unwrap_or(0);

        let spectator_count = (m.participants.len()).saturating_sub(m.players.len()) as u32;

        PublicMatch {
            id: m.match_id.clone(),
            options: m.options.clone(),
            phase: m.phase,
            players,
            owner_seat_idx: u8::try_from(owner_seat_idx).unwrap_or(0),
            pause_request: Maybe(m.pause_request.as_ref().map(|req| PublicPauseRequest {
                requested_by_team: req.requested_by_team,
                awaiting_team: req.awaiting_team,
                requested_by_seat_idx: req.requested_by_seat_idx,
                expires_at_ms: req.expires_at_ms,
            })),
            pending_unpause: Maybe(m.pending_unpause.as_ref().map(|pending| {
                PublicPendingUnpause {
                    resume_at_ms: pending.resume_at_ms,
                }
            })),
            spectator_count,
            team_points: m.team_points,
        }
    }

    fn begin_pending_unpause_locked(
        m: &mut MatchState,
        actor_team: TeamIdx,
        trigger: PendingUnpauseTrigger,
        now_ms: i64,
    ) -> Option<(Uuid, i64)> {
        if m.pending_unpause.is_some() {
            return None;
        }

        let opponent_team = opposing_team(actor_team);
        let opponents_connected = team_has_connected_player(m, opponent_team);
        let resume_at_ms = if opponents_connected {
            now_ms + UNPAUSE_COUNTDOWN_MS
        } else {
            now_ms
        };

        let token = Uuid::new_v4();
        m.pending_unpause = Some(PendingUnpauseState {
            token,
            resume_at_ms,
            trigger,
        });

        Some((token, resume_at_ms.saturating_sub(now_ms)))
    }

    async fn expire_pause_request(self, match_id: String, token: Uuid) {
        sleep(ms_to_duration(PAUSE_REQUEST_TIMEOUT_MS)).await;

        let mut broadcast = false;
        {
            let mut s = self.state.lock().await;
            if let Some(m) = s.matches.get_mut(&match_id) {
                if m.pause_request.as_ref().map(|req| req.token) == Some(token) {
                    m.pause_request = None;
                    broadcast = true;
                }
            }
        }

        if broadcast {
            self.emit_match_update_to_match(&match_id, None).await;
            self.broadcast_lobby_match_upsert(&match_id).await;
        }
    }

    async fn auto_unpause_after(self, match_id: String, token: Uuid, delay_ms: i64) {
        sleep(ms_to_duration(delay_ms)).await;
        self.trigger_auto_unpause(match_id, token).await;
    }

    async fn trigger_auto_unpause(&self, match_id: String, token: Uuid) {
        let now_ms = Self::now_ms();
        let mut spawn_resume: Option<(String, Uuid, i64)> = None;
        let mut immediate_resume: Option<Uuid> = None;

        {
            let mut s = self.state.lock().await;
            let Some(m) = s.matches.get_mut(&match_id) else {
                return;
            };

            let Some(auto) = m.auto_unpause.clone() else {
                return;
            };

            if auto.token != token {
                return;
            }

            if m.phase != MatchPhase::Paused {
                m.auto_unpause = None;
                m.pending_unpause = None;
                return;
            }

            let trigger = PendingUnpauseTrigger::AutoTimeout;
            if let Some((pending_token, delay_ms)) =
                Self::begin_pending_unpause_locked(m, auto.paused_by_team, trigger, now_ms)
            {
                m.auto_unpause = None;
                if delay_ms == 0 {
                    immediate_resume = Some(pending_token);
                } else {
                    spawn_resume = Some((match_id.clone(), pending_token, delay_ms));
                }
            } else {
                m.auto_unpause = None;
            }
        }

        if let Some(token) = immediate_resume {
            self.complete_pending_unpause(match_id.clone(), token).await;
        }

        if let Some((mid, token, delay_ms)) = spawn_resume {
            let rt = self.clone();
            tokio::spawn(async move {
                rt.await_pending_unpause(mid, token, delay_ms).await;
            });
        }
    }

    async fn await_pending_unpause(self, match_id: String, token: Uuid, delay_ms: i64) {
        sleep(ms_to_duration(delay_ms)).await;
        self.complete_pending_unpause(match_id, token).await;
    }

    async fn complete_pending_unpause(&self, match_id: String, token: Uuid) {
        let now_ms = Self::now_ms();
        let history_event: GameHistoryEvent;

        {
            let mut s = self.state.lock().await;
            let Some(m) = s.matches.get_mut(&match_id) else {
                return;
            };

            let Some(pending) = m.pending_unpause.clone() else {
                return;
            };

            if pending.token != token {
                return;
            }

            m.pending_unpause = None;
            m.pause_request = None;
            m.auto_unpause = None;
            if m.phase == MatchPhase::Paused {
                m.phase = MatchPhase::Started;
            }

            match pending.trigger {
                PendingUnpauseTrigger::Manual {
                    actor_user_id,
                    actor_seat_idx,
                    actor_team_idx,
                } => {
                    history_event = GameHistoryEvent::GameAction {
                        match_id: match_id.clone(),
                        actor_seat_idx: Some(actor_seat_idx),
                        actor_team_idx: Some(actor_team_idx),
                        actor_user_id,
                        ty: "match.resume".into(),
                        data: serde_json::json!({
                            "server_time_ms": now_ms,
                            "trigger": "manual",
                        }),
                    };
                }
                PendingUnpauseTrigger::AutoTimeout => {
                    history_event = GameHistoryEvent::GameAction {
                        match_id: match_id.clone(),
                        actor_seat_idx: None,
                        actor_team_idx: None,
                        actor_user_id: 0,
                        ty: "match.resume".into(),
                        data: serde_json::json!({
                            "server_time_ms": now_ms,
                            "trigger": "auto_timeout",
                        }),
                    };
                }
            }
        }

        self.emit_history(history_event);
        self.emit_match_update_to_match(&match_id, None).await;
        self.broadcast_lobby_match_upsert(&match_id).await;
    }

    fn private_player_for(m: &MatchState, recipient_key: Option<&str>) -> Option<PrivatePlayer> {
        let pkey = recipient_key?;
        let idx = m.players.iter().position(|p| p.key == pkey)?;

        // Important: we still need a private `me` view during the lobby phase so the
        // client can know `seat_idx` and enable ready/start UX.
        let Some(g) = m.game.as_ref() else {
            return Some(PrivatePlayer {
                seat_idx: u8::try_from(idx).expect("seat idx fits in u8"),
                hand: vec![],
                used: vec![],
                commands: vec![],
                has_flor: false,
                envido_points: 0,
            });
        };

        let mut cards_for_calc: Vec<String> = Vec::new();
        let hand = g.hands().get(idx).cloned().unwrap_or_default();
        let used = g.used_hands().get(idx).cloned().unwrap_or_default();
        cards_for_calc.extend(hand.iter().cloned());
        cards_for_calc.extend(used.iter().cloned());

        let commands = if (g.public.turn_seat_idx as usize) == idx {
            g.possible_commands_for_player(idx, m.options.flor)
        } else {
            vec![]
        };

        Some(PrivatePlayer {
            seat_idx: u8::try_from(idx).expect("seat idx fits in u8"),
            hand,
            used,
            commands,
            has_flor: trucoshi_game::has_flor(&cards_for_calc),
            envido_points: trucoshi_game::calculate_envido_points(&cards_for_calc),
        })
    }

    async fn emit_match_snapshot_to_match(
        &self,
        match_id: &str,
        correlated: Option<(Uuid, String)>,
    ) {
        let participants = {
            let s = self.state.lock().await;
            s.matches
                .get(match_id)
                .map(|m| m.participants.iter().copied().collect::<Vec<_>>())
                .unwrap_or_default()
        };

        for sid in participants {
            let id = correlated
                .as_ref()
                .and_then(|(corr_sid, corr_id)| (*corr_sid == sid).then(|| corr_id.clone()));
            self.emit_match_snapshot_to(sid, match_id, id).await;
        }
    }

    async fn emit_match_snapshot_to(&self, session_id: Uuid, match_id: &str, id: Option<String>) {
        let (m, pkey) = {
            let s = self.state.lock().await;
            let m = s.matches.get(match_id).cloned();
            let pkey = s
                .sessions
                .get(&session_id)
                .and_then(|sess| sess.player_key.clone());
            (m, pkey)
        };

        let Some(m) = m else {
            self.send_to(
                session_id,
                Self::err_out(id, "MATCH_NOT_FOUND", "match not found"),
            )
            .await;
            return;
        };

        let match_data = Self::public_match_for(&m);
        let me = Maybe(Self::private_player_for(&m, pkey.as_deref()));

        self.send_to(
            session_id,
            WsOutMessage {
                v: Default::default(),
                msg: S2cMessage::MatchSnapshot(MatchSnapshotData {
                    match_: match_data,
                    me,
                }),
                id: id.clone().into(),
            },
        )
        .await;

        // If gameplay is running, also send a gameplay snapshot.
        //
        // IMPORTANT (v2): echo correlation id when the snapshot was triggered by a
        // `*.snapshot.get` request.
        if let Some(g) = m.game.as_ref() {
            self.send_to(
                session_id,
                WsOutMessage {
                    v: Default::default(),
                    msg: S2cMessage::GameSnapshot(GameSnapshotData {
                        match_id: match_id.to_string(),
                        game: g.public.clone().into(),
                    }),
                    id: id.clone().into(),
                },
            )
            .await;
        }
    }

    async fn emit_game_snapshot_to(&self, session_id: Uuid, match_id: &str, id: Option<String>) {
        let game = {
            let s = self.state.lock().await;
            s.matches
                .get(match_id)
                .and_then(|m| m.game.as_ref().map(|g| g.public.clone().into()))
        };

        let Some(game) = game else {
            self.send_to(session_id, Self::err_out(id, "NO_GAME", "game not started"))
                .await;
            return;
        };

        self.send_to(
            session_id,
            WsOutMessage {
                v: Default::default(),
                msg: S2cMessage::GameSnapshot(GameSnapshotData {
                    match_id: match_id.to_string(),
                    game,
                }),
                id: id.into(),
            },
        )
        .await;
    }

    async fn emit_match_update_to_match(&self, match_id: &str, correlated: Option<(Uuid, String)>) {
        let participants = {
            let s = self.state.lock().await;
            s.matches
                .get(match_id)
                .map(|m| m.participants.iter().copied().collect::<Vec<_>>())
                .unwrap_or_default()
        };

        for sid in participants {
            let id = correlated
                .as_ref()
                .and_then(|(corr_sid, corr_id)| (*corr_sid == sid).then(|| corr_id.clone()));
            self.emit_match_update_to(sid, match_id, id).await;
        }
    }

    async fn emit_match_update_to(&self, session_id: Uuid, match_id: &str, id: Option<String>) {
        let (m, pkey) = {
            let s = self.state.lock().await;
            let Some(m) = s.matches.get(match_id) else {
                return;
            };
            let pkey = s
                .sessions
                .get(&session_id)
                .and_then(|sess| sess.player_key.clone());
            (m.clone(), pkey)
        };

        let match_data = Self::public_match_for(&m);
        let me = Maybe(Self::private_player_for(&m, pkey.as_deref()));

        self.send_to(
            session_id,
            WsOutMessage {
                v: Default::default(),
                msg: S2cMessage::MatchUpdate(MatchUpdateData {
                    match_: match_data,
                    me,
                }),
                id: id.into(),
            },
        )
        .await;
    }

    async fn broadcast_game_update_to_match(
        &self,
        match_id: &str,
        correlated: Option<(Uuid, String)>,
    ) {
        let (participants, game) = {
            let s = self.state.lock().await;
            let participants = s
                .matches
                .get(match_id)
                .map(|m| m.participants.iter().copied().collect::<Vec<_>>())
                .unwrap_or_default();
            let game: Option<crate::protocol::ws::v2::schema::PublicGameState> = s
                .matches
                .get(match_id)
                .and_then(|m| m.game.as_ref().map(|g| g.public.clone().into()));
            (participants, game)
        };

        let Some(game) = game else {
            return;
        };

        for sid in participants {
            let id = correlated
                .as_ref()
                .and_then(|(corr_sid, corr_id)| (*corr_sid == sid).then(|| corr_id.clone()));

            self.send_to(
                sid,
                WsOutMessage {
                    v: Default::default(),
                    msg: S2cMessage::GameUpdate(GameUpdateData {
                        match_id: match_id.to_string(),
                        game: game.clone(),
                    }),
                    id: id.into(),
                },
            )
            .await;
        }
    }

    async fn join_match_internal(
        &self,
        session_id: Uuid,
        match_id: &str,
        name: String,
        requested_team: Option<TeamIdx>,
        user_id: i64,
    ) -> Result<JoinResult, (String, String)> {
        let mut s = self.state.lock().await;
        let now_ms = Self::now_ms();

        if !s.matches.contains_key(match_id) {
            return Err(("MATCH_NOT_FOUND".into(), "match not found".into()));
        }

        let reconnected_key = {
            let m = s.matches.get_mut(match_id).expect("match exists");
            if let Some(idx) = m.players.iter().position(|p| p.user_id == user_id) {
                let key = m.players[idx].key.clone();

                if let Some(p) = m.players.get_mut(idx) {
                    p.name = name.clone();
                    p.ready = false;
                    p.last_active_ms = now_ms;
                    p.disconnected_at_ms = None;
                }

                m.participants.insert(session_id);
                Some(key)
            } else {
                None
            }
        };

        if let Some(key) = reconnected_key {
            if let Some(sess) = s.sessions.get_mut(&session_id) {
                sess.active_match_id = Some(match_id.to_string());
                sess.player_key = Some(key);
            }

            return Ok(JoinResult::Reconnected);
        }

        {
            let m = s.matches.get_mut(match_id).expect("match exists");

            if m.phase != MatchPhase::Lobby {
                return Err(("MATCH_NOT_JOINABLE".into(), "match already started".into()));
            }

            if m.players.len() >= (m.options.max_players as usize) {
                return Err(("MATCH_FULL".into(), "match is full".into()));
            }

            let key = Uuid::new_v4().to_string();
            let team_capacity = (m.options.max_players / 2) as usize;
            let team_0 = m
                .players
                .iter()
                .filter(|p| p.team == TeamIdx::TEAM_0)
                .count();
            let team_1 = m
                .players
                .iter()
                .filter(|p| p.team == TeamIdx::TEAM_1)
                .count();

            let team: TeamIdx = if let Some(t) = requested_team {
                match t {
                    t if t == TeamIdx::TEAM_0 => {
                        if team_0 >= team_capacity {
                            return Err(("TEAM_FULL".into(), "team 0 is full".into()));
                        }
                        TeamIdx::TEAM_0
                    }
                    t if t == TeamIdx::TEAM_1 => {
                        if team_1 >= team_capacity {
                            return Err(("TEAM_FULL".into(), "team 1 is full".into()));
                        }
                        TeamIdx::TEAM_1
                    }
                    _ => return Err(("BAD_REQUEST".into(), "invalid team".into())),
                }
            } else {
                if team_0 <= team_1 {
                    if team_0 < team_capacity {
                        TeamIdx::TEAM_0
                    } else if team_1 < team_capacity {
                        TeamIdx::TEAM_1
                    } else {
                        return Err(("MATCH_FULL".into(), "no free slots".into()));
                    }
                } else if team_1 < team_capacity {
                    TeamIdx::TEAM_1
                } else if team_0 < team_capacity {
                    TeamIdx::TEAM_0
                } else {
                    return Err(("MATCH_FULL".into(), "no free slots".into()));
                }
            };

            m.players.push(PlayerState {
                key: key.clone(),
                user_id,
                name,
                team,
                ready: false,
                last_active_ms: now_ms,
                disconnected_at_ms: None,
            });

            m.participants.insert(session_id);

            if let Some(sess) = s.sessions.get_mut(&session_id) {
                sess.active_match_id = Some(match_id.to_string());
                sess.player_key = Some(key);
            }

            return Ok(JoinResult::NewSeat);
        }
    }

    /// Returns `true` if the match was removed (became empty).
    async fn leave_match_internal(
        &self,
        session_id: Uuid,
        match_id: &str,
        key: &str,
        user_id: i64,
        reason: &str,
    ) -> bool {
        let mut removed = false;
        let mut history_left: Option<GameHistoryEvent> = None;
        let mut history_finish: Option<[u8; 2]> = None;
        {
            let mut s = self.state.lock().await;

            if let Some(sess) = s.sessions.get_mut(&session_id) {
                sess.active_match_id = None;
                sess.player_key = None;
            }

            if let Some(m) = s.matches.get_mut(match_id) {
                m.participants.remove(&session_id);

                // Capture the leaving seat/team/name before we remove them.
                if let Some(idx) = m.players.iter().position(|p| p.key == key) {
                    if let Some(p) = m.players.get(idx) {
                        history_left = Some(GameHistoryEvent::PlayerLeft {
                            match_id: match_id.to_string(),
                            seat_idx: u8::try_from(idx).unwrap_or(0),
                            team_idx: p.team.as_u8(),
                            user_id,
                            display_name: p.name.clone(),
                            reason: reason.to_string(),
                        });
                    }
                }

                m.players.retain(|p| p.key != key);

                // If the owner left, transfer ownership to the first remaining player.
                if m.owner_key == key {
                    if let Some(new_owner) = m.players.first() {
                        m.owner_key = new_owner.key.clone();
                    }
                }

                // If someone leaves mid-match, end the match.
                //
                // This keeps the realtime state consistent without attempting mid-hand
                // substitutions/rebalancing.
                if m.phase != MatchPhase::Lobby {
                    // Avoid double-emitting a finish event.
                    if m.phase != MatchPhase::Finished {
                        history_finish = Some(m.team_points);
                    }

                    m.phase = MatchPhase::Finished;
                    m.reset_pause_state();
                    m.game = None;
                    m.pending_game = None;
                }

                // Remove match if empty.
                if m.players.is_empty() {
                    s.matches.remove(match_id);
                    removed = true;
                }
            }
        }

        if let Some(ev) = history_left.take() {
            self.emit_history(ev);
        }

        if let Some(team_points) = history_finish.take() {
            self.emit_history(GameHistoryEvent::MatchFinished {
                match_id: match_id.to_string(),
                team_points,
                reason: "player_left".into(),
            });
        }

        self.leave_room_internal(session_id, match_id).await;
        if !removed {
            self.emit_match_update_to_match(match_id, None).await;
        }
        removed
    }

    /// Detach a spectator/watch session from a match.
    ///
    /// Unlike `leave_match_internal`, this does not remove a player seat.
    async fn leave_match_watch_internal(
        &self,
        session_id: Uuid,
        match_id: &str,
        user_id: i64,
        reason: &str,
    ) {
        let mut removed = false;
        {
            let mut s = self.state.lock().await;

            if let Some(m) = s.matches.get_mut(match_id) {
                removed = m.participants.remove(&session_id);
            }

            if let Some(sess) = s.sessions.get_mut(&session_id) {
                if sess.active_match_id.as_deref() == Some(match_id) {
                    sess.active_match_id = None;
                }
                sess.player_key = None;
            }
        }

        if removed {
            self.emit_history(GameHistoryEvent::SpectatorLeft {
                match_id: match_id.to_string(),
                user_id,
                reason: reason.to_string(),
            });
        }

        // Match chat room id == match_id.
        self.leave_room_internal(session_id, match_id).await;
    }

    async fn mark_session_active(&self, session_id: Uuid) {
        let now = Self::now_ms();
        let (active_match_id, player_key) = {
            let mut s = self.state.lock().await;
            if let Some(sess) = s.sessions.get_mut(&session_id) {
                sess.last_active_ms = now;
                (sess.active_match_id.clone(), sess.player_key.clone())
            } else {
                return;
            }
        };

        if let (Some(match_id), Some(player_key)) = (active_match_id, player_key) {
            let mut s = self.state.lock().await;
            if let Some(m) = s.matches.get_mut(&match_id) {
                if let Some(p) = m.players.iter_mut().find(|p| p.key == player_key) {
                    p.last_active_ms = now;
                    p.disconnected_at_ms = None;
                }
            }
        }
    }

    async fn handle_player_disconnect(&self, session_id: Uuid, match_id: &str, player_key: &str) {
        let now_ms = Self::now_ms();
        {
            let mut s = self.state.lock().await;
            if let Some(m) = s.matches.get_mut(match_id) {
                m.participants.remove(&session_id);
                if let Some(p) = m.players.iter_mut().find(|p| p.key == player_key) {
                    p.ready = false;
                    p.disconnected_at_ms.get_or_insert(now_ms);
                }
            }
        }

        self.emit_match_update_to_match(match_id, None).await;
        self.broadcast_lobby_match_upsert(match_id).await;
        self.sweep_inactive_players().await;
    }

    async fn sweep_inactive_players(&self) {
        struct PendingRemoval {
            match_id: String,
            player_key: String,
            user_id: i64,
            seat_idx: u8,
            team_idx: u8,
            display_name: String,
            session_id: Option<Uuid>,
            reason: String,
        }

        let now = Self::now_ms();
        let mut removals: Vec<PendingRemoval> = Vec::new();

        {
            let s = self.state.lock().await;
            for (match_id, m) in s.matches.iter() {
                let abandon_ms = m.options.abandon_time_ms.max(1);
                let lobby_deadline = abandon_ms.saturating_add(m.options.reconnect_grace_ms.max(1));

                for (idx, player) in m.players.iter().enumerate() {
                    let seat_idx = u8::try_from(idx).unwrap_or(0);
                    if let Some(disconnected_at) = player.disconnected_at_ms {
                        if now.saturating_sub(disconnected_at) >= abandon_ms {
                            let session_id = s.sessions.iter().find_map(|(sid, sess)| {
                                (sess.player_key.as_deref() == Some(player.key.as_str()))
                                    .then_some(*sid)
                            });
                            removals.push(PendingRemoval {
                                match_id: match_id.clone(),
                                player_key: player.key.clone(),
                                user_id: player.user_id,
                                seat_idx,
                                team_idx: player.team.as_u8(),
                                display_name: player.name.clone(),
                                session_id,
                                reason: "disconnect_timeout".into(),
                            });
                        }

                        continue;
                    }

                    if m.phase == MatchPhase::Lobby
                        && now.saturating_sub(player.last_active_ms) >= lobby_deadline
                    {
                        let session_id = s.sessions.iter().find_map(|(sid, sess)| {
                            (sess.player_key.as_deref() == Some(player.key.as_str()))
                                .then_some(*sid)
                        });
                        removals.push(PendingRemoval {
                            match_id: match_id.clone(),
                            player_key: player.key.clone(),
                            user_id: player.user_id,
                            seat_idx,
                            team_idx: player.team.as_u8(),
                            display_name: player.name.clone(),
                            session_id,
                            reason: "lobby_inactivity".into(),
                        });
                    }
                }
            }
        }

        for removal in removals {
            let PendingRemoval {
                match_id,
                player_key,
                user_id,
                seat_idx,
                team_idx,
                display_name,
                session_id,
                reason,
            } = removal;

            let removal_sid = if let Some(sid) = session_id {
                self.send_to(
                    sid,
                    WsOutMessage {
                        v: Default::default(),
                        msg: S2cMessage::MatchKicked(MatchKickedData {
                            match_id: match_id.clone(),
                            reason: reason.clone(),
                        }),
                        id: Default::default(),
                    },
                )
                .await;
                sid
            } else {
                Uuid::nil()
            };

            let removed = self
                .leave_match_internal(removal_sid, &match_id, &player_key, user_id, &reason)
                .await;

            let event_time = Self::now_ms();
            self.emit_history(GameHistoryEvent::GameAction {
                match_id: match_id.clone(),
                actor_seat_idx: None,
                actor_team_idx: None,
                actor_user_id: 0,
                ty: "match.kick".into(),
                data: serde_json::json!({
                    "target_seat_idx": seat_idx,
                    "target_team_idx": team_idx,
                    "target_user_id": user_id,
                    "target_display_name": display_name,
                    "reason": reason,
                    "trigger": "sweep",
                    "server_time_ms": event_time,
                }),
            });

            if removed {
                self.broadcast_lobby_match_remove(&match_id).await;
            } else {
                self.broadcast_lobby_match_upsert(&match_id).await;
            }
        }
    }

    async fn join_room_internal(&self, session_id: Uuid, room_id: &str) {
        let mut s = self.state.lock().await;

        if let Some(sess) = s.sessions.get_mut(&session_id) {
            sess.rooms.insert(room_id.to_string());
        }

        let room = s
            .rooms
            .entry(room_id.to_string())
            .or_insert_with(|| ChatRoomState {
                id: room_id.to_string(),
                messages: vec![],
                participants: HashSet::new(),
            });
        room.participants.insert(session_id);
    }

    async fn leave_room_internal(&self, session_id: Uuid, room_id: &str) {
        let mut s = self.state.lock().await;

        if let Some(sess) = s.sessions.get_mut(&session_id) {
            sess.rooms.remove(room_id);
        }

        if let Some(room) = s.rooms.get_mut(room_id) {
            room.participants.remove(&session_id);
        }

        let remove = s
            .rooms
            .get(room_id)
            .map(|r| r.participants.is_empty())
            .unwrap_or(false);
        if remove {
            s.rooms.remove(room_id);
        }
    }

    async fn append_chat_message(
        &self,
        room_id: &str,
        session_id: Uuid,
        content: String,
    ) -> Option<ChatAppendResult> {
        let mut s = self.state.lock().await;

        let (user, history) = {
            let sess = s.sessions.get(&session_id)?;
            let active_match_id = sess.active_match_id.clone();
            let player_key = sess.player_key.clone();
            let actor_user_id = sess.user_id;

            let (seat_idx, team, name) = active_match_id
                .as_deref()
                .and_then(|msid| s.matches.get(msid))
                .and_then(|m| {
                    let pk = player_key.as_deref()?;
                    let idx = m.players.iter().position(|p| p.key == pk)?;
                    let p = m.players.get(idx)?;
                    Some((
                        Maybe(Some(u8::try_from(idx).expect("seat idx fits in u8"))),
                        Maybe(Some(p.team)),
                        p.name.clone(),
                    ))
                })
                .unwrap_or((Maybe(None), Maybe(None), format!("User{}", actor_user_id)));

            let seat_opt: Option<u8> = seat_idx.clone().into();
            let team_opt: Option<TeamIdx> = team.clone().into();

            let history =
                if active_match_id.as_deref() == Some(room_id) && s.matches.contains_key(room_id) {
                    Some(ChatHistoryContext {
                        match_id: room_id.to_string(),
                        actor_seat_idx: seat_opt,
                        actor_team_idx: team_opt.map(|t| t.as_u8()),
                        actor_user_id,
                    })
                } else {
                    None
                };

            (
                PublicChatUser {
                    name,
                    seat_idx,
                    team,
                },
                history,
            )
        };

        let room = s
            .rooms
            .entry(room_id.to_string())
            .or_insert_with(|| ChatRoomState {
                id: room_id.to_string(),
                messages: vec![],
                participants: HashSet::new(),
            });

        let msg = PublicChatMessage {
            id: Uuid::new_v4().to_string(),
            date_ms: Self::now_ms(),
            user,
            system: false,
            content,
        };

        room.messages.push(msg.clone());

        // trim
        if room.messages.len() > 200 {
            let drain = room.messages.len().saturating_sub(200);
            room.messages.drain(0..drain);
        }

        Some(ChatAppendResult {
            message: msg,
            history,
        })
    }

    async fn emit_chat_snapshot_to(&self, session_id: Uuid, room_id: &str, id: Option<String>) {
        let room = {
            let s = self.state.lock().await;
            s.rooms.get(room_id).cloned().unwrap_or(ChatRoomState {
                id: room_id.to_string(),
                messages: vec![],
                participants: HashSet::new(),
            })
        };

        self.send_to(
            session_id,
            WsOutMessage {
                v: Default::default(),
                msg: S2cMessage::ChatSnapshot(ChatSnapshotData {
                    room: PublicChatRoom {
                        id: room.id,
                        messages: room.messages,
                    },
                }),
                id: id.into(),
            },
        )
        .await;
    }

    async fn broadcast_chat_message(
        &self,
        room_id: &str,
        message: PublicChatMessage,
        correlated: Option<(Uuid, String)>,
    ) {
        let participants = {
            let s = self.state.lock().await;
            s.rooms
                .get(room_id)
                .map(|r| r.participants.iter().copied().collect::<Vec<_>>())
                .unwrap_or_default()
        };

        for sid in participants {
            let id = correlated
                .as_ref()
                .and_then(|(corr_sid, corr_id)| (*corr_sid == sid).then(|| corr_id.clone()));

            self.send_to(
                sid,
                WsOutMessage {
                    v: Default::default(),
                    msg: S2cMessage::ChatMessage(ChatMessageData {
                        room_id: room_id.to_string(),
                        message: message.clone(),
                    }),
                    id: id.into(),
                },
            )
            .await;
        }
    }

    #[allow(dead_code)]
    async fn broadcast_chat_snapshot(&self, room_id: &str) {
        let (participants, room) = {
            let s = self.state.lock().await;
            let participants = s
                .rooms
                .get(room_id)
                .map(|r| r.participants.iter().copied().collect::<Vec<_>>())
                .unwrap_or_default();

            let room = s.rooms.get(room_id).cloned().unwrap_or(ChatRoomState {
                id: room_id.to_string(),
                messages: vec![],
                participants: HashSet::new(),
            });

            (participants, room)
        };

        let msg = WsOutMessage {
            v: Default::default(),
            msg: S2cMessage::ChatSnapshot(ChatSnapshotData {
                room: PublicChatRoom {
                    id: room.id,
                    messages: room.messages,
                },
            }),
            id: Default::default(),
        };

        self.broadcast_to(&participants, &msg).await;
    }

    async fn maybe_start_pending_hand(&self, match_id: &str, correlated: Option<(Uuid, String)>) {
        let should_broadcast = {
            let mut s = self.state.lock().await;
            let Some(m) = s.matches.get_mut(match_id) else {
                return;
            };

            if m.phase == MatchPhase::Finished {
                m.pending_game = None;
                return;
            }

            let Some(next) = m.pending_game.take() else {
                return;
            };

            m.game = Some(next);
            true
        };

        if should_broadcast {
            self.emit_match_update_to_match(match_id, correlated.clone())
                .await;
            self.broadcast_game_update_to_match(match_id, correlated)
                .await;
            self.broadcast_lobby_match_upsert(match_id).await;
        }
    }
}

#[cfg(test)]
mod e2e_smoke_tests {
    use super::*;
    use crate::history::GameHistoryEvent;
    use crate::protocol::ws::v2::messages::{
        MatchKickData, MatchOptionsSetData, MatchPauseVoteData, MatchWatchData,
    };
    use crate::protocol::ws::v2::schema::GameCommand;
    use crate::protocol::ws::v2::{
        ChatSayData, GamePlayCardData, GameSayData, MatchCreateData, MatchJoinData, MatchReadyData,
        MatchRefData, WsInMessage,
    };
    use tokio::sync::mpsc::error::TryRecvError;

    async fn add_session(
        rt: &Realtime,
        user_id: i64,
    ) -> (Uuid, tokio::sync::mpsc::UnboundedReceiver<WsOutMessage>) {
        let session_id = Uuid::new_v4();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<WsOutMessage>();

        let mut s = rt.state.lock().await;
        s.connections += 1;
        s.sessions.insert(
            session_id,
            SessionState {
                user_id,
                tx,
                active_match_id: None,
                player_key: None,
                rooms: std::collections::HashSet::new(),
                last_active_ms: Realtime::now_ms(),
            },
        );

        (session_id, rx)
    }

    fn drain_receiver(rx: &mut tokio::sync::mpsc::UnboundedReceiver<WsOutMessage>) {
        loop {
            match rx.try_recv() {
                Ok(_) => continue,
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }
    }

    async fn recv_error(
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<WsOutMessage>,
    ) -> Option<ErrorPayload> {
        for _ in 0..10 {
            match rx.recv().await {
                Some(msg) => {
                    if let S2cMessage::Error(err) = msg.msg {
                        return Some(err);
                    }
                }
                None => break,
            }
        }

        None
    }
    async fn run_match_to_completion(rt: &Realtime, owner: Uuid, player: Uuid) -> String {
        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("owner should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "p2".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        for (sid, ready) in [(owner, true), (player, true)] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        loop {
            let (phase, turn_session_id, hand_len) = {
                let s = rt.state.lock().await;
                let m = s
                    .matches
                    .get(&match_id)
                    .unwrap_or_else(|| panic!("match not found: {match_id}"));

                let phase = m.phase;
                if phase == MatchPhase::Finished {
                    (phase, owner, 0)
                } else {
                    let g = m.game.as_ref().expect("game state initialized");
                    let turn_seat_idx = g.public.turn_seat_idx as usize;
                    let key = m.players[turn_seat_idx].key.clone();
                    let turn_session_id = *s
                        .sessions
                        .iter()
                        .find_map(|(sid, sess)| {
                            (sess.player_key.as_deref() == Some(key.as_str())).then_some(sid)
                        })
                        .expect("session for current turn");
                    let hand_len = g.hands().get(turn_seat_idx).map(|h| h.len()).unwrap_or(0);
                    (phase, turn_session_id, hand_len)
                }
            };

            if phase == MatchPhase::Finished {
                break;
            }

            assert!(hand_len > 0, "turn player should have cards");

            rt.handle_message(
                turn_session_id,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::GamePlayCard(GamePlayCardData {
                        match_id: match_id.clone(),
                        card_idx: 0,
                    }),
                },
            )
            .await;
        }

        match_id
    }

    #[tokio::test]
    async fn e2e_gameplay_smoke_two_clients_full_hand_finishes_match() {
        let rt = Realtime::new();

        let (s1, _rx1) = add_session(&rt, -1).await;
        let (s2, _rx2) = add_session(&rt, -2).await;

        // Create a 2-player match that finishes after a single hand.
        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&s1)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        // Join as second client.
        rt.handle_message(
            s2,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "p2".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        // Ready both clients.
        for (sid, ready) in [(s1, true), (s2, true)] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready,
                    }),
                },
            )
            .await;
        }

        // Start (owner only).
        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        // Play out a full hand with two clients by always playing card_idx=0 on the current turn.
        let mut plays = 0;
        loop {
            let (phase, team_points, turn_session_id, turn_hand_len) = {
                let s = rt.state.lock().await;
                let m = s
                    .matches
                    .get(&match_id)
                    .unwrap_or_else(|| panic!("match not found: {match_id}"));

                let phase = m.phase;
                let team_points = m.team_points;

                if phase == MatchPhase::Finished {
                    (phase, team_points, s1, 0)
                } else {
                    let g = m.game.as_ref().expect("game state initialized after start");
                    let turn_seat_idx = g.public.turn_seat_idx as usize;

                    let key = m
                        .players
                        .get(turn_seat_idx)
                        .unwrap_or_else(|| panic!("missing player for seat {turn_seat_idx}"))
                        .key
                        .clone();

                    let turn_session_id = *s
                        .sessions
                        .iter()
                        .find_map(|(sid, sess)| {
                            (sess.player_key.as_deref() == Some(key.as_str())).then_some(sid)
                        })
                        .expect("expected a session for current turn player");

                    let hand_len = g.hands().get(turn_seat_idx).map(|h| h.len()).unwrap_or(0);

                    (phase, team_points, turn_session_id, hand_len)
                }
            };

            if phase == MatchPhase::Finished {
                assert!(
                    team_points[0] + team_points[1] > 0,
                    "expected points awarded"
                );
                break;
            }

            assert!(turn_hand_len > 0, "current turn player should have cards");

            plays += 1;
            assert!(
                plays <= 12,
                "expected match to finish quickly; plays={plays}"
            );

            rt.handle_message(
                turn_session_id,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::GamePlayCard(GamePlayCardData {
                        match_id: match_id.clone(),
                        card_idx: 0,
                    }),
                },
            )
            .await;
        }
    }

    #[tokio::test]
    async fn e2e_spectator_watch_omits_me_and_rejects_seat_only_actions() {
        use tokio::time::{Duration, timeout};

        let rt = Realtime::new();

        let (s1, _rx1) = add_session(&rt, -1).await;
        let (s2, _rx2) = add_session(&rt, -2).await;
        let (spec, mut rx_spec) = add_session(&rt, -3).await;

        // Create a 2-player match.
        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&s1)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        // Join as second client.
        rt.handle_message(
            s2,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "p2".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        // Ready both clients and start.
        for (sid, ready) in [(s1, true), (s2, true)] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        // The spectator session existed during match setup and may have received lobby broadcasts.
        // Drain them so the next assertions are about the watch flow.
        while rx_spec.try_recv().is_ok() {}

        // Spectator watches an in-progress match.
        rt.handle_message(
            spec,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchWatch(MatchWatchData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        // Expect a match snapshot (with `me` omitted) and a game snapshot.
        let mut saw_match_snapshot = false;
        let mut saw_game_snapshot = false;

        for _ in 0..20 {
            let out = timeout(Duration::from_millis(500), rx_spec.recv())
                .await
                .expect("expected spectator outbound message")
                .expect("rx open");

            match out.msg {
                S2cMessage::MatchSnapshot(MatchSnapshotData { me, .. }) => {
                    assert!(me.0.is_none(), "spectator match.snapshot must omit me");
                    saw_match_snapshot = true;
                }
                S2cMessage::GameSnapshot(_) => {
                    saw_game_snapshot = true;
                }
                _ => {}
            }

            if saw_match_snapshot && saw_game_snapshot {
                break;
            }
        }

        assert!(saw_match_snapshot, "expected match.snapshot for spectator");
        assert!(saw_game_snapshot, "expected game.snapshot for spectator");

        // The watch flow may also broadcast match/lobby updates (e.g. spectator_count).
        // Drain anything pending so the next assertion is about the seat-only action.
        while rx_spec.try_recv().is_ok() {}

        // Spectators should not be able to perform seat-only actions (e.g. play a card).
        rt.handle_message(
            spec,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::GamePlayCard(GamePlayCardData {
                    match_id: match_id.clone(),
                    card_idx: 0,
                }),
            },
        )
        .await;

        // Skip over any unrelated broadcasts and find the expected error.
        let mut saw_err = false;
        for _ in 0..10 {
            let out = timeout(Duration::from_millis(250), rx_spec.recv())
                .await
                .expect("expected spectator outbound message")
                .expect("rx open");

            match out.msg {
                S2cMessage::Error(ErrorPayload { code, .. }) => {
                    assert_eq!(code, "NOT_A_PLAYER");
                    saw_err = true;
                    break;
                }
                _ => {}
            }
        }

        assert!(
            saw_err,
            "expected NOT_A_PLAYER error for spectator seat-only action"
        );
    }

    #[tokio::test]
    async fn e2e_spectator_watch_updates_spectator_count() {
        use tokio::time::{Duration, timeout};

        let rt = Realtime::new();

        let (s1, mut rx1) = add_session(&rt, -1).await;
        let (s2, _rx2) = add_session(&rt, -2).await;
        let (spec, _rx_spec) = add_session(&rt, -3).await;

        // Create a 2-player match.
        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&s1)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            s2,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "p2".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        // Drain the creator's queue so assertions are about the watch flow.
        while rx1.try_recv().is_ok() {}

        // Spectator watches the match.
        rt.handle_message(
            spec,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchWatch(MatchWatchData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        // Creator should observe the updated spectator_count via match.update and/or lobby.upsert.
        let mut saw_match_update = false;
        let mut saw_lobby_upsert = false;

        for _ in 0..30 {
            let out = timeout(Duration::from_millis(500), rx1.recv())
                .await
                .expect("expected creator outbound message")
                .expect("rx open");

            match out.msg {
                S2cMessage::MatchUpdate(MatchUpdateData { match_, .. }) => {
                    assert_eq!(match_.id, match_id);
                    assert_eq!(match_.spectator_count, 1);
                    saw_match_update = true;
                }
                S2cMessage::LobbyMatchUpsert(LobbyMatchUpsertData { match_ }) => {
                    if match_.id == match_id {
                        assert_eq!(match_.spectator_count, 1);
                        saw_lobby_upsert = true;
                    }
                }
                _ => {}
            }

            if saw_match_update && saw_lobby_upsert {
                break;
            }
        }

        assert!(
            saw_match_update,
            "expected match.update with spectator_count"
        );
        assert!(
            saw_lobby_upsert,
            "expected lobby.match.upsert with spectator_count"
        );

        // Now have the spectator leave and ensure counts drop back to 0.
        while rx1.try_recv().is_ok() {}

        rt.handle_message(
            spec,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchLeave(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        let mut saw_zero = false;
        for _ in 0..30 {
            let out = timeout(Duration::from_millis(500), rx1.recv())
                .await
                .expect("expected creator outbound message")
                .expect("rx open");

            match out.msg {
                S2cMessage::MatchUpdate(MatchUpdateData { match_, .. }) => {
                    if match_.id == match_id {
                        assert_eq!(match_.spectator_count, 0);
                        saw_zero = true;
                        break;
                    }
                }
                _ => {}
            }
        }

        assert!(
            saw_zero,
            "expected match.update reflecting spectator_count=0"
        );
    }

    #[tokio::test]
    async fn match_ready_is_rejected_after_match_start() {
        use tokio::time::{Duration, timeout};

        let rt = Realtime::new();

        let (s1, _rx1) = add_session(&rt, -1).await;
        let (s2, mut rx2) = add_session(&rt, -2).await;

        // Create + join a 2-player match.
        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&s1)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            s2,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "p2".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        // Ready both clients and start.
        for (sid, ready) in [(s1, true), (s2, true)] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        // Drain any prior broadcasts to s2.
        while rx2.try_recv().is_ok() {}

        // After the match is started, `match.ready` is no longer valid.
        rt.handle_message(
            s2,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_ready_after_start".to_string()).into(),
                msg: C2sMessage::MatchReady(MatchReadyData {
                    match_id: match_id.clone(),
                    ready: false,
                }),
            },
        )
        .await;

        let out = timeout(Duration::from_millis(250), rx2.recv())
            .await
            .expect("expected error response")
            .expect("rx open");

        assert_eq!(out.id.0.as_deref(), Some("corr_ready_after_start"));

        match out.msg {
            S2cMessage::Error(ErrorPayload { code, .. }) => {
                assert_eq!(code, "BAD_STATE");
            }
            other => panic!("expected error payload; got {other:?}"),
        }
    }

    #[tokio::test]
    async fn match_start_requires_all_players_ready() {
        use tokio::time::{Duration, timeout};

        let rt = Realtime::new();

        let (s1, mut rx1) = add_session(&rt, -1).await;
        let (s2, _rx2) = add_session(&rt, -2).await;

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&s1)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            s2,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "p2".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        // Only the owner is ready; the joiner is not.
        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchReady(MatchReadyData {
                    match_id: match_id.clone(),
                    ready: true,
                }),
            },
        )
        .await;

        // Drain any prior broadcasts to s1.
        while rx1.try_recv().is_ok() {}

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_start_not_ready".to_string()).into(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        let out = timeout(Duration::from_millis(250), rx1.recv())
            .await
            .expect("expected error response")
            .expect("rx open");

        assert_eq!(out.id.0.as_deref(), Some("corr_start_not_ready"));

        match out.msg {
            S2cMessage::Error(ErrorPayload { code, .. }) => {
                assert_eq!(code, "NOT_READY");
            }
            other => panic!("expected error payload; got {other:?}"),
        }

        let phase = {
            let s = rt.state.lock().await;
            s.matches.get(&match_id).expect("match exists").phase
        };

        assert_eq!(phase, MatchPhase::Lobby, "match should remain in lobby");
    }

    #[tokio::test]
    async fn match_start_rejects_unbalanced_teams() {
        use tokio::time::{Duration, timeout};

        let rt = Realtime::new();

        let (s1, mut rx1) = add_session(&rt, -1).await;
        let (s2, _rx2) = add_session(&rt, -2).await;

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 4,
                        flor: true,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&s1)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            s2,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "p2".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                }),
            },
        )
        .await;

        for sid in [s1, s2] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready: true,
                    }),
                },
            )
            .await;
        }

        // Drain any prior broadcasts to s1.
        while rx1.try_recv().is_ok() {}

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_start_unbalanced".to_string()).into(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        let out = timeout(Duration::from_millis(250), rx1.recv())
            .await
            .expect("expected error response")
            .expect("rx open");

        assert_eq!(out.id.0.as_deref(), Some("corr_start_unbalanced"));

        match out.msg {
            S2cMessage::Error(ErrorPayload { code, .. }) => {
                assert_eq!(code, "UNBALANCED_TEAMS");
            }
            other => panic!("expected error payload; got {other:?}"),
        }

        let phase = {
            let s = rt.state.lock().await;
            s.matches.get(&match_id).expect("match exists").phase
        };

        assert_eq!(phase, MatchPhase::Lobby, "match should remain in lobby");
    }

    #[tokio::test]
    async fn match_pause_and_resume_require_owner() {
        use tokio::time::{Duration, sleep, timeout};

        let rt = Realtime::new();
        let (owner, mut rx_owner) = add_session(&rt, 210).await;
        let (player, mut rx_player) = add_session(&rt, 211).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        for sid in [owner, player] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready: true,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        drain_receiver(&mut rx_player);

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_non_owner_pause".into()).into(),
                msg: C2sMessage::MatchPause(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        let out = timeout(Duration::from_millis(250), rx_player.recv())
            .await
            .expect("expected error response")
            .expect("rx open");

        assert_eq!(out.id.0.as_deref(), Some("corr_non_owner_pause"));

        match out.msg {
            S2cMessage::Error(ErrorPayload { code, .. }) => {
                assert_eq!(code, "NOT_OWNER");
            }
            other => panic!("expected error payload; got {other:?}"),
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPause(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            assert_eq!(m.phase, MatchPhase::Started);
            assert!(
                m.pause_request.is_some(),
                "expected pause request to be pending"
            );
        }

        drain_receiver(&mut rx_owner);

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_owner_vote".into()).into(),
                msg: C2sMessage::MatchPauseVote(MatchPauseVoteData {
                    match_id: match_id.clone(),
                    accept: true,
                }),
            },
        )
        .await;

        let out = timeout(Duration::from_millis(250), rx_owner.recv())
            .await
            .expect("expected error response")
            .expect("rx open");

        assert_eq!(out.id.0.as_deref(), Some("corr_owner_vote"));

        match out.msg {
            S2cMessage::Error(ErrorPayload { code, .. }) => {
                assert_eq!(code, "NOT_PENDING_TEAM");
            }
            other => panic!("expected error payload; got {other:?}"),
        }

        // Decline the pending pause so we can request another one.
        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPauseVote(MatchPauseVoteData {
                    match_id: match_id.clone(),
                    accept: false,
                }),
            },
        )
        .await;

        {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            assert!(
                m.pause_request.is_none(),
                "decline should clear pause request"
            );
            assert_eq!(m.phase, MatchPhase::Started);
        }

        // Request again and accept it this time.
        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPause(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPauseVote(MatchPauseVoteData {
                    match_id: match_id.clone(),
                    accept: true,
                }),
            },
        )
        .await;

        {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            assert_eq!(m.phase, MatchPhase::Paused);
            assert!(m.pause_request.is_none());
        }

        drain_receiver(&mut rx_player);

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_non_owner_resume".into()).into(),
                msg: C2sMessage::MatchResume(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        let out = timeout(Duration::from_millis(250), rx_player.recv())
            .await
            .expect("expected error response")
            .expect("rx open");

        assert_eq!(out.id.0.as_deref(), Some("corr_non_owner_resume"));

        match out.msg {
            S2cMessage::Error(ErrorPayload { code, .. }) => {
                assert_eq!(code, "NOT_OWNER");
            }
            other => panic!("expected error payload; got {other:?}"),
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchResume(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        sleep(Duration::from_millis(
            (super::UNPAUSE_COUNTDOWN_MS + 50) as u64,
        ))
        .await;

        {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            assert_eq!(m.phase, MatchPhase::Started);
            assert!(m.pending_unpause.is_none());
        }
    }

    #[tokio::test]
    async fn match_pause_blocks_gameplay_until_resume() {
        use tokio::time::{Duration, sleep, timeout};

        let rt = Realtime::new();
        let (owner, mut rx_owner) = add_session(&rt, 212).await;
        let (player, mut rx_player) = add_session(&rt, 213).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        for sid in [owner, player] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready: true,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPause(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPauseVote(MatchPauseVoteData {
                    match_id: match_id.clone(),
                    accept: true,
                }),
            },
        )
        .await;

        drain_receiver(&mut rx_player);

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::GamePlayCard(GamePlayCardData {
                    match_id: match_id.clone(),
                    card_idx: 0,
                }),
            },
        )
        .await;

        let err = timeout(Duration::from_millis(250), recv_error(&mut rx_player))
            .await
            .expect("expected response while paused");

        let err = err.expect("expected an error payload");
        assert_eq!(err.code, "MATCH_PAUSED");

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchResume(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        sleep(Duration::from_millis(
            (super::UNPAUSE_COUNTDOWN_MS + 50) as u64,
        ))
        .await;

        let (turn_session_id, turn_seat_idx, turn_hand_before) = {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            assert_eq!(m.phase, MatchPhase::Started);
            let g = m.game.as_ref().expect("game state initialized");
            let seat_idx = g.public.turn_seat_idx as usize;
            let hand_len = g.hands().get(seat_idx).map(|h| h.len()).unwrap_or(0);
            let key = m
                .players
                .get(seat_idx)
                .expect("player for seat")
                .key
                .clone();
            let turn_session_id = *s
                .sessions
                .iter()
                .find_map(|(sid, sess)| {
                    (sess.player_key.as_deref() == Some(key.as_str())).then_some(sid)
                })
                .expect("session for current turn");
            (turn_session_id, seat_idx, hand_len)
        };

        assert!(turn_hand_before > 0, "turn player should still have cards");

        let rx_turn = if turn_session_id == owner {
            &mut rx_owner
        } else if turn_session_id == player {
            &mut rx_player
        } else {
            panic!("unexpected turn session id");
        };

        drain_receiver(rx_turn);

        rt.handle_message(
            turn_session_id,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::GamePlayCard(GamePlayCardData {
                    match_id: match_id.clone(),
                    card_idx: 0,
                }),
            },
        )
        .await;

        if let Ok(maybe_err) = timeout(Duration::from_millis(250), recv_error(rx_turn)).await {
            if let Some(err) = maybe_err {
                assert_ne!(err.code, "MATCH_PAUSED");
            }
        }

        let hand_len_after = {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            let g = m.game.as_ref().expect("game state initialized");
            g.hands().get(turn_seat_idx).map(|h| h.len()).unwrap_or(0)
        };

        assert_eq!(hand_len_after + 1, turn_hand_before);
    }

    #[tokio::test]
    async fn match_pause_resume_emit_history_events() {
        use tokio::time::{Duration, sleep, timeout};

        let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
        let rt = Realtime::new_with_history(history_tx);
        let (owner, _rx_owner) = add_session(&rt, 214).await;
        let (player, _rx_player) = add_session(&rt, 215).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        for sid in [owner, player] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready: true,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPause(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPauseVote(MatchPauseVoteData {
                    match_id: match_id.clone(),
                    accept: true,
                }),
            },
        )
        .await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchResume(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        sleep(Duration::from_millis(
            (super::UNPAUSE_COUNTDOWN_MS + 50) as u64,
        ))
        .await;

        let mut saw_pause = false;
        let mut saw_resume = false;

        for _ in 0..40 {
            let event = timeout(Duration::from_millis(750), history_rx.recv())
                .await
                .expect("history channel event")
                .expect("history channel open");

            if let GameHistoryEvent::GameAction {
                ty,
                actor_user_id,
                actor_seat_idx,
                actor_team_idx,
                data,
                ..
            } = event
            {
                match ty.as_str() {
                    "match.pause" => {
                        assert_eq!(actor_user_id, 214);
                        assert_eq!(actor_seat_idx, Some(0));
                        assert_eq!(actor_team_idx, Some(0));
                        assert_eq!(
                            data.get("requires_vote").and_then(|v| v.as_bool()),
                            Some(true)
                        );
                        assert!(data.get("server_time_ms").is_some());
                        saw_pause = true;
                    }
                    "match.resume" => {
                        // Manual resume is recorded after the countdown completes.
                        assert_eq!(actor_user_id, 214);
                        assert_eq!(actor_seat_idx, Some(0));
                        assert_eq!(actor_team_idx, Some(0));
                        assert_eq!(data.get("trigger").and_then(|v| v.as_str()), Some("manual"));
                        assert!(data.get("server_time_ms").is_some());
                        saw_resume = true;
                    }
                    _ => {}
                }
            }

            if saw_pause && saw_resume {
                break;
            }
        }

        assert!(saw_pause, "expected match.pause history event");
        assert!(saw_resume, "expected match.resume history event");
    }

    #[tokio::test]
    async fn match_pause_request_expires_without_vote() {
        use tokio::time::{Duration, sleep, timeout};

        let rt = Realtime::new();
        let (owner, _rx_owner) = add_session(&rt, 216).await;
        let (player, mut rx_player) = add_session(&rt, 217).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        for sid in [owner, player] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready: true,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPause(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            assert!(m.pause_request.is_some());
        }

        sleep(Duration::from_millis(
            (super::PAUSE_REQUEST_TIMEOUT_MS + 50) as u64,
        ))
        .await;

        {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            assert!(m.pause_request.is_none());
            assert_eq!(m.phase, MatchPhase::Started);
        }

        drain_receiver(&mut rx_player);

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_vote_expired".into()).into(),
                msg: C2sMessage::MatchPauseVote(MatchPauseVoteData {
                    match_id: match_id.clone(),
                    accept: true,
                }),
            },
        )
        .await;

        let err = timeout(Duration::from_millis(250), rx_player.recv())
            .await
            .expect("expected error after expiration")
            .expect("rx open");

        assert_eq!(err.id.0.as_deref(), Some("corr_vote_expired"));

        match err.msg {
            S2cMessage::Error(ErrorPayload { code, .. }) => {
                assert_eq!(code, "NO_PAUSE_REQUEST");
            }
            other => panic!("expected error payload; got {other:?}"),
        }
    }

    #[tokio::test]
    async fn match_pause_auto_unpause_emits_history_event() {
        use tokio::time::{Duration, sleep};

        let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
        let rt = Realtime::new_with_history(history_tx);
        let (owner, _rx_owner) = add_session(&rt, 218).await;
        let (player, _rx_player) = add_session(&rt, 219).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        abandon_time_ms: 25,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        for sid in [owner, player] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready: true,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPause(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchPauseVote(MatchPauseVoteData {
                    match_id: match_id.clone(),
                    accept: true,
                }),
            },
        )
        .await;

        let wait_ms = 25 + super::UNPAUSE_COUNTDOWN_MS + 100;
        sleep(Duration::from_millis(wait_ms as u64)).await;

        let mut saw_pause = false;
        let mut saw_auto_resume = false;

        while let Ok(event) = history_rx.try_recv() {
            if let GameHistoryEvent::GameAction {
                ty,
                actor_user_id,
                actor_seat_idx,
                actor_team_idx,
                data,
                ..
            } = event
            {
                match ty.as_str() {
                    "match.pause" => {
                        saw_pause = true;
                        assert_eq!(
                            data.get("requires_vote").and_then(|v| v.as_bool()),
                            Some(true)
                        );
                    }
                    "match.resume" => {
                        saw_auto_resume = true;
                        assert_eq!(actor_user_id, 0);
                        assert_eq!(actor_seat_idx, None);
                        assert_eq!(actor_team_idx, None);
                        assert_eq!(
                            data.get("trigger").and_then(|v| v.as_str()),
                            Some("auto_timeout")
                        );
                    }
                    _ => {}
                }
            }
        }

        assert!(saw_pause, "expected match.pause history event");
        assert!(saw_auto_resume, "expected auto match.resume event");
    }

    #[tokio::test]
    async fn owner_can_update_match_options_in_lobby() {
        use tokio::time::{Duration, timeout};

        let rt = Realtime::new();
        let (owner, mut rx_owner) = add_session(&rt, 600).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 6,
                        match_points: 12,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        drain_receiver(&mut rx_owner);

        let updated_options = MatchOptions {
            max_players: 4,
            flor: false,
            match_points: 12,
            falta_envido: 1,
            turn_time_ms: 45_000,
            abandon_time_ms: 90_000,
            reconnect_grace_ms: 2_500,
        };

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_set_options".into()).into(),
                msg: C2sMessage::MatchOptionsSet(MatchOptionsSetData {
                    match_id: match_id.clone(),
                    options: updated_options,
                }),
            },
        )
        .await;

        let msg = timeout(Duration::from_millis(250), rx_owner.recv())
            .await
            .expect("expected match update")
            .expect("ws channel open");

        assert_eq!(msg.id.0.as_deref(), Some("corr_set_options"));

        match msg.msg {
            S2cMessage::MatchUpdate(data) => {
                assert_eq!(data.match_.options, updated_options);
            }
            other => panic!("expected match update; got {other:?}"),
        }

        {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            assert_eq!(m.options, updated_options);
        }
    }

    #[tokio::test]
    async fn match_options_set_rejects_non_owner_and_non_lobby() {
        use tokio::time::{Duration, timeout};

        let rt = Realtime::new();
        let (owner, mut rx_owner) = add_session(&rt, 601).await;
        let (player, mut rx_player) = add_session(&rt, 602).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 6,
                        match_points: 9,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        drain_receiver(&mut rx_owner);
        drain_receiver(&mut rx_player);

        let requested_options = MatchOptions {
            max_players: 6,
            match_points: 11,
            turn_time_ms: 30_000,
            ..MatchOptions::default()
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_non_owner".into()).into(),
                msg: C2sMessage::MatchOptionsSet(MatchOptionsSetData {
                    match_id: match_id.clone(),
                    options: requested_options,
                }),
            },
        )
        .await;

        let msg = timeout(Duration::from_millis(250), rx_player.recv())
            .await
            .expect("expected error")
            .expect("ws channel open");

        assert_eq!(msg.id.0.as_deref(), Some("corr_non_owner"));

        match msg.msg {
            S2cMessage::Error(err) => assert_eq!(err.code, "NOT_OWNER"),
            other => panic!("expected error; got {other:?}"),
        }

        for sid in [owner, player] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready: true,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        drain_receiver(&mut rx_owner);

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_bad_state".into()).into(),
                msg: C2sMessage::MatchOptionsSet(MatchOptionsSetData {
                    match_id: match_id.clone(),
                    options: requested_options,
                }),
            },
        )
        .await;

        let msg = timeout(Duration::from_millis(250), rx_owner.recv())
            .await
            .expect("expected error")
            .expect("ws channel open");

        assert_eq!(msg.id.0.as_deref(), Some("corr_bad_state"));

        match msg.msg {
            S2cMessage::Error(err) => assert_eq!(err.code, "BAD_STATE"),
            other => panic!("expected error; got {other:?}"),
        }
    }

    #[tokio::test]
    async fn match_options_set_validates_max_players_and_team_sizes() {
        use tokio::time::{Duration, timeout};

        let rt = Realtime::new();
        let (owner, mut rx_owner) = add_session(&rt, 603).await;
        let (p2, _rx_p2) = add_session(&rt, 604).await;
        let (p3, _rx_p3) = add_session(&rt, 605).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 6,
                        match_points: 9,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        for (session_id, name) in [(p2, "p2"), (p3, "p3")] {
            rt.handle_message(
                session_id,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchJoin(MatchJoinData {
                        match_id: match_id.clone(),
                        name: name.to_string(),
                        team: Some(TeamIdx::TEAM_0).into(),
                    }),
                },
            )
            .await;
        }

        drain_receiver(&mut rx_owner);

        let too_small = MatchOptions {
            max_players: 2,
            ..MatchOptions::default()
        };

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_too_low".into()).into(),
                msg: C2sMessage::MatchOptionsSet(MatchOptionsSetData {
                    match_id: match_id.clone(),
                    options: too_small,
                }),
            },
        )
        .await;

        let msg = timeout(Duration::from_millis(250), rx_owner.recv())
            .await
            .expect("expected error")
            .expect("ws channel open");

        assert_eq!(msg.id.0.as_deref(), Some("corr_too_low"));

        match msg.msg {
            S2cMessage::Error(err) => assert_eq!(err.code, "MAX_PLAYERS_TOO_LOW"),
            other => panic!("expected error; got {other:?}"),
        }

        drain_receiver(&mut rx_owner);

        let too_many_on_team = MatchOptions {
            max_players: 4,
            ..MatchOptions::default()
        };

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Some("corr_team_too_large".into()).into(),
                msg: C2sMessage::MatchOptionsSet(MatchOptionsSetData {
                    match_id: match_id.clone(),
                    options: too_many_on_team,
                }),
            },
        )
        .await;

        let msg = timeout(Duration::from_millis(250), rx_owner.recv())
            .await
            .expect("expected error")
            .expect("ws channel open");

        assert_eq!(msg.id.0.as_deref(), Some("corr_team_too_large"));

        match msg.msg {
            S2cMessage::Error(err) => assert_eq!(err.code, "TEAM_TOO_LARGE"),
            other => panic!("expected error; got {other:?}"),
        }
    }

    #[tokio::test]
    async fn match_options_set_emits_history_event() {
        use tokio::time::{Duration, timeout};

        let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
        let rt = Realtime::new_with_history(history_tx);
        let (owner, _rx_owner) = add_session(&rt, 700).await;
        let (player, _rx_player) = add_session(&rt, 701).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 6,
                        match_points: 9,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        let updated_options = MatchOptions {
            max_players: 4,
            flor: false,
            match_points: 11,
            falta_envido: 1,
            turn_time_ms: 45_000,
            abandon_time_ms: 60_000,
            reconnect_grace_ms: 3_000,
        };

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchOptionsSet(MatchOptionsSetData {
                    match_id: match_id.clone(),
                    options: updated_options,
                }),
            },
        )
        .await;

        let mut saw_event = false;
        for _ in 0..40 {
            let event = timeout(Duration::from_millis(250), history_rx.recv())
                .await
                .expect("history event")
                .expect("history channel open");

            if let GameHistoryEvent::GameAction {
                ty,
                actor_user_id,
                actor_seat_idx,
                actor_team_idx,
                data,
                ..
            } = event
            {
                if ty == "match.options.set" {
                    assert_eq!(actor_user_id, 700);
                    assert_eq!(actor_seat_idx, Some(0));
                    assert_eq!(actor_team_idx, Some(0));
                    assert!(
                        data.get("server_time_ms")
                            .and_then(|v| v.as_i64())
                            .is_some(),
                        "server_time_ms missing"
                    );

                    let previous = data
                        .get("previous")
                        .and_then(|v| v.as_object())
                        .expect("previous options payload");
                    let updated = data
                        .get("updated")
                        .and_then(|v| v.as_object())
                        .expect("updated options payload");

                    assert_eq!(
                        previous.get("max_players").and_then(|v| v.as_u64()),
                        Some(6)
                    );
                    assert_eq!(updated.get("max_players").and_then(|v| v.as_u64()), Some(4));
                    assert_eq!(previous.get("flor").and_then(|v| v.as_bool()), Some(true));
                    assert_eq!(updated.get("flor").and_then(|v| v.as_bool()), Some(false));

                    saw_event = true;
                    break;
                }
            }
        }

        assert!(saw_event, "expected match.options.set history event");
    }

    #[tokio::test]
    async fn owner_can_kick_player_in_lobby() {
        use tokio::time::{Duration, timeout};

        let rt = Realtime::new();
        let (owner, _rx_owner) = add_session(&rt, 200).await;
        let (target, mut rx_target) = add_session(&rt, 201).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            target,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "target".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        drain_receiver(&mut rx_target);

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchKick(MatchKickData {
                    match_id: match_id.clone(),
                    seat_idx: 1,
                }),
            },
        )
        .await;

        let kicked = timeout(Duration::from_millis(250), async {
            loop {
                match rx_target.recv().await {
                    Some(msg) => match msg.msg {
                        S2cMessage::MatchKicked(data) => break data,
                        _ => continue,
                    },
                    None => panic!("target channel closed before kick event"),
                }
            }
        })
        .await
        .expect("expected kick event");

        assert_eq!(kicked.match_id, match_id);
        assert_eq!(kicked.reason, "owner_kick");

        {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            assert_eq!(m.players.len(), 1, "target should be removed");
        }
    }

    #[tokio::test]
    async fn owner_kick_emits_history_event() {
        use crate::history::GameHistoryEvent;
        use tokio::time::{Duration, timeout};

        let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
        let rt = Realtime::new_with_history(history_tx);
        let (owner, _rx_owner) = add_session(&rt, 500).await;
        let (target, mut rx_target) = add_session(&rt, 501).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            target,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "target".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        drain_receiver(&mut rx_target);

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchKick(MatchKickData {
                    match_id: match_id.clone(),
                    seat_idx: 1,
                }),
            },
        )
        .await;

        let mut saw_kick = false;
        for _ in 0..20 {
            let event = timeout(Duration::from_millis(250), history_rx.recv())
                .await
                .expect("history channel event")
                .expect("history channel open");

            if let GameHistoryEvent::GameAction {
                ty,
                actor_user_id,
                actor_seat_idx,
                actor_team_idx,
                data,
                ..
            } = event
            {
                if ty == "match.kick" {
                    assert_eq!(actor_user_id, 500);
                    assert_eq!(actor_seat_idx, Some(0));
                    assert_eq!(actor_team_idx, Some(0));
                    assert_eq!(
                        data.get("reason").and_then(|v| v.as_str()),
                        Some("owner_kick")
                    );
                    assert_eq!(
                        data.get("trigger").and_then(|v| v.as_str()),
                        Some("owner_request")
                    );
                    assert_eq!(
                        data.get("target_user_id").and_then(|v| v.as_i64()),
                        Some(501)
                    );
                    saw_kick = true;
                    break;
                }
            }
        }

        assert!(saw_kick, "expected match.kick history event");
    }

    #[tokio::test]
    async fn player_can_reconnect_after_disconnect() {
        use tokio::time::{Duration, timeout};

        let rt = Realtime::new();
        let (owner, mut rx_owner) = add_session(&rt, 300).await;
        let (player, _rx_player) = add_session(&rt, 301).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        for sid in [owner, player] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready: true,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        while rx_owner.try_recv().is_ok() {}

        let player_key = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&player)
                .and_then(|sess| sess.player_key.clone())
                .expect("player key")
        };

        {
            let mut s = rt.state.lock().await;
            s.sessions.remove(&player);
        }

        rt.handle_player_disconnect(player, &match_id, &player_key)
            .await;

        let (player_reconnect, mut rx_reconnect) = add_session(&rt, 301).await;

        rt.handle_message(
            player_reconnect,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player_return".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        if let Ok(maybe_err) =
            timeout(Duration::from_millis(250), recv_error(&mut rx_reconnect)).await
        {
            assert!(maybe_err.is_none());
        }

        {
            let s = rt.state.lock().await;
            let sess = s.sessions.get(&player_reconnect).expect("session exists");
            assert_eq!(sess.active_match_id.as_deref(), Some(match_id.as_str()));
            let m = s.matches.get(&match_id).expect("match exists");
            assert_eq!(m.players.len(), 2, "seat count should remain constant");
            assert!(
                m.players
                    .iter()
                    .any(|p| sess.player_key.as_deref() == Some(p.key.as_str()))
            );
        }
    }

    #[tokio::test]
    async fn inactive_players_are_swept_after_timeout() {
        let rt = Realtime::new();
        let (owner, _rx_owner) = add_session(&rt, 400).await;
        let (target, _rx_target) = add_session(&rt, 401).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        abandon_time_ms: 10,
                        reconnect_grace_ms: 5,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let (match_id, owner_key) = {
            let s = rt.state.lock().await;
            let sess = s.sessions.get(&owner).expect("owner session");
            let m = s
                .matches
                .get(sess.active_match_id.as_ref().expect("match id"))
                .expect("match exists");
            (sess.active_match_id.clone().unwrap(), m.owner_key.clone())
        };

        rt.handle_message(
            target,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "target".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        {
            let mut s = rt.state.lock().await;
            let m = s.matches.get_mut(&match_id).expect("match exists");
            if let Some(p) = m.players.iter_mut().find(|p| p.key != owner_key) {
                p.disconnected_at_ms = Some(0);
            }
        }

        rt.sweep_inactive_players().await;

        {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            assert_eq!(m.players.len(), 1, "sweeper should remove inactive player");
        }
    }

    #[tokio::test]
    async fn sweep_kick_emits_history_event() {
        use crate::history::GameHistoryEvent;
        use tokio::time::{Duration, timeout};

        let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
        let rt = Realtime::new_with_history(history_tx);
        let (owner, _rx_owner) = add_session(&rt, 520).await;
        let (target, _rx_target) = add_session(&rt, 521).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        abandon_time_ms: 10,
                        reconnect_grace_ms: 5,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            target,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "target".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        {
            let mut s = rt.state.lock().await;
            let m = s.matches.get_mut(&match_id).expect("match exists");
            if let Some(p) = m.players.iter_mut().find(|p| p.user_id == 521) {
                p.disconnected_at_ms = Some(0);
            }
        }

        rt.sweep_inactive_players().await;

        let mut saw_kick = false;
        for _ in 0..20 {
            let event = timeout(Duration::from_millis(250), history_rx.recv())
                .await
                .expect("history channel event")
                .expect("history channel open");

            if let GameHistoryEvent::GameAction {
                ty,
                actor_user_id,
                actor_seat_idx,
                actor_team_idx,
                data,
                ..
            } = event
            {
                if ty == "match.kick" {
                    assert_eq!(actor_user_id, 0);
                    assert!(actor_seat_idx.is_none());
                    assert!(actor_team_idx.is_none());
                    assert_eq!(
                        data.get("reason").and_then(|v| v.as_str()),
                        Some("disconnect_timeout")
                    );
                    assert_eq!(data.get("trigger").and_then(|v| v.as_str()), Some("sweep"));
                    assert_eq!(
                        data.get("target_user_id").and_then(|v| v.as_i64()),
                        Some(521)
                    );
                    saw_kick = true;
                    break;
                }
            }
        }

        assert!(saw_kick, "expected sweep match.kick history event");
    }

    #[tokio::test]
    async fn history_events_emitted_for_match_lifecycle() {
        use crate::history::GameHistoryEvent;
        use tokio::time::{Duration, timeout};

        let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
        let rt = Realtime::new_with_history(history_tx);

        let (s1, _rx1) = add_session(&rt, -1).await;
        let (s2, _rx2) = add_session(&rt, -2).await;

        // Create a 2-player match that finishes after a single hand.
        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&s1)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        // Join as second client.
        rt.handle_message(
            s2,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "p2".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        // Ready both clients.
        for (sid, ready) in [(s1, true), (s2, true)] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready,
                    }),
                },
            )
            .await;
        }

        // Start (owner only).
        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        // Play out until finished.
        loop {
            let (phase, turn_session_id, turn_hand_len) = {
                let s = rt.state.lock().await;
                let m = s.matches.get(&match_id).expect("match exists");
                let phase = m.phase;

                if phase == MatchPhase::Finished {
                    (phase, s1, 0)
                } else {
                    let g = m.game.as_ref().expect("game started");
                    let turn_seat_idx = g.public.turn_seat_idx as usize;

                    let key = m.players[turn_seat_idx].key.clone();
                    let turn_session_id = *s
                        .sessions
                        .iter()
                        .find_map(|(sid, sess)| {
                            (sess.player_key.as_deref() == Some(key.as_str())).then_some(sid)
                        })
                        .expect("turn session");

                    let hand_len = g.hands().get(turn_seat_idx).map(|h| h.len()).unwrap_or(0);

                    (phase, turn_session_id, hand_len)
                }
            };

            if phase == MatchPhase::Finished {
                break;
            }

            assert!(turn_hand_len > 0, "turn player should have cards");

            rt.handle_message(
                turn_session_id,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::GamePlayCard(GamePlayCardData {
                        match_id: match_id.clone(),
                        card_idx: 0,
                    }),
                },
            )
            .await;
        }

        // We now emit gameplay actions to history, so we can't assume MatchFinished is the 4th
        // event. Instead, assert that lifecycle events happen in order and that at least one
        // gameplay action was observed before the match finished.

        let mut pre_start: Vec<GameHistoryEvent> = Vec::new();

        // Drain events until we see the match start.
        loop {
            let ev = timeout(Duration::from_millis(500), history_rx.recv())
                .await
                .expect("history event timeout")
                .expect("history channel closed");

            let started = matches!(ev, GameHistoryEvent::MatchStarted { .. });
            pre_start.push(ev);

            if started {
                break;
            }

            assert!(
                pre_start.len() <= 10,
                "too many pre-start history events: {pre_start:?}"
            );
        }

        let idx_created = pre_start
            .iter()
            .position(|ev| matches!(ev, GameHistoryEvent::MatchCreated { .. }))
            .expect("expected MatchCreated event");

        let idx_joined = pre_start
            .iter()
            .position(|ev| matches!(ev, GameHistoryEvent::PlayerJoined { .. }))
            .expect("expected PlayerJoined event");

        let idx_started = pre_start
            .iter()
            .position(|ev| matches!(ev, GameHistoryEvent::MatchStarted { .. }))
            .expect("expected MatchStarted event");

        assert!(
            idx_created < idx_started,
            "MatchCreated must precede MatchStarted"
        );
        assert!(
            idx_joined < idx_started,
            "PlayerJoined must precede MatchStarted"
        );

        let ready_actions = pre_start
            .iter()
            .filter_map(|ev| match ev {
                GameHistoryEvent::GameAction { ty, data, .. } if ty == "match.ready" => Some(data),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            ready_actions.len(),
            2,
            "expected two match.ready history actions (one per client)"
        );

        for data in ready_actions {
            assert_eq!(
                data.get("ready").and_then(|v| v.as_bool()),
                Some(true),
                "expected ready=true in match.ready data: {data}"
            );
        }

        // Continue draining until finished; require at least one gameplay action.
        let mut saw_gameplay_action = false;
        loop {
            let ev = timeout(Duration::from_millis(500), history_rx.recv())
                .await
                .expect("history event timeout")
                .expect("history channel closed");

            if let GameHistoryEvent::GameAction { ty, .. } = &ev {
                if ty.starts_with("game.") {
                    saw_gameplay_action = true;
                }
            }

            if matches!(ev, GameHistoryEvent::MatchFinished { .. }) {
                break;
            }
        }

        assert!(
            saw_gameplay_action,
            "expected at least one gameplay history action (game.*)"
        );
    }

    #[tokio::test]
    async fn chat_messages_emit_history_events_for_match_rooms() {
        use crate::history::GameHistoryEvent;
        use tokio::time::{Duration, timeout};

        let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
        let rt = Realtime::new_with_history(history_tx);

        let (s1, _rx1) = add_session(&rt, 101).await;

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&s1)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        let message = "mate?".to_string();
        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::ChatSay(ChatSayData {
                    room_id: match_id.clone(),
                    content: message.clone(),
                }),
            },
        )
        .await;

        let mut chat_event: Option<(String, Option<u8>, serde_json::Value)> = None;
        for _ in 0..20 {
            let ev = timeout(Duration::from_millis(250), history_rx.recv())
                .await
                .expect("history event timeout")
                .expect("history channel closed");

            if let GameHistoryEvent::GameAction {
                match_id: ev_match_id,
                actor_seat_idx,
                ty,
                data,
                ..
            } = ev
            {
                if ty == "chat.message" {
                    chat_event = Some((ev_match_id, actor_seat_idx, data));
                    break;
                }
            }
        }

        let (ev_match_id, actor_seat_idx, data) =
            chat_event.expect("expected chat.message history event");

        assert_eq!(ev_match_id, match_id);
        assert_eq!(actor_seat_idx, Some(0));
        assert_eq!(
            data.get("room_id").and_then(|v| v.as_str()),
            Some(match_id.as_str())
        );
        assert_eq!(
            data.get("message")
                .and_then(|m| m.get("content"))
                .and_then(|v| v.as_str()),
            Some(message.as_str())
        );
    }

    #[tokio::test]
    async fn flor_commands_disabled_when_option_off() {
        let rt = Realtime::new();

        let (s1, mut rx1) = add_session(&rt, -1).await;
        let (s2, mut rx2) = add_session(&rt, -2).await;

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        flor: false,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&s1)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            s2,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "p2".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        for (sid, ready) in [(s1, true), (s2, true)] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        drain_receiver(&mut rx1);
        drain_receiver(&mut rx2);

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::GameSay(GameSayData {
                    match_id: match_id.clone(),
                    command: GameCommand::Flor,
                }),
            },
        )
        .await;

        let err = recv_error(&mut rx1)
            .await
            .unwrap_or_else(|| panic!("expected error response for flor command"));

        assert_eq!(err.code, "COMMAND_NOT_ALLOWED");
    }

    #[tokio::test]
    async fn history_events_emitted_for_spectator_watch_and_leave() {
        use crate::history::GameHistoryEvent;
        use tokio::time::{Duration, timeout};

        let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
        let rt = Realtime::new_with_history(history_tx);

        let (s1, _rx1) = add_session(&rt, -1).await;
        let (spectator, _rx_spec) = add_session(&rt, -3).await;

        rt.handle_message(
            s1,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        flor: true,
                        match_points: 3,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&s1)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("creator should be in a match")
        };

        rt.handle_message(
            spectator,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchWatch(MatchWatchData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        rt.handle_message(
            spectator,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchLeave(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        let mut saw_watch = false;
        let mut saw_unwatch = false;

        for _ in 0..20 {
            let ev = timeout(Duration::from_millis(500), history_rx.recv())
                .await
                .expect("history event timeout")
                .expect("history channel closed");

            match ev {
                GameHistoryEvent::SpectatorJoined {
                    match_id: mid,
                    user_id,
                } => {
                    if mid == match_id {
                        assert_eq!(user_id, -3);
                        saw_watch = true;
                    }
                }
                GameHistoryEvent::SpectatorLeft {
                    match_id: mid,
                    user_id,
                    reason,
                } => {
                    if mid == match_id {
                        assert_eq!(user_id, -3);
                        assert_eq!(reason, "client_leave");
                        saw_unwatch = true;
                    }
                }
                _ => {}
            }

            if saw_watch && saw_unwatch {
                break;
            }
        }

        assert!(saw_watch, "expected SpectatorJoined history event");
        assert!(saw_unwatch, "expected SpectatorLeft history event");
    }

    #[tokio::test]
    async fn rematch_spawns_new_lobby_with_same_players() {
        let rt = Realtime::new();
        let (owner, _rx_owner) = add_session(&rt, 900).await;
        let (player, _rx_player) = add_session(&rt, 901).await;

        let match_id = run_match_to_completion(&rt, owner, player).await;

        let (old_owner_key, old_player_key) = {
            let s = rt.state.lock().await;
            let m = s.matches.get(&match_id).expect("match exists");
            let owner_key = m.owner_key.clone();
            let other_key = m
                .players
                .iter()
                .find(|p| p.key != owner_key)
                .map(|p| p.key.clone())
                .expect("second player key");
            (owner_key, other_key)
        };

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchRematch(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        let (_new_match_id, new_owner_key, new_player_key, new_players) = {
            let s = rt.state.lock().await;
            assert!(
                s.matches.get(&match_id).is_none(),
                "old match should be removed"
            );
            let owner_sess = s.sessions.get(&owner).expect("owner session");
            let player_sess = s.sessions.get(&player).expect("player session");
            let new_match_id = owner_sess
                .active_match_id
                .clone()
                .expect("owner moved to new match");
            assert_eq!(
                player_sess.active_match_id.as_deref(),
                Some(new_match_id.as_str())
            );
            let new_match = s.matches.get(&new_match_id).expect("new match");
            assert_eq!(new_match.phase, MatchPhase::Lobby);
            assert_eq!(new_match.team_points, [0, 0]);
            assert!(new_match.game.is_none());
            assert!(new_match.players.iter().all(|p| !p.ready));
            (
                new_match_id,
                owner_sess.player_key.clone().expect("owner key"),
                player_sess.player_key.clone().expect("player key"),
                new_match.players.clone(),
            )
        };

        assert_ne!(new_owner_key, old_owner_key, "owner key should rotate");
        assert_ne!(new_player_key, old_player_key, "player key should rotate");
        assert_eq!(new_players.len(), 2);
        let names: Vec<_> = new_players.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"p1"));
        assert!(names.contains(&"p2"));
    }

    #[tokio::test]
    async fn rematch_requires_owner() {
        let rt = Realtime::new();
        let (owner, _rx_owner) = add_session(&rt, 910).await;
        let (player, mut rx_player) = add_session(&rt, 911).await;

        let match_id = run_match_to_completion(&rt, owner, player).await;

        drain_receiver(&mut rx_player);

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchRematch(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        let err = recv_error(&mut rx_player)
            .await
            .expect("expected NOT_OWNER error");
        assert_eq!(err.code, "NOT_OWNER");
    }

    #[tokio::test]
    async fn rematch_requires_finished_match() {
        let rt = Realtime::new();
        let (owner, mut rx_owner) = add_session(&rt, 920).await;
        let (player, _rx_player) = add_session(&rt, 921).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "p1".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 1,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("match id")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "p2".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        for (sid, ready) in [(owner, true), (player, true)] {
            rt.handle_message(
                sid,
                WsInMessage {
                    v: Default::default(),
                    id: Default::default(),
                    msg: C2sMessage::MatchReady(MatchReadyData {
                        match_id: match_id.clone(),
                        ready,
                    }),
                },
            )
            .await;
        }

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchStart(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        drain_receiver(&mut rx_owner);

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchRematch(MatchRefData {
                    match_id: match_id.clone(),
                }),
            },
        )
        .await;

        let err = recv_error(&mut rx_owner)
            .await
            .expect("expected BAD_STATE error");
        assert_eq!(err.code, "BAD_STATE");
    }

    #[tokio::test]
    async fn rematch_emits_history_events_for_new_match() {
        use tokio::time::{Duration, timeout};

        let (history_tx, mut history_rx) = tokio::sync::mpsc::unbounded_channel();
        let rt = Realtime::new_with_history(history_tx);
        let (owner, _rx_owner) = add_session(&rt, 930).await;
        let (player, _rx_player) = add_session(&rt, 931).await;

        let old_match_id = run_match_to_completion(&rt, owner, player).await;

        while history_rx.try_recv().is_ok() {}

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchRematch(MatchRefData {
                    match_id: old_match_id.clone(),
                }),
            },
        )
        .await;

        let new_match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("owner should move to new match")
        };

        let mut saw_rematch = false;
        let mut saw_match_created = false;
        let mut new_match_join_events = 0;

        for _ in 0..50 {
            let event = timeout(Duration::from_millis(500), history_rx.recv())
                .await
                .expect("expected history event during rematch")
                .expect("history channel open");

            match event {
                GameHistoryEvent::GameAction {
                    match_id, ty, data, ..
                } if ty == "match.rematch" && match_id == old_match_id => {
                    assert_eq!(
                        data.get("new_match_id").and_then(|v| v.as_str()),
                        Some(new_match_id.as_str())
                    );
                    assert_eq!(data.get("player_count").and_then(|v| v.as_u64()), Some(2));
                    saw_rematch = true;
                }
                GameHistoryEvent::MatchCreated {
                    match_id, owner, ..
                } if match_id == new_match_id => {
                    assert_eq!(owner.display_name, "p1");
                    saw_match_created = true;
                }
                GameHistoryEvent::PlayerJoined {
                    match_id,
                    display_name,
                    ..
                } if match_id == new_match_id => {
                    assert_eq!(display_name, "p2");
                    new_match_join_events += 1;
                }
                _ => {}
            }

            if saw_rematch && saw_match_created && new_match_join_events == 1 {
                break;
            }
        }

        assert!(saw_rematch, "expected match.rematch history event");
        assert!(
            saw_match_created,
            "expected MatchCreated event for new lobby"
        );
        assert_eq!(
            new_match_join_events, 1,
            "expected second player to emit a PlayerJoined event"
        );
    }

    #[tokio::test]
    async fn list_active_matches_includes_lobby_entries_and_skips_finished() {
        let rt = Realtime::new();
        let (owner, _rx_owner) = add_session(&rt, 5_000).await;
        let (player, _rx_player) = add_session(&rt, 5_001).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 12,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("owner should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        {
            let mut s = rt.state.lock().await;
            if let Some(m) = s.matches.get_mut(&match_id) {
                if let Some(p) = m.players.iter_mut().find(|p| p.user_id == 5_001) {
                    p.disconnected_at_ms = Some(42);
                }
            }
        }

        let owner_matches = rt.list_active_matches_for_user(5_000).await;
        assert_eq!(owner_matches.len(), 1);
        assert_eq!(owner_matches[0].match_.id, match_id);
        assert!(owner_matches[0].me.is_owner);
        assert_eq!(owner_matches[0].me.seat_idx, 0);
        assert!(!owner_matches[0].me.ready);

        let player_matches = rt.list_active_matches_for_user(5_001).await;
        assert_eq!(player_matches.len(), 1);
        assert_eq!(player_matches[0].me.seat_idx, 1);
        let disc: Option<i64> = player_matches[0].me.disconnected_at_ms.clone().into();
        assert_eq!(disc, Some(42));
        assert!(!player_matches[0].me.is_owner);

        {
            let mut s = rt.state.lock().await;
            if let Some(m) = s.matches.get_mut(&match_id) {
                m.phase = MatchPhase::Finished;
            }
        }

        assert!(rt.list_active_matches_for_user(5_000).await.is_empty());
        assert!(rt.list_active_matches_for_user(5_001).await.is_empty());
    }

    #[tokio::test]
    async fn me_active_matches_get_returns_snapshot_for_owner() {
        let rt = Realtime::new();
        let (owner, mut rx_owner) = add_session(&rt, 6_000).await;
        let (player, _rx_player) = add_session(&rt, 6_001).await;

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchCreate(MatchCreateData {
                    name: "owner".into(),
                    team: Some(TeamIdx::TEAM_0).into(),
                    options: Some(MatchOptions {
                        max_players: 2,
                        match_points: 12,
                        turn_time_ms: 30_000,
                        ..MatchOptions::default()
                    })
                    .into(),
                }),
            },
        )
        .await;

        let match_id = {
            let s = rt.state.lock().await;
            s.sessions
                .get(&owner)
                .and_then(|sess| sess.active_match_id.clone())
                .expect("owner should be in a match")
        };

        rt.handle_message(
            player,
            WsInMessage {
                v: Default::default(),
                id: Default::default(),
                msg: C2sMessage::MatchJoin(MatchJoinData {
                    match_id: match_id.clone(),
                    name: "player".into(),
                    team: Some(TeamIdx::TEAM_1).into(),
                }),
            },
        )
        .await;

        drain_receiver(&mut rx_owner);

        rt.handle_message(
            owner,
            WsInMessage {
                v: Default::default(),
                id: Some("req-1".into()).into(),
                msg: C2sMessage::MeActiveMatchesGet,
            },
        )
        .await;

        let msg = rx_owner.recv().await.expect("ws response");
        let echoed_id: Option<String> = msg.id.clone().into();
        assert_eq!(echoed_id.as_deref(), Some("req-1"));

        let snapshot = match msg.msg {
            S2cMessage::MeActiveMatches(data) => data,
            other => panic!("unexpected message: {other:?}"),
        };

        assert_eq!(snapshot.matches.len(), 1);
        assert_eq!(snapshot.matches[0].match_.id, match_id);
        assert!(snapshot.matches[0].me.is_owner);
        assert_eq!(snapshot.matches[0].me.seat_idx, 0);
        assert_eq!(snapshot.matches[0].match_.phase, MatchPhase::Lobby);
    }
}
