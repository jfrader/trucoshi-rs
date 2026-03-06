use crate::history::{GameHistoryEvent, HistoryPlayer};
use crate::protocol::ws::v2::{
    C2sMessage, ChatMessageData, ChatSnapshotData, ErrorPayload, GameSnapshotData, GameUpdateData,
    HelloData, LobbyMatchRemoveData, LobbyMatchUpsertData, LobbySnapshotData, MatchLeftData,
    MatchSnapshotData, MatchUpdateData, PongData, S2cMessage, WsInMessage, WsOutMessage,
    schema::{
        HandState, LobbyMatch, MatchOptions, MatchPhase, Maybe, PrivatePlayer, PublicChatMessage,
        PublicChatRoom, PublicChatUser, PublicMatch, PublicPlayer, TeamIdx,
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
use tracing::{debug, warn};
use trucoshi_game::{CommandOutcome, PlayOutcome};
use uuid::Uuid;

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
}

#[derive(Debug, Clone)]
pub struct ChatRoomState {
    pub id: String,
    pub messages: Vec<PublicChatMessage>,
    pub participants: HashSet<Uuid>,
}

#[derive(Debug, Clone)]
pub struct PlayerState {
    pub key: String,
    pub name: String,
    pub team: TeamIdx,
    pub ready: bool,
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
        {
            let mut s = self.state.lock().await;
            if let Some(sess) = s.sessions.remove(&session_id) {
                if let Some(msid) = sess.active_match_id {
                    maybe_leave = Some((msid, sess.player_key, sess.user_id));
                }
            }
            s.connections = s.connections.saturating_sub(1);
            debug!(connections = s.connections, %session_id, "ws disconnected");
        }

        if let Some((msid, pkey, user_id)) = maybe_leave {
            if let Some(pkey) = pkey.as_deref() {
                let removed = self
                    .leave_match_internal(session_id, &msid, pkey, user_id, "disconnect")
                    .await;
                if removed {
                    self.broadcast_lobby_match_remove(&msid).await;
                } else {
                    self.broadcast_lobby_match_upsert(&msid).await;
                }
            } else {
                // spectator: just detach from match participants / rooms
                self.leave_match_watch_internal(session_id, &msid).await;

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

                let player = PlayerState {
                    key: owner_key.clone(),
                    name,
                    team,
                    ready: false,
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
                    let owner_user_id = {
                        let s = self.state.lock().await;
                        s.sessions
                            .get(&session_id)
                            .map(|sess| sess.user_id)
                            .unwrap_or(0)
                    };

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
                    .join_match_internal(session_id, &match_id, name, team)
                    .await;

                match res {
                    Ok(()) => {
                        self.join_room_internal(session_id, &match_id).await;

                        // Best-effort: emit persistence events.
                        {
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
                let exists = {
                    let mut s = self.state.lock().await;
                    match s.matches.get_mut(&match_id) {
                        Some(m) => {
                            m.participants.insert(session_id);

                            if let Some(sess) = s.sessions.get_mut(&session_id) {
                                sess.active_match_id = Some(match_id.clone());
                                sess.player_key = None;
                            }

                            true
                        }
                        None => false,
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
                    self.leave_match_watch_internal(session_id, &msid).await;

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
                                actor_seat_idx: u8::try_from(seat_idx).unwrap_or(0),
                                actor_team_idx: p.team.as_u8(),
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

                    if m.owner_key != pkey {
                        err = Some(("NOT_OWNER".into(), "only the owner can pause".into()));
                    } else if m.phase != MatchPhase::Started {
                        err = Some(("BAD_STATE".into(), "match not started".into()));
                    } else {
                        if let Some(seat_idx) = m.players.iter().position(|p| p.key == pkey) {
                            if let Some(p) = m.players.get(seat_idx) {
                                history_action = Some(GameHistoryEvent::GameAction {
                                    match_id: msid.clone(),
                                    actor_seat_idx: u8::try_from(seat_idx).unwrap_or(0),
                                    actor_team_idx: p.team.as_u8(),
                                    actor_user_id,
                                    ty: "match.pause".into(),
                                    data: serde_json::json!({
                                        "server_time_ms": now_ms,
                                    }),
                                });
                            }
                        }

                        m.phase = MatchPhase::Paused;
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

                    if m.owner_key != pkey {
                        err = Some(("NOT_OWNER".into(), "only the owner can resume".into()));
                    } else if m.phase != MatchPhase::Paused {
                        err = Some(("BAD_STATE".into(), "match not paused".into()));
                    } else {
                        if let Some(seat_idx) = m.players.iter().position(|p| p.key == pkey) {
                            if let Some(p) = m.players.get(seat_idx) {
                                history_action = Some(GameHistoryEvent::GameAction {
                                    match_id: msid.clone(),
                                    actor_seat_idx: u8::try_from(seat_idx).unwrap_or(0),
                                    actor_team_idx: p.team.as_u8(),
                                    actor_user_id,
                                    ty: "match.resume".into(),
                                    data: serde_json::json!({
                                        "server_time_ms": now_ms,
                                    }),
                                });
                            }
                        }

                        m.phase = MatchPhase::Started;
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

                                let actor_seat_idx = u8::try_from(from_player_idx).unwrap_or(0);
                                let actor_team_idx = m
                                    .players
                                    .get(from_player_idx)
                                    .map(|p| p.team.as_u8())
                                    .unwrap_or(0);
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
                                let allowed = g.possible_commands_for_player(from_player_idx);
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

                                let actor_seat_idx = u8::try_from(from_player_idx).unwrap_or(0);
                                let actor_team_idx = m
                                    .players
                                    .get(from_player_idx)
                                    .map(|p| p.team.as_u8())
                                    .unwrap_or(0);
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

            C2sMessage::ChatJoin(d) => {
                self.join_room_internal(session_id, &d.room_id).await;
                self.emit_chat_snapshot_to(session_id, &d.room_id, id).await;
            }

            C2sMessage::ChatSay(d) => {
                let msg = self
                    .append_chat_message(&d.room_id, session_id, d.content)
                    .await;
                if msg.is_none() {
                    self.send_to(
                        session_id,
                        Self::err_out(id, "BAD_REQUEST", "unknown session"),
                    )
                    .await;
                    return;
                }

                let correlated = id.clone().map(|id| (session_id, id));
                self.broadcast_chat_message(&d.room_id, msg.expect("chat msg exists"), correlated)
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
            spectator_count,
            team_points: m.team_points,
        }
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
            g.possible_commands_for_player(idx)
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
    ) -> Result<(), (String, String)> {
        let mut s = self.state.lock().await;

        let already_in_match = s
            .sessions
            .get(&session_id)
            .map(|sess| sess.active_match_id.is_some())
            .unwrap_or(false);

        if already_in_match {
            return Err(("ALREADY_IN_MATCH".into(), "already in a match".into()));
        }

        let Some(m) = s.matches.get_mut(match_id) else {
            return Err(("MATCH_NOT_FOUND".into(), "match not found".into()));
        };

        if m.phase != MatchPhase::Lobby {
            return Err(("MATCH_NOT_JOINABLE".into(), "match already started".into()));
        }

        if m.players.len() >= (m.options.max_players as usize) {
            return Err(("MATCH_FULL".into(), "match is full".into()));
        }

        let key = Uuid::new_v4().to_string();

        // Pick team (0/1) with capacity.
        //
        // Protocol v2: if the caller explicitly requests a team, we either honor it or
        // reject with an error (no silent reassignment).
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
            // Auto-assign to the least-populated team.
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
            name,
            team,
            ready: false,
        });

        m.participants.insert(session_id);

        if let Some(sess) = s.sessions.get_mut(&session_id) {
            sess.active_match_id = Some(match_id.to_string());
            sess.player_key = Some(key);
        }

        Ok(())
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
    async fn leave_match_watch_internal(&self, session_id: Uuid, match_id: &str) {
        {
            let mut s = self.state.lock().await;

            if let Some(m) = s.matches.get_mut(match_id) {
                m.participants.remove(&session_id);
            }

            if let Some(sess) = s.sessions.get_mut(&session_id) {
                if sess.active_match_id.as_deref() == Some(match_id) {
                    sess.active_match_id = None;
                }
                sess.player_key = None;
            }
        }

        // Match chat room id == match_id.
        self.leave_room_internal(session_id, match_id).await;
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
    ) -> Option<PublicChatMessage> {
        let mut s = self.state.lock().await;

        let user = s.sessions.get(&session_id).map(|sess| {
            let (seat_idx, team, name) = sess
                .active_match_id
                .as_deref()
                .and_then(|msid| s.matches.get(msid))
                .and_then(|m| {
                    let pk = sess.player_key.as_deref()?;
                    let idx = m.players.iter().position(|p| p.key == pk)?;
                    let p = m.players.get(idx)?;
                    Some((
                        Maybe(Some(u8::try_from(idx).expect("seat idx fits in u8"))),
                        Maybe(Some(p.team)),
                        p.name.clone(),
                    ))
                })
                .unwrap_or((Maybe(None), Maybe(None), format!("User{}", sess.user_id)));

            PublicChatUser {
                name,
                seat_idx,
                team,
            }
        })?;

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

        Some(msg)
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
    use crate::protocol::ws::v2::messages::MatchWatchData;
    use crate::protocol::ws::v2::{
        GamePlayCardData, MatchCreateData, MatchJoinData, MatchReadyData, MatchRefData, WsInMessage,
    };

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
            },
        );

        (session_id, rx)
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
                        flor: true,
                        match_points: 1,
                        turn_time_ms: 30_000,
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
                        flor: true,
                        match_points: 1,
                        turn_time_ms: 30_000,
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
                        flor: true,
                        match_points: 1,
                        turn_time_ms: 30_000,
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
                        flor: true,
                        match_points: 1,
                        turn_time_ms: 30_000,
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
                        flor: true,
                        match_points: 1,
                        turn_time_ms: 30_000,
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
}
