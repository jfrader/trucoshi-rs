use std::collections::HashMap;

use serde_json::json;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{debug, warn};
use trucoshi_realtime::history::{GameHistoryEvent, HistoryPlayer};

fn db_user_id(user_id: i64) -> Option<i64> {
    // Guests are negative ids; they should not FK into `users`.
    (user_id > 0).then_some(user_id)
}

pub trait GameHistoryBackend: Clone + Send + Sync + 'static {
    async fn gh_create_match(
        &self,
        server_version: &str,
        protocol_version: i32,
        rng_seed: i64,
        options: serde_json::Value,
    ) -> anyhow::Result<i64>;

    async fn gh_add_player(
        &self,
        match_db_id: i64,
        seat_idx: i32,
        team_idx: i32,
        user_id: Option<i64>,
        display_name: &str,
    ) -> anyhow::Result<i64>;

    async fn gh_append_event(
        &self,
        match_db_id: i64,
        seq: i64,
        actor_seat_idx: Option<i32>,
        actor_user_id: Option<i64>,
        r#type: &str,
        data: serde_json::Value,
    ) -> anyhow::Result<i64>;

    async fn gh_finish_match(&self, match_db_id: i64) -> anyhow::Result<()>;
}

impl GameHistoryBackend for trucoshi_store::Store {
    async fn gh_create_match(
        &self,
        server_version: &str,
        protocol_version: i32,
        rng_seed: i64,
        options: serde_json::Value,
    ) -> anyhow::Result<i64> {
        self.gh_create_match(
            Some(server_version),
            Some(protocol_version),
            Some(rng_seed),
            &options,
        )
        .await
    }

    async fn gh_add_player(
        &self,
        match_db_id: i64,
        seat_idx: i32,
        team_idx: i32,
        user_id: Option<i64>,
        display_name: &str,
    ) -> anyhow::Result<i64> {
        self.gh_add_player(match_db_id, seat_idx, team_idx, user_id, display_name)
            .await
    }

    async fn gh_append_event(
        &self,
        match_db_id: i64,
        seq: i64,
        actor_seat_idx: Option<i32>,
        actor_user_id: Option<i64>,
        r#type: &str,
        data: serde_json::Value,
    ) -> anyhow::Result<i64> {
        self.gh_append_event(
            match_db_id,
            seq,
            actor_seat_idx,
            actor_user_id,
            r#type,
            &data,
        )
        .await
    }

    async fn gh_finish_match(&self, match_db_id: i64) -> anyhow::Result<()> {
        self.gh_finish_match(match_db_id).await
    }
}

#[derive(Debug, Clone, Default)]
struct WorkerState {
    match_db_ids: HashMap<String, i64>,
    next_seq: HashMap<String, i64>,
}

impl WorkerState {
    fn seq_next(&mut self, match_id: &str) -> i64 {
        let entry = self.next_seq.entry(match_id.to_string()).or_insert(0);
        let v = *entry;
        *entry = entry.saturating_add(1);
        v
    }
}

async fn ensure_player_row<B: GameHistoryBackend>(
    backend: &B,
    match_db_id: i64,
    player: &HistoryPlayer,
) -> anyhow::Result<i64> {
    backend
        .gh_add_player(
            match_db_id,
            i32::from(player.seat_idx),
            i32::from(player.team_idx),
            db_user_id(player.user_id),
            &player.display_name,
        )
        .await
}

pub async fn run_game_history_worker<B: GameHistoryBackend>(
    backend: B,
    mut rx: UnboundedReceiver<GameHistoryEvent>,
) {
    let mut st = WorkerState::default();

    while let Some(ev) = rx.recv().await {
        match ev {
            GameHistoryEvent::MatchCreated {
                match_id,
                server_version,
                protocol_version,
                rng_seed,
                options,
                owner,
            } => {
                let db_id = match backend
                    .gh_create_match(&server_version, protocol_version, rng_seed, options)
                    .await
                {
                    Ok(id) => id,
                    Err(e) => {
                        warn!(error = %e, %match_id, "game_history: create match failed");
                        continue;
                    }
                };

                st.match_db_ids.insert(match_id.clone(), db_id);
                st.next_seq.insert(match_id.clone(), 0);

                if let Err(e) = ensure_player_row(&backend, db_id, &owner).await {
                    warn!(error = %e, %match_id, db_id, "game_history: add owner failed");
                }

                let seq = st.seq_next(&match_id);
                let data = json!({
                    "ws_match_id": match_id,
                    "owner": {
                        "seat_idx": owner.seat_idx,
                        "team_idx": owner.team_idx,
                        "user_id": owner.user_id,
                        "display_name": owner.display_name,
                    }
                });

                if let Err(e) = backend
                    .gh_append_event(
                        db_id,
                        seq,
                        Some(i32::from(owner.seat_idx)),
                        db_user_id(owner.user_id),
                        "match.create",
                        data,
                    )
                    .await
                {
                    warn!(error = %e, db_id, "game_history: append match.create failed");
                }
            }

            GameHistoryEvent::PlayerJoined {
                match_id,
                seat_idx,
                team_idx,
                user_id,
                display_name,
            } => {
                let Some(&db_id) = st.match_db_ids.get(&match_id) else {
                    debug!(%match_id, "game_history: drop PlayerJoined (unknown match)");
                    continue;
                };

                if let Err(e) = backend
                    .gh_add_player(
                        db_id,
                        i32::from(seat_idx),
                        i32::from(team_idx),
                        db_user_id(user_id),
                        &display_name,
                    )
                    .await
                {
                    warn!(error = %e, %match_id, db_id, "game_history: add player failed");
                }

                let seq = st.seq_next(&match_id);
                let data = json!({
                    "ws_match_id": match_id,
                    "player": {
                        "seat_idx": seat_idx,
                        "team_idx": team_idx,
                        "user_id": user_id,
                        "display_name": display_name,
                    }
                });

                if let Err(e) = backend
                    .gh_append_event(
                        db_id,
                        seq,
                        Some(i32::from(seat_idx)),
                        db_user_id(user_id),
                        "match.join",
                        data,
                    )
                    .await
                {
                    warn!(error = %e, %match_id, db_id, "game_history: append match.join failed");
                }
            }

            GameHistoryEvent::MatchStarted { match_id } => {
                let Some(&db_id) = st.match_db_ids.get(&match_id) else {
                    debug!(%match_id, "game_history: drop MatchStarted (unknown match)");
                    continue;
                };

                let seq = st.seq_next(&match_id);
                let data = json!({ "ws_match_id": match_id });

                if let Err(e) = backend
                    .gh_append_event(db_id, seq, None, None, "match.start", data)
                    .await
                {
                    warn!(error = %e, %match_id, db_id, "game_history: append match.start failed");
                }
            }

            GameHistoryEvent::MatchFinished {
                match_id,
                team_points,
                reason,
            } => {
                let Some(&db_id) = st.match_db_ids.get(&match_id) else {
                    debug!(%match_id, "game_history: drop MatchFinished (unknown match)");
                    continue;
                };

                if let Err(e) = backend.gh_finish_match(db_id).await {
                    warn!(error = %e, %match_id, db_id, "game_history: finish match failed");
                }

                let seq = st.seq_next(&match_id);
                let data = json!({
                    "ws_match_id": match_id,
                    "team_points": team_points,
                    "reason": reason,
                });

                if let Err(e) = backend
                    .gh_append_event(db_id, seq, None, None, "match.finish", data)
                    .await
                {
                    warn!(error = %e, %match_id, db_id, "game_history: append match.finish failed");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Debug, Clone, PartialEq)]
    enum Op {
        CreateMatch,
        AddPlayer { seat_idx: i32 },
        AppendEvent { seq: i64, ty: String },
        Finish,
    }

    #[derive(Clone, Default)]
    struct MockBackend {
        ops: Arc<Mutex<Vec<Op>>>,
        next_match_id: Arc<Mutex<i64>>,
    }

    impl MockBackend {
        async fn take_ops(&self) -> Vec<Op> {
            let mut g = self.ops.lock().await;
            std::mem::take(&mut *g)
        }
    }

    impl GameHistoryBackend for MockBackend {
        async fn gh_create_match(
            &self,
            _server_version: &str,
            _protocol_version: i32,
            _rng_seed: i64,
            _options: serde_json::Value,
        ) -> anyhow::Result<i64> {
            self.ops.lock().await.push(Op::CreateMatch);
            let mut g = self.next_match_id.lock().await;
            *g += 1;
            Ok(*g)
        }

        async fn gh_add_player(
            &self,
            _match_db_id: i64,
            seat_idx: i32,
            _team_idx: i32,
            _user_id: Option<i64>,
            _display_name: &str,
        ) -> anyhow::Result<i64> {
            self.ops.lock().await.push(Op::AddPlayer { seat_idx });
            Ok(1)
        }

        async fn gh_append_event(
            &self,
            _match_db_id: i64,
            seq: i64,
            _actor_seat_idx: Option<i32>,
            _actor_user_id: Option<i64>,
            r#type: &str,
            _data: serde_json::Value,
        ) -> anyhow::Result<i64> {
            self.ops.lock().await.push(Op::AppendEvent {
                seq,
                ty: r#type.to_string(),
            });
            Ok(1)
        }

        async fn gh_finish_match(&self, _match_db_id: i64) -> anyhow::Result<()> {
            self.ops.lock().await.push(Op::Finish);
            Ok(())
        }
    }

    #[tokio::test]
    async fn worker_sequences_events_per_match() {
        let backend = MockBackend::default();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let task = tokio::spawn(run_game_history_worker(backend.clone(), rx));

        tx.send(GameHistoryEvent::MatchCreated {
            match_id: "m1".into(),
            server_version: "0.1.0".into(),
            protocol_version: 2,
            rng_seed: 123,
            options: json!({ "ws_match_id": "m1" }),
            owner: HistoryPlayer {
                seat_idx: 0,
                team_idx: 0,
                user_id: -1,
                display_name: "p1".into(),
            },
        })
        .unwrap();

        tx.send(GameHistoryEvent::PlayerJoined {
            match_id: "m1".into(),
            seat_idx: 1,
            team_idx: 1,
            user_id: -2,
            display_name: "p2".into(),
        })
        .unwrap();

        tx.send(GameHistoryEvent::MatchStarted {
            match_id: "m1".into(),
        })
        .unwrap();

        tx.send(GameHistoryEvent::MatchFinished {
            match_id: "m1".into(),
            team_points: [1, 0],
            reason: "score_reached".into(),
        })
        .unwrap();

        drop(tx);
        task.await.unwrap();

        let ops = backend.take_ops().await;

        // Ensure seq increments and operations happened in the expected general shape.
        assert_eq!(ops[0], Op::CreateMatch);
        assert_eq!(ops[1], Op::AddPlayer { seat_idx: 0 });
        assert_eq!(
            ops[2],
            Op::AppendEvent {
                seq: 0,
                ty: "match.create".into()
            }
        );
        assert_eq!(ops[3], Op::AddPlayer { seat_idx: 1 });
        assert_eq!(
            ops[4],
            Op::AppendEvent {
                seq: 1,
                ty: "match.join".into()
            }
        );
        assert_eq!(
            ops[5],
            Op::AppendEvent {
                seq: 2,
                ty: "match.start".into()
            }
        );
        assert_eq!(ops[6], Op::Finish);
        assert_eq!(
            ops[7],
            Op::AppendEvent {
                seq: 3,
                ty: "match.finish".into()
            }
        );
    }
}
