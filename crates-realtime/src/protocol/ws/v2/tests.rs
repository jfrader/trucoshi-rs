use super::{C2sMessage, S2cMessage, WsInMessage, WsOutMessage};

#[cfg(feature = "json-schema")]
use schemars::schema::RootSchema;

#[test]
fn ws_in_message_rejects_wrong_version() {
    let raw = r#"{"v":1,"msg":{"type":"lobby.snapshot.get"}}"#;
    let err = serde_json::from_str::<WsInMessage>(raw).unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("unsupported ws protocol version") || s.contains("expected"),
        "unexpected error: {s}"
    );
}

#[test]
fn ws_in_message_allows_missing_id() {
    let raw = r#"{"v":2,"msg":{"type":"lobby.snapshot.get"}}"#;
    let msg = serde_json::from_str::<WsInMessage>(raw).expect("should parse");
    matches!(msg.msg, C2sMessage::LobbySnapshotGet);
    assert!(Option::<String>::from(msg.id).is_none());
}

#[test]
fn ws_in_message_rejects_null_id() {
    let raw = r#"{"v":2,"id":null,"msg":{"type":"lobby.snapshot.get"}}"#;
    let err = serde_json::from_str::<WsInMessage>(raw).unwrap_err();
    let s = err.to_string();
    assert!(s.contains("null is not allowed"), "unexpected error: {s}");
}

#[test]
fn ws_out_message_rejects_wrong_version() {
    let raw = r#"{"v":1,"msg":{"type":"hello","data":{"sessionId":"s","serverVersion":"x"}}}"#;
    let err = serde_json::from_str::<WsOutMessage>(raw).unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("unsupported ws protocol version") || s.contains("expected"),
        "unexpected error: {s}"
    );
}

#[test]
fn ws_protocol_is_strict_about_snake_case_fields() {
    // Ensure we do NOT accept camelCase aliases, even if the correct snake_case field exists.
    let raw_in = r#"{"v":2,"msg":{"type":"ping","data":{"client_time_ms":1,"clientTimeMs":1}}}"#;
    let err = serde_json::from_str::<WsInMessage>(raw_in).unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("unknown field") && s.contains("clientTimeMs"),
        "unexpected error: {s}"
    );

    let raw_out = r#"{"v":2,"msg":{"type":"hello","data":{"session_id":"s","server_version":"x","sessionId":"s"}}}"#;
    let err = serde_json::from_str::<WsOutMessage>(raw_out).unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("unknown field") && s.contains("sessionId"),
        "unexpected error: {s}"
    );
}

#[test]
fn ws_protocol_is_strict_about_message_type_strings() {
    let raw = r#"{"v":2,"msg":{"type":"matchJoin","data":{"match_id":"m","name":"n"}}}"#;
    let err = serde_json::from_str::<WsInMessage>(raw).unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("unknown") || s.contains("variant"),
        "unexpected error: {s}"
    );
}

#[test]
fn ws_protocol_rejects_invalid_match_max_players() {
    // Protocol v2: max_players must be one of 2/4/6.
    let raw = r#"{
        "v": 2,
        "msg": {
            "type": "match.create",
            "data": {
                "name": "x",
                "options": {
                    "max_players": 5,
                    "flor": true,
                    "match_points": 9,
                    "turn_time_ms": 30000
                }
            }
        }
    }"#;

    let err = serde_json::from_str::<WsInMessage>(raw).unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("max_players")
            && (s.contains("2, 4, 6") || s.contains("2") && s.contains("4") && s.contains("6")),
        "unexpected error: {s}"
    );
}

#[test]
fn ws_out_message_allows_missing_id() {
    let raw = r#"{"v":2,"msg":{"type":"error","data":{"code":"X","message":"y"}}}"#;
    let msg = serde_json::from_str::<WsOutMessage>(raw).expect("should parse");
    matches!(msg.msg, S2cMessage::Error(_));
    assert!(Option::<String>::from(msg.id).is_none());
}

#[test]
fn ws_out_message_rejects_null_id() {
    let raw = r#"{"v":2,"id":null,"msg":{"type":"error","data":{"code":"X","message":"y"}}}"#;
    let err = serde_json::from_str::<WsOutMessage>(raw).unwrap_err();
    let s = err.to_string();
    assert!(s.contains("null is not allowed"), "unexpected error: {s}");
}

#[cfg(feature = "json-schema")]
fn read_json(path: &std::path::Path) -> serde_json::Value {
    let txt = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
    serde_json::from_str(&txt).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
}

#[cfg(feature = "json-schema")]
fn assert_schema_file_up_to_date(filename: &str, mut schema: RootSchema) {
    // Mirror the metadata patching performed by `ws_schema_v2`.
    use schemars::schema::Metadata;

    let (id, title) = match filename {
        "in.json" => (
            "https://trucoshi.dev/schemas/ws/v2/in.json",
            "Trucoshi WebSocket Protocol v2 — Inbound Frame (client -> server)",
        ),
        "out.json" => (
            "https://trucoshi.dev/schemas/ws/v2/out.json",
            "Trucoshi WebSocket Protocol v2 — Outbound Frame (server -> client)",
        ),
        "c2s.json" => (
            "https://trucoshi.dev/schemas/ws/v2/c2s.json",
            "Trucoshi WebSocket Protocol v2 — Client -> Server Payload",
        ),
        "s2c.json" => (
            "https://trucoshi.dev/schemas/ws/v2/s2c.json",
            "Trucoshi WebSocket Protocol v2 — Server -> Client Payload",
        ),
        other => panic!("unexpected schema filename: {other}"),
    };

    schema.schema.metadata = Some(Box::new(Metadata {
        id: Some(id.to_string()),
        title: Some(title.to_string()),
        ..Default::default()
    }));

    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .unwrap_or_else(|| panic!("expected crates-realtime to have a parent dir"));
    let path = repo_root.join("schemas/ws/v2").join(filename);

    let on_disk = read_json(&path);
    let generated = serde_json::to_value(&schema).expect("schema to value");

    assert_eq!(
        on_disk, generated,
        "schemas/ws/v2/{filename} is out of date; run: npm run gen:ws (repo root)"
    );
}

#[test]
#[cfg(feature = "json-schema")]
fn ws_v2_schema_files_are_up_to_date() {
    use super::json_schema::{
        c2s_message_schema, s2c_message_schema, ws_in_message_schema, ws_out_message_schema,
    };

    assert_schema_file_up_to_date("in.json", ws_in_message_schema());
    assert_schema_file_up_to_date("out.json", ws_out_message_schema());
    assert_schema_file_up_to_date("c2s.json", c2s_message_schema());
    assert_schema_file_up_to_date("s2c.json", s2c_message_schema());
}

fn is_snake_case_segment(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    // `snake_case` segment means:
    // - lowercase letters, digits, and underscores only
    // - no leading/trailing underscores
    // - no consecutive underscores
    let bytes = s.as_bytes();
    if bytes.first() == Some(&b'_') || bytes.last() == Some(&b'_') {
        return false;
    }

    let mut prev_underscore = false;
    for &b in bytes {
        let ok = (b'a'..=b'z').contains(&b) || (b'0'..=b'9').contains(&b) || b == b'_';
        if !ok {
            return false;
        }
        if b == b'_' {
            if prev_underscore {
                return false;
            }
            prev_underscore = true;
        } else {
            prev_underscore = false;
        }
    }

    true
}

fn is_ws_type_string(s: &str) -> bool {
    let mut parts = s.split('.');
    let first = parts.next();
    let Some(first) = first else {
        return false;
    };
    if !is_snake_case_segment(first) {
        return false;
    }
    for p in parts {
        if !is_snake_case_segment(p) {
            return false;
        }
    }
    true
}

#[test]
fn ws_protocol_message_type_strings_are_snake_case_dot_namespaces() {
    use super::schema::{
        HandState, LobbyMatch, MatchOptions, MatchPhase, Maybe, PlayedCard, PublicChatMessage,
        PublicChatRoom, PublicChatUser, PublicGameState, PublicMatch, PublicPlayer, TeamIdx,
    };
    use super::{
        ChatJoinData, ChatMessageData, ChatSayData, ChatSnapshotData, ErrorPayload,
        GamePlayCardData, GameSayData, GameSnapshotData, GameUpdateData, HelloData,
        LobbyMatchRemoveData, LobbyMatchUpsertData, LobbySnapshotData, MatchCreateData,
        MatchJoinData, MatchLeftData, MatchReadyData, MatchRefData, MatchSnapshotData,
        MatchUpdateData, PingData, PongData,
    };

    fn type_of<T: serde::Serialize>(msg: T) -> String {
        let v = serde_json::to_value(msg).expect("serialize msg");
        v.get("type")
            .and_then(|v| v.as_str())
            .expect("message must have a type tag")
            .to_string()
    }

    let lobby_match = LobbyMatch {
        id: "m".into(),
        options: MatchOptions::default(),
        phase: MatchPhase::Lobby,
        players: vec![PublicPlayer {
            name: "p".into(),
            team: TeamIdx::TEAM_0,
            ready: false,
        }],
        owner_seat_idx: 0,
    };

    let public_match = PublicMatch {
        id: "m".into(),
        options: MatchOptions::default(),
        phase: MatchPhase::Lobby,
        players: vec![PublicPlayer {
            name: "p".into(),
            team: TeamIdx::TEAM_0,
            ready: false,
        }],
        owner_seat_idx: 0,
        team_points: [0, 0],
    };

    let public_game = PublicGameState {
        hand_state: HandState::WaitingPlay,
        forehand_seat_idx: 0,
        turn_seat_idx: 0,
        rounds: vec![vec![PlayedCard {
            seat_idx: 0,
            card: "1e".into(),
        }]],
        winner_team_idx: Maybe(None),
    };

    let chat_user = PublicChatUser {
        name: "u".into(),
        seat_idx: Maybe(None),
        team: Maybe(None),
    };

    let chat_msg = PublicChatMessage {
        id: "x".into(),
        date_ms: 0,
        user: chat_user,
        system: false,
        content: "hi".into(),
    };

    let c2s_types = vec![
        type_of(C2sMessage::Ping(PingData { client_time_ms: 0 })),
        type_of(C2sMessage::LobbySnapshotGet),
        type_of(C2sMessage::MatchCreate(MatchCreateData {
            name: "n".into(),
            team: Default::default(),
            options: Default::default(),
        })),
        type_of(C2sMessage::MatchJoin(MatchJoinData {
            match_id: "m".into(),
            name: "n".into(),
            team: Default::default(),
        })),
        type_of(C2sMessage::MatchLeave(MatchRefData {
            match_id: "m".into(),
        })),
        type_of(C2sMessage::MatchReady(MatchReadyData {
            match_id: "m".into(),
            ready: true,
        })),
        type_of(C2sMessage::MatchSnapshotGet(MatchRefData {
            match_id: "m".into(),
        })),
        type_of(C2sMessage::GameSnapshotGet(MatchRefData {
            match_id: "m".into(),
        })),
        type_of(C2sMessage::MatchStart(MatchRefData {
            match_id: "m".into(),
        })),
        type_of(C2sMessage::MatchPause(MatchRefData {
            match_id: "m".into(),
        })),
        type_of(C2sMessage::MatchResume(MatchRefData {
            match_id: "m".into(),
        })),
        type_of(C2sMessage::ChatJoin(ChatJoinData {
            room_id: "r".into(),
        })),
        type_of(C2sMessage::ChatSay(ChatSayData {
            room_id: "r".into(),
            content: "c".into(),
        })),
        type_of(C2sMessage::GamePlayCard(GamePlayCardData {
            match_id: "m".into(),
            card_idx: 0,
        })),
        type_of(C2sMessage::GameSay(GameSayData {
            match_id: "m".into(),
            command: super::schema::GameCommand::Truco,
        })),
    ];

    let s2c_types = vec![
        type_of(S2cMessage::Pong(PongData {
            server_time_ms: 0,
            client_time_ms: 0,
        })),
        type_of(S2cMessage::Hello(HelloData {
            session_id: "s".into(),
            server_version: "v".into(),
        })),
        type_of(S2cMessage::LobbySnapshot(LobbySnapshotData {
            matches: vec![lobby_match.clone()],
        })),
        type_of(S2cMessage::LobbyMatchUpsert(LobbyMatchUpsertData {
            match_: lobby_match,
        })),
        type_of(S2cMessage::LobbyMatchRemove(LobbyMatchRemoveData {
            match_id: "m".into(),
        })),
        type_of(S2cMessage::MatchSnapshot(MatchSnapshotData {
            match_: public_match.clone(),
            me: Default::default(),
        })),
        type_of(S2cMessage::MatchUpdate(MatchUpdateData {
            match_: public_match,
            me: Default::default(),
        })),
        type_of(S2cMessage::MatchLeft(MatchLeftData {
            match_id: "m".into(),
        })),
        type_of(S2cMessage::GameSnapshot(GameSnapshotData {
            match_id: "m".into(),
            game: public_game.clone(),
        })),
        type_of(S2cMessage::GameUpdate(GameUpdateData {
            match_id: "m".into(),
            game: public_game,
        })),
        type_of(S2cMessage::ChatSnapshot(ChatSnapshotData {
            room: PublicChatRoom {
                id: "r".into(),
                messages: vec![chat_msg.clone()],
            },
        })),
        type_of(S2cMessage::ChatMessage(ChatMessageData {
            room_id: "r".into(),
            message: chat_msg,
        })),
        type_of(S2cMessage::Error(ErrorPayload {
            code: "X".into(),
            message: "Y".into(),
        })),
    ];

    for t in c2s_types.iter().chain(s2c_types.iter()) {
        assert!(
            is_ws_type_string(t),
            "ws message type must be dot-separated snake_case: {t}"
        );
    }

    // Also ensure there are no accidental duplicates within each direction.
    let uniq_c2s: std::collections::HashSet<_> = c2s_types.iter().collect();
    assert_eq!(
        uniq_c2s.len(),
        c2s_types.len(),
        "duplicate client->server ws message type strings"
    );

    let uniq_s2c: std::collections::HashSet<_> = s2c_types.iter().collect();
    assert_eq!(
        uniq_s2c.len(),
        s2c_types.len(),
        "duplicate server->client ws message type strings"
    );
}
