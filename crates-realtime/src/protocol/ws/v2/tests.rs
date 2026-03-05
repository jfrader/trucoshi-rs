use super::{C2sMessage, S2cMessage, WsInMessage, WsOutMessage};

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
