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
