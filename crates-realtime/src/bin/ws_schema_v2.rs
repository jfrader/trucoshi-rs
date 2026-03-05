//! Dumps JSON Schemas for the WebSocket protocol (v2).
//!
//! Usage:
//!   cargo run -p trucoshi-realtime --features json-schema --bin ws_schema_v2
//!   cargo run -p trucoshi-realtime --features json-schema --bin ws_schema_v2 -- ./schemas/ws/v2
//!
//! If an output directory is provided, four files are written:
//!   - in.json (client -> server frame envelope)
//!   - out.json (server -> client frame envelope)
//!   - c2s.json (client -> server payload only)
//!   - s2c.json (server -> client payload only)
//!
//! The filenames intentionally match the schema `$id` paths:
//!   - https://trucoshi.dev/schemas/ws/v2/in.json
//!   - https://trucoshi.dev/schemas/ws/v2/out.json
//!   - https://trucoshi.dev/schemas/ws/v2/c2s.json
//!   - https://trucoshi.dev/schemas/ws/v2/s2c.json

#[cfg(feature = "json-schema")]
use schemars::schema::Metadata;

#[cfg(feature = "json-schema")]
use std::{env, fs, path::PathBuf};

#[cfg(not(feature = "json-schema"))]
fn main() {
    eprintln!(
        "ws_schema_v2 requires the `json-schema` feature.\n\nTry:\n  cargo run -p trucoshi-realtime --features json-schema --bin ws_schema_v2 -- ./schemas/ws/v2"
    );
    std::process::exit(2);
}

#[cfg(feature = "json-schema")]
fn main() {
    let out_dir: Option<PathBuf> = env::args().nth(1).map(PathBuf::from);

    let mut in_schema = trucoshi_realtime::protocol::ws::v2::json_schema::ws_in_message_schema();
    let mut out_schema = trucoshi_realtime::protocol::ws::v2::json_schema::ws_out_message_schema();

    let mut c2s_schema = trucoshi_realtime::protocol::ws::v2::json_schema::c2s_message_schema();
    let mut s2c_schema = trucoshi_realtime::protocol::ws::v2::json_schema::s2c_message_schema();

    // Add stable $id values so downstream tooling can cache/version safely.
    in_schema.schema.metadata = Some(Box::new(Metadata {
        id: Some("https://trucoshi.dev/schemas/ws/v2/in.json".to_string()),
        title: Some(
            "Trucoshi WebSocket Protocol v2 — Inbound Frame (client -> server)".to_string(),
        ),
        ..Default::default()
    }));
    out_schema.schema.metadata = Some(Box::new(Metadata {
        id: Some("https://trucoshi.dev/schemas/ws/v2/out.json".to_string()),
        title: Some(
            "Trucoshi WebSocket Protocol v2 — Outbound Frame (server -> client)".to_string(),
        ),
        ..Default::default()
    }));

    c2s_schema.schema.metadata = Some(Box::new(Metadata {
        id: Some("https://trucoshi.dev/schemas/ws/v2/c2s.json".to_string()),
        title: Some("Trucoshi WebSocket Protocol v2 — Client -> Server Payload".to_string()),
        ..Default::default()
    }));
    s2c_schema.schema.metadata = Some(Box::new(Metadata {
        id: Some("https://trucoshi.dev/schemas/ws/v2/s2c.json".to_string()),
        title: Some("Trucoshi WebSocket Protocol v2 — Server -> Client Payload".to_string()),
        ..Default::default()
    }));

    // NOTE: the `v` field schema is enforced by the `WsVersion` type itself (const == 2), so we
    // don't manually patch the generated schema here.

    let in_json = serde_json::to_string_pretty(&in_schema).expect("serialize in schema");
    let out_json = serde_json::to_string_pretty(&out_schema).expect("serialize out schema");
    let c2s_json = serde_json::to_string_pretty(&c2s_schema).expect("serialize c2s schema");
    let s2c_json = serde_json::to_string_pretty(&s2c_schema).expect("serialize s2c schema");

    match out_dir {
        None => {
            println!("--- ws-v2-in.json ---\n{}\n", in_json);
            println!("--- ws-v2-out.json ---\n{}\n", out_json);
            println!("--- ws-v2-c2s.json ---\n{}\n", c2s_json);
            println!("--- ws-v2-s2c.json ---\n{}\n", s2c_json);
        }
        Some(dir) => {
            fs::create_dir_all(&dir).expect("create output dir");
            fs::write(dir.join("in.json"), &in_json).expect("write in.json");
            fs::write(dir.join("out.json"), &out_json).expect("write out.json");
            fs::write(dir.join("c2s.json"), &c2s_json).expect("write c2s.json");
            fs::write(dir.join("s2c.json"), &s2c_json).expect("write s2c.json");
        }
    }
}
