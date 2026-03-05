//! JSON Schema helpers for the WebSocket protocol (v2).
//!
//! This module is behind the `json-schema` feature so the main dependency surface stays minimal.

#![cfg(feature = "json-schema")]

use schemars::{schema::RootSchema, schema_for};

use super::{C2sMessage, S2cMessage, WsInMessage, WsOutMessage};

/// JSON schema for inbound WS frames (`client -> server`).
pub fn ws_in_message_schema() -> RootSchema {
    schema_for!(WsInMessage)
}

/// JSON schema for outbound WS frames (`server -> client`).
pub fn ws_out_message_schema() -> RootSchema {
    schema_for!(WsOutMessage)
}

/// JSON schema for the inbound message payload (`client -> server`).
pub fn c2s_message_schema() -> RootSchema {
    schema_for!(C2sMessage)
}

/// JSON schema for the outbound message payload (`server -> client`).
pub fn s2c_message_schema() -> RootSchema {
    schema_for!(S2cMessage)
}
