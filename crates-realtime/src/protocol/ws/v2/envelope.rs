use serde::{Deserialize, Serialize};

#[cfg(feature = "json-schema")]
use schemars::JsonSchema;

use super::schema::Maybe;
use super::{C2sMessage, S2cMessage};

/// WebSocket protocol version for `ws::v2`.
pub const WS_PROTOCOL_VERSION: u16 = 2;

/// Typed protocol version for v2 envelopes.
///
/// We intentionally **fail deserialization** if the version is not exactly `2`, to
/// keep the protocol strict and avoid legacy compatibility quirks.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u16", into = "u16")]
pub struct WsVersion(pub u16);

#[cfg(feature = "json-schema")]
impl JsonSchema for WsVersion {
    fn schema_name() -> String {
        "WsVersion".to_string()
    }

    fn json_schema(r#gen: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        use schemars::schema::{InstanceType, Schema, SchemaObject, SingleOrVec};
        use serde_json::json;

        let mut obj = SchemaObject {
            instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Integer))),
            format: None,
            ..Default::default()
        };

        // Enforce the exact protocol version in the generated schema.
        obj.const_value = Some(json!(WS_PROTOCOL_VERSION));

        // Preserve u16-ish bounds for nicer tooling (even though `const` already pins it).
        obj.number = Some(Box::new(schemars::schema::NumberValidation {
            minimum: Some(WS_PROTOCOL_VERSION.into()),
            maximum: Some(WS_PROTOCOL_VERSION.into()),
            ..Default::default()
        }));

        // Also register the underlying schema to keep generators happy.
        let _ = r#gen.subschema_for::<u16>();

        Schema::Object(obj)
    }
}

impl Default for WsVersion {
    fn default() -> Self {
        Self(WS_PROTOCOL_VERSION)
    }
}

impl From<WsVersion> for u16 {
    fn from(v: WsVersion) -> Self {
        v.0
    }
}

impl TryFrom<u16> for WsVersion {
    type Error = String;

    fn try_from(v: u16) -> Result<Self, Self::Error> {
        if v == WS_PROTOCOL_VERSION {
            Ok(Self(v))
        } else {
            Err(format!(
                "unsupported ws protocol version {}; expected {}",
                v, WS_PROTOCOL_VERSION
            ))
        }
    }
}

/// WebSocket inbound frame (client -> server).
///
/// Protocol v2 uses an explicit `v` version field and a tagged event payload.
///
/// Wire format (v2):
///
/// ```json
/// {
///   "v": 2,
///   "id": "optional-correlation-id",
///   "msg": { "type": "ping", "data": { "client_time_ms": 123 } }
/// }
///
/// All message `type` strings use dot-separated namespaces and `snake_case`.
///
/// Field names within each message use Rust's field names (snake_case).
/// ```
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WsInMessage {
    /// Protocol version (must be exactly 2).
    pub v: WsVersion,

    /// Tagged payload.
    pub msg: C2sMessage,

    /// Optional correlation id (client sets; server echoes in responses).
    ///
    /// Missing: allowed. Explicit `null`: rejected.
    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub id: Maybe<String>,
}

/// WebSocket outbound frame (server -> client).
#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WsOutMessage {
    /// Protocol version (must be exactly 2).
    pub v: WsVersion,

    /// Tagged payload.
    pub msg: S2cMessage,

    /// Optional correlation id (client sets; server echoes in responses).
    ///
    /// Missing: allowed. Explicit `null`: rejected.
    #[serde(default, skip_serializing_if = "Maybe::is_none")]
    pub id: Maybe<String>,
}
