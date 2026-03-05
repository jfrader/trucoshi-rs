use serde::{Deserialize, Serialize};

#[cfg(feature = "json-schema")]
use schemars::JsonSchema;

#[cfg_attr(feature = "json-schema", derive(JsonSchema))]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ErrorPayload {
    /// Machine-readable error code.
    pub code: String,

    /// Human-readable error message.
    ///
    /// Protocol v2 is strict: this field is always present.
    pub message: String,
}
