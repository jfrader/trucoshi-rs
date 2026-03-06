#[cfg(test)]
mod tests {
    use schemars::schema::{Metadata, RootSchema};

    fn read_json(path: &std::path::Path) -> serde_json::Value {
        let txt = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path:?}: {e}"));
        serde_json::from_str(&txt).unwrap_or_else(|e| panic!("parse {path:?}: {e}"))
    }

    fn assert_schema_file_up_to_date(filename: &str, mut schema: RootSchema) {
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

        // Mirror the metadata patching performed by `ws_schema_v2`.
        schema.schema.metadata = Some(Box::new(Metadata {
            id: Some(id.to_string()),
            title: Some(title.to_string()),
            ..Default::default()
        }));

        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir
            .parent()
            .unwrap_or_else(|| panic!("expected schema-check crate to have a parent dir"));
        let path = repo_root.join("schemas/ws/v2").join(filename);

        let on_disk = read_json(&path);
        let generated = serde_json::to_value(&schema).expect("schema to value");

        assert_eq!(
            on_disk, generated,
            "schemas/ws/v2/{filename} is out of date; run: npm run gen:ws (repo root)"
        );
    }

    #[test]
    fn ws_v2_schema_files_are_up_to_date() {
        use trucoshi_realtime::protocol::ws::v2::json_schema::{
            c2s_message_schema, s2c_message_schema, ws_in_message_schema, ws_out_message_schema,
        };

        assert_schema_file_up_to_date("in.json", ws_in_message_schema());
        assert_schema_file_up_to_date("out.json", ws_out_message_schema());
        assert_schema_file_up_to_date("c2s.json", c2s_message_schema());
        assert_schema_file_up_to_date("s2c.json", s2c_message_schema());
    }
}
