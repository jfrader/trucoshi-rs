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

    fn repo_root() -> std::path::PathBuf {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .unwrap_or_else(|| panic!("expected schema-check crate to have a parent dir"))
            .to_path_buf()
    }

    fn is_valid_ws_v2_message_type(s: &str) -> bool {
        // Dot-separated snake_case segments, e.g. "match.snapshot.get".
        // - no empty segments
        // - segments must start/end with [a-z0-9]
        // - allowed chars: [a-z0-9_]
        // - no consecutive underscores
        fn is_valid_seg(seg: &str) -> bool {
            if seg.is_empty() {
                return false;
            }

            let b = seg.as_bytes();
            let is_alnum = |c: u8| matches!(c, b'a'..=b'z' | b'0'..=b'9');

            if !is_alnum(b[0]) || !is_alnum(b[b.len() - 1]) {
                return false;
            }

            let mut prev_underscore = false;
            for &c in b {
                match c {
                    b'a'..=b'z' | b'0'..=b'9' => prev_underscore = false,
                    b'_' => {
                        if prev_underscore {
                            return false;
                        }
                        prev_underscore = true;
                    }
                    _ => return false,
                }
            }

            true
        }

        s.split('.').all(is_valid_seg)
    }

    fn collect_message_types(schema: &serde_json::Value) -> std::collections::BTreeSet<String> {
        // Collect `properties.type.enum[]` values across all schema branches.
        fn walk(v: &serde_json::Value, out: &mut std::collections::BTreeSet<String>) {
            match v {
                serde_json::Value::Object(map) => {
                    if let Some(serde_json::Value::Object(props)) = map.get("properties") {
                        if let Some(serde_json::Value::Object(ty)) = props.get("type") {
                            if let Some(serde_json::Value::Array(vals)) = ty.get("enum") {
                                for x in vals {
                                    if let serde_json::Value::String(s) = x {
                                        out.insert(s.clone());
                                    }
                                }
                            }
                        }
                    }

                    for v in map.values() {
                        walk(v, out);
                    }
                }
                serde_json::Value::Array(xs) => {
                    for x in xs {
                        walk(x, out);
                    }
                }
                _ => {}
            }
        }

        let mut out = std::collections::BTreeSet::new();
        walk(schema, &mut out);
        out
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

    #[test]
    fn ws_v2_message_type_strings_are_strict_snake_case() {
        let root = repo_root();

        for filename in ["c2s.json", "s2c.json"] {
            let schema = read_json(&root.join("schemas/ws/v2").join(filename));
            let types = collect_message_types(&schema);

            assert!(
                !types.is_empty(),
                "expected to find message type enums in schemas/ws/v2/{filename}"
            );

            for t in types {
                assert!(
                    is_valid_ws_v2_message_type(&t),
                    "invalid ws v2 message type {t:?} in schemas/ws/v2/{filename}"
                );
            }
        }
    }
}
