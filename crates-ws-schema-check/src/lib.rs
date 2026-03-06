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

    fn is_valid_ws_v2_field_name(s: &str) -> bool {
        // Snake_case field name, e.g. "match_id".
        // - must start/end with [a-z0-9]
        // - allowed chars: [a-z0-9_]
        // - no consecutive underscores
        if s.is_empty() {
            return false;
        }

        let b = s.as_bytes();
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

    fn assert_properties_and_required_are_strict_snake_case(
        v: &serde_json::Value,
        filename: &str,
        json_path: &str,
    ) {
        match v {
            serde_json::Value::Object(map) => {
                if let Some(serde_json::Value::Object(props)) = map.get("properties") {
                    let prop_names: std::collections::BTreeSet<&str> =
                        props.keys().map(|k| k.as_str()).collect();

                    for name in props.keys() {
                        assert!(
                            is_valid_ws_v2_field_name(name),
                            "invalid ws v2 field name {name:?} in schemas/ws/v2/{filename} at {json_path}/properties"
                        );
                    }

                    if let Some(serde_json::Value::Array(required)) = map.get("required") {
                        for r in required {
                            let serde_json::Value::String(name) = r else {
                                continue;
                            };

                            assert!(
                                is_valid_ws_v2_field_name(name),
                                "invalid ws v2 required field name {name:?} in schemas/ws/v2/{filename} at {json_path}/required"
                            );

                            assert!(
                                prop_names.contains(name.as_str()),
                                "schemas/ws/v2/{filename}: required field {name:?} is missing from properties at {json_path}"
                            );
                        }
                    }
                }

                for (k, v) in map {
                    let child_path = format!("{json_path}/{k}");
                    assert_properties_and_required_are_strict_snake_case(v, filename, &child_path);
                }
            }
            serde_json::Value::Array(xs) => {
                for (i, x) in xs.iter().enumerate() {
                    let child_path = format!("{json_path}[{i}]");
                    assert_properties_and_required_are_strict_snake_case(x, filename, &child_path);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn ws_v2_schema_property_names_are_strict_snake_case() {
        let root = repo_root();

        for filename in ["in.json", "out.json", "c2s.json", "s2c.json"] {
            let schema = read_json(&root.join("schemas/ws/v2").join(filename));
            assert_properties_and_required_are_strict_snake_case(&schema, filename, "$");
        }
    }

    #[test]
    fn ws_v2_typescript_files_are_up_to_date() {
        // The JSON schema drift check above ensures schemas/ws/v2/*.json are in sync with
        // `ws_schema_v2` (Rust). This adds a second guard for the generated TypeScript
        // types under schemas/ws/v2/*.ts.
        let root = repo_root();

        let script = root.join("scripts/gen-ws-types.mjs");
        assert!(script.exists(), "expected {script:?} to exist");

        let output = std::process::Command::new("node")
            .current_dir(&root)
            .arg(script)
            .arg("--check")
            .output()
            .expect("run node scripts/gen-ws-types.mjs --check");

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            panic!(
                "schemas/ws/v2/*.ts is out of date; run: npm run gen:ws\n\nstdout:\n{stdout}\n\nstderr:\n{stderr}"
            );
        }
    }
}
