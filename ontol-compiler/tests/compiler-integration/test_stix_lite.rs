use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches, type_binding::TypeBinding, TestCompile,
};
use serde_json::json;
use test_log::test;

pub const STIX_ON: &str = include_str!("../../../examples/stix_lite/stix.on");

#[test]
fn test_stix_lite() {
    STIX_ON.compile_ok(|env| {
        let attack_pattern = TypeBinding::new(&env, "attack-pattern");
        assert_error_msg!(
            attack_pattern.deserialize_data(json!({
                "type": "attack-pattern",
            })),
            r#"missing properties, expected all of "spec_version", "created", "modified", "name" at line 1 column 25"#
        );

        // BUG: In this domain at least, the ID is allocated externally.
        // the "POST" body is currently a UNION of (id OR data), instead of an intersection
        assert_json_io_matches!(
            attack_pattern,
            json!({
                "type": "attack-pattern",
                "spec_version": "2.1",
                "created": "2023-01-01T00:00:00+00:00",
                "modified": "2023-01-01T00:00:00+00:00",
                "name": "My attack pattern"
            })
        );

        assert_json_io_matches!(
            attack_pattern,
            json!({
                "type": "attack-pattern",
                "spec_version": "2.1",
                "created": "2023-01-01T00:00:00+00:00",
                "modified": "2023-01-01T00:00:00+00:00",
                "name": "My attack pattern",
                "created_by_ref": "identity--a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
            })
        );
    });
}
