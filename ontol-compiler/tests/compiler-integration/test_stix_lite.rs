use ontol_test_utils::{assert_json_io_matches, type_binding::TypeBinding, TestCompile};
use serde_json::json;
use test_log::test;

pub const STIX_ONT: &str = include_str!("../../../examples/stix_lite/stix.ont");

#[test]
// BUG: Something weird going on in many-cardinality:
#[should_panic = "While serializing value"]
fn test_stix_lite() {
    STIX_ONT.compile_ok(|env| {
        let attack_pattern = TypeBinding::new(&env, "attack_pattern");
        assert_json_io_matches!(
            attack_pattern,
            json!({
                "type": "attack-pattern",
                "spec_version": "2.1",
                "created_by_ref": "identity--a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
                "created": "2023-01-01T00:00:00.000Z",
                "modified": "2023-01-01T00:00:00.000Z"
            })
        );
    });
}
