use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches,
    examples::{
        stix::{STIX, STIX_COMMON, STIX_META, STIX_OPEN_VOCAB},
        Root, SI,
    },
    serde_helper::*,
    TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;

fn stix_bundle() -> TestPackages {
    TestPackages::with_sources([STIX.root(), STIX_META, STIX_COMMON, STIX_OPEN_VOCAB, SI])
}

#[test]
fn test_stix_lite() {
    let test = stix_bundle().compile();
    let [attack_pattern] = test.bind(["attack-pattern"]);
    assert_error_msg!(
        serde_create(&attack_pattern).to_data(json!({
            "type": "attack-pattern",
        })),
        r#"missing properties, expected all of "name", "spec_version", "created", "modified" at line 1 column 25"#
    );

    // BUG: In this domain at least, the ID is allocated externally.
    // the "POST" body is currently a UNION of (id OR data), instead of an intersection
    assert_json_io_matches!(serde_create(&attack_pattern), {
        "type": "attack-pattern",
        "spec_version": "2.1",
        "created": "2023-01-01T00:00:00+00:00",
        "modified": "2023-01-01T00:00:00+00:00",
        "name": "My attack pattern",
        "confidence": 42,
    });

    assert_json_io_matches!(serde_create(&attack_pattern), {
        "type": "attack-pattern",
        "spec_version": "2.1",
        "created": "2023-01-01T00:00:00+00:00",
        "modified": "2023-01-01T00:00:00+00:00",
        "name": "My attack pattern",
        "created_by_ref": "identity--a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
    });
}
