use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches, serde_utils::*, SourceName, TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;

const STIX_COMMON: &str = include_str!("../../../examples/stix_lite/stix_common.on");
const STIX_META: &str = include_str!("../../../examples/stix_lite/stix_meta.on");
const STIX_DOMAIN: &str = include_str!("../../../examples/stix_lite/stix_domain.on");

fn stix_bundle() -> TestPackages {
    TestPackages::with_sources([
        (SourceName::root(), STIX_DOMAIN),
        (SourceName("stix_meta"), STIX_META),
        (SourceName("stix_common"), STIX_COMMON),
    ])
}

#[test]
fn test_stix_lite() {
    stix_bundle().compile_ok(|test| {
        let [attack_pattern] = test.bind(["attack-pattern"]);
        assert_error_msg!(
            create_de(&attack_pattern).data(json!({
                "type": "attack-pattern",
            })),
            r#"missing properties, expected all of "spec_version", "created", "modified", "name" at line 1 column 25"#
        );

        // BUG: In this domain at least, the ID is allocated externally.
        // the "POST" body is currently a UNION of (id OR data), instead of an intersection
        assert_json_io_matches!(attack_pattern, Create, {
            "type": "attack-pattern",
            "spec_version": "2.1",
            "created": "2023-01-01T00:00:00+00:00",
            "modified": "2023-01-01T00:00:00+00:00",
            "name": "My attack pattern",
            "confidence": 42,
        });

        assert_json_io_matches!(attack_pattern, Create, {
            "type": "attack-pattern",
            "spec_version": "2.1",
            "created": "2023-01-01T00:00:00+00:00",
            "modified": "2023-01-01T00:00:00+00:00",
            "name": "My attack pattern",
            "created_by_ref": "identity--a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
        });
    });
}
