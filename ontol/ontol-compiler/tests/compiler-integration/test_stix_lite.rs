use ontol_macros::test;
use ontol_runtime::{tuple::CardinalIdx, DefIdSet};
use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches, examples::stix::stix_bundle, serde_helper::*,
    OntolTest, TestCompile,
};
use serde_json::json;

#[test]
fn test_stix_lite() {
    let test = stix_bundle().compile();
    stix_ontology_smoke(&test);

    let [attack_pattern] = test.bind(["attack-pattern"]);
    assert_error_msg!(
        serde_create(&attack_pattern).to_value(json!({
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

fn stix_ontology_smoke(test: &OntolTest) {
    let [windows_registry_key, user_account] = test.bind(["windows-registry-key", "user-account"]);

    let (_, creator_user_ref) = windows_registry_key
        .def
        .data_relationship_by_name("creator_user_ref", test.ontology())
        .unwrap();
    let creator_edge_projection = creator_user_ref.edge_kind().unwrap();

    assert_eq!(creator_edge_projection.subject, CardinalIdx(0));
    assert_eq!(creator_edge_projection.object, CardinalIdx(1));

    let (_, reg_key_user_account_edge_info) = test
        .ontology()
        .find_domain(test.root_package())
        .unwrap()
        .edges()
        .find(|(edge_id, _)| *edge_id == &creator_edge_projection.id)
        .unwrap();

    assert_eq!(
        &reg_key_user_account_edge_info.cardinals[0].target,
        &DefIdSet::from_iter([windows_registry_key.def_id()])
    );
    assert_eq!(
        &reg_key_user_account_edge_info.cardinals[1].target,
        &DefIdSet::from_iter([user_account.def_id()])
    );
}
