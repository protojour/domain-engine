use ontol_examples::stix::stix_bundle;
use ontol_macros::test;
use ontol_runtime::{debug::OntolDebug, ontology::domain::DefRepr, tuple::CardinalIdx, DefIdSet};
use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches, serde_helper::*, OntolTest, TestCompile,
};
use serde_json::json;

#[test]
fn test_stix_lite() {
    let test = stix_bundle().compile();
    stix_ontology_smoke(&test);

    let [stix_object, attack_pattern, relationship] =
        test.bind(["stix-object", "attack-pattern", "relationship"]);
    assert_error_msg!(
        serde_create(&attack_pattern).to_value(json!({
            "type": "attack-pattern",
        })),
        r#"missing properties, expected all of "spec_version", "created", "modified", "name" at line 1 column 25"#
    );

    // can create through concrete type
    assert_json_io_matches!(serde_create(&attack_pattern), {
        "id": "attack-pattern--11111111-1111-1111-1111-111111111111",
        "type": "attack-pattern",
        "spec_version": "2.1",
        "created": "2023-01-01T00:00:00Z",
        "modified": "2023-01-01T00:00:00Z",
        "name": "My attack pattern",
        "confidence": 42,
    });

    // can create through type union
    assert_json_io_matches!(serde_create(&stix_object), {
        "id": "attack-pattern--11111111-1111-1111-1111-111111111111",
        "type": "attack-pattern",
        "spec_version": "2.1",
        "created": "2023-01-01T00:00:00Z",
        "modified": "2023-01-01T00:00:00Z",
        "name": "My attack pattern",
        "confidence": 42,
    });

    assert_json_io_matches!(serde_create(&attack_pattern), {
        "type": "attack-pattern",
        "spec_version": "2.1",
        "created": "2023-01-01T00:00:00Z",
        "modified": "2023-01-01T00:00:00Z",
        "name": "My attack pattern",
        "created_by_ref": "identity--a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
    });

    assert_json_io_matches!(serde_create(&relationship), {
        "type": "relationship",
        "id": "relationship--0008005f-ca51-47c3-8369-55ee5de1c65a",
        "created": "2017-12-14T16:46:06.044Z",
        // "x_mitre_version": "1.0",
        "external_references": [
            {
                "source_name": "Zscaler-SpyNote",
                "url": "https://www.zscaler.com/blogs/research/spynote-rat-posing-netflix-app",
                "description": "Shivang Desai. (2017, January 23). SpyNote RAT posing as Netflix app. Retrieved January 26, 2017."
            }
        ],
        // "x_mitre_deprecated": false,
        "revoked": false,
        "description": "[SpyNote RAT](https://attack.mitre.org/software/S0305) uses an Android broadcast receiver to automatically start when the device boots.(Citation: Zscaler-SpyNote)",
        "modified": "2022-04-12T10:01:44.682Z",
        "created_by_ref": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
        "relationship_type": "uses",
        "source_ref": "malware--20dbaf05-59b8-4dc6-8777-0b17f4553a23",
        "target_ref": "attack-pattern--3775a580-a1d1-46c4-8147-c614a715f2e9",
        // "x_mitre_attack_spec_version": "2.1.0",
        // "x_mitre_modified_by_ref": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
        "spec_version": "2.1",
        // "x_mitre_domains": ["mobile-attack"]
    });
}

#[test]
fn test_stix_object_course_of_action() {
    let test = stix_bundle().compile();

    let [stix_object] = test.bind(["stix-object"]);

    assert_json_io_matches!(serde_create(&stix_object), {
        "spec_version": "2.1",
        "type": "course-of-action",
        "id": "course-of-action--0beabf44-e8d8-4ae4-9122-ef56369a2564",
        "created_by_ref": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
        "created": "2017-10-25T14:48:51.657Z",
        "modified": "2018-01-17T12:56:55.080Z",
        "name": "Use Recent OS Version",
        "description": "New mobile operating system versions bring not only patches against discovered vulnerabilities but also often bring security architecture improvements that provide resilience against potential vulnerabilities or weaknesses that have not yet been discovered. They may also bring improvements that block use of observed adversary techniques.",
        "external_references": [
            {
                "source_name": "mitre-attack-mobile",
                "url": "https://attack.mitre.org/mobile/index.php/Mitigation/MOB-M1006",
                "external_id": "MOB-M1006"
            }
        ],
        "object_marking_refs": ["marking-definition--fa42a846-8d90-4e51-bc29-71d5b4802168"],
    });
}

fn stix_ontology_smoke(test: &OntolTest) {
    let [user_account, attack_pattern, process, windows_registry_key] = test.bind([
        "user-account",
        "attack-pattern",
        "process",
        "windows-registry-key",
    ]);
    let [string] = test.bind(["stix_common.string"]);

    let (_, creator_user_ref) = windows_registry_key
        .def
        .data_relationship_by_name("creator_user_ref", test.ontology())
        .unwrap();
    let creator_edge_projection = creator_user_ref.edge_kind().unwrap();

    assert_eq!(creator_edge_projection.subject, CardinalIdx(1));
    assert_eq!(creator_edge_projection.object, CardinalIdx(0));

    let reg_key_user_account_edge_info = test
        .ontology()
        .find_edge(creator_edge_projection.edge_id)
        .unwrap();

    assert_eq!(
        &reg_key_user_account_edge_info.cardinals[1].target,
        &DefIdSet::from_iter([windows_registry_key.def_id(), process.def_id()])
    );
    assert_eq!(
        &reg_key_user_account_edge_info.cardinals[0].target,
        &DefIdSet::from_iter([user_account.def_id()])
    );

    let (_, aliases) = attack_pattern
        .def
        .data_relationship_by_name("aliases", test.ontology())
        .unwrap();
    let aliases_def = test.ontology().def(aliases.target.def_id());
    let aliases_repr = aliases_def.repr();
    let Some(DefRepr::Text) = aliases_repr else {
        panic!("aliases_repr was {:?}", aliases_repr.debug(test.ontology()));
    };

    let Some(DefRepr::Text) = test.ontology().def(string.def_id()).repr() else {
        panic!("ontol common string was not DefReprText");
    };
}
