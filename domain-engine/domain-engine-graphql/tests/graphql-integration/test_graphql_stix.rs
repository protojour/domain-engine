use domain_engine_graphql::{context::ServiceCtx, juniper::graphql_value};
use domain_engine_test_utils::graphql_test_utils::{Exec, TestCompileSchema};
use ontol_macros::datastore_test;
use ontol_test_utils::{
    examples::stix::{stix_bundle, STIX},
    expect_eq,
};

use crate::mk_engine_default;

/// There should only be one stix test since the domain is so big
/// FIXME: Arango doesn't yet understand ONTOL symbolic edges.
/// FIXME: GraphQL mutations do not run in the same transaction, so foreign key defer does not work
#[datastore_test(tokio::test)]
async fn test_graphql_stix(ds: &str) {
    let (test, [schema]) = stix_bundle().compile_schemas([STIX.0]);
    let ctx: ServiceCtx = mk_engine_default(test.ontology_owned(), ds).await.into();

    // first, create an identity
    r#"mutation {
        identity(create:[
            {
                id: "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                modified: "2017-06-01T00:00:00.000Z",
                object_marking_refs: [],
                name: "The MITRE Corporation",
                created: "2017-06-01T00:00:00.000Z",
                type: "identity",
                identity_class: "organization",
                spec_version: "2.1",
            }
        ]) {
            node { id }
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    expect_eq!(
        actual = r#"mutation {
            url(create:[
                {
                    type: "url"
                    value: "http://jøkkagnork"
                    defanged:true
                    object_marking_refs: []
                    granular_markings: []
                }
            ]) {
                node { defanged }
            }
            attack_pattern(create:[
                {
                    type: "attack-pattern",
                    id: "attack-pattern--2204c371-6100-4ae0-82f3-25c07c29772a",
                    created_by_ref: "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                    created: "2017-10-25T14:48:08.613Z",
                    modified: "2018-01-17T12:56:55.080Z",
                    name: "Abuse Accessibility Features",
                    description: "A malicious app could abuse Android's accessibility features to capture sensitive data or perform other malicious actions, as demonstrated in a proof of concept created by Skycure (Citation: Skycure-Accessibility).\n\nPlatforms: Android",
                    aliases: ["aliasA", "aliasB"],
                    kill_chain_phases: [
                        {
                            kill_chain_name: "mitre-mobile-attack",
                            phase_name: "collection"
                        },
                        {
                            kill_chain_name: "mitre-mobile-attack",
                            phase_name: "credential-access"
                        }
                    ],
                    external_references: [
                        {
                            source_name: "mitre-mobile-attack",
                            url: "https://attack.mitre.org/mobile/index.php/Technique/MOB-T1056",
                            external_id: "MOB-T1056"
                        },
                        {
                            source_name: "Skycure-Accessibility",
                            description: "Yair Amit. (2016, March 3). \u201cAccessibility Clickjacking\u201d \u2013 The Next Evolution in Android Malware that Impacts More Than 500 Million Devices. Retrieved December 21, 2016.",
                            url: "https://www.skycure.com/blog/accessibility-clickjacking/"
                        }
                    ],

                    # FIXME: GraphQL mutations do not run in the same transaction
                    # object_marking_refs: ["marking-definition--fa42a846-8d90-4e51-bc29-71d5b4802168"],
                    object_marking_refs: [],

                    # "x_mitre_platforms": ["Android"],
                    # "x_mitre_tactic_type": ["Post-Adversary Device Access"],
                    spec_version: "2.1",
                    # "x_mitre_attack_spec_version": "2.1.0",
                    # "x_mitre_domains": ["mobile-attack"],
                    # "x_mitre_modified_by_ref": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                    # "x_mitre_version": "1.0"
                },
            ]) {
                node { id }
            }
            course_of_action(create:[
                {
                    type: "course-of-action",
                    id: "course-of-action--0beabf44-e8d8-4ae4-9122-ef56369a2564",
                    created_by_ref: "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                    created: "2017-10-25T14:48:51.657Z",
                    modified: "2018-01-17T12:56:55.080Z",
                    name: "Use Recent OS Version",
                    description: "New mobile operating system versions bring not only patches against discovered vulnerabilities but also often bring security architecture improvements that provide resilience against potential vulnerabilities or weaknesses that have not yet been discovered. They may also bring improvements that block use of observed adversary techniques.",
                    external_references: [
                        {
                            source_name: "mitre-attack-mobile",
                            url: "https://attack.mitre.org/mobile/index.php/Mitigation/MOB-M1006",
                            external_id: "MOB-M1006"
                        }
                    ],
                    # object_marking_refs: ["marking-definition--fa42a846-8d90-4e51-bc29-71d5b4802168"],
                    object_marking_refs: [],
                    spec_version: "2.1",
                    # "x_mitre_attack_spec_version": "2.1.0",
                    # "x_mitre_domains": ["mobile-attack"],
                    # "x_mitre_modified_by_ref": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                    # "x_mitre_version": "1.0"
                }
            ]) {
                node { id }
            }
            relationship(create:[
                {
                    type: "relationship",
                    id: "relationship--55f12292-dc9d-4bfd-9de9-2d07cd67b044",
                    created_by_ref: "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                    created: "2017-10-25T14:48:53.734Z",
                    modified: "2018-01-17T12:56:55.080Z",
                    relationship_type: "mitigates",
                    source_ref: "course-of-action--0beabf44-e8d8-4ae4-9122-ef56369a2564",
                    target_ref: "attack-pattern--2204c371-6100-4ae0-82f3-25c07c29772a",
                    # object_marking_refs: ["marking-definition--fa42a846-8d90-4e51-bc29-71d5b4802168"],
                    object_marking_refs: [],
                    spec_version: "2.1",
                    # "x_mitre_attack_spec_version": "2.1.0",
                    # "x_mitre_domains": ["mobile-attack"],
                    # "x_mitre_modified_by_ref": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                    # "x_mitre_version": "1.0"
                }
            ]) {
                node { id }
            }
        }
        "#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "url": [{ "node": { "defanged": true } }],
            "attack_pattern": [{ "node": { "id": "attack-pattern--2204c371-6100-4ae0-82f3-25c07c29772a" }}],
            "course_of_action": [{ "node": { "id": "course-of-action--0beabf44-e8d8-4ae4-9122-ef56369a2564" }}],
            "relationship": [{ "node": { "id": "relationship--55f12292-dc9d-4bfd-9de9-2d07cd67b044" }}],
        })),
    );
}
