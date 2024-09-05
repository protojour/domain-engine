use domain_engine_core::Session;
use domain_engine_graphql::{
    context::ServiceCtx,
    gql_scalar::GqlScalar,
    ontology_schema::{OntologyCtx, OntologySchema},
};
use domain_engine_test_utils::graphql_test_utils::{Exec, TestCompileSchema};
use juniper::graphql_value;
use ontol_macros::datastore_test;
use ontol_test_utils::{
    examples::stix::{stix_bundle, STIX},
    expect_eq,
};
use tracing::info;

use crate::mk_engine_default;

#[derive(Default)]
pub struct OntologyParams {
    pub def_id: Option<String>,
}

impl From<OntologyParams> for juniper::Variables<GqlScalar> {
    fn from(value: OntologyParams) -> Self {
        let mut variables = juniper::Variables::default();
        if let Some(def_id) = value.def_id {
            variables.insert("defId".to_string(), def_id.to_string().into());
        }
        variables
    }
}

#[datastore_test(tokio::test)]
async fn test_stix_ontology(ds: &str) {
    let (test, [stix_schema]) = stix_bundle().compile_schemas([STIX.0]);
    let [identity] = test.bind(["identity"]);

    let engine = mk_engine_default(test.ontology_owned(), ds).await;
    let domain_ctx: ServiceCtx = engine.clone().into();
    let ontology_ctx = OntologyCtx::new(engine, Session::default());
    let ontology_schema = OntologySchema::new_with_scalar_value(
        Default::default(),
        Default::default(),
        Default::default(),
    );

    // insert Stix "identity" through domain schema
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
                roles: ["roleA", "roleB"],
                external_references: [
                    {
                        source_name: "mitre-mobile-attack",
                        url: "https://attack.mitre.org/mobile/index.php/Technique/MOB-T1056",
                        external_id: "MOB-T1056"
                    },
                ],
            }
        ]) {
            node { id }
        }
    }"#
    .exec([], &stix_schema, &domain_ctx)
    .await
    .unwrap();

    expect_eq!(
        actual = r"{ domains { id name } }"
            .exec([], &ontology_schema, &ontology_ctx)
            .await,
        expected = Ok(graphql_value!({
            "domains": [
                {
                    "id": "01GNYFZP30ED0EZ1579TH0D55P",
                    "name": "ontol",
                },
                {
                    "id": "01GZ13EAM0RY693MSJ75XZARHY",
                    "name": "stix",
                },
                {
                    "id": "01J5C5GB45Q3C0E4YYRPP8SN4R",
                    "name": "stix_common",
                },
                {
                    "id": "01J6SACJ2A3P8FNEQ2S20M3DV7",
                    "name": "stix_edges",
                },
                {
                    "id": "01H7ZN6PJ0NHEFMHW2PBN4FEPZ",
                    "name": "stix_interface",
                },
                {
                    "id": "01J5C5JJKM7XWR56TTYH9B7VRN",
                    "name": "stix_open_vocab",
                },
                {
                    "id": "01H7ZZ5KMG538CXJMNJQVTMP9K",
                    "name": "SI",
                },
            ]
        }))
    );

    expect_eq!(
        actual = r"
            query def($defId: DefId!) {
                def(defId: $defId) {
                    id
                    kind
                    dataRelationships {
                        propId
                        name
                    }
                }
            }
        "
        .exec(
            OntologyParams {
                def_id: Some(identity.graphql_def_id())
            },
            &ontology_schema,
            &ontology_ctx
        )
        .await,
        expected = Ok(graphql_value!({
            "def": {
                "id": "01GZ13EAM0RY693MSJ75XZARHY:54",
                "kind": "ENTITY",
                "dataRelationships": [
                    { "propId": "p@1:54:0", "name": "type" },
                    { "propId": "p@1:54:1", "name": "id" },
                    { "propId": "p@1:54:2", "name": "spec_version" },
                    { "propId": "p@1:54:3", "name": "created" },
                    { "propId": "p@1:54:4", "name": "modified" },
                    { "propId": "p@1:54:5", "name": "confidence" },
                    { "propId": "p@1:54:6", "name": "revoked" },
                    { "propId": "p@1:54:7", "name": "labels" },
                    { "propId": "p@1:54:8", "name": "lang" },
                    { "propId": "p@1:54:9", "name": "external_references" },
                    { "propId": "p@1:54:10", "name": "created_by_ref" },
                    { "propId": "p@1:54:11", "name": "object_marking_refs" },
                    { "propId": "p@1:54:12", "name": "granular_markings" },
                    { "propId": "p@1:54:13", "name": "name" },
                    { "propId": "p@1:54:14", "name": "description" },
                    { "propId": "p@1:54:15", "name": "roles" },
                    { "propId": "p@1:54:16", "name": "identity_class" },
                    { "propId": "p@1:54:17", "name": "sectors" },
                    { "propId": "p@1:54:18", "name": "contact_information" },
                ]
            }
        }))
    );

    expect_eq!(
        actual = r#"
            query vertices($defId: DefId!) {
                vertices(defId: $defId, first: 100, withAddress: false, withDefId: false) {
                    elements
                }
            }
            "#
        .exec(
            OntologyParams {
                def_id: Some(identity.graphql_def_id()),
            },
            &ontology_schema,
            &ontology_ctx,
        )
        .await,
        expected = Ok(graphql_value!({
            "vertices": {
                "elements": [
                    {
                        "type": "struct",
                        "attrs": [
                            {
                                "propId": "p@1:54:0",
                                "attr": "unit",
                                "type": "text",
                                "value": "identity",
                            },
                            {
                                "propId": "p@1:54:1",
                                "attr": "unit",
                                "type": "text",
                                "value": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                            },
                            {
                                "propId": "p@1:54:2",
                                "attr": "unit",
                                "type": "text",
                                "value": "2.1",
                            },
                            {
                                "propId": "p@1:54:3",
                                "attr": "unit",
                                "type": "datetime",
                                "value": "2017-06-01T00:00:00Z",
                            },
                            {
                                "propId": "p@1:54:4",
                                "attr": "unit",
                                "type": "datetime",
                                "value": "2017-06-01T00:00:00Z",
                            },
                            {
                                "propId": "p@1:54:9",
                                "attr": "matrix",
                                "columns": [
                                    [
                                        {
                                            "type": "struct",
                                            "attrs": [
                                                {
                                                    "propId": "p@2:11:0",
                                                    "attr": "unit",
                                                    "type": "text",
                                                    "value": "mitre-mobile-attack"
                                                },
                                                {
                                                    "propId": "p@2:11:2",
                                                    "attr": "unit",
                                                    "type": "text",
                                                    "value": "https://attack.mitre.org/mobile/index.php/Technique/MOB-T1056"
                                                },
                                                {
                                                    "propId": "p@2:11:4",
                                                    "attr": "unit",
                                                    "type": "text",
                                                    "value": "MOB-T1056"
                                                },
                                            ]
                                        }
                                    ]
                                ]
                            },
                            {
                                "propId": "p@1:54:13",
                                "attr": "unit",
                                "type": "text",
                                "value": "The MITRE Corporation"
                            },
                            {
                                "propId": "p@1:54:15",
                                "attr": "matrix",
                                "columns": [
                                    [
                                        { "type": "text", "value": "roleA" },
                                        { "type": "text", "value": "roleB" },
                                    ]
                                ]
                            },
                            {
                                "propId": "p@1:54:16",
                                "attr": "unit",
                                "type": "text",
                                "value": "organization"
                            },
                        ]
                    }
                ]
            }
        }))
    );

    {
        let vertices_with_address = r#"
            query vertices($defId: DefId!) {
                vertices(defId: $defId, first: 100) {
                    elements
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            "#
        .exec(
            OntologyParams {
                def_id: Some(identity.graphql_def_id()),
            },
            &ontology_schema,
            &ontology_ctx,
        )
        .await
        .unwrap();

        let vertices = vertices_with_address
            .as_object_value()
            .unwrap()
            .get_field_value("vertices")
            .unwrap()
            .as_object_value()
            .unwrap();
        let elements = vertices
            .get_field_value("elements")
            .unwrap()
            .as_list_value()
            .unwrap();
        let vertex = elements.first().unwrap().as_object_value().unwrap();
        let address: &juniper::Value<GqlScalar> = vertex.get_field_value("address").unwrap();
        let juniper::Value::Scalar(GqlScalar::String(address)) = address else {
            panic!("address was not a string: {address:?}");
        };
        info!("address is `{address}`");
    }
}
