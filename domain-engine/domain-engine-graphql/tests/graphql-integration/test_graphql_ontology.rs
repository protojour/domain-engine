use domain_engine_core::Session;
use domain_engine_graphql::{
    context::ServiceCtx,
    gql_scalar::GqlScalar,
    ontology_schema::{OntologyCtx, OntologySchema},
};
use domain_engine_test_utils::graphql_test_utils::{Exec, TestCompileSchema};
use juniper::graphql_value;
use ontol_macros::datastore_test;
use ontol_runtime::DefId;
use ontol_test_utils::{
    examples::stix::{stix_bundle, STIX},
    expect_eq,
};
use tracing::info;

use crate::mk_engine_default;

#[derive(Default)]
struct Params {
    def_id: Option<DefId>,
}

impl From<Params> for juniper::Variables<GqlScalar> {
    fn from(value: Params) -> Self {
        let mut variables = juniper::Variables::default();
        if let Some(def_id) = value.def_id {
            variables.insert("defId".to_string(), format!("{:?}", def_id).into());
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
            query def($defId: String!) {
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
            Params {
                def_id: Some(identity.def_id())
            },
            &ontology_schema,
            &ontology_ctx
        )
        .await,
        expected = Ok(graphql_value!({
            "def": {
                "id": "def@1:59",
                "kind": "ENTITY",
                "dataRelationships": [
                    { "propId": "p@1:59:0", "name": "type" },
                    { "propId": "p@1:59:1", "name": "id" },
                    { "propId": "p@1:59:2", "name": "spec_version" },
                    { "propId": "p@1:59:3", "name": "created" },
                    { "propId": "p@1:59:4", "name": "modified" },
                    { "propId": "p@1:59:5", "name": "confidence" },
                    { "propId": "p@1:59:6", "name": "revoked" },
                    { "propId": "p@1:59:7", "name": "labels" },
                    { "propId": "p@1:59:8", "name": "lang" },
                    { "propId": "p@1:59:9", "name": "external_references" },
                    { "propId": "p@1:59:10", "name": "created_by_ref" },
                    { "propId": "p@1:59:11", "name": "object_marking_refs" },
                    { "propId": "p@1:59:12", "name": "granular_markings" },
                    { "propId": "p@1:59:13", "name": "name" },
                    { "propId": "p@1:59:14", "name": "description" },
                    { "propId": "p@1:59:15", "name": "roles" },
                    { "propId": "p@1:59:16", "name": "identity_class" },
                    { "propId": "p@1:59:17", "name": "sectors" },
                    { "propId": "p@1:59:18", "name": "contact_information" },
                ]
            }
        }))
    );

    expect_eq!(
        actual = r#"
            query vertices($defId: String!) {
                vertices(defId: $defId, first: 100, withAddress: false, withDefId: false) {
                    elements
                }
            }
            "#
        .exec(
            Params {
                def_id: Some(identity.def_id()),
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
                                "propId": "p@1:59:0",
                                "attr": "unit",
                                "type": "text",
                                "value": "identity",
                            },
                            {
                                "propId": "p@1:59:1",
                                "attr": "unit",
                                "type": "text",
                                "value": "identity--c78cb6e5-0c4b-4611-8297-d1b8b55e40b5",
                            },
                            {
                                "propId": "p@1:59:2",
                                "attr": "unit",
                                "type": "text",
                                "value": "2.1",
                            },
                            {
                                "propId": "p@1:59:3",
                                "attr": "unit",
                                "type": "datetime",
                                "value": "2017-06-01T00:00:00Z",
                            },
                            {
                                "propId": "p@1:59:4",
                                "attr": "unit",
                                "type": "datetime",
                                "value": "2017-06-01T00:00:00Z",
                            },
                            {
                                "propId": "p@1:59:13",
                                "attr": "unit",
                                "type": "text",
                                "value": "The MITRE Corporation"
                            },
                            {
                                "propId": "p@1:59:16",
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
            query vertices($defId: String!) {
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
            Params {
                def_id: Some(identity.def_id()),
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
