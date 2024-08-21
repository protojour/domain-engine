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

const VERTICES: &str = r#"
query vertices($defId: String!) {
    vertices(defId: $defId, first: 100) {
        elements {
            address
        }
        pageInfo {
            hasNextPage
            endCursor
        }
    }
}
"#;

struct VerticesParams {
    def_id: DefId,
}

impl VerticesParams {
    fn into_variables(self) -> juniper::Variables<GqlScalar> {
        let mut variables = juniper::Variables::default();
        variables.insert("defId".to_string(), format!("{:?}", self.def_id).into());
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

    {
        let vertices_response = VERTICES
            .exec(
                VerticesParams {
                    def_id: identity.def_id(),
                }
                .into_variables(),
                &ontology_schema,
                &ontology_ctx,
            )
            .await
            .unwrap();

        let vertices = vertices_response
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
