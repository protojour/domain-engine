use domain_engine_core::Session;
use domain_engine_graphql::{
    context::ServiceCtx,
    gql_scalar::GqlScalar,
    ontology_schema::{OntologyCtx, OntologySchema},
};
use domain_engine_test_utils::graphql_test_utils::{Exec, TestCompileSchema, ValueExt};
use juniper::{graphql_value, InputValue};
use ontol_macros::datastore_test;
use ontol_runtime::DefId;
use ontol_test_utils::{
    examples::{FINDINGS, GUITAR_SYNTH_UNION},
    expect_eq, TestPackages,
};
use tracing::info;

use crate::{mk_engine_default, test_graphql_ontology::OntologyParams};

#[datastore_test(tokio::test)]
async fn findings(ds: &str) {
    let (test, [findings_schema, guitar_schema]) =
        TestPackages::with_static_sources([FINDINGS, GUITAR_SYNTH_UNION])
            .with_roots([FINDINGS.0, GUITAR_SYNTH_UNION.0])
            .compile_schemas([FINDINGS.0, GUITAR_SYNTH_UNION.0]);
    let engine = mk_engine_default(test.ontology_owned(), ds).await;
    let domain_ctx: ServiceCtx = engine.clone().into();
    let ontology_ctx = OntologyCtx::new(engine, Session::default());
    let ontology_schema = OntologySchema::new_with_scalar_value(
        Default::default(),
        Default::default(),
        Default::default(),
    );

    let [guitar, finding_session] =
        test.bind(["guitar_synth_union.guitar", "findings.FindingSession"]);

    info!("create a guitar to be found");
    r#"mutation {
        guitar(
            create: [{
                type: "guitar",
                string_count: 6,
            }]
        ) {
            node {
                instrument_id
            }
        }
    }"#
    .exec([], &guitar_schema, &domain_ctx)
    .await
    .unwrap();

    info!("create a finding session");
    let session_value = r#"mutation {
        FindingSession(
            create: [{
                name: "favorite instruments"
            }]
        ) {
            node {
                id
            }
        }
    }"#
    .exec([], &findings_schema, &domain_ctx)
    .await
    .unwrap();

    let finding_session_id = session_value
        .field("FindingSession")
        .element(0)
        .field("node")
        .field("id")
        .scalar();

    let guitar_address =
        ontology_find_single_address(guitar.def_id(), &ontology_schema, &ontology_ctx).await;
    let _finding_session_address =
        ontology_find_single_address(finding_session.def_id(), &ontology_schema, &ontology_ctx)
            .await;

    info!("found guitar address {guitar_address}");

    info!("register finding (of a guitar)");
    r#"mutation addFinding($sessionId: ID!, $found: ID!) {
        FindingSession(
            update: [{
                id: $sessionId
                findings: { add: [$found] }
            }]
        ) {
            node {
                id
            }
        }
    }"#
    .exec(
        [
            (
                "sessionId".to_owned(),
                InputValue::Scalar(finding_session_id.clone()),
            ),
            (
                "found".to_owned(),
                InputValue::Scalar(guitar_address.clone()),
            ),
        ],
        &findings_schema,
        &domain_ctx,
    )
    .await
    .unwrap();

    info!("list findings");
    expect_eq!(
        actual = r#"{
            findings {
                nodes {
                    name
                    findings {
                        nodes
                    }
                }
            }
        }"#
        .exec([], &findings_schema, &domain_ctx)
        .await,
        expected = Ok(graphql_value!({
            "findings": {
                "nodes": [
                    {
                        "name": "favorite instruments",
                        "findings": {
                            "nodes": [guitar_address]
                        }
                    }
                ]
            }
        }))
    );
}

async fn ontology_find_single_address(
    def_id: DefId,
    ontology_schema: &OntologySchema,
    ontology_ctx: &OntologyCtx,
) -> GqlScalar {
    let vertices = r"
        query vertices($defId: String!) {
            vertices(defId: $defId, first: 1, withAddress: true) {
                elements
            }
        }
    "
    .exec(
        OntologyParams {
            def_id: Some(def_id),
        },
        ontology_schema,
        ontology_ctx,
    )
    .await
    .unwrap();

    vertices
        .field("vertices")
        .field("elements")
        .element(0)
        .field("address")
        .scalar()
        .clone()
}
