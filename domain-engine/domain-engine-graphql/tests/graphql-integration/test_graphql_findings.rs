use domain_engine_core::Session;
use domain_engine_graphql::{
    domain::{DomainSchema, context::ServiceCtx},
    gql_scalar::GqlScalar,
    ontology::{OntologyCtx, OntologySchema},
};
use domain_engine_test_utils::graphql_test_utils::{Exec, TestCompileSchema, ValueExt};
use juniper::{InputValue, graphql_value};
use ontol_examples::{findings, guitar_synth_union};
use ontol_macros::datastore_test;
use ontol_test_utils::{TestPackages, expect_eq};
use tracing::info;

use crate::{mk_engine_default, test_graphql_ontology::OntologyParams};

#[datastore_test(tokio::test)]
async fn test_findings(ds: &str) {
    let (test, [findings_schema, guitar_schema]) =
        TestPackages::with_sources([findings(), guitar_synth_union()])
            .with_entrypoints([findings().0, guitar_synth_union().0])
            .compile_schemas(["findings", "guitar_synth_union"]);
    let engine = mk_engine_default(test.ontology_owned(), ds).await;
    let domain_ctx: ServiceCtx = engine.clone().into();
    let ontology_ctx = OntologyCtx::new(engine, Session::default());
    let ontology_schema = OntologySchema::new_with_scalar_value(
        Default::default(),
        Default::default(),
        Default::default(),
    );

    let [guitar, synth, finding_session] = test.bind([
        "guitar_synth_union.guitar",
        "guitar_synth_union.synth",
        "findings.FindingSession",
    ]);

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

    info!("create a synth to be found");
    r#"mutation {
        synth(
            create: [{
                type: "synth",
                polyphony: 1,
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
            create: [
                { name: "favorite guitars" },
                { name: "favorite synths" },
            ]
        ) {
            node {
                id
            }
        }
    }"#
    .exec([], &findings_schema, &domain_ctx)
    .await
    .unwrap();

    let favorite_guitars = session_value
        .field("FindingSession")
        .element(0)
        .field("node")
        .field("id")
        .scalar();
    let favorite_synths = session_value
        .field("FindingSession")
        .element(1)
        .field("node")
        .field("id")
        .scalar();

    let guitar_address =
        ontology_find_single_address(guitar.graphql_def_id(), &ontology_schema, &ontology_ctx)
            .await;
    let synth_address =
        ontology_find_single_address(synth.graphql_def_id(), &ontology_schema, &ontology_ctx).await;
    let _finding_session_address = ontology_find_single_address(
        finding_session.graphql_def_id(),
        &ontology_schema,
        &ontology_ctx,
    )
    .await;

    info!("register finding (of a guitar)");
    add_finding(
        favorite_guitars,
        &guitar_address,
        &findings_schema,
        &domain_ctx,
    )
    .await;

    info!("register finding (of a synth)");
    add_finding(
        favorite_synths,
        &synth_address,
        &findings_schema,
        &domain_ctx,
    )
    .await;

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
                        "name": "favorite guitars",
                        "findings": {
                            "nodes": [guitar_address]
                        }
                    },
                    {
                        "name": "favorite synths",
                        "findings": {
                            "nodes": [synth_address]
                        }
                    }
                ]
            }
        }))
    );
}

async fn add_finding(
    finding_session_id: &GqlScalar,
    found_id: &GqlScalar,
    findings_schema: &DomainSchema,
    domain_ctx: &ServiceCtx,
) {
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
            ("found".to_owned(), InputValue::Scalar(found_id.clone())),
        ],
        findings_schema,
        domain_ctx,
    )
    .await
    .unwrap();
}

async fn ontology_find_single_address(
    graphql_def_id: String,
    ontology_schema: &OntologySchema,
    ontology_ctx: &OntologyCtx,
) -> GqlScalar {
    let vertices = r"
        query vertices($defId: DefId!) {
            vertices(defId: $defId, first: 1, withAddress: true) {
                elements
            }
        }
    "
    .exec(
        OntologyParams {
            def_id: Some(graphql_def_id),
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
