use std::sync::Arc;

use domain_engine_core::{DomainEngine, Session};
use domain_engine_graphql::{
    domain::context::ServiceCtx,
    ontology::{OntologyCtx, OntologySchema},
};
use domain_engine_test_utils::{
    dynamic_data_store::DynamicDataStoreFactory,
    graphql_test_utils::{Exec, TestCompileSingletonSchema},
    system::mock_current_time_monotonic,
};
use juniper::graphql_value;
use ontol_examples::conduit::conduit_db;
use ontol_macros::datastore_test;
use ontol_test_utils::expect_eq;
use tracing::info;

// FIXME: inmemory DB needs to implement universal create and update timestamps
#[datastore_test(tokio::test, ignore("inmemory"))]
async fn test_conduit_search(ds: &str) {
    let (test, conduit_schema) = conduit_db().1.compile_single_schema();
    let domain_engine = Arc::new(
        DomainEngine::builder(test.ontology_owned())
            .system(Box::new(unimock::Unimock::new(
                mock_current_time_monotonic(),
            )))
            .build(
                DynamicDataStoreFactory::new(ds).tantivy_index(),
                Session::default(),
            )
            .await
            .unwrap(),
    );
    let domain_ctx: ServiceCtx = domain_engine.clone().into();
    let ontology_ctx = OntologyCtx::new(domain_engine.clone(), Session::default());
    let ontology_schema = OntologySchema::new_with_scalar_value(
        Default::default(),
        Default::default(),
        Default::default(),
    );

    r#"mutation {
        User(create: [
            {
                user_id: "341da6d9-981f-44ba-9cac-823376a7dd13",
                username: "steinbeck",
                email: "john.steinbeck@mailinator.com",
                password_hash: "s3cr3t",
            },
            {
                user_id: "9f054215-97df-4b3f-acef-5ced93e23d39",
                username: "mary",
                email: "maryshelley@gmail.com",
                password_hash: "s3cr3t",
            },
        ]) {
            node { user_id }
        }
    }"#
    .exec([], &conduit_schema, &domain_ctx)
    .await
    .unwrap();

    r#"mutation {
        Article(create: [
            {
                slug: "of-mice-and-men",
                title: "Of Mice and Men",
                description: "a little excerpt",
                body: "A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool. On one side of the river the golden foothill slopes curve up to the strong and rocky Gabilan Mountains, but on the valley side the water is lined with trees—willows fresh and green with every spring, carrying in their lower leaf junctures the debris of the winter’s flooding; and sycamores with mottled, white, recumbent limbs and branches that arch over the pool",
                author: { user_id: "341da6d9-981f-44ba-9cac-823376a7dd13" }
                tags: [{ tag: "tragedy" }]
            },
            {
                slug: "frankenstein",
                title: "Frankenstein; or, The Modern Prometheus",
                description: "a little excerpt",
                body: "You will rejoice to hear that no disaster has accompanied the commencement of an enterprise which you have regarded with such evil forebodings. I arrived here yesterday, and my first task is to assure my dear sister of my welfare and increasing confidence in the success of my undertaking.",
                author: { user_id: "9f054215-97df-4b3f-acef-5ced93e23d39" }
                tags: [{ tag: "horror" }]
            }
        ]) {
            node { slug }
        }
    }"#
    .exec([], &conduit_schema, &domain_ctx)
    .await
    .unwrap();

    info!("awaiting index change..");

    while domain_engine
        .get_data_store()
        .unwrap()
        .api()
        .background_search_indexer_running()
    {
        domain_engine
            .index_mutated_signal()
            .clone()
            .changed()
            .await
            .unwrap();
    }

    info!("done indexing");

    expect_eq!(
        actual = r"{
            vertexSearch(
                limit: 10
                withAddress: false
                withDefId: false
                withAttrs: false
            ) {
                results {
                    vertex
                    score
                }
            }
        }"
        .exec([], &ontology_schema, &ontology_ctx)
        .await,
        expected = Ok(graphql_value!({
            "vertexSearch": {
                "results": [
                    {
                        "vertex": {
                            "type": "struct",
                            "update_time": "1976-01-01T00:00:00Z"
                        },
                        "score": 1.0
                    },
                    {
                        "vertex": {
                            "type": "struct",
                            "update_time": "1975-01-01T00:00:00Z"
                        },
                        "score": 1.0
                    },
                    {
                        "vertex": {
                            "type": "struct",
                            "update_time": "1972-01-01T00:00:00Z"
                        },
                        "score": 1.0
                    },
                    {
                        "vertex": {
                            "type": "struct",
                            "update_time": "1971-01-01T00:00:00Z"
                        },
                        "score": 1.0
                    }
                ]
            }
        }))
    );
}
