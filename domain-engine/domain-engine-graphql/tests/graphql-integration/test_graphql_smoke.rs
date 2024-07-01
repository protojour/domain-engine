//! smoke tests for loading GraphQL schemas into memory.
//!
//! These tests should not test specific logic, just verify that things don't crash

use domain_engine_core::{DomainEngine, Session};
use domain_engine_graphql::context::ServiceCtx;
use domain_engine_store_inmemory::InMemoryDataStoreFactory;
use domain_engine_test_utils::{
    graphql_test_utils::{Exec, TestCompileSchema, TestCompileSingletonSchema},
    system::mock_current_time_monotonic,
};
use ontol_macros::test;
use ontol_test_utils::{examples::DEMO, src_name, TestPackages};
use unimock::Unimock;

#[test]
fn test_graphql_demo_compile() {
    DEMO.1.compile_single_schema();
}

// regression test for https://gitlab.com/protojour/memoriam/domain-engine/-/issues/111
#[test(tokio::test)]
async fn test_default_mapping_error() {
    let (test, [schema]) = TestPackages::with_static_sources([
        (
            src_name("events"),
            "
            use 'events_db' as db

            def event (
                rel .'_id'[rel .gen: auto]|id: (rel .is: serial)
                rel .'level'[rel .default := 1]: i64
            )

            map(
                event (
                    '_id': id,
                    'level': level,
                ),
                db.event (
                    '_id': id,
                    'level': level,
                ),
            )

            map events (
                (),
                event { ..@match db.event() }
            )
            ",
        ),
        (
            src_name("events_db"),
            "
            def event (
                rel .'_id'[rel .gen: auto]|id: (rel .is: serial)
                rel .'level'[rel .default := 1]: i64
            )
            ",
        ),
    ])
    .compile_schemas([src_name("events")]);

    let ctx: ServiceCtx = DomainEngine::builder(test.ontology_owned())
        .system(Box::new(Unimock::new(mock_current_time_monotonic())))
        .build_sync(InMemoryDataStoreFactory, Session::default())
        .unwrap()
        .into();

    r#"mutation {
        event(create: [{ }]) {
            node {
                _id
            }
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();
}
