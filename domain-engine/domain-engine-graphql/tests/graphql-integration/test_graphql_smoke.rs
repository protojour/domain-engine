//! smoke tests for loading GraphQL schemas into memory.
//!
//! These tests should not test specific logic, just verify that things don't crash

use domain_engine_core::{DomainEngine, Session};
use domain_engine_graphql::domain::context::ServiceCtx;
use domain_engine_store_inmemory::InMemoryConnection;
use domain_engine_test_utils::{
    graphql_test_utils::{Exec, TestCompileSchema, TestCompileSingletonSchema},
    system::mock_current_time_monotonic,
};
use ontol_examples::demo;
use ontol_macros::test;
use ontol_test_utils::{TestPackages, file_url};
use unimock::Unimock;

#[test]
fn test_graphql_demo_compile() {
    demo().1.compile_single_schema();
}

// regression test for https://gitlab.com/protojour/memoriam/domain-engine/-/issues/111
#[test(tokio::test)]
async fn test_default_mapping_error() {
    let (test, [schema]) = TestPackages::with_static_sources([
        (
            file_url("events"),
            "
            use 'events_db' as db

            def event (
                rel. '_id'[rel* gen: auto]: (rel* is: serial)
                rel* 'level'[rel* default := 1]: i64
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
            file_url("events_db"),
            "
            domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
            def event (
                rel. '_id'[rel* gen: auto]: (rel* is: serial)
                rel* 'level'[rel* default := 1]: i64
            )
            ",
        ),
    ])
    .compile_schemas(["events"]);

    let ctx: ServiceCtx = DomainEngine::builder(test.ontology_owned())
        .system(Box::new(Unimock::new(mock_current_time_monotonic())))
        .build_sync(InMemoryConnection, Session::default())
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
