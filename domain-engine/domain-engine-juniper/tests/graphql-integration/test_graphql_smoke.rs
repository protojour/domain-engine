//! smoke tests for loading GraphQL schemas into memory.
//!
//! These tests should not test specific logic, just verify that things don't crash

use domain_engine_test_utils::graphql_test_utils::TestCompileSingletonSchema;
use ontol_test_utils::examples::DEMO;
use test_log::test;

#[test]
fn test_graphql_demo_compile() {
    DEMO.1.compile_single_schema_with_datastore();
}
