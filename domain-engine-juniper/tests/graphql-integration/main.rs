use domain_engine_juniper::create_graphql_schema;
use ontol_test_utils::{TestCompile, TEST_PKG};
use test_log::test;

fn main() {}

#[test]
fn test_create_empty_schema() {
    "".compile_ok(|env| {
        create_graphql_schema(env, TEST_PKG).unwrap();
    });
}
