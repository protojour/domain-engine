use domain_engine_juniper::{create_graphql_schema, GqlContext};
use ontol_test_utils::{TestCompile, TEST_PKG};
use test_log::test;

fn main() {}

#[test]
fn test_create_empty_schema() {
    "".compile_ok(|env| {
        create_graphql_schema(env, TEST_PKG).unwrap();
    });
}

#[test]
fn test_basic_schema() {
    "
    type foo {
        rel [id] string
        rel ['prop'] int
    }
    "
    .compile_ok(|env| {
        let schema = create_graphql_schema(env, TEST_PKG).unwrap();

        let introspection =
            juniper::introspect(&schema, &GqlContext, juniper::IntrospectionFormat::All).unwrap();

        println!("{introspection:?}");
    });
}
