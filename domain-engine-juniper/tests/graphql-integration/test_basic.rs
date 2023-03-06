use domain_engine_juniper::create_graphql_schema;
use juniper::graphql_value;
use ontol_test_utils::{TestCompile, TEST_PKG};
use test_log::test;

use crate::{Exec, TestCompileSchema};

#[test]
fn test_create_empty_schema() {
    "".compile_ok(|env| {
        create_graphql_schema(env, TEST_PKG).unwrap();
    });
}

#[test(tokio::test)]
async fn test_basic_schema() {
    let schema = "
    type foo {
        rel [id] string
        rel ['prop'] int
    }
    "
    .compile_schema();

    assert_eq!(
        "{
            fooList {
                edges {
                    node {
                        prop
                    }
                }
            }    
        }"
        .exec(&schema)
        .await
        .unwrap(),
        graphql_value!({
            "fooList": None,
        }),
    );

    assert_eq!(
        "mutation {
            createfoo(
                input: {
                    prop: 42
                }
            ) {
                prop
            }
        }"
        .exec(&schema)
        .await
        .unwrap(),
        graphql_value!(None),
    );
}
