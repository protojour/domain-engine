use domain_engine_juniper::create_graphql_schema;
use juniper::graphql_value;
use ontol_test_utils::{TestCompile, TEST_PKG};
use test_log::test;

use crate::{Exec, TestCompileSchema};

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../examples/artist_and_instrument.ont");
const _GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.ont");

#[test]
fn test_graphql_empty_schema() {
    "".compile_ok(|env| {
        create_graphql_schema(env, TEST_PKG).unwrap();
    });
}

#[test(tokio::test)]
async fn test_graphql_basic_schema() {
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

#[test(tokio::test)]
#[ignore = "not fully implemented yet"]
async fn test_artist_and_instrument() {
    let _schema = ARTIST_AND_INSTRUMENT.compile_schema();
}
