use domain_engine_juniper::create_graphql_schema;
use juniper::graphql_value;
use ontol_test_utils::{assert_error_msg, TestCompile, TEST_PKG};
use test_log::test;

use crate::{Exec, TestCompileSchema};

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../examples/artist_and_instrument.ont");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.ont");

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
async fn test_artist_and_instrument_connections() {
    let schema = ARTIST_AND_INSTRUMENT.compile_schema();

    assert_eq!(
        "{
            artistList {
                edges {
                    node {
                        _id
                        plays {
                            edges {
                                node {
                                    _id
                                    name
                                }
                            }
                        }
                    }
                }
            }
        }"
        .exec(&schema)
        .await
        .unwrap(),
        graphql_value!({
            "artistList": None,
        }),
    );

    assert_eq!(
        "{
            instrumentList {
                edges {
                    node {
                        _id
                        name
                        played_by {
                            edges {
                                node {
                                    name
                                }
                            }
                        }
                    }
                }
            }
        }"
        .exec(&schema)
        .await
        .unwrap(),
        graphql_value!({
            "instrumentList": None,
        }),
    );
}

#[test(tokio::test)]
async fn test_guitar_synth_union_smoke_test() {
    let schema = GUITAR_SYNTH_UNION.compile_schema();

    // `instrument` is a union so fields cannot be queries directly
    assert_error_msg!(
        "{
            artistList {
                edges {
                    node {
                        plays {
                            edges {
                                node {
                                    name
                                }
                            }
                        }
                    }
                }
            }
        }"
        .exec(&schema)
        .await,
        r#"GraphQL: Unknown field "name" on type "instrument". At 7:36"#
    );
}
