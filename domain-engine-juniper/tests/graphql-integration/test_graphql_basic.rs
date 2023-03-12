use domain_engine_juniper::create_graphql_schema;
use juniper::graphql_value;
use ontol_test_utils::{assert_error_msg, TestCompile, TEST_PKG};
use pretty_assertions::assert_eq;
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
        .await,
        Ok(graphql_value!({
            "fooList": {
                "edges": None,
            },
        })),
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
        .await,
        Ok(graphql_value!(None)),
    );
}

#[test(tokio::test)]
async fn test_graphql_artist_and_instrument_connections() {
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
        .await,
        Ok(graphql_value!({
            "artistList": {
                "edges": None,
            },
        })),
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
        .await,
        Ok(graphql_value!({
            "instrumentList": {
                "edges": None
            },
        })),
    );

    // BUG:
    assert_error_msg!(
        r#"
mutation {
    createartist(input: {
        name: "Someone",
        plays: "invalid"
    }) {
        _id
    }
}
        "#
        .exec(&schema)
        .await,
        r#"Execution: invalid type: string "invalid", expected sequence with minimum length 0 in input at line 4 column 15 (field at line 2 column 4)"#
    );
}

#[test(tokio::test)]
async fn test_graphql_guitar_synth_union_smoke_test() {
    let schema = GUITAR_SYNTH_UNION.compile_schema();

    // `instrument` is a union so fields cannot be queries directly
    assert_eq!(
        "{
            artistList {
                edges {
                    node {
                        plays {
                            edges {
                                node {
                                    __typename
                                    ... on guitar {
                                        string_count
                                    }
                                    ... on synth {
                                        polyphony
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }"
        .exec(&schema)
        .await,
        Ok(graphql_value!({
            "artistList": {
                "edges": None
            },
        })),
    );
}
