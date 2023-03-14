use juniper::graphql_value;
use pretty_assertions::assert_eq;
use test_log::test;

use crate::{Exec, TestCompileSchema};

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../examples/artist_and_instrument.ont");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.ont");

#[test]
fn test_graphql_empty_schema() {
    "".compile_schema();
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
                "edges": [],
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
                "edges": [],
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
                "edges": []
            },
        })),
    );

    assert_eq!(
        r#"
        mutation {
            createartist(input: {
                name: "Someone",
                plays: [
                    {
                        name: "Instrument",
                        _edge: {
                            how_much: "A lot"
                        }
                    },
                    {
                        _id: "instrument/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
                        _edge: {
                            how_much: "A little bit less"
                        }
                    }
                ]
            }) {
                _id
                name
            }
        }
        "#
        .exec(&schema)
        .await,
        Ok(graphql_value!(None))
    );
}

#[test(tokio::test)]
async fn test_graphql_guitar_synth_union_smoke_test() {
    let schema = GUITAR_SYNTH_UNION.compile_schema();

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
                "edges": []
            },
        })),
    );
}
