use domain_engine_core::DomainAPIMock;
use juniper::graphql_value;
use ontol_test_utils::TEST_PKG;
use pretty_assertions::assert_eq;
use serde_json::json;
use test_log::test;
use unimock::*;

use crate::{Exec, TestCompileSchema};

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../examples/artist_and_instrument.ont");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.ont");

#[test]
fn test_graphql_empty_schema() {
    "".compile_schema(&|_| {});
}

#[test(tokio::test)]
async fn test_graphql_basic_schema() {
    let schema = "
    type foo {
        rel [id] string
        rel ['prop'] int
    }
    "
    .compile_schema(&|_| {});

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
    let schema = ARTIST_AND_INSTRUMENT.compile_schema(&|mocker| {
        mocker.api.add_entity(
            mocker.def_id_by_name("artist"),
            json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"),
            json!({
                "name": "Radiohead"
            }),
        );
    });

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
                "edges": [{
                    "node": {
                        "_id": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                        "plays": {
                            "edges": []
                        }
                    }
                }],
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
    let schema = GUITAR_SYNTH_UNION
        .schema_builder()
        .api_mock(Unimock::new(
            DomainAPIMock::query_entities
                .next_call(matching!(&TEST_PKG, _))
                .returns(Ok(vec![])),
        ))
        .build();

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
