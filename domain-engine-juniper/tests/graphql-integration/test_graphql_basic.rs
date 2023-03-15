use domain_engine_core::EngineAPIMock;
use juniper::graphql_value;
use ontol_test_utils::type_binding::TypeBinding;
use pretty_assertions::assert_eq;
use serde_json::json;
use test_log::test;
use unimock::*;

use crate::{mock_default_config, mock_query_entities_empty, MockExec, TestCompileSchema};

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../examples/artist_and_instrument.ont");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.ont");

#[test]
fn test_graphql_empty_schema() {
    "".compile_schema();
}

#[test(tokio::test)]
async fn test_graphql_basic_schema() {
    let (_env, schema) = "
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
        .mock_exec(
            &schema,
            (mock_default_config(), mock_query_entities_empty())
        )
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
        .mock_exec(&schema, ())
        .await,
        Ok(graphql_value!(None)),
    );
}

#[test(tokio::test)]
async fn test_graphql_artist_and_instrument_connections() {
    let (env, schema) = ARTIST_AND_INSTRUMENT.compile_schema();
    let artist = TypeBinding::new(&env, "artist");
    let artist_entity = artist
        .value_builder()
        .id(json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"))
        .data(json!({
            "name": "Radiohead"
        }))
        .to_attribute();

    assert_eq!(
        "{
            artistList {
                edges {
                    node {
                        _id
                        name
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
        .mock_exec(
            &schema,
            (
                mock_default_config(),
                EngineAPIMock::query_entities
                    .next_call(matching!(_))
                    .returns(Ok(vec![artist_entity]))
            )
        )
        .await,
        Ok(graphql_value!({
            "artistList": {
                "edges": [{
                    "node": {
                        "_id": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                        "name": "Radiohead",
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
        .mock_exec(
            &schema,
            (mock_default_config(), mock_query_entities_empty())
        )
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
        .mock_exec(&schema, ())
        .await,
        Ok(graphql_value!(None))
    );
}

#[test(tokio::test)]
async fn test_graphql_guitar_synth_union_smoke_test() {
    let (env, schema) = GUITAR_SYNTH_UNION.compile_schema();
    let artist = TypeBinding::new(&env, "artist");
    let artist_entity = artist
        .value_builder()
        .id(json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"))
        .data(json!({
            "name": "foobar",
            "plays": [
                {
                    "type": "synth",
                    "polyphony": 42,
                },
                {
                    "type": "guitar",
                    "string_count": 91,
                }
            ]
        }))
        .to_attribute();

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
        .mock_exec(
            &schema,
            (
                mock_default_config(),
                EngineAPIMock::query_entities
                    .next_call(matching!(_))
                    .returns(Ok(vec![artist_entity]))
            )
        )
        .await,
        Ok(graphql_value!({
            "artistList": {
                "edges": [{
                    "node": {
                        "plays": {
                            "edges": [
                                {
                                    "node": {
                                        "__typename": "synth",
                                        // BUG: Floating point
                                        "polyphony": 42.0
                                    },
                                },
                                {
                                    "node": {
                                        "__typename": "guitar",
                                        // BUG: Floating point
                                        "string_count": 91.0
                                    }
                                }
                            ]
                        }
                    }
                }]
            },
        })),
    );
}
