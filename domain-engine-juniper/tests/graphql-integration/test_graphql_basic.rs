use domain_engine_core::EngineAPIMock;
use juniper::graphql_value;
use ontol_runtime::value::Attribute;
use ontol_test_utils::{
    type_binding::{ToSequence, TypeBinding},
    SourceName, TestPackages,
};
use pretty_assertions::assert_eq;
use serde_json::json;
use test_log::test;
use unimock::*;

use crate::{
    mock_default_config, mock_gql_context, mock_query_entities_empty, Exec, TestCompileSchema,
};

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../examples/artist_and_instrument.on");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.on");

#[test]
fn test_graphql_empty_schema() {
    "".compile_schema();
}

#[test(tokio::test)]
async fn test_graphql_basic_schema() {
    let (env, schema) = "
    pub type foo_id { fmt '' => string => . }
    pub type foo {
        rel foo_id identifies: .
        rel .'prop': int
    }
    "
    .compile_schema();
    let foo = TypeBinding::new(&env, "foo");
    let entity = foo.entity_builder(json!("my_id"), json!({ "prop": 42 }));

    {
        let ctx = mock_gql_context((mock_default_config(), mock_query_entities_empty()));
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
            .exec(&schema, &ctx)
            .await,
            Ok(graphql_value!({
                "fooList": {
                    "edges": [],
                },
            })),
        );
    }

    {
        let ctx = mock_gql_context(
            EngineAPIMock::create_entity
                .next_call(matching!(_, _))
                .returns(Ok(entity.into())),
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
            .exec(&schema, &ctx)
            .await,
            Ok(graphql_value!({
                "createfoo": {
                    // BUG: floating point
                    "prop": 42.0
                }
            })),
        );
    }
}

#[test(tokio::test)]
async fn test_graphql_basic_inherent_auto_id() {
    let (_, schema) = "
    pub type foo {
        rel .'id'(rel .gen: auto)|id: { rel .is: string }
    }
    "
    .compile_schema();

    {
        let ctx = mock_gql_context((mock_default_config(), mock_query_entities_empty()));
        assert_eq!(
            "{
                fooList {
                    edges {
                        node {
                            id
                        }
                    }
                }
            }"
            .exec(&schema, &ctx)
            .await,
            Ok(graphql_value!({
                "fooList": {
                    "edges": [],
                },
            })),
        );
    }
}

#[test(tokio::test)]
async fn test_inner_struct() {
    let (env, schema) = "
    pub type foo_id { fmt '' => string => . }
    type inner {
        rel .'prop': string
    }
    pub type foo {
        rel foo_id identifies: .
        rel .'inner': inner
    }
    "
    .compile_schema();

    let foo = TypeBinding::new(&env, "foo");
    let entity = foo.entity_builder(json!("my_id"), json!({ "inner": { "prop": "yo" } }));

    {
        let ctx = mock_gql_context((mock_default_config(), mock_query_entities_empty()));
        assert_eq!(
            "{
            fooList {
                edges {
                    node {
                        inner {
                            prop
                        }
                    }
                }
            }
        }"
            .exec(&schema, &ctx)
            .await,
            Ok(graphql_value!({
                "fooList": {
                    "edges": [],
                },
            })),
        );
    }

    {
        let ctx = mock_gql_context(
            EngineAPIMock::create_entity
                .next_call(matching!(_, _))
                .returns(Ok(entity.into())),
        );
        assert_eq!(
            r#"mutation {
                createfoo(
                    input: {
                        inner: {
                            prop: "yo"
                        }
                    }
                ) {
                    inner {
                        prop
                    }
                }
            }"#
            .exec(&schema, &ctx)
            .await,
            Ok(graphql_value!({
                "createfoo": {
                    "inner": {
                        "prop": "yo"
                    }
                }
            })),
        );
    }
}

#[test(tokio::test)]
async fn test_docs_introspection() {
    let (_env, schema) = "
    type Key {
        rel .is: string
    }

    /// this is a type
    pub type PublicType {
        rel Key identifies: .
        /// this is a field
        rel .'relation': string
    }
    "
    .compile_schema();

    {
        let ctx = mock_gql_context(());

        assert_eq!(
            r#"
            {
                __type(name: "PublicType") {
                    name
                    description
                    fields {
                        name
                        description
                    }
                }
            }
            "#
            .exec(&schema, &ctx)
            .await,
            Ok(graphql_value!({
                "__type": {
                    "name": "PublicType",
                    "description": "this is a type",
                    "fields": [
                        {
                            "name": "relation",
                            "description": "this is a field"
                        }
                    ]
                }
            }))
        );
    }
}

#[test(tokio::test)]
async fn test_graphql_artist_and_instrument_connections() {
    let (env, schema) = ARTIST_AND_INSTRUMENT.compile_schema();
    let artist = TypeBinding::new(&env, "artist");
    let instrument = TypeBinding::new(&env, "instrument");
    let plays = TypeBinding::new(&env, "plays");
    let ziggy: Attribute = artist
        .entity_builder(
            json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"),
            json!({
                "name": "Ziggy",
            }),
        )
        .relationship(
            "plays",
            vec![instrument
                .entity_builder(
                    json!("instrument/88832e20-8c6e-46b4-af79-27b19b889a58"),
                    json!({
                        "name": "Guitar",
                    }),
                )
                .to_attr(plays.value_builder(json!({ "how_much": "A lot" })))]
            .to_sequence_attribute(&instrument),
        )
        .into();

    {
        let ctx = mock_gql_context((
            mock_default_config(),
            EngineAPIMock::query_entities
                .next_call(matching!(_))
                .returns(Ok(vec![ziggy.clone()])),
        ));
        assert_eq!(
            "{
                artistList {
                    edges {
                        node {
                            ID
                            name
                            plays {
                                edges {
                                    node {
                                        ID
                                        name
                                    }
                                    how_much
                                }
                            }
                        }
                    }
                }
            }"
            .exec(&schema, &ctx)
            .await,
            Ok(graphql_value!({
                "artistList": {
                    "edges": [{
                        "node": {
                            "ID": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                            "name": "Ziggy",
                            "plays": {
                                "edges": [
                                    {
                                        "node": {
                                            "ID": "instrument/88832e20-8c6e-46b4-af79-27b19b889a58",
                                            "name": "Guitar",
                                        },
                                        "how_much": "A lot"
                                    }
                                ]
                            }
                        }
                    }],
                },
            })),
        );
    }

    {
        let ctx = mock_gql_context((mock_default_config(), mock_query_entities_empty()));
        assert_eq!(
            "{
                instrumentList {
                    edges {
                        node {
                            ID
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
            .exec(&schema, &ctx)
            .await,
            Ok(graphql_value!({
                "instrumentList": {
                    "edges": []
                },
            })),
        );
    }

    {
        let ctx = mock_gql_context(
            EngineAPIMock::create_entity
                .next_call(matching!(_, _))
                .returns(Ok(ziggy.value)),
        );
        assert_eq!(
            r#"
            mutation {
                createartist(input: {
                    name: "Ziggy",
                    plays: [
                        {
                            name: "Instrument",
                            _edge: {
                                how_much: "A lot"
                            }
                        },
                        {
                            ID: "instrument/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
                            _edge: {
                                how_much: "A little bit less"
                            }
                        }
                    ]
                }) {
                    ID
                    name
                }
            }
            "#
            .exec(&schema, &ctx)
            .await,
            Ok(graphql_value!({
                "createartist": {
                    "ID": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                    "name": "Ziggy"
                }
            }))
        );
    }
}

#[test(tokio::test)]
async fn test_graphql_guitar_synth_union_smoke_test() {
    let (env, schema) = GUITAR_SYNTH_UNION.compile_schema();
    let artist = TypeBinding::new(&env, "artist");
    let artist_entity: Attribute = artist
        .entity_builder(
            json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"),
            json!({
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
            }),
        )
        .into();

    {
        let ctx = mock_gql_context((
            mock_default_config(),
            EngineAPIMock::query_entities
                .next_call(matching!(_))
                .returns(Ok(vec![artist_entity])),
        ));

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
            .exec(&schema, &ctx)
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
}

#[test(tokio::test)]
async fn test_graphql_municipalities() {
    let (_env, schema) = TestPackages::with_sources([
        (
            SourceName::root(),
            include_str!("../../../examples/municipalities.on"),
        ),
        (
            SourceName("geojson"),
            include_str!("../../../examples/geojson.on"),
        ),
    ])
    .compile_schema();

    {
        let ctx = mock_gql_context((mock_default_config(), mock_query_entities_empty()));
        assert_eq!(
            "{
                municipalityList {
                    edges {
                        node {
                            code
                            geometry {
                                __typename
                                ... on _domain2_Polygon {
                                    coordinates
                                }
                                ... on _domain2_GeometryCollection {
                                    geometries {
                                        ... on _domain2_Polygon {
                                            coordinates
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }"
            .exec(&schema, &ctx)
            .await,
            Ok(graphql_value!({
                "municipalityList": {
                    "edges": []
                }
            })),
        );
    }
}
