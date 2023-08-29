use domain_engine_core::data_store::DataStoreAPIMock;
use juniper::graphql_value;
use ontol_runtime::value::Attribute;
use ontol_test_utils::{
    examples::{ARTIST_AND_INSTRUMENT, GEOJSON, GUITAR_SYNTH_UNION, MUNICIPALITIES, WGS},
    expect_eq,
    type_binding::ToSequence,
    SourceName, TestPackages,
};
use serde_json::json;
use test_log::test;
use unimock::*;

use crate::{
    gql_ctx_mock_data_store, mock_data_store_query_entities_empty, Exec, TestCompileSchema,
};

const ROOT: SourceName = SourceName::root();

#[test]
fn test_graphql_empty_schema() {
    "".compile_schemas([ROOT]);
}

#[test(tokio::test)]
async fn test_graphql_basic_schema() {
    let (test, [schema]) = "
    pub def foo_id { fmt '' => string => . }
    pub def foo {
        rel foo_id identifies: .
        rel .'prop': i64
    }
    "
    .compile_schemas([ROOT]);
    let [foo] = test.bind(["foo"]);
    let entity = foo.entity_builder(json!("my_id"), json!({ "prop": 42 }));

    expect_eq!(
        actual = "{
            fooList {
                edges {
                    node {
                        prop
                    }
                }
            }
        }"
        .exec(
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()),
            []
        )
        .await,
        expected = Ok(graphql_value!({
            "fooList": {
                "edges": [],
            },
        })),
    );

    expect_eq!(
        actual = "mutation {
            createfoo(
                input: {
                    prop: 42
                }
            ) {
                prop
            }
        }"
        .exec(
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::store_new_entity
                    .next_call(matching!(_, _, _))
                    .returns(Ok(entity.into()))
            ),
            []
        )
        .await,
        expected = Ok(graphql_value!({
            "createfoo": {
                // BUG: Floating point. The ontol type should be `i32` to make this an integer.
                // BUG: Also, `i64` cannot be serialized to floating point.
                "prop": 42.0
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_basic_inherent_auto_id_anonymous_type() {
    let (test, [schema]) = "
    pub def foo {
        rel .'id'(rel .gen: auto)|id: { rel .is: string }
    }
    "
    .compile_schemas([ROOT]);

    expect_eq!(
        actual = "{
            fooList {
                edges {
                    node {
                        id
                    }
                }
            }
        }"
        .exec(
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()),
            []
        )
        .await,
        expected = Ok(graphql_value!({
            "fooList": {
                "edges": [],
            },
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_value_type_as_field() {
    "
    def foo { rel .is: string }
    pub def bar {
        rel .'id'(rel .gen: auto)|id: { rel .is: string }
        rel .'foo': foo
    }
    "
    .compile_schemas([ROOT]);
}

#[test(tokio::test)]
async fn test_graphql_value_type_in_array() {
    "
    def foo { rel .is: string }
    pub def bar {
        rel .'id'(rel .gen: auto)|id: { rel .is: string }
        rel .'foo': [foo]
    }
    "
    .compile_schemas([ROOT]);
}

#[test(tokio::test)]
async fn test_inner_struct() {
    let (test, [schema]) = "
    pub def foo_id { fmt '' => string => . }
    def inner {
        rel .'prop': string
    }
    pub def foo {
        rel foo_id identifies: .
        rel .'inner': inner
    }
    "
    .compile_schemas([ROOT]);

    let [foo] = test.bind(["foo"]);
    let entity = foo.entity_builder(json!("my_id"), json!({ "inner": { "prop": "yo" } }));

    expect_eq!(
        actual = "{
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
        .exec(
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()),
            []
        )
        .await,
        expected = Ok(graphql_value!({
            "fooList": {
                "edges": [],
            },
        })),
    );

    expect_eq!(
        actual = r#"mutation {
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
        .exec(
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::store_new_entity
                    .next_call(matching!(_, _, _))
                    .returns(Ok(entity.into()))
            ),
            []
        )
        .await,
        expected = Ok(graphql_value!({
            "createfoo": {
                "inner": {
                    "prop": "yo"
                }
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_docs_introspection() {
    let (test, [schema]) = "
    def Key {
        rel .is: string
    }

    /// this is a type
    pub def PublicType {
        rel Key identifies: .
        /// this is a field
        rel .'relation': string
    }
    "
    .compile_schemas([ROOT]);

    expect_eq!(
        actual = r#"{
            __type(name: "PublicType") {
                name
                description
                fields {
                    name
                    description
                }
            }
        }"#
        .exec(&schema, &gql_ctx_mock_data_store(&test, ROOT, ()), [])
        .await,
        expected = Ok(graphql_value!({
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
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_artist_and_instrument_connections() {
    let (test, [schema]) = ARTIST_AND_INSTRUMENT.1.compile_schemas([ROOT]);
    let [artist, instrument, plays] = test.bind(["artist", "instrument", "plays"]);
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

    expect_eq!(
        actual = "{
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
        .exec(
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::query
                    .next_call(matching!(_))
                    .returns(Ok(vec![ziggy.clone()]))
            ),
            []
        )
        .await,
        expected = Ok(graphql_value!({
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

    expect_eq!(
        actual = "{
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
        .exec(
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()),
            []
        )
        .await,
        expected = Ok(graphql_value!({
            "instrumentList": {
                "edges": []
            },
        })),
    );

    expect_eq!(
        actual = r#"
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
        .exec(
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::store_new_entity
                    .next_call(matching!(_, _, _))
                    .returns(Ok(ziggy.value))
            ),
            []
        )
        .await,
        expected = Ok(graphql_value!({
            "createartist": {
                "ID": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                "name": "Ziggy"
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_guitar_synth_union_smoke_test() {
    let (test, [schema]) = GUITAR_SYNTH_UNION.1.compile_schemas([ROOT]);
    let [artist] = test.bind(["artist"]);
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

    expect_eq!(
        actual = "{
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
        .exec(
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::query
                    .next_call(matching!(_, _))
                    .returns(Ok(vec![artist_entity]))
            ),
            []
        )
        .await,
        expected = Ok(graphql_value!({
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

#[test(tokio::test)]
async fn test_graphql_municipalities() {
    let (test, [schema]) = TestPackages::with_sources([(ROOT, MUNICIPALITIES.1), GEOJSON, WGS])
        .compile_schemas([ROOT]);

    {
        expect_eq!(
            actual = "{
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
            .exec(
                &schema,
                &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()),
                []
            )
            .await,
            expected = Ok(graphql_value!({
                "municipalityList": {
                    "edges": []
                }
            })),
        );
    }
}
