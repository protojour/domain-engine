use domain_engine_core::data_store::DataStoreAPIMock;
use domain_engine_juniper::gql_scalar::GqlScalar;
use juniper::{graphql_value, parser::SourcePosition, ExecutionError, FieldError};
use ontol_runtime::value::{Attribute, Value};
use ontol_test_utils::{
    examples::{ARTIST_AND_INSTRUMENT, GEOJSON, GUITAR_SYNTH_UNION, MUNICIPALITIES, WGS},
    expect_eq,
    type_binding::ToSequence,
    SourceName, TestPackages,
};
use serde_json::json;
use test_log::test;
use unimock::*;

use domain_engine_test_utils::{
    graphql::{
        gql_ctx_mock_data_store, mock_data_store_query_entities_empty, Exec, TestCompileSchema,
        TestError,
    },
    parser_document_utils::{
        find_input_object_type, find_object_field, find_object_type, FieldInfo,
    },
    DbgTag,
};

const ROOT: SourceName = SourceName::root();

#[test]
#[should_panic = "GraphqlInterfaceNotFound"]
fn test_graphql_schema_for_entityless_domain_should_not_be_generated() {
    "".compile_schemas([ROOT]);
}

#[test(tokio::test)]
async fn test_graphql_int_scalars() {
    let (test, [schema]) = "
    pub def foo_id { fmt '' => text => . }
    def smallint {
        rel .is: integer
        rel .min: 0
        rel .max: 255
    }
    pub def foo {
        rel foo_id identifies: .
        rel .'small': smallint
        rel .'big': i64
    }
    "
    .compile_schemas([ROOT]);

    {
        let parser_document = schema.as_parser_document();
        let foo_object = find_object_type(&parser_document, "foo").unwrap();

        expect_eq!(
            actual = find_object_field(foo_object, "small")
                .unwrap()
                .field_type
                .to_string(),
            expected = "Int!"
        );
        expect_eq!(
            actual = find_object_field(foo_object, "big")
                .unwrap()
                .field_type
                .to_string(),
            expected = "_ontol_i64!"
        );
    }

    let [foo] = test.bind(["foo"]);
    let entity = foo.entity_builder(
        json!("my_id"),
        json!({ "small": 42, "big": 112233445566778899 as i64 }),
    );

    expect_eq!(
        actual = "{
            fooList {
                edges {
                    node {
                        small
                        big
                    }
                }
            }
        }"
        .exec(
            DbgTag("list"),
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()).await,
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
                    small: 42
                    big: 1337
                }
            ) {
                small
                big
            }
        }"
        .exec(
            DbgTag("mutation"),
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::store_new_entity
                    .next_call(matching!(_, _, _))
                    .returns(Ok(entity.into())),
            )
            .await,
            []
        )
        .await,
        expected = Ok(graphql_value!({
            "createfoo": {
                "small": 42,
                "big": juniper::Value::Scalar(GqlScalar::I64(112233445566778899))
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_basic_inherent_auto_id_anonymous_type() {
    let (test, [schema]) = "
    pub def foo {
        rel .'id'(rel .gen: auto)|id: { rel .is: text }
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
            DbgTag("list"),
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()).await,
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

#[test]
fn test_graphql_value_type_as_field() {
    "
    def foo { rel .is: text }
    pub def bar {
        rel .'id'(rel .gen: auto)|id: { rel .is: text }
        rel .'foo': foo
    }
    "
    .compile_schemas([ROOT]);
}

#[test]
fn test_graphql_value_type_in_array() {
    "
    def foo { rel .is: text }
    pub def bar {
        rel .'id'(rel .gen: auto)|id: { rel .is: text }
        rel .'foo': [foo]
    }
    "
    .compile_schemas([ROOT]);
}

#[test(tokio::test)]
async fn test_inner_struct() {
    let (test, [schema]) = "
    pub def foo_id { fmt '' => text => . }
    def inner {
        rel .'prop': text
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
            DbgTag("list"),
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()).await,
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
            DbgTag("mutation"),
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::store_new_entity
                    .next_call(matching!(_, _, _))
                    .returns(Ok(entity.into()))
            )
            .await,
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
        rel .is: text
    }

    /// this is a type
    pub def PublicType {
        rel Key identifies: .
        /// this is a field
        rel .'relation': text
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
        .exec(
            DbgTag("inspect"),
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, ()).await,
            []
        )
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
            DbgTag("artistList"),
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::query
                    .next_call(matching!(_))
                    .returns(Ok(vec![ziggy.clone()]))
            )
            .await,
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
            DbgTag("instrumentList"),
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()).await,
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
            DbgTag("createartist"),
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::store_new_entity
                    .next_call(matching!(_, _, _))
                    .returns(Ok(ziggy.value))
            )
            .await,
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
async fn test_graphql_guitar_synth_union_selection() {
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
            DbgTag("artistList"),
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::query
                    .next_call(matching!(_, _))
                    .returns(Ok(vec![artist_entity]))
            )
            .await,
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
                                        "polyphony": 42
                                    },
                                },
                                {
                                    "node": {
                                        "__typename": "guitar",
                                        "string_count": 91
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

#[test]
fn test_graphql_guitar_synth_union_input_union_field_list() {
    let (_test, [schema]) = GUITAR_SYNTH_UNION.1.compile_schemas([ROOT]);
    let parser_document = schema.as_parser_document();

    let instrument_edge_input =
        find_input_object_type(&parser_document, "instrumentEdgeInput").unwrap();

    let field_names: Vec<_> = instrument_edge_input
        .fields
        .iter()
        .map(FieldInfo::from)
        .collect();

    // The fields of instrumentEdgeInput consist of a union of guitar and synth.
    expect_eq!(
        actual = field_names.as_slice(),
        expected = &[
            // The instrument_id should only be included once,
            // and should use snake_case instead of kebab-case:
            FieldInfo::from(("instrument_id", "ID")),
            // from "guitar".
            FieldInfo::from(("type", "String")),
            // from "guitar".
            FieldInfo::from(("string_count", "_ontol_i64")),
            // from "synth". Note the synth `type` is deduplicated.
            FieldInfo::from(("polyphony", "_ontol_i64")),
            // object properties ordered last
            FieldInfo::from(("played_by", "[artistEdgeInput!]")),
        ]
    );
}

#[test(tokio::test)]
async fn test_graphql_guitar_synth_union_input_exec() {
    let (test, [schema]) = GUITAR_SYNTH_UNION.1.compile_schemas([ROOT]);
    let [artist] = test.bind(["artist"]);
    let ziggy: Value = artist
        .entity_builder(
            json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"),
            json!({
                "name": "Ziggy",
                "plays": []
            }),
        )
        .into();

    expect_eq!(
        actual = r#"
            mutation {
                createartist(input: {
                    name: "Ziggy",
                    plays: [
                        {
                            instrument_id: "synth/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8"
                        },
                        {
                            type: "guitar",
                            string_count: 6
                        },
                        {
                            type: "synth",
                            polyphony: 12
                        },
                    ]
                }) {
                    artist_id
                    name
                }
            }
        "#
        .exec(
            DbgTag("createArtist"),
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::store_new_entity
                    .next_call(matching!(_, _, _))
                    .returns(Ok(ziggy))
            )
            .await,
            []
        )
        .await,
        expected = Ok(graphql_value!({
            "createartist": {
                "artist_id": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                "name": "Ziggy"
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_guitar_synth_union_input_error_span() {
    let (test, [schema]) = GUITAR_SYNTH_UNION.1.compile_schemas([ROOT]);
    let expected_error: ExecutionError<GqlScalar> = ExecutionError::new(
        SourcePosition::new(40, 2, 16),
        &["createartist"],
        FieldError::new(
            "invalid map value, expected `instrument` (one of id, id, `guitar`, `synth`) in input at line 5 column 24",
            juniper::Value::Null
        )
    );

    expect_eq!(
        actual = r#"
            mutation {
                createartist(input: {
                    name: "Ziggy",
                    plays: [
                        {
                            instrument_id: "bogus_value_here"
                        }
                    ]
                }) {
                    artist_id
                    name
                }
            }
        "#
        .exec(
            DbgTag("createartist"),
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, ()).await,
            []
        )
        .await,
        expected = Err(TestError::Execution(vec![expected_error])),
    );
}

#[test(tokio::test)]
async fn test_graphql_municipalities() {
    let (test, [schema]) = TestPackages::with_sources([(ROOT, MUNICIPALITIES.1), GEOJSON, WGS])
        .compile_schemas([ROOT]);

    expect_eq!(
        actual = "{
            municipalityList {
                edges {
                    node {
                        code
                        geometry {
                            __typename
                            ... on _geojson_Polygon {
                                coordinates
                            }
                            ... on _geojson_GeometryCollection {
                                geometries {
                                    ... on _geojson_Polygon {
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
            DbgTag("list"),
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()).await,
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

#[test(tokio::test)]
async fn test_graphql_municipalities_named_query() {
    let (test, [schema]) = TestPackages::with_sources([(ROOT, MUNICIPALITIES.1), GEOJSON, WGS])
        .compile_schemas([ROOT]);

    let error = r#"{
        municipality(code: "OSL") {
            code
            geometry {
                __typename
                ... on _geojson_Polygon {
                    coordinates
                }
                ... on _geojson_GeometryCollection {
                    geometries {
                        ... on _geojson_Polygon {
                            coordinates
                        }
                    }
                }
            }
        }
    }"#
    .exec(
        DbgTag("OSL"),
        &schema,
        &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()).await,
        [],
    )
    .await
    .unwrap_err();

    let TestError::Execution(exec_errors) = error else {
        panic!();
    };
    expect_eq!(
        actual = format!(
            "{}",
            exec_errors.into_iter().next().unwrap().error().message()
        ),
        expected = "Entity not found"
    );
}

#[test]
fn test_graphql_municipalities_geojson_union() {
    let (_test, [schema]) = TestPackages::with_sources([(ROOT, MUNICIPALITIES.1), GEOJSON, WGS])
        .compile_schemas([ROOT]);

    let parser_document = schema.as_parser_document();

    let geometry_union_input =
        find_input_object_type(&parser_document, "_geojson_GeometryUnionInput").unwrap();

    let field_names: Vec<_> = geometry_union_input
        .fields
        .iter()
        .map(FieldInfo::from)
        .collect();

    expect_eq!(
        actual = field_names.as_slice(),
        expected = &[
            FieldInfo::from(("type", "String")),
            FieldInfo::from(("coordinates", "_ontol_json")),
            FieldInfo::from(("geometries", "_geojson_LeafGeometryUnionInput!")),
        ]
    );
}
