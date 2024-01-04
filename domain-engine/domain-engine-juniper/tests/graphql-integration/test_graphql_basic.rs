use domain_engine_core::data_store::{DataStoreAPIMock, Request, Response};
use domain_engine_juniper::{
    context::ServiceCtx,
    gql_scalar::GqlScalar,
    juniper::{self, graphql_value},
    Schema,
};
use ontol_runtime::{
    config::DataStoreConfig,
    interface::serde::processor::ProcessorProfileFlags,
    sequence::{Sequence, SubSequence},
    value::Attribute,
};
use ontol_test_utils::{
    examples::{ARTIST_AND_INSTRUMENT, GEOJSON, GITMESH, GUITAR_SYNTH_UNION, MUNICIPALITIES, WGS},
    expect_eq,
    type_binding::ToSequence,
    SourceName, TestPackages,
};
use serde_json::json;
use test_log::test;
use unimock::*;

use domain_engine_test_utils::{
    graphql_test_utils::{
        gql_ctx_mock_data_store, mock_data_store_query_entities_empty, Exec, GraphqlTestResultExt,
        TestCompileSchema, TestCompileSingletonSchema, TestError,
    },
    parser_document_utils::{
        find_input_object_type, find_object_field, find_object_type, FieldInfo,
    },
};

const ROOT: SourceName = SourceName::root();

#[test]
#[should_panic = "GraphqlInterfaceNotFound"]
fn test_graphql_schema_for_entityless_domain_should_not_be_generated() {
    "".compile_schemas([ROOT]);
}

#[test(tokio::test)]
async fn test_graphql_int_scalars() {
    let (test, schema) = "
    def foo_id (fmt '' => text => .)
    def smallint (
        rel .is: integer
        rel .min: 0
        rel .max: 255
    )
    def foo (
        rel foo_id identifies: .
        rel .'small': smallint
        rel .'big': i64
    )

    map foos(
        (),
        foo: {..foo match()},
    )
    "
    .compile_single_schema_with_datastore();

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

    expect_eq!(
        actual = "{
            foos {
                edges {
                    node {
                        small
                        big
                    }
                }
            }
        }"
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()),
        )
        .await,
        expected = Ok(graphql_value!({
            "foos": {
                "edges": [],
            },
        })),
    );

    let store_entity_mock = DataStoreAPIMock::execute
        .next_call(matching!(Request::BatchWrite(_), _session))
        .returns(Ok(Response::one_inserted(
            foo.entity_builder(
                json!("my_id"),
                json!({ "small": 42, "big": 112233445566778899 as i64 }),
            )
            .into(),
        )));

    expect_eq!(
        actual = "mutation {
            foo(create: [{
                small: 42
                big: 1337
            }]) {
                node {
                    small
                    big
                }
            }
        }"
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, store_entity_mock),
        )
        .await,
        expected = Ok(graphql_value!({
            "foo": [{
                "node": {
                    "small": 42,
                    "big": juniper::Value::Scalar(GqlScalar::I64(112233445566778899))
                }
            }]
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_basic_inherent_auto_id_anonymous_type() {
    let (test, schema) = "
    def foo (
        rel .'id'(rel .gen: auto)|id: (rel .is: text)
    )
    map foos(
        (),
        foo: {..foo match()}
    )
    "
    .compile_single_schema_with_datastore();

    expect_eq!(
        actual = "{
            foos {
                edges {
                    node {
                        id
                    }
                }
            }
        }"
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()),
        )
        .await,
        expected = Ok(graphql_value!({
            "foos": {
                "edges": [],
            },
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_basic_pagination() {
    let (test, schema) = "
    def foo (
        rel .'id'(rel .gen: auto)|id: (rel .is: text)
    )
    map foos(
        (),
        foo: { ..foo match() },
    )
    "
    .compile_single_schema_with_datastore();

    expect_eq!(
        actual = "{
            foos(first: null, after: null) {
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }"
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()),
        )
        .await,
        expected = Ok(graphql_value!({
            "foos": {
                "pageInfo": {
                    "endCursor": null,
                    "hasNextPage": false
                }
            },
        })),
    );

    expect_eq!(
        actual = r#"{
            foos(first: 42, after: "MQ==") {
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }"#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::execute
                    .next_call(matching!(Request::Query(_), _session))
                    .answers(|(request, _)| {
                        let Request::Query(entity_select) = request else {
                            panic!();
                        };
                        assert_eq!(entity_select.include_total_len, false);
                        assert_eq!(entity_select.limit, 42);
                        assert_eq!(entity_select.after_cursor.as_deref().unwrap(), &['1' as u8]);

                        Ok(Response::Query(Sequence::new_sub(
                            [],
                            SubSequence {
                                end_cursor: Some(Box::new(['2' as u8])),
                                has_next: true,
                                total_len: Some(42),
                            },
                        )))
                    })
            ),
        )
        .await,
        expected = Ok(graphql_value!({
            "foos": {
                "pageInfo": {
                    "endCursor": "Mg==",
                    "hasNextPage": true
                }
            },
        })),
    );

    expect_eq!(
        actual = r#"{
            foos(first: 1) {
                totalCount
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }"#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::execute
                    .next_call(matching!(Request::Query(_), _session))
                    .answers(|(request, _)| {
                        let Request::Query(entity_select) = request else {
                            panic!();
                        };
                        assert_eq!(entity_select.include_total_len, true);
                        assert_eq!(entity_select.limit, 1);
                        assert_eq!(entity_select.after_cursor, None);

                        Ok(Response::Query(Sequence::new_sub(
                            [],
                            SubSequence {
                                end_cursor: Some(vec!['1' as u8].into_boxed_slice()),
                                has_next: true,
                                total_len: Some(42),
                            },
                        )))
                    })
            ),
        )
        .await,
        expected = Ok(graphql_value!({
            "foos": {
                "totalCount": 42,
                "pageInfo": {
                    "endCursor": "MQ==",
                    "hasNextPage": true
                }
            },
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_nodes() {
    let (test, schema) = "
    def foo (
        rel .'id'(rel .gen: auto)|id: (rel .is: text)
    )
    map foos(
        (),
        foo: { ..foo match() }
    )
    "
    .compile_single_schema_with_datastore();

    let [foo] = test.bind(["foo"]);
    let query_mock = DataStoreAPIMock::execute
        .next_call(matching!(Request::Query(_), _))
        .returns(Ok(Response::Query(Sequence::new([foo
            .entity_builder(json!("id"), json!({}))
            .into()]))));

    expect_eq!(
        actual = "{
            foos {
                nodes { id }
                edges { node { id } }
            }
        }"
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, query_mock),
        )
        .await,
        expected = Ok(graphql_value!({
            "foos": {
                "nodes": [
                    { "id": "id" }
                ],
                "edges": [{
                    "node": { "id": "id" }
                }]
            },
        })),
    );
}

#[test]
fn test_graphql_value_type_as_field() {
    "
    def foo (rel .is: text)
    def bar (
        rel .'id'(rel .gen: auto)|id: (rel .is: text)
        rel .'foo': foo
    )
    "
    .compile_schemas([ROOT]);
}

#[test]
fn test_graphql_value_type_in_array() {
    "
    def foo (rel .is: text)
    def bar (
        rel .'id'(rel .gen: auto)|id: (rel .is: text)
        rel .'foo': {foo}
    )
    "
    .compile_schemas([ROOT]);
}

#[test(tokio::test)]
async fn test_inner_struct() {
    let (test, schema) = "
    def foo_id (fmt '' => text => .)
    def inner (
        rel .'prop': text
    )
    def foo (
        rel foo_id identifies: .
        rel .'inner': inner
    )
    map foos(
        (),
        foo: { ..foo match() },
    )
    "
    .compile_single_schema_with_datastore();

    let [foo] = test.bind(["foo"]);

    expect_eq!(
        actual = "{
            foos {
                nodes {
                    inner {
                        prop
                    }
                }
            }
        }"
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()),
        )
        .await,
        expected = Ok(graphql_value!({
            "foos": {
                "nodes": [],
            },
        })),
    );

    let store_entity_mock = DataStoreAPIMock::execute
        .next_call(matching!(Request::BatchWrite(_), _session))
        .returns(Ok(Response::one_inserted(
            foo.entity_builder(json!("my_id"), json!({ "inner": { "prop": "yo" } }))
                .into(),
        )));

    expect_eq!(
        actual = r#"mutation {
            foo(create: [{
                inner: {
                    prop: "yo"
                }
            }]) {
                node {
                    inner {
                        prop
                    }
                }
            }
        }"#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, store_entity_mock),
        )
        .await,
        expected = Ok(graphql_value!({
            "foo": [{
                "node": {
                    "inner": {
                        "prop": "yo"
                    }
                }
            }]
        })),
    );
}

#[test(tokio::test)]
async fn test_docs_introspection() {
    let (test, schema) = "
    def Key (
        rel .is: text
    )

    /// this is a type
    def PublicType (
        rel Key identifies: .
        /// this is a field
        rel .'relation': text
    )
    "
    .compile_single_schema_with_datastore();

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
        .exec([], &schema, &gql_ctx_mock_data_store(&test, ROOT, ()))
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
    let (test, schema) = ARTIST_AND_INSTRUMENT
        .1
        .compile_single_schema_with_datastore();
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
            artists {
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
            [],
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::execute
                    .next_call(matching!(Request::Query(_), _session))
                    .returns(Ok(Response::Query(Sequence::new([ziggy.clone()]))))
            )
        )
        .await,
        expected = Ok(graphql_value!({
            "artists": {
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
            instruments {
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
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty()),
        )
        .await,
        expected = Ok(graphql_value!({
            "instruments": {
                "edges": []
            },
        })),
    );

    expect_eq!(
        actual = r#"mutation {
            artist(create: [
                {
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
                }
            ]) {
                node {
                    ID
                    name
                }
            }
        }"#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::execute
                    .next_call(matching!(Request::BatchWrite(..), _session))
                    .returns(Ok(Response::one_inserted(ziggy.value)))
            )
        )
        .await,
        expected = Ok(graphql_value!({
            "artist": [{
                "node": {
                    "ID": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                    "name": "Ziggy"
                }
            }]
        })),
    );
}

#[test(tokio::test)]
async fn test_unified_mutation_error_on_unrecognized_arg() {
    let (test, schema) = ARTIST_AND_INSTRUMENT
        .1
        .compile_single_schema_with_datastore();

    expect_eq!(
        actual = "mutation { artist(bogus: null) { deleted } }"
            .exec([], &schema, &gql_ctx_mock_data_store(&test, ROOT, ()))
            .await
            .unwrap_first_graphql_error_msg(),
        expected = "Unknown argument \"bogus\" on field \"artist\" of type \"Mutation\". At 0:18\n"
    );
}

#[test(tokio::test)]
async fn test_unified_mutation_create() {
    let (test, schema) = ARTIST_AND_INSTRUMENT
        .1
        .compile_single_schema_with_datastore();
    let ziggy: Attribute = test.bind(["artist"])[0]
        .entity_builder(
            json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"),
            json!({
                "name": "Ziggy",
            }),
        )
        .into();

    expect_eq!(
        actual = r#"
            mutation {
                artist(create: [
                    {
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
                    }
                ]) {
                    node {
                        ID
                        name
                    }
                    deleted
                }
            }
        "#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                ROOT,
                DataStoreAPIMock::execute
                    .next_call(matching!(Request::BatchWrite(..), _session))
                    .returns(Ok(Response::one_inserted(ziggy.value)))
            )
        )
        .await,
        expected = Ok(graphql_value!({
            "artist": [
                {
                    "node": {
                        "ID": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                        "name": "Ziggy"
                    },
                    "deleted": false
                }
            ],
        })),
    );
}

#[test(tokio::test)]
async fn test_create_through_mapped_domain() {
    let (test, [schema]) = TestPackages::with_sources([
        (
            ROOT,
            "
            use 'artist' as a

            def player (
                rel .'id'(rel .gen: auto)|id: (rel .is: uuid)
                rel .'nick': text
            )

            map(
                player(
                    'id': id,
                    'nick': n,
                ),
                a.artist(
                    'ID': id,
                    'name': n,
                ),
            )
        ",
        ),
        // BUG: Not using `artist_and_instrument` here right now because of https://gitlab.com/protojour/memoriam/domain-engine/-/issues/86
        (
            SourceName("artist"),
            "
            def artist (
                rel .'ID'(rel .gen: auto)|id: (fmt '' => 'artist/' => uuid => .)
                rel .'name': text
            )
            ",
        ),
    ])
    .with_data_store(SourceName("artist"), DataStoreConfig::Default)
    .compile_schemas([ROOT]);

    let ziggy: Attribute = test.bind(["artist.artist"])[0]
        .entity_builder(
            json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"),
            json!({ "name": "Ziggy" }),
        )
        .into();

    expect_eq!(
        actual = r#"
            mutation {
                player(create: [{ nick: "Ziggy" }]) {
                    node { id nick }
                    deleted
                }
            }
        "#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                SourceName("artist"),
                DataStoreAPIMock::execute
                    .next_call(matching!(Request::BatchWrite(..), _session))
                    .returns(Ok(Response::one_inserted(ziggy.value)))
            )
        )
        .await,
        expected = Ok(graphql_value!({
            "player": [
                {
                    "node": {
                        "id": "88832e20-8c6e-46b4-af79-27b19b889a58",
                        "nick": "Ziggy"
                    },
                    "deleted": false
                }
            ],
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_guitar_synth_union_selection() {
    let (test, schema) = GUITAR_SYNTH_UNION.1.compile_single_schema_with_datastore();
    let [artist] = test.bind(["artist"]);

    let query_mock = DataStoreAPIMock::execute
        .next_call(matching!(Request::Query(_), _session))
        .returns(Ok(Response::Query(Sequence::new([artist
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
            .into()]))));

    expect_eq!(
        actual = "{
            artists {
                nodes {
                    plays {
                        nodes {
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
        }"
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, query_mock)
        )
        .await,
        expected = Ok(graphql_value!({
            "artists": {
                "nodes": [{
                    "plays": {
                        "nodes": [
                            {
                                "__typename": "synth",
                                "polyphony": 42
                            },
                            {
                                "__typename": "guitar",
                                "string_count": 91
                            }
                        ]
                    }
                }]
            },
        })),
    );
}

#[test]
fn test_graphql_guitar_synth_union_input_union_field_list() {
    let (_test, schema) = GUITAR_SYNTH_UNION.1.compile_single_schema_with_datastore();
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
            FieldInfo::from(("played_by", "artistPatchEdgesInput!")),
        ]
    );
}

#[test(tokio::test)]
async fn test_graphql_guitar_synth_union_input_exec() {
    let (test, schema) = GUITAR_SYNTH_UNION.1.compile_single_schema_with_datastore();
    let [artist] = test.bind(["artist"]);
    let store_entity_mock = DataStoreAPIMock::execute
        .next_call(matching!(Request::BatchWrite(..), _session))
        .returns(Ok(Response::one_inserted(
            artist
                .entity_builder(
                    json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"),
                    json!({
                        "name": "Ziggy",
                        "plays": []
                    }),
                )
                .into(),
        )));

    expect_eq!(
        actual = r#"mutation {
            artist(create: [{
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
            }]) {
                node {
                    artist_id
                    name
                }
            }
        }"#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, store_entity_mock)
        )
        .await,
        expected = Ok(graphql_value!({
            "artist": [{
                "node": {
                    "artist_id": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                    "name": "Ziggy"
                }
            }]
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_guitar_synth_union_input_error_span() {
    let (test, schema) = GUITAR_SYNTH_UNION.1.compile_single_schema_with_datastore();

    expect_eq!(
        actual = r#"mutation {
            artist(create: [{
                name: "Ziggy",
                plays: [
                    {
                        instrument_id: "bogus_value_here"
                    }
                ]
            }]) {
                node {
                    artist_id
                    name
                }
            }
        }"#
        .exec([], &schema, &gql_ctx_mock_data_store(&test, ROOT, ()))
        .await
        .unwrap_first_exec_error_msg(),
        expected = "invalid map value, expected `instrument` (one of id, id, `guitar`, `synth`) in input at line 4 column 20"
    );
}

#[test(tokio::test)]
async fn test_graphql_municipalities() {
    let (test, [schema]) = TestPackages::with_sources([(ROOT, MUNICIPALITIES.1), GEOJSON, WGS])
        .compile_schemas([ROOT]);

    expect_eq!(
        actual = "{
            municipalities {
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
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty())
        )
        .await,
        expected = Ok(graphql_value!({
            "municipalities": {
                "edges": []
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_municipalities_named_query() {
    let (test, [schema]) = TestPackages::with_sources([(ROOT, MUNICIPALITIES.1), GEOJSON, WGS])
        .compile_schemas([ROOT]);
    let [municipality] = test.bind(["municipality"]);

    async fn fetch_osl(
        schema: &Schema,
        ctx: &ServiceCtx,
    ) -> Result<juniper::Value<GqlScalar>, TestError> {
        // NOTE: Here it's apparent that geojson can't be modelled
        // in GraphQL. Geometry is a union where many of the variants
        // have a "coordinates" field of differing types.
        // GraphQL requires the client to make field aliases when the types are different.
        // But with the aliases, the returned JSON is no longer geojson.
        // See also https://gitlab.com/protojour/memoriam/domain-engine/-/issues/7.
        r#"{
            municipality(code: "OSL") {
                code
                geometry {
                    __typename
                    ... on _geojson_Point {
                        point_coordinates: coordinates
                    }
                    ... on _geojson_Polygon {
                        polygon_coordinates: coordinates
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
        .exec([], schema, ctx)
        .await
    }

    expect_eq!(
        actual = fetch_osl(
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, mock_data_store_query_entities_empty())
        )
        .await,
        expected = Ok(graphql_value!({
            "municipality": null
        }))
    );

    let query_mock = DataStoreAPIMock::execute
        .next_call(matching!(Request::Query(_), _session))
        .returns(Ok(Response::Query(Sequence::new([municipality
            .entity_builder(
                json!("OSL"),
                json!({
                    "code": "OSL",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [10.738889, 59.913333],
                    }
                }),
            )
            .into()]))));

    expect_eq!(
        actual = fetch_osl(&schema, &gql_ctx_mock_data_store(&test, ROOT, query_mock)).await,
        expected = Ok(graphql_value!({
            "municipality": {
                "code": "OSL",
                "geometry": {
                    "__typename": "_geojson_Point",
                    "point_coordinates": [10.738889, 59.913333]
                }
            }
        }))
    );
}

#[test]
fn test_graphql_municipalities_geojson_union() {
    let (_test, [schema]) = TestPackages::with_sources([(ROOT, MUNICIPALITIES.1), GEOJSON, WGS])
        .with_data_store(ROOT, DataStoreConfig::Default)
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

#[test(tokio::test)]
async fn test_graphql_open_data() {
    let (test, schema) = "
    def(open) foo (
        rel .'id'(rel .gen: auto)|id: (rel .is: text)
    )
    "
    .compile_single_schema_with_datastore();
    let [foo] = test.bind(["foo"]);
    let store_entity_mock = DataStoreAPIMock::execute
        .next_call(matching!(Request::BatchWrite(..), _session))
        .returns(Ok(Response::one_inserted(
            foo.entity_builder(json!("the-id"), json!({}))
                .with_open_data(json!({ "foo": "bar" }))
                .into(),
        )));

    expect_eq!(
        actual = r#"mutation {
            foo(create: [{ open_prop: "hei" }]) {
                node {
                    id
                    _open_data
                }
            }
        }"#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, store_entity_mock)
                .with_serde_processor_profile_flags(
                    ProcessorProfileFlags::DESERIALIZE_OPEN_DATA
                        | ProcessorProfileFlags::SERIALIZE_OPEN_DATA
                )
        )
        .await,
        expected = Ok(graphql_value!({
            "foo": [{
                "node": {
                    "id": "the-id",
                    "_open_data": {
                        "foo": "bar"
                    }
                }
            }]
        })),
    );
}

#[test(tokio::test)]
async fn test_open_data_disabled() {
    let (test, schema) = "
    def(open) foo (
        rel .'id'(rel .gen: auto)|id: (rel .is: text)
    )
    "
    .compile_single_schema_with_datastore();
    let [foo] = test.bind(["foo"]);

    expect_eq!(
        actual = r#"mutation {
            foo(create: [{ open_prop: "hei" }]) {
                node { id }
            }
        }"#
        .exec([], &schema, &gql_ctx_mock_data_store(&test, ROOT, ()))
        .await
        .unwrap_first_exec_error_msg(),
        expected = "unknown property `open_prop` in input at line 1 column 25"
    );

    let store_entity_mock = DataStoreAPIMock::execute
        .next_call(matching!(Request::BatchWrite(..), _session))
        .returns(Ok(Response::one_inserted(
            foo.entity_builder(json!("the-id"), json!({})).into(),
        )));

    expect_eq!(
        actual = r#"mutation {
            foo(create: [{}]) {
                node {
                    id
                    _open_data
                }
            }
        }"#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, ROOT, store_entity_mock)
        )
        .await
        .unwrap_first_exec_error_msg(),
        expected = "open data is not available in this GraphQL context"
    );
}

#[test(tokio::test)]
async fn test_gitmesh_id_error() {
    let (test, schema) = GITMESH.1.compile_single_schema_with_datastore();

    expect_eq!(
        actual = r#"mutation {
            Repository(
                create: [
                    {
                        handle: "badproj"
                        owner: {
                            id: "BOGUS_PREFIX/bob"
                        }
                    }
                ]
            ) { node { id } }
        }"#
        .exec([], &schema, &gql_ctx_mock_data_store(&test, ROOT, ()))
        .await
        .unwrap_first_exec_error_msg(),
        expected =
            "invalid map value, expected `RepositoryOwner` (id or id) in input at line 5 column 31"
    );
}
