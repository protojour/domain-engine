//! GraphQL "unit" tests, i.e. only mocked datastore

use domain_engine_core::transact::{ReqMessage, RespMessage, ValueReason};
use domain_engine_graphql::{
    context::ServiceCtx,
    gql_scalar::GqlScalar,
    juniper::{self, graphql_value},
    Schema,
};
use ontol_macros::test;
use ontol_runtime::{
    attr::AttrMatrix,
    interface::serde::processor::ProcessorProfileFlags,
    sequence::{Sequence, SubSequence},
    value::Value,
};
use ontol_test_utils::{
    examples::{ARTIST_AND_INSTRUMENT, GEOJSON, GUITAR_SYNTH_UNION, MUNICIPALITIES, WGS},
    expect_eq, src_name, SrcName, TestPackages,
};
use serde_json::json;
use unimock::*;

use domain_engine_test_utils::{
    graphql_test_utils::{
        gql_ctx_mock_data_store, Exec, GraphqlTestResultExt, TestCompileSchema,
        TestCompileSingletonSchema, TestError,
    },
    mock_datastore::{
        mock_data_store_query_entities_empty, respond_inserted, respond_queried, LinearTransactMock,
    },
    parser_document_utils::{
        find_input_object_type, find_object_field, find_object_type, FieldInfo,
    },
};

fn root() -> SrcName {
    SrcName::default()
}

#[test]
#[should_panic = "GraphqlInterfaceNotFound"]
fn test_graphql_schema_for_entityless_domain_should_not_be_generated() {
    "".compile_single_schema();
}

#[test(tokio::test)]
async fn version() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id': (rel* is: text)
    )
    "
    .compile_single_schema();

    expect_eq!(
        actual = "{_version}"
            .exec([], &schema, &gql_ctx_mock_data_store(&test, &[root()], ()),)
            .await,
        expected = Ok(graphql_value!({
            "_version": "0.0.0",
        })),
    );
}

#[test]
fn field_order() {
    let (_test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def ent1 (
        rel. 'id': (rel* is: text)
        rel* 'subjects'::'obj_1' {subject}
    )
    def subject (
        rel* is: a
        rel* is: b
        rel. 'id': (rel* is: text)
        rel* 'connection': {ent1}
        rel* 'field': text
        rel* is: c
        rel* is: d
    )
    def ent2 (
        rel. 'id': (rel* is: text)
        rel* 'subjects'::'obj_2' {subject}
    )

    def a(rel* 'a': text)
    def b(rel* 'b': text)
    def c(rel* 'c': text)
    def d(rel* 'd': text)
    "
    .compile_single_schema();

    let document = schema.as_document();
    let field_names: Vec<&str> = find_object_type(&document, "subject")
        .unwrap()
        .fields
        .iter()
        .map(|field| field.name)
        .collect();

    expect_eq!(
        actual = field_names,
        expected = vec![
            "id",
            "field",
            "a",
            "b",
            "c",
            "d",
            // collections are placed after regular fields:
            "connection",
            "obj_1",
            "obj_2"
        ]
    );
}

#[test(tokio::test)]
async fn int_scalars() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo_id (fmt '' => text => .)
    def smallint (
        rel* is: integer
        rel* min: 0
        rel* max: 255
    )
    def foo (
        rel. 'id': foo_id
        rel* 'small': smallint
        rel* 'big': i64
    )

    map foos(
        (),
        foo {..@match foo()},
    )
    "
    .compile_single_schema();

    {
        let document = schema.as_document();
        let foo_object = find_object_type(&document, "foo").unwrap();

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
            &gql_ctx_mock_data_store(&test, &[root()], mock_data_store_query_entities_empty()),
        )
        .await,
        expected = Ok(graphql_value!({
            "foos": {
                "edges": [],
            },
        })),
    );

    let store_entity_mock = LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Insert(..)), ..], _session))
        .returns(respond_inserted([foo
            .entity_builder(
                json!("my_id"),
                json!({ "id": "my_id", "small": 42, "big": 112233445566778899_i64 }),
            )
            .into()]));

    expect_eq!(
        actual = "mutation {
            foo(create: [{
                id: \"my_id\"
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
            &gql_ctx_mock_data_store(&test, &[root()], store_entity_mock),
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
async fn non_entity_set_mutation() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id': (rel* is: text)
        rel* 'bars': {bar}
    )
    def bar (
        rel* 'field': text
    )
    "
    .compile_single_schema();

    let [foo] = test.bind(["foo"]);

    {
        let document = schema.as_document();
        let foo_input = find_input_object_type(&document, "fooInput").unwrap();
        let bar_field_info = FieldInfo::from(
            foo_input
                .fields
                .iter()
                .find(|field| field.name == "bars")
                .unwrap(),
        );

        assert_eq!(bar_field_info.field_type, "[barInput!]!");
    }

    expect_eq!(
        actual = "mutation {
            foo(create: []) {
                node {
                    id
                    bars { field }
                }
            }
        }"
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                &[root()],
                LinearTransactMock::transact
                    .next_call(matching!([Ok(ReqMessage::Insert(..)), ..], _session))
                    .returns(respond_inserted([foo
                        .entity_builder(
                            json!("my_id"),
                            json!({ "id": "N/A", "bars": [{ "field": "text" }] })
                        )
                        .into()]))
            ),
        )
        .await,
        expected = Ok(graphql_value!({
            "foo": [{
                "node": { "id": "N/A", "bars": [{"field": "text" }]}
            }],
        })),
    );
}

#[test(tokio::test)]
async fn basic_inherent_auto_id_anonymous_type() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id'[rel* gen: auto]: (rel* is: text)
    )
    map foos(
        (),
        foo {..@match foo()}
    )
    "
    .compile_single_schema();

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
            &gql_ctx_mock_data_store(&test, &[root()], mock_data_store_query_entities_empty()),
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
async fn basic_pagination() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id'[rel* gen: auto]: (rel* is: text)
    )
    map foos(
        (),
        foo { ..@match foo() },
    )
    "
    .compile_single_schema();

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
            &gql_ctx_mock_data_store(&test, &[root()], mock_data_store_query_entities_empty()),
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
                &[root()],
                LinearTransactMock::transact
                    .next_call(matching!([Ok(ReqMessage::Query(..))], _session))
                    .answers(&|_, req_messages, _| {
                        let Ok(ReqMessage::Query(_, entity_select)) = req_messages.first().unwrap()
                        else {
                            panic!();
                        };
                        assert!(!entity_select.include_total_len);
                        assert_eq!(entity_select.limit, 42);
                        assert_eq!(entity_select.after_cursor.as_deref().unwrap(), &[b'1']);

                        Ok(vec![Ok(RespMessage::SequenceStart(
                            0,
                            Some(Box::new(SubSequence {
                                end_cursor: Some(Box::new([b'2'])),
                                has_next: true,
                                total_len: Some(42),
                            })),
                        ))])
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
                &[root()],
                LinearTransactMock::transact
                    .next_call(matching!([Ok(ReqMessage::Query(0, _))], _session))
                    .answers(&|_, req_messages, _| {
                        let Ok(ReqMessage::Query(_, entity_select)) = req_messages.first().unwrap()
                        else {
                            panic!();
                        };
                        assert!(entity_select.include_total_len);
                        assert_eq!(entity_select.limit, 1);
                        assert_eq!(entity_select.after_cursor, None);

                        Ok(vec![Ok(RespMessage::SequenceStart(
                            0,
                            Some(Box::new(SubSequence {
                                end_cursor: Some(Box::new([b'1'])),
                                has_next: true,
                                total_len: Some(42),
                            })),
                        ))])
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
async fn nodes() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id'[rel* gen: auto]: (rel* is: text)
    )
    map foos(
        (),
        foo { ..@match foo() }
    )
    "
    .compile_single_schema();

    let [foo] = test.bind(["foo"]);
    let query_mock = LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Query(..))], _))
        .returns(respond_queried([foo
            .entity_builder(json!("id"), json!({}))
            .into()]));

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
            &gql_ctx_mock_data_store(&test, &[root()], query_mock),
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
fn value_type_as_field() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (rel* is: text)
    def bar (
        rel. 'id'[rel* gen: auto]: (rel* is: text)
        rel* 'foo': foo
    )
    "
    .compile_single_schema();
}

#[test]
fn value_type_in_array() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (rel* is: text)
    def bar (
        rel. 'id'[rel* gen: auto]: (rel* is: text)
        rel* 'foo': {foo}
    )
    "
    .compile_single_schema();
}

#[test(tokio::test)]
async fn inner_struct() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo_id (fmt '' => text => .)
    def inner (
        rel* 'prop': text
    )
    def foo (
        rel. 'id': foo_id
        rel* 'inner': inner
    )
    map foos(
        (),
        foo { ..@match foo() },
    )
    "
    .compile_single_schema();

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
            &gql_ctx_mock_data_store(&test, &[root()], mock_data_store_query_entities_empty()),
        )
        .await,
        expected = Ok(graphql_value!({
            "foos": {
                "nodes": [],
            },
        })),
    );

    let store_entity_mock = LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Insert(..)), ..], _session))
        .returns(respond_inserted([foo
            .entity_builder(
                json!("my_id"),
                json!({ "id": "my_id", "inner": { "prop": "yo" } }),
            )
            .into()]));

    expect_eq!(
        actual = r#"mutation {
            foo(create: [{
                id: "1"
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
            &gql_ctx_mock_data_store(&test, &[root()], store_entity_mock),
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
async fn docs_introspection() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def Key (
        rel* is: text
    )

    /// this is a type
    def PublicType (
        rel. 'id': Key
        /// this is a field
        rel* 'relation': text
    )
    "
    .compile_single_schema();

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
        .exec([], &schema, &gql_ctx_mock_data_store(&test, &[root()], ()))
        .await,
        expected = Ok(graphql_value!({
            "__type": {
                "name": "PublicType",
                "description": "this is a type",
                "fields": [
                    {
                        "name": "id",
                        "description": null
                    },
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
async fn artist_and_instrument_connections() {
    let (test, schema) = ARTIST_AND_INSTRUMENT.1.compile_single_schema();
    let [artist, instrument, plays] = test.bind(["artist", "instrument", "plays"]);
    let ziggy: Value = artist
        .entity_builder(
            json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"),
            json!({
                "name": "Ziggy",
            }),
        )
        .relationship(
            "plays",
            AttrMatrix::from_iter([
                Sequence::from_iter([instrument
                    .entity_builder(
                        json!("instrument/88832e20-8c6e-46b4-af79-27b19b889a58"),
                        json!({
                            "name": "Guitar",
                        }),
                    )
                    .to_value()]),
                Sequence::from_iter([plays
                    .value_builder(json!({ "how_much": "A lot" }))
                    .to_value()]),
            ])
            .into(),
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
                &[root()],
                LinearTransactMock::transact
                    .next_call(matching!([Ok(ReqMessage::Query(0, _))], _session))
                    .returns(respond_queried([ziggy.clone()]))
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
            &gql_ctx_mock_data_store(&test, &[root()], mock_data_store_query_entities_empty()),
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
                &[root()],
                LinearTransactMock::transact
                    .next_call(matching!([Ok(ReqMessage::Insert(0, _)), ..], _session))
                    .returns(respond_inserted([ziggy]))
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
async fn unified_mutation_error_on_unrecognized_arg() {
    let (test, schema) = ARTIST_AND_INSTRUMENT.1.compile_single_schema();

    expect_eq!(
        actual = "mutation { artist(bogus: null) { deleted } }"
            .exec([], &schema, &gql_ctx_mock_data_store(&test, &[root()], ()))
            .await
            .unwrap_first_graphql_error_msg(),
        expected = "Unknown argument \"bogus\" on field \"artist\" of type \"Mutation\". At 0:18\n"
    );
}

#[test(tokio::test)]
async fn unified_mutation_create() {
    let (test, schema) = ARTIST_AND_INSTRUMENT.1.compile_single_schema();
    let ziggy: Value = test.bind(["artist"])[0]
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
                &[root()],
                LinearTransactMock::transact
                    .next_call(matching!([Ok(ReqMessage::Insert(0, _)), ..], _session))
                    .returns(respond_inserted([ziggy]))
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
async fn create_through_mapped_domain() {
    let (test, [schema]) = TestPackages::with_static_sources([
        (
            root(),
            "
            use 'artist_and_instrument' as ai

            def player (
                rel. 'id'[rel* gen: auto]: (rel* is: uuid)
                rel* 'nick': text
            )

            map(
                player(
                    'id': id,
                    'nick': n,
                ),
                ai.artist(
                    'ID': id,
                    'name': n,
                ),
            )
        ",
        ),
        ARTIST_AND_INSTRUMENT,
    ])
    .compile_schemas([root()]);

    let ziggy: Value = test.bind(["artist_and_instrument.artist"])[0]
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
                &[ARTIST_AND_INSTRUMENT.0],
                LinearTransactMock::transact
                    .next_call(matching!([Ok(ReqMessage::Insert(0, _)), ..], _session))
                    .returns(respond_inserted([ziggy]))
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
async fn create_through_three_domains() {
    let (test, [schema]) = TestPackages::with_static_sources([
        (
            root(),
            "
            use 'player' as player

            def actor (
                rel. 'ID'[rel* gen: auto]: (fmt '' => 'actor/' => uuid => .)
                rel* 'alias': text
            )

            map(
                actor('ID': id, 'alias': c),
                player.player('id': id, 'nick': c),
            )
        ",
        ),
        (
            src_name("player"),
            "
            use 'artist_and_instrument' as ai

            def player (
                rel. 'id'[rel* gen: auto]: (rel* is: uuid)
                rel* 'nick': text
            )

            map(
                player('id': id, 'nick': n),
                ai.artist('ID': id, 'name': n),
            )
        ",
        ),
        ARTIST_AND_INSTRUMENT,
    ])
    .compile_schemas([root()]);

    let ziggy: Value = test.bind(["artist_and_instrument.artist"])[0]
        .entity_builder(
            json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"),
            json!({ "name": "Ziggy" }),
        )
        .into();

    expect_eq!(
        actual = r#"
            mutation {
                actor(create: [{ alias: "Ziggy" }]) {
                    node { ID alias }
                    deleted
                }
            }
        "#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(
                &test,
                &[ARTIST_AND_INSTRUMENT.0],
                LinearTransactMock::transact
                    .next_call(matching!([Ok(ReqMessage::Insert(0, _)), ..], _session))
                    .returns(respond_inserted([ziggy]))
            )
        )
        .await,
        expected = Ok(graphql_value!({
            "actor": [
                {
                    "node": {
                        "ID": "actor/88832e20-8c6e-46b4-af79-27b19b889a58",
                        "alias": "Ziggy"
                    },
                    "deleted": false
                }
            ],
        })),
    );
}

#[test(tokio::test)]
async fn guitar_synth_union_selection() {
    let (test, schema) = GUITAR_SYNTH_UNION.1.compile_single_schema();
    let [artist] = test.bind(["artist"]);

    let query_mock = LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Query(0, _))], _session))
        .returns(Ok(vec![
            Ok(RespMessage::SequenceStart(0, None)),
            Ok(RespMessage::Element(
                artist
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
                    .into(),
                ValueReason::Queried,
            )),
        ]));

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
            &gql_ctx_mock_data_store(&test, &[root()], query_mock)
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
fn guitar_synth_union_input_union_field_list() {
    let (_test, schema) = GUITAR_SYNTH_UNION.1.compile_single_schema();
    let document = schema.as_document();

    let instrument_edge_input = find_input_object_type(&document, "instrumentEdgeInput").unwrap();

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
async fn graphql_guitar_synth_union_input_exec() {
    let (test, schema) = GUITAR_SYNTH_UNION.1.compile_single_schema();
    let [artist] = test.bind(["artist"]);
    let store_entity_mock = LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Insert(..)), ..], _session))
        .returns(respond_inserted([artist
            .entity_builder(
                json!("artist/88832e20-8c6e-46b4-af79-27b19b889a58"),
                json!({
                    "name": "Ziggy",
                    "plays": []
                }),
            )
            .into()]));

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
            &gql_ctx_mock_data_store(&test, &[root()], store_entity_mock)
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
async fn guitar_synth_union_input_error_span() {
    let (test, schema) = GUITAR_SYNTH_UNION.1.compile_single_schema();

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
        .exec([], &schema, &gql_ctx_mock_data_store(&test, &[root()], ()))
        .await
        .unwrap_first_exec_error_msg(),
        expected = "invalid type, expected `instrument` (`guitar` or `synth`) in input at line 4 column 20"
    );
}

#[test(tokio::test)]
async fn municipalities() {
    let (test, [schema]) = TestPackages::with_static_sources([MUNICIPALITIES, GEOJSON, WGS])
        .compile_schemas([MUNICIPALITIES.0]);

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
            &gql_ctx_mock_data_store(
                &test,
                &[MUNICIPALITIES.0],
                mock_data_store_query_entities_empty()
            )
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
async fn municipalities_named_query() {
    let (test, [schema]) = TestPackages::with_static_sources([MUNICIPALITIES, GEOJSON, WGS])
        .compile_schemas([MUNICIPALITIES.0]);
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
            &gql_ctx_mock_data_store(
                &test,
                &[MUNICIPALITIES.0],
                mock_data_store_query_entities_empty()
            )
        )
        .await,
        expected = Ok(graphql_value!({
            "municipality": null
        }))
    );

    let query_mock = LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Query(..))], _session))
        .returns(Ok(vec![
            Ok(RespMessage::SequenceStart(0, None)),
            Ok(RespMessage::Element(
                municipality
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
                    .into(),
                ValueReason::Queried,
            )),
        ]));

    expect_eq!(
        actual = fetch_osl(
            &schema,
            &gql_ctx_mock_data_store(&test, &[MUNICIPALITIES.0], query_mock)
        )
        .await,
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
fn municipalities_geojson_union() {
    let (_test, [schema]) = TestPackages::with_static_sources([MUNICIPALITIES, GEOJSON, WGS])
        .compile_schemas([MUNICIPALITIES.0]);

    let document = schema.as_document();

    let geometry_union_input =
        find_input_object_type(&document, "_geojson_GeometryUnionInput").unwrap();

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
async fn open_data() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def @open foo (
        rel. 'id'[rel* gen: auto]: (rel* is: text)
    )
    "
    .compile_single_schema();
    let [foo] = test.bind(["foo"]);
    let store_entity_mock = LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Insert(..)), ..], _session))
        .returns(respond_inserted([foo
            .entity_builder(json!("the-id"), json!({}))
            .with_open_data(json!({ "foo": "bar" }))
            .into()]));

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
            &gql_ctx_mock_data_store(&test, &[root()], store_entity_mock)
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
async fn open_data_disabled() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def @open foo (
        rel. 'id'[rel* gen: auto]: (rel* is: text)
    )
    "
    .compile_single_schema();
    let [foo] = test.bind(["foo"]);

    expect_eq!(
        actual = r#"mutation {
            foo(create: [{ open_prop: "hei" }]) {
                node { id }
            }
        }"#
        .exec([], &schema, &gql_ctx_mock_data_store(&test, &[root()], ()))
        .await
        .unwrap_first_exec_error_msg(),
        expected = "unknown property `open_prop` in input at line 1 column 25"
    );

    let store_entity_mock = LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Insert(..)), ..], _session))
        .returns(respond_inserted([foo
            .entity_builder(json!("the-id"), json!({}))
            .into()]));

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
            &gql_ctx_mock_data_store(&test, &[root()], store_entity_mock)
        )
        .await
        .unwrap_first_exec_error_msg(),
        expected = "open data is not available in this GraphQL context"
    );
}

// A bug when using both .is and .'member' from foreign domain
#[test(tokio::test)]
async fn test_extension_and_member_from_foreign_domain() {
    let (_test, [_]) = TestPackages::with_static_sources([
        (
            src_name("entry"),
            "
            domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
            use 'helper' as helper

            def foo (
                rel. 'id': (rel* is: uuid)
                rel* is: helper.ext
                rel* 'member'?: {helper.member}
            )
        ",
        ),
        (
            src_name("helper"),
            "
            def ext (rel* 'ext-field': text)
            def member (rel* 'member-field': text)
            ",
        ),
    ])
    .compile_schemas([src_name("entry")]);
}

/// Regresssion test for `no PatchEdges available for relation sequence "targets"``
#[test(tokio::test)]
async fn test_const_in_union_bug() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def union (
        rel* is?: member
        rel* 'targets'::'unions' {target}
    )

    def member (
        rel. 'id'[rel* gen: auto]: (fmt '' => 'member/' => text => .)
        rel* 'const': 'const'
    )

    def target (
        rel. 'id'[rel* gen: auto]: (fmt '' => 'target/' => text => .)
    )
    "
    .compile_single_schema();
}

// regression test for https://gitlab.com/protojour/memoriam/domain-engine/-/issues/109
#[test(tokio::test)]
async fn test_constant_index_panic() {
    let (test, [schema]) = TestPackages::with_static_sources([
        (
            src_name("events"),
            "
            use 'events_db' as db

            def event (
                rel. '_id'[rel* gen: auto]: (rel* is: serial)
                rel* '_class': 'event'
            )

            map(
                event (
                    '_id': id,
                    '_class': class,
                ),
                db.event (
                    '_id': id,
                    '_class': class,
                ),
            )

            map events (
                (),
                event { ..@match db.event() }
            )
            ",
        ),
        (
            src_name("events_db"),
            "
            domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
            def event (
                rel. '_id'[rel* gen: auto]: (rel* is: serial)
                rel* '_class': 'event'
            )
            ",
        ),
    ])
    .compile_schemas([src_name("events")]);

    let [event] = test.bind(["events_db.event"]);

    let store_entity_mock = LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Query(..)), ..], _session))
        .returns(respond_queried([event
            .entity_builder(json!("0"), json!({ "_class": "event" }))
            .into()]));

    let _ = "{ events { nodes { _id _class } } }"
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, &[src_name("events")], store_entity_mock),
        )
        .await;
}

#[test(tokio::test)]
async fn flattened_union_entity() {
    let (test, [schema]) = TestPackages::with_static_sources([(
        SrcName::default(),
        "
        domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
        def kind ()

        def foo (
            rel. 'id': (rel* is: text)
            rel* kind: (
                rel* is?: bar
                rel* is?: qux
            )
        )

        def bar (
            rel* 'kind': 'bar'
            rel* 'data': text
            rel* 'bar': i64
        )
        def qux (
            rel* 'kind': 'qux'
            rel* 'data': i64
            rel* 'qux': text
        )

        map foos (
            (),
            foo { ..@match foo() }
        )
        ",
    )])
    .compile_schemas([SrcName::default()]);

    let document = schema.as_document();

    let geometry_union_input = find_input_object_type(&document, "fooInput").unwrap();

    let field_names: Vec<_> = geometry_union_input
        .fields
        .iter()
        .map(FieldInfo::from)
        .collect();

    expect_eq!(
        actual = field_names.as_slice(),
        expected = &[
            FieldInfo::from(("id", "ID!")),
            FieldInfo::from(("kind", "String!")),
            FieldInfo::from(("data", "_ontol_json")),
            FieldInfo::from(("bar", "_ontol_i64")),
            FieldInfo::from(("qux", "String")),
        ]
    );

    let [foo] = test.bind(["foo"]);

    let store_entity_mock = LinearTransactMock::transact
        .next_call(matching!([Ok(ReqMessage::Insert(..)), ..], _session))
        .returns(respond_inserted([foo
            .entity_builder(
                json!("id"),
                json!({ "id": "id", "kind": "bar", "data": "value", "bar": 1337 }),
            )
            .into()]));

    expect_eq!(
        actual = r#"mutation {
            foo(create: [{
                id: "id",
                kind: "bar",
                data: "data",
                bar: 42
            }]) {
                node {
                    id
                    kind
                    ...on foo_kind_bar {
                        textData: data
                        bar
                    }
                    ...on foo_kind_qux {
                        intData: data
                        qux
                    }
                }
            }
        }"#
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, &[root()], store_entity_mock)
        )
        .await,
        expected = Ok(graphql_value!({
            "foo": [{
                "node": {
                    "id": "id",
                    "kind": "bar",
                    "textData": "value",
                    "bar": 1337
                }
            }]
        })),
    );
}

#[test(tokio::test)]
async fn schema_bug1() {
    let (_test, [_schema]) = TestPackages::with_static_sources([(
        SrcName::default(),
        "
        domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
        def foo (rel. 'id': (rel* is: text))
        def bar (rel. 'id': (rel* is: text))
        rel {bar} 'foo': foo
        rel {foo} 'bar2'::'foo2'? {bar}
        ",
    )])
    .compile_schemas([SrcName::default()]);
}
