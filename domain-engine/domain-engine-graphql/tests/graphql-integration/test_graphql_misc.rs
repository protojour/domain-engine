use std::sync::Arc;

use domain_engine_core::{DomainEngine, Session};
use domain_engine_graphql::{context::ServiceCtx, juniper::graphql_value};
use domain_engine_test_utils::dynamic_data_store::DynamicDataStoreFactory;
use domain_engine_test_utils::graphql_test_utils::GraphqlTestResultExt;
use domain_engine_test_utils::{
    graphql_test_utils::{Exec, GraphqlValueResultExt, TestCompileSchema},
    graphql_value_unordered,
    system::mock_current_time_monotonic,
    unimock,
};
use ontol_macros::datastore_test;
use ontol_runtime::ontology::Ontology;
use ontol_test_utils::examples::EDGE_ENTITY_UNION;
use ontol_test_utils::{
    examples::{entity_subtype, EDGE_ENTITY_SIMPLE, GUITAR_SYNTH_UNION},
    expect_eq, SrcName, TestPackages,
};
use tracing::info;

async fn make_domain_engine(ontology: Arc<Ontology>, datastore: &str) -> DomainEngine {
    DomainEngine::builder(ontology)
        .system(Box::new(unimock::Unimock::new(
            mock_current_time_monotonic(),
        )))
        .build(DynamicDataStoreFactory::new(datastore), Session::default())
        .await
        .unwrap()
}

#[datastore_test(tokio::test)]
async fn test_guitar_synth_union_mutation_and_query(ds: &str) {
    let (test, [schema]) = TestPackages::with_static_sources([GUITAR_SYNTH_UNION])
        .compile_schemas([GUITAR_SYNTH_UNION.0]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), ds).await.into();

    expect_eq!(
        actual = r#"mutation {
            artist(
                create: [{
                    name: "Bowie",
                    plays: [
                        {
                            type: "guitar"
                            string_count: 6
                        },
                        {
                            type: "synth"
                            polyphony: 1
                        },
                        {
                            type: "synth"
                            polyphony: 6
                        }
                    ]
                }]
            ) {
                node {
                    name
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
        }"#
        .exec([], &schema, &ctx)
        .await
        .unordered(),
        expected = Ok(graphql_value_unordered!({
            "artist": [{
                "node": {
                    "name": "Bowie",
                    "plays": {
                        "nodes": [
                            {
                                "__typename": "guitar",
                                "string_count": 6
                            },
                            {
                                "__typename": "synth",
                                "polyphony": 1
                            },
                            {
                                "__typename": "synth",
                                "polyphony": 6
                            },
                        ]
                    }
                }
            }]
        })),
    );

    expect_eq!(
        actual = r#"{
            artists {
                nodes {
                    name
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
        }"#
        .exec([], &schema, &ctx)
        .await
        .unordered(),
        expected = Ok(graphql_value_unordered!({
            "artists": {
                "nodes": [{
                    "name": "Bowie",
                    "plays": {
                        "nodes": [
                            {
                                "__typename": "guitar",
                                "string_count": 6
                            },
                            {
                                "__typename": "synth",
                                "polyphony": 1
                            },
                            {
                                "__typename": "synth",
                                "polyphony": 6
                            },
                        ]
                    }
                }]
            }
        })),
    );
}

/// FIXME: implement for arango
#[datastore_test(tokio::test, ignore("arango"))]
async fn test_entity_subtype(ds: &str) {
    let (test, [derived_schema, db_schema]) =
        TestPackages::with_static_sources([entity_subtype::DERIVED, entity_subtype::DB])
            .compile_schemas([entity_subtype::DERIVED.0, entity_subtype::DB.0]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), ds).await.into();

    r#"mutation {
        foo(create: [
            {
                id: "1",
                type: "bar",
                name: "NAME!"
            },
            {
                id: "2",
                type: "baz"
            }
        ]) { node { id } }
    }"#
    .exec([], &db_schema, &ctx)
    .await
    .unwrap();

    expect_eq!(
        actual = r#"{
            bars {
                nodes {
                    id
                    name
                }
            }
        }"#
        .exec([], &derived_schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "bars": {
                "nodes": [{
                    "id": "1",
                    "name": "NAME!"
                }]
            }
        }))
    );
}

#[datastore_test(tokio::test)]
async fn sym_edge_simple(ds: &str) {
    let (test, [schema]) = TestPackages::with_static_sources([(
        SrcName::default(),
        "
        domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
        sym {
            (a) prop: (b),
            (b) reverse_prop: (a),
        }

        def foo (
            rel. 'id': (rel* is: text)
            rel* prop: {bar}
        )
        def bar (
            rel. 'id': (rel* is: text)
            rel* reverse_prop: {foo}
        )

        map bars (
            (),
            bar { ..@match bar() }
        )
        ",
    )])
    .compile_schemas([SrcName::default()]);

    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), ds).await.into();

    r#"mutation {
        foo(
            create: [{
                id: "foo1"
                prop: [{ id: "bar1" }]
            }]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    expect_eq!(
        actual = r#"{
            bars {
                nodes {
                    id
                    reverse_prop {
                        nodes {
                            id
                        }
                    }
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "bars": {
                "nodes": [{
                    "id": "bar1",
                    "reverse_prop": {
                        "nodes": [{ "id": "foo1" }]
                    }
                }]
            }
        }))
    );
}

/// FIXME: implement for arango
#[datastore_test(tokio::test, ignore("arango"))]
async fn edge_entity_simple_happy_path(ds: &str) {
    let (test, [schema]) = TestPackages::with_static_sources([EDGE_ENTITY_SIMPLE])
        .compile_schemas([EDGE_ENTITY_SIMPLE.0]);

    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), ds).await.into();

    info!("Create 3 vertices");

    r#"mutation {
        bar(
            create: [{ id: "bar1", bar_field: "VALUE" }]
        ) { node { id } }
        link(
            create: [{ id: "link1", from: "foo1", to: "bar1" }]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    info!("Query data");

    expect_eq!(
        actual = r#"{
            links { nodes { id, from, to } }
            foos { nodes { id, related_to { nodes { id } } } }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "links": { "nodes": [{
                "id": "link1",
                "from": "foo1",
                "to": "bar1",
            }]},
            "foos": {
                "nodes": [{
                    "id": "foo1",
                    "related_to": {
                        "nodes": [{ "id": "bar1" }]
                    }
                }]
            }
        }))
    );

    info!("Delete link");

    r#"mutation {
        link(delete: ["link1"]) { deleted }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    info!("Query data after link deletion");

    expect_eq!(
        actual = r#"{
            links { nodes { id, from, to } }
            foos { nodes { id, related_to { nodes { id } } } }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "links": { "nodes": [] },
            "foos": {
                "nodes": [{
                    "id": "foo1",
                    "related_to": { "nodes": [] }
                }]
            }
        }))
    );
}

#[datastore_test(tokio::test, ignore("arango"))]
async fn edge_entity_simple_foreign_violation(ds: &str) {
    let (test, [schema]) = TestPackages::with_static_sources([EDGE_ENTITY_SIMPLE])
        .compile_schemas([EDGE_ENTITY_SIMPLE.0]);

    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), ds).await.into();

    // foo is self-identifying, bar is not
    expect_eq!(
        actual = r#"mutation {
            link(
                create: [{ id: "link1", from: "foo1", to: "bar1" }]
            ) { node { id from to } }
        }"#
        .exec([], &schema, &ctx)
        .await
        .unwrap_first_exec_error_msg(),
        expected = "unresolved foreign key: \"bar1\"",
    );
}

/// FIXME: implement for arango
#[datastore_test(tokio::test, ignore("arango"))]
async fn edge_entity_union_happy_path(ds: &str) {
    let (test, [schema]) = TestPackages::with_static_sources([EDGE_ENTITY_UNION])
        .compile_schemas([EDGE_ENTITY_UNION.0]);

    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), ds).await.into();

    info!("Create 3 vertices");

    r#"mutation {
        link(
            create: [{ id: "link/1", from: "foo/1", to: "baz/1" }]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    info!("Query data");

    expect_eq!(
        actual = r#"{
            links { nodes { id, from, to } }
            foos {
                nodes {
                    id
                    related_to {
                        nodes {
                            ... on baz { id }
                            ... on qux { id }
                        }
                    }
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "links": { "nodes": [{
                "id": "link/1",
                "from": "foo/1",
                "to": "baz/1",
            }]},
            "foos": {
                "nodes": [{
                    "id": "foo/1",
                    "related_to": {
                        "nodes": [{ "id": "baz/1" }]
                    }
                }]
            }
        }))
    );

    info!("Delete link");

    r#"mutation {
        link(delete: ["link/1"]) { deleted }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    info!("Query data after link deletion");

    expect_eq!(
        actual = r#"{
            links { nodes { id, from, to } }
            foos {
                nodes {
                    id
                    related_to {
                        nodes {
                            ... on baz { id }
                            ... on qux { id }
                        }
                    }
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "links": { "nodes": [] },
            "foos": {
                "nodes": [{
                    "id": "foo/1",
                    "related_to": { "nodes": [] }
                }]
            }
        }))
    );
}

#[datastore_test(tokio::test, ignore("arango"))]
async fn edge_entity_union_foreign_violation(ds: &str) {
    let (test, [schema]) = TestPackages::with_static_sources([EDGE_ENTITY_UNION])
        .compile_schemas([EDGE_ENTITY_UNION.0]);

    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), ds).await.into();

    // foo is self-identifying, qux is not
    expect_eq!(
        actual = r#"mutation {
            link(
                create: [{ id: "link/1", from: "foo/1", to: "qux/1" }]
            ) { node { id from to } }
        }"#
        .exec([], &schema, &ctx)
        .await
        .unwrap_first_exec_error_msg(),
        expected = "unresolved foreign key: \"qux/1\""
    );
}
