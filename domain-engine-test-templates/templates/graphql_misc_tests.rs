use std::sync::Arc;

use domain_engine_core::DomainEngine;
use domain_engine_juniper::{
    context::ServiceCtx,
    juniper::{graphql_value, InputValue},
};
use domain_engine_test_utils::graphql_test_utils::{Exec, TestCompileSchema, ValueExt};
use ontol_runtime::{config::DataStoreConfig, ontology::Ontology};
use ontol_test_utils::{
    examples::{GITMESH, GUITAR_SYNTH_UNION},
    expect_eq, SourceName, TestPackages,
};
use test_log::test;

const ROOT: SourceName = SourceName::root();

async fn make_domain_engine(ontology: Arc<Ontology>) -> DomainEngine {
    DomainEngine::test_builder(ontology)
        .build(crate::TestDataStoreFactory::default())
        .await
        .unwrap()
}

#[test(tokio::test)]
async fn test_guitar_synth_union_mutation_and_query() {
    let (test, [schema]) = TestPackages::with_sources([(ROOT, GUITAR_SYNTH_UNION.1)])
        .with_data_store(ROOT, DataStoreConfig::Default)
        .compile_schemas([SourceName::root()]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology.clone()).await.into();

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
        .await,
        expected = Ok(graphql_value!({
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
        .await,
        expected = Ok(graphql_value!({
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
                        ]
                    }
                }]
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_gitmesh_misc() {
    let (test, [schema]) = TestPackages::with_sources([(ROOT, GITMESH.1)])
        .with_data_store(ROOT, DataStoreConfig::Default)
        .compile_schemas([SourceName::root()]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology.clone()).await.into();

    r#"mutation {
        User(
            create: [
                { id: "user/bob" email: "bob@bob.com" }
                { id: "user/alice" email: "alice@alice.com" }
            ]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    r#"mutation {
        User(
            create: [{ id: "user/bob" email: "bob2@bob.com" }]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .expect_err("user/bob already exists, so this should be an error");

    r#"mutation {
        Organization(
            create: [
                {
                    id: "org/lolsoft"
                    members: [
                        { id: "user/bob" _edge: { role: "admin" } }
                        { id: "user/alice" _edge: { role: "contributor" } }
                    ]
                }
            ]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    r#"mutation {
        Repository(
            create: [
                {
                    handle: "coolproj"
                    owner: { id: "user/bob" }
                }
                {
                    handle: "awesomeproj"
                    owner: { id: "org/lolsoft" }
                }
            ]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    expect_eq!(
        actual = r#"{
            repositories {
                nodes {
                    handle
                    owner {
                        ... on User { id email }
                    }
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        // What happens is that the `awesomeproj` which has an Organzation owner that didn't get selected,
        // just returns an empty object:
        expected = Ok(graphql_value!({
            "repositories": {
                "nodes": [
                    {
                        "handle": "coolproj",
                        "owner": {
                            "id": "user/bob",
                            "email": "bob@bob.com"
                        },
                    },
                    {
                        "handle": "awesomeproj",
                        "owner": {}
                    },
                ]
            }
        })),
    );

    // With all selections:
    expect_eq!(
        actual = r#"{
            repositories {
                nodes {
                    handle
                    owner {
                        ... on Organization {
                            id
                            members {
                                edges {
                                    node { id }
                                    role
                                }
                            }
                        }
                        ... on User { id email }
                    }
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "repositories": {
                "nodes": [
                    {
                        "handle": "coolproj",
                        "owner": {
                            "id": "user/bob",
                            "email": "bob@bob.com"
                        },
                    },
                    {
                        "handle": "awesomeproj",
                        "owner": {
                            "id": "org/lolsoft",
                            "members": {
                                "edges": [
                                    {
                                        "node": { "id": "user/bob" },
                                        "role": "admin",
                                    },
                                    {
                                        "node": { "id": "user/alice" },
                                        "role": "contributor",
                                    },
                                ]
                            }
                        }
                    },
                ]
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_gitmesh_update_owner_relation() {
    let (test, [schema]) = TestPackages::with_sources([(ROOT, GITMESH.1)])
        .with_data_store(ROOT, DataStoreConfig::Default)
        .compile_schemas([SourceName::root()]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology.clone()).await.into();

    let response = r#"mutation {
        User(
            create: [
                { id: "user/bob" email: "bob@bob.com" }
                { id: "user/alice" email: "alice@alice.com" }
            ]
        ) { node { id } }
        Repository(
            create: [
                {
                    handle: "coolproj"
                    owner: { id: "user/bob" }
                }
            ]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    let repo_id = response
        .field("Repository")
        .element(0)
        .field("node")
        .field("id")
        .scalar()
        .clone();

    tracing::info!("Start update");

    expect_eq!(
        actual = r#"mutation set_owner($repoId: ID!, $newUserId: ID!) {
            Repository(
                update: [{
                    id: $repoId
                    owner: { id: $newUserId }
                }]
            ) {
                node {
                    handle
                    owner {
                        ... on User { id email }
                    }
                }
            }
        }"#
        .exec(
            [
                ("repoId".to_owned(), InputValue::Scalar(repo_id)),
                (
                    "newUserId".to_owned(),
                    InputValue::Scalar("user/alice".into()),
                ),
            ],
            &schema,
            &ctx,
        )
        .await,
        expected = Ok(graphql_value!({
            "Repository": [
                {
                    "node": {
                        "handle": "coolproj",
                        "owner": {
                            "id": "user/alice",
                            "email": "alice@alice.com"
                        }
                    }
                }
            ]
        }))
    );
}
