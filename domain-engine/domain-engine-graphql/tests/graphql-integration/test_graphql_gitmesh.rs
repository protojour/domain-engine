use domain_engine_graphql::{
    context::ServiceCtx,
    juniper::{graphql_value, InputValue},
};
use domain_engine_test_utils::graphql_test_utils::{
    gql_ctx_mock_data_store, TestCompileSingletonSchema,
};
use domain_engine_test_utils::{
    graphql_test_utils::{
        Exec, GraphqlTestResultExt, GraphqlValueResultExt, TestCompileSchema, ValueExt,
    },
    graphql_value_unordered,
};
use ontol_macros::{datastore_test, test};
use ontol_test_utils::{default_short_name, examples::gitmesh, expect_eq, TestPackages};
use tracing::info;

use crate::mk_engine_default;

#[test(tokio::test)]
async fn gitmesh_id_error() {
    let (test, schema) = gitmesh().1.compile_single_schema();

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
        .exec(
            [],
            &schema,
            &gql_ctx_mock_data_store(&test, &[default_short_name()], ())
        )
        .await
        .unwrap_first_exec_error_msg(),
        expected =
            "invalid type, expected `RepositoryOwner` (`User` or `Organization`) in input at line 5 column 31"
    );
}

#[datastore_test(tokio::test)]
async fn misc(ds: &str) {
    let (test, [schema]) =
        TestPackages::with_static_sources([gitmesh()]).compile_schemas(["gitmesh"]);
    let ctx: ServiceCtx = mk_engine_default(test.ontology_owned(), ds).await.into();

    info!("Create two users");

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

    info!("Create duplicate user, should fail");

    expect_eq!(
        actual = r#"mutation {
            User(
                create: [{ id: "user/bob" email: "bob2@bob.com" }]
            ) { node { id } }
        }"#
        .exec([], &schema, &ctx)
        .await
        .unwrap_first_exec_error_msg(),
        expected = "entity already exists"
    );

    info!("Create organization");

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

    info!("Create two repositories");

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
            users {
                nodes {
                    email
                    member_of {
                        edges {
                            node {
                                id
                                repositories {
                                    nodes { handle }
                                }
                            }
                            role
                        }
                    }
                    repositories {
                        nodes { handle }
                    }
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "users": {
                "nodes": [
                    {
                        "email": "bob@bob.com",
                        "member_of" : {
                            "edges": [
                                {
                                    "node": {
                                        "id": "org/lolsoft",
                                        "repositories": {
                                            "nodes": [
                                                { "handle": "awesomeproj" }
                                            ]
                                        }
                                    },
                                    "role": "admin",
                                }
                            ]
                        },
                        "repositories": {
                            "nodes": [
                                { "handle": "coolproj" }
                            ]
                        }
                    },
                    {
                        "email": "alice@alice.com",
                        "member_of" : {
                            "edges": [
                                {
                                    "node": {
                                        "id": "org/lolsoft",
                                        "repositories": {
                                            "nodes": [
                                                { "handle": "awesomeproj" }
                                            ]
                                        }
                                    },
                                    "role": "contributor",
                                }
                            ]
                        },
                        "repositories": { "nodes": [] }
                    },
                ]
            }
        })),
    );

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
        // just returns null:
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
                        "owner": null
                    },
                ]
            }
        })),
    );

    info!("Last query");

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
        .await
        .unordered(),
        expected = Ok(graphql_value_unordered!({
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

#[datastore_test(tokio::test)]
async fn fancy_filters(ds: &str) {
    let (test, [schema]) =
        TestPackages::with_static_sources([gitmesh()]).compile_schemas(["gitmesh"]);
    let ctx: ServiceCtx = mk_engine_default(test.ontology_owned(), ds).await.into();

    r#"mutation {
        User(
            create: [
                { id: "user/bob" email: "b@ob.com" }
                { id: "user/alice" email: "ali@ce.com" }
                { id: "user/tracy" email: "tr@acy.com" }
                { id: "user/alf" email: "al@f.com" }
            ]
        ) { node { id } }
        Organization(
            create: [
                {
                    id: "org/lolsoft"
                    members: [
                        { id: "user/alice" _edge: { role: "janitor" } }
                        { id: "user/alf" _edge: { role: "president" } }
                    ]
                }
            ]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    expect_eq!(
        actual = r#"{
            users(input: {ids: ["user/bob", "user/tracy"]}) {
                nodes { id }
            }
        }"#
        .exec([], &schema, &ctx)
        .await
        .unordered(),
        expected = Ok(graphql_value_unordered!({
            "users": {
                "nodes": [
                    { "id": "user/bob" },
                    { "id": "user/tracy" },
                ]
            }
        })),
    );

    expect_eq!(
        actual = r#"{
            users(input: {member_of: ["org/lolsoft"]}) {
                nodes { id }
            }
        }"#
        .exec([], &schema, &ctx)
        .await
        .unordered(),
        expected = Ok(graphql_value_unordered!({
            "users": {
                "nodes": [
                    { "id": "user/alice" },
                    { "id": "user/alf" },
                ]
            }
        })),
    );
}

#[datastore_test(tokio::test)]
async fn update_owner_relation(ds: &str) {
    let (test, [schema]) =
        TestPackages::with_static_sources([gitmesh()]).compile_schemas(["gitmesh"]);
    let ctx: ServiceCtx = mk_engine_default(test.ontology_owned(), ds).await.into();

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

/// FIXME: implement for arango
#[datastore_test(tokio::test)]
async fn patch_members(ds: &str) {
    let (test, [schema]) =
        TestPackages::with_static_sources([gitmesh()]).compile_schemas(["gitmesh"]);
    let ctx: ServiceCtx = mk_engine_default(test.ontology_owned(), ds).await.into();

    r#"mutation {
        User(
            create: [
                { id: "user/bob" email: "bob@bob.com" }
                { id: "user/alice" email: "alice@alice.com" }
            ]
        ) { node { id } }
        Organization(
            create: [{ id: "org/lolsoft" }]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    info!("add bob as a contributor");

    r#"mutation {
        Organization(
            update: [{
                id: "org/lolsoft"
                members: {
                    add: [{
                        id: "user/bob"
                        _edge: {
                            role: "contributor"
                        }
                    }]
                }
            }]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    info!("bob should be a member in the query");

    expect_eq!(
        actual = r#"{
            organizations {
                nodes {
                    members {
                        nodes { id }
                    }
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "organizations": {
                "nodes": [
                    {
                        "members": {
                            "nodes": [
                                { "id": "user/bob" }
                            ]
                        }
                    }
                ]
            }
        })),
    );

    info!("alice is not a member, so upgrading to admin should fail");

    expect_eq!(
        actual = r#"mutation {
            Organization(
                update: [{
                    id: "org/lolsoft"
                    members: {
                        update: [{
                            id: "user/alice"
                            _edge: {
                                role: "admin"
                            }
                        }]
                    }
                }]
            ) { node { id } }
        }"#
        .exec([], &schema, &ctx)
        .await
        .unwrap_first_exec_error_msg(),
        expected = "edge not found"
    );

    info!("upgrade bob to admin should succeed");

    r#"mutation {
        Organization(
            update: [{
                id: "org/lolsoft"
                members: {
                    update: [{
                        id: "user/bob"
                        _edge: {
                            role: "admin"
                        }
                    }]
                }
            }]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    info!("delete bob");

    // Delete a member
    r#"mutation {
        Organization(
            update: [{
                id: "org/lolsoft"
                members: {
                    remove: [{ id: "user/bob" }]
                }
            }]
        ) { node { id } }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    expect_eq!(
        actual = r#"{organizations { nodes { members { nodes { id }}}}}"#
            .exec([], &schema, &ctx)
            .await,
        expected = Ok(graphql_value!({
            "organizations": {
                "nodes": [{ "members": { "nodes": [] } }]
            }
        })),
    );
}

#[datastore_test(tokio::test)]
async fn ownership_transfer(ds: &str) {
    let (test, [schema]) =
        TestPackages::with_static_sources([gitmesh()]).compile_schemas(["gitmesh"]);
    let ctx: ServiceCtx = mk_engine_default(test.ontology_owned(), ds).await.into();

    let response = r#"mutation {
        User(
            create: [{ id: "user/bob" email: "bob@bob.com" }]
        ) { node { id } }
        Organization(
            create: [{ id: "org/lolsoft" }]
        ) { node { id } }
        Repository(
            create: [
                {
                    handle: "l33tc0d3"
                    owner: { id: "org/lolsoft" }
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

    tracing::info!("Move repository over to user/bob");

    expect_eq!(
        actual = r#"mutation add_repo($userId: ID!, $repoId: ID!) {
            User(
                update: [{
                    id: $userId
                    repositories: {
                        add: [{ id: $repoId }]
                    }
                }]
            ) {
                node {
                    repositories {
                        nodes { handle }
                    }
                }
            }
        }"#
        .exec(
            [
                ("userId".to_owned(), InputValue::Scalar("user/bob".into())),
                ("repoId".to_owned(), InputValue::Scalar(repo_id)),
            ],
            &schema,
            &ctx,
        )
        .await,
        expected = Ok(graphql_value!({
            "User": [
                {
                    "node": {
                        "repositories": {
                            "nodes": [{ "handle": "l33tc0d3" }]
                        }
                    }
                }
            ]
        }))
    );

    tracing::info!("The organization should no longer own the repository");

    expect_eq!(
        actual = r#"{organizations { nodes { repositories { nodes { id }}}}}"#
            .exec([], &schema, &ctx)
            .await,
        expected = Ok(graphql_value!({
            "organizations": {
                "nodes": [{ "repositories": { "nodes": [] } }]
            }
        })),
    );
}
