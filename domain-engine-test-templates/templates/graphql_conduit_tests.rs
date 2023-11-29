use std::collections::HashMap;
use std::sync::Arc;

use domain_engine_core::DomainEngine;
use domain_engine_juniper::{
    context::ServiceCtx,
    gql_scalar::GqlScalar,
    juniper::{self, graphql_value, InputValue, Value},
    Schema,
};
use domain_engine_test_utils::graphql_test_utils::{Exec, GraphQLPageDebug, TestCompileSchema};
use ontol_runtime::{config::DataStoreConfig, ontology::Ontology};
use ontol_test_utils::{
    examples::conduit::{BLOG_POST_PUBLIC, CONDUIT_DB},
    expect_eq, SourceName, TestPackages,
};
use test_log::test;
use tracing::info;

const ROOT: SourceName = SourceName::root();

fn conduit_db_only() -> TestPackages {
    TestPackages::with_sources([(ROOT, CONDUIT_DB.1)])
        .with_data_store(ROOT, DataStoreConfig::Default)
}

async fn make_domain_engine(ontology: Arc<Ontology>) -> DomainEngine {
    DomainEngine::test_builder(ontology)
        .build(crate::TestDataStoreFactory::default())
        .await
        .unwrap()
}

#[test(tokio::test)]
async fn test_graphql_conduit_db() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([SourceName::root()]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology.clone()).await.into();

    expect_eq!(
        actual = r#"mutation {
            User(
                create: [{
                    username: "u1",
                    email: "a@b",
                    password_hash: "s3cr3t",
                }]
            ) {
                node {
                    username
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "User": [{
                "node": {
                    "username": "u1"
                }
            }]
        })),
    );

    expect_eq!(
        actual = "{
            users {
                edges {
                    node {
                        username
                        email
                        password_hash
                        bio
                    }
                }
            }
        }"
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "users": {
                "edges": [
                    {
                        "node": {
                            "username": "u1",
                            "email": "a@b",
                            "password_hash": "s3cr3t",
                            "bio": "",
                        }
                    }
                ]
            },
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_conduit_db_create_with_foreign_reference() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([SourceName::root()]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology.clone()).await.into();

    let response = r#"mutation {
        createUser(
            input: {
                username: "u1",
                email: "a@b",
                password_hash: "s3cr3t",
            }
        ) {
            user_id
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    let user_id = response
        .as_object_value()
        .and_then(|response| response.get_field_value("createUser"))
        .and_then(juniper::Value::as_object_value)
        .and_then(|create_user| create_user.get_field_value("user_id"))
        .and_then(juniper::Value::as_scalar_value)
        .unwrap()
        .clone();

    expect_eq!(
        actual = r#"mutation new_article($authorId: ID!) {
            createArticle(
                input: {
                    slug: "the-slug",
                    title: "The title",
                    description: "An article",
                    body: "THE BODY",
                    author: {
                        user_id: $authorId
                    }
                    tags: [{ tag: "foobar" }]
                }
            ) {
                slug
                author {
                    username
                }
            }
        }"#
        .exec(
            [("authorId".to_owned(), InputValue::Scalar(user_id))],
            &schema,
            &ctx
        )
        .await,
        expected = Ok(graphql_value!({
            "createArticle": {
                "slug": "the-slug",
                "author": {
                    "username": "u1"
                }
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_conduit_db_query_article_with_tags() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([SourceName::root()]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology.clone()).await.into();

    let _response = r#"mutation {
        createArticle(
            input: {
                slug: "the-slug",
                title: "The title",
                description: "An article",
                body: "THE BODY",
                author: {
                    username: "u1",
                    email: "a@b",
                    password_hash: "s3cr3t",
                }
                tags: [{ tag: "foobar" }]
            }
        ) {
            slug
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    expect_eq!(
        actual = r#"{
            articles {
                edges {
                    node {
                        tags {
                            edges {
                                node {
                                    tag
                                }
                            }
                        }
                    }
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "articles": {
                "edges": [{
                    "node": {
                        "tags": {
                            "edges": [{
                                "node": {
                                    "tag": "foobar"
                                }
                            }]
                        }
                    }
                }]
            }
        })),
    );
}

struct BlogPostConduit {
    domain_engine: Arc<DomainEngine>,
    db_schema: Schema,
    blog_schema: Schema,
}

impl BlogPostConduit {
    async fn new() -> Self {
        let test_packages = TestPackages::with_sources([(ROOT, BLOG_POST_PUBLIC.1), CONDUIT_DB])
            .with_data_store(CONDUIT_DB.0, DataStoreConfig::Default);

        let (test, [db_schema, blog_schema]) = test_packages.compile_schemas([CONDUIT_DB.0, ROOT]);
        Self {
            domain_engine: Arc::new(make_domain_engine(test.ontology.clone()).await),
            db_schema,
            blog_schema,
        }
    }

    fn ctx(&self) -> ServiceCtx {
        self.domain_engine.clone().into()
    }

    async fn create_db_article(&self) {
        // Insert using the data store domain directly:
        expect_eq!(
            actual = r#"mutation {
                createArticle(
                    input: {
                        slug: "the-slug",
                        title: "The title",
                        description: "An article",
                        body: "THE BODY",
                        author: {
                            username: "teh_user",
                            email: "a@b",
                            password_hash: "s3cr3t"
                        }
                    }
                ) {
                    slug
                }
            }"#
            .exec([], &self.db_schema, &self.ctx())
            .await,
            expected = Ok(graphql_value!({
                "createArticle": { "slug": "the-slug" }
            })),
        );
    }

    async fn create_db_article_with_tag(&self) {
        expect_eq!(
            actual = r#"mutation {
                createArticle(
                    input: {
                        slug: "the-slug",
                        title: "The title",
                        description: "An article",
                        body: "THE BODY",
                        author: {
                            username: "teh_user",
                            email: "a@b",
                            password_hash: "s3cr3t"
                        }
                        tags: [{ tag: "foobar" }]
                    }
                ) {
                    slug
                }
            }"#
            .exec([], &self.db_schema, &self.ctx())
            .await,
            expected = Ok(graphql_value!({
                "createArticle": { "slug": "the-slug" }
            })),
        );
    }
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_implicit_join() {
    let test = BlogPostConduit::new().await;
    test.create_db_article().await;

    expect_eq!(
        actual = r#"{
            posts(input: { written_by: "teh_user" }) {
                edges {
                    node {
                        contents
                        written_by
                    }
                }
            }
        }"#
        .exec([], &test.blog_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "posts": {
                "edges": [
                    {
                        "node": {
                            "contents": "THE BODY",
                            "written_by": "teh_user",
                        }
                    }
                ]
            }
        })),
    );

    expect_eq!(
        actual = r#"{
            posts(input: { written_by: "someone_else" }) {
                edges {
                    node {
                        contents
                        written_by
                    }
                }
            }
        }"#
        .exec([], &test.blog_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({ "posts": { "edges": [] } })),
    );
}

/// The purpose of this test is to ensure that SubSequence information
/// is threaded through the domain engine pipeline.
#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_paginated() {
    let test = BlogPostConduit::new().await;
    for _ in 0..3 {
        test.create_db_article().await;
    }

    let mut prev_end_cursor: Option<InputValue<GqlScalar>> = None;

    for index in 0..3 {
        info!("Executing page {index}");

        let response: Value<GqlScalar> = r#"
            query paginated_posts($after: String) {
                posts(input: { written_by: "teh_user" }, first: 1, after: $after) {
                    nodes {
                        contents
                        written_by
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    totalCount
                }
            }
        "#
        .exec(
            if let Some(cursor) = &prev_end_cursor {
                HashMap::from([("after".to_owned(), cursor.clone())])
            } else {
                HashMap::default()
            },
            &test.blog_schema,
            &test.ctx(),
        )
        .await
        .unwrap();

        let debug = GraphQLPageDebug::parse_connection(&response, "posts").unwrap();
        assert_eq!(debug.total_count, Some(3));
        assert_eq!(debug.has_next_page, index < 2);
        prev_end_cursor = debug
            .end_cursor
            .map(|cursor| InputValue::Scalar(GqlScalar::String(cursor.into())));
    }
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_tags() {
    let test = BlogPostConduit::new().await;
    test.create_db_article_with_tag().await;

    expect_eq!(
        actual = "{
            posts {
                edges {
                    node {
                        contents
                        written_by
                        tags
                    }
                }
            }
        }"
        .exec([], &test.blog_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "posts": {
                "edges": [
                    {
                        "node": {
                            "contents": "THE BODY",
                            "written_by": "teh_user",
                            "tags": ["foobar"]
                        }
                    }
                ]
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_no_join_real() {
    let test = BlogPostConduit::new().await;
    test.create_db_article().await;

    expect_eq!(
        actual = "{
            posts {
                edges {
                    node {
                        contents
                    }
                }
            }
        }"
        .exec([], &test.blog_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "posts": {
                "edges": [
                    {
                        "node": {
                            "contents": "THE BODY",
                        }
                    }
                ]
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_conduit_db_article_shallow_update() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([SourceName::root()]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology.clone()).await.into();

    let response = r#"mutation {
        Article(
            create: [{
                slug: "the-slug",
                title: "The old title",
                description: "An article",
                body: "THE BODY",
                author: {
                    username: "u1",
                    email: "a@b",
                    password_hash: "s3cr3t",
                }
                tags: [{ tag: "foobar" }]
            }]
        ) {
            node {
                article_id
                updated_at
            }
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    let article_id = response
        .as_object_value()
        .and_then(|response| response.get_field_value("Article"))
        .and_then(juniper::Value::as_list_value)
        .map(|list| &list[0])
        .and_then(juniper::Value::as_object_value)
        .and_then(|mutation| mutation.get_field_value("node"))
        .and_then(juniper::Value::as_object_value)
        .and_then(|node| node.get_field_value("article_id"))
        .and_then(juniper::Value::as_scalar_value)
        .unwrap()
        .clone();

    let update_mutation = format!(
        r#"mutation {{
            Article(
                update: [{{
                    article_id: "{article_id}"
                    title: "THE NEW TITLE"
                }}]
            ) {{
                node {{
                    article_id
                    slug
                    title
                    created_at
                    updated_at
                    author {{
                        username
                    }}
                }}
            }}
        }}"#
    );
    expect_eq!(
        actual = update_mutation.exec([], &schema, &ctx).await,
        expected = Ok(graphql_value!({
            "Article": [{
                "node": {
                    "article_id": article_id,
                    "slug": "the-slug",
                    "title": "THE NEW TITLE",
                    // Note: The TestSystem counts one year between successive reads of the time :)
                    "created_at": "1970-01-01T00:00:00+00:00",
                    "updated_at": "1971-01-01T00:00:00+00:00",
                    "author": {
                        "username": "u1"
                    }
                }
            }]
        }))
    );
}

#[test(tokio::test)]
async fn test_graphql_conduit_db_user_deletion() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([SourceName::root()]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology.clone()).await.into();

    let response = r#"mutation {
        User(
            create: [{
                username: "to_be_deleted",
                email: "a@b",
                password_hash: "s3cr3t",
            }]
        ) {
            node {
                user_id
            }
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    let user_id = response
        .as_object_value()
        .and_then(|response| response.get_field_value("User"))
        .and_then(juniper::Value::as_list_value)
        .map(|list| &list[0])
        .and_then(juniper::Value::as_object_value)
        .and_then(|mutation| mutation.get_field_value("node"))
        .and_then(juniper::Value::as_object_value)
        .and_then(|node| node.get_field_value("user_id"))
        .and_then(juniper::Value::as_scalar_value)
        .unwrap()
        .clone();

    r#"mutation {
        User(
            create: [{
                username: "retained_user",
                email: "b@c",
                password_hash: "123abc",
            }]
        ) {
            node {
                user_id
            }
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    // Do the action deletion: One real user and one bogus user ID
    expect_eq!(
        actual = {
            let mutation = format!(
                r#"mutation {{
                    User(delete: ["{user_id}", "deadbeef-baad-baad-baad-c0ffeeeeeeee"]) {{
                        deleted
                    }}
                }}"#
            );
            mutation.exec([], &schema, &ctx).await
        },
        expected = Ok(graphql_value!({
            "User": [
                { "deleted": true },
                { "deleted": false }
            ]
        })),
    );

    // Verify that the user was actually deleted
    expect_eq!(
        actual = "{ users { nodes { username } } }"
            .exec([], &schema, &ctx)
            .await,
        expected = Ok(graphql_value!({
            "users": {
                "nodes": [
                    { "username": "retained_user" }
                ]
            },
        })),
    );
}
