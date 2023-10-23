use std::sync::Arc;

use domain_engine_core::DomainEngine;
use domain_engine_juniper::{context::ServiceCtx, Schema};
use domain_engine_test_utils::graphql::{Exec, TestCompileSchema};
use domain_engine_test_utils::DbgTag;
use juniper::{graphql_value, InputValue};
use ontol_runtime::config::DataStoreConfig;
use ontol_test_utils::{
    examples::conduit::{BLOG_POST_PUBLIC, CONDUIT_DB},
    expect_eq, SourceName, TestPackages,
};
use test_log::test;

const ROOT: SourceName = SourceName::root();

fn conduit_db_only() -> TestPackages {
    TestPackages::with_sources([(ROOT, CONDUIT_DB.1)])
        .with_data_store(ROOT, DataStoreConfig::Default)
}

#[test(tokio::test)]
async fn test_graphql_conduit_db() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([SourceName::root()]);
    let ctx: ServiceCtx = DomainEngine::test_builder(test.ontology.clone())
        .build::<crate::TestDataStoreFactory>()
        .await
        .into();

    expect_eq!(
        actual = r#"mutation {
            createUser(
                input: {
                    username: "u1",
                    email: "a@b",
                    password_hash: "s3cr3t",
                }
            ) {
                username
            }
        }"#
        .exec([], &schema, &ctx, DbgTag("mutation"))
        .await,
        expected = Ok(graphql_value!({
            "createUser": {
                "username": "u1"
            }
        })),
    );

    expect_eq!(
        actual = "{
            UserList {
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
        .exec([], &schema, &ctx, DbgTag("list"))
        .await,
        expected = Ok(graphql_value!({
            "UserList": {
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
    let ctx: ServiceCtx = DomainEngine::test_builder(test.ontology.clone())
        .build::<crate::TestDataStoreFactory>()
        .await
        .into();

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
    .exec([], &schema, &ctx, DbgTag("create_u1"))
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
            &ctx,
            DbgTag("new_article")
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
    let ctx: ServiceCtx = DomainEngine::test_builder(test.ontology.clone())
        .build::<crate::TestDataStoreFactory>()
        .await
        .into();

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
    .exec([], &schema, &ctx, DbgTag("mut"))
    .await
    .unwrap();

    expect_eq!(
        actual = r#"{
            ArticleList {
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
        .exec([], &schema, &ctx, DbgTag("list"))
        .await,
        expected = Ok(graphql_value!({
            "ArticleList": {
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
            domain_engine: Arc::new(
                DomainEngine::test_builder(test.ontology.clone())
                    .build::<crate::TestDataStoreFactory>()
                    .await,
            ),
            db_schema,
            blog_schema,
        }
    }

    fn ctx(&self) -> ServiceCtx {
        ServiceCtx {
            domain_engine: self.domain_engine.clone(),
        }
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
            .exec(
                [],
                &self.db_schema,
                &self.ctx(),
                DbgTag("create_db_article")
            )
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
            .exec(
                [],
                &self.db_schema,
                &self.ctx(),
                DbgTag("create_db_article_with_tag"),
            )
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
        actual = "{
            BlogPostList {
                edges {
                    node {
                        contents
                        written_by
                    }
                }
            }
        }"
        .exec([], &test.blog_schema, &test.ctx(), DbgTag("list"))
        .await,
        expected = Ok(graphql_value!({
            "BlogPostList": {
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
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_implicit_join_named_query() {
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
        .exec([], &test.blog_schema, &test.ctx(), DbgTag("teh_user"))
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
        .exec([], &test.blog_schema, &test.ctx(), DbgTag("someone_else"))
        .await,
        expected = Ok(graphql_value!({ "posts": { "edges": [] } })),
    );
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_tags() {
    let test = BlogPostConduit::new().await;
    test.create_db_article_with_tag().await;

    expect_eq!(
        actual = "{
            BlogPostList {
                edges {
                    node {
                        contents
                        written_by
                        tags
                    }
                }
            }
        }"
        .exec([], &test.blog_schema, &test.ctx(), DbgTag("list"))
        .await,
        expected = Ok(graphql_value!({
            "BlogPostList": {
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
            BlogPostList {
                edges {
                    node {
                        contents
                    }
                }
            }
        }"
        .exec([], &test.blog_schema, &test.ctx(), DbgTag("list"))
        .await,
        expected = Ok(graphql_value!({
            "BlogPostList": {
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
