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
    let gql_context: ServiceCtx = DomainEngine::test_builder(test.ontology.clone())
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
        .exec(DbgTag("mutation"), &schema, &gql_context, [])
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
        .exec(DbgTag("list"), &schema, &gql_context, [])
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
    let gql_context: ServiceCtx = DomainEngine::test_builder(test.ontology.clone())
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
    .exec(DbgTag("create_u1"), &schema, &gql_context, [])
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
            DbgTag("new_article"),
            &schema,
            &gql_context,
            [("authorId".to_owned(), InputValue::Scalar(user_id))]
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
    let gql_context: ServiceCtx = DomainEngine::test_builder(test.ontology.clone())
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
    .exec(DbgTag("mut"), &schema, &gql_context, [])
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
        .exec(DbgTag("list"), &schema, &gql_context, [])
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

    fn gql_context(&self) -> ServiceCtx {
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
                DbgTag("create_db_article"),
                &self.db_schema,
                &self.gql_context(),
                []
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
                DbgTag("create_db_article_with_tag"),
                &self.db_schema,
                &self.gql_context(),
                []
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
    let ctx = BlogPostConduit::new().await;
    ctx.create_db_article().await;

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
        .exec(DbgTag("list"), &ctx.blog_schema, &ctx.gql_context(), [])
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
    let ctx = BlogPostConduit::new().await;
    ctx.create_db_article().await;

    expect_eq!(
        actual = r#"{
            posts(written_by: "teh_user") {
                contents
                written_by
            }
        }"#
        .exec(DbgTag("teh_user"), &ctx.blog_schema, &ctx.gql_context(), [])
        .await,
        expected = Ok(graphql_value!({
            "posts": [
                {
                    "contents": "THE BODY",
                    "written_by": "teh_user",
                }
            ]
        })),
    );

    expect_eq!(
        actual = r#"{
            posts(written_by: "someone_else") {
                contents
                written_by
            }
        }"#
        .exec(
            DbgTag("someone_else"),
            &ctx.blog_schema,
            &ctx.gql_context(),
            []
        )
        .await,
        expected = Ok(graphql_value!({ "posts": [] })),
    );
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_tags() {
    let ctx = BlogPostConduit::new().await;
    ctx.create_db_article_with_tag().await;

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
        .exec(DbgTag("list"), &ctx.blog_schema, &ctx.gql_context(), [])
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
    let ctx = BlogPostConduit::new().await;
    ctx.create_db_article().await;

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
        .exec(DbgTag("list"), &ctx.blog_schema, &ctx.gql_context(), [])
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
