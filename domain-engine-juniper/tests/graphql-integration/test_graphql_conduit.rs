use std::sync::Arc;

use domain_engine_core::{data_store::DataStoreAPIMock, DomainEngine};
use domain_engine_juniper::{context::ServiceCtx, Schema};
use fnv::FnvHashMap;
use juniper::{graphql_value, InputValue};
use ontol_runtime::{
    config::DataStoreConfig,
    query::{EntityQuery, Query, StructOrUnionQuery, StructQuery},
    DefId, PackageId,
};
use ontol_test_utils::{
    examples::conduit::{BLOG_POST_PUBLIC, CONDUIT_DB},
    expect_eq, OntolTest, SourceName, TestPackages,
};
use test_log::test;
use unimock::{matching, MockFn};

use crate::{gql_ctx_mock_data_store, Exec, TestCompileSchema};

const ROOT: SourceName = SourceName::root();

fn conduit_db_only() -> TestPackages {
    TestPackages::with_sources([(ROOT, CONDUIT_DB.1)])
        .with_data_store(ROOT, DataStoreConfig::InMemory)
}

#[test(tokio::test)]
async fn test_graphql_in_memory_conduit_db() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([SourceName::root()]);
    let gql_context: ServiceCtx = DomainEngine::test_builder(test.ontology.clone())
        .build()
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
        .exec(&schema, &gql_context, [])
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
        .exec(&schema, &gql_context, [])
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
async fn test_graphql_in_memory_conduit_db_create_with_foreign_reference() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([SourceName::root()]);
    let gql_context: ServiceCtx = DomainEngine::test_builder(test.ontology.clone())
        .build()
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
    .exec(&schema, &gql_context, [])
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
async fn test_graphql_in_memory_conduit_db_query_article_with_tags() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([SourceName::root()]);
    let gql_context: ServiceCtx = DomainEngine::test_builder(test.ontology.clone())
        .build()
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
    .exec(&schema, &gql_context, [])
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
        .exec(&schema, &gql_context, [])
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
    test: OntolTest,
    domain_engine: Arc<DomainEngine>,
    db_schema: Schema,
    blog_schema: Schema,
}

impl BlogPostConduit {
    fn new() -> Self {
        let test_packages = TestPackages::with_sources([(ROOT, BLOG_POST_PUBLIC.1), CONDUIT_DB])
            .with_data_store(CONDUIT_DB.0, DataStoreConfig::InMemory);

        let (test, [db_schema, blog_schema]) = test_packages.compile_schemas([CONDUIT_DB.0, ROOT]);
        Self {
            domain_engine: Arc::new(DomainEngine::test_builder(test.ontology.clone()).build()),
            test,
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
            .exec(&self.db_schema, &self.gql_context(), [])
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
            .exec(&self.db_schema, &self.gql_context(), [])
            .await,
            expected = Ok(graphql_value!({
                "createArticle": { "slug": "the-slug" }
            })),
        );
    }
}

#[test(tokio::test)]
async fn test_graphql_in_memory_blog_post_conduit_implicit_join() {
    let ctx = BlogPostConduit::new();
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
        .exec(&ctx.blog_schema, &ctx.gql_context(), [])
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
async fn test_graphql_in_memory_blog_post_conduit_tags() {
    let ctx = BlogPostConduit::new();
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
        .exec(&ctx.blog_schema, &ctx.gql_context(), [])
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
async fn test_graphql_in_memory_blog_post_conduit_no_join_mocked() {
    let ctx = BlogPostConduit::new();
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
        .exec(
            &ctx.blog_schema,
            &gql_ctx_mock_data_store(
                &ctx.test,
                CONDUIT_DB.0,
                DataStoreAPIMock::query
                    .next_call(matching!(
                        _,
                        // The purpose of this is to check that the data store
                        // receives a correctly structured query.
                        // Note: The hard coded PropertyIds used below are unstable
                        // and will likely change if the ONTOL files change.
                        // Just update as necessary, the point is the structure, not the property IDs.
                        eq!(&EntityQuery {
                            source: StructOrUnionQuery::Struct(StructQuery {
                                def_id: DefId(PackageId(2), 21),
                                properties: FnvHashMap::from_iter([
                                    // This is the "body" property:
                                    ("S:2:31".parse().unwrap(), Query::Leaf),
                                    (
                                        // This is the `author` property:
                                        "S:2:55".parse().unwrap(),
                                        Query::Struct(StructQuery {
                                            def_id: DefId(PackageId(2), 7),
                                            properties: FnvHashMap::from_iter([(
                                                // This is the `username` property:
                                                "S:2:12".parse().unwrap(),
                                                Query::Leaf
                                            )])
                                        })
                                    )
                                ]),
                            }),
                            cursor: None,
                            limit: 20,
                        })
                    ))
                    .returns(Ok(vec![]))
            ),
            []
        )
        .await,
        expected = Ok(graphql_value!({ "BlogPostList": { "edges": [] } }))
    );
}

#[test(tokio::test)]
async fn test_graphql_in_memory_blog_post_conduit_no_join_real() {
    let ctx = BlogPostConduit::new();
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
        .exec(&ctx.blog_schema, &ctx.gql_context(), [])
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
