use std::sync::Arc;

use domain_engine_core::{data_store::DataStoreAPIMock, DomainEngine};
use domain_engine_juniper::{GqlContext, Schema};
use fnv::FnvHashMap;
use juniper::graphql_value;
use ontol_runtime::{
    config::DataStoreConfig,
    query::{EntityQuery, Query, StructOrUnionQuery, StructQuery},
    DefId, PackageId,
};
use ontol_test_utils::{expect_eq, SourceName, TestEnv, TestPackages};
use test_log::test;
use unimock::{matching, MockFn};

use crate::{gql_ctx_mock_data_store, Exec, TestCompileSchema};

const ROOT: SourceName = SourceName::root();
const CONDUIT_DB: SourceName = SourceName("conduit_db");

#[test(tokio::test)]
async fn test_graphql_in_memory_conduit_db() {
    let test_packages = TestPackages::with_sources([(
        ROOT,
        include_str!("../../../examples/conduit/conduit_db.on"),
    )])
    .with_data_store(ROOT, DataStoreConfig::InMemory);

    let (test_env, [schema]) = test_packages.compile_schemas([SourceName::root()]);
    let gql_context: GqlContext = DomainEngine::builder(test_env.env.clone()).build().into();

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
        .exec(&schema, &gql_context)
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
        .exec(&schema, &gql_context)
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

struct BlogPostConduit {
    test_env: TestEnv,
    domain_engine: Arc<DomainEngine>,
    db_schema: Schema,
    blog_schema: Schema,
}

impl BlogPostConduit {
    fn new() -> Self {
        let test_packages = TestPackages::with_sources([
            (
                ROOT,
                include_str!("../../../examples/conduit/blog_post_public.on"),
            ),
            (
                CONDUIT_DB,
                include_str!("../../../examples/conduit/conduit_db.on"),
            ),
        ])
        .with_data_store(CONDUIT_DB, DataStoreConfig::InMemory);

        let (test_env, [db_schema, blog_schema]) =
            test_packages.compile_schemas([CONDUIT_DB, ROOT]);
        Self {
            domain_engine: Arc::new(DomainEngine::builder(test_env.env.clone()).build()),
            test_env,
            db_schema,
            blog_schema,
        }
    }

    fn gql_context(&self) -> GqlContext {
        GqlContext {
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
            .exec(&self.db_schema, &self.gql_context())
            .await,
            expected = Ok(graphql_value!({
                "createArticle": {
                    "slug": "the-slug"
                }
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
        .exec(&ctx.blog_schema, &ctx.gql_context())
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
async fn test_graphql_in_memory_blog_post_conduit_empty_tags() {
    let ctx = BlogPostConduit::new();
    ctx.create_db_article().await;

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
        .exec(&ctx.blog_schema, &ctx.gql_context())
        .await,
        expected = Ok(graphql_value!({
            "BlogPostList": {
                "edges": [
                    {
                        "node": {
                            "contents": "THE BODY",
                            "written_by": "teh_user",
                            "tags": []
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
                &ctx.test_env,
                CONDUIT_DB,
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
                                def_id: DefId(PackageId(2), 28),
                                properties: FnvHashMap::from_iter([
                                    // This is the "body" property:
                                    ("S:2:43".parse().unwrap(), Query::Leaf),
                                    (
                                        // This is the `author` property:
                                        "S:2:72".parse().unwrap(),
                                        Query::Struct(StructQuery {
                                            def_id: DefId(PackageId(2), 7),
                                            properties: FnvHashMap::from_iter([(
                                                // This is the `username` property:
                                                "S:2:14".parse().unwrap(),
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
            )
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
        .exec(&ctx.blog_schema, &ctx.gql_context())
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
