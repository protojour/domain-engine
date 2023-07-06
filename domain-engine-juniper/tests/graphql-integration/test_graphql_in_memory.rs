use std::sync::Arc;

use domain_engine_core::DomainEngine;
use domain_engine_juniper::{GqlContext, Schema};
use juniper::graphql_value;
use ontol_runtime::config::DataStoreConfig;
use ontol_test_utils::{expect_eq, SourceName, TestPackages};
use test_log::test;

use crate::{Exec, TestCompileSchema};

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
            domain_engine: Arc::new(DomainEngine::builder(test_env.env).build()),
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
// BUG:
// The generated ontol_vm mapping code is intolerant
// when it comes to missing attributes.
// Here the attribute needs to be missing since GraphQL
// does not request it, and the source attribute belongs to a foreign entity.
#[should_panic = "Attribute S:2:74 not present"]
async fn test_graphql_in_memory_blog_post_conduit_no_join() {
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
