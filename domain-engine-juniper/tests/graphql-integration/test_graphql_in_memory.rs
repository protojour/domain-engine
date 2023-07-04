use domain_engine_core::DomainEngine;
use domain_engine_juniper::GqlContext;
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

#[test(tokio::test)]
async fn test_graphql_in_memory_blog_post_on_conduit_db() {
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

    let (test_env, [db_schema, blog_schema]) = test_packages.compile_schemas([CONDUIT_DB, ROOT]);
    let gql_context: GqlContext = DomainEngine::builder(test_env.env.clone()).build().into();

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
                        username: "u1",
                        email: "a@b",
                        password_hash: "s3cr3t"
                    }
                }
            ) {
                slug
            }
        }"#
        .exec(&db_schema, &gql_context)
        .await,
        expected = Ok(graphql_value!({
            "createArticle": {
                "slug": "the-slug"
            }
        })),
    );

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
        .exec(&blog_schema, &gql_context)
        .await,
        expected = Ok(graphql_value!({
            "BlogPostList": {
                "edges": [
                    {
                        "node": {
                            "contents": "THE BODY"
                        }
                    }
                ]
            }
        })),
    );
}
