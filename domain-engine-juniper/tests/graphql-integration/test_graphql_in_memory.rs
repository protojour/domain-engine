use std::sync::Arc;

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
    let gql_context = GqlContext {
        domain_engine: Arc::new(DomainEngine::builder(test_env.env.clone()).build()),
    };

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
// FIXME: Currently fails! Implement query translation!
#[should_panic = "Invalid entity DefId"]
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

    let (test_env, [_db_schema, blog_schema]) = test_packages.compile_schemas([CONDUIT_DB, ROOT]);
    let gql_context = GqlContext {
        domain_engine: Arc::new(DomainEngine::builder(test_env.env.clone()).build()),
    };

    // TODO: Insert using data store domain:
    // expect_eq!(
    //     actual = r#"mutation {
    //         createUser(
    //             input: {
    //                 username: "u1",
    //                 email: "a@b",
    //                 password_hash: "s3cr3t",
    //             }
    //         ) {
    //             username
    //         }
    //     }"#
    //     .exec(&db_schema, &gql_context)
    //     .await,
    //     expected = Ok(graphql_value!({
    //         "createUser": {
    //             "username": "u1"
    //         }
    //     })),
    // );

    expect_eq!(
        actual = "{
            BlogPostList {
                edges {
                    node {
                        post_id
                        body
                    }
                }
            }
        }"
        .exec(&blog_schema, &gql_context)
        .await,
        expected = Ok(graphql_value!({
            "edges": []
        })),
    );
}
