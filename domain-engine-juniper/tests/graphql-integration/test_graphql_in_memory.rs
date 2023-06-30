use std::sync::Arc;

use domain_engine_core::DomainEngine;
use domain_engine_juniper::GqlContext;
use juniper::graphql_value;
use ontol_runtime::config::DataStoreConfig;
use ontol_test_utils::{expect_eq, SourceName, TestPackages};
use test_log::test;

use crate::{Exec, TestCompileSchema};

#[test(tokio::test)]
async fn test_graphql_conduit_db_in_memory() {
    let test_packages = TestPackages::with_sources([(
        SourceName::root(),
        include_str!("../../../examples/conduit/conduit_db.on"),
    )])
    .with_data_store(SourceName::root(), DataStoreConfig::InMemory);

    let (test_env, schema) = test_packages.compile_schema();
    let gql_context = GqlContext {
        engine_api: Arc::new(DomainEngine::new(test_env.env.clone())),
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
        .exec(&schema, &gql_context,)
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
        .exec(&schema, &gql_context,)
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
