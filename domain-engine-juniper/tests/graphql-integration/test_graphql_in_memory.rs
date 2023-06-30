use std::sync::Arc;

use domain_engine_core::DomainEngine;
use domain_engine_juniper::GqlContext;
use ontol_runtime::config::DataStoreConfig;
use ontol_test_utils::{SourceName, TestPackages};
use test_log::test;

use crate::{Exec, TestCompileSchema};

#[test(tokio::test)]
// FIXME:
#[should_panic = "BUG: unhandled combination"]
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

    r#"mutation {
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
    .await
    .unwrap();
}
