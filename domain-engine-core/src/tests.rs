use ontol_runtime::config::DataSourceConfig;
use ontol_test_utils::{SourceName, TestCompile, TestPackages};
use test_log::test;

use crate::DomainEngine;

const CONDUIT_DB: &str = include_str!("../../examples/conduit/conduit_db.on");

#[test(tokio::test)]
async fn test_conduit_in_memory_data_source() {
    TestPackages::with_sources([(SourceName::root(), CONDUIT_DB)])
        .with_data_source(SourceName::root(), DataSourceConfig::InMemory)
        .compile_ok(|test_env| {
            let _domain_engine = DomainEngine::new(test_env.env.clone());
        });
}
