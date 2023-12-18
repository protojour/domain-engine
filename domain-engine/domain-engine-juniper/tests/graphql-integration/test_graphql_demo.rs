use domain_engine_test_utils::graphql_test_utils::TestCompileSchema;
use ontol_test_utils::{examples::DEMO, SourceName};
use test_log::test;

#[test(tokio::test)]
async fn test_graphql_input_deserialization_error() {
    let _ = DEMO.1.compile_schemas([SourceName::root()]);
}
