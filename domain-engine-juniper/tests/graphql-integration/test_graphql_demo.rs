use ontol_test_utils::{examples::DEMO, SourceName};
use test_log::test;

use crate::TestCompileSchema;

#[test(tokio::test)]
async fn test_graphql_input_deserialization_error() {
    let _ = DEMO.1.compile_schemas([SourceName::root()]);
}
