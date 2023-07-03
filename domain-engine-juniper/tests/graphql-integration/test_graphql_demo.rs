use ontol_test_utils::SourceName;
use test_log::test;

use crate::TestCompileSchema;

pub const DEMO: &str = include_str!("../../../examples/demo.on");

#[test(tokio::test)]
async fn test_graphql_input_deserialization_error() {
    let _ = DEMO.compile_schemas([SourceName::root()]);
}
