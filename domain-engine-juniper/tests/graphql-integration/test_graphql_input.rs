use ontol_test_utils::assert_error_msg;
use test_log::test;

use crate::{mock_gql_context, Exec, TestCompileSchema};

#[test(tokio::test)]
async fn test_graphql_input_deserialization_error() {
    let (_, schema) = "
    pub type foo_id { rel '' [string] }
    pub type foo {
        rel foo_id [identifies]
        rel ['prop'] 'const'
    }
    "
    .compile_schema();

    let ctx = mock_gql_context(());
    assert_error_msg!(
        r#"
        mutation {
            createfoo(
                input: {
                    prop: "invalid"
                }
            ) {
                prop
            }
        }"#
        .exec(&schema, &ctx)
        .await,
        r#"Execution: invalid type: string "invalid", expected "const" in input at line 4 column 26 (field at line 2 column 12)"#
    );
}
