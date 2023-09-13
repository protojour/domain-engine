use ontol_test_utils::{assert_error_msg, SourceName};
use test_log::test;

use crate::{gql_ctx_mock_data_store, Exec, TestCompileSchema};

#[test(tokio::test)]
async fn test_graphql_input_deserialization_error() {
    let (test, [schema]) = "
    pub def foo_id { fmt '' => text => . }
    pub def foo {
        rel foo_id identifies: .
        rel .'prop': 'const'
    }
    "
    .compile_schemas([SourceName::root()]);

    let ctx = gql_ctx_mock_data_store(&test, SourceName::root(), ());
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
        .exec(&schema, &ctx, [])
        .await,
        r#"Execution: invalid type: string "invalid", expected "const" in input at line 4 column 26 (field at line 2 column 12)"#
    );
}
