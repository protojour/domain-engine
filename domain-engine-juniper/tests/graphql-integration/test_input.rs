use ontol_test_utils::assert_error_msg;
use test_log::test;

use crate::{Exec, TestCompileSchema};

#[test(tokio::test)]
async fn test_input_deserialization_error() {
    let schema = "
    type foo {
        rel [id] string
        rel ['prop'] 'const'
    }
    "
    .compile_schema();

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
        .exec(&schema)
        .await,
        r#"Execution: invalid type: string "invalid", expected "const" (at line 2 column 12)"#
    );
}
