use juniper::graphql_value;
use ontol_test_utils::{assert_error_msg, expect_eq, SourceName};
use test_log::test;

use crate::{gql_ctx_mock_data_store, Exec, TestCompileSchema};

const ROOT: SourceName = SourceName::root();

#[test(tokio::test)]
async fn test_graphql_input_deserialization_error() {
    let (test, [schema]) = "
    pub def foo {
        rel .id: { fmt '' => text => . }
        rel .'prop': 'const'
    }
    "
    .compile_schemas([SourceName::root()]);

    let ctx = gql_ctx_mock_data_store(&test, ROOT, ());
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

#[test(tokio::test)]
// BUG in juniper
#[should_panic = "Invalid value for argument"]
async fn test_graphql_input_constructor_sequence() {
    let (test, [schema]) = "
    def tuple {
        rel .0: i64
        rel .1: text
    }

    pub def foo {
        rel .id: { fmt '' => text => . }
        rel .'prop': tuple
    }
    "
    .compile_schemas([SourceName::root()]);

    expect_eq!(
        actual = r#"
        mutation {
            createfoo(
                input: {
                    prop: [42, "text"]
                }
            ) {
                prop
            }
        }"#
        .exec(&schema, &gql_ctx_mock_data_store(&test, ROOT, ()), [])
        .await,
        expected = Ok(graphql_value!({
            "municipalityList": {
                "edges": []
            }
        })),
    );
}
