use domain_engine_test_utils::graphql_test_utils::{
    gql_ctx_mock_data_store, Exec, TestCompileSchema,
};
use ontol_test_utils::{assert_error_msg, SourceName};
use test_log::test;

const ROOT: SourceName = SourceName::root();

#[test(tokio::test)]
async fn test_graphql_input_deserialization_error() {
    let (test, [schema]) = "
    def foo {
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
        .exec([], &schema, &ctx)
        .await,
        r#"Execution: invalid type: string "invalid", expected "const" in input at line 4 column 26 (field at line 2 column 12)"#
    );
}

#[test(tokio::test)]
// The point of this test is to pass the JSON array through juniper's
// layer and let the input hit the data store.
// This requires a juniper patch.
#[should_panic = "No mock implementation found"]
async fn test_graphql_input_constructor_sequence_as_json_scalar() {
    let (test, [schema]) = "
    def tuple {
        rel .0: i64
        rel .1: text
    }

    def foo {
        rel .id: { fmt '' => text => . }
        rel .'prop': tuple
    }
    "
    .compile_schemas([SourceName::root()]);

    r#"
    mutation {
        createfoo(
            input: {
                prop: [42, "text"]
            }
        ) {
            prop
        }
    }"#
    .exec([], &schema, &gql_ctx_mock_data_store(&test, ROOT, ()))
    .await
    .unwrap();
}
