use domain_engine_test_utils::graphql_test_utils::{
    gql_ctx_mock_data_store, Exec, TestCompileSingletonSchema,
};
use ontol_macros::test;
use ontol_test_utils::{assert_error_msg, default_short_name};

#[test(tokio::test)]
async fn test_graphql_input_deserialization_error() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id': (fmt '' => serial => .)
        rel* 'prop': 'const'
    )
    "
    .compile_single_schema();

    let ctx = gql_ctx_mock_data_store(&test, &[default_short_name()], ());
    assert_error_msg!(
        r#"mutation {
            foo(create: [{
                id: "1"
                prop: "invalid"
            }]) {
                node { prop }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        r#"Execution: invalid type: string "invalid", expected "const" in input at line 3 column 22 (field at line 1 column 12)"#
    );
}

#[test(tokio::test)]
// The point of this test is to pass the JSON array through juniper's
// layer and let the input hit the data store.
// This requires a juniper patch.
#[should_panic = "No mock implementation found"]
async fn test_graphql_input_constructor_sequence_as_json_scalar() {
    let (test, schema) = "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def tuple (
        rel* 0: i64
        rel* 1: text
    )

    def foo (
        rel. 'id': (fmt '' => serial => .)
        rel* 'prop': tuple
    )
    "
    .compile_single_schema();

    r#"mutation {
        foo(create: [{
            id: "1"
            prop: [42, "text"]
        }]) {
            node { prop }
        }
    }"#
    .exec(
        [],
        &schema,
        &gql_ctx_mock_data_store(&test, &[default_short_name()], ()),
    )
    .await
    .unwrap();
}
