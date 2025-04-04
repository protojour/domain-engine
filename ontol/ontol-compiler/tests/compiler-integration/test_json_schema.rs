use ontol_examples::artist_and_instrument;
use ontol_macros::test;
use ontol_runtime::interface::{
    json_schema::build_openapi_schemas, serde::processor::ProcessorMode::*,
};
use ontol_test_utils::{TestCompile, expect_eq};
use serde_json::json;

#[test]
fn json_schema_from_simple_entity() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def some_id (fmt '' => text => .)

    /// This is type entity
    def entity (
        rel. 'id': some_id

        /// This is property 'foo'
        rel* 'foo': text

        /// This is property 'bar'
        rel* 'bar': true

        // This is just a regular comment
        rel* 'baz': i64
    )
    "
    .compile_then(|test| {
        let [entity] = test.bind(["entity"]);
        expect_eq!(
            actual = entity.new_json_schema(Create),
            expected = json!({
                "$defs": {
                    "1_entity": {
                        "type": "object",
                        "description": "This is type entity",
                        "properties": {
                            "id": {
                                "type": "string",
                            },
                            "foo": {
                                "type": "string",
                                "description": "This is property 'foo'",
                            },
                            "bar": {
                                "type": "boolean",
                                "enum": [true],
                                "description": "This is property 'bar'",
                            },
                            "baz": {
                                "type": "integer",
                                "format": "int64",
                            },
                        },
                        "required": [
                            "id",
                            "foo",
                            "bar",
                            "baz"
                        ],
                    },
                },
                "allOf": [
                    {
                        "$ref": "#/$defs/1_entity",
                    },
                ],
                "unevaluatedProperties": false,
            }),
        )
    });
}

// https://gitlab.com/protojour/memoriam/domain-engine/-/issues/83
#[test]
fn test_artist_and_instrument_json_schema() {
    artist_and_instrument().1.compile_then(|test| {
        serde_json::to_string(&build_openapi_schemas(
            test.ontology(),
            test.entrypoint(),
            test.ontology().domain_by_index(test.entrypoint()).unwrap(),
        ))
        .unwrap();
    });
}
