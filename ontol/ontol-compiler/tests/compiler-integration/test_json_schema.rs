use ontol_runtime::interface::{
    json_schema::build_openapi_schemas, serde::processor::ProcessorMode::*,
};
use ontol_test_utils::{examples::ARTIST_AND_INSTRUMENT, expect_eq, TestCompile};
use serde_json::json;
use test_log::test;

#[test]
fn json_schema_from_simple_entity() {
    "
    def some_id (fmt '' => text => .)

    /// This is type entity
    def entity (
        rel .id: some_id

        /// This is property 'foo'
        rel .'foo': text

        /// This is property 'bar'
        rel .'bar': true

        // This is just a regular comment
        rel .'baz': i64
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
    ARTIST_AND_INSTRUMENT.1.compile_then(|test| {
        serde_json::to_string(&build_openapi_schemas(
            &test.ontology(),
            test.root_package(),
            test.ontology().find_domain(test.root_package()).unwrap(),
        ))
        .unwrap();
    });
}
