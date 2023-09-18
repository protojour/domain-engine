use ontol_runtime::interface::serde::processor::ProcessorMode::*;
use ontol_test_utils::{expect_eq, TestCompile};
use serde_json::json;
use test_log::test;

#[test]
fn json_schema_from_simple_entity() {
    "
    def some_id { fmt '' => text => . }

    /// This is type entity
    pub def entity {
        rel some_id identifies: .

        /// This is property 'foo'
        rel .'foo': text

        /// This is property 'bar'
        rel .'bar': true

        // This is just a regular comment
        rel .'baz': i64
    }
    "
    .compile_ok(|test| {
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
