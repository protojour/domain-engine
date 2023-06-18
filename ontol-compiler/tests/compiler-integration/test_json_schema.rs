use ontol_runtime::serde::processor::ProcessorMode::*;
use ontol_test_utils::{type_binding::TypeBinding, TestCompile};
use pretty_assertions::assert_eq;
use serde_json::json;
use test_log::test;

#[test]
fn json_schema_from_simple_entity() {
    "
    type some_id { fmt '' => string => . }

    /// This is type entity
    pub type entity {
        rel some_id identifies: .

        /// This is property 'foo'
        rel .'foo': string

        /// This is property 'bar'
        rel .'bar': true

        // This is just a regular comment
        rel .'baz': int
    }
    "
    .compile_ok(|env| {
        assert_eq!(
            json!({
                "$defs": {
                    "entity": {
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
                        "$ref": "#/$defs/entity",
                    },
                ],
                "unevaluatedProperties": false,
            }),
            TypeBinding::new(&env, "entity").json_schema(Create)
        )
    });
}
