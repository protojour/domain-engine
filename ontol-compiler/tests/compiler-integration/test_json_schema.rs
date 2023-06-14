use ontol_runtime::serde::processor::ProcessorMode::*;
use ontol_test_utils::{type_binding::TypeBinding, TestCompile};
use pretty_assertions::assert_eq;
use serde_json::json;
use test_log::test;

#[test]
fn json_schema_from_simple_entity() {
    "
    pub type some_id { fmt '' => string => _ }
    pub type entity {
        rel some_id identifies: _
        rel _ 'foo': string
    }
    "
    .compile_ok(|env| {
        assert_eq!(
            json!({
                "$defs": {
                    "1_4": {
                        "properties": {
                            "foo": {
                                "type": "string",
                            },
                        },
                        "required": [
                            "foo",
                        ],
                        "type": "object",
                    },
                },
                "allOf": [
                    {
                        "$ref": "#/$defs/1_4",
                    },
                ],
                "unevaluatedProperties": false,
            }),
            TypeBinding::new(&env, "entity").json_schema(Create)
        )
    });
}
