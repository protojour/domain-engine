use ontol_runtime::serde::processor::ProcessorMode;
use ontol_test_utils::{type_binding::TypeBinding, TestCompile};
use pretty_assertions::assert_eq;
use serde_json::json;
use test_log::test;

pub const GEOJSON: &str = include_str!("../../../examples/geojson.on");

#[test]
fn test_fake_primitives() {
    "
    pub type foo {
        rel _ 's': string
        rel _ 'i': int
    }
    "
    .compile_ok(|test_env| {
        let foo = TypeBinding::new(&test_env, "foo");

        assert_eq!(
            foo.serialize_json(&foo.new_fake(ProcessorMode::Create)),
            json!({
                "s": "Patricia McClure",
                "i": -1732221745,
            })
        );
    });
}

#[test]
fn test_fake_geojson() {
    GEOJSON.compile_ok(|test_env| {
        let geometry = TypeBinding::new(&test_env, "Geometry");

        assert_eq!(
            geometry.serialize_json(&geometry.new_fake(ProcessorMode::Create)),
            json!({
                "type": "Polygon",
                "coordinates": [
                    [-1732221745, 70099678],
                    [-1214996992, -194249765],
                    [2117826670, -1428302769],
                    [-140280828, -2108307479],
                    [428606290, 1128193060]
                ]
            })
        );
    });
}
