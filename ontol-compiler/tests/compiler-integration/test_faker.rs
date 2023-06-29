use ontol_runtime::serde::processor::ProcessorMode;
use ontol_test_utils::{expect_eq, type_binding::*, TestCompile};
use serde_json::json;
use test_log::test;

const GEOJSON: &str = include_str!("../../../examples/geojson.on");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.on");

#[test]
fn test_fake_primitives() {
    "
    pub type foo {
        rel .'s': string
        rel .'i': int
    }
    "
    .compile_ok(|test_env| {
        let foo = TypeBinding::new(&test_env, "foo");

        expect_eq!(
            actual = read_ser(&foo).json(&foo.new_fake(ProcessorMode::Inspect)),
            expected = json!({
                "s": "mollitia sit porro tenetur",
                "i": 2117826670,
            }),
        );
    });
}

#[test]
fn test_fake_string_like_types() {
    "
    pub type foo {
        rel .'id': uuid
        rel .'created_at': datetime
    }
    "
    .compile_ok(|test_env| {
        let foo = TypeBinding::new(&test_env, "foo");

        expect_eq!(
            actual = read_ser(&foo).json(&foo.new_fake(ProcessorMode::Inspect)),
            expected = json!({
                "id": "042da2de-98c0-64cf-94c2-5463ca1c3fbe",
                "created_at": "1943-07-25T19:00:15.149284864+00:00",
            }),
        );
    });
}

#[test]
fn test_fake_geojson() {
    GEOJSON.compile_ok(|test_env| {
        let geometry = TypeBinding::new(&test_env, "Geometry");

        expect_eq!(
            actual = read_ser(&geometry).json(&geometry.new_fake(ProcessorMode::Inspect)),
            expected = json!({
                "type": "Polygon",
                "coordinates": [
                    [-1732221745, 70099678],
                    [-1214996992, -194249765],
                    [2117826670, -1428302769],
                    [-140280828, -2108307479],
                    [428606290, 1128193060]
                ]
            }),
        );
    });
}

#[test]
fn test_fake_guitar_synth() {
    GUITAR_SYNTH_UNION.compile_ok(|test_env| {
        let artist = TypeBinding::new(&test_env, "artist");

        expect_eq!(
            actual = read_ser(&artist).json(&artist.new_fake(ProcessorMode::Inspect)),
            expected = json!({
                "artist-id": "mollitia sit porro tenetur",
                "name": "delectus molestias aspernatur voluptatem reprehenderit",
                "plays": [
                    {
                        "instrument-id": "guitar/9fe5a078-ccaa-f029-bccc-b1436ec3ffee",
                        "type": "guitar",
                        "string_count": -245468183,
                    }
                ]
            }),
        );
    });
}
