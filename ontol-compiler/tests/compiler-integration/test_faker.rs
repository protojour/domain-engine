use ontol_runtime::serde::processor::ProcessorMode;
use ontol_test_utils::{
    examples::{Root, GEOJSON, GUITAR_SYNTH_UNION, WGS},
    expect_eq,
    serde_utils::*,
    TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;

#[test]
fn test_fake_primitives() {
    "
    pub type foo {
        rel .'s': string
        rel .'i': i64
    }
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
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
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
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
    TestPackages::with_sources([GEOJSON.root(), WGS]).compile_ok(|test| {
        let [geometry] = test.bind(["Geometry"]);
        expect_eq!(
            actual = read_ser(&geometry).json(&geometry.new_fake(ProcessorMode::Inspect)),
            expected = json!({
                "type": "Polygon",
                "coordinates": [
                    [0.016321353287595652, 0.9547727023524024],
                    [0.6674473470759333, 0.5091214128693887],
                    [0.2626779163488635, 0.1856411528755303],
                    [0.720634883796082, 0.47809322766615],
                    [0.7067125942841727, 0.7238569168072458],
                ]
            }),
        );
    });
}

#[test]
fn test_fake_guitar_synth() {
    GUITAR_SYNTH_UNION.1.compile_ok(|test| {
        let [artist] = test.bind(["artist"]);
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
