use ontol_runtime::interface::serde::processor::ProcessorMode;
use ontol_test_utils::{
    examples::{Root, GEOJSON, GUITAR_SYNTH_UNION, WGS},
    expect_eq,
    serde_helper::*,
    TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;

#[test]
fn test_fake_primitives() {
    "
    pub def foo {
        rel .'s': text
        rel .'i': i64
    }
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        expect_eq!(
            actual = serde_read(&foo).as_json(&foo.new_fake(ProcessorMode::Raw)),
            expected = json!({
                "s": "mollitia sit porro tenetur",
                "i": 2117826670,
            }),
        );
    });
}

#[test]
fn test_fake_text_like_types() {
    "
    pub def foo {
        rel .'id': uuid
        rel .'created_at': datetime
    }
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        expect_eq!(
            actual = serde_read(&foo).as_json(&foo.new_fake(ProcessorMode::Raw)),
            expected = json!({
                "id": "042da2de-98c0-64cf-94c2-5463ca1c3fbe",
                "created_at": "1943-07-25T19:00:15.149284864+00:00",
            }),
        );
    });
}

#[test]
fn test_fake_geojson() {
    let test = TestPackages::with_sources([GEOJSON.root(), WGS]).compile();
    let [geometry] = test.bind(["Geometry"]);
    expect_eq!(
        actual = serde_read(&geometry).as_json(&geometry.new_fake(ProcessorMode::Raw)),
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
}

#[test]
fn test_fake_guitar_synth() {
    let test = GUITAR_SYNTH_UNION.1.compile();
    let [artist] = test.bind(["artist"]);
    expect_eq!(
        actual = serde_read(&artist).as_json(&artist.new_fake(ProcessorMode::Raw)),
        expected = json!({
            "artist-id": "mollitia sit porro tenetur",
            "name": "delectus molestias aspernatur voluptatem reprehenderit",
            "plays": [
                {
                    "instrument-id": "guitar/c6f2c9be-dcaa-7bfa-8763-dd16ca1baa0a",
                    "type": "guitar",
                    "string_count": 1500328598,
                }
            ]
        }),
    );
}
