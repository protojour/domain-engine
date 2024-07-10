use ontol_macros::test;
use ontol_runtime::{attr::AttrRef, interface::serde::processor::ProcessorMode};
use ontol_test_utils::{
    examples::{GEOJSON, GUITAR_SYNTH_UNION, WGS},
    expect_eq,
    serde_helper::*,
    TestCompile, TestPackages,
};
use serde_json::json;

#[test]
fn test_fake_primitives() {
    "
    def foo (
        rel* 's': text
        rel* 'i': i64
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        expect_eq!(
            actual = serde_read(&foo).as_json(AttrRef::Unit(&foo.new_fake(ProcessorMode::Raw))),
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
    def foo (
        rel* 'id': uuid
        rel* 'created_at': datetime
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        expect_eq!(
            actual = serde_read(&foo).as_json(AttrRef::Unit(&foo.new_fake(ProcessorMode::Raw))),
            expected = json!({
                "id": "be63cfde-00db-4e4f-04e9-5224a7b8351a",
                "created_at": "2249-06-21T19:23:34.131225284Z",
            }),
        );
    });
}

#[test]
fn test_fake_geojson() {
    let test = TestPackages::with_static_sources([GEOJSON, WGS]).compile();
    let [geometry] = test.bind(["Geometry"]);
    expect_eq!(
        actual =
            serde_read(&geometry).as_json(AttrRef::Unit(&geometry.new_fake(ProcessorMode::Raw))),
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
        actual = serde_read(&artist).as_json(AttrRef::Unit(&artist.new_fake(ProcessorMode::Raw))),
        expected = json!({
            "artist-id": "mollitia sit porro tenetur",
            "name": "delectus molestias aspernatur voluptatem reprehenderit",
            "plays": [
                {
                    "instrument-id": "guitar/aec6f2c9-eccf-9a1c-faba-639dd16ca1ba",
                    "type": "guitar",
                    "string_count": -1917199177,
                }
            ]
        }),
    );
}
