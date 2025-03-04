use ontol_examples::{AsAtlas, geojson, guitar_synth_union, wgs};
use ontol_macros::test;
use ontol_runtime::{attr::AttrRef, interface::serde::processor::ProcessorMode};
use ontol_test_utils::{TestCompile, TestPackages, expect_eq, serde_helper::*};
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
                "s": "culpa mollitia sit porro tenetur",
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
    let test = TestPackages::with_sources([geojson(), wgs().as_atlas("wgs")]).compile();
    let [geometry] = test.bind(["Geometry"]);
    expect_eq!(
        actual =
            serde_read(&geometry).as_json(AttrRef::Unit(&geometry.new_fake(ProcessorMode::Raw))),
        expected = json!({
            "type": "MultiPolygon",
            "coordinates": [[
                [0.5966856962957163, 0.7171114683189246],
                [0.4930949469457317, 0.9673383246798644],
                [0.09979267849333606, 0.3921394141536853],
                [0.2609328155372387, 0.2651969652438151],
                [0.12183820979625291, 0.6705140631428715],
            ]]
        }),
    );
}

#[test]
fn test_fake_guitar_synth() {
    let test = guitar_synth_union().1.compile();
    let [artist] = test.bind(["artist"]);
    expect_eq!(
        actual = serde_read(&artist).as_json(AttrRef::Unit(&artist.new_fake(ProcessorMode::Raw))),
        expected = json!({
            "artist-id": "culpa mollitia sit porro tenetur",
            "name": "libero delectus molestias aspernatur",
            "plays": [
                {
                    "instrument-id": "guitar/7eabbaec-c2c9-eccf-9a1c-fabab639dd16",
                    "type": "guitar",
                    "string_count": 1684225959,
                }
            ]
        }),
    );
}
