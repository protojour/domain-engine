use std::sync::Arc;

use ontol_examples::{geojson, wgs, AsAtlas};
use ontol_macros::test;
use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches, file_url, serde_helper::*, TestCompile, TestPackages,
};
use serde_json::json;

#[test]
fn test_geojson() {
    TestPackages::with_sources(
        [geojson(), wgs().as_atlas("wgs")]
    ).compile_then(|test| {
        let [geometry] = test.bind(["Geometry"]);
        assert_json_io_matches!(serde_create(&geometry), {
            "type": "Point",
            "coordinates": [1.0, 2.0]
        });
        assert_json_io_matches!(serde_create(&geometry), {
            "type": "Polygon",
            "coordinates": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [1.0, 2.0]]
        });
        assert_json_io_matches!(serde_create(&geometry), {
            "type": "MultiPolygon", "coordinates": [
                [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [1.0, 2.0]],
                [[2.0, 3.0], [4.0, 5.0], [6.0, 7.0], [2.0, 3.0]],
            ]
        });
        assert_json_io_matches!(serde_create(&geometry), {
            "type": "GeometryCollection",
            "geometries": [
                {
                    "type": "Point",
                    "coordinates": [1.0, 2.0],
                }
            ]
        });
        assert_error_msg!(
            serde_create(&geometry).to_value_variant(json!({ "type": "Point", "coordinates": [[1.0, 2.0]] })),
            "invalid type: sequence, expected float at line 1 column 42"
        );
        assert_error_msg!(
            serde_create(&geometry).to_value_variant(json!({ "type": "Polygon", "coordinates": [1.0, 2.0] })),
            "invalid type: floating point `1.0`, expected sequence with length 2 at line 1 column 42"
        );
        assert_error_msg!(
            serde_create(&geometry).to_value_variant(json!({ "type": "LineString", "coordinates": [[1.0, 2.0]] })),
            "invalid length 1, expected sequence with minimum length 2 at line 1 column 47"
        );
        assert_error_msg!(
            serde_create(&geometry).to_value_variant(json!({ "type": "Polygon", "coordinates": [[1.0, 2.0]] })),
            "invalid length 1, expected sequence with minimum length 4 at line 1 column 44"
        );
        assert_error_msg!(
            serde_create(&geometry).to_value_variant(json!([])),
            "invalid type: sequence, expected `Geometry` (one of `Point`, `MultiPoint`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`, `GeometryCollection`) at line 1 column 2"
        );
        assert_error_msg!(
            serde_create(&geometry).to_value_variant(json!({ "type": "bogus" })),
            "invalid type, expected `Geometry` (one of `Point`, `MultiPoint`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`, `GeometryCollection`) at line 1 column 16"
        );
    });
}

#[test]
fn test_municipalities() {
    TestPackages::with_sources([
        (
            file_url("entry"),
            Arc::new(
                "
                domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
                use 'geojson' as geojson
    
                def kommunenummer(
                    fmt '' => /[0-9]{4}/ => .
                )
    
                def kommune(
                    rel. 'kommunenummer': kommunenummer
                    rel* 'geometry': geojson.Polygon
                )
                "
                .to_string(),
            ),
        ),
        geojson(),
        wgs().as_atlas("wgs"),
    ])
    .compile_then(|_test| {});
}
