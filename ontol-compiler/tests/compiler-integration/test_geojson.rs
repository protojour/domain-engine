use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches, serde_utils::*, SourceName, TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;

use crate::examples::{GEOJSON, WGS};

#[test]
fn test_geojson() {
    TestPackages::with_sources(
        [(SourceName::root(), GEOJSON.1), WGS]
    ).compile_ok(|test| {
        let [geometry] = test.bind(["Geometry"]);
        assert_json_io_matches!(geometry, Create, {
            "type": "Point",
            "coordinates": [1.0, 2.0]
        });
        assert_json_io_matches!(geometry, Create, {
            "type": "Polygon",
            "coordinates": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [1.0, 2.0]]
        });
        assert_json_io_matches!(geometry, Create, {
            "type": "MultiPolygon", "coordinates": [
                [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0], [1.0, 2.0]],
                [[2.0, 3.0], [4.0, 5.0], [6.0, 7.0], [2.0, 3.0]],
            ]
        });
        assert_json_io_matches!(geometry, Create, {
            "type": "GeometryCollection",
            "geometries": [
                {
                    "type": "Point",
                    "coordinates": [1.0, 2.0],
                }
            ]
        });
        assert_error_msg!(
            create_de(&geometry).data_variant(json!({ "type": "Point", "coordinates": [[1.0, 2.0]] })),
            "invalid type: sequence, expected float at line 1 column 42"
        );
        assert_error_msg!(
            create_de(&geometry).data_variant(json!({ "type": "Polygon", "coordinates": [1.0, 2.0] })),
            "invalid type: floating point `1`, expected sequence with length 2 at line 1 column 42"
        );
        assert_error_msg!(
            create_de(&geometry).data_variant(json!({ "type": "LineString", "coordinates": [[1.0, 2.0]] })),
            "invalid length 1, expected sequence with minimum length 2 at line 1 column 47"
        );
        assert_error_msg!(
            create_de(&geometry).data_variant(json!({ "type": "Polygon", "coordinates": [[1.0, 2.0]] })),
            "invalid length 1, expected sequence with minimum length 4 at line 1 column 44"
        );
        assert_error_msg!(
            create_de(&geometry).data_variant(json!([])),
            "invalid type: sequence, expected `Geometry` (one of `Point`, `MultiPoint`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`, `GeometryCollection`) at line 1 column 2"
        );
        assert_error_msg!(
            create_de(&geometry).data_variant(json!({ "type": "bogus" })),
            "invalid map value, expected `Geometry` (one of `Point`, `MultiPoint`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`, `GeometryCollection`) at line 1 column 16"
        );
    });
}

#[test]
fn test_municipalities() {
    TestPackages::with_sources([
        GEOJSON,
        WGS,
        (
            SourceName::root(),
            "
            use 'geojson' as geojson

            type kommunenummer {
                fmt '' => /[0-9]{4}/ => .
            }

            type kommune {
                rel .'kommunenummer': kommunenummer
                rel .'geometry': geojson.Polygon
            }
            ",
        ),
    ])
    .compile_ok(|_test| {});
}
