use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches, type_binding::TypeBinding, SourceName, TestCompile,
    TestPackages,
};
use serde_json::json;
use test_log::test;

pub const GEOJSON: &str = include_str!("../../../examples/geojson.on");

#[test]
fn test_geojson() {
    GEOJSON.compile_ok(|env| {
        let geometry = TypeBinding::new(&env, "Geometry");
        assert_json_io_matches!(
            geometry,
            json!({ "type": "Point", "coordinates": [1, 2]})
        );
        assert_json_io_matches!(
            geometry,
            json!({ "type": "Polygon", "coordinates": [[1, 2], [3, 4], [5, 6], [1, 2]]})
        );
        assert_json_io_matches!(
            geometry,
            json!({ "type": "MultiPolygon", "coordinates": [
                [[1, 2], [3, 4], [5, 6], [1, 2]],
                [[2, 3], [4, 5], [6, 7], [2, 3]],
            ]})
        );
        assert_json_io_matches!(
            geometry,
            json!({
                "type": "GeometryCollection",
                "geometries": [
                    {
                        "type": "Point",
                        "coordinates": [1, 2],
                    }
                ]
            })
        );
        assert_error_msg!(
            geometry.deserialize_data_variant(json!({ "type": "Point", "coordinates": [[1, 2]] })),
            "invalid type: sequence, expected integer at line 1 column 38"
        );
        assert_error_msg!(
            geometry.deserialize_data_variant(json!({ "type": "Polygon", "coordinates": [1, 2] })),
            "invalid type: integer `1`, expected sequence with length 2 at line 1 column 38"
        );
        assert_error_msg!(
            geometry.deserialize_data_variant(json!({ "type": "LineString", "coordinates": [[1, 2]] })),
            "invalid length 1, expected sequence with minimum length 2 at line 1 column 43"
        );
        assert_error_msg!(
            geometry.deserialize_data_variant(json!({ "type": "Polygon", "coordinates": [[1, 2]] })),
            "invalid length 1, expected sequence with minimum length 4 at line 1 column 40"
        );
        assert_error_msg!(
            geometry.deserialize_data_variant(json!([])),
            "invalid type: sequence, expected `Geometry` (one of `Point`, `MultiPoint`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`, `GeometryCollection`) at line 1 column 2"
        );
        assert_error_msg!(
            geometry.deserialize_data_variant(json!({ "type": "bogus" })),
            "invalid map value, expected `Geometry` (one of `Point`, `MultiPoint`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`, `GeometryCollection`) at line 1 column 16"
        );
    });
}

#[test]
fn test_municipalities() {
    TestPackages::with_sources([
        (SourceName("geojson"), GEOJSON),
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
    .compile_ok(|_env| {});
}
