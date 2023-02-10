use crate::{
    assert_error_msg, assert_json_io_matches, util::serialize_json, util::TypeBinding, TestCompile,
};
use serde_json::json;
use test_log::test;

#[test]
fn test_geojson() {
    r#"
    ; TODO: domain-private types?
    (type! position)
    (rel! (position) _ (tuple! (int) (int)))
    ; ALTERNATIVE:
    ; (rel! (position) 0..1 (int))

    (type! position-list)
    (rel! (position-list) _[2..] (position))
    ; ALTERNATIVE:
    ; (rel! (position-list) 0 (position))
    ; (rel! (position-list) 1.. (position))

    (type! position-ring)
    (rel! (position-ring) _[4..] (position))
    ; ALTERNATIVE:
    ; (rel! (position-ring) 0..2 (position))
    ; (rel! (position-ring) 3.. (position))

    (type! Geometry)
    (type! LeafGeometry)

    (type! Point)
    (rel! (Point) type "Point")
    (rel! (Point) coordinates (position))

    (type! MultiPoint)
    (rel! (MultiPoint) type "MultiPoint")
    (rel! (MultiPoint) coordinates[] (position))

    (type! LineString)
    (rel! (LineString) type "LineString")
    (rel! (LineString) coordinates (position-list))

    (type! MultiLineString)
    (rel! (MultiLineString) type "MultiLineString")
    (rel! (MultiLineString) coordinates[] (position-list))

    (type! Polygon)
    (rel! (Polygon) type "Polygon")
    (rel! (Polygon) coordinates (position-ring))

    (type! MultiPolygon)
    (rel! (MultiPolygon) type "MultiPolygon")
    (rel! (MultiPolygon) coordinates[] (position-ring))

    (type! GeometryCollection)
    (rel! (GeometryCollection) type "GeometryCollection")
    (rel! (GeometryCollection) geometries[] (LeafGeometry))

    (rel! (LeafGeometry) _ (Point))
    (rel! (LeafGeometry) _ (MultiPoint))
    (rel! (LeafGeometry) _ (LineString))
    (rel! (LeafGeometry) _ (MultiLineString))
    (rel! (LeafGeometry) _ (Polygon))
    (rel! (LeafGeometry) _ (MultiPolygon))

    ; Note: Geometry is an extension of LeafGeometry + GeometryCollection
    (rel! (Geometry) _ (Point))
    (rel! (Geometry) _ (MultiPoint))
    (rel! (Geometry) _ (LineString))
    (rel! (Geometry) _ (MultiLineString))
    (rel! (Geometry) _ (Polygon))
    (rel! (Geometry) _ (MultiPolygon))
    (rel! (Geometry) _ (GeometryCollection))

    (type! GeometryOrNull)
    (rel! (GeometryOrNull) _ (Geometry))
    ; TODO: Not supported yet (union tree):
    ; (rel! (GeometryOrNull) _ (null))

    (type! Feature)
    (rel! (Feature) type "Feature")
    (rel! (Feature) geometry (GeometryOrNull))

    ; FIXME: Features is a map of "anything".
    ; This could be a good use case for generics in ONTOL.
    (rel! (Feature) properties (null))

    (type! FeatureCollection)
    (rel! (FeatureCollection) type "FeatureCollection")
    (rel! (FeatureCollection) features[] (Feature))
    "#
    .compile_ok(|env| {
        let geometry = TypeBinding::new(env, "Geometry");
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
            "invalid type: integer `1`, expected finite tuple with length 2 at line 1 column 38"
        );
        assert_error_msg!(
            geometry.deserialize_data_variant(json!({ "type": "LineString", "coordinates": [[1, 2]] })),
            "invalid length 1, expected array[2..] at line 1 column 43"
        );
        assert_error_msg!(
            geometry.deserialize_data_variant(json!({ "type": "Polygon", "coordinates": [[1, 2]] })),
            "invalid length 1, expected array[4..] at line 1 column 40"
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
