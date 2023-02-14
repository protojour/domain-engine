use crate::{
    assert_error_msg, assert_json_io_matches, util::serialize_json, util::TypeBinding, TestCompile,
};
use serde_json::json;
use test_log::test;

#[test]
fn test_geojson() {
    "
    ; TODO: domain-private types?
    (type! position)
    (rel! position { 0..2 } int)

    (type! position-list)
    (rel! position-list { ..2 } position)
    (rel! position-list { 2.. } position)

    (type! position-ring)
    (rel! position-ring { ..4 } position)
    (rel! position-ring { 4.. } position)

    (type! Geometry)
    (type! LeafGeometry)

    (type! Point)
    (rel! Point { 'type' } 'Point')
    (rel! Point { 'coordinates' } position)

    (type! MultiPoint)
    (rel! MultiPoint { 'type' } 'MultiPoint')
    (rel! MultiPoint { 'coordinates'* } position)

    (type! LineString)
    (rel! LineString { 'type' } 'LineString')
    (rel! LineString { 'coordinates' } position-list)

    (type! MultiLineString)
    (rel! MultiLineString { 'type' } 'MultiLineString')
    (rel! MultiLineString { 'coordinates'* } position-list)

    (type! Polygon)
    (rel! Polygon { 'type' } 'Polygon')
    (rel! Polygon { 'coordinates' } position-ring)

    (type! MultiPolygon)
    (rel! MultiPolygon { 'type' } 'MultiPolygon')
    (rel! MultiPolygon { 'coordinates'* } position-ring)

    (type! GeometryCollection)
    (rel! GeometryCollection { 'type' } 'GeometryCollection')
    (rel! GeometryCollection { 'geometries'* } LeafGeometry)

    (rel! LeafGeometry {} Point)
    (rel! LeafGeometry {} MultiPoint)
    (rel! LeafGeometry {} LineString)
    (rel! LeafGeometry {} MultiLineString)
    (rel! LeafGeometry {} Polygon)
    (rel! LeafGeometry {} MultiPolygon)

    ; Note: Geometry is an extension of LeafGeometry + GeometryCollection
    (rel! Geometry {} Point)
    (rel! Geometry {} MultiPoint)
    (rel! Geometry {} LineString)
    (rel! Geometry {} MultiLineString)
    (rel! Geometry {} Polygon)
    (rel! Geometry {} MultiPolygon)
    (rel! Geometry {} GeometryCollection)

    (type! GeometryOrNull)
    (rel! GeometryOrNull {} Geometry)
    ; TODO: Not supported yet (union tree):
    ; (rel! GeometryOrNull {} null)

    (type! Feature)
    (rel! Feature { 'type' } 'Feature')
    (rel! Feature { 'geometry' } GeometryOrNull)

    ; FIXME: Features is a map of 'anything'.
    ; This could be a good use case for generics in ONTOL.
    (rel! Feature { 'properties' } null)

    (type! FeatureCollection)
    (rel! FeatureCollection { 'type' } 'FeatureCollection')
    (rel! FeatureCollection { 'features'* } Feature)
    "
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
            "invalid type: integer `1`, expected sequence with length 2 at line 1 column 38"
        );
        println!("This test");
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
