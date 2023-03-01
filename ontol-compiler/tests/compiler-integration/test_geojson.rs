use crate::{
    assert_error_msg, assert_json_io_matches, util::type_binding::TypeBinding, SourceName,
    TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;

pub const GEOJSON: &'static str = "
// TODO: domain-private types?
type position
rel position { 0..2 } int

type position-list
rel position-list { ..2 } position
rel position-list { 2.. } position

type position-ring
rel position-ring { ..4 } position
rel position-ring { 4.. } position

type Geometry
type LeafGeometry

type Point
rel Point { 'type' } 'Point'
rel Point { 'coordinates' } position

type MultiPoint
rel MultiPoint { 'type' } 'MultiPoint'
rel MultiPoint { 'coordinates'* } position

type LineString
rel LineString { 'type' } 'LineString'
rel LineString { 'coordinates' } position-list

type MultiLineString
rel MultiLineString { 'type' } 'MultiLineString'
rel MultiLineString { 'coordinates'* } position-list

type Polygon {
    rel { 'type' } 'Polygon'
    rel { 'coordinates' } position-ring
}

type MultiPolygon {
    rel { 'type' } 'MultiPolygon'
    rel { 'coordinates'* } position-ring
}

type GeometryCollection
rel GeometryCollection { 'type' } 'GeometryCollection'
rel GeometryCollection { 'geometries'* } LeafGeometry

rel . { Point } LeafGeometry
rel . { MultiPoint } LeafGeometry
rel . { LineString } LeafGeometry
rel . { MultiLineString } LeafGeometry
rel . { Polygon } LeafGeometry
rel . { MultiPolygon } LeafGeometry

// Note: Geometry is an extension of LeafGeometry + GeometryCollection
rel . { Point } Geometry
rel . { MultiPoint } Geometry
rel . { LineString } Geometry
rel . { MultiLineString } Geometry
rel . { Polygon } Geometry
rel . { MultiPolygon } Geometry
rel . { GeometryCollection } Geometry

type GeometryOrNull
rel . { Geometry } GeometryOrNull
// TODO: Not supported yet (union tree):
// rel . { . } GeometryOrNull

type Feature
rel Feature { 'type' } 'Feature'
rel Feature { 'geometry' } GeometryOrNull

// FIXME: Features is a map of 'anything'.
// This could be a good use case for generics in ONTOL.
rel Feature { 'properties' } .

type FeatureCollection
rel FeatureCollection { 'type' } 'FeatureCollection'
rel FeatureCollection { 'features'* } Feature
";

#[test]
fn test_geojson() {
    GEOJSON.compile_ok(|env| {
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

#[test]
fn test_municipalities() {
    TestPackages::with_sources([
        (SourceName("geojson"), GEOJSON),
        (
            SourceName::root(),
            "
            use 'geojson' as geojson

            type kommunenummer {
                rel '' { /[0-9]{4}/ }
            }

            type kommune {
                rel { 'kommunenummer' } kommunenummer
                rel { 'geometry' } geojson.Polygon
            }
            ",
        ),
    ])
    .compile_ok(|env| {});
}
