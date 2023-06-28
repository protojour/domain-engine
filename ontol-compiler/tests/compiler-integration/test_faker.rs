use ontol_runtime::serde::processor::ProcessorMode;
use ontol_test_utils::{expect_eq, type_binding::TypeBinding, TestCompile};
use serde_json::json;
use test_log::test;

const GEOJSON: &str = include_str!("../../../examples/geojson.on");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.on");

#[test]
fn test_fake_primitives() {
    "
    pub type foo {
        rel .'s': string
        rel .'i': int
    }
    "
    .compile_ok(|test_env| {
        let foo = TypeBinding::new(&test_env, "foo");

        expect_eq!(
            actual = foo.ser_read().json(&foo.new_fake(ProcessorMode::Read)),
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
    .compile_ok(|test_env| {
        let foo = TypeBinding::new(&test_env, "foo");

        expect_eq!(
            actual = foo.ser_read().json(&foo.new_fake(ProcessorMode::Read)),
            expected = json!({
                "id": "042da2de-98c0-64cf-94c2-5463ca1c3fbe",
                "created_at": "1943-07-25T19:00:15.149284864+00:00",
            }),
        );
    });
}

#[test]
fn test_fake_geojson() {
    GEOJSON.compile_ok(|test_env| {
        let geometry = TypeBinding::new(&test_env, "Geometry");

        expect_eq!(
            actual = geometry
                .ser_read()
                .json(&geometry.new_fake(ProcessorMode::Read)),
            expected = json!({
                "type": "Polygon",
                "coordinates": [
                    [-1732221745, 70099678],
                    [-1214996992, -194249765],
                    [2117826670, -1428302769],
                    [-140280828, -2108307479],
                    [428606290, 1128193060]
                ]
            }),
        );
    });
}

#[test]
fn test_fake_guitar_synth() {
    GUITAR_SYNTH_UNION.compile_ok(|test_env| {
        let artist = TypeBinding::new(&test_env, "artist");

        expect_eq!(
            actual = artist
                .ser_read()
                .json(&artist.new_fake(ProcessorMode::Read)),
            expected = json!({
                "artist-id": "mollitia sit porro tenetur",
                "name": "delectus molestias aspernatur voluptatem reprehenderit",
                "plays": [
                    {
                        "instrument-id": "guitar/9fe5a078-ccaa-f029-bccc-b1436ec3ffee",
                        "type": "guitar",
                        "string_count": -245468183,
                        "played-by": [
                            {
                                "artist-id": "amet consectetur tempore",
                                "name": "eaque omnis neque repellat nam",
                                "plays": [
                                    {
                                        "instrument-id": "guitar/7165ee7a-5fde-9f8b-badd-ab0efe6cb2c5",
                                        "type": "guitar",
                                        "string_count": 270115708,
                                        "played-by": [
                                            {
                                                "artist-id": "optio alias ipsa ut et",
                                                "name": "nemo aut pariatur quia",
                                                "plays": [
                                                    {
                                                        "instrument-id": "synth/3d58b86a-fcad-849f-ab0c-ebf52f2badd2",
                                                        "type": "synth",
                                                        "polyphony": 1898028637,
                                                        // The faker recursion limit kicked in:
                                                        "played-by": []
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }),
        );
    });
}
