use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use serde_json::json;
use test_log::test;

use crate::{assert_error_msg, assert_json_io_matches, util::TypeBinding, TestCompile, TEST_PKG};

#[test]
fn test_entity_experiment_etc() {
    r#"
    (type! artist-id)

    (entity! artist)
    (rel! (artist) name (string))

    (rel! (artist-id) _ "artist/{uuid}")
    ; (rel! (artist-id) uuid (string))

    ; (rel! (artist-id) id! (artist)) ;; ERROR cannot mix named and anonymous relations on the same type

    (entity! record)
    (rel! (record) name (string))
    ; i.e. syntax sugar for:
    ; (rel! (record) name @(unit) []name (string))

    (entity! instrument)
    (rel! (instrument) name (string))

    (type! plays)
    (rel! (plays) how_much (string))

    (rel! (artist) plays[] @(plays) played_by[] (instrument))
    "#
    .compile_ok(|env| {
        let artist = TypeBinding::new(env, "artist");
        let instrument = TypeBinding::new(env, "instrument");

        assert_json_io_matches!(
            artist,
            json!({
                "name": "Zappa",
                "plays": [
                    {
                        "name": "guitar",
                        // FIXME: This should be an optional property:
                        "played_by": [],
                        "_edge": {
                            "how_much": "a lot"
                        }
                    }
                ]
            })
        );

        assert_json_io_matches!(
            instrument,
            json!({
                "name": "guitar",
                "played_by": [
                    {
                        "name": "Zappa",
                        // FIXME: This should be an optional property:
                        "plays": [],
                        "_edge": {
                            "how_much": "a lot"
                        }
                    }
                ]
            })
        );

        assert_error_msg!(
            artist.deserialize_data(json!({
                "name": "Herbie Hancock",
                "plays": [{ "name": "piano" }]
            })),
            r#"missing properties, expected "played_by" and "_edge" at line 1 column 50"#
        );
    });
}

#[test]
#[ignore = "This is not working yet"]
fn test_entity_one_to_one_relationship() {
    r#"
    (entity! a)

    (rel! (a) name (string))
    (rel! (a) parent_of child_of[] (a))
    "#
    .compile_ok(|env| {
        let a = TypeBinding::new(env, "a");
        assert_json_io_matches!(
            a,
            json!({
                "name": "a",
                "parent_of": {
                    "name": "b",
                    "child_of": [],
                },
                // "child_of": [],
            })
        );
    });
}
