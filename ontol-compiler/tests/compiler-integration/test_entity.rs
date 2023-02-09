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
    ; (rel! (record) name @(unit) name[] (string))

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
            r#"missing properties, expected "_edge" at line 1 column 50"#
        );
    });
}

#[test]
fn test_entity_self_relationship() {
    r#"
    (entity! node)

    (rel! (node) name (string))
    (rel! (node) children[] parent? (node))
    "#
    .compile_ok(|env| {
        let node = TypeBinding::new(env, "node");
        assert_json_io_matches!(
            node,
            json!({
                "name": "a",
                "parent": {
                    "name": "b",
                },
            })
        );

        assert_json_io_matches!(
            node,
            json!({
                "name": "b",
                "children": [
                    {
                        "name": "a",
                    }
                ]
            })
        );
    });
}
