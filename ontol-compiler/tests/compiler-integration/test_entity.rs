use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use serde_json::json;
use test_log::test;

use crate::{assert_error_msg, assert_json_io_matches, util::TypeBinding, TestCompile, TEST_PKG};

#[test]
fn test_entity_experiment_etc() {
    "
    type artist-id {
        rel '' { 'artist/' } { uuid }
    }
    type instrument-id {
        rel '' { 'instrument/' } { uuid }
    }

    type artist
    rel artist { 'name' } string
    rel artist { id } artist-id

    type record
    rel record { 'name' } string

    type instrument {
        rel { id } instrument-id
        rel { 'name' } string
    }

    type plays
    rel plays { 'how_much' } string

    rel artist { 'plays'* | 'played_by'*: plays } instrument
    "
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

        assert_json_io_matches!(
            artist,
            json!({
                "name": "Jimi Hendrix",
                "plays": [
                    {
                        "_id": "instrument/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
                        "_edge": {
                            "how_much": "all the time"
                        }
                    }
                ]
            })
        );
    });
}

#[test]
fn test_entity_self_relationship() {
    "
    type node {
        rel { id } string
        rel { 'name' } string
        rel { 'children'* | 'parent'? } node
    }
    "
    .compile_ok(|env| {
        let node = TypeBinding::new(env, "node");

        assert_error_msg!(
            node.deserialize_data(json!({})),
            r#"missing properties, expected "name" at line 1 column 2"#
        );

        assert_json_io_matches!(node, json!({ "name": "a" }));

        assert_json_io_matches!(
            node,
            json!({
                "name": "a",
                "children": [
                    {
                        "name": "b",
                    }
                ]
            })
        );

        assert_json_io_matches!(
            node,
            json!({
                "name": "b",
                "parent": {
                    "name": "a",
                },
                "children": [
                    {
                        "name": "c",
                    }
                ]
            })
        );
    });
}

#[test]
fn test_entity_self_relationship_mandatory_object() {
    "
    type node {
        rel { id } string
        rel { 'children'* | 'parent' } node
    }
    "
    .compile_ok(|env| {
        let node = TypeBinding::new(env, "node");

        assert_error_msg!(
            node.deserialize_data(json!({})),
            r#"missing properties, expected "parent" at line 1 column 2"#
        );
    });
}
