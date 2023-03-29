use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches, type_binding::TypeBinding, TestCompile,
};
use pretty_assertions::assert_eq;
use serde_json::json;
use test_log::test;

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../examples/artist_and_instrument.ont");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.ont");

#[test]
fn id_should_not_identify_two_things() {
    "
    type foo
    type bar
    type id {
        rel _ [identifies] foo
        rel _ [identifies] bar // ERROR already identifies another type
    }
    "
    .compile_fail();
}

#[test]
fn artist_and_instrument_io_artist() {
    ARTIST_AND_INSTRUMENT.compile_ok(|env| {
        let artist = TypeBinding::new(&env, "artist");
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
    });
}

#[test]
fn artist_and_instrument_io_instrument() {
    ARTIST_AND_INSTRUMENT.compile_ok(|env| {
        let instrument = TypeBinding::new(&env, "instrument");
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
    });
}

#[test]
fn artist_and_instrument_error_artist() {
    ARTIST_AND_INSTRUMENT.compile_ok(|env| {
        let artist = TypeBinding::new(&env, "artist");
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
fn artist_and_instrument_id_as_relation_object() {
    ARTIST_AND_INSTRUMENT.compile_ok(|env| {
        let artist = TypeBinding::new(&env, "artist");
        let plays = artist.find_property("plays").unwrap();

        let instrument_id = TypeBinding::new(&env, "instrument-id");
        let example_id = "instrument/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8";

        assert_json_io_matches!(
            artist,
            json!({
                "name": "Jimi Hendrix",
                "plays": [
                    {
                        "_id": example_id,
                        "_edge": {
                            "how_much": "all the time"
                        }
                    }
                ]
            })
        );

        let john = artist.deserialize_value(
            json!({
                "name": "John McLaughlin",
                "plays": [
                    {
                        "_id": example_id,
                        "_edge": {
                            "how_much": "much."
                        }
                    }
                ]
        })).unwrap();

        let plays_attributes = john
            .get_attribute_value(plays)
            .unwrap()
            .cast_ref::<Vec<_>>();

        // The value of the `plays` attribute is an `artist-id`
        assert_eq!(
            instrument_id.serialize_json(&plays_attributes[0].value),
            json!(example_id)
        );

        assert_error_msg!(
            artist.deserialize_data(json!({
                "name": "Santana",
                "plays": [
                    {
                        "_id": "junk",
                    }
                ]
            })),
            r#"invalid type: string "junk", expected string matching /\Ainstrument/([0-9A-Fa-f]{8}\-?[0-9A-Fa-f]{4}\-?[0-9A-Fa-f]{4}\-?[0-9A-Fa-f]{4}\-?[0-9A-Fa-f]{12})\z/ at line 1 column 41"#
        );
        assert_error_msg!(
            artist.deserialize_data(json!({
                "name": "Robert Fripp",
                "plays": [{ "_id": example_id }]
            })),
            r#"missing properties, expected "_edge" at line 1 column 89"#
        );

        // The following tests show that { "_id" } and the property map is a type union:
        assert_error_msg!(
            artist.deserialize_data(json!({
                "name": "Tony Levin",
                "plays": [{ "_id": example_id, "name": "Chapman stick" }]
            })),
            r#"unknown property `name` at line 1 column 93"#
        );
        assert_error_msg!(
            artist.deserialize_data(json!({
                "name": "Allan Holdsworth",
                "plays": [{ "name": "Synthaxe", "_id": example_id }]
            })),
            r#"unknown property `name` at line 1 column 99"#
        );
    });
}

#[test]
fn test_entity_self_relationship_optional_object() {
    "
    pub type node_id { rel '' [string] _ }
    pub type node {
        rel node_id [identifies] _
        rel _ ['name'] string
        rel _ ['children'* | 'parent'?] node
    }
    "
    .compile_ok(|env| {
        let node = TypeBinding::new(&env, "node");

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
    pub type node_id { rel '' [string] _ }
    pub type node {
        rel node_id [identifies] _
        rel _ ['children'* | 'parent'] node
    }
    "
    .compile_ok(|env| {
        let node = TypeBinding::new(&env, "node");

        assert_error_msg!(
            node.deserialize_data(json!({})),
            r#"missing properties, expected "parent" at line 1 column 2"#
        );
    });
}

#[test]
fn entity_union_simple() {
    GUITAR_SYNTH_UNION.compile_ok(|env| {
        let instrument = TypeBinding::new(&env, "instrument");

        assert_json_io_matches!(
            instrument,
            json!({
                "type": "synth",
                "polyphony": 8,
            })
        );
    });
}

#[test]
fn entity_union_with_object_relation() {
    GUITAR_SYNTH_UNION.compile_ok(|env| {
        let instrument = TypeBinding::new(&env, "instrument");

        assert_json_io_matches!(
            instrument,
            json!({
                "type": "synth",
                "polyphony": 8,
                "played-by": [
                    {
                        "_id": "some_artist"
                    }
                ]
            })
        );
    });
}

#[test]
fn entity_union_in_relation_with_ids() {
    GUITAR_SYNTH_UNION.compile_ok(|env| {
        let artist = TypeBinding::new(&env, "artist");
        let plays = artist.find_property("plays").unwrap();

        assert!(artist.type_info.entity_info.is_some());

        let guitar_id = TypeBinding::new(&env, "guitar_id");
        let synth_id = TypeBinding::new(&env, "synth_id");

        assert!(guitar_id.type_info.entity_info.is_none());

        let json = json!({
            "name": "Someone",
            "plays": [
                { "_id": "guitar/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" },
                { "_id": "synth/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" }
            ]
        });

        assert_json_io_matches!(artist, json.clone());

        let artist_value = artist.deserialize_value(json.clone()).unwrap();

        let plays_attributes = artist_value
            .get_attribute_value(plays)
            .unwrap()
            .cast_ref::<Vec<_>>();

        let guitar_id_attr = &plays_attributes[0];
        let synth_id_attr = &plays_attributes[1];

        assert_ne!(guitar_id.type_info.def_id, synth_id.type_info.def_id);
        assert_eq!(guitar_id_attr.value.type_def_id, guitar_id.type_info.def_id);
        assert_eq!(synth_id_attr.value.type_def_id, synth_id.type_info.def_id);
    });
}

#[test]
fn entity_relationship_without_reverse() {
    "
    pub type lang_id { rel '' [string] _ }
    pub type prog_id { rel '' [string] _ }
    pub type language { rel lang_id [identifies] _ }
    pub type programmer {
        rel prog_id [identifies] _
        rel _ ['name'] string
        rel _ ['favorite-language'] language
    }
    "
    .compile_ok(|env| {
        let programmer = TypeBinding::new(&env, "programmer");
        assert_json_io_matches!(
            programmer,
            json!({ "name": "audun", "favorite-language": { "_id": "rust" }})
        );
    });
}

#[test]
fn recursive_entity_union() {
    "
    pub type animal_id { rel '' ['animal/'] [string] _ }
    pub type plant_id { rel '' ['plant/'] [string] _ }
    pub type owner_id { rel '' [string] _ }

    pub type lifeform // ERROR entity variants of the union are not uniquely identifiable
    pub type animal {
        rel animal_id [identifies] _
        rel _ ['class'] 'animal'
        rel _ ['eats'*] lifeform
    }
    pub type plant {
        rel plant_id [identifies] _
        rel _ ['class'] 'plant'
    }
    rel () [animal] lifeform
    rel () [plant] lifeform

    pub type owner {
        rel owner_id [identifies] _
        rel _ ['name'] string
        rel _ ['owns'*] lifeform
    }
    "
    .compile_ok(|env| {
        let lifeform = TypeBinding::new(&env, "lifeform");
        assert_json_io_matches!(
            lifeform,
            json!({
                "class": "animal",
                "eats": [
                    { "class": "plant" },
                    { "class": "animal", "eats": [] },
                ]
            })
        );
    });
}
