use ontol_runtime::value::Value;
use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches, expect_eq, type_binding::*, TestCompile,
};
use serde_json::json;
use test_log::test;

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../examples/artist_and_instrument.on");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../examples/guitar_synth_union.on");

#[test]
fn id_cannot_identify_two_things() {
    "
    type foo
    type bar
    type id {
        rel .identifies: foo
        rel .identifies: bar // ERROR already identifies a type
    }
    "
    .compile_fail();
}

#[test]
fn entity_without_inherent_id() {
    "
    pub type some_id { fmt '' => string => . }
    pub type entity {
        rel .id: some_id
        rel .'foo': string
    }
    "
    .compile_ok(|env| {
        let entity = TypeBinding::new(&env, "entity");
        assert_json_io_matches!(entity, Create, { "foo": "foo" });
    });
}

#[test]
fn inherent_id_no_autogen() {
    "
    type foo_id { rel .is: string }
    pub type foo {
        rel .id: foo_id
        rel .'key': foo_id
        rel .'children': [foo]
    }
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(&env, "foo");
        assert_json_io_matches!(foo, Create, { "key": "id", "children": [{ "key": "foreign_id" }] });

        let entity: Value = foo.entity_builder(json!("id"), json!({ "key": "id" })).into();
        expect_eq!(
            actual = read_ser(&foo).json(&entity),
            expected = json!({ "key": "id" }),
        );
    });
}

#[test]
fn inherent_id_autogen() {
    "
    type foo_id { rel .is: string }
    pub type foo {
        rel .'key'(rel .gen: auto)|id: foo_id
        rel .'children': [foo]
    }
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(&env, "foo");
        assert_json_io_matches!(foo, Create, { "children": [{ "key": "foreign_id" }] });

        let entity: Value = foo.entity_builder(json!("generated_id"), json!({})).into();
        expect_eq!(
            actual = read_ser(&foo).json(&entity),
            expected = json!({ "key": "generated_id" }),
        );
    });
}

#[test]
fn id_and_inherent_property_inline_type() {
    "
    pub type foo {
        rel .'key'|id: { rel . is: string }
        rel .'children': [foo]
    }
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(&env, "foo");
        assert_json_io_matches!(foo, Create, {
            "key": "outer",
            "children": [{ "key": "inner" }]
        });
        // Since there is no `.rel gen: auto` for the id, it is required:
        assert_error_msg!(
            create_de(&foo).data(json!({
                "children": [{ "key": "inner" }]
            })),
            r#"missing properties, expected "key" at line 1 column 30"#
        );
    });
}

#[test]
fn entity_id_inline_fmt() {
    "
    pub type foo {
        rel .'key'|id: { fmt '' => 'foo/' => uuid => . }
    }
    "
    .compile_ok(|_| {});
}

#[test]
fn artist_and_instrument_io_artist() {
    ARTIST_AND_INSTRUMENT.compile_ok(|env| {
        let artist = TypeBinding::new(&env, "artist");
        assert_json_io_matches!(artist, Create, {
            "name": "Zappa",
            "plays": [
                {
                    "name": "guitar",
                    "_edge": {
                        "how_much": "a lot"
                    }
                }
            ]
        });
    });
}

#[test]
fn artist_and_instrument_io_instrument() {
    ARTIST_AND_INSTRUMENT.compile_ok(|env| {
        let instrument = TypeBinding::new(&env, "instrument");
        assert_json_io_matches!(instrument, Create, {
            "name": "guitar",
            "played_by": [
                {
                    "name": "Zappa",
                    "_edge": {
                        "how_much": "a lot"
                    }
                }
            ]
        });
    });
}

#[test]
fn artist_and_instrument_error_artist() {
    ARTIST_AND_INSTRUMENT.compile_ok(|env| {
        let artist = TypeBinding::new(&env, "artist");
        assert_error_msg!(
            create_de(&artist).data(json!({
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
        let [artist, instrument_id] = TypeBinding::new_n(&env, ["artist", "instrument-id"]);
        let plays = artist.find_property("plays").unwrap();
        let example_id = "instrument/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8";

        assert_json_io_matches!(artist, Create, {
            "name": "Jimi Hendrix",
            "plays": [
                {
                    "ID": example_id,
                    "_edge": {
                        "how_much": "all the time"
                    }
                }
            ]
        });

        let john = create_de(&artist).value(
            json!({
                "name": "John McLaughlin",
                "plays": [
                    {
                        "ID": example_id,
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
        expect_eq!(
            actual = create_ser(&instrument_id).json(&plays_attributes[0].value),
            expected = json!(example_id)
        );

        assert_error_msg!(
            create_de(&artist).data(json!({
                "name": "Santana",
                "plays": [
                    {
                        "ID": "junk",
                    }
                ]
            })),
            r#"invalid type: string "junk", expected string matching /\Ainstrument/([0-9A-Fa-f]{32}|[0-9A-Fa-f]{8}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{12})\z/ at line 1 column 40"#
        );
        assert_error_msg!(
            create_de(&artist).data(json!({
                "name": "Robert Fripp",
                "plays": [{ "ID": example_id }]
            })),
            r#"missing properties, expected "_edge" at line 1 column 88"#
        );

        // The following tests show that { "ID" } and the property map is a type union:
        assert_error_msg!(
            create_de(&artist).data(json!({
                "name": "Tony Levin",
                "plays": [{ "ID": example_id, "name": "Chapman stick" }]
            })),
            r#"unknown property `name` at line 1 column 92"#
        );
        assert_error_msg!(
            create_de(&artist).data(json!({
                "name": "Allan Holdsworth",
                "plays": [{ "name": "Synthaxe", "ID": example_id }]
            })),
            r#"unknown property `name` at line 1 column 98"#
        );
    });
}

#[test]
fn test_entity_self_relationship_optional_object() {
    "
    pub type node_id { fmt '' => string => . }
    pub type node {
        rel node_id identifies: .
        rel .'name': string
        rel .'children'::'parent'? [node]
    }
    "
    .compile_ok(|env| {
        let node = TypeBinding::new(&env, "node");

        assert_error_msg!(
            create_de(&node).data(json!({})),
            r#"missing properties, expected "name" at line 1 column 2"#
        );

        assert_json_io_matches!(node, Create, { "name": "a" });

        assert_json_io_matches!(node, Create, {
            "name": "a",
            "children": [{
                "name": "b",
            }]
        });

        assert_json_io_matches!(node, Create, {
            "name": "b",
            "parent": {
                "name": "a",
            },
            "children": [{
                "name": "c",
            }]
        });
    });
}

#[test]
fn test_entity_self_relationship_mandatory_object() {
    "
    pub type node_id { fmt '' => string => . }
    pub type node {
        rel node_id identifies: .
        rel .'children'::'parent' [.]
    }
    "
    .compile_ok(|env| {
        let node = TypeBinding::new(&env, "node");
        assert_error_msg!(
            create_de(&node).data(json!({})),
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
            Create,
            {
                "type": "synth",
                "polyphony": 8,
            }
        );
    });
}

#[test]
fn entity_union_with_object_relation() {
    GUITAR_SYNTH_UNION.compile_ok(|env| {
        let instrument = TypeBinding::new(&env, "instrument");

        assert_json_io_matches!(instrument, Create, {
            "type": "synth",
            "polyphony": 8,
            "played-by": [{
                "artist-id": "some_artist"
            }]
        });
    });
}

#[test]
fn entity_union_in_relation_with_ids() {
    GUITAR_SYNTH_UNION.compile_ok(|env| {
        let [artist, guitar_id, synth_id] =
            TypeBinding::new_n(&env, ["artist", "guitar_id", "synth_id"]);
        let plays = artist.find_property("plays").unwrap();

        assert!(artist.type_info.entity_info.is_some());
        assert!(guitar_id.type_info.entity_info.is_none());

        let json = json!({
            "name": "Someone",
            "plays": [
                { "instrument-id": "guitar/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" },
                { "instrument-id": "synth/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" }
            ]
        });

        assert_json_io_matches!(artist, Create, {
            "name": "Someone",
            "plays": [
                { "instrument-id": "guitar/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" },
                { "instrument-id": "synth/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" }
            ]
        });

        let artist_value = create_de(&artist).value(json.clone()).unwrap();

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
    pub type lang_id { fmt '' => string => . }
    pub type prog_id { fmt '' => string => . }
    pub type language {
        rel lang_id identifies: .
        rel .'lang-id': lang_id
    }
    pub type programmer {
        rel prog_id identifies: .
        rel .'name': string
        rel .'favorite-language': language
    }
    "
    .compile_ok(|env| {
        let programmer = TypeBinding::new(&env, "programmer");
        assert_json_io_matches!(programmer, Create, {
            "name": "audun",
            "favorite-language": { "lang-id": "rust" }
        });
    });
}

#[test]
fn recursive_entity_union() {
    "
    pub type animal_id { fmt '' => 'animal/' => string => . }
    pub type plant_id { fmt '' => 'plant/' => string => . }
    pub type owner_id { fmt '' => string => . }

    pub type lifeform
    pub type animal {
        rel animal_id identifies: .
        rel .'class': 'animal'
        rel .'eats': [lifeform]
    }
    pub type plant {
        rel plant_id identifies: .
        rel .'class': 'plant'
    }
    rel lifeform is?: animal
    rel lifeform is?: plant

    pub type owner {
        rel owner_id identifies: .
        rel .'name': string
        rel .'owns': [lifeform]
    }
    "
    .compile_ok(|env| {
        let lifeform = TypeBinding::new(&env, "lifeform");
        assert_json_io_matches!(lifeform, Create, {
            "class": "animal",
            "eats": [
                { "class": "plant" },
                { "class": "animal", "eats": [] },
            ]
        });
    });
}
