use ontol_runtime::value::Value;
use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches,
    examples::{ARTIST_AND_INSTRUMENT, GUITAR_SYNTH_UNION},
    expect_eq,
    serde_helper::*,
    TestCompile,
};
use serde_json::json;
use test_log::test;

#[test]
fn id_cannot_identify_two_things() {
    "
    def foo {}
    def bar {}
    def id {
        rel .identifies: foo
        rel .identifies: bar // ERROR already identifies a type
    }
    "
    .compile_fail();
}

#[test]
fn entity_without_inherent_id() {
    "
    def(pub) some_id { fmt '' => text => . }
    def(pub) entity {
        rel .id: some_id
        rel .'foo': text
    }
    "
    .compile_then(|test| {
        let [entity] = test.bind(["entity"]);
        assert_json_io_matches!(entity, Create, { "foo": "foo" });
    });
}

#[test]
fn inherent_id_no_autogen() {
    "
    def foo_id { rel .is: text }
    def(pub) foo {
        rel .id: foo_id
        rel .'key': foo_id
        rel .'children': [foo]
    }
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, { "key": "id", "children": [{ "key": "foreign_id" }] });

        let entity: Value = foo.entity_builder(json!("id"), json!({ "key": "id" })).into();
        expect_eq!(
            actual = serde_read(&foo).as_json(&entity),
            expected = json!({ "key": "id" }),
        );
    });
}

#[test]
fn inherent_id_autogen() {
    "
    def foo_id { rel .is: text }
    def(pub) foo {
        rel .'key'(rel .gen: auto)|id: foo_id
        rel .'children': [foo]
    }
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, { "children": [{ "key": "foreign_id" }] });

        let entity: Value = foo.entity_builder(json!("generated_id"), json!({})).into();
        expect_eq!(
            actual = serde_read(&foo).as_json(&entity),
            expected = json!({ "key": "generated_id" }),
        );
    });
}

#[test]
fn id_and_inherent_property_inline_type() {
    "
    def(pub) foo {
        rel .'key'|id: { rel . is: text }
        rel .'children': [foo]
    }
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, {
            "key": "outer",
            "children": [{ "key": "inner" }]
        });
        // Since there is no `.rel gen: auto` for the id, it is required:
        assert_error_msg!(
            serde_create(&foo).to_data(json!({
                "children": [{ "key": "inner" }]
            })),
            r#"missing properties, expected "key" at line 1 column 30"#
        );
    });
}

#[test]
fn entity_id_inline_fmt() {
    "
    def(pub) foo {
        rel .'key'|id: { fmt '' => 'foo/' => uuid => . }
    }
    "
    .compile();
}

#[test]
fn artist_and_instrument_io_artist() {
    let test = ARTIST_AND_INSTRUMENT.1.compile();
    let [artist] = test.bind(["artist"]);
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
}

#[test]
fn artist_and_instrument_io_instrument() {
    let test = ARTIST_AND_INSTRUMENT.1.compile();
    let [instrument] = test.bind(["instrument"]);
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
}

#[test]
fn artist_and_instrument_error_artist() {
    let test = ARTIST_AND_INSTRUMENT.1.compile();
    let [artist] = test.bind(["artist"]);
    assert_error_msg!(
        serde_create(&artist).to_data(json!({
            "name": "Herbie Hancock",
            "plays": [{ "name": "piano" }]
        })),
        r#"missing properties, expected "_edge" at line 1 column 50"#
    );
}

#[test]
fn artist_and_instrument_id_as_relation_object() {
    let test = ARTIST_AND_INSTRUMENT.1.compile();
    let [artist, instrument_id] = test.bind(["artist", "instrument-id"]);
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

    let john = serde_create(&artist)
        .to_value(json!({
                "name": "John McLaughlin",
                "plays": [
                    {
                        "ID": example_id,
                        "_edge": {
                            "how_much": "much."
                        }
                    }
                ]
        }))
        .unwrap();

    let plays_attributes = john
        .get_attribute_value(plays)
        .unwrap()
        .cast_ref::<Vec<_>>();

    // The value of the `plays` attribute is an `artist-id`
    expect_eq!(
        actual = serde_create(&instrument_id).as_json(&plays_attributes[0].value),
        expected = json!(example_id)
    );

    assert_error_msg!(
        serde_create(&artist).to_data(json!({
            "name": "Santana",
            "plays": [
                {
                    "ID": "junk",
                }
            ]
        })),
        r#"invalid type: string "junk", expected string matching /(?:\A(?:instrument/)((?:[0-9A-Fa-f]{32}|(?:[0-9A-Fa-f]{8}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{12})))\z)/ at line 1 column 40"#
    );
    assert_error_msg!(
        serde_create(&artist).to_data(json!({
            "name": "Robert Fripp",
            "plays": [{ "ID": example_id }]
        })),
        r#"missing properties, expected "_edge" at line 1 column 88"#
    );

    // The following tests show that { "ID" } and the property map is a type union:
    assert_error_msg!(
        serde_create(&artist).to_data(json!({
            "name": "Tony Levin",
            "plays": [{ "ID": example_id, "name": "Chapman stick" }]
        })),
        r#"unknown property `name` at line 1 column 92"#
    );
    assert_error_msg!(
        serde_create(&artist).to_data(json!({
            "name": "Allan Holdsworth",
            "plays": [{ "name": "Synthaxe", "ID": example_id }]
        })),
        r#"unknown property `name` at line 1 column 98"#
    );
}

#[test]
fn test_entity_self_relationship_optional_object() {
    "
    def(pub) node_id { fmt '' => text => . }
    def(pub) node {
        rel node_id identifies: .
        rel .'name': text
        rel .'children'::'parent'? [node]
    }
    "
    .compile_then(|test| {
        let [node] = test.bind(["node"]);
        assert_error_msg!(
            serde_create(&node).to_data(json!({})),
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
    def(pub) node_id { fmt '' => text => . }
    def(pub) node {
        rel node_id identifies: .
        rel .'children'::'parent' [.]
    }
    "
    .compile_then(|test| {
        let [node] = test.bind(["node"]);
        assert_error_msg!(
            serde_create(&node).to_data(json!({})),
            r#"missing properties, expected "parent" at line 1 column 2"#
        );
    });
}

#[test]
fn entity_union_simple() {
    let test = GUITAR_SYNTH_UNION.1.compile();
    let [instrument] = test.bind(["instrument"]);
    assert_json_io_matches!(
        instrument,
        Create,
        {
            "type": "synth",
            "polyphony": 8,
        }
    );
}

#[test]
fn entity_union_with_object_relation() {
    let test = GUITAR_SYNTH_UNION.1.compile();
    let [instrument] = test.bind(["instrument"]);
    assert_json_io_matches!(instrument, Create, {
        "type": "synth",
        "polyphony": 8,
        "played-by": [{
            "artist-id": "some_artist"
        }]
    });
}

#[test]
fn entity_union_in_relation_with_ids() {
    let test = GUITAR_SYNTH_UNION.1.compile();
    let [artist, guitar_id, synth_id] = test.bind(["artist", "guitar_id", "synth_id"]);
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

    let artist_value = serde_create(&artist).to_value(json.clone()).unwrap();

    let plays_attributes = artist_value
        .get_attribute_value(plays)
        .unwrap()
        .cast_ref::<Vec<_>>();

    let guitar_id_attr = &plays_attributes[0];
    let synth_id_attr = &plays_attributes[1];

    assert_ne!(guitar_id.type_info.def_id, synth_id.type_info.def_id);
    assert_eq!(guitar_id_attr.value.type_def_id, guitar_id.type_info.def_id);
    assert_eq!(synth_id_attr.value.type_def_id, synth_id.type_info.def_id);
}

#[test]
fn entity_relationship_without_reverse() {
    "
    def(pub) lang_id { fmt '' => text => . }
    def(pub) prog_id { fmt '' => text => . }
    def(pub) language {
        rel lang_id identifies: .
        rel .'lang-id': lang_id
    }
    def(pub) programmer {
        rel prog_id identifies: .
        rel .'name': text
        rel .'favorite-language': language
    }
    "
    .compile_then(|test| {
        let [programmer] = test.bind(["programmer"]);
        assert_json_io_matches!(programmer, Create, {
            "name": "audun",
            "favorite-language": { "lang-id": "rust" }
        });
    });
}

#[test]
fn recursive_entity_union() {
    "
    def(pub) animal_id { fmt '' => 'animal/' => text => . }
    def(pub) plant_id { fmt '' => 'plant/' => text => . }
    def(pub) owner_id { fmt '' => text => . }

    def(pub) lifeform {}
    def(pub) animal {
        rel animal_id identifies: .
        rel .'class': 'animal'
        rel .'eats': [lifeform]
    }
    def(pub) plant {
        rel plant_id identifies: .
        rel .'class': 'plant'
    }
    rel lifeform is?: animal
    rel lifeform is?: plant

    def(pub) owner {
        rel owner_id identifies: .
        rel .'name': text
        rel .'owns': [lifeform]
    }
    "
    .compile_then(|test| {
        let [lifeform] = test.bind(["lifeform"]);
        assert_json_io_matches!(lifeform, Create, {
            "class": "animal",
            "eats": [
                { "class": "plant" },
                { "class": "animal", "eats": [] },
            ]
        });
    });
}
