use ontol_macros::test;
use ontol_runtime::{attr::AttrRef, value::Value, PackageId};
use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches,
    examples::{ARTIST_AND_INSTRUMENT, GUITAR_SYNTH_UNION},
    expect_eq,
    serde_helper::*,
    TestCompile,
};
use serde_json::json;

#[test]
fn id_cannot_identify_two_things() {
    "
    def foo (rel .id: my_id)
    def bar (
        rel .id: my_id // ERROR already identifies a type
    )
    def my_id (rel .is: text)
    "
    .compile_fail();
}

#[test]
fn entity_without_inherent_id() {
    "
    def some_id (fmt '' => text => .)
    def entity (
        rel .id: some_id
        rel .'foo': text
    )
    "
    .compile_then(|test| {
        let [entity] = test.bind(["entity"]);
        assert_json_io_matches!(serde_create(&entity), { "foo": "foo" });
    });
}

#[test]
fn inherent_id_no_autogen() {
    "
    def foo_id (rel .is: text)
    def foo (
        rel .id: foo_id
        rel .'key': foo_id
        rel .'children': {foo}
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "key": "id", "children": [{ "key": "foreign_id" }] });

        let entity: Value = foo.entity_builder(json!("id"), json!({ "key": "id" })).into();
        expect_eq!(
            actual = serde_read(&foo).as_json(AttrRef::Unit(&entity)),
            expected = json!({ "key": "id" }),
        );
    });
}

#[test]
fn inherent_id_autogen() {
    "
    def foo_id (rel .is: text)
    def foo (
        rel .'key'[rel .gen: auto]|id: foo_id
        rel .'children': {foo}
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "children": [{ "key": "foreign_id" }] });

        let entity: Value = foo.entity_builder(json!("generated_id"), json!({})).into();
        expect_eq!(
            actual = serde_read(&foo).as_json(AttrRef::Unit(&entity)),
            expected = json!({ "key": "generated_id" }),
        );
    });
}

#[test]
fn id_and_inherent_property_inline_type() {
    "
    def foo (
        rel .'key'|id: (rel . is: text)
        rel .'children': {foo}
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), {
            "key": "outer",
            "children": [{ "key": "inner" }]
        });
        // Since there is no `.rel gen: auto` for the id, it is required:
        assert_error_msg!(
            serde_create(&foo).to_value(json!({
                "children": [{ "key": "inner" }]
            })),
            r#"missing properties, expected "key" at line 1 column 30"#
        );
    });
}

#[test]
fn entity_id_inline_fmt_uuid() {
    "
    def foo (
        rel .'key'|id: ( fmt '' => 'foo/' => uuid => . )
    )
    "
    .compile();
}

#[test]
fn entity_id_inline_fmt_serial() {
    "
    def foo (
        rel .'key'|id: ( fmt '' => 'foo/' => serial => . )
    )
    "
    .compile();
}

#[test]
fn artist_and_instrument_io_artist() {
    let test = ARTIST_AND_INSTRUMENT.1.compile();
    let [artist] = test.bind(["artist"]);
    assert_json_io_matches!(serde_create(&artist), {
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
    assert_json_io_matches!(serde_create(&instrument), {
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
        serde_create(&artist).to_value(json!({
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

    assert_json_io_matches!(serde_create(&artist), {
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
        .to_value_nocheck(json!({
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

    let plays_attributes = john.get_attribute(plays).unwrap().as_matrix().unwrap();
    // .cast_ref::<Vec<_>>();

    // The value of the `plays` attribute is an `artist-id`
    expect_eq!(
        actual = serde_create(&instrument_id).as_json(plays_attributes.get_ref(0, 0).unwrap()),
        expected = json!(example_id)
    );

    assert_error_msg!(
        serde_create(&artist).to_value(json!({
            "name": "Robert Fripp",
            "plays": [{ "ID": example_id }]
        })),
        r#"missing properties, expected "_edge" at line 1 column 88"#
    );

    // The following tests show that { "ID" } and the property map is a type union:
    assert_error_msg!(
        serde_create(&artist).to_value(json!({
            "name": "Tony Levin",
            "plays": [{ "ID": example_id, "name": "Chapman stick" }]
        })),
        r#"unknown property `name` at line 1 column 92"#
    );
    assert_error_msg!(
        serde_create(&artist).to_value(json!({
            "name": "Allan Holdsworth",
            "plays": [{ "name": "Synthaxe", "ID": example_id }]
        })),
        r#"unknown property `name` at line 1 column 98"#
    );
}

#[test]
fn artist_and_instrument_id_as_relation_object_invalid_id_format() {
    let test = ARTIST_AND_INSTRUMENT.1.compile();
    let [artist] = test.bind(["artist"]);

    assert_error_msg!(
        serde_create(&artist).to_value(json!({
            "name": "Santana",
            "plays": [
                {
                    "ID": "junk",
                }
            ]
        })),
        r#"invalid type: string "junk", expected string matching /(?:\A(?:instrument/)((?:[0-9A-Fa-f]{32}|(?:[0-9A-Fa-f]{8}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{12})))\z)/ at line 1 column 40"#
    );
}

#[test]
fn test_entity_self_relationship_optional_object() {
    "
    def node_id (fmt '' => text => .)
    def node (
        rel .id: node_id
        rel .'name': text
        rel .'children'::'parent'? {node}
    )
    "
    .compile_then(|test| {
        let [node] = test.bind(["node"]);
        assert_error_msg!(
            serde_create(&node).to_value(json!({})),
            r#"missing properties, expected "name" at line 1 column 2"#
        );

        assert_json_io_matches!(serde_create(&node), { "name": "a" });

        assert_json_io_matches!(serde_create(&node), {
            "name": "a",
            "children": [{
                "name": "b",
            }]
        });

        assert_json_io_matches!(serde_create(&node), {
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
    def node_id (fmt '' => text => .)
    def node (
        rel .id: node_id
        rel .'children'::'parent' {.}
    )
    "
    .compile_then(|test| {
        let [node] = test.bind(["node"]);
        assert_error_msg!(
            serde_create(&node).to_value(json!({})),
            r#"missing properties, expected "parent" at line 1 column 2"#
        );
    });
}

#[test]
fn entity_union_simple() {
    let test = GUITAR_SYNTH_UNION.1.compile();
    let [instrument] = test.bind(["instrument"]);
    assert_json_io_matches!(
        serde_create(&instrument),
        {
            "type": "synth",
            "polyphony": 8,
        }
    );
    assert_json_io_matches!(
        serde_read(&instrument),
        {
            "instrument-id": "synth/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
            "type": "synth",
            "polyphony": 8,
        }
    );
}

#[test]
fn entity_union_with_union_def_id_larger_than_id() {
    "
    def Repository (
        rel .'id'[rel .gen: auto]|id: (rel .is: uuid)
        rel {.}'owner'::'repositories' RepositoryOwner
    )

    def org_id (fmt '' => 'org/' => text => .)

    def Organization (
        rel .'id'|id: org_id
    )

    def RepositoryOwner (
        rel .is?: Organization
    )
    "
    .compile();
}

#[test]
fn entity_union_with_object_relation() {
    let test = GUITAR_SYNTH_UNION.1.compile();
    let [instrument] = test.bind(["instrument"]);
    assert_json_io_matches!(serde_create(&instrument), {
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

    assert!(artist.type_info.entity_info().is_some());
    assert!(guitar_id.type_info.entity_info().is_none());

    let json = json!({
        "name": "Someone",
        "plays": [
            { "instrument-id": "guitar/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" },
            { "instrument-id": "synth/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" }
        ]
    });

    assert_json_io_matches!(serde_create(&artist), {
        "name": "Someone",
        "plays": [
            { "instrument-id": "guitar/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" },
            { "instrument-id": "synth/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" }
        ]
    });

    let artist_value = serde_create(&artist)
        .to_value_nocheck(json.clone())
        .unwrap();

    let plays_attributes = artist_value
        .get_attribute(plays)
        .unwrap()
        .as_matrix()
        .unwrap();

    let guitar_id_attr = plays_attributes.get_row_cloned(0).unwrap();
    let synth_id_attr = plays_attributes.get_row_cloned(1).unwrap();

    assert_ne!(guitar_id.type_info.def_id, synth_id.type_info.def_id);
    assert_eq!(
        guitar_id_attr.as_unit().unwrap().type_def_id(),
        guitar_id.type_info.def_id
    );
    assert_eq!(
        synth_id_attr.as_unit().unwrap().type_def_id(),
        synth_id.type_info.def_id
    );
}

#[test]
fn entity_relationship_without_reverse() {
    "
    def lang_id (fmt '' => text => .)
    def prog_id (fmt '' => text => .)
    def language (
        rel .id: lang_id
        rel .'lang-id': lang_id
    )
    def programmer (
        rel .id: prog_id
        rel .'name': text
        rel .'favorite-language': language
    )
    "
    .compile_then(|test| {
        let [programmer] = test.bind(["programmer"]);
        assert_json_io_matches!(serde_create(&programmer), {
            "name": "audun",
            "favorite-language": { "lang-id": "rust" }
        });
    });
}

#[test]
fn recursive_entity_union() {
    "
    def animal_id (fmt '' => 'animal/' => text => .)
    def plant_id (fmt '' => 'plant/' => text => .)
    def owner_id (fmt '' => text => .)

    def lifeform ()
    def animal (
        rel .id: animal_id
        rel .'class': 'animal'
        rel .'eats': {lifeform}
    )
    def plant (
        rel .id: plant_id
        rel .'class': 'plant'
    )
    rel lifeform is?: animal
    rel lifeform is?: plant

    def owner (
        rel .id: owner_id
        rel .'name': text
        rel .'owns': {lifeform}
    )
    "
    .compile_then(|test| {
        let [lifeform] = test.bind(["lifeform"]);
        assert_json_io_matches!(serde_create(&lifeform), {
            "class": "animal",
            "eats": [
                { "class": "plant" },
                { "class": "animal", "eats": [] },
            ]
        });
    });
}

#[test]
fn serial_gen_auto() {
    "
    def foo (
        rel .'id'[rel .gen: auto]|id: (fmt '' => 'prefix/' => serial => .)
    )
    "
    .compile();
}

#[test]
fn entity_order_ok() {
    "
    def foo (
        rel .'id'|id: (rel .is: text)
        rel .'name': text
        rel .order[
            rel .0: 'name'
            rel .direction: descending
        ]: by_name
    )
    def @symbol by_name ()
    "
    .compile();
}

#[test]
fn store_key_in_type_info() {
    "
    def foobar_edge (
        rel .store_key: 'fubar'
    )

    def foo (
        rel .'id'|id: (rel .is: text)
        rel .store_key: 'fu'
        rel .'bar'[rel .is: foobar_edge]: bar
    )

    def bar (
        rel .'id'|id: (rel .is: text)
        rel .'foo'[rel .store_key: 'baaah']: bar
    )
    "
    .compile_then(|test| {
        let ontology = test.ontology();
        let domain = ontology.find_domain(PackageId::second()).unwrap();

        for type_info in domain.type_infos() {
            if let Some(text_constant) = type_info.name() {
                let name = &ontology[text_constant];

                if name == "foobar_edge" {
                    assert_eq!(&ontology[type_info.store_key.unwrap()], "fubar");
                }

                if name == "foo" {
                    assert_eq!(&ontology[type_info.store_key.unwrap()], "fu");

                    let (_prop_id, _rel_info, projection) =
                        type_info.edge_relationships().next().unwrap();
                    let edge_info = ontology.find_edge(projection.id).unwrap();
                    assert_eq!(&ontology[edge_info.store_key.unwrap()], "fubar");
                }

                if name == "bar" {
                    assert_eq!(&ontology[type_info.store_key.unwrap()], "bar");

                    let (_prop_id, _rel_info, projection) =
                        type_info.edge_relationships().next().unwrap();
                    let edge_info = ontology.find_edge(projection.id).unwrap();
                    assert_eq!(&ontology[edge_info.store_key.unwrap()], "baaah");
                }
            }
        }
    });
}
