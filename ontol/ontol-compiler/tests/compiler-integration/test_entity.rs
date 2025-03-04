use itertools::Itertools;
use ontol_examples as examples;
use ontol_macros::test;
use ontol_runtime::{
    DefIdSet, DomainIndex, attr::AttrRef, ontology::domain::DataRelationshipTarget,
    tuple::CardinalIdx, value::Value,
};
use ontol_test_utils::{
    TestCompile, assert_error_msg, assert_json_io_matches, expect_eq, serde_helper::*,
};
use serde_json::json;

#[test]
#[ignore = "non-inherent ID in this way is no longer possible"]
fn entity_without_inherent_id() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def some_id (fmt '' => text => .)
    def entity (
        rel. id: some_id
        rel. 'foo': text
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo_id (rel* is: text)
    def foo (
        rel. 'key': foo_id
        rel* ancestry.children: {foo}
    )
    arc ancestry {
        (p) children: (c)
    }
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo_id (rel* is: text)
    def foo (
        rel. 'key'[rel* gen: auto]: foo_id
        rel* ancestry.children: {foo}
    )
    arc ancestry {
        (p) children: (c)
    }
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'key': (rel* is: text)
        rel* ancestry.children: {foo}
    )
    arc ancestry {
        (p) children: (c)
    }
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'key': ( fmt '' => 'foo/' => uuid => . )
    )
    "
    .compile();
}

#[test]
fn entity_id_inline_fmt_serial() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'key': ( fmt '' => 'foo/' => serial => . )
    )
    "
    .compile();
}

#[test]
fn artist_and_instrument_io_artist() {
    let test = examples::artist_and_instrument().1.compile();
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
    let test = examples::artist_and_instrument().1.compile();
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
    let test = examples::artist_and_instrument().1.compile();
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
fn artist_and_instrument_id_as_relation_object_ok() {
    let test = examples::artist_and_instrument().1.compile();
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

    assert_error_msg!(
        serde_create(&artist).to_value(json!({
            "name": "Tony Levin",
            "plays": [{ "ID": example_id, "name": "Chapman stick" }]
        })),
        r#"missing properties, expected "_edge" at line 1 column 109"#
    );
    assert_error_msg!(
        serde_create(&artist).to_value(json!({
            "name": "Allan Holdsworth",
            "plays": [{ "name": "Synthaxe", "ID": example_id }]
        })),
        r#"missing properties, expected "_edge" at line 1 column 110"#
    );
}

#[test]
fn artist_and_instrument_id_as_relation_object_invalid_id_format() {
    let test = examples::artist_and_instrument().1.compile();
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
        r#"invalid type: string "junk", expected string matching /(?:\A(?:instrument/)((?:[0-9A-Fa-f]{32}|(?:[0-9A-Fa-f]{8}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{12})))\z)/ at line 1 column 39"#
    );
}

#[test]
fn test_entity_self_relationship_optional_object_sym() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    arc ancestry {
        (p) children: (c),
        (c) parent: (p),
    }

    def node_id (fmt '' => text => .)
    def node (
        rel. 'id': node_id
        rel* 'name': text
        rel* ancestry.children: {node}
        rel* ancestry.parent?: node
    )
    "
    .compile_then(|test| {
        let [node] = test.bind(["node"]);
        assert_error_msg!(
            serde_create(&node).to_value(json!({})),
            r#"missing properties, expected "id" and "name" at line 1 column 2"#
        );

        assert_json_io_matches!(serde_create(&node), { "id": "1", "name": "a" });

        assert_json_io_matches!(serde_create(&node), {
            "id": "1",
            "name": "a",
            "children": [{
                "id": "2",
            }]
        });

        assert_json_io_matches!(serde_create(&node), {
            "id": "1",
            "name": "b",
            "parent": {
                "name": "a",
                "id": "2",
            },
            "children": [{
                "name": "c",
                "id": "3",
            }]
        });
    });
}

#[test]
fn test_entity_self_relationship_optional_object() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    arc ancestry { (p) children: (c), (c) parent: (p) }

    def node_id (fmt '' => serial => .)
    def node (
        rel. 'id': node_id
        rel* 'name': text
        rel* ancestry.children: {node}
        rel* ancestry.parent?: node
    )
    "
    .compile_then(|test| {
        let [node] = test.bind(["node"]);
        assert_error_msg!(
            serde_create(&node).to_value(json!({ "id": "1" })),
            r#"missing properties, expected "name" at line 1 column 10"#
        );

        assert_json_io_matches!(serde_create(&node), { "id": "1", "name": "a" });

        assert_json_io_matches!(serde_create(&node), {
            "id": "1",
            "name": "a",
            "children": [{ "id": "2" }]
        });

        assert_json_io_matches!(serde_create(&node), {
            "id": "2",
            "name": "b",
            "parent": { "id": "1" },
            "children": [{ "id": "3" }]
        });
    });
}

#[test]
fn test_entity_self_relationship_mandatory_object() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    arc ancestry { (p) children: (c), (c) parent: (p) }

    def node_id (fmt '' => serial => .)
    def node (
        rel. 'id': node_id
        rel* ancestry.children: {node}
        rel* ancestry.parent: node
    )
    "
    .compile_then(|test| {
        let [node] = test.bind(["node"]);
        assert_error_msg!(
            serde_create(&node).to_value(json!({ "id": "1" })),
            r#"missing properties, expected "parent" at line 1 column 10"#
        );
    });
}

#[test]
fn entity_union_simple() {
    let test = examples::guitar_synth_union().1.compile();
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def Repository (
        rel. 'id'[rel* gen: auto]: (rel* is: uuid)
        rel* repo_ownership.owner: RepositoryOwner
    )

    arc repo_ownership {
        (repo) owner: (owner),
        (owner) repositories: (repo),
    }

    def org_id (fmt '' => 'org/' => text => .)

    def Organization (
        rel. 'id': org_id
        rel* repo_ownership.repositories: {Repository}
    )

    def RepositoryOwner (
        rel* is?: Organization
    )
    "
    .compile();
}

#[test]
fn entity_union_with_object_relation() {
    let test = examples::guitar_synth_union().1.compile();
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
    let test = examples::guitar_synth_union().1.compile();
    let [artist, guitar_id, synth_id] = test.bind(["artist", "guitar_id", "synth_id"]);
    let plays = artist.find_property("plays").unwrap();

    assert!(artist.def.entity().is_some());
    assert!(guitar_id.def.entity().is_none());

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

    assert_ne!(guitar_id.def.id, synth_id.def.id);
    assert_eq!(
        guitar_id_attr.as_unit().unwrap().type_def_id(),
        guitar_id.def.id
    );
    assert_eq!(
        synth_id_attr.as_unit().unwrap().type_def_id(),
        synth_id.def.id
    );
}

#[test]
fn entity_relationship_without_reverse() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def lang_id (fmt '' => text => .)
    def prog_id (fmt '' => serial => .)
    def language (
        rel. 'lang-id': lang_id
    )
    def programmer (
        rel. 'id': prog_id
        rel* 'name': text
        rel* fav.favorite-language: language
    )
    arc fav {
        (p) favorite-language: (l)
    }
    "
    .compile_then(|test| {
        let [programmer] = test.bind(["programmer"]);
        assert_json_io_matches!(serde_create(&programmer), {
            "id": "1",
            "name": "audun",
            "favorite-language": { "lang-id": "rust" }
        });
    });
}

#[test]
fn recursive_entity_union() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def animal_id (fmt '' => 'animal/' => serial => .)
    def plant_id (fmt '' => 'plant/' => serial => .)
    def owner_id (fmt '' => text => .)

    def lifeform ()
    arc diet {
        (organism) eats: (nutrition)
    }
    def animal (
        rel. 'id'[rel* gen: auto]: animal_id
        rel* 'class': 'animal'
        rel* diet.eats: {lifeform}
    )
    def plant (
        rel. 'id'[rel* gen: auto]: plant_id
        rel* 'class': 'plant'
    )
    rel {lifeform} is?: animal
    rel {lifeform} is?: plant

    arc ownership {
        (owner) owns: (organism)
    }

    def owner (
        rel. 'id': owner_id
        rel* 'name': text
        rel* ownership.owns: {lifeform}
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id'[rel* gen: auto]: (fmt '' => 'prefix/' => serial => .)
    )
    "
    .compile();
}

#[test]
fn entity_order_ok() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'id': (rel* is: text)
        rel* 'name': text
        rel* order[
            rel* 0: 'name'
            rel* direction: descending
        ]: by_name
    )
    sym { by_name }
    "
    .compile();
}

#[test]
fn store_key_in_def_info() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foobar_edge (
        rel* store_key: 'fubar'
    )

    def foo (
        rel. 'id': (rel* is: text)
        rel* store_key: 'fu'
        rel* linkage.bar: bar
    )

    def bar (
        rel. 'id': (rel* is: text)
        rel* linkage.foo: bar
    )

    arc linkage {
        (f) bar: (b) with: foobar_edge,
        (b) foo: (f) with: foobar_edge,
    }
    "
    .compile_then(|test| {
        let ontology = test.ontology();
        let domain = ontology.domain_by_index(DomainIndex::second()).unwrap();

        for def in domain.defs() {
            if let Some(text_constant) = def.ident() {
                let name = &ontology[text_constant];

                if name == "foobar_edge" {
                    assert_eq!(&ontology[def.store_key.unwrap()], "fubar");
                }

                if name == "foo" {
                    assert_eq!(&ontology[def.store_key.unwrap()], "fu");

                    let (_prop_id, _rel_info, _projection) =
                        def.edge_relationships().next().unwrap();
                    // TODO: "store_key" for eges has been deprecated
                    // let edge_info = ontology.find_edge(projection.id).unwrap();
                    // assert_eq!(&ontology[edge_info.store_key.unwrap()], "fubar");
                }

                if name == "bar" {
                    assert_eq!(&ontology[def.store_key.unwrap()], "bar");

                    let (_prop_id, _rel_info, _projection) =
                        def.edge_relationships().next().unwrap();
                    // TODO: "store_key" for eges has been deprecated
                    // let edge_info = ontology.find_edge(projection.id).unwrap();
                    // assert_eq!(&ontology[edge_info.store_key.unwrap()], "baaah");
                }
            }
        }
    });
}

#[test]
fn edge_entity_simple() {
    let test = examples::edge_entity_simple().1.compile();
    let ontology = test.ontology();
    let domain = ontology.domain_by_index(DomainIndex::second()).unwrap();
    let [foo_id, foo, bar_id, link] = test.bind(["foo_id", "foo", "bar_id", "link"]);

    {
        let (_, edge) = domain.edges().next().unwrap();

        assert_eq!(edge.cardinals.len(), 3);
        assert!(edge.cardinals.iter().all(|cardinal| cardinal.is_entity()));

        assert!(!edge.cardinals[0].is_one_to_one());
        assert!(!edge.cardinals[1].is_one_to_one());
        assert!(edge.cardinals[2].is_one_to_one());
    }

    assert!(foo.def.entity().is_some());
    assert!(link.def.entity().is_some());

    {
        let mut edge_relationships = foo.def.edge_relationships();
        let (.., related_to) = edge_relationships.next().unwrap();

        assert_eq!(related_to.subject, CardinalIdx(0));
        assert_eq!(related_to.object, CardinalIdx(1));
        assert!(!related_to.pinned);
    }

    {
        let mut link_relationships = link.def.edge_relationships().collect_vec();
        link_relationships.sort_by_key(|(rel_id, _, _)| *rel_id);
        let (_, from, from_proj) = link_relationships.first().unwrap();
        let (_, to, to_proj) = link_relationships.get(1).unwrap();

        let DataRelationshipTarget::Unambiguous(from_target) = from.target else {
            panic!()
        };
        assert_eq!(foo_id.def_id(), from_target);
        assert_eq!(CardinalIdx(2), from_proj.subject);
        assert_eq!(CardinalIdx(0), from_proj.object);
        assert!(from_proj.pinned);

        let DataRelationshipTarget::Unambiguous(to_target) = to.target else {
            panic!()
        };
        assert_eq!(bar_id.def_id(), to_target);
        assert_eq!(CardinalIdx(2), to_proj.subject);
        assert_eq!(CardinalIdx(1), to_proj.object);
        assert!(to_proj.pinned);
    }
}

#[test]
fn edge_entity_union() {
    let test = examples::edge_entity_union().1.compile();
    let ontology = test.ontology();
    let domain = ontology.domain_by_index(DomainIndex::second()).unwrap();

    let [foo_id, bar_id, baz_id, qux_id] = test.bind(["foo_id", "bar_id", "baz_id", "qux_id"]);
    let [foo, bar, baz, qux, link] = test.bind(["foo", "bar", "baz", "qux", "link"]);

    assert!(foo.def.entity().is_some());
    assert!(link.def.entity().is_some());

    {
        let (_, edge) = domain.edges().next().unwrap();

        assert_eq!(edge.cardinals.len(), 3);
        assert!(edge.cardinals.iter().all(|cardinal| cardinal.is_entity()));

        assert!(!edge.cardinals[0].is_one_to_one());
        assert_eq!(
            DefIdSet::from_iter([foo.def_id(), bar.def_id()]),
            edge.cardinals[0].target
        );
        assert_eq!(
            DefIdSet::from_iter([baz.def_id(), qux.def_id()]),
            edge.cardinals[1].target
        );
        assert!(!edge.cardinals[1].is_one_to_one());
        assert_eq!(
            DefIdSet::from_iter([link.def_id()]),
            edge.cardinals[2].target
        );
        assert!(edge.cardinals[2].is_one_to_one());
    }

    {
        let mut edge_relationships = foo.def.edge_relationships();
        let (.., related_to) = edge_relationships.next().unwrap();

        assert_eq!(related_to.subject, CardinalIdx(0));
        assert_eq!(related_to.object, CardinalIdx(1));
        assert!(!related_to.pinned);
    }

    {
        let (_, from, from_proj) = link
            .def
            .edge_relationships()
            .find(|(_, rel, _)| &ontology[rel.name] == "from")
            .unwrap();
        let (_, to, to_proj) = link
            .def
            .edge_relationships()
            .find(|(_, rel, _)| &ontology[rel.name] == "to")
            .unwrap();

        assert_eq!(&ontology[from.name], "from");
        assert_eq!(&ontology[to.name], "to");

        let from_target_variants = ontology.union_variants(from.target.def_id());
        assert_eq!(&[foo_id.def_id(), bar_id.def_id()], from_target_variants);
        assert_eq!(CardinalIdx(2), from_proj.subject);
        assert_eq!(CardinalIdx(0), from_proj.object);
        assert!(from_proj.pinned);

        let to_target_variants = ontology.union_variants(to.target.def_id());
        assert_eq!(&[baz_id.def_id(), qux_id.def_id()], to_target_variants);
        assert_eq!(CardinalIdx(2), to_proj.subject);
        assert_eq!(CardinalIdx(1), to_proj.object);
        assert!(to_proj.pinned);
    }
}

#[test]
fn edge_entity_subtype_db() {
    let test = examples::entity_subtype::db().1.compile();
    let [foo] = test.bind(["foo"]);

    let data_rels = &foo.def.data_relationships;

    assert_eq!(data_rels.len(), 2);

    let (_prop_id, anonymous) = data_rels.iter().nth(1).unwrap();

    let DataRelationshipTarget::Union(_) = &anonymous.target else {
        panic!("not a union: {:?}", anonymous.target);
    };
}
