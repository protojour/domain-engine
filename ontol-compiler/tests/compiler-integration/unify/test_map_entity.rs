use crate::unify::assert_domain_map;
use ontol_test_utils::{SourceName, TestCompile, TestPackages};
use serde_json::json;
use test_log::test;

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../../examples/artist_and_instrument.on");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../../examples/guitar_synth_union.on");

#[test]
fn should_map_inherent_capturing_pattern_id() {
    "
    pub type foo {
        rel .'id'|id: { fmt '' => 'foo/' => uuid => . }
    }
    pub type bar {
        rel .'id'|id: { fmt '' => 'bar/' => uuid => . }
    }

    map {
        foo { 'id': id }
        bar { 'id': id }
    }
    "
    .compile_ok(|test| {
        assert_domain_map(
            &test,
            ("foo", "bar"),
            json!({ "id": "foo/67e55044-10b1-426f-9247-bb680e5fe0c8" }),
            json!({ "id": "bar/67e55044-10b1-426f-9247-bb680e5fe0c8" }),
        );
    });
}

#[test]
fn test_extract_rel_params() {
    "
    pub type a1_id { fmt '' => 'a1/' => uuid => . }
    pub type a2_id { fmt '' => 'a2/' => uuid => . }
    pub type b1_id { fmt '' => 'b1/' => uuid => . }
    pub type b2_id { fmt '' => 'b2/' => uuid => . }

    type a2 {
        rel a2_id identifies: .
        rel .'foo': string
    }
    type b2 {
        rel b2_id identifies: .
        rel .'foo': string
        rel .'bar': string
    }

    type a_edge {
        rel .'bar': string
    }

    pub type a1 {
        rel a1_id identifies: .
        rel .'foreign'(rel .is: a_edge): a2
    }
    pub type b1 {
        rel b1_id identifies: .
        rel .'foreign': b2
    }

    map {
        a1 {
            'foreign'('bar': b): a2 {
                'foo': f
            }
        }
        b1 {
            'foreign': b2 {
                'foo': f
                'bar': b
            }
        }
    }
    "
    .compile_ok(|test| {
        assert_domain_map(
            &test,
            ("a1", "b1"),
            json!({
                "foreign": {
                    "foo": "FOO",
                    "_edge": {
                        "bar": "BAR"
                    }
                }
            }),
            json!({
                "foreign": {
                    "foo": "FOO",
                    "bar": "BAR",
                }
            }),
        );

        assert_domain_map(
            &test,
            ("b1", "a1"),
            json!({
                "foreign": {
                    "foo": "FOO",
                    "bar": "BAR",
                }
            }),
            json!({
                "foreign": {
                    "foo": "FOO",
                    "_edge": {
                        "bar": "BAR"
                    }
                }
            }),
        );
    });
}

#[test]
fn test_rel_params_implicit_map() {
    "
    pub type a_id { fmt '' => 'a/' => uuid => . }
    pub type b_id { fmt '' => 'a/' => uuid => . }
    pub type a_inner_id { fmt '' => 'a_inner/' => uuid => . }
    pub type b_inner_id { fmt '' => 'b_inner/' => uuid => . }

    type a_inner {
        rel a_inner_id identifies: .
        rel .'a_prop': string
    }
    type b_inner {
        rel b_inner_id identifies: .
        rel .'b_prop': string
    }

    type a_edge { rel .'aa': string }
    type b_edge { rel .'bb': string }

    pub type a {
        rel a_id identifies: .
        rel .'foreign'(rel .is: a_edge): a_inner
    }
    pub type b {
        rel b_id identifies: .
        rel .'foreign'(rel .is: b_edge): b_inner
    }

    map {
        a_inner { 'a_prop': x }
        b_inner { 'b_prop': x }
    }
    map {
        a_edge { 'aa': x }
        b_edge { 'bb': x }
    }
    map {
        a { 'foreign': x }
        b { 'foreign': x }
    }
    "
    .compile_ok(|test| {
        assert_domain_map(
            &test,
            ("a", "b"),
            json!({
                "foreign": {
                    "a_prop": "PROP",
                    "_edge": {
                        "aa": "EDGE"
                    }
                }
            }),
            json!({
                "foreign": {
                    "b_prop": "PROP",
                    "_edge": {
                        "bb": "EDGE"
                    }
                }
            }),
        );
    });
}

#[test]
fn test_map_relation_sequence_default_fallback() {
    "
    pub type foo_inner { rel .'foo_id'|id: { rel .is: string } }
    pub type bar_inner { rel .'bar_id'|id: { rel .is: string } }
    rel [foo_inner] 'bars'::'foos' [bar_inner]

    pub type bar {
        rel .'id'|id: { rel .is: string }
        // BUG: want to use string here:
        rel .'foos': [{rel .is: string }]
    }

    map => {
        bar_inner {
            'bar_id': id
            'foos': [foo_inner { 'foo_id': foo }]
        }
        bar {
            'id': id
            'foos': [foo]
        }
    }
    "
    .compile_ok(|test| {
        // The point of this test is to show that a
        // "foos" in input is not needed to produce a "foo" in output,
        // since the input "foo" is an inverted/object relation sequence property:
        assert_domain_map(
            &test,
            ("bar_inner", "bar"),
            json!({ "bar_id": "B" }),
            json!({ "id": "B", "foos": [] }),
        );
        assert_domain_map(
            &test,
            ("bar_inner", "bar"),
            json!({ "bar_id": "B", "foos": [{ "foo_id": "F" }]}),
            json!({ "id": "B", "foos": ["F"] }),
        );
    });
}

const WORK: &str = "
pub type worker_id { fmt '' => 'worker/' => uuid => . }
pub type tech_id { fmt '' => 'tech/' => uuid => . }

pub type worker
pub type technology

with worker {
    rel .'ID': worker_id
    rel worker_id identifies: .
    rel .'name': string

    rel .'technologies': [technology]
}

with technology {
    rel .'ID': tech_id
    rel tech_id identifies: .
    rel .'name': string
}
";

const DEV: &str = "
pub type lang_id { fmt '' => uuid => . }
pub type dev_id { fmt '' => uuid => . }

pub type language
pub type developer

with language {
    rel .'id': lang_id
    rel lang_id identifies: .

    rel .'name': string
    rel .'developers': [developer]
}

with developer {
    rel .'id': dev_id
    rel dev_id identifies: .
    rel .'name': string
}
";

#[test]
fn test_map_invert() {
    TestPackages::with_sources([
        (SourceName("work"), WORK),
        (SourceName("dev"), DEV),
        (
            SourceName::root(),
            "
            use 'work' as work
            use 'dev' as dev

            map {
                work.worker {
                    'ID': p_id
                    'name': p_name
                    'technologies': [work.technology {
                        'ID': tech_id
                        'name': tech_name
                    }]
                }
                dev.language {
                    'id': tech_id
                    'name': tech_name
                    'developers': [dev.developer { // ERROR TODO: Incompatible aggregation group
                        'id': p_id
                        'name': p_name
                    }]
                }
            }
            ",
        ),
    ])
    .compile_fail()
}

#[test]
fn artist_etc_routing() {
    TestPackages::with_sources([
        (SourceName("gsu"), GUITAR_SYNTH_UNION),
        (SourceName("ai"), ARTIST_AND_INSTRUMENT),
        (
            SourceName::root(),
            "
            use 'gsu' as gsu
            use 'ai' as ai

            rel gsu route(
                map {
                    gsu.artist {
                        'artist-id': id
                        'name': n
                        'plays': [p] // ERROR cannot convert this `instrument` from `instrument`: These types are not equated.
                    }
                    ai.artist {
                        'ID': id
                        'name': n
                        'plays': [p] // ERROR unbound variable// ERROR cannot convert this `instrument` from `instrument`: These types are not equated.
                    }
                }

                map {
                    gsu.synth {
                        'instrument-id': id
                        'type': t // ERROR unbound variable
                        'polyphony': p // ERROR unbound variable
                    }
                    ai.instrument {
                        'ID': id
                        'name': n // ERROR unbound variable
                    }
                }
            ): ai
            ",
        ),
    ])
    .compile_fail()
}
