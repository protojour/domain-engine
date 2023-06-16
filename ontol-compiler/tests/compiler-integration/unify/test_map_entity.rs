use crate::unify::assert_domain_map;
use ontol_test_utils::{SourceName, TestCompile, TestPackages};
use serde_json::json;
use test_log::test;

const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../../examples/artist_and_instrument.on");
const GUITAR_SYNTH_UNION: &str = include_str!("../../../../examples/guitar_synth_union.on");

#[test]
fn test_extract_rel_params() {
    "
    pub type a1_id { fmt '' => 'a1/' => uuid => _ }
    pub type a2_id { fmt '' => 'a2/' => uuid => _ }
    pub type b1_id { fmt '' => 'b1/' => uuid => _ }
    pub type b2_id { fmt '' => 'b2/' => uuid => _ }

    type a2 {
        rel a2_id identifies: _
        rel _ 'foo': string
    }
    type b2 {
        rel b2_id identifies: _
        rel _ 'foo': string
        rel _ 'bar': string
    }

    type a_edge {
        rel _ 'bar': string
    }

    pub type a1 {
        rel a1_id identifies: _
        rel _ 'foreign'(rel _ is: a_edge): a2
    }
    pub type b1 {
        rel b1_id identifies: _
        rel _ 'foreign': b2
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
    .compile_ok(|test_env| {
        assert_domain_map(
            &test_env,
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
            &test_env,
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
    pub type a_id { fmt '' => 'a/' => uuid => _ }
    pub type b_id { fmt '' => 'a/' => uuid => _ }
    pub type a_inner_id { fmt '' => 'a_inner/' => uuid => _ }
    pub type b_inner_id { fmt '' => 'b_inner/' => uuid => _ }

    type a_inner {
        rel a_inner_id identifies: _
        rel _ 'a_prop': string
    }
    type b_inner {
        rel b_inner_id identifies: _
        rel _ 'b_prop': string
    }

    type a_edge { rel _ 'aa': string }
    type b_edge { rel _ 'bb': string }

    pub type a {
        rel a_id identifies: _
        rel _ 'foreign'(rel _ is: a_edge): a_inner
    }
    pub type b {
        rel b_id identifies: _
        rel _ 'foreign'(rel _ is: b_edge): b_inner
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
    .compile_ok(|test_env| {
        assert_domain_map(
            &test_env,
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

const WORK: &str = "
pub type worker_id { fmt '' => 'worker/' => uuid => _ }
pub type tech_id { fmt '' => 'tech/' => uuid => _ }

pub type worker
pub type technology

with worker {
    rel _ 'ID': worker_id
    rel worker_id identifies: _
    rel _ 'name': string

    rel _ 'technologies': [technology]
}

with technology {
    rel _ 'ID': tech_id
    rel tech_id identifies: _
    rel _ 'name': string
}
";

const DEV: &str = "
pub type lang_id { fmt '' => uuid => _ }
pub type dev_id { fmt '' => uuid => _ }

pub type language
pub type developer

with language {
    rel _ 'id': lang_id
    rel lang_id identifies: _

    rel _ 'name': string
    rel _ 'developers': [developer]
}

with developer {
    rel _ 'id': dev_id
    rel dev_id identifies: _
    rel _ 'name': string
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
                        'artist-id': id // ERROR cannot convert this `artist_id` from `artist-id`: These types are not equated.
                        'name': n
                        'plays': [p] // ERROR cannot convert this `instrument` from `instrument`: These types are not equated.
                    }
                    ai.artist {
                        'ID': id // ERROR cannot convert this `artist-id` from `artist_id`: These types are not equated.
                        'name': n
                        'plays': [p] // ERROR cannot convert this `instrument` from `instrument`: These types are not equated.
                    }
                }

                map {
                    gsu.synth {
                        'instrument-id': id // ERROR cannot convert this `synth_id` from `instrument-id`: These types are not equated.
                        'type': t // ERROR unbound variable
                        'polyphony': p // ERROR unbound variable
                    }
                    ai.instrument {
                        'ID': id // ERROR cannot convert this `instrument-id` from `synth_id`: These types are not equated.
                        'name': n // ERROR unbound variable
                    }
                }
            ): ai
            ",
        ),
    ])
    .compile_fail()
}
