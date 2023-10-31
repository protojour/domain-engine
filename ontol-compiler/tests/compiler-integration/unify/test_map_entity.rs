use ontol_test_utils::{
    examples::{ARTIST_AND_INSTRUMENT, GUITAR_SYNTH_UNION},
    SourceName, TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;

#[test]
fn should_map_inherent_capturing_pattern_id() {
    "
    def(pub) foo {
        rel .'id'|id: { fmt '' => 'foo/' => uuid => . }
    }
    def(pub) bar {
        rel .'id'|id: { fmt '' => 'bar/' => uuid => . }
    }

    map {
        foo { 'id': id }
        bar { 'id': id }
    }
    "
    .compile_then(|test| {
        test.mapper(()).assert_map_eq(
            ("foo", "bar"),
            json!({ "id": "foo/67e55044-10b1-426f-9247-bb680e5fe0c8" }),
            json!({ "id": "bar/67e55044-10b1-426f-9247-bb680e5fe0c8" }),
        );
    });
}

#[test]
fn test_extract_rel_params() {
    "
    def(pub) a1_id { fmt '' => 'a1/' => uuid => . }
    def(pub) a2_id { fmt '' => 'a2/' => uuid => . }
    def(pub) b1_id { fmt '' => 'b1/' => uuid => . }
    def(pub) b2_id { fmt '' => 'b2/' => uuid => . }

    def a2 {
        rel a2_id identifies: .
        rel .'foo': text
    }
    def b2 {
        rel b2_id identifies: .
        rel .'foo': text
        rel .'bar': text
    }

    def a_edge {
        rel .'bar': text
    }

    def(pub) a1 {
        rel a1_id identifies: .
        rel .'foreign'(rel .is: a_edge): a2
    }
    def(pub) b1 {
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
    .compile_then(|test| {
        test.mapper(()).assert_map_eq(
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

        test.mapper(()).assert_map_eq(
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
    def(pub) a_id { fmt '' => 'a/' => uuid => . }
    def(pub) b_id { fmt '' => 'a/' => uuid => . }
    def(pub) a_inner_id { fmt '' => 'a_inner/' => uuid => . }
    def(pub) b_inner_id { fmt '' => 'b_inner/' => uuid => . }

    def a_inner {
        rel a_inner_id identifies: .
        rel .'a_prop': text
    }
    def b_inner {
        rel b_inner_id identifies: .
        rel .'b_prop': text
    }

    def a_edge { rel .'aa': text }
    def b_edge { rel .'bb': text }

    def(pub) a {
        rel a_id identifies: .
        rel .'foreign'(rel .is: a_edge): a_inner
    }
    def(pub) b {
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
    .compile_then(|test| {
        test.mapper(()).assert_map_eq(
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
    def(pub) foo_inner { rel .'foo_id'|id: { rel .is: text } }
    def(pub) bar_inner { rel .'bar_id'|id: { rel .is: text } }
    rel [foo_inner] 'bars'::'foos' [bar_inner]

    def(pub) bar {
        rel .'id'|id: { rel .is: text }
        rel .'foos': [text]
    }

    map {
        bar_inner match {
            'bar_id': id
            'foos': [..foo_inner { 'foo_id': foo }]
        }
        bar {
            'id': id
            'foos': [..foo]
        }
    }
    "
    .compile_then(|test| {
        // The point of this test is to show that a
        // "foos" in input is not needed to produce a "foo" in output,
        // since the input "foo" is an inverted/object relation sequence property:
        test.mapper(()).assert_map_eq(
            ("bar_inner", "bar"),
            json!({ "bar_id": "B" }),
            json!({ "id": "B", "foos": [] }),
        );
        test.mapper(()).assert_map_eq(
            ("bar_inner", "bar"),
            json!({ "bar_id": "B", "foos": [{ "foo_id": "F" }]}),
            json!({ "id": "B", "foos": ["F"] }),
        );
    });
}

const WORK: &str = "
def(pub) worker_id { fmt '' => 'worker/' => uuid => . }
def(pub) tech_id { fmt '' => 'tech/' => uuid => . }

def(pub) worker {}
def(pub) technology {}

def worker {
    rel .'ID': worker_id
    rel worker_id identifies: .
    rel .'name': text

    rel .'technologies': [technology]
}

def technology {
    rel .'ID': tech_id
    rel tech_id identifies: .
    rel .'name': text
}
";

const DEV: &str = "
def(pub) lang_id { fmt '' => uuid => . }
def(pub) dev_id { fmt '' => uuid => . }

def(pub) language {}
def(pub) developer {}

def language {
    rel .'id': lang_id
    rel lang_id identifies: .

    rel .'name': text
    rel .'developers': [developer]
}

def developer {
    rel .'id': dev_id
    rel dev_id identifies: .
    rel .'name': text
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
                    'technologies': [..work.technology {
                        'ID': tech_id
                        'name': tech_name
                    }]
                }
                dev.language {
                    'id': tech_id
                    'name': tech_name
                    'developers': [..dev.developer { // ERROR TODO: Incompatible aggregation group
                        'id': p_id
                        'name': p_name
                    }]
                }
            }
            ",
        ),
    ])
    .compile_fail();
}

#[test]
fn artist_etc_routing() {
    TestPackages::with_sources([
        (SourceName("gsu"), GUITAR_SYNTH_UNION.1),
        (SourceName("ai"), ARTIST_AND_INSTRUMENT.1),
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
                        'plays': [..p] // ERROR cannot convert this `instrument` from `instrument`: These types are not equated.
                    }
                    ai.artist {
                        'ID': id
                        'name': n
                        'plays': [..p] // ERROR unbound variable// ERROR cannot convert this `instrument` from `instrument`: These types are not equated.
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
    .compile_fail();
}
