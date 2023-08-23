use ontol_test_utils::{test_map::IntoKey, SourceName, TestCompile, TestPackages};
use serde_json::json;
use test_log::test;

#[test]
fn test_map_simple() {
    "
    pub type foo
    pub type bar
    rel foo 'f': string
    rel bar 'b': string
    map {
        foo { 'f': x }
        bar { 'b': x }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("foo", "bar"),
            json!({ "f": "my_value"}),
            json!({ "b": "my_value"}),
        );
        test.assert_domain_map(
            ("bar", "foo"),
            json!({ "b": "my_value"}),
            json!({ "f": "my_value"}),
        );
    });
}

#[test]
fn test_map_value_to_primitive() {
    "
    pub type my_string { rel.is: string }
    pub type foo { rel .'a': my_string }
    pub type bar { rel .'b': string }
    map {
        foo { 'a': x }
        bar { 'b': x }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("foo", "bar"),
            json!({ "a": "my_value"}),
            json!({ "b": "my_value"}),
        );
        test.assert_domain_map(
            ("bar", "foo"),
            json!({ "b": "my_value"}),
            json!({ "a": "my_value"}),
        );
    });
}

#[test]
fn test_meters() {
    "
    pub type meters
    pub type millimeters
    rel meters is: i64
    rel millimeters is: i64
    map {
        meters: x / 1000
        millimeters: x
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(("meters", "millimeters"), json!(5), json!(5000));
        test.assert_domain_map(("millimeters", "meters"), json!(5000), json!(5));
    });
}

#[test]
fn test_temperature() {
    "
    pub type celsius {
        rel .is: i64
    }
    pub type fahrenheit {
        rel .is: i64
    }

    map {
        celsius: x
        fahrenheit: x * 9 / 5 + 32
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(("celsius", "fahrenheit"), json!(10), json!(50));
        test.assert_domain_map(("fahrenheit", "celsius"), json!(50), json!(10));
    });
}

#[test]
fn test_nested_optional_attribute() {
    "
    type seconds {
        rel .is: i64
    }
    type years {
        rel .is: i64
    }

    map {
        seconds: y * 60 * 60 * 24 * 365
        years: y
    }

    pub type person {
        rel .'age'?: years
    }
    pub type creature {
        rel .'age'?: seconds
    }

    unify {
        person { 'age'?: a }
        creature { 'age'?: a }
    }

    pub type person_container {
        rel .'person'?: person
    }
    pub type creature_container {
        rel .'creature'?: creature
    }

    unify {
        person_container {
            'person'?: person {
                'age'?: a
            }
        }
        creature_container {
            'creature'?: creature {
                'age'?: a
            }
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(("person", "creature"), json!({}), json!({}));
        test.assert_domain_map(
            ("person", "creature"),
            json!({ "age": 42 }),
            json!({ "age": 1324512000 }),
        );
        test.assert_domain_map(
            ("person_container", "creature_container"),
            json!({}),
            json!({}),
        );
        test.assert_domain_map(
            ("person_container", "creature_container"),
            json!({ "person": {} }),
            json!({ "creature": {} }),
        );
        test.assert_domain_map(
            ("person_container", "creature_container"),
            json!({ "person": { "age": 42 } }),
            json!({ "creature": { "age": 1324512000 } }),
        );
    });
}

#[test]
fn test_map_value_to_map_no_func() {
    "
    pub type one
    pub type two
    rel one is: string
    rel two 'a': string
    map {
        one: x
        two {
            'a': x
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(("one", "two"), json!("foo"), json!({ "a": "foo" }));
        test.assert_domain_map(("two", "one"), json!({ "a": "foo" }), json!("foo"));
    });
}

#[test]
fn test_map_value_to_map_func() {
    "
    pub type one
    pub type two
    rel one is: i64
    rel two 'a': i64
    map {
        one: x
        two {
            'a': x * 2
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(("one", "two"), json!(2), json!({ "a": 4 }));
        test.assert_domain_map(("two", "one"), json!({ "a": 4 }), json!(2));
    });
}

#[test]
fn test_deep_structural_map() {
    "
    pub type foo
    type foo_inner

    with foo {
        rel .'a': string
        rel .'inner': foo_inner
    }
    with foo_inner {
        rel .'b': string
        rel .'c': string
    }

    pub type bar
    type bar_inner

    with bar {
        rel .'a': string
        rel .'b': string
        rel .'inner': bar_inner
    }

    with bar_inner {
        rel .'c': string
    }

    map {
        foo {
            'a': a
            'inner': foo_inner {
                'b': b
                'c': c
            }
        }
        bar {
            'a': a
            'b': b
            'inner': bar_inner {
                'c': c
            }
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("foo", "bar"),
            json!({ "a": "A", "inner": { "b": "B", "c": "C" }}),
            json!({ "a": "A", "b": "B", "inner": { "c": "C" }}),
        );
        test.assert_domain_map(
            ("bar", "foo"),
            json!({ "a": "A", "b": "B", "inner": { "c": "C" }}),
            json!({ "a": "A", "inner": { "b": "B", "c": "C" }}),
        );
    });
}

#[test]
fn test_map_matching_sequence() {
    "
    pub type foo
    pub type bar
    rel foo 'a': [i64]
    rel bar 'b': [i64]
    map {
        foo {
            'a': [..x]
        }
        bar {
            'b': [..x]
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(("foo", "bar"), json!({ "a": [42] }), json!({ "b": [42] }));
        test.assert_domain_map(("bar", "foo"), json!({ "b": [42] }), json!({ "a": [42] }));
    });
}

// map call inside sequence
const MAP_IN_SEQUENCE: &str = "
type foo { rel .'f': string }
type bar { rel .'b': string }
pub type foos { rel .'foos': [foo] }
pub type bars { rel .'bars': [bar] }

map {
    foos { 'foos': [..x] }
    bars { 'bars': [..x] }
}
map {
    foo { 'f': x }
    bar { 'b': x }
}
";

#[test]
fn test_map_in_sequence_item_empty() {
    MAP_IN_SEQUENCE.compile_ok(|test| {
        test.assert_domain_map(
            ("foos", "bars"),
            json!({ "foos": [] }),
            json!({ "bars": [] }),
        );
        test.assert_domain_map(
            ("bars", "foos"),
            json!({ "bars": [] }),
            json!({ "foos": [] }),
        );
    });
}

#[test]
fn test_map_in_sequence_item_one() {
    MAP_IN_SEQUENCE.compile_ok(|test| {
        test.assert_domain_map(
            ("foos", "bars"),
            json!({ "foos": [{ "f": "42" }] }),
            json!({ "bars": [{ "b": "42" }] }),
        );
        test.assert_domain_map(
            ("bars", "foos"),
            json!({ "bars": [{ "b": "42" }] }),
            json!({ "foos": [{ "f": "42" }] }),
        );
    });
}

#[test]
fn test_map_in_sequence_item_many() {
    MAP_IN_SEQUENCE.compile_ok(|test| {
        test.assert_domain_map(
            ("foos", "bars"),
            json!({ "foos": [{ "f": "42" }, { "f": "84" }] }),
            json!({ "bars": [{ "b": "42" }, { "b": "84" }] }),
        );
        test.assert_domain_map(
            ("bars", "foos"),
            json!({ "bars": [{ "b": "42" }, { "b": "84" }] }),
            json!({ "foos": [{ "f": "42" }, { "f": "84" }] }),
        );
    });
}

#[test]
fn test_sequence_cross_parallel() {
    "
    type foo { rel .'f': string }
    type bar { rel .'b': string }
    map {
        foo { 'f': x }
        bar { 'b': x }
    }

    pub type foos {
        rel .'f1': [foo]
        rel .'f2': [foo]
    }
    pub type bars {
        rel .'b1': [bar]
        rel .'b2': [bar]
    }
    map {
        foos {
            'f1': [..a]
            'f2': [..b]
        }
        bars {
            'b2': [..b]
            'b1': [..a]
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("foos", "bars"),
            json!({ "f1": [{ "f": "1A" }, { "f": "1B" }], "f2": [{ "f": "2" }] }),
            json!({ "b1": [{ "b": "1A" }, { "b": "1B" }], "b2": [{ "b": "2" }] }),
        );
    });
}

#[test]
fn test_sequence_inner_loop() {
    "
    type foo { rel .'P': string }
    type bar { rel .'Q': string }
    map {
        foo { 'P': x }
        bar { 'Q': x }
    }

    pub type f0 {
        rel .'a': [foo]
        rel .'b': [foo]
    }
    pub type f1 {
        rel .'a': [f0]
        rel .'b': [f0]
    }
    pub type b0 {
        rel .'a': [bar]
        rel .'b': [bar]
    }
    pub type b1 {
        rel .'a': [b0]
        rel .'b': [b0]
    }
    map {
        f1 {
            'a': [..f0 {
                'a': [..v0]
                'b': [..v1]
            }]
            'b': [..f0 {
                'a': [..v2]
                'b': [..v3]
            }]
        }
        b1 {
            'b': [..b0 {
                'b': [..v3]
                'a': [..v2]
            }]
            'a': [..b0 {
                'b': [..v1]
                'a': [..v0]
            }]
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("f1", "b1"),
            json!({ "a": [{ "a": [{ "P": "0" }], "b": [{ "P": "1" }]}], "b": [{ "a": [{ "P": "2" }], "b": [{ "P": "3" }]}]}),
            json!({ "a": [{ "a": [{ "Q": "0" }], "b": [{ "Q": "1" }]}], "b": [{ "a": [{ "Q": "2" }], "b": [{ "Q": "3" }]}]}),
        );
    });
}

#[test]
fn test_sequence_flat_map1() {
    "
    pub type foo
    type foo_inner
    pub type bar

    with foo {
        rel .'a': string
        rel .'inner': [foo_inner]
    }
    with foo_inner {
        rel .'b': string
    }
    with bar {
        rel .'a': string
        rel .'b': string
    }

    map {
        foo {
            'a': a
            'inner': [..foo_inner {
                'b': b
            }]
        }
        [bar {
            'a': a
            'b': b
        }]
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("foo", "bar".seq()),
            json!({ "a": "A", "inner": [{ "b": "B0" }, { "b": "B1" }] }),
            json!([{ "a": "A", "b": "B0" }, { "a": "A", "b": "B1" }]),
        );
    });
}

// BUG: This should work at least one way:
#[test]
fn test_sequence_composer1() {
    "
    type foo {
        rel .'a': string
        rel .'b': string
    }
    type bar {
        rel .'ab': [string]
    }

    map {
        foo {
            'a': a
            'b': b
        }
        bar {
            'ab': [a b] // ERROR TODO: maximum one element per sequence for now// ERROR TODO: requires spreading (`..`)// ERROR TODO: Incompatible aggregation group
        }
    }
    "
    .compile_fail();
}

#[test]
fn test_map_complex_flow() {
    // FIXME: This should be a one-way mapping.
    // there is no way two variables (e.g. `two.a` and `two.c`) can flow back into the same slot without data loss.
    // But perhaps let's accept that this might be what the user wants.
    // For example, when two `:x`es flow into one property, we can choose the first one.
    "
    pub type one {
        rel .'a': string
        rel .'b': string
    }
    pub type two {
        rel .'a': string
        rel .'b': string
        rel .'c': string
        rel .'d': string
    }

    map {
        one {
            'a': x
            'b': y
        }
        two {
            'a': x
            'b': y
            'c': x
            'd': y
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("one", "two"),
            json!({ "a": "X", "b": "Y" }),
            json!({ "a": "X", "b": "Y", "c": "X", "d": "Y" }),
        );
    });
}

#[test]
fn test_map_delegation() {
    TestPackages::with_sources([
        (
            SourceName::root(),
            "
            use 'SI' as SI

            pub type car {
                rel .'length': SI.meters
            }
            pub type vehicle {
                rel .'length': SI.millimeters
            }

            map {
                car { 'length': l }
                vehicle { 'length': l }
            }
            ",
        ),
        (
            SourceName("SI"),
            "
            pub type meters {
                rel .is: i64
            }
            pub type millimeters {
                rel .is: i64
            }

            map {
                meters: m
                millimeters: m * 1000
            }
            ",
        ),
    ])
    .compile_ok(|test| {
        test.assert_domain_map(
            ("car", "vehicle"),
            json!({ "length": 3 }),
            json!({ "length": 3000 }),
        );
        test.assert_domain_map(
            ("vehicle", "car"),
            json!({ "length": 2000 }),
            json!({ "length": 2 }),
        );
    });
}

#[test]
// BUG: Not yet implemented
#[should_panic]
fn test_map_delegation_abstract_types() {
    TestPackages::with_sources([
        (
            SourceName("SI"),
            "
            pub type meters {
                rel .is: number
            }
            pub type millimeters {
                rel .is: number
            }

            map {
                meters: m
                millimeters: m * 1000
            }
            ",
        ),
        (
            SourceName("car"),
            "
            use 'SI' as SI
            pub type car {
                rel .'length': {
                    rel .is: f64
                    rel .is: SI.meters
                }
            }
            ",
        ),
        (
            SourceName("vehicle"),
            "
            use 'SI' as SI
            pub type vehicle {
                rel .'length': {
                    rel .is: f64
                    rel .is: SI.millimeters
                }
            }
            ",
        ),
        (
            SourceName::root(),
            "
            use 'car' as c
            use 'vehicle' as v

            map {
                c.car { 'length': len }
                v.vehicle { 'length': len }
            }
            ",
        ),
    ])
    .compile_ok(|test| {
        test.assert_domain_map(
            ("c.car", "c.vehicle"),
            json!({ "length": 3 }),
            json!({ "length": 3000 }),
        );
        test.assert_domain_map(
            ("v.vehicle", "c.car"),
            json!({ "length": 2000 }),
            json!({ "length": 2 }),
        );
    });
}

#[test]
fn test_map_dependent_scoping() {
    "
    pub type one {
        rel .'total_weight': i64
        rel .'net_weight': i64
    }
    pub type two {
        rel .'net_weight': i64
        rel .'container_weight': i64
    }

    map {
        one {
            'total_weight': t
            'net_weight': n
        }
        two {
            'net_weight': n
            'container_weight': t - n
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("one", "two"),
            json!({ "total_weight": 100, "net_weight": 75 }),
            json!({ "net_weight": 75, "container_weight": 25 }),
        );
        test.assert_domain_map(
            ("two", "one"),
            json!({ "net_weight": 75, "container_weight": 25 }),
            json!({ "total_weight": 100, "net_weight": 75 }),
        );
    });
}

#[test]
fn test_seq_scope_escape1() {
    "
    type foo {}

    pub type bar {
        rel .'foo': foo
        rel .'p1': [string]
    }

    pub type baz {
        rel .'foo': foo
        rel .'p1': [string]
    }

    pub type qux {
        rel .'baz': baz
    }

    map => {
        bar {
            'foo': foo {}
            'p1': [..p1]
        }
        qux {
            'baz': baz {
                'foo': foo {}
                'p1': [..p1]
            }
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("bar", "qux"),
            json!({ "foo": {}, "p1": ["1"] }),
            json!({ "baz": { "foo": {}, "p1": ["1"] } }),
        );
    });
}

#[test]
fn test_seq_scope_escape2() {
    "
    type foo {
        rel .'p0': [string]
    }

    pub type bar {
        rel .'foo': foo
        rel .'p1': [string]
    }

    pub type baz {
        rel .'foo': foo
        rel .'p1': [string]
    }

    pub type qux {
        rel .'baz': baz
    }

    map => {
        bar {
            'foo': foo {
                'p0': [..p0]
            }
            'p1': [..p1]
        }
        qux {
            'baz': baz {
                'foo': foo {
                    'p0': [..p0]
                }
                'p1': [..p1]
            }
        }
    }
    "
    .compile_ok(|test| {
        test.assert_domain_map(
            ("bar", "qux"),
            json!({ "foo": { "p0": ["0"] }, "p1": ["1"] }),
            json!({ "baz": { "foo": { "p0": ["0"] }, "p1": ["1"] } }),
        );
    });
}
