use ontol_test_utils::{test_map::AsKey, SourceName, TestCompile, TestPackages};
use serde_json::json;
use test_log::test;

#[test]
fn test_map_simple() {
    "
    pub def foo {
        rel foo 'f': text
    }
    pub def bar {
        rel bar 'b': text
    }
    map {
        foo { 'f': x }
        bar { 'b': x }
    }
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({ "f": "my_value"}),
            json!({ "b": "my_value"}),
        );
        test.mapper().assert_map_eq(
            ("bar", "foo"),
            json!({ "b": "my_value"}),
            json!({ "f": "my_value"}),
        );
    });
}

#[test]
fn test_map_value_to_primitive() {
    "
    pub def string { rel.is: text }
    pub def foo { rel .'a': string }
    pub def bar { rel .'b': text }
    map {
        foo { 'a': x }
        bar { 'b': x }
    }
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({ "a": "my_value"}),
            json!({ "b": "my_value"}),
        );
        test.mapper().assert_map_eq(
            ("bar", "foo"),
            json!({ "b": "my_value"}),
            json!({ "a": "my_value"}),
        );
    });
}

#[test]
fn test_meters() {
    "
    pub def meters { rel .is: i64 }
    pub def millimeters { rel .is: i64 }
    map {
        meters: x / 1000
        millimeters: x
    }
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("meters", "millimeters"), json!(5), json!(5000));
        test.mapper()
            .assert_map_eq(("millimeters", "meters"), json!(5000), json!(5));
    });
}

#[test]
fn test_temperature() {
    "
    pub def celsius { rel .is: i64 }
    pub def fahrenheit { rel .is: i64 }
    map {
        celsius: x
        fahrenheit: x * 9 / 5 + 32
    }
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("celsius", "fahrenheit"), json!(10), json!(50));
        test.mapper()
            .assert_map_eq(("fahrenheit", "celsius"), json!(50), json!(10));
    });
}

#[test]
fn test_nested_optional_attribute() {
    "
    def seconds {
        rel .is: i64
    }
    def years {
        rel .is: i64
    }

    map {
        seconds: y * 60 * 60 * 24 * 365
        years: y
    }

    pub def person {
        rel .'age'?: years
    }
    pub def creature {
        rel .'age'?: seconds
    }

    unify {
        person { 'age'?: a }
        creature { 'age'?: a }
    }

    pub def person_container {
        rel .'person'?: person
    }
    pub def creature_container {
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
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("person", "creature"), json!({}), json!({}));
        test.mapper().assert_map_eq(
            ("person", "creature"),
            json!({ "age": 42 }),
            json!({ "age": 1324512000 }),
        );
        test.mapper().assert_map_eq(
            ("person_container", "creature_container"),
            json!({}),
            json!({}),
        );
        test.mapper().assert_map_eq(
            ("person_container", "creature_container"),
            json!({ "person": {} }),
            json!({ "creature": {} }),
        );
        test.mapper().assert_map_eq(
            ("person_container", "creature_container"),
            json!({ "person": { "age": 42 } }),
            json!({ "creature": { "age": 1324512000 } }),
        );
    });
}

#[test]
fn test_map_value_to_struct_no_func() {
    "
    pub def one {
        rel .is: text
    }
    pub def two {
        rel .'a': text
    }
    map {
        one: x
        two {
            'a': x
        }
    }
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("one", "two"), json!("foo"), json!({ "a": "foo" }));
        test.mapper()
            .assert_map_eq(("two", "one"), json!({ "a": "foo" }), json!("foo"));
    });
}

#[test]
fn test_map_value_to_struct_func() {
    "
    pub def one {
        rel .is: i64
    }
    pub def two {
        rel .'a': i64
    }
    map {
        one: x
        two {
            'a': x * 2
        }
    }
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("one", "two"), json!(2), json!({ "a": 4 }));
        test.mapper()
            .assert_map_eq(("two", "one"), json!({ "a": 4 }), json!(2));
    });
}

#[test]
fn test_map_into_default_field_using_default_value() {
    "
    pub def empty {}
    pub def target {
        rel .'field'(rel .default := 'Default!'): text
    }
    map {
        empty {}
        target {}
    }
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("empty", "target"),
            json!({}),
            json!({ "field": "Default!" }),
        );
        test.mapper().assert_map_eq(
            ("target", "empty"),
            json!({ "field": "whatever" }),
            json!({}),
        );
    });
}

#[test]
fn test_map_into_default_field_using_provided_value() {
    "
    pub def required {
        rel .'field': text
    }
    pub def target {
        rel .'field'(rel .default := 'Default!'): text
    }
    map {
        required { 'field': val }
        target { 'field': val }
    }
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("required", "target"),
            json!({ "field": "This" }),
            json!({ "field": "This" }),
        );
        test.mapper().assert_map_eq(
            ("target", "required"),
            json!({ "field": "This" }),
            json!({ "field": "This" }),
        );
    });
}

#[test]
fn test_map_into_default_field_using_map_provided() {
    "
    pub def empty {}
    pub def target {
        rel .'field'(rel .default := 'Default!'): text
    }
    map {
        empty {}
        target { 'field': 'Mapped!' }
    }
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("empty", "target"),
            json!({}),
            json!({ "field": "Mapped!" }),
        );
        test.mapper().assert_map_eq(
            ("target", "empty"),
            json!({ "field": "whatever" }),
            json!({}),
        );
    });
}

#[test]
fn test_deep_structural_map() {
    "
    pub def foo {
        rel .'a': text
    }
    def foo_inner {
        rel .'b': text
        rel .'c': text
    }
    def foo {
        rel .'inner': foo_inner
    }

    def bar_inner {}
    pub def bar {
        rel .'a': text
        rel .'b': text
        rel .'inner': bar_inner
    }

    def bar_inner {
        rel .'c': text
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
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({ "a": "A", "inner": { "b": "B", "c": "C" }}),
            json!({ "a": "A", "b": "B", "inner": { "c": "C" }}),
        );
        test.mapper().assert_map_eq(
            ("bar", "foo"),
            json!({ "a": "A", "b": "B", "inner": { "c": "C" }}),
            json!({ "a": "A", "inner": { "b": "B", "c": "C" }}),
        );
    });
}

#[test]
fn test_map_matching_sequence() {
    "
    pub def foo {
        rel .'a': [i64]
    }
    pub def bar {
        rel .'b': [i64]
    }
    map {
        foo {
            'a': [..x]
        }
        bar {
            'b': [..x]
        }
    }
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("foo", "bar"), json!({ "a": [42] }), json!({ "b": [42] }));
        test.mapper()
            .assert_map_eq(("bar", "foo"), json!({ "b": [42] }), json!({ "a": [42] }));
    });
}

// map call inside sequence
const MAP_IN_SEQUENCE: &str = "
def foo { rel .'f': text }
def bar { rel .'b': text }
pub def foos { rel .'foos': [foo] }
pub def bars { rel .'bars': [bar] }

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
    let test = MAP_IN_SEQUENCE.compile();
    test.mapper().assert_map_eq(
        ("foos", "bars"),
        json!({ "foos": [] }),
        json!({ "bars": [] }),
    );
    test.mapper().assert_map_eq(
        ("bars", "foos"),
        json!({ "bars": [] }),
        json!({ "foos": [] }),
    );
}

#[test]
fn test_map_in_sequence_item_one() {
    let test = MAP_IN_SEQUENCE.compile();
    test.mapper().assert_map_eq(
        ("foos", "bars"),
        json!({ "foos": [{ "f": "42" }] }),
        json!({ "bars": [{ "b": "42" }] }),
    );
    test.mapper().assert_map_eq(
        ("bars", "foos"),
        json!({ "bars": [{ "b": "42" }] }),
        json!({ "foos": [{ "f": "42" }] }),
    );
}

#[test]
fn test_map_in_sequence_item_many() {
    let test = MAP_IN_SEQUENCE.compile();
    test.mapper().assert_map_eq(
        ("foos", "bars"),
        json!({ "foos": [{ "f": "42" }, { "f": "84" }] }),
        json!({ "bars": [{ "b": "42" }, { "b": "84" }] }),
    );
    test.mapper().assert_map_eq(
        ("bars", "foos"),
        json!({ "bars": [{ "b": "42" }, { "b": "84" }] }),
        json!({ "foos": [{ "f": "42" }, { "f": "84" }] }),
    );
}

#[test]
fn test_sequence_cross_parallel() {
    "
    def foo { rel .'f': text }
    def bar { rel .'b': text }
    map {
        foo { 'f': x }
        bar { 'b': x }
    }

    pub def foos {
        rel .'f1': [foo]
        rel .'f2': [foo]
    }
    pub def bars {
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
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foos", "bars"),
            json!({ "f1": [{ "f": "1A" }, { "f": "1B" }], "f2": [{ "f": "2" }] }),
            json!({ "b1": [{ "b": "1A" }, { "b": "1B" }], "b2": [{ "b": "2" }] }),
        );
    });
}

#[test]
fn test_sequence_inner_loop() {
    "
    def foo { rel .'P': text }
    def bar { rel .'Q': text }
    map {
        foo { 'P': x }
        bar { 'Q': x }
    }

    pub def f0 {
        rel .'a': [foo]
        rel .'b': [foo]
    }
    pub def f1 {
        rel .'a': [f0]
        rel .'b': [f0]
    }
    pub def b0 {
        rel .'a': [bar]
        rel .'b': [bar]
    }
    pub def b1 {
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
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("f1", "b1"),
            json!({ "a": [{ "a": [{ "P": "0" }], "b": [{ "P": "1" }]}], "b": [{ "a": [{ "P": "2" }], "b": [{ "P": "3" }]}]}),
            json!({ "a": [{ "a": [{ "Q": "0" }], "b": [{ "Q": "1" }]}], "b": [{ "a": [{ "Q": "2" }], "b": [{ "Q": "3" }]}]}),
        );
    });
}

#[test]
fn test_sequence_flat_map1() {
    "
    pub def foo {
        rel .'a': text

        def foo_inner {
            rel .'b': text
        }
        rel .'inner': [foo_inner]
    }
    pub def bar {
        rel .'a': text
        rel .'b': text
    }

    map {
        foo {
            'a': a
            'inner': [..foo_inner {
                'b': b
            }]
        }
        bar: [
            ..bar {
                'a': a
                'b': b
            }
        ]
    }
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar".seq()),
            json!({ "a": "A", "inner": [{ "b": "B0" }, { "b": "B1" }] }),
            json!([{ "a": "A", "b": "B0" }, { "a": "A", "b": "B1" }]),
        );
    });
}

// FIXME: This should work both ways in principe, even if the backward mapping is fallible:
#[test]
fn test_sequence_composer_no_iteration() {
    "
    def foo {
        rel .'a': i64
        rel .'b': i64
    }
    def bar {
        rel .'ab': [i64]
    }

    map {
        foo match {
            'a': a
            'b': b
        }
        bar {
            'ab': [a b]
        }
    }
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({
                "a": 1,
                "b": 2,
            }),
            json!({ "ab": [1, 2] }),
        );
    });
}

#[test]
fn test_sequence_composer_with_iteration() {
    "
    def foo {
        rel .'a': i64
        rel .'b': [i64]
        rel .'c': i64
    }
    def bar {
        rel .'abc': [i64]
    }

    map {
        foo match {
            'a': a
            'b': [..b]
            'c': c
        }
        bar {
            'abc': [a ..b c]
        }
    }
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({
                "a": 1,
                "b": [2, 3],
                "c": 4
            }),
            json!({
                "abc": [1, 2, 3, 4]
            }),
        );
    });
}

#[test]
fn test_map_complex_flow() {
    // FIXME: This should be a one-way mapping.
    // there is no way two variables (e.g. `two.a` and `two.c`) can flow back into the same slot without data loss.
    // But perhaps let's accept that this might be what the user wants.
    // For example, when two `:x`es flow into one property, we can choose the first one.
    "
    pub def one {
        rel .'a': text
        rel .'b': text
    }
    pub def two {
        rel .'a': text
        rel .'b': text
        rel .'c': text
        rel .'d': text
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
    .compile_then(|test| {
        test.mapper().assert_map_eq(
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

            pub def car {
                rel .'length': SI.meters
            }
            pub def vehicle {
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
            pub def meters {
                rel .is: i64
            }
            pub def millimeters {
                rel .is: i64
            }

            map {
                meters: m
                millimeters: m * 1000
            }
            ",
        ),
    ])
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("car", "vehicle"),
            json!({ "length": 3 }),
            json!({ "length": 3000 }),
        );
        test.mapper().assert_map_eq(
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
            pub def meters {
                rel .is: number
            }
            pub def millimeters {
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
            pub def car {
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
            pub def vehicle {
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
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("c.car", "c.vehicle"),
            json!({ "length": 3 }),
            json!({ "length": 3000 }),
        );
        test.mapper().assert_map_eq(
            ("v.vehicle", "c.car"),
            json!({ "length": 2000 }),
            json!({ "length": 2 }),
        );
    });
}

#[test]
fn test_map_dependent_scoping() {
    "
    pub def one {
        rel .'total_weight': i64
        rel .'net_weight': i64
    }
    pub def two {
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
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("one", "two"),
            json!({ "total_weight": 100, "net_weight": 75 }),
            json!({ "net_weight": 75, "container_weight": 25 }),
        );
        test.mapper().assert_map_eq(
            ("two", "one"),
            json!({ "net_weight": 75, "container_weight": 25 }),
            json!({ "total_weight": 100, "net_weight": 75 }),
        );
    });
}

#[test]
fn test_seq_scope_escape1() {
    "
    def foo {}

    pub def bar {
        rel .'foo': foo
        rel .'p1': [text]
    }

    pub def baz {
        rel .'foo': foo
        rel .'p1': [text]
    }

    pub def qux {
        rel .'baz': baz
    }

    map {
        bar match {
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
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("bar", "qux"),
            json!({ "foo": {}, "p1": ["1"] }),
            json!({ "baz": { "foo": {}, "p1": ["1"] } }),
        );
    });
}

#[test]
fn test_seq_scope_escape2() {
    "
    def foo {
        rel .'p0': [text]
    }

    pub def bar {
        rel .'foo': foo
        rel .'p1': [text]
    }

    pub def baz {
        rel .'foo': foo
        rel .'p1': [text]
    }

    pub def qux {
        rel .'baz': baz
    }

    map {
        bar match {
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
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("bar", "qux"),
            json!({ "foo": { "p0": ["0"] }, "p1": ["1"] }),
            json!({ "baz": { "foo": { "p0": ["0"] }, "p1": ["1"] } }),
        );
    });
}
