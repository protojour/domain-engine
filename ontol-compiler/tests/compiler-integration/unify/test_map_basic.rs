use crate::unify::{assert_domain_map, IntoKey};
use ontol_test_utils::{SourceName, TestCompile, TestPackages};
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
    .compile_ok(|env| {
        assert_domain_map(
            &env,
            ("foo", "bar"),
            json!({ "f": "my_value"}),
            json!({ "b": "my_value"}),
        );
        assert_domain_map(
            &env,
            ("bar", "foo"),
            json!({ "b": "my_value"}),
            json!({ "f": "my_value"}),
        );
    });
}

#[test]
fn test_meters() {
    "
    pub type meters
    pub type millimeters
    rel meters is: int
    rel millimeters is: int
    map {
        meters: x / 1000
        millimeters: x
    }
    "
    .compile_ok(|env| {
        assert_domain_map(&env, ("meters", "millimeters"), json!(5), json!(5000));
        assert_domain_map(&env, ("millimeters", "meters"), json!(5000), json!(5));
    });
}

#[test]
fn test_temperature() {
    "
    pub type celsius {
        rel .is: int
    }
    pub type fahrenheit {
        rel .is: int
    }

    map {
        celsius: x
        fahrenheit: x * 9 / 5 + 32
    }
    "
    .compile_ok(|env| {
        assert_domain_map(&env, ("celsius", "fahrenheit"), json!(10), json!(50));
        assert_domain_map(&env, ("fahrenheit", "celsius"), json!(50), json!(10));
    });
}

#[test]
fn test_nested_optional_attribute() {
    "
    type seconds {
        rel .is: int
    }
    type years {
        rel .is: int
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
    .compile_ok(|env| {
        assert_domain_map(&env, ("person", "creature"), json!({}), json!({}));
        assert_domain_map(
            &env,
            ("person", "creature"),
            json!({ "age": 42 }),
            json!({ "age": 1324512000 }),
        );
        assert_domain_map(
            &env,
            ("person_container", "creature_container"),
            json!({}),
            json!({}),
        );
        assert_domain_map(
            &env,
            ("person_container", "creature_container"),
            json!({ "person": {} }),
            json!({ "creature": {} }),
        );
        assert_domain_map(
            &env,
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
    .compile_ok(|env| {
        assert_domain_map(&env, ("one", "two"), json!("foo"), json!({ "a": "foo" }));
        assert_domain_map(&env, ("two", "one"), json!({ "a": "foo" }), json!("foo"));
    });
}

#[test]
fn test_map_value_to_map_func() {
    "
    pub type one
    pub type two
    rel one is: int
    rel two 'a': int
    map {
        one: x
        two {
            'a': x * 2
        }
    }
    "
    .compile_ok(|env| {
        assert_domain_map(&env, ("one", "two"), json!(2), json!({ "a": 4 }));
        assert_domain_map(&env, ("two", "one"), json!({ "a": 4 }), json!(2));
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
    .compile_ok(|test_env| {
        assert_domain_map(
            &test_env,
            ("foo", "bar"),
            json!({ "a": "A", "inner": { "b": "B", "c": "C" }}),
            json!({ "a": "A", "b": "B", "inner": { "c": "C" }}),
        );
        assert_domain_map(
            &test_env,
            ("bar", "foo"),
            json!({ "a": "A", "b": "B", "inner": { "c": "C" }}),
            json!({ "a": "A", "inner": { "b": "B", "c": "C" }}),
        );
    });
}

#[test]
fn test_map_matching_array() {
    "
    pub type foo
    pub type bar
    rel foo 'a': [int]
    rel bar 'b': [int]
    map {
        foo {
            'a': [x]
        }
        bar {
            'b': [x]
        }
    }
    "
    .compile_ok(|env| {
        assert_domain_map(
            &env,
            ("foo", "bar"),
            json!({ "a": [42] }),
            json!({ "b": [42] }),
        );
        assert_domain_map(
            &env,
            ("bar", "foo"),
            json!({ "b": [42] }),
            json!({ "a": [42] }),
        );
    });
}

// map call inside array
const MAP_IN_ARRAY: &str = "
type foo { rel .'f': string }
type bar { rel .'b': string }
pub type foos { rel .'foos': [foo] }
pub type bars { rel .'bars': [bar] }

map {
    foos { 'foos': [x] }
    bars { 'bars': [x] }
}
map {
    foo { 'f': x }
    bar { 'b': x }
}
";

#[test]
fn test_map_in_array_item_empty() {
    MAP_IN_ARRAY.compile_ok(|env| {
        assert_domain_map(
            &env,
            ("foos", "bars"),
            json!({ "foos": [] }),
            json!({ "bars": [] }),
        );
        assert_domain_map(
            &env,
            ("bars", "foos"),
            json!({ "bars": [] }),
            json!({ "foos": [] }),
        );
    });
}

#[test]
fn test_map_in_array_item_one() {
    MAP_IN_ARRAY.compile_ok(|env| {
        assert_domain_map(
            &env,
            ("foos", "bars"),
            json!({ "foos": [{ "f": "42" }] }),
            json!({ "bars": [{ "b": "42" }] }),
        );
        assert_domain_map(
            &env,
            ("bars", "foos"),
            json!({ "bars": [{ "b": "42" }] }),
            json!({ "foos": [{ "f": "42" }] }),
        );
    });
}

#[test]
fn test_map_in_array_item_many() {
    MAP_IN_ARRAY.compile_ok(|env| {
        assert_domain_map(
            &env,
            ("foos", "bars"),
            json!({ "foos": [{ "f": "42" }, { "f": "84" }] }),
            json!({ "bars": [{ "b": "42" }, { "b": "84" }] }),
        );
        assert_domain_map(
            &env,
            ("bars", "foos"),
            json!({ "bars": [{ "b": "42" }, { "b": "84" }] }),
            json!({ "foos": [{ "f": "42" }, { "f": "84" }] }),
        );
    });
}

#[test]
fn test_aggr_cross_parallel() {
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
            'f1': [a]
            'f2': [b]
        }
        bars {
            'b2': [b]
            'b1': [a]
        }
    }
    "
    .compile_ok(|test_env| {
        assert_domain_map(
            &test_env,
            ("foos", "bars"),
            json!({ "f1": [{ "f": "1A" }, { "f": "1B" }], "f2": [{ "f": "2" }] }),
            json!({ "b1": [{ "b": "1A" }, { "b": "1B" }], "b2": [{ "b": "2" }] }),
        );
    });
}

#[test]
fn test_aggr_multi_level() {
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
            'a': [f0 {
                'a': [v0]
                'b': [v1]
            }]
            'b': [f0 {
                'a': [v2]
                'b': [v3]
            }]
        }
        b1 {
            'b': [b0 {
                'b': [v3]
                'a': [v2]
            }]
            'a': [b0 {
                'b': [v1]
                'a': [v0]
            }]
        }
    }
    "
    .compile_ok(|test_env| {
        assert_domain_map(
            &test_env,
            ("f1", "b1"),
            json!({ "a": [{ "a": [{ "P": "0" }], "b": [{ "P": "1" }]}], "b": [{ "a": [{ "P": "2" }], "b": [{ "P": "3" }]}]}),
            json!({ "a": [{ "a": [{ "Q": "0" }], "b": [{ "Q": "1" }]}], "b": [{ "a": [{ "Q": "2" }], "b": [{ "Q": "3" }]}]}),
        );
    });
}

#[test]
fn test_flat_map1() {
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
            'inner': [foo_inner {
                'b': b
            }]
        }
        [bar {
            'a': a
            'b': b
        }]
    }
    "
    .compile_ok(|test_env| {
        assert_domain_map(
            &test_env,
            ("foo", "bar".seq()),
            json!({ "a": "A", "inner": [{ "b": "B0" }, { "b": "B1" }] }),
            json!([{ "a": "A", "b": "B0" }, { "a": "A", "b": "B1" }]),
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
    .compile_ok(|test_env| {
        assert_domain_map(
            &test_env,
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
            use 'si' as si

            pub type car {
                rel .'length': si.meters
            }
            pub type vehicle {
                rel .'length': si.millimeters
            }

            map {
                car {
                    'length': l
                }
                vehicle {
                    'length': l
                }
            }
            ",
        ),
        (
            SourceName("si"),
            "
            pub type meters {
                rel .is: int
            }
            pub type millimeters {
                rel .is: int
            }

            map {
                meters: m
                millimeters: m * 1000
            }
            ",
        ),
    ])
    .compile_ok(|env| {
        assert_domain_map(
            &env,
            ("car", "vehicle"),
            json!({ "length": 3 }),
            json!({ "length": 3000 }),
        );
        assert_domain_map(
            &env,
            ("vehicle", "car"),
            json!({ "length": 2000 }),
            json!({ "length": 2 }),
        );
    });
}

#[test]
fn test_map_dependent_scoping() {
    "
    pub type one {
        rel .'total_weight': int
        rel .'net_weight': int
    }
    pub type two {
        rel .'net_weight': int
        rel .'container_weight': int
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
    .compile_ok(|test_env| {
        assert_domain_map(
            &test_env,
            ("one", "two"),
            json!({ "total_weight": 100, "net_weight": 75 }),
            json!({ "net_weight": 75, "container_weight": 25 }),
        );

        assert_domain_map(
            &test_env,
            ("two", "one"),
            json!({ "net_weight": 75, "container_weight": 25 }),
            json!({ "total_weight": 100, "net_weight": 75 }),
        );
    });
}
