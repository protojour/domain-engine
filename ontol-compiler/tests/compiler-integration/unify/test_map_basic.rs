use crate::unify::{assert_domain_map, IntoKey};
use ontol_test_utils::{SourceName, TestCompile, TestPackages};
use serde_json::json;
use test_log::test;
use tracing::debug;

#[test]
fn test_map_simple() {
    "
    pub type foo
    pub type bar
    rel foo 'f': string
    rel bar 'b': string
    map x {
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
    map x {
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
        rel _ is: int
    }
    pub type fahrenheit {
        rel _ is: int
    }

    map x {
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
fn test_optional_attribute() {
    "
    pub type person {
        rel _ 'age'?: int
    }
    pub type creature {
        rel _ 'years'?: int
    }

    unify x {
        person { 'age'?: x }
        creature { 'years'?: x }
    }

    pub type person_container {
        rel _ 'person'?: person
    }
    pub type creature_container {
        rel _ 'creature'?: creature
    }

    unify x {
        person_container {
            'person'?: person {
                'age'?: x
            }
        }
        creature_container {
            'creature'?: creature {
                'years'?: x
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
            json!({ "years": 42 }),
        );

        assert_domain_map(
            &env,
            ("person_container", "creature_container"),
            json!({}),
            json!({}),
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
    map x {
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
    map x {
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
        rel _ 'a': string
        rel _ 'inner': foo_inner
    }
    with foo_inner {
        rel _ 'b': string
        rel _ 'c': string
    }

    pub type bar
    type bar_inner

    with bar {
        rel _ 'a': string
        rel _ 'b': string
        rel _ 'inner': bar_inner
    }

    with bar_inner {
        rel _ 'c': string
    }

    map a b c {
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
    });
}

#[test]
fn test_map_matching_array() {
    "
    pub type foo
    pub type bar
    rel foo 'a': [int]
    rel bar 'b': [int]
    map x {
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
type foo { rel _ 'f': string }
type bar { rel _ 'b': string }
pub type foos { rel _ 'foos': [foo] }
pub type bars { rel _ 'bars': [bar] }

map x {
    foos { 'foos': [x] }
    bars { 'bars': [x] }
}
map x {
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
    type foo { rel _ 'f': string }
    type bar { rel _ 'b': string }
    map x {
        foo { 'f': x }
        bar { 'b': x }
    }

    pub type foos {
        rel _ 'f1': [foo]
        rel _ 'f2': [foo]
    }
    pub type bars {
        rel _ 'b1': [bar]
        rel _ 'b2': [bar]
    }
    map a b {
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
    type foo { rel _ 'P': string }
    type bar { rel _ 'Q': string }
    map x {
        foo { 'P': x }
        bar { 'Q': x }
    }

    pub type f0 {
        rel _ 'a': [foo]
        rel _ 'b': [foo]
    }
    pub type f1 {
        rel _ 'a': [f0]
        rel _ 'b': [f0]
    }
    pub type b0 {
        rel _ 'a': [bar]
        rel _ 'b': [bar]
    }
    pub type b1 {
        rel _ 'a': [b0]
        rel _ 'b': [b0]
    }
    map v0 v1 v2 v3 {
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
        rel _ 'a': string
        rel _ 'inner': [foo_inner]
    }
    with foo_inner {
        rel _ 'b': string
    }
    with bar {
        rel _ 'a': string
        rel _ 'b': string
    }

    map a b {
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
        rel _ 'a': string
        rel _ 'b': string
    }
    pub type two {
        rel _ 'a': string
        rel _ 'b': string
        rel _ 'c': string
        rel _ 'd': string
    }

    map x y {
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
            json!({ "a": "A", "b": "B" }),
            json!({ "a": "A", "b": "B", "c": "A", "d": "B" }),
        );

        // FIXME: Property probe does not make completely sense for this mapping:
        let domain = test_env.env.find_domain(test_env.root_package).unwrap();
        let mut property_probe = test_env.env.new_property_probe();
        let property_map = property_probe
            .probe_from_serde_operator(
                &test_env.env,
                domain.type_info(*domain.type_names.get("one").unwrap()),
                domain.type_info(*domain.type_names.get("two").unwrap()),
            )
            .unwrap();
        debug!("{property_map:?}");
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
                rel _ 'length': si.meters
            }
            pub type vehicle {
                rel _ 'length': si.millimeters
            }

            map l {
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
                rel _ is: int
            }
            pub type millimeters {
                rel _ is: int
            }

            map m {
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
