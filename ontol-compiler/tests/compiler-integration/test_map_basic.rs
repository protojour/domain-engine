use ontol_test_utils::{type_binding::TypeBinding, SourceName, TestCompile, TestEnv, TestPackages};
use serde_json::json;
use test_log::test;
use tracing::debug;

fn assert_translate(
    test_env: &TestEnv,
    translation: (&str, &str),
    input: serde_json::Value,
    expected: serde_json::Value,
) {
    let input_binding = TypeBinding::new(test_env, translation.0);
    let output_binding = TypeBinding::new(test_env, translation.1);

    let value = input_binding.deserialize_value(input).unwrap();

    let procedure = match test_env.env.get_translator(
        input_binding.type_info.def_id,
        output_binding.type_info.def_id,
    ) {
        Some(procedure) => procedure,
        None => panic!(
            "No translator found for ({:?}, {:?})",
            input_binding.type_info.def_id, output_binding.type_info.def_id
        ),
    };

    let mut translator = test_env.env.new_translator();
    let value = translator.trace_eval(procedure, [value]);

    let output_json = output_binding.serialize_json(&value);

    assert_eq!(expected, output_json);
}

#[test]
fn test_map_simple() {
    "
    pub type foo
    pub type bar
    rel foo 'f': string
    rel bar 'b': string
    map (x) {
        foo {
            rel 'f': x
        }
        bar {
            rel 'b': x
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(
            &env,
            ("foo", "bar"),
            json!({ "f": "my_value"}),
            json!({ "b": "my_value"}),
        );
        assert_translate(
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
    map (x) {
        meters { x / 1000 }
        millimeters { x }
    }
    "
    .compile_ok(|env| {
        assert_translate(&env, ("meters", "millimeters"), json!(5), json!(5000));
        assert_translate(&env, ("millimeters", "meters"), json!(5000), json!(5));
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

    map (x) {
        celsius { x }
        fahrenheit { x * 9 / 5 + 32 }
    }
    "
    .compile_ok(|env| {
        assert_translate(&env, ("celsius", "fahrenheit"), json!(10), json!(50));
        assert_translate(&env, ("fahrenheit", "celsius"), json!(50), json!(10));
    });
}

#[test]
fn test_map_value_to_map() {
    "
    pub type one
    pub type two
    rel one is: string
    rel two 'a': string
    map (x) {
        one { x }
        two {
            rel 'a': x
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(&env, ("one", "two"), json!("foo"), json!({ "a": "foo" }));
        assert_translate(&env, ("two", "one"), json!({ "a": "foo" }), json!("foo"));
    });
}

#[test]
fn test_map_value_to_map_func() {
    "
    pub type one
    pub type two
    rel one is: int
    rel two 'a': int
    map (x) {
        one { x }
        two {
            rel 'a': x * 2
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(&env, ("one", "two"), json!(2), json!({ "a": 4 }));
        assert_translate(&env, ("two", "one"), json!({ "a": 4 }), json!(2));
    });
}

#[test]
fn test_map_matching_array() {
    "
    pub type foo
    pub type bar
    rel foo ['a']: int
    rel bar ['b']: int
    map (x) {
        foo {
            rel 'a': x
        }
        bar {
            rel 'b': x
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(
            &env,
            ("foo", "bar"),
            json!({ "a": [42] }),
            json!({ "b": [42] }),
        );
        assert_translate(
            &env,
            ("bar", "foo"),
            json!({ "b": [42] }),
            json!({ "a": [42] }),
        );
    });
}

#[test]
// BUG: There are big codegen problems, missing good intermediate representations
// #[should_panic = "Value at Local(1) is not a sequence"]
fn test_map_translate_array_item() {
    "
    type foo { rel _ 'f': string }
    type bar { rel _ 'b': string }
    pub type foos { rel _ ['foos']: foo }
    pub type bars { rel _ ['bars']: bar }

    map (x) {
        foos {
            rel 'foos': x
        }
        bars {
            rel 'bars': x
        }
    }
    map (x) {
        foo {
            rel 'f': x
        }
        bar {
            rel 'b': x
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(
            &env,
            ("foos", "bars"),
            json!({ "foos": [{ "f": "42" }] }),
            // FIXME: actually implement the mapping:
            json!({ "bars": [] }),
        );
    });
}

#[test]
fn test_map_complex_flow() {
    // FIXME: This should be a one-way translation.
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

    map (x y) {
        one {
            rel 'a': x
            rel 'b': y
        }
        two {
            rel 'a': x
            rel 'b': y
            rel 'c': x
            rel 'd': y
        }
    }
    "
    .compile_ok(|test_env| {
        assert_translate(
            &test_env,
            ("one", "two"),
            json!({ "a": "A", "b": "B" }),
            json!({ "a": "A", "b": "B", "c": "A", "d": "B" }),
        );

        // FIXME: Property probe does not make completely sense for this translation:
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

            map (l) {
                car {
                    rel 'length': l
                }
                vehicle {
                    rel 'length': l
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

            map (m) {
                meters { m }
                millimeters { m * 1000 }
            }
            ",
        ),
    ])
    .compile_ok(|env| {
        assert_translate(
            &env,
            ("car", "vehicle"),
            json!({ "length": 3 }),
            json!({ "length": 3000 }),
        );
        assert_translate(
            &env,
            ("vehicle", "car"),
            json!({ "length": 2000 }),
            json!({ "length": 2 }),
        );
    });
}
