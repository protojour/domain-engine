use ontol_runtime::{
    env::Env,
    property_probe::{self, PropertyProbe},
    translate::Translator,
};
use serde_json::json;
use test_log::test;

use crate::{util::TypeBinding, TestCompile, TEST_PKG};

fn assert_translate(
    env: &Env,
    translation: (&str, &str),
    input: serde_json::Value,
    expected: serde_json::Value,
) {
    let input_binding = TypeBinding::new(env, translation.0);
    let output_binding = TypeBinding::new(env, translation.1);

    let value = input_binding.deserialize_value(input).unwrap();

    let procedure = match env.get_translator(
        input_binding.type_info.def_id,
        output_binding.type_info.def_id,
    ) {
        Some(procedure) => procedure,
        None => panic!(
            "No translator found for ({:?}, {:?})",
            input_binding.type_info.def_id, output_binding.type_info.def_id
        ),
    };

    let mut translator = Translator::new(&env.lib);
    let value = translator.trace_eval(procedure, [value]);

    let output_json = output_binding.serialize_json(&value);

    assert_eq!(expected, output_json);
}

#[test]
fn test_eq_simple() {
    "
    type foo
    type bar
    rel foo { 'f' } string
    rel bar { 'b' } string
    eq (:x) {
        foo {
            rel { 'f' } :x
        }
        bar {
            rel { 'b' } :x
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(
            env,
            ("foo", "bar"),
            json!({ "f": "my_value"}),
            json!({ "b": "my_value"}),
        );
        assert_translate(
            env,
            ("bar", "foo"),
            json!({ "b": "my_value"}),
            json!({ "f": "my_value"}),
        );
    })
}

#[test]
fn test_meters() {
    "
    type meters
    type millimeters
    rel . { int } meters
    rel . { int } millimeters
    eq (:x) {
        meters { :x / 1000 }
        millimeters { :x }
    }
    "
    .compile_ok(|env| {
        assert_translate(env, ("meters", "millimeters"), json!(5), json!(5000));
        assert_translate(env, ("millimeters", "meters"), json!(5000), json!(5));
    })
}

#[test]
fn test_temperature() {
    "
    type celsius {
        rel . { int }
    }
    type fahrenheit {
        rel . { int }
    }

    eq (:x) {
        celsius { :x }
        fahrenheit { :x * 9 / 5 + 32 }
    }
    "
    .compile_ok(|env| {
        assert_translate(env, ("celsius", "fahrenheit"), json!(10), json!(50));
        assert_translate(env, ("fahrenheit", "celsius"), json!(50), json!(10));
    })
}

#[test]
fn test_eq_value_to_map() {
    "
    type one
    type two
    rel . { string } one
    rel two { 'a' } string
    eq (:x) {
        one { :x }
        two {
            rel { 'a' } :x
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(env, ("one", "two"), json!("foo"), json!({ "a": "foo" }));
        assert_translate(env, ("two", "one"), json!({ "a": "foo" }), json!("foo"));
    })
}

#[test]
fn test_eq_value_to_map_func() {
    "
    type one
    type two
    rel . { int } one
    rel two { 'a' } int
    eq (:x) {
        one { :x }
        two {
            rel { 'a' } :x * 2
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(env, ("one", "two"), json!(2), json!({ "a": 4 }));
        assert_translate(env, ("two", "one"), json!({ "a": 4 }), json!(2));
    })
}

#[test]
fn test_eq_simple_array() {
    "
    type foo
    type bar
    rel foo { 'a'* } int
    rel bar { 'b'* } int
    eq (:x) {
        foo {
            rel { 'a' } :x
        }
        bar {
            rel { 'b' } :x
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(
            env,
            ("foo", "bar"),
            json!({ "a": [42] }),
            json!({ "b": [42] }),
        );
        assert_translate(
            env,
            ("bar", "foo"),
            json!({ "b": [42] }),
            json!({ "a": [42] }),
        );
    })
}

#[test]
fn test_eq_complex_flow() {
    // FIXME: This should be a one-way translation.
    // there is no way two variables (e.g. `two.a` and `two.c`) can flow back into the same slot without data loss.
    // But perhaps let's accept that this might be what the user wants.
    // For example, when two `:x`es flow into one property, we can choose the first one.
    "
    type one {
        rel { 'a' } string
        rel { 'b' } string
    }
    type two {
        rel { 'a' } string
        rel { 'b' } string
        rel { 'c' } string
        rel { 'd' } string
    }

    eq(:x :y) {
        one {
            rel { 'a' } :x
            rel { 'b' } :y
        }
        two {
            rel { 'a' } :x
            rel { 'b' } :y
            rel { 'c' } :x
            rel { 'd' } :y
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(
            env,
            ("one", "two"),
            json!({ "a": "A", "b": "B" }),
            json!({ "a": "A", "b": "B", "c": "A", "d": "B" }),
        );

        // FIXME: Property probe does not make completely sense for this translation:
        let domain = env.get_domain(&TEST_PKG).unwrap();
        let mut property_probe = PropertyProbe::new(&env.lib);
        let property_map = property_probe
            .probe_from_serde_operator(
                env,
                domain.types.get("one").unwrap(),
                domain.types.get("two").unwrap(),
            )
            .unwrap();
        println!("{property_map:?}");
    })
}

#[test]
fn test_eq_delegation() {
    "
    type meters {
        rel . { int }
    }
    type millimeters {
        rel . { int }
    }

    eq(:m) {
        meters { :m }
        millimeters { :m * 1000 }
    }

    type car {
        rel { 'length' } meters
    }

    type vehicle {
        rel { 'length' } millimeters
    }
    
    eq(:l) {
        car {
            rel { 'length' } :l
        }
        vehicle {
            rel { 'length' } :l
        }
    }
    "
    .compile_ok(|env| {
        assert_translate(
            env,
            ("car", "vehicle"),
            json!({ "length": 3 }),
            json!({ "length": 3000 }),
        );
        assert_translate(
            env,
            ("vehicle", "car"),
            json!({ "length": 2000 }),
            json!({ "length": 2 }),
        );
    })
}
