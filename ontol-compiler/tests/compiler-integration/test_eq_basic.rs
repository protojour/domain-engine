use ontol_runtime::{env::Env, vm::Vm};
use serde_json::json;

use crate::{util::TypeBinding, TestCompile};

fn assert_translate(
    env: &Env,
    translation: (&str, &str),
    input: serde_json::Value,
    expected: serde_json::Value,
) {
    let input_binding = TypeBinding::new(env, translation.0);
    let output_binding = TypeBinding::new(env, translation.1);

    let value = input_binding.deserialize(env, input).unwrap();

    let entry_point = match env.get_translator(input_binding.def_id, output_binding.def_id) {
        Some(entry_point) => entry_point,
        None => panic!(
            "No translator found for ({:?}, {:?})",
            input_binding.def_id, output_binding.def_id
        ),
    };

    let mut vm = Vm::new(&env.program);

    let value = vm.eval_log(entry_point, vec![value]);

    let output_json = output_binding.serialize_json(env, &value);

    assert_eq!(output_json, expected);
}

#[test]
fn test_eq_simple() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) f (string))
    (rel! (bar) b (string))
    (eq! (:x)
        (obj! foo
            (f :x)
        )
        (obj! bar
            (b :x)
        )
    )
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
    (type! meters)
    (type! millimeters)
    (rel! (meters) _ (number))
    (rel! (millimeters) _ (number))
    (eq! (:x)
        (obj! meters
            (_ :x)
        )
        (obj! millimeters
            (_ (* :x 1000))
        )
    )
    "
    .compile_ok(|env| {
        assert_translate(env, ("meters", "millimeters"), json!(5), json!(5000));
        assert_translate(env, ("millimeters", "meters"), json!(5000), json!(5));
    })
}

#[test]
fn test_eq_value_to_map() {
    "
    (type! one)
    (type! two)
    (rel! (one) _ (string))
    (rel! (two) a (string))
    (eq! (:x)
        (obj! one (_ :x))
        (obj! two (a :x))
    )
    "
    .compile_ok(|env| {
        assert_translate(env, ("one", "two"), json!("foo"), json!({ "a": "foo" }));
        assert_translate(env, ("two", "one"), json!({ "a": "foo" }), json!("foo"));
    })
}

#[test]
fn test_eq_value_to_map_func() {
    "
    (type! one)
    (type! two)
    (rel! (one) _ (number))
    (rel! (two) a (number))
    (eq! (:x)
        (obj! one (_ :x))
        (obj! two (a (* :x 2)))
    )
    "
    .compile_ok(|env| {
        assert_translate(env, ("one", "two"), json!(2), json!({ "a": 4 }));
        assert_translate(env, ("two", "one"), json!({ "a": 4 }), json!(2));
    })
}

#[test]
fn test_eq_complex_flow() {
    // FIXME: This should be a one-way translation.
    // there is no way two variables (e.g. `two.a` and `two.c`) can flow back into the same slot without data loss.
    // But perheps let's accept that this might be what the user wants.
    // For example, when two `:x`es flow into one property, we can choose the first one.
    "
    (type! one)
    (type! two)
    (rel! (one) a (string))
    (rel! (one) b (string))
    (rel! (two) a (string))
    (rel! (two) b (string))
    (rel! (two) c (string))
    (rel! (two) d (string))
    (eq! (:x :y)
        (obj! one (a :x) (b :y))
        (obj! two (a :x) (b :y) (c :x) (d :y))
    )
    "
    .compile_ok(|env| {
        assert_translate(
            env,
            ("one", "two"),
            json!({ "a": "A", "b": "B" }),
            json!({ "a": "A", "b": "B", "c": "A", "d": "B" }),
        );
    })
}
