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

    let entry_point = env
        .get_translator(input_binding.def_id, output_binding.def_id)
        .unwrap();

    let mut vm = Vm::new(&env.program);

    let value = vm.eval(entry_point, vec![value]);

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
    .compile_ok(|env| {})
}
