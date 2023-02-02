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

    let value = input_binding.deserialize_value(env, input).unwrap();

    let procedure = match env.get_translator(input_binding.def_id, output_binding.def_id) {
        Some(procedure) => procedure,
        None => panic!(
            "No translator found for ({:?}, {:?})",
            input_binding.def_id, output_binding.def_id
        ),
    };

    let mut translator = Translator::new(&env.lib);
    let value = translator.trace_eval(procedure, [value]);

    let output_json = output_binding.serialize_json(env, &value);

    assert_eq!(expected, output_json);
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
            (_ (/ :x 1000))
        )
        (obj! millimeters
            (_ :x)
        )
    )
    "
    .compile_ok(|env| {
        assert_translate(env, ("meters", "millimeters"), json!(5), json!(5000));
        assert_translate(env, ("millimeters", "meters"), json!(5000), json!(5));
    })
}

#[test]
#[ignore = "support this!"]
fn test_temperature() {
    "
    (type! celsius)
    (type! fahrenheit)
    (rel! (celsius) _ (number))
    (rel! (fahrenheit) _ (number))
    (eq! (:x)
        (obj! celsius
            (_ (+ (* :x (/ 9 5)) 32))
        )
        (obj! fahrenheit
            (_ :x)
        )
    )
    "
    .compile_ok(|env| {
        assert_translate(env, ("c", "f"), json!(10), json!(50));
        assert_translate(env, ("f", "c"), json!(50), json!(10));
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
#[ignore = "find a solution for default values"]
fn test_eq_default() {
    r#"
    (type! default)
    (rel! (default) _ (string))
    (rel! (default) _)
    (eq! (:x)
        (obj! default)
        (obj! default (_ "default value!"))
    )
    "#
    .compile_ok(|env| {})
}

#[test]
fn test_eq_complex_flow() {
    // FIXME: This should be a one-way translation.
    // there is no way two variables (e.g. `two.a` and `two.c`) can flow back into the same slot without data loss.
    // But perhaps let's accept that this might be what the user wants.
    // For example, when two `:x`es flow into one property, we can choose the first one.
    r#"
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
    "#
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
