use assert_matches::assert_matches;
use ontol_lang::{binding::DomainBinding, env::Env, serde::SerdeOperator, Value};
use serde::de::DeserializeSeed;
use serde_json::json;
use smartstring::alias::String;

use crate::{assert_error_msg, TestCompile, TEST_PKG};

struct TypeBinding<'m> {
    domain_binding: DomainBinding<'m>,
    type_name: String,
}

impl<'m> TypeBinding<'m> {
    fn new(env: &mut Env<'m>, type_name: &str) -> Self {
        let binding = Self {
            domain_binding: env.bindings_builder().new_binding(TEST_PKG),
            type_name: type_name.into(),
        };
        println!(
            "deserializing `{type_name}` with operator {:?}",
            binding.operator(env)
        );
        binding
    }

    fn operator<'e>(&self, env: &'e Env<'m>) -> SerdeOperator<'e, 'm> {
        self.domain_binding
            .get_serde_operator(env, &self.type_name)
            .expect("No serde operator available")
    }

    fn deserialize<'e>(
        &self,
        env: &'e Env<'m>,
        json: serde_json::Value,
    ) -> Result<Value, serde_json::Error> {
        let json_string = serde_json::to_string(&json).unwrap();
        self.operator(env)
            .deserialize(&mut serde_json::Deserializer::from_str(&json_string))
    }
}

#[test]
fn deserialize_empty_type() {
    "(type! foo)".compile_ok(|mut env| {
        let foo = TypeBinding::new(&mut env, "foo");
        assert_error_msg!(
            foo.deserialize(&env, json!(42)),
            "invalid type: integer `42`, expected type `foo` at line 1 column 2"
        );
        assert_error_msg!(
            foo.deserialize(&env, json!({ "bar": 5 })),
            "unknown property `bar` at line 1 column 6"
        );
        assert_matches!(
            foo.deserialize(&env, json!({})),
            Ok(Value::Compound(attrs)) if attrs.is_empty()
        );
    });
}

#[test]
fn deserialize_number() {
    "
    (type! foo)
    (rel! (foo) _ (number))
    "
    .compile_ok(|mut env| {
        let foo = TypeBinding::new(&mut env, "foo");
        assert_matches!(foo.deserialize(&env, json!(42)), Ok(Value::Number(42)));
        assert_matches!(foo.deserialize(&env, json!(-42)), Ok(Value::Number(-42)));

        assert_error_msg!(
            foo.deserialize(&env, json!({})),
            "invalid type: map, expected number at line 1 column 0"
        );
        assert_error_msg!(
            foo.deserialize(&env, json!("boom")),
            "invalid type: string \"boom\", expected number at line 1 column 6"
        );
    });
}

#[test]
fn deserialize_string() {
    "
    (type! foo)
    (rel! (foo) _ (string))
    "
    .compile_ok(|mut env| {
        let foo = TypeBinding::new(&mut env, "foo");
        assert_matches!(
            foo.deserialize(&env, json!("hei")),
            Ok(Value::String(s)) if s == "hei"
        );

        assert_error_msg!(
            foo.deserialize(&env, json!({})),
            "invalid type: map, expected string at line 1 column 0"
        );
    });
}

#[test]
fn deserialize_object_properties() {
    "
    (type! obj)
    (rel! (obj) a (string))
    (rel! (obj) b (number))
    "
    .compile_ok(|mut env| {
        let obj = TypeBinding::new(&mut env, "obj");
        assert_matches!(
            obj.deserialize(&env, json!({ "a": "hei", "b": 42 })),
            Ok(Value::Compound(_))
        );

        assert_error_msg!(
            obj.deserialize(&env, json!({ "a": "hei", "b": 42, "c": false })),
            "unknown property `c` at line 1 column 21"
        );
        assert_error_msg!(
            obj.deserialize(&env, json!({})),
            "missing properties, expected `a` and `b` at line 1 column 2"
        );
    });
}

#[test]
fn deserialize_nested() {
    "
    (type! one)
    (type! two)
    (type! three)
    (rel! (one) two (two))
    (rel! (one) three (three))
    (rel! (two) three (three))
    (rel! (three) _ (string))
    "
    .compile_ok(|mut env| {
        let one = TypeBinding::new(&mut env, "one");
        assert_matches!(
            one.deserialize(&env, json!({
                "two": {
                    "three": "a"
                },
                "three": "b"
            })),
            Ok(Value::Compound(a)) if a.len() == 2
        );
    });
}

#[test]
fn deserialize_recursive() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) b (bar))
    (rel! (bar) f (foo))
    "
    .compile_ok(|mut env| {
        let foo = TypeBinding::new(&mut env, "foo");
        assert_error_msg!(
            foo.deserialize(
                &env,
                json!({
                    "b": {
                        "f": {
                            "b": 42
                        }
                    },
                })
            ),
            "invalid type: integer `42`, expected type `bar` at line 1 column 17"
        );
    });
}
