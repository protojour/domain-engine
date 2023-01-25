use assert_matches::assert_matches;
use ontol_lang::{binding::DomainBinding, env::Env, Value};
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
        Self {
            domain_binding: env.bindings_builder().new_binding(TEST_PKG),
            type_name: type_name.into(),
        }
    }

    fn deserialize(&self, json: serde_json::Value) -> Result<Value, serde_json::Error> {
        let operator = self
            .domain_binding
            .get_serde_operator(&self.type_name)
            .expect("No serde operator available");
        let json_string = serde_json::to_string(&json).unwrap();
        operator.deserialize(&mut serde_json::Deserializer::from_str(&json_string))
    }
}

#[test]
fn instantiate_empty_type_expects_empty_object() {
    "(type! foo)".compile_ok(|mut env| {
        let binding = TypeBinding::new(&mut env, "foo");
        assert_error_msg!(
            binding.deserialize(json!(42)),
            "invalid type: integer `42`, expected type `foo` at line 1 column 2"
        );
        assert_error_msg!(
            binding.deserialize(json!({ "bar": 5 })),
            "unknown property `bar` at line 1 column 6"
        );
        assert_matches!(
            binding.deserialize(json!({})),
            Ok(Value::Compound(attrs)) if attrs.is_empty()
        );
    });
}

#[test]
#[ignore]
fn instantiate_anonymous() {
    "
    (type! foo)
    (rel! (foo) _ (number))
    "
    .compile_ok(|mut env| {
        let binding = TypeBinding::new(&mut env, "foo");
        assert_error_msg!(binding.deserialize(json!(42)), "fdjskfdsljkl");
        assert_error_msg!(binding.deserialize(json!({ "bar": 5 })), "");
        assert_matches!(
            binding.deserialize(json!({})),
            Ok(Value::Compound(attrs)) if attrs.is_empty()
        );
    });
}
