use assert_matches::assert_matches;
use ontol_lang::{binding::InstantiateError, env::Env, Value};
use serde_json::json;

use crate::{TestCompile, TEST_PKG};

fn instantiate(
    env: &Env,
    type_name: &str,
    json: serde_json::Value,
) -> Result<Value, InstantiateError> {
    env.new_binding(TEST_PKG).instantiate_json(type_name, json)
}

#[test]
fn instantiate_empty_type_expects_empty_object() {
    "(type! foo)".compile_ok(|env| {
        assert_matches!(
            instantiate(&env, "foo", json!(42)),
            Err(InstantiateError::ExpectedEmptyObject)
        );
        assert_matches!(
            instantiate(&env, "foo", json!({ "bar": 5 })),
            Err(InstantiateError::ExpectedEmptyObject)
        );
        assert_matches!(
            instantiate(&env, "foo", json!({})),
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
    .compile_ok(|env| {
        assert_matches!(
            instantiate(&env, "foo", json!(42)),
            Err(InstantiateError::ExpectedEmptyObject)
        );
        assert_matches!(
            instantiate(&env, "foo", json!({ "bar": 5 })),
            Err(InstantiateError::ExpectedEmptyObject)
        );
        assert_matches!(
            instantiate(&env, "foo", json!({})),
            Ok(Value::Compound(attrs)) if attrs.is_empty()
        );
    });
}
