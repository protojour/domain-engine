use assert_matches::assert_matches;
use ontol_lang::{env::Env, Value};
use serde::de::DeserializeSeed;
use serde_json::json;

use crate::{TestCompile, TEST_PKG};

fn instantiate(
    env: &mut Env,
    type_name: &str,
    json: serde_json::Value,
) -> Result<Value, serde_json::Error> {
    let domain_binding = env.bindings_builder().new_binding(TEST_PKG);
    let operator = domain_binding
        .get_serde_operator(type_name)
        .expect("No serde operator available");
    let json_string = serde_json::to_string(&json).unwrap();
    operator.deserialize(&mut serde_json::Deserializer::from_str(&json_string))
}

macro_rules! assert_error_msg {
    ($e:expr, $msg:expr) => {
        match $e {
            Ok(v) => panic!("Expected error, was {v:?}"),
            Err(e) => {
                let msg = format!("{e}");
                assert_eq!(msg.as_str(), $msg);
            }
        }
    };
}

#[test]
fn instantiate_empty_type_expects_empty_object() {
    "(type! foo)".compile_ok(|mut env| {
        assert_error_msg!(
            instantiate(&mut env, "foo", json!(42)),
            "invalid type: integer `42`, expected type foo at line 1 column 2"
        );
        assert_error_msg!(
            instantiate(&mut env, "foo", json!({ "bar": 5 })),
            "unknown property `bar` at line 1 column 6"
        );
        assert_matches!(
            instantiate(&mut env, "foo", json!({})),
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
        assert_error_msg!(instantiate(&mut env, "foo", json!(42)), "fdjskfdsljkl");
        assert_error_msg!(instantiate(&mut env, "foo", json!({ "bar": 5 })), "");
        assert_matches!(
            instantiate(&mut env, "foo", json!({})),
            Ok(Value::Compound(attrs)) if attrs.is_empty()
        );
    });
}
