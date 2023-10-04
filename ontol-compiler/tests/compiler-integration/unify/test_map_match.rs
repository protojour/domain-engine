use ontol_runtime::value::{Data, Value};
use ontol_test_utils::{test_map::TestYieldMock, TestCompile};
use serde_json::json;
use test_log::test;
use unimock::{matching, MockFn};

// BUG: This should work somehow
#[test]
fn test_map_match_non_inherent_id() {
    r#"
    pub def key { rel .is: text }
    pub def ent { rel .id: key }
    map {
        key: key
        ent match { // ERROR no properties expected
            id: key
        }
    }
    "#
    .compile_fail();
}

#[test]
fn test_map_match_scalar_key() {
    r#"
    pub def key { rel .is: text }
    pub def foo {
        rel .'key'|id: key
        rel .'prop': text
    }
    map {
        key: key
        foo match { 'key': key }
    }
    "#
    .compile_then(|_test| {});
}

#[test]
fn test_map_match_parameterless_query() {
    r#"
    pub def key { rel .is: text }
    pub def foo {
        rel .'key'|id: key
        rel .'prop': text
    }
    map q {
        {}
        foo: [..foo match {}]
    }
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        let output: Value = foo
            .entity_builder(json!("key"), json!({"key": "key", "prop": "test"}))
            .into();

        test.yielding_mapper(
            TestYieldMock::process_yield
                .next_call(matching!(_))
                .returns(Value::new(
                    Data::Sequence(vec![output.into()]),
                    foo.def_id(),
                )),
        )
        .assert_named_forward_map(
            "q",
            json!({}),
            json!([
                {
                    "key": "key",
                    "prop": "test"
                }
            ]),
        );
    });
}
