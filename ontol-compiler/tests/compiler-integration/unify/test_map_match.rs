use ontol_runtime::value::Value;
use ontol_test_utils::{test_map::YielderMock, TestCompile};
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
    map query {
        key: key
        foo match { 'key': key }
    }
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper(
            YielderMock::yield_match
                // BUG: Should have condition clauses here:
                .next_call(matching!(eq!("")))
                .returns(Value::from(
                    foo.value_builder(json!({ "key": "x", "prop": "y" })),
                )),
        )
        .assert_named_forward_map(
            "query",
            json!("input"),
            json!({
                "key": "x",
                "prop": "y"
            }),
        );
    });
}

#[test]
fn test_map_match_parameterless_query() {
    r#"
    pub def key { rel .is: text }
    pub def foo {
        rel .'key'|id: key
        rel .'prop': text
    }
    map query {
        {}
        foo: [..foo match {}]
    }
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper(
            YielderMock::yield_match
                .next_call(matching!(eq!("")))
                .returns(Value::sequence_of([foo
                    .value_builder(json!({ "key": "key", "prop": "test" }))
                    .into()])),
        )
        .assert_named_forward_map(
            "query",
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
