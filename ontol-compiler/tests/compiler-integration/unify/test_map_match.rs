use indoc::indoc;
use ontol_runtime::{format_utils::Literal, value::Value};
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
                .next_call(matching!(eq!(&Literal(indoc! { r#"
                    (root $b)
                    (attr $b S:1:4 (_ Text("input")))
                    "#
                }))))
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
                .next_call(matching!(eq!(&Literal(indoc! { "
                    (root $c)
                    "
                }))))
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

#[test]
fn test_map_match_anonymous_query_mandatory_parameters() {
    r#"
    pub def key { rel .is: text }
    pub def foo {
        rel .'key'|id: key
        rel .'prop_a': text
        rel .'prop_b': text
    }
    map query {
        {
            'input_a': a
            'input_b': b
        }
        foo: [..foo match {
            'prop_a': a
            'prop_b': b
        }]
    }
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper(
            YielderMock::yield_match
                .next_call(matching!(eq!(&Literal(indoc! { r#"
                    (root $e)
                    (attr $e S:1:6 (_ Text("A")))
                    (attr $e S:1:7 (_ Text("B")))
                    "#
                }))))
                .returns(Value::sequence_of([foo
                    .value_builder(json!({ "key": "key", "prop_a": "a!", "prop_b": "b!" }))
                    .into()])),
        )
        .assert_named_forward_map(
            "query",
            json!({
                "input_a": "A",
                "input_b": "B"
            }),
            json!([
                {
                    "key": "key",
                    "prop_a": "a!",
                    "prop_b": "b!"
                }
            ]),
        );
    });
}

#[test]
fn test_map_match_anonymous_with_translation() {
    r#"
    pub def key { rel .is: text }
    pub def foo {
        rel .'key'|id: key
        rel .'foo': text
    }
    pub def bar {
        rel .'bar': text
    }
    map {
        foo match { 'foo': x }
        bar { 'bar': x }
    }
    map query {
        { 'input': x }
        bar: [..foo match {
            'foo': x
        }]
    }
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper(
            YielderMock::yield_match
                .next_call(matching!(eq!(&Literal(indoc! { r#"
                    (root $d)
                    (attr $d S:1:6 (_ Text("X")))
                    "#
                }))))
                .returns(Value::sequence_of([foo
                    .value_builder(json!({ "key": "key", "foo": "x!" }))
                    .into()])),
        )
        .assert_named_forward_map(
            "query",
            json!({ "input": "X", }),
            json!([
                { "bar": "x!", }
            ]),
        );
    });
}
