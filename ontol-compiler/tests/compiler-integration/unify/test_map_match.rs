use indoc::indoc;
use ontol_runtime::{format_utils::Literal, ontology::ValueCardinality, value::Value};
use ontol_test_utils::{test_map::YielderMock, TestCompile};
use serde_json::json;
use test_log::test;
use unimock::{matching, MockFn};

// BUG: This should work somehow
#[test]
fn test_map_match_non_inherent_id() {
    r#"
    def key (rel .is: text)
    def ent (rel .id: key)
    map(
        key: key,
        ent match( // ERROR no properties expected
            id: key
        )
    )
    "#
    .compile_fail();
}

#[test]
fn test_map_match_scalar_key() {
    r#"
    def key (rel .is: text)
    def foo (
        rel .'key'|id: key
        rel .'prop': text
    )
    map query(
        key: key,
        foo match('key': key),
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::One),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-entity $a def@1:2)
                            (attr $a S:1:4 (_ 'input'))
                        "#
                        }))
                    ))
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
    def key (rel .is: text)
    def foo (
        rel .'key'|id: key
        rel .'prop': text
    )
    map query(
        (),
        foo: { ..foo match() }
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::Many),
                        eq!(&Literal(indoc! { "
                            (root $a)
                            (is-entity $a def@1:2)
                        "
                        }))
                    ))
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
fn test_map_match_query_mandatory_properties() {
    r#"
    def key (rel .is: text)
    def foo (
        rel .'key'|id: key
        rel .'prop_a': text
        rel .'prop_b': text
    )
    map query(
        (
            'input_a': a,
            'input_b': b,
        ),
        foo: { ..foo match(
            'prop_a': a,
            'prop_b': b,
        )},
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::Many),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-entity $a def@1:2)
                            (attr $a S:1:6 (_ 'A'))
                            (attr $a S:1:7 (_ 'B'))
                        "#
                        }))
                    ))
                    .returns(Value::sequence_of([foo
                        .value_builder(json!({ "key": "key", "prop_a": "a!", "prop_b": "b!" }))
                        .into()])),
            )
            .assert_named_forward_map(
                "query",
                json!({ "input_a": "A", "input_b": "B" }),
                json!([{ "key": "key", "prop_a": "a!", "prop_b": "b!" }]),
            );
    });
}

#[test]
fn test_map_match_query_optional_property() {
    r#"
    def key (rel .is: text)
    def foo (
        rel .'key'|id: key
        rel .'prop_a': text
        rel .'prop_b': text
    )
    map query(
        (
            'input_a': a,
            'input_b'?: b,
        ),
        foo: {..foo match(
            'prop_a': a,
            'prop_b'?: b,
        )}
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        let return_value = Value::sequence_of([foo
            .value_builder(json!({ "key": "key", "prop_a": "a!", "prop_b": "b!" }))
            .into()]);

        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::Many),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-entity $a def@1:2)
                            (attr $a S:1:6 (_ 'A'))
                        "#
                        }))
                    ))
                    .returns(return_value.clone()),
            )
            .assert_named_forward_map(
                "query",
                json!({ "input_a": "A", }),
                json!([{ "key": "key", "prop_a": "a!", "prop_b": "b!" }]),
            );

        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::Many),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-entity $a def@1:2)
                            (attr $a S:1:6 (_ 'A'))
                            (attr $a S:1:7 (_ 'B'))
                        "#
                        }))
                    ))
                    .returns(return_value),
            )
            .assert_named_forward_map(
                "query",
                json!({ "input_a": "A", "input_b": "B", }),
                json!([{ "key": "key", "prop_a": "a!", "prop_b": "b!" }]),
            );
    });
}

#[test]
fn test_map_match_anonymous_with_translation() {
    r#"
    def key (rel .is: text)
    def foo (
        rel .'key'|id: key
        rel .'foo': text
    )
    def bar (
        rel .'bar': text
    )
    map(
        foo match('foo': x),
        bar('bar': x),
    )
    map query(
        ('input': x),
        bar: {..foo match(
            'foo': x
        )}
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::Many),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-entity $a def@1:2)
                            (attr $a S:1:7 (_ 'X'))
                        "#
                        }))
                    ))
                    .returns(Value::sequence_of([foo
                        .value_builder(json!({ "key": "key", "foo": "x!" }))
                        .into()])),
            )
            .assert_named_forward_map("query", json!({ "input": "X", }), json!([{ "bar": "x!", }]));
    });
}

#[test]
fn test_map_sequence_filter_in_set() {
    r#"
    def key (rel .is: text)
    def foo (
        rel .'key'|id: key
        rel .'foo': text
    )
    def bar (
        rel .'bar': text
    )
    map(
        foo match('foo': x),
        bar('bar': x),
    )
    map query(
        ('input': { ..x }),
        bar: {
            ..foo match('foo': in { ..x })
        }
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::Many),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-entity $a def@1:2)
                            (match-prop $a S:1:7 (element-in $b))
                            (member $b (_ 'X'))
                            (member $b (_ 'Y'))
                        "#
                        }))
                    ))
                    .returns(Value::sequence_of([foo
                        .value_builder(json!({ "key": "key", "foo": "x!" }))
                        .into()])),
            )
            .assert_named_forward_map(
                "query",
                json!({ "input": ["X", "Y"], }),
                json!([{ "bar": "x!", }]),
            );
    });
}
