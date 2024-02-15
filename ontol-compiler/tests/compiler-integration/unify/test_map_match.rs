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
                            (match-prop $a S:1:4 (element-in $b))
                            (member $b (_ 'input'))
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
                            (match-prop $a S:1:6 (element-in $b))
                            (match-prop $a S:1:7 (element-in $c))
                            (member $b (_ 'A'))
                            (member $c (_ 'B'))
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
                            (match-prop $a S:1:6 (element-in $b))
                            (member $b (_ 'A'))
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
                            (match-prop $a S:1:6 (element-in $b))
                            (match-prop $a S:1:7 (element-in $c))
                            (member $b (_ 'A'))
                            (member $c (_ 'B'))
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
                            (match-prop $a S:1:7 (element-in $b))
                            (member $b (_ 'X'))
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
fn test_map_match_sequence_filter_in_set() {
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

mod match_contains_all {
    use super::*;
    use test_log::test;

    const TEST_DOMAIN: &str = r#"
    def foo (
        rel .'key'|id: (rel .is: text)
        rel .'tags': {text}
        rel .'sub': sub
    )
    def sub (
        rel .'tags': {text}
    )
    map foos(
        (
            'foo_tags'?: { ..tags },
            'sub_tag'?: sub_tag
        ),
        foo: {
            ..foo match(
                'tags'?: contains all { ..tags },
                'sub'?: sub(
                    'tags': contains all { sub_tag }
                )
            )
        }
    )
    "#;

    fn foo_json() -> serde_json::Value {
        json!({ "key": "key", "tags": [], "sub": { "tags": [] } })
    }

    #[test]
    fn foo_tags() {
        let test = TEST_DOMAIN.compile();
        let [foo] = test.bind(["foo"]);
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::Many),
                        eq!(&Literal(indoc! { r#"
                                (root $a)
                                (is-entity $a def@1:1)
                                (match-prop $a S:1:7 (superset-of $b))
                                (member $b (_ 'x!'))
                                (member $b (_ 'y!'))
                            "#
                        }))
                    ))
                    .returns(Value::sequence_of([foo.value_builder(foo_json()).into()])),
            )
            .assert_named_forward_map(
                "foos",
                json!({ "foo_tags": ["x!", "y!"] }),
                json!([foo_json()]),
            );
    }

    #[test]
    fn sub_tag() {
        let test = TEST_DOMAIN.compile();
        let [foo] = test.bind(["foo"]);
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::Many),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-entity $a def@1:1)
                            (match-prop $a S:1:8 (element-in $c))
                            (match-prop $b S:1:9 (superset-of $d))
                            (member $c (_ $b))
                            (member $d (_ 'x!'))
                        "#
                        }))
                    ))
                    .returns(Value::sequence_of([foo.value_builder(foo_json()).into()])),
            )
            .assert_named_forward_map("foos", json!({ "sub_tag": "x!", }), json!([foo_json()]));
    }
}

#[test]
// BUG: Should be able to filter inside edge->bar like this
fn test_map_match_in_sub_multi_edge() {
    r#"
    def foo (
        rel .'key'|id: (rel .is: text)
        rel .'bars': {bar}
    )
    def bar (
        rel .'key'|id: (rel .is: text)
        rel .'tags': {text}
    )
    map foos(
        ('bar_tags'?: { ..tags }),
        foo: {
            ..foo match(
                'bars'?: bar match( // ERROR {bar} variable must be enclosed in {}
                    'tags': contains all { ..tags }
                )
            )
        }
    )
    "#
    .compile_fail(); // BUG
}
