use indoc::indoc;
use ontol_macros::test;
use ontol_runtime::{format_utils::Literal, property::ValueCardinality, value::Value};
use ontol_test_utils::{
    TestCompile, assert_error_msg, serde_helper::serde_create, test_map::YielderMock,
};
use serde_json::json;
use unimock::{MockFn, matching};

#[test]
fn test_map_match_scalar_key() {
    r#"
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def key (rel* is: text)
    def foo (
        rel. 'key': key
        rel* 'prop': text
    )
    map query(
        key(key),
        @match foo('key': key),
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::Unit),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-def $a def@1:2)
                            (match-prop $a p@1:2:0 (element-in $b))
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def key (rel* is: text)
    def foo (
        rel. 'key': key
        rel* 'prop': text
    )
    map query(
        (),
        foo { ..@match foo() }
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::IndexSet),
                        eq!(&Literal(indoc! { "
                            (root $a)
                            (is-def $a def@1:2)
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def key (rel* is: text)
    def foo (
        rel. 'key': key
        rel* 'prop_a': text
        rel* 'prop_b': text
    )
    map query(
        (
            'input_a': a,
            'input_b': b,
        ),
        foo { ..@match foo(
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
                        eq!(&ValueCardinality::IndexSet),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-def $a def@1:2)
                            (match-prop $a p@1:2:1 (element-in $b))
                            (match-prop $a p@1:2:2 (element-in $c))
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def key (rel* is: text)
    def foo (
        rel. 'key': key
        rel* 'prop_a': text
        rel* 'prop_b': text
    )
    map query(
        (
            'input_a': a,
            'input_b'?: b,
        ),
        foo {..@match foo(
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
                        eq!(&ValueCardinality::IndexSet),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-def $a def@1:2)
                            (match-prop $a p@1:2:1 (element-in $b))
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
                        eq!(&ValueCardinality::IndexSet),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-def $a def@1:2)
                            (match-prop $a p@1:2:1 (element-in $b))
                            (match-prop $a p@1:2:2 (element-in $c))
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def key (rel* is: text)
    def foo (
        rel. 'key': key
        rel* 'foo': text
    )
    def bar (
        rel* 'bar': text
    )
    map(
        @match foo('foo': x),
        bar('bar': x),
    )
    map query(
        ('input': x),
        bar {..@match foo(
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
                        eq!(&ValueCardinality::IndexSet),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-def $a def@1:2)
                            (match-prop $a p@1:2:1 (element-in $b))
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
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def key (rel* is: text)
    def foo (
        rel. 'key': key
        rel* 'foo': text
    )
    def bar (
        rel* 'bar': text
    )
    map(
        @match foo('foo': x),
        bar('bar': x),
    )
    map query(
        ('input': { ..x }),
        bar {
            ..@match foo('foo': @in { ..x })
        }
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::IndexSet),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-def $a def@1:2)
                            (match-prop $a p@1:2:1 (element-in $b))
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
    use ontol_macros::test;

    const TEST_DOMAIN: &str = r#"
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'key': (rel* is: text)
        rel* 'tags': {text}
        rel* 'sub': sub
    )
    def sub (
        rel* 'tags': {text}
    )
    map foos(
        (
            'foo_tags'?: { ..tags },
            'sub_tag'?: sub_tag
        ),
        foo {
            ..@match foo(
                'tags'?: @contains_all { ..tags },
                'sub'?: sub(
                    'tags': @contains_all { sub_tag }
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
                        eq!(&ValueCardinality::IndexSet),
                        eq!(&Literal(indoc! { r#"
                                (root $a)
                                (is-def $a def@1:1)
                                (match-prop $a p@1:1:1 (superset-of $b))
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
                        eq!(&ValueCardinality::IndexSet),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-def $a def@1:1)
                            (match-prop $a p@1:1:2 (element-in $c))
                            (match-prop $b p@1:2:0 (superset-of $d))
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
fn test_map_match_in_sub_multi_edge() {
    r#"
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    arc foobar {
        (foo) bars: (bar),
    }
    def foo (
        rel. 'key': (rel* is: text)
        rel* foobar.bars: {bar}
    )
    def bar (
        rel. 'key': (rel* is: text)
        rel* 'tags': {text}
    )
    map foos(
        ('bar_tags'?: { ..tags }),
        foo {
            ..@match foo(
                foobar.bars?: @match bar(
                    'tags': @contains_all { ..tags }
                )
            )
        }
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        let foo_json = json!({ "key": "f", "bars": []});
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::IndexSet),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-def $a def@1:3)
                            (match-prop $a p@1:3:1 (element-in $c))
                            (is-def $b def@1:4)
                            (match-prop $b p@1:4:1 (superset-of $d))
                            (member $c (_ $b))
                            (member $d (_ 'x'))
                            (member $d (_ 'y'))
                        "#
                        }))
                    ))
                    .returns(Value::sequence_of([foo
                        .value_builder(foo_json.clone())
                        .into()])),
            )
            .assert_named_forward_map(
                "foos",
                json!({ "bar_tags": ["x", "y"], }),
                json!([foo_json]),
            );
    });
}

#[test]
fn test_map_with_order_constant() {
    r#"
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'key': (rel* is: text)
        rel* 'field': text
        rel* order[
            rel* 0: 'field'
            rel* direction: descending
        ]: by_field
    )
    sym { by_field }

    map foos(
        (),
        foo {
            ..@match foo(
                order: by_field(),
                direction: descending()
            )
        }
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        let foo_json = json!({ "key": "k", "field": "x" });
        test.mapper()
            .with_mock_yielder(
                YielderMock::yield_match
                    .next_call(matching!(
                        eq!(&ValueCardinality::IndexSet),
                        eq!(&Literal(indoc! { r#"
                            (root $a)
                            (is-def $a def@1:1)
                            (order 'by_field')
                            (direction Descending)
                        "#
                        }))
                    ))
                    .returns(Value::sequence_of([foo
                        .value_builder(foo_json.clone())
                        .into()])),
            )
            .assert_named_forward_map("foos", json!({}), json!([foo_json]));
    });
}

#[test]
fn test_map_with_order_variable() {
    r#"
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'key': (rel* is: text)
        rel* 'field': text
        rel* order[
            rel* 0: 'field'
            rel* direction: descending
        ]: by_field
    )
    sym { by_field }

    map foos(
        ( 'sort': ord, 'dir': dir ),
        foo {
            ..@match foo(
                order: ord,
                direction: dir,
            )
        }
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        let input = test.mapper().named_map_input_binding("foos");

        assert_error_msg!(
            serde_create(&input).to_value(json!({ "sort": "bogus" })),
            "invalid type: string \"bogus\", expected \"by_field\" at line 1 column 15"
        );
        assert_error_msg!(
            serde_create(&input).to_value(json!({ "dir": "bogus" })),
            "invalid type: string \"bogus\", expected `<anonymous>` (`ascending` or `descending`) at line 1 column 14"
        );

        let foo_json = json!({ "key": "k", "field": "x" });

         test.mapper()
             .with_mock_yielder(
                 YielderMock::yield_match
                     .next_call(matching!(
                         eq!(&ValueCardinality::IndexSet),
                         eq!(&Literal(indoc! { r#"
                             (root $a)
                             (is-def $a def@1:1)
                             (order 'by_field')
                             (direction Ascending)
                         "#
                         }))
                     ))
                     .returns(Value::sequence_of([foo
                         .value_builder(foo_json.clone())
                         .into()])),
             )
             .assert_named_forward_map("foos", json!({ "sort": "by_field", "dir": "ascending" }), json!([foo_json]));
    });
}

#[test]
fn test_map_with_order_indexset() {
    r#"
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo (
        rel. 'key': (rel* is: text)
        rel* 'a': text
        rel* 'b': text
        rel* order[
            rel* 0: 'a'
            rel* direction: descending
        ]: by_a
        rel* order[
            rel* 0: 'a'
            rel* direction: descending
        ]: by_b
    )
    sym { by_a, by_b }

    map foos(
        (
            'sort': {..ord},
        ),
        foo {
            ..@match foo(
                order: {..ord},
            )
        }
    )
    "#
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        let input = test.mapper().named_map_input_binding("foos");

        assert_error_msg!(
            serde_create(&input).to_value(json!({ "sort": "bogus" })),
            "invalid type: string \"bogus\", expected sequence with minimum length 0 at line 1 column 15"
        );
        assert_error_msg!(
            serde_create(&input).to_value(json!({ "sort": ["bogus"] })),
            "invalid type: string \"bogus\", expected `<anonymous>` (`by_a` or `by_b`) at line 1 column 16"
        );
        assert_error_msg!(
            serde_create(&input).to_value(json!({ "sort": ["by_a", "by_b", "by_a"] })),
            "invalid index-set: attribute[2] equals attribute[0] at line 1 column 30"
        );

        let foo_json = json!({ "key": "k", "a": "x", "b": "y" });

         test.mapper()
             .with_mock_yielder(
                 YielderMock::yield_match
                     .next_call(matching!(
                         eq!(&ValueCardinality::IndexSet),
                         eq!(&Literal(indoc! { r#"
                             (root $a)
                             (is-def $a def@1:1)
                             (order 'by_a' 'by_b')
                         "#
                         }))
                     ))
                     .returns(Value::sequence_of([foo
                         .value_builder(foo_json.clone())
                         .into()])),
             )
             .assert_named_forward_map("foos", json!({ "sort": ["by_a", "by_b"] }), json!([foo_json]));
    });
}
