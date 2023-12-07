use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use ontol_test_utils::{assert_error_msg, assert_json_io_matches, serde_helper::*, TestCompile};
use serde_json::json;
use test_log::test;

#[test]
fn constant_text_pattern() {
    "
    def foo (
        fmt '' => 'foo' => .
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), "foo");
        assert_error_msg!(
            serde_create(&foo).to_data(json!("fo")),
            r#"invalid type: string "fo", expected string matching /(?:\A(?:foo)\z)/ at line 1 column 4"#
        );
    });
}

#[test]
fn concatenated_constant_string_constructor_pattern() {
    "
    def foobar (
        fmt '' => 'foo' => 'bar' => .
    )
    "
    .compile_then(|test| {
        let [foobar] = test.bind(["foobar"]);
        assert_json_io_matches!(serde_create(&foobar), "foobar");
        assert_error_msg!(
            serde_create(&foobar).to_data(json!("fooba")),
            r#"invalid type: string "fooba", expected string matching /(?:\A(?:foobar)\z)/ at line 1 column 7"#
        );
    });
}

#[test]
fn uuid_in_string_constructor_pattern() {
    "
    def foo (
        fmt '' => 'foo/' => uuid => .
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);

        assert_matches!(
            serde_create(&foo).to_data(json!("foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8")),
            Ok(Data::Struct(attrs)) if attrs.len() == 1
        );
        assert_json_io_matches!(serde_create(&foo), "foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
        // UUID gets normalized when serialized:
        assert_json_io_matches!(serde_create(&foo),
            "foo/a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8" == "foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8"
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!("foo")),
            r#"invalid type: string "foo", expected string matching /(?:\A(?:foo/)((?:[0-9A-Fa-f]{32}|(?:[0-9A-Fa-f]{8}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{12})))\z)/ at line 1 column 5"#
        );
    });
}

#[test]
fn text_prefix() {
    "
    def prefix (
        fmt '' => 'prefix/' => text => .
    )
    "
    .compile_then(|test| {
        let [prefix] = test.bind(["prefix"]);

        assert_matches!(
            serde_create(&prefix).to_data(json!("prefix/foobar")),
            Ok(Data::Struct(attrs)) if attrs.len() == 1
        );
        assert_json_io_matches!(serde_create(&prefix), "prefix/foobar");
    });
}

#[test]
fn test_text_pattern_constructor_union() {
    "
    def foo (
        fmt '' => 'foo/' => uuid => .
    )
    def bar (
        fmt '' => 'bar/' => uuid => .
    )
    def foobar (
        rel .is?: foo
        rel .is?: bar
    )
    "
    .compile_then(|test| {
        let [foobar] = test.bind(["foobar"]);
        assert_matches!(
            serde_create(&foobar).to_data_variant(json!("foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8")),
            Ok(Data::Struct(attrs)) if attrs.len() == 1
        );
        assert_json_io_matches!(serde_create(&foobar), "foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
        assert_json_io_matches!(serde_create(&foobar), "bar/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
        assert_error_msg!(
            serde_create(&foobar).to_data(json!("foo/invalid-uuid")),
            r#"invalid type: string "foo/invalid-uuid", expected `foobar` (`text_pattern` or `text_pattern`) at line 1 column 18"#
        );
        assert_error_msg!(
            serde_create(&foobar).to_data(json!("baz/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8")),
            r#"invalid type: string "baz/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8", expected `foobar` (`text_pattern` or `text_pattern`) at line 1 column 42"#
        );
    });
}

#[test]
fn test_regex_property() {
    "
    def foo (
        rel .'prop': /abc*/
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "prop": "abc" });
        assert_json_io_matches!(serde_create(&foo), { "prop": "123abc" });
        assert_json_io_matches!(serde_create(&foo), { "prop": "123abcccc" });
        assert_error_msg!(
            serde_create(&foo).to_data(json!({ "prop": "123" })),
            r#"invalid type: string "123", expected string matching /(?:(?:ab)c*)/ at line 1 column 13"#
        );
    });
}

#[test]
fn test_simple_regex_pattern_constructor() {
    "
    def re (
        fmt '' => /a/ => /bc*/ => .
    )
    "
    .compile_then(|test| {
        let [re] = test.bind(["re"]);
        assert_json_io_matches!(serde_create(&re), "ab");
        assert_json_io_matches!(serde_create(&re), "abc" == "ab");
        assert_json_io_matches!(serde_create(&re), "abccccc" == "ab");
        assert_error_msg!(
            serde_create(&re).to_data(json!("a")),
            r#"invalid type: string "a", expected string matching /(?:\A(?:ab)c*\z)/ at line 1 column 3"#
        );
    });
}

#[test]
#[ignore = "figure out index relations"]
fn test_text_patterns() {
    "
    def hex
    rel '' /a-zA-Z0-9/: hex

    def uuid
    rel
        ''
        [hex{8}]
        ['-']
        [hex{4}]
        ['-']
        [hex{4}]
        ['-']
        [hex{4}]
        ['-']
        [hex{12}]
        uuid

    type my_id
    rel '' 'my/': [uuid] my_id
    "
    .compile_fail();
}

#[test]
fn regex_named_group_as_relation() {
    "
    def lol (
        rel .is: /abc(?<named>.)/
    )
    "
    .compile_then(|_test| {});
}

#[test]
#[ignore]
fn sequence_automata() {
    r#"
    rel
        []
        [int]
        [int]
        position

    rel
        []
        [position]
        [position]
        [position*]
        position-list

    rel
        position-list
        [position]
        [position]
        [position*]
        position-ring
    "#
    .compile_fail();
}
