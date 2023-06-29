use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use ontol_test_utils::{assert_error_msg, assert_json_io_matches, type_binding::*, TestCompile};
use serde_json::json;
use test_log::test;

#[test]
fn constant_string_pattern() {
    "
    pub type foo
    fmt '' => 'foo' => foo
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(&env, "foo");
        assert_json_io_matches!(foo, Create, "foo");
        assert_error_msg!(
            create_de(&foo).data(json!("fo")),
            r#"invalid type: string "fo", expected string matching /\Afoo\z/ at line 1 column 4"#
        );
    });
}

#[test]
fn concatenated_constant_string_constructor_pattern() {
    "
    pub type foobar
    fmt '' => 'foo' => 'bar' => foobar
    "
    .compile_ok(|env| {
        let foobar = TypeBinding::new(&env, "foobar");
        assert_json_io_matches!(foobar, Create, "foobar");
        assert_error_msg!(
            create_de(&foobar).data(json!("fooba")),
            r#"invalid type: string "fooba", expected string matching /\Afoobar\z/ at line 1 column 7"#
        );
    });
}

#[test]
fn uuid_in_string_constructor_pattern() {
    "
    pub type foo
    fmt '' => 'foo/' => uuid => foo
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(&env, "foo");

        assert_matches!(
            create_de(&foo).data(json!("foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8")),
            Ok(Data::Struct(attrs)) if attrs.len() == 1
        );
        assert_json_io_matches!(foo, Create, "foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
        // UUID gets normalized when serialized:
        assert_json_io_matches!(foo, Create,
            "foo/a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8" == "foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8"
        );
        assert_error_msg!(
            create_de(&foo).data(json!("foo")),
            r#"invalid type: string "foo", expected string matching /\Afoo/([0-9A-Fa-f]{32}|[0-9A-Fa-f]{8}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{4}\-[0-9A-Fa-f]{12})\z/ at line 1 column 5"#
        );
    });
}

#[test]
fn test_string_pattern_constructor_union() {
    "
    type foo
    type bar
    pub type foobar

    fmt '' => 'foo/' => uuid => foo
    fmt '' => 'bar/' => uuid => bar

    rel foobar is?: foo
    rel foobar is?: bar
    "
    .compile_ok(|env| {
        let foobar = TypeBinding::new(&env, "foobar");
        assert_matches!(
            create_de(&foobar).data_variant(json!("foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8")),
            Ok(Data::Struct(attrs)) if attrs.len() == 1
        );
        assert_json_io_matches!(foobar, Create, "foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
        assert_json_io_matches!(foobar, Create, "bar/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
        assert_error_msg!(
            create_de(&foobar).data(json!("foo/invalid-uuid")),
            r#"invalid type: string "foo/invalid-uuid", expected `foobar` (`string_pattern` or `string_pattern`) at line 1 column 18"#
        );
        assert_error_msg!(
            create_de(&foobar).data(json!("baz/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8")),
            r#"invalid type: string "baz/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8", expected `foobar` (`string_pattern` or `string_pattern`) at line 1 column 42"#
        );
    });
}

#[test]
fn test_regex_property() {
    "
    pub type foo
    rel foo 'prop': /abc*/
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(&env, "foo");
        assert_json_io_matches!(foo, Create, { "prop": "abc" });
        assert_json_io_matches!(foo, Create, { "prop": "123abc" });
        assert_json_io_matches!(foo, Create, { "prop": "123abcccc" });
        assert_error_msg!(
            create_de(&foo).data(json!({ "prop": "123" })),
            r#"invalid type: string "123", expected string matching /abc*/ at line 1 column 13"#
        );
    });
}

#[test]
fn test_simple_regex_pattern_constructor() {
    "
    pub type re
    fmt '' => /a/ => /bc*/ => re
    "
    .compile_ok(|env| {
        let re = TypeBinding::new(&env, "re");
        assert_json_io_matches!(re, Create, "ab");
        assert_json_io_matches!(re, Create, "abc" == "ab");
        assert_json_io_matches!(re, Create, "abccccc" == "ab");
        assert_error_msg!(
            create_de(&re).data(json!("a")),
            r#"invalid type: string "a", expected string matching /\Aabc*\z/ at line 1 column 3"#
        );
    });
}

#[test]
#[ignore = "figure out index relations"]
fn test_string_patterns() {
    "
    type hex
    rel '' /a-zA-Z0-9/: hex

    type uuid
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
    .compile_fail()
}

#[test]
fn regex_named_group_as_relation() {
    "
    type lol
    rel lol is: /abc(?<named>.)/ // ERROR invalid regex: unrecognized flag
    "
    .compile_fail()
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
