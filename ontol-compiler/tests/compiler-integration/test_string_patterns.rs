use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use pretty_assertions::assert_eq;
use serde_json::json;
use test_log::test;

use crate::{assert_error_msg, assert_json_io_matches, util::TypeBinding, TestCompile};

#[test]
fn constant_string_pattern() {
    "
    (type! foo)
    (rel! '' { 'foo' } foo)
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!("foo"));
        assert_error_msg!(
            foo.deserialize_data(json!("fo")),
            r#"invalid type: string "fo", expected string matching /\Afoo\z/ at line 1 column 4"#
        );
    })
}

#[test]
fn concatenated_constant_string_pattern() {
    "
    (type! foobar)
    (rel! '' { 'foo' } { 'bar' } foobar)
    "
    .compile_ok(|env| {
        let foobar = TypeBinding::new(env, "foobar");
        assert_json_io_matches!(foobar, json!("foobar"));
        assert_error_msg!(
            foobar.deserialize_data(json!("fooba")),
            r#"invalid type: string "fooba", expected string matching /\Afoobar\z/ at line 1 column 7"#
        );
    })
}

#[test]
fn uuid_in_string_pattern() {
    "
    (type! foo)
    (rel! '' { 'foo/' } { uuid } foo)
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");

        let data = foo.deserialize_data(json!("foo/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8")).unwrap();

        // FIXME: Map should have one property
        assert_matches!(
            &data,
            Data::Map(map) if map.is_empty()
        );

        // FIXME: Should contain the UUID:
        assert_eq!(
            foo.serialize_data_json(env, &data),
            json!("foo/")
        );

        assert_error_msg!(
            foo.deserialize_data(json!("foo")),
            r#"invalid type: string "foo", expected string matching /\Afoo/[0-9A-Fa-f]{8}\-?[0-9A-Fa-f]{4}\-?[0-9A-Fa-f]{4}\-?[0-9A-Fa-f]{4}\-?[0-9A-Fa-f]{12}\z/ at line 1 column 5"#
        );
    })
}

#[test]
#[ignore = "figure out index relations"]
fn test_string_patterns() {
    "
    (type! hex)
    (rel! '' { /a-zA-Z0-9/ } hex)

    (type! uuid)
    (rel!
        ''
        { hex{8} }
        { '-' }
        { hex{4} }
        { '-' }
        { hex{4} }
        { '-' }
        { hex{4} }
        { '-' }
        { hex{12} }
        uuid
    )

    (type! my_id)
    (rel! '' { 'my/' } { uuid } my_id)
    "
    .compile_fail()
}

#[test]
fn regex_named_group_as_relation() {
    "
    (type! lol)
    (rel! _ { /abc(?<named>.)/ } lol) ;; ERROR parse error: expected end of list
    "
    .compile_fail()
}

#[test]
#[ignore]
fn automata() {
    r#"
    (rel!
        []
        { int }
        { int }
        position
    )

    (rel!
        []
        { position }
        { position }
        { position* }
        position-list
    )
    
    (rel!
        position-list
        { position }
        { position }
        { position* }
        position-ring
    )
    "#
    .compile_fail();
}
