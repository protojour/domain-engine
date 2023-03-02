use crate::{
    assert_error_msg, assert_json_io_matches, util::type_binding::TypeBinding, TestCompile,
};
use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use serde_json::json;
use test_log::test;

#[test]
fn test_serde_empty_type() {
    "type foo".compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!({}));
    });
}

#[test]
fn test_serde_value_type() {
    "
    type foo
    rel . [string] foo
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!("string"));
    });
}

#[test]
fn test_serde_map_type() {
    "
    type foo
    rel foo ['a'] string
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!({ "a": "string" }));
    });
}

#[test]
fn test_serde_complex_type() {
    "
    type foo
    type bar
    rel foo ['a'] string
    rel foo ['b'] bar
    rel bar ['c'] string
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!({ "a": "A", "b": { "c": "C" }}));
    });
}

#[test]
fn test_serde_sequence() {
    "
    type t
    rel t [0] string
    rel t [1] int
    "
    .compile_ok(|env| {
        let t = TypeBinding::new(env, "t");
        assert_json_io_matches!(t, json!(["a", 1]));
    });
}

#[test]
fn test_serde_value_union1() {
    "
    type u
    rel . ['a'] u
    rel . ['b'] u
    "
    .compile_ok(|env| {
        let u = TypeBinding::new(env, "u");
        assert_json_io_matches!(u, json!("a"));
    });
}

#[test]
fn test_serde_string_or_unit() {
    "
    type string-or-unit
    rel . [string] string-or-unit
    rel . [.] string-or-unit

    type foo
    rel foo ['a'] string-or-unit
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!({ "a": "string" }));
        assert_json_io_matches!(foo, json!({ "a": null }));
    });
}

#[test]
fn test_serde_map_union() {
    "
    type foo
    type bar
    rel foo ['type'] 'foo'
    rel foo ['c'] int
    rel bar ['type'] 'bar'
    rel bar ['d'] int

    type u
    rel . [foo] u
    rel . [bar] u
    "
    .compile_ok(|env| {
        let u = TypeBinding::new(env, "u");
        assert_json_io_matches!(u, json!({ "type": "foo", "c": 7}));
    });
}

#[test]
fn test_serde_many_cardinality() {
    "
    type foo
    rel foo ['s'*] string
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!({ "s": []}));
        assert_json_io_matches!(foo, json!({ "s": ["a", "b"]}));
    });
}

#[test]
fn test_serde_infinite_sequence() {
    "
    type foo
    rel foo [ ..2] int
    rel foo [2..4] string
    rel foo [5..6] int
    rel foo [6.. ] int
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(foo, json!([42, 43, "a", "b", null, 44]));
        assert_json_io_matches!(foo, json!([42, 43, "a", "b", null, 44, 45, 46]));
        assert_error_msg!(
            foo.deserialize_data(json!([77])),
            "invalid length 1, expected sequence with minimum length 6 at line 1 column 4"
        );
    });
}

#[test]
fn test_serde_uuid() {
    "
    type my_id
    rel . [uuid] my_id
    "
    .compile_ok(|env| {
        let my_id = TypeBinding::new(env, "my_id");
        assert_matches!(
            my_id.deserialize_data_variant(json!("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8")),
            Ok(Data::Uuid(_))
        );
        assert_json_io_matches!(my_id, json!("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8"));
        assert_error_msg!(
            my_id.deserialize_data(json!(42)),
            "invalid type: integer `42`, expected `uuid` at line 1 column 2"
        );
        assert_error_msg!(
            my_id.deserialize_data(json!("foobar")),
            r#"invalid type: string "foobar", expected `uuid` at line 1 column 8"#
        );
    });
}

#[test]
fn test_jsonml() {
    "
    type element
    type tag
    type tag_name
    type attributes

    rel . [tag] element
    rel . [string] element

    rel tag [0] tag_name

    // BUG: should have default `{}` and serde would skip this if not a map
    // (also in serialization if this equals the default value!)
    rel tag [1] attributes

    rel tag [2..] element

    rel . ['div'] tag_name
    rel . ['em'] tag_name
    rel . ['strong'] tag_name

    // BUG: should accept any string as key
    rel attributes ['class'?] string
    "
    .compile_ok(|env| {
        let element = TypeBinding::new(env, "element");

        assert_json_io_matches!(element, json!("text"));
        assert_json_io_matches!(element, json!(["div", {}]));
        assert_json_io_matches!(element, json!(["div", {}, "text"]));
        assert_json_io_matches!(
            element,
            json!(["div", {}, ["em", {}, "text1"], ["strong", { "class": "foo" }]])
        );
    });
}
