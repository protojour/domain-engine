//! Tests for deserialization, including errors

use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use ontol_test_utils::{assert_error_msg, serde_helper::*, TestCompile};
use serde_json::json;
use test_log::test;

#[test]
fn deserialize_empty_type() {
    "def(pub) foo {}".compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_error_msg!(
            serde_create(&foo).to_data(json!(42)),
            "invalid type: integer `42`, expected type `foo` at line 1 column 2"
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!({ "bar": 5 })),
            "unknown property `bar` at line 1 column 6"
        );
        assert_matches!(
            serde_create(&foo).to_data(json!({})),
            Ok(Data::Struct(attrs)) if attrs.is_empty()
        );
    });
}

#[test]
fn deserialize_is_i64() {
    "
    def(pub) foo {}
    rel foo is: i64
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_data_variant(json!(42)),
            Ok(Data::I64(42))
        );
        assert_matches!(
            serde_create(&foo).to_data_variant(json!(-42)),
            Ok(Data::I64(-42))
        );

        assert_error_msg!(
            serde_create(&foo).to_data(json!({})),
            "invalid type: map, expected integer at line 1 column 0"
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!("boom")),
            "invalid type: string \"boom\", expected integer at line 1 column 6"
        );
    });
}

#[test]
fn deserialize_is_maybe_i64() {
    "
    def(pub) foo {}
    rel foo is?: i64
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_data_variant(json!(42)),
            Ok(Data::I64(42))
        );
        assert_matches!(
            serde_create(&foo).to_data_variant(json!(-42)),
            Ok(Data::I64(-42))
        );

        assert_error_msg!(
            serde_create(&foo).to_data(json!({})),
            "invalid type: map, expected integer at line 1 column 0"
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!("boom")),
            "invalid type: string \"boom\", expected integer at line 1 column 6"
        );
    });
}

#[test]
fn deserialize_string() {
    "
    def(pub) foo {}
    rel foo is?: text
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_data_variant(json!("hei")),
            Ok(Data::Text(s)) if s == "hei"
        );

        assert_error_msg!(
            serde_create(&foo).to_data(json!({})),
            "invalid type: map, expected string at line 1 column 0"
        );
    });
}

#[test]
fn deserialize_object_properties() {
    "
    def(pub) obj {}
    rel obj 'a': text
    rel obj 'b': i64
    "
    .compile_then(|test| {
        let [obj] = test.bind(["obj"]);
        assert_matches!(
            serde_create(&obj).to_data(json!({ "a": "hei", "b": 42 })),
            Ok(Data::Struct(_))
        );

        assert_error_msg!(
            serde_create(&obj).to_data(json!({ "a": "hei", "b": 42, "c": false })),
            "unknown property `c` at line 1 column 21"
        );
        assert_error_msg!(
            serde_create(&obj).to_data(json!({ "a": "hei", "b": 42, "_edge": { "param": 42 } })),
            "`_edge` property not accepted here at line 1 column 8"
        );
        assert_error_msg!(
            serde_create(&obj).to_data(json!({})),
            r#"missing properties, expected "a" and "b" at line 1 column 2"#
        );
    });
}

#[test]
fn deserialize_read_only_property_error() {
    "
    def(pub) obj {}
    rel obj 'created'(rel .gen: create_time): datetime
    "
    .compile_then(|test| {
        let [obj] = test.bind(["obj"]);
        assert_error_msg!(
            serde_create(&obj).to_data(json!({ "created": "something" })),
            "property `created` is read-only at line 1 column 10"
        );
    });
}

#[test]
fn deserialize_nested() {
    "
    def(pub) one {}
    def(pub) two {}
    def three {}
    rel one 'x': two
    rel one 'y': three
    rel two 'y': three
    rel three is?: text
    "
    .compile_then(|test| {
        let [one] = test.bind(["one"]);
        assert_matches!(
            serde_create(&one).to_data(json!({
                "x": {
                    "y": "a"
                },
                "y": "b"
            })),
            Ok(Data::Struct(a)) if a.len() == 2
        );
    });
}

#[test]
fn deserialize_recursive() {
    "
    def(pub) foo {}
    def(pub) bar {}
    rel foo 'b': bar
    rel bar 'f': foo
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_error_msg!(
            serde_create(&foo).to_data(json!({
                "b": {
                    "f": {
                        "b": 42
                    }
                },
            })),
            "invalid type: integer `42`, expected type `bar` at line 1 column 17"
        );
    });
}

#[test]
fn deserialize_union_of_primitives() {
    "
    def(pub) foo {}
    rel foo is?: text
    rel foo is?: i64
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_data_variant(json!(42)),
            Ok(Data::I64(42))
        );
        assert_matches!(serde_create(&foo).to_data_variant(json!("qux")), Ok(Data::Text(s)) if s == "qux");
        assert_error_msg!(
            serde_create(&foo).to_data(json!({})),
            "invalid type: map, expected `foo` (`int` or `string`) at line 1 column 2"
        );
    });
}

#[test]
fn deserialize_string_constant() {
    "
    def(pub) foo {}
    rel foo is?: 'my_value'
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_data_variant(json!("my_value")),
            Ok(Data::Text(s)) if s == "my_value"
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!("other value")),
            r#"invalid type: string "other value", expected "my_value" at line 1 column 13"#
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!(42)),
            r#"invalid type: integer `42`, expected "my_value" at line 1 column 2"#
        );
    });
}

#[test]
fn deserialize_finite_non_uniform_sequence() {
    "
    def(pub) foo {}
    rel foo 0: i64
    rel foo 1: 'a'
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_data(json!([42, "a"])),
            Ok(Data::Sequence(seq)) if seq.attrs.len() == 2
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!([77])),
            "invalid length 1, expected sequence with length 2 at line 1 column 4"
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!([11, "a", "boom"])),
            "trailing characters at line 1 column 9"
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!([14, "b"])),
            r#"invalid type: string "b", expected "a" at line 1 column 7"#
        );
    });
}

#[test]
fn deserialize_finite_uniform_sequence() {
    "
    def(pub) foo {}
    rel foo ..2: i64
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_data(json!([42, 42])),
            Ok(Data::Sequence(seq)) if seq.attrs.len() == 2
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!([77])),
            "invalid length 1, expected sequence with length 2 at line 1 column 4"
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!([11, "a"])),
            r#"invalid type: string "a", expected integer at line 1 column 7"#
        );
        assert_error_msg!(
            serde_create(&foo).to_data(json!([14, 15, 16])),
            r#"trailing characters at line 1 column 8"#
        );
    });
}

#[test]
fn deserialize_string_union() {
    "
    def(pub) foo {}
    rel foo is?: 'a'
    rel foo is?: 'b'
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_matches!(
            serde_create(&foo).to_data_variant(json!("a")),
            Ok(Data::Text(a)) if a == "a"
        );
        assert_error_msg!(
            serde_create(&foo).to_data_variant(json!("junk")),
            r#"invalid type: string "junk", expected `foo` ("a" or "b") at line 1 column 6"#
        );
    });
}

#[test]
fn deserialize_map_union() {
    "
    def(pub) foo {}
    def(pub) bar {}
    rel foo 'variant': 'foo'
    rel bar 'variant': 'bar'
    rel bar 'prop': i64

    def(pub) union {
        rel .is?: foo
        rel .is?: bar
    }
    "
    .compile_then(|test| {
        let [union] = test.bind(["union"]);
        assert_matches!(
            serde_create(&union).to_data_variant(json!({ "variant": "foo" })),
            Ok(Data::Struct(attrs)) if attrs.len() == 1
        );
        assert_matches!(
            serde_create(&union).to_data_variant(json!({ "variant": "bar", "prop": 42 })),
            Ok(Data::Struct(attrs)) if attrs.len() == 2
        );
        assert_matches!(
            serde_create(&union).to_data_variant(json!({ "prop": 42, "variant": "bar" })),
            Ok(Data::Struct(attrs)) if attrs.len() == 2
        );
        assert_error_msg!(
            serde_create(&union).to_data_variant(json!("junk")),
            r#"invalid type: string "junk", expected `union` (`foo` or `bar`) at line 1 column 6"#
        );
        assert_error_msg!(
            serde_create(&union).to_data_variant(json!({ "variant": "bar" })),
            r#"missing properties, expected "prop" at line 1 column 17"#
        );
    });
}

#[test]
fn union_tree() {
    "
    def u1 {
        rel .is?: '1a'
        rel .is?: '1b'
    }
    def u2 {
        rel .is?: '2a'
        rel .is?: '2b'
    }
    def u3 {
        rel .is?: u1
        rel .is?: u2
    }
    "
    .compile_then(|test| {
        let [u3] = test.bind(["u3"]);
        assert_matches!(
            serde_create(&u3).to_data_variant(json!("1a")),
            Ok(Data::Text(s)) if s == "1a"
        );
        assert_error_msg!(
            serde_create(&u3).to_data_variant(json!("ugh")),
            r#"invalid type: string "ugh", expected `u3` (one of "1a", "1b", "2a", "2b") at line 1 column 5"#
        );
    });
}

#[test]
fn test_deserialize_open_data() {
    "
    def(pub|open) foo {
        rel .'closed': i64
    }
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);

        let Data::Struct(map) = serde_create(&foo)
            .enable_open_data()
            .to_data(json!({
                "closed": 1,
                "int": 2,
                "float": 3.14,
                "bool": true,
                "null": null,
                "dict": {
                    "key": "value"
                },
                "array": ["value"]
            }))
            .unwrap()
        else {
            panic!();
        };

        let open_data = &map
            .get(&test.ontology.ontol_domain_meta().open_data_property_id())
            .unwrap()
            .value;
        let Data::Dict(open_data) = &open_data.data else {
            panic!();
        };

        assert!(!open_data.contains_key("closed"));
        assert!(open_data.contains_key("int"));
        assert!(open_data.contains_key("float"));
        assert!(open_data.contains_key("bool"));
        assert!(open_data.contains_key("null"));
        assert!(open_data.contains_key("dict"));
        assert!(open_data.contains_key("array"));
    });
}
