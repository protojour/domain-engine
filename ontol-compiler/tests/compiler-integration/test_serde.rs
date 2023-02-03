use crate::{assert_error_msg, util::serialize_json, util::TypeBinding, TestCompile};
use serde_json::json;
use test_log::test;

macro_rules! assert_json_io_matches {
    ($env:expr, $binding:expr, $json:expr) => {
        let input = $json;
        let value = match $binding.deserialize_value($env, input.clone()) {
            Ok(value) => value,
            Err(err) => panic!("deserialize failed: {err}"),
        };
        let output = serialize_json($env, &value);

        pretty_assertions::assert_eq!(input, output);
    };
}

#[test]
fn test_serde_empty_type() {
    "(type! foo)".compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(env, foo, json!({}));
    });
}

#[test]
fn test_serde_value_type() {
    "
    (type! foo)
    (rel! (foo) _ (string))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(env, foo, json!("string"));
    });
}

#[test]
fn test_serde_map_type() {
    "
    (type! foo)
    (rel! (foo) a (string))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(env, foo, json!({ "a": "string" }));
    });
}

#[test]
fn test_serde_complex_type() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) a (string))
    (rel! (foo) b (bar))
    (rel! (bar) c (string))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(env, foo, json!({ "a": "A", "b": { "c": "C" }}));
    });
}

#[test]
fn test_serde_tuple() {
    "
    (type! t)
    (rel! (t) _ (tuple! (string) (int)))
    "
    .compile_ok(|env| {
        let t = TypeBinding::new(env, "t");
        assert_json_io_matches!(env, t, json!(["a", 1]));
    });
}

#[test]
fn test_serde_value_union1() {
    r#"
    (type! u)
    (rel! (u) _ "a")
    (rel! (u) _ "b")
    "#
    .compile_ok(|env| {
        let u = TypeBinding::new(env, "u");
        assert_json_io_matches!(env, u, json!("a"));
    });
}

#[test]
fn test_serde_string_or_null() {
    r#"
    (type! string-or-null)
    (rel! (string-or-null) _ (string))
    (rel! (string-or-null) _ (null))

    (type! foo)
    (rel! (foo) a (string-or-null))
    "#
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(env, foo, json!({ "a": "string" }));
        assert_json_io_matches!(env, foo, json!({ "a": null }));
    });
}

#[test]
fn test_serde_map_union() {
    r#"
    (type! foo)
    (type! bar)
    (rel! (foo) type "foo")
    (rel! (foo) c (int))
    (rel! (bar) type "bar")
    (rel! (bar) d (int))

    (type! u)
    (rel! (u) _ (foo))
    (rel! (u) _ (bar))
    "#
    .compile_ok(|env| {
        let u = TypeBinding::new(env, "u");
        assert_json_io_matches!(env, u, json!({ "type": "foo", "c": 7}));
    });
}

#[test]
fn test_serde_many_cardinality() {
    "
    (type! foo)
    (rel! (foo) s[] (string))
    "
    .compile_ok(|env| {
        let foo = TypeBinding::new(env, "foo");
        assert_json_io_matches!(env, foo, json!({ "s": ["a", "b" ]}));
        /*
        assert_matches!(
            foo.deserialize_data(env, json!({ "s": ["a", "b"] })),
            Ok(Data::Map(map)) if map.len() == 1
        );
        */
    });
}

#[test]
fn test_serde_geojson() {
    r#"
    (type! position)
    (rel! (position) _ (tuple! (int) (int)))

    (type! Point)
    (rel! (Point) type "Point")
    (rel! (Point) coordinates (position))

    (type! Polygon)
    (rel! (Polygon) type "Polygon")
    ; BUG: need to store in list:
    (rel! (Polygon) coordinates (tuple! (position) (position)))

    (type! Geometry)
    (rel! (Geometry) _ (Point))
    (rel! (Geometry) _ (Polygon))
    "#
    .compile_ok(|env| {
        let geometry = TypeBinding::new(env, "Geometry");
        assert_json_io_matches!(
            env,
            geometry,
            json!({ "type": "Point", "coordinates": [1, 2]})
        );
        assert_json_io_matches!(
            env,
            geometry,
            json!({ "type": "Polygon", "coordinates": [[1, 2], [3, 4]]})
        );
        assert_error_msg!(
            geometry.deserialize_data_variant(env, json!([])),
            "invalid type: sequence, expected `Geometry` (`Point` or `Polygon`) at line 1 column 2"
        );
        assert_error_msg!(
            geometry.deserialize_data_variant(env, json!({ "type": "bogus" })),
            "invalid map value, expected `Geometry` (`Point` or `Polygon`) at line 1 column 16"
        );
    });
}
