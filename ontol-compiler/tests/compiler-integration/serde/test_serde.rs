use assert_matches::assert_matches;
use ontol_runtime::{
    interface::serde::processor::{
        ProcessorLevel, ProcessorMode, ProcessorProfile, ProcessorProfileFlags, ScalarFormat,
    },
    value::Value,
};
use ontol_test_utils::{
    assert_error_msg, assert_json_io_matches,
    examples::{ARTIST_AND_INSTRUMENT, GITMESH},
    expect_eq,
    serde_helper::*,
    TestCompile,
};
use serde::de::DeserializeSeed;
use serde_json::json;
use test_log::test;

#[test]
fn test_serde_empty_type() {
    "def foo ()".compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), {});
    });
}

#[test]
fn test_serde_type_alias() {
    "
    def foo ( rel .is: text )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), "string");
    });
}

#[test]
fn test_serde_booleans() {
    "
    def f ( rel .is: false )
    def t ( rel .is: true )
    def b ( rel .is: boolean )
    "
    .compile_then(|test| {
        let [f, t, b] = test.bind(["f", "t", "b"]);

        assert_json_io_matches!(serde_create(&f), false);
        assert_json_io_matches!(serde_create(&t), true);
        assert_json_io_matches!(serde_create(&b), false);
        assert_json_io_matches!(serde_create(&b), true);

        assert_error_msg!(
            serde_create(&f).to_value(json!(true)),
            "invalid type: boolean `true`, expected false at line 1 column 4"
        );
        assert_error_msg!(
            serde_create(&t).to_value(json!(false)),
            "invalid type: boolean `false`, expected true at line 1 column 5"
        );
    });
}

#[test]
fn test_serde_struct_type() {
    "
    def foo ( rel .'a': text )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "a": "string" });
    });
}

#[test]
fn test_serde_struct_optional_field() {
    "
    def foo (
        rel .'a'?: text
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), {} == {});
        assert_json_io_matches!(serde_create(&foo), { "a": null } == {});
        assert_json_io_matches!(serde_create(&foo), { "a": "A" });
    });
}

#[test]
fn test_serde_complex_type() {
    "
    def foo ()
    def bar ()
    rel foo 'a': text
    rel foo 'b': bar
    rel bar 'c': text
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "a": "A", "b": { "c": "C" }});
    });
}

#[test]
fn test_serde_sequence() {
    "
    def t (
        rel .0: text
        rel .1: i64
    )
    "
    .compile_then(|test| {
        let [t] = test.bind(["t"]);
        assert_json_io_matches!(serde_create(&t), ["a", 1]);
    });
}

#[test]
fn test_serde_value_union1() {
    "
    def u (
        rel .is?: 'a'
        rel .is?: 'b'
    )
    "
    .compile_then(|test| {
        let [u] = test.bind(["u"]);
        assert_json_io_matches!(serde_create(&u), "a");
    });
}

#[test]
fn test_serde_string_or_unit() {
    "
    def text-or-unit (
        rel .is?: text
        rel .is?: ()
    )

    def foo (
        rel .'a': text-or-unit
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "a": "string" });
        assert_json_io_matches!(serde_create(&foo), { "a": null });
    });
}

#[test]
fn test_serde_map_union() {
    "
    def foo (
        rel .'type': 'foo'
        rel .'c': i64
    )
    def bar (
        rel .'type': 'bar'
        rel .'d': i64
    )
    def u (
        rel .is?: foo
        rel .is?: bar
    )
    "
    .compile_then(|test| {
        let [u] = test.bind(["u"]);
        assert_json_io_matches!(serde_create(&u), { "type": "foo", "c": 7});
    });
}

#[test]
fn test_serde_noop_intersection() {
    "
    def bar ()
    def foo (
        rel .is: bar
        rel .'foobar': bar
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "foobar": {} });
    });
}

#[test]
fn test_serde_many_cardinality() {
    "
    def foo (
        rel .'s': {text}
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "s": []});
        assert_json_io_matches!(serde_create(&foo), { "s": ["a", "b"]});
    });
}

#[test]
fn test_serde_infinite_sequence() {
    "
    def foo (
        rel .  ..2: i64
        rel . 2..4: text
        rel . 5..6: i64
        rel . 6.. : i64
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), [42, 43, "a", "b", null, 44]);
        assert_json_io_matches!(serde_create(&foo), [42, 43, "a", "b", null, 44, 45, 46]);
        assert_error_msg!(
            serde_create(&foo).to_value(json!([77])),
            "invalid length 1, expected sequence with minimum length 6 at line 1 column 4"
        );
    });
}

#[test]
fn test_serde_uuid() {
    "
    def my_id ( rel .is: uuid )
    "
    .compile_then(|test| {
        let [my_id] = test.bind(["my_id"]);
        assert_matches!(
            serde_create(&my_id).to_value(json!("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8")),
            Ok(Value::OctetSequence(..))
        );
        assert_json_io_matches!(serde_create(&my_id), "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
        assert_error_msg!(
            serde_create(&my_id).to_value(json!(42)),
            "invalid type: integer `42`, expected `uuid` at line 1 column 2"
        );
        assert_error_msg!(
            serde_create(&my_id).to_value(json!("foobar")),
            r#"invalid type: string "foobar", expected `uuid` at line 1 column 8"#
        );
    });
}

#[test]
fn test_serde_datetime() {
    "
    def my_dt ( rel .is: datetime )
    "
    .compile_then(|test| {
        let [my_dt] = test.bind(["my_dt"]);
        assert_matches!(
            serde_create(&my_dt).to_value(json!("1983-10-01T01:31:32.59+01:00")),
            Ok(Value::ChronoDateTime(..))
        );
        assert_json_io_matches!(
            serde_create(&my_dt),
            "1983-10-01T01:31:32.59+01:00" == "1983-10-01T00:31:32.590+00:00"
        );
        assert_error_msg!(
            serde_create(&my_dt).to_value(json!(42)),
            "invalid type: integer `42`, expected `datetime` at line 1 column 2"
        );
        assert_error_msg!(
            serde_create(&my_dt).to_value(json!("foobar")),
            r#"invalid type: string "foobar", expected `datetime` at line 1 column 8"#
        );
    });
}

#[test]
fn test_integer_default() {
    "
    def foo (
        rel .'bar'[rel .default := 42]: i64
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "bar": 1 } == { "bar": 1 });
        assert_json_io_matches!(serde_create(&foo), {} == { "bar": 42 });
    });
}

#[test]
fn test_boolean_default() {
    "
    def foo (
        rel .'active'[rel .default := true]: boolean
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "active": false } == { "active": false });
        assert_json_io_matches!(serde_create(&foo), {} == { "active": true });
    });
}

#[test]
fn test_i64_range_constrained() {
    "
    def percentage (
        rel .is: i64
        rel .min: 0
        rel .max: 100
    )
    "
    .compile_then(|test| {
        let [percentage] = test.bind(["percentage"]);
        assert_json_io_matches!(serde_create(&percentage), 0 == 0);
        assert_json_io_matches!(serde_create(&percentage), 100 == 100);
        assert_error_msg!(
            serde_create(&percentage).to_value_variant(json!(1000)),
            r#"invalid type: integer `1000`, expected integer in range 0..=100 at line 1 column 4"#
        );
    });
}

#[test]
fn test_integer_range_constrained() {
    "
    def foo (
        rel .is: integer
        rel .min: -1
        rel .max: 1
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), 0 == 0);
        assert_json_io_matches!(serde_create(&foo), (-1) == (-1));
        assert_error_msg!(
            serde_create(&foo).to_value_variant(json!(-2)),
            r#"invalid type: integer `-2`, expected integer in range -1..=1 at line 1 column 2"#
        );
    });
}

#[test]
fn test_f64_range_constrained() {
    "
    def fraction (
        rel .is: f64
        rel .min: 0
        rel .max: 1
    )
    "
    .compile_then(|test| {
        let [fraction] = test.bind(["fraction"]);
        assert_json_io_matches!(serde_create(&fraction), 0 == 0.0);
        assert_json_io_matches!(serde_create(&fraction), 0.0 == 0.0);
        assert_json_io_matches!(serde_create(&fraction), 0.5 == 0.5);
        assert_json_io_matches!(serde_create(&fraction), 1.0 == 1.0);
        assert_error_msg!(
            serde_create(&fraction).to_value_variant(json!(3.14)),
            r#"invalid type: floating point `3.14`, expected float in range 0..=1 at line 1 column 4"#
        );
    });
}

#[test]
fn test_float_default() {
    "
    def foo (
        rel .'bar'[rel .default := 42]: f64
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "bar": 1.618 } == { "bar": 1.618 });
        assert_json_io_matches!(serde_create(&foo), {} == { "bar": 42.0 });
    });
}

#[test]
fn test_string_default() {
    "
    def foo (
        rel .'bar'[rel .default := 'baz']: text
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "bar": "yay" } == { "bar": "yay" });
        assert_json_io_matches!(serde_create(&foo), {} == { "bar": "baz" });
    });
}

#[test]
fn test_prop_union() {
    "
    def vec3(
        /// A vector component
        rel .'x'|'y'|'z': (
            rel .is: i64
        )
    )
    "
    .compile_then(|test| {
        let [vec3] = test.bind(["vec3"]);
        assert_json_io_matches!(serde_create(&vec3), { "x": 1, "y": 2, "z": 3 });
    });
}

#[test]
// BUG: this mirrors how enums are constucted for text, should work
fn union_integers() {
    "
    def level (
        rel .is?: 1
        rel .is?: 2
        rel .is?: 3
    )
    "
    .compile_then(|test| {
        let [level] = test.bind(["level"]);
        assert_json_io_matches!(serde_create(&level), 1);
        assert_error_msg!(
            serde_create(&level).to_value_variant(json!(42)),
            r#"invalid type: integer `42`, expected `level` (one of 1, 2, 3) at line 1 column 2"#
        );
    });
}

#[test]
fn test_jsonml() {
    "
    def tag ()
    def tag_name ()
    def attributes ()

    def element (
        rel .is?: tag
        rel .is?: text
    )

    rel tag 0: tag_name

    // BUG: should have default `{}` and serde would skip this if not a map
    // (also in serialization if this equals the default value!)
    rel tag 1: attributes

    rel tag 2..: element

    rel tag_name is?: 'div'
    rel tag_name is?: 'em'
    rel tag_name is?: 'strong'

    // BUG: should accept any string as key
    rel attributes 'class'?: text
    "
    .compile_then(|test| {
        let [element] = test.bind(["element"]);

        assert_json_io_matches!(serde_create(&element), "text");
        assert_json_io_matches!(serde_create(&element), ["div", {}]);
        assert_json_io_matches!(serde_create(&element), ["div", {}, "text"]);
        assert_json_io_matches!(serde_create(&element),
            ["div", {}, ["em", {}, "text1"], ["strong", { "class": "foo" }]]
        );
    });
}

#[test]
fn test_serde_with_raw_id_overridde_profile() {
    let processor_profile = ProcessorProfile {
        overridden_id_property_key: Some("__ID"),
        ignored_property_keys: &["IGNORE1", "IGNORE2"],
        id_format: ScalarFormat::RawText,
        flags: ProcessorProfileFlags::empty(),
    };

    "
    def foo (
        rel .'prefix_id'|id: (
            fmt '' => 'prefix/' => uuid => .
        )
    )

    def bar (
        rel .'int_id'|id: ( rel .is: i64 )
    )
    "
    .compile_then(|test| {
        let [foo, bar] = test.bind(["foo", "bar"]);

        let foo_value = serde_create(&foo)
            .with_profile(processor_profile.clone())
            .to_value_raw(json!({
                "IGNORE1": 1,
                "__ID": "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
                "IGNORE2": 2
            }))
            .unwrap();

        expect_eq!(
            actual = serde_create(&foo)
                .with_profile(processor_profile.clone())
                .as_json(&foo_value),
            expected = json!({ "__ID": "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" })
        );

        expect_eq!(
            actual = serde_create(&foo).as_json(&foo_value),
            expected = json!({ "prefix_id": "prefix/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8" }),
        );

        let bar_value = serde_create(&bar)
            .with_profile(processor_profile.clone())
            .to_value_raw(json!({ "__ID": "1337" }))
            .unwrap();

        expect_eq!(
            actual = ontol_test_utils::serde_helper::serde_create(&bar)
                .with_profile(processor_profile.clone())
                .as_json(&bar_value),
            expected = json!({ "__ID": "1337" })
        );

        expect_eq!(
            actual = serde_create(&bar).as_json(&bar_value),
            expected = json!({ "int_id": 1337 }),
        );
    });
}

#[test]
fn test_serde_with_raw_prefix_text_id_overridde_profile() {
    let processor_profile = ProcessorProfile {
        overridden_id_property_key: Some("__ID"),
        ignored_property_keys: &[],
        id_format: ScalarFormat::RawText,
        flags: ProcessorProfileFlags::empty(),
    };

    "
    def baz (
        rel .'prefix_id'|id: (
            fmt '' => 'prefix/' => text => .
        )
    )
    "
    .compile_then(|test| {
        let [baz] = test.bind(["baz"]);

        let baz_value = serde_create(&baz)
            .with_profile(processor_profile.clone())
            .to_value_raw(json!({ "__ID": "mytext" }))
            .unwrap();

        expect_eq!(
            actual = ontol_test_utils::serde_helper::serde_create(&baz)
                .with_profile(processor_profile.clone())
                .as_json(&baz_value),
            expected = json!({ "__ID": "mytext" })
        );

        expect_eq!(
            actual = serde_create(&baz).as_json(&baz_value),
            expected = json!({ "prefix_id": "prefix/mytext" }),
        );
    });
}

#[test]
fn test_serde_open_properties() {
    "
    def(open) foo (
        rel .'closed': text
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo).enable_open_data(), {
            "closed": "A",
            "int": 2,
            "float": 3.14,
            "bool": true,
            "null": null,
            "dict": {
                "key": "value"
            },
            "array": ["value"]
        });
    });
}

#[test]
fn test_serde_recursion_limit() {
    "
    def foo (
        rel .'child': foo
    )
    "
    .compile_then(|test| {
        const RECURSION_LIMIT: u16 = 32;
        let mut json = String::new();

        for _ in 0..RECURSION_LIMIT + 1 {
            json.push_str(r#"{ "child": "#);
        }

        let [foo] = test.bind(["foo"]);
        let error = test
            .ontology
            .new_serde_processor(foo.serde_operator_addr(), ProcessorMode::Create)
            .with_level(ProcessorLevel::new_root_with_recursion_limit(
                RECURSION_LIMIT,
            ))
            .deserialize(&mut serde_json::Deserializer::from_str(&json))
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Recursion limit exceeded at line 1 column 361"
        );
    });
}

#[test]
fn test_serialize_raw_tree_only() {
    "
    def bar (
        rel .'key'|id: ( rel .is: text )
        rel .'bar_field': text
    )
    def foo (
        rel .'key'|id: ( rel .is: text )
        rel .'foo_field': text
        rel .'bar'?: bar
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        let entity = serde_raw(&foo)
            .to_value_raw(json!({
                "key": "a",
                "foo_field": "1",
                "bar": {
                    "key": "b",
                    "bar_field": "2"
                }
            }))
            .unwrap();
        assert_eq!(
            serde_raw_tree_only(&foo).as_json(&entity),
            json!({
                "key": "a",
                "foo_field": "1"
            })
        );
    });
}

#[test]
fn test_serialize_raw_tree_only_artist_and_instrument() {
    ARTIST_AND_INSTRUMENT.1.compile_then(|test| {
        let [artist] = test.bind(["artist"]);
        let entity = serde_raw(&artist)
            .to_value_raw(json!({
                "ID": "artist/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
                "name": "Jimi",
                "plays": [
                    {
                        "ID": "instrument/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
                        "name": "guitar",
                        "_edge": {
                            "how_much": "a lot"
                        }
                    }
                ]
            }))
            .unwrap();
        assert_eq!(
            serde_raw_tree_only(&artist).as_json(&entity),
            json!({
                "ID": "artist/a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
                "name": "Jimi",
            })
        );
    });
}

#[test]
fn test_serde_gitmesh_id_union() {
    GITMESH.1.compile_then(|test| {
        let [repository] = test.bind(["Repository"]);
        assert_json_io_matches!(serde_create(&repository), {
            "handle": "repo1",
            "owner": { "id": "user/bob" },
        });
    });
}
