use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use ontol_test_utils::{assert_error_msg, assert_json_io_matches, serde_utils::*, TestCompile};
use serde_json::json;
use test_log::test;

#[test]
fn test_serde_empty_type() {
    "pub def foo".compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, {});
    });
}

#[test]
fn test_serde_value_type() {
    "
    pub def foo
    rel foo is: string
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, "string");
    });
}

#[test]
fn test_serde_booleans() {
    "
    pub def f { rel .is: false }
    pub def t { rel .is: true }
    pub def b { rel .is: boolean }
    "
    .compile_ok(|test| {
        let [f, t, b] = test.bind(["f", "t", "b"]);

        assert_json_io_matches!(f, Create, false);
        assert_json_io_matches!(t, Create, true);
        assert_json_io_matches!(b, Create, false);
        assert_json_io_matches!(b, Create, true);

        assert_error_msg!(
            create_de(&f).data(json!(true)),
            "invalid type: boolean `true`, expected false at line 1 column 4"
        );
        assert_error_msg!(
            create_de(&t).data(json!(false)),
            "invalid type: boolean `false`, expected true at line 1 column 5"
        );
    });
}

#[test]
fn test_serde_map_type() {
    "
    pub def foo
    rel foo 'a': string
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, { "a": "string" });
    });
}

#[test]
fn test_serde_complex_type() {
    "
    pub def foo
    def bar
    rel foo 'a': string
    rel foo 'b': bar
    rel bar 'c': string
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, { "a": "A", "b": { "c": "C" }});
    });
}

#[test]
fn test_serde_sequence() {
    "
    pub def t
    rel t 0: string
    rel t 1: i64
    "
    .compile_ok(|test| {
        let [t] = test.bind(["t"]);
        assert_json_io_matches!(t, Create, ["a", 1]);
    });
}

#[test]
fn test_serde_value_union1() {
    "
    pub def u
    rel u is?: 'a'
    rel u is?: 'b'
    "
    .compile_ok(|test| {
        let [u] = test.bind(["u"]);
        assert_json_io_matches!(u, Create, "a");
    });
}

#[test]
fn test_serde_string_or_unit() {
    "
    def string-or-unit
    rel string-or-unit is?: string
    rel string-or-unit is?: ()

    pub def foo
    rel foo 'a': string-or-unit
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, { "a": "string" });
        assert_json_io_matches!(foo, Create, { "a": null });
    });
}

#[test]
fn test_serde_map_union() {
    "
    def foo
    def bar
    rel foo 'type': 'foo'
    rel foo 'c': i64
    rel bar 'type': 'bar'
    rel bar 'd': i64

    pub def u
    rel u is?: foo
    rel u is?: bar
    "
    .compile_ok(|test| {
        let [u] = test.bind(["u"]);
        assert_json_io_matches!(u, Create, { "type": "foo", "c": 7});
    });
}

#[test]
fn test_serde_noop_intersection() {
    "
    def bar
    pub def foo {
        rel .is: bar
        rel .'foobar': bar
    }
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, { "foobar": {} });
    });
}

#[test]
fn test_serde_many_cardinality() {
    "
    pub def foo
    rel foo 's': [string]
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, { "s": []});
        assert_json_io_matches!(foo, Create, { "s": ["a", "b"]});
    });
}

#[test]
fn test_serde_infinite_sequence() {
    "
    pub def foo
    rel foo  ..2: i64
    rel foo 2..4: string
    rel foo 5..6: i64
    rel foo 6.. : i64
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, [42, 43, "a", "b", null, 44]);
        assert_json_io_matches!(foo, Create, [42, 43, "a", "b", null, 44, 45, 46]);
        assert_error_msg!(
            create_de(&foo).data(json!([77])),
            "invalid length 1, expected sequence with minimum length 6 at line 1 column 4"
        );
    });
}

#[test]
fn test_serde_uuid() {
    "
    pub def my_id
    rel my_id is: uuid
    "
    .compile_ok(|test| {
        let [my_id] = test.bind(["my_id"]);
        assert_matches!(
            create_de(&my_id).data(json!("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8")),
            Ok(Data::Uuid(_))
        );
        assert_json_io_matches!(my_id, Create, "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
        assert_error_msg!(
            create_de(&my_id).data(json!(42)),
            "invalid type: integer `42`, expected `uuid` at line 1 column 2"
        );
        assert_error_msg!(
            create_de(&my_id).data(json!("foobar")),
            r#"invalid type: string "foobar", expected `uuid` at line 1 column 8"#
        );
    });
}

#[test]
fn test_serde_datetime() {
    "
    pub def my_dt
    rel my_dt is: datetime
    "
    .compile_ok(|test| {
        let [my_dt] = test.bind(["my_dt"]);
        assert_matches!(
            create_de(&my_dt).data(json!("1983-10-01T01:31:32.59+01:00")),
            Ok(Data::ChronoDateTime(_))
        );
        assert_json_io_matches!(
            my_dt,
            Create,
            "1983-10-01T01:31:32.59+01:00" == "1983-10-01T00:31:32.590+00:00"
        );
        assert_error_msg!(
            create_de(&my_dt).data(json!(42)),
            "invalid type: integer `42`, expected `datetime` at line 1 column 2"
        );
        assert_error_msg!(
            create_de(&my_dt).data(json!("foobar")),
            r#"invalid type: string "foobar", expected `datetime` at line 1 column 8"#
        );
    });
}

#[test]
fn test_integer_default() {
    "
    pub def foo {
        rel .'bar'(rel .default := 42): i64
    }
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, { "bar": 1 } == { "bar": 1 });
        assert_json_io_matches!(foo, Create, {} == { "bar": 42 });
    });
}

#[test]
fn test_i64_range_constrained() {
    "
    pub def percentage {
        rel .is: i64
        rel .min: 0
        rel .max: 100
    }
    "
    .compile_ok(|test| {
        let [percentage] = test.bind(["percentage"]);
        assert_json_io_matches!(percentage, Create, 0 == 0);
        assert_json_io_matches!(percentage, Create, 100 == 100);
        assert_error_msg!(
            create_de(&percentage).data_variant(json!(1000)),
            r#"invalid type: integer `1000`, expected integer in range 0..=100 at line 1 column 4"#
        );
    });
}

#[test]
fn test_integer_range_constrained() {
    "
    pub def foo {
        rel .is: integer
        rel .min: -1
        rel .max: 1
    }
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, 0 == 0);
        assert_json_io_matches!(foo, Create, (-1) == (-1));
        assert_error_msg!(
            create_de(&foo).data_variant(json!(-2)),
            r#"invalid type: integer `-2`, expected integer in range -1..=1 at line 1 column 2"#
        );
    });
}

#[test]
fn test_f64_range_constrained() {
    "
    pub def fraction {
        rel .is: f64
        rel .min: 0
        rel .max: 1
    }
    "
    .compile_ok(|test| {
        let [fraction] = test.bind(["fraction"]);
        assert_json_io_matches!(fraction, Create, 0 == 0.0);
        assert_json_io_matches!(fraction, Create, 0.0 == 0.0);
        assert_json_io_matches!(fraction, Create, 0.5 == 0.5);
        assert_json_io_matches!(fraction, Create, 1.0 == 1.0);
        assert_error_msg!(
            create_de(&fraction).data_variant(json!(3.14)),
            r#"invalid type: floating point `3.14`, expected float in range 0..=1 at line 1 column 4"#
        );
    });
}

#[test]
fn test_float_default() {
    "
    pub def foo {
        rel .'bar'(rel .default := 42): f64
    }
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, { "bar": 1.618 } == { "bar": 1.618 });
        assert_json_io_matches!(foo, Create, {} == { "bar": 42.0 });
    });
}

#[test]
fn test_string_default() {
    "
    pub def foo {
        rel .'bar'(rel .default := 'baz'): string
    }
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(foo, Create, { "bar": "yay" } == { "bar": "yay" });
        assert_json_io_matches!(foo, Create, {} == { "bar": "baz" });
    });
}

#[test]
fn test_prop_union() {
    "
    pub def vec3 {
        /// A vector component
        rel .'x'|'y'|'z': {
            rel .is: i64
        }
    }
    "
    .compile_ok(|test| {
        let [foo] = test.bind(["vec3"]);
        assert_json_io_matches!(foo, Create, { "x": 1, "y": 2, "z": 3 });
    });
}

#[test]
fn test_jsonml() {
    "
    def tag
    def tag_name
    def attributes

    pub def element {
        rel .is?: tag
        rel .is?: string
    }

    rel tag 0: tag_name

    // BUG: should have default `{}` and serde would skip this if not a map
    // (also in serialization if this equals the default value!)
    rel tag 1: attributes

    rel tag 2..: element

    rel tag_name is?: 'div'
    rel tag_name is?: 'em'
    rel tag_name is?: 'strong'

    // BUG: should accept any string as key
    rel attributes 'class'?: string
    "
    .compile_ok(|test| {
        let [element] = test.bind(["element"]);

        assert_json_io_matches!(element, Create, "text");
        assert_json_io_matches!(element, Create, ["div", {}]);
        assert_json_io_matches!(element, Create, ["div", {}, "text"]);
        assert_json_io_matches!(element, Create,
            ["div", {}, ["em", {}, "text1"], ["strong", { "class": "foo" }]]
        );
    });
}
