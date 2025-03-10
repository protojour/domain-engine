use ontol_macros::test;
use ontol_test_utils::{
    TestCompile, TestPackages, file_url, serde_helper::serde_raw, test_map::AsKey,
};
use serde_json::json;

#[test]
fn test_map_simple() {
    "
    def foo (
        rel {foo} 'f': text
    )
    def bar (
        rel {bar} 'b': text
    )
    map(
        foo('f': x),
        bar('b': x),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({ "f": "my_value"}),
            json!({ "b": "my_value"}),
        );
        test.mapper().assert_map_eq(
            ("bar", "foo"),
            json!({ "b": "my_value"}),
            json!({ "f": "my_value"}),
        );
    });
}

#[test]
fn test_map_value_to_primitive() {
    "
    def string (rel* is: text)
    def foo (rel* 'a': string)
    def bar (rel* 'b': text)
    map(
        foo('a': x),
        bar('b': x),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({ "a": "my_value"}),
            json!({ "b": "my_value"}),
        );
        test.mapper().assert_map_eq(
            ("bar", "foo"),
            json!({ "b": "my_value"}),
            json!({ "a": "my_value"}),
        );
    });
}

#[test]
fn test_meters() {
    "
    def meters (rel* is: i64)
    def millimeters (rel* is: i64)
    map(
        millimeters(m * 1000),
        meters(m),
    )
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("meters", "millimeters"), json!(5), json!(5000));
        test.mapper()
            .assert_map_eq(("millimeters", "meters"), json!(5000), json!(5));
    });
}

#[test]
fn test_temperature() {
    "
    def celsius (rel* is: f64)
    def fahrenheit (rel* is: f64)
    map(
        fahrenheit(c * 9 / 5 + 32),
        celsius(c),
    )
    "
    .compile_then(|test| {
        let c_to_f = ("celsius", "fahrenheit");

        test.mapper().assert_map_eq(c_to_f, json!(10), json!(50.0));
        test.mapper().assert_map_eq(c_to_f, json!(0), json!(32.0));
        test.mapper()
            .assert_map_eq(c_to_f, json!(100), json!(212.0));
        test.mapper().assert_map_eq(c_to_f, json!(37), json!(98.6));
        test.mapper()
            .assert_map_eq(c_to_f, json!(-273.15), json!(-459.67));

        let f_to_c = ("fahrenheit", "celsius");
        test.mapper().assert_map_eq(f_to_c, json!(50), json!(10.0));
    });
}

#[test]
fn test_nested_optional_attribute() {
    "
    def seconds (rel* is: i64)
    def years (rel* is: i64)

    map(
        seconds(y * 60 * 60 * 24 * 365),
        years(y)
    )

    def person (rel* 'age'?: years)
    def creature (rel* 'age'?: seconds)

    map(
        person('age'?: a),
        creature('age'?: a),
    )

    def person_container (rel* 'person'?: person)
    def creature_container (rel* 'creature'?: creature)

    map(
        person_container(
            'person': person(
                'age'?: a
            )
        ),
        creature_container(
            'creature': creature(
                'age'?: a
            )
        ),
    )
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("person", "creature"), json!({}), json!({}));
        test.mapper().assert_map_eq(
            ("person", "creature"),
            json!({ "age": 42 }),
            json!({ "age": 1324512000 }),
        );
        test.mapper().assert_map_eq(
            ("person_container", "creature_container"),
            json!({}),
            json!({ "creature": {} }),
        );
        test.mapper().assert_map_eq(
            ("person_container", "creature_container"),
            json!({ "person": {} }),
            json!({ "creature": {} }),
        );
        test.mapper().assert_map_eq(
            ("person_container", "creature_container"),
            json!({ "person": { "age": 42 } }),
            json!({ "creature": { "age": 1324512000 } }),
        );
    });
}

#[test]
fn test_map_value_to_struct_no_func() {
    "
    def one (rel* is: text)
    def two (rel* 'a': text)
    map(
        one(x),
        two('a': x)
    )
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("one", "two"), json!("foo"), json!({ "a": "foo" }));
        test.mapper()
            .assert_map_eq(("two", "one"), json!({ "a": "foo" }), json!("foo"));
    });
}

#[test]
fn test_map_value_to_struct_func() {
    "
    def one (rel* is: i64)
    def two (rel* 'a': i64)
    map(
        one(x),
        two('a': x * 2),
    )
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("one", "two"), json!(2), json!({ "a": 4 }));
        test.mapper()
            .assert_map_eq(("two", "one"), json!({ "a": 4 }), json!(2));
    });
}

#[test]
fn test_map_into_default_field_using_default_value() {
    "
    def empty ()
    def target (
        rel* 'field'[rel* default := 'Default!']: text
    )
    map(
        empty(),
        target()
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("empty", "target"),
            json!({}),
            json!({ "field": "Default!" }),
        );
        test.mapper().assert_map_eq(
            ("target", "empty"),
            json!({ "field": "whatever" }),
            json!({}),
        );
    });
}

#[test]
fn test_map_into_default_field_using_provided_value() {
    "
    def required (
        rel* 'field': text
    )
    def target (
        rel* 'field'[rel* default := 'Default!']: text
    )
    map(
        required('field': val),
        target('field': val),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("required", "target"),
            json!({ "field": "This" }),
            json!({ "field": "This" }),
        );
        test.mapper().assert_map_eq(
            ("target", "required"),
            json!({ "field": "This" }),
            json!({ "field": "This" }),
        );
    });
}

#[test]
fn test_map_into_default_field_using_map_provided() {
    "
    def empty ()
    def target (
        rel* 'field'[rel* default := 'Default!']: text
    )
    map(
        empty(),
        target('field': 'Mapped!'),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("empty", "target"),
            json!({}),
            json!({ "field": "Mapped!" }),
        );
        test.mapper().assert_map_eq(
            ("target", "empty"),
            json!({ "field": "whatever" }),
            json!({}),
        );
    });
}

#[test]
fn test_deep_structural_map() {
    "
    def foo (
        rel* 'a': text
    )
    def foo_inner (
        rel* 'b': text
        rel* 'c': text
    )
    def foo (
        rel* 'inner': foo_inner
    )

    def bar_inner ()
    def bar (
        rel* 'a': text
        rel* 'b': text
        rel* 'inner': bar_inner
    )

    def bar_inner (
        rel* 'c': text
    )

    map(
        foo(
            'a': a,
            'inner': foo_inner('b': b, 'c': c),
        ),
        bar(
            'a': a,
            'b': b,
            'inner': bar_inner('c': c),
        ),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({ "a": "A", "inner": { "b": "B", "c": "C" }}),
            json!({ "a": "A", "b": "B", "inner": { "c": "C" }}),
        );
        test.mapper().assert_map_eq(
            ("bar", "foo"),
            json!({ "a": "A", "b": "B", "inner": { "c": "C" }}),
            json!({ "a": "A", "inner": { "b": "B", "c": "C" }}),
        );
    });
}

#[test]
fn test_map_matching_sequence() {
    "
    def foo (
        rel* 'a': {i64}
    )
    def bar (
        rel* 'b': {i64}
    )
    map(
        foo(
            'a': {..x}
        ),
        bar(
            'b': {..x}
        ),
    )
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("foo", "bar"), json!({ "a": [42] }), json!({ "b": [42] }));
        test.mapper()
            .assert_map_eq(("bar", "foo"), json!({ "b": [42] }), json!({ "a": [42] }));
    });
}

// map call inside sequence
const MAP_IN_SEQUENCE: &str = "
def foo (rel* 'f': text)
def bar (rel* 'b': text)
def foos (rel* 'foos': {foo})
def bars (rel* 'bars': {bar})

map(
    foos('foos': {..x}),
    bars('bars': {..x}),
)
map(
    foo('f': x),
    bar('b': x),
)
";

#[test]
fn test_map_in_sequence_item_empty() {
    let test = MAP_IN_SEQUENCE.compile();
    test.mapper().assert_map_eq(
        ("foos", "bars"),
        json!({ "foos": [] }),
        json!({ "bars": [] }),
    );
    test.mapper().assert_map_eq(
        ("bars", "foos"),
        json!({ "bars": [] }),
        json!({ "foos": [] }),
    );
}

#[test]
fn test_map_in_sequence_item_one() {
    let test = MAP_IN_SEQUENCE.compile();
    test.mapper().assert_map_eq(
        ("foos", "bars"),
        json!({ "foos": [{ "f": "42" }] }),
        json!({ "bars": [{ "b": "42" }] }),
    );
    test.mapper().assert_map_eq(
        ("bars", "foos"),
        json!({ "bars": [{ "b": "42" }] }),
        json!({ "foos": [{ "f": "42" }] }),
    );
}

#[test]
fn test_map_in_sequence_item_many() {
    let test = MAP_IN_SEQUENCE.compile();
    test.mapper().assert_map_eq(
        ("foos", "bars"),
        json!({ "foos": [{ "f": "42" }, { "f": "84" }] }),
        json!({ "bars": [{ "b": "42" }, { "b": "84" }] }),
    );
    test.mapper().assert_map_eq(
        ("bars", "foos"),
        json!({ "bars": [{ "b": "42" }, { "b": "84" }] }),
        json!({ "foos": [{ "f": "42" }, { "f": "84" }] }),
    );
}

#[test]
fn test_sequence_cross_parallel() {
    "
    def foo (rel* 'f': text)
    def bar (rel* 'b': text)
    map(
        foo('f': x),
        bar('b': x),
    )

    def foos (
        rel* 'f1': {foo}
        rel* 'f2': {foo}
    )
    def bars (
        rel* 'b1': {bar}
        rel* 'b2': {bar}
    )
    map(
        foos(
            'f1': {..a},
            'f2': {..b},
        ),
        bars(
            'b2': {..b},
            'b1': {..a},
        ),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foos", "bars"),
            json!({ "f1": [{ "f": "1A" }, { "f": "1B" }], "f2": [{ "f": "2" }] }),
            json!({ "b1": [{ "b": "1A" }, { "b": "1B" }], "b2": [{ "b": "2" }] }),
        );
    });
}

#[test]
fn test_sequence_inner_loop() {
    "
    def foo (rel* 'P': text)
    def bar (rel* 'Q': text)
    map(
        foo('P': x),
        bar('Q': x),
    )

    def f0(
        rel* 'a': {foo}
        rel* 'b': {foo}
    )
    def f1(
        rel* 'a': {f0}
        rel* 'b': {f0}
    )
    def b0(
        rel* 'a': {bar}
        rel* 'b': {bar}
    )
    def b1(
        rel* 'a': {b0}
        rel* 'b': {b0}
    )
    map(
        f1(
            'a': {..f0(
                'a': {..v0},
                'b': {..v1},
            )},
            'b': {..f0(
                'a': {..v2},
                'b': {..v3},
            )},
        ),
        b1(
            'b': {..b0(
                'b': {..v3},
                'a': {..v2},
            )},
            'a': {..b0(
                'b': {..v1},
                'a': {..v0},
            )},
        ),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("f1", "b1"),
            json!({ "a": [{ "a": [{ "P": "0" }], "b": [{ "P": "1" }]}], "b": [{ "a": [{ "P": "2" }], "b": [{ "P": "3" }]}]}),
            json!({ "a": [{ "a": [{ "Q": "0" }], "b": [{ "Q": "1" }]}], "b": [{ "a": [{ "Q": "2" }], "b": [{ "Q": "3" }]}]}),
        );
    });
}

#[test]
fn test_sequence_flat_map1() {
    "
    def foo (
        rel* 'a': text

        def foo_inner (
            rel* 'b': text
        )
        rel* 'inner': {foo_inner}
    )
    def bar (
        rel* 'a': text
        rel* 'b': text
    )

    map(
        @match foo(
            'a': a,
            'inner': {..foo_inner(
                'b': b
            )}
        ),
        bar {
            ..bar(
                'a': a,
                'b': b,
            )
        }
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar".seq()),
            json!({ "a": "A", "inner": [{ "b": "B0" }, { "b": "B1" }] }),
            json!([{ "a": "A", "b": "B0" }, { "a": "A", "b": "B1" }]),
        );
    });
}

// FIXME: This should work both ways in principle, even if the backward mapping is fallible:
#[test]
fn test_sequence_composer_no_iteration() {
    "
    def foo (
        rel* 'a': i64
        rel* 'b': i64
    )
    def bar (
        rel* 'ab': {i64}
    )

    map(
        @match foo(
            'a': a,
            'b': b,
        ),
        bar(
            'ab': {a, b}
        ),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({
                "a": 1,
                "b": 2,
            }),
            json!({ "ab": [1, 2] }),
        );
    });
}

#[test]
fn test_sequence_composer_with_iteration() {
    "
    def foo (
        rel* 'a': i64
        rel* 'b': {i64}
        rel* 'c': i64
    )
    def bar (
        rel* 'abc': {i64}
    )

    map(
        @match foo(
            'a': a,
            'b': {..b},
            'c': c,
        ),
        bar(
            'abc': {a, ..b, c}
        ),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("foo", "bar"),
            json!({
                "a": 1,
                "b": [2, 3],
                "c": 4
            }),
            json!({
                "abc": [1, 2, 3, 4]
            }),
        );
    });
}

#[test]
fn test_map_complex_flow() {
    // FIXME: This should be a one-way mapping.
    // there is no way two variables (e.g. `two.a` and `two.c`) can flow back into the same slot without data loss.
    // But perhaps let's accept that this might be what the user wants.
    // For example, when two `:x`es flow into one property, we can choose the first one.
    "
    def one (
        rel* 'a': text
        rel* 'b': text
    )
    def two (
        rel* 'a': text
        rel* 'b': text
        rel* 'c': text
        rel* 'd': text
    )

    map(
        @match one(
            'a': x,
            'b': y,
        ),
        two(
            'a': x,
            'b': y,
            'c': x,
            'd': y,
        )
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("one", "two"),
            json!({ "a": "X", "b": "Y" }),
            json!({ "a": "X", "b": "Y", "c": "X", "d": "Y" }),
        );
    });
}

#[test]
fn test_map_delegation() {
    TestPackages::with_static_sources([
        (
            file_url("root"),
            "
            use 'SI' as SI

            def car (rel* 'length': SI.meters)
            def vehicle (rel* 'length': SI.millimeters)

            map(
                car('length': l),
                vehicle('length': l),
            )
            ",
        ),
        (
            file_url("SI"),
            "
            def meters (rel* is: f64)
            def millimeters (rel* is: f64)

            map(
                millimeters(m * 1000),
                meters(m),
            )
            ",
        ),
    ])
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("car", "vehicle"),
            json!({ "length": 3 }),
            json!({ "length": 3000.0 }),
        );
        test.mapper().assert_map_eq(
            ("vehicle", "car"),
            json!({ "length": 2000 }),
            json!({ "length": 2.0 }),
        );
    });
}

#[test]
fn test_map_abstract_scalar_repr_templating() {
    TestPackages::with_static_sources([
        (
            file_url("vehicle"),
            "
            use 'car' as car
            use 'SI' as SI

            def vehicle (
                rel* 'l': length
                rel* 'h': height
            )
            def length (
                rel* is: SI.millimeters
                rel* is: f64
            )
            def height (
                rel* is: SI.millimeters
                rel* is: i64
            )

            map(
                vehicle('l': l, 'h': h),
                car.car('l': l, 'h': h),
            )
            ",
        ),
        (
            file_url("car"),
            "
            use 'SI' as SI

            def car (
                rel* 'l': length
                rel* 'h': height
            )
            def length (
                rel* is: SI.meters
                rel* is: f64
            )
            def height (
                rel* is: SI.meters
                rel* is: i64
            )
            ",
        ),
        (
            file_url("SI"),
            "
            def meters (rel* is: number)
            def millimeters (rel* is: number)

            map @abstract(
                millimeters(m * 1000),
                meters(m),
            )
            ",
        ),
    ])
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("car.car", "vehicle.vehicle"),
            json!({ "l": 3.0, "h": 1 }),
            json!({ "l": 3000.0, "h": 1000 }),
        );
        test.mapper().assert_map_eq(
            ("vehicle.vehicle", "car.car"),
            json!({ "l": 2000.0, "h": 1000 }),
            json!({ "l": 2.0, "h": 1 }),
        );
    });
}

#[test]
fn test_map_abstract_scalar_trivial() {
    TestPackages::with_static_sources([
        (
            file_url("vehicle"),
            "
            use 'car' as car
            use 'SI' as SI

            def vehicle (
                rel* 'l': length
            )
            def length (
                rel* is: SI.millimeters
                rel* is: f64
            )

            map(
                vehicle('l': l),
                car.car('l': l),
            )
            ",
        ),
        (
            file_url("car"),
            "
            use 'SI' as SI

            def car (
                rel* 'l': length
            )
            def length (
                rel* is: SI.millimeters
                rel* is: f64
            )
            ",
        ),
        (
            file_url("SI"),
            "
            def meters (rel* is: number)
            def millimeters (rel* is: number)

            map @abstract(
                millimeters(m * 1000),
                meters(m),
            )
            ",
        ),
    ])
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("car.car", "vehicle.vehicle"),
            json!({ "l": 3000.0 }),
            json!({ "l": 3000.0 }),
        );
        test.mapper().assert_map_eq(
            ("vehicle.vehicle", "car.car"),
            json!({ "l": 2000.0 }),
            json!({ "l": 2000.0 }),
        );
        test.mapper().assert_map_eq(
            ("car.length", "vehicle.length"),
            json!(3000.0),
            json!(3000.0),
        );
    });
}

#[test]
fn test_map_dependent_scoping() {
    "
    def one (
        rel* 'total_weight': i64
        rel* 'net_weight': i64
    )
    def two (
        rel* 'net_weight': i64
        rel* 'container_weight': i64
    )

    map(
        one(
            'total_weight': t,
            'net_weight': n,
        ),
        two(
            'net_weight': n,
            'container_weight': t - n,
        )
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("one", "two"),
            json!({ "total_weight": 100, "net_weight": 75 }),
            json!({ "net_weight": 75, "container_weight": 25 }),
        );
        test.mapper().assert_map_eq(
            ("two", "one"),
            json!({ "net_weight": 75, "container_weight": 25 }),
            json!({ "total_weight": 100, "net_weight": 75 }),
        );
    });
}

#[test]
fn test_seq_scope_escape1() {
    "
    def foo ()

    def bar (
        rel* 'foo': foo
        rel* 'p1': {text}
    )

    def baz (
        rel* 'foo': foo
        rel* 'p1': {text}
    )

    def qux (
        rel* 'baz': baz
    )

    map(
        @match bar(
            'foo': foo(),
            'p1': {..p1},
        ),
        qux(
            'baz': baz(
                'foo': foo(),
                'p1': {..p1},
            )
        )
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("bar", "qux"),
            json!({ "foo": {}, "p1": ["1"] }),
            json!({ "baz": { "foo": {}, "p1": ["1"] } }),
        );
    });
}

#[test]
fn test_seq_scope_escape2() {
    "
    def foo (
        rel* 'p0': {text}
    )

    def bar (
        rel* 'foo': foo
        rel* 'p1': {text}
    )

    def baz (
        rel* 'foo': foo
        rel* 'p1': {text}
    )

    def qux (
        rel* 'baz': baz
    )

    map(
        @match bar(
            'foo': foo(
                'p0': {..p0},
            ),
            'p1': {..p1},
        ),
        qux(
            'baz': baz(
                'foo': foo(
                    'p0': {..p0}
                ),
                'p1': {..p1}
            ),
        ),
    )
    "
    .compile_then(|test| {
        test.mapper().assert_map_eq(
            ("bar", "qux"),
            json!({ "foo": { "p0": ["0"] }, "p1": ["1"] }),
            json!({ "baz": { "foo": { "p0": ["0"] }, "p1": ["1"] } }),
        );
    });
}

#[test]
fn test_map_open_data_on_root_struct() {
    "
    def @open foo (rel* 'p0': {text})
    def @open bar (rel* 'p1': {text})

    map(
        foo('p0': {..x}),
        bar('p1': {..x}),
    )
    "
    .compile_then(|test| {
        let mapper = test
            .mapper()
            .with_serde_helper(|type_binding| serde_raw(type_binding).enable_open_data());

        mapper.assert_map_eq(
            ("foo", "bar"),
            json!({ "p0": ["X"], "open": { "key": "value" } }),
            json!({ "p1": ["X"], "open": { "key": "value" }}),
        );
    });
}

#[test]
fn test_map_spread_concat() {
    "
    def foo (rel* 'p0': {text} rel* 'p1': {text})
    def bar (rel* 'p2': {text})

    map(
        @match foo(
            'p0': { ..p0 },
            'p1': { ..p1 }
        ),
        bar(
            'p2': { ..p0, ..p1 }
        ),
    )
    "
    .compile_then(|test| {
        let mapper = test
            .mapper()
            .with_serde_helper(|type_binding| serde_raw(type_binding).enable_open_data());

        mapper.assert_map_eq(
            ("foo", "bar"),
            json!({ "p0": ["a", "b"], "p1": ["c"] }),
            json!({ "p2": ["a", "b", "c"] }),
        );
    });
}

#[test]
fn test_map_symbol_simple() {
    "
    sym { a, b }

    map(a(), b())
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq(("a", "b"), json!("a"), json!("b"));
    });
}

#[test]
fn test_map_symbol_auto_union() {
    "
    sym { a, b }
    def ab (
        rel* is?: a
        rel* is?: b
    )

    sym { x, y }
    def xy (
        rel* is?: x
        rel* is?: y
    )

    sym { f, g }
    def fg (
        rel* is?: f
        rel* is?: g
    )

    map(a(), x())
    map(b(), y())
    map(b(), f())
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq_variant(("ab", "xy"), json!("a"), json!("x"));
        test.mapper()
            .assert_map_eq_variant(("ab", "xy"), json!("b"), json!("y"));
        test.mapper()
            .assert_map_eq_variant(("xy", "ab"), json!("x"), json!("a"));

        assert!(test.mapper().get_mapper_proc(("ab", "fg")).is_none());
    });
}

#[test]
fn test_auto_union_forced_surjection() {
    "
    sym { a, b, c }
    def abc (
        rel* is?: a
        rel* is?: b
        rel* is?: c
    )

    sym { x, y }
    def xy (
        rel* is?: x
        rel* is?: y
    )

    // abc maps to xy, but not vice versa:
    map(a(), x())
    map(b(), y())
    map(c(), y())
    "
    .compile_then(|test| {
        test.mapper()
            .assert_map_eq_variant(("abc", "xy"), json!("c"), json!("y"));

        assert!(test.mapper().get_mapper_proc(("xy", "abc")).is_none());
    });
}
