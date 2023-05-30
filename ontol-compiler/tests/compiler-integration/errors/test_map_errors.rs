use ontol_test_utils::TestCompile;
use pretty_assertions::assert_eq;
use test_log::test;

#[test]
fn map_undeclared_variable() {
    "
    type foo
    type bar
    map {
        foo: x // ERROR undeclared variable
        bar {}
    }
    "
    .compile_fail()
}

#[test]
#[ignore = "there are no functions right now, only infix operators"]
fn map_incorrect_function_arguments() {
    "
    map {
        (+ ;; ERROR function takes 2 parameters, but 1 was supplied
            x
        )
        42
    }
    "
    .compile_fail()
}

#[test]
fn map_obj_non_domain_type_and_unit_type() {
    "
    type foo
    map {
        number {} // ERROR expected domain type
        foo { // ERROR no properties expected
            'prop': x
        }
    }
    "
    .compile_fail()
}

#[test]
fn map_attribute_mismatch() {
    "
    type foo
    type bar
    rel foo 'prop0': bar
    rel foo 'prop1': bar
    rel foo 'prop2': bar
    rel bar is: int
    map {
        foo: // ERROR expected named property// ERROR missing property `prop0`// ERROR missing property `prop1`// ERROR missing property `prop2`
            x
        bar {} // ERROR expected expression
    }
    "
    .compile_fail()
}

#[test]
fn map_duplicate_unknown_property() {
    "
    type foo
    type bar
    rel foo 'a': bar
    map {
        foo {
            'a': x
            'a': x // ERROR duplicate property
            'b': x // ERROR unknown property
        }
        bar {}
    }
    "
    .compile_fail()
}

#[test]
fn map_type_mismatch_simple() {
    "
    type foo
    type bar
    rel foo 'prop': string
    rel bar 'prop': int
    map {
        foo {
            'prop': x
        }
        bar {
            'prop':
                x // ERROR type mismatch: expected `int`, found `string`
        }
    }
    "
    .compile_fail_then(|errors| {
        assert_eq!("x", errors[0].span_text);
    })
}

#[test]
fn map_type_mismatch_in_func() {
    "
    type foo
    type bar
    rel foo 'prop': string
    rel bar 'prop': int
    map {
        foo {
            'prop': x
        }
        bar {
            'prop':
                x // ERROR type mismatch: expected `int`, found `string`
                * 2
        }
    }
    "
    .compile_fail()
}

#[test]
fn map_array_mismatch() {
    "
    type foo
    rel foo 'a': [string]
    rel foo 'b': [string]

    type bar
    rel bar 'a': string
    rel bar 'b': [int]

    map {
        foo {
            'a': x // ERROR [string] variable must be enclosed in []
            'b': y // ERROR [string] variable must be enclosed in []
        }
        bar {
            'a': x
            'b': y // ERROR [int] variable must be enclosed in []
        }
    }
    "
    .compile_fail();
}

#[test]
fn array_map_without_brackets() {
    "
    type foo { rel _ 'a': [string] }
    type bar { rel _ 'b': [string] }

    map {
        foo {
            'a': x // ERROR [string] variable must be enclosed in []
        }
        bar {
            'b': x // ERROR [string] variable must be enclosed in []
        }
    }
    "
    .compile_fail();
}

#[test]
fn union_in_named_relationship() {
    "
    type foo
    rel foo 'a': string
    rel foo 'a': int // ERROR union in named relationship is not supported yet. Make a union type instead.
    "
    .compile_fail();
}

#[test]
fn only_entities_may_have_reverse_relationship() {
    "
    type foo
    type bar
    rel [foo] 'a'::'aa' bar {} // ERROR only entities may have named reverse relationship
    rel [foo] 'b'::'bb' string // ERROR only entities may have named reverse relationship
    "
    .compile_fail()
}

#[test]
fn unresolved_transitive_map() {
    "
    type a { rel _ is?: int }
    type b { rel _ is?: int }

    type c { rel _ 'p0': a }
    type d { rel _ 'p1': b }

    map {
        c {
            'p0':
                x // ERROR cannot convert this `a` from `b`: These types are not equated.
        }
        d {
            'p1':
                x // ERROR cannot convert this `b` from `a`: These types are not equated.
        }
    }
    "
    .compile_fail();
}

#[test]
fn map_union() {
    "
    type foo {
        rel _ 'type': 'foo'
    }
    type bar {
        rel _ 'type': 'bar'
    }
    type foobar {
        rel _ is?: foo
        rel _ is?: bar
    }

    map {
        foobar {} // ERROR cannot map a union, map each variant instead
        foo {} // ERROR missing property `type`
    }
    "
    .compile_fail();
}
