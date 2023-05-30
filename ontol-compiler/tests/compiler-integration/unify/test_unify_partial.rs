use ontol_test_utils::TestCompile;
use test_log::test;

#[test]
#[should_panic = "partial unification"]
fn test_unify_partial() {
    "
    type Filter {
        rel _ 'foo'?: string
        rel _ 'bar'?: string
    }

    type Foo {
        rel _ 'foo': string
        rel _ 'bar': string
    }

    unify {
        Filter { 'foo': foo } // ERROR missing property `bar`
        Foo { 'foo': foo } // ERROR type mismatch: expected `string`, found `string?`// ERROR missing property `bar`
    }
    "
    .compile_fail()
}
