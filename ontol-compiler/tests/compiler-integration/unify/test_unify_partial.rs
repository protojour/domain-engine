use ontol_test_utils::TestCompile;
use test_log::test;

#[test]
fn test_unify_partial() {
    "
    type Filter {
        rel .'foo'?: string
        rel .'bar'?: string
    }

    type Foo {
        rel .'foo': string
        rel .'bar': string
    }

    unify {
        Filter { 'foo': foo } // ERROR TODO: required to be optional?// ERROR missing property `bar`
        Foo { 'foo': foo } // ERROR missing property `bar`
    }
    "
    .compile_fail()
}
