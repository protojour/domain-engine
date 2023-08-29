use ontol_test_utils::TestCompile;
use test_log::test;

#[test]
fn test_unify_partial() {
    "
    def Filter {
        rel .'foo'?: string
        rel .'bar'?: string
    }

    def Foo {
        rel .'foo': string
        rel .'bar': string
    }

    unify {
        Filter { 'foo': foo } // ERROR TODO: required to be optional?
        Foo { 'foo': foo } // ERROR missing property `bar`// NOTE Consider using `match {}`
    }
    "
    .compile_fail()
}
