use ontol_test_utils::TestCompile;
use test_log::test;

#[test]
fn test_unify_partial() {
    "
    def Filter (
        rel .'foo'?: text
        rel .'bar'?: text
    )

    def Foo (
        rel .'foo': text
        rel .'bar': text
    )

    map(
        Filter('foo': foo), // ERROR TODO: optional binding required, as a default value cannot be created
        Foo('foo': foo) // ERROR missing property `bar`// NOTE Consider using `match {}`
    )
    "
    .compile_fail();
}
