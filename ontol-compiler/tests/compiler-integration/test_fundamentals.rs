use ontol_runtime::{interface::serde::operator::SerdeOperator, RelationshipId};
use ontol_test_utils::{
    assert_json_io_matches, expect_eq, serde_helper::serde_create, type_binding::TypeBinding,
    OntolTest, SourceName, TestCompile, TestPackages,
};
use test_log::test;

#[test]
fn test_relations_are_distinct_for_different_domains() {
    TestPackages::with_sources([
        (
            SourceName("other"),
            "
            def foo (
                rel .'prop': text
            )
            ",
        ),
        (
            SourceName::root(),
            "
            use 'other' as other

            def foo (
                rel .'prop': text
            )
            ",
        ),
    ])
    .compile_then(|test| {
        let [foo, other_foo] = test.bind(["foo", "other.foo"]);
        let ontology = &test.ontology;

        let root_domain = ontology.find_domain(foo.def_id().package_id()).unwrap();
        expect_eq!(actual = root_domain.unique_name, expected = "test_root.on");

        let other_domain = ontology
            .find_domain(other_foo.def_id().package_id())
            .unwrap();
        expect_eq!(actual = other_domain.unique_name, expected = "other");

        fn extract_prop_rel_id<'o>(binding: &TypeBinding, test: &'o OntolTest) -> RelationshipId {
            let operator = test
                .ontology
                .get_serde_operator(binding.serde_operator_addr());

            match operator {
                SerdeOperator::Struct(struct_op) => {
                    struct_op
                        .properties
                        .values()
                        .next()
                        .unwrap()
                        .property_id
                        .relationship_id
                }
                _ => panic!(),
            }
        }

        let prop = extract_prop_rel_id(&foo, &test);
        let other_prop = extract_prop_rel_id(&other_foo, &test);

        assert_eq!(prop.0.package_id(), foo.def_id().package_id());
        assert_eq!(other_prop.0.package_id(), other_foo.def_id().package_id());
    });
}

#[test]
fn ontol_domain_is_defined_in_the_namespace() {
    "
    def i64(
        rel .is: boolean
    )
    def text (
        rel .is: ontol.i64
    )
    def integer (
        rel .is: text
    )
    "
    .compile_then(|test| {
        let [integer] = test.bind(["integer"]);
        assert_json_io_matches!(serde_create(&integer), 42);
    });
}

#[test]
fn cannot_redefine_ontol() {
    "def ontol () // ERROR TODO: definition of external identifier".compile_fail();
}

#[test]
fn can_refer_to_future_definitions() {
    "
    def a (rel .'b': b)
    def b (rel .'a': a)
    "
    .compile();
}

#[test]
// BUG: .default expected to work for booleans
#[should_panic = "variable not found"]
fn default_for_boolean() {
    "
    def foo (
        rel .'active'(rel .default := true): boolean
    )
    "
    .compile_fail();
}
