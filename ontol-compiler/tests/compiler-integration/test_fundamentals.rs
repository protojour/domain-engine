use ontol_runtime::{serde::operator::SerdeOperator, RelationshipId};
use ontol_test_utils::{
    type_binding::TypeBinding, OntolTest, SourceName, TestCompile, TestPackages,
};
use test_log::test;

#[test]
fn test_relations_are_distinct_for_different_domains() {
    TestPackages::with_sources([
        (
            SourceName("other"),
            "
            pub type foo {
                rel .'prop': string
            }
            ",
        ),
        (
            SourceName::root(),
            "
            use 'other' as other

            pub type foo {
                rel .'prop': string
            }
            ",
        ),
    ])
    .compile_ok(|test| {
        let [foo, other_foo] = test.bind(["foo", "other.foo"]);

        fn extract_prop_rel_id<'o>(binding: &TypeBinding, test: &'o OntolTest) -> RelationshipId {
            let operator = test
                .ontology
                .get_serde_operator(binding.serde_operator_id());

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
