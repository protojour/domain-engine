use ontol_compiler::package::ONTOL_PKG;
use ontol_macros::test;
use ontol_runtime::{interface::serde::operator::SerdeOperator, RelationshipId};
use ontol_test_utils::{
    assert_json_io_matches, expect_eq, serde_helper::serde_create, src_name,
    type_binding::TypeBinding, OntolTest, TestCompile, TestPackages,
};

#[test]
fn test_relations_are_distinct_for_different_domains() {
    TestPackages::with_static_sources([
        (
            src_name("entry"),
            "
            use 'other' as other

            def foo (
                rel .'prop': text
            )
            ",
        ),
        (
            src_name("other"),
            "
            def foo (
                rel .'prop': text
            )
            ",
        ),
    ])
    .compile_then(|test| {
        let [foo, other_foo] = test.bind(["foo", "other.foo"]);
        let ontology = test.ontology();

        let root_domain = ontology.find_domain(foo.def_id().package_id()).unwrap();
        expect_eq!(
            actual = &ontology[root_domain.unique_name()],
            expected = "entry"
        );

        let other_domain = ontology
            .find_domain(other_foo.def_id().package_id())
            .unwrap();
        expect_eq!(
            actual = &ontology[other_domain.unique_name()],
            expected = "other"
        );

        fn extract_prop_rel_id(binding: &TypeBinding, test: &OntolTest) -> RelationshipId {
            let operator = &test.ontology()[binding.serde_operator_addr()];

            match operator {
                SerdeOperator::Struct(struct_op) => {
                    struct_op
                        .properties
                        .iter()
                        .map(|(_key, property)| property)
                        .find(|property| !property.is_rel_params())
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
fn ontol_domain_is_documented() {
    "".compile_then(|test| {
        let ontol_domain = test.ontology().find_domain(ONTOL_PKG).unwrap();
        let text = ontol_domain
            .find_type_info_by_name(test.ontology().find_text_constant("text").unwrap())
            .unwrap();
        assert!(test.ontology().get_docs(text.def_id).is_some());
    });
}
