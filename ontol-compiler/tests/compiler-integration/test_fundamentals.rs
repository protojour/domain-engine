use ontol_runtime::{interface::serde::operator::SerdeOperator, RelationshipId};
use ontol_test_utils::{
    assert_json_io_matches, type_binding::TypeBinding, OntolTest, SourceName, TestCompile,
    TestPackages,
};
use test_log::test;

#[test]
fn test_relations_are_distinct_for_different_domains() {
    TestPackages::with_sources([
        (
            SourceName("other"),
            "
            pub def foo {
                rel .'prop': text
            }
            ",
        ),
        (
            SourceName::root(),
            "
            use 'other' as other

            pub def foo {
                rel .'prop': text
            }
            ",
        ),
    ])
    .compile_then(|test| {
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

#[test]
fn ontol_domain_is_defined_in_the_namespace() {
    "
    pub def i64 {
        rel .is: boolean
    }
    pub def text {
        rel .is: ontol.i64
    }
    pub def integer {
        rel .is: text
    }
    "
    .compile_then(|test| {
        let [integer] = test.bind(["integer"]);
        assert_json_io_matches!(integer, Create, 42);
    });
}

#[test]
fn cannot_redefine_ontol() {
    "pub def ontol {} // ERROR TODO: definition of external identifier".compile_fail();
}
