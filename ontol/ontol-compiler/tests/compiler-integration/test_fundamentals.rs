use ontol_macros::test;
use ontol_runtime::{interface::serde::operator::SerdeOperator, DomainIndex, PropId};
use ontol_test_utils::{
    assert_json_io_matches, def_binding::DefBinding, expect_eq, file_url,
    serde_helper::serde_create, OntolTest, TestCompile, TestPackages,
};

#[test]
fn test_relations_are_distinct_for_different_domains() {
    TestPackages::with_static_sources([
        (
            file_url("entry"),
            "
            domain 01JCRBWXR3QZHH9ESGN6VGBCZZ (rel. name: 'entry')

            use 'other' as other

            def foo (
                rel* 'prop': text
            )
            ",
        ),
        (
            file_url("other"),
            "
            domain 01JCRBXMW8XHGJFAQRWCFSJT1J (rel. name: 'other')

            def foo (
                rel* 'prop': text
            )
            ",
        ),
    ])
    .compile_then(|test| {
        let [foo, other_foo] = test.bind(["foo", "other.foo"]);
        let ontology = test.ontology();

        let root_domain = ontology
            .domain_by_index(foo.def_id().domain_index())
            .unwrap();
        expect_eq!(
            actual = &ontology[root_domain.unique_name()],
            expected = "entry"
        );

        let other_domain = ontology
            .domain_by_index(other_foo.def_id().domain_index())
            .unwrap();
        expect_eq!(
            actual = &ontology[other_domain.unique_name()],
            expected = "other"
        );

        fn extract_prop_rel_id(binding: &DefBinding, test: &OntolTest) -> PropId {
            let operator = &test.ontology()[binding.serde_operator_addr()];

            match operator {
                SerdeOperator::Struct(struct_op) => {
                    struct_op
                        .properties
                        .iter()
                        .map(|(_key, property)| property)
                        .find(|property| !property.is_rel_params())
                        .unwrap()
                        .id
                }
                _ => panic!(),
            }
        }

        let prop = extract_prop_rel_id(&foo, &test);
        let other_prop = extract_prop_rel_id(&other_foo, &test);

        assert_eq!(prop.0.domain_index(), foo.def_id().domain_index());
        assert_eq!(
            other_prop.0.domain_index(),
            other_foo.def_id().domain_index()
        );
    });
}

#[test]
fn ontol_domain_is_defined_in_the_namespace() {
    "
    def i64(
        rel* is: boolean
    )
    def text (
        rel* is: ontol.i64
    )
    def integer (
        rel* is: text
    )
    "
    .compile_then(|test| {
        let [integer] = test.bind(["integer"]);
        assert_json_io_matches!(serde_create(&integer), 42);
    });
}

#[test]
fn ontol_domain_is_documented() {
    let test = "".compile();
    let ontol_domain = test
        .ontology()
        .domain_by_index(DomainIndex::ontol())
        .unwrap();

    let text = ontol_domain
        .find_def_by_name(test.ontology().find_text_constant("text").unwrap())
        .unwrap();
    assert!(test.ontology().get_def_docs(text.id).is_some());

    let domain_doc = test
        .ontology()
        .get_def_docs(test.ontology().domains().next().unwrap().1.def_id());
    assert!(domain_doc.is_some());
}

#[test]
fn test_domain_topology_generation() {
    let test = TestPackages::with_static_sources([
        (
            file_url("entry"),
            "
            domain 01JCRBWXR3QZHH9ESGN6VGBCZZ (rel. name: 'entry')
            use 'other' as other
            ",
        ),
        (
            file_url("other"),
            "
            domain 01JCRBXMW8XHGJFAQRWCFSJT1J (rel. name: 'other')
            ",
        ),
    ])
    .compile();

    let mut domains = test.ontology().domains();
    let ontol = domains.next().unwrap().1;
    assert_eq!("ontol", &test.ontology()[ontol.unique_name()]);
    assert_eq!(1, ontol.topology_generation().0, "not entrypoint");

    let entry = domains.next().unwrap().1;
    assert_eq!("entry", &test.ontology()[entry.unique_name()]);
    assert_eq!(0, entry.topology_generation().0, "entrypoint");

    let other = domains.next().unwrap().1;
    assert_eq!("other", &test.ontology()[other.unique_name()]);
    assert_eq!(1, other.topology_generation().0, "not entrypoint");
}
