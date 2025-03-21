use ontol_macros::test;
use ontol_runtime::{
    debug::OntolDebug,
    ontology::domain::{DefRepr, DefReprUnionBound},
};
use ontol_test_utils::{TestCompile, TestPackages, file_url};
use tracing::info;

#[test]
fn test_repr_valid_mesh1() {
    TestPackages::with_static_sources([
        (
            file_url("entry"),
            "
            domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
            use 'si' as si

            def length (
                rel* is: si.meters
                rel* is: i64
            )

            def bar (
                rel. 'id': (rel* is: text)
                rel* 'len': length
            )
            ",
        ),
        (
            file_url("si"),
            "
            domain 22222222222TEST22222222222 ()
            def meters (rel* is: number)
            ",
        ),
    ])
    .compile_then(|test| {
        let [meters, length] = test.bind(["si.meters", "length"]);

        assert!(
            meters.def.operator_addr.is_none(),
            "meters is an abstract type"
        );
        assert!(
            length.def.operator_addr.is_some(),
            "length is a concrete type"
        );
    });
}

#[test]
fn test_macro_repr() {
    "
    def @macro m (
        rel* 'prop': text
    )
    "
    .compile_then(|test| {
        let [m] = test.bind(["m"]);
        let Some(DefRepr::Macro) = m.def.repr() else {
            panic!("should be DefRepr::Macro");
        };
    });
}

#[test]
fn test_macro_in_macro_repr() {
    "
    def foo (rel* is: m1)
    def @macro m2 (rel* 'prop': text)
    def @macro m1 (rel* is: m2)
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        let (rel_id, _rel_info) = foo
            .def
            .data_relationship_by_name("prop", test.ontology())
            .unwrap();

        assert_eq!(rel_id.0, foo.def_id());
    });
}

#[test]
fn test_text_constant_repr() {
    "
    def a (
        rel* 'b': 'c'
    )
    "
    .compile_then(|test| {
        let [a] = test.bind(["a"]);
        let (_, rel_info) = a.def.data_relationships.iter().next().unwrap();
        let target_def_id = rel_info.target.def_id();

        let target_def = test.ontology().def(target_def_id);
        let Some(DefRepr::TextConstant(_)) = target_def.repr() else {
            panic!(
                "{target_def_id:?} not a text constant, but {:?}",
                target_def.repr().debug(test.ontology())
            );
        };
    });
}

#[test]
fn test_text_constant_union_repr() {
    "
    def u (
        rel* is?: 'A'
        rel* is?: 'B'
    )
    "
    .compile_then(|test| {
        let [u] = test.bind(["u"]);
        let Some(DefRepr::Union(_, DefReprUnionBound::Scalar(inner))) = u.def.repr() else {
            panic!()
        };
        let DefRepr::Text = inner.as_ref() else {
            panic!("not text: {:?}", inner.debug(test.ontology()));
        };
    });
}

#[test]
fn test_regex_def() {
    "
    def foo (
        rel* 'prop': /[a-z]/
    )
    "
    .compile_then(|test| {
        let [foo] = test.bind(["foo"]);
        let (_prop_id, rel_info) = foo
            .def
            .data_relationship_by_name("prop", test.ontology())
            .unwrap();

        let regex_def = test.ontology().def(rel_info.target.def_id());
        info!("regex def: {:?}", regex_def.id);
    });
}
