use ontol_macros::test;
use ontol_runtime::{
    debug::OntolDebug,
    ontology::domain::{DefRepr, DefReprUnionBound},
};
use ontol_test_utils::{src_name, TestCompile, TestPackages};

#[test]
fn test_repr_valid_mesh1() {
    TestPackages::with_static_sources([
        (
            src_name("entry"),
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
            src_name("si"),
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
