use ontol_runtime::env::{
    DataFlow, PropertyFlow,
    PropertyFlowRelationship::{self, *},
};
use ontol_test_utils::expect_eq;
use test_log::test;

use crate::codegen::data_flow_analyzer::DataFlowAnalyzer;

#[derive(Clone, Copy)]
struct TestLang;

impl ontol_hir::Lang for TestLang {
    type Node<'a> = ontol_hir::Kind<'a, Self>;

    fn make_node<'a>(&self, kind: ontol_hir::Kind<'a, Self>) -> Self::Node<'a> {
        kind
    }
}

fn analyze<'a>(arg: &str, hir: &str) -> DataFlow {
    let node = ontol_hir::parse::Parser::new(TestLang)
        .parse(hir)
        .unwrap()
        .0;
    DataFlowAnalyzer::new()
        .analyze::<TestLang>(ontol_hir::Binder(arg.parse().unwrap()), &node)
        .unwrap()
}

fn prop_flow(prop: &str, relationship: PropertyFlowRelationship) -> PropertyFlow {
    PropertyFlow {
        property: prop.parse().unwrap(),
        relationship,
    }
}

#[test]
fn test_analyze1() {
    let data_flow = analyze(
        "b",
        "
            (struct ($a)
                (match-prop $b O:0:0
                    (($_ $c)
                        (prop $a S:0:0
                            (#u (map $c))
                        )
                    )
                )
                (match-prop $b O:1:1
                    (($_ $d)
                        (prop $a S:1:1
                            (#u
                                (match-prop $d O:2:2
                                    (($_ $e) $e)
                                )
                            )
                        )
                    )
                )
            )
            ",
    );

    expect_eq!(
        actual = data_flow,
        expected = DataFlow {
            properties: vec![
                prop_flow("S:0:0", DependentOn("O:0:0".parse().unwrap())),
                prop_flow("S:1:1", DependentOn("O:2:2".parse().unwrap())),
                prop_flow("O:0:0", None),
                prop_flow("O:1:1", None),
                prop_flow("O:2:2", ChildOf("O:1:1".parse().unwrap())),
            ]
        }
    );

    /*
    expect_eq!(
        actual = data_flow,
        expected = DataFlow {
            properties: vec![
                prop_flow("S:0:0", DependentOn(1)),
                prop_flow("O:0:0", None),
                prop_flow("S:1:1", DependentOn(4)),
                prop_flow("O:1:1", None),
                prop_flow("O:2:2", ChildOf(3)),
            ]
        }
    );
    */
}
