use ontol_runtime::{
    env::{
        DataFlow, PropertyCardinality, PropertyFlow,
        PropertyFlowData::{self, *},
        ValueCardinality,
    },
    DefId, RelationshipId,
};
use ontol_test_utils::expect_eq;
use test_log::test;
use unimock::{matching, MockFn, Unimock};

use crate::{
    codegen::data_flow_analyzer::DataFlowAnalyzer,
    def::{
        DefReference, LookupRelationshipMetaMock, RelParams, Relation, RelationId, RelationKind,
        Relationship, RelationshipMeta,
    },
    typed_hir::TypedHir,
    SpannedBorrow, NO_SPAN,
};

const MOCK_RELATIONSHIP: Relationship = Relationship {
    relation_id: RelationId(DefId::unit()),
    subject: (
        DefReference {
            def_id: DefId::unit(),
            pattern_bindings: None,
        },
        NO_SPAN,
    ),
    subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::One),
    object: (
        DefReference {
            def_id: DefId::unit(),
            pattern_bindings: None,
        },
        NO_SPAN,
    ),
    object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::One),
    object_prop: None,
    rel_params: RelParams::Unit,
};
const MOCK_RELATION: Relation = Relation {
    kind: RelationKind::Named(DefReference {
        def_id: DefId::unit(),
        pattern_bindings: None,
    }),
    subject_prop: None,
};

fn analyze<'a>(arg: &str, hir: &str) -> DataFlow {
    let node = ontol_hir::parse::Parser::new(TypedHir)
        .parse(hir)
        .unwrap()
        .0;
    DataFlowAnalyzer::new(&Unimock::new(
        LookupRelationshipMetaMock::lookup_relationship_meta
            .each_call(matching!(_))
            .answers(|_| {
                Ok(RelationshipMeta {
                    relationship_id: RelationshipId(DefId::unit()),
                    relationship: SpannedBorrow {
                        value: &MOCK_RELATIONSHIP,
                        span: &NO_SPAN,
                    },
                    relation: SpannedBorrow {
                        value: &MOCK_RELATION,
                        span: &NO_SPAN,
                    },
                })
            }),
    ))
    .analyze(arg.parse().unwrap(), &node)
    .unwrap()
}

fn prop_flow(prop: &str, relationship: PropertyFlowData) -> PropertyFlow {
    PropertyFlow {
        id: prop.parse().unwrap(),
        data: relationship,
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

    let expected_cardinality = Cardinality((PropertyCardinality::Mandatory, ValueCardinality::One));

    expect_eq!(
        actual = data_flow,
        expected = DataFlow {
            properties: vec![
                prop_flow("S:0:0", DependentOn("O:0:0".parse().unwrap())),
                prop_flow("S:1:1", DependentOn("O:2:2".parse().unwrap())),
                prop_flow("O:0:0", Type(DefId::unit())),
                prop_flow("O:0:0", expected_cardinality.clone()),
                prop_flow("O:1:1", Type(DefId::unit())),
                prop_flow("O:1:1", expected_cardinality.clone()),
                prop_flow("O:2:2", Type(DefId::unit())),
                prop_flow("O:2:2", expected_cardinality),
                prop_flow("O:2:2", ChildOf("O:1:1".parse().unwrap())),
            ]
        }
    );
}
