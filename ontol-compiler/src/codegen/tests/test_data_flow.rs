use ontol_runtime::{
    ontology::{PropertyCardinality, PropertyFlow, PropertyFlowData, ValueCardinality},
    DefId, RelationshipId,
};
use ontol_test_utils::expect_eq;
use test_log::test;
use tracing::debug;
use unimock::{matching, MockFn, Unimock};

use crate::{
    codegen::data_flow_analyzer::DataFlowAnalyzer,
    def::{DefKind, LookupRelationshipMetaMock, RelParams, Relationship, RelationshipMeta},
    typed_hir::TypedHir,
    SpannedBorrow, NO_SPAN,
};

const MOCK_RELATIONSHIP: Relationship = Relationship {
    relation_def_id: DefId::unit(),
    subject: (DefId::unit(), NO_SPAN),
    subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::One),
    object: (DefId::unit(), NO_SPAN),
    object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::One),
    object_prop: None,
    rel_params: RelParams::Unit,
};
const MOCK_RELATION: DefKind = DefKind::StringLiteral("mock-relation");

fn analyze<'a>(arg: &str, hir: &str) -> Vec<PropertyFlow> {
    let node = ontol_hir::parse::Parser::new(TypedHir)
        .parse(hir)
        .unwrap()
        .0;
    let deps = Unimock::new(
        LookupRelationshipMetaMock::relationship_meta
            .each_call(matching!(_))
            .returns({
                RelationshipMeta {
                    relationship_id: RelationshipId(DefId::unit()),
                    relationship: SpannedBorrow {
                        value: &MOCK_RELATIONSHIP,
                        span: &NO_SPAN,
                    },
                    relation_def_kind: SpannedBorrow {
                        value: &MOCK_RELATION,
                        span: &NO_SPAN,
                    },
                }
            }),
    );
    let mut analyzer = DataFlowAnalyzer::new(&deps);
    let flow = analyzer.analyze(arg.parse().unwrap(), &node).unwrap();

    debug!("post analysis: {analyzer:#?}");

    flow
}

fn prop_flow(prop: &str, relationship: PropertyFlowData) -> PropertyFlow {
    PropertyFlow {
        id: prop.parse().unwrap(),
        data: relationship,
    }
}

fn dependent_on(prop: &str) -> PropertyFlowData {
    PropertyFlowData::DependentOn(prop.parse().unwrap())
}

fn child_of(prop: &str) -> PropertyFlowData {
    PropertyFlowData::ChildOf(prop.parse().unwrap())
}

const fn default_cardinality() -> PropertyFlowData {
    PropertyFlowData::Cardinality((PropertyCardinality::Mandatory, ValueCardinality::One))
}

const fn default_type() -> PropertyFlowData {
    PropertyFlowData::Type(DefId::unit())
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
        expected = vec![
            prop_flow("S:0:0", dependent_on("O:0:0")),
            prop_flow("S:1:1", dependent_on("O:2:2")),
            prop_flow("O:0:0", default_type()),
            prop_flow("O:0:0", default_cardinality()),
            prop_flow("O:1:1", default_type()),
            prop_flow("O:1:1", default_cardinality()),
            prop_flow("O:2:2", default_type()),
            prop_flow("O:2:2", default_cardinality()),
            prop_flow("O:2:2", child_of("O:1:1")),
        ]
    );
}

#[test]
fn test_analyze_seq1() {
    let data_flow = analyze(
        "b",
        "
        (struct ($a)
            (match-prop $b O:0:0
                ((seq-default $c)
                    (prop $a S:0:0
                        (#u
                            (gen $c ($d $_ $e)
                                (push $d #u
                                    (match-prop $e O:1:1
                                        (($_ $f) $f)
                                    )
                                )
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
        expected = vec![
            prop_flow("S:0:0", dependent_on("O:1:1")),
            prop_flow("O:0:0", default_type()),
            prop_flow("O:0:0", default_cardinality()),
            prop_flow("O:1:1", default_type()),
            prop_flow("O:1:1", default_cardinality()),
            prop_flow("O:1:1", child_of("O:0:0")),
        ]
    );
}
