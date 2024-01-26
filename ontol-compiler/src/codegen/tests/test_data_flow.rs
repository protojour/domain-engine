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
const MOCK_RELATION: DefKind = DefKind::TextLiteral("mock-relation");

fn analyze<'a>(arg: &str, hir: &str) -> Vec<PropertyFlow> {
    let node = ontol_hir::parse::Parser::new(TypedHir)
        .parse_root(hir)
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
    let flow = analyzer
        .analyze(arg.parse().unwrap(), node.as_ref())
        .unwrap();

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
        "$b",
        "
        (struct ($a)
            (match-prop $b O:1:0
                (($_ $c)
                    (prop $a S:1:0
                        (#u (map $c))
                    )
                )
            )
            (match-prop $b O:2:1
                (($_ $d)
                    (prop $a S:2:1
                        (#u
                            (match-prop $d O:3:2
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
            prop_flow("S:1:0", dependent_on("O:1:0")),
            prop_flow("S:2:1", dependent_on("O:3:2")),
            prop_flow("O:1:0", default_type()),
            prop_flow("O:1:0", default_cardinality()),
            prop_flow("O:2:1", default_type()),
            prop_flow("O:2:1", default_cardinality()),
            prop_flow("O:3:2", default_type()),
            prop_flow("O:3:2", default_cardinality()),
            prop_flow("O:3:2", child_of("O:2:1")),
        ]
    );
}

#[test]
fn test_analyze_seq1() {
    let data_flow = analyze(
        "$b",
        "
        (struct ($a)
            (match-prop $b O:1:0
                ((seq-default $c)
                    (prop $a S:1:0
                        (#u
                            (make-seq ($d)
                                (for-each $c ($_ $e)
                                    (insert $d #u
                                        (match-prop $e O:2:1
                                            (($_ $f) $f)
                                        )
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
            prop_flow("S:1:0", dependent_on("O:2:1")),
            prop_flow("O:1:0", default_type()),
            prop_flow("O:1:0", default_cardinality()),
            prop_flow("O:2:1", default_type()),
            prop_flow("O:2:1", default_cardinality()),
            prop_flow("O:2:1", child_of("O:1:0")),
        ]
    );
}

#[test]
fn test_analyze_regex_match1() {
    let data_flow = analyze(
        "$b",
        "
        (struct ($a)
            (match-prop $b O:1:0
                (($_ $c)
                    (match-regex $c def@0:0
                        (((1 $d))
                            (prop $a S:1:0
                                (#u $d)
                            )
                        )
                        (((2 $e))
                            (prop $a S:1:1
                                (#u $e)
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
            prop_flow("S:1:0", dependent_on("O:1:0")),
            prop_flow("S:1:1", dependent_on("O:1:0")),
            prop_flow("O:1:0", default_type()),
            prop_flow("O:1:0", default_cardinality()),
        ]
    );
}
