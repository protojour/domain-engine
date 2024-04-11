use ontol_runtime::{
    ontology::map::{PropertyFlow, PropertyFlowData},
    property::{PropertyCardinality, ValueCardinality},
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
    subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
    object: (DefId::unit(), NO_SPAN),
    object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
    object_prop: None,
    rel_params: RelParams::Unit,
};
const MOCK_RELATION: DefKind = DefKind::TextLiteral("mock-relation");

#[track_caller]
fn analyze(arg: &str, hir: &str) -> Vec<PropertyFlow> {
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
    PropertyFlowData::Cardinality((PropertyCardinality::Mandatory, ValueCardinality::Unit))
}

const fn default_type() -> PropertyFlowData {
    PropertyFlowData::Type(DefId::unit())
}

#[test]
fn test_analyze_ssa1() {
    let data_flow = analyze(
        "$b",
        "
        (block
            (let-prop $_ $c ($b O:1:0))
            (let-prop $_ $d ($b O:2:1))
            (let-prop $_ $e ($d O:3:2))
            (struct ($a)
                (prop $a S:1:0 (#u (map $c)))
                (prop $a S:2:1 (#u $e))
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
fn test_analyze_ssa2() {
    let data_flow = analyze(
        "$f",
        "
        (block
            (let-prop $_ $a ($f S:2:26))
            (let-prop $_ $b ($f S:2:33))
            (let-prop $_ $g ($f S:2:55))
            (let-prop $_ $c ($g S:2:15))
            (let-prop-default $_ $e ($f O:2:57) #u (make-seq ($j)))
            (struct ($i)
                (catch (@k)
                    (try? @k $a)
                    (prop?! $i S:1:4
                        (#u (map $a))
                    )
                )
                (prop! $i S:1:6
                    (#u $b)
                )
                (prop! $i S:1:7
                    (#u $c)
                )
                (prop! $i S:1:8
                    (#u
                        (make-seq ($l)
                            (for-each $e ($_ $h)
                                (let-prop $_ $d ($h S:2:53))
                                (insert $l #u (map $d))
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
            prop_flow("S:1:4", dependent_on("S:2:26")),
            prop_flow("S:1:6", dependent_on("S:2:33")),
            prop_flow("S:1:7", dependent_on("S:2:15")),
            prop_flow("S:1:8", dependent_on("S:2:53")),
            prop_flow("S:2:15", default_type()),
            prop_flow("S:2:15", default_cardinality()),
            prop_flow("S:2:15", child_of("S:2:55")),
            prop_flow("S:2:26", default_type()),
            prop_flow("S:2:26", default_cardinality()),
            prop_flow("S:2:33", default_type()),
            prop_flow("S:2:33", default_cardinality()),
            prop_flow("S:2:53", default_type()),
            prop_flow("S:2:53", default_cardinality()),
            prop_flow("S:2:53", child_of("O:2:57")),
            prop_flow("S:2:55", default_type()),
            prop_flow("S:2:55", default_cardinality()),
            prop_flow("O:2:57", default_type()),
            prop_flow("O:2:57", default_cardinality()),
        ]
    );
}

#[test]
fn test_analyze_seq1() {
    let data_flow = analyze(
        "$b",
        "
        (block
            (let-prop-default $_ $c ($b O:1:0) #u (make-seq ($s)))
            (struct ($a)
                (prop $a S:1:0
                    (#u
                        (make-seq ($d)
                            (for-each $c ($_ $e)
                                (let-prop $_ $f ($e O:2:1))
                                (insert $d #u $f)
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
        (block
            (let-prop $_ $c ($b O:1:0))
            (let-regex ((1 $d)) ((2 $e)) def@0:0 $c)
            (struct ($a)
                (catch (@f)
                    (try? @f $d)
                    (prop $a S:1:0
                        (#u $d)
                    )
                )
                (catch (@g)
                    (try? @g $e)
                    (prop $a S:1:1
                        (#u $e)
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
