use ontol_macros::test;
use ontol_runtime::{
    ontology::{
        domain::EdgeCardinalProjection,
        map::{PropertyFlow, PropertyFlowData},
    },
    property::{PropertyCardinality, ValueCardinality},
    tuple::CardinalIdx,
    DefId, DefRelTag, EdgeId, RelId,
};
use ontol_test_utils::expect_eq;
use tracing::debug;

use crate::{
    codegen::data_flow_analyzer::DataFlowAnalyzer,
    def::{DefKind, Defs},
    package::ONTOL_PKG,
    relation::{RelCtx, RelDefMeta, RelParams, Relationship},
    typed_hir::TypedHir,
    SpannedBorrow, NO_SPAN,
};

const MOCK_RELATIONSHIP: Relationship = Relationship {
    relation_def_id: DefId::unit(),
    projection: EdgeCardinalProjection {
        id: EdgeId(ONTOL_PKG, 0),
        object: CardinalIdx(1),
        subject: CardinalIdx(0),
        one_to_one: false,
    },
    relation_span: NO_SPAN,
    subject: (DefId::unit(), NO_SPAN),
    subject_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
    object: (DefId::unit(), NO_SPAN),
    object_cardinality: (PropertyCardinality::Mandatory, ValueCardinality::Unit),
    rel_params: RelParams::Unit,
};
const MOCK_RELATION: DefKind = DefKind::TextLiteral("mock-relation");

#[track_caller]
fn analyze(arg: &str, hir: &str) -> Vec<PropertyFlow> {
    let node = ontol_hir::parse::Parser::new(TypedHir)
        .parse_root(hir)
        .unwrap()
        .0;
    let defs = Defs::default();
    let rel_ctx = RelCtx::default();
    let rel_def_meta = |_, _, _| RelDefMeta {
        rel_id: RelId(DefId::unit(), DefRelTag(0)),
        relationship: SpannedBorrow {
            value: &MOCK_RELATIONSHIP,
            span: &NO_SPAN,
        },
        relation_def_kind: SpannedBorrow {
            value: &MOCK_RELATION,
            span: &NO_SPAN,
        },
    };
    let mut analyzer = DataFlowAnalyzer::new(&defs, &rel_ctx, &rel_def_meta);
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

#[test]
fn test_analyze_ssa1() {
    let data_flow = analyze(
        "$b",
        "
        (block
            (let-prop $c ($b rel@1:0:0))
            (let-prop $d ($b rel@1:0:1))
            (let-prop $e ($d rel@1:0:2))
            (struct ($a)
                (prop $a rel@2:0:0 [(map $c) #u])
                (prop $a rel@2:0:1 [$e #u])
            )
        )
        ",
    );

    expect_eq!(
        actual = data_flow,
        // note: type information is not included in ontol-hir tests:
        expected = vec![
            prop_flow("rel@1:0:0", default_cardinality()),
            prop_flow("rel@1:0:1", default_cardinality()),
            prop_flow("rel@1:0:2", default_cardinality()),
            prop_flow("rel@1:0:2", child_of("rel@1:0:1")),
            prop_flow("rel@2:0:0", dependent_on("rel@1:0:0")),
            prop_flow("rel@2:0:1", dependent_on("rel@1:0:2")),
        ]
    );
}

#[test]
fn test_analyze_ssa2() {
    let data_flow = analyze(
        "$f",
        "
        (block
            (let-prop $a ($f rel@2:0:26))
            (let-prop $b ($f rel@2:0:33))
            (let-prop $g ($f rel@2:0:55))
            (let-prop $c ($g rel@2:0:15))
            (let-prop-default $e ($f rel@2:0:57) (make-seq))
            (struct ($i)
                (catch (@k)
                    (try? @k $a)
                    (prop?! $i rel@1:0:4
                        [(map $a) #u]
                    )
                )
                (prop! $i rel@1:0:6
                    [$b #u]
                )
                (prop! $i rel@1:0:7
                    [$c #u]
                )
                (prop! $i rel@1:0:8
                    (make-seq ($l)
                        (for-each $e ($h)
                            (let-prop $d ($h rel@2:0:53))
                            (insert $l (map $d))
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
            prop_flow("rel@1:0:4", dependent_on("rel@2:0:26")),
            prop_flow("rel@1:0:6", dependent_on("rel@2:0:33")),
            prop_flow("rel@1:0:7", dependent_on("rel@2:0:15")),
            prop_flow("rel@1:0:8", dependent_on("rel@2:0:53")),
            prop_flow("rel@2:0:15", default_cardinality()),
            prop_flow("rel@2:0:15", child_of("rel@2:0:55")),
            prop_flow("rel@2:0:26", default_cardinality()),
            prop_flow("rel@2:0:33", default_cardinality()),
            prop_flow("rel@2:0:53", default_cardinality()),
            prop_flow("rel@2:0:53", child_of("rel@2:0:57")),
            prop_flow("rel@2:0:55", default_cardinality()),
            prop_flow("rel@2:0:57", default_cardinality()),
        ]
    );
}

#[test]
fn test_analyze_seq1() {
    let data_flow = analyze(
        "$b",
        "
        (block
            (let-prop-default $c ($b rel@1:0:0) (make-seq ($s)))
            (struct ($a)
                (prop $a rel@2:0:0
                    (make-seq ($d)
                        (for-each $c ($e)
                            (let-prop $f ($e rel@2:0:1))
                            (insert $d $f)
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
            prop_flow("rel@1:0:0", default_cardinality()),
            prop_flow("rel@2:0:0", dependent_on("rel@2:0:1")),
            prop_flow("rel@2:0:1", default_cardinality()),
            prop_flow("rel@2:0:1", child_of("rel@1:0:0")),
        ]
    );
}

#[test]
fn test_analyze_regex_match1() {
    let data_flow = analyze(
        "$b",
        "
        (block
            (let-prop $c ($b rel@1:0:0))
            (let-regex ((1 $d)) ((2 $e)) def@0:0 $c)
            (struct ($a)
                (catch (@f)
                    (try? @f $d)
                    (prop $a rel@2:0:0
                        $d
                    )
                )
                (catch (@g)
                    (try? @g $e)
                    (prop $a rel@2:0:1
                        $e
                    )
                )
            )
        )
        ",
    );

    expect_eq!(
        actual = data_flow,
        expected = vec![
            prop_flow("rel@1:0:0", default_cardinality()),
            prop_flow("rel@2:0:0", dependent_on("rel@1:0:0")),
            prop_flow("rel@2:0:1", dependent_on("rel@1:0:0")),
        ]
    );
}
