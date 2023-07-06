use ontol_runtime::{
    env::{
        DataFlow, PropertyFlow,
        PropertyFlowData::{self, *},
    },
    DefId,
};
use ontol_test_utils::expect_eq;
use test_log::test;

use crate::{
    codegen::data_flow_analyzer::DataFlowAnalyzer,
    def::{LookupRelationshipMeta, Relation, Relationship, RelationshipMeta},
    typed_hir::TypedHir,
    SpannedBorrow, NO_SPAN,
};

struct MockDefs<'c> {
    relationship: &'c Relationship<'static>,
    relation: &'c Relation<'static>,
}

impl<'c> LookupRelationshipMeta<'c> for MockDefs<'c> {
    fn lookup_relationship_meta(
        &self,
        relationship_id: ontol_runtime::RelationshipId,
    ) -> Result<RelationshipMeta<'c>, ()> {
        Ok(RelationshipMeta {
            relationship_id,
            relationship: SpannedBorrow {
                value: self.relationship,
                span: &NO_SPAN,
            },
            relation: SpannedBorrow {
                value: self.relation,
                span: &NO_SPAN,
            },
        })
    }
}

fn analyze<'a>(arg: &str, hir: &str) -> DataFlow {
    let node = ontol_hir::parse::Parser::new(TypedHir)
        .parse(hir)
        .unwrap()
        .0;
    DataFlowAnalyzer
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

    expect_eq!(
        actual = data_flow,
        expected = DataFlow {
            properties: vec![
                prop_flow("S:0:0", DependentOn("O:0:0".parse().unwrap())),
                prop_flow("S:1:1", DependentOn("O:2:2".parse().unwrap())),
                prop_flow("O:0:0", Type(DefId::unit())),
                prop_flow("O:1:1", Type(DefId::unit())),
                prop_flow("O:2:2", Type(DefId::unit())),
                prop_flow("O:2:2", ChildOf("O:1:1".parse().unwrap())),
            ]
        }
    );
}
