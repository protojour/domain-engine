use ontol_runtime::DefId;

use crate::{
    def::{DefKind, Defs, Relation, RelationKind},
    package::CORE_PKG,
    SourceSpan,
};

#[derive(Debug)]
pub enum PrimitiveKind {
    /// The unit data type which contains no information
    Unit,
    /// The false value
    False,
    /// The true value
    True,
    /// The set of false and true
    Bool,
    /// All the integers
    Int,
    /// All numbers (realistically all rational numbers, as all computer numbers are rational)
    Number,
    /// Set of all strings
    String,
}

/// Set of fundamental/primitive definitions
#[derive(Debug)]
pub struct Primitives {
    pub unit: DefId,
    pub false_value: DefId,
    pub true_value: DefId,
    pub bool: DefId,
    pub is_relation: DefId,
    pub identifies_relation: DefId,
    pub indexed_relation: DefId,
    pub route_relation: DefId,
    pub empty_sequence: DefId,
    pub empty_string: DefId,
    pub int: DefId,
    pub number: DefId,
    pub string: DefId,
}

impl Primitives {
    pub fn new(defs: &mut Defs) -> Self {
        let primitives = Self {
            unit: defs.add_primitive(PrimitiveKind::Unit),

            false_value: defs.add_primitive(PrimitiveKind::False),
            true_value: defs.add_primitive(PrimitiveKind::True),
            bool: defs.add_primitive(PrimitiveKind::Bool),

            is_relation: defs.add_def(
                DefKind::Relation(Relation {
                    kind: RelationKind::Is,
                    subject_prop: None,
                    object_prop: None,
                }),
                CORE_PKG,
                SourceSpan::none(),
            ),
            identifies_relation: defs.add_def(
                DefKind::Relation(Relation {
                    kind: RelationKind::Identifies,
                    subject_prop: None,
                    object_prop: None,
                }),
                CORE_PKG,
                SourceSpan::none(),
            ),
            indexed_relation: defs.add_def(
                DefKind::Relation(Relation {
                    kind: RelationKind::Indexed,
                    subject_prop: None,
                    object_prop: None,
                }),
                CORE_PKG,
                SourceSpan::none(),
            ),
            route_relation: defs.add_def(
                DefKind::Relation(Relation {
                    kind: RelationKind::Route,
                    subject_prop: None,
                    object_prop: None,
                }),
                CORE_PKG,
                SourceSpan::none(),
            ),
            empty_sequence: defs.add_def(DefKind::EmptySequence, CORE_PKG, SourceSpan::none()),
            empty_string: defs.add_def(DefKind::StringLiteral(""), CORE_PKG, SourceSpan::none()),

            int: defs.add_primitive(PrimitiveKind::Int),
            number: defs.add_primitive(PrimitiveKind::Number),
            string: defs.add_primitive(PrimitiveKind::String),
        };

        assert_eq!(DefId::unit(), primitives.unit);

        primitives
    }
}
