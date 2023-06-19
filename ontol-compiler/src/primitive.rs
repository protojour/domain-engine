use ontol_runtime::DefId;

use crate::{
    def::{BuiltinRelationKind, DefKind, Defs},
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
    pub id_relation: DefId,
    pub indexed_relation: DefId,
    pub min_relation: DefId,
    pub max_relation: DefId,
    pub default_relation: DefId,
    pub route_relation: DefId,
    pub empty_sequence: DefId,
    pub empty_string: DefId,
    pub int: DefId,
    pub number: DefId,
    pub string: DefId,
    pub doc: Doc,
}

#[derive(Debug)]
pub struct Doc {
    pub doc_relation: DefId,
    pub example_relation: DefId,
}

impl Primitives {
    pub fn new(defs: &mut Defs) -> Self {
        let primitives = Self {
            unit: defs.add_primitive(PrimitiveKind::Unit),

            false_value: defs.add_primitive(PrimitiveKind::False),
            true_value: defs.add_primitive(PrimitiveKind::True),
            bool: defs.add_primitive(PrimitiveKind::Bool),

            is_relation: defs.add_builtin_relation(BuiltinRelationKind::Is),
            identifies_relation: defs.add_builtin_relation(BuiltinRelationKind::Identifies),
            id_relation: defs.add_builtin_relation(BuiltinRelationKind::Id),
            indexed_relation: defs.add_builtin_relation(BuiltinRelationKind::Indexed),
            min_relation: defs.add_builtin_relation(BuiltinRelationKind::Min),
            max_relation: defs.add_builtin_relation(BuiltinRelationKind::Max),
            default_relation: defs.add_builtin_relation(BuiltinRelationKind::Default),
            route_relation: defs.add_builtin_relation(BuiltinRelationKind::Route),
            empty_sequence: defs.add_def(DefKind::EmptySequence, CORE_PKG, SourceSpan::none()),
            empty_string: defs.add_def(DefKind::StringLiteral(""), CORE_PKG, SourceSpan::none()),

            int: defs.add_primitive(PrimitiveKind::Int),
            number: defs.add_primitive(PrimitiveKind::Number),
            string: defs.add_primitive(PrimitiveKind::String),

            // Documentation
            doc: Doc {
                doc_relation: defs.add_builtin_relation(BuiltinRelationKind::Doc),
                example_relation: defs.add_builtin_relation(BuiltinRelationKind::Example),
            },
        };

        assert_eq!(DefId::unit(), primitives.unit);

        primitives
    }
}
