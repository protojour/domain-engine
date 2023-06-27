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
    /// The unit type that carries no information
    pub unit: DefId,

    /// The false type, a subtype of bool
    pub false_value: DefId,

    /// The true type, a subtype of bool
    pub true_value: DefId,

    /// The bool type, the union of true and false
    pub bool: DefId,

    /// The empty sequence type.
    pub empty_sequence: DefId,

    /// The empty string type.
    pub empty_string: DefId,

    /// The string type: All valid unicode strings.
    pub string: DefId,

    /// The integer type. TODO: Implementation details.
    pub int: DefId,

    /// The abstract type of a number. Supertype of integers and fractionals.
    pub number: DefId,

    /// Builtin relations
    pub relations: Relations,

    pub generators: Generators,

    /// Documentation relations
    pub doc: Doc,
}

#[derive(Debug)]
pub struct Relations {
    /// Relation between a type and a representation of the type.
    pub is: DefId,

    /// Relation between an ID type and the entity that it identifies.
    pub identifies: DefId,

    /// Relation between an entity and its primary id type.
    pub id: DefId,

    /// Relation between a tuple and one of its fields.
    pub indexed: DefId,

    /// TODO: Remove? "min" and "max" are type attributes, not field attributes
    pub min: DefId,
    pub max: DefId,

    /// Relation between a field and its default value.
    pub default: DefId,

    /// Relation between a field and its read-only mode generator function.
    pub gen: DefId,

    /// TODO: Remove, probably
    pub route: DefId,
}

#[derive(Debug)]
pub struct Doc {
    pub doc_relation: DefId,
    pub example_relation: DefId,
}

#[derive(Debug)]
pub struct Generators {
    pub auto: DefId,
    pub create_time: DefId,
    pub update_time: DefId,
}

impl Primitives {
    pub fn new(defs: &mut Defs) -> Self {
        let primitives = Self {
            unit: defs.add_primitive(PrimitiveKind::Unit),

            false_value: defs.add_primitive(PrimitiveKind::False),
            true_value: defs.add_primitive(PrimitiveKind::True),
            bool: defs.add_primitive(PrimitiveKind::Bool),

            empty_sequence: defs.add_def(DefKind::EmptySequence, CORE_PKG, SourceSpan::none()),
            empty_string: defs.add_def(DefKind::StringLiteral(""), CORE_PKG, SourceSpan::none()),
            int: defs.add_primitive(PrimitiveKind::Int),
            number: defs.add_primitive(PrimitiveKind::Number),
            string: defs.add_primitive(PrimitiveKind::String),

            relations: Relations {
                is: defs.add_builtin_relation(BuiltinRelationKind::Is),
                identifies: defs.add_builtin_relation(BuiltinRelationKind::Identifies),
                id: defs.add_builtin_relation(BuiltinRelationKind::Id),
                indexed: defs.add_builtin_relation(BuiltinRelationKind::Indexed),
                min: defs.add_builtin_relation(BuiltinRelationKind::Min),
                max: defs.add_builtin_relation(BuiltinRelationKind::Max),
                default: defs.add_builtin_relation(BuiltinRelationKind::Default),
                gen: defs.add_builtin_relation(BuiltinRelationKind::Gen),
                route: defs.add_builtin_relation(BuiltinRelationKind::Route),
            },

            generators: Generators {
                auto: defs.add_def(DefKind::EmptySequence, CORE_PKG, SourceSpan::none()),
                create_time: defs.add_def(DefKind::EmptySequence, CORE_PKG, SourceSpan::none()),
                update_time: defs.add_def(DefKind::EmptySequence, CORE_PKG, SourceSpan::none()),
            },

            doc: Doc {
                doc_relation: defs.add_builtin_relation(BuiltinRelationKind::Doc),
                example_relation: defs.add_builtin_relation(BuiltinRelationKind::Example),
            },
        };

        assert_eq!(DefId::unit(), primitives.unit);

        primitives
    }
}
