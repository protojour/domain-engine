use ontol_runtime::DefId;

use crate::{
    def::{BuiltinRelationKind, DefKind, Defs},
    package::ONTOL_PKG,
    NO_SPAN,
};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum PrimitiveKind {
    /// The unit data type which contains no information
    Unit,
    /// The set of false and true
    Boolean,
    /// The false value
    False,
    /// The true value
    True,
    /// All numbers (realistically all rational numbers, as all computer numbers are rational)
    Number,
    /// All the integers
    Integer,
    /// 64-bit signed integers
    I64,
    /// All the floats
    Float,
    /// 32-bit floating point
    F32,
    /// 64-bit floating point
    F64,
    /// Set of all possible texts
    Text,
}

impl PrimitiveKind {
    pub fn is_concrete(&self) -> bool {
        !matches!(self, Self::Number | Self::Integer | Self::Float)
    }
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

    /// The empty text type.
    pub empty_text: DefId,

    /// The text type: All valid unicode sequences.
    pub text: DefId,

    /// The abstract type of a number. Supertype of integers and fractionals.
    pub number: DefId,

    /// The integer type.
    pub integer: DefId,

    /// The 64 bit signed integer type
    pub i64: DefId,

    /// The float type.
    pub float: DefId,

    /// 32 bit floating point
    pub f32: DefId,

    /// 64 bit floating point
    pub f64: DefId,

    /// The definition of the ontol domain
    pub ontol_domain: DefId,

    /// Builtin relations
    pub relations: OntolRelations,

    pub generators: Generators,

    /// Documentation relations
    pub doc: Doc,
}

/// Built-in relation types
#[derive(Debug)]
pub struct OntolRelations {
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
            unit: defs.add_primitive(PrimitiveKind::Unit, None),

            false_value: defs.add_primitive(PrimitiveKind::False, Some("false")),
            true_value: defs.add_primitive(PrimitiveKind::True, Some("true")),
            bool: defs.add_primitive(PrimitiveKind::Boolean, Some("boolean")),

            empty_sequence: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
            empty_text: defs.add_def(DefKind::TextLiteral(""), ONTOL_PKG, NO_SPAN),
            number: defs.add_primitive(PrimitiveKind::Number, Some("number")),
            integer: defs.add_primitive(PrimitiveKind::Integer, Some("integer")),
            i64: defs.add_primitive(PrimitiveKind::I64, Some("i64")),
            float: defs.add_primitive(PrimitiveKind::Float, Some("float")),
            f32: defs.add_primitive(PrimitiveKind::F32, Some("f32")),
            f64: defs.add_primitive(PrimitiveKind::F64, Some("f64")),
            text: defs.add_primitive(PrimitiveKind::Text, Some("text")),
            ontol_domain: defs.alloc_def_id(ONTOL_PKG),

            relations: OntolRelations {
                is: defs.add_builtin_relation(BuiltinRelationKind::Is, Some("is")),
                identifies: defs
                    .add_builtin_relation(BuiltinRelationKind::Identifies, Some("identifies")),
                id: defs.add_builtin_relation(BuiltinRelationKind::Id, Some("id")),
                indexed: defs.add_builtin_relation(BuiltinRelationKind::Indexed, None),
                min: defs.add_builtin_relation(BuiltinRelationKind::Min, Some("min")),
                max: defs.add_builtin_relation(BuiltinRelationKind::Max, Some("max")),
                default: defs.add_builtin_relation(BuiltinRelationKind::Default, Some("default")),
                gen: defs.add_builtin_relation(BuiltinRelationKind::Gen, Some("gen")),
                route: defs.add_builtin_relation(BuiltinRelationKind::Route, Some("route")),
            },

            generators: Generators {
                auto: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
                create_time: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
                update_time: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
            },

            doc: Doc {
                doc_relation: defs.add_builtin_relation(BuiltinRelationKind::Doc, None),
                example_relation: defs
                    .add_builtin_relation(BuiltinRelationKind::Example, Some("example")),
            },
        };

        assert_eq!(DefId::unit(), primitives.unit);

        primitives
    }
}
