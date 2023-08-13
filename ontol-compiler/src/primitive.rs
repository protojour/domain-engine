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
    Bool,
    /// The false value
    False,
    /// The true value
    True,
    /// All numbers (realistically all rational numbers, as all computer numbers are rational)
    Number,
    /// All the integers
    Int,
    /// 64-bit signed integers
    I64,
    /// All the floats
    Float,
    /// 64-bit floating point
    F64,
    /// Set of all strings
    String,
}

impl PrimitiveKind {
    pub fn is_concrete(&self) -> bool {
        matches!(
            self,
            Self::Unit | Self::Bool | Self::True | Self::False | Self::I64 | Self::String
        )
    }

    pub fn ident(&self) -> &'static str {
        match self {
            Self::Unit => "unit",
            Self::Bool => "bool",
            Self::False => "false",
            Self::True => "true",
            Self::Number => "number",
            Self::Int => "int",
            Self::I64 => "i64",
            Self::Float => "float",
            Self::F64 => "f64",
            Self::String => "string",
        }
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

    /// The empty string type.
    pub empty_string: DefId,

    /// The string type: All valid unicode strings.
    pub string: DefId,

    /// The abstract type of a number. Supertype of integers and fractionals.
    pub number: DefId,

    /// The integer type.
    pub int: DefId,

    /// The 64 bit signed integer type
    pub i64: DefId,

    /// The float type.
    pub float: DefId,

    pub f64: DefId,

    /// Builtin relations
    pub relations: Relations,

    pub generators: Generators,

    /// Documentation relations
    pub doc: Doc,
}

impl Primitives {
    pub fn list_primitives(&self) -> [(DefId, Option<&'static str>, PrimitiveKind); 10] {
        [
            (self.unit, None, PrimitiveKind::Unit),
            (self.bool, Some("bool"), PrimitiveKind::Bool),
            (self.false_value, Some("false"), PrimitiveKind::False),
            (self.true_value, Some("true"), PrimitiveKind::True),
            (self.number, Some("number"), PrimitiveKind::Number),
            (self.int, Some("int"), PrimitiveKind::Int),
            (self.i64, Some("i64"), PrimitiveKind::I64),
            (self.float, Some("float"), PrimitiveKind::Float),
            (self.f64, Some("f64"), PrimitiveKind::F64),
            (self.string, Some("string"), PrimitiveKind::String),
        ]
    }

    pub fn list_relations(&self) -> [(DefId, &'static str); 8] {
        [
            (self.relations.is, "is"),
            (self.relations.identifies, "identifies"),
            (self.relations.id, "id"),
            (self.relations.min, "min"),
            (self.relations.max, "max"),
            (self.relations.default, "default"),
            (self.relations.gen, "gen"),
            (self.relations.route, "route"),
        ]
    }
}

/// Built-in relation types
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

            empty_sequence: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
            empty_string: defs.add_def(DefKind::StringLiteral(""), ONTOL_PKG, NO_SPAN),
            number: defs.add_primitive(PrimitiveKind::Number),
            int: defs.add_primitive(PrimitiveKind::Int),
            i64: defs.add_primitive(PrimitiveKind::I64),
            float: defs.add_primitive(PrimitiveKind::Float),
            f64: defs.add_primitive(PrimitiveKind::F64),
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
                auto: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
                create_time: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
                update_time: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
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
