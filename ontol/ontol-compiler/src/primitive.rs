use ontol_macros::RustDoc;
use ontol_runtime::DefId;

use crate::{
    def::{BuiltinRelationKind, DefKind, Defs, TypeDef, TypeDefFlags},
    package::ONTOL_PKG,
    NO_SPAN,
};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug, RustDoc)]
pub enum PrimitiveKind {
    /// The unit data type which contains no information
    Unit,
    /// The set of `false` and `true`.
    /// ```ontol
    /// rel .'active': boolean
    /// ```
    Boolean,
    /// The false value.
    /// ```ontol
    /// rel .'documentation_done': false
    /// ```
    False,
    /// The true value.
    /// ```ontol
    /// rel .'truism': true
    /// ```
    True,
    /// The set of all numbers (realistically all rational numbers, as all computer numbers are rational).
    /// `number` is an abstract type.
    /// ```ontol
    /// rel .is: number
    /// ```
    Number,
    /// The set of all the integers.
    /// `integer` is an abstract type.
    /// ```ontol
    /// rel .is: integer
    /// ```
    Integer,
    /// 64-bit signed integers.
    /// ```ontol
    /// rel .is: i64
    /// ```
    I64,
    /// The set of all the floating-point numbers.
    /// `float` is an abstract type.
    /// ```ontol
    /// rel .is: float
    /// ```
    Float,
    /// 32-bit floating point.
    /// ```ontol
    /// rel .is: f32
    /// ```
    F32,
    /// 64-bit floating point.
    /// ```ontol
    /// rel .is: f64
    /// ```
    F64,
    /// Unsigned 64-bit integer that can be used as a numeric identifier in data stores.
    /// While `serial` is an integer, it cannot be used in arithmetic.
    /// ```ontol
    /// rel .'id'|id: (rel .is: serial)
    /// ```
    Serial,
    /// The set of all possible texts.
    /// ```ontol
    /// rel .is: text
    /// ```
    Text,
    /// Open relationship to domainless data
    OpenDataRelationship,
}

impl PrimitiveKind {
    pub fn is_concrete(&self) -> bool {
        !matches!(self, Self::Number | Self::Integer | Self::Float)
    }
}

/// Set of fundamental/primitive definitions.
///
/// This struct is for quick lookup by the compiler.
#[derive(Debug)]
pub struct Primitives {
    pub unit: DefId,
    pub false_value: DefId,
    pub true_value: DefId,
    pub bool: DefId,
    pub empty_sequence: DefId,
    pub empty_text: DefId,
    pub text: DefId,
    pub number: DefId,
    pub integer: DefId,
    pub i64: DefId,
    pub float: DefId,
    pub f32: DefId,
    pub f64: DefId,
    pub serial: DefId,
    pub direction_union: DefId,

    /// The definition of the ontol domain
    pub ontol_domain: DefId,

    /// An open, domainless relationship between some value and arbitrary, quasi-structured data
    pub open_data_relationship: DefId,

    /// Builtin relations
    pub relations: OntolRelations,

    pub generators: Generators,

    pub symbols: OntolSymbols,

    /// Documentation relations
    pub doc: Doc,
}

/// Built-in relation types
///
/// For documentation, see [BuiltinRelationKind].
#[derive(Debug)]
pub struct OntolRelations {
    pub is: DefId,
    pub identifies: DefId,
    pub id: DefId,
    pub indexed: DefId,
    pub store_key: DefId,
    pub min: DefId,
    pub max: DefId,
    pub default: DefId,
    pub gen: DefId,
    pub order: DefId,
    pub direction: DefId,
}

/// Built-in symbols (named subtypes of text)
#[derive(Debug)]
pub struct OntolSymbols {
    pub ascending: DefId,
    pub descending: DefId,
}

#[derive(Debug)]
pub struct Doc {
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
            serial: defs.add_primitive(PrimitiveKind::Serial, Some("serial")),
            text: defs.add_primitive(PrimitiveKind::Text, Some("text")),
            direction_union: defs.add_def(
                DefKind::Type(TypeDef {
                    ident: None,
                    rel_type_for: None,
                    flags: TypeDefFlags::CONCRETE | TypeDefFlags::PUBLIC,
                }),
                ONTOL_PKG,
                NO_SPAN,
            ),
            ontol_domain: defs.alloc_def_id(ONTOL_PKG),
            open_data_relationship: defs.add_primitive(PrimitiveKind::OpenDataRelationship, None),

            relations: OntolRelations {
                is: defs.add_builtin_relation(BuiltinRelationKind::Is, Some("is")),
                identifies: defs.add_builtin_relation(BuiltinRelationKind::Identifies, None),
                id: defs.add_builtin_relation(BuiltinRelationKind::Id, Some("id")),
                indexed: defs.add_builtin_relation(BuiltinRelationKind::Indexed, None),
                store_key: defs
                    .add_builtin_relation(BuiltinRelationKind::StoreKey, Some("store_key")),
                min: defs.add_builtin_relation(BuiltinRelationKind::Min, Some("min")),
                max: defs.add_builtin_relation(BuiltinRelationKind::Max, Some("max")),
                default: defs.add_builtin_relation(BuiltinRelationKind::Default, Some("default")),
                gen: defs.add_builtin_relation(BuiltinRelationKind::Gen, Some("gen")),
                order: defs.add_builtin_relation(BuiltinRelationKind::Order, Some("order")),
                direction: defs
                    .add_builtin_relation(BuiltinRelationKind::Direction, Some("direction")),
            },

            generators: Generators {
                auto: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
                create_time: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
                update_time: defs.add_def(DefKind::EmptySequence, ONTOL_PKG, NO_SPAN),
            },

            symbols: OntolSymbols {
                ascending: defs.add_builtin_symbol("ascending"),
                descending: defs.add_builtin_symbol("descending"),
            },

            doc: Doc {
                example_relation: defs
                    .add_builtin_relation(BuiltinRelationKind::Example, Some("example")),
            },
        };

        assert_eq!(DefId::unit(), primitives.unit);

        primitives
    }
}
