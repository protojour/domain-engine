use ontol_macros::RustDoc;
use ontol_runtime::{DefId, EdgeId, OntolDefTag};

use crate::{
    def::{BuiltinRelationKind, DefKind, Defs, TypeDef, TypeDefFlags},
    edge::EdgeCtx,
    package::ONTOL_PKG,
};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug, RustDoc)]
pub enum PrimitiveKind {
    /// The unit data type which contains no information
    Unit,
    /// The set of `false` and `true`.
    /// ```ontol
    /// rel* 'active': boolean
    /// ```
    Boolean,
    /// The false value.
    /// ```ontol
    /// rel* 'documentation_done': false
    /// ```
    False,
    /// The true value.
    /// ```ontol
    /// rel* 'truism': true
    /// ```
    True,
    /// The set of all numbers (realistically all rational numbers, as all computer numbers are rational).
    /// `number` is an abstract type.
    /// ```ontol
    /// rel* is: number
    /// ```
    Number,
    /// The set of all the integers.
    /// `integer` is an abstract type.
    /// ```ontol
    /// rel* is: integer
    /// ```
    Integer,
    /// 64-bit signed integers.
    /// ```ontol
    /// rel* is: i64
    /// ```
    I64,
    /// The set of all the floating-point numbers.
    /// `float` is an abstract type.
    /// ```ontol
    /// rel* is: float
    /// ```
    Float,
    /// 32-bit floating point.
    /// ```ontol
    /// rel* is: f32
    /// ```
    F32,
    /// 64-bit floating point.
    /// ```ontol
    /// rel* is: f64
    /// ```
    F64,
    /// Unsigned 64-bit integer that can be used as a numeric identifier in data stores.
    /// While `serial` is an integer, it cannot be used in arithmetic.
    /// ```ontol
    /// rel. 'id': (rel* is: serial)
    /// ```
    Serial,
    /// The set of all possible texts.
    /// ```ontol
    /// rel* is: text
    /// ```
    Text,
    /// Address of something in a data store
    DataStoreAddress,
    /// Open relationship to domainless data
    OpenDataRelationship,
    EdgeRelationship,
    FlatUnionRelationship,
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
    pub boolean: DefId,
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

    /// The address of something in a data store
    pub data_store_address: DefId,

    /// An open, domainless relationship between some value and arbitrary, quasi-structured data
    pub open_data_relationship: DefId,
    pub edge_relationship: DefId,
    pub flat_union_relationship: DefId,

    /// Builtin relations
    pub relations: OntolRelations,

    pub edges: OntolEdges,

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

#[derive(Debug)]
pub struct OntolEdges {
    pub is: EdgeId,
    pub identifies: EdgeId,
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
    pub fn new(defs: &mut Defs, edge_ctx: &mut EdgeCtx) -> Self {
        let primitives = Self {
            unit: defs.add_primitive(OntolDefTag::Unit, PrimitiveKind::Unit, None),

            false_value: defs.add_primitive(
                OntolDefTag::False,
                PrimitiveKind::False,
                Some("false"),
            ),
            true_value: defs.add_primitive(OntolDefTag::True, PrimitiveKind::True, Some("true")),
            boolean: defs.add_primitive(
                OntolDefTag::Boolean,
                PrimitiveKind::Boolean,
                Some("boolean"),
            ),

            empty_sequence: defs.add_ontol(OntolDefTag::EmptySequence, DefKind::EmptySequence),
            empty_text: defs.add_ontol(OntolDefTag::EmptyText, DefKind::TextLiteral("")),
            number: defs.add_primitive(OntolDefTag::Number, PrimitiveKind::Number, Some("number")),
            integer: defs.add_primitive(
                OntolDefTag::Integer,
                PrimitiveKind::Integer,
                Some("integer"),
            ),
            i64: defs.add_primitive(OntolDefTag::I64, PrimitiveKind::I64, Some("i64")),
            float: defs.add_primitive(OntolDefTag::Float, PrimitiveKind::Float, Some("float")),
            f32: defs.add_primitive(OntolDefTag::F32, PrimitiveKind::F32, Some("f32")),
            f64: defs.add_primitive(OntolDefTag::F64, PrimitiveKind::F64, Some("f64")),
            serial: defs.add_primitive(OntolDefTag::Serial, PrimitiveKind::Serial, Some("serial")),
            text: defs.add_primitive(OntolDefTag::Text, PrimitiveKind::Text, Some("text")),
            direction_union: defs.add_ontol(
                OntolDefTag::DirectionUnion,
                DefKind::Type(TypeDef {
                    ident: None,
                    rel_type_for: None,
                    flags: TypeDefFlags::CONCRETE | TypeDefFlags::PUBLIC,
                }),
            ),
            ontol_domain: DefId(ONTOL_PKG, OntolDefTag::OntolDomain as u16),
            data_store_address: defs.add_primitive(
                OntolDefTag::DataStoreAddress,
                PrimitiveKind::DataStoreAddress,
                None,
            ),
            open_data_relationship: defs.add_primitive(
                OntolDefTag::OpenDataRelationship,
                PrimitiveKind::OpenDataRelationship,
                None,
            ),
            edge_relationship: defs.add_primitive(
                OntolDefTag::EdgeRelationship,
                PrimitiveKind::EdgeRelationship,
                None,
            ),
            flat_union_relationship: defs.add_primitive(
                OntolDefTag::FlatUnionRelationship,
                PrimitiveKind::FlatUnionRelationship,
                None,
            ),

            relations: OntolRelations {
                is: defs.add_builtin_relation(OntolDefTag::Is, BuiltinRelationKind::Is, Some("is")),
                identifies: defs.add_builtin_relation(
                    OntolDefTag::Identifies,
                    BuiltinRelationKind::Identifies,
                    None,
                ),
                id: defs.add_builtin_relation(OntolDefTag::Id, BuiltinRelationKind::Id, None),
                indexed: defs.add_builtin_relation(
                    OntolDefTag::Indexed,
                    BuiltinRelationKind::Indexed,
                    None,
                ),
                store_key: defs.add_builtin_relation(
                    OntolDefTag::StoreKey,
                    BuiltinRelationKind::StoreKey,
                    Some("store_key"),
                ),
                min: defs.add_builtin_relation(
                    OntolDefTag::Min,
                    BuiltinRelationKind::Min,
                    Some("min"),
                ),
                max: defs.add_builtin_relation(
                    OntolDefTag::Max,
                    BuiltinRelationKind::Max,
                    Some("max"),
                ),
                default: defs.add_builtin_relation(
                    OntolDefTag::Default,
                    BuiltinRelationKind::Default,
                    Some("default"),
                ),
                gen: defs.add_builtin_relation(
                    OntolDefTag::Gen,
                    BuiltinRelationKind::Gen,
                    Some("gen"),
                ),
                order: defs.add_builtin_relation(
                    OntolDefTag::Order,
                    BuiltinRelationKind::Order,
                    Some("order"),
                ),
                direction: defs.add_builtin_relation(
                    OntolDefTag::Direction,
                    BuiltinRelationKind::Direction,
                    Some("direction"),
                ),
            },
            edges: OntolEdges {
                is: edge_ctx.alloc_edge_id(ONTOL_PKG),
                identifies: edge_ctx.alloc_edge_id(ONTOL_PKG),
            },

            generators: Generators {
                auto: defs.add_ontol(OntolDefTag::Auto, DefKind::EmptySequence),
                create_time: defs.add_ontol(OntolDefTag::CreateTime, DefKind::EmptySequence),
                update_time: defs.add_ontol(OntolDefTag::UpdateTime, DefKind::EmptySequence),
            },

            symbols: OntolSymbols {
                ascending: defs.add_builtin_symbol(OntolDefTag::Ascending, "ascending"),
                descending: defs.add_builtin_symbol(OntolDefTag::Descending, "descending"),
            },

            doc: Doc {
                example_relation: defs.add_builtin_relation(
                    OntolDefTag::Example,
                    BuiltinRelationKind::Example,
                    Some("example"),
                ),
            },
        };

        assert_eq!(DefId::unit(), primitives.unit);

        primitives
    }
}
