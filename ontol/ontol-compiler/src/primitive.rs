use ontol_macros::RustDoc;
use ontol_runtime::{DefId, OntolDefTag};

use crate::{
    def::{BuiltinRelationKind, DefKind, Defs, TypeDef, TypeDefFlags},
    edge::EdgeId,
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
    /// A sequence of octets.
    Octets,
    /// Reference to some vertex.
    /// The vertex can come from any domain.
    Vertex,
    /// Address of something in a data store
    DataStoreAddress,
    /// Open relationship to domainless data
    OpenDataRelationship,
    EdgeRelationship,
    FlatUnionRelationship,
    Format,
}

impl PrimitiveKind {
    pub fn is_concrete(&self) -> bool {
        !matches!(self, Self::Number | Self::Integer | Self::Float)
    }
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

pub fn init_ontol_primitives(defs: &mut Defs) {
    for tag in 0..OntolDefTag::_LastEntry as u16 {
        register_ontol_def_tag(OntolDefTag::try_from(tag).unwrap(), defs);
    }
}

fn register_ontol_def_tag(tag: OntolDefTag, defs: &mut Defs) {
    match tag {
        OntolDefTag::Ontol => {}
        OntolDefTag::Unit => {
            defs.add_primitive(tag, PrimitiveKind::Unit, &[]);
        }
        OntolDefTag::False => {
            defs.add_primitive(tag, PrimitiveKind::False, &["false"]);
        }
        OntolDefTag::True => {
            defs.add_primitive(tag, PrimitiveKind::True, &["true"]);
        }
        OntolDefTag::Boolean => {
            defs.add_primitive(tag, PrimitiveKind::Boolean, &["boolean"]);
        }
        OntolDefTag::EmptySequence => {
            defs.add_ontol(tag, DefKind::EmptySequence);
        }
        OntolDefTag::EmptyText => {
            defs.add_ontol(tag, DefKind::TextLiteral(""));
        }
        OntolDefTag::Number => {
            defs.add_primitive(tag, PrimitiveKind::Number, &["number"]);
        }
        OntolDefTag::Integer => {
            defs.add_primitive(tag, PrimitiveKind::Integer, &["integer"]);
        }
        OntolDefTag::I64 => {
            defs.add_primitive(tag, PrimitiveKind::I64, &["i64"]);
        }
        OntolDefTag::Float => {
            defs.add_primitive(tag, PrimitiveKind::Float, &["float"]);
        }
        OntolDefTag::F32 => {
            defs.add_primitive(tag, PrimitiveKind::F32, &["f32"]);
        }
        OntolDefTag::F64 => {
            defs.add_primitive(tag, PrimitiveKind::F64, &["f64"]);
        }
        OntolDefTag::Serial => {
            defs.add_primitive(tag, PrimitiveKind::Serial, &["serial"]);
        }
        OntolDefTag::Text => {
            defs.add_primitive(tag, PrimitiveKind::Text, &["text"]);
        }
        OntolDefTag::Octets => {
            defs.add_primitive(tag, PrimitiveKind::Octets, &["octets"]);
        }
        OntolDefTag::Vertex => {
            defs.add_primitive(tag, PrimitiveKind::Vertex, &["vertex"]);
        }
        OntolDefTag::Uuid => {}
        OntolDefTag::Ulid => {}
        OntolDefTag::DateTime => {}
        OntolDefTag::RelationOpenData => {
            defs.add_primitive(tag, PrimitiveKind::OpenDataRelationship, &[]);
        }
        OntolDefTag::RelationEdge => {
            defs.add_primitive(tag, PrimitiveKind::EdgeRelationship, &[]);
        }
        OntolDefTag::RelationFlatUnion => {
            defs.add_primitive(tag, PrimitiveKind::FlatUnionRelationship, &[]);
        }
        OntolDefTag::RelationIs => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Is, &["is"]);
        }
        OntolDefTag::RelationIdentifies => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Identifies, &[]);
        }
        OntolDefTag::RelationId => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Id, &[]);
        }
        OntolDefTag::RelationIndexed => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Indexed, &[]);
        }
        OntolDefTag::RelationStoreKey => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::StoreKey, &["store_key"]);
        }
        OntolDefTag::RelationMin => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Min, &["min"]);
        }
        OntolDefTag::RelationMax => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Max, &["max"]);
        }
        OntolDefTag::RelationDefault => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Default, &["default"]);
        }
        OntolDefTag::RelationGen => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Gen, &["gen"]);
        }
        OntolDefTag::RelationOrder => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Order, &["order"]);
        }
        OntolDefTag::RelationDirection => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Direction, &["direction"]);
        }
        OntolDefTag::RelationExample => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Example, &["example"]);
        }
        OntolDefTag::RelationDataStoreAddress => {
            defs.add_primitive(tag, PrimitiveKind::DataStoreAddress, &[]);
        }
        OntolDefTag::UnionDirection => {
            defs.add_ontol(
                tag,
                DefKind::Type(TypeDef {
                    ident: None,
                    rel_type_for: None,
                    flags: TypeDefFlags::CONCRETE | TypeDefFlags::PUBLIC,
                }),
            );
        }
        OntolDefTag::SymAscending => {
            defs.add_builtin_symbol(tag, "ascending");
        }
        OntolDefTag::SymDescending => {
            defs.add_builtin_symbol(tag, "descending");
        }
        OntolDefTag::Auto => {
            defs.add_ontol(OntolDefTag::Auto, DefKind::EmptySequence);
        }
        OntolDefTag::CreateTime => {
            defs.add_ontol(OntolDefTag::CreateTime, DefKind::EmptySequence);
        }
        OntolDefTag::UpdateTime => {
            defs.add_ontol(OntolDefTag::UpdateTime, DefKind::EmptySequence);
        }
        OntolDefTag::Format => {
            defs.add_ontol(tag, DefKind::BuiltinModule(&["format"]));
        }
        OntolDefTag::FormatHex => {
            defs.add_primitive(tag, PrimitiveKind::Format, &["format", "hex"]);
        }
        OntolDefTag::FormatBase64 => {
            defs.add_primitive(tag, PrimitiveKind::Format, &["format", "base64"]);
        }
        OntolDefTag::_LastEntry => {}
    }
}
