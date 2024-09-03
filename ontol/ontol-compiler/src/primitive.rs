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
    OctetStream,
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
            defs.add_primitive(tag, PrimitiveKind::Unit, None);
        }
        OntolDefTag::False => {
            defs.add_primitive(tag, PrimitiveKind::False, Some("false"));
        }
        OntolDefTag::True => {
            defs.add_primitive(tag, PrimitiveKind::True, Some("true"));
        }
        OntolDefTag::Boolean => {
            defs.add_primitive(tag, PrimitiveKind::Boolean, Some("boolean"));
        }
        OntolDefTag::EmptySequence => {
            defs.add_ontol(tag, DefKind::EmptySequence);
        }
        OntolDefTag::EmptyText => {
            defs.add_ontol(tag, DefKind::TextLiteral(""));
        }
        OntolDefTag::Number => {
            defs.add_primitive(tag, PrimitiveKind::Number, Some("number"));
        }
        OntolDefTag::Integer => {
            defs.add_primitive(tag, PrimitiveKind::Integer, Some("integer"));
        }
        OntolDefTag::I64 => {
            defs.add_primitive(tag, PrimitiveKind::I64, Some("i64"));
        }
        OntolDefTag::Float => {
            defs.add_primitive(tag, PrimitiveKind::Float, Some("float"));
        }
        OntolDefTag::F32 => {
            defs.add_primitive(tag, PrimitiveKind::F32, Some("f32"));
        }
        OntolDefTag::F64 => {
            defs.add_primitive(tag, PrimitiveKind::F64, Some("f64"));
        }
        OntolDefTag::Serial => {
            defs.add_primitive(tag, PrimitiveKind::Serial, Some("serial"));
        }
        OntolDefTag::Text => {
            defs.add_primitive(tag, PrimitiveKind::Text, Some("text"));
        }
        OntolDefTag::OctetStream => {
            defs.add_primitive(tag, PrimitiveKind::OctetStream, None);
        }
        OntolDefTag::Uuid => {}
        OntolDefTag::Ulid => {}
        OntolDefTag::DateTime => {}
        OntolDefTag::RelationOpenData => {
            defs.add_primitive(tag, PrimitiveKind::OpenDataRelationship, None);
        }
        OntolDefTag::RelationEdge => {
            defs.add_primitive(tag, PrimitiveKind::EdgeRelationship, None);
        }
        OntolDefTag::RelationFlatUnion => {
            defs.add_primitive(tag, PrimitiveKind::FlatUnionRelationship, None);
        }
        OntolDefTag::RelationIs => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Is, Some("is"));
        }
        OntolDefTag::RelationIdentifies => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Identifies, None);
        }
        OntolDefTag::RelationId => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Id, None);
        }
        OntolDefTag::RelationIndexed => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Indexed, None);
        }
        OntolDefTag::RelationStoreKey => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::StoreKey, Some("store_key"));
        }
        OntolDefTag::RelationMin => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Min, Some("min"));
        }
        OntolDefTag::RelationMax => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Max, Some("max"));
        }
        OntolDefTag::RelationDefault => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Default, Some("default"));
        }
        OntolDefTag::RelationGen => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Gen, Some("gen"));
        }
        OntolDefTag::RelationOrder => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Order, Some("order"));
        }
        OntolDefTag::RelationDirection => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Direction, Some("direction"));
        }
        OntolDefTag::RelationExample => {
            defs.add_builtin_relation(tag, BuiltinRelationKind::Example, Some("example"));
        }
        OntolDefTag::RelationDataStoreAddress => {
            defs.add_primitive(tag, PrimitiveKind::DataStoreAddress, None);
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
        OntolDefTag::GeneratorAuto => {
            defs.add_ontol(OntolDefTag::GeneratorAuto, DefKind::EmptySequence);
        }
        OntolDefTag::GeneratorCreateTime => {
            defs.add_ontol(OntolDefTag::GeneratorCreateTime, DefKind::EmptySequence);
        }
        OntolDefTag::GeneratorUpdateTime => {
            defs.add_ontol(OntolDefTag::GeneratorUpdateTime, DefKind::EmptySequence);
        }
        OntolDefTag::_LastEntry => {}
    }
}
