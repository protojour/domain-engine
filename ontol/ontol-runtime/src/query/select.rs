use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::filter::Filter;
use crate::{DefId, PropId};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Select {
    /// Some kind of unit/scalar attribute.
    /// For a struct value that is a vertex, this dynamically resolves to domain entity id.
    Unit,
    /// Explicitly selects the domain entity id of the vertex
    EntityId,
    /// Selects the vertex address of the vertex
    VertexAddress,
    Struct(StructSelect),
    StructUnion(DefId, Vec<StructSelect>),
    Entity(EntitySelect),
}

/// FIXME: This doesn't necessarily always represent an entity select.
/// In the exposed domain it can represent any kind of struct.
/// The point is that it must be _translated_ to an entity select in a data store domain.
/// TODO: Rename?
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntitySelect {
    pub source: StructOrUnionSelect,
    pub filter: Filter,
    pub limit: Option<usize>,
    pub after_cursor: Option<Box<[u8]>>,
    pub include_total_len: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StructOrUnionSelect {
    Struct(StructSelect),
    Union(DefId, Vec<StructSelect>),
}

impl StructOrUnionSelect {
    pub fn def_id(&self) -> DefId {
        match self {
            Self::Struct(struct_) => struct_.def_id,
            Self::Union(def_id, _) => *def_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructSelect {
    pub def_id: DefId,
    /// TODO: It should be possible to select ONTOL attributes,
    /// and attributes may be multivalued.
    pub properties: BTreeMap<PropId, Select>,
}

impl StructSelect {
    /// Method should only be used in test code
    pub fn into_default_domain_entity_select(self) -> EntitySelect {
        EntitySelect {
            source: StructOrUnionSelect::Struct(self),
            filter: Filter::default_for_domain(),
            limit: Some(20),
            after_cursor: None,
            include_total_len: false,
        }
    }
}

impl From<StructSelect> for Select {
    fn from(value: StructSelect) -> Self {
        Select::Struct(value)
    }
}
