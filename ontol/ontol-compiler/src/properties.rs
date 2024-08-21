use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{property::Cardinality, DefId, DefPropTag, PropId};
use tracing::warn;

use crate::{
    relation::RelId,
    repr::{repr_ctx::ReprCtx, repr_model::ReprKind},
    sequence::Sequence,
    text_patterns::TextPatternSegment,
};

/// Context that tracks relation and relationship information
#[derive(Default)]
pub struct PropCtx {
    pub properties_by_def_id: FnvHashMap<DefId, Properties>,
}

impl PropCtx {
    pub fn property_by_id(&self, prop_id: PropId) -> Option<&Property> {
        self.properties_table_by_def_id(prop_id.0)?.get(&prop_id)
    }

    pub fn properties_by_def_id(&self, domain_type_id: DefId) -> Option<&Properties> {
        self.properties_by_def_id.get(&domain_type_id)
    }

    pub fn alloc_prop_id(&mut self, def_id: DefId) -> PropId {
        let properties = self.properties_by_def_id_mut(def_id);
        let tag = properties.next_prop_tag;
        properties.next_prop_tag += 1;
        PropId(def_id, DefPropTag(tag))
    }

    pub fn properties_table_by_def_id(
        &self,
        domain_type_id: DefId,
    ) -> Option<&IndexMap<PropId, Property>> {
        self.properties_by_def_id
            .get(&domain_type_id)
            .and_then(|properties| properties.table.as_ref())
    }

    pub fn properties_by_def_id_mut(&mut self, domain_type_id: DefId) -> &mut Properties {
        self.properties_by_def_id.entry(domain_type_id).or_default()
    }

    pub fn append_prop(&mut self, def_id: DefId, property: Property) -> PropId {
        let prop_id = self.alloc_prop_id(def_id);
        let properties = self.properties_by_def_id_mut(def_id);
        let table = properties.table.get_or_insert_with(Default::default);
        table.insert(prop_id, property);
        prop_id
    }

    pub fn identified_by(&self, domain_type_id: DefId) -> Option<RelId> {
        let properties = self.properties_by_def_id(domain_type_id)?;
        properties.identified_by
    }

    /// Stable-sort property tables such that all Subject property roles appear before Object roles.
    pub fn sort_property_tables(&mut self) {
        for properties in &mut self.properties_by_def_id.values_mut() {
            if let Some(_table) = &mut properties.table {
                warn!("TODO: sort by cardinal idx");

                // let mut table_vec: Vec<_> = std::mem::take(table).into_iter().collect();
                // table_vec.sort_by(|(id_a, _), (id_b, _)| id_a.role.cmp(&id_b.role));
                // *table = table_vec.into_iter().collect();
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct Properties {
    next_prop_tag: u16,
    pub constructor: Constructor,
    pub table: Option<IndexMap<PropId, Property>>,
    pub identifies: Option<RelId>,
    pub identified_by: Option<RelId>,
}

impl Properties {
    pub fn constructor(&self) -> &Constructor {
        &self.constructor
    }

    /// Get the property table, create it if None
    pub fn table_mut(&mut self) -> &mut IndexMap<PropId, Property> {
        self.table.get_or_insert_with(Default::default)
    }
}

#[derive(Debug)]
pub struct Property {
    pub rel_id: RelId,
    pub cardinality: Cardinality,
    pub is_entity_id: bool,
    /// supplies only partial information to an edge,
    /// therefore cannot be an input property
    pub is_edge_partial: bool,
}

/// The "Constructor" represents different (exclusive) ways
/// a type may be represented.
/// Not sure about the naming of this type.
///
/// TODO: Replace this with ReprKind..
/// A "constructor" concept may still nice to have, e.g. in relation to deserialization.
#[derive(Default, Debug)]
pub enum Constructor {
    /// There is nothing special about this type, it is just a "struct" consisting of relations to other types.
    #[default]
    Transparent,
    /// The type is a tuple-like sequence of other types
    Sequence(Sequence),
    /// The type is a text pattern
    TextFmt(TextPatternSegment),
}

/// Check if a def identifies any entities/vertices
pub fn identifies_any(def_id: DefId, prop_ctx: &PropCtx, repr_ctx: &ReprCtx) -> bool {
    match repr_ctx.get_repr_kind(&def_id) {
        Some(ReprKind::Union(members, _bound)) => members
            .iter()
            .any(|(def_id, _span)| identifies_any(*def_id, prop_ctx, repr_ctx)),
        _ => prop_ctx
            .properties_by_def_id(def_id)
            .is_some_and(|p| p.identifies.is_some()),
    }
}
