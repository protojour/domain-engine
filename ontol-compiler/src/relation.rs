use fnv::{FnvHashMap, FnvHashSet};
use indexmap::IndexMap;
use ontol_runtime::{discriminator::UnionDiscriminator, value::PropertyId, DefId, RelationshipId};

use crate::{
    def::{Cardinality, RelationId},
    patterns::StringPatternSegment,
    sequence::Sequence,
    SourceSpan,
};

#[derive(Default, Debug)]
pub struct Relations {
    pub relations: FnvHashMap<DefId, RelationId>,
    pub properties_by_type: FnvHashMap<DefId, Properties>,
    /// A map from "idenfities" relationship to named relationship:
    pub inherent_id_map: FnvHashMap<RelationshipId, RelationshipId>,

    pub default_const_objects: FnvHashMap<RelationshipId, DefId>,

    pub value_unions: FnvHashSet<DefId>,
    pub string_pattern_constructors: FnvHashSet<DefId>,
    pub union_discriminators: FnvHashMap<DefId, UnionDiscriminator>,
}

impl Relations {
    pub fn properties_by_type(&self, domain_type_id: DefId) -> Option<&Properties> {
        self.properties_by_type.get(&domain_type_id)
    }

    pub fn properties_by_type_mut(&mut self, domain_type_id: DefId) -> &mut Properties {
        self.properties_by_type.entry(domain_type_id).or_default()
    }
}

#[derive(Default, Debug)]
pub struct Properties {
    pub constructor: Constructor,
    pub map: Option<IndexMap<PropertyId, Property>>,
    pub identifies: Option<RelationshipId>,
    pub identified_by: Option<RelationshipId>,
}

impl Properties {
    pub fn constructor(&self) -> &Constructor {
        &self.constructor
    }

    pub fn insert_map_property(&mut self, property_id: PropertyId, property: Property) {
        let map = self.map.get_or_insert_with(Default::default);
        map.insert(property_id, property);
    }
}

#[derive(Debug)]
pub struct Property {
    pub cardinality: Cardinality,
    pub is_entity_id: bool,
}

/// The "Constructor" represents different (exclusive) ways
/// a type may be represented.
/// Not sure about the naming of this type.
#[derive(Default, Debug)]
pub enum Constructor {
    /// There is nothing special about this type, it is just a "struct" consisting of relations to other types.
    #[default]
    Struct,
    /// The type represents an abstraction of another type, using one [is] relation.
    Value(RelationshipId, SourceSpan, Cardinality),
    /// The type represents an intersection any number of other types.
    /// It has several [is] relations.
    Intersection(Vec<(RelationshipId, SourceSpan, Cardinality)>),
    /// The type represents a union (either-or) of other types.
    /// Union uses a Vec even if we have to prove that properties have disjoint types.
    /// serializers etc should try things in sequence anyway.
    Union(Vec<(RelationshipId, SourceSpan)>),
    /// The type is a tuple-like sequence of other types
    Sequence(Sequence),
    /// The type is a string pattern
    StringFmt(StringPatternSegment),
}
