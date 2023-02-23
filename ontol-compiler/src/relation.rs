use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use ontol_runtime::{discriminator::UnionDiscriminator, value::PropertyId, DefId, RelationId};

use crate::{def::Cardinality, patterns::StringPatternSegment, sequence::Sequence, SourceSpan};

#[derive(Clone, Copy, Debug)]
pub struct RelationshipId(pub DefId);

#[derive(Default, Debug)]
pub struct Relations {
    pub relations: HashMap<DefId, RelationId>,
    pub properties_by_type: HashMap<DefId, Properties>,
    pub relationships_by_subject: HashMap<(DefId, RelationId), RelationshipId>,
    pub relationships_by_object: HashMap<(DefId, RelationId), RelationshipId>,

    pub value_unions: HashSet<DefId>,
    pub string_pattern_constructors: HashSet<DefId>,
    pub union_discriminators: HashMap<DefId, UnionDiscriminator>,
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
    pub map: Option<IndexMap<PropertyId, Cardinality>>,
    pub id: Option<RelationId>,
}

impl Properties {
    pub fn constructor(&self) -> &Constructor {
        &self.constructor
    }

    pub fn insert_map_property(&mut self, property_id: PropertyId, cardinality: Cardinality) {
        let map = self.map.get_or_insert_with(|| Default::default());
        map.insert(property_id, cardinality);
    }
}

#[derive(Default, Debug)]
pub enum Constructor {
    #[default]
    Identity,
    Value(RelationshipId, SourceSpan, Cardinality),
    /// ValueUnion uses a Vec even if we have to prove that properties have disjoint types.
    /// serializers etc should try things in sequence anyway.
    ValueUnion(Vec<(RelationshipId, SourceSpan)>),
    Sequence(Sequence),
    StringPattern(StringPatternSegment),
}
