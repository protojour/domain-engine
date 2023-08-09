use fnv::{FnvHashMap, FnvHashSet};
use indexmap::IndexMap;
use ontol_runtime::{
    discriminator::UnionDiscriminator,
    ontology::{Cardinality, PropertyCardinality},
    value::PropertyId,
    value_generator::ValueGenerator,
    DefId, RelationshipId,
};

use crate::{def::RelationId, patterns::StringPatternSegment, sequence::Sequence, SourceSpan};

#[derive(Default, Debug)]
pub struct Relations {
    pub relations: FnvHashMap<DefId, RelationId>,
    pub properties_by_def_id: FnvHashMap<DefId, Properties>,
    /// A map from "idenfities" relationship to named relationship:
    pub inherent_id_map: FnvHashMap<RelationshipId, RelationshipId>,

    pub string_pattern_constructors: FnvHashSet<DefId>,
    pub union_discriminators: FnvHashMap<DefId, UnionDiscriminator>,

    /// `default` relationships:
    pub default_const_objects: FnvHashMap<RelationshipId, DefId>,
    /// `gen` relations, what the user wrote directly:
    pub value_generators_unchecked: FnvHashMap<RelationshipId, (DefId, SourceSpan)>,
    /// `gen` relations after proper type check:
    pub value_generators: FnvHashMap<RelationshipId, ValueGenerator>,

    pub ontology_mesh: FnvHashMap<DefId, IndexMap<Is, SourceSpan>>,
}

impl Relations {
    pub fn properties_by_def_id(&self, domain_type_id: DefId) -> Option<&Properties> {
        self.properties_by_def_id.get(&domain_type_id)
    }

    pub fn properties_by_def_id_mut(&mut self, domain_type_id: DefId) -> &mut Properties {
        self.properties_by_def_id.entry(domain_type_id).or_default()
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Is {
    pub def_id: DefId,
    pub cardinality: PropertyCardinality,

    /// Special optimization in the `ontol` domain:
    /// `bool` is (maybe) `true` or `false`,
    /// but don't treat that as a union _proper_, because `bool` should just be used directly as an alias for the union.
    pub is_ontol_alias: bool,
}

impl Is {
    pub fn is_optional(&self) -> bool {
        matches!(self.cardinality, PropertyCardinality::Optional)
    }
}

#[derive(Default, Debug)]
pub struct Properties {
    pub constructor: Constructor,
    pub table: Option<IndexMap<PropertyId, Property>>,
    pub identifies: Option<RelationshipId>,
    pub identified_by: Option<RelationshipId>,
}

impl Properties {
    pub fn constructor(&self) -> &Constructor {
        &self.constructor
    }

    pub fn insert_map_property(&mut self, property_id: PropertyId, property: Property) {
        let table = self.table.get_or_insert_with(Default::default);
        table.insert(property_id, property);
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
///
/// TODO: Replace this with ReprKind..
/// A "constructor" concept may still nice to have, e.g. in relation to deserialization.
#[derive(Default, Debug)]
pub enum Constructor {
    /// There is nothing special about this type, it is just a "struct" consisting of relations to other types.
    #[default]
    Transparent,
    /// The type represents an abstraction of another type, using one [is] relation.
    Value(RelationshipId, SourceSpan, Cardinality),
    /// The type is a tuple-like sequence of other types
    Sequence(Sequence),
    /// The type is a string pattern
    StringFmt(StringPatternSegment),
}
