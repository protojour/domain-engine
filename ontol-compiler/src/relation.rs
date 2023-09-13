use std::collections::BTreeSet;

use fnv::{FnvHashMap, FnvHashSet};
use indexmap::IndexMap;
use ontol_runtime::{
    discriminator::UnionDiscriminator, ontology::Cardinality, value::PropertyId,
    value_generator::ValueGenerator, DefId, PackageId, RelationshipId,
};

use crate::{sequence::Sequence, text_patterns::TextPatternSegment, SourceSpan};

#[derive(Default, Debug)]
pub struct Relations {
    pub properties_by_def_id: FnvHashMap<DefId, Properties>,
    /// A map from "idenfities" relationship to named relationship:
    pub inherent_id_map: FnvHashMap<RelationshipId, RelationshipId>,

    pub text_pattern_constructors: FnvHashSet<DefId>,
    pub union_discriminators: FnvHashMap<DefId, UnionDiscriminator>,

    /// `default` relationships:
    pub default_const_objects: FnvHashMap<RelationshipId, DefId>,
    /// `gen` relations, what the user wrote directly:
    pub value_generators_unchecked: FnvHashMap<RelationshipId, (DefId, SourceSpan)>,
    /// `gen` relations after proper type check:
    pub value_generators: FnvHashMap<RelationshipId, ValueGenerator>,

    /// `is` relations
    pub ontology_mesh: FnvHashMap<DefId, IndexMap<Is, SourceSpan>>,

    /// `rel type` parameters (instantiated) for various types
    pub type_params: FnvHashMap<DefId, IndexMap<DefId, TypeParam>>,

    pub rel_type_constraints: FnvHashMap<DefId, RelTypeConstraints>,
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
    pub rel: TypeRelation,
}

impl Is {
    pub fn is_super(&self) -> bool {
        matches!(self.rel, TypeRelation::Super | TypeRelation::ImplicitSuper)
    }

    pub fn is_sub(&self) -> bool {
        matches!(self.rel, TypeRelation::Subset | TypeRelation::SubVariant)
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum TypeRelation {
    /// Explicit supertype relation
    Super,
    /// Implicit supertype relation, no actual "inheritance" (built-in only)
    ImplicitSuper,
    /// Explicit subvariant relation
    SubVariant,
    /// Implicit subset relation (built-in only)
    Subset,
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

    /// Get the property table, create it if None
    pub fn table_mut(&mut self) -> &mut IndexMap<PropertyId, Property> {
        self.table.get_or_insert_with(Default::default)
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
    /// The type is a tuple-like sequence of other types
    Sequence(Sequence),
    /// The type is a text pattern
    TextFmt(TextPatternSegment),
}

#[derive(Default, Debug)]
pub struct RelTypeConstraints {
    /// Constraints for the subject type
    pub subject_set: BTreeSet<DefId>,

    /// Constraints for the object type
    pub object: Vec<RelObjectConstraint>,
}

#[derive(Debug)]
pub enum RelObjectConstraint {
    /// The object type must be a constant of the subject type
    ConstantOfSubjectType,
    Generator,
}

#[derive(Clone, Debug)]
pub struct TypeParam {
    pub object: DefId,
    pub definition_site: PackageId,
    pub span: SourceSpan,
}
