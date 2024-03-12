use std::collections::BTreeSet;

use fnv::{FnvHashMap, FnvHashSet};
use indexmap::IndexMap;
use ontol_runtime::{
    interface::discriminator::UnionDiscriminator, ontology::Cardinality, value::PropertyId,
    value_generator::ValueGenerator, DefId, PackageId, RelationshipId,
};

use crate::{sequence::Sequence, text_patterns::TextPatternSegment, SourceSpan};

#[derive(Default)]
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
    /// `order` relations
    pub order_relationships: FnvHashMap<DefId, Vec<RelationshipId>>,

    /// `rel type` parameters (instantiated) for various types
    pub type_params: FnvHashMap<DefId, IndexMap<DefId, TypeParam>>,

    pub rel_type_constraints: FnvHashMap<DefId, RelTypeConstraints>,
}

impl Relations {
    pub fn properties_by_def_id(&self, domain_type_id: DefId) -> Option<&Properties> {
        self.properties_by_def_id.get(&domain_type_id)
    }

    pub fn properties_table_by_def_id(
        &self,
        domain_type_id: DefId,
    ) -> Option<&IndexMap<PropertyId, Property>> {
        self.properties_by_def_id
            .get(&domain_type_id)
            .and_then(|properties| properties.table.as_ref())
    }

    pub fn properties_by_def_id_mut(&mut self, domain_type_id: DefId) -> &mut Properties {
        self.properties_by_def_id.entry(domain_type_id).or_default()
    }

    pub fn identified_by(&self, domain_type_id: DefId) -> Option<RelationshipId> {
        let properties = self.properties_by_def_id(domain_type_id)?;
        properties.identified_by
    }

    /// Stable-sort property tables such that all Subject property roles appear before Object roles.
    pub fn sort_property_tables(&mut self) {
        for properties in &mut self.properties_by_def_id.values_mut() {
            if let Some(table) = &mut properties.table {
                let mut table_vec: Vec<_> = std::mem::take(table).into_iter().collect();

                table_vec.sort_by(|(id_a, _), (id_b, _)| id_a.role.cmp(&id_b.role));

                *table = table_vec.into_iter().collect();
            }
        }
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

/// Cache of which DefId is a member of which unions
pub struct UnionMemberCache {
    pub(crate) cache: FnvHashMap<DefId, BTreeSet<DefId>>,
}
