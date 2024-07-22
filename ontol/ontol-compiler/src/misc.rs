use std::collections::BTreeSet;

use fnv::{FnvHashMap, FnvHashSet};
use indexmap::IndexMap;
use ontol_runtime::{
    interface::discriminator::Discriminant, ontology::ontol::ValueGenerator, DefId, PackageId,
    RelId,
};

use crate::SourceSpan;

#[derive(Default)]
pub struct MiscCtx {
    /// A map from "idenfities" relationship to named relationship:
    pub inherent_id_map: FnvHashMap<RelId, RelId>,

    pub text_pattern_constructors: FnvHashSet<DefId>,
    pub union_discriminators: FnvHashMap<DefId, UnionDiscriminator>,

    /// `default` relationships:
    pub default_const_objects: FnvHashMap<RelId, DefId>,
    /// `gen` relations, what the user wrote directly:
    pub value_generators_unchecked: FnvHashMap<RelId, (DefId, SourceSpan)>,
    /// `gen` relations after proper type check:
    pub value_generators: FnvHashMap<RelId, ValueGenerator>,
    /// `order` relations
    pub order_relationships: FnvHashMap<DefId, Vec<RelId>>,
    /// `direction` relations
    pub direction_relationships: FnvHashMap<DefId, (RelId, DefId)>,

    /// `rel type` parameters (instantiated) for various types
    pub type_params: FnvHashMap<DefId, IndexMap<DefId, TypeParam>>,

    pub rel_type_constraints: FnvHashMap<DefId, RelTypeConstraints>,
}

#[derive(Debug)]
pub struct UnionDiscriminator {
    pub variants: Vec<UnionDiscriminatorVariant>,
}

#[derive(Debug)]
pub struct UnionDiscriminatorVariant {
    pub discriminant: Discriminant,
    pub role: UnionDiscriminatorRole,
    pub def_id: DefId,
}

#[derive(Debug)]
pub enum UnionDiscriminatorRole {
    Data,
    IdentifierOf(DefId),
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
    #[allow(unused)]
    Generator,
}

#[derive(Clone, Debug)]
pub struct TypeParam {
    pub object: DefId,
    pub definition_site: PackageId,
    pub span: SourceSpan,
}
