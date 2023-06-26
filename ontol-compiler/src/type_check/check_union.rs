use std::collections::HashSet;

use fnv::{FnvHashMap, FnvHashSet};
use indexmap::{IndexMap, IndexSet};
use ontol_runtime::{
    discriminator::{Discriminant, UnionDiscriminator, VariantDiscriminator, VariantPurpose},
    env::{PropertyCardinality, ValueCardinality},
    smart_format,
    value::PropertyId,
    DataModifier, DefId, DefVariant,
};
use patricia_tree::PatriciaMap;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    def::{Def, RelationId, RelationKind},
    error::CompileError,
    patterns::StringPatternSegment,
    relation::{Constructor, Property},
    sequence::Sequence,
    types::{FormatType, Type},
    SourceSpan, SpannedCompileError,
};

use super::TypeCheck;

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub fn check_unions(&mut self) {
        let value_unions = std::mem::take(&mut self.relations.value_unions);

        for value_union_def_id in value_unions {
            for error in self.check_value_union(value_union_def_id) {
                self.errors.push(error);
            }
        }
    }

    fn check_value_union(&mut self, value_union_def_id: DefId) -> Vec<SpannedCompileError> {
        // An error set to avoid reporting the same error more than once
        let mut error_set = ErrorSet::default();

        let union_def = self.defs.map.get(&value_union_def_id).unwrap();

        let properties = self
            .relations
            .properties_by_type(value_union_def_id)
            .unwrap();
        let Constructor::Union(relationship_ids) = &properties.constructor else {
            panic!("not a union");
        };

        let mut inherent_builder = DiscriminatorBuilder::default();
        // Also verify that entity ids are disjoint:
        let mut entity_id_builder = DiscriminatorBuilder::default();

        let mut used_variants: FnvHashSet<DefId> = Default::default();

        for (relationship_id, span) in relationship_ids {
            let meta = self
                .defs
                .lookup_relationship_meta(*relationship_id)
                .expect("BUG: problem getting relationship meta");

            debug!("check union {:?}", meta.relationship);

            let variant_def = match &meta.relation.kind {
                RelationKind::Named(def) | RelationKind::FmtTransition(def, _) => def.def_id,
                _ => meta.relationship.object.0.def_id,
            };

            if used_variants.contains(&variant_def) {
                error_set.report(
                    variant_def,
                    UnionCheckError::DuplicateAnonymousRelation,
                    span,
                );
                continue;
            }

            self.add_variant_to_builder(&mut inherent_builder, variant_def, &mut error_set, span);

            if let Some(properties) = self.relations.properties_by_type(variant_def) {
                if let Some(id_relationship_id) = &properties.identified_by {
                    let identifies_meta = self
                        .defs
                        .lookup_relationship_meta(*id_relationship_id)
                        .expect("BUG: problem getting relationship meta");

                    self.add_variant_to_builder(
                        &mut entity_id_builder,
                        identifies_meta.relationship.subject.0.def_id,
                        &mut error_set,
                        span,
                    );
                }
            }

            used_variants.insert(variant_def);
        }

        self.limit_property_discriminators(
            value_union_def_id,
            union_def,
            &mut inherent_builder,
            &mut error_set,
        );
        self.verify_disjoint_string_patterns(
            value_union_def_id,
            union_def,
            &mut inherent_builder,
            UnionCheckError::SharedPrefixInPatternUnion,
            &mut error_set,
        );
        self.verify_disjoint_string_patterns(
            value_union_def_id,
            union_def,
            &mut entity_id_builder,
            UnionCheckError::NonDisjointIdsInEntityUnion,
            &mut error_set,
        );

        let union_discriminator = self.make_union_discriminator(inherent_builder, &error_set);
        self.relations
            .union_discriminators
            .insert(value_union_def_id, union_discriminator);

        error_set
            .errors
            .into_iter()
            .flat_map(|(_, errors)| errors.into_iter())
            .map(|(union_error, span)| self.make_compile_error(union_error).spanned(&span))
            .collect()
    }

    fn add_variant_to_builder<'t, 'b>(
        &'t self,
        builder: &'b mut DiscriminatorBuilder<'t>,
        variant_def: DefId,
        error_set: &mut ErrorSet,
        span: &SourceSpan,
    ) where
        't: 'b,
    {
        let variant_ty = self.def_types.map.get(&variant_def).unwrap_or_else(|| {
            let def = self.defs.get_def_kind(variant_def);
            panic!("No type found for {def:?}");
        });

        debug!("Add variant to builder variant_ty: {variant_ty:?}");

        match variant_ty {
            Type::Unit(def_id) => builder.unit = Some(*def_id),
            Type::Int(_) => builder.number = Some(IntDiscriminator(variant_def)),
            Type::String(_) => {
                builder.string = StringDiscriminator::Any(variant_def);
                builder.any_string.push(variant_def);
            }
            Type::StringConstant(def_id) => {
                let string_literal = self.defs.get_string_representation(*def_id);
                builder.add_string_literal(string_literal, *def_id);
            }
            Type::Domain(domain_def_id) => match self.find_domain_type_match_data(*domain_def_id) {
                Ok(DomainTypeMatchData::Map(property_set)) => {
                    self.add_property_set_to_discriminator(
                        builder,
                        variant_def,
                        property_set,
                        span,
                        error_set,
                    );
                }
                Ok(DomainTypeMatchData::Sequence(_)) => {
                    if builder.sequence.is_some() {
                        error_set.report(
                            variant_def,
                            UnionCheckError::CannotDiscriminateType,
                            span,
                        );
                    } else {
                        builder.sequence = Some(variant_def);
                    }
                }
                Ok(DomainTypeMatchData::ConstructorStringPattern(segment)) => {
                    builder.add_string_pattern(segment, variant_def);
                }
                Err(error) => {
                    error_set.report(variant_def, error, span);
                }
            },
            _ => {
                error_set.report(variant_def, UnionCheckError::CannotDiscriminateType, span);
            }
        }
    }

    fn find_domain_type_match_data(
        &self,
        mut def_id: DefId,
    ) -> Result<DomainTypeMatchData<'_>, UnionCheckError> {
        loop {
            match self.relations.properties_by_type(def_id) {
                Some(properties) => match &properties.constructor {
                    Constructor::Struct => match &properties.map {
                        Some(property_set) => {
                            return Ok(DomainTypeMatchData::Map(property_set));
                        }
                        None => {
                            return Err(UnionCheckError::UnitTypePartOfUnion(def_id));
                        }
                    },
                    Constructor::Value(
                        relationship_id,
                        _,
                        (PropertyCardinality::Mandatory, ValueCardinality::One),
                    ) => {
                        let meta = self
                            .defs
                            .lookup_relationship_meta(*relationship_id)
                            .expect("BUG: problem getting realtionship meta");

                        def_id = meta.relationship.object.0.def_id;
                        continue;
                    }
                    Constructor::Value(_, _, _) => {
                        todo!("test non-standard value cardinality");
                    }
                    Constructor::Intersection(_) => {
                        todo!()
                    }
                    Constructor::Union(_) => {
                        return Err(UnionCheckError::UnionTreeNotSupported);
                    }
                    Constructor::Sequence(sequence) => {
                        return Ok(DomainTypeMatchData::Sequence(sequence));
                    }
                    Constructor::StringFmt(segment) => {
                        return Ok(DomainTypeMatchData::ConstructorStringPattern(segment));
                    }
                },
                None => {
                    return Err(UnionCheckError::UnitTypePartOfUnion(def_id));
                }
            }
        }
    }

    fn add_property_set_to_discriminator(
        &self,
        discriminator_builder: &mut DiscriminatorBuilder,
        variant_def: DefId,
        property_set: &IndexMap<PropertyId, Property>,
        span: &SourceSpan,
        error_set: &mut ErrorSet,
    ) {
        let mut map_discriminator_candidate = MapDiscriminatorCandidate {
            result_type: variant_def,
            property_candidates: vec![],
        };

        for (property_id, _cardinality) in property_set {
            let meta = self
                .defs
                .lookup_relationship_meta(property_id.relationship_id)
                .expect("BUG: problem getting relationship meta");

            let (object_reference, _) = &meta.relationship.object;
            let object_ty = self.def_types.map.get(&object_reference.def_id).unwrap();
            let Some(property_name) = meta.relation.object_prop(self.defs) else {
                continue;
            };

            debug!(
                "trying rel {:?} {:?} ty: {object_ty:?}",
                meta.relationship, meta.relation
            );

            match object_ty {
                Type::IntConstant(_) => {
                    todo!("Cannot match against numeric constants yet");
                }
                Type::StringConstant(def_id) => {
                    let string_literal = self.defs.get_string_representation(*def_id);
                    map_discriminator_candidate.property_candidates.push(
                        PropertyDiscriminatorCandidate {
                            relation_id: meta.relationship.relation_id,
                            discriminant: Discriminant::HasStringAttribute(
                                property_id.relationship_id,
                                property_name.into(),
                                string_literal.into(),
                            ),
                        },
                    );
                }
                _ => {}
            }
        }

        if map_discriminator_candidate.property_candidates.is_empty() {
            debug!("no prop candidates for variant");
            error_set.report(variant_def, UnionCheckError::CannotDiscriminateType, span);
        } else {
            discriminator_builder
                .map_discriminator_candidates
                .push(map_discriminator_candidate);
        }
    }

    fn limit_property_discriminators(
        &self,
        union_def_id: DefId,
        union_def: &Def,
        builder: &mut DiscriminatorBuilder,
        error_set: &mut ErrorSet,
    ) {
        let total_candidates = builder.map_discriminator_candidates.len();
        if total_candidates == 0 {
            debug!("no candidates");
            return;
        }

        let mut relation_counters: FnvHashMap<RelationId, usize> = Default::default();

        for discriminator in &builder.map_discriminator_candidates {
            for property_candidate in &discriminator.property_candidates {
                *relation_counters
                    .entry(property_candidate.relation_id)
                    .or_default() += 1;
            }
        }

        if let Some((selected_relation, _)) = relation_counters
            .into_iter()
            .find(|(_, count)| *count == total_candidates)
        {
            debug!("selected relation {selected_relation:?}");

            for discriminator in &mut builder.map_discriminator_candidates {
                discriminator
                    .property_candidates
                    .retain(|property_candidate| {
                        property_candidate.relation_id == selected_relation
                    })
            }
        } else {
            error_set.report(
                union_def_id,
                UnionCheckError::NoUniformDiscriminatorFound,
                &union_def.span,
            );
        }
    }

    /// For now, patterns must have a unique constant prefix.
    fn verify_disjoint_string_patterns(
        &self,
        union_def_id: DefId,
        union_def: &Def,
        builder: &mut DiscriminatorBuilder,
        error_variant: UnionCheckError,
        error_set: &mut ErrorSet,
    ) {
        if builder.any_string.len() > 1 {
            error_set.report(union_def_id, error_variant, &union_def.span);
            return;
        }

        if builder.pattern_candidates.is_empty() {
            return;
        }

        let mut prefix_index: PatriciaMap<FnvHashSet<DefId>> = Default::default();
        let mut guaranteed_ambiguous_count = 0;

        for (variant_def_id, segment) in &builder.pattern_candidates {
            let prefix = segment.constant_prefix();

            if let Some(prefix) = prefix {
                if let Some(set) = prefix_index.get_mut(&prefix) {
                    set.insert(*variant_def_id);
                } else {
                    prefix_index.insert(prefix, HashSet::from_iter([*variant_def_id]));
                }
            } else {
                guaranteed_ambiguous_count += 1;
            }
        }

        if guaranteed_ambiguous_count > 1 {
            error_set.report(union_def_id, error_variant, &union_def.span);
            return;
        }

        // Also check for ambiguity with string literals
        if let StringDiscriminator::Literals(literals) = &builder.string {
            for (literal, variant_def_id) in literals {
                if let Some(set) = prefix_index.get_mut(literal) {
                    set.insert(*variant_def_id);
                } else {
                    prefix_index.insert(literal, HashSet::from_iter([*variant_def_id]));
                }
            }
        }

        if !prefix_index.is_empty() && !builder.any_string.is_empty() {
            error_set.report(union_def_id, error_variant, &union_def.span);
            return;
        }

        let pattern_variants: HashSet<_> = builder.pattern_candidates.keys().copied().collect();
        let mut is_error = false;

        for (prefix, variant_set) in prefix_index.iter() {
            for variant_def_id in variant_set {
                if pattern_variants.contains(variant_def_id) {
                    if variant_set.len() > 1 {
                        // two or more variants have the same prefix
                        is_error = true;
                    }

                    let common_prefixes_len = prefix_index.common_prefixes(&prefix).count();
                    if common_prefixes_len > 1 {
                        // another variant shares a common prefix with this one
                        is_error = true;
                    }
                }
            }
        }

        if is_error {
            error_set.report(union_def_id, error_variant, &union_def.span);
        }
    }

    fn make_union_discriminator(
        &self,
        builder: DiscriminatorBuilder,
        error_set: &ErrorSet,
    ) -> UnionDiscriminator {
        let mut union_discriminator = UnionDiscriminator { variants: vec![] };

        if let Some(def_id) = builder.unit {
            union_discriminator.variants.push(VariantDiscriminator {
                discriminant: Discriminant::IsUnit,
                purpose: VariantPurpose::Data,
                def_variant: DefVariant::new(def_id, DataModifier::NONE),
            })
        }

        if let Some(number) = builder.number {
            union_discriminator.variants.push(VariantDiscriminator {
                discriminant: Discriminant::IsInt,
                purpose: VariantPurpose::Data,
                def_variant: DefVariant::new(number.0, DataModifier::NONE),
            })
        }

        // match patterns before strings
        for (variant_def_id, _) in builder.pattern_candidates {
            union_discriminator.variants.push(VariantDiscriminator {
                discriminant: Discriminant::MatchesCapturingStringPattern(variant_def_id),
                purpose: VariantPurpose::Data,
                def_variant: DefVariant::new(variant_def_id, DataModifier::NONE),
            });
        }

        match builder.string {
            StringDiscriminator::None => {}
            StringDiscriminator::Any(def_id) => {
                union_discriminator.variants.push(VariantDiscriminator {
                    discriminant: Discriminant::IsString,
                    purpose: VariantPurpose::Data,
                    def_variant: DefVariant::new(def_id, DataModifier::NONE),
                });
            }
            StringDiscriminator::Literals(literals) => {
                for (literal, def_id) in literals {
                    union_discriminator.variants.push(VariantDiscriminator {
                        discriminant: Discriminant::IsStringLiteral(literal),
                        purpose: VariantPurpose::Data,
                        def_variant: DefVariant::new(def_id, DataModifier::NONE),
                    });
                }
            }
        }

        if let Some(sequence_def_id) = builder.sequence {
            union_discriminator.variants.push(VariantDiscriminator {
                discriminant: Discriminant::IsSequence,
                purpose: VariantPurpose::Data,
                def_variant: DefVariant::new(sequence_def_id, DataModifier::NONE),
            });
        }

        for map_discriminator in builder.map_discriminator_candidates {
            for candidate in map_discriminator.property_candidates {
                union_discriminator.variants.push(VariantDiscriminator {
                    discriminant: candidate.discriminant,
                    purpose: VariantPurpose::Data,
                    def_variant: DefVariant::new(map_discriminator.result_type, DataModifier::NONE),
                })
            }
        }

        if union_discriminator.variants.is_empty() {
            assert!(!error_set.errors.is_empty());
        }

        union_discriminator
    }

    fn make_compile_error(&self, union_error: UnionCheckError) -> CompileError {
        match union_error {
            UnionCheckError::UnitTypePartOfUnion(def_id) => {
                let ty = self.def_types.map.get(&def_id).unwrap();
                CompileError::UnitTypePartOfUnion(smart_format!(
                    "{}",
                    FormatType(ty, self.defs, self.primitives)
                ))
            }
            UnionCheckError::CannotDiscriminateType => CompileError::CannotDiscriminateType,
            UnionCheckError::UnionTreeNotSupported => CompileError::UnionTreeNotSupported,
            UnionCheckError::DuplicateAnonymousRelation => {
                CompileError::DuplicateAnonymousRelationship
            }
            UnionCheckError::NoUniformDiscriminatorFound => {
                CompileError::NoUniformDiscriminatorFound
            }
            UnionCheckError::SharedPrefixInPatternUnion => CompileError::SharedPrefixInPatternUnion,
            UnionCheckError::NonDisjointIdsInEntityUnion => {
                CompileError::NonDisjointIdsInEntityUnion
            }
        }
    }
}

enum DomainTypeMatchData<'a> {
    Map(&'a IndexMap<PropertyId, Property>),
    Sequence(&'a Sequence),
    ConstructorStringPattern(&'a StringPatternSegment),
}

#[derive(Default)]
struct DiscriminatorBuilder<'a> {
    unit: Option<DefId>,
    number: Option<IntDiscriminator>,
    any_string: Vec<DefId>,
    string: StringDiscriminator,
    sequence: Option<DefId>,
    pattern_candidates: IndexMap<DefId, &'a StringPatternSegment>,
    map_discriminator_candidates: Vec<MapDiscriminatorCandidate>,
}

impl<'a> DiscriminatorBuilder<'a> {
    fn add_string_literal(&mut self, lit: &str, def_id: DefId) {
        match &mut self.string {
            StringDiscriminator::None => {
                self.string = StringDiscriminator::Literals([(lit.into(), def_id)].into());
            }
            StringDiscriminator::Any(_) => {}
            StringDiscriminator::Literals(set) => {
                set.insert((lit.into(), def_id));
            }
        }
    }

    fn add_string_pattern(&mut self, segment: &'a StringPatternSegment, variant_def_id: DefId) {
        self.pattern_candidates.insert(variant_def_id, segment);
    }
}

struct IntDiscriminator(DefId);

#[derive(Default)]
enum StringDiscriminator {
    #[default]
    None,
    Any(DefId),
    Literals(IndexSet<(String, DefId)>),
}

struct MapDiscriminatorCandidate {
    result_type: DefId,
    property_candidates: Vec<PropertyDiscriminatorCandidate>,
}

struct PropertyDiscriminatorCandidate {
    relation_id: RelationId,
    discriminant: Discriminant,
}

#[derive(Default)]
struct ErrorSet {
    errors: FnvHashMap<DefId, FnvHashMap<UnionCheckError, SourceSpan>>,
}

impl ErrorSet {
    fn report(&mut self, def_id: DefId, error: UnionCheckError, span: &SourceSpan) {
        self.errors
            .entry(def_id)
            .or_default()
            .entry(error)
            .or_insert_with(|| *span);
    }
}

#[derive(Hash, Eq, PartialEq)]
enum UnionCheckError {
    UnitTypePartOfUnion(DefId),
    CannotDiscriminateType,
    UnionTreeNotSupported,
    DuplicateAnonymousRelation,
    NoUniformDiscriminatorFound,
    SharedPrefixInPatternUnion,
    NonDisjointIdsInEntityUnion,
}
