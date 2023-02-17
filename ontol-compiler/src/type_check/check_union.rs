use std::collections::{HashMap, HashSet};

use indexmap::{IndexMap, IndexSet};
use ontol_runtime::{
    discriminator::{Discriminant, UnionDiscriminator, VariantDiscriminator},
    smart_format,
    value::PropertyId,
    DefId, RelationId,
};
use patricia_tree::PatriciaMap;
use smartstring::alias::String;
use tracing::debug;

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Cardinality, Def, PropertyCardinality, RelationIdent, ValueCardinality},
    error::CompileError,
    patterns::StringPatternSegment,
    relation::{Constructor, MapProperties},
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
        let Constructor::ValueUnion(relationship_ids) = &properties.constructor else {
            panic!("not a union");
        };

        let mut builder = DiscriminatorBuilder::default();
        let mut used_variants: HashSet<DefId> = Default::default();

        for (relationship_id, span) in relationship_ids {
            let (relationship, relation) = self
                .get_relationship_meta(*relationship_id)
                .expect("BUG: problem getting property meta");

            debug!("check union {relationship:?}");

            let variant_def = match relation.ident {
                RelationIdent::Named(def_id) | RelationIdent::Typed(def_id) => def_id,
                _ => relationship.object.0,
            };

            if used_variants.contains(&variant_def) {
                error_set.report(
                    variant_def,
                    UnionCheckError::DuplicateAnonymousRelation,
                    span,
                );
                continue;
            }

            let variant_ty = self.def_types.map.get(&variant_def).unwrap_or_else(|| {
                let def = self.defs.get_def_kind(variant_def);
                panic!("No type found for {def:?}");
            });

            match variant_ty {
                Type::Unit(def_id) => builder.unit = Some(*def_id),
                Type::Int(_) => builder.number = Some(IntDiscriminator(variant_def)),
                Type::String(_) => {
                    builder.string = StringDiscriminator::Any(variant_def);
                }
                Type::StringConstant(def_id) => {
                    let string_literal = self.defs.get_string_representation(*def_id);
                    builder.add_string_literal(string_literal, *def_id);
                }
                Type::Domain(domain_def_id) => {
                    match self.find_domain_type_match_data(*domain_def_id) {
                        Ok(DomainTypeMatchData::Map(property_set)) => {
                            self.add_property_set_to_discriminator(
                                &mut builder,
                                variant_def,
                                property_set,
                                span,
                                &mut error_set,
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
                    }
                }
                _ => {
                    error_set.report(variant_def, UnionCheckError::CannotDiscriminateType, span);
                }
            }

            used_variants.insert(variant_def);
        }

        self.limit_property_discriminators(
            value_union_def_id,
            union_def,
            &mut builder,
            &mut error_set,
        );
        self.verify_string_patterns_are_disjunctive(
            value_union_def_id,
            union_def,
            &mut builder,
            &mut error_set,
        );

        let union_discriminator = self.make_union_discriminator(builder, &error_set);
        self.relations
            .union_discriminators
            .insert(value_union_def_id, union_discriminator);

        error_set
            .errors
            .into_iter()
            .flat_map(|(_, errors)| errors.into_iter())
            .map(|(union_error, span)| {
                self.make_compile_error(union_error)
                    .spanned(self.sources, &span)
            })
            .collect()
    }

    fn find_domain_type_match_data(
        &self,
        mut def_id: DefId,
    ) -> Result<DomainTypeMatchData<'_>, UnionCheckError> {
        loop {
            match self.relations.properties_by_type(def_id) {
                Some(properties) => match &properties.constructor {
                    Constructor::Identity => match &properties.map {
                        MapProperties::Empty => {
                            return Err(UnionCheckError::UnitTypePartOfUnion(def_id));
                        }
                        MapProperties::Map(property_set) => {
                            return Ok(DomainTypeMatchData::Map(property_set));
                        }
                    },
                    Constructor::Value(
                        relationship_id,
                        _,
                        (PropertyCardinality::Mandatory, ValueCardinality::One),
                    ) => {
                        let (relationship, _) = self
                            .get_relationship_meta(*relationship_id)
                            .expect("BUG: problem getting property meta");

                        def_id = relationship.object.0;
                        continue;
                    }
                    Constructor::Value(_, _, _) => {
                        todo!("test non-standard value cardinality");
                    }
                    Constructor::ValueUnion(_) => {
                        return Err(UnionCheckError::UnionTreeNotSupported);
                    }
                    Constructor::Sequence(sequence) => {
                        return Ok(DomainTypeMatchData::Sequence(sequence));
                    }
                    Constructor::StringPattern(segment) => {
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
        property_set: &IndexMap<PropertyId, Cardinality>,
        span: &SourceSpan,
        error_set: &mut ErrorSet,
    ) {
        let mut map_discriminator_candidate = MapDiscriminatorCandidate {
            result_type: variant_def,
            property_candidates: vec![],
        };

        for (property_id, _cardinality) in property_set {
            let (relationship, relation) = self
                .get_subject_property_meta(variant_def, property_id.relation_id)
                .expect("BUG: problem getting property meta");

            let (object_def, _) = relationship.object;
            let object_ty = self.def_types.map.get(&object_def).unwrap();
            let Some(property_name) = relation.object_prop(self.defs) else {
                continue;
            };

            debug!("trying rel {relationship:?} {relation:?} ty: {object_ty:?}");

            match object_ty {
                Type::IntConstant(_) => {
                    todo!("Cannot match against numeric constants yet");
                }
                Type::StringConstant(def_id) => {
                    let string_literal = self.defs.get_string_representation(*def_id);
                    map_discriminator_candidate.property_candidates.push(
                        PropertyDiscriminatorCandidate {
                            relation_id: relationship.relation_id,
                            discriminant: Discriminant::HasStringAttribute(
                                property_id.relation_id,
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

        let mut relation_counters: HashMap<RelationId, usize> = Default::default();

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
    fn verify_string_patterns_are_disjunctive(
        &self,
        union_def_id: DefId,
        union_def: &Def,
        builder: &mut DiscriminatorBuilder,
        error_set: &mut ErrorSet,
    ) {
        if builder.pattern_candidates.is_empty() {
            return;
        }

        let mut prefix_index: PatriciaMap<HashSet<DefId>> = Default::default();

        for (variant_def_id, segment) in &builder.pattern_candidates {
            let prefix = segment.constant_prefix();

            if let Some(prefix) = prefix {
                if let Some(set) = prefix_index.get_mut(&prefix) {
                    set.insert(*variant_def_id);
                } else {
                    prefix_index.insert(prefix, [*variant_def_id].into());
                }
            }
        }

        // Also check for ambiguity with string literals
        if let StringDiscriminator::Literals(literals) = &builder.string {
            for (literal, variant_def_id) in literals {
                if let Some(set) = prefix_index.get_mut(literal) {
                    set.insert(*variant_def_id);
                } else {
                    prefix_index.insert(literal, [*variant_def_id].into());
                }
            }
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
            error_set.report(
                union_def_id,
                UnionCheckError::SharedPrefixInPatternUnion,
                &union_def.span,
            );
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
                result_type: def_id,
            })
        }

        if let Some(number) = builder.number {
            union_discriminator.variants.push(VariantDiscriminator {
                discriminant: Discriminant::IsInt,
                result_type: number.0,
            })
        }

        // match patterns before strings
        for (variant_def_id, _) in builder.pattern_candidates {
            union_discriminator.variants.push(VariantDiscriminator {
                discriminant: Discriminant::MatchesCapturingStringPattern,
                result_type: variant_def_id,
            });
        }

        match builder.string {
            StringDiscriminator::None => {}
            StringDiscriminator::Any(def_id) => {
                union_discriminator.variants.push(VariantDiscriminator {
                    discriminant: Discriminant::IsString,
                    result_type: def_id,
                });
            }
            StringDiscriminator::Literals(literals) => {
                for (literal, def_id) in literals {
                    union_discriminator.variants.push(VariantDiscriminator {
                        discriminant: Discriminant::IsStringLiteral(literal),
                        result_type: def_id,
                    });
                }
            }
        }

        if let Some(sequence_def_id) = builder.sequence {
            union_discriminator.variants.push(VariantDiscriminator {
                discriminant: Discriminant::IsSequence,
                result_type: sequence_def_id,
            });
        }

        for map_discriminator in builder.map_discriminator_candidates {
            for candidate in map_discriminator.property_candidates {
                union_discriminator.variants.push(VariantDiscriminator {
                    discriminant: candidate.discriminant,
                    result_type: map_discriminator.result_type,
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
                CompileError::UnitTypePartOfUnion(smart_format!("{}", FormatType(ty, self.defs)))
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
        }
    }
}

enum DomainTypeMatchData<'a> {
    Map(&'a IndexMap<PropertyId, Cardinality>),
    Sequence(&'a Sequence),
    ConstructorStringPattern(&'a StringPatternSegment),
}

#[derive(Default)]
struct DiscriminatorBuilder<'a> {
    unit: Option<DefId>,
    number: Option<IntDiscriminator>,
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
    errors: HashMap<DefId, HashMap<UnionCheckError, SourceSpan>>,
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
}
