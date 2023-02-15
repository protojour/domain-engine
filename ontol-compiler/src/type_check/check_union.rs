use std::collections::{HashMap, HashSet};

use indexmap::{IndexMap, IndexSet};
use ontol_runtime::{
    discriminator::{Discriminant, UnionDiscriminator, VariantDiscriminator},
    smart_format, DefId, RelationId,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    compiler_queries::GetPropertyMeta,
    def::{Cardinality, Def, PropertyCardinality, ValueCardinality},
    error::CompileError,
    relation::{Constructor, SubjectProperties},
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
        let mut used_objects: HashSet<DefId> = Default::default();

        for (relationship_id, span) in relationship_ids {
            let (relationship, _) = self
                .get_relationship_meta(*relationship_id)
                .expect("BUG: problem getting property meta");
            let (object_def, _) = relationship.object;

            debug!("check union relationship {relationship:?}");

            if used_objects.contains(&object_def) {
                error_set.report(
                    object_def,
                    UnionCheckError::DuplicateAnonymousRelation,
                    span,
                );
                continue;
            }

            let object_ty = self.def_types.map.get(&object_def).unwrap();

            match object_ty {
                Type::Unit(def_id) => builder.unit = Some(*def_id),
                Type::Int(_) => builder.number = Some(IntDiscriminator(object_def)),
                Type::String(_) => {
                    builder.string = StringDiscriminator::Any(object_def);
                }
                Type::StringConstant(def_id) => {
                    let string_literal = self.defs.get_string_literal(*def_id);
                    builder.add_string_literal(string_literal, *def_id);
                }
                Type::Domain(domain_def_id) => {
                    match self.find_domain_type_match_data(*domain_def_id) {
                        Ok(DomainTypeMatchData::Map(property_set)) => {
                            self.add_property_set_to_discriminator(
                                &mut builder,
                                object_def,
                                property_set,
                                span,
                                &mut error_set,
                            );
                        }
                        Ok(DomainTypeMatchData::Sequence(_)) => {
                            if builder.sequence.is_some() {
                                error_set.report(
                                    object_def,
                                    UnionCheckError::CannotDiscriminateType,
                                    span,
                                );
                            } else {
                                builder.sequence = Some(object_def);
                            }
                        }
                        Err(error) => {
                            error_set.report(object_def, error, span);
                        }
                    }
                }
                _ => {
                    error_set.report(object_def, UnionCheckError::CannotDiscriminateType, span);
                }
            }

            used_objects.insert(object_def);
        }

        self.limit_property_discriminators(
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
                    Constructor::Identity => match &properties.subject {
                        SubjectProperties::Empty => {
                            return Err(UnionCheckError::UnitTypePartOfUnion(def_id));
                        }
                        SubjectProperties::Map(property_set) => {
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
                },
                None => {
                    return Err(UnionCheckError::CannotDiscriminateType);
                }
            }
        }
    }

    fn add_property_set_to_discriminator(
        &self,
        discriminator_builder: &mut DiscriminatorBuilder,
        object_def: DefId,
        property_set: &IndexMap<RelationId, Cardinality>,
        span: &SourceSpan,
        error_set: &mut ErrorSet,
    ) {
        let mut map_discriminator_candidate = MapDiscriminatorCandidate {
            result_type: object_def,
            property_candidates: vec![],
        };

        for (relation_id, _cardinality) in property_set {
            let (relationship, relation) = self
                .get_subject_property_meta(object_def, *relation_id)
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
                    let string_literal = self.defs.get_string_literal(*def_id);
                    map_discriminator_candidate.property_candidates.push(
                        PropertyDiscriminatorCandidate {
                            relation_id: relationship.relation_id,
                            discriminant: Discriminant::HasStringAttribute(
                                *relation_id,
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
            error_set.report(object_def, UnionCheckError::CannotDiscriminateType, span);
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

        let Some((selected_relation, _)) = relation_counters
            .into_iter()
            .find(|(_, count)| *count == total_candidates) else {
            error_set.report(union_def_id, UnionCheckError::NoUniformDiscriminatorFound, &union_def.span);
            return;
        };

        debug!("selected relation {selected_relation:?}");

        for discriminator in &mut builder.map_discriminator_candidates {
            discriminator
                .property_candidates
                .retain(|property_candidate| property_candidate.relation_id == selected_relation)
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
        }
    }
}

enum DomainTypeMatchData<'a> {
    Map(&'a IndexMap<RelationId, Cardinality>),
    Sequence(&'a Sequence),
}

#[derive(Default)]
struct DiscriminatorBuilder {
    unit: Option<DefId>,
    number: Option<IntDiscriminator>,
    string: StringDiscriminator,
    sequence: Option<DefId>,
    map_discriminator_candidates: Vec<MapDiscriminatorCandidate>,
}

impl DiscriminatorBuilder {
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
}
