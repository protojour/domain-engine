use std::collections::{HashMap, HashSet};

use indexmap::IndexSet;
use ontol_runtime::{DefId, PropertyId};

use crate::{
    compiler_queries::GetPropertyMeta, error::CompileError, relation::SubjectProperties,
    types::Type, SourceSpan, SpannedCompileError,
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

        let properties = self
            .relations
            .properties_by_type(value_union_def_id)
            .unwrap();
        let SubjectProperties::ValueUnion(property_ids) = &properties.subject else {
            panic!("not a union");
        };

        let mut discriminator_builder = DiscriminatorBuilder::default();
        let mut used_objects: HashSet<DefId> = Default::default();

        for (property_id, span) in property_ids {
            let (_, relationship, _) = self
                .get_property_meta(*property_id)
                .expect("BUG: problem getting property meta");
            let object_def = relationship.object;

            if used_objects.contains(&object_def) {
                error_set.report(
                    object_def,
                    UnionCheckError::DuplicateAnonymousRelation,
                    span,
                );
            }

            let object_ty = self.def_types.map.get(&object_def).unwrap();

            match object_ty {
                Type::Number => discriminator_builder.number = Some(NumberDiscriminator {}),
                Type::String => discriminator_builder.string = Some(StringDiscriminator {}),
                Type::Domain(domain_def_id) => {
                    match self.find_subject_map_properties(*domain_def_id) {
                        Ok(property_set) => {
                            self.add_property_set_to_discriminator(
                                &mut discriminator_builder,
                                object_def,
                                property_set,
                            );
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

        error_set
            .errors
            .into_iter()
            .flat_map(|(_, errors)| errors.into_iter())
            .map(|(union_error, span)| {
                self.make_compile_error(union_error)
                    .spanned(&self.sources, &span)
            })
            .collect()
    }

    fn add_property_set_to_discriminator(
        &self,
        discriminator_builder: &mut DiscriminatorBuilder,
        object_def: DefId,
        property_set: &IndexSet<PropertyId>,
    ) {
        for property_id in property_set {
            let (_, _, relation) = self
                .get_property_meta(*property_id)
                .expect("BUG: problem getting property meta");
        }
        discriminator_builder.add_property_set(object_def, property_set);

        todo!()
    }

    fn find_subject_map_properties(
        &self,
        mut def_id: DefId,
    ) -> Result<&IndexSet<PropertyId>, UnionCheckError> {
        loop {
            match self.relations.properties_by_type(def_id) {
                Some(properties) => match &properties.subject {
                    SubjectProperties::Unit => {
                        return Err(UnionCheckError::UnitTypePartOfUnion(def_id));
                    }
                    SubjectProperties::Value(property_id, _) => {
                        let (_, relationship, _) = self
                            .get_property_meta(*property_id)
                            .expect("BUG: problem getting property meta");

                        def_id = relationship.object;
                        continue;
                    }
                    SubjectProperties::ValueUnion(_) => {
                        return Err(UnionCheckError::UnionTreeNotSupported);
                    }
                    SubjectProperties::Map(property_set) => {
                        return Ok(property_set);
                    }
                },
                None => {
                    return Err(UnionCheckError::CannotDiscriminateType);
                }
            }
        }
    }

    fn make_compile_error(&self, union_error: UnionCheckError) -> CompileError {
        match union_error {
            UnionCheckError::UnitTypePartOfUnion(def_id) => {
                let def = self.defs.map.get(&def_id);
                CompileError::UnitTypePartOfUnion(
                    def.and_then(|def| def.kind.diagnostics_identifier())
                        .unwrap_or_else(|| "?")
                        .into(),
                )
            }
            UnionCheckError::CannotDiscriminateType => CompileError::CannotDiscriminateType,
            UnionCheckError::UnionTreeNotSupported => CompileError::UnionTreeNotSupported,
            UnionCheckError::DuplicateAnonymousRelation => CompileError::DuplicateAnonymousRelation,
        }
    }
}

#[derive(Default)]
struct DiscriminatorBuilder {
    number: Option<NumberDiscriminator>,
    string: Option<StringDiscriminator>,
    property: Option<PropertyDiscriminator>,
}

impl DiscriminatorBuilder {
    fn add_property_set(&mut self, def_id: DefId, property_set: &IndexSet<PropertyId>) {
        let map_discriminator = self.property.get_or_insert_with(Default::default);
    }
}

struct NumberDiscriminator {}
struct StringDiscriminator {}

#[derive(Default)]
struct PropertyDiscriminator {}

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
}
