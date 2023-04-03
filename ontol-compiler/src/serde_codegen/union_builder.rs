use std::collections::BTreeMap;

use ontol_runtime::{
    discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
    serde::{
        operator::{SerdeOperator, SerdeOperatorId, ValueUnionVariant},
        SerdeKey,
    },
    smart_format, DefId, DefVariant,
};
use smartstring::alias::String;

use super::serde_generator::SerdeGenerator;

pub struct UnionBuilder {
    // variants _sorted_ by purpose
    variant_candidates_by_purpose: BTreeMap<VariantPurpose, Vec<ValueUnionVariant>>,
    def_variant: DefVariant,
}

impl UnionBuilder {
    pub fn new(def_variant: DefVariant) -> Self {
        Self {
            variant_candidates_by_purpose: Default::default(),
            def_variant,
        }
    }
}

impl UnionBuilder {
    pub fn build(
        self,
        generator: &mut SerdeGenerator,
        mut map_operator_fn: impl FnMut(&mut SerdeGenerator, SerdeOperatorId, DefId) -> SerdeOperatorId,
    ) -> Result<Vec<ValueUnionVariant>, String> {
        // sanity check
        let mut ambiguous_discriminant_debug: BTreeMap<Discriminant, usize> = Default::default();

        let mut variant_candidates: Vec<_> = self
            .variant_candidates_by_purpose
            .into_iter()
            .flat_map(|(_, variants)| variants)
            .collect();

        for candidate in &mut variant_candidates {
            let result_type = candidate.discriminator.def_variant.def_id;

            candidate.operator_id = map_operator_fn(generator, candidate.operator_id, result_type);

            match &mut candidate.discriminator.discriminant {
                Discriminant::MapFallback => {
                    panic!("MapFallback should have been filtered already");
                }
                Discriminant::IsSingletonProperty(relation_id, prop) => {
                    // TODO: We don't know that we have to do any disambiguation here
                    // (there might be only one singleton property)
                    let operator = generator.get_serde_operator(SerdeKey::no_modifier(result_type));

                    if let Some(SerdeOperator::CapturingStringPattern(def_id)) = operator {
                        // convert this
                        candidate.discriminator.discriminant =
                            Discriminant::HasAttributeMatchingStringPattern(
                                *relation_id,
                                prop.clone(),
                                *def_id,
                            );
                    }
                }
                _ => {}
            }

            *ambiguous_discriminant_debug
                .entry(candidate.discriminator.discriminant.clone())
                .or_default() += 1;
        }

        for (discriminant, count) in ambiguous_discriminant_debug {
            if count > 1 {
                return Err(smart_format!("Discriminant {discriminant:?} is ambiguous:"));
            }
        }

        Ok(variant_candidates)
    }

    pub fn add_root_discriminator(
        &mut self,
        generator: &mut SerdeGenerator,
        discriminator: &VariantDiscriminator,
    ) -> Result<(), String> {
        let operator_id = match generator.get_serde_operator_id(SerdeKey::Def(
            self.def_variant.with_def(discriminator.def_variant.def_id),
        )) {
            Some(operator_id) => operator_id,
            None => return Ok(()),
        };

        // Push with empty scope ('root scope')
        self.push_discriminator(generator, &[], discriminator, operator_id)
    }

    fn push_discriminator(
        &mut self,
        generator: &SerdeGenerator,
        scope: &[&VariantDiscriminator],
        discriminator: &VariantDiscriminator,
        operator_id: SerdeOperatorId,
    ) -> Result<(), String> {
        let operator = &generator.operators_by_id[operator_id.0 as usize];
        match operator {
            SerdeOperator::Union(union_op) => {
                for inner_discriminator in union_op.unfiltered_variants() {
                    let mut child_scope: Vec<&VariantDiscriminator> = vec![];
                    child_scope.extend(scope.iter());
                    child_scope.push(discriminator);

                    self.push_discriminator(
                        generator,
                        &child_scope,
                        &inner_discriminator.discriminator,
                        inner_discriminator.operator_id,
                    )?;
                }
                Ok(())
            }
            _other => {
                // debug!("PUSH DISCR scope={scope:#?} discriminator={discriminator:#?} {other:?}");
                match discriminator.discriminant {
                    Discriminant::MapFallback => {
                        if let Some(scoping) = scope.last() {
                            self.variant_candidates_by_purpose
                                .entry(scoping.purpose)
                                .or_default()
                                .push(ValueUnionVariant {
                                    discriminator: (*scoping).clone(),
                                    operator_id,
                                });

                            Ok(())
                        } else {
                            Err(smart_format!("MapFallback without scoping"))
                        }
                    }
                    _ => {
                        self.variant_candidates_by_purpose
                            .entry(discriminator.purpose)
                            .or_default()
                            .push(ValueUnionVariant {
                                discriminator: discriminator.clone(),
                                operator_id,
                            });
                        Ok(())
                    }
                }
            }
        }
    }
}
