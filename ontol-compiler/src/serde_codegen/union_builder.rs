use std::collections::BTreeMap;

use ontol_runtime::{
    discriminator::{Discriminant, VariantDiscriminator},
    serde::{SerdeOperator, SerdeOperatorId, SerdeOperatorKey, ValueUnionDiscriminator},
    smart_format, DefId,
};
use smartstring::alias::String;
use tracing::debug;

use super::serde_generator::SerdeGenerator;

#[derive(Default)]
pub struct UnionBuilder {
    discriminator_candidates: Vec<ValueUnionDiscriminator>,
}

impl UnionBuilder {
    pub fn build(
        self,
        generator: &mut SerdeGenerator,
        mut map_operator_fn: impl FnMut(&mut SerdeGenerator, SerdeOperatorId, DefId) -> SerdeOperatorId,
    ) -> Result<Vec<ValueUnionDiscriminator>, String> {
        let mut discriminators_by_discriminant: BTreeMap<
            Discriminant,
            Vec<(SerdeOperatorId, DefId)>,
        > = Default::default();

        for candidate in self.discriminator_candidates {
            let result_type = candidate.discriminator.result_type;
            let operator_id = map_operator_fn(generator, candidate.operator_id, result_type);

            match candidate.discriminator.discriminant {
                Discriminant::MapFallback => {
                    panic!("MapFallback should have been filtered already");
                }
                Discriminant::IsSingletonProperty(relation_id, prop) => {
                    // TODO: We don't know that we have to do any disambiguation here
                    // (there might be only one singleton property)
                    let operator =
                        generator.get_serde_operator(SerdeOperatorKey::Identity(result_type));

                    match operator {
                        Some(SerdeOperator::CapturingStringPattern(def_id)) => {
                            // convert this
                            discriminators_by_discriminant
                                .entry(Discriminant::HasAttributeMatchingStringPattern(
                                    relation_id,
                                    prop,
                                    *def_id,
                                ))
                                .or_default()
                                .push((operator_id, result_type));
                        }
                        _ => {
                            discriminators_by_discriminant
                                .entry(Discriminant::IsSingletonProperty(relation_id, prop))
                                .or_default()
                                .push((operator_id, result_type));
                        }
                    }
                }
                discriminant => {
                    discriminators_by_discriminant
                        .entry(discriminant)
                        .or_default()
                        .push((operator_id, result_type));
                }
            }
        }

        let mut discriminators = vec![];

        for (discriminant, entries) in discriminators_by_discriminant {
            if entries.len() > 1 {
                return Err(smart_format!(
                    "BUG: Discriminant {discriminant:?} has multiple entries: {entries:?}"
                ));
            }

            if let Some((operator_id, result_type)) = entries.into_iter().next() {
                discriminators.push(ValueUnionDiscriminator {
                    discriminator: VariantDiscriminator {
                        discriminant,
                        result_type,
                    },
                    operator_id,
                });
            }
        }

        Ok(discriminators)
    }

    pub fn add_root_discriminator(
        &mut self,
        generator: &mut SerdeGenerator,
        discriminator: &VariantDiscriminator,
    ) -> Result<(), String> {
        let operator_id = match generator
            .get_serde_operator_id(SerdeOperatorKey::Identity(discriminator.result_type))
        {
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
            SerdeOperator::ValueUnionType(value_union) => {
                for inner_discriminator in &value_union.discriminators {
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
            other => {
                debug!("PUSH DISCR scope={scope:#?} discriminator={discriminator:#?} {other:?}");
                match discriminator.discriminant {
                    Discriminant::MapFallback => {
                        if let Some(scoping) = scope.last() {
                            self.discriminator_candidates.push(ValueUnionDiscriminator {
                                discriminator: (*scoping).clone(),
                                operator_id,
                            });
                            Ok(())
                        } else {
                            Err(smart_format!("MapFallback without scoping"))
                        }
                    }
                    _ => {
                        self.discriminator_candidates.push(ValueUnionDiscriminator {
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
