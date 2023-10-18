use std::collections::BTreeMap;

use ontol_runtime::{
    interface::discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
    interface::serde::{
        operator::{SerdeOperator, SerdeOperatorAddr, ValueUnionVariant},
        SerdeDef, SerdeKey,
    },
    smart_format, DefId,
};
use smartstring::alias::String;

use super::serde_generator::SerdeGenerator;

pub struct UnionBuilder {
    // variants _sorted_ by purpose
    variant_candidates_by_purpose: BTreeMap<VariantPurpose, Vec<ValueUnionVariant>>,
    def: SerdeDef,
}

impl UnionBuilder {
    pub fn new(def: SerdeDef) -> Self {
        Self {
            variant_candidates_by_purpose: Default::default(),
            def,
        }
    }
}

impl UnionBuilder {
    pub fn build(
        self,
        generator: &mut SerdeGenerator,
        mut map_operator_fn: impl FnMut(
            &mut SerdeGenerator,
            SerdeOperatorAddr,
            DefId,
        ) -> SerdeOperatorAddr,
    ) -> Result<Vec<ValueUnionVariant>, String> {
        // sanity check
        let mut ambiguous_discriminant_debug: BTreeMap<Discriminant, usize> = Default::default();

        let mut variant_candidates: Vec<_> = self
            .variant_candidates_by_purpose
            .into_iter()
            .flat_map(|(_, variants)| variants)
            .collect();

        for candidate in &mut variant_candidates {
            let result_type = candidate.discriminator.serde_def.def_id;

            candidate.addr = map_operator_fn(generator, candidate.addr, result_type);

            match &mut candidate.discriminator.discriminant {
                Discriminant::StructFallback => {
                    panic!("MapFallback should have been filtered already");
                }
                Discriminant::IsSingletonProperty(relationship_id, prop) => {
                    // TODO: We don't know that we have to do any disambiguation here
                    // (there might be only one singleton property)
                    let operator = generator.gen_operator(SerdeKey::Def(SerdeDef::new(
                        result_type,
                        self.def.modifier.cross_def_flags(),
                    )));

                    if let Some(SerdeOperator::CapturingTextPattern(def_id)) = operator {
                        // convert this
                        candidate.discriminator.discriminant =
                            Discriminant::HasAttributeMatchingTextPattern(
                                *relationship_id,
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
        discriminator: &VariantDiscriminator,
        generator: &mut SerdeGenerator,
    ) -> Result<(), String> {
        let addr = match generator.gen_addr(SerdeKey::Def(
            self.def.with_def(discriminator.serde_def.def_id),
        )) {
            Some(addr) => addr,
            None => return Ok(()),
        };

        // Push with empty scope ('root scope')
        self.push_discriminator(discriminator, addr, &[], generator)
    }

    fn push_discriminator(
        &mut self,
        discriminator: &VariantDiscriminator,
        addr: SerdeOperatorAddr,
        scope: &[&VariantDiscriminator],
        generator: &SerdeGenerator,
    ) -> Result<(), String> {
        let operator = &generator.operators_by_addr[addr.0 as usize];
        match operator {
            SerdeOperator::Union(union_op) => {
                for variant in union_op.unfiltered_variants() {
                    let mut child_scope: Vec<&VariantDiscriminator> = vec![];
                    child_scope.extend(scope.iter());
                    child_scope.push(discriminator);

                    self.push_discriminator(
                        &variant.discriminator,
                        variant.addr,
                        &child_scope,
                        generator,
                    )?;
                }
                Ok(())
            }
            _other => {
                // debug!("PUSH DISCR scope={scope:#?} discriminator={discriminator:#?} {other:?}");
                match discriminator.discriminant {
                    Discriminant::StructFallback => {
                        if let Some(scoping) = scope.last() {
                            self.variant_candidates_by_purpose
                                .entry(scoping.purpose)
                                .or_default()
                                .push(ValueUnionVariant {
                                    discriminator: (*scoping).clone(),
                                    addr,
                                });

                            Ok(())
                        } else {
                            Err(smart_format!("StructFallback without scoping"))
                        }
                    }
                    _ => {
                        self.variant_candidates_by_purpose
                            .entry(discriminator.purpose)
                            .or_default()
                            .push(ValueUnionVariant {
                                discriminator: discriminator.clone(),
                                addr,
                            });
                        Ok(())
                    }
                }
            }
        }
    }
}
