use std::collections::BTreeMap;

use ontol_runtime::{
    debug::OntolDebug,
    interface::discriminator::{Discriminant, VariantDiscriminator, VariantPurpose},
    interface::{
        discriminator::LeafDiscriminant,
        serde::{
            operator::{SerdeOperator, SerdeOperatorAddr, SerdeUnionVariant},
            SerdeDef, SerdeModifier,
        },
    },
};
use tracing::trace;

use super::{
    serde_generator::{operator_to_leaf_discriminant, SerdeGenerator},
    SerdeKey,
};

pub struct UnionBuilder {
    // variants _sorted_ by purpose
    variant_candidates_by_purpose: BTreeMap<VariantPurpose, Vec<SerdeUnionVariant>>,
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
            SerdeDef,
        ) -> SerdeOperatorAddr,
    ) -> Result<Vec<SerdeUnionVariant>, String> {
        // sanity check
        let mut ambiguous_discriminant_debug: BTreeMap<Discriminant, usize> = Default::default();

        let mut variant_candidates: Vec<_> = self
            .variant_candidates_by_purpose
            .into_iter()
            .flat_map(|(_, variants)| variants)
            .collect();

        if variant_candidates.is_empty() {
            panic!("Empty");
        }

        for candidate in &mut variant_candidates {
            let result_def = candidate.discriminator.serde_def;

            candidate.addr = map_operator_fn(generator, candidate.addr, result_def);

            match &mut candidate.discriminator.discriminant {
                Discriminant::StructFallback => {
                    panic!("MapFallback should have been filtered already");
                }
                Discriminant::HasAttribute(.., leaf @ LeafDiscriminant::IsAny) => {
                    // TODO: We don't know that we have to do any disambiguation here
                    // (there might be only one singleton property)
                    if let Some(operator) = generator.gen_operator_lazy(SerdeKey::Def(
                        SerdeDef::new(result_def.def_id, self.def.modifier.cross_def_flags()),
                    )) {
                        *leaf = operator_to_leaf_discriminant(operator);
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
                return Err(format!("Discriminant {discriminant:?} is ambiguous:"));
            }
        }

        Ok(variant_candidates)
    }

    pub fn add_root_discriminator(
        &mut self,
        discriminator: &VariantDiscriminator,
        generator: &mut SerdeGenerator,
    ) -> Result<(), String> {
        let mut discriminator = discriminator.clone();

        let inner_def = match discriminator.purpose {
            VariantPurpose::Identification { entity_id } => {
                let mut inner_def = self.def.with_def(entity_id);
                inner_def.modifier = SerdeModifier::PRIMARY_ID;
                inner_def
            }
            VariantPurpose::Data => {
                let cross_def_flags = self.def.modifier.cross_def_flags();

                discriminator.serde_def.modifier |= SerdeModifier::INHERENT_PROPS;
                discriminator.serde_def.modifier |= cross_def_flags;

                let mut inner_def = self.def.with_def(discriminator.serde_def.def_id);
                inner_def.modifier |= SerdeModifier::INHERENT_PROPS;
                inner_def.modifier |= cross_def_flags;
                inner_def
            }
            VariantPurpose::RawDynamicEntity => unreachable!(),
        };

        let addr = match generator.gen_addr_lazy(SerdeKey::Def(inner_def)) {
            Some(addr) => addr,
            None => {
                panic!();
            }
        };

        // Push with empty scope ('root scope')
        self.push_discriminator(&discriminator, addr, &[], generator)
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
                    trace!(
                        "push UNION variant discriminator for {variant:?} {discriminator:?}",
                        variant = variant.debug(&())
                    );

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
                // info!("PUSH DISCR scope={scope:#?} discriminator={discriminator:#?} {other:?}");
                match discriminator.discriminant {
                    Discriminant::StructFallback => {
                        if let Some(scoping) = scope.last() {
                            self.variant_candidates_by_purpose
                                .entry(scoping.purpose)
                                .or_default()
                                .push(SerdeUnionVariant {
                                    discriminator: (*scoping).clone(),
                                    addr,
                                });

                            Ok(())
                        } else {
                            Err("StructFallback without scoping".to_string())
                        }
                    }
                    _ => {
                        self.variant_candidates_by_purpose
                            .entry(discriminator.purpose)
                            .or_default()
                            .push(SerdeUnionVariant {
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
