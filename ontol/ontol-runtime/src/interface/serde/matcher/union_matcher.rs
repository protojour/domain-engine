use std::slice;

use crate::{
    format_utils::{Backticks, LogicOp, Missing},
    interface::{
        discriminator::{Discriminant, LeafDiscriminant},
        serde::{
            operator::{PossibleVariants, SerdeOperator},
            processor::{ProcessorLevel, ProcessorMode, ProcessorProfile, SubProcessorContext},
        },
    },
    ontology::{ontol::TextConstant, Ontology},
    value::Value,
    DefId,
};

use super::{
    map_matchers::MapMatcher,
    primitive_matchers::u64_to_i64,
    sequence_matcher::{SequenceKind, SequenceRangesMatcher},
    text_matchers::try_deserialize_custom_string,
    ValueMatcher,
};

pub struct UnionMatcher<'on, 'p> {
    pub typename: TextConstant,
    pub possible_variants: PossibleVariants<'on>,
    pub ctx: SubProcessorContext,
    pub ontology: &'on Ontology,
    pub profile: &'p ProcessorProfile<'p>,
    pub mode: ProcessorMode,
    pub level: ProcessorLevel,
}

impl<'on, 'p> ValueMatcher for UnionMatcher<'on, 'p> {
    /// FIXME: This should probably talk more in "serde terms" than "ontol terms"?
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} ({})",
            Backticks(&self.ontology[self.typename]),
            Missing {
                items: self
                    .possible_variants
                    .into_iter()
                    .map(|discriminator| self
                        .ontology
                        .new_serde_processor(discriminator.addr, self.mode))
                    .collect(),
                logic_op: LogicOp::Or,
            }
        )
    }

    fn match_unit(&self) -> Result<Value, ()> {
        let def_id = self.match_leaf_discriminant(LeafDiscriminant::IsUnit)?;
        Ok(Value::Unit(def_id.try_into().map_err(|_| ())?))
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        u64_to_i64(value).and_then(|value| self.match_i64(value))
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        for variant in self.possible_variants {
            let Discriminant::MatchesLeaf(leaf_discriminant) = &variant.discriminant else {
                continue;
            };

            match leaf_discriminant {
                LeafDiscriminant::IsIntLiteral(literal) if value == *literal => {}
                LeafDiscriminant::IsInt => {}
                _ => continue,
            }
            return Ok(Value::I64(
                value,
                variant.serde_def.def_id.try_into().map_err(|_| ())?,
            ));
        }
        Err(())
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        for variant in self.possible_variants {
            let Discriminant::MatchesLeaf(scalar_discriminant) = &variant.discriminant else {
                continue;
            };

            match scalar_discriminant {
                LeafDiscriminant::IsText => {
                    return try_deserialize_custom_string(
                        self.ontology,
                        variant.serde_def.def_id,
                        str,
                    )
                    .map_err(|_| ())
                }
                LeafDiscriminant::IsTextLiteral(constant) => {
                    if str == &self.ontology[*constant] {
                        return try_deserialize_custom_string(
                            self.ontology,
                            variant.serde_def.def_id,
                            str,
                        )
                        .map_err(|_| ());
                    }
                }
                LeafDiscriminant::MatchesCapturingTextPattern(def_id) => {
                    let result_type = variant.serde_def.def_id;
                    let pattern = self.ontology.data.text_patterns.get(def_id).unwrap();

                    if let Ok(value) = pattern.try_capturing_match(str, result_type, self.ontology)
                    {
                        return Ok(value);
                    }
                }
                _ => {}
            }
        }

        Err(())
    }

    fn match_sequence(&self) -> Result<SequenceRangesMatcher, ()> {
        for variant in self.possible_variants {
            let Discriminant::MatchesLeaf(leaf_discriminant) = &variant.discriminant else {
                continue;
            };

            if leaf_discriminant == &LeafDiscriminant::IsSequence {
                match &self.ontology[variant.addr] {
                    SerdeOperator::RelationList(seq_op) => {
                        return Ok(SequenceRangesMatcher::new(
                            slice::from_ref(&seq_op.range),
                            SequenceKind::AttrMatrixList,
                            seq_op.def.def_id,
                            self.ctx,
                        ))
                    }
                    SerdeOperator::ConstructorSequence(seq_op) => {
                        return Ok(SequenceRangesMatcher::new(
                            &seq_op.ranges,
                            SequenceKind::ValueList,
                            seq_op.def.def_id,
                            self.ctx,
                        ))
                    }
                    _ => panic!("not a sequence"),
                }
            }
        }

        Err(())
    }

    fn match_map(&self) -> Result<MapMatcher<'on, 'p>, ()> {
        if !self.possible_variants.into_iter().any(|variant| {
            matches!(
                &variant.discriminant,
                Discriminant::StructFallback | Discriminant::HasAttribute(..)
            )
        }) {
            // None of the discriminators are matching a map.
            return Err(());
        }

        Ok(MapMatcher {
            possible_variants: self.possible_variants,
            ctx: self.ctx,
            ontology: self.ontology,
            profile: self.profile,
            mode: self.mode,
            level: self.level,
        })
    }
}

impl<'on, 'p> UnionMatcher<'on, 'p> {
    fn match_leaf_discriminant(&self, discriminant: LeafDiscriminant) -> Result<DefId, ()> {
        for variant in self.possible_variants {
            let Discriminant::MatchesLeaf(leaf_discriminant) = &variant.discriminant else {
                continue;
            };

            if leaf_discriminant == &discriminant {
                return Ok(variant.serde_def.def_id);
            }
        }

        Err(())
    }
}
