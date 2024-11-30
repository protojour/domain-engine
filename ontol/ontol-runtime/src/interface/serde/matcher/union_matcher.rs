use std::slice;

use itertools::Itertools;

use crate::{
    format_utils::{Backticks, LogicOp, LogicalConcat},
    interface::{
        discriminator::{Discriminant, LeafDiscriminant},
        serde::{
            operator::{PossibleVariants, SerdeOperator},
            processor::{
                ProcessorLevel, ProcessorMode, ProcessorProfile, SerdeProcessor,
                SubProcessorContext,
            },
            OntologyCtx,
        },
    },
    ontology::ontol::TextConstant,
    value::Value,
    DefId,
};

use super::{
    map_matcher::MapMatcher,
    primitive_matchers::u64_to_i64,
    sequence_matcher::{SequenceKind, SequenceRangesMatcher},
    text_matchers::try_deserialize_custom_string,
    ValueMatcher,
};

pub struct UnionMatcher<'on, 'p> {
    pub typename: TextConstant,
    pub possible_variants: PossibleVariants<'on>,
    pub ctx: SubProcessorContext,
    pub ontology: OntologyCtx<'on>,
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
            Backticks(&self.ontology.defs[self.typename]),
            LogicalConcat {
                items: self
                    .possible_variants
                    .into_iter()
                    .dedup_by(|a, b| { a.deserialize.def_id == b.deserialize.def_id })
                    .map(|variant| SerdeProcessor::new(
                        variant.deserialize.addr,
                        self.mode,
                        &self.ontology
                    ))
                    .collect(),
                logic_op: LogicOp::Or,
            }
        )
    }

    fn match_unit(&self) -> Result<Value, ()> {
        let def_id = self.match_leaf_discriminant(LeafDiscriminant::IsUnit)?;
        Ok(Value::Unit(def_id.into()))
    }

    fn match_u64(&self, value: u64) -> Result<Value, ()> {
        u64_to_i64(value).and_then(|value| self.match_i64(value))
    }

    fn match_i64(&self, value: i64) -> Result<Value, ()> {
        for variant in self.possible_variants {
            let Discriminant::MatchesLeaf(leaf_discriminant) = variant.discriminant() else {
                continue;
            };

            match leaf_discriminant {
                LeafDiscriminant::IsIntLiteral(literal) if value == *literal => {}
                LeafDiscriminant::IsInt => {}
                _ => continue,
            }
            return Ok(Value::I64(value, variant.deserialize.def_id.into()));
        }
        Err(())
    }

    fn match_str(&self, str: &str) -> Result<Value, ()> {
        for variant in self.possible_variants {
            let Discriminant::MatchesLeaf(scalar_discriminant) = variant.discriminant() else {
                continue;
            };

            match scalar_discriminant {
                LeafDiscriminant::IsText => {
                    return try_deserialize_custom_string(
                        str,
                        variant.deserialize.def_id,
                        self.ontology,
                    )
                    .map_err(|_| ())
                }
                LeafDiscriminant::IsTextLiteral(constant) => {
                    if str == &self.ontology.defs[*constant] {
                        return try_deserialize_custom_string(
                            str,
                            variant.deserialize.def_id,
                            self.ontology,
                        )
                        .map_err(|_| ());
                    }
                }
                LeafDiscriminant::MatchesCapturingTextPattern(def_id) => {
                    let result_type = variant.deserialize.def_id;
                    let pattern = self.ontology.defs.text_patterns.get(def_id).unwrap();

                    if let Ok(value) = pattern.try_capturing_match(str, result_type, &self.ontology)
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
            let Discriminant::MatchesLeaf(leaf_discriminant) = variant.discriminant() else {
                continue;
            };

            if leaf_discriminant == &LeafDiscriminant::IsSequence {
                match &self.ontology.serde[variant.deserialize.addr] {
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
        if !self
            .possible_variants
            .into_iter()
            .any(|variant| matches!(variant.discriminant(), Discriminant::HasAttribute(..)))
        {
            // None of the discriminators are matching a map.
            return Err(());
        }

        Ok(MapMatcher::new(
            self.possible_variants,
            self.ontology,
            self.ctx,
            self.profile,
            self.mode,
            self.level,
        ))
    }
}

impl UnionMatcher<'_, '_> {
    fn match_leaf_discriminant(&self, discriminant: LeafDiscriminant) -> Result<DefId, ()> {
        for variant in self.possible_variants {
            let Discriminant::MatchesLeaf(leaf_discriminant) = variant.discriminant() else {
                continue;
            };

            if leaf_discriminant == &discriminant {
                return Ok(variant.deserialize.def_id);
            }
        }

        Err(())
    }
}
