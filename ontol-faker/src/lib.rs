#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use fake::{Fake, Faker};
use ontol_runtime::{
    interface::serde::{
        operator::{FilteredVariants, SerdeOperator},
        processor::{ProcessorLevel, ProcessorMode, SerdeProcessor},
    },
    ontology::Ontology,
    sequence::Sequence,
    text_like_types::TextLikeType,
    text_pattern::TextPattern,
    value::{Attribute, Data, Value},
    DefId,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use regex_generate::Generator;
use tracing::debug;
use uuid::Uuid;

const MAX_REPEAT: u32 = 128;

const SENSIBLE_RECURSION_LEVEL: u16 = 5;

#[derive(Debug)]
pub enum Error {
    NoSerializationInfo,
    RecursionLimitExceeded,
}

impl From<ontol_runtime::interface::serde::processor::RecursionLimitError> for Error {
    fn from(_: ontol_runtime::interface::serde::processor::RecursionLimitError) -> Self {
        Self::RecursionLimitExceeded
    }
}

pub fn new_constant_fake(
    ontology: &Ontology,
    def_id: DefId,
    processor_mode: ProcessorMode,
) -> Result<Value, Error> {
    let seed = [
        1, 0, 0, 0, 23, 0, 0, 0, 200, 1, 0, 0, 210, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ];
    let mut rng = StdRng::from_seed(seed);
    FakeGenerator {
        ontology,
        rng: &mut rng,
        processor_mode,
    }
    .fake_value(def_id)
}

struct FakeGenerator<'a, R: Rng> {
    ontology: &'a Ontology,
    rng: &'a mut R,
    processor_mode: ProcessorMode,
}

impl<'a, R: Rng> FakeGenerator<'a, R> {
    pub fn fake_value(&mut self, def_id: DefId) -> Result<Value, Error> {
        let type_info = self.ontology.get_type_info(def_id);
        let addr = type_info.operator_addr.ok_or(Error::NoSerializationInfo)?;

        Ok(self
            .fake_attribute(
                self.ontology
                    .new_serde_processor(addr, self.processor_mode)
                    .with_level(ProcessorLevel::new_root_with_recursion_limit(32)),
            )?
            .value)
    }

    fn fake_attribute(&mut self, processor: SerdeProcessor) -> Result<Attribute, Error> {
        debug!("fake attribute {processor:?}");
        let value = match processor.value_operator {
            SerdeOperator::Unit => Value::unit(),
            SerdeOperator::False(def_id) => Value::new(Data::I64(0), *def_id),
            SerdeOperator::True(def_id) => Value::new(Data::I64(1), *def_id),
            SerdeOperator::Boolean(def_id) => {
                let value: bool = Faker.fake_with_rng(self.rng);
                Value::new(Data::I64(if value { 1 } else { 0 }), *def_id)
            }
            SerdeOperator::I64(def_id, range) => {
                let int: i64 = if let Some(range) = range {
                    self.rng.gen_range(range.clone())
                } else {
                    // limit range
                    let int: i32 = self.rng.gen();
                    int.into()
                };
                Value::new(Data::I64(int), *def_id)
            }
            SerdeOperator::I32(def_id, range) => {
                let int: i32 = if let Some(range) = range {
                    self.rng.gen_range(range.clone())
                } else {
                    self.rng.gen()
                };
                Value::new(Data::I64(int as i64), *def_id)
            }
            SerdeOperator::F64(def_id, range) => {
                let float: f64 = if let Some(range) = range {
                    self.rng.gen_range(range.clone())
                } else {
                    self.rng.gen()
                };
                Value::new(
                    Data::F64(if float == 0.0 || float == -0.0 {
                        0.1
                    } else {
                        float
                    }),
                    *def_id,
                )
            }
            SerdeOperator::String(def_id) => {
                let mut string: std::string::String =
                    fake::faker::lorem::en::Sentence(3..6).fake_with_rng(self.rng);
                // Remove the last dot
                string.pop();
                Value::new(Data::Text(string.into()), *def_id)
            }
            SerdeOperator::StringConstant(s, def_id) => Value::new(Data::Text(s.clone()), *def_id),
            SerdeOperator::TextPattern(def_id) => {
                if let Some(string_like_type) = self.ontology.get_text_like_type(*def_id) {
                    match string_like_type {
                        TextLikeType::Uuid => {
                            let uuid: Uuid = Faker.fake_with_rng(self.rng);
                            Value::new(
                                Data::OctetSequence(uuid.as_bytes().iter().cloned().collect()),
                                *def_id,
                            )
                        }
                        TextLikeType::DateTime => {
                            Value::new(Data::ChronoDateTime(Faker.fake_with_rng(self.rng)), *def_id)
                        }
                    }
                } else {
                    let text_pattern = self.ontology.get_text_pattern(*def_id).unwrap();
                    let text = rand_text_matching_pattern(text_pattern, &mut self.rng);
                    Value::new(Data::Text(text.into()), *def_id)
                }
            }
            SerdeOperator::CapturingTextPattern(def_id) => {
                let text_pattern = self.ontology.get_text_pattern(*def_id).unwrap();
                let text = rand_text_matching_pattern(text_pattern, &mut self.rng);
                let data = text_pattern
                    .try_capturing_match(&text, self.ontology)
                    .unwrap();

                Value::new(data, *def_id)
            }
            SerdeOperator::DynamicSequence => {
                return Ok(Value::new(Data::Sequence(Sequence::new([])), DefId::unit()).into());
            }
            SerdeOperator::RelationSequence(seq_op) => {
                return if processor.level().current_level() > SENSIBLE_RECURSION_LEVEL {
                    Ok(Value::new(Data::Sequence(Sequence::new([])), seq_op.def.def_id).into())
                } else {
                    let variant = &seq_op.ranges[0];
                    let attr = self.fake_attribute(processor.narrow(variant.addr))?;

                    Ok(Value::new(Data::Sequence(Sequence::new([attr])), seq_op.def.def_id).into())
                }
            }
            SerdeOperator::ConstructorSequence(seq_op) => {
                let mut attrs = vec![];

                for range in &seq_op.ranges {
                    if let Some(rep) = range.finite_repetition {
                        for _ in 0..(rep as usize) {
                            attrs.push(self.fake_attribute(processor.new_child(range.addr)?)?);
                        }
                    } else {
                        attrs.push(self.fake_attribute(processor.new_child(range.addr)?)?);
                    }
                }

                Value::new(Data::Sequence(Sequence::new(attrs)), seq_op.def.def_id)
            }
            SerdeOperator::Alias(alias_op) => {
                return self.fake_attribute(processor.narrow(alias_op.inner_addr));
            }
            SerdeOperator::Union(union_op) => {
                return match union_op.variants(self.processor_mode, processor.level()) {
                    FilteredVariants::Single(id) => self.fake_attribute(processor.narrow(id)),
                    FilteredVariants::Union(variants) => {
                        let index: usize = self.rng.gen_range(0..variants.len());
                        let variant = &variants[index];

                        self.fake_attribute(processor.narrow(variant.addr))
                    }
                }
            }
            SerdeOperator::Struct(struct_op) => {
                let mut attrs = BTreeMap::default();
                for (_, property) in &struct_op.properties {
                    let attr = self.fake_attribute(processor.new_child(property.value_addr)?)?;
                    attrs.insert(property.property_id, attr);
                }

                Value::new(Data::Struct(attrs), struct_op.def.def_id)
            }
            SerdeOperator::IdSingletonStruct(_name, inner_addr) => {
                return self.fake_attribute(processor.narrow(*inner_addr))
            }
        };

        let rel_params = if let Some(rel_params_addr) = processor.ctx.rel_params_addr {
            self.fake_attribute(processor.new_child(rel_params_addr)?)?
                .value
        } else {
            Value::unit()
        };

        Ok(Attribute { value, rel_params })
    }
}

fn rand_text_matching_pattern(text_pattern: &TextPattern, rng: &mut impl Rng) -> String {
    let mut gen = Generator::new(text_pattern.regex.as_str(), rng, MAX_REPEAT).unwrap();
    let mut bytes = vec![];
    gen.generate(&mut bytes).unwrap();
    String::from_utf8(bytes).unwrap()
}
