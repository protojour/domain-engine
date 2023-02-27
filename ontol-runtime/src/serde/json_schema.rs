use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

use super::{JsonSchema, SequenceRange, SerdeOperator, SerdeProcessor};

impl<'e> Serialize for JsonSchema<'e> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.processor.value_operator {
            SerdeOperator::Unit => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "object")?;
                map.end()
            }
            // FIXME: Distinguish different number types
            SerdeOperator::Int(_) | SerdeOperator::Number(_) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("type", "integer")?;
                map.serialize_entry("format", "int64")?;
                map.end()
            }
            SerdeOperator::String(_) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "string")?;
                map.end()
            }
            SerdeOperator::StringConstant(literal, _) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "string")?;
                map.serialize_entry("enum", &[literal])?;
                map.end()
            }
            SerdeOperator::StringPattern(def_id)
            | SerdeOperator::CapturingStringPattern(def_id) => {
                let pattern = self.processor.env.string_patterns.get(def_id).unwrap();
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "string")?;
                map.serialize_entry("pattern", pattern.regex.as_str())?;
                map.end()
            }
            SerdeOperator::Sequence(ranges, _) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("type", "array")?;
                match ranges.len() {
                    0 => {
                        map.serialize_entry::<str, [&'static str]>("items", &[])?;
                    }
                    1 => {
                        let range = ranges.iter().next().unwrap();
                        match &range.finite_repetition {
                            Some(_) => map.serialize_entry(
                                "items",
                                &ArrayItemsSchema {
                                    processor: &self.processor,
                                    ranges: ranges.as_slice(),
                                },
                            )?,
                            None => {
                                map.serialize_entry(
                                    "items",
                                    &self
                                        .processor
                                        .env
                                        .new_serde_processor(
                                            range.operator_id,
                                            self.processor.rel_params_operator_id,
                                        )
                                        .schema(),
                                )?;
                            }
                        }
                    }
                    len => {
                        let last_range = ranges.last().unwrap();
                        if last_range.finite_repetition.is_some() {
                            map.serialize_entry(
                                "items",
                                &ArrayItemsSchema {
                                    processor: &self.processor,
                                    ranges: ranges.as_slice(),
                                },
                            )?;
                        } else {
                            map.serialize_entry(
                                "items",
                                &ArrayItemsSchema {
                                    processor: &self.processor,
                                    ranges: &ranges[..len - 1],
                                },
                            )?;
                            map.serialize_entry(
                                "additionalItems",
                                &self
                                    .processor
                                    .env
                                    .new_serde_processor(
                                        last_range.operator_id,
                                        self.processor.rel_params_operator_id,
                                    )
                                    .schema(),
                            )?;
                        }
                    }
                }
                map.end()
            }
            SerdeOperator::ValueType(value_type) => todo!(),
            SerdeOperator::ValueUnionType(value_union_type) => {
                todo!()
            }
            SerdeOperator::Id(inner_operator_id) => {
                todo!()
            }
            SerdeOperator::MapType(map_type) => {
                todo!()
            }
        }
    }
}

struct ArrayItemsSchema<'e> {
    processor: &'e SerdeProcessor<'e>,
    ranges: &'e [SequenceRange],
}

impl<'e> Serialize for ArrayItemsSchema<'e> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        for range in self.ranges {
            if let Some(repetition) = range.finite_repetition {
                for _ in 0..repetition {
                    seq.serialize_element(
                        &self
                            .processor
                            .env
                            .new_serde_processor(
                                range.operator_id,
                                self.processor.rel_params_operator_id,
                            )
                            .schema(),
                    )?;
                }
            }
        }
        seq.end()
    }
}
