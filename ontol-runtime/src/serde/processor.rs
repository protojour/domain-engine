use std::fmt::{Debug, Display};

use crate::{
    env::Env,
    format_utils::{Backticks, CommaSeparated, DoubleQuote},
    value::PropertyId,
};

use super::operator::{SerdeOperator, SerdeOperatorId};

#[derive(Copy, Clone, Debug)]
pub enum ProcessorLevel {
    Root,
    Child,
}

#[derive(Copy, Clone, Debug)]
pub enum ProcessorMode {
    Select,
    Create,
    Update,
}

/// SerdeProcessor is handle serializing and deserializing domain types in an optimized way.
/// Each serde-enabled type has its own operator, which is cached
/// in the compilerironment.
#[derive(Clone, Copy)]
pub struct SerdeProcessor<'e> {
    /// The operator used for (de)serializing this value
    pub(crate) value_operator: &'e SerdeOperator,

    /// The edge properties used for (de)serializing the _edge data_
    /// related to this value when it's associated with a "parent value" through a relation.
    ///
    /// Generally, non-unit edge data can only be represented on a relation between two map types.
    /// The parent (often the subject) map has an attribute that is another child map.
    /// The edge data would be injected in the child map as the `_edge` property.
    pub(crate) rel_params_operator_id: Option<SerdeOperatorId>,

    pub(crate) level: ProcessorLevel,

    /// The environment, via which new SerdeOperators can be created.
    pub(crate) env: &'e Env,
}

impl<'e> SerdeProcessor<'e> {
    /// Return a processor that helps to _narrow the value_ that this processor represents.
    pub fn narrow(&self, operator_id: SerdeOperatorId) -> Self {
        self.env
            .new_serde_processor(operator_id, self.rel_params_operator_id, self.level)
    }

    pub fn narrow_with_rel(
        &self,
        operator_id: SerdeOperatorId,
        rel_params_operator_id: Option<SerdeOperatorId>,
    ) -> Self {
        self.env
            .new_serde_processor(operator_id, rel_params_operator_id, self.level)
    }

    /// Return a processor that processes a new value that is a child of value of this processor.
    pub fn new_child(&self, operator_id: SerdeOperatorId) -> Self {
        self.env
            .new_serde_processor(operator_id, None, ProcessorLevel::Child)
    }

    pub fn new_child_with_rel(
        &self,
        operator_id: SerdeOperatorId,
        rel_params_operator_id: Option<SerdeOperatorId>,
    ) -> Self {
        self.env
            .new_serde_processor(operator_id, rel_params_operator_id, ProcessorLevel::Child)
    }

    pub fn find_property(&self, prop: &str) -> Option<PropertyId> {
        self.search_property(prop, self.value_operator)
    }

    fn search_property(&self, prop: &str, operator: &'e SerdeOperator) -> Option<PropertyId> {
        match operator {
            SerdeOperator::ValueUnionType(union_type) => {
                for discriminator in union_type.variants() {
                    let next_operator = self.narrow(discriminator.operator_id);
                    if let Some(property_id) =
                        self.search_property(prop, next_operator.value_operator)
                    {
                        return Some(property_id);
                    }
                }

                None
            }
            SerdeOperator::MapType(map_type) => map_type
                .properties
                .get(prop)
                .map(|serde_property| serde_property.property_id),
            _ => None,
        }
    }
}

impl<'e> Debug for SerdeProcessor<'e> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // This structure might contain cycles (through operator id),
        // so just print the topmost level.
        f.debug_struct("SerdeProcessor")
            .field("operator", self.value_operator)
            .field("rel_params_operator_id", &self.rel_params_operator_id)
            .finish()
    }
}

impl<'e> Display for SerdeProcessor<'e> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.value_operator {
            SerdeOperator::Unit => write!(f, "unit"),
            SerdeOperator::Int(_) => write!(f, "`int`"),
            SerdeOperator::Number(_) => write!(f, "`number`"),
            SerdeOperator::String(_) => write!(f, "`string`"),
            SerdeOperator::StringConstant(lit, _) => DoubleQuote(lit).fmt(f),
            SerdeOperator::StringPattern(_) | SerdeOperator::CapturingStringPattern(_) => {
                write!(f, "`string_pattern`")
            }
            SerdeOperator::RelationSequence(sequence_type) => {
                write!(
                    f,
                    "[{}..]",
                    self.narrow(sequence_type.ranges[0].operator_id)
                )
            }
            SerdeOperator::ConstructorSequence(sequence_type) => {
                let mut processors = vec![];
                let mut finite = true;
                for range in &sequence_type.ranges {
                    if let Some(finite_repetition) = range.finite_repetition {
                        for _ in 0..finite_repetition {
                            processors.push(self.narrow(range.operator_id));
                        }
                        finite = true;
                    } else {
                        finite = false;
                        processors.push(self.narrow(range.operator_id));
                    }
                }

                let dot_dot = if finite { "" } else { ".." };

                write!(
                    f,
                    "[{processors}{dot_dot}]",
                    processors = CommaSeparated(&processors)
                )
            }
            SerdeOperator::ValueType(value_type) => Backticks(&value_type.typename).fmt(f),
            SerdeOperator::ValueUnionType(_) => write!(f, "union"),
            SerdeOperator::Id(_) => write!(f, "id"),
            SerdeOperator::MapType(map_type) => Backticks(&map_type.typename).fmt(f),
        }
    }
}
