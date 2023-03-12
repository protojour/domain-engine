use std::fmt::{Debug, Display};

use crate::{
    env::Env,
    format_utils::{Backticks, CommaSeparated, DoubleQuote},
    value::PropertyId,
};

use super::operator::{FilteredVariants, SerdeOperator, SerdeOperatorId};

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

    /// The environment, via which new SerdeOperators can be created.
    pub(crate) env: &'e Env,

    pub(crate) mode: ProcessorMode,
    pub(crate) level: ProcessorLevel,
}

impl<'e> SerdeProcessor<'e> {
    /// Return a processor that helps to _narrow the value_ that this processor represents.
    pub fn narrow(&self, operator_id: SerdeOperatorId) -> Self {
        self.env.new_serde_processor(
            operator_id,
            self.rel_params_operator_id,
            self.mode,
            self.level,
        )
    }

    pub fn narrow_with_rel(
        &self,
        operator_id: SerdeOperatorId,
        rel_params_operator_id: Option<SerdeOperatorId>,
    ) -> Self {
        self.env
            .new_serde_processor(operator_id, rel_params_operator_id, self.mode, self.level)
    }

    /// Return a processor that processes a new value that is a child of value of this processor.
    pub fn new_child(&self, operator_id: SerdeOperatorId) -> Self {
        self.env
            .new_serde_processor(operator_id, None, self.mode, ProcessorLevel::Child)
    }

    pub fn new_child_with_rel(
        &self,
        operator_id: SerdeOperatorId,
        rel_params_operator_id: Option<SerdeOperatorId>,
    ) -> Self {
        self.env.new_serde_processor(
            operator_id,
            rel_params_operator_id,
            self.mode,
            ProcessorLevel::Child,
        )
    }

    pub fn find_property(&self, prop: &str) -> Option<PropertyId> {
        self.search_property(prop, self.value_operator)
    }

    fn search_property(&self, prop: &str, operator: &'e SerdeOperator) -> Option<PropertyId> {
        match operator {
            SerdeOperator::Union(union_op) => match union_op.variants(self.mode, self.level) {
                FilteredVariants::Single(id) => self.narrow(id).search_property(prop, operator),
                FilteredVariants::Multi(variants) => {
                    for variant in variants {
                        let next_operator = self.narrow(variant.operator_id);
                        if let Some(property_id) =
                            self.search_property(prop, next_operator.value_operator)
                        {
                            return Some(property_id);
                        }
                    }

                    None
                }
            },
            SerdeOperator::Map(map_op) => map_op
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
            SerdeOperator::RelationSequence(seq_op) => {
                write!(f, "[{}..]", self.narrow(seq_op.ranges[0].operator_id))
            }
            SerdeOperator::ConstructorSequence(seq_op) => {
                let mut processors = vec![];
                let mut finite = true;
                for range in &seq_op.ranges {
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
            SerdeOperator::ValueType(value_op) => Backticks(&value_op.typename).fmt(f),
            SerdeOperator::Union(_) => write!(f, "union"),
            SerdeOperator::Id(_) => write!(f, "id"),
            SerdeOperator::Map(map_op) => Backticks(&map_op.typename).fmt(f),
        }
    }
}
