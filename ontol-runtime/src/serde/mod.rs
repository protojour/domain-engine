use std::{
    fmt::{Debug, Display},
    ops::Range,
};

use indexmap::IndexMap;
use smallvec::SmallVec;
use smartstring::alias::String;

use crate::{
    discriminator::VariantDiscriminator,
    env::Env,
    format_utils::{Backticks, CommaSeparated, DoubleQuote},
    value::PropertyId,
    DefId,
};

mod deserialize;
mod deserialize_matcher;
mod serialize;

const EDGE_PROPERTY: &str = "_edge";

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
    pub(crate) edge_operator_id: Option<SerdeOperatorId>,

    /// The environment, via which new SerdeOperators can be created.
    pub(crate) env: &'e Env,
}

/// SerdeOperatorId is an index into a vector of SerdeOperators.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct SerdeOperatorId(pub u32);

#[derive(Debug)]
pub enum SerdeOperator {
    Unit,
    Int(DefId),
    Number(DefId),
    String(DefId),
    StringConstant(String, DefId),
    FiniteTuple(SmallVec<[SerdeOperatorId; 3]>, DefId),
    InfiniteTuple(SmallVec<[SerdeOperatorId; 3]>, DefId),
    Array(DefId, SerdeOperatorId),
    RangeArray(DefId, Range<Option<u16>>, SerdeOperatorId),
    // A type with just one anonymous property
    ValueType(ValueType),
    // A type with multiple anonymous properties, equivalent to a union of types
    ValueUnionType(ValueUnionType),
    // A type with many properties
    MapType(MapType),
}

#[derive(Debug)]
pub struct ValueType {
    pub typename: String,
    pub type_def_id: DefId,
    pub inner_operator_id: SerdeOperatorId,
}

#[derive(Debug)]
pub struct ValueUnionType {
    pub typename: String,
    pub discriminators: Vec<ValueUnionDiscriminator>,
}

#[derive(Debug)]
pub struct ValueUnionDiscriminator {
    pub discriminator: VariantDiscriminator,
    pub operator_id: SerdeOperatorId,
}

#[derive(Debug)]
pub struct MapType {
    pub typename: String,
    pub type_def_id: DefId,
    pub properties: IndexMap<String, SerdeProperty>,
    pub n_mandatory_properties: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct SerdeProperty {
    pub property_id: PropertyId,
    pub value_operator_id: SerdeOperatorId,
    pub optional: bool,
    pub edge_operator_id: Option<SerdeOperatorId>,
}

impl<'e> Debug for SerdeProcessor<'e> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // This structure might contain cycles (through operator id),
        // so just print the topmost level.
        f.debug_struct("SerdeProcessor")
            .field("operator", self.value_operator)
            .field("edge_operator_id", &self.edge_operator_id)
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
            SerdeOperator::FiniteTuple(ids, _) => {
                let processors = ids
                    .iter()
                    .map(|id| self.env.new_serde_processor(*id))
                    .collect::<Vec<_>>();
                write!(f, "[{}]", CommaSeparated(&processors))
            }
            SerdeOperator::InfiniteTuple(ids, _) => {
                let processors = ids
                    .iter()
                    .map(|id| self.env.new_serde_processor(*id))
                    .collect::<Vec<_>>();
                write!(f, "[{}..]", CommaSeparated(&processors))
            }
            SerdeOperator::Array(_, element_operator_id) => {
                let inner_processor = self.env.new_serde_processor(*element_operator_id);
                write!(f, "{inner_processor}[]")
            }
            SerdeOperator::RangeArray(_, range, element_operator_id) => {
                let inner_processor = self.env.new_serde_processor(*element_operator_id);
                write!(f, "{inner_processor}[")?;
                if let Some(start) = range.start {
                    write!(f, "{start}")?;
                }
                write!(f, "..")?;
                if let Some(end) = range.end {
                    write!(f, "{end}")?;
                }

                Ok(())
            }
            SerdeOperator::ValueType(value_type) => Backticks(&value_type.typename).fmt(f),
            SerdeOperator::ValueUnionType(_) => write!(f, "union"),
            SerdeOperator::MapType(map_type) => Backticks(&map_type.typename).fmt(f),
        }
    }
}
