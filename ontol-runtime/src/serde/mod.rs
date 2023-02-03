use std::fmt::{Debug, Display};

use indexmap::IndexMap;
use smallvec::SmallVec;
use smartstring::alias::String;

use crate::{
    discriminator::VariantDiscriminator,
    env::Env,
    format_utils::{Backticks, CommaSeparated, DoubleQuote},
    DefId, RelationId,
};

mod deserialize;
mod deserialize_matcher;
mod serialize;

/// SerdeOperator is handle serializing and deserializing domain types in an optimized way.
/// Each serde-enabled type has its own operator, which is cached
/// in the compilerironment.
#[derive(Clone, Copy)]
pub struct SerdeProcessor<'e> {
    pub(crate) current: &'e SerdeOperator,
    pub(crate) env: &'e Env,
}

impl<'e> Debug for SerdeProcessor<'e> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // This structure might contain cycles (through operator id),
        // so just print the topmost level.
        f.debug_struct("SerdeProcessor")
            .field("current", self.current)
            .finish()
    }
}

impl<'e> Display for SerdeProcessor<'e> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.current {
            SerdeOperator::Unit => write!(f, "unit"),
            SerdeOperator::Int(_) => write!(f, "`int`"),
            SerdeOperator::Number(_) => write!(f, "`number`"),
            SerdeOperator::String(_) => write!(f, "`string`"),
            SerdeOperator::StringConstant(lit, _) => DoubleQuote(lit).fmt(f),
            SerdeOperator::Tuple(ids, _) => {
                let processors = ids
                    .iter()
                    .map(|id| self.env.new_serde_processor(*id))
                    .collect::<Vec<_>>();
                write!(f, "[{}]", CommaSeparated(&processors))
            }
            SerdeOperator::Array(_, element_operator_id) => {
                let inner_processor = self.env.new_serde_processor(*element_operator_id);
                write!(f, "{}[]", inner_processor)
            }
            SerdeOperator::ValueType(value_type) => Backticks(&value_type.typename).fmt(f),
            SerdeOperator::ValueUnionType(_) => write!(f, "union"),
            SerdeOperator::MapType(map_type) => Backticks(&map_type.typename).fmt(f),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct SerdeOperatorId(pub u32);

#[derive(Debug)]
pub enum SerdeOperator {
    Unit,
    Int(DefId),
    Number(DefId),
    String(DefId),
    StringConstant(String, DefId),
    Tuple(SmallVec<[SerdeOperatorId; 3]>, DefId),
    Array(DefId, SerdeOperatorId),
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
}

#[derive(Clone, Copy, Debug)]
pub struct SerdeProperty {
    pub relation_id: RelationId,
    pub operator_id: SerdeOperatorId,
}
