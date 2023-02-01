use std::fmt::Debug;

use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{discriminator::VariantDiscriminator, DefId, PropertyId};

mod deserialize;
mod deserialize_union;
mod serialize;

/// SerdeOperator is handle serializing and deserializing domain types in an optimized way.
/// Each serde-enabled type has its own operator, which is cached
/// in the compilerironment.
#[derive(Clone, Copy)]
pub struct SerdeProcessor<'e> {
    current: &'e SerdeOperator,
    registry: SerdeRegistry<'e>,
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

#[derive(Clone, Copy)]
pub struct SerdeRegistry<'e> {
    operators: &'e [SerdeOperator],
}

impl<'e> SerdeRegistry<'e> {
    pub fn new(operators: &'e [SerdeOperator]) -> Self {
        Self { operators }
    }

    pub fn make_processor(self, operator_id: SerdeOperatorId) -> SerdeProcessor<'e> {
        SerdeProcessor {
            current: &self.operators[operator_id.0 as usize],
            registry: self,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct SerdeOperatorId(pub u32);

#[derive(Debug)]
pub enum SerdeOperator {
    Unit,
    Number(DefId),
    String(DefId),
    // A type with just one anonymous property
    ValueType(ValueType),
    // A type with multiple anonymous properties, equivalent to a union of types
    ValueUnionType(ValueUnionType),
    // A type with many properties
    MapType(MapType),
}

impl SerdeOperator {
    fn typename(&self) -> &str {
        match self {
            Self::Unit => "unit",
            Self::Number(_) => "number",
            Self::String(_) => "string",
            Self::ValueType(value_type) => &value_type.typename,
            Self::ValueUnionType(_) => "union",
            Self::MapType(map_type) => &map_type.typename,
        }
    }
}

#[derive(Debug)]
pub struct ValueType {
    pub typename: String,
    pub type_def_id: DefId,
    pub property: SerdeProperty,
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
    pub property_id: PropertyId,
    pub operator_id: SerdeOperatorId,
}
