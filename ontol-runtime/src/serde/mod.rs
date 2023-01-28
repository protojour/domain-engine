use std::fmt::Debug;

use indexmap::IndexMap;
use smartstring::alias::String;

use crate::PropertyId;

mod deserialize;
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
    Number,
    String,
    // A type with just one anonymous property
    ValueType(ValueType),
    // A type with many properties
    MapType(MapType),
}

#[derive(Debug)]
pub struct ValueType {
    pub typename: String,
    pub property: SerdeProperty,
}

#[derive(Debug)]
pub struct MapType {
    pub typename: String,
    pub properties: IndexMap<String, SerdeProperty>,
}

#[derive(Clone, Copy, Debug)]
pub struct SerdeProperty {
    pub property_id: PropertyId,
    pub operator_id: SerdeOperatorId,
}
