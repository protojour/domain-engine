use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{relation::PropertyId, Value};

mod deserialize;
mod serialize;

/// SerdeOperator is handle serializing and deserializing domain types in an optimized way.
/// Each serde-enabled type has its own operator, which is cached
/// in the compilerironment.
#[derive(Clone, Copy, Debug)]
pub struct SerdeProcessor<'e> {
    pub(crate) current: &'e SerdeOperator,
    pub(crate) registry: SerdeRegistry<'e>,
}

pub trait SerializeValue {
    fn serialize_value<S: serde::Serializer>(
        &self,
        value: &Value,
        serializer: S,
    ) -> Result<S::Ok, S::Error>;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SerdeRegistry<'e> {
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
pub struct SerdeOperatorId(pub(crate) u32);

#[derive(Debug)]
pub(crate) enum SerdeOperator {
    Unit,
    Number,
    String,
    // A type with just one anonymous property
    ValueType(ValueType),
    // A type with many properties
    MapType(MapType),
}

#[derive(Debug)]
pub(crate) struct ValueType {
    pub typename: String,
    pub property: SerdeProperty,
}

#[derive(Debug)]
pub(crate) struct MapType {
    pub typename: String,
    pub properties: IndexMap<String, SerdeProperty>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SerdeProperty {
    pub property_id: PropertyId,
    pub operator_id: SerdeOperatorId,
}
