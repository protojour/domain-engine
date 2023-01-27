use indexmap::IndexMap;
use smartstring::alias::String;

use crate::{binding::Bindings, def::DefId, relation::PropertyId, Value};

mod deserialize;
mod serialize;

/// SerdeOperator is handle serializing and deserializing domain types in an optimized way.
/// Each serde-enabled type has its own operator, which is cached
/// in the compilerironment.
#[derive(Clone, Copy, Debug)]
pub struct SerdeOperator<'e, 'm> {
    pub(crate) kind: &'m SerdeOperatorKind<'m>,
    pub(crate) bindings: &'e Bindings<'m>,
}

pub trait SerializeValue {
    fn serialize_value<S: serde::Serializer>(
        &self,
        value: &Value,
        serializer: S,
    ) -> Result<S::Ok, S::Error>;
}

#[derive(Debug)]
pub(crate) enum SerdeOperatorKind<'m> {
    Number,
    String,
    // A type with just one anonymous property
    ValueType(ValueType<'m>),
    // A type with many properties
    MapType(MapType<'m>),
    Recursive(DefId),
}

#[derive(Debug)]
pub(crate) struct ValueType<'m> {
    pub typename: String,
    pub property: SerdeProperty<'m>,
}

#[derive(Debug)]
pub(crate) struct MapType<'m> {
    pub typename: String,
    pub properties: IndexMap<String, SerdeProperty<'m>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SerdeProperty<'m> {
    pub property_id: PropertyId,
    pub kind: &'m SerdeOperatorKind<'m>,
}
