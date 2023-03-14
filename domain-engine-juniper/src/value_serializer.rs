//! Serializes ontol_runtime Value into GraphQL value

use std::collections::HashMap;

use ontol_runtime::{
    serde::operator::{MapOperator, SerdeOperator, SerdeOperatorId},
    value::{Data, Value},
    DefId,
};
use smartstring::alias::String;

use crate::virtual_schema::{
    data::{FieldData, FieldKind, ObjectKind, TypeKind},
    VirtualSchema,
};

#[derive(Debug)]
pub enum SerializedValue {
    Connection(Vec<SerializedValue>),
    Map(DefId, HashMap<String, SerializedAttribute>),
    Scalar(Value, SerdeOperatorId),
}

#[derive(Debug)]
pub struct SerializedAttribute {
    pub value: SerializedValue,
    pub rel_params: Option<SerializedValue>,
}

pub struct ValueSerializer<'e> {
    virtual_schema: &'e VirtualSchema,
}

impl<'e> ValueSerializer<'e> {
    pub fn new(virtual_schema: &'e VirtualSchema) -> Self {
        Self { virtual_schema }
    }

    pub fn find_operator_id(&self, field_data: &FieldData) -> Option<SerdeOperatorId> {
        match self
            .virtual_schema
            .lookup_type_data(field_data.field_type.unit)
        {
            Ok(type_data) => match &type_data.kind {
                TypeKind::Object(object_data) => match &object_data.kind {
                    ObjectKind::Node(node_data) => Some(node_data.operator_id),
                    ObjectKind::Edge(_) => object_data
                        .fields
                        .values()
                        .find(|field_data| matches!(field_data.kind, FieldKind::Node))
                        .and_then(|field_data| self.find_operator_id(field_data)),
                    ObjectKind::Connection => object_data
                        .fields
                        .values()
                        .find(|field_data| matches!(field_data.kind, FieldKind::Edges))
                        .and_then(|field_data| self.find_operator_id(field_data)),
                    _ => panic!("BUG: unhandled object kind"),
                },
                TypeKind::Union(_) => todo!("Union operator id"),
                TypeKind::CustomScalar(_) => panic!("BUG: Scalars not handled here"),
            },
            Err(_) => None,
        }
    }

    pub fn serialize_values(
        &self,
        value: Vec<Value>,
        operator_id: SerdeOperatorId,
    ) -> SerializedValue {
        SerializedValue::Connection(
            value
                .into_iter()
                .map(|v| self.serialize_value(v, operator_id))
                .collect(),
        )
    }

    pub fn serialize_value(&self, value: Value, operator_id: SerdeOperatorId) -> SerializedValue {
        match self.virtual_schema.env().get_serde_operator(operator_id) {
            SerdeOperator::Map(map_op) => self.serialize_map(value, map_op),
            _ => SerializedValue::Scalar(value, operator_id),
        }
    }

    fn serialize_map(&self, value: Value, map_op: &MapOperator) -> SerializedValue {
        let mut attributes = match value.data {
            Data::Map(attributes) => attributes,
            other => panic!("BUG: Serialize expected map attributes, got {other:?}"),
        };

        let mut map = HashMap::new();

        for (name, serde_prop) in &map_op.properties {
            let attribute = match attributes.remove(&serde_prop.property_id) {
                Some(value) => value,
                None => {
                    continue;
                }
            };

            let value = self.serialize_value(attribute.value, serde_prop.value_operator_id);
            let rel_params = serde_prop
                .rel_params_operator_id
                .map(|operator_id| self.serialize_value(attribute.rel_params, operator_id));

            map.insert(name.clone(), SerializedAttribute { value, rel_params });
        }

        SerializedValue::Map(map_op.def_variant.def_id, map)
    }
}
