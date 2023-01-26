use serde::ser::SerializeMap;

use crate::{binding::Bindings, Value};

use super::{MapType, SerdeOperator, SerdeOperatorKind, SerializeValue};

type Res<S> = Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>;

impl<'e, 'm> SerializeValue for SerdeOperator<'e, 'm> {
    fn serialize_value<S: serde::Serializer>(&self, value: &Value, serializer: S) -> Res<S> {
        match self.kind {
            SerdeOperatorKind::Number => self.serialize_number(value, serializer),
            SerdeOperatorKind::String => self.serialize_string(value, serializer),
            SerdeOperatorKind::ValueType(value_type) => SerdeOperator {
                kind: value_type.property.kind,
                bindings: self.bindings,
            }
            .serialize_value(value, serializer),
            SerdeOperatorKind::MapType(map_type) => self.serialize_map(map_type, value, serializer),
            SerdeOperatorKind::Recursive(def_id) => {
                let kind = match self.bindings.serde_operator_kinds.get(&def_id) {
                    Some(Some(kind)) => kind,
                    _ => panic!("Could not resolve recursive serde operator"),
                };
                SerdeOperator {
                    kind,
                    bindings: self.bindings,
                }
                .serialize_value(value, serializer)
            }
        }
    }
}

impl<'e, 'm> SerdeOperator<'e, 'm> {
    fn serialize_number<S: serde::Serializer>(&self, value: &Value, serializer: S) -> Res<S> {
        match value {
            Value::Number(num) => serializer.serialize_i64(*num),
            other => panic!("BUG: Serialize expected number, got {other:?}"),
        }
    }

    fn serialize_string<S: serde::Serializer>(&self, value: &Value, serializer: S) -> Res<S> {
        match value {
            Value::String(string) => serializer.serialize_str(string),
            other => panic!("BUG: Serialize expected string, got {other:?}"),
        }
    }

    fn serialize_map<S: serde::Serializer>(
        &self,
        map_type: &MapType,
        value: &Value,
        serializer: S,
    ) -> Res<S> {
        let attributes = match value {
            Value::Compound(attributes) => attributes,
            other => panic!("BUG: Serialize expected compound attributes, got {other:?}"),
        };

        let mut serialize_map = serializer.serialize_map(Some(attributes.len()))?;

        for (name, serde_prop) in &map_type.properties {
            let value = match attributes.get(&serde_prop.property_id) {
                Some(value) => value,
                None => panic!(
                    "While serializing `{}`, property `{}` was not found",
                    map_type.typename, name
                ),
            };

            serialize_map.serialize_entry(
                name,
                &Proxy {
                    value,
                    kind: serde_prop.kind,
                    bindings: self.bindings,
                },
            )?;
        }
        serialize_map.end()
    }
}

struct Proxy<'v, 'e, 'm> {
    value: &'v Value,
    kind: &'m SerdeOperatorKind<'m>,
    bindings: &'e Bindings<'m>,
}

impl<'v, 'e, 'm> serde::Serialize for Proxy<'v, 'e, 'm> {
    fn serialize<S>(&self, serializer: S) -> Res<S>
    where
        S: serde::Serializer,
    {
        SerdeOperator {
            kind: self.kind,
            bindings: self.bindings,
        }
        .serialize_value(self.value, serializer)
    }
}
