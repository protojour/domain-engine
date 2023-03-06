use ontol_runtime::serde::SerdeOperator;

use crate::{
    adapter::{DomainAdapter, NodeKind, TypeAdapter},
    gql_scalar::GqlScalar,
    input::register_domain_argument,
    macros::impl_graphql_value,
    type_info::GraphqlTypeName,
};

pub struct MapInputValue {
    pub input_value: juniper::InputValue<GqlScalar>,
}

pub struct MapInputValueTypeInfo(pub TypeAdapter<NodeKind>);

impl GraphqlTypeName for MapInputValueTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.0.type_data().input_type_name
    }
}

impl_graphql_value!(MapInputValue, TypeInfo = MapInputValueTypeInfo);

impl juniper::GraphQLType<GqlScalar> for MapInputValue {
    fn name(info: &MapInputValueTypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &Self::TypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let type_data = &info.0.data().type_data;
        let domain_adapter = DomainAdapter {
            domain_data: info.0.domain_data.clone(),
        };
        let operator = info
            .0
            .domain_data
            .env
            .get_serde_operator(type_data.operator_id);

        let mut arguments = vec![];

        if let SerdeOperator::MapType(map_type) = operator {
            for (name, property) in &map_type.properties {
                arguments.push(register_domain_argument(
                    name,
                    property.value_operator_id,
                    &domain_adapter,
                    registry,
                ));
            }
        } else {
            panic!()
        }

        registry
            .build_input_object_type::<Self>(info, &arguments)
            .into_meta()
    }
}

// impl juniper::marker::IsInputType<GqlScalar> for InputValue {}
// impl juniper::marker::IsOutputType<GqlScalar> for InputValue {}

impl juniper::ToInputValue<GqlScalar> for MapInputValue {
    fn to_input_value(&self) -> juniper::InputValue<GqlScalar> {
        let v = juniper::Value::scalar(42);
        juniper::ToInputValue::to_input_value(&v)
    }
}

impl juniper::FromInputValue<GqlScalar> for MapInputValue {
    fn from_input_value(value: &juniper::InputValue<GqlScalar>) -> Option<Self> {
        Some(MapInputValue {
            input_value: value.clone(),
        })
    }
}

impl juniper::ParseScalarValue<GqlScalar> for MapInputValue {
    fn from_str(
        _value: juniper::parser::ScalarToken<'_>,
    ) -> juniper::ParseScalarResult<'_, GqlScalar> {
        panic!("TODO: this won't work")
    }
}

/*
fn value_to_json(value: &juniper::InputValue<GqlScalar>) -> Option<serde_json::Value> {
    use serde_json::{Number, Value};
    match value {
        juniper::InputValue::Null => Some(Value::Null),
        juniper::InputValue::Scalar(GqlScalar::I32(value)) => Some(Value::Number((*value).into())),
        juniper::InputValue::Scalar(GqlScalar::F64(value)) => {
            Some(Value::Number(Number::from_f64(*value)?))
        }
        juniper::InputValue::Scalar(GqlScalar::Bool(value)) => Some(Value::Bool(*value)),
        juniper::InputValue::Scalar(GqlScalar::String(value)) => {
            Some(Value::String(value.to_string()))
        }
        juniper::InputValue::Enum(value) => Some(Value::String(value.to_string())),
        juniper::InputValue::Variable(var) => {
            error!("Tried to convert a variable InputValue: {var:?}");
            None
        }
        juniper::InputValue::List(vec) => {
            let elements = vec
                .iter()
                .map(|element| value_to_json(&element.item))
                .collect::<Option<_>>()?;

            Some(Value::Array(elements))
        }
        juniper::InputValue::Object(props) => {
            let elements = props
                .iter()
                .map(|(key, value)| {
                    let value = value_to_json(&value.item)?;

                    Some((key.item.to_string(), value))
                })
                .collect::<Option<_>>()?;

            Some(Value::Object(elements))
        }
    }
}
*/
