use juniper::graphql_value;
use ontol_runtime::{
    env::Env,
    serde::{SerdeOperator, SerdeOperatorId},
};
use serde::de::DeserializeSeed;
use tracing::debug;

use crate::{
    adapter::DomainAdapter,
    gql_scalar::GqlScalar,
    input_value_deserializer::InputValueDeserializer,
    templates::map_input_value::{MapInputValue, MapInputValueTypeInfo},
};

pub fn register_domain_argument<'r>(
    name: &str,
    operator_id: SerdeOperatorId,
    domain_adapter: &DomainAdapter,
    registry: &mut juniper::Registry<'r, GqlScalar>,
) -> juniper::meta::Argument<'r, GqlScalar> {
    let operator = domain_adapter
        .domain_data
        .env
        .get_serde_operator(operator_id);

    debug!("register argument {name} {operator:?}");

    match operator {
        SerdeOperator::Unit => {
            panic!()
        }
        SerdeOperator::Int(_) => registry.arg::<i32>(name, &()),
        SerdeOperator::Number(_) => registry.arg::<f64>(name, &()),
        SerdeOperator::String(_) => registry.arg::<String>(name, &()),
        SerdeOperator::StringConstant(_, _) => registry.arg::<String>(name, &()),
        SerdeOperator::StringPattern(_) => registry.arg::<String>(name, &()),
        SerdeOperator::CapturingStringPattern(_) => registry.arg::<String>(name, &()),
        SerdeOperator::Sequence(_) => {
            todo!()
            // registry.arg::<CustomScalar>(name, &()),
        }
        SerdeOperator::ValueType(value_type) => {
            register_domain_argument(name, value_type.inner_operator_id, domain_adapter, registry)
        }
        SerdeOperator::ValueUnionType(_) => {
            todo!()
        }
        SerdeOperator::Id(_) => {
            todo!()
        }
        SerdeOperator::MapType(_) => registry.arg::<MapInputValue>(
            name,
            &MapInputValueTypeInfo(domain_adapter.node_adapter(operator_id)),
        ),
    }
}

pub fn deserialize_argument(
    arguments: &juniper::Arguments<GqlScalar>,
    name: &str,
    operator_id: SerdeOperatorId,
    env: &Env,
) -> Result<ontol_runtime::value::Attribute, juniper::FieldError<GqlScalar>> {
    let input_value = arguments.get::<MapInputValue>(name).unwrap();

    debug!("deserializing {:?}", input_value.input_value);

    env.new_serde_processor(operator_id, None)
        .deserialize(InputValueDeserializer::<serde_json::Error> {
            value: input_value.input_value,
            error: std::marker::PhantomData,
        })
        .map_err(|json_error| juniper::FieldError::new(json_error, graphql_value!(None)))
}
