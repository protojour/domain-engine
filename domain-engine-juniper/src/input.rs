use ontol_runtime::serde::{SerdeOperator, SerdeOperatorId};
use tracing::debug;

use crate::{
    adapter::DomainAdapter,
    gql_scalar::GqlScalar,
    templates::map_input_value::{MapInputValue, MapInputValueTypeInfo},
};

pub fn register_domain_argument<'r>(
    name: &str,
    operator_id: SerdeOperatorId,
    domain_adapter: &DomainAdapter,
    registry: &mut juniper::Registry<'r, GqlScalar>,
) -> juniper::meta::Argument<'r, GqlScalar> {
    debug!("register argument {name}");

    match domain_adapter
        .domain_data
        .env
        .get_serde_operator(operator_id)
    {
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
