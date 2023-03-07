use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use ontol_runtime::serde::{SerdeOperator, SerdeOperatorId};
use tracing::{debug, warn};

use crate::{
    adapter::{data::DomainData, DynamicKind, DynamicRef, TypeAdapter, UnionAdapter},
    gql_scalar::GqlScalar,
    templates::{
        map_input_value::{MapInputValue, MapInputValueTypeInfo},
        node::{Node, NodeTypeInfo},
        union::{Union, UnionTypeInfo},
    },
    GqlContext,
};

/// Juniper registry and domain adapter combined into
/// one type to enable a nice API
pub struct RegistryWrapper<'a, 'r> {
    pub registry: &'a mut juniper::Registry<'r, GqlScalar>,
    pub domain: &'a Arc<DomainData>,
}

impl<'a, 'r> RegistryWrapper<'a, 'r> {
    pub fn new(
        registry: &'a mut juniper::Registry<'r, GqlScalar>,
        domain: &'a Arc<DomainData>,
    ) -> Self {
        Self { domain, registry }
    }

    pub fn register_domain_field(
        &mut self,
        field_name: &str,
        adapter: TypeAdapter<DynamicKind>,
    ) -> juniper::meta::Field<'r, GqlScalar> {
        let domain_data = adapter.domain_data.clone();
        match adapter.data() {
            DynamicRef::Node(node_ref) => self.registry.field_convert::<Node, _, GqlContext>(
                field_name,
                &NodeTypeInfo(TypeAdapter {
                    domain_data,
                    operator_id: node_ref.type_data.operator_id,
                    _kind: std::marker::PhantomData,
                }),
            ),
            DynamicRef::Entity(entity_ref) => self.registry.field_convert::<Node, _, GqlContext>(
                field_name,
                &NodeTypeInfo(TypeAdapter {
                    domain_data,
                    operator_id: entity_ref.type_data.operator_id,
                    _kind: std::marker::PhantomData,
                }),
            ),
            DynamicRef::Union(union_ref) => self.registry.field_convert::<Union, _, GqlContext>(
                field_name,
                &UnionTypeInfo(UnionAdapter {
                    domain_data,
                    operator_id: union_ref.union_data.operator_id,
                }),
            ),
            DynamicRef::Scalar(_) => panic!("Unsupported scalar here"),
        }
    }

    pub fn register_domain_argument(
        &mut self,
        name: &str,
        operator_id: SerdeOperatorId,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        let operator = self.domain.env.get_serde_operator(operator_id);

        debug!("register argument {name} {operator:?}");

        match operator {
            SerdeOperator::Unit => {
                panic!()
            }
            SerdeOperator::Int(_) => self.arg::<i32>(name, &()),
            SerdeOperator::Number(_) => self.arg::<f64>(name, &()),
            SerdeOperator::String(_) => self.arg::<String>(name, &()),
            SerdeOperator::StringConstant(_, _) => self.arg::<String>(name, &()),
            SerdeOperator::StringPattern(_) => self.arg::<String>(name, &()),
            SerdeOperator::CapturingStringPattern(_) => self.arg::<String>(name, &()),
            SerdeOperator::RelationSequence(_) => {
                warn!("Skipping relation sequence for now");
                self.arg::<String>(name, &())
                // registry.arg::<CustomScalar>(name, &()),
            }
            SerdeOperator::ConstructorSequence(_) => {
                warn!("Skipping constructor sequence for now");
                self.arg::<String>(name, &())
                // registry.arg::<CustomScalar>(name, &()),
            }
            SerdeOperator::ValueType(value_type) => {
                self.register_domain_argument(name, value_type.inner_operator_id)
            }
            SerdeOperator::ValueUnionType(_) => {
                todo!()
            }
            SerdeOperator::Id(_) => {
                todo!()
            }
            SerdeOperator::MapType(_) => {
                let type_info = MapInputValueTypeInfo(self.domain.node_adapter(operator_id));

                self.arg::<MapInputValue>(name, &type_info)
            }
        }
    }

    pub fn get_domain_type(&mut self, adapter: TypeAdapter<DynamicKind>) -> juniper::Type<'r> {
        let domain_data = adapter.domain_data.clone();
        match adapter.data() {
            DynamicRef::Node(node_ref) => {
                self.registry.get_type::<Node>(&NodeTypeInfo(TypeAdapter {
                    domain_data,
                    operator_id: node_ref.type_data.operator_id,
                    _kind: std::marker::PhantomData,
                }))
            }
            DynamicRef::Entity(entity_ref) => {
                self.registry.get_type::<Node>(&NodeTypeInfo(TypeAdapter {
                    domain_data,
                    operator_id: entity_ref.type_data.operator_id,
                    _kind: std::marker::PhantomData,
                }))
            }
            DynamicRef::Union(union_ref) => {
                self.registry
                    .get_type::<Union>(&UnionTypeInfo(UnionAdapter {
                        domain_data,
                        operator_id: union_ref.union_data.operator_id,
                    }))
            }
            DynamicRef::Scalar(_) => panic!("Unsupported scalar here"),
        }
    }
}

// These impls mean we can treat RegistryWrapper as a smart pointer to juniper::Registry:
impl<'a, 'r> Deref for RegistryWrapper<'a, 'r> {
    type Target = juniper::Registry<'r, GqlScalar>;

    fn deref(&self) -> &Self::Target {
        self.registry
    }
}

impl<'a, 'r> DerefMut for RegistryWrapper<'a, 'r> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.registry
    }
}
