use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use ontol_runtime::{
    serde::{SerdeOperator, SerdeOperatorId},
    DefId,
};
use tracing::{debug, warn};

use crate::{
    adapter::{
        data::{DomainData, Field, FieldKind},
        DynamicKind, DynamicRef, EdgeAdapter, TypeAdapter,
    },
    gql_scalar::GqlScalar,
    templates::{
        connection::{Connection, ConnectionTypeInfo},
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
    pub domain_data: &'a Arc<DomainData>,
}

impl<'a, 'r> RegistryWrapper<'a, 'r> {
    pub fn new(
        registry: &'a mut juniper::Registry<'r, GqlScalar>,
        domain_data: &'a Arc<DomainData>,
    ) -> Self {
        Self {
            domain_data,
            registry,
        }
    }

    pub fn register_domain_field(
        &mut self,
        name: &str,
        field: &Field,
    ) -> juniper::meta::Field<'r, GqlScalar> {
        let domain_data = &self.domain_data;
        let env = &self.domain_data.env;

        match &field.kind {
            FieldKind::Scalar(serde_operator_id) => {
                match env.get_serde_operator(*serde_operator_id) {
                    SerdeOperator::Unit => {
                        todo!("unit fields");
                    }
                    SerdeOperator::Int(_) => {
                        self.registry.field_convert::<i32, _, GqlContext>(name, &())
                    }
                    SerdeOperator::Number(_) => {
                        todo!("number fields");
                    }
                    SerdeOperator::String(_)
                    | SerdeOperator::StringConstant(..)
                    | SerdeOperator::StringPattern(_)
                    | SerdeOperator::CapturingStringPattern(_) => self
                        .registry
                        .field_convert::<std::string::String, _, GqlContext>(name, &()),
                    _ => {
                        panic!()
                    }
                }
            }
            FieldKind::Node(node, _operator_id) => {
                let adapter = self.domain_data.type_adapter::<DynamicKind>(*node);
                match adapter.data() {
                    DynamicRef::Node(_) => self.registry.field_convert::<Node, _, GqlContext>(
                        name,
                        &NodeTypeInfo(domain_data.type_adapter(*node)),
                    ),
                    DynamicRef::Entity(_) => self.registry.field_convert::<Node, _, GqlContext>(
                        name,
                        &NodeTypeInfo(domain_data.type_adapter(*node)),
                    ),
                    DynamicRef::Union(union_data) => {
                        self.registry.field_convert::<Union, _, GqlContext>(
                            name,
                            &UnionTypeInfo(domain_data.type_adapter(union_data.def_id)),
                        )
                    }
                    DynamicRef::Scalar(_) => panic!("Unsupported scalar here"),
                }
            }
            FieldKind::Edge { .. } => {
                todo!()
            }
            FieldKind::EntityRelationship {
                subject,
                node,
                node_operator,
                ..
            } => self
                .registry
                .field_convert::<Option<Connection>, _, GqlContext>(
                    name,
                    &ConnectionTypeInfo(EdgeAdapter {
                        domain_data: self.domain_data.clone(),
                        subject: Some(*subject),
                        node_id: *node,
                        node_operator_id: *node_operator,
                    }),
                ),
        }
    }

    pub fn register_operator_argument(
        &mut self,
        name: &str,
        operator_id: SerdeOperatorId,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        let operator = self.domain_data.env.get_serde_operator(operator_id);

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
                self.register_operator_argument(name, value_type.inner_operator_id)
            }
            SerdeOperator::ValueUnionType(union_type) => {
                self.register_def_argument(name, union_type.union_def_variant.def_id)
            }
            SerdeOperator::Id(_) => {
                todo!()
            }
            SerdeOperator::MapType(map_type) => {
                self.register_def_argument(name, map_type.def_variant.def_id)
            }
        }
    }

    pub fn register_def_argument(
        &mut self,
        name: &str,
        def_id: DefId,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        let type_adapter = self.domain_data.type_adapter::<DynamicKind>(def_id);

        match type_adapter.data() {
            DynamicRef::Node(_) | DynamicRef::Entity(_) => {
                let type_info = MapInputValueTypeInfo(self.domain_data.type_adapter(def_id));
                self.arg::<MapInputValue>(name, &type_info)
            }
            DynamicRef::Union(_) => {
                todo!()
            }
            DynamicRef::Scalar(_) => {
                panic!()
            }
        }
    }

    pub fn get_domain_type(&mut self, adapter: TypeAdapter<DynamicKind>) -> juniper::Type<'r> {
        let domain_data = &adapter.domain_data;
        match adapter.data() {
            DynamicRef::Node(node_ref) => self.registry.get_type::<Node>(&NodeTypeInfo(
                domain_data.type_adapter(node_ref.type_data.def_id),
            )),
            DynamicRef::Entity(entity_ref) => self.registry.get_type::<Node>(&NodeTypeInfo(
                domain_data.type_adapter(entity_ref.type_data.def_id),
            )),
            DynamicRef::Union(union_data) => self
                .registry
                .get_type::<Union>(&UnionTypeInfo(domain_data.type_adapter(union_data.def_id))),
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
