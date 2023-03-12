use std::sync::Arc;

use juniper::GraphQLValue;
use ontol_runtime::serde::{
    operator::{FilteredVariants, SerdeOperator, SerdeOperatorId},
    processor::{ProcessorLevel, ProcessorMode},
};
use tracing::{debug, warn};

use crate::{
    gql_scalar::GqlScalar,
    templates::{indexed_input_value::IndexedInputValue, indexed_type::IndexedType},
    virtual_schema::{
        data::{
            Argument, ArgumentsKind, NativeScalarRef, Optionality, TypeIndex, TypeKind,
            TypeModifier, TypeRef, UnitTypeRef,
        },
        VirtualIndexedTypeInfo, VirtualSchema,
    },
};

/// Juniper registry and virtual schema combined into
/// one type to enable a nice unified API
pub struct VirtualRegistry<'a, 'r> {
    pub virtual_schema: &'a Arc<VirtualSchema>,
    pub registry: &'a mut juniper::Registry<'r, GqlScalar>,
}

impl<'a, 'r> VirtualRegistry<'a, 'r> {
    pub fn new(
        virtual_schema: &'a Arc<VirtualSchema>,
        registry: &'a mut juniper::Registry<'r, GqlScalar>,
    ) -> Self {
        Self {
            registry,
            virtual_schema,
        }
    }

    pub fn get_fields(
        &mut self,
        type_index: TypeIndex,
    ) -> Vec<juniper::meta::Field<'r, GqlScalar>> {
        let type_data = self.virtual_schema.type_data(type_index);
        match &type_data.kind {
            TypeKind::Object(object) => object
                .fields
                .iter()
                .map(|(name, field_data)| juniper::meta::Field {
                    name: name.clone(),
                    description: None,
                    arguments: self.get_field_arguments(&field_data.arguments),
                    field_type: self.get_type(field_data.field_type),
                    deprecation_status: juniper::meta::DeprecationStatus::Current,
                })
                .collect(),
            _ => vec![],
        }
    }

    #[inline]
    fn get_type(&mut self, type_ref: TypeRef) -> juniper::Type<'r> {
        match type_ref.unit {
            UnitTypeRef::Indexed(type_index) => self.get_modified_type::<IndexedType>(
                &VirtualIndexedTypeInfo {
                    virtual_schema: self.virtual_schema.clone(),
                    type_index,
                    mode: ProcessorMode::Select,
                    level: ProcessorLevel::Root,
                },
                type_ref.modifier,
            ),
            UnitTypeRef::ID(_operator_id) => {
                self.get_modified_type::<juniper::ID>(&(), type_ref.modifier)
            }
            UnitTypeRef::NativeScalar(NativeScalarRef::Unit) => {
                todo!("Unit type")
            }
            UnitTypeRef::NativeScalar(NativeScalarRef::Bool) => {
                self.get_modified_type::<bool>(&(), type_ref.modifier)
            }
            UnitTypeRef::NativeScalar(NativeScalarRef::Int(_)) => {
                self.get_modified_type::<i32>(&(), type_ref.modifier)
            }
            UnitTypeRef::NativeScalar(NativeScalarRef::Number(_)) => {
                self.get_modified_type::<f64>(&(), type_ref.modifier)
            }
            UnitTypeRef::NativeScalar(NativeScalarRef::String(_)) => {
                self.get_modified_type::<std::string::String>(&(), type_ref.modifier)
            }
        }
    }

    #[inline]
    fn get_modified_type<T>(
        &mut self,
        type_info: &<T as GraphQLValue<GqlScalar>>::TypeInfo,
        modifier: TypeModifier,
    ) -> juniper::Type<'r>
    where
        T: juniper::GraphQLType<GqlScalar>,
    {
        match modifier {
            TypeModifier::Unit(Optionality::Mandatory) => self.registry.get_type::<T>(type_info),
            TypeModifier::Unit(Optionality::Optional) => {
                self.registry.get_type::<Option<T>>(type_info)
            }
            TypeModifier::Array(Optionality::Mandatory, Optionality::Mandatory) => {
                self.registry.get_type::<Vec<T>>(type_info)
            }
            TypeModifier::Array(Optionality::Mandatory, Optionality::Optional) => {
                self.registry.get_type::<Vec<Option<T>>>(type_info)
            }
            TypeModifier::Array(Optionality::Optional, Optionality::Mandatory) => {
                self.registry.get_type::<Option<Vec<T>>>(type_info)
            }
            TypeModifier::Array(Optionality::Optional, Optionality::Optional) => {
                self.registry.get_type::<Option<Vec<Option<T>>>>(type_info)
            }
        }
    }

    pub fn collect_operator_arguments(
        &mut self,
        operator_id: SerdeOperatorId,
        output: &mut Vec<juniper::meta::Argument<'r, GqlScalar>>,
        mode: ProcessorMode,
        level: ProcessorLevel,
    ) {
        let serde_operator = self.virtual_schema.env().get_serde_operator(operator_id);

        match serde_operator {
            SerdeOperator::Map(map_op) => {
                for (name, property) in &map_op.properties {
                    output.push(self.get_operator_argument(name, property.value_operator_id))
                }
            }
            SerdeOperator::Union(union_op) => match union_op.variants(mode, level) {
                FilteredVariants::Single(operator_id) => {
                    self.collect_operator_arguments(operator_id, output, mode, level);
                }
                FilteredVariants::Multi(variants) => {
                    warn!("Multiple variants in union: {variants:#?}");
                }
            },
            other => {
                panic!("{other:?}");
            }
        }
    }

    pub fn get_operator_argument(
        &mut self,
        name: &str,
        operator_id: SerdeOperatorId,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        let operator = self.virtual_schema.env().get_serde_operator(operator_id);

        debug!("register argument {name} {operator:?}");

        match operator {
            SerdeOperator::Unit => {
                todo!("unit argument")
            }
            SerdeOperator::Int(_) => self.registry.arg::<i32>(name, &()),
            SerdeOperator::Number(_) => self.registry.arg::<f64>(name, &()),
            SerdeOperator::String(_) => self.registry.arg::<String>(name, &()),
            SerdeOperator::StringConstant(_, _) => self.registry.arg::<String>(name, &()),
            SerdeOperator::StringPattern(_) => self.registry.arg::<String>(name, &()),
            SerdeOperator::CapturingStringPattern(_) => self.registry.arg::<String>(name, &()),
            SerdeOperator::RelationSequence(_) => {
                warn!("Skipping relation sequence for now: {name}");
                self.registry.arg::<String>(name, &())
                // registry.arg::<CustomScalar>(name, &()),
            }
            SerdeOperator::ConstructorSequence(_) => {
                warn!("Skipping constructor sequence for now");
                self.registry.arg::<String>(name, &())
                // registry.arg::<CustomScalar>(name, &()),
            }
            SerdeOperator::ValueType(value_op) => {
                self.get_operator_argument(name, value_op.inner_operator_id)
            }
            SerdeOperator::Union(_union_op) => {
                // self.register_def_argument(name, union_type.union_def_variant.def_id)
                todo!()
            }
            SerdeOperator::Id(_) => {
                panic!()
            }
            SerdeOperator::Map(_map_op) => {
                // self.register_def_argument(name, map_type.def_variant.def_id)
                todo!()
            }
        }
    }

    fn get_field_arguments(
        &mut self,
        arguments: &ArgumentsKind,
    ) -> Option<Vec<juniper::meta::Argument<'r, GqlScalar>>> {
        match arguments {
            ArgumentsKind::Empty => None,
            ArgumentsKind::ConnectionQuery => None,
            ArgumentsKind::CreateMutation { input } => Some(vec![self.get_field_argument(input)]),
            ArgumentsKind::UpdateMutation { id, input } => Some(vec![
                self.get_field_argument(id),
                self.get_field_argument(input),
            ]),
            ArgumentsKind::DeleteMutation { id } => Some(vec![self.get_field_argument(id)]),
        }
    }

    fn get_field_argument(
        &mut self,
        argument: &Argument,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        match argument {
            Argument::Input(type_index, _, mode) => self.registry.arg::<IndexedInputValue>(
                argument.name(),
                &self
                    .virtual_schema
                    .indexed_type_info(*type_index, *mode, ProcessorLevel::Root),
            ),
            Argument::Id(operator_id, _) => {
                self.get_operator_argument(argument.name(), *operator_id)
            }
        }
    }
}
