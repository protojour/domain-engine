use std::sync::Arc;

use indexmap::IndexMap;
use juniper::GraphQLValue;
use ontol_runtime::serde::operator::{FilteredVariants, SerdeOperator, SerdeOperatorId};
use smartstring::alias::String;
use tracing::{debug, warn};

use crate::{
    gql_scalar::GqlScalar,
    templates::{attribute_type::AttributeType, indexed_input_value::IndexedInputValue},
    virtual_schema::{
        argument::{ArgKind, DomainFieldArg, FieldArg},
        data::{
            FieldData, FieldKind, NativeScalarKind, Optionality, TypeIndex, TypeKind, TypeModifier,
            TypeRef, UnitTypeRef,
        },
        QueryLevel, TypingPurpose, VirtualIndexedTypeInfo, VirtualSchema,
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
            virtual_schema,
            registry,
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
                    arguments: self.get_arguments_for_field(&field_data.kind),
                    field_type: self
                        .get_type::<AttributeType>(field_data.field_type, TypingPurpose::Selection),
                    deprecation_status: juniper::meta::DeprecationStatus::Current,
                })
                .collect(),
            _ => vec![],
        }
    }

    /// Convert fields to input arguments
    pub fn _convert_fields_to_arguments(
        &mut self,
        fields: &IndexMap<String, FieldData>,
        typing_purpose: TypingPurpose,
    ) -> Vec<juniper::meta::Argument<'r, GqlScalar>> {
        fields
            .iter()
            .map(|(name, field_data)| {
                let arg_type =
                    self.get_type::<IndexedInputValue>(field_data.field_type, typing_purpose);
                juniper::meta::Argument::new(name, arg_type)
            })
            .collect()
    }

    pub fn collect_operator_arguments(
        &mut self,
        operator_id: SerdeOperatorId,
        output: &mut Vec<juniper::meta::Argument<'r, GqlScalar>>,
        typing_purpose: TypingPurpose,
    ) {
        let serde_operator = self.virtual_schema.env().get_serde_operator(operator_id);

        match serde_operator {
            SerdeOperator::Map(map_op) => {
                let opt = match typing_purpose {
                    TypingPurpose::PartialInput => Optionality::Optional,
                    _ => Optionality::Mandatory,
                };

                for (name, property) in &map_op.properties {
                    output.push(self.get_operator_argument(
                        name,
                        property.value_operator_id,
                        property.rel_params_operator_id,
                        opt,
                    ))
                }
            }
            SerdeOperator::Union(union_op) => {
                let (mode, level) = typing_purpose.mode_and_level();
                match union_op.variants(mode, level) {
                    FilteredVariants::Single(operator_id) => {
                        self.collect_operator_arguments(operator_id, output, typing_purpose);
                    }
                    FilteredVariants::Multi(variants) => {
                        warn!("Multiple variants in union: {variants:#?}");
                        for variant in variants {
                            self.collect_operator_arguments(
                                variant.operator_id,
                                output,
                                TypingPurpose::PartialInput,
                            );
                        }
                    }
                }
            }
            SerdeOperator::Id(id_operator_id) => output.push(self.get_operator_argument(
                "_id",
                *id_operator_id,
                None,
                Optionality::Optional,
            )),
            other => {
                panic!("{other:?}");
            }
        }
    }

    pub fn get_operator_argument(
        &mut self,
        name: &str,
        operator_id: SerdeOperatorId,
        rel_params: Option<SerdeOperatorId>,
        opt: Optionality,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        let operator = self.virtual_schema.env().get_serde_operator(operator_id);

        debug!("register argument {name} {operator:?}");

        use std::string::String;

        match operator {
            SerdeOperator::Unit => {
                todo!("unit argument")
            }
            SerdeOperator::Int(_) => self.get_native_argument::<i32>(name, opt),
            SerdeOperator::Number(_) => self.get_native_argument::<f64>(name, opt),
            SerdeOperator::String(_) => self.get_native_argument::<String>(name, opt),
            SerdeOperator::StringConstant(_, _) => self.get_native_argument::<String>(name, opt),
            SerdeOperator::StringPattern(_) => self.get_native_argument::<String>(name, opt),
            SerdeOperator::CapturingStringPattern(_) => {
                self.get_native_argument::<String>(name, opt)
            }
            SerdeOperator::RelationSequence(seq_op) => {
                let type_index = self
                    .virtual_schema
                    .type_index_by_def(seq_op.def_variant.def_id, QueryLevel::Edge { rel_params })
                    .expect("Should have an edge type for relation sequence");

                self.registry.arg::<Option<Vec<IndexedInputValue>>>(
                    name,
                    &self
                        .virtual_schema
                        .indexed_type_info(type_index, TypingPurpose::ReferenceInput),
                )
            }
            SerdeOperator::ConstructorSequence(_) => {
                warn!("Skipping constructor sequence for now");
                self.registry.arg::<bool>(name, &())
                // registry.arg::<CustomScalar>(name, &()),
            }
            SerdeOperator::ValueType(value_op) => {
                self.get_operator_argument(name, value_op.inner_operator_id, rel_params, opt)
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

    fn get_native_argument<T>(
        &mut self,
        name: &str,
        optionality: Optionality,
    ) -> juniper::meta::Argument<'r, GqlScalar>
    where
        T: juniper::GraphQLType<GqlScalar>
            + juniper::FromInputValue<GqlScalar>
            + juniper::GraphQLValue<GqlScalar, TypeInfo = ()>,
    {
        match optionality {
            Optionality::Mandatory => self.registry.arg::<T>(name, &()),
            Optionality::Optional => self.registry.arg::<Option<T>>(name, &()),
        }
    }

    fn get_arguments_for_field(
        &mut self,
        field_kind: &FieldKind,
    ) -> Option<Vec<juniper::meta::Argument<'r, GqlScalar>>> {
        match field_kind {
            FieldKind::Connection { first, after, .. } => Some(vec![
                self.registry.arg::<Option<i32>>(first.name(), &()),
                self.registry
                    .arg::<Option<std::string::String>>(after.name(), &()),
            ]),
            FieldKind::CreateMutation { input } => Some(vec![self.get_domain_arg(input)]),
            FieldKind::UpdateMutation { id, input } => {
                Some(vec![self.get_domain_arg(id), self.get_domain_arg(input)])
            }
            FieldKind::DeleteMutation { id } => Some(vec![self.get_domain_arg(id)]),
            FieldKind::Property(_)
            | FieldKind::EdgeProperty(_)
            | FieldKind::Id(_)
            | FieldKind::Edges
            | FieldKind::Node => None,
        }
    }

    fn get_domain_arg(
        &mut self,
        argument: &dyn DomainFieldArg,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        match argument.kind() {
            ArgKind::Def(type_index, _) => self.registry.arg::<IndexedInputValue>(
                argument.name(),
                &self
                    .virtual_schema
                    .indexed_type_info(type_index, argument.typing_purpose()),
            ),
            ArgKind::Operator(operator_id) => self.get_operator_argument(
                argument.name(),
                operator_id,
                None,
                Optionality::Mandatory,
            ),
        }
    }

    #[inline]
    pub fn get_type<I>(
        &mut self,
        type_ref: TypeRef,
        typing_purpose: TypingPurpose,
    ) -> juniper::Type<'r>
    where
        I: juniper::GraphQLType<GqlScalar>
            + juniper::GraphQLType<GqlScalar, TypeInfo = VirtualIndexedTypeInfo>,
    {
        match type_ref.unit {
            UnitTypeRef::Indexed(type_index) => self.get_modified_type::<I>(
                &VirtualIndexedTypeInfo {
                    virtual_schema: self.virtual_schema.clone(),
                    type_index,
                    typing_purpose,
                },
                type_ref.modifier,
            ),
            UnitTypeRef::Scalar(scalar_ref) => match &scalar_ref.kind {
                NativeScalarKind::Unit => {
                    todo!("Unit type")
                }
                NativeScalarKind::Bool => self.get_modified_type::<bool>(&(), type_ref.modifier),
                NativeScalarKind::Int(_) => self.get_modified_type::<i32>(&(), type_ref.modifier),
                NativeScalarKind::Number(_) => {
                    self.get_modified_type::<f64>(&(), type_ref.modifier)
                }
                NativeScalarKind::String => {
                    self.get_modified_type::<std::string::String>(&(), type_ref.modifier)
                }
                NativeScalarKind::ID => {
                    self.get_modified_type::<juniper::ID>(&(), type_ref.modifier)
                }
            },
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
}
