use std::sync::Arc;

use juniper::{GraphQLValue, ID};
use ontol_runtime::serde::operator::{
    FilteredVariants, SerdeOperator, SerdeOperatorId, SerdePropertyFlags,
};
use tracing::{trace, warn};

use crate::{
    gql_scalar::GqlScalar,
    templates::{attribute_type::AttributeType, indexed_input_value::IndexedInputValue},
    virtual_schema::{
        argument::{ArgKind, DomainFieldArg, FieldArg},
        data::{
            FieldKind, NativeScalarKind, Optionality, TypeIndex, TypeKind, TypeModifier, TypeRef,
            UnitTypeRef,
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
                    description: match &field_data.kind {
                        FieldKind::Property(property) => self
                            .virtual_schema
                            .ontology()
                            .get_docs(property.property_id.relationship_id.0),
                        _ => None,
                    },
                    arguments: self.get_arguments_for_field(&field_data.kind),
                    field_type: self
                        .get_type::<AttributeType>(field_data.field_type, TypingPurpose::Selection),
                    deprecation_status: juniper::meta::DeprecationStatus::Current,
                })
                .collect(),
            _ => vec![],
        }
    }

    pub fn collect_operator_arguments(
        &mut self,
        operator_id: SerdeOperatorId,
        output: &mut Vec<juniper::meta::Argument<'r, GqlScalar>>,
        typing_purpose: TypingPurpose,
    ) {
        let serde_operator = self
            .virtual_schema
            .ontology()
            .get_serde_operator(operator_id);

        match serde_operator {
            SerdeOperator::Struct(struct_op) => {
                let (mode, _) = typing_purpose.mode_and_level();

                for (name, property) in struct_op.filter_properties(mode, None) {
                    if property.is_read_only()
                        && matches!(
                            typing_purpose,
                            TypingPurpose::Input | TypingPurpose::PartialInput
                        )
                    {
                        continue;
                    }

                    output.push(self.get_operator_argument(
                        name,
                        property.value_operator_id,
                        property.rel_params_operator_id,
                        property.flags,
                        TypeModifier::Unit(match typing_purpose {
                            TypingPurpose::PartialInput => Optionality::Optional,
                            _ => {
                                if property.is_optional() || property.value_generator.is_some() {
                                    Optionality::Optional
                                } else {
                                    Optionality::Mandatory
                                }
                            }
                        }),
                    ))
                }
            }
            SerdeOperator::Union(union_op) => {
                let (mode, level) = typing_purpose.mode_and_level();
                match union_op.variants(mode, level) {
                    FilteredVariants::Single(operator_id) => {
                        self.collect_operator_arguments(operator_id, output, typing_purpose);
                    }
                    FilteredVariants::Union(variants) => {
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
            SerdeOperator::Alias(alias_op) => {
                self.collect_operator_arguments(alias_op.inner_operator_id, output, typing_purpose);
            }
            SerdeOperator::PrimaryId(property_name, id_operator_id) => {
                output.push(self.get_operator_argument(
                    property_name,
                    *id_operator_id,
                    None,
                    SerdePropertyFlags::ENTITY_ID,
                    TypeModifier::Unit(Optionality::Optional),
                ))
            }
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
        property_flags: SerdePropertyFlags,
        modifier: TypeModifier,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        let operator = self
            .virtual_schema
            .ontology()
            .get_serde_operator(operator_id);

        trace!("register argument '{name}': {operator:?}");

        if property_flags.contains(SerdePropertyFlags::ENTITY_ID) {
            return self.get_native_argument::<ID>(name, modifier);
        }

        match operator {
            SerdeOperator::Unit => {
                todo!("unit argument")
            }
            SerdeOperator::False(_) | SerdeOperator::True(_) | SerdeOperator::Bool(_) => {
                self.get_native_argument::<bool>(name, modifier)
            }
            SerdeOperator::I64(_) => return self.get_native_argument::<i32>(name, modifier),
            SerdeOperator::F64(_) => return self.get_native_argument::<f64>(name, modifier),
            SerdeOperator::String(_) => return self.get_native_argument::<String>(name, modifier),
            SerdeOperator::StringConstant(_, _) => {
                self.get_native_argument::<String>(name, modifier)
            }
            SerdeOperator::StringPattern(_) => self.get_native_argument::<String>(name, modifier),
            SerdeOperator::CapturingStringPattern(_) => {
                self.get_native_argument::<String>(name, modifier)
            }
            SerdeOperator::DynamicSequence => panic!("No dynamic sequence expected here"),
            SerdeOperator::RelationSequence(seq_op) => {
                match self
                    .virtual_schema
                    .type_index_by_def(seq_op.def_variant.def_id, QueryLevel::Edge { rel_params })
                {
                    Some(type_index) => self.registry.arg::<Option<Vec<IndexedInputValue>>>(
                        name,
                        &self
                            .virtual_schema
                            .indexed_type_info(type_index, TypingPurpose::ReferenceInput),
                    ),
                    None => self.get_operator_argument(
                        name,
                        seq_op.ranges[0].operator_id,
                        rel_params,
                        property_flags,
                        TypeModifier::Array(modifier.unit_optionality(), Optionality::Mandatory),
                    ),
                }
            }
            SerdeOperator::ConstructorSequence(_) => {
                warn!("Skipping constructor sequence for now");
                return self.registry.arg::<bool>(name, &());
                // registry.arg::<CustomScalar>(name, &()),
            }
            SerdeOperator::Alias(alias_op) => self.get_operator_argument(
                name,
                alias_op.inner_operator_id,
                rel_params,
                property_flags,
                modifier,
            ),
            SerdeOperator::Union(union_op) => {
                let def_id = union_op.union_def_variant().def_id;
                let type_info = self.virtual_schema.ontology().get_type_info(def_id);

                // If this is an entity, use Edge + ReferenceInput
                // to get the option of just specifying an ID.
                // TODO: Ensure this for create mutations only
                let (query_level, typing_purpose) = if type_info.entity_info.is_some() {
                    (
                        QueryLevel::Edge { rel_params: None },
                        TypingPurpose::ReferenceInput,
                    )
                } else {
                    (QueryLevel::Node, TypingPurpose::Input)
                };

                let type_index = self
                    .virtual_schema
                    .type_index_by_def(def_id, query_level)
                    .expect("No union found");

                let info = self
                    .virtual_schema
                    .indexed_type_info(type_index, typing_purpose);

                match modifier.unit_optionality() {
                    Optionality::Mandatory => self.registry.arg::<IndexedInputValue>(name, &info),
                    Optionality::Optional => {
                        self.registry.arg::<Option<IndexedInputValue>>(name, &info)
                    }
                }
            }
            SerdeOperator::Struct(struct_op) => {
                let def_id = struct_op.def_variant.def_id;
                let type_index = self
                    .virtual_schema
                    .type_index_by_def(def_id, QueryLevel::Node)
                    .expect("No struct found");

                let info = self
                    .virtual_schema
                    .indexed_type_info(type_index, TypingPurpose::Input);

                match modifier.unit_optionality() {
                    Optionality::Mandatory => {
                        return self.registry.arg::<IndexedInputValue>(name, &info);
                    }
                    Optionality::Optional => {
                        return self.registry.arg::<Option<IndexedInputValue>>(name, &info)
                    }
                }
            }
            SerdeOperator::PrimaryId(..) => {
                panic!()
            }
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
                SerdePropertyFlags::default(),
                TypeModifier::Unit(Optionality::Mandatory),
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
            UnitTypeRef::NativeScalar(scalar_ref) => match &scalar_ref.kind {
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

    fn get_native_argument<T>(
        &mut self,
        name: &str,
        modifier: TypeModifier,
    ) -> juniper::meta::Argument<'r, GqlScalar>
    where
        T: juniper::GraphQLType<GqlScalar>
            + juniper::FromInputValue<GqlScalar>
            + juniper::GraphQLValue<GqlScalar, TypeInfo = ()>,
    {
        match modifier {
            TypeModifier::Unit(Optionality::Mandatory) => self.registry.arg::<T>(name, &()),
            TypeModifier::Unit(Optionality::Optional) => self.registry.arg::<Option<T>>(name, &()),
            TypeModifier::Array(Optionality::Mandatory, Optionality::Mandatory) => {
                self.registry.arg::<Vec<T>>(name, &())
            }
            TypeModifier::Array(Optionality::Mandatory, Optionality::Optional) => {
                self.registry.arg::<Vec<Option<T>>>(name, &())
            }
            TypeModifier::Array(Optionality::Optional, Optionality::Mandatory) => {
                self.registry.arg::<Option<Vec<T>>>(name, &())
            }
            TypeModifier::Array(Optionality::Optional, Optionality::Optional) => {
                self.registry.arg::<Option<Vec<Option<T>>>>(name, &())
            }
        }
    }
}
