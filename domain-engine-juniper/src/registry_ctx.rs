use std::sync::Arc;

use juniper::{GraphQLValue, ID};
use ontol_runtime::{
    interface::graphql::{
        argument::{ArgKind, DomainFieldArg, FieldArg},
        data::{
            FieldKind, NativeScalarKind, Optionality, TypeAddr, TypeKind, TypeModifier, TypeRef,
            UnitTypeRef,
        },
        schema::{QueryLevel, TypingPurpose},
    },
    interface::{
        graphql::argument::DefaultArg,
        serde::operator::{FilteredVariants, SerdeOperator, SerdeOperatorAddr, SerdePropertyFlags},
    },
    value::PropertyId,
    Role,
};
use tracing::trace;

use crate::{
    context::{SchemaCtx, SchemaType},
    gql_scalar::GqlScalar,
    templates::{attribute_type::AttributeType, input_type::InputType},
};

/// SchemaCtx and juniper Registry combined together to provide more ergonimic API
pub(crate) struct RegistryCtx<'a, 'r> {
    pub schema_ctx: &'a Arc<SchemaCtx>,
    pub registry: &'a mut juniper::Registry<'r, GqlScalar>,
}

#[derive(Debug)]
pub enum CollectOperatorError {
    Scalar,
}

impl<'a, 'r> RegistryCtx<'a, 'r> {
    pub fn new(
        schema_ctx: &'a Arc<SchemaCtx>,
        registry: &'a mut juniper::Registry<'r, GqlScalar>,
    ) -> Self {
        Self {
            schema_ctx,
            registry,
        }
    }

    pub fn get_fields(&mut self, type_addr: TypeAddr) -> Vec<juniper::meta::Field<'r, GqlScalar>> {
        // This is part of a big recursive algorithm, so iterator mapping is avoided
        let mut fields = vec![];

        let type_data = self.schema_ctx.schema.type_data(type_addr);

        if let TypeKind::Object(object) = &type_data.kind {
            for (name, field_data) in &object.fields {
                let field = juniper::meta::Field {
                    name: name.clone(),
                    description: match &field_data.kind {
                        FieldKind::Property(property) => self
                            .schema_ctx
                            .ontology
                            .get_docs(property.property_id.relationship_id.0),
                        _ => None,
                    },
                    arguments: self.get_arguments_to_field(&field_data.kind),
                    field_type: self
                        .get_type::<AttributeType>(field_data.field_type, TypingPurpose::Selection),
                    deprecation_status: juniper::meta::DeprecationStatus::Current,
                };

                fields.push(field);
            }
        }

        fields
    }

    pub fn collect_operator_arguments(
        &mut self,
        operator_addr: SerdeOperatorAddr,
        output: &mut Vec<juniper::meta::Argument<'r, GqlScalar>>,
        typing_purpose: TypingPurpose,
        filter: ArgumentFilter,
    ) -> Result<(), CollectOperatorError> {
        let serde_operator = self.schema_ctx.ontology.get_serde_operator(operator_addr);

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

                    if !filter.filter_property(name, Some(property.property_id), output) {
                        continue;
                    }

                    output.push(self.get_operator_argument(
                        name,
                        property.value_addr,
                        property.rel_params_addr,
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

                Ok(())
            }
            SerdeOperator::Union(union_op) => {
                let (mode, level) = typing_purpose.mode_and_level();
                match union_op.variants(mode, level) {
                    FilteredVariants::Single(operator_addr) => {
                        self.collect_operator_arguments(
                            operator_addr,
                            output,
                            typing_purpose,
                            filter,
                        )?;
                    }
                    FilteredVariants::Union(variants) => {
                        // FIXME: Instead of just deduplicating the properties,
                        // the documentation for each of them should be combined in some way.
                        // When a union is used as an InputValue, the GraphQL functions purely
                        // as a documentation layer anyway.
                        for variant in variants {
                            self.collect_operator_arguments(
                                variant.addr,
                                output,
                                TypingPurpose::PartialInput,
                                ArgumentFilter {
                                    deduplicate: true,
                                    skip_subject: false,
                                    skip_object: true,
                                },
                            )?;
                        }
                        for variant in variants {
                            self.collect_operator_arguments(
                                variant.addr,
                                output,
                                TypingPurpose::PartialInput,
                                ArgumentFilter {
                                    deduplicate: true,
                                    skip_subject: true,
                                    skip_object: false,
                                },
                            )?;
                        }
                    }
                }

                Ok(())
            }
            SerdeOperator::Alias(alias_op) => {
                self.collect_operator_arguments(
                    alias_op.inner_addr,
                    output,
                    typing_purpose,
                    filter,
                )?;

                Ok(())
            }
            SerdeOperator::IdSingletonStruct(property_name, id_operator_addr) => {
                if filter.filter_property(property_name, None, output) {
                    output.push(self.get_operator_argument(
                        property_name,
                        *id_operator_addr,
                        None,
                        SerdePropertyFlags::ENTITY_ID,
                        TypeModifier::Unit(Optionality::Optional),
                    ));
                }

                Ok(())
            }
            _ => Err(CollectOperatorError::Scalar),
        }
    }

    pub fn get_operator_argument(
        &mut self,
        name: &str,
        operator_addr: SerdeOperatorAddr,
        rel_params: Option<SerdeOperatorAddr>,
        property_flags: SerdePropertyFlags,
        modifier: TypeModifier,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        let operator = self.schema_ctx.ontology.get_serde_operator(operator_addr);

        trace!("register argument '{name}': {operator:?}");

        if property_flags.contains(SerdePropertyFlags::ENTITY_ID) {
            return self.modified_arg::<ID>(name, modifier, &());
        }

        match operator {
            SerdeOperator::Unit => {
                todo!("unit argument")
            }
            SerdeOperator::False(_) | SerdeOperator::True(_) | SerdeOperator::Boolean(_) => {
                self.modified_arg::<bool>(name, modifier, &())
            }
            SerdeOperator::I32(_, _) => return self.modified_arg::<i32>(name, modifier, &()),
            SerdeOperator::I64(_, _) => {
                let i64_schema_type = self.schema_ctx.get_schema_type(
                    self.schema_ctx.schema.i64_custom_scalar.unwrap(),
                    TypingPurpose::Input,
                );

                self.modified_arg::<InputType>(name, modifier, &i64_schema_type)
            }
            SerdeOperator::F64(_, _) => return self.modified_arg::<f64>(name, modifier, &()),
            SerdeOperator::String(_) => return self.modified_arg::<String>(name, modifier, &()),
            SerdeOperator::StringConstant(_, _) => self.modified_arg::<String>(name, modifier, &()),
            SerdeOperator::TextPattern(_) => self.modified_arg::<String>(name, modifier, &()),
            SerdeOperator::CapturingTextPattern(_) => {
                self.modified_arg::<String>(name, modifier, &())
            }
            SerdeOperator::DynamicSequence => panic!("No dynamic sequence expected here"),
            SerdeOperator::RelationSequence(seq_op) => {
                match self
                    .schema_ctx
                    .type_addr_by_def(seq_op.def.def_id, QueryLevel::Edge { rel_params })
                {
                    Some(type_addr) => self.registry.arg::<Option<Vec<InputType>>>(
                        name,
                        &self
                            .schema_ctx
                            .get_schema_type(type_addr, TypingPurpose::InputOrReference),
                    ),
                    None => self.get_operator_argument(
                        name,
                        seq_op.ranges[0].addr,
                        rel_params,
                        property_flags,
                        TypeModifier::Array(modifier.unit_optionality(), Optionality::Mandatory),
                    ),
                }
            }
            SerdeOperator::ConstructorSequence(_) => {
                let json_scalar_info = self.schema_ctx.get_schema_type(
                    self.schema_ctx.schema.json_scalar,
                    TypingPurpose::PartialInput,
                );

                self.registry
                    .arg::<Option<InputType>>(name, &json_scalar_info)
            }
            SerdeOperator::Alias(alias_op) => self.get_operator_argument(
                name,
                alias_op.inner_addr,
                rel_params,
                property_flags,
                modifier,
            ),
            SerdeOperator::Union(union_op) => {
                let def_id = union_op.union_def().def_id;
                let type_info = self.schema_ctx.ontology.get_type_info(def_id);

                // If this is an entity, use Edge + ReferenceInput
                // to get the option of just specifying an ID.
                // TODO: Ensure this for create mutations only
                let (query_level, typing_purpose) = if type_info.entity_info.is_some() {
                    (
                        QueryLevel::Edge { rel_params: None },
                        TypingPurpose::InputOrReference,
                    )
                } else {
                    (QueryLevel::Node, TypingPurpose::Input)
                };

                let type_addr = self
                    .schema_ctx
                    .type_addr_by_def(def_id, query_level)
                    .expect("No union found");

                let info = self.schema_ctx.get_schema_type(type_addr, typing_purpose);

                match modifier.unit_optionality() {
                    Optionality::Mandatory => self.registry.arg::<InputType>(name, &info),
                    Optionality::Optional => self.registry.arg::<Option<InputType>>(name, &info),
                }
            }
            SerdeOperator::Struct(struct_op) => {
                let def_id = struct_op.def.def_id;
                let type_addr = self
                    .schema_ctx
                    .type_addr_by_def(def_id, QueryLevel::Node)
                    .expect("No struct found");

                let info = self
                    .schema_ctx
                    .get_schema_type(type_addr, TypingPurpose::Input);

                match modifier.unit_optionality() {
                    Optionality::Mandatory => self.registry.arg::<InputType>(name, &info),
                    Optionality::Optional => self.registry.arg::<Option<InputType>>(name, &info),
                }
            }
            SerdeOperator::IdSingletonStruct(..) => {
                panic!()
            }
        }
    }

    fn get_arguments_to_field(
        &mut self,
        field_kind: &FieldKind,
    ) -> Option<Vec<juniper::meta::Argument<'r, GqlScalar>>> {
        match field_kind {
            FieldKind::ConnectionProperty {
                first_arg: first,
                after_arg: after,
                ..
            } => Some(vec![
                self.registry.arg::<Option<i32>>(first.name(), &()),
                self.registry
                    .arg::<Option<std::string::String>>(after.name(), &()),
            ]),
            FieldKind::MapConnection {
                input_arg,
                first_arg,
                after_arg,
                ..
            } => Some(vec![
                self.get_domain_field_arg(input_arg),
                self.registry.arg::<Option<i32>>(first_arg.name(), &()),
                self.registry
                    .arg::<Option<std::string::String>>(after_arg.name(), &()),
            ]),
            FieldKind::MapFind { input_arg, .. } => {
                Some(vec![self.get_domain_field_arg(input_arg)])
            }
            FieldKind::CreateMutation { input } => Some(vec![self.get_domain_field_arg(input)]),
            FieldKind::UpdateMutation { id, input } => Some(vec![
                self.get_domain_field_arg(id),
                self.get_domain_field_arg(input),
            ]),
            FieldKind::DeleteMutation { id } => Some(vec![self.get_domain_field_arg(id)]),
            FieldKind::Property(_)
            | FieldKind::EdgeProperty(_)
            | FieldKind::Id(_)
            | FieldKind::Edges
            | FieldKind::Nodes
            | FieldKind::PageInfo
            | FieldKind::Node
            | FieldKind::TotalCount
            | FieldKind::OpenAttributes => None,
        }
    }

    fn get_domain_field_arg(
        &mut self,
        field_arg: &dyn DomainFieldArg,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        let arg = match field_arg.kind() {
            ArgKind::Addr(type_addr) => {
                let schema_type = self
                    .schema_ctx
                    .get_schema_type(type_addr, field_arg.typing_purpose());

                self.registry
                    .arg::<InputType>(field_arg.name(), &schema_type)
            }
            ArgKind::Operator(addr) => self.get_operator_argument(
                field_arg.name(),
                addr,
                None,
                SerdePropertyFlags::default(),
                TypeModifier::Unit(Optionality::Mandatory),
            ),
        };

        match field_arg.default_arg() {
            None => arg,
            Some(DefaultArg::EmptyObject) => arg.default_value(juniper::InputValue::Object(vec![])),
        }
    }

    #[inline]
    pub fn get_type<I>(
        &mut self,
        type_ref: TypeRef,
        typing_purpose: TypingPurpose,
    ) -> juniper::Type<'r>
    where
        I: juniper::GraphQLType<GqlScalar> + juniper::GraphQLType<GqlScalar, TypeInfo = SchemaType>,
    {
        match type_ref.unit {
            UnitTypeRef::Addr(type_addr) => self.modified_type::<I>(
                &SchemaType {
                    schema_ctx: self.schema_ctx.clone(),
                    type_addr,
                    typing_purpose,
                },
                type_ref.modifier,
            ),
            UnitTypeRef::NativeScalar(scalar_ref) => match &scalar_ref.kind {
                NativeScalarKind::Unit => {
                    todo!("Unit type")
                }
                NativeScalarKind::Boolean => self.modified_type::<bool>(&(), type_ref.modifier),
                NativeScalarKind::Int(_) => self.modified_type::<i32>(&(), type_ref.modifier),
                NativeScalarKind::Number(_) => self.modified_type::<f64>(&(), type_ref.modifier),
                NativeScalarKind::String => {
                    self.modified_type::<std::string::String>(&(), type_ref.modifier)
                }
                NativeScalarKind::ID => self.modified_type::<juniper::ID>(&(), type_ref.modifier),
            },
        }
    }

    #[inline]
    fn modified_type<T>(
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

    fn modified_arg<T>(
        &mut self,
        name: &str,
        modifier: TypeModifier,
        type_info: &<T as GraphQLValue<GqlScalar>>::TypeInfo,
    ) -> juniper::meta::Argument<'r, GqlScalar>
    where
        T: juniper::GraphQLType<GqlScalar>
            + juniper::FromInputValue<GqlScalar>
            + juniper::GraphQLValue<GqlScalar>,
    {
        match modifier {
            TypeModifier::Unit(Optionality::Mandatory) => self.registry.arg::<T>(name, type_info),
            TypeModifier::Unit(Optionality::Optional) => {
                self.registry.arg::<Option<T>>(name, type_info)
            }
            TypeModifier::Array(Optionality::Mandatory, Optionality::Mandatory) => {
                self.registry.arg::<Vec<T>>(name, type_info)
            }
            TypeModifier::Array(Optionality::Mandatory, Optionality::Optional) => {
                self.registry.arg::<Vec<Option<T>>>(name, type_info)
            }
            TypeModifier::Array(Optionality::Optional, Optionality::Mandatory) => {
                self.registry.arg::<Option<Vec<T>>>(name, type_info)
            }
            TypeModifier::Array(Optionality::Optional, Optionality::Optional) => {
                self.registry.arg::<Option<Vec<Option<T>>>>(name, type_info)
            }
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct ArgumentFilter {
    deduplicate: bool,
    skip_subject: bool,
    skip_object: bool,
}

impl ArgumentFilter {
    fn filter_property(
        &self,
        name: &str,
        property_id: Option<PropertyId>,
        output: &Vec<juniper::meta::Argument<GqlScalar>>,
    ) -> bool {
        if let Some(property_id) = property_id {
            if self.skip_subject && matches!(property_id.role, Role::Subject) {
                return false;
            }
            if self.skip_object && matches!(property_id.role, Role::Object) {
                return false;
            }
        }

        if self.deduplicate {
            for arg in output {
                if arg.name == name {
                    return false;
                }
            }
        }

        true
    }
}
