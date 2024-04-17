use std::sync::Arc;

use juniper::{GraphQLValue, ID};
use ontol_runtime::{
    debug::NoFmt,
    interface::graphql::{
        argument::{ArgKind, DomainFieldArg, FieldArg},
        data::{
            FieldKind, NativeScalarKind, Optionality, TypeAddr, TypeKind, TypeModifier, TypeRef,
            UnitTypeRef,
        },
        schema::{QueryLevel, TypingPurpose},
    },
    interface::{
        discriminator::{
            leaf_discriminant_scalar_union_for_has_attribute, LeafDiscriminantScalarUnion,
            VariantPurpose,
        },
        graphql::argument::DefaultArg,
        serde::{
            operator::{
                AppliedVariants, SerdeOperator, SerdeOperatorAddr, SerdePropertyFlags,
                SerdePropertyKind,
            },
            processor::ProcessorProfileFlags,
        },
    },
    property::{PropertyId, Role},
};
use tracing::{debug, trace, trace_span};

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

    pub fn build_input_object_meta_type(
        &mut self,
        info: &SchemaType,
        arguments: &[juniper::meta::Argument<'r, GqlScalar>],
    ) -> juniper::meta::MetaType<'r, GqlScalar> {
        let mut builder = if arguments.is_empty() {
            // Hack for empty input types
            let json_scalar = self.schema_ctx.get_schema_type(
                self.schema_ctx.schema.json_scalar,
                TypingPurpose::PartialInput,
            );
            let arguments = [self
                .registry
                .arg::<Option<InputType>>("_", &json_scalar)
                .description(
                "This argument is not a real argument; it acts as a marker for an input type without fields.",
            )];

            self.registry
                .build_input_object_type::<InputType>(info, &arguments)
        } else {
            self.registry
                .build_input_object_type::<InputType>(info, arguments)
        };

        if let Some(docs) = info.docs_str() {
            builder = builder.description(docs);
        }

        builder.into_meta()
    }

    pub fn get_fields(&mut self, type_addr: TypeAddr) -> Vec<juniper::meta::Field<'r, GqlScalar>> {
        // This is part of a big recursive algorithm, so iterator mapping is avoided
        let mut fields = vec![];

        let type_data = self.schema_ctx.schema.type_data(type_addr);

        if let TypeKind::Object(object) = &type_data.kind {
            for (name, field_data) in object.fields.iter() {
                let field = juniper::meta::Field {
                    name: name.string.as_str().into(),
                    description: match &field_data.kind {
                        FieldKind::Property {
                            id: property_id, ..
                        } => self
                            .schema_ctx
                            .ontology
                            .get_docs(property_id.relationship_id.0)
                            .map(|docs_constant| self.schema_ctx.ontology[docs_constant].into()),
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
        let serde_operator = &self.schema_ctx.ontology[operator_addr];

        match serde_operator {
            SerdeOperator::Struct(struct_op) => {
                let (mode, _) = typing_purpose.mode_and_level();

                for (key, property) in
                    struct_op.filter_properties(mode, None, ProcessorProfileFlags::default())
                {
                    if property.is_rel_params() {
                        continue;
                    }

                    if property.is_read_only()
                        && matches!(
                            typing_purpose,
                            TypingPurpose::Input | TypingPurpose::PartialInput
                        )
                    {
                        continue;
                    }

                    if !filter.filter_property(key.arc_str(), Some(property.property_id), output) {
                        continue;
                    }

                    let type_modifier = TypeModifier::Unit(match typing_purpose {
                        TypingPurpose::PartialInput => Optionality::Optional,
                        _ => {
                            if property.is_optional() || property.value_generator.is_some() {
                                Optionality::Optional
                            } else {
                                Optionality::Mandatory
                            }
                        }
                    });

                    let mut argument = match &property.kind {
                        SerdePropertyKind::Plain { .. } | SerdePropertyKind::FlatUnionData => self
                            .get_operator_argument(
                                key.arc_str(),
                                property.value_addr,
                                match &property.kind {
                                    SerdePropertyKind::Plain { rel_params_addr } => {
                                        *rel_params_addr
                                    }
                                    _ => None,
                                },
                                property.flags,
                                type_modifier,
                                typing_purpose,
                            ),
                        SerdePropertyKind::FlatUnionDiscriminator { union_addr } => {
                            let SerdeOperator::Union(union_op) =
                                &self.schema_ctx.ontology[*union_addr]
                            else {
                                continue;
                            };

                            let scalar_union = leaf_discriminant_scalar_union_for_has_attribute(
                                union_op.unfiltered_discriminators(),
                            );

                            if scalar_union == LeafDiscriminantScalarUnion::TEXT {
                                self.modified_arg::<String>(key.arc_str(), type_modifier, &())
                            } else if scalar_union == LeafDiscriminantScalarUnion::INT {
                                let i64_schema_type = self.schema_ctx.get_schema_type(
                                    self.schema_ctx.schema.i64_custom_scalar.unwrap(),
                                    TypingPurpose::Input,
                                );

                                self.modified_arg::<InputType>(
                                    key.arc_str(),
                                    type_modifier,
                                    &i64_schema_type,
                                )
                            } else {
                                continue;
                            }
                        }
                    };

                    if let Some(docs_constant) = self
                        .schema_ctx
                        .ontology
                        .get_docs(property.property_id.relationship_id.0)
                    {
                        argument = argument.description(&self.schema_ctx.ontology[docs_constant]);
                    }

                    output.push(argument);
                }

                Ok(())
            }
            SerdeOperator::Union(union_op) => {
                let (mode, level) = typing_purpose.mode_and_level();
                match union_op.applied_variants(mode, level) {
                    AppliedVariants::Unambiguous(operator_addr) => {
                        self.collect_operator_arguments(
                            operator_addr,
                            output,
                            typing_purpose,
                            filter,
                        )?;
                    }
                    AppliedVariants::OneOf(possible_variants) => {
                        debug!(
                            "UNION ARGUMENTS for {:?}",
                            NoFmt(union_op.unfiltered_variants())
                        );

                        // FIXME: Instead of just deduplicating the properties,
                        // the documentation for each of them should be combined in some way.
                        // When a union is used as an InputValue, the GraphQL functions purely
                        // as a documentation layer anyway.
                        for variant in possible_variants {
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

                        for variant in possible_variants {
                            match variant.purpose {
                                VariantPurpose::Data | VariantPurpose::RawDynamicEntity => {
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
                                VariantPurpose::Identification { .. } => {}
                            }
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
            SerdeOperator::IdSingletonStruct(_entity_id, property_name, id_operator_addr) => {
                let property_name = &self.schema_ctx.ontology[*property_name];
                if filter.filter_property(property_name, None, output) {
                    output.push(self.get_operator_argument(
                        property_name,
                        *id_operator_addr,
                        None,
                        SerdePropertyFlags::ENTITY_ID,
                        TypeModifier::Unit(Optionality::Optional),
                        typing_purpose,
                    ));
                }

                Ok(())
            }
            _ => Err(CollectOperatorError::Scalar),
        }
    }

    fn get_operator_argument(
        &mut self,
        name: &str,
        operator_addr: SerdeOperatorAddr,
        rel_params: Option<SerdeOperatorAddr>,
        property_flags: SerdePropertyFlags,
        modifier: TypeModifier,
        typing_purpose: TypingPurpose,
    ) -> juniper::meta::Argument<'r, GqlScalar> {
        let operator = &self.schema_ctx.ontology[operator_addr];

        let _entered = trace_span!("arg", name).entered();
        trace!(
            "register argument: {:?}",
            self.schema_ctx.ontology.debug(operator)
        );

        if property_flags.contains(SerdePropertyFlags::ENTITY_ID) {
            return self.modified_arg::<ID>(name, modifier, &());
        }

        match operator {
            SerdeOperator::AnyPlaceholder => {
                let json_scalar = self
                    .schema_ctx
                    .get_schema_type(self.schema_ctx.schema.json_scalar, typing_purpose);
                self.modified_arg::<InputType>(name, modifier, &json_scalar)
            }
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
            SerdeOperator::String(_) | SerdeOperator::Serial(_) => {
                return self.modified_arg::<String>(name, modifier, &())
            }
            SerdeOperator::StringConstant(_, _) => self.modified_arg::<String>(name, modifier, &()),
            SerdeOperator::TextPattern(_) => self.modified_arg::<String>(name, modifier, &()),
            SerdeOperator::CapturingTextPattern(_) => {
                self.modified_arg::<String>(name, modifier, &())
            }
            SerdeOperator::DynamicSequence => panic!("No dynamic sequence expected here"),
            SerdeOperator::RelationList(seq_op) | SerdeOperator::RelationIndexSet(seq_op) => {
                if seq_op.to_entity && matches!(typing_purpose, TypingPurpose::PartialInput) {
                    let type_addr = self
                        .schema_ctx
                        .type_addr_by_def(seq_op.def.def_id, QueryLevel::Connection { rel_params })
                        .unwrap_or_else(|| {
                            panic!("no PatchEdges available for relation sequence \"{name}\"")
                        });

                    self.modified_arg::<InputType>(
                        name,
                        TypeModifier::Unit(Optionality::Mandatory),
                        &self
                            .schema_ctx
                            .get_schema_type(type_addr, TypingPurpose::InputOrReference),
                    )
                } else {
                    let array_modifier = TypeModifier::Array {
                        array: modifier.unit_optionality(),
                        element: Optionality::Mandatory,
                    };

                    match self
                        .schema_ctx
                        .type_addr_by_def(seq_op.def.def_id, QueryLevel::Edge { rel_params })
                    {
                        Some(type_addr) => self.modified_arg::<InputType>(
                            name,
                            array_modifier,
                            &self
                                .schema_ctx
                                .get_schema_type(type_addr, TypingPurpose::InputOrReference),
                        ),
                        None => self.get_operator_argument(
                            name,
                            seq_op.range.addr,
                            rel_params,
                            property_flags,
                            array_modifier,
                            typing_purpose,
                        ),
                    }
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
                typing_purpose,
            ),
            SerdeOperator::Union(union_op) => {
                let def_id = union_op.union_def().def_id;
                let type_info = self.schema_ctx.ontology.get_type_info(def_id);

                // trace!(
                //     "union unfiltered variants: {:?}",
                //     union_op.unfiltered_variants()
                // );

                // If this is an entity, use Edge + ReferenceInput
                // to get the option of just specifying an ID.
                // TODO: Ensure this for create mutations only
                let (query_level, typing_purpose) = if type_info.entity_info().is_some() {
                    (
                        QueryLevel::Edge { rel_params: None },
                        TypingPurpose::InputOrReference,
                    )
                } else {
                    (QueryLevel::Node, TypingPurpose::Input)
                };

                let (mode, level) = typing_purpose.mode_and_level();

                match union_op.applied_variants(mode, level) {
                    AppliedVariants::Unambiguous(single_addr) => self.get_operator_argument(
                        name,
                        single_addr,
                        rel_params,
                        property_flags,
                        modifier,
                        typing_purpose,
                    ),
                    AppliedVariants::OneOf(_variants) => {
                        let type_addr = self
                            .schema_ctx
                            .type_addr_by_def(def_id, query_level)
                            .unwrap_or_else(|| {
                                panic!(
                                    "no union found: {def_id:?}. union_op={union_op:#?}",
                                    union_op = NoFmt(union_op)
                                )
                            });

                        let info = self.schema_ctx.get_schema_type(type_addr, typing_purpose);

                        match modifier.unit_optionality() {
                            Optionality::Mandatory => self.registry.arg::<InputType>(name, &info),
                            Optionality::Optional => {
                                self.registry.arg::<Option<InputType>>(name, &info)
                            }
                        }
                    }
                }
            }
            SerdeOperator::Struct(struct_op) => {
                let def_id = struct_op.def.def_id;
                let type_addr = self
                    .schema_ctx
                    .type_addr_by_def(def_id, QueryLevel::Node)
                    .unwrap_or_else(|| {
                        let type_info = self.schema_ctx.ontology.get_type_info(def_id);
                        panic!(
                            "struct not found for {def_id:?} {name:?}",
                            name = self.schema_ctx.ontology.debug(&type_info.name())
                        );
                    });

                let info = self
                    .schema_ctx
                    .get_schema_type(type_addr, TypingPurpose::Input);

                self.modified_arg::<InputType>(name, modifier, &info)
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
        let ontology = &self.schema_ctx.ontology;
        let mut arguments = vec![];

        match field_kind {
            FieldKind::ConnectionProperty(field) => {
                arguments.extend([
                    self.registry
                        .arg::<Option<i32>>(field.first_arg.name(ontology), &()),
                    self.registry
                        .arg::<Option<std::string::String>>(field.after_arg.name(ontology), &()),
                ]);
            }
            FieldKind::MapConnection(field) => {
                arguments.extend(self.get_domain_field_arg(&field.input_arg));
                arguments.extend([
                    self.registry
                        .arg::<Option<i32>>(field.first_arg.name(ontology), &()),
                    self.registry
                        .arg::<Option<std::string::String>>(field.after_arg.name(ontology), &()),
                ]);
            }
            FieldKind::MapFind(field) => {
                arguments.extend(self.get_domain_field_arg(&field.input_arg));
            }
            FieldKind::EntityMutation(field) => {
                if let Some(create_arg) = &field.create_arg {
                    arguments.extend(self.get_domain_field_arg(create_arg));
                }
                if let Some(update_arg) = &field.update_arg {
                    arguments.extend(self.get_domain_field_arg(update_arg));
                }
                if let Some(delete_arg) = &field.delete_arg {
                    arguments.extend(self.get_domain_field_arg(delete_arg));
                }
            }
            FieldKind::Deleted
            | FieldKind::Property { .. }
            | FieldKind::FlattenedPropertyDiscriminator { .. }
            | FieldKind::FlattenedProperty { .. }
            | FieldKind::EdgeProperty { .. }
            | FieldKind::Id(_)
            | FieldKind::Edges
            | FieldKind::Nodes
            | FieldKind::PageInfo
            | FieldKind::Node
            | FieldKind::TotalCount
            | FieldKind::OpenData
            | FieldKind::Version => {}
        }

        if arguments.is_empty() {
            None
        } else {
            Some(arguments)
        }
    }

    fn get_domain_field_arg(
        &mut self,
        field_arg: &dyn DomainFieldArg,
    ) -> Option<juniper::meta::Argument<'r, GqlScalar>> {
        let ontology = &self.schema_ctx.ontology;
        let arg = match field_arg.kind() {
            ArgKind::Addr(type_addr, modifier) => {
                let schema_type = self
                    .schema_ctx
                    .get_schema_type(type_addr, field_arg.typing_purpose());

                self.modified_arg::<InputType>(field_arg.name(ontology), modifier, &schema_type)
            }
            ArgKind::Operator(addr) => self.get_operator_argument(
                field_arg.name(ontology),
                addr,
                None,
                SerdePropertyFlags::default(),
                TypeModifier::Unit(Optionality::Mandatory),
                TypingPurpose::Selection,
            ),
            ArgKind::Hidden => return None,
        };

        Some(match field_arg.default_arg() {
            Some(default_arg) => arg.default_value(match default_arg {
                DefaultArg::EmptyObject => juniper::InputValue::Object(vec![]),
                DefaultArg::EmptyList => juniper::InputValue::List(vec![]),
            }),
            None => arg,
        })
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
            TypeModifier::Array { array, element } => match (array, element) {
                (Optionality::Mandatory, Optionality::Mandatory) => {
                    self.registry.get_type::<Vec<T>>(type_info)
                }
                (Optionality::Mandatory, Optionality::Optional) => {
                    self.registry.get_type::<Vec<Option<T>>>(type_info)
                }
                (Optionality::Optional, Optionality::Mandatory) => {
                    self.registry.get_type::<Option<Vec<T>>>(type_info)
                }
                (Optionality::Optional, Optionality::Optional) => {
                    self.registry.get_type::<Option<Vec<Option<T>>>>(type_info)
                }
            },
        }
    }

    pub fn modified_arg<T>(
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
            TypeModifier::Array { array, element } => match (array, element) {
                (Optionality::Mandatory, Optionality::Mandatory) => {
                    self.registry.arg::<Vec<T>>(name, type_info)
                }
                (Optionality::Mandatory, Optionality::Optional) => {
                    self.registry.arg::<Vec<Option<T>>>(name, type_info)
                }
                (Optionality::Optional, Optionality::Mandatory) => {
                    self.registry.arg::<Option<Vec<T>>>(name, type_info)
                }
                (Optionality::Optional, Optionality::Optional) => {
                    self.registry.arg::<Option<Vec<Option<T>>>>(name, type_info)
                }
            },
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
