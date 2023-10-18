use std::{collections::BTreeMap, sync::Arc};

use juniper::graphql_value;
use ontol_runtime::{
    interface::graphql::{
        data::{
            FieldKind, ObjectData, ObjectKind, TypeData, TypeIndex, TypeKind, TypeModifier,
            TypeRef, UnionData,
        },
        schema::TypingPurpose,
    },
    interface::serde::{
        operator::SerdeOperator,
        processor::{ProcessorLevel, ProcessorMode, DOMAIN_PROFILE},
    },
    value::{Attribute, Data, PropertyId, Value},
    DefId,
};
use tracing::trace;

use crate::{
    context::{SchemaCtx, SchemaType},
    gql_scalar::{GqlScalar, GqlScalarSerializer},
    registry_ctx::RegistryCtx,
    templates::{resolve_schema_type_field, sequence_type::SequenceType},
    ServiceCtx,
};

use super::input_type::InputType;

/// AttributeType combines two things:
///
/// 1. An ONTOL Attribute
/// 2. A SchemaType, metadata info about a GraphQL type
///
/// and combines this information to derive GraphQL resolver logic for the data.
///
/// AttributeType is an output type, not used for input values.
#[derive(Clone, Copy)]
pub struct AttributeType<'v> {
    pub attr: &'v Attribute,
}

impl<'v> ::juniper::GraphQLValue<GqlScalar> for AttributeType<'v> {
    type Context = ServiceCtx;
    type TypeInfo = SchemaType;

    fn type_name<'i>(&self, info: &'i SchemaType) -> Option<&'i str> {
        Some(info.typename())
    }

    fn concrete_type_name(&self, _context: &ServiceCtx, info: &SchemaType) -> String {
        match &info.type_data().kind {
            TypeKind::Union(union_data) => {
                let (_, variant_data) = self.find_union_variant(union_data, &info.schema_ctx);
                variant_data.typename.to_string()
            }
            _ => panic!(
                "should not need concrete type name for other things than unions or interfaces"
            ),
        }
    }

    fn resolve_into_type(
        &self,
        info: &SchemaType,
        _type_name: &str,
        selection_set: Option<&[juniper::Selection<GqlScalar>]>,
        executor: &juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::ExecutionResult<GqlScalar> {
        match &info.type_data().kind {
            TypeKind::Union(union_data) => {
                let (variant_type_index, _) = self.find_union_variant(union_data, &info.schema_ctx);
                self.resolve(
                    &info
                        .schema_ctx
                        .get_schema_type(variant_type_index, TypingPurpose::Selection),
                    selection_set,
                    executor,
                )
            }
            _ => panic!("BUG: resolve_into_type called for non-union"),
        }
    }

    fn resolve_field(
        &self,
        info: &SchemaType,
        field_name: &str,
        _arguments: &juniper::Arguments<crate::gql_scalar::GqlScalar>,
        executor: &juniper::Executor<Self::Context, crate::gql_scalar::GqlScalar>,
    ) -> juniper::ExecutionResult<crate::gql_scalar::GqlScalar> {
        match &info.type_data().kind {
            TypeKind::Object(object_data) => {
                self.resolve_object_field(&info.schema_ctx, object_data, field_name, executor)
            }
            TypeKind::Union(_) => panic!("union should be resolved earlier"),
            TypeKind::CustomScalar(_) => {
                panic!("Scalar value must be resolved using SerdeProcessor")
            }
        }
    }
}

impl<'v> juniper::GraphQLType<GqlScalar> for AttributeType<'v> {
    fn name(info: &SchemaType) -> Option<&str> {
        Some(info.typename())
    }

    fn meta<'r>(
        info: &SchemaType,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let mut reg = RegistryCtx::new(&info.schema_ctx, registry);
        match &info.type_data().kind {
            TypeKind::Object(_) => {
                let fields = reg.get_fields(info.type_index);
                let mut builder = registry.build_object_type::<Self>(info, &fields);
                if let Some(description) = info.description() {
                    builder = builder.description(&description);
                }
                builder.into_meta()
            }
            TypeKind::Union(union_data) => {
                let types: Vec<_> = union_data
                    .variants
                    .iter()
                    .map(|type_index| {
                        reg.registry.get_type::<AttributeType>(&SchemaType {
                            schema_ctx: info.schema_ctx.clone(),
                            type_index: *type_index,
                            typing_purpose: TypingPurpose::Selection,
                        })
                    })
                    .collect();

                registry.build_union_type::<Self>(info, &types).into_meta()
            }
            TypeKind::CustomScalar(_) => {
                let mut builder = registry.build_scalar_type::<InputType>(info);
                if let Some(description) = info.description() {
                    builder = builder.description(&description);
                }
                builder.into_meta()
            }
        }
    }
}

// Note: No need for async for now.
// in the future there might be a need for this.
/*
impl<'v> juniper::GraphQLValueAsync<GqlScalar> for IndexedType<'v> {
    /// TODO: Might implement resolve_async instead, so we can have just one query
    fn resolve_field_async<'a>(
        &'a self,
        info: &'a Self::TypeInfo,
        _field_name: &'a str,
        _arguments: &'a juniper::Arguments<GqlScalar>,
        _executor: &'a juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::BoxFuture<'a, juniper::ExecutionResult<GqlScalar>> {
        Box::pin(async move {
            match (&info.type_data().kind, self.value) {
                _ => Ok(graphql_value!(None)),
            }
        })
    }
}
*/

impl<'v> AttributeType<'v> {
    fn find_union_variant<'s>(
        &self,
        union_data: &'s UnionData,
        ctx: &'s SchemaCtx,
    ) -> (TypeIndex, &'s TypeData) {
        for variant_type_index in &union_data.variants {
            let variant_type_data = ctx.schema.type_data(*variant_type_index);
            match &variant_type_data.kind {
                TypeKind::Object(ObjectData {
                    kind: ObjectKind::Node(node_data),
                    ..
                }) => {
                    if node_data.def_id == self.attr.value.type_def_id {
                        return (*variant_type_index, variant_type_data);
                    }
                }
                _ => panic!("Unsupported union variant"),
            }
        }

        panic!("union variant not found")
    }

    fn resolve_object_field(
        &self,
        schema_ctx: &Arc<SchemaCtx>,
        object_data: &ObjectData,
        field_name: &str,
        executor: &juniper::Executor<ServiceCtx, GqlScalar>,
    ) -> juniper::ExecutionResult<crate::gql_scalar::GqlScalar> {
        let field_data = object_data.fields.get(field_name).unwrap();
        let field_type = field_data.field_type;

        trace!("resolve object field `{field_name}`: {:?}", self.attr);

        match (&field_data.kind, &self.attr.value.data) {
            (FieldKind::Edges, Data::Sequence(seq)) => resolve_schema_type_field(
                SequenceType { seq },
                schema_ctx
                    .find_schema_type_by_unit(field_type.unit, TypingPurpose::Selection)
                    .unwrap(),
                executor,
            ),
            (FieldKind::Node, Data::Struct(_)) => resolve_schema_type_field(
                self,
                schema_ctx
                    .find_schema_type_by_unit(field_type.unit, TypingPurpose::Selection)
                    .unwrap(),
                executor,
            ),
            (
                FieldKind::Connection {
                    property_id: Some(property_id),
                    ..
                },
                Data::Struct(attrs),
            ) => {
                let type_info = schema_ctx
                    .find_schema_type_by_unit(field_type.unit, TypingPurpose::Selection)
                    .unwrap();

                match attrs.get(property_id) {
                    Some(attribute) => resolve_schema_type_field(
                        AttributeType { attr: attribute },
                        type_info,
                        executor,
                    ),
                    None => {
                        let empty = Attribute {
                            value: Value::new(Data::Sequence(vec![]), DefId::unit()),
                            rel_params: Value::unit(),
                        };

                        resolve_schema_type_field(
                            AttributeType { attr: &empty },
                            type_info,
                            executor,
                        )
                    }
                }
            }
            (FieldKind::Property(property_data), Data::Struct(attrs)) => {
                trace!("lookup property {field_name} => {:?}", property_data);
                Self::resolve_property(
                    attrs,
                    property_data.property_id,
                    field_type,
                    schema_ctx,
                    executor,
                )
            }
            (FieldKind::Id(id_property_data), Data::Struct(attrs)) => Self::resolve_property(
                attrs,
                PropertyId::subject(id_property_data.relationship_id),
                field_type,
                schema_ctx,
                executor,
            ),
            (FieldKind::EdgeProperty(property_data), _) => match &self.attr.rel_params.data {
                Data::Struct(rel_attrs) => Self::resolve_property(
                    rel_attrs,
                    property_data.property_id,
                    field_type,
                    schema_ctx,
                    executor,
                ),
                other => {
                    panic!("BUG: Tried to read edge property from {other:?}");
                }
            },
            other => panic!("BUG: unhandled combination: {other:#?}"),
        }
    }

    fn resolve_property(
        map: &BTreeMap<PropertyId, Attribute>,
        property_id: PropertyId,
        type_ref: TypeRef,
        schema_ctx: &Arc<SchemaCtx>,
        executor: &juniper::Executor<ServiceCtx, crate::gql_scalar::GqlScalar>,
    ) -> juniper::ExecutionResult<crate::gql_scalar::GqlScalar> {
        let attribute = match map.get(&property_id) {
            Some(attribute) => attribute,
            None => {
                return Ok(graphql_value!(None));
            }
        };

        match schema_ctx.lookup_type_index(type_ref.unit) {
            Ok(type_index) => {
                let type_info = schema_ctx.get_schema_type(type_index, TypingPurpose::Selection);
                match &schema_ctx.type_data(type_info.type_index).kind {
                    TypeKind::CustomScalar(scalar_data) => {
                        let gql_scalar = schema_ctx
                            .ontology
                            .new_serde_processor(
                                scalar_data.operator_addr,
                                ProcessorMode::Read,
                                ProcessorLevel::new_root(),
                                &DOMAIN_PROFILE,
                            )
                            .serialize_value(&attribute.value, None, GqlScalarSerializer)?;

                        Ok(juniper::Value::Scalar(gql_scalar))
                    }
                    TypeKind::Object(_) | TypeKind::Union(_) => resolve_schema_type_field(
                        AttributeType { attr: attribute },
                        type_info,
                        executor,
                    ),
                }
            }
            Err(scalar_ref) => match (
                type_ref.modifier,
                schema_ctx
                    .ontology
                    .get_serde_operator(scalar_ref.operator_addr),
            ) {
                (TypeModifier::Array(..), SerdeOperator::RelationSequence(operator)) => {
                    let attributes = attribute.value.cast_ref::<Vec<_>>();
                    let processor = schema_ctx.ontology.new_serde_processor(
                        operator.ranges[0].addr,
                        ProcessorMode::Read,
                        ProcessorLevel::new_root(),
                        &DOMAIN_PROFILE,
                    );

                    let graphql_values: Vec<juniper::Value<GqlScalar>> = attributes
                        .iter()
                        .map(|attr| -> juniper::ExecutionResult<GqlScalar> {
                            let scalar = processor.serialize_value(
                                &attr.value,
                                None,
                                GqlScalarSerializer,
                            )?;
                            Ok(juniper::Value::Scalar(scalar))
                        })
                        .collect::<Result<_, _>>()?;

                    Ok(juniper::Value::List(graphql_values))
                }
                _ => {
                    let scalar = schema_ctx
                        .ontology
                        .new_serde_processor(
                            scalar_ref.operator_addr,
                            ProcessorMode::Read,
                            ProcessorLevel::new_root(),
                            &DOMAIN_PROFILE,
                        )
                        .serialize_value(&attribute.value, None, GqlScalarSerializer)?;

                    Ok(juniper::Value::Scalar(scalar))
                }
            },
        }
    }
}
