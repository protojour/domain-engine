use std::sync::Arc;

use fnv::FnvHashMap;
use juniper::{graphql_value, FieldError};
use ontol_runtime::{
    interface::graphql::{
        data::{
            FieldKind, ObjectData, ObjectInterface, ObjectKind, TypeAddr, TypeData, TypeKind,
            TypeModifier, TypeRef, UnitTypeRef,
        },
        schema::TypingPurpose,
    },
    interface::serde::{
        operator::SerdeOperator,
        processor::{ProcessorLevel, ProcessorMode, ProcessorProfileFlags},
        serialize_raw,
    },
    property::PropertyId,
    sequence::{Sequence, SubSequence},
    value::{Attribute, Value},
    DefId,
};
use serde::Serialize;
use tracing::{trace_span, warn};

use crate::{
    context::{SchemaCtx, SchemaType},
    gql_scalar::GqlScalar,
    registry_ctx::RegistryCtx,
    templates::{
        page_info_type::PageInfoType, resolve_schema_type_field, sequence_type::SequenceType,
    },
    value_serializer::JuniperValueSerializer,
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

    fn concrete_type_name(&self, context: &ServiceCtx, info: &SchemaType) -> String {
        let (_, type_data) = self.resolve_to_concrete_type_data(info);
        context.domain_engine.ontology()[type_data.typename].to_string()
    }

    fn resolve_into_type(
        &self,
        info: &SchemaType,
        _type_name: &str,
        selection_set: Option<&[juniper::Selection<GqlScalar>]>,
        executor: &juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::ExecutionResult<GqlScalar> {
        let (type_addr, _) = self.resolve_to_concrete_type_data(info);
        self.resolve(
            &info
                .schema_ctx
                .get_schema_type(type_addr, TypingPurpose::Selection),
            selection_set,
            executor,
        )
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
                self.resolve_object_field(field_name, object_data, &info.schema_ctx, executor)
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
            TypeKind::Object(object_data) => {
                let fields = reg.get_fields(info.type_addr);
                if fields.is_empty() {
                    let mut builder = registry.build_scalar_type::<InputType>(info);
                    if let Some(docs) = info.docs_str() {
                        builder = builder.description(docs);
                    }
                    builder.into_meta()
                } else {
                    match &object_data.interface {
                        ObjectInterface::Implements(interfaces) => {
                            let implements_interfaces: Vec<_> = interfaces
                                .iter()
                                .map(|addr| {
                                    reg.get_type::<AttributeType>(
                                        TypeRef::mandatory(UnitTypeRef::Addr(*addr)),
                                        TypingPurpose::Selection,
                                    )
                                })
                                .collect();

                            let mut builder = reg
                                .registry
                                .build_object_type::<Self>(info, &fields)
                                .interfaces(&implements_interfaces);

                            if let Some(docs) = info.docs_str() {
                                builder = builder.description(docs);
                            }
                            builder.into_meta()
                        }
                        ObjectInterface::Interface => {
                            if let Some(implementors) = reg
                                .schema_ctx
                                .schema
                                .interface_implementors
                                .get(&info.type_addr)
                            {
                                for implementor in implementors {
                                    // ensure instantiation of all implementors
                                    reg.get_type::<AttributeType>(
                                        TypeRef::mandatory(UnitTypeRef::Addr(implementor.addr)),
                                        TypingPurpose::Selection,
                                    );
                                }
                            }

                            let mut builder = registry.build_interface_type::<Self>(info, &fields);
                            if let Some(docs) = info.docs_str() {
                                builder = builder.description(docs);
                            }
                            builder.into_meta()
                        }
                    }
                }
            }
            TypeKind::Union(union_data) => {
                let types: Vec<_> = union_data
                    .variants
                    .iter()
                    .map(|type_addr| {
                        reg.registry.get_type::<AttributeType>(&SchemaType {
                            schema_ctx: info.schema_ctx.clone(),
                            type_addr: *type_addr,
                            typing_purpose: TypingPurpose::Selection,
                        })
                    })
                    .collect();

                let mut builder = registry.build_union_type::<Self>(info, &types);
                if let Some(docs) = info.docs_str() {
                    builder = builder.description(docs);
                }
                builder.into_meta()
            }
            TypeKind::CustomScalar(_) => {
                let mut builder = registry.build_scalar_type::<InputType>(info);
                if let Some(docs) = info.docs_str() {
                    builder = builder.description(docs);
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
    fn resolve_to_concrete_type_data<'s>(&self, info: &'s SchemaType) -> (TypeAddr, &'s TypeData) {
        match &info.type_data().kind {
            TypeKind::Union(union_data) => {
                for variant_type_addr in &union_data.variants {
                    let variant_type_data = info.schema_ctx.schema.type_data(*variant_type_addr);
                    match &variant_type_data.kind {
                        TypeKind::Object(ObjectData {
                            kind: ObjectKind::Node(node_data),
                            ..
                        }) => {
                            if node_data.def_id == self.attr.val.type_def_id() {
                                return (*variant_type_addr, variant_type_data);
                            }
                        }
                        _ => panic!("Unsupported union variant"),
                    }
                }

                panic!(
                    "union variant not found for ({value:?}) {variants:?}",
                    value = self.attr.val,
                    variants = union_data.variants
                );
            }
            TypeKind::Object(object_data) => match &object_data.interface {
                ObjectInterface::Interface => {
                    let implementor = info
                        .schema_ctx
                        .downcast_interface(info.type_addr, &self.attr.val)
                        .expect("concrete implementation interface not found");
                    (
                        implementor.addr,
                        info.schema_ctx.schema.type_data(implementor.addr),
                    )
                }
                ObjectInterface::Implements(_) => {
                    panic!("already concrete")
                }
            },
            _ => panic!(
                "should not need concrete type name for other things than unions or interfaces"
            ),
        }
    }

    fn resolve_object_field(
        &self,
        field_name: &str,
        object_data: &ObjectData,
        schema_ctx: &Arc<SchemaCtx>,
        executor: &juniper::Executor<ServiceCtx, GqlScalar>,
    ) -> juniper::ExecutionResult<crate::gql_scalar::GqlScalar> {
        let field_data = object_data.fields.get(field_name).unwrap();
        let field_type = field_data.field_type;

        let _entered = trace_span!("rslv", name = field_name).entered();

        // trace!("resolve object field `{field_name}`: {:?}", self.attr);

        match (&self.attr.val, &field_data.kind) {
            (Value::Struct(..), FieldKind::Node) => resolve_schema_type_field(
                self,
                schema_ctx
                    .find_schema_type_by_unit(field_type.unit, TypingPurpose::Selection)
                    .unwrap(),
                executor,
            ),
            (Value::Unit(_), FieldKind::Node) => Ok(juniper::Value::Null),
            (Value::Struct(attrs, _), FieldKind::Property { id, .. }) => {
                resolve_property(attrs, *id, field_type, schema_ctx, executor)
            }
            (Value::Struct(attrs, _), FieldKind::FlattenedProperty { proxy, id, .. }) => {
                resolve_flattened_property(attrs, *proxy, *id, field_type, schema_ctx, executor)
            }
            (Value::Struct(attrs, _), FieldKind::ConnectionProperty { property_id, .. }) => {
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
                            rel: Value::unit(),
                            val: Value::Sequence(Sequence::default(), DefId::unit()),
                        };

                        resolve_schema_type_field(
                            AttributeType { attr: &empty },
                            type_info,
                            executor,
                        )
                    }
                }
            }
            (Value::Struct(attrs, _), FieldKind::Id(id_property_data)) => resolve_property(
                attrs,
                PropertyId::subject(id_property_data.relationship_id),
                field_type,
                schema_ctx,
                executor,
            ),
            (Value::Struct(attrs, _), FieldKind::OpenData) => {
                if !executor
                    .context()
                    .serde_processor_profile_flags
                    .contains(ProcessorProfileFlags::SERIALIZE_OPEN_DATA)
                {
                    return Err(FieldError::new(
                        "open data is not available in this GraphQL context",
                        graphql_value!(null),
                    ));
                }

                match attrs.get(
                    &schema_ctx
                        .ontology
                        .ontol_domain_meta()
                        .open_data_property_id(),
                ) {
                    Some(open_data_attr) => Ok(serialize_raw(
                        &open_data_attr.val,
                        &schema_ctx.ontology,
                        ProcessorLevel::new_root_with_recursion_limit(32),
                        JuniperValueSerializer,
                    )?),
                    None => Ok(juniper::Value::Null),
                }
            }
            (
                Value::Sequence(seq, _),
                FieldKind::Nodes | FieldKind::Edges | FieldKind::EntityMutation { .. },
            ) => resolve_schema_type_field(
                SequenceType { seq },
                schema_ctx
                    .find_schema_type_by_unit(field_type.unit, TypingPurpose::Selection)
                    .unwrap(),
                executor,
            ),
            (Value::Sequence(seq, _), FieldKind::PageInfo) => resolve_schema_type_field(
                PageInfoType { seq },
                schema_ctx
                    .find_schema_type_by_unit(field_type.unit, TypingPurpose::Selection)
                    .unwrap(),
                executor,
            ),
            (Value::Sequence(seq, _), FieldKind::TotalCount) => Ok(seq
                .sub()
                .and_then(SubSequence::total_len)
                .serialize(JuniperValueSerializer)?),
            (Value::Unit(_), FieldKind::Property { .. }) => Ok(juniper::Value::Null),
            (
                _,
                FieldKind::EdgeProperty {
                    id: property_id, ..
                },
            ) => match &self.attr.rel {
                Value::Struct(rel_attrs, _) => resolve_property(
                    rel_attrs.as_ref(),
                    *property_id,
                    field_type,
                    schema_ctx,
                    executor,
                ),
                other => {
                    panic!("BUG: Tried to read edge property from {other:?}");
                }
            },
            (value, FieldKind::Deleted) => {
                Ok(juniper::Value::Scalar(GqlScalar::Boolean(match value {
                    Value::I64(bool, _) => *bool != 0,
                    _ => false,
                })))
            }
            other => panic!("BUG: unhandled combination: {other:#?}"),
        }
    }
}

fn resolve_property(
    map: &FnvHashMap<PropertyId, Attribute>,
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

    match schema_ctx.lookup_type_by_addr(type_ref.unit) {
        Ok(type_addr) => {
            let schema_type = schema_ctx.get_schema_type(type_addr, TypingPurpose::Selection);
            match &schema_ctx.type_data(schema_type.type_addr).kind {
                TypeKind::CustomScalar(scalar_data) => Ok(schema_ctx
                    .ontology
                    .new_serde_processor(scalar_data.operator_addr, ProcessorMode::Read)
                    .serialize_value(&attribute.val, None, JuniperValueSerializer)?),
                TypeKind::Object(_) | TypeKind::Union(_) => {
                    match (type_ref.modifier, &attribute.val) {
                        (TypeModifier::Array { .. }, Value::Sequence(seq, _)) => {
                            resolve_schema_type_field(SequenceType { seq }, schema_type, executor)
                        }
                        _ => resolve_schema_type_field(
                            AttributeType { attr: attribute },
                            schema_type,
                            executor,
                        ),
                    }
                }
            }
        }
        Err(scalar_ref) => match (
            type_ref.modifier,
            &schema_ctx.ontology[scalar_ref.operator_addr],
        ) {
            (TypeModifier::Array { .. }, SerdeOperator::RelationList(operator)) => {
                let attributes = attribute.val.cast_ref::<Vec<_>>();
                let processor = schema_ctx
                    .ontology
                    .new_serde_processor(operator.range.addr, ProcessorMode::Read);

                let graphql_values: Vec<juniper::Value<GqlScalar>> = attributes
                    .iter()
                    .map(|attr| -> juniper::ExecutionResult<GqlScalar> {
                        Ok(processor.serialize_value(&attr.val, None, JuniperValueSerializer)?)
                    })
                    .collect::<Result<_, _>>()?;

                Ok(juniper::Value::List(graphql_values))
            }
            _ => Ok(schema_ctx
                .ontology
                .new_serde_processor(scalar_ref.operator_addr, ProcessorMode::Read)
                .serialize_value(&attribute.val, None, JuniperValueSerializer)?),
        },
    }
}

fn resolve_flattened_property(
    map: &FnvHashMap<PropertyId, Attribute>,
    proxy_property_id: PropertyId,
    property_id: PropertyId,
    type_ref: TypeRef,
    schema_ctx: &Arc<SchemaCtx>,
    executor: &juniper::Executor<ServiceCtx, crate::gql_scalar::GqlScalar>,
) -> juniper::ExecutionResult<crate::gql_scalar::GqlScalar> {
    let attribute = match map.get(&proxy_property_id) {
        Some(attribute) => attribute,
        None => {
            return Ok(graphql_value!(None));
        }
    };
    let Value::Struct(attrs, _) = &attribute.val else {
        warn!("flattened property not found through proxy");
        return Ok(graphql_value!(None));
    };

    resolve_property(attrs, property_id, type_ref, schema_ctx, executor)
}
