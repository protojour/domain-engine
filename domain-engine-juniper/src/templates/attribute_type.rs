use std::{collections::BTreeMap, sync::Arc};

use juniper::graphql_value;
use ontol_runtime::{
    serde::processor::{ProcessorLevel, ProcessorMode},
    value::{Attribute, Data, PropertyId, Value},
    DefId,
};
use tracing::debug;

use crate::{
    gql_scalar::{GqlScalar, GqlScalarSerializer},
    templates::{resolve_virtual_schema_field, sequence_type::SequenceType},
    type_info::GraphqlTypeName,
    virtual_registry::VirtualRegistry,
    virtual_schema::{
        data::{
            FieldKind, ObjectData, ObjectKind, TypeData, TypeIndex, TypeKind, TypeRef, UnionData,
        },
        TypingPurpose, VirtualIndexedTypeInfo, VirtualSchema,
    },
    GqlContext,
};

#[derive(Clone, Copy)]
pub struct AttributeType<'v> {
    pub attr: &'v Attribute,
}

impl<'v> AttributeType<'v> {
    fn find_union_variant<'s>(
        &self,
        union_data: &'s UnionData,
        virtual_schema: &'s VirtualSchema,
    ) -> (TypeIndex, &'s TypeData) {
        for variant_type_index in &union_data.variants {
            let variant_type_data = virtual_schema.type_data(*variant_type_index);
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
}

impl<'v> ::juniper::GraphQLValue<GqlScalar> for AttributeType<'v> {
    type Context = GqlContext;
    type TypeInfo = VirtualIndexedTypeInfo;

    fn type_name<'i>(&self, type_info: &'i Self::TypeInfo) -> Option<&'i str> {
        Some(type_info.graphql_type_name())
    }

    fn concrete_type_name(&self, _context: &Self::Context, info: &Self::TypeInfo) -> String {
        match &info.type_data().kind {
            TypeKind::Union(union_data) => {
                let (_, variant_data) = self.find_union_variant(union_data, &info.virtual_schema);
                variant_data.typename.to_string()
            }
            _ => panic!(
                "should not need concrete type name for other things than unions or interfaces"
            ),
        }
    }

    fn resolve_into_type(
        &self,
        info: &Self::TypeInfo,
        _type_name: &str,
        selection_set: Option<&[juniper::Selection<GqlScalar>]>,
        executor: &juniper::Executor<Self::Context, GqlScalar>,
    ) -> juniper::ExecutionResult<GqlScalar> {
        match &info.type_data().kind {
            TypeKind::Union(union_data) => {
                let (variant_type_index, _) =
                    self.find_union_variant(union_data, &info.virtual_schema);
                self.resolve(
                    &info
                        .virtual_schema
                        .indexed_type_info(variant_type_index, TypingPurpose::Selection),
                    selection_set,
                    executor,
                )
            }
            _ => panic!("BUG: resolve_into_type called for non-union"),
        }
    }

    fn resolve_field(
        &self,
        info: &Self::TypeInfo,
        field_name: &str,
        _arguments: &juniper::Arguments<crate::gql_scalar::GqlScalar>,
        executor: &juniper::Executor<Self::Context, crate::gql_scalar::GqlScalar>,
    ) -> juniper::ExecutionResult<crate::gql_scalar::GqlScalar> {
        let virtual_schema = &info.virtual_schema;
        match &info.type_data().kind {
            TypeKind::Object(object_data) => {
                let field_data = object_data.fields.get(field_name).unwrap();
                let type_ref = field_data.field_type;

                debug!("resolve field `{field_name}`: {:?}", self.attr);

                match (&field_data.kind, &self.attr.value.data) {
                    (FieldKind::Edges, Data::Sequence(seq)) => resolve_virtual_schema_field(
                        SequenceType { seq },
                        virtual_schema
                            .indexed_type_info_by_unit(type_ref.unit, TypingPurpose::Selection)
                            .unwrap(),
                        executor,
                    ),
                    (FieldKind::Node, Data::Map(_)) => resolve_virtual_schema_field(
                        self,
                        virtual_schema
                            .indexed_type_info_by_unit(type_ref.unit, TypingPurpose::Selection)
                            .unwrap(),
                        executor,
                    ),
                    (
                        FieldKind::Connection {
                            property_id: Some(property_id),
                            ..
                        },
                        Data::Map(map),
                    ) => {
                        let type_info = virtual_schema
                            .indexed_type_info_by_unit(type_ref.unit, TypingPurpose::Selection)
                            .unwrap();

                        match map.get(property_id) {
                            Some(attribute) => resolve_virtual_schema_field(
                                AttributeType { attr: attribute },
                                type_info,
                                executor,
                            ),
                            None => {
                                let empty = Attribute {
                                    value: Value::new(Data::Sequence(vec![]), DefId::unit()),
                                    rel_params: Value::unit(),
                                };

                                resolve_virtual_schema_field(
                                    AttributeType { attr: &empty },
                                    type_info,
                                    executor,
                                )
                            }
                        }
                    }
                    (FieldKind::Property(property_data), Data::Map(map)) => {
                        debug!("lookup property {field_name}");
                        resolve_property(
                            map,
                            property_data.property_id,
                            type_ref,
                            &info.virtual_schema,
                            executor,
                        )
                    }
                    (FieldKind::Id(id_property_data), Data::Map(map)) => resolve_property(
                        map,
                        PropertyId::subject(id_property_data.relation_id),
                        type_ref,
                        &info.virtual_schema,
                        executor,
                    ),
                    (FieldKind::EdgeProperty(property_data), _) => {
                        match &self.attr.rel_params.data {
                            Data::Map(rel_map) => resolve_property(
                                rel_map,
                                property_data.property_id,
                                type_ref,
                                &info.virtual_schema,
                                executor,
                            ),
                            other => {
                                panic!("Tried to read edge property from {other:?}");
                            }
                        }
                    }
                    (field, value) => panic!("unhandled combination: ({field:?} ? {value:?})"),
                }
            }
            TypeKind::Union(_) => todo!("union"),
            TypeKind::CustomScalar(_) => {
                panic!("Scalar value must be resolved using a different type")
            }
        }
    }
}

impl<'v> juniper::GraphQLType<GqlScalar> for AttributeType<'v> {
    fn name(info: &Self::TypeInfo) -> Option<&str> {
        Some(info.graphql_type_name())
    }

    fn meta<'r>(
        info: &Self::TypeInfo,
        registry: &mut juniper::Registry<'r, GqlScalar>,
    ) -> juniper::meta::MetaType<'r, GqlScalar>
    where
        GqlScalar: 'r,
    {
        let mut reg = VirtualRegistry::new(&info.virtual_schema, registry);
        match &info.type_data().kind {
            TypeKind::Object(_) => {
                let fields = reg.get_fields(info.type_index);

                registry
                    .build_object_type::<Self>(info, &fields)
                    .into_meta()
            }
            TypeKind::Union(union_data) => {
                let types: Vec<_> = union_data
                    .variants
                    .iter()
                    .map(|type_index| {
                        reg.registry
                            .get_type::<AttributeType>(&VirtualIndexedTypeInfo {
                                virtual_schema: info.virtual_schema.clone(),
                                type_index: *type_index,
                                typing_purpose: TypingPurpose::Selection,
                            })
                    })
                    .collect();

                registry.build_union_type::<Self>(info, &types).into_meta()
            }
            TypeKind::CustomScalar(_) => todo!(),
        }
    }
}

fn resolve_property(
    map: &BTreeMap<PropertyId, Attribute>,
    property_id: PropertyId,
    type_ref: TypeRef,
    virtual_schema: &Arc<VirtualSchema>,
    _executor: &juniper::Executor<GqlContext, crate::gql_scalar::GqlScalar>,
) -> juniper::ExecutionResult<crate::gql_scalar::GqlScalar> {
    let attribute = match map.get(&property_id) {
        Some(attribute) => attribute,
        None => return Ok(graphql_value!(None)),
    };

    match virtual_schema.lookup_type_index(type_ref.unit) {
        Ok(_type_index) => {
            panic!("type_index property. TODO: resolve using IndexedType")
        }
        Err(scalar_ref) => {
            let scalar = virtual_schema
                .env()
                .new_serde_processor(
                    scalar_ref.operator_id,
                    None,
                    ProcessorMode::Select,
                    ProcessorLevel::Root,
                )
                .serialize_value(&attribute.value, None, GqlScalarSerializer)?;

            Ok(juniper::Value::Scalar(scalar))
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
