use std::collections::BTreeMap;

use juniper::graphql_value;
use ontol_runtime::{
    value::{Attribute, Data, PropertyId, Value},
    DefId,
};
use tracing::debug;

use crate::{
    gql_scalar::GqlScalar,
    resolve::resolve_indexed_type,
    type_info::GraphqlTypeName,
    virtual_registry::VirtualRegistry,
    virtual_schema::{
        data::{FieldKind, ObjectKind, TypeKind},
        TypingPurpose, VirtualIndexedTypeInfo,
    },
    GqlContext,
};

#[derive(Clone, Copy)]
pub struct IndexedType<'v> {
    pub attr: &'v Attribute,
}

impl<'v> ::juniper::GraphQLValue<GqlScalar> for IndexedType<'v> {
    type Context = GqlContext;
    type TypeInfo = VirtualIndexedTypeInfo;

    fn type_name<'i>(&self, type_info: &'i Self::TypeInfo) -> Option<&'i str> {
        Some(type_info.graphql_type_name())
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
                let unit_type_ref = field_data.field_type.unit;

                debug!("resolve field {field_name}");

                match (&field_data.kind, &object_data.kind, &self.attr.value.data) {
                    (FieldKind::Edges, _, Data::Sequence(seq)) => resolve_indexed_type(
                        seq.iter()
                            .map(|attribute| IndexedType { attr: attribute })
                            .collect::<Vec<_>>(),
                        virtual_schema
                            .indexed_type_info_by_unit(unit_type_ref, TypingPurpose::Selection)
                            .unwrap(),
                        executor,
                    ),
                    (FieldKind::Node, _, Data::Map(_)) => resolve_indexed_type(
                        self,
                        virtual_schema
                            .indexed_type_info_by_unit(unit_type_ref, TypingPurpose::Selection)
                            .unwrap(),
                        executor,
                    ),
                    (
                        FieldKind::Connection {
                            property_id: Some(property_id),
                            ..
                        },
                        _,
                        Data::Map(map),
                    ) => {
                        let type_info = virtual_schema
                            .indexed_type_info_by_unit(unit_type_ref, TypingPurpose::Selection)
                            .unwrap();

                        match map.get(property_id) {
                            Some(attribute) => resolve_indexed_type(
                                IndexedType { attr: attribute },
                                type_info,
                                executor,
                            ),
                            None => {
                                let empty = Attribute {
                                    value: Value::new(Data::Sequence(vec![]), DefId::unit()),
                                    rel_params: Value::unit(),
                                };

                                resolve_indexed_type(
                                    IndexedType { attr: &empty },
                                    type_info,
                                    executor,
                                )
                            }
                        }
                    }
                    (
                        FieldKind::Property(property_data),
                        ObjectKind::Node(_node_data),
                        Data::Map(map),
                    ) => {
                        debug!("lookup property {field_name}");
                        resolve_property(map, property_data.property_id, executor)
                    }
                    (
                        FieldKind::Id(_id_property_data),
                        ObjectKind::Node(_node_data),
                        Data::Map(_map),
                    ) => {
                        //resolve_property(map, PropertyId::subject(id_property_data.relation_id)),
                        Ok(graphql_value!("FIXME/test_id"))
                    }
                    (field, _, value) => panic!("unhandled combination: ({field:?} ? {value:?})"),
                }
            }
            TypeKind::Union(_) => todo!("union"),
            TypeKind::CustomScalar(_) => {
                panic!("Scalar value must be resolved using a different type")
            }
        }
    }
}

impl<'v> juniper::GraphQLType<GqlScalar> for IndexedType<'v> {
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
                            .get_type::<IndexedType>(&VirtualIndexedTypeInfo {
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

fn resolve_property(
    map: &BTreeMap<PropertyId, Attribute>,
    property_id: PropertyId,
    _executor: &juniper::Executor<GqlContext, crate::gql_scalar::GqlScalar>,
) -> juniper::ExecutionResult<crate::gql_scalar::GqlScalar> {
    match map.get(&property_id) {
        Some(_attribute) => {
            panic!("existing property");
        }
        None => Ok(graphql_value!(None)),
    }
}
