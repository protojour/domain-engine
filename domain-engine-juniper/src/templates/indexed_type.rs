use crate::{
    gql_scalar::GqlScalar,
    resolve::resolve_value,
    type_info::GraphqlTypeName,
    value_serializer::SerializedValue,
    virtual_registry::VirtualRegistry,
    virtual_schema::{
        data::{FieldKind, TypeKind},
        TypingPurpose, VirtualIndexedTypeInfo,
    },
    GqlContext,
};

pub struct IndexedType<'v> {
    pub value: &'v SerializedValue,
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

                match (self.value, &field_data.kind) {
                    (SerializedValue::Map(_, _map), FieldKind::Property(_property_id)) => {
                        todo!("property");
                    }
                    (SerializedValue::Connection(values), FieldKind::Edges) => {
                        let type_index = virtual_schema
                            .lookup_type_index(field_data.field_type.unit)
                            .unwrap();

                        resolve_value(
                            values
                                .iter()
                                .map(|value| IndexedType { value })
                                .collect::<Vec<_>>(),
                            virtual_schema.indexed_type_info(type_index, TypingPurpose::Selection),
                            executor,
                        )
                    }
                    other => panic!("unhandled combination: {other:?}"),
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
