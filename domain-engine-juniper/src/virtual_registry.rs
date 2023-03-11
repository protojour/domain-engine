use std::sync::Arc;

use juniper::GraphQLValue;

use crate::{
    gql_scalar::GqlScalar,
    new_templates::indexed_type::IndexedType,
    virtual_schema::{
        data::{
            NativeScalarRef, Optionality, TypeIndex, TypeKind, TypeModifier, TypeRef, UnitTypeRef,
        },
        VirtualIndexedTypeInfo, VirtualSchema,
    },
};

/// Juniper registry and domain adapter combined into
/// one type to enable a nice API
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
                    arguments: None,
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
}
