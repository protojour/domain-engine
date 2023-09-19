use std::sync::Arc;

use ontol_runtime::{
    interface::graphql::{
        data::{NativeScalarRef, TypeData, TypeIndex, TypeKind, UnitTypeRef},
        schema::{GraphqlSchema, QueryLevel, TypingPurpose},
    },
    ontology::Ontology,
    DefId, PackageId,
};

pub struct SchemaCtx {
    pub schema: Arc<GraphqlSchema>,
    pub ontology: Arc<Ontology>,
}

impl SchemaCtx {
    pub fn package_id(&self) -> PackageId {
        todo!()
    }

    pub fn indexed_type_info(
        self: &Arc<Self>,
        type_index: TypeIndex,
        typing_purpose: TypingPurpose,
    ) -> IndexedTypeInfo {
        IndexedTypeInfo {
            schema_ctx: self.clone(),
            type_index,
            typing_purpose,
        }
    }

    pub fn indexed_type_info_by_unit(
        self: &Arc<Self>,
        type_ref: UnitTypeRef,
        typing_purpose: TypingPurpose,
    ) -> Result<IndexedTypeInfo, NativeScalarRef> {
        self.lookup_type_index(type_ref)
            .map(|type_index| IndexedTypeInfo {
                schema_ctx: self.clone(),
                type_index,
                typing_purpose,
            })
    }

    pub fn query_type_info(self: &Arc<Self>) -> IndexedTypeInfo {
        self.indexed_type_info(self.schema.query, TypingPurpose::Selection)
    }

    pub fn mutation_type_info(self: &Arc<Self>) -> IndexedTypeInfo {
        self.indexed_type_info(self.schema.mutation, TypingPurpose::Selection)
    }

    pub fn type_data(&self, type_index: TypeIndex) -> &TypeData {
        &self.schema.types[type_index.0 as usize]
    }

    pub fn lookup_type_index(&self, type_ref: UnitTypeRef) -> Result<TypeIndex, NativeScalarRef> {
        match type_ref {
            UnitTypeRef::Indexed(type_index) => Ok(type_index),
            UnitTypeRef::NativeScalar(scalar_ref) => Err(scalar_ref),
        }
    }

    pub fn lookup_type_data(&self, type_ref: UnitTypeRef) -> Result<&TypeData, NativeScalarRef> {
        self.lookup_type_index(type_ref)
            .map(|type_index| self.type_data(type_index))
    }

    pub fn type_index_by_def(&self, def_id: DefId, query_level: QueryLevel) -> Option<TypeIndex> {
        self.schema
            .type_index_by_def
            .get(&(def_id, query_level))
            .cloned()
    }
}

#[derive(Clone)]
pub struct IndexedTypeInfo {
    pub schema_ctx: Arc<SchemaCtx>,
    pub type_index: TypeIndex,
    pub typing_purpose: TypingPurpose,
}

impl IndexedTypeInfo {
    pub fn ontology(&self) -> &Ontology {
        &self.schema_ctx.ontology
    }

    pub fn type_data(&self) -> &TypeData {
        self.schema_ctx.schema.type_data(self.type_index)
    }

    pub fn typename(&self) -> &str {
        let type_data = &self.type_data();
        match (&type_data.kind, self.typing_purpose) {
            (TypeKind::CustomScalar(_), _) => &type_data.typename,
            (_, TypingPurpose::Selection) => &type_data.typename,
            (_, TypingPurpose::Input | TypingPurpose::ReferenceInput) => {
                type_data.input_typename.as_deref().unwrap_or_else(|| {
                    panic!("No input typename available for `{}`", type_data.typename)
                })
            }
            (_, TypingPurpose::PartialInput) => type_data
                .partial_input_typename
                .as_deref()
                .expect("No partial input typename available"),
        }
    }

    pub fn description(&self) -> Option<String> {
        self.type_data().description(self.ontology())
    }
}
