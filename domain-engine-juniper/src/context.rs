//! The juniper implementation needs two different "context objects" to operate.
//!
//! They are both in this file.

use std::sync::Arc;

use domain_engine_core::DomainEngine;
use ontol_runtime::{
    interface::graphql::{
        data::{NativeScalarRef, TypeData, TypeIndex, TypeKind, UnitTypeRef},
        schema::{GraphqlSchema, QueryLevel, TypingPurpose},
    },
    ontology::Ontology,
    DefId, PackageId,
};

/// ServiceCtx represents the "backend" of the GraphQL schema.
///
/// it's responsible for resolving queries and mutations.
#[derive(Clone)]
pub struct ServiceCtx {
    pub domain_engine: Arc<DomainEngine>,
}

impl From<DomainEngine> for ServiceCtx {
    fn from(value: DomainEngine) -> Self {
        Self {
            domain_engine: Arc::new(value),
        }
    }
}

impl From<Arc<DomainEngine>> for ServiceCtx {
    fn from(value: Arc<DomainEngine>) -> Self {
        Self {
            domain_engine: value,
        }
    }
}

impl juniper::Context for ServiceCtx {}

/// SchemaCtx holds static information about the structure and types used in the GraphQL schema,
/// before it's hooked up to the ServiceCtx.
///
/// SchemaCtx contains the full ontol-runtime blueprint of the schema.
/// The juniper schema is built using this information.
pub struct SchemaCtx {
    pub schema: Arc<GraphqlSchema>,
    pub ontology: Arc<Ontology>,
}

impl SchemaCtx {
    pub fn package_id(&self) -> PackageId {
        todo!()
    }

    pub fn get_schema_type(
        self: &Arc<Self>,
        type_index: TypeIndex,
        typing_purpose: TypingPurpose,
    ) -> SchemaType {
        SchemaType {
            schema_ctx: self.clone(),
            type_index,
            typing_purpose,
        }
    }

    pub fn find_schema_type_by_unit(
        self: &Arc<Self>,
        type_ref: UnitTypeRef,
        typing_purpose: TypingPurpose,
    ) -> Result<SchemaType, NativeScalarRef> {
        self.lookup_type_index(type_ref)
            .map(|type_index| SchemaType {
                schema_ctx: self.clone(),
                type_index,
                typing_purpose,
            })
    }

    pub fn query_schema_type(self: &Arc<Self>) -> SchemaType {
        self.get_schema_type(self.schema.query, TypingPurpose::Selection)
    }

    pub fn mutation_schema_type(self: &Arc<Self>) -> SchemaType {
        self.get_schema_type(self.schema.mutation, TypingPurpose::Selection)
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

/// SchemaType represents a type that exists in the SchemaCtx.
///
/// i.e. it does not represent a NativeScalar, as that's not defined by the schema itself.
///
/// SchemaType represents a type by using a TypeIndex and a TypingPurpose,
/// and by looking up this in the SchemaCtx, finds metadata about the type that it represents.
#[derive(Clone)]
pub struct SchemaType {
    pub schema_ctx: Arc<SchemaCtx>,
    pub type_index: TypeIndex,
    pub typing_purpose: TypingPurpose,
}

impl SchemaType {
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
            (_, TypingPurpose::Input | TypingPurpose::InputOrReference) => {
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
