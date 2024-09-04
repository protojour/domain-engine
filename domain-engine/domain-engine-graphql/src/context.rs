//! The juniper implementation needs two different "context objects" to operate.
//!
//! They are both in this file.

use std::sync::Arc;

use domain_engine_core::DomainEngine;
use ontol_runtime::{
    interface::{
        graphql::{
            data::{NativeScalarRef, TypeAddr, TypeData, TypeKind, UnitTypeRef},
            schema::{GraphqlSchema, InterfaceImplementor, QueryLevel, TypingPurpose},
        },
        serde::processor::ProcessorProfileFlags,
    },
    ontology::{ontol::TextConstant, Ontology},
    value::Value,
    DefId,
};
use tracing::debug;

/// ServiceCtx represents the "backend" of the GraphQL schema.
///
/// it's responsible for resolving queries and mutations.
#[derive(Clone)]
pub struct ServiceCtx {
    pub domain_engine: Arc<DomainEngine>,
    pub serde_processor_profile_flags: ProcessorProfileFlags,
    pub session: domain_engine_core::Session,
}

impl ServiceCtx {
    pub fn with_serde_processor_profile_flags(self, flags: ProcessorProfileFlags) -> Self {
        Self {
            serde_processor_profile_flags: flags,
            ..self
        }
    }
}

impl From<DomainEngine> for ServiceCtx {
    fn from(value: DomainEngine) -> Self {
        Self {
            domain_engine: Arc::new(value),
            serde_processor_profile_flags: Default::default(),
            session: Default::default(),
        }
    }
}

impl From<Arc<DomainEngine>> for ServiceCtx {
    fn from(value: Arc<DomainEngine>) -> Self {
        Self {
            domain_engine: value,
            serde_processor_profile_flags: Default::default(),
            session: Default::default(),
        }
    }
}

impl juniper::Context for ServiceCtx {}

/// SchemaCtx holds static information about the structure and types used in the GraphQL schema,
/// before it's hooked up to the ServiceCtx.
///
/// SchemaCtx contains the full ontol-runtime blueprint of the schema.
/// The juniper schema is built using this information.
pub(crate) struct SchemaCtx {
    pub schema: Arc<GraphqlSchema>,
    pub ontology: Arc<Ontology>,
}

impl SchemaCtx {
    pub fn ontology(&self) -> &Ontology {
        &self.ontology
    }

    pub fn get_schema_type(
        self: &Arc<Self>,
        type_addr: TypeAddr,
        typing_purpose: TypingPurpose,
    ) -> SchemaType {
        SchemaType {
            schema_ctx: self.clone(),
            type_addr,
            typing_purpose,
        }
    }

    pub fn find_schema_type_by_unit(
        self: &Arc<Self>,
        type_ref: UnitTypeRef,
        typing_purpose: TypingPurpose,
    ) -> Result<SchemaType, NativeScalarRef> {
        self.lookup_type_by_addr(type_ref)
            .map(|type_addr| SchemaType {
                schema_ctx: self.clone(),
                type_addr,
                typing_purpose,
            })
    }

    pub fn query_schema_type(self: &Arc<Self>) -> SchemaType {
        self.get_schema_type(self.schema.query, TypingPurpose::Selection)
    }

    pub fn mutation_schema_type(self: &Arc<Self>) -> SchemaType {
        self.get_schema_type(self.schema.mutation, TypingPurpose::Selection)
    }

    pub fn type_data(&self, type_addr: TypeAddr) -> &TypeData {
        &self.schema.types[type_addr.0 as usize]
    }

    pub fn type_data_by_typename(&self, typename: &str) -> Option<&TypeData> {
        self.schema
            .type_addr_by_typename
            .get(typename)
            .map(|addr| self.type_data(*addr))
    }

    pub fn lookup_type_by_addr(&self, type_ref: UnitTypeRef) -> Result<TypeAddr, NativeScalarRef> {
        match type_ref {
            UnitTypeRef::Addr(type_addr) => Ok(type_addr),
            UnitTypeRef::NativeScalar(scalar_ref) => Err(scalar_ref),
        }
    }

    pub fn lookup_type_data(&self, type_ref: UnitTypeRef) -> Result<&TypeData, NativeScalarRef> {
        self.lookup_type_by_addr(type_ref)
            .map(|type_addr| self.type_data(type_addr))
    }

    pub fn type_addr_by_def(&self, def_id: DefId, query_level: QueryLevel) -> Option<TypeAddr> {
        self.schema
            .type_addr_by_def
            .get(&(def_id, query_level))
            .cloned()
    }

    pub fn downcast_interface(
        &self,
        interface_addr: TypeAddr,
        value: &Value,
    ) -> Option<&InterfaceImplementor> {
        let implementors = self.schema.interface_implementors.get(&interface_addr)?;
        let Value::Struct(attrs, _) = value else {
            debug!("Must be a struct in order to implement an interface");
            return None;
        };

        implementors.iter().find(|implementor| {
            implementor
                .attribute_predicate
                .iter()
                .all(|(pred_relationship, pred_type)| {
                    attrs
                        .get(pred_relationship)
                        .and_then(|attr| attr.as_unit())
                        .map(|value| value.type_def_id() == *pred_type)
                        .unwrap_or(false)
                })
        })
    }
}

/// SchemaType represents a type that exists in the SchemaCtx.
///
/// i.e. it does not represent a NativeScalar, as that's not defined by the schema itself.
///
/// SchemaType represents a type by using a TypeAddr and a TypingPurpose,
/// and by looking up this in the SchemaCtx, finds metadata about the type that it represents.
#[derive(Clone)]
pub struct SchemaType {
    pub(crate) schema_ctx: Arc<SchemaCtx>,
    pub(crate) type_addr: TypeAddr,
    pub(crate) typing_purpose: TypingPurpose,
}

impl SchemaType {
    pub(crate) fn ontology(&self) -> &Ontology {
        &self.schema_ctx.ontology
    }

    pub(crate) fn type_data(&self) -> &TypeData {
        self.schema_ctx.schema.type_data(self.type_addr)
    }

    pub(crate) fn typename(&self) -> &str {
        let ontology = &self.schema_ctx.ontology;
        let type_data = &self.type_data();
        match (&type_data.kind, self.typing_purpose) {
            (TypeKind::CustomScalar(_), _) => &ontology[type_data.typename],
            (_, TypingPurpose::Selection | TypingPurpose::EntityId) => {
                &ontology[type_data.typename]
            }
            (_, TypingPurpose::Input | TypingPurpose::InputOrReference) => type_data
                .input_typename
                .map(|constant| &ontology[constant])
                .unwrap_or_else(|| {
                    panic!(
                        "No input typename available for `{}`",
                        &ontology[type_data.typename]
                    )
                }),
            (_, TypingPurpose::PartialInput) => type_data
                .partial_input_typename
                .map(|constant| &ontology[constant])
                .expect("No partial input typename available"),
        }
    }

    pub(crate) fn docs(&self) -> Option<TextConstant> {
        self.type_data().docs(self.ontology())
    }

    pub(crate) fn docs_str(&self) -> Option<&str> {
        self.docs()
            .map(|docs_constant| &self.schema_ctx.ontology[docs_constant])
    }
}
