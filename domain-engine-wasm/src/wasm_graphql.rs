use std::sync::Arc;

use domain_engine_core::{data_store::DefaultDataStoreFactory, DomainEngine};
use domain_engine_juniper::{create_graphql_schema, juniper, Schema};
use ontol_runtime::{ontology::Ontology, PackageId};
use serde::Serialize;
use wasm_bindgen::prelude::*;

use crate::{
    wasm_error::WasmError,
    wasm_gc::{WasmGc, WasmWeak},
    wasm_util::js_serializer,
    WasmSystem,
};

/// Note: remember to free() the object from JS
#[wasm_bindgen]
pub struct WasmGraphqlSchema {
    pub(crate) schema: Option<Schema>,
    domain_engine: WasmGc<DomainEngine>,
}

#[wasm_bindgen]
impl WasmGraphqlSchema {
    pub(crate) async fn from_weak_domain_engine(
        domain_engine: WasmWeak<DomainEngine>,
        package_id: PackageId,
    ) -> Result<Self, WasmError> {
        Ok(Self {
            schema: Some(create_graphql_schema(
                package_id,
                domain_engine.upgrade()?.ontology_owned(),
            )?),
            domain_engine: WasmGc::Weak(domain_engine),
        })
    }

    pub(crate) async fn from_ontology(
        ontology: WasmWeak<Ontology>,
        package_id: PackageId,
    ) -> Result<Self, WasmError> {
        let ontology = ontology.upgrade()?;
        // Since the domain engine currently gets created here,
        // its data store (if any) won't be shared with other interfaces.
        let domain_engine = DomainEngine::builder(ontology.clone())
            .system(Box::new(WasmSystem))
            .build(DefaultDataStoreFactory)
            .await
            .map_err(|e| WasmError::Generic(format!("{e}")))?;

        Ok(Self {
            schema: Some(create_graphql_schema(package_id, ontology)?),
            domain_engine: WasmGc::Strong(Arc::new(domain_engine)),
        })
    }

    /// Release resources
    pub fn release(&mut self) -> bool {
        self.schema = None;
        self.domain_engine.release()
    }

    pub async fn execute(
        &self,
        document: String,
        operation_name: Option<String>,
        variables: JsValue,
    ) -> Result<JsValue, WasmError> {
        let juniper_variables = serde_wasm_bindgen::from_value(variables)?;

        let service_ctx: domain_engine_juniper::context::ServiceCtx =
            self.domain_engine.upgrade()?.into();

        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| WasmError::Generic("Schema has been released".to_string()))?;

        let (value, execution_errors) = juniper::execute(
            &document,
            operation_name.as_deref(),
            schema,
            &juniper_variables,
            &service_ctx,
        )
        .await?;

        if !execution_errors.is_empty() {
            return Err(WasmError::Generic(
                execution_errors
                    .into_iter()
                    .map(|error| format!("{error:?}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ));
        }

        Ok(value.serialize(&js_serializer())?)
    }
}
