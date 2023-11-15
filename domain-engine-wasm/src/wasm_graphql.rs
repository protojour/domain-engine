use std::sync::Arc;

use domain_engine_core::{data_store::DefaultDataStoreFactory, system::SystemAPI, DomainEngine};
use domain_engine_juniper::{create_graphql_schema, juniper, Schema};
use ontol_runtime::{ontology::Ontology, PackageId};
use serde::Serialize;
use wasm_bindgen::prelude::*;

use crate::{wasm_error::WasmError, wasm_util::js_serializer};

#[wasm_bindgen]
pub struct WasmGraphqlSchema {
    pub(crate) ontology: Arc<Ontology>,
    pub(crate) schema: Schema,
    service_ctx: domain_engine_juniper::context::ServiceCtx,
}

pub struct WasmSystem;

impl SystemAPI for WasmSystem {
    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

#[wasm_bindgen]
impl WasmGraphqlSchema {
    pub(crate) async fn create(
        ontology: Arc<Ontology>,
        package_id: PackageId,
    ) -> Result<Self, WasmError> {
        // Since the domain engine currently gets created here,
        // its data store (if any) won't be shared with other interfaces.
        let domain_engine = DomainEngine::builder(ontology.clone())
            .system(Box::new(WasmSystem))
            .build(DefaultDataStoreFactory)
            .await
            .map_err(|e| WasmError::Generic(format!("{e}")))?;

        let schema = create_graphql_schema(package_id, ontology.clone())?;

        Ok(Self {
            ontology,
            schema,
            service_ctx: Arc::new(domain_engine).into(),
        })
    }

    pub async fn execute(
        &self,
        document: String,
        operation_name: Option<String>,
        variables: JsValue,
    ) -> Result<JsValue, WasmError> {
        let juniper_variables = serde_wasm_bindgen::from_value(variables)?;

        let (value, execution_errors) = juniper::execute(
            &document,
            operation_name.as_deref(),
            &self.schema,
            &juniper_variables,
            &self.service_ctx,
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
