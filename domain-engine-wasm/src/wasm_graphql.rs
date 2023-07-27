use std::sync::Arc;

use domain_engine_core::DomainEngine;
use domain_engine_juniper::{create_graphql_schema, juniper, GqlContext, Schema};
use ontol_runtime::{ontology::Ontology, PackageId};
use serde::Serialize;
use wasm_bindgen::prelude::*;

use crate::{wasm_error::WasmError, wasm_util::js_serializer};

#[wasm_bindgen]
pub struct WasmGraphqlSchema {
    pub(crate) ontology: Arc<Ontology>,
    pub(crate) schema: Schema,
    domain_engine: Arc<DomainEngine>,
}

#[wasm_bindgen]
impl WasmGraphqlSchema {
    pub(crate) fn create(
        ontology: Arc<Ontology>,
        package_id: PackageId,
    ) -> Result<Self, WasmError> {
        // Since the domain engine current gets created here,
        // its data store (if any) won't be shared with other interfaces.
        let domain_engine = DomainEngine::builder(ontology.clone()).build();

        let schema = create_graphql_schema(package_id, ontology.clone())?;

        Ok(Self {
            ontology,
            schema,
            domain_engine: Arc::new(domain_engine),
        })
    }

    pub async fn execute(
        &self,
        document: String,
        operation_name: Option<String>,
        variables: JsValue,
    ) -> Result<JsValue, WasmError> {
        let gql_context = GqlContext {
            domain_engine: self.domain_engine.clone(),
        };

        let juniper_variables = serde_wasm_bindgen::from_value(variables)?;

        let (value, execution_errors) = juniper::execute(
            &document,
            operation_name.as_deref(),
            &self.schema,
            &juniper_variables,
            &gql_context,
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
