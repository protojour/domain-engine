use std::sync::Arc;

use domain_engine_juniper::{create_graphql_schema, juniper, GqlContext, Schema};
use ontol_runtime::{env::Env, PackageId};
use serde::Serialize;
use unimock::Unimock;
use wasm_bindgen::prelude::*;

use crate::{wasm_error::WasmError, wasm_util::js_serializer};

#[wasm_bindgen]
pub struct WasmGraphqlSchema {
    pub(crate) env: Arc<Env>,
    pub(crate) schema: Schema,
}

#[wasm_bindgen]
impl WasmGraphqlSchema {
    pub(crate) fn create(env: Arc<Env>, package_id: PackageId) -> Result<Self, WasmError> {
        let schema = create_graphql_schema(package_id, env.clone())?;
        Ok(Self { env, schema })
    }

    pub async fn execute(
        &self,
        document: String,
        operation_name: Option<String>,
        variables: JsValue,
    ) -> Result<JsValue, WasmError> {
        let gql_context = GqlContext {
            engine_api: Arc::new(Unimock::new(())),
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
            return Err(WasmError {
                msg: execution_errors
                    .into_iter()
                    .map(|error| format!("{error:?}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            });
        }

        Ok(value.serialize(&js_serializer())?)
    }
}
