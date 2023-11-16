use std::sync::Arc;

use anyhow::anyhow;
use domain_engine_core::{
    data_store::{
        DataStoreAPI, DataStoreFactorySync, DefaultDataStoreFactory, Request as DataStoreRequest,
        Response as DataStoreResponse,
    },
    DomainEngine, DomainError, DomainResult,
};

use ontol_runtime::{config::DataStoreConfig, ontology::Ontology, PackageId};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{
    wasm_domain::WasmDomain,
    wasm_error::{WasmError, WasmResult},
    wasm_gc::WasmWeak,
    wasm_graphql::WasmGraphqlSchema,
    WasmSystem,
};

/// Note: remember to free() the object from JS
#[wasm_bindgen]
pub struct WasmDomainEngine {
    pub(crate) domain_engine: Arc<DomainEngine>,
}

#[wasm_bindgen]
impl WasmDomainEngine {
    pub(crate) fn build(
        ontology: Arc<Ontology>,
        data_store_factory: WasmDataStoreFactory,
    ) -> WasmResult<Self> {
        let domain_engine = DomainEngine::builder(ontology.clone())
            .system(Box::new(WasmSystem))
            .build_sync(data_store_factory)
            .map_err(|e| WasmError::Generic(format!("{e}")))?;

        Ok(Self {
            domain_engine: Arc::new(domain_engine),
        })
    }

    pub async fn create_graphql_schema(
        &self,
        domain: &WasmDomain,
    ) -> Result<WasmGraphqlSchema, WasmError> {
        WasmGraphqlSchema::from_weak_domain_engine(
            WasmWeak::from_arc(&self.domain_engine),
            domain.package_id,
        )
        .await
    }
}

#[wasm_bindgen]
#[derive(Default)]
pub struct WasmDataStoreFactory {
    js_closure: Option<(JsValue, js_sys::Function)>,
}

#[wasm_bindgen]
impl WasmDataStoreFactory {
    /// Create a new default data store factory.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set up the data store factory for to produce the "closure" data store.
    /// This data store will call the specified js closure with a bincode request
    /// and expect a bincode response (optionally as a promise).
    pub fn set_closure(&mut self, this: JsValue, func: js_sys::Function) {
        self.js_closure = Some((this, func));
    }
}

impl DataStoreFactorySync for WasmDataStoreFactory {
    fn new_api_sync(
        &self,
        config: &DataStoreConfig,
        ontology: &Ontology,
        package_id: PackageId,
    ) -> anyhow::Result<Box<dyn DataStoreAPI + Send + Sync>> {
        match config {
            DataStoreConfig::ByName(name) if name == "closure" => {
                let (request_tx, request_rx) = tokio::sync::mpsc::channel(8);

                Ok(Box::new(ChannelDataStore {
                    request_tx,
                    worker_join_handle: tokio::task::spawn_local(request_executor_task(
                        request_rx,
                        self.js_closure
                            .clone()
                            .ok_or_else(|| anyhow!("No JS closure provided"))?,
                    )),
                }))
            }
            _ => DefaultDataStoreFactory.new_api_sync(config, ontology, package_id),
        }
    }
}

/// A channel request consists of a request and a oneshot sender for
/// sending back the result.
struct ChannelRequest(
    DataStoreRequest,
    tokio::sync::oneshot::Sender<anyhow::Result<DataStoreResponse>>,
);

/// The ChannelDataStore is needed bcause DataStoreAPI has to be provably thread-safe,
/// but js_sys values are not threadsafe, so the data store cannot own them.
///
/// Therefore we communicate over channel to a worker task running on the thread-local tokio executor.
struct ChannelDataStore {
    request_tx: tokio::sync::mpsc::Sender<ChannelRequest>,
    worker_join_handle: tokio::task::JoinHandle<()>,
}

#[async_trait::async_trait]
impl DataStoreAPI for ChannelDataStore {
    async fn execute(
        &self,
        request: DataStoreRequest,
        _engine: &DomainEngine,
    ) -> DomainResult<DataStoreResponse> {
        self.execute_over_channel(request)
            .await
            .map_err(DomainError::DataStore)
    }
}

impl ChannelDataStore {
    async fn execute_over_channel(
        &self,
        request: DataStoreRequest,
    ) -> anyhow::Result<DataStoreResponse> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.request_tx
            .send(ChannelRequest(request, response_tx))
            .await
            .map_err(|_| anyhow!("Failed to send request over channel"))?;

        response_rx
            .await
            .map_err(|_| anyhow!("Failed to receive response over channel"))?
    }
}

impl Drop for ChannelDataStore {
    fn drop(&mut self) {
        self.worker_join_handle.abort();
    }
}

async fn request_executor_task(
    mut request_rx: tokio::sync::mpsc::Receiver<ChannelRequest>,
    js_closure: (JsValue, js_sys::Function),
) {
    while let Some(ChannelRequest(request, response_tx)) = request_rx.recv().await {
        let result = invoke_request_closure(&js_closure, request).await;
        let _ = response_tx.send(result);
    }
}

async fn invoke_request_closure(
    (this, js_closure): &(JsValue, js_sys::Function),
    request: DataStoreRequest,
) -> anyhow::Result<DataStoreResponse> {
    let js_request_array = js_sys::Uint8Array::from(bincode::serialize(&request)?.as_slice());

    let js_response = {
        let value = js_closure
            .call1(this, &js_request_array)
            .map_err(|exception| anyhow!("JS exception: {exception:?}"))?;

        match value.dyn_into::<js_sys::Promise>() {
            Ok(promise) => JsFuture::from(promise)
                .await
                .map_err(|exception| anyhow!("JS promise exception: {exception:?}"))?,
            Err(response) => response,
        }
    };

    let bincode_buffer = js_response
        .dyn_into::<js_sys::Uint8Array>()
        .map_err(|_| anyhow!("JS return value expected Uint8Array"))?
        .to_vec();

    Ok(bincode::deserialize(&bincode_buffer)?)
}
