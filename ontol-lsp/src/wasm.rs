use crate::Backend;
use futures_util::stream::TryStreamExt;
use js_sys::{AsyncIterator, Uint8Array};
use std::io::{Error, ErrorKind};
use tower_lsp::{LspService, Server};
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::stream::JsStream;
use web_sys::WritableStream;

#[wasm_bindgen]
pub struct WasmServer {
    into_server: AsyncIterator,
    from_server: WritableStream,
}

#[wasm_bindgen]
impl WasmServer {
    #[wasm_bindgen(constructor)]
    pub fn new(into_server: AsyncIterator, from_server: WritableStream) -> Self {
        Self {
            into_server,
            from_server,
        }
    }

    pub async fn start(self) -> std::result::Result<(), JsValue> {
        console_error_panic_hook::set_once();
        web_sys::console::log_1(&"Starting language server".into());

        let input = JsStream::from(self.into_server);
        let input = input
            .map_ok(|value| {
                value
                    .dyn_into::<Uint8Array>()
                    .expect("could not cast stream item to Uint8Array")
                    .to_vec()
            })
            .map_err(|_err| Error::from(ErrorKind::Other))
            .into_async_read();

        let output =
            JsCast::unchecked_into::<wasm_streams::writable::sys::WritableStream>(self.from_server);
        let output = wasm_streams::WritableStream::from_raw(output);
        let output = output.into_async_write();

        let (service, messages) = LspService::new(Backend::new);

        Server::new(input, output, messages).serve(service).await;

        Ok(())
    }
}
