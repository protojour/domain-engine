use tower_lsp::{LspService, Server};
use wasm_bindgen::prelude::{*, JsCast};
use wasm_bindgen_futures::stream::JsStream;

pub mod server;

#[wasm_bindgen]
extern "C" {

}
