use futures_util::stream::TryStreamExt;
use js_sys::{AsyncIterator, Uint8Array};
use lsp_types::request::{
    GotoDeclarationParams, GotoDeclarationResponse, GotoTypeDefinitionParams,
    GotoTypeDefinitionResponse,
};
use state::{get_builtins, Document, State};
use tokio::sync::RwLock;
use tower_lsp::jsonrpc::{Error as RpcError, Result};
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::stream::JsStream;
use web_sys::WritableStream;

mod state;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

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
        let output = output.try_into_async_write().map_err(|err| err.0)?;

        let (service, messages) = LspService::new(Backend::new);
        Server::new(input, output, messages).serve(service).await;

        Ok(())
    }
}

pub struct Backend {
    pub client: Client,
    state: RwLock<State>,
}

impl Backend {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            state: RwLock::new(State::default()),
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Options(
                    TextDocumentSyncOptions {
                        open_close: Some(true),
                        change: Some(TextDocumentSyncKind::FULL),
                        will_save: Some(false),
                        will_save_wait_until: Some(false),
                        save: Some(TextDocumentSyncSaveOptions::Supported(true)),
                    },
                )),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                completion_provider: Some(CompletionOptions {
                    resolve_provider: Some(true),
                    completion_item: Some(CompletionOptionsCompletionItem {
                        label_details_support: Some(true),
                    }),
                    ..Default::default()
                }),
                definition_provider: None,      // Some(OneOf::Left(true)),
                type_definition_provider: None, // Some(TypeDefinitionProviderCapability::Simple(true)),
                references_provider: None,      // Some(OneOf::Left(true)),
                workspace: None,                // Some(WorkspaceServerCapabilities {
                //     workspace_folders: Some(WorkspaceFoldersServerCapabilities {
                //         supported: Some(true),
                //         change_notifications: None,
                //     }),
                //     file_operations: None
                // }),
                ..Default::default()
            },
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::LOG, "Client and server initialized")
            .await;
    }

    async fn shutdown(&self) -> Result<()> {
        self.client
            .log_message(MessageType::LOG, "Shutting down language server")
            .await;
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let mut state = self.state.write().await;
        state.docs.insert(
            params.text_document.uri.to_string(),
            Document {
                text: params.text_document.text,
                ..Default::default()
            },
        );
        state.parse_statements(params.text_document.uri.as_str());
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let mut state = self.state.write().await;
        for change in params.content_changes {
            state.docs.insert(
                params.text_document.uri.to_string(),
                Document {
                    text: change.text,
                    ..Default::default()
                },
            );
        }
        state.parse_statements(params.text_document.uri.as_str());
    }

    async fn did_save(&self, params: DidSaveTextDocumentParams) {
        self.state
            .write()
            .await
            .parse_statements(params.text_document.uri.as_str());
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        self.state
            .write()
            .await
            .docs
            .remove(params.text_document.uri.as_str());
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = params
            .text_document_position_params
            .text_document
            .uri
            .as_str();
        let pos = params.text_document_position_params.position;
        match self.state.read().await.docs.get(uri) {
            Some(doc) => {
                let hd = doc.get_hover_docs(uri, pos.line as usize, pos.character as usize);
                Ok(Some(Hover {
                    contents: HoverContents::Array(vec![
                        MarkedString::from_language_code("ontol".to_string(), hd.path),
                        MarkedString::from_language_code("ontol".to_string(), hd.signature),
                        MarkedString::from_markdown(hd.docs),
                    ]),
                    range: None,
                }))
            }
            None => Ok(None),
        }
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let uri = params.text_document_position.text_document.uri.as_str();
        match self.state.read().await.docs.get(uri) {
            Some(doc) => {
                let mut builtin = get_builtins();
                let mut symbols = doc
                    .symbols
                    .iter()
                    .map(|s| CompletionItem {
                        label: s.to_string(),
                        kind: Some(CompletionItemKind::VARIABLE),
                        detail: Some(match doc.types.contains_key(s) {
                            true => format!("type {}", s),
                            false => s.to_string(),
                        }),
                        ..Default::default()
                    })
                    .collect::<Vec<CompletionItem>>();
                symbols.append(&mut builtin);
                Ok(Some(CompletionResponse::Array(symbols)))
            }
            None => Ok(None),
        }
    }

    async fn completion_resolve(&self, _params: CompletionItem) -> Result<CompletionItem> {
        Err(RpcError::method_not_found())
    }

    async fn goto_definition(
        &self,
        _params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        Err(RpcError::method_not_found())
    }

    async fn goto_type_definition(
        &self,
        _params: GotoTypeDefinitionParams,
    ) -> Result<Option<GotoTypeDefinitionResponse>> {
        Err(RpcError::method_not_found())
    }

    async fn references(&self, _params: ReferenceParams) -> Result<Option<Vec<Location>>> {
        Err(RpcError::method_not_found())
    }

    async fn goto_declaration(
        &self,
        _params: GotoDeclarationParams,
    ) -> Result<Option<GotoDeclarationResponse>> {
        Err(RpcError::method_not_found())
    }

    async fn document_link(
        &self,
        _params: DocumentLinkParams,
    ) -> Result<Option<Vec<DocumentLink>>> {
        Err(RpcError::method_not_found())
    }

    async fn document_link_resolve(&self, _params: DocumentLink) -> Result<DocumentLink> {
        Err(RpcError::method_not_found())
    }

    async fn document_symbol(
        &self,
        _params: DocumentSymbolParams,
    ) -> Result<Option<DocumentSymbolResponse>> {
        Err(RpcError::method_not_found())
    }

    async fn signature_help(&self, _params: SignatureHelpParams) -> Result<Option<SignatureHelp>> {
        Err(RpcError::method_not_found())
    }

    async fn code_action(&self, _params: CodeActionParams) -> Result<Option<CodeActionResponse>> {
        Err(RpcError::method_not_found())
    }
}
