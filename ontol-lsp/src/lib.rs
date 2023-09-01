use core::panic;

use state::{get_builtins, get_domain_name, get_path_and_name, get_span_range, Document, State};
use tokio::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

mod state;
#[cfg(feature = "wasm")]
pub mod wasm;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const NAME: &str = env!("CARGO_PKG_NAME");

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

    /// Diagnose from the given root document and publish findings to client
    async fn diagnose(&self, uri: &Url) {
        let diagnostics = self.get_diagnostics(uri).await;
        self.client
            .publish_diagnostics(uri.clone(), diagnostics, None)
            .await;
    }

    /// Compile code and convert errors to Diagnostics
    async fn get_diagnostics(&self, uri: &Url) -> Vec<Diagnostic> {
        let state = self.state.read().await;
        let mut diagnostics = vec![];

        match state.compile(uri.as_str()) {
            Ok(_) => {}
            Err(err) => {
                for err in err.errors {
                    match state.docs.get(uri.as_str()) {
                        Some(doc) => {
                            diagnostics.push(Diagnostic {
                                range: get_span_range(&doc.text, &err.span),
                                severity: Some(DiagnosticSeverity::ERROR),
                                source: Some(NAME.to_string()),
                                message: err.error.to_string(),
                                related_information: Some(
                                    err.notes
                                        .iter()
                                        .map(|note| DiagnosticRelatedInformation {
                                            location: Location {
                                                uri: uri.clone(),
                                                range: get_span_range(&doc.text, &note.span),
                                            },
                                            message: note.note.to_string(),
                                        })
                                        .collect(),
                                ),
                                ..Default::default()
                            });
                        }
                        None => panic!("Cannot find document {uri}"),
                    }
                }
            }
        }
        diagnostics
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, _params: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            server_info: Some(ServerInfo {
                name: NAME.to_string(),
                version: Some(VERSION.to_string()),
            }),
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Options(
                    TextDocumentSyncOptions {
                        open_close: Some(true),
                        change: Some(TextDocumentSyncKind::FULL),
                        will_save: None,
                        will_save_wait_until: None,
                        save: Some(TextDocumentSyncSaveOptions::Supported(true)),
                    },
                )),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                completion_provider: Some(CompletionOptions {
                    completion_item: Some(CompletionOptionsCompletionItem {
                        label_details_support: Some(true),
                    }),
                    ..Default::default()
                }),
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
        {
            let uri = params.text_document.uri.to_string();
            let mut state = self.state.write().await;
            let (path, filename) = get_path_and_name(uri.as_str());
            let name = get_domain_name(filename);

            state.roots.insert(uri.to_string());
            state.docs.insert(
                uri.to_string(),
                Document {
                    uri: uri.to_string(),
                    path: path.to_string(),
                    name: name.to_string(),
                    text: params.text_document.text,
                    ..Default::default()
                },
            );
            state.parse_statements(&uri);
        }
        self.diagnose(&params.text_document.uri).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        {
            let uri = params.text_document.uri.as_str();
            let mut state = self.state.write().await;
            match state.docs.get_mut(uri) {
                Some(doc) => {
                    for change in params.content_changes {
                        doc.text = change.text;
                    }
                }
                None => todo!(), // unlikely, but could happen
            }
            state.parse_statements(uri);
        }
        self.diagnose(&params.text_document.uri).await;
    }

    async fn did_save(&self, params: DidSaveTextDocumentParams) {
        {
            let uri = params.text_document.uri.as_str();
            let mut state = self.state.write().await;
            state.parse_statements(uri);
        }
        self.diagnose(&params.text_document.uri).await;
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        let uri = params.text_document.uri.as_str();
        let mut state = self.state.write().await;
        state.docs.remove(uri);
        state.roots.remove(uri);
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
                        detail: Some(match doc.defs.contains_key(s) {
                            true => format!("def {}", s),
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
}
