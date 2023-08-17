use state::{get_builtins, get_span_range, Document, State};
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

    async fn diagnose(&self, uri: &Url) {
        let diags = self.get_diagnostics(uri).await;
        self.client
            .publish_diagnostics(uri.clone(), diags, None)
            .await;
    }

    async fn get_diagnostics(&self, uri: &Url) -> Vec<Diagnostic> {
        let state = self.state.read().await;
        let mut diags = vec![];
        match state.compile() {
            Ok(_) => {}
            Err(err) => {
                for err in err.errors {
                    // TODO: improve some fields here
                    match state.docs.get(uri.as_str()) {
                        Some(doc) => {
                            diags.push(Diagnostic {
                                range: get_span_range(&doc.text, &err.span),
                                severity: Some(DiagnosticSeverity::ERROR),
                                source: Some(NAME.to_string()),
                                message: err.error.to_string(),
                                related_information: Some(
                                    err.notes
                                        .iter()
                                        .map(|note| DiagnosticRelatedInformation {
                                            // TODO: might be wrong
                                            location: Location {
                                                uri: uri.clone(),
                                                range: get_span_range(&doc.text, &note.span),
                                            },
                                            message: note.note.to_string(),
                                        })
                                        .collect(),
                                ),
                                ..Default::default()
                            })
                        }
                        None => panic!("Doc should be readable at this point"),
                    }
                }
            }
        }
        diags
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
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
                        will_save: Some(false),
                        will_save_wait_until: Some(false),
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
            let mut state = self.state.write().await;
            // TODO: (pre-)compiler should figure out the root
            if state.root.is_none() {
                state.root = Some(params.text_document.uri.to_string())
            }
            state.docs.insert(
                params.text_document.uri.to_string(),
                Document {
                    text: params.text_document.text,
                    ..Default::default()
                },
            );
            state.parse_statements(params.text_document.uri.as_str());
        }
        self.diagnose(&params.text_document.uri).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        {
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
        self.diagnose(&params.text_document.uri).await;
    }

    async fn did_save(&self, params: DidSaveTextDocumentParams) {
        {
            let mut state = self.state.write().await;
            state.parse_statements(params.text_document.uri.as_str());
        }
        self.diagnose(&params.text_document.uri).await;
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        let mut state = self.state.write().await;
        state.docs.remove(params.text_document.uri.as_str());
        // TODO: set a new state
        if Some(params.text_document.uri.to_string()) == state.root {
            state.root = None
        }
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
}
