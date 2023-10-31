#![forbid(unsafe_code)]

use docs::get_core_completions;
use ontol_parser::ast::Statement;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use state::{
    build_uri, get_domain_name, get_path_and_name, get_range, get_reference_name, get_signature,
    get_span_range, parse_map_arm, read_file, Document, State,
};
use tokio::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

mod docs;
mod state;
#[cfg(feature = "wasm")]
pub mod wasm;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const NAME: &str = env!("CARGO_PKG_NAME");

pub struct Backend {
    pub client: Client,
    state: RwLock<State>,
    params: RwLock<InitializeParams>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct OpenFileArgs {
    ref_uri: String,
    src_uri: String,
}

impl Backend {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            state: RwLock::new(State::default()),
            params: RwLock::new(InitializeParams::default()),
        }
    }

    /// Initialize backend
    async fn init(&self, params: InitializeParams) {
        let mut p = self.params.write().await;
        *p = params
    }

    /// Diagnose from the given root document and publish findings to client
    async fn diagnose(&self, url: &Url) {
        let diagnostics = self.get_diagnostics(url).await;
        self.client
            .publish_diagnostics(url.clone(), diagnostics, None)
            .await;
    }

    /// Compile code and convert errors to Diagnostics
    async fn get_diagnostics(&self, url: &Url) -> Vec<Diagnostic> {
        let mut restart = true;
        let mut diagnostics = vec![];
        let mut state = self.state.write().await;

        while restart {
            diagnostics.clear();
            match state.compile(url.as_str()) {
                Ok(_) => break,
                Err(err) => {
                    restart = false;
                    for err in err.errors {
                        // handle caught panics
                        if let ontol_compiler::CompileError::BUG(err) = &err.error {
                            self.client.show_message(MessageType::ERROR, err).await;
                        }

                        // handle missing packages if any
                        let mut data: Value = json!({});
                        if let ontol_compiler::CompileError::PackageNotFound(reference) = &err.error
                        {
                            let (path, _) = get_path_and_name(url.as_str());
                            let source_name = get_reference_name(reference);
                            let ref_uri = build_uri(path, source_name);

                            if cfg!(feature = "wasm") {
                                data = json!(OpenFileArgs {
                                    ref_uri: ref_uri.clone(),
                                    src_uri: url.to_string(),
                                });
                            } else if let Ok(text) = read_file(&ref_uri) {
                                state.docs.insert(
                                    ref_uri.to_string(),
                                    Document {
                                        uri: ref_uri.to_string(),
                                        path: path.to_string(),
                                        name: source_name.to_string(),
                                        text,
                                        ..Default::default()
                                    },
                                );
                                state.parse_statements(&ref_uri);
                                restart = true;
                            }
                        }

                        // restart diagnostics if missing packages are resolved
                        if restart {
                            continue;
                        }

                        if let Some(doc) = state.get_doc_by_sourceid(&err.span.source_id) {
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
                                                uri: url.clone(),
                                                range: get_span_range(&doc.text, &note.span),
                                            },
                                            message: note.note.to_string(),
                                        })
                                        .collect(),
                                ),
                                data: Some(data),
                                ..Default::default()
                            });
                        }
                    }
                }
            }
        }
        diagnostics
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        self.init(params).await;

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
                definition_provider: Some(OneOf::Left(true)),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                completion_provider: Some(CompletionOptions {
                    completion_item: Some(CompletionOptionsCompletionItem {
                        label_details_support: Some(true),
                    }),
                    ..Default::default()
                }),
                document_symbol_provider: Some(OneOf::Left(true)),
                code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
                execute_command_provider: Some(ExecuteCommandOptions {
                    commands: vec!["ontol-lsp/openFile".to_string()],
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
            let uri = params.text_document.uri.as_str();
            let (path, filename) = get_path_and_name(uri);
            let name = get_domain_name(filename);
            let mut state = self.state.write().await;
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
            state.parse_statements(uri);
        }
        self.diagnose(&params.text_document.uri).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        {
            let uri = params.text_document.uri.as_str();
            let mut state = self.state.write().await;
            if let Some(doc) = state.docs.get_mut(uri) {
                for change in params.content_changes {
                    doc.text = change.text;
                }
                state.parse_statements(uri);
            }
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
        if let Some(src_id) = state.get_sourceid_by_uri(uri) {
            state.srcref.remove(&src_id);
        }
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let uri = params
            .text_document_position_params
            .text_document
            .uri
            .as_str();
        let pos = params.text_document_position_params.position;
        let state = self.state.read().await;
        match state.get_definition(uri, &pos) {
            Some(location) => Ok(Some(GotoDefinitionResponse::Scalar(location))),
            None => Ok(None),
        }
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = params
            .text_document_position_params
            .text_document
            .uri
            .as_str();
        let pos = params.text_document_position_params.position;
        let state = self.state.read().await;
        match state.get_hoverdoc(uri, &pos) {
            Some(doc_panel) => Ok(Some(Hover {
                contents: HoverContents::Array(doc_panel.to_markdown_vec()),
                range: None,
            })),
            None => Ok(None),
        }
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let uri = params.text_document_position.text_document.uri.as_str();
        match self.state.read().await.docs.get(uri) {
            Some(doc) => {
                let mut builtin = get_core_completions();
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

    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> Result<Option<DocumentSymbolResponse>> {
        let uri = params.text_document.uri.as_str();
        let state = self.state.read().await;
        match state.docs.get(uri) {
            #![allow(deprecated)]
            Some(doc) => {
                let mut symbols = Vec::<SymbolInformation>::with_capacity(doc.statements.len());
                for (stmt, range) in doc.statements.iter() {
                    let name = match stmt {
                        Statement::Use(stmt) => {
                            format!("use '{}' as {}", stmt.reference.0, stmt.as_ident.0)
                        }
                        Statement::Def(stmt) => match &stmt.public {
                            Some(_) => format!("def(pub) {}", stmt.ident.0),
                            None => format!("def {}", stmt.ident.0),
                        },
                        Statement::Rel(_) => {
                            let sig = get_signature(&doc.text, range, &state.regex);
                            let parens_stripped = state.regex.rel_parens.replace(&sig, "");
                            state
                                .regex
                                .rel_subject
                                .replace(&parens_stripped, "")
                                .to_string()
                        }
                        Statement::Fmt(_) => get_signature(&doc.text, range, &state.regex),
                        Statement::Map(stmt) => {
                            let first = parse_map_arm(&stmt.first.0);
                            let second = parse_map_arm(&stmt.second.0);
                            format!("map {} {}", first, second)
                        }
                    };
                    let kind = match stmt {
                        Statement::Use(_) => SymbolKind::NAMESPACE,
                        Statement::Def(_) => SymbolKind::STRUCT,
                        Statement::Rel(_) => SymbolKind::FIELD,
                        Statement::Fmt(_) => SymbolKind::CONSTRUCTOR,
                        Statement::Map(_) => SymbolKind::INTERFACE,
                    };
                    let symbol = SymbolInformation {
                        name,
                        kind,
                        tags: None,
                        deprecated: None,
                        location: Location {
                            uri: params.text_document.uri.clone(),
                            range: get_range(&doc.text, range),
                        },
                        container_name: None,
                    };
                    symbols.push(symbol);
                }
                Ok(Some(DocumentSymbolResponse::Flat(symbols)))
            }
            None => Ok(None),
        }
    }

    async fn code_action(&self, params: CodeActionParams) -> Result<Option<CodeActionResponse>> {
        if !cfg!(feature = "wasm") {
            return Ok(None);
        }
        let mut actions: Vec<CodeActionOrCommand> = vec![];
        for diag in params.context.diagnostics {
            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                title: "Open missing file...".to_string(),
                kind: Some(CodeActionKind::QUICKFIX),
                diagnostics: Some(vec![diag.clone()]),
                command: Some(Command {
                    title: "Open missing file...".to_string(),
                    command: "ontol-lsp.openFile".to_string(),
                    arguments: Some(vec![diag.data.clone().into()]),
                }),
                is_preferred: Some(true),
                ..Default::default()
            }));
        }
        Ok(Some(actions))
    }

    async fn execute_command(&self, params: ExecuteCommandParams) -> Result<Option<Value>> {
        if params.command.as_str() == "ontol-lsp.openFile" {
            if let Some(val) = params.arguments.get(0) {
                let args: OpenFileArgs = serde_json::from_value(val.clone()).unwrap_or_default();
                let uri = Url::parse(&args.ref_uri).unwrap();

                let result = self
                    .client
                    .show_document(ShowDocumentParams {
                        uri,
                        external: None,
                        take_focus: None,
                        selection: None,
                    })
                    .await;

                if result.is_ok() {
                    let uri = Url::parse(&args.src_uri).unwrap();
                    self.diagnose(&uri).await;
                }
            }
        }
        Ok(None)
    }
}
