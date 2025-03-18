#![forbid(unsafe_code)]

use ontol_parser::cst::inspect as insp;
use ontol_parser::cst::view::{NodeView, NodeViewExt, TokenView, TypedView};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use state::{
    Document, State, build_uri, get_domain_name, get_path_and_name, get_range, get_span_range,
    read_file,
};
use tokio::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

mod docs;
mod state;

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
            state: RwLock::new(State::new()),
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
                        if let ontol_compiler::CompileError::Bug(err) = &err.error {
                            self.client.show_message(MessageType::ERROR, err).await;
                        }

                        // handle missing packages if any
                        let mut data: Value = json!({});
                        if let ontol_compiler::CompileError::DomainNotFound(domain_url) = &err.error
                        {
                            let (path, _) = get_path_and_name(url.as_str());
                            let source_name = domain_url.short_name();
                            let ref_uri = build_uri(path, source_name);

                            if domain_url.url().scheme() == "atlas" {
                                continue;
                            }

                            if let Ok(text) = read_file(&ref_uri) {
                                state.docs.insert(
                                    ref_uri.to_string(),
                                    Document {
                                        uri: ref_uri.to_string(),
                                        path: path.to_string(),
                                        name: source_name.to_string(),
                                        text: text.into(),
                                        ..Default::default()
                                    },
                                );
                                state.parse_statements(&ref_uri);
                                restart = true;
                            } else {
                                data = json!(OpenFileArgs {
                                    ref_uri: ref_uri.clone(),
                                    src_uri: url.to_string(),
                                });
                            }
                        }

                        // restart diagnostics if missing packages are resolved
                        if restart {
                            continue;
                        }

                        if let Some(doc) = state.get_doc_by_sourceid(&err.span().source_id) {
                            diagnostics.push(Diagnostic {
                                range: get_span_range(&doc.text, &err.span()),
                                severity: Some(DiagnosticSeverity::ERROR),
                                source: Some(NAME.to_string()),
                                message: err.error.to_string(),
                                related_information: Some(
                                    err.notes
                                        .into_iter()
                                        .map(|note| DiagnosticRelatedInformation {
                                            location: Location {
                                                uri: url.clone(),
                                                range: get_span_range(&doc.text, &note.span()),
                                            },
                                            message: note.into_note().to_string(),
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
                    commands: vec!["ontol.openMissingFile".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
        })
    }

    async fn shutdown(&self) -> Result<()> {
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
                    text: params.text_document.text.into(),
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
                    doc.text = change.text.into();
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
        let state = self.state.read().await;
        match state.docs.get(uri) {
            Some(doc) => {
                let mut builtin = state.core_completions.clone();
                let mut symbols = doc
                    .symbols
                    .iter()
                    .map(|s| CompletionItem {
                        label: s.to_string(),
                        kind: Some(CompletionItemKind::VARIABLE),
                        detail: Some(match doc.cst_defs.contains_key(s) {
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

        let Some(doc) = state.docs.get(uri) else {
            return Ok(None);
        };
        let Some(root_node) = doc.cst_root_node.as_ref() else {
            return Ok(None);
        };
        let insp::Node::Ontol(ontol) = root_node.view(&doc.lex, &doc.text).node() else {
            return Ok(None);
        };

        let mut symbols = Vec::<DocumentSymbol>::new();

        for statement in ontol.statements() {
            let (name, detail, kind) = match statement {
                insp::Statement::DomainStatement(_) => (
                    "domain".to_string(),
                    Some(doc.name.clone()),
                    SymbolKind::NAMESPACE,
                ),
                insp::Statement::UseStatement(stmt) => {
                    let uri = match stmt.uri() {
                        Some(uri) => match uri.text() {
                            Some(Ok(uri)) => uri,
                            _ => String::default(),
                        },
                        None => String::default(),
                    };
                    (format!("use {uri}"), None, SymbolKind::NAMESPACE)
                }
                insp::Statement::DefStatement(stmt) => {
                    let modifiers = stmt
                        .modifiers()
                        .filter_map(|m| m.token())
                        .map(|t| t.slice().to_string())
                        .collect::<Vec<_>>()
                        .join(" ");
                    let detail = (!modifiers.is_empty()).then_some(modifiers);
                    let id = match stmt.ident_path() {
                        Some(ident_path) => match ident_path.symbols().next() {
                            Some(sym) => sym.slice().to_string(),
                            None => String::default(),
                        },
                        None => String::default(),
                    };
                    (format!("def {id}"), detail, SymbolKind::STRUCT)
                }
                insp::Statement::SymStatement(stmt) => {
                    let syms = stmt
                        .sym_relations()
                        .map(|rel| match rel.decl() {
                            Some(decl) => match decl.symbol() {
                                Some(sym) => sym.slice().to_string(),
                                None => String::default(),
                            },
                            None => String::default(),
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    (format!("sym {syms}"), None, SymbolKind::PROPERTY)
                }
                insp::Statement::ArcStatement(stmt) => {
                    let id = match stmt.ident_path() {
                        Some(ident_path) => match ident_path.symbols().next() {
                            Some(sym) => sym.slice().to_string(),
                            None => String::default(),
                        },
                        None => String::default(),
                    };
                    (format!("arc {id}"), None, SymbolKind::INTERFACE)
                }
                insp::Statement::MapStatement(stmt) => {
                    let modifiers = stmt
                        .modifiers()
                        .filter_map(|m| m.token())
                        .map(|t| t.slice().to_string())
                        .collect::<Vec<_>>()
                        .join(" ");
                    let detail = (!modifiers.is_empty()).then_some(modifiers);
                    let id = match stmt.ident_path() {
                        Some(ident_path) => match ident_path.symbols().next() {
                            Some(sym) => sym.slice().to_string(),
                            None => "()".to_string(),
                        },
                        None => "()".to_string(),
                    };
                    (format!("map {id}"), detail, SymbolKind::INTERFACE)
                }
                _ => continue,
            };

            let range = get_range(&doc.text, &statement.view().span().into());

            #[expect(deprecated)]
            let symbol = DocumentSymbol {
                name,
                detail,
                kind,
                range,
                selection_range: range,
                tags: None,
                deprecated: None,
                children: None,
            };
            symbols.push(symbol);
        }

        Ok(Some(DocumentSymbolResponse::Nested(symbols)))
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
                    command: "ontol.openMissingFile".to_string(),
                    arguments: Some(vec![diag.data.clone().into()]),
                }),
                is_preferred: Some(true),
                ..Default::default()
            }));
        }
        Ok(Some(actions))
    }

    async fn execute_command(&self, params: ExecuteCommandParams) -> Result<Option<Value>> {
        if params.command.as_str() == "ontol.openMissingFile" {
            if let Some(val) = params.arguments.first() {
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
