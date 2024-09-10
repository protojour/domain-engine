use crate::docs::get_core_completions;
use lsp_types::{CompletionItem, Location, MarkedString, Position, Range, Url};
use ontol_compiler::ontol_syntax::OntolTreeSyntax;
use ontol_compiler::{
    error::UnifiedCompileError,
    mem::Mem,
    topology::{DepGraphBuilder, DomainReference, GraphState, ParsedDomain},
    CompileError, SourceId, SourceSpan, Sources, NO_SPAN,
};
use ontol_parser::cst::inspect as insp;
use ontol_parser::cst::tree::{SyntaxNode, TreeNodeView, TreeTokenView};
use ontol_parser::cst::view::{self, NodeView, NodeViewExt, TokenView};
use ontol_parser::lexer::kind::Kind;
use ontol_parser::lexer::Lex;
use ontol_runtime::ontology::{config::DomainConfig, domain::Def, Ontology};
use ontol_runtime::DomainIndex;
use std::fmt::Debug;
use std::{
    collections::{HashMap, HashSet},
    format,
    io::Error,
    panic,
    sync::Arc,
};

type UsizeRange = std::ops::Range<usize>;

/// Language server state
#[derive(Clone)]
pub struct State {
    /// Current documents, by uri
    pub docs: HashMap<String, Document>,

    /// Doc uris indexed by SourceId, as seen in UnifiedCompileErrors
    pub srcref: HashMap<SourceId, String>,

    /// Ontology with only the `ontol` domain
    pub ontology: Arc<Ontology>,

    /// Fast lookup for ontology/domain Def
    pub ontol_def: HashMap<String, Def>,

    /// Prebuilt data structure
    pub core_completions: Vec<CompletionItem>,
}

impl Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("docs", &self.docs)
            .field("srcref", &self.srcref)
            .field("core_completions", &self.core_completions)
            .finish()
    }
}

/// A document and its various constituents
#[derive(Clone, Debug, Default)]
pub struct Document {
    /// Full uri as string
    pub uri: String,

    /// Full uri path, no filename
    pub path: String,

    /// Document name, no extension
    pub name: String,

    /// Document text
    pub text: Arc<String>,

    /// Unique symbols/names in this document
    pub symbols: HashSet<String>,

    /// Lexed document
    pub lex: Lex,

    /// Root syntax node
    pub cst_root_node: Option<SyntaxNode>,

    pub cst_defs: HashMap<String, SyntaxNode>,

    /// Use Statements
    pub cst_imports: Vec<SyntaxNode>,

    /// Package names by local alias
    pub aliases: HashMap<String, String>,
}

/// Helper struct for building hover documentation
#[derive(Clone, Debug, Default)]
pub struct HoverDoc {
    /// The "path" of the hovered item
    pub path: String,

    /// A stripped-down definition of the item (use, def, rel and fmt)
    pub signature: String,

    /// Markdown docs from doc comments, if any
    pub docs: String,

    /// Debug output, ignore
    pub(crate) debug: String,
}

impl State {
    /// Compile ontology for docs, prepare data, and return a new State
    pub fn new() -> Self {
        let mem = Mem::default();
        let ontology = ontol_compiler::compile(Default::default(), Default::default(), &mem)
            .unwrap()
            .into_ontology();

        let ontol_domain = ontology.domain_by_index(DomainIndex::ontol()).unwrap();
        let mut ontol_def = HashMap::new();

        for def in ontol_domain.defs() {
            if let Some(name) = def.ident() {
                let name = &ontology[name];
                ontol_def.insert(name.to_string(), def.clone());
            }
        }

        let core_completions = get_core_completions();

        Self {
            docs: Default::default(),
            srcref: Default::default(),
            ontology: Arc::new(ontology),
            ontol_def,
            core_completions,
        }
    }

    /// Parse ONTOL file, collecting tokens, statements and related data
    pub fn parse_statements(&mut self, uri: &str) {
        if let Some(doc) = self.docs.get_mut(uri) {
            doc.symbols.clear();
            doc.lex = Default::default();
            doc.cst_defs.clear();
            doc.cst_root_node = None;
            doc.cst_imports.clear();

            /// Recursively explore the AST, collecting defs and other statements
            fn cst_explore<'a, I: Iterator<Item = insp::Statement<TreeNodeView<'a>>>>(
                iter: I,
                aliases: &mut HashMap<String, String>,
                defs: &mut HashMap<String, SyntaxNode>,
            ) {
                for statement in iter {
                    match statement {
                        insp::Statement::DomainStatement(_) => {}
                        insp::Statement::UseStatement(stmt @ insp::UseStatement(_view)) => {
                            if let Some(Ok(name)) = stmt.name().and_then(|name| name.text()) {
                                if let Some(ident_path) = stmt.ident_path() {
                                    if let Some(last_sym) = ident_path.symbols().last() {
                                        aliases.insert(last_sym.slice().to_string(), name);
                                    }
                                }
                            }
                        }
                        insp::Statement::DefStatement(stmt @ insp::DefStatement(view)) => {
                            if let Some(path) = stmt.ident_path() {
                                if let Some(sym) = path.symbols().last() {
                                    defs.insert(
                                        sym.slice().to_string(),
                                        view.syntax_node().clone(),
                                    );
                                }
                            }

                            if let Some(body) = stmt.body() {
                                cst_explore(body.statements(), aliases, defs);
                            }
                        }
                        insp::Statement::SymStatement(_) => {}
                        insp::Statement::ArcStatement(_) => {}
                        insp::Statement::RelStatement(stmt) => {
                            if let Some(set) = stmt.fwd_set() {
                                for relation in set.relations() {
                                    if let Some(params) = relation.rel_params() {
                                        cst_explore(params.statements(), aliases, defs);
                                    }
                                }
                            }
                        }
                        insp::Statement::FmtStatement(_) => {}
                        insp::Statement::MapStatement(_) => {}
                    }
                }
            }

            let (flat_syntax_tree, _) = ontol_parser::cst_parse(&doc.text);
            let syntax_tree = flat_syntax_tree.unflatten();

            if let insp::Node::Ontol(ontol) = syntax_tree.view(&doc.text).node() {
                cst_explore(ontol.statements(), &mut doc.aliases, &mut doc.cst_defs);
            }

            let (root_node, lex) = syntax_tree.split();

            doc.cst_root_node = Some(root_node);
            doc.lex = lex;
        }
    }

    /// Build package graph from the given root and compile topology
    pub fn compile(&mut self, root_uri: &str) -> Result<(), UnifiedCompileError> {
        let (root_path, filename) = get_path_and_name(root_uri);
        let root_name = get_domain_name(filename);

        let mut ontol_sources = Sources::default();
        let mut package_graph_builder = DepGraphBuilder::with_roots([root_name.into()]);

        let topology = loop {
            match package_graph_builder.transition()? {
                GraphState::RequestPackages { builder, requests } => {
                    package_graph_builder = builder;

                    for request in requests {
                        let source_name = get_reference_name(&request.reference);
                        let package_config = DomainConfig::default();
                        let request_uri = build_uri(root_path, source_name);

                        if let Some(doc) = self.docs.get(&request_uri) {
                            let (flat_tree, errors) = ontol_parser::cst_parse(&doc.text);

                            let package = ParsedDomain::new(
                                request,
                                Box::new(OntolTreeSyntax {
                                    tree: flat_tree.unflatten(),
                                    source_text: doc.text.clone(),
                                }),
                                errors,
                                package_config,
                                &mut ontol_sources,
                            );
                            self.srcref.insert(package.src.id, request_uri);
                            package_graph_builder.provide_domain(package);
                        }
                    }
                }
                GraphState::Built(topology) => break topology,
            }
        };

        panic::catch_unwind(|| {
            let mem = Mem::default();
            ontol_compiler::compile(topology, ontol_sources.clone(), &mem).map(|_| ())
        })
        .unwrap_or_else(|err| {
            let message = if let Some(message) = err.downcast_ref::<&str>() {
                format!("Caught unexpected error: {message}")
            } else {
                "Caught unknown error.".to_string()
            };
            Err(UnifiedCompileError {
                errors: vec![CompileError::BUG(message).span(NO_SPAN)],
            })
        })
    }

    /// Get Document if given SourceId is known
    pub fn get_doc_by_sourceid(&self, source_id: &SourceId) -> Option<&Document> {
        match self.srcref.get(source_id) {
            Some(uri) => self.docs.get(uri),
            None => None,
        }
    }

    /// Get SourceId if source map points to the given uri
    pub fn get_sourceid_by_uri(&self, uri: &str) -> Option<SourceId> {
        for (key, val) in self.srcref.iter() {
            if val.as_str() == uri {
                return Some(*key);
            }
        }
        None
    }

    /// Get definition Location for the targeted statement
    pub fn get_definition(&self, uri: &str, pos: &Position) -> Option<Location> {
        let doc = self.docs.get(uri)?;
        let Ok(cursor): Result<u32, _> = get_byte_pos(&doc.text, pos).try_into() else {
            return None;
        };
        let root_node = doc.cst_root_node.as_ref()?;

        let (path, _) = locate_token(cursor, root_node.view(&doc.lex, &doc.text))?;

        // search up, from node to root
        for parent in path.into_iter().rev() {
            if let insp::Node::IdentPath(ident_path) = parent.node() {
                let lookup_path: Vec<String> = ident_path
                    .symbols()
                    .map(|sym| sym.slice().to_string())
                    .collect();

                match lookup_path.len() {
                    0 => {
                        return None;
                    }
                    1 => {
                        return self
                            .get_location_for_ident(doc, lookup_path.first().unwrap().as_str());
                    }
                    _ => return self.get_location_for_ext_ident(doc, &lookup_path),
                }
            }
        }

        None
    }

    /// Get Location given a local identifier
    fn get_location_for_ident(&self, doc: &Document, ident: &str) -> Option<Location> {
        let node = doc.cst_defs.get(ident)?;

        Some(Location {
            uri: Url::parse(&doc.uri).ok()?,
            range: get_range(&doc.text, &node.view(&doc.lex, &doc.text).span().into()),
        })
    }

    /// Get Location given an external identifier
    fn get_location_for_ext_ident(&self, doc: &Document, path: &[String]) -> Option<Location> {
        let root_alias = path.first()?;
        let ident = path.get(1)?;
        let root = doc.aliases.get(root_alias)?;

        let uri = build_uri(&doc.path, root);
        let other_doc = self.docs.get(&uri)?;

        self.get_location_for_ident(other_doc, ident)
    }

    /// Build hover documentation for the targeted statement
    pub fn get_hoverdoc(&self, uri: &str, pos: &Position) -> Option<HoverDoc> {
        let doc = self.docs.get(uri)?;
        let Ok(cursor): Result<u32, _> = get_byte_pos(&doc.text, pos).try_into() else {
            return None;
        };
        let root_node = doc.cst_root_node.as_ref()?;

        let (path, token) = locate_token(cursor, root_node.view(&doc.lex, &doc.text))?;

        match token.kind() {
            Kind::KwUse => return self.get_ontol_docs("use"),
            Kind::KwDef => return self.get_ontol_docs("def"),
            Kind::KwRel => return self.get_ontol_docs("rel"),
            Kind::KwFmt => return self.get_ontol_docs("fmt"),
            Kind::KwMap => return self.get_ontol_docs("map"),
            Kind::DotDot => return self.get_ontol_docs(".."),
            Kind::Question => return self.get_ontol_docs("?"),
            Kind::Modifier => {
                return self.get_ontol_docs(token.slice());
            }
            _ => {}
        }

        let mut hover = HoverDoc::default();

        fn hover_path_if_unset(hover: &mut HoverDoc, path: &str) {
            if hover.path.is_empty() {
                hover.path = path.to_string();
            }
        }

        // bottom-up search, from leaf token to root
        for parent in path.into_iter().rev() {
            match parent.node() {
                insp::Node::IdentPath(ident_path) => {
                    let lookup_path: Vec<String> = ident_path
                        .symbols()
                        .map(|sym| sym.slice().to_string())
                        .collect::<Vec<_>>();

                    match lookup_path.len() {
                        0 => {}
                        1 => {
                            return self.get_hoverdoc_for_ident(doc, &lookup_path[0]);
                        }
                        _ => return self.get_hoverdoc_for_ext_ident(doc, &lookup_path),
                    }
                }
                insp::Node::DomainStatement(_) => {
                    hover_path_if_unset(&mut hover, "domain");
                }
                insp::Node::UseStatement(_) => {
                    hover_path_if_unset(&mut hover, "use");
                }
                insp::Node::DefStatement(_) => {
                    hover_path_if_unset(&mut hover, "def");
                }
                insp::Node::RelStatement(_) => {
                    hover_path_if_unset(&mut hover, "rel");
                }
                insp::Node::FmtStatement(_) => {
                    hover_path_if_unset(&mut hover, "fmt");
                }
                _ => {}
            }

            if hover.docs.is_empty() {
                let mut doc_comments = parent.local_tokens_filter(Kind::DocComment).peekable();

                if doc_comments.peek().is_some() {
                    let lines = doc_comments
                        .map(|token| token.slice().strip_prefix("///").unwrap().to_string());

                    hover.docs = ontol_parser::join_doc_lines(lines).unwrap_or(Default::default());
                }
            }
        }

        hover_path_if_unset(&mut hover, &doc.name);

        Some(hover)
    }

    /// Get HoverDoc given a local identifier
    fn get_hoverdoc_for_ident(&self, doc: &Document, ident: &str) -> Option<HoverDoc> {
        let Some(def) = doc.cst_defs.get(ident) else {
            return self.get_ontol_docs(ident);
        };

        let insp::Node::DefStatement(stmt) = def.view(&doc.lex, &doc.text).node() else {
            return None;
        };

        let ident_path = stmt.ident_path()?;
        let last_segment = ident_path.symbols().last()?;
        let doc_comments = get_doc_comments(stmt.view());

        Some(HoverDoc {
            path: format!(
                "{name}.{ident}",
                name = doc.name,
                ident = last_segment.slice()
            ),
            signature: format!("def {ident}"),
            docs: doc_comments.unwrap_or(String::new()),
            ..Default::default()
        })
    }

    /// Get HoverDoc given an external identifier
    fn get_hoverdoc_for_ext_ident(&self, doc: &Document, path: &[String]) -> Option<HoverDoc> {
        let root_alias = path.first()?;
        let root = doc.aliases.get(root_alias)?;

        let uri = build_uri(&doc.path, root);
        let other_doc = self.docs.get(&uri)?;

        self.get_hoverdoc_for_ident(other_doc, path.get(1)?)
    }
}

impl HoverDoc {
    /// Build a HoverDoc from just path and docs, ignoring signature
    pub fn from(path: &str, docs: &str) -> Self {
        Self {
            path: path.to_string(),
            docs: docs.to_string(),
            ..Default::default()
        }
    }

    /// Convert HoverDoc a Vec of Markdown strings
    pub fn to_markdown_vec(&self) -> Vec<MarkedString> {
        let mut vec = vec![MarkedString::from_language_code(
            "ontol".to_string(),
            self.path.clone(),
        )];
        if !self.signature.is_empty() {
            vec.push(MarkedString::from_language_code(
                "ontol".to_string(),
                self.signature.clone(),
            ));
        }
        vec.push(MarkedString::from_markdown(self.docs.clone()));
        vec.push(MarkedString::from_markdown(self.debug.clone()));
        vec
    }
}

/// Read file contents for URI
pub fn read_file(uri: &str) -> Result<String, Error> {
    let path = get_base_path(uri);
    let text = std::fs::read_to_string(std::path::Path::new(&path))?;
    Ok(text)
}

/// Get source name for a PackageReference
pub fn get_reference_name(reference: &DomainReference) -> &str {
    match reference {
        DomainReference::Named(source_name) => source_name.as_str(),
    }
}

/// Split URI into schema/path and filename
pub fn get_path_and_name(uri: &str) -> (&str, &str) {
    match uri.rsplit_once('/') {
        Some((path, name)) => (path, name),
        None => ("", uri),
    }
}

/// Strip `file://` prefix and return the base path
pub fn get_base_path(uri: &str) -> &str {
    match uri.strip_prefix("file://") {
        Some(name) => name,
        None => uri,
    }
}

/// Strip `.on` suffix and return the base domain name
pub fn get_domain_name(filename: &str) -> &str {
    match filename.strip_suffix(".on") {
        Some(name) => name,
        None => filename,
    }
}

/// Rebuild URI from full schema/path and domain name
pub fn build_uri(path: &str, name: &str) -> String {
    format!("{}/{}.on", path, name)
}

/// Convert a byte index SourceSpan to a zero-based line/char Range
pub fn get_span_range(text: &str, span: &SourceSpan) -> Range {
    get_range(text, &span.span.into())
}

/// Convert a byte index Range to a zero-based line/char Range
pub fn get_range(text: &str, span: &UsizeRange) -> Range {
    let mut lsp_range = Range::default();
    let mut start_set = false;
    let mut cursor = 0;

    for (line_index, line) in text.lines().enumerate() {
        // utf-16 is LSP standard encoding
        let len = line.encode_utf16().count();
        if !start_set {
            if cursor + len < span.start {
                cursor += len + 1;
            } else {
                lsp_range.start.line = line_index as u32;
                lsp_range.start.character = (span.start - cursor) as u32;
                start_set = true
            }
        }
        if start_set {
            if cursor + len < span.end {
                cursor += len + 1;
            } else {
                lsp_range.end.line = line_index as u32;
                lsp_range.end.character = (span.end - cursor) as u32;
                break;
            }
        }
    }
    lsp_range
}

/// Convert line/col Position to byte index
fn get_byte_pos(text: &str, pos: &Position) -> usize {
    let mut cursor: usize = 0;
    for (line_index, line) in text.lines().enumerate() {
        if line_index == pos.line as usize {
            cursor += pos.character as usize;
            break;
        }
        cursor += line.encode_utf16().count() + 1;
    }
    cursor
}

/// Locate a token in the tree given a cursor position.
///
/// Also returns the syntax path (from top to bottom) leading to that token.
fn locate_token(pos: u32, parent: TreeNodeView) -> Option<(Vec<TreeNodeView>, TreeTokenView)> {
    fn search<'a>(
        pos: u32,
        path: &mut Vec<TreeNodeView<'a>>,
        parent: TreeNodeView<'a>,
    ) -> Option<TreeTokenView<'a>> {
        for sub in parent.children() {
            match sub {
                view::Item::Node(node) => {
                    if node.span().contains(pos) {
                        path.push(node);
                        return search(pos, path, node);
                    }
                }
                view::Item::Token(token) => {
                    if token.span().contains(pos) {
                        return Some(token);
                    }
                }
            }
        }

        None
    }

    let mut path = vec![];

    search(pos, &mut path, parent).map(|token| (path, token))
}

fn get_doc_comments(parent: impl NodeViewExt) -> Option<String> {
    let mut doc_comments = parent.local_tokens_filter(Kind::DocComment).peekable();

    if doc_comments.peek().is_some() {
        let lines =
            doc_comments.map(|token| token.slice().strip_prefix("///").unwrap().to_string());

        Some(ontol_parser::join_doc_lines(lines).unwrap_or(Default::default()))
    } else {
        None
    }
}
