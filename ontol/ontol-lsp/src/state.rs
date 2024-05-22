use crate::docs::{get_core_completions, get_ontol_var, COMPLETIONS};
use crate::old_parser::{self, ast_lex, parse_statements, Spanned};
use crate::old_parser::{
    ast::{
        AnyPattern, DefStatement, MapArm, Path, Statement, StructPattern, StructPatternParameter,
        Type, TypeOrPattern, UseStatement,
    },
    token::Token,
};
use derivative::Derivative;
use either::Either;
use lsp_types::{CompletionItem, Location, MarkedString, Position, Range, Url};
use ontol_compiler::ontol_syntax::OntolTreeSyntax;
use ontol_compiler::{
    error::UnifiedCompileError,
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, ParsedPackage, ONTOL_PKG},
    CompileError, SourceId, SourceSpan, Sources, SpannedCompileError, NO_SPAN,
};
use ontol_parser::cst_parse;
use ontol_runtime::ontology::{config::PackageConfig, domain::TypeInfo, Ontology};
use regex::Regex;
use std::{
    collections::{HashMap, HashSet},
    format,
    io::Error,
    panic,
    sync::Arc,
};
use substring::Substring;

type UsizeRange = std::ops::Range<usize>;

/// Language server state
#[derive(Derivative)]
#[derivative(Clone, Debug)]
pub struct State {
    /// Current documents, by uri
    pub docs: HashMap<String, Document>,

    /// Doc uris indexed by SourceId, as seen in UnifiedCompileErrors
    pub srcref: HashMap<SourceId, String>,

    /// Precompiled regex
    pub regex: CompiledRegex,

    /// Ontology with only the `ontol` domain
    #[derivative(Debug = "ignore")]
    pub ontology: Arc<Ontology>,

    /// Fast lookup for TypeInfo
    #[derivative(Debug = "ignore")]
    pub ontol_type_info: HashMap<String, TypeInfo>,

    /// Prebuilt data structure
    pub core_completions: Vec<CompletionItem>,
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

    /// Lexer tokens, low value
    pub tokens: Vec<Spanned<Token>>,

    /// Unique symbols/names in this document
    pub symbols: HashSet<String>,

    /// All AST Statements
    pub statements: Vec<Spanned<Statement>>,

    /// Def Statements indexed by name
    pub defs: HashMap<String, Spanned<DefStatement>>,

    /// Use Statements
    pub imports: Vec<UseStatement>,

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

/// Precompiled regex used in building documentation
#[derive(Clone, Debug)]
pub struct CompiledRegex {
    pub comments: Regex,
    pub rel_parens: Regex,
    pub rel_subject: Regex,
}

impl State {
    /// Compile ontology for docs, prepare data, and return a new State
    pub fn new() -> Self {
        let mem = Mem::default();
        let ontology = ontol_compiler::compile(Default::default(), Default::default(), &mem)
            .unwrap()
            .into_ontology();

        let ontol_domain = ontology.find_domain(ONTOL_PKG).unwrap();
        let mut ontol_type_info = HashMap::new();

        for type_info in ontol_domain.type_infos() {
            if let Some(name) = type_info.name() {
                let name = &ontology[name];
                ontol_type_info.insert(name.to_string(), type_info.clone());
            }
        }

        let core_completions = get_core_completions();

        Self {
            docs: Default::default(),
            srcref: Default::default(),
            regex: Default::default(),
            ontology: Arc::new(ontology),
            ontol_type_info,
            core_completions,
        }
    }

    /// Parse ONTOL file, collecting tokens, statements and related data
    pub fn parse_statements(&mut self, uri: &str) {
        let reserved_words = COMPLETIONS.map(|(label, _)| label);

        if let Some(doc) = self.docs.get_mut(uri) {
            doc.tokens.clear();
            doc.symbols.clear();
            doc.imports.clear();
            doc.defs.clear();

            let (tokens, _) = ast_lex(doc.text.as_str());
            doc.tokens = tokens;

            for (token, _) in &doc.tokens {
                if let Token::Sym(sym) = token {
                    if !reserved_words.contains(&sym.as_str()) {
                        doc.symbols.insert(sym.to_string());
                    }
                }
            }

            /// Recursively explore the AST, collecting defs and other statements
            fn explore(
                statements: &Vec<Spanned<Statement>>,
                nested: &mut Vec<Spanned<Statement>>,
                imports: &mut Vec<UseStatement>,
                defs: &mut HashMap<String, Spanned<DefStatement>>,
                level: u8,
            ) {
                for (statement, range) in statements {
                    if level > 0 {
                        nested.push((statement.clone(), range.clone()))
                    }
                    match statement {
                        Statement::Use(stmt) => imports.push(stmt.clone()),
                        Statement::Def(stmt) => {
                            let name = stmt.ident.0.to_string();
                            defs.insert(name, (stmt.clone(), range.clone()));
                            explore(&stmt.block.0, nested, imports, defs, level + 1)
                        }
                        Statement::Rel(stmt) => {
                            for rel in &stmt.relations {
                                if let Some((ctx_block, _)) = &rel.ctx_block {
                                    explore(ctx_block, nested, imports, defs, level + 1)
                                }
                            }
                        }
                        Statement::Fmt(_) => (),
                        Statement::Map(_) => (),
                    }
                }
            }

            let (mut statements, _) = parse_statements(&doc.text);

            let mut nested: Vec<Spanned<Statement>> = vec![];

            explore(&statements, &mut nested, &mut doc.imports, &mut doc.defs, 0);

            doc.aliases = doc
                .imports
                .iter()
                .map(|stmt: &UseStatement| {
                    (stmt.as_ident.0.to_string(), stmt.reference.0.to_string())
                })
                .collect::<HashMap<_, _>>();

            statements.append(&mut nested);
            doc.statements = statements;
        }
    }

    /// Build package graph from the given root and compile topology
    pub fn compile(&mut self, root_uri: &str) -> Result<(), UnifiedCompileError> {
        let (root_path, filename) = get_path_and_name(root_uri);
        let root_name = get_domain_name(filename);

        let mut ontol_sources = Sources::default();
        let mut package_graph_builder = PackageGraphBuilder::with_roots([root_name.into()]);

        let topology = loop {
            match package_graph_builder.transition()? {
                GraphState::RequestPackages { builder, requests } => {
                    package_graph_builder = builder;

                    for request in requests {
                        let source_name = get_reference_name(&request.reference);
                        let package_config = PackageConfig::default();
                        let request_uri = build_uri(root_path, source_name);

                        if let Some(doc) = self.docs.get(&request_uri) {
                            let (flat_tree, errors) = cst_parse(&doc.text);

                            let package = ParsedPackage::new(
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
                            package_graph_builder.provide_package(package);
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
                errors: vec![
                    (SpannedCompileError {
                        error: CompileError::BUG(message),
                        span: NO_SPAN,
                        notes: vec![],
                    }),
                ],
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
        if let Some(doc) = self.docs.get(uri) {
            let cursor = get_byte_pos(&doc.text, pos);
            let mut loc: Option<Location> = None;

            for (statement, span) in doc.statements.iter() {
                if !in_range(span, cursor) {
                    continue;
                }
                match statement {
                    Statement::Use(stmt) => {
                        let (path, _) = get_path_and_name(uri);
                        let ref_uri = build_uri(path, &stmt.reference.0);
                        if self.docs.contains_key(&ref_uri) {
                            return Some(Location {
                                uri: Url::parse(&ref_uri).unwrap(),
                                range: Range::default(),
                            });
                        }
                    }
                    Statement::Def(stmt) => {
                        if let Some((_, span)) = doc.defs.get(stmt.ident.0.as_str()) {
                            loc = Some(Location {
                                uri: Url::parse(uri).unwrap(),
                                range: get_range(&doc.text, span),
                            });
                        }
                    }
                    Statement::Rel(stmt) => {
                        let mut rel_loc: Option<Location> = None;
                        if in_range(&stmt.subject.1, cursor) {
                            match &stmt.subject.0 {
                                Either::Left(_) => return loc,
                                Either::Right(Type::Path(path)) => {
                                    rel_loc = self.get_location_for_path(doc, path);
                                }
                                _ => {}
                            }
                        } else if in_range(&stmt.object.1, cursor) {
                            if let Either::Right(type_or_pattern) = &stmt.object.0 {
                                match type_or_pattern {
                                    TypeOrPattern::Type(Type::Path(path)) => {
                                        rel_loc = self.get_location_for_path(doc, path);
                                    }
                                    TypeOrPattern::Pattern(pattern) => {
                                        if let Some(path) = get_pattern_path(pattern, &cursor) {
                                            rel_loc = self.get_location_for_path(doc, &path);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        if rel_loc.is_some() {
                            loc = rel_loc;
                        }
                    }
                    Statement::Fmt(stmt) => {
                        let mut fmt_loc: Option<Location> = None;
                        if in_range(&stmt.origin.1, cursor) {
                            if let Type::Path(path) = &stmt.origin.0 {
                                fmt_loc = self.get_location_for_path(doc, path);
                            }
                        } else {
                            for trans in &stmt.transitions {
                                if in_range(&trans.1, cursor) {
                                    match &trans.0 {
                                        Either::Left(_) => return loc,
                                        Either::Right(Type::Path(path)) => {
                                            fmt_loc = self.get_location_for_path(doc, path);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        if fmt_loc.is_some() {
                            loc = fmt_loc;
                        }
                    }
                    Statement::Map(stmt) => {
                        if in_range(&stmt.first.1, cursor) {
                            if let Some(path) = get_map_arm_path(&stmt.first.0, &cursor) {
                                return self.get_location_for_path(doc, &path);
                            }
                        } else if in_range(&stmt.second.1, cursor) {
                            if let Some(path) = get_map_arm_path(&stmt.second.0, &cursor) {
                                return self.get_location_for_path(doc, &path);
                            }
                        }
                    }
                }
            }
            return loc;
        }
        None
    }

    /// Get Location for Path
    fn get_location_for_path(&self, doc: &Document, path: &Path) -> Option<Location> {
        match path {
            Path::Ident(ident) => self.get_location_for_ident(doc, ident.as_str()),
            Path::Path(path) => {
                let path = path
                    .iter()
                    .map(|(sstring, _)| sstring.to_string())
                    .collect::<Vec<String>>();
                self.get_location_for_ext_ident(doc, &path)
            }
        }
    }

    /// Get Location given a local identifier
    fn get_location_for_ident(&self, doc: &Document, ident: &str) -> Option<Location> {
        if let Some((_, span)) = doc.defs.get(ident) {
            return Some(Location {
                uri: Url::parse(&doc.uri).unwrap(),
                range: get_range(&doc.text, span),
            });
        }
        None
    }

    /// Get Location given an external identifier
    fn get_location_for_ext_ident(&self, doc: &Document, path: &[String]) -> Option<Location> {
        if let Some(root_alias) = path.first() {
            if let Some(ident) = path.get(1) {
                if let Some(root) = doc.aliases.get(root_alias) {
                    let uri = build_uri(&doc.path, root);
                    if let Some(doc) = self.docs.get(&uri) {
                        return self.get_location_for_ident(doc, ident);
                    }
                }
            }
        }
        None
    }

    /// Build hover documentation for the targeted statement
    pub fn get_hoverdoc(&self, uri: &str, pos: &Position) -> Option<HoverDoc> {
        if let Some(doc) = self.docs.get(uri) {
            let cursor = get_byte_pos(&doc.text, pos);
            let mut hover = HoverDoc::from(&doc.name, "");

            // check statements, may overlap
            for (statement, span) in doc.statements.iter() {
                if !span.contains(&cursor) {
                    continue;
                }

                match statement {
                    Statement::Use(stmt) => {
                        hover.path = format!("{} as {}", &stmt.reference.0, &stmt.as_ident.0);
                        hover.signature = get_signature(&doc.text, span.clone(), &self.regex);
                        break;
                    }
                    Statement::Def(stmt) => {
                        hover.path = format!("{}.{}", &doc.name, &stmt.ident.0);
                        hover.signature = get_signature(&doc.text, span.clone(), &self.regex);
                        hover.docs = stmt.docs.as_deref().unwrap_or("").to_string();
                        break;
                    }
                    Statement::Rel(stmt) => {
                        if stmt.subject.1.contains(&cursor) {
                            match &stmt.subject.0 {
                                Either::Left(_) => return self.get_ontol_docs("."),
                                Either::Right(Type::Path(path)) => {
                                    return self.get_hoverdoc_for_path(doc, path);
                                }
                                _ => {}
                            }
                        } else if stmt.object.1.contains(&cursor) {
                            if let Either::Right(type_or_pattern) = &stmt.object.0 {
                                match type_or_pattern {
                                    TypeOrPattern::Type(Type::Path(path)) => {
                                        return self.get_hoverdoc_for_path(doc, path);
                                    }
                                    TypeOrPattern::Pattern(pattern) => {
                                        if let Some(path) = get_pattern_path(pattern, &cursor) {
                                            return self.get_hoverdoc_for_path(doc, &path);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        hover.signature = get_signature(&doc.text, span.clone(), &self.regex);
                        hover.docs = stmt.docs.as_deref().unwrap_or("").to_string();
                    }
                    Statement::Fmt(stmt) => {
                        if stmt.origin.1.contains(&cursor) {
                            if let Type::Path(path) = &stmt.origin.0 {
                                return self.get_hoverdoc_for_path(doc, path);
                            }
                        } else {
                            for trans in &stmt.transitions {
                                if trans.1.contains(&cursor) {
                                    match &trans.0 {
                                        Either::Left(_) => return self.get_ontol_docs("."),
                                        Either::Right(Type::Path(path)) => {
                                            return self.get_hoverdoc_for_path(doc, path);
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        hover.signature = get_signature(&doc.text, span.clone(), &self.regex);
                        hover.docs = stmt.docs.as_deref().unwrap_or("").to_string();
                        break;
                    }
                    Statement::Map(stmt) => {
                        if stmt.first.1.contains(&cursor) {
                            if let Some(path) = get_map_arm_path(&stmt.first.0, &cursor) {
                                return self.get_hoverdoc_for_path(doc, &path);
                            }
                        } else if stmt.second.1.contains(&cursor) {
                            if let Some(path) = get_map_arm_path(&stmt.second.0, &cursor) {
                                return self.get_hoverdoc_for_path(doc, &path);
                            }
                        }
                        let ident = if let Some(ident) = &stmt.ident {
                            format!("{} ", ident.0)
                        } else {
                            "".to_string()
                        };
                        let first = parse_map_arm(&stmt.first.0);
                        let second = parse_map_arm(&stmt.second.0);
                        hover.path = format!("map {}{}() {}()", ident, first, second);
                        break;
                    }
                }
            }

            // check tokens, may replace hover
            for (index, (token, span)) in doc.tokens.iter().enumerate() {
                if span.contains(&cursor) {
                    match token {
                        Token::Use => return self.get_ontol_docs("use"),
                        Token::Def => return self.get_ontol_docs("def"),
                        Token::Rel => return self.get_ontol_docs("rel"),
                        Token::Fmt => return self.get_ontol_docs("fmt"),
                        Token::Map => return self.get_ontol_docs("map"),
                        Token::DotDot => return self.get_ontol_docs(".."),
                        Token::Modifier(modifier) => {
                            return self.get_ontol_docs(format!("{modifier}").as_str())
                        }
                        // Token::Number(num) => todo!(),
                        // Token::TextLiteral(text) => todo!(),
                        // Token::Regex(regex) => todo!(),
                        Token::Sigil(sigil) => {
                            if *sigil == '?' {
                                return self.get_ontol_docs("?");
                            }
                        }
                        Token::Sym(ident) => {
                            // Peek ahead and behind, try to find a def path if any
                            if index > 1 && index < (doc.tokens.len() - 2) {
                                let pp = &doc.tokens[index - 2];
                                let p = &doc.tokens[index - 1];
                                let n = &doc.tokens[index + 1];
                                let nn = &doc.tokens[index + 2];

                                match (&pp.0, &p.0, &n.0, &nn.0) {
                                    (Token::Sym(root), Token::Sigil('.'), _, _) => {
                                        let path = [root.to_string(), ident.to_string()];
                                        if let Some(doc) =
                                            self.get_hoverdoc_for_ext_ident(doc, &path)
                                        {
                                            return Some(doc);
                                        }
                                    }
                                    (_, _, Token::Sigil('.'), Token::Sym(ext_ident)) => {
                                        let path = [ident.to_string(), ext_ident.to_string()];
                                        if let Some(doc) =
                                            self.get_hoverdoc_for_ext_ident(doc, &path)
                                        {
                                            return Some(doc);
                                        }
                                    }
                                    _ => {}
                                };
                            }

                            let doc_opt = self
                                .get_hoverdoc_for_ident(doc, ident)
                                .or(self.get_ontol_docs(ident));

                            match (doc_opt, hover.path.starts_with("map")) {
                                (Some(doc_opt), _) => return Some(doc_opt),
                                (None, true) => return Some(get_ontol_var(ident)),
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }

            return Some(hover);
        }
        None
    }

    /// Get HoverDoc for Path
    fn get_hoverdoc_for_path(&self, doc: &Document, path: &Path) -> Option<HoverDoc> {
        match path {
            Path::Ident(ident) => self.get_hoverdoc_for_ident(doc, ident.as_str()),
            Path::Path(path) => {
                let path = path
                    .iter()
                    .map(|(sstring, _)| sstring.to_string())
                    .collect::<Vec<String>>();
                self.get_hoverdoc_for_ext_ident(doc, &path)
            }
        }
    }

    /// Get HoverDoc given a local identifier
    fn get_hoverdoc_for_ident(&self, doc: &Document, ident: &str) -> Option<HoverDoc> {
        match doc.defs.get(ident) {
            Some((stmt, span)) => Some(HoverDoc {
                path: format!("{}.{}", doc.name, stmt.ident.0),
                signature: get_signature(&doc.text, span.clone(), &self.regex),
                docs: stmt.docs.as_deref().unwrap_or("").to_string(),
                ..Default::default()
            }),
            None => self.get_ontol_docs(ident),
        }
    }

    /// Get HoverDoc given an external identifier
    fn get_hoverdoc_for_ext_ident(&self, doc: &Document, path: &[String]) -> Option<HoverDoc> {
        if let Some(root_alias) = path.first() {
            if let Some(ident) = path.get(1) {
                if let Some(root) = doc.aliases.get(root_alias) {
                    let uri = build_uri(&doc.path, root);
                    if let Some(doc) = self.docs.get(&uri) {
                        return self.get_hoverdoc_for_ident(doc, ident);
                    }
                }
            }
        }
        None
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

impl CompiledRegex {
    /// Precompile regexes used in documentation
    fn new() -> Self {
        Self {
            comments: Regex::new(r"(?m-s)^\s*?\/\/.*\n?").unwrap(),
            rel_parens: Regex::new(r"(?ms)\(.*?\)").unwrap(),
            rel_subject: Regex::new(r"(?ms):.*").unwrap(),
        }
    }
}

impl Default for CompiledRegex {
    fn default() -> Self {
        Self::new()
    }
}

/// Read file contents for URI
pub fn read_file(uri: &str) -> Result<String, Error> {
    let path = get_base_path(uri);
    let text = std::fs::read_to_string(std::path::Path::new(&path))?;
    Ok(text)
}

/// Get source name for a PackageReference
pub fn get_reference_name(reference: &PackageReference) -> &str {
    match reference {
        PackageReference::Named(source_name) => source_name.as_str(),
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

/// Check if index is in Range, inclusive
fn in_range(span: &UsizeRange, cursor: usize) -> bool {
    cursor >= span.start && cursor <= span.end
}

/// Get a stripped-down rendition of a statement
pub fn get_signature(text: &str, span: impl Into<UsizeRange>, regex: &CompiledRegex) -> String {
    let span = span.into();
    let sig = text.substring(span.start, span.end);
    let stripped = &regex.comments.replace_all(sig, "");
    stripped.trim().replace("\n\n", "\n").to_string()
}

/// Parse the identifier or path for a map arm to String
pub fn parse_map_arm(arm: &MapArm) -> String {
    match arm {
        MapArm::Struct(s) => match &s.path {
            Some(path) => parse_path(&path.0),
            None => "".into(),
        },
        MapArm::Set(s) => match &s.path {
            Some(path) => parse_path(&path.0),
            None => "".into(),
        },
    }
}

/// Parse a map path to String
fn parse_path(path: &Path) -> String {
    match path {
        Path::Ident(id) => id.to_string(),
        Path::Path(p) => p
            .iter()
            .map(|(s, _)| s.to_string())
            .collect::<Vec<String>>()
            .join("."),
    }
}

/// Try to find a Path in a MapArm
fn get_map_arm_path(arm: &MapArm, cursor: &usize) -> Option<Path> {
    match arm {
        MapArm::Struct(s) => get_struct_pattern_path(s, cursor),
        MapArm::Set(s) => s.path.as_ref().map(|(path, _)| path.clone()),
    }
}

/// Try to find a Path in a StructPattern
fn get_struct_pattern_path(s: &StructPattern, cursor: &usize) -> Option<Path> {
    let path = s.path.as_ref()?;
    let span = &path.1;
    if in_range(span, *cursor) {
        return Some(path.0.to_owned());
    }
    if let StructPatternParameter::Attributes(attrs) = &s.param {
        for (arg, span) in attrs {
            match arg {
                old_parser::ast::StructPatternAttributeKind::Attr(attr) => {
                    if in_range(span, *cursor) {
                        return get_pattern_path(&attr.object.0, cursor);
                    }
                }
                old_parser::ast::StructPatternAttributeKind::Spread(_) => (),
            }
        }
    }
    None
}

/// Try to find a Path in a Pattern
fn get_pattern_path(pattern: &AnyPattern, cursor: &usize) -> Option<Path> {
    match pattern {
        AnyPattern::Struct((s, _)) => get_struct_pattern_path(s, cursor),
        AnyPattern::Set((set, _)) => {
            for (elem, span) in &set.elements {
                if in_range(span, *cursor) {
                    return get_pattern_path(&elem.pattern.0, cursor);
                }
            }
            None
        }
        AnyPattern::Expr(_) => None,
    }
}
