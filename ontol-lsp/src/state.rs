use chumsky::prelude::*;
use lsp_types::{CompletionItem, CompletionItemKind, MarkedString, Position, Range};
use ontol_compiler::{
    error::UnifiedCompileError,
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, ParsedPackage},
    Compiler, SourceCodeRegistry, SourceId, SourceSpan, Sources,
};
use ontol_parser::{
    ast::{DefStatement, MapArm, Path, Statement, UseStatement},
    lexer::lexer,
    parse_statements, Spanned, Token,
};
use ontol_runtime::config::PackageConfig;
use regex::Regex;
use std::{
    collections::{HashMap, HashSet},
    format,
    io::Error,
};
use substring::Substring;

/// Language server state
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct State {
    pub docs: HashMap<String, Document>,
    pub source_map: HashMap<SourceId, String>,
}

/// A document and its various constituents
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct Document {
    pub uri: String,
    pub path: String,
    pub name: String,
    pub text: String,
    pub tokens: Vec<Spanned<Token>>,
    pub symbols: HashSet<String>,
    pub statements: Vec<Spanned<Statement>>,
    pub defs: HashMap<String, Spanned<DefStatement>>,
    pub imports: Vec<UseStatement>,
    pub aliases: HashMap<String, String>,
}

/// Helper struct for building hover documentation
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct HoverDoc {
    pub path: String,
    pub signature: String,
    pub docs: String,
}

impl State {
    /// Parse ONTOL file, collecting tokens, statements and related data
    pub fn parse_statements(&mut self, uri: &str) {
        if let Some(doc) = self.docs.get_mut(uri) {
            doc.tokens.clear();
            doc.symbols.clear();
            doc.imports.clear();
            doc.defs.clear();

            let (tokens, _) = lexer().parse_recovery(doc.text.as_str());

            if let Some(tokens) = tokens {
                doc.tokens = tokens;
            }

            for (token, _) in &doc.tokens {
                if let Token::Sym(sym) = token {
                    if !RESERVED_WORDS.contains(&sym.as_str()) {
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

            let (mut statements, _) = parse_statements(doc.text.as_str());
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
        let mut source_code_registry = SourceCodeRegistry::default();
        let mut package_graph_builder = PackageGraphBuilder::new(root_name.into());

        let topology = loop {
            match package_graph_builder.transition()? {
                GraphState::RequestPackages { builder, requests } => {
                    package_graph_builder = builder;

                    for request in requests {
                        let source_name = get_reference_name(&request.reference);
                        let package_config = PackageConfig::default();
                        let request_uri = build_uri(root_path, source_name);

                        if let Some(doc) = self.docs.get(&request_uri) {
                            let package = ParsedPackage::parse(
                                request,
                                &doc.text,
                                package_config,
                                &mut ontol_sources,
                                &mut source_code_registry,
                            );
                            self.source_map.insert(package.src.id, request_uri);
                            package_graph_builder.provide_package(package);
                        }
                    }
                }
                GraphState::Built(topology) => break topology,
            }
        };

        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, ontol_sources.clone()).with_ontol();
        compiler.compile_package_topology(topology)
    }

    /// Get Document if given SourceId is known
    pub fn get_doc_by_sourceid(&self, source_id: &SourceId) -> Option<&Document> {
        match self.source_map.get(source_id) {
            Some(uri) => self.docs.get(uri),
            None => None,
        }
    }

    /// Get SourceId if source map points to the given uri
    pub fn get_sourceid_by_uri(&self, uri: &str) -> Option<SourceId> {
        for (key, val) in self.source_map.iter() {
            if val.as_str() == uri {
                return Some(*key);
            }
        }
        None
    }

    /// Build hover documentation for the targeted statement
    pub fn get_hover_docs(&self, uri: &str, lineno: usize, col: usize) -> Option<HoverDoc> {
        match self.docs.get(uri) {
            None => None,
            Some(doc) => {
                // convert line/col position to byte position
                let mut cursor = 0;
                for (line_index, line) in doc.text.lines().enumerate() {
                    if line_index == lineno {
                        cursor += col;
                        break;
                    } else {
                        cursor += line.len() + 1
                    }
                }

                // check statements, may overlap
                let mut hover = HoverDoc::default();
                for (statement, range) in doc.statements.iter() {
                    if cursor > range.start() && cursor < range.end() {
                        hover.path = doc.name.to_owned();
                        hover.signature = get_signature(&doc.text, range);

                        match statement {
                            Statement::Use(stmt) => {
                                hover.path = format!("{} as {}", stmt.reference.0, stmt.as_ident.0);
                                break;
                            }
                            Statement::Def(stmt) => {
                                hover.path += &stmt.ident.0;
                                hover.docs = stmt.docs.join("\n");
                            }
                            Statement::Rel(stmt) => {
                                hover.docs = stmt.docs.join("\n");
                            }
                            Statement::Fmt(stmt) => {
                                hover.docs = stmt.docs.join("\n");
                                break;
                            }
                            Statement::Map(stmt) => {
                                let first = match &stmt.first.0 .1 {
                                    MapArm::Binding { .. } => "",
                                    MapArm::Struct(s) => match &s.path.0 {
                                        Path::Ident(id) => id.as_str(),
                                        Path::Path(_) => "",
                                    },
                                };
                                let second = match &stmt.second.0 .1 {
                                    MapArm::Binding { .. } => "",
                                    MapArm::Struct(s) => match &s.path.0 {
                                        Path::Ident(id) => id.as_str(),
                                        Path::Path(_) => "",
                                    },
                                };
                                hover.path = format!("map {} {}", first, second);
                                hover.docs.clear();
                                break;
                            }
                        }
                    }
                }

                // check tokens, may replace hover
                let mut prev_ident: Option<&str> = None;
                for (token, range) in doc.tokens.iter() {
                    if cursor > range.start() && cursor < range.end() {
                        match token {
                            Token::Sym(ident) => {
                                match doc.defs.get(ident.as_str()) {
                                    // local defs
                                    Some((stmt, range)) => {
                                        hover.path = format!("{}.{}", doc.path, stmt.ident.0);
                                        hover.signature = get_signature(&doc.text, range);
                                        hover.docs = stmt.docs.join("\n");
                                    }
                                    // nonlocal defs
                                    None => {
                                        match get_ontol_core_documentation(ident.as_str()) {
                                            Some(doc_panel) => hover = doc_panel,
                                            None => {
                                                // imported defs
                                                if let Some(name) = prev_ident {
                                                    let uri = build_uri(&doc.path, name);
                                                    if let Some(doc) = self.docs.get(uri.as_str()) {
                                                        if let Some((stmt, range)) =
                                                            &doc.defs.get(name)
                                                        {
                                                            hover.signature =
                                                                get_signature(&doc.text, range);
                                                            hover.path = format!(
                                                                "{}.{}",
                                                                &doc.name, stmt.ident.0
                                                            );
                                                            hover.docs = stmt.docs.join("\n");
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                prev_ident = Some(ident)
                            }
                            Token::Use => hover = get_ontol_core_documentation("use").unwrap(),
                            Token::Def => hover = get_ontol_core_documentation("def").unwrap(),
                            Token::Rel => hover = get_ontol_core_documentation("rel").unwrap(),
                            Token::Fmt => hover = get_ontol_core_documentation("fmt").unwrap(),
                            Token::Map => hover = get_ontol_core_documentation("map").unwrap(),
                            Token::Pub => hover = get_ontol_core_documentation("pub").unwrap(),
                            Token::Number(number) => {
                                hover =
                                    HoverDoc::from(number.as_str(), "### Value\nNumeric literal")
                            }
                            Token::StringLiteral(string) => {
                                if !hover.signature.starts_with("use") {
                                    hover = HoverDoc::from(
                                        &format!("'{}'", string),
                                        "#### Value\nString literal",
                                    )
                                }
                            }
                            Token::Regex(regex) => {
                                hover = HoverDoc::from(
                                    &format!("/{}/", regex),
                                    "#### Value\nRegular expression",
                                )
                            }
                            _ => {}
                        }
                    }
                }

                Some(hover)
            }
        }
    }
}

impl HoverDoc {
    /// Build a HoverDoc from just path and docs, ignoring signature
    fn from(path: &str, docs: &str) -> Self {
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
            ))
        }
        vec.push(MarkedString::from_markdown(self.docs.clone()));
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
    let mut range = Range::new(Position::new(0, 0), Position::new(0, 0));
    let mut start_set = false;
    let mut cursor = 0;

    for (line_index, line) in text.lines().enumerate() {
        if !start_set {
            if cursor + line.len() < span.start as usize {
                cursor += line.len() + 1;
            } else {
                range.start.line = line_index as u32;
                range.start.character = span.start - cursor as u32;
                start_set = true
            }
        }
        if start_set {
            if cursor + line.len() < span.end as usize {
                cursor += line.len() + 1;
            } else {
                range.end.line = line_index as u32;
                range.end.character = span.end - cursor as u32;
                break;
            }
        }
    }
    range
}

/// Get a stripped-down rendition of a statement
pub fn get_signature(text: &str, range: &std::ops::Range<usize>) -> String {
    let sig = text.substring(range.start(), range.end());

    let comments_rx = Regex::new(r"(?m-s)^\s*?\/\/.*\n?").unwrap();
    let stripped = &comments_rx.replace_all(sig, "");

    if stripped.starts_with(' ') {
        let wspace_rx = Regex::new(r"(?m)^\s*").unwrap();
        wspace_rx.replace_all(stripped, "").to_string()
    } else {
        stripped.to_string()
    }
}

/// Get a HoverDoc for ONTOL core
pub fn get_ontol_core_documentation(ident: &str) -> Option<HoverDoc> {
    // TODO: collect hard-coded docs below from `ontol_domain.rs`
    match ident {
        "use" => Some(HoverDoc::from(
            "use",
            "#### Use statement\nImports the domain following `use` and provides a local namespace alias after `as`.",
        )),
        "pub" => Some(HoverDoc::from(
            "pub",
            "#### Public modifier\nMarks a `def` as public and importable from other domains.",
        )),
        "def" => Some(HoverDoc::from(
            "def",
            "#### Def statement\nDefines a type.",
        )),
        "rel" => Some(HoverDoc::from(
            "rel",
            "#### Relation statement\nDefines a relation between types, either for properties or ."
        )),
        "fmt" => Some(HoverDoc::from(
            "fmt",
            "#### Format statement\nPattern building and -matching for string or sequence."
        )),
        "map" => Some(HoverDoc::from(
            "map",
            "#### Map statement\nMaps one `def` to another"
        )),
        "match" => Some(HoverDoc::from(
            "match",
            "#### Pattern modifier\nPartial matching for `map` arm."
        )),
        "id" => Some(HoverDoc::from(
            "ontol.id",
            "#### Relation prop\nBinds an id to an entity.",
        )),
        "is" => Some(HoverDoc::from(
            "ontol.is",
            "#### Relation prop\nBinds a def to a union.",
        )),
        "default" => Some(HoverDoc::from(
            "ontol.default",
            "#### Relation prop\nAssigns a default value to a property if none is given.",
        )),
        "example" => Some(HoverDoc::from(
            "ontol.example",
            "#### Relation prop\nGives an example value for documentation."
        )),
        "gen" => Some(HoverDoc::from(
            "ontol.gen",
            "#### Relation prop\nUse a generator.",
        )),
        "auto" => Some(HoverDoc::from(
            "ontol.auto",
            "#### Generator\nAutogenerate value.",
        )),
        "create_time" => Some(HoverDoc::from(
            "ontol.create_time",
            "#### Generator\nAutogenerate create datetime.",
        )),
        "update_time" => Some(HoverDoc::from(
            "ontol.update_time",
            "#### Generator\nAutogenerate update datetime.",
        )),
        "boolean" => Some(HoverDoc::from(
            "ontol.boolean",
            "#### Scalar\nBoolean type."
        )),
        "true" => Some(HoverDoc::from(
            "true",
            "#### Value\nBoolean `true` value."
        )),
        "false" => Some(HoverDoc::from(
            "false",
            "#### Value\nBoolean `false` value."
        )),
        "number" => Some(HoverDoc::from(
            "ontol.number",
            "#### Scalar\nAbstract number type.",
        )),
        "integer" => Some(HoverDoc::from(
            "ontol.integer",
            "#### Scalar\nAbstract integer type.",
        )),
        "i64" => Some(HoverDoc::from(
            "ontol.i64",
            "#### Scalar\n64 bit signed integer.",
        )),
        "float" => Some(HoverDoc::from(
            "ontol.float",
            "#### Scalar\nAbstract floating point number type.",
        )),
        "f64" => Some(HoverDoc::from(
            "ontol.f64",
            "#### Scalar\n64 bit floating point number.",
        )),
        "string" => Some(HoverDoc::from(
            "ontol.string",
            "#### Scalar\nAny UTF-8 string.",
        )),
        "datetime" => Some(HoverDoc::from(
            "ontol.datetime",
            "#### Scalar\nRFC 3339-formatted datetime string.",
        )),
        "date" => Some(HoverDoc::from(
            "ontol.date",
            "#### Scalar\nRFC 3339-formatted date string.",
        )),
        "time" => Some(HoverDoc::from(
            "ontol.time",
            "#### Scalar\nRFC 3339-formatted time string.",
        )),
        "uuid" => Some(HoverDoc::from(
            "ontol.uuid",
            "#### Scalar\nUUID v4 string."
        )),
        "regex" => Some(HoverDoc::from(
            "ontol.regex",
            "#### Scalar\nRegular expression.",
        )),
        _ => None,
    }
}

/// Completions for built-in keywords and defs
pub fn get_builtins() -> Vec<CompletionItem> {
    vec![
        CompletionItem {
            label: "def".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("def …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "rel".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("rel … … …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "map".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("map { … … }".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "use".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("use … as …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "as".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("… as …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "pub".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("pub def …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "fmt".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("fmt …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "match".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("match".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "id".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("…|id: …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "is".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("is: …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "gen".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("gen: …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "auto".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("gen: auto".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "create_time".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("gen: create_time".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "update_time".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("gen: update_time".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "default".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("default := …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "default".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("default := …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "boolean".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("boolean".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "number".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("number".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "integer".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("integer".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "float".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("floating point number".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "i64".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("i64".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "f64".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("f64".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "string".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("utf-8 string".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "datetime".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("iso datetime string".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "date".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("iso date string".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "time".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("iso time string".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "uuid".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("uuid v4 string".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "regex".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("regular expression".to_string()),
            ..Default::default()
        },
    ]
}

/// A list of reserved words in ONTOL, to separate them from user-defined symbols
const RESERVED_WORDS: [&str; 32] = [
    "use",
    "as",
    "pub",
    "def",
    "rel",
    "fmt",
    "map",
    "unify",
    "match",
    "id",
    "is",
    "has",
    "default",
    "example",
    "gen",
    "auto",
    "create_time",
    "update_time",
    "number",
    "boolean",
    "integer",
    "i64",
    "float",
    "f64",
    "string",
    "datetime",
    "date",
    "time",
    "uuid",
    "regex",
    "seq",
    "dict",
];
