use chumsky::prelude::*;
use lsp_types::{CompletionItem, CompletionItemKind, Position, Range};
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
    pub roots: HashSet<String>,
    pub sourcemap: HashMap<SourceId, String>,
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
    pub defs: HashMap<String, Spanned<DefStatement>>,
    pub statements: Vec<Spanned<Statement>>,
    pub imports: Vec<UseStatement>,
}

/// Helper struct for building hover documentation
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct DocPanel {
    pub path: String,
    pub signature: String,
    pub docs: String,
}

impl State {
    /// Parse ONTOL file, collecting tokens, symbols, statements and imports
    pub fn parse_statements(&mut self, url: &str) {
        if let Some(doc) = self.docs.get_mut(url) {
            let (tokens, _) = lexer().parse_recovery(doc.text.as_str());

            if let Some(tokens) = tokens {
                doc.tokens = tokens;
            }

            doc.symbols.clear();
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

            doc.imports.clear();
            doc.defs.clear();

            let (mut statements, _) = parse_statements(doc.text.as_str());
            let mut nested: Vec<Spanned<Statement>> = vec![];

            explore(&statements, &mut nested, &mut doc.imports, &mut doc.defs, 0);

            statements.append(&mut nested);
            doc.statements = statements;
        }
    }

    /// Build package graph from the given root and compile topology
    pub fn compile(&self, root_url: &str) -> Result<(), UnifiedCompileError> {
        let (root_path, filename) = get_path_and_name(root_url);
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
                            package_graph_builder.provide_package(ParsedPackage::parse(
                                request,
                                &doc.text,
                                package_config,
                                &mut ontol_sources,
                                &mut source_code_registry,
                            ));
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

impl Document {
    /// Get a stripped-down version of a statement
    pub fn get_signature(&self, range: &std::ops::Range<usize>) -> String {
        let sig = self.text.substring(range.start(), range.end());

        let comments = Regex::new(r"(?m-s)^\s*?\/\/.*\n?").unwrap();
        let no_comments = &comments.replace_all(sig, "");

        if no_comments.starts_with(' ') {
            let wspace = Regex::new(r"(?m)^\s*").unwrap();
            wspace.replace_all(no_comments, "").to_string()
        } else {
            no_comments.to_string()
        }
    }

    /// Build hover documentation for the targeted statement
    pub fn get_hover_docs(&self, url: &str, lineno: usize, col: usize) -> DocPanel {
        let mut cursor = 0;

        for (line_index, line) in self.text.lines().enumerate() {
            if line_index == lineno {
                cursor += col;
                break;
            } else {
                cursor += line.len() + 1
            }
        }

        let mut dp = DocPanel {
            path: match url.split('/').last() {
                Some(last) => match last.split('.').next() {
                    Some(next) => next.to_string(),
                    None => last.to_string(),
                },
                None => url.to_string(),
            },
            ..Default::default()
        };

        let basepath = dp.path.clone();

        for (statement, range) in self.statements.iter() {
            if cursor > range.start() && cursor < range.end() {
                dp.signature = self.get_signature(range);

                match statement {
                    Statement::Use(stmt) => {
                        dp.path = format!("{}", stmt.as_ident.0);
                        break;
                    }
                    Statement::Def(stmt) => {
                        dp.path += &format!(".{}", stmt.ident.0);
                        dp.docs = stmt.docs.join("\n");
                    }
                    Statement::Rel(stmt) => {
                        dp.docs = stmt.docs.join("\n");
                    }
                    Statement::Fmt(stmt) => {
                        dp.docs = stmt.docs.join("\n");
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
                        dp.path = format!("map {} {}", first, second);
                        dp.docs.clear();
                        break;
                    }
                }
            }
        }

        let mut last_rel = 0;
        for (token, range) in &self.tokens {
            if let Token::Rel = token {
                last_rel = range.start();
            }
            if cursor > range.start() && cursor < range.end() {
                match token {
                    Token::Open(_) => (),
                    Token::Close(_) => (),
                    Token::Sigil(_) => (),
                    Token::Use => (),
                    Token::Def => (),
                    Token::Rel => (),
                    Token::Fmt => (),
                    Token::Map => (),
                    Token::Pub => (),
                    Token::FatArrow => (),
                    Token::Number(_) => (),
                    Token::StringLiteral(_) => (),
                    Token::Regex(_) => (),
                    Token::Sym(val) => {
                        if let Some((stmt, range)) = self.defs.get(val.as_str()) {
                            dp.path = format!("{}.{}", basepath, stmt.ident.0);
                            dp.signature = self.get_signature(range);
                            dp.docs = stmt.docs.join("\n");
                        } else {
                            // TODO: collect hard-coded docs below from
                            match val.as_str() {
                                "id" => {
                                    dp.path = "ontol.id".to_string();
                                    dp.signature = self.get_signature(&std::ops::Range {
                                        start: last_rel,
                                        end: range.end(),
                                    });
                                    dp.docs =
                                        "### Relation prop\nBinds an id to an entity".to_string();
                                }
                                "is" => {
                                    dp.path = "ontol.is".to_string();
                                    dp.signature = self.get_signature(&std::ops::Range {
                                        start: last_rel,
                                        end: range.end(),
                                    });
                                    dp.docs =
                                        "### Relation prop\nBinds a def to a union".to_string();
                                }
                                "gen" => {
                                    dp.path = "ontol.gen".to_string();
                                    dp.signature = self.get_signature(&std::ops::Range {
                                        start: last_rel,
                                        end: range.end(),
                                    });
                                    dp.docs = "### Relation prop\nHave id be generated".to_string();
                                }
                                "auto" => {
                                    dp.path = "ontol.auto".to_string();
                                    dp.signature = self.get_signature(&std::ops::Range {
                                        start: last_rel,
                                        end: range.end(),
                                    });
                                    dp.docs = "### Relation prop\nBackend dictates id generation"
                                        .to_string();
                                }
                                "default" => {
                                    dp.path = "ontol.default".to_string();
                                    dp.signature = self.get_signature(&std::ops::Range {
                                        start: last_rel,
                                        end: range.end(),
                                    });
                                    dp.docs =
                                        "### Relation prop\nAssigns a default value to a property if none is given".to_string();
                                }
                                "boolean" => {
                                    dp.path = "ontol.boolean".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs = "### Scalar\nBoolean".to_string();
                                }
                                "number" => {
                                    dp.path = "ontol.number".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs = "### Scalar\nNumber".to_string();
                                }
                                "integer" => {
                                    dp.path = "ontol.integer".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs = "### Scalar\nInteger".to_string();
                                }
                                "i64" => {
                                    dp.path = "ontol.i64".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs = "### Scalar\n64 bit signed integer".to_string();
                                }
                                "float" => {
                                    dp.path = "ontol.float".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs = "### Scalar\nFloating point number".to_string();
                                }
                                "f64" => {
                                    dp.path = "ontol.f64".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs =
                                        "### Scalar\n64 bit floating point number".to_string();
                                }
                                "string" => {
                                    dp.path = "ontol.string".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs = "### Scalar\nUTF-8 string".to_string();
                                }
                                "datetime" => {
                                    dp.path = "ontol.datetime".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs = "### Scalar\nRFC 3339-formatted datetime string"
                                        .to_string();
                                }
                                "date" => {
                                    dp.path = "ontol.date".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs =
                                        "### Scalar\nRFC 3339-formatted date string".to_string();
                                }
                                "time" => {
                                    dp.path = "ontol.time".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs =
                                        "### Scalar\nRFC 3339-formatted time string".to_string();
                                }
                                "uuid" => {
                                    dp.path = "ontol.uuid".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs = "### Scalar\nUUID v4".to_string();
                                }
                                "regex" => {
                                    dp.path = "ontol.regex".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs = "### Scalar\nRegular expression".to_string();
                                }
                                _ => (),
                            }
                        }
                    }
                    Token::DocComment(_) => (),
                }
            }
        }
        dp
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
            detail: Some("rel … …: …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "map".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("map { …, … }".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "use".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("use … ".to_string()),
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
            detail: Some("fmt … => …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "unify".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("unify { …, … }".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "id".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("rel .…|id: …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "is".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("rel .is: …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "gen".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("rel .gen: …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "auto".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("… .gen: auto".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "default".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            detail: Some("rel .default := …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "boolean".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("boolean".to_string()),
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
            label: "number".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("number".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "string".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            detail: Some("string".to_string()),
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
            detail: Some("uuid v4".to_string()),
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
const RESERVED_WORDS: [&str; 29] = [
    "use", "as", "pub", "def", "rel", "fmt", "map", "unify", "id", "is", "has", "gen", "auto",
    "default", "example", "number", "boolean", "integer", "i64", "float", "f64", "string",
    "datetime", "date", "time", "uuid", "regex", "seq", "dict",
];
