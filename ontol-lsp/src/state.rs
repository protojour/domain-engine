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
#[derive(Clone, Debug, Default)]
pub struct State {
    pub docs: HashMap<String, Document>,
    pub source_map: HashMap<SourceId, String>,
    pub regex: CompiledRegex,
}

/// A document and its various constituents
#[derive(Clone, Debug, Default)]
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
#[derive(Clone, Debug, Default)]
pub struct HoverDoc {
    pub path: String,
    pub signature: String,
    pub docs: String,
}

#[derive(Clone, Debug)]
pub struct CompiledRegex {
    pub comments: Regex,
    pub leading_whitespace: Regex,
    pub rel_parens: Regex,
    pub rel_subject: Regex,
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
                    if cursor >= range.start() && cursor < range.end() {
                        if hover.path.is_empty() {
                            hover.path = doc.name.to_string();
                        }
                        hover.signature = get_signature(&doc.text, range, &self.regex);

                        match statement {
                            Statement::Use(stmt) => {
                                hover.path = format!("{} as {}", stmt.reference.0, stmt.as_ident.0);
                                break;
                            }
                            Statement::Def(stmt) => {
                                hover.path += &format!(".{}", stmt.ident.0);
                                hover.docs = stmt.docs.join("\n");
                            }
                            Statement::Rel(stmt) => {
                                hover.docs = stmt.docs.join("\n");
                            }
                            Statement::Fmt(stmt) => {
                                hover.docs = stmt.docs.join("\n");
                            }
                            Statement::Map(stmt) => {
                                let first = parse_map_arm(&stmt.first.0 .1);
                                let second = parse_map_arm(&stmt.second.0 .1);
                                hover.path = format!("map {} {}", first, second);
                                hover.docs.clear();
                                break;
                            }
                        }
                    }
                }

                // check tokens, may replace hover
                for (token, range) in doc.tokens.iter() {
                    if cursor >= range.start() && cursor < range.end() {
                        match token {
                            Token::Sigil(sigil) => {
                                if *sigil == '?' {
                                    hover = get_ontol_docs("?").unwrap()
                                }
                            }
                            Token::Use => hover = get_ontol_docs("use").unwrap(),
                            Token::Def => hover = get_ontol_docs("def").unwrap(),
                            Token::Rel => hover = get_ontol_docs("rel").unwrap(),
                            Token::Fmt => hover = get_ontol_docs("fmt").unwrap(),
                            Token::Map => hover = get_ontol_docs("map").unwrap(),
                            Token::Pub => hover = get_ontol_docs("pub").unwrap(),
                            Token::Sym(ident) => {
                                match doc.defs.get(ident.as_str()) {
                                    // local defs
                                    Some((stmt, range)) => {
                                        hover.path = format!("{}.{}", doc.name, stmt.ident.0);
                                        hover.signature = get_signature(&doc.text, range, &self.regex);
                                        hover.docs = stmt.docs.join("\n");
                                    }
                                    // nonlocal defs
                                    None => {
                                        match get_ontol_docs(ident.as_str()) {
                                            Some(doc) => hover = doc,
                                            None => hover = HoverDoc::from(
                                                ident.as_str(),
                                                "#### Map variable\nLocal map variable or type reference"
                                            ),
                                        }
                                    }
                                }
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

impl CompiledRegex {
    pub fn new() -> Self {
        Self {
            comments: Regex::new(r"(?m-s)^\s*?\/\/.*\n?").unwrap(),
            leading_whitespace: Regex::new(r"(?m)^\s*").unwrap(),
            rel_parens: Regex::new(r"\(.*?\)").unwrap(),
            rel_subject: Regex::new(r":.*").unwrap(),
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
    let range = span.start as usize..span.end as usize;
    get_range(text, &range)
}

/// Convert a byte index Range to a zero-based line/char Range
pub fn get_range(text: &str, range: &std::ops::Range<usize>) -> Range {
    let mut lsp_range = Range::new(Position::new(0, 0), Position::new(0, 0));
    let mut start_set = false;
    let mut cursor = 0;

    for (line_index, line) in text.lines().enumerate() {
        if !start_set {
            if cursor + line.len() < range.start {
                cursor += line.len() + 1;
            } else {
                lsp_range.start.line = line_index as u32;
                lsp_range.start.character = (range.start - cursor) as u32;
                start_set = true
            }
        }
        if start_set {
            if cursor + line.len() < range.end {
                cursor += line.len() + 1;
            } else {
                lsp_range.end.line = line_index as u32;
                lsp_range.end.character = (range.end - cursor) as u32;
                break;
            }
        }
    }
    lsp_range
}

/// Get a stripped-down rendition of a statement
pub fn get_signature(text: &str, range: &std::ops::Range<usize>, regex: &CompiledRegex) -> String {
    let sig = text.substring(range.start(), range.end());
    let stripped = &regex.comments.replace_all(sig, "");
    regex
        .leading_whitespace
        .replace_all(stripped, "")
        .to_string()
}

/// Parse the identifier or path for a map arm to String
pub fn parse_map_arm(arm: &MapArm) -> String {
    match arm {
        MapArm::Struct(s) => parse_path(&s.path.0),
        MapArm::Binding { path, expr: _ } => parse_path(&path.0),
    }
}

/// Parse a map path to String
pub fn parse_path(path: &Path) -> String {
    match path {
        Path::Ident(id) => id.to_string(),
        Path::Path(p) => p
            .iter()
            .map(|(s, _)| s.to_string())
            .collect::<Vec<String>>()
            .join("."),
    }
}

/// Get a HoverDoc for ONTOL core keywords, values and types
pub fn get_ontol_docs(ident: &str) -> Option<HoverDoc> {
    match ident {
        "use" => Some(HoverDoc::from(
            "use",
            "#### Use statement\nImports the domain following `use` and provides a local namespace alias after `as`.",
        )),
        "def" => Some(HoverDoc::from(
            "def",
            "#### Def statement\nDefines a type.",
        )),
        "rel" => Some(HoverDoc::from(
            "rel",
            "#### Relation statement\nDefines properties related to a type, or relations between types."
        )),
        "fmt" => Some(HoverDoc::from(
            "fmt",
            "#### Format statement\nPattern building and -matching for string or sequence."
        )),
        "map" => Some(HoverDoc::from(
            "map",
            "#### Map statement\nMapping between two types."
        )),
        "pub" => Some(HoverDoc::from(
            "pub",
            "#### Public modifier\nMarks a type as public and importable from other domains.",
        )),
        "match" => Some(HoverDoc::from(
            "match",
            "#### Partial modifier\nPartial matching for `map` arm."
        )),
        "id" => Some(HoverDoc::from(
            "ontol.id",
            "#### Relation prop\nBinds an identifier to a type, making it an entity.",
        )),
        "is" => Some(HoverDoc::from(
            "ontol.is",
            "#### Relation prop\nThe subject type takes on all properties of another type, or binds the subject type to a union.",
        )),
        "min" => Some(HoverDoc::from(
            "ontol.min",
            "#### Relation prop\nMinimum value for the subject type.",
        )),
        "max" => Some(HoverDoc::from(
            "ontol.max",
            "#### Relation prop\nMaximum value for the subject type.",
        )),
        "default" => Some(HoverDoc::from(
            "ontol.default",
            "#### Relation prop\nAssigns a default value to a type or property if none is given.",
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
            "#### Primitive\nBoolean type."
        )),
        "true" => Some(HoverDoc::from(
            "ontol.true",
            "#### Primitive\nBoolean `true` value."
        )),
        "false" => Some(HoverDoc::from(
            "ontol.false",
            "#### Primitive\nBoolean `false` value."
        )),
        "number" => Some(HoverDoc::from(
            "ontol.number",
            "#### Primitive\nAbstract number type.",
        )),
        "integer" => Some(HoverDoc::from(
            "ontol.integer",
            "#### Primitive\nAbstract integer type.",
        )),
        "i64" => Some(HoverDoc::from(
            "ontol.i64",
            "#### Primitive\n64-bit signed integer.",
        )),
        "float" => Some(HoverDoc::from(
            "ontol.float",
            "#### Primitive\nAbstract floating point number type.",
        )),
        "f64" => Some(HoverDoc::from(
            "ontol.f64",
            "#### Primitive\n64-bit floating point number.",
        )),
        "string" => Some(HoverDoc::from(
            "ontol.string",
            "#### Primitive\nAny UTF-8 string.",
        )),
        "datetime" => Some(HoverDoc::from(
            "ontol.datetime",
            "#### Primitive\nRFC 3339-formatted datetime string.",
        )),
        "date" => Some(HoverDoc::from(
            "ontol.date",
            "#### Primitive\nRFC 3339-formatted date string.",
        )),
        "time" => Some(HoverDoc::from(
            "ontol.time",
            "#### Primitive\nRFC 3339-formatted time string.",
        )),
        "uuid" => Some(HoverDoc::from(
            "ontol.uuid",
            "#### Primitive\nUUID v4 string."
        )),
        "regex" => Some(HoverDoc::from(
            "regex",
            "#### Primitive\nRegular expression.",
        )),
        "?" => Some(HoverDoc::from(
            "?",
            "#### Optional modifier\nMakes the property optional.",
        )),
        _ => None,
    }
}

/// Completions for built-in keywords and defs
pub fn get_core_completions() -> Vec<CompletionItem> {
    vec![
        CompletionItem {
            label: "ontol".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "def".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "rel".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "map".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "use".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "as".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "pub".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "fmt".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "match".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionItem {
            label: "id".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "is".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "min".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "max".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "gen".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "auto".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "create_time".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "update_time".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "default".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "example".to_string(),
            kind: Some(CompletionItemKind::PROPERTY),
            ..Default::default()
        },
        CompletionItem {
            label: "boolean".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "true".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "false".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "number".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "integer".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "float".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "i64".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "f64".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "string".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "datetime".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "date".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "time".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "uuid".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
        CompletionItem {
            label: "regex".to_string(),
            kind: Some(CompletionItemKind::UNIT),
            ..Default::default()
        },
    ]
}

/// A list of reserved words in ONTOL, to separate them from user-defined symbols
const RESERVED_WORDS: [&str; 34] = [
    "ontol",
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
    "min",
    "max",
    "default",
    "example",
    "gen",
    "auto",
    "create_time",
    "update_time",
    "number",
    "boolean",
    "true",
    "false",
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
];
