use chumsky::prelude::*;
use lsp_types::{CompletionItem, CompletionItemKind, Position, Range};
use ontol_compiler::{
    error::{CompileError, UnifiedCompileError},
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, ParsedPackage},
    Compiler, SourceCodeRegistry, SourceSpan, Sources, SpannedCompileError,
};
use ontol_parser::{
    ast::{DefStatement, MapArm, Path, Statement},
    lexer::lexer,
    parse_statements, Spanned, Token,
};
use ontol_runtime::config::PackageConfig;
use regex::Regex;
use std::{
    collections::{HashMap, HashSet},
    format,
};
use substring::Substring;

#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct State {
    pub docs: HashMap<String, Document>,
    pub root: Option<String>,
}

impl State {
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

            fn explore(
                statements: &Vec<Spanned<Statement>>,
                nested: &mut Vec<Spanned<Statement>>,
                types: &mut HashMap<String, Spanned<DefStatement>>,
                level: u8,
            ) {
                for (statement, range) in statements {
                    if level > 0 {
                        nested.push((statement.clone(), range.clone()))
                    }
                    match statement {
                        Statement::Use(_) => (),
                        Statement::Def(stmt) => {
                            let name = stmt.ident.0.to_string();
                            types.insert(name, (stmt.clone(), range.clone()));

                            explore(&stmt.block.0, nested, types, level + 1);
                        }
                        Statement::Rel(stmt) => {
                            for rel in &stmt.relations {
                                if let Some((ctx_block, _)) = &rel.ctx_block {
                                    explore(ctx_block, nested, types, level + 1)
                                }
                            }
                        }
                        Statement::Fmt(_) => (),
                        Statement::Map(_) => (),
                    }
                }
            }

            doc.types.clear();

            let (mut statements, _) = parse_statements(doc.text.as_str());
            let mut nested: Vec<Spanned<Statement>> = vec![];

            explore(&statements, &mut nested, &mut doc.types, 0);

            statements.append(&mut nested);
            doc.statements = statements;
        }
    }

    pub fn compile(&self) -> Result<(), UnifiedCompileError> {
        if let Some(root) = &self.root {
            let root_name = get_source_name(root);
            let mut ontol_sources = Sources::default();
            let mut source_code_registry = SourceCodeRegistry::default();
            let mut package_graph_builder = PackageGraphBuilder::new(root_name.into());

            let topology = loop {
                match package_graph_builder.transition().unwrap() {
                    GraphState::RequestPackages { builder, requests } => {
                        package_graph_builder = builder;

                        for request in requests {
                            let source_name = match &request.reference {
                                PackageReference::Named(source_name) => source_name.as_str(),
                            };
                            let package_config = PackageConfig::default();

                            match self.docs.get(&format!("file:///{}.on", source_name)) {
                                Some(source_text) => {
                                    package_graph_builder.provide_package(ParsedPackage::parse(
                                        request,
                                        &source_text.text,
                                        package_config,
                                        &mut ontol_sources,
                                        &mut source_code_registry,
                                    ));
                                }
                                None => {
                                    // return Ok(());
                                    return Err(UnifiedCompileError {
                                        errors: vec![SpannedCompileError {
                                            error: CompileError::PackageNotFound,
                                            span: SourceSpan::default(),
                                            notes: vec![],
                                        }],
                                    });
                                }
                            }
                        }
                    }
                    GraphState::Built(topology) => break topology,
                }
            };

            let mem = Mem::default();
            let mut compiler = Compiler::new(&mem, ontol_sources.clone()).with_ontol();
            return compiler.compile_package_topology(topology);
        }
        // TODO: Handle
        Ok(())
    }
}

fn get_source_name(name: &str) -> &str {
    match name.strip_prefix("file:///") {
        Some(name) => match name.strip_suffix(".on") {
            Some(name) => name,
            None => name,
        },
        None => name,
    }
}

#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct Document {
    pub text: String,
    pub tokens: Vec<Spanned<Token>>,
    pub symbols: HashSet<String>,
    pub types: HashMap<String, Spanned<DefStatement>>,
    pub statements: Vec<Spanned<Statement>>,
    // pub errors: Vec<Error>,
}

#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct DocPanel {
    pub path: String,
    pub signature: String,
    pub docs: String,
}

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

    pub fn get_hover_docs(&self, url: &str, lineno: usize, col: usize) -> DocPanel {
        let mut cursor = 0;

        for (index, line) in self.text.lines().enumerate() {
            if index == lineno {
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
                        if let Some((stmt, range)) = self.types.get(val.as_str()) {
                            dp.path = format!("{}.{}", basepath, stmt.ident.0);
                            dp.signature = self.get_signature(range);
                            dp.docs = stmt.docs.join("\n");
                        } else {
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
                                        "### Relation prop\nBinds a type to a union".to_string();
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
                                        "### Relation prop\nAssigns a default to value".to_string();
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
                                "float" => {
                                    dp.path = "ontol.float".to_string();
                                    dp.signature = val.to_string();
                                    dp.docs = "### Scalar\nFloating point number".to_string();
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

pub fn get_builtins() -> Vec<CompletionItem> {
    vec![
        CompletionItem {
            label: "type".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("type …".to_string()),
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
            detail: Some("pub type …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "fmt".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("fmt … => …".to_string()),
            ..Default::default()
        },
        CompletionItem {
            label: "with".to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some("with … { … }".to_string()),
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
            label: "int".to_string(),
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

const RESERVED_WORDS: [&str; 24] = [
    "use", "as", "pub", "type", "with", "rel", "fmt", "map", "unify", "id", "is", "gen", "auto",
    "default", "boolean", "int", "float", "number", "string", "datetime", "date", "time", "uuid",
    "regex",
];
