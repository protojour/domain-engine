#![forbid(unsafe_code)]

use ast::Statement;
use chumsky::{prelude::*, Stream};
use cst::{grammar, parser::CstParser, tree::FlatSyntaxTree};
use lexer::cst_lex;
use unindent::unindent;

use std::ops::Range;

pub mod ast;
pub mod cst;
pub mod lexer;
mod modifier;
mod parser;
pub mod syntax;

#[cfg(test)]
mod cst_test;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);

pub use lexer::Token;

#[cfg(test)]
mod tests;

const TEST_CST: bool = false;

pub enum Error {
    Lex(Simple<char>),
    Parse(Simple<Token>),
}

#[derive(Debug)]
pub struct UnescapeError {
    pub msg: String,
    pub span: Range<usize>,
}

pub fn parse_statements(input: &str) -> (Vec<Spanned<Statement>>, Vec<Error>) {
    let mut errors = vec![];

    if TEST_CST {
        let (flat_tree, cst_errors) = cst_parse(input);
        let _ = flat_tree.unflatten();

        errors.extend(cst_errors);
    }

    let tokens = {
        let (tokens, lex_errors) = lexer::ast_lex(input);

        if !TEST_CST {
            for lex_error in lex_errors {
                errors.push(Error::Lex(lex_error));
            }
        }

        tokens
    };

    let statements = {
        let len = tokens.len();
        let stream = Stream::from_iter(len..len + 1, tokens.into_iter());
        let (statements, parse_errors) = parser::statement_sequence().parse_recovery(stream);

        if !TEST_CST {
            for parse_error in parse_errors {
                errors.push(Error::Parse(parse_error));
            }
        }

        statements
    };

    (statements.unwrap_or_default(), errors)
}

pub fn cst_parse(source: &str) -> (FlatSyntaxTree, Vec<Error>) {
    let (lexed, lex_errors) = cst_lex(source);

    let mut errors = vec![];
    for lex_error in lex_errors {
        errors.push(Error::Lex(lex_error));
    }

    let mut cst_parser = CstParser::from_lexed_source(source, lexed);

    grammar::ontol(&mut cst_parser);

    let (tree, parse_errors) = cst_parser.finish();

    for (span, msg) in parse_errors {
        errors.push(Error::Parse(Simple::custom(span, msg)));
    }

    (tree, errors)
}

pub fn join_doc_lines<'a>(doc_lines: impl Iterator<Item = &'a str>) -> Option<String> {
    let mut joined = "\n".to_string();

    let mut line_iter = doc_lines.peekable();
    let mut count = 0;

    while let Some(line) = line_iter.next() {
        count += 1;
        joined.push_str(line);
        if line_iter.peek().is_some() {
            joined.push('\n');
        }
    }

    if count > 0 {
        Some(unindent(&joined))
    } else {
        None
    }
}
