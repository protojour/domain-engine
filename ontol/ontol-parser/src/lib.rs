#![forbid(unsafe_code)]

use cst::{grammar, parser::CstParser, tree::FlatSyntaxTree};
use lexer::cst_lex;
use unindent::unindent;

use std::ops::Range;

pub mod cst;
pub mod lexer;
pub mod syntax;

#[cfg(test)]
mod cst_test;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);

#[cfg(test)]
mod tests;

pub enum Error {
    Lex(ParserError),
    Parse(ParserError),
}

#[derive(Debug)]
pub struct ParserError {
    pub msg: String,
    pub span: Range<usize>,
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
        errors.push(Error::Parse(ParserError { msg, span }));
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
