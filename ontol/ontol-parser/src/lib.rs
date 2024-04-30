#![forbid(unsafe_code)]

use cst::{grammar, parser::CstParser, tree::FlatSyntaxTree};
use lexer::cst_lex;
use unindent::unindent;

use std::{fmt::Debug, ops::Range};

pub mod cst;
pub mod lexer;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod cst_test;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct U32Span {
    pub start: u32,
    pub end: u32,
}

impl U32Span {
    pub fn len(&self) -> usize {
        self.end as usize - self.start as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains_usize(&self, pos: usize) -> bool {
        self.start as usize <= pos && self.end as usize > pos
    }
}

impl Debug for U32Span {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

impl From<Range<usize>> for U32Span {
    fn from(value: Range<usize>) -> Self {
        Self {
            start: value.start as u32,
            end: value.end as u32,
        }
    }
}

impl From<U32Span> for Range<usize> {
    fn from(value: U32Span) -> Self {
        value.start as usize..value.end as usize
    }
}

pub enum Error {
    Lex(ParserError),
    Parse(ParserError),
}

#[derive(Debug)]
pub struct ParserError {
    pub msg: String,
    pub span: U32Span,
}

pub trait ToUsizeRange {
    fn to_usize_range(&self) -> Range<usize>;
}

impl ToUsizeRange for U32Span {
    fn to_usize_range(&self) -> Range<usize> {
        self.start as usize..self.end as usize
    }
}

pub fn cst_parse(source: &str) -> (FlatSyntaxTree, Vec<Error>) {
    cst_parse_grammar(source, grammar::ontol)
}

pub fn cst_parse_grammar(
    source: &str,
    grammar: fn(&mut CstParser),
) -> (FlatSyntaxTree, Vec<Error>) {
    let (lexed, lex_errors) = cst_lex(source);

    let mut errors = vec![];
    for lex_error in lex_errors {
        errors.push(Error::Lex(lex_error));
    }

    let mut cst_parser = CstParser::from_lexed_source(source, lexed);

    grammar(&mut cst_parser);

    let (tree, parse_errors) = cst_parser.finish();

    for (span, msg) in parse_errors {
        errors.push(Error::Parse(ParserError { msg, span }));
    }

    (tree, errors)
}

pub fn join_doc_lines<T: AsRef<str>>(doc_lines: impl Iterator<Item = T>) -> Option<String> {
    let mut joined = "\n".to_string();

    let mut line_iter = doc_lines.peekable();
    let mut count = 0;

    while let Some(line) = line_iter.next() {
        count += 1;
        joined.push_str(line.as_ref());
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
