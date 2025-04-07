#![forbid(unsafe_code)]

use cst::{grammar, parser::CstParser, tree::FlatSyntaxTree};
use lexer::cst_lex;
use ontol_core::{error::SpannedMsgError, span::U32Span};
use unindent::unindent;

use std::ops::Range;

pub mod basic_syntax;
pub mod cst;
pub mod lexer;
pub mod source;
pub mod topology;

pub enum ParserError {
    Lex(SpannedMsgError),
    Parse(SpannedMsgError),
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod cst_test;

pub trait ToUsizeRange {
    fn to_usize_range(&self) -> Range<usize>;
}

impl ToUsizeRange for U32Span {
    fn to_usize_range(&self) -> Range<usize> {
        self.start as usize..self.end as usize
    }
}

pub fn cst_parse(source: &str) -> (FlatSyntaxTree, Vec<ParserError>) {
    cst_parse_grammar(source, grammar::ontol)
}

pub fn cst_parse_grammar(
    source: &str,
    grammar: fn(&mut CstParser),
) -> (FlatSyntaxTree, Vec<ParserError>) {
    let (lexed, lex_errors) = cst_lex(source);

    let mut errors = vec![];
    for lex_error in lex_errors {
        errors.push(ParserError::Lex(lex_error));
    }

    let mut cst_parser = CstParser::from_lexed_source(source, lexed);

    grammar(&mut cst_parser);

    let (tree, parse_errors) = cst_parser.finish();

    for (span, msg) in parse_errors {
        errors.push(ParserError::Parse(SpannedMsgError { msg, span }));
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
