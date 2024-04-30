use std::ops::Range;

use logos::Logos;

pub mod kind;
pub mod unescape;

use kind::Kind;

use crate::{ParserError, ToUsizeRange, U32Span};

const DEFAULT_TOKEN_CAPACITY: usize = 64;

/// The new lexer for CST
pub fn cst_lex(source: &str) -> (Lex, Vec<ParserError>) {
    if source.len() > u32::MAX as usize {
        panic!("ONTOL file too large");
    }

    let mut tokens = Vec::with_capacity(DEFAULT_TOKEN_CAPACITY);
    let mut pos: Vec<u32> = Vec::with_capacity(DEFAULT_TOKEN_CAPACITY);

    let mut error_ranges: Vec<U32Span> = vec![];

    let mut lexer = Kind::lexer(source);

    while let Some(result) = lexer.next() {
        pos.push(lexer.span().start as u32);
        match result {
            Ok(kind) => {
                tokens.push(kind);
            }
            Err(_) => {
                tokens.push(Kind::Error);
                extend_contiguous_ranges(&mut error_ranges, lexer.span());
            }
        }
    }

    pos.push(source.len() as u32);

    let errors = error_ranges
        .into_iter()
        .map(|span| format_lex_error(span, source))
        .collect();

    (Lex { tokens, pos }, errors)
}

/// Raw lexed source for the CST parser
pub struct Lex {
    tokens: Vec<Kind>,
    pos: Vec<u32>,
}

impl Lex {
    pub fn tokens(&self) -> &[Kind] {
        &self.tokens
    }

    pub fn kind(&self, index: usize) -> Kind {
        self.tokens[index]
    }

    pub fn span(&self, index: usize) -> U32Span {
        let start = self.pos[index];
        let end = self.pos[index + 1];

        U32Span { start, end }
    }

    pub fn span_end(&self, index: usize) -> u32 {
        self.pos[index + 1]
    }

    pub fn opt_span(&self, index: usize) -> Option<U32Span> {
        if index < self.tokens.len() {
            Some(self.span(index))
        } else {
            None
        }
    }
}

pub fn format_lex_error(span: U32Span, source: &str) -> ParserError {
    let msg = if span.len() > 1 {
        format!("illegal characters `{}`", &source[span.to_usize_range()])
    } else {
        format!("illegal character `{}`", &source[span.to_usize_range()])
    };

    ParserError { msg, span }
}

pub fn extend_contiguous_ranges(ranges: &mut Vec<U32Span>, span: Range<usize>) {
    match ranges.last_mut() {
        Some(last) => {
            if span.start == last.end as usize {
                last.end = span.end as u32;
            } else {
                ranges.push(span.into());
            }
        }
        None => {
            ranges.push(span.into());
        }
    }
}
