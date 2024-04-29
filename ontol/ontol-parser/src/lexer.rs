use std::ops::Range;

use logos::Logos;

pub mod kind;
pub mod unescape;

use kind::Kind;

use crate::ParserError;

/// The new lexer for CST
pub fn cst_lex(source: &str) -> (Lex, Vec<ParserError>) {
    let mut tokens = vec![];
    let mut pos: Vec<usize> = vec![];

    let mut error_ranges: Vec<Range<usize>> = vec![];

    let mut lexer = Kind::lexer(source);

    while let Some(result) = lexer.next() {
        pos.push(lexer.span().start);
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

    pos.push(source.len());

    let errors = error_ranges
        .into_iter()
        .map(|span| format_lex_error(span, source))
        .collect();

    (Lex { tokens, pos }, errors)
}

/// Raw lexed source for the CST parser
pub struct Lex {
    pub tokens: Vec<Kind>,
    pos: Vec<usize>,
}

impl Lex {
    pub fn span(&self, index: usize) -> Range<usize> {
        let start = self.pos[index];
        let end = self.pos[index + 1];

        start..end
    }

    pub fn span_end(&self, index: usize) -> usize {
        self.pos[index + 1]
    }

    pub fn opt_span(&self, index: usize) -> Option<Range<usize>> {
        if index < self.tokens.len() {
            Some(self.span(index))
        } else {
            None
        }
    }
}

pub fn format_lex_error(span: Range<usize>, source: &str) -> ParserError {
    let msg = if span.len() > 1 {
        format!("illegal characters `{}`", &source[span.clone()])
    } else {
        format!("illegal character `{}`", &source[span.clone()])
    };

    ParserError { msg, span }
}

pub fn extend_contiguous_ranges(ranges: &mut Vec<Range<usize>>, span: Range<usize>) {
    match ranges.last_mut() {
        Some(last) => {
            if span.start == last.end {
                last.end = span.end;
            } else {
                ranges.push(span);
            }
        }
        None => {
            ranges.push(span);
        }
    }
}
