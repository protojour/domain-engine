use std::ops::Range;

use crate::{ParserError, lexer::kind::Kind};

pub type UnescapeTextResult = Result<String, Vec<ParserError>>;

pub fn unescape_text_literal(kind: Kind, slice: &str, span: Range<usize>) -> UnescapeTextResult {
    let slice = &slice[1..slice.len() - 1];
    let mut out = String::with_capacity(slice.len());

    let mut chars = slice.char_indices();

    let mut errors = vec![];

    while let Some((_idx, char)) = chars.next() {
        match char {
            '\\' => match chars.next().unwrap() {
                (_, '\\') => out.push('\\'),
                (_, 'b') => out.push('\x08'),
                (_, 'f') => out.push('\x0C'),
                (_, 'n') => out.push('\n'),
                (_, 'r') => out.push('\r'),
                (_, 't') => out.push('\t'),
                (idx, '\'') => {
                    if kind == Kind::SingleQuoteText {
                        out.push('\'');
                    } else {
                        errors.push(invalid_escape_code(span.start, idx));
                    }
                }
                (idx, '"') => {
                    if kind == Kind::DoubleQuoteText {
                        out.push('"');
                    } else {
                        errors.push(invalid_escape_code(span.start, idx));
                    }
                }
                (idx, _other) => errors.push(invalid_escape_code(span.start, idx)),
            },
            _ => out.push(char),
        }
    }

    if errors.is_empty() {
        Ok(out)
    } else {
        Err(errors)
    }
}

pub fn unescape_regex(slice: &str) -> String {
    let slice = &slice[1..slice.len() - 1];
    let mut out = String::with_capacity(slice.len());

    let mut chars = slice.chars();

    // handle initial escape of space
    if let Some(next) = chars.next() {
        if next == '\\' {
            if let Some(next) = chars.next() {
                match next {
                    ' ' | '/' => {
                        out.push(next);
                    }
                    _ => {
                        out.push('\\');
                        out.push(next);
                    }
                }
            }
        } else {
            out.push(next);
        }
    }

    while let Some(char) = chars.next() {
        if char == '\\' {
            let next = chars.next().unwrap();
            if next == '/' {
                out.push('/');
            } else {
                out.push('\\');
                out.push(next);
            }
        } else {
            out.push(char);
        }
    }

    out
}

fn invalid_escape_code(span_start: usize, offset: usize) -> ParserError {
    let start = span_start + offset;
    let end = start + 1;

    ParserError {
        span: (start..end).into(),
        msg: "invalid escape code".into(),
    }
}
