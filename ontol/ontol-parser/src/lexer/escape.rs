use std::ops::Range;

use chumsky::error::Simple;
use logos::Lexer;

use crate::lexer::kind::Kind;

pub fn escape_text_literal(
    lexer: &Lexer<Kind>,
    kind: Kind,
    errors: &mut Vec<Simple<char>>,
) -> String {
    let slice = lexer.slice();
    let slice = &slice[1..slice.len() - 1];
    let mut out = String::with_capacity(slice.len());

    let mut chars = slice.char_indices();

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
                        errors.push(invalid_escape_code(lexer, idx));
                    }
                }
                (idx, '"') => {
                    if kind == Kind::DoubleQuoteText {
                        out.push('"');
                    } else {
                        errors.push(invalid_escape_code(lexer, idx));
                    }
                }
                (idx, _other) => errors.push(invalid_escape_code(lexer, idx)),
            },
            _ => out.push(char),
        }
    }

    out
}

pub fn escape_regex(lexer: &Lexer<Kind>) -> String {
    let slice = lexer.slice();
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

fn invalid_escape_code(lexer: &Lexer<Kind>, offset: usize) -> Simple<char> {
    Simple::custom(char_span(lexer, offset), "invalid escape code")
}

fn char_span(lexer: &Lexer<Kind>, offset: usize) -> Range<usize> {
    let start = lexer.span().start + offset;
    start..start + 1
}
