#![forbid(unsafe_code)]

use ast::Statement;
use chumsky::{prelude::*, Stream};
use cst::{grammar, parser::CstParser};
use lexer::LexedSource;

use std::ops::Range;

pub mod ast;
mod cst;
pub mod lexer;
mod modifier;
mod parser;

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

pub fn parse_statements(input: &str) -> (Vec<Spanned<Statement>>, Vec<Error>) {
    let mut errors = vec![];

    if TEST_CST {
        let (lexed, lex_errors) = LexedSource::lex(input);

        for lex_error in lex_errors {
            errors.push(Error::Lex(lex_error));
        }

        let mut cst_parser = CstParser::from_lexed_source(input, lexed);

        grammar::ontol(&mut cst_parser);

        let (_, parse_errors) = cst_parser.finish();

        for (span, msg) in parse_errors {
            errors.push(Error::Parse(Simple::custom(span, msg)));
        }
    }

    let tokens = {
        let (tokens, lex_errors) = lexer::lex(input);

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

        for parse_error in parse_errors {
            errors.push(Error::Parse(parse_error));
        }
        statements
    };

    (statements.unwrap_or_default(), errors)
}
