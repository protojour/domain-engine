use ast::Stmt;
use chumsky::{prelude::*, Stream};

use std::ops::Range;

pub mod ast;

mod lexer;
mod parser;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);

pub use lexer::Token;

pub enum Error {
    Lex(Simple<char>),
    Parse(Simple<Token>),
}

pub fn parse_statements(input: &str) -> Result<Vec<Spanned<Stmt>>, Vec<Error>> {
    let mut errors = vec![];
    let (tokens, lex_errors) = lexer::lexer().parse_recovery(input);

    for lex_error in lex_errors {
        errors.push(Error::Lex(lex_error));
    }

    let statements = if let Some(tokens) = tokens {
        let len = tokens.len();
        let stream = Stream::from_iter(len..len + 1, tokens.into_iter());
        let (statements, parse_errors) = parser::stmt_seq().parse_recovery(stream);

        for parse_error in parse_errors {
            errors.push(Error::Parse(parse_error));
        }
        statements
    } else {
        None
    };

    if let Some(statements) = statements {
        Ok(statements)
    } else {
        Err(errors)
    }
}
