use chumsky::prelude::*;
use smartstring::alias::String;

// note: reusing some old parsers for now:
use crate::parse::tree;

use super::Spanned;

/*
fn span_map<T, U, F: Fn(T) -> U>(f: F) -> impl Fn(Spanned<T>) -> Spanned<U> {
    move |(data, span)| (f(data), span)
}
*/

#[allow(dead_code)]
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Token {
    Bracket(char),
    Dot,
    Colon,
    Questionmark,
    Underscore,
    Type,
    Rel,
    StringLiteral(String),
    Regex(String),
    Sym(String),
}

#[allow(dead_code)]
pub fn lexer() -> impl Parser<char, Vec<Spanned<Token>>, Error = Simple<char>> {
    let ident = tree::ident().map(|ident| match ident.as_str() {
        "type" => Token::Type,
        "rel" => Token::Rel,
        _ => Token::Sym(ident),
    });

    one_of("(){}[]")
        .map(|c| Token::Bracket(c))
        .or(just(".").map(|_| Token::Dot))
        .or(just(":").map(|_| Token::Colon))
        .or(just("?").map(|_| Token::Questionmark))
        .or(just("_").map(|_| Token::Underscore))
        .or(tree::double_quote_string_literal().map(Token::StringLiteral))
        .or(tree::single_quote_string_literal().map(Token::StringLiteral))
        .or(tree::regex().map(Token::Regex))
        .or(ident)
        .map_with_span(|token, span| (token, span))
        .padded()
        .repeated()
}
