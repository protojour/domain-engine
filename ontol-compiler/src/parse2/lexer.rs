use chumsky::prelude::*;
use smartstring::alias::String;

// note: reusing some old parsers for now:
use crate::parse::tree;

use super::Spanned;

#[allow(dead_code)]
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Token {
    Open(char),
    Close(char),
    Sigil(char),
    Type,
    Rel,
    Eq,
    Number(String),
    StringLiteral(String),
    Regex(String),
    Sym(String),
}

#[allow(dead_code)]
pub fn lexer() -> impl Parser<char, Vec<Spanned<Token>>, Error = Simple<char>> {
    let ident = tree::ident().map(|ident| match ident.as_str() {
        "type" => Token::Type,
        "rel" => Token::Rel,
        "eq" => Token::Eq,
        _ => Token::Sym(ident),
    });

    one_of(".:?_+-*/|")
        .map(|c| Token::Sigil(c))
        .or(one_of("({[").map(|c| Token::Open(c)))
        .or(one_of(")}]").map(|c| Token::Close(c)))
        .or(tree::num().map(Token::Number))
        .or(tree::double_quote_string_literal().map(Token::StringLiteral))
        .or(tree::single_quote_string_literal().map(Token::StringLiteral))
        .or(tree::regex().map(Token::Regex))
        .or(ident)
        .map_with_span(|token, span| (token, span))
        .padded()
        .repeated()
}
