use chumsky::prelude::*;
use smartstring::alias::String;

// note: reusing some old parsers for now:
use crate::s_parse::tree;

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
    DocComment(String),
}

#[allow(dead_code)]
pub fn lexer() -> impl Parser<char, Vec<Spanned<Token>>, Error = Simple<char>> {
    let ident = tree::ident().map(|ident| match ident.as_str() {
        "type" => Token::Type,
        "rel" => Token::Rel,
        "eq" => Token::Eq,
        _ => Token::Sym(ident),
    });

    let comment = just("//")
        .ignore_then(filter(|c: &char| *c != '/'))
        .ignore_then(take_until(just('\n')))
        .padded();

    doc_comment()
        .or(one_of(".:?_+-*/|").map(Token::Sigil))
        .or(one_of("({[").map(Token::Open))
        .or(one_of(")}]").map(Token::Close))
        .or(tree::num().map(Token::Number))
        .or(tree::double_quote_string_literal().map(Token::StringLiteral))
        .or(tree::single_quote_string_literal().map(Token::StringLiteral))
        .or(tree::regex().map(Token::Regex))
        .or(ident)
        .map_with_span(|token, span| (token, span))
        .padded_by(comment.repeated())
        .padded()
        .repeated()
}

pub fn doc_comment() -> impl Parser<char, Token, Error = Simple<char>> {
    just("///")
        .ignore_then(take_until(just('\n')))
        .map(|(vec, _)| Token::DocComment(String::from_iter(vec.into_iter())))
}
