use std::fmt::Display;

use chumsky::prelude::*;
use smartstring::alias::String;

// note: reusing some old parsers for now:
use crate::s_parse::tree;

use super::Spanned;

#[allow(dead_code)]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
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

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(c) | Self::Close(c) | Self::Sigil(c) => write!(f, "`{c}`"),
            Self::Type => write!(f, "`type`"),
            Self::Rel => write!(f, "`rel`"),
            Self::Eq => write!(f, "`eq`"),
            Self::Number(_) => write!(f, "`number`"),
            Self::StringLiteral(_) => write!(f, "`string`"),
            Self::Regex(_) => write!(f, "`regex`"),
            Self::Sym(sym) => write!(f, "`{sym}`"),
            Self::DocComment(_) => write!(f, "`doc_comment`"),
        }
    }
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
        .or(one_of(".:?_+-*|").map(Token::Sigil))
        .or(one_of("({[").map(Token::Open))
        .or(one_of(")}]").map(Token::Close))
        .or(tree::num().map(Token::Number))
        .or(tree::double_quote_string_literal().map(Token::StringLiteral))
        .or(tree::single_quote_string_literal().map(Token::StringLiteral))
        .or(tree::regex().map(Token::Regex))
        .or(just('/').then_ignore(none_of("/")).map(Token::Sigil))
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
