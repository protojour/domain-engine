use std::{fmt::Display, ops::Range};

use chumsky::prelude::*;
use logos::Logos;
use smartstring::alias::String;

use crate::Spanned;

use self::escape::{escape_regex, escape_text_literal};

pub(crate) mod escape;
pub(crate) mod kind;

use kind::Kind;

#[allow(dead_code)]
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Token {
    Open(char),
    Close(char),
    Sigil(char),
    Use,
    Def,
    Rel,
    Fmt,
    Map,
    FatArrow,
    DotDot,
    Number(String),
    TextLiteral(String),
    Regex(String),
    Sym(String),
    Modifier(Modifier),
    UnknownModifer(String),
    DocComment(std::string::String),
    /// Used in error reporting only
    Expected(&'static str),
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open(c) | Self::Close(c) | Self::Sigil(c) => write!(f, "`{c}`"),
            Self::Use => write!(f, "`use`"),
            Self::Def => write!(f, "`def`"),
            Self::Rel => write!(f, "`rel`"),
            Self::Fmt => write!(f, "`fmt`"),
            Self::Map => write!(f, "`map`"),
            Self::DotDot => write!(f, "`..`"),
            Self::FatArrow => write!(f, "`=>`"),
            Self::Number(_) => write!(f, "`number`"),
            Self::TextLiteral(_) => write!(f, "`string`"),
            Self::Regex(_) => write!(f, "`regex`"),
            Self::Sym(sym) => write!(f, "`{sym}`"),
            Self::Modifier(m) => write!(f, "`{m}`"),
            Self::UnknownModifer(m) => write!(f, "`{m}`"),
            Self::DocComment(_) => write!(f, "`doc_comment`"),
            Self::Expected(expected) => write!(f, "{expected}"),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Modifier {
    Private,
    Open,
    Extern,
    Symbol,
    Match,
    In,
    AllIn,
    ContainsAll,
    Intersects,
    Equals,
}

impl Display for Modifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Modifier::Private => "private",
            Modifier::Open => "open",
            Modifier::Extern => "extern",
            Modifier::Symbol => "symbol",
            Modifier::Match => "match",
            Modifier::In => "in",
            Modifier::AllIn => "all_in",
            Modifier::ContainsAll => "contains_all",
            Modifier::Intersects => "intersects",
            Modifier::Equals => "equals",
        };

        write!(f, "@{str}")
    }
}

pub fn lex(input: &str) -> (Vec<Spanned<Token>>, Vec<Simple<char>>) {
    let mut lexer = Kind::lexer(input);
    let mut tokens: Vec<Spanned<Token>> = vec![];
    let mut errors: Vec<Simple<char>> = vec![];

    let mut error_range: Option<Range<usize>> = None;

    while let Some(result) = lexer.next() {
        match result {
            Ok(kind) => {
                maybe_report_invalid_span(error_range.take(), input, &mut errors);

                let token: Token = match kind {
                    Kind::Whitespace | Kind::Comment => {
                        continue;
                    }
                    Kind::DocComment => {
                        let slice = lexer.slice();
                        let slice = slice.strip_prefix("///").unwrap();

                        Token::DocComment(slice.into())
                    }
                    Kind::ParenOpen | Kind::CurlyOpen | Kind::SquareOpen => {
                        Token::Open(lexer.slice().chars().next().unwrap())
                    }
                    Kind::ParenClose | Kind::CurlyClose | Kind::SquareClose => {
                        Token::Close(lexer.slice().chars().next().unwrap())
                    }
                    Kind::Dot
                    | Kind::Comma
                    | Kind::Colon
                    | Kind::Question
                    | Kind::Underscore
                    | Kind::Plus
                    | Kind::Minus
                    | Kind::Star
                    | Kind::Div
                    | Kind::Equals
                    | Kind::Lt
                    | Kind::Gt
                    | Kind::Pipe => Token::Sigil(lexer.slice().chars().next().unwrap()),
                    Kind::DotDot => Token::DotDot,
                    Kind::FatArrow => Token::FatArrow,
                    Kind::KwUse => Token::Use,
                    Kind::KwDef => Token::Def,
                    Kind::KwRel => Token::Rel,
                    Kind::KwFmt => Token::Fmt,
                    Kind::KwMap => Token::Map,
                    Kind::Modifier => match lexer.slice() {
                        "@private" => Token::Modifier(Modifier::Private),
                        "@open" => Token::Modifier(Modifier::Open),
                        "@extern" => Token::Modifier(Modifier::Extern),
                        "@symbol" => Token::Modifier(Modifier::Symbol),
                        "@match" => Token::Modifier(Modifier::Match),
                        "@in" => Token::Modifier(Modifier::In),
                        "@all_in" => Token::Modifier(Modifier::AllIn),
                        "@contains_all" => Token::Modifier(Modifier::ContainsAll),
                        "@intersects" => Token::Modifier(Modifier::Intersects),
                        "@equals" => Token::Modifier(Modifier::Equals),
                        _ => Token::UnknownModifer(lexer.slice().into()),
                    },
                    Kind::Number => Token::Number(lexer.slice().into()),
                    Kind::DoubleQuoteText => {
                        Token::TextLiteral(escape_text_literal(&lexer, kind, &mut errors))
                    }
                    Kind::SingleQuoteText => {
                        Token::TextLiteral(escape_text_literal(&lexer, kind, &mut errors))
                    }
                    Kind::Regex => Token::Regex(escape_regex(&lexer)),
                    Kind::Sym => Token::Sym(lexer.slice().into()),
                };

                tokens.push((token, lexer.span()));
            }
            Err(_) => match &mut error_range {
                Some(range) => {
                    range.end = lexer.span().end();
                }
                None => {
                    error_range = Some(lexer.span());
                }
            },
        }
    }

    maybe_report_invalid_span(error_range.take(), input, &mut errors);

    (tokens, errors)
}

fn maybe_report_invalid_span(
    span: Option<Range<usize>>,
    input: &str,
    errors: &mut Vec<Simple<char>>,
) {
    if let Some(span) = span {
        let msg = if span.len() > 1 {
            format!("illegal characters `{}`", &input[span.clone()])
        } else {
            format!("illegal character `{}`", &input[span.clone()])
        };

        errors.push(Simple::custom(span, msg));
    }
}
