use std::{fmt::Display, ops::Range, str::FromStr};

use chumsky::prelude::*;
use logos::Logos;

use crate::{modifier::Modifier, Spanned};

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

/// The classic lexer that is the basis for the AST parser
pub fn lex(input: &str) -> (Vec<Spanned<Token>>, Vec<Simple<char>>) {
    let mut lexer = Kind::lexer(input);
    let mut tokens: Vec<Spanned<Token>> = vec![];
    let mut errors: Vec<Simple<char>> = vec![];

    let mut error_ranges: Vec<Range<usize>> = vec![];

    while let Some(result) = lexer.next() {
        match result {
            Ok(kind) => {
                let token: Token = match kind {
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
                    Kind::Modifier => Modifier::from_str(lexer.slice())
                        .map(Token::Modifier)
                        .unwrap_or_else(|_| Token::UnknownModifer(lexer.slice().into())),
                    Kind::Number => Token::Number(lexer.slice().into()),
                    Kind::DoubleQuoteText => {
                        Token::TextLiteral(escape_text_literal(&lexer, kind, &mut errors))
                    }
                    Kind::SingleQuoteText => {
                        Token::TextLiteral(escape_text_literal(&lexer, kind, &mut errors))
                    }
                    Kind::Regex => Token::Regex(escape_regex(&lexer)),
                    Kind::Sym => Token::Sym(lexer.slice().into()),
                    Kind::Whitespace
                    | Kind::Comment
                    | Kind::Eof
                    | Kind::Error
                    | Kind::Ontol
                    | Kind::UseStatement
                    | Kind::DefStatement
                    | Kind::DefBody
                    | Kind::RelStatement
                    | Kind::RelSubject
                    | Kind::RelFwdSet
                    | Kind::RelBackwdSet
                    | Kind::Relation
                    | Kind::RelObject
                    | Kind::RelParams
                    | Kind::PropCardinality
                    | Kind::FmtStatement
                    | Kind::MapStatement
                    | Kind::MapArm
                    | Kind::UnitTypeRef
                    | Kind::SetTypeRef
                    | Kind::SeqTypeRef
                    | Kind::This
                    | Kind::Literal
                    | Kind::Range
                    | Kind::Location
                    | Kind::IdentPath
                    | Kind::PatStruct
                    | Kind::PatSet
                    | Kind::PatAtom
                    | Kind::PatBinary
                    | Kind::StructParamAttrProp
                    | Kind::StructParamAttrUnit
                    | Kind::Spread
                    | Kind::StructAttrRelArgs
                    | Kind::SetElement => {
                        continue;
                    }
                };

                tokens.push((token, lexer.span()));
            }
            Err(_) => extend_contiguous_ranges(&mut error_ranges, lexer.span()),
        }
    }

    let errors = error_ranges
        .into_iter()
        .map(|span| format_lex_error(span, input))
        .collect();

    (tokens, errors)
}

/// Raw lexed source for the CST parser
pub struct LexedSource {
    pub tokens: Vec<Kind>,
    pos: Vec<usize>,
}

impl LexedSource {
    pub fn lex(source: &str) -> (Self, Vec<Simple<char>>) {
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

        (Self { tokens, pos }, errors)
    }

    pub fn span(&self, index: usize) -> Range<usize> {
        let start = self.pos[index];
        let end = self.pos[index + 1];

        start..end
    }

    pub fn opt_span(&self, index: usize) -> Option<Range<usize>> {
        if index < self.tokens.len() {
            Some(self.span(index))
        } else {
            None
        }
    }
}

fn format_lex_error(span: Range<usize>, source: &str) -> Simple<char> {
    let msg = if span.len() > 1 {
        format!("illegal characters `{}`", &source[span.clone()])
    } else {
        format!("illegal character `{}`", &source[span.clone()])
    };

    Simple::custom(span, msg)
}

fn extend_contiguous_ranges(ranges: &mut Vec<Range<usize>>, span: Range<usize>) {
    match ranges.last_mut() {
        Some(last) => {
            if span.start() == last.end() {
                last.end = span.end();
            } else {
                ranges.push(span);
            }
        }
        None => {
            ranges.push(span);
        }
    }
}
