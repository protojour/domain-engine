//! Temporary inclusion of ONTOL's old chumsky parser.
//!
//! We keep this here until the LSP gets rewritten to use CST trees.

use std::{ops::Range, str::FromStr};

use chumsky::{Parser, Stream};
use logos::Logos;
use ontol_parser::{
    lexer::{
        extend_contiguous_ranges, format_lex_error,
        kind::Kind,
        unescape::{unescape_regex, unescape_text_literal},
    },
    Error, ParserError, U32Span,
};

use self::{ast::Statement, modifier::Modifier, token::Token};

pub mod ast;
pub mod modifier;
pub mod parser;
pub mod token;

pub type Span = Range<usize>;
pub type Spanned<T> = (T, Span);

pub fn parse_statements(input: &str) -> (Vec<Spanned<Statement>>, Vec<Error>) {
    let mut errors = vec![];

    let tokens = {
        let (tokens, lex_errors) = ast_lex(input);

        for lex_error in lex_errors {
            errors.push(Error::Lex(lex_error));
        }

        tokens
    };

    let statements = {
        let len = tokens.len();
        let stream = Stream::from_iter(len..len + 1, tokens.into_iter());
        let (statements, parse_errors) = parser::statement_sequence().parse_recovery(stream);

        for chumsky_error in parse_errors {
            errors.push(Error::Parse(ParserError {
                msg: format!("{}", chumsky_error),
                span: chumsky_error.span().into(),
            }));
        }

        statements
    };

    (statements.unwrap_or_default(), errors)
}

/// The classic lexer that is the basis for the AST parser
pub fn ast_lex(input: &str) -> (Vec<Spanned<Token>>, Vec<ParserError>) {
    let mut lexer = Kind::lexer(input);
    let mut tokens: Vec<Spanned<Token>> = vec![];
    let mut errors: Vec<ParserError> = vec![];

    let mut error_ranges: Vec<U32Span> = vec![];

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
                    Kind::DoubleQuoteText | Kind::SingleQuoteText => {
                        match unescape_text_literal(kind, lexer.slice(), lexer.span()) {
                            Ok(string) => Token::TextLiteral(string),
                            Err(unescape_errors) => {
                                for error in unescape_errors {
                                    errors.push(error);
                                }

                                Token::TextLiteral(String::new())
                            }
                        }
                    }
                    Kind::Regex => Token::Regex(unescape_regex(lexer.slice())),
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
                    | Kind::TypeModUnit
                    | Kind::TypeModSet
                    | Kind::TypeModList
                    | Kind::This
                    | Kind::Literal
                    | Kind::NumberRange
                    | Kind::RangeStart
                    | Kind::RangeEnd
                    | Kind::Name
                    | Kind::IdentPath
                    | Kind::PatStruct
                    | Kind::PatSet
                    | Kind::PatAtom
                    | Kind::PatBinary
                    | Kind::StructParamAttrProp
                    | Kind::StructParamAttrUnit
                    | Kind::Spread
                    | Kind::RelArgs
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
