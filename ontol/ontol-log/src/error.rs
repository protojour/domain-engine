use std::backtrace::Backtrace;

use ontol_core::{error::SpannedMsgError, span::U32Span};
use ontol_parser::{ParserError, topology::MakeParseError};

use crate::token::Token;

#[derive(Debug)]
pub enum SemError {
    Ontol,
    ArcSyntax(U32Span),
    Syntax(Backtrace),
    SymbolAlreadyDefined,
    Lookup(Token, U32Span),
    MissingContext(U32Span),
    UnionInUnion,
    SubScoping(U32Span),
    Apply(ApplyError),
    Parse(SpannedMsgError),
    Lex(SpannedMsgError),
    ParseMulti(Vec<SpannedMsgError>),
    ArcSlotRemoval,
    Todo(String),
}

#[derive(Debug)]
pub enum ApplyError {
    UseNotFound,
    DefNotFound,
    ArcNotFound,
    RelNotFound,
    RelWithoutKey,
    DuplicateIdent,
    IdentNotRemovable,
}

pub trait OptionExt<T> {
    /// Turn into a syntax error
    fn stx_err(self) -> Result<T, SemError>;

    fn state_err(self) -> Result<T, SemError>;
}

impl<T> OptionExt<T> for Option<T> {
    fn stx_err(self) -> Result<T, SemError> {
        self.ok_or(SemError::Syntax(Backtrace::capture()))
    }

    fn state_err(self) -> Result<T, SemError> {
        self.ok_or_else(|| SemError::Todo("BUG: state".to_string()))
    }
}

impl MakeParseError for SemError {
    fn make_parse_error(msg: String, span: U32Span) -> Self {
        Self::Parse(SpannedMsgError { msg, span })
    }
}

impl From<ParserError> for SemError {
    fn from(value: ParserError) -> Self {
        match value {
            ParserError::Lex(err) => Self::Lex(err),
            ParserError::Parse(err) => Self::Parse(err),
        }
    }
}
