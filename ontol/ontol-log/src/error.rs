use std::backtrace::Backtrace;

use ontol_parser::{ParserError, U32Span};

use crate::{sem_model::ApplyError, token::Token};

#[derive(Debug)]
pub enum SemError {
    Ontol,
    UseSyntax,
    ArcSyntax,
    Syntax(Backtrace),
    SymbolAlreadyDefined,
    PrevRelNotFound,
    Lookup(Token, U32Span),
    MissingContext,
    UnionInUnion,
    SubScoping,
    Apply(ApplyError),
    StmtMatch,
    Parse(Vec<ParserError>),
    ArcSlotRemoval,
    Todo(String),
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
