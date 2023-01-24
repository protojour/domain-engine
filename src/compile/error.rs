use chumsky::prelude::Simple;
use miette::Diagnostic;
use thiserror::Error;

use crate::{
    parse::tree::Tree,
    source::{SourceSpan, Sources},
};

#[derive(Debug, Error, Diagnostic)]
#[error("oops")]
pub struct UnifiedCompileError {
    #[related]
    pub errors: Vec<SpannedCompileError>,
}

#[derive(Debug, Error, Diagnostic)]
#[error("")]
pub struct SpannedCompileError {
    pub error: CompileError,

    pub span: SourceSpan,

    #[source_code]
    pub miette_source: miette::NamedSource,

    #[label]
    pub miette_span: miette::SourceSpan,
}

#[derive(Debug, Error, Diagnostic)]
pub enum CompileError {
    #[error("lex error")]
    Lex(Simple<char>),
    #[error("parse error")]
    Parse(Simple<Tree>),
    #[error("wrong number of arguments")]
    WrongNumberOfArguments,
    #[error("not callable")]
    NotCallable,
    #[error("type not found")]
    TypeNotFound,
    #[error("invalid type")]
    InvalidType,
    #[error("invalid number")]
    InvalidNumber,
    #[error("expected domain type")]
    DomainTypeExpected,
}

impl CompileError {
    pub fn spanned(self, sources: &Sources, span: &SourceSpan) -> SpannedCompileError {
        let source = sources
            .get_compiled_source(span.source_id)
            .expect("BUG: Source is not being compiled");

        SpannedCompileError {
            error: self,
            span: span.clone(),
            miette_source: miette::NamedSource::new(
                source.name.as_str().clone(),
                source.text.clone(),
            ),
            miette_span: miette::SourceSpan::new(
                (span.start as usize).into(),
                (span.end as usize).into(),
            ),
        }
    }
}

#[derive(Default, Debug)]
pub struct CompileErrors {
    pub(crate) errors: Vec<SpannedCompileError>,
}

impl CompileErrors {
    pub fn push(&mut self, error: SpannedCompileError) {
        self.errors.push(error);
    }
}
