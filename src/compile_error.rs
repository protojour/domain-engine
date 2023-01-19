use chumsky::prelude::Simple;
use miette::Diagnostic;
use thiserror::Error;

use crate::{
    parse::tree::Tree,
    source::{CompileSrc, SourceSpan, Sources},
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
    error: CompileError,

    #[source_code]
    src: miette::NamedSource,

    #[label]
    span: miette::SourceSpan,
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
}

impl CompileError {
    pub fn spanned(self, session: &Sources, span: &SourceSpan) -> SpannedCompileError {
        let source = session
            .get_compiled_source(span.source_id)
            .expect("BUG: Source is not being compiled");

        SpannedCompileError {
            error: self,
            src: miette::NamedSource::new(source.name.as_str().clone(), source.text.clone()),
            span: miette::SourceSpan::new(
                (span.range.start as usize).into(),
                (span.range.end as usize).into(),
            ),
        }
    }
}

#[derive(Default)]
pub struct CompileErrors {
    pub(crate) errors: Vec<SpannedCompileError>,
}

impl CompileErrors {
    pub fn push(&mut self, error: SpannedCompileError) {
        self.errors.push(error);
    }
}
