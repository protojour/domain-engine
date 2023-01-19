use chumsky::prelude::Simple;
use miette::Diagnostic;
use thiserror::Error;

use crate::{
    env::Env,
    misc::{CompileSrc, SourceSpan},
    parse::tree::Tree,
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
}

impl SpannedCompileError {
    pub fn new(error: CompileError, source_code: &CompileSrc) -> Self {
        Self {
            error,
            src: miette::NamedSource::new(source_code.name.to_string(), source_code.text.clone()),
        }
    }
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
    pub fn spanned(self, span: &SourceSpan) -> SpannedCompileError {
        SpannedCompileError {
            error: self,
            src: miette::NamedSource::new("", ""),
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
