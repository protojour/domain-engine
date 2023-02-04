use std::{fmt::Display, hash::Hash};

use chumsky::{error::SimpleReason, prelude::Simple};
use miette::Diagnostic;
use smartstring::alias::String;
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
    Lex(ChumskyError<char>),
    #[error("parse error: {0}")]
    Parse(ChumskyError<Tree>),
    #[error("function takes {expected} parameters, but {actual} was supplied")]
    IncorrectNumberOfArguments { expected: u8, actual: u8 },
    #[error("not callable")]
    NotCallable,
    #[error("type not found")]
    TypeNotFound,
    #[error("invalid type")]
    InvalidType,
    #[error("invalid integer")]
    InvalidInteger,
    #[error("expected domain type")]
    DomainTypeExpected,
    #[error("duplicate anonymous relationship")]
    DuplicateAnonymousRelationship,
    #[error("cannot mix named and anonymous relations on the same type")]
    CannotMixNamedAndAnonymousRelations,
    #[error("no properties expected")]
    NoPropertiesExpected,
    #[error("expected anonymous property")]
    AnonymousPropertyExpected,
    #[error("expected named property")]
    NamedPropertyExpected,
    #[error("unknown property")]
    UnknownProperty,
    #[error("duplicate property")]
    DuplicateProperty,
    #[error("type mismatch: expected `{expected}`, found `{actual}`")]
    TypeMismatch { actual: String, expected: String },
    #[error("missing property `{0}`")]
    MissingProperty(String),
    #[error("undeclared variable")]
    UndeclaredVariable,
    #[error("cannot discriminate type")]
    CannotDiscriminateType,
    #[error("union tree not supported")]
    UnionTreeNotSupported,
    #[error("unit type `{0}` cannot be part of a union")]
    UnitTypePartOfUnion(String),
    #[error("no uniform discriminator found for union variants")]
    NoUniformDiscriminatorFound,
    #[error("union in named relationship is not supported yet. Make a union type instead.")]
    UnionInNamedRelationshipNotSupported,
    #[error("unsupported cardinality")]
    UnsupportedCardinality,
    #[error("cannot equate")]
    CannotEquate,
}

#[derive(Debug)]
pub struct ChumskyError<I: Hash> {
    inner: Simple<I>,
}

impl<I: Hash> ChumskyError<I> {
    pub fn new(inner: Simple<I>) -> Self {
        Self { inner }
    }
}

impl<I: Hash + Eq> Display for ChumskyError<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.reason() {
            SimpleReason::Unclosed { .. } => write!(f, "unclosed"),
            SimpleReason::Unexpected => write!(f, "unexpected"),
            SimpleReason::Custom(str) => write!(f, "{str}"),
        }
    }
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
