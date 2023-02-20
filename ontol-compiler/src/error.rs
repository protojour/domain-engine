use std::{fmt::Display, hash::Hash};

use chumsky::{error::SimpleReason, prelude::Simple};
use miette::Diagnostic;
use ontol_runtime::format_utils::{LogicOp, Missing};
use smartstring::alias::String;
use thiserror::Error;

use crate::{
    parse::lexer::Token,
    s_parse::tree::Tree,
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
    Parse(ChumskyError<Token>),
    #[error("parse error: {0}")]
    SParse(ChumskyError<Tree>),
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
    #[error("invalid subject type. Must be a domain type, unit, empty sequence or empty string")]
    InvalidSubjectType,
    #[error("invalid mix of relationship type for subject")]
    InvalidMixOfRelationshipTypeForSubject,
    #[error("cannot mix index relation identifiers and edge types")]
    CannotMixIndexedRelationIdentsAndEdgeTypes,
    #[error("no properties expected")]
    NoPropertiesExpected,
    #[error("expected expression")]
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
    #[error("variants of the union have prefixes that are prefixes of other variants")]
    SharedPrefixInPatternUnion,
    #[error("union in named relationship is not supported yet. Make a union type instead.")]
    UnionInNamedRelationshipNotSupported,
    #[error("unsupported cardinality")]
    UnsupportedCardinality,
    #[error("invalid cardinality combination in union")]
    InvalidCardinaltyCombinationInUnion,
    #[error("cannot convert this `{output}` from `{input}`: These types are not equated.")]
    CannotConvertMissingEquation { input: String, output: String },
    #[error("only entities may have named reverse relationship")]
    NonEntityInReverseRelationship,
    #[error("overlapping indexes")]
    OverlappingSequenceIndexes,
    #[error("unsupported sequence index type")]
    UnsupportedSequenceIndexType,
    #[error("constructor mismatch")]
    ConstructorMismatch,
    #[error("cannot concatenate string pattern")]
    CannotConcatenateStringPattern,
    #[error("invalid regex: {0}")]
    InvalidRegex(String),
}

#[derive(Debug)]
pub struct ChumskyError<I: Eq + Hash> {
    inner: Box<Simple<I>>,
}

impl<I: Eq + Hash> ChumskyError<I> {
    pub fn new(inner: Simple<I>) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl<I: Hash + Eq + Display> Display for ChumskyError<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.reason() {
            SimpleReason::Unclosed { .. } => write!(f, "unclosed"),
            SimpleReason::Unexpected => {
                let mut reported = false;
                if let Some(found) = self.inner.found() {
                    write!(f, "found {found}")?;
                    reported = true;
                }
                if self.inner.expected().len() > 0 {
                    if self.inner.found().is_some() {
                        write!(f, ", ")?;
                    }

                    let missing_items: Vec<_> = self
                        .inner
                        .expected()
                        .filter_map(|item| item.as_ref())
                        .collect();

                    if !missing_items.is_empty() {
                        let missing = Missing {
                            items: missing_items,
                            logic_op: LogicOp::Or,
                        };

                        write!(f, "expected {missing}")?;
                        reported = true;
                    }
                }

                if !reported {
                    write!(f, "unexpected end of file")?;
                }

                Ok(())
            }
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
            span: *span,
            miette_source: miette::NamedSource::new(source.name.as_str(), source.text.clone()),
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
