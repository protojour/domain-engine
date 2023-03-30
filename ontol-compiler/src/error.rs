use std::fmt::Display;

use chumsky::{error::SimpleReason, prelude::Simple};
use miette::Diagnostic;
use ontol_parser::Token;
use ontol_runtime::format_utils::{LogicOp, Missing};
use smartstring::alias::String;
use thiserror::Error;

use crate::source::SourceSpan;

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
}

#[derive(Debug, Error, Diagnostic)]
pub enum CompileError {
    #[error("lex error: {0}")]
    Lex(LexError),
    #[error("parse error: {0}")]
    Parse(ParseError),
    #[error("package not found")]
    PackageNotFound,
    #[error("this rel is contextual; specify only subject or object")]
    TooMuchContextInContextualRel,
    #[error("invalid expression")]
    InvalidExpression,
    #[error("function takes {expected} parameters, but {actual} was supplied")]
    IncorrectNumberOfArguments { expected: u8, actual: u8 },
    #[error("not callable")]
    NotCallable,
    #[error("type not found")]
    TypeNotFound,
    #[error("private type")]
    PrivateType,
    #[error("duplicate type definition")]
    DuplicateTypeDefinition,
    #[error("namespace not found")]
    NamespaceNotFound,
    #[error("invalid type")]
    InvalidType,
    #[error("invalid integer")]
    InvalidInteger,
    #[error("expected domain type")]
    DomainTypeExpected,
    #[error("duplicate anonymous relationship")]
    DuplicateAnonymousRelationship,
    #[error("invalid subject type. Must be a domain type, empty sequence or empty string")]
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
    #[error("already identifies another type")]
    AlreadyIdentifiesAType,
    #[error("must identify a type within the domain")]
    MustIdentifyWithinDomain,
    #[error("entity variants of the union are not uniquely identifiable")]
    NonDisjointIdsInEntityUnion,
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
    #[error("invalid type parameters")]
    InvalidTypeParameters,
    #[error("unknown type parameter")]
    UnknownTypeParameter,
}

#[derive(Debug)]
pub struct LexError {
    inner: Box<Simple<char>>,
}

impl LexError {
    pub fn new(inner: Simple<char>) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl Display for LexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.inner.reason() {
            SimpleReason::Unclosed { .. } => write!(f, "unclosed"),
            SimpleReason::Unexpected => {
                if let Some(found) = self.inner.found() {
                    write!(f, "illegal character `{found}`")?;
                } else {
                    write!(f, "illegal character")?;
                }

                Ok(())
            }
            SimpleReason::Custom(str) => write!(f, "{str}"),
        }
    }
}

#[derive(Debug)]
pub struct ParseError {
    inner: Box<Simple<Token>>,
}

impl ParseError {
    pub fn new(inner: Simple<Token>) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl Display for ParseError {
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

                    let mut missing_items: Vec<_> = self
                        .inner
                        .expected()
                        .filter_map(|item| item.as_ref())
                        .collect();

                    missing_items.sort();

                    if !missing_items.is_empty() {
                        let missing = Missing {
                            items: missing_items,
                            logic_op: LogicOp::Or,
                        };

                        write!(f, "expected {missing}")?;
                        reported = true;
                    }
                } else if let Some(label) = self.inner.label() {
                    if self.inner.found().is_some() {
                        write!(f, ", ")?;
                    }

                    write!(f, "expected {label}")?;
                    reported = true;
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
    pub fn spanned(self, span: &SourceSpan) -> SpannedCompileError {
        SpannedCompileError {
            error: self,
            span: *span,
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
