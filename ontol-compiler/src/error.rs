use std::fmt::Display;

use chumsky::{error::SimpleReason, prelude::Simple};
use ontol_parser::Token;
use ontol_runtime::format_utils::{Backticks, CommaSeparated, FmtMap, LogicOp, Missing};
use smartstring::alias::String;
use thiserror::Error;

use crate::source::SourceSpan;

#[derive(Debug, Error)]
#[error("oops")]
pub struct UnifiedCompileError {
    pub errors: Vec<SpannedCompileError>,
}

#[derive(Debug, Error)]
#[error("")]
pub struct SpannedCompileError {
    pub error: CompileError,
    pub span: SourceSpan,
    pub notes: Vec<SpannedNote>,
}

#[derive(Debug)]
pub enum CompileError {
    Lex(LexError),
    Parse(ParseError),
    PackageNotFound,
    WildcardNeedsContextualBlock,
    InvalidExpression,
    IncorrectNumberOfArguments { expected: u8, actual: u8 },
    NotCallable,
    TypeNotFound,
    PrivateType,
    DuplicateTypeDefinition,
    NamespaceNotFound,
    InvalidType,
    InvalidInteger,
    DomainTypeExpected,
    InvalidRelationType,
    DuplicateAnonymousRelationship,
    SubjectMustBeDomainType,
    ObjectMustBeDataType,
    FmtTooFewTransitions,
    FmtMisplacedWildcard,
    InvalidMixOfRelationshipTypeForSubject,
    CannotMixIndexedRelationIdentsAndEdgeTypes,
    NoPropertiesExpected,
    ExpectedExpressionAttribute,
    NamedPropertyExpected,
    UnknownProperty,
    DuplicateProperty,
    TypeMismatch { actual: String, expected: String },
    MissingProperties(Vec<String>),
    UnboundVariable,
    VariableMustBeSequenceEnclosed(String),
    CannotDiscriminateType,
    UnitTypePartOfUnion(String),
    NoUniformDiscriminatorFound,
    SharedPrefixInPatternUnion,
    AlreadyIdentifiesAType,
    AlreadyIdentified,
    MustIdentifyWithinDomain,
    NonDisjointIdsInEntityUnion,
    UnionInNamedRelationshipNotSupported,
    UnsupportedCardinality,
    InvalidCardinaltyCombinationInUnion,
    CannotConvertMissingMapping { input: String, output: String },
    NonEntityInReverseRelationship,
    OverlappingSequenceIndexes,
    UnsupportedSequenceIndexType,
    ConstructorMismatch,
    CannotConcatenateStringPattern,
    InvalidRegex(String),
    InvalidTypeParameters,
    UnknownTypeParameter,
    CannotMapUnion,
    NoRelationParametersExpected,
    IncompatibleLiteral,
    CannotGenerateValue(String),
    TypeNotRepresentable,
    MutationOfSealedType,
    IntersectionOfDisjointTypes,
    CircularSubtypingRelation,
    AmbiguousNumberResolution,
    DuplicateTypeParam(String),
    TODO(String),
}

impl std::error::Error for CompileError {}

// TODO: Should be consistent on casing
impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Lex(err) => write!(f, "lex error: {err}"),
            Self::Parse(err) => {
                write!(f, "parse error: {err}")
            }
            Self::PackageNotFound => write!(f, "package not found"),
            Self::WildcardNeedsContextualBlock => {
                write!(f, "using `.` requires a contextual block")
            }
            Self::InvalidExpression => write!(f, "invalid expression"),
            Self::IncorrectNumberOfArguments { expected, actual } => write!(
                f,
                "function takes {expected} parameters, but {actual} was supplied",
            ),
            Self::NotCallable => write!(f, "not callable"),
            Self::TypeNotFound => write!(f, "type not found"),
            Self::PrivateType => write!(f, "private type"),
            Self::DuplicateTypeDefinition => {
                write!(f, "duplicate type definition")
            }
            Self::NamespaceNotFound => write!(f, "namespace not found"),
            Self::InvalidType => write!(f, "invalid type"),
            Self::InvalidInteger => write!(f, "invalid integer"),
            Self::DomainTypeExpected => write!(f, "expected domain type"),
            Self::InvalidRelationType => write!(f, "invalid relation type"),
            Self::DuplicateAnonymousRelationship => {
                write!(f, "duplicate anonymous relationship")
            }
            Self::SubjectMustBeDomainType => write!(f, "subject must be a domain type"),
            Self::ObjectMustBeDataType => write!(f, "object must be a data type"),
            Self::FmtTooFewTransitions => {
                write!(f, "fmt needs at least two transitions: `fmt a => b => c`")
            }
            Self::FmtMisplacedWildcard => {
                write!(f, "fmt only supports `.` at the final target position")
            }
            Self::InvalidMixOfRelationshipTypeForSubject => {
                write!(f, "invalid mix of relationship type for subject")
            }
            Self::CannotMixIndexedRelationIdentsAndEdgeTypes => {
                write!(f, "cannot mix index relation identifiers and edge types")
            }
            Self::NoPropertiesExpected => write!(f, "no properties expected"),
            Self::ExpectedExpressionAttribute => write!(f, "expected expression attribute"),
            Self::NamedPropertyExpected => write!(f, "expected named property"),
            Self::UnknownProperty => write!(f, "unknown property"),
            Self::DuplicateProperty => write!(f, "duplicate property"),
            Self::TypeMismatch { actual, expected } => {
                write!(f, "type mismatch: expected `{expected}`, found `{actual}`")
            }
            Self::MissingProperties(props) => {
                write!(
                    f,
                    "{msg} {props}",
                    msg = if props.len() == 1 {
                        "missing property"
                    } else {
                        "missing properties"
                    },
                    props = CommaSeparated(FmtMap(props, Backticks))
                )
            }
            Self::UnboundVariable => write!(f, "unbound variable"),
            Self::VariableMustBeSequenceEnclosed(var) => {
                write!(f, "[{var}] variable must be enclosed in []")
            }
            Self::CannotDiscriminateType => write!(f, "cannot discriminate type"),
            Self::UnitTypePartOfUnion(name) => {
                write!(f, "unit type `{name}` cannot be part of a union",)
            }
            Self::NoUniformDiscriminatorFound => {
                write!(f, "no uniform discriminator found for union variants")
            }
            Self::SharedPrefixInPatternUnion => write!(
                f,
                "variants of the union have prefixes that are prefixes of other variants"
            ),
            Self::AlreadyIdentifiesAType => write!(f, "already identifies a type"),
            Self::AlreadyIdentified => write!(
                f,
                "already identified by another type; secondary ids not supported (yet)"
            ),
            Self::MustIdentifyWithinDomain => {
                write!(f, "must identify a type within the domain")
            }
            Self::NonDisjointIdsInEntityUnion => write!(
                f,
                "entity variants of the union are not uniquely identifiable"
            ),
            Self::UnionInNamedRelationshipNotSupported => write!(
                f,
                "union in named relationship is not supported yet. Make a union type instead."
            ),
            Self::UnsupportedCardinality => write!(f, "unsupported cardinality"),
            Self::InvalidCardinaltyCombinationInUnion => {
                write!(f, "invalid cardinality combination in union")
            }
            Self::CannotConvertMissingMapping { input, output } => write!(
                f,
                "cannot convert this `{output}` from `{input}`: These types are not equated.",
            ),
            Self::NonEntityInReverseRelationship => {
                write!(f, "only entities may have named reverse relationship")
            }
            Self::OverlappingSequenceIndexes => write!(f, "overlapping indexes"),
            Self::UnsupportedSequenceIndexType => write!(f, "unsupported sequence index type"),
            Self::ConstructorMismatch => write!(f, "constructor mismatch"),
            Self::CannotConcatenateStringPattern => write!(f, "cannot concatenate string pattern"),
            Self::InvalidRegex(regex) => write!(f, "invalid regex: {regex}"),
            Self::InvalidTypeParameters => write!(f, "invalid type parameters"),
            Self::UnknownTypeParameter => write!(f, "unknown type parameter"),
            Self::CannotMapUnion => write!(f, "cannot map a union, map each variant instead"),
            Self::NoRelationParametersExpected => write!(f, "no relation parameters expected"),
            Self::IncompatibleLiteral => write!(f, "Incompatible literal"),
            Self::CannotGenerateValue(name) => write!(f, "Cannot generate a value of type {name}"),
            Self::TypeNotRepresentable => write!(f, "type not representable"),
            Self::MutationOfSealedType => write!(f, "Type is sealed and cannot be modified"),
            Self::IntersectionOfDisjointTypes => write!(f, "Intersection of disjoint types"),
            Self::CircularSubtypingRelation => write!(f, "Circular subtyping relation"),
            Self::AmbiguousNumberResolution => write!(f, "ambiguous number resolution"),
            Self::DuplicateTypeParam(ident) => write!(f, "duplicate type param `{ident}`"),
            Self::TODO(msg) => write!(f, "TODO: {msg}"),
        }
    }
}

#[derive(Debug)]
pub struct SpannedNote {
    pub note: Note,
    pub span: SourceSpan,
}

impl SpannedNote {
    pub const fn new(note: Note, span: SourceSpan) -> Self {
        Self { note, span }
    }
}

#[derive(Debug, Error)]
pub enum Note {
    #[error("Consider using `match {{}}`")]
    ConsiderUsingMatch,
    #[error("Type is abstract")]
    TypeIsAbstract,
    #[error("Type of field is abstract")]
    FieldTypeIsAbstract,
    #[error("Base type is {0}")]
    BaseTypeIs(String),
    #[error("Number type is abstract")]
    NumberTypeIsAbstract,
    #[error("defined here")]
    DefinedHere,
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
            notes: vec![],
        }
    }
}

#[derive(Default, Debug)]
pub struct CompileErrors {
    pub(crate) errors: Vec<SpannedCompileError>,
}

impl CompileErrors {
    pub fn extend(&mut self, errors: CompileErrors) {
        self.errors.extend(errors.errors);
    }

    pub fn push(&mut self, error: SpannedCompileError) {
        self.errors.push(error);
    }

    pub fn error(&mut self, error: CompileError, span: &SourceSpan) {
        self.error_with_notes(error, span, vec![]);
    }

    pub fn error_with_notes(
        &mut self,
        error: CompileError,
        span: &SourceSpan,
        notes: Vec<SpannedNote>,
    ) {
        self.errors.push(SpannedCompileError {
            error,
            span: *span,
            notes,
        });
    }
}
