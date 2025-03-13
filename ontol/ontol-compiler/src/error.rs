use std::fmt::Debug;

use ontol_core::url::DomainUrl;
use ontol_runtime::format_utils::CommaSeparated;

use crate::source::SourceSpan;

pub struct UnifiedCompileError {
    pub errors: Vec<SpannedCompileError>,
}

impl Debug for UnifiedCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnifiedCompileError").finish()
    }
}

pub struct SpannedCompileError {
    pub error: CompileError,
    pub notes: Vec<SpannedNote>,
    span: SourceSpan,
}

impl SpannedCompileError {
    pub fn with_note(mut self, note: SpannedNote) -> Self {
        self.notes.push(note);
        self
    }

    pub fn with_notes(mut self, notes: impl IntoIterator<Item = SpannedNote>) -> Self {
        self.notes.extend(notes);
        self
    }

    pub(crate) fn report(self, ctx: &mut impl AsMut<CompileErrors>) {
        ctx.as_mut().errors.push(self);
    }

    pub fn span(&self) -> SourceSpan {
        self.span
    }
}

/// Compile error
///
/// The error messages are the doc comments of each variant, due to the displaydoc derive.
/// Only the first doc comment line is the message.
#[derive(displaydoc::Display)]
#[ignore_extra_doc_attributes]
pub enum CompileError {
    /// lex error: {0}
    Lex(String),
    /// parse error: {0}
    Parse(String),
    /// number parse error: {0}
    NumberParse(String),
    /// invalid domain reference
    InvalidDomainReference,
    /// domain not found
    DomainNotFound(DomainUrl),
    /// persisted domain is missing domain header
    PersistedDomainIsMissingDomainHeader,
    /// using `.` requires a contextual block
    WildcardNeedsContextualBlock,
    /// invalid expression
    InvalidExpression,
    /// function takes {expected} parameters, but {actual} was supplied
    IncorrectNumberOfArguments { expected: u8, actual: u8 },
    /// not callable
    NotCallable,
    /// definition not found in this scope
    DefinitionNotFound,
    /// private definition
    PrivateDefinition,
    /// namespace not found
    NamespaceNotFound,
    /// invalid type
    InvalidType,
    /// invalid integer
    InvalidInteger,
    /// invalid boolean
    InvalidBoolean,
    /// expected domain type
    DomainTypeExpected,
    /// invalid relation type
    InvalidRelationType,
    /// duplicate anonymous relationship
    DuplicateAnonymousRelationship,
    /// the object of a flattened relationship must be a union
    FlattenedRelationshipObjectMustBeStructUnion,
    /// subject must be a domain type
    SubjectMustBeDomainType,
    /// object must be a data type
    ObjectMustBeDataType,
    /// fmt needs at least two transitions: `fmt a => b => c`
    FmtTooFewTransitions,
    /// fmt only supports `.` at the final target position
    FmtMisplacedSelf,
    /// invalid mix of relationship type for subject
    InvalidMixOfRelationshipTypeForSubject,
    /// cannot mix index relation identifiers and edge types
    CannotMixIndexedRelationIdentsAndEdgeTypes,
    /// no properties expected
    NoPropertiesExpected,
    /// expected attribute
    ExpectedPatternAttribute,
    /// pattern requires an iterated variable (`..x`)
    PatternRequiresIteratedVariable,
    /// expected named property
    NamedPropertyExpected,
    /// unknown property
    UnknownProperty,
    /// duplicate property
    DuplicateProperty,
    /// property is not optional
    PropertyNotOptional,
    /// type mismatch: expected {expected}, found {actual}
    TypeMismatch { actual: String, expected: String },
    /// {0}
    MissingProperties(MissingProperties),
    /// unbound variable
    UnboundVariable,
    /// {{{0}}} variable must be enclosed in {{}}
    VariableMustBeSequenceEnclosed(String),
    /// cannot discriminate type
    CannotDiscriminateType,
    /// unit type {0} cannot be part of a union
    UnitTypePartOfUnion(String),
    /// impossible mix of entities and non-entities in union
    CannotMixNonEntitiesInUnion,
    /// no uniform discriminator found for union variants
    NoUniformDiscriminatorFound,
    /// variants of the union have prefixes that are prefixes of other variants
    SharedPrefixInPatternUnion,
    /// already identifies a type
    AlreadyIdentifiesAType,
    /// already identified by another type; secondary ids not supported (yet)
    AlreadyIdentified,
    /// must identify a type within the domain
    MustIdentifyWithinDomain,
    /// entity variants of the union are not uniquely identifiable
    NonDisjointIdsInEntityUnion,
    /// union in named relationship is not supported yet. Make a union instead
    UnionInNamedRelationshipNotSupported,
    /// entitities cannot be used as supertypes
    EntityCannotBeSupertype,
    /// unsupported cardinality
    UnsupportedCardinality,
    /// invalid cardinality combination in union
    InvalidCardinaltyCombinationInUnion,
    /// cardinality mismatch
    InferenceCardinalityMismatch,
    /// cannot convert this {output} from {input}: These types are not equated.
    CannotConvertMissingMapping { input: String, output: String },
    /// only entities may have named reverse relationship
    NonEntityInReverseRelationship,
    /// relation subject must be an entity
    RelationSubjectMustBeEntity,
    /// entity relationships must be sets instead of lists. Use `{{}}` syntax.
    EntityRelationshipCannotBeAList,
    /// order identifier must be a symbol in this domain
    EntityOrderMustBeSymbolInThisDomain,
    /// order tuple parameters expected
    EntityOrderMustSpecifyParameters,
    /// overlapping indexes
    OverlappingSequenceIndexes,
    /// unsupported sequence index type
    UnsupportedSequenceIndexType,
    /// constructor mismatch
    ConstructorMismatch,
    /// cannot concatenate string pattern
    CannotConcatenateStringPattern,
    /// invalid regex: {0}
    InvalidRegex(String),
    /// invalid type parameters
    InvalidTypeParameters,
    /// unknown type parameter
    UnknownTypeParameter,
    /// cannot map a union, map each variant instead
    CannotMapUnion,
    /// cannot map an abstract type
    CannotMapAbstract,
    /// conflicting map
    ConflictingMap,
    /// unknown map direction for extern map
    ExternMapUnknownDirection,
    /// no relation parameters expected
    NoRelationParametersExpected,
    /// expected explicit struct path
    ExpectedExplicitStructPath,
    /// incompatible literal
    IncompatibleLiteral,
    /// cannot generate a value of type {0}
    CannotGenerateValue(String),
    /// type not representable
    TypeNotRepresentable,
    /// anonymous union abstraction
    ///
    /// A type is represented using `is` -> `is?`, which is invalid.
    /// The first `is` makes the union an anonymous member, and thus unnameable in a map.
    /// The solution for that is to use union flattening using a domain-specific unit type.
    AnonymousUnionAbstraction,
    /// definition is sealed and cannot be modified
    MutationOfSealedDef,
    /// intersection of disjoint types
    IntersectionOfDisjointTypes,
    /// circular subtyping relation
    CircularSubtypingRelation,
    /// ambiguous number resolution
    AmbiguousNumberResolution,
    /// duplicate type param `{0}`
    DuplicateTypeParam(String),
    /// requires spreading (`..`)
    RequiresSpreading,
    /// duplicate map identifier
    DuplicateMapIdentifier,
    /// unsolvable equation
    UnsolvableEquation,
    /// unsupported variable duplication. Try to simplify the expression
    UnsupportedVariableDuplication,
    /// spread label must be the last argument
    SpreadLabelMustBeLastArgument,
    /// modifier not recognized in this context
    InvalidModifier,
    /// entity-to-entity relationship must use an arc
    EntityToEntityRelationshipMustUseArc,
    /// Arc must have a unique identifier
    ArcMustHaveUniqueIdentifier,
    /// expected another item
    ArcClauseExpectedTrailingItem,
    /// expected sym variable (`(var)`)
    ArcClauseExpectedVariable,
    /// expected symbol
    ArcClauseExpectedSymbol,
    /// edge arity overflow
    EdgeArityOverflow,
    /// must be entity to participate in edge
    MustBeEntityToParticipateInEdge,
    /// existential variable is not associated with a definition
    ArcNoDefinitionForExistentialVar,
    /// BUG: {0}
    ///
    /// A message regarded as a bug in the compiler
    Bug(String),
    /// TODO: {0}
    ///
    /// An TODO message is an "immature" compile error, probably requires better UX design
    /// for presenting to the user
    Todo(String),
}

impl CompileError {
    pub fn span(self, span: SourceSpan) -> SpannedCompileError {
        SpannedCompileError {
            error: self,
            notes: vec![],
            span,
        }
    }
}

impl CompileError {
    /// Construct a [CompileError::Todo] message
    #[allow(non_snake_case)]
    pub fn TODO(msg: impl Into<String>) -> Self {
        Self::Todo(msg.into())
    }

    /// Construct a [CompileError::Bug] message
    #[allow(non_snake_case)]
    pub fn BUG(msg: impl Into<String>) -> Self {
        Self::Bug(msg.into())
    }
}

#[derive(Debug)]
pub struct SpannedNote {
    note: Note,
    span: SourceSpan,
}

impl SpannedNote {
    pub const fn new(note: Note, span: SourceSpan) -> Self {
        Self { note, span }
    }

    pub fn into_note(self) -> Note {
        self.note
    }

    pub fn span(&self) -> SourceSpan {
        self.span
    }
}

#[derive(Debug, displaydoc::Display)]
pub enum Note {
    /// consider using `match {{}}`
    ConsiderUsingMatch,
    /// type is abstract
    TypeIsAbstract,
    /// type cannot be part of a struct union
    CannotBePartOfStructUnion,
    /// type of field is abstract
    FieldTypeIsAbstract,
    /// base type is {0}
    BaseTypeIs(String),
    /// number type is abstract
    NumberTypeIsAbstract,
    /// defined here
    DefinedHere,
    /// use a domain-specific unit type instead
    UseDomainSpecificUnitType,
    /// consider defining a `map @abstract(..)` involving the same types, outside this scope, as this would explicitly state the direction`
    AbtractMapSuggestion,
    /// already defined here
    AlreadyDefinedHere,
    /// this symbol
    ThisSymbol,
}

impl Note {
    pub fn span(self, span: SourceSpan) -> SpannedNote {
        SpannedNote { note: self, span }
    }
}

#[derive(Default)]
pub struct CompileErrors {
    pub(crate) errors: Vec<SpannedCompileError>,
}

impl CompileErrors {
    pub fn extend(&mut self, errors: CompileErrors) {
        self.errors.extend(errors.errors);
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

impl AsMut<CompileErrors> for CompileErrors {
    fn as_mut(&mut self) -> &mut CompileErrors {
        self
    }
}

#[derive(Debug)]
pub struct MissingProperties(pub Vec<String>);

impl std::fmt::Display for MissingProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{msg} {props}",
            msg = if self.0.len() == 1 {
                "missing property"
            } else {
                "missing properties"
            },
            props = CommaSeparated(&self.0)
        )
    }
}
