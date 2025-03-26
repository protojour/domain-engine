use std::{collections::BTreeSet, fmt::Debug};

use arcstr::ArcStr;
use either::Either;
use ontol_core::{
    property::{Cardinality, PropertyCardinality, ValueCardinality},
    tag::OntolDefTag,
};
use ontol_parser::lexer::kind::Kind;
use serde::{Deserialize, Serialize};
use thin_vec::ThinVec;
use ulid::Ulid;

use crate::{tag::Tag, token::Token, with_span::WithSpan};

#[derive(Default)]
pub struct Log {
    events: Vec<EKind>,
    release: usize,
    stage: usize,
    sync: usize,
}

impl Log {
    #[inline]
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn all(&self) -> &[EKind] {
        &self.events
    }

    pub fn staged(&self) -> &[EKind] {
        &self.events[self.stage..self.events.len()]
    }

    pub fn stage(&mut self, event: EKind) {
        self.events.push(event);
    }

    /// Lock staging before trying to extend the log.
    pub fn lock_stage(&mut self) {
        self.stage = self.events.len();
        self.sync = self.stage;
    }

    pub fn lock_sync(&mut self) {
        self.sync = self.events.len();
    }

    pub fn unsynced(&self) -> &[EKind] {
        &self.events[self.sync..self.events.len()]
    }

    pub fn clear_unsynced(&mut self) {
        while self.len() > self.sync {
            self.events.pop();
        }
    }

    pub fn history_slice(&self, watermark: usize) -> &[EKind] {
        &self.events[0..watermark]
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum EKind {
    Start(Box<Ulid>),
    DomainAdd(Tag, ThinVec<DomainProp>),
    DomainChange(Tag, ThinVec<DomainProp>),
    DomainRemove(Tag),
    UseAdd(Tag, ThinVec<UseProp>),
    UseChange(Tag, ThinVec<UseProp>),
    UseRemove(Tag),
    DefAdd(Tag, ThinVec<DefProp>),
    DefChange(Tag, ThinVec<DefProp>),
    DefRemove(Tag),
    ArcAdd(Tag, ThinVec<ArcProp>),
    ArcChange(Tag, ThinVec<ArcProp>),
    ArcRemove(Tag),
    RelAdd(Tag, ThinVec<RelProp>),
    RelChange(Tag, ThinVec<RelProp>),
    RelRemove(Tag),
    FmtAdd(Tag, ThinVec<FmtProp>),
    FmtChange(Tag, ThinVec<FmtProp>),
    FmtRemove(Tag),
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum DomainProp {
    Id(Ulid),
    Doc(ArcStr),
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum UseProp {
    Domain(Tag),
    Ident(WithSpan<Token>),
    Uri(WithSpan<ArcStr>),
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum DefProp {
    Domain(Tag),
    // TODO: Remove "old" token
    Ident(WithSpan<Token>),
    SymSet,
    Doc(ArcStr),
    DocRemove,
    ModAdd(WithSpan<DefModifier>),
    ModRemove(WithSpan<DefModifier>),
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum ArcProp {
    Domain(Tag),
    Ident(WithSpan<Token>),
    SlotSymbol(ArcCoord, WithSpan<Token>),
    Var(ArcCoord, WithSpan<Token>),
    TypeParam(ArcCoord, WithSpan<TypeRef>),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
pub struct ArcCoord(pub u8, pub u8);

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum RelProp {
    Domain(Tag),
    DefCtxSet(Tag),
    DefCtxRemove,
    Rel(WithSpan<TypeRef>),
    SubjCrd(RelCrd),
    ObjCrd(RelCrd),
    SubjAdd(WithSpan<TypeRef>),
    SubjRm(WithSpan<TypeRef>),
    ObjAdd(WithSpan<TypeRef>),
    ObjRm(WithSpan<TypeRef>),
    ObjPatternSet(Pattern),
    ObjPatternRm(Pattern),
}

/// Cardinality model
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum RelCrd {
    Unit,
    Set,
    List,
    OptUnit,
    OptSet,
    OptList,
}

impl From<Cardinality> for RelCrd {
    fn from(value: Cardinality) -> Self {
        match value {
            (PropertyCardinality::Optional, ValueCardinality::Unit) => Self::OptUnit,
            (PropertyCardinality::Optional, ValueCardinality::IndexSet) => Self::OptSet,
            (PropertyCardinality::Optional, ValueCardinality::List) => Self::OptList,
            (PropertyCardinality::Mandatory, ValueCardinality::Unit) => Self::Unit,
            (PropertyCardinality::Mandatory, ValueCardinality::IndexSet) => Self::Set,
            (PropertyCardinality::Mandatory, ValueCardinality::List) => Self::List,
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum FmtProp {
    Domain(Tag),
    DefCtxSet(Tag),
    DefCtxRemove,
    Transitions(ThinVec<TypeRef>),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum DefModifier {
    Private,
    Open,
    Extern,
    Macro,
    Crdt,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum TypeRef {
    Text(ArcStr),
    Number(ArcStr),
    Regex(ArcStr),
    NumberRange(ArcStr, ArcStr),
    Path(PathRef),
    Anonymous(ThinVec<OntolTree>),
}

pub type TypeUnion = BTreeSet<WithSpan<TypeRef>>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum TypeRefOrUnionOrPattern {
    Type(WithSpan<TypeRef>),
    Union(TypeUnion),
    Pattern(Pattern),
}

impl From<WithSpan<TypeRef>> for Either<WithSpan<TypeRef>, TypeUnion> {
    fn from(value: WithSpan<TypeRef>) -> Self {
        Either::Left(value)
    }
}

impl TypeRefOrUnionOrPattern {
    pub fn to_type_set(self) -> Either<TypeUnion, Pattern> {
        match self {
            Self::Type(t) => Either::Left([t].into_iter().collect()),
            Self::Union(u) => Either::Left(u),
            Self::Pattern(p) => Either::Right(p),
        }
    }

    pub fn to_either_ref(&self) -> Either<Either<&WithSpan<TypeRef>, &TypeUnion>, &Pattern> {
        match self {
            TypeRefOrUnionOrPattern::Type(t) => Either::Left(Either::Left(t)),
            TypeRefOrUnionOrPattern::Union(u) => Either::Left(Either::Right(u)),
            TypeRefOrUnionOrPattern::Pattern(p) => Either::Right(p),
        }
    }
}

impl From<Either<WithSpan<TypeRef>, TypeUnion>> for TypeRefOrUnionOrPattern {
    fn from(value: Either<WithSpan<TypeRef>, TypeUnion>) -> Self {
        match value {
            Either::Left(t) => Self::Type(t),
            Either::Right(u) => Self::Union(u),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum PathRef {
    Local(Tag),
    Foreign(Tag, Tag),
    LocalArc(Tag, ArcCoord),
    ForeignArc(Tag, Tag, ArcCoord),
    Ontol(OntolDefTag),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum Pattern {
    Struct(Box<StructPattern>),
    Set(()),
    Atom(()),
    Binary(()),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct StructPattern {
    type_ref: PathRef,
    params: Vec<StructParam>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum StructParam {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum OntolTree {
    Node(Kind, ThinVec<OntolTree>),
    Token(Kind, ArcStr),
}
