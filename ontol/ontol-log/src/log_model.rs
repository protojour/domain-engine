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

use crate::{tag::Tag, token::Token};

#[derive(Default, Debug)]
pub struct LogModel {
    events: Vec<Event>,
}

impl LogModel {
    pub fn new() -> Self {
        Self {
            events: vec![Event {
                kind: EKind::Start(Box::new(Ulid::new())),
            }],
        }
    }

    pub fn push(&mut self, kind: EKind) {
        self.events.push(Event { kind });
    }

    pub fn extend(&mut self, model: LogModel) {
        self.events.extend(model.events);
    }

    pub fn event_kinds(&self) -> impl Iterator<Item = &EKind> {
        self.events.iter().map(|event| &event.kind)
    }
}

#[derive(Debug)]
struct Event {
    kind: EKind,
}

#[derive(PartialEq, Serialize, Deserialize, Debug)]
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
    RelAdd(Tag, ThinVec<RelProp>),
    RelChange(Tag, ThinVec<RelProp>),
    RelRemove(Tag),
    FmtAdd(Tag, ThinVec<FmtProp>),
    FmtChange(Tag, ThinVec<FmtProp>),
    FmtRemove(Tag),
}

#[derive(PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum DomainProp {
    Id(Ulid),
    Doc(ArcStr),
}

#[derive(PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum UseProp {
    Ident(Token),
    Uri(ArcStr),
}

#[derive(PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum DefProp {
    Domain(Tag),
    Ident(Token),
    ChangeIdent(Token, Token),
    SymSet,
    Doc(ArcStr),
    DocRemove,
    ModAdd(DefModifier),
    ModRemove(DefModifier),
}

#[derive(PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum ArcProp {
    Domain(Tag),
    Ident(Token),
    SlotSymbol(ArcCoord, Token),
    Var(ArcCoord, Token),
    TypeParam(ArcCoord, TypeRef),
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
pub struct ArcCoord(pub u8, pub u8);

#[derive(PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum RelProp {
    Domain(Tag),
    DefCtxSet(Tag),
    DefCtxRemove,
    Rel(TypeRef),
    SubjCrd(RelCrd),
    ObjCrd(RelCrd),
    SubjAdd(TypeRef),
    SubjRm(TypeRef),
    ObjAdd(TypeRef),
    ObjRm(TypeRef),
    ObjPatternSet(Pattern),
    ObjPatternRm,
}

/// Cardinality model
#[derive(Clone, Copy, PartialEq, Serialize, Deserialize, Debug)]
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

#[derive(PartialEq, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum FmtProp {
    Domain(Tag),
    DefCtxSet(Tag),
    DefCtxRemove,
    Transitions(ThinVec<TypeRef>),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum DefModifier {
    Private,
    Open,
    Extern,
    Macro,
    Crdt,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum TypeRef {
    Text(ArcStr),
    Number(ArcStr),
    Regex(ArcStr),
    NumberRange(ArcStr, ArcStr),
    Path(PathRef),
    Anonymous(ThinVec<OntolTree>),
}

pub type TypeUnion = BTreeSet<TypeRef>;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum TypeRefOrUnionOrPattern {
    Type(TypeRef),
    Union(TypeUnion),
    Pattern(Pattern),
}

impl From<TypeRef> for Either<TypeRef, TypeUnion> {
    fn from(value: TypeRef) -> Self {
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
}

impl From<Either<TypeRef, TypeUnion>> for TypeRefOrUnionOrPattern {
    fn from(value: Either<TypeRef, TypeUnion>) -> Self {
        match value {
            Either::Left(t) => Self::Type(t),
            Either::Right(u) => Self::Union(u),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum PathRef {
    Local(Tag),
    Foreign(Tag, Tag),
    LocalArc(Tag, ArcCoord),
    ForeignArc(Tag, Tag, ArcCoord),
    Ontol(OntolDefTag),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum Pattern {
    Struct(Box<StructPattern>),
    Set(()),
    Atom(()),
    Binary(()),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct StructPattern {
    type_ref: PathRef,
    params: Vec<StructParam>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum StructParam {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum OntolTree {
    Node(Kind, ThinVec<OntolTree>),
    Token(Kind, ArcStr),
}
