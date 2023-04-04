use std::ops::Range;

use smartstring::alias::String;

use super::{Span, Spanned};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Statement {
    Use(UseStatement),
    Type(TypeStatement),
    With(WithStatement),
    Rel(RelStatement),
    Fmt(FmtStatement),
    Map(MapStatement),
}

impl Statement {
    #[allow(unused)]
    pub fn docs(&self) -> &[String] {
        match self {
            Self::Use(_) => &[],
            Self::Type(ty) => &ty.docs,
            Self::With(_) => &[],
            Self::Rel(rel) => &rel.docs,
            Self::Fmt(rel) => &rel.docs,
            Self::Map(_) => &[],
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct UseStatement {
    pub kw: Span,
    pub reference: Spanned<String>,
    pub as_ident: Spanned<String>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct TypeStatement {
    pub docs: Vec<String>,
    pub visibility: Spanned<Visibility>,
    pub kw: Span,
    pub ident: Spanned<String>,
    pub params: Option<Spanned<Vec<Spanned<TypeParam>>>>,
    pub ctx_block: Option<Spanned<Vec<Spanned<Statement>>>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct WithStatement {
    pub kw: Span,
    pub ty: Spanned<Type>,
    pub statements: Spanned<Vec<Spanned<Statement>>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RelStatement {
    pub docs: Vec<String>,
    pub kw: Span,
    pub subject: Spanned<Option<Type>>,
    pub relation: Relation,
    pub object: Spanned<Option<Type>>,
    pub ctx_block: Option<Spanned<Vec<Spanned<Statement>>>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct FmtStatement {
    pub docs: Vec<String>,
    pub kw: Span,
    pub origin: Spanned<Type>,
    pub transitions: Vec<Spanned<Option<Type>>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Relation {
    pub ty: RelType,
    pub subject_cardinality: Option<Cardinality>,
    pub object_prop_ident: Option<Spanned<String>>,
    pub object_cardinality: Option<Cardinality>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum RelType {
    Type(Spanned<Type>),
    // TODO: Remove this in favor of sequence constructors?
    IntRange(Spanned<Range<Option<u16>>>),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Cardinality {
    Optional,
    Many,
    OptionalMany,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RelChain {
    pub subject: Option<Spanned<Type>>,
    pub connection: Relation,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MapStatement {
    pub kw: Span,
    pub variables: Vec<Spanned<String>>,
    pub first: Spanned<MapType>,
    pub second: Spanned<MapType>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MapType {
    pub path: Spanned<Path>,
    pub attributes: Vec<MapAttribute>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum MapAttribute {
    Expr(Spanned<Expression>),
    Rel(Spanned<MapAttributeRel>),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MapAttributeRel {
    pub kw: Span,
    pub subject: Option<Spanned<Expression>>,
    pub relation: Spanned<Type>,
    pub object: Option<Spanned<Expression>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Type {
    Unit,
    Path(Path, Option<Spanned<Vec<Spanned<TypeParamPattern>>>>),
    NumberLiteral(String),
    StringLiteral(String),
    Regex(String),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Expression {
    Variable(String),
    Path(Path),
    NumberLiteral(String),
    StringLiteral(String),
    Binary(Box<Spanned<Expression>>, BinaryOp, Box<Spanned<Expression>>),
    // Call(Spanned<String>, Vec<Spanned<Expr>>),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Path {
    Ident(String),
    Path(Vec<Spanned<String>>),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Visibility {
    Private,
    Public,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct TypeParam {
    pub ident: Spanned<String>,
    pub default: Option<Spanned<Type>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct TypeParamPattern {
    pub ident: Spanned<String>,
    pub binding: TypeParamPatternBinding,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum TypeParamPatternBinding {
    None,
    Equals(Spanned<Type>),
}
