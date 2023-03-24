use std::ops::Range;

use smartstring::alias::String;

use super::{Span, Spanned};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Statement {
    Use(UseStatement),
    Type(TypeStatement),
    Rel(RelStatement),
    Eq(EqStatement),
}

impl Statement {
    #[allow(unused)]
    pub fn docs(&self) -> &[String] {
        match self {
            Self::Use(_) => &[],
            Self::Type(ty) => &ty.docs,
            Self::Rel(rel) => &rel.docs,
            Self::Eq(_) => &[],
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
    pub rel_block: Option<Spanned<Vec<Spanned<RelStatement>>>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RelStatement {
    pub docs: Vec<String>,
    pub kw: Span,
    pub subject: Option<Spanned<Type>>,
    pub connection: RelConnection,
    pub chain: Vec<ChainedSubjectConnection>,
    pub object: Option<Spanned<Type>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ChainedSubjectConnection {
    pub subject: Option<Spanned<Type>>,
    pub connection: RelConnection,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RelConnection {
    pub ty: RelType,
    pub subject_cardinality: Option<Cardinality>,
    pub rel_params: Option<Spanned<Type>>,
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
    pub connection: RelConnection,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct EqStatement {
    pub kw: Span,
    pub variables: Vec<Spanned<String>>,
    pub first: Spanned<EqType>,
    pub second: Spanned<EqType>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct EqType {
    pub path: Spanned<Path>,
    pub attributes: Vec<EqAttribute>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum EqAttribute {
    Expr(Spanned<Expression>),
    Rel(Spanned<EqAttributeRel>),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct EqAttributeRel {
    pub kw: Span,
    pub subject: Option<Spanned<Expression>>,
    pub connection: Spanned<Type>,
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
