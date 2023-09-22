use std::ops::Range;

use either::Either;
use smartstring::alias::String;

use super::{Span, Spanned};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Statement {
    Use(UseStatement),
    Def(DefStatement),
    Rel(RelStatement),
    Fmt(FmtStatement),
    Map(MapStatement),
}

impl Statement {
    #[allow(unused)]
    pub fn docs(&self) -> &[String] {
        match self {
            Self::Use(_) => &[],
            Self::Def(ty) => &ty.docs,
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
pub struct DefStatement {
    pub docs: Vec<String>,
    pub visibility: Spanned<Visibility>,
    pub kw: Span,
    pub ident: Spanned<String>,
    pub block: Spanned<Vec<Spanned<Statement>>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RelStatement {
    pub docs: Vec<String>,
    pub kw: Span,
    pub subject: Spanned<Either<Dot, Type>>,
    pub relations: Vec<Relation>,
    pub object: Spanned<Either<Dot, TypeOrPattern>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct FmtStatement {
    pub docs: Vec<String>,
    pub kw: Span,
    pub origin: Spanned<Type>,
    pub transitions: Vec<Spanned<Either<Dot, Type>>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Relation {
    pub ty: RelType,
    pub subject_cardinality: Option<Cardinality>,
    pub object_prop_ident: Option<Spanned<String>>,
    pub ctx_block: Option<Spanned<Vec<Spanned<Statement>>>>,
    pub object_cardinality: Option<Cardinality>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Dot;

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
pub struct MapStatement {
    pub docs: Vec<String>,
    pub kw: Span,
    pub ident: Option<Spanned<String>>,
    pub first: Spanned<(UnitOrSeq, MapArm)>,
    pub second: Spanned<(UnitOrSeq, MapArm)>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum MapArm {
    Binding {
        path: Spanned<Path>,
        expr: Spanned<ExprPattern>,
    },
    Struct(StructPattern),
}

/// A pattern is either `struct {}` or leaf expr.
/// An expr cannot contain another struct pattern.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Pattern {
    Expr(Spanned<ExprPattern>),
    Struct(Spanned<StructPattern>),
    Seq(Vec<Spanned<SeqPatternElement>>),
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum UnitOrSeq {
    Unit,
    Seq,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct StructPattern {
    pub path: Spanned<Path>,
    pub modifier: Option<Spanned<StructPatternModifier>>,
    pub attributes: Vec<Spanned<StructPatternAttr>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum StructPatternModifier {
    Match,
}

/// relation attribute within `struct { .. }`
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct StructPatternAttr {
    pub relation: Spanned<Type>,
    pub relation_attrs: Option<Spanned<Vec<Spanned<StructPatternAttr>>>>,
    pub option: Option<Spanned<()>>,
    pub object: Spanned<Pattern>,
}

/// items within `[]`
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SeqPatternElement {
    pub spread: Option<Span>,
    pub pattern: Spanned<Pattern>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum ExprPattern {
    Variable(String),
    NumberLiteral(String),
    TextLiteral(String),
    RegexLiteral(String),
    Binary(
        Box<Spanned<ExprPattern>>,
        BinaryOp,
        Box<Spanned<ExprPattern>>,
    ),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum TypeOrPattern {
    Type(Type),
    Pattern(Pattern),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Type {
    Unit,
    Path(Path),
    AnonymousStruct(Spanned<Vec<Spanned<Statement>>>),
    NumberLiteral(String),
    TextLiteral(String),
    Regex(String),
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
