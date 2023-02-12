use std::ops::Range;

use chumsky::prelude::*;
use smartstring::alias::String;

use crate::parse::{tree::Tree, Spanned};

pub type ParseResult<T> = Result<Spanned<T>, Simple<Tree>>;

pub enum Ast {
    Import(Path),
    Type(Spanned<String>),
    Entity(Spanned<String>),
    Rel(Box<Rel>),
    Eq(Eq),
    Comment(String),
}

pub struct Eq {
    pub variables: Vec<Spanned<String>>,
    pub first: Spanned<Expr>,
    pub second: Spanned<Expr>,
}

#[derive(Debug)]
pub enum Expr {
    Variable(String),
    Sym(String),
    Literal(Literal),
    Call(Spanned<String>, Vec<Spanned<Expr>>),
    Obj(Spanned<String>, Vec<Spanned<Attribute>>),
}

#[derive(Debug)]
pub struct Attribute {
    pub property: Spanned<Property>,
    pub value: Spanned<Expr>,
}

#[derive(Debug)]
pub enum Property {
    Named(String),
    Wildcard,
}

pub enum Type {
    Sym(String),
    Literal(Literal),
}

pub struct Rel {
    pub subject: Spanned<Type>,
    pub ident: Spanned<SymOrIntRangeOrWildcard>,
    pub subject_cardinality: Option<Cardinality>,
    pub rel_params: Option<Spanned<Type>>,
    pub object_cardinality: Option<Cardinality>,
    pub object_prop_ident: Option<Spanned<String>>,
    pub object: Spanned<Type>,
}

pub enum SymOrIntRangeOrWildcard {
    Sym(String),
    IntRange(Range<Option<u16>>),
    Wildcard,
}

pub enum Cardinality {
    Optional,
    Many,
    OptionalMany,
}

#[derive(Debug)]
pub enum Literal {
    String(String),
    Int(String),
}

pub struct Path(pub Vec<Spanned<String>>);
