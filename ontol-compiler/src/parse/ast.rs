use chumsky::prelude::*;
use smartstring::alias::String;

use crate::parse::{tree::Tree, Spanned};

pub type ParseResult<T> = Result<Spanned<T>, Simple<Tree>>;

pub enum Ast {
    Import(Path),
    Type(Spanned<String>),
    Rel(Rel),
    Eq(Eq),
    Comment(String),
}

pub struct Eq {
    pub params: Vec<Spanned<String>>,
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
    pub property: Spanned<String>,
    pub value: Spanned<Expr>,
}

pub enum Type {
    Sym(String),
    Literal(Literal),
}

pub struct Rel {
    pub subject: Spanned<Type>,
    pub ident: Spanned<Option<String>>,
    pub object: Spanned<Type>,
}

#[derive(Debug)]
pub enum Literal {
    String(String),
    Number(String),
}

pub struct Path(pub Vec<Spanned<String>>);
