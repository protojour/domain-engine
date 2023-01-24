use chumsky::prelude::*;
use smartstring::alias::String;

use crate::parse::{tree::Tree, Spanned};

pub type ParseResult<T> = Result<Spanned<T>, Simple<Tree>>;

pub enum Ast {
    Import(Path),
    Type(Spanned<String>),
    Rel(Rel),
    Data(Data),
    Eq(Eq),
}

pub struct Eq {
    pub params: (),
    pub first: Spanned<Expr>,
    pub second: Spanned<Expr>,
}

#[derive(Debug)]
pub enum Expr {
    Sym(String),
    Literal(Literal),
    Call(Spanned<String>, Vec<Spanned<Expr>>),
}

pub struct Data {
    pub ident: Spanned<String>,
    pub ty: Spanned<Type>,
}

pub enum Type {
    Sym(String),
    Record(Record),
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

pub struct Record {
    pub fields: Vec<Spanned<RecordField>>,
}

pub struct RecordField {
    pub ident: Spanned<String>,
    pub ty: Spanned<Type>,
}

pub struct Path(pub Vec<Spanned<String>>);
