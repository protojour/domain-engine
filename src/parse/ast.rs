use chumsky::prelude::*;
use smartstring::alias::String;

use crate::parse::{tree::Tree, SString, Spanned};

pub type ParseResult<T> = Result<Spanned<T>, Simple<Tree>>;

pub enum Ast {
    Import(Path),
    List(Vec<Spanned<Ast>>),
    Data(Data),
}

pub struct Data {
    pub ident: Spanned<SString>,
    pub ty: Spanned<Type>,
}

pub enum Type {
    Sym(String),
    Record(Record),
}

pub struct Record {
    pub fields: Vec<Spanned<RecordField>>,
}

pub struct RecordField {
    pub ident: Spanned<String>,
    pub ty: Spanned<Type>,
}

pub struct Path(pub Vec<Spanned<SString>>);
