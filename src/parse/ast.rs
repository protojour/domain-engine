use chumsky::prelude::*;

use crate::parse::{tree::Tree, tree_stream::TreeStream, SString, Spanned};

use super::Span;

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
    Sym(SString),
    Record(Record),
}

pub struct Record {
    pub fields: Vec<Spanned<RecordField>>,
}

pub struct RecordField {
    pub ident: Spanned<SString>,
    pub ty: Spanned<Type>,
}

pub struct Path(pub Vec<Spanned<SString>>);
