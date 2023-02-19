use smartstring::alias::String;

use super::{Span, Spanned};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Stmt {
    Type(TypeStmt),
    Rel(RelStmt),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct TypeStmt {
    pub kw: Span,
    pub ident: Spanned<String>,
    pub rel_block: Spanned<Option<Vec<RelStmt>>>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RelStmt {
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
    pub ty: Spanned<Type>,
    // pub subject_cardinality: Option<Cardinality>,
    pub rel_params: Option<Spanned<Type>>,
    pub object_prop_ident: Option<Spanned<String>>,
    // pub object_cardinality: Option<Cardinality>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct RelChain {
    pub subject: Option<Spanned<Type>>,
    pub connection: RelConnection,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Type {
    Unit,
    // TODO: Support segments
    Path(String),
    NumberLiteral(String),
    StringLiteral(String),
    Regex(String),
}
