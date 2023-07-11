use fnv::FnvHashMap;
use ontol_runtime::DefId;
use smartstring::alias::String;

use crate::{def::DefReference, source::SourceSpan, Compiler};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct ExprId(pub u32);

#[derive(Debug)]
pub struct Expr {
    pub id: ExprId,
    pub kind: ExprKind,
    pub span: SourceSpan,
}

type PropertyKey = (DefReference, SourceSpan);

#[derive(Debug)]
pub enum ExprKind {
    /// Function call
    Call(DefId, Box<[Expr]>),
    Struct {
        /// The user-supplied type of the struct, None means anonymous
        type_path: Option<TypePath>,
        attributes: Box<[ExprStructAttr]>,
    },
    /// Expression enclosed in sequence brackets: `[expr]`
    Seq(ExprId, Box<Expr>),
    Variable(ExprId),
    ConstI64(i64),
    ConstString(String),
}

#[derive(Debug)]
pub struct ExprStructAttr {
    pub key: PropertyKey,
    pub rel: Option<Expr>,
    pub bind_option: bool,
    pub value: Expr,
}

#[derive(Debug)]
pub struct TypePath {
    pub def_id: DefId,
    pub span: SourceSpan,
}

impl<'m> Compiler<'m> {
    pub fn expr(&mut self, kind: ExprKind, span: SourceSpan) -> Expr {
        Expr {
            id: self.expressions.alloc_expr_id(),
            kind,
            span,
        }
    }
}

#[derive(Debug)]
pub struct Expressions {
    next_expr_id: ExprId,
    pub table: FnvHashMap<ExprId, Expr>,
}

impl Default for Expressions {
    fn default() -> Self {
        Self {
            next_expr_id: ExprId(0),
            table: Default::default(),
        }
    }
}

impl Expressions {
    pub fn alloc_expr_id(&mut self) -> ExprId {
        let id = self.next_expr_id;
        self.next_expr_id.0 += 1;
        id
    }
}
