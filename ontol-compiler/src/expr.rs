use ontol_runtime::DefId;

use crate::{def::DefReference, source::SourceSpan, Compiler};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct ExprId(pub u32);

#[derive(Debug)]
pub struct Expr {
    pub id: ExprId,
    pub kind: ExprKind,
    pub span: SourceSpan,
}

type ObjKey = (DefReference, SourceSpan);

#[derive(Debug)]
pub enum ExprKind {
    /// Function call
    Call(DefId, Box<[Expr]>),
    /// Object constructor
    Obj(TypePath, Box<[(ObjKey, Expr)]>),
    Variable(ExprId),
    Constant(i64),
}

#[derive(Debug)]
pub struct TypePath {
    pub def_id: DefId,
    pub span: SourceSpan,
}

impl<'m> Compiler<'m> {
    pub fn expr(&mut self, kind: ExprKind, span: SourceSpan) -> Expr {
        Expr {
            id: self.defs.alloc_expr_id(),
            kind,
            span,
        }
    }
}
