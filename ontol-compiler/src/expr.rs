use ontol_runtime::DefId;
use smartstring::alias::String;

use crate::{compiler::Compiler, source::SourceSpan};

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct ExprId(pub u32);

#[derive(Debug)]
pub struct Expr {
    pub id: ExprId,
    pub kind: ExprKind,
    pub span: SourceSpan,
}

#[derive(Debug)]
pub enum ExprKind {
    /// Function call
    Call(DefId, Vec<Expr>),
    /// Object constructor
    Obj(DefId, Vec<(String, Expr)>),
    Variable(ExprId),
    Constant(i32),
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
