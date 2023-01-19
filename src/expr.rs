use crate::{def::DefId, env::Env, source::SourceSpan};

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct ExprId(pub u32);

#[derive(Debug)]
pub struct Expr {
    pub id: ExprId,
    pub kind: ExprKind,
    pub span: SourceSpan,
}

#[derive(Debug)]
pub enum ExprKind {
    Constant(i32),
    Call(DefId, Vec<Expr>),
    Variable(ExprId),
}

impl<'m> Env<'m> {
    pub fn expr(&mut self, kind: ExprKind, span: SourceSpan) -> Expr {
        Expr {
            id: self.alloc_expr_id(),
            kind,
            span,
        }
    }
}
