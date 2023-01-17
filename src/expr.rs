use crate::{def::DefId, env::Env, misc::SourceSpan};

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ExprId(pub u32);

pub struct Expr {
    pub id: ExprId,
    pub kind: ExprKind,
    pub span: SourceSpan,
}

pub enum ExprKind {
    Constant(i32),
    Call(DefId, Vec<Expr>),
    Variable(ExprId),
}

impl<'m> Env<'m> {
    pub fn expr(&mut self, kind: ExprKind, span: SourceSpan) -> Expr {
        let id = ExprId(self.expr_counter);
        self.expr_counter += 1;
        Expr { id, kind, span }
    }
}
