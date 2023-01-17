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
    pub fn expr(&self, kind: ExprKind, span: SourceSpan) -> Expr {
        let id = ExprId(
            self.expr_counter
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );
        Expr { id, kind, span }
    }
}
