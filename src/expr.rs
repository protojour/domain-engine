use crate::misc::SourceSpan;

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ExprId(u32);

pub struct Expr {
    pub id: ExprId,
    pub kind: ExprKind,
    pub span: SourceSpan,
}

pub enum ExprKind {
    Constant(i32),
    Call(ExprId, Vec<Expr>),
    Variable(),
}

pub struct ExprAlloc {
    next_id: ExprId,
}

impl Default for ExprAlloc {
    fn default() -> Self {
        Self { next_id: ExprId(0) }
    }
}

impl ExprAlloc {
    pub fn expr(&mut self, kind: ExprKind, span: SourceSpan) -> Expr {
        let id = self.next_id;
        self.next_id.0 += 1;
        Expr { id, kind, span }
    }
}
