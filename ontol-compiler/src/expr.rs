use fnv::FnvHashMap;
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
    Struct(TypePath, Box<[ExprStructAttr]>),
    /// Expression enclosed in sequence brackets: `[expr]`
    Seq(ExprId, Box<Expr>),
    Variable(ExprId),
    Constant(i64),
}

#[derive(Debug)]
pub struct ExprStructAttr {
    pub key: ObjKey,
    pub bind_option: bool,
    pub expr: Expr,
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
    pub map: FnvHashMap<ExprId, Expr>,
}

impl Default for Expressions {
    fn default() -> Self {
        Self {
            next_expr_id: ExprId(0),
            map: Default::default(),
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
