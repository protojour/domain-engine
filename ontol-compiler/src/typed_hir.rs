use std::fmt::{Debug, Display};

use ontol_hir::{
    display::{Print, Printer, Sep},
    kind::NodeKind,
    Var,
};

use crate::{
    types::{Type, TypeRef},
    SourceSpan,
};

#[derive(Clone, Copy)]
pub struct TypedHir;

impl ontol_hir::Lang for TypedHir {
    type Node<'m> = TypedHirNode<'m>;

    fn make_node<'m>(&self, kind: NodeKind<'m, Self>) -> Self::Node<'m> {
        TypedHirNode {
            kind,
            meta: Meta {
                ty: &Type::Error,
                span: SourceSpan::none(),
            },
        }
    }
}

pub type TypedHirKind<'m> = ontol_hir::kind::NodeKind<'m, TypedHir>;

#[derive(Clone)]
pub struct TypedHirNode<'m> {
    pub kind: TypedHirKind<'m>,
    pub meta: Meta<'m>,
}

impl<'m> Debug for TypedHirNode<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<'m> TypedHirNode<'m> {
    pub fn split(self) -> (TypedHirKind<'m>, Meta<'m>) {
        (self.kind, self.meta)
    }
}

impl<'m> ontol_hir::Node<'m, TypedHir> for TypedHirNode<'m> {
    fn kind(&self) -> &NodeKind<'m, TypedHir> {
        &self.kind
    }

    fn kind_mut(&mut self) -> &mut NodeKind<'m, TypedHir> {
        &mut self.kind
    }
}

impl<'m> std::fmt::Display for TypedHirNode<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Printer::default().print(Sep::None, &self.kind, f)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Meta<'m> {
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

pub struct HirFunc<'m> {
    pub arg: TypedBinder<'m>,
    pub body: TypedHirNode<'m>,
}

impl<'m> Display for HirFunc<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "|{}| {}", self.arg.variable, self.body)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TypedBinder<'m> {
    pub variable: Var,
    pub ty: TypeRef<'m>,
}
