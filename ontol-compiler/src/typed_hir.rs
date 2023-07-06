use std::fmt::{Debug, Display};

use ontol_hir::{
    display::{Print, Printer, Sep},
    Var,
};

use crate::{
    types::{Type, TypeRef},
    SourceSpan, NO_SPAN,
};

/// An ontol_hir language "dialect" with type information and source spans.
#[derive(Clone, Copy)]
pub struct TypedHir;

impl ontol_hir::Lang for TypedHir {
    type Node<'m> = TypedHirNode<'m>;
    type Binder<'m> = TypedBinder<'m>;

    fn make_node<'m>(&self, kind: ontol_hir::Kind<'m, Self>) -> Self::Node<'m> {
        TypedHirNode(
            kind,
            Meta {
                ty: &Type::Error,
                span: NO_SPAN,
            },
        )
    }

    fn make_binder<'a>(&self, var: Var) -> Self::Binder<'a> {
        TypedBinder {
            var,
            ty: &Type::Error,
        }
    }
}

pub type TypedHirKind<'m> = ontol_hir::Kind<'m, TypedHir>;

/// The typed ontol_hir node type.
#[derive(Clone)]
pub struct TypedHirNode<'m>(pub TypedHirKind<'m>, pub Meta<'m>);

impl<'m> Debug for TypedHirNode<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<'m> TypedHirNode<'m> {
    pub fn split(self) -> (TypedHirKind<'m>, Meta<'m>) {
        (self.0, self.1)
    }
}

impl<'m> TypedHirNode<'m> {
    pub fn into_kind(self) -> ontol_hir::Kind<'m, TypedHir> {
        self.0
    }

    pub fn meta(&self) -> &Meta<'m> {
        &self.1
    }

    pub fn meta_mut(&mut self) -> &mut Meta<'m> {
        &mut self.1
    }

    pub fn ty(&self) -> TypeRef<'m> {
        self.1.ty
    }

    pub fn span(&self) -> SourceSpan {
        self.1.span
    }
}

impl<'m> ontol_hir::GetKind<'m, TypedHir> for TypedHirNode<'m> {
    fn kind(&self) -> &ontol_hir::Kind<'m, TypedHir> {
        &self.0
    }

    fn kind_mut(&mut self) -> &mut ontol_hir::Kind<'m, TypedHir> {
        &mut self.0
    }
}

impl<'m> std::fmt::Display for TypedHirNode<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Printer::default().print(Sep::None, &self.0, f)?;
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
        write!(f, "|{}| {}", self.arg.var, self.body)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TypedBinder<'m> {
    pub var: Var,
    pub ty: TypeRef<'m>,
}

impl<'m> ontol_hir::GetVar<'m, TypedHir> for TypedBinder<'m> {
    fn var(&self) -> &Var {
        &self.var
    }

    fn var_mut(&mut self) -> &mut Var {
        &mut self.var
    }
}
