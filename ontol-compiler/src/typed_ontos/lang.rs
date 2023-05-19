use ontos::{
    display::{Print, Printer, Sep},
    kind::NodeKind,
};

use crate::{
    types::{Type, TypeRef},
    SourceSpan,
};

#[derive(Clone, Copy)]
pub struct TypedOntos;

impl ontos::Lang for TypedOntos {
    type Node<'m> = OntosNode<'m>;

    fn make_node<'m>(&self, kind: NodeKind<'m, Self>) -> Self::Node<'m> {
        OntosNode {
            kind,
            meta: Meta {
                ty: &Type::Tautology,
                span: SourceSpan::none(),
            },
        }
    }
}

pub type OntosKind<'m> = ontos::kind::NodeKind<'m, TypedOntos>;

#[derive(Clone)]
pub struct OntosNode<'m> {
    pub kind: OntosKind<'m>,
    pub meta: Meta<'m>,
}

impl<'m> OntosNode<'m> {
    pub fn split(self) -> (OntosKind<'m>, Meta<'m>) {
        (self.kind, self.meta)
    }
}

impl<'m> ontos::Node<'m, TypedOntos> for OntosNode<'m> {
    fn kind(&self) -> &NodeKind<'m, TypedOntos> {
        &self.kind
    }
}

impl<'m> std::fmt::Display for OntosNode<'m> {
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
