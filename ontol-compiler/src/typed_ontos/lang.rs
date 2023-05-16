use ontos::{kind::NodeKind, Lang, Node};

use crate::{
    types::{Type, TypeRef},
    SourceSpan,
};

#[derive(Clone, Copy)]
pub struct TypedOntos;

impl Lang for TypedOntos {
    type Node<'a> = OntosNode<'a>;

    fn make_node<'a>(&self, kind: NodeKind<'a, Self>) -> Self::Node<'a> {
        OntosNode {
            kind,
            ty: &Type::Tautology,
            span: SourceSpan::none(),
        }
    }
}

pub struct OntosNode<'m> {
    pub kind: NodeKind<'m, TypedOntos>,
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

impl<'m> Node<'m, TypedOntos> for OntosNode<'m> {
    fn kind(&self) -> &NodeKind<'m, TypedOntos> {
        &self.kind
    }
}
