use ontos::kind::NodeKind;

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

impl<'m> ontos::Node<'m, TypedOntos> for OntosNode<'m> {
    fn kind(&self) -> &NodeKind<'m, TypedOntos> {
        &self.kind
    }
}
