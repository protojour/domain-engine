use ontol_hir::kind::{Attribute, NodeKind, PropVariant};

use crate::typed_hir::{Meta, TypedHirNode};

use super::tagged_node::TaggedKind;

#[derive(Clone)]
pub enum UnifiedTargetNode<'m> {
    Tagged(UnifiedTaggedNode<'m>),
    Hir(TypedHirNode<'m>),
}

#[derive(Clone)]
pub struct UnifiedTaggedNode<'m> {
    pub kind: TaggedKind,
    pub meta: Meta<'m>,
    pub children: UnifiedTargetNodes<'m>,
}

#[derive(Clone, Default)]
pub struct UnifiedTargetNodes<'m>(pub Vec<UnifiedTargetNode<'m>>);

impl<'m> UnifiedTaggedNode<'m> {
    pub fn into_target(self) -> UnifiedTargetNode<'m> {
        UnifiedTargetNode::Tagged(self)
    }
}

impl<'m> UnifiedTargetNode<'m> {
    pub fn into_hir_node(self) -> TypedHirNode<'m> {
        match self {
            Self::Tagged(UnifiedTaggedNode {
                kind,
                meta,
                children,
            }) => {
                let kind = match kind {
                    TaggedKind::VariableRef(var) => NodeKind::VariableRef(var),
                    TaggedKind::Unit => NodeKind::Unit,
                    TaggedKind::Int(int) => NodeKind::Int(int),
                    TaggedKind::Call(proc) => NodeKind::Call(proc, children.into_hir()),
                    TaggedKind::Map => NodeKind::Map(Box::new(children.into_hir_one())),
                    TaggedKind::Let(binder) => {
                        let (def, body) = children.one_then_rest_into_hir();
                        NodeKind::Let(binder, Box::new(def), body)
                    }
                    TaggedKind::Seq(binder) => {
                        let (rel, val) = children.into_hir_pair();
                        NodeKind::Seq(
                            binder,
                            Attribute {
                                rel: Box::new(rel),
                                val: Box::new(val),
                            },
                        )
                    }
                    TaggedKind::Struct(binder) => NodeKind::Struct(binder, children.into_hir()),
                    TaggedKind::Prop(optional, struct_var, id) => NodeKind::Prop(
                        optional,
                        struct_var,
                        id,
                        children
                            .0
                            .into_iter()
                            .filter_map(|node| match node {
                                Self::Tagged(UnifiedTaggedNode {
                                    kind: TaggedKind::PropVariant(_, _, _, dimension),
                                    meta,
                                    children,
                                }) => {
                                    let (rel, val) = children.into_hir_pair();

                                    Some(PropVariant {
                                        dimension,
                                        attr: Attribute {
                                            rel: Box::new(rel),
                                            val: Box::new(val),
                                        },
                                    })
                                }
                                _ => None,
                            })
                            .collect(),
                    ),
                    other @ TaggedKind::PropVariant(..) => {
                        panic!("{other:?} should not be handled at top level")
                    }
                };

                TypedHirNode { kind, meta }
            }
            Self::Hir(hir) => hir,
        }
    }
}

impl<'m> UnifiedTargetNodes<'m> {
    pub fn into_hir_iterator(self) -> impl Iterator<Item = TypedHirNode<'m>> {
        self.0.into_iter().map(UnifiedTargetNode::into_hir_node)
    }

    pub fn into_hir(self) -> Vec<TypedHirNode<'m>> {
        Self::collect(self.0.into_iter())
    }

    pub fn one_then_rest_into_hir(self) -> (TypedHirNode<'m>, Vec<TypedHirNode<'m>>) {
        let mut iterator = self.0.into_iter();
        let first = iterator.next().unwrap();
        let rest = Self::collect(iterator);
        (first.into_hir_node(), rest)
    }

    pub fn into_hir_one(self) -> TypedHirNode<'m> {
        self.0.into_iter().next().unwrap().into_hir_node()
    }

    pub fn into_hir_pair(self) -> (TypedHirNode<'m>, TypedHirNode<'m>) {
        let mut iterator = self.0.into_iter();
        let first = iterator.next().unwrap();
        let second = iterator.next().unwrap();
        (first.into_hir_node(), second.into_hir_node())
    }

    fn collect(iterator: impl Iterator<Item = UnifiedTargetNode<'m>>) -> Vec<TypedHirNode<'m>> {
        iterator.map(UnifiedTargetNode::into_hir_node).collect()
    }
}
