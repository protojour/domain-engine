use bit_set::BitSet;
use ontos::{
    kind::{MatchArm, NodeKind, PropVariant},
    Binder, Variable,
};

use crate::typed_ontos::lang::{Meta, OntosNode};

#[derive(Clone, Copy, Debug)]
pub struct TaggedTree;

impl ontos::Lang for TaggedTree {
    type Node<'m> = TaggedNode<'m>;

    fn make_node<'a>(&self, _kind: NodeKind<'a, Self>) -> Self::Node<'a> {
        unimplemented!()
    }
}

type TaggedKind<'m> = NodeKind<'m, TaggedTree>;

#[allow(unused)]
pub struct TaggedNode<'m> {
    pub kind: TaggedKind<'m>,
    pub meta: Meta<'m>,
    pub free_variables: BitSet,
}

impl<'m> ontos::Node<'m, TaggedTree> for TaggedNode<'m> {
    fn kind(&self) -> &NodeKind<'m, TaggedTree> {
        &self.kind
    }

    fn kind_mut(&mut self) -> &mut NodeKind<'m, TaggedTree> {
        &mut self.kind
    }
}

impl<'m> TaggedNode<'m> {
    fn new(kind: TaggedKind<'m>, meta: Meta<'m>) -> Self {
        Self {
            kind,
            meta,
            free_variables: BitSet::new(),
        }
    }

    fn with_free_variable(mut self, var: Variable) -> Self {
        self.free_variables.insert(var.0 as usize);
        self
    }

    pub fn into_ontos_node(self) -> OntosNode<'m> {
        fn nodes_to_ontos(nodes: Vec<TaggedNode>) -> Vec<OntosNode> {
            nodes.into_iter().map(TaggedNode::into_ontos_node).collect()
        }

        let kind = match self.kind {
            NodeKind::VariableRef(var) => NodeKind::VariableRef(var),
            NodeKind::Unit => NodeKind::Unit,
            NodeKind::Int(int) => NodeKind::Int(int),
            NodeKind::Call(proc, args) => NodeKind::Call(proc, nodes_to_ontos(args)),
            NodeKind::Map(arg) => NodeKind::Map(Box::new(arg.into_ontos_node())),
            NodeKind::Let(binder, def, body) => NodeKind::Let(
                binder,
                Box::new(def.into_ontos_node()),
                nodes_to_ontos(body),
            ),
            NodeKind::Seq(binder, nodes) => NodeKind::Seq(binder, nodes_to_ontos(nodes)),
            NodeKind::Struct(binder, nodes) => NodeKind::Struct(binder, nodes_to_ontos(nodes)),
            NodeKind::Prop(struct_var, id, variant) => NodeKind::Prop(
                struct_var,
                id,
                PropVariant {
                    dimension: variant.dimension,
                    rel: Box::new((*variant.rel).into_ontos_node()),
                    val: Box::new((*variant.val).into_ontos_node()),
                },
            ),
            NodeKind::MapSeq(var, binder, nodes) => {
                NodeKind::MapSeq(var, binder, nodes_to_ontos(nodes))
            }
            NodeKind::MatchProp(struct_var, id, arms) => NodeKind::MatchProp(
                struct_var,
                id,
                arms.into_iter()
                    .map(|arm| MatchArm {
                        pattern: arm.pattern,
                        nodes: nodes_to_ontos(arm.nodes),
                    })
                    .collect(),
            ),
        };

        OntosNode {
            kind,
            meta: self.meta,
        }
    }
}

#[derive(Default)]
pub struct Tagger {
    in_scope: BitSet,
}

impl Tagger {
    pub fn enter_binder<T>(&mut self, binder: Binder, func: impl FnOnce(&mut Self) -> T) -> T {
        if !self.in_scope.insert(binder.0 .0 as usize) {
            panic!("Malformed ONTOS: {binder:?} variable was already in scope");
        }
        let value = func(self);
        self.in_scope.remove(binder.0 .0 as usize);
        value
    }

    pub fn tag_nodes<'m>(&mut self, children: Vec<OntosNode<'m>>) -> Vec<TaggedNode<'m>> {
        children
            .into_iter()
            .map(|child| self.tag_node(child))
            .collect()
    }

    pub fn tag_node<'m>(&mut self, node: OntosNode<'m>) -> TaggedNode<'m> {
        let (kind, meta) = node.split();
        match kind {
            NodeKind::VariableRef(var) => {
                let tagged_node = TaggedNode::new(NodeKind::VariableRef(var), meta);
                if self.in_scope.contains(var.0 as usize) {
                    tagged_node
                } else {
                    tagged_node.with_free_variable(var)
                }
            }
            NodeKind::Unit => TaggedNode::new(NodeKind::Unit, meta),
            NodeKind::Int(int) => TaggedNode::new(NodeKind::Int(int), meta),
            NodeKind::Let(binder, definition, body) => {
                let definition = *definition;
                let definition = self.tag_node(definition);
                let def_free_vars = definition.free_variables.clone();
                let mut tagged = self.enter_binder(binder, |zelf| {
                    zelf.tag_union_children(
                        body,
                        move |body| NodeKind::Let(binder, Box::new(definition), body),
                        meta,
                    )
                });
                tagged.free_variables.union_with(&def_free_vars);
                tagged
            }
            NodeKind::Call(proc, args) => {
                self.tag_union_children(args, |args| NodeKind::Call(proc, args), meta)
            }
            NodeKind::Map(arg) => {
                let arg = self.tag_node(*arg);
                let free_variables = arg.free_variables.clone();
                TaggedNode {
                    kind: NodeKind::Map(Box::new(arg)),
                    meta,
                    free_variables,
                }
            }
            NodeKind::Seq(binder, nodes) => self.enter_binder(binder, |zelf| {
                zelf.tag_union_children(nodes, |nodes| NodeKind::Seq(binder, nodes), meta)
            }),
            NodeKind::Struct(binder, nodes) => self.enter_binder(binder, |zelf| {
                zelf.tag_union_children(nodes, |nodes| NodeKind::Struct(binder, nodes), meta)
            }),
            NodeKind::Prop(struct_var, prop, variant) => {
                let rel = self.tag_node(*variant.rel);
                let val = self.tag_node(*variant.val);
                let free_variables =
                    union_bitsets([&rel.free_variables, &val.free_variables].into_iter());

                TaggedNode {
                    kind: NodeKind::Prop(
                        struct_var,
                        prop,
                        PropVariant {
                            dimension: variant.dimension,
                            rel: Box::new(rel),
                            val: Box::new(val),
                        },
                    ),
                    meta,
                    free_variables,
                }
            }
            NodeKind::MapSeq(..) => {
                todo!()
            }
            NodeKind::MatchProp(..) => {
                unimplemented!("BUG: MatchProp is an output node")
            }
        }
    }

    fn tag_union_children<'m>(
        &mut self,
        children: Vec<OntosNode<'m>>,
        func: impl FnOnce(Vec<TaggedNode<'m>>) -> TaggedKind<'m>,
        meta: Meta<'m>,
    ) -> TaggedNode<'m> {
        let children = self.tag_nodes(children);
        let free_variables = union_bitsets(children.iter().map(|arg| &arg.free_variables));
        TaggedNode {
            kind: func(children),
            meta,
            free_variables,
        }
    }
}

pub fn union_bitsets<'a>(iterator: impl Iterator<Item = &'a BitSet>) -> BitSet {
    let mut output = BitSet::new();
    for item in iterator {
        output.union_with(item);
    }
    output
}

#[cfg(test)]
mod tests {
    use bit_set::BitSet;
    use ontos::{parse::Parser, Variable};

    use crate::typed_ontos::lang::TypedOntos;

    fn free_variables(iterator: impl Iterator<Item = Variable>) -> BitSet {
        let mut bit_set = BitSet::new();
        for var in iterator {
            bit_set.insert(var.0 as usize);
        }
        bit_set
    }

    #[test]
    fn test_tag() {
        let src = "
            (struct ($0)
                (prop $0 s:0:0
                    ($1
                        (struct ($2)
                            (prop $2 s:0:1
                                ($3 $4)
                            )
                        )
                    )
                )
            )
        ";
        let node = Parser::new(TypedOntos).parse(src).unwrap().0;
        let tagged_node = super::Tagger::default().tag_node(node);
        assert_eq!(
            free_variables([Variable(1), Variable(3), Variable(4)].into_iter()),
            tagged_node.free_variables,
        );
    }
}
