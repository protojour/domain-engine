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
            NodeKind::Let(binder, def, body) => NodeKind::Let(
                binder,
                Box::new(def.into_ontos_node()),
                nodes_to_ontos(body),
            ),
            NodeKind::Seq(binder, nodes) => NodeKind::Seq(binder, nodes_to_ontos(nodes)),
            NodeKind::Struct(binder, nodes) => NodeKind::Struct(binder, nodes_to_ontos(nodes)),
            NodeKind::Prop(struct_var, prop, variant) => NodeKind::Prop(
                struct_var,
                prop,
                PropVariant {
                    rel: Box::new((*variant.rel).into_ontos_node()),
                    val: Box::new((*variant.val).into_ontos_node()),
                },
            ),
            NodeKind::MapSeq(var, binder, nodes) => {
                NodeKind::MapSeq(var, binder, nodes_to_ontos(nodes))
            }
            NodeKind::MatchProp(struct_var, prop, arms) => NodeKind::MatchProp(
                struct_var,
                prop,
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
pub struct TagCtx {
    in_scope: BitSet,
}

impl TagCtx {
    pub fn enter_binder<T>(&mut self, binder: Binder, func: impl FnOnce(&mut Self) -> T) -> T {
        if !self.in_scope.insert(binder.0 .0 as usize) {
            panic!("Malformed ONTOS: {binder:?} variable was already in scope");
        }
        let value = func(self);
        self.in_scope.remove(binder.0 .0 as usize);
        value
    }
}

pub fn tag_nodes<'m>(children: Vec<OntosNode<'m>>, ctx: &mut TagCtx) -> Vec<TaggedNode<'m>> {
    children
        .into_iter()
        .map(|child| tag_node(child, ctx))
        .collect()
}

fn tag_node<'m>(node: OntosNode<'m>, ctx: &mut TagCtx) -> TaggedNode<'m> {
    let (kind, meta) = node.split();
    match kind {
        NodeKind::VariableRef(var) => {
            let tagged_node = TaggedNode::new(NodeKind::VariableRef(var), meta);
            if ctx.in_scope.contains(var.0 as usize) {
                tagged_node
            } else {
                tagged_node.with_free_variable(var)
            }
        }
        NodeKind::Unit => TaggedNode::new(NodeKind::Unit, meta),
        NodeKind::Int(int) => TaggedNode::new(NodeKind::Int(int), meta),
        NodeKind::Let(binder, definition, body) => {
            let definition = *definition;
            let definition = tag_node(definition, ctx);
            let def_free_vars = definition.free_variables.clone();
            let mut tagged = ctx.enter_binder(binder, |ctx| {
                tag_union_children(
                    body,
                    move |body| NodeKind::Let(binder, Box::new(definition), body),
                    meta,
                    ctx,
                )
            });
            tagged.free_variables.union_with(&def_free_vars);
            tagged
        }
        NodeKind::Call(proc, args) => {
            tag_union_children(args, |args| NodeKind::Call(proc, args), meta, ctx)
        }
        NodeKind::Seq(binder, nodes) => ctx.enter_binder(binder, |ctx| {
            tag_union_children(nodes, |nodes| NodeKind::Seq(binder, nodes), meta, ctx)
        }),
        NodeKind::Struct(binder, nodes) => ctx.enter_binder(binder, |ctx| {
            tag_union_children(nodes, |nodes| NodeKind::Struct(binder, nodes), meta, ctx)
        }),
        NodeKind::Prop(struct_var, prop, variant) => {
            let rel = tag_node(*variant.rel, ctx);
            let val = tag_node(*variant.val, ctx);
            let free_variables =
                union_bitsets([&rel.free_variables, &val.free_variables].into_iter());

            TaggedNode {
                kind: NodeKind::Prop(
                    struct_var,
                    prop,
                    PropVariant {
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
    children: Vec<OntosNode<'m>>,
    func: impl FnOnce(Vec<TaggedNode<'m>>) -> TaggedKind<'m>,
    meta: Meta<'m>,
    ctx: &mut TagCtx,
) -> TaggedNode<'m> {
    let children = tag_nodes(children, ctx);
    let free_variables = union_bitsets(children.iter().map(|arg| &arg.free_variables));
    TaggedNode {
        kind: func(children),
        meta,
        free_variables,
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

    use super::tag_node;

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
            (struct (#0)
                (prop #0 a
                    (#1
                        (struct (#2)
                            (prop #2 b
                                (#3 #4)
                            )
                        )
                    )
                )
            )
        ";
        let node = Parser::new(TypedOntos).parse(src).unwrap().0;
        let mut ctx = super::TagCtx::default();
        let tagged_node = tag_node(node, &mut ctx);
        assert_eq!(
            free_variables([Variable(1), Variable(3), Variable(4)].into_iter()),
            tagged_node.free_variables,
        );
    }
}
