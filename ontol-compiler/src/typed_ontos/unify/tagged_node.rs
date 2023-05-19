use bit_set::BitSet;
use ontos::kind::{Binder, NodeKind, Variable};

use crate::typed_ontos::lang::{Meta, OntosKind, OntosNode};

#[derive(Clone, Copy, Debug)]
pub struct TaggedTree;

impl ontos::Lang for TaggedTree {
    type Node<'m> = TaggedNode<'m>;

    fn make_node<'a>(&self, _kind: NodeKind<'a, Self>) -> Self::Node<'a> {
        unimplemented!()
    }
}

impl<'m> OntosNode<'m> {
    fn split(self) -> (OntosKind<'m>, Meta<'m>) {
        (self.kind, self.meta)
    }
}

type TaggedKind<'m> = NodeKind<'m, TaggedTree>;

#[derive(Debug)]
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
        NodeKind::Call(proc, args) => {
            tag_union_children(args, |args| NodeKind::Call(proc, args), meta, ctx)
        }
        NodeKind::Seq(binder, nodes) => ctx.enter_binder(binder, |ctx| {
            tag_union_children(nodes, |nodes| NodeKind::Seq(binder, nodes), meta, ctx)
        }),
        NodeKind::Struct(binder, nodes) => ctx.enter_binder(binder, |ctx| {
            tag_union_children(nodes, |nodes| NodeKind::Struct(binder, nodes), meta, ctx)
        }),
        NodeKind::Prop(struct_var, prop, rel, val) => {
            let rel = tag_node(*rel, ctx);
            let val = tag_node(*val, ctx);
            let free_variables =
                union_bitsets([&rel.free_variables, &val.free_variables].into_iter());

            TaggedNode {
                kind: NodeKind::Prop(struct_var, prop, Box::new(rel), Box::new(val)),
                meta,
                free_variables,
            }
        }
        NodeKind::Destruct(..) => {
            unimplemented!("BUG: Destruct is an output node")
        }
        NodeKind::MatchProp(..) => {
            unimplemented!("BUG: MatchProp is an output node")
        }
        NodeKind::MapSeq(..) => {
            todo!()
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
    use ontos::{kind::Variable, parse::Parser};

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
                (prop #0 a #1
                    (struct (#2)
                        (prop #2 b #3 #4)
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
/*
fn subst_invert<'m>(node: OntosNode<'m>, ctx: &mut Ctx<'m>) -> Option<InvertedNode<'m>> {
    subst_filter_map_kind(node, |kind| match kind {
        NodeKind::Struct(binder, nodes) => {
            let children = nodes
                .into_iter()
                .filter_map(|node| subst_invert(node, ctx))
                .filter_map(Inverted::unbound);

            Some(Inverted {
                value: NodeKind::Destruct(binder.0, children.collect()),
                free_var: Some(binder.0),
            })
        }
        NodeKind::Prop(var, prop, rel, val) => {
            let rel = subst_invert(*rel, ctx).and_then(Inverted::bindable);
            let val = subst_invert(*val, ctx).and_then(Inverted::bindable);

            let match_arm = match (rel, val) {
                (None, None) => return None,
                (Some((rel_var, rel)), None) => MatchArm {
                    pattern: PropPattern::Present(
                        PatternBinding::Binder(rel_var),
                        PatternBinding::Wildcard,
                    ),
                    node: rel,
                },
                (None, Some((val_var, val))) => MatchArm {
                    pattern: PropPattern::Present(
                        PatternBinding::Wildcard,
                        PatternBinding::Binder(val_var),
                    ),
                    node: val,
                },
                (Some((rel_var, rel)), Some((val_var, val))) => MatchArm {
                    pattern: PropPattern::Present(
                        PatternBinding::Binder(rel_var),
                        PatternBinding::Binder(val_var),
                    ),
                    node: val,
                },
            };

            Some(Inverted {
                value: NodeKind::MatchProp(var, prop, vec![match_arm]),
                free_var: None,
            })
        }
        _ => None,
    })
}

fn subst_filter_map_kind<'m>(
    node: OntosNode<'m>,
    mut f: impl FnMut(OntosKind<'m>) -> Option<InvertedKind<'m>>,
) -> Option<InvertedNode<'m>> {
    let inverted_kind = f(node.kind)?;
    let node = OntosNode {
        kind: inverted_kind.value,
        ty: node.ty,
        span: node.span,
    };
    Some(Inverted {
        free_var: inverted_kind.free_var,
        value: node,
    })
}
*/
