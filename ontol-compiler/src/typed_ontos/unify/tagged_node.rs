use std::fmt::Debug;

use bit_set::BitSet;
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use ontos::{
    kind::{Attribute, Dimension, NodeKind, PropVariant},
    Binder, Label, Variable,
};

use crate::{
    typed_ontos::lang::{Meta, OntosNode},
    types::Type,
    SourceSpan,
};

#[derive(Debug)]
pub enum TaggedKind {
    VariableRef(Variable),
    Unit,
    Int(i64),
    Let(Binder),
    Call(BuiltinProc),
    Map,
    Seq(Label),
    Struct(Binder),
    Prop(Variable, PropertyId),
    PropVariant(Variable, PropertyId, Dimension),
}

// Note: This is more granular than ontos nodes.
// prop and match arms should be "untyped".
#[derive(Debug)]
pub struct TaggedNode<'m> {
    pub kind: TaggedKind,
    pub children: TaggedNodes<'m>,
    pub meta: Meta<'m>,
    pub free_variables: BitSet,
}

#[derive(Debug)]
pub struct TaggedNodes<'m>(pub Vec<TaggedNode<'m>>);

impl<'m> TaggedNode<'m> {
    fn new(kind: TaggedKind, meta: Meta<'m>) -> Self {
        Self {
            kind,
            children: TaggedNodes(vec![]),
            meta,
            free_variables: BitSet::new(),
        }
    }

    fn union_var(mut self, var: Variable) -> Self {
        self.free_variables.insert(var.0 as usize);
        self
    }

    fn union_label(mut self, label: Label) -> Self {
        self.free_variables.insert(label.0 as usize);
        self
    }

    pub fn into_ontos_node(self) -> OntosNode<'m> {
        let kind = match self.kind {
            TaggedKind::VariableRef(var) => NodeKind::VariableRef(var),
            TaggedKind::Unit => NodeKind::Unit,
            TaggedKind::Int(int) => NodeKind::Int(int),
            TaggedKind::Call(proc) => NodeKind::Call(proc, self.children.into_ontos()),
            TaggedKind::Map => NodeKind::Map(Box::new(self.children.into_ontos_one())),
            TaggedKind::Let(binder) => {
                let (def, body) = self.children.one_then_rest_into_ontos();
                NodeKind::Let(binder, Box::new(def), body)
            }
            TaggedKind::Seq(binder) => {
                let (rel, val) = self.children.into_ontos_pair();
                NodeKind::Seq(
                    binder,
                    Attribute {
                        rel: Box::new(rel),
                        val: Box::new(val),
                    },
                )
            }
            TaggedKind::Struct(binder) => NodeKind::Struct(binder, self.children.into_ontos()),
            TaggedKind::Prop(struct_var, id) => NodeKind::Prop(
                struct_var,
                id,
                self.children
                    .0
                    .into_iter()
                    .map(|node| match node.kind {
                        TaggedKind::PropVariant(_, _, dimension) => {
                            let (rel, val) = node.children.into_ontos_pair();

                            PropVariant {
                                dimension,
                                attr: Attribute {
                                    rel: Box::new(rel),
                                    val: Box::new(val),
                                },
                            }
                        }
                        _ => panic!(),
                    })
                    .collect(),
            ),
            other => panic!("{other:?} should not be handled at top level"),
        };

        OntosNode {
            kind,
            meta: self.meta,
        }
    }
}

impl<'m> TaggedNodes<'m> {
    pub fn into_ontos(self) -> Vec<OntosNode<'m>> {
        Self::collect(self.0.into_iter())
    }

    pub fn one_then_rest_into_ontos(self) -> (OntosNode<'m>, Vec<OntosNode<'m>>) {
        let mut iterator = self.0.into_iter();
        let first = iterator.next().unwrap();
        let rest = Self::collect(iterator);
        (first.into_ontos_node(), rest)
    }

    pub fn into_ontos_one(self) -> OntosNode<'m> {
        self.0.into_iter().next().unwrap().into_ontos_node()
    }

    pub fn into_ontos_pair(self) -> (OntosNode<'m>, OntosNode<'m>) {
        let mut iterator = self.0.into_iter();
        let first = iterator.next().unwrap();
        let second = iterator.next().unwrap();
        (first.into_ontos_node(), second.into_ontos_node())
    }

    fn collect(iterator: impl Iterator<Item = TaggedNode<'m>>) -> Vec<OntosNode<'m>> {
        iterator.map(TaggedNode::into_ontos_node).collect()
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
                let tagged_node = TaggedNode::new(TaggedKind::VariableRef(var), meta);
                if self.in_scope.contains(var.0 as usize) {
                    tagged_node
                } else {
                    tagged_node.union_var(var)
                }
            }
            NodeKind::Unit => TaggedNode::new(TaggedKind::Unit, meta),
            NodeKind::Int(int) => TaggedNode::new(TaggedKind::Int(int), meta),
            NodeKind::Let(binder, definition, body) => {
                let definition = *definition;
                let definition = self.tag_node(definition);
                let def_free_vars = definition.free_variables.clone();
                let mut tagged = self.enter_binder(binder, |zelf| {
                    zelf.tag_children(body, TaggedKind::Let(binder), meta)
                });
                tagged.free_variables.union_with(&def_free_vars);
                tagged
            }
            NodeKind::Call(proc, args) => self.tag_children(args, TaggedKind::Call(proc), meta),
            NodeKind::Map(arg) => {
                let arg = self.tag_node(*arg);
                TaggedNode {
                    free_variables: arg.free_variables.clone(),
                    kind: TaggedKind::Map,
                    children: TaggedNodes(vec![arg]),
                    meta,
                }
            }
            NodeKind::Seq(label, attr) => {
                let rel = self.tag_node(*attr.rel);
                let val = self.tag_node(*attr.val);

                TaggedNode {
                    free_variables: union_free_variables([&rel, &val]),
                    kind: TaggedKind::Seq(label),
                    children: TaggedNodes(vec![rel, val]),
                    meta,
                }
                .union_label(label)
            }
            NodeKind::Struct(binder, nodes) => self.enter_binder(binder, |zelf| {
                zelf.tag_children(nodes, TaggedKind::Struct(binder), meta)
            }),
            NodeKind::Prop(struct_var, prop, variants) => {
                let mut free_variables = BitSet::new();

                let variants = variants
                    .into_iter()
                    .map(|variant| {
                        let rel = self.tag_node(*variant.attr.rel);
                        let val = self.tag_node(*variant.attr.val);

                        let mut variant_variables = BitSet::new();

                        variant_variables.union_with(&rel.free_variables);
                        variant_variables.union_with(&val.free_variables);

                        if let Dimension::Seq(label) = variant.dimension {
                            variant_variables.insert(label.0 as usize);
                        }

                        free_variables.union_with(&variant_variables);

                        TaggedNode {
                            kind: TaggedKind::PropVariant(struct_var, prop, variant.dimension),
                            free_variables: variant_variables,
                            children: TaggedNodes(vec![rel, val]),
                            meta: Meta {
                                ty: &Type::Tautology,
                                span: SourceSpan::none(),
                            },
                        }
                    })
                    .collect();

                TaggedNode {
                    free_variables,
                    kind: TaggedKind::Prop(struct_var, prop),
                    children: TaggedNodes(variants),
                    meta,
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

    fn tag_children<'m>(
        &mut self,
        children: Vec<OntosNode<'m>>,
        kind: TaggedKind,
        meta: Meta<'m>,
    ) -> TaggedNode<'m> {
        let children = self.tag_nodes(children);
        TaggedNode {
            free_variables: union_free_variables(children.as_slice()),
            kind,
            children: TaggedNodes(children),
            meta,
        }
    }
}

pub fn union_free_variables<'a, 'm: 'a>(
    collection: impl IntoIterator<Item = &'a TaggedNode<'m>>,
) -> BitSet {
    let mut output = BitSet::new();
    for item in collection.into_iter().map(|node| &node.free_variables) {
        output.union_with(item);
    }
    output
}

pub struct DebugVariables<'a>(pub &'a BitSet);

impl<'a> Debug for DebugVariables<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let mut iterator = self.0.iter().peekable();
        while let Some(var) = iterator.next() {
            write!(f, "{}", Variable(var as u32))?;
            if iterator.peek().is_some() {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
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
            (struct ($a)
                (prop $a S:0:0
                    ($b
                        (struct ($c)
                            (prop $c S:0:1
                                ($d $e)
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
