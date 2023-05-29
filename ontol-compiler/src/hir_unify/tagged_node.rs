use derive_debug_extras::DebugExtras;
use std::fmt::Debug;
use tracing::debug;

use bit_set::BitSet;
use ontol_hir::{
    kind::{Attribute, Dimension, NodeKind, PropVariant},
    Binder, Label, Variable,
};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};

use crate::{
    typed_hir::{Meta, TypedHirNode},
    types::TypeRef,
    SourceSpan,
};

#[derive(DebugExtras)]
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
    PresentPropVariant(Variable, PropertyId, Dimension),
    NotPresentPropVariant(Variable, PropertyId),
}

// Note: This is more granular than typed_hir nodes.
// prop and match arms should be "untyped".
pub struct TaggedNode<'m> {
    pub kind: TaggedKind,
    pub children: TaggedNodes<'m>,
    pub meta: Meta<'m>,
    pub free_variables: BitSet,
}

impl<'m> Debug for TaggedNode<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TaggedNode")
            .field(&self.kind)
            .field(&self.children.0)
            .finish()
    }
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

    pub fn into_hir_node(self) -> TypedHirNode<'m> {
        let kind = match self.kind {
            TaggedKind::VariableRef(var) => NodeKind::VariableRef(var),
            TaggedKind::Unit => NodeKind::Unit,
            TaggedKind::Int(int) => NodeKind::Int(int),
            TaggedKind::Call(proc) => NodeKind::Call(proc, self.children.into_hir()),
            TaggedKind::Map => NodeKind::Map(Box::new(self.children.into_hir_one())),
            TaggedKind::Let(binder) => {
                let (def, body) = self.children.one_then_rest_into_hir();
                NodeKind::Let(binder, Box::new(def), body)
            }
            TaggedKind::Seq(binder) => {
                let (rel, val) = self.children.into_hir_pair();
                NodeKind::Seq(
                    binder,
                    Attribute {
                        rel: Box::new(rel),
                        val: Box::new(val),
                    },
                )
            }
            TaggedKind::Struct(binder) => NodeKind::Struct(binder, self.children.into_hir()),
            TaggedKind::Prop(struct_var, id) => NodeKind::Prop(
                struct_var,
                id,
                self.children
                    .0
                    .into_iter()
                    .map(|node| match node.kind {
                        TaggedKind::PresentPropVariant(_, _, dimension) => {
                            let (rel, val) = node.children.into_hir_pair();

                            PropVariant::Present {
                                dimension,
                                attr: Attribute {
                                    rel: Box::new(rel),
                                    val: Box::new(val),
                                },
                            }
                        }
                        _ => PropVariant::NotPresent,
                    })
                    .collect(),
            ),
            other => panic!("{other:?} should not be handled at top level"),
        };

        TypedHirNode {
            kind,
            meta: self.meta,
        }
    }
}

impl<'m> TaggedNodes<'m> {
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

    fn collect(iterator: impl Iterator<Item = TaggedNode<'m>>) -> Vec<TypedHirNode<'m>> {
        iterator.map(TaggedNode::into_hir_node).collect()
    }
}

pub struct Tagger<'m> {
    in_scope: BitSet,
    labels: BitSet,
    unit_type: TypeRef<'m>,
}

impl<'m> Tagger<'m> {
    pub fn new(unit_type: TypeRef<'m>) -> Self {
        Self {
            in_scope: BitSet::new(),
            labels: BitSet::new(),
            unit_type,
        }
    }

    pub fn enter_binder<T>(&mut self, binder: Binder, func: impl FnOnce(&mut Self) -> T) -> T {
        if !self.in_scope.insert(binder.0 .0 as usize) {
            panic!("Malformed HIR: {binder:?} variable was already in scope");
        }
        let value = func(self);
        self.in_scope.remove(binder.0 .0 as usize);
        value
    }

    pub fn tag_nodes(
        &mut self,
        iterator: impl Iterator<Item = TypedHirNode<'m>>,
    ) -> Vec<TaggedNode<'m>> {
        iterator.map(|child| self.tag_node(child)).collect()
    }

    pub fn tag_node(&mut self, node: TypedHirNode<'m>) -> TaggedNode<'m> {
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
            NodeKind::Let(binder, definition, body) => self.enter_binder(binder, |zelf| {
                zelf.make_tagged(
                    TaggedKind::Let(binder),
                    [*definition].into_iter().chain(body),
                    meta,
                )
            }),
            NodeKind::Call(proc, args) => {
                self.make_tagged(TaggedKind::Call(proc), args.into_iter(), meta)
            }
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
                self.register_label(label);
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
                zelf.make_tagged(TaggedKind::Struct(binder), nodes.into_iter(), meta)
            }),
            NodeKind::Prop(struct_var, prop, variants) => {
                let mut free_variables = BitSet::new();

                let variants = variants
                    .into_iter()
                    .map(|variant| {
                        match variant {
                            PropVariant::Present { dimension, attr } => {
                                debug!("rel ty: {:?}", attr.rel.meta.ty);
                                debug!("val ty: {:?}", attr.val.meta.ty);

                                let rel = self.tag_node(*attr.rel);
                                let val = self.tag_node(*attr.val);

                                let val_ty = val.meta.ty;

                                let mut variant_variables = BitSet::new();

                                variant_variables.union_with(&rel.free_variables);
                                variant_variables.union_with(&val.free_variables);

                                if let Dimension::Seq(label) = dimension {
                                    self.register_label(label);
                                    variant_variables.insert(label.0 as usize);
                                    free_variables.insert(label.0 as usize);
                                } else {
                                    free_variables.union_with(&variant_variables);
                                }

                                TaggedNode {
                                    kind: TaggedKind::PresentPropVariant(
                                        struct_var, prop, dimension,
                                    ),
                                    free_variables: variant_variables,
                                    children: TaggedNodes(vec![rel, val]),
                                    meta: Meta {
                                        // BUG: Not correct
                                        ty: val_ty,
                                        span: SourceSpan::none(),
                                    },
                                }
                            }
                            PropVariant::NotPresent => TaggedNode {
                                kind: TaggedKind::NotPresentPropVariant(struct_var, prop),
                                free_variables: BitSet::new(),
                                children: TaggedNodes(vec![]),
                                meta: Meta {
                                    ty: self.unit_type,
                                    span: SourceSpan::none(),
                                },
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
            NodeKind::MatchProp(..) => {
                unimplemented!("BUG: MatchProp is an output node")
            }
            NodeKind::Gen(..) => {
                todo!()
            }
            NodeKind::Iter(..) => {
                todo!()
            }
            NodeKind::Push(..) => {
                todo!()
            }
        }
    }

    fn make_tagged(
        &mut self,
        kind: TaggedKind,
        children: impl Iterator<Item = TypedHirNode<'m>>,
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

    fn register_label(&mut self, label: ontol_hir::Label) {
        if self.labels.contains(label.0 as usize) {
            panic!("Duplicate label: {label}");
        }

        self.labels.insert(label.0 as usize);
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

#[cfg(test)]
mod tests {
    use bit_set::BitSet;
    use ontol_hir::{parse::Parser, Variable};

    use crate::{typed_hir::TypedHir, types::Type};

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
        let node = Parser::new(TypedHir).parse(src).unwrap().0;
        let tagged_node = super::Tagger::new(&Type::Error).tag_node(node);
        assert_eq!(
            free_variables([Variable(1), Variable(3), Variable(4)].into_iter()),
            tagged_node.free_variables,
        );
    }
}
