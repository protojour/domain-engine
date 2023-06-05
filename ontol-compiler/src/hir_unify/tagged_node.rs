use derive_debug_extras::DebugExtras;
use smallvec::SmallVec;
use std::fmt::Debug;
use tracing::debug;

use bit_set::BitSet;
use ontol_hir::{
    kind::{Attribute, Dimension, NodeKind, Optional, PropVariant},
    Binder, Label, Variable,
};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};

use crate::{
    typed_hir::{Meta, TypedHir, TypedHirNode},
    types::TypeRef,
    SourceSpan,
};

use super::u_node::{
    AttrUNodeKind, BlockUNodeKind, ExprUNodeKind, LeafUNodeKind, UAttr, UBlockBody, UNode,
    UNodeKind,
};

#[derive(DebugExtras, Clone, Copy)]
pub enum TaggedKind {
    VariableRef(Variable),
    Unit,
    Int(i64),
    Let(Binder),
    Call(BuiltinProc),
    Map,
    Seq(Label),
    Struct(Binder),
    Prop(Optional, Variable, PropertyId),
    PropVariant(Optional, Variable, PropertyId, Dimension),
}

// Note: This is more granular than typed_hir nodes.
// prop and match arms should be "untyped".
pub struct TaggedNode<'m> {
    pub kind: TaggedKind,
    pub children: TaggedNodes<'m>,
    pub meta: Meta<'m>,
    pub free_variables: BitSet,
    pub option_depth: u16,
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
            TaggedKind::Prop(optional, struct_var, id) => NodeKind::Prop(
                optional,
                struct_var,
                id,
                self.children
                    .0
                    .into_iter()
                    .filter_map(|node| match node.kind {
                        TaggedKind::PropVariant(_, _, _, dimension) => {
                            let (rel, val) = node.children.into_hir_pair();

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
    option_depth: u16,
    #[allow(unused)]
    unit_type: TypeRef<'m>,
}

#[derive(Default)]
struct Union {
    free_variables: BitSet,
}

impl Union {
    fn plus<'m>(&mut self, u_node: UNode<'m>) -> UNode<'m> {
        self.free_variables.union_with(&u_node.free_variables);
        u_node
    }

    fn plus_vec<'m>(&mut self, u_nodes: Vec<UNode<'m>>) -> Vec<UNode<'m>> {
        for u_node in &u_nodes {
            self.free_variables.union_with(&u_node.free_variables);
        }
        u_nodes
    }

    fn plus_block_body<'m>(&mut self, u_nodes: Vec<UNode<'m>>) -> UBlockBody<'m> {
        let u_nodes = self.plus_vec(u_nodes);
        UBlockBody {
            sub_scoping: Default::default(),
            nodes: u_nodes,
            dependent_scopes: Default::default(),
        }
    }
}

pub struct UNodes<'m>(SmallVec<[UNode<'m>; 1]>);

impl<'m> UNodes<'m> {
    pub fn unwrap_one(self) -> UNode<'m> {
        let mut iterator = self.0.into_iter();
        let item = iterator.next().unwrap();
        if iterator.next().is_some() {
            panic!("More than one UNode");
        }
        item
    }
}

impl<'m> From<UNode<'m>> for UNodes<'m> {
    fn from(value: UNode<'m>) -> Self {
        Self([value].into())
    }
}

impl<'m> Tagger<'m> {
    pub fn new(unit_type: TypeRef<'m>) -> Self {
        Self {
            in_scope: BitSet::new(),
            labels: BitSet::new(),
            option_depth: 0,
            unit_type,
        }
    }

    pub fn to_u_nodes(&mut self, node: TypedHirNode<'m>) -> UNodes<'m> {
        let (kind, meta) = node.split();
        match kind {
            NodeKind::VariableRef(var) => {
                let u_node = self.make_leaf(LeafUNodeKind::VariableRef(var), meta);

                if self.in_scope.contains(var.0 as usize) {
                    u_node
                } else {
                    u_node.union_var(var)
                }
                .into()
            }
            NodeKind::Unit => self.make_leaf(LeafUNodeKind::Unit, meta).into(),
            NodeKind::Int(int) => self.make_leaf(LeafUNodeKind::Int(int), meta).into(),
            NodeKind::Let(binder, def, body) => self.enter_binder(binder, |zelf| {
                let mut union = Union::default();
                let def = Box::new(union.plus(zelf.to_u_nodes(*def).unwrap_one()));
                let body = union.plus_block_body(
                    body.into_iter()
                        .flat_map(|hir| zelf.to_u_nodes(hir).0.into_iter())
                        .collect(),
                );
                UNode {
                    free_variables: union.free_variables,
                    kind: UNodeKind::Block(BlockUNodeKind::Let(binder, def), body),
                    meta,
                }
                .into()
            }),
            NodeKind::Call(proc, args) => {
                self.to_expr_u_nodes(ExprUNodeKind::Call(proc), args, meta)
            }
            NodeKind::Map(arg) => self.to_expr_u_nodes(ExprUNodeKind::Map, vec![*arg], meta),
            NodeKind::Seq(label, attr) => {
                self.register_label(label);

                let (mut free_variables, attr) = self.to_u_attr(*attr.rel, *attr.val);
                free_variables.insert(label.0 as usize);

                UNode {
                    free_variables,
                    kind: UNodeKind::Attr(AttrUNodeKind::Seq(label), attr),
                    meta,
                }
                .into()
            }
            NodeKind::Struct(binder, nodes) => self.enter_binder(binder, |zelf| {
                let mut union = Union::default();
                let body = union.plus_block_body(
                    nodes
                        .into_iter()
                        .flat_map(|hir| zelf.to_u_nodes(hir).0.into_iter())
                        .collect(),
                );
                UNode {
                    free_variables: union.free_variables,
                    kind: UNodeKind::Block(BlockUNodeKind::Struct(binder), body),
                    meta,
                }
                .into()
            }),
            NodeKind::Prop(optional, struct_var, id, variants) => {
                if optional.0 {
                    self.enter_option(|zelf| {
                        zelf.to_prop_variants(optional, struct_var, id, variants, meta)
                    })
                } else {
                    self.to_prop_variants(optional, struct_var, id, variants, meta)
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

    fn to_expr_u_nodes(
        &mut self,
        kind: ExprUNodeKind,
        args: Vec<TypedHirNode<'m>>,
        meta: Meta<'m>,
    ) -> UNodes<'m> {
        let mut union = Union::default();
        let args = union.plus_vec(
            args.into_iter()
                .flat_map(|hir| self.to_u_nodes(hir).0.into_iter())
                .collect(),
        );
        UNode {
            free_variables: union.free_variables,
            kind: UNodeKind::Expr(kind, args),
            meta,
        }
        .into()
    }

    fn to_prop_variants(
        &mut self,
        optional: Optional,
        struct_var: Variable,
        property_id: PropertyId,
        variants: Vec<PropVariant<'m, TypedHir>>,
        meta: Meta<'m>,
    ) -> UNodes<'m> {
        let variants = variants
            .into_iter()
            .map(|PropVariant { dimension, attr }| {
                debug!("rel ty: {:?}", attr.rel.meta.ty);
                debug!("val ty: {:?}", attr.val.meta.ty);

                let (mut variant_variables, attr) = self.to_u_attr(*attr.rel, *attr.val);

                let val_ty = attr.val.meta.ty;

                if let Dimension::Seq(label) = dimension {
                    self.register_label(label);
                    variant_variables.insert(label.0 as usize);
                } else {
                    // free_variables.union_with(&variant_variables);
                }

                UNode {
                    free_variables: variant_variables,
                    kind: UNodeKind::Attr(
                        AttrUNodeKind::PropVariant(optional, struct_var, property_id),
                        attr,
                    ),
                    meta: Meta {
                        // BUG: Not correct
                        ty: val_ty,
                        span: SourceSpan::none(),
                    },
                }
            })
            .collect();

        UNodes(variants)
    }

    fn to_u_attr(&mut self, rel: TypedHirNode<'m>, val: TypedHirNode<'m>) -> (BitSet, UAttr<'m>) {
        let mut free_variables = BitSet::new();
        let rel = self.to_u_nodes(rel).unwrap_one();
        free_variables.union_with(&rel.free_variables);

        let val = self.to_u_nodes(val).unwrap_one();
        free_variables.union_with(&val.free_variables);

        (
            free_variables,
            UAttr {
                rel: Box::new(rel),
                val: Box::new(val),
            },
        )
    }

    fn make_leaf(&mut self, kind: LeafUNodeKind, meta: Meta<'m>) -> UNode<'m> {
        UNode {
            free_variables: BitSet::new(),
            kind: UNodeKind::Leaf(kind),
            meta,
        }
    }

    fn make_u_node(&mut self, kind: UNodeKind<'m>, meta: Meta<'m>) -> UNode<'m> {
        UNode {
            free_variables: BitSet::new(),
            kind,
            meta,
        }
    }

    /*
    pub fn tag_node2(&mut self, node: TypedHirNode<'m>) -> TargetNode<'m> {
        let (kind, meta) = node.split();
        match kind {
            NodeKind::VariableRef(var) => {
                let tagged_node = self.make_target(TaggedKind::VariableRef(var), [], meta);
                if self.in_scope.contains(var.0 as usize) {
                    tagged_node
                } else {
                    tagged_node.union_var(var)
                }
            }
            NodeKind::Unit => self.make_target(TaggedKind::Unit, [], meta),
            NodeKind::Int(int) => self.make_target(TaggedKind::Int(int), [], meta),
            NodeKind::Let(binder, definition, body) => self.enter_binder(binder, |zelf| {
                zelf.make_target(
                    TaggedKind::Let(binder),
                    [*definition].into_iter().chain(body),
                    meta,
                )
            }),
            NodeKind::Call(proc, args) => {
                self.make_target(TaggedKind::Call(proc), args.into_iter(), meta)
            }
            NodeKind::Map(arg) => {
                let arg = self.tag_node2(*arg);
                TargetNode {
                    free_variables: arg.free_variables.clone(),
                    kind: TaggedKind::Map,
                    sub_nodes: NodeSet::new(vec![arg]),
                    meta,
                    option_depth: self.option_depth,
                }
            }
            NodeKind::Seq(label, attr) => {
                self.register_label(label);
                let rel = self.tag_node2(*attr.rel);
                let val = self.tag_node2(*attr.val);

                TargetNode {
                    free_variables: union_free_variables2([&rel, &val]),
                    kind: TaggedKind::Seq(label),
                    sub_nodes: NodeSet::new(vec![rel, val]),
                    meta,
                    option_depth: self.option_depth,
                }
                .union_label(label)
            }
            NodeKind::Struct(binder, nodes) => self.enter_binder(binder, |zelf| {
                zelf.make_target(TaggedKind::Struct(binder), nodes.into_iter(), meta)
            }),
            NodeKind::Prop(optional, struct_var, id, variants) => {
                if optional.0 {
                    self.enter_option(|zelf| {
                        zelf.tag_prop2(optional, struct_var, id, variants, meta)
                    })
                } else {
                    self.tag_prop2(optional, struct_var, id, variants, meta)
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

    fn tag_prop2(
        &mut self,
        optional: Optional,
        struct_var: Variable,
        id: PropertyId,
        variants: Vec<PropVariant<'m, TypedHir>>,
        meta: Meta<'m>,
    ) -> TargetNode<'m> {
        let mut free_variables = BitSet::new();

        let variants = variants
            .into_iter()
            .map(|PropVariant { dimension, attr }| {
                debug!("rel ty: {:?}", attr.rel.meta.ty);
                debug!("val ty: {:?}", attr.val.meta.ty);

                let rel = self.tag_node2(*attr.rel);
                let val = self.tag_node2(*attr.val);

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

                TargetNode {
                    kind: TaggedKind::PropVariant(optional, struct_var, id, dimension),
                    free_variables: variant_variables,
                    sub_nodes: NodeSet::new(vec![rel, val]),
                    meta: Meta {
                        // BUG: Not correct
                        ty: val_ty,
                        span: SourceSpan::none(),
                    },
                    option_depth: self.option_depth,
                }
            })
            .collect();

        TargetNode {
            free_variables,
            kind: TaggedKind::Prop(optional, struct_var, id),
            sub_nodes: NodeSet::new(variants),
            meta,
            option_depth: self.option_depth,
        }
    }

    fn make_target(
        &mut self,
        kind: TaggedKind,
        children: impl IntoIterator<Item = TypedHirNode<'m>>,
        meta: Meta<'m>,
    ) -> TargetNode<'m> {
        let unscoped = self.tag_sub_nodes(children.into_iter());
        TargetNode {
            free_variables: union_free_variables2(unscoped.as_slice()),
            kind,
            sub_nodes: NodeSet::new(unscoped),
            meta,
            option_depth: self.option_depth,
        }
    }
    */

    pub fn tag_node(&mut self, node: TypedHirNode<'m>) -> TaggedNode<'m> {
        let (kind, meta) = node.split();
        match kind {
            NodeKind::VariableRef(var) => {
                let tagged_node = self.make_tagged(TaggedKind::VariableRef(var), [], meta);
                if self.in_scope.contains(var.0 as usize) {
                    tagged_node
                } else {
                    tagged_node.union_var(var)
                }
            }
            NodeKind::Unit => self.make_tagged(TaggedKind::Unit, [], meta),
            NodeKind::Int(int) => self.make_tagged(TaggedKind::Int(int), [], meta),
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
                    option_depth: self.option_depth,
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
                    option_depth: self.option_depth,
                }
                .union_label(label)
            }
            NodeKind::Struct(binder, nodes) => self.enter_binder(binder, |zelf| {
                zelf.make_tagged(TaggedKind::Struct(binder), nodes.into_iter(), meta)
            }),
            NodeKind::Prop(optional, struct_var, id, variants) => {
                if optional.0 {
                    self.enter_option(|zelf| {
                        zelf.tag_prop(optional, struct_var, id, variants, meta)
                    })
                } else {
                    self.tag_prop(optional, struct_var, id, variants, meta)
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

    fn tag_prop(
        &mut self,
        optional: Optional,
        struct_var: Variable,
        id: PropertyId,
        variants: Vec<PropVariant<'m, TypedHir>>,
        meta: Meta<'m>,
    ) -> TaggedNode<'m> {
        let mut free_variables = BitSet::new();

        let variants = variants
            .into_iter()
            .map(|PropVariant { dimension, attr }| {
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
                    kind: TaggedKind::PropVariant(optional, struct_var, id, dimension),
                    free_variables: variant_variables,
                    children: TaggedNodes(vec![rel, val]),
                    meta: Meta {
                        // BUG: Not correct
                        ty: val_ty,
                        span: SourceSpan::none(),
                    },
                    option_depth: self.option_depth,
                }
            })
            .collect();

        TaggedNode {
            free_variables,
            kind: TaggedKind::Prop(optional, struct_var, id),
            children: TaggedNodes(variants),
            meta,
            option_depth: self.option_depth,
        }
    }

    fn tag_nodes(
        &mut self,
        iterator: impl Iterator<Item = TypedHirNode<'m>>,
    ) -> Vec<TaggedNode<'m>> {
        iterator.map(|child| self.tag_node(child)).collect()
    }

    fn make_tagged(
        &mut self,
        kind: TaggedKind,
        children: impl IntoIterator<Item = TypedHirNode<'m>>,
        meta: Meta<'m>,
    ) -> TaggedNode<'m> {
        let children = self.tag_nodes(children.into_iter());
        TaggedNode {
            free_variables: union_free_variables(children.as_slice()),
            kind,
            children: TaggedNodes(children),
            meta,
            option_depth: self.option_depth,
        }
    }

    fn register_label(&mut self, label: ontol_hir::Label) {
        if self.labels.contains(label.0 as usize) {
            panic!("Duplicate label: {label}");
        }

        self.labels.insert(label.0 as usize);
    }

    fn enter_binder<T>(&mut self, binder: Binder, func: impl FnOnce(&mut Self) -> T) -> T {
        if !self.in_scope.insert(binder.0 .0 as usize) {
            panic!("Malformed HIR: {binder:?} variable was already in scope");
        }
        let value = func(self);
        self.in_scope.remove(binder.0 .0 as usize);
        value
    }

    fn enter_option<T>(&mut self, func: impl FnOnce(&mut Self) -> T) -> T {
        self.option_depth += 1;
        let value = func(self);
        self.option_depth -= 1;
        value
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
