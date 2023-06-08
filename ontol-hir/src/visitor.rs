use ontol_runtime::value::PropertyId;

use crate::{
    kind::{
        Dimension, IterBinder, MatchArm, NodeKind, Optional, PatternBinding, PropPattern,
        PropVariant,
    },
    Label, Lang, Node, Var,
};

macro_rules! visitor_trait_methods {
    ($ref:tt, $kind:ident, $iter:ident) => {
        fn visit_node(&mut self, index: usize, node: arg!($ref L::Node<'l>)) {
            self.visit_kind(index, node.$kind());
        }

        #[allow(unused_variables)]
        fn visit_kind(&mut self, index: usize, kind: arg!($ref NodeKind<'l, L>)) {
            self.traverse_kind(kind);
        }

        #[allow(unused_variables)]
        fn visit_prop(
            &mut self,
            optional: arg!($ref Optional),
            struct_var: arg!($ref Var),
            id: arg!($ref PropertyId),
            variants: arg!($ref Vec<PropVariant<'l, L>>)
        ) {
            self.traverse_prop(struct_var, id, variants);
        }

        #[allow(unused_variables)]
        fn visit_prop_variant(&mut self, index: usize, variant: arg!($ref PropVariant<'l, L>)) {
            self.traverse_prop_variant(variant);
        }

        #[allow(unused_variables)]
        fn visit_match_arm(&mut self, index: usize, match_arm: arg!($ref MatchArm<'l, L>)) {
            self.traverse_match_arm(match_arm);
        }

        #[allow(unused_variables)]
        fn visit_pattern_binding(&mut self, index: usize, binding: arg!($ref PatternBinding)) {
            self.traverse_pattern_binding(binding);
        }

        #[allow(unused_variables)]
        fn visit_property_id(&mut self, id: arg!($ref PropertyId)) {}

        #[allow(unused_variables)]
        fn visit_binder(&mut self, var: arg!($ref Var)) {}

        #[allow(unused_variables)]
        fn visit_iter_binder(&mut self, binder: arg!($ref IterBinder)) {
            self.traverse_iter_binder(binder);
        }

        #[allow(unused_variables)]
        fn visit_var(&mut self, var: arg!($ref Var)) {}

        #[allow(unused_variables)]
        fn visit_label(&mut self, label: arg!($ref Label)) {}

        fn traverse_kind(&mut self, kind: arg!($ref NodeKind<'l, L>)) {
            match kind {
                NodeKind::Var(var) => {
                    self.visit_var(var);
                }
                NodeKind::Int(_) => {}
                NodeKind::Unit => {}
                NodeKind::Call(_proc, params) => {
                    for (index, arg) in params.$iter().enumerate() {
                        self.visit_node(index, arg);
                    }
                }
                NodeKind::Map(arg) => {
                    self.visit_node(0, arg);
                }
                NodeKind::Let(binder, def, body) => {
                    self.visit_binder(borrow!($ref binder.0));
                    self.visit_node(0, def);
                    for (index, node) in body.$iter().enumerate() {
                        self.visit_node(index + 1, node);
                    }
                }
                NodeKind::Seq(label, spec) => {
                    self.visit_label(label);
                    self.visit_node(0, borrow!($ref spec.rel));
                    self.visit_node(1, borrow!($ref spec.val));
                }
                NodeKind::Struct(binder, children) => {
                    self.visit_binder(borrow!($ref binder.0));
                    for (index, child) in children.$iter().enumerate() {
                        self.visit_node(index, child);
                    }
                }
                NodeKind::Prop(optional, struct_var, id, variants) => {
                    self.visit_prop(optional, struct_var, id, variants);
                }
                NodeKind::MatchProp(struct_var, id, arms) => {
                    self.visit_var(struct_var);
                    self.visit_property_id(id);
                    for (index, arm) in arms.$iter().enumerate() {
                        self.visit_match_arm(index, arm);
                    }
                }
                NodeKind::Gen(seq_var, binder, children) => {
                    self.visit_var(seq_var);
                    self.visit_iter_binder(binder);
                    for (index, child) in children.$iter().enumerate() {
                        self.visit_node(index, child);
                    }
                }
                NodeKind::Iter(seq_var, binder, children) => {
                    self.visit_var(seq_var);
                    self.visit_iter_binder(binder);
                    for (index, child) in children.$iter().enumerate() {
                        self.visit_node(index, child);
                    }
                }
                NodeKind::Push(seq_var, attr) => {
                    self.visit_var(seq_var);
                    self.visit_node(0, borrow!($ref attr.rel));
                    self.visit_node(1, borrow!($ref attr.val));
                }
            }
        }

        #[allow(unused_variables)]
        fn traverse_prop(
            &mut self,
            struct_var: arg!($ref Var),
            id: arg!($ref PropertyId),
            variants: arg!($ref Vec<PropVariant<'l, L>>),
        ) {
            self.visit_var(struct_var);
            self.visit_property_id(id);
            for (index, variant) in variants.$iter().enumerate() {
                self.visit_prop_variant(index, variant);
            }
        }

        fn traverse_prop_variant(&mut self, variant: arg!($ref PropVariant<'l, L>)) {
            if let Dimension::Seq(label) = borrow!($ref variant.dimension) {
                self.visit_label(label);
            }
            self.visit_node(0, borrow!($ref variant.attr.rel));
            self.visit_node(1, borrow!($ref variant.attr.val));
        }

        fn traverse_match_arm(&mut self, match_arm: arg!($ref MatchArm<'l, L>)) {
            match borrow!($ref match_arm.pattern) {
                PropPattern::Attr(rel, val) => {
                    self.visit_pattern_binding(0, rel);
                    self.visit_pattern_binding(1, val);
                }
                PropPattern::Seq(val) => {
                    self.visit_pattern_binding(0, val);
                }
                PropPattern::Absent => {}
            }
        }

        fn traverse_pattern_binding(&mut self, binding: arg!($ref PatternBinding)) {
            match binding {
                PatternBinding::Binder(var) => self.visit_binder(var),
                PatternBinding::Wildcard => {}
            }
        }

        fn traverse_iter_binder(&mut self, binder: arg!($ref IterBinder)) {
            self.traverse_pattern_binding(borrow!($ref binder.seq));
            self.traverse_pattern_binding(borrow!($ref binder.rel));
            self.traverse_pattern_binding(borrow!($ref binder.val));
        }
    }
}

macro_rules! arg {
    ((& $lt:lifetime) $ty:ty) => {
        & $lt $ty
    };
    ((&) $ty:ty) => {
        & $ty
    };
    ((&mut) $ty:ty) => {
        &mut $ty
    };
}

macro_rules! borrow {
    ((& $lt:lifetime) $e:expr) => {
        &$e
    };
    ((&) $e:expr) => {
        &$e
    };
    ((&mut) $e:expr) => {
        &mut $e
    };
}

pub trait HirVisitor<'p, 'l: 'p, L: Lang + 'l> {
    visitor_trait_methods!((&'p), kind, iter);
}

pub trait HirMutVisitor<'l, L: Lang + 'l> {
    visitor_trait_methods!((&mut), kind_mut, iter_mut);
}
