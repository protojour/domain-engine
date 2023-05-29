use ontol_runtime::value::PropertyId;

use crate::{
    kind::{Dimension, IterBinder, MatchArm, NodeKind, PatternBinding, PropPattern, PropVariant},
    Label, Lang, Node, Variable,
};

macro_rules! visitor_trait {
    ($mut:tt, $ident:ident, $kind:ident, $iter:ident) => {
        pub trait $ident<'a, L: Lang + 'a> {
            fn visit_node(&mut self, index: usize, node: param!($mut L::Node<'a>)) {
                self.visit_kind(index, node.$kind());
            }

            #[allow(unused_variables)]
            fn visit_kind(&mut self, index: usize, kind: param!($mut NodeKind<'a, L>)) {
                self.traverse_kind(kind);
            }

            #[allow(unused_variables)]
            fn visit_prop_variant(&mut self, index: usize, variant: param!($mut PropVariant<'a, L>)) {
                self.traverse_prop_variant(variant);
            }

            #[allow(unused_variables)]
            fn visit_match_arm(&mut self, index: usize, match_arm: param!($mut MatchArm<'a, L>)) {
                self.traverse_match_arm(match_arm);
            }

            #[allow(unused_variables)]
            fn visit_pattern_binding(&mut self, index: usize, binding: param!($mut PatternBinding)) {
                self.traverse_pattern_binding(binding);
            }

            #[allow(unused_variables)]
            fn visit_property_id(&mut self, id: param!($mut PropertyId)) {}

            #[allow(unused_variables)]
            fn visit_binder(&mut self, variable: param!($mut Variable)) {}

            #[allow(unused_variables)]
            fn visit_iter_binder(&mut self, binder: param!($mut IterBinder)) {
                self.traverse_iter_binder(binder);
            }

            #[allow(unused_variables)]
            fn visit_variable(&mut self, variable: param!($mut Variable)) {}

            #[allow(unused_variables)]
            fn visit_label(&mut self, label: param!($mut Label)) {}

            fn traverse_kind(&mut self, kind: param!($mut NodeKind<'a, L>)) {
                match kind {
                    NodeKind::VariableRef(var) => {
                        self.visit_variable(var);
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
                        self.visit_binder(borrow!($mut binder.0));
                        self.visit_node(0, def);
                        for (index, node) in body.$iter().enumerate() {
                            self.visit_node(index + 1, node);
                        }
                    }
                    NodeKind::Seq(label, spec) => {
                        self.visit_label(label);
                        self.visit_node(0, borrow!($mut spec.rel));
                        self.visit_node(1, borrow!($mut spec.val));
                    }
                    NodeKind::Struct(binder, children) => {
                        self.visit_binder(borrow!($mut binder.0));
                        for (index, child) in children.$iter().enumerate() {
                            self.visit_node(index, child);
                        }
                    }
                    NodeKind::Prop(struct_var, id, variants) => {
                        self.visit_variable(struct_var);
                        self.visit_property_id(id);
                        for (index, variant) in variants.$iter().enumerate() {
                            self.visit_prop_variant(index, variant);
                        }
                    }
                    NodeKind::MatchProp(struct_var, id, arms) => {
                        self.visit_variable(struct_var);
                        self.visit_property_id(id);
                        for (index, arm) in arms.$iter().enumerate() {
                            self.visit_match_arm(index, arm);
                        }
                    }
                    NodeKind::Gen(seq_var, binder, children) => {
                        self.visit_variable(seq_var);
                        self.visit_iter_binder(binder);
                        for (index, child) in children.$iter().enumerate() {
                            self.visit_node(index, child);
                        }
                    }
                    NodeKind::Iter(seq_var, binder, children) => {
                        self.visit_variable(seq_var);
                        self.visit_iter_binder(binder);
                        for (index, child) in children.$iter().enumerate() {
                            self.visit_node(index, child);
                        }
                    }
                    NodeKind::Push(seq_var, attr) => {
                        self.visit_variable(seq_var);
                        self.visit_node(0, borrow!($mut attr.rel));
                        self.visit_node(1, borrow!($mut attr.val));
                    }
                }
            }

            fn traverse_prop_variant(&mut self, variant: param!($mut PropVariant<'a, L>)) {
                match variant {
                    PropVariant::Present { dimension, attr } => {
                        if let Dimension::Seq(label) = dimension {
                            self.visit_label(label);
                        }
                        self.visit_node(0, borrow!($mut attr.rel));
                        self.visit_node(1, borrow!($mut attr.val));
                    }
                    PropVariant::Absent => {}
                }
            }

            fn traverse_match_arm(&mut self, match_arm: param!($mut MatchArm<'a, L>)) {
                match borrow!($mut match_arm.pattern) {
                    PropPattern::Attr(rel, val) | PropPattern::SeqAttr(rel, val) => {
                        self.visit_pattern_binding(0, rel);
                        self.visit_pattern_binding(1, val);
                    }
                    PropPattern::Seq(val) => {
                        self.visit_pattern_binding(0, val);
                    }
                    PropPattern::Absent => {}
                }
            }

            fn traverse_pattern_binding(&mut self, binding: param!($mut PatternBinding)) {
                match binding {
                    PatternBinding::Binder(var) => self.visit_binder(var),
                    PatternBinding::Wildcard => {}
                }
            }

            fn traverse_iter_binder(&mut self, binder: param!($mut IterBinder)) {
                self.traverse_pattern_binding(borrow!($mut binder.seq));
                self.traverse_pattern_binding(borrow!($mut binder.rel));
                self.traverse_pattern_binding(borrow!($mut binder.val));
            }
        }
    };
}

macro_rules! param {
    (ref $ty:ty) => {
        &$ty
    };
    (mut $ty:ty) => {
        &mut $ty
    };
}

macro_rules! borrow {
    (ref $e:expr) => {
        &$e
    };
    (mut $e:expr) => {
        &mut $e
    };
}

visitor_trait!(ref, HirVisitor, kind, iter);
visitor_trait!(mut, HirMutVisitor, kind_mut, iter_mut);
