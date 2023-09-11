use ontol_runtime::value::PropertyId;

use crate::*;

macro_rules! visitor_trait_methods {
    ($ref:tt, $kind:ident, $var:ident, $label:ident, $iter:ident) => {
        fn visit_node(&mut self, index: usize, node: arg!($ref L::Node<'l>)) {
            self.visit_kind(index, node.$kind());
        }

        #[allow(unused_variables)]
        fn visit_kind(&mut self, index: usize, kind: arg!($ref Kind<'l, L>)) {
            self.traverse_kind(kind);
        }

        #[allow(unused_variables)]
        fn visit_prop(
            &mut self,
            optional: arg!($ref Optional),
            struct_var: arg!($ref Var),
            id: arg!($ref PropertyId),
            variants: arg!($ref [PropVariant<'l, L>])
        ) {
            self.traverse_prop(struct_var, id, variants);
        }

        #[allow(unused_variables)]
        fn visit_prop_variant(&mut self, index: usize, variant: arg!($ref PropVariant<'l, L>)) {
            self.traverse_prop_variant(variant);
        }

        #[allow(unused_variables)]
        fn visit_seq_prop_element(&mut self, index: usize, element: arg!($ref SeqPropertyElement<'l, L>)) {
            self.traverse_seq_prop_element(index, element);
        }

        #[allow(unused_variables)]
        fn visit_prop_match_arm(&mut self, index: usize, match_arm: arg!($ref PropMatchArm<'l, L>)) {
            self.traverse_prop_match_arm(match_arm);
        }

        #[allow(unused_variables)]
        fn visit_capture_match_arm(&mut self, index: usize, match_arm: arg!($ref CaptureMatchArm<'l, L>)) {
            self.traverse_capture_match_arm(match_arm);
        }

        #[allow(unused_variables)]
        fn visit_pattern_binding(&mut self, index: usize, binding: arg!($ref Binding<'l, L>)) {
            self.traverse_pattern_binding(binding);
        }

        #[allow(unused_variables)]
        fn visit_property_id(&mut self, id: arg!($ref PropertyId)) {}

        #[allow(unused_variables)]
        fn visit_binder(&mut self, var: arg!($ref Var)) {}

        #[allow(unused_variables)]
        fn visit_var(&mut self, var: arg!($ref Var)) {}

        #[allow(unused_variables)]
        fn visit_label(&mut self, label: arg!($ref Label)) {}

        fn traverse_kind(&mut self, kind: arg!($ref Kind<'l, L>)) {
            match kind {
                Kind::Var(var) => {
                    self.visit_var(var);
                }
                Kind::Unit | Kind::I64(_) | Kind::F64(_) | Kind::String(_) | Kind::Const(_) => {}
                Kind::Call(_proc, params) => {
                    for (index, arg) in params.$iter().enumerate() {
                        self.visit_node(index, arg);
                    }
                }
                Kind::Map(arg) => {
                    self.visit_node(0, arg);
                }
                Kind::Let(binder, def, body) => {
                    self.visit_binder(binder.$var());
                    self.visit_node(0, def);
                    for (index, node) in body.$iter().enumerate() {
                        self.visit_node(index + 1, node);
                    }
                }
                Kind::DeclSeq(label, spec) => {
                    self.visit_label(label.$label());
                    self.visit_node(0, borrow!($ref spec.rel));
                    self.visit_node(1, borrow!($ref spec.val));
                }
                Kind::Struct(binder, _flags, children) => {
                    self.visit_binder(binder.$var());
                    for (index, child) in children.$iter().enumerate() {
                        self.visit_node(index, child);
                    }
                }
                Kind::Prop(optional, struct_var, prop_id, variants) => {
                    self.visit_prop(optional, struct_var, prop_id, variants);
                }
                Kind::MatchProp(struct_var, prop_id, arms) => {
                    self.visit_var(struct_var);
                    self.visit_property_id(prop_id);
                    for (index, arm) in arms.$iter().enumerate() {
                        self.visit_prop_match_arm(index, arm);
                    }
                }
                Kind::Sequence(binder, children) => {
                    self.visit_binder(binder.$var());
                    for (index, child) in children.$iter().enumerate() {
                        self.visit_node(index, child);
                    }
                }
                Kind::ForEach(seq_var, (rel, val), children) => {
                    self.visit_var(seq_var);
                    self.traverse_pattern_binding(rel);
                    self.traverse_pattern_binding(val);
                    for (index, child) in children.$iter().enumerate() {
                        self.visit_node(index, child);
                    }
                }
                Kind::SeqPush(seq_var, attr) => {
                    self.visit_var(seq_var);
                    self.visit_node(0, borrow!($ref attr.rel));
                    self.visit_node(1, borrow!($ref attr.val));
                }
                Kind::StringPush(to_var, node) => {
                    self.visit_var(to_var);
                    self.visit_node(0, node);
                }
                Kind::Regex(label, _, capture_groups_list) => {
                    if let Some(label) = label {
                        self.visit_label(label.$label());
                    }

                    for capture_groups in capture_groups_list.$iter() {
                        for capture_group in capture_groups.$iter() {
                            self.visit_binder(capture_group.binder.$var());
                        }
                    }
                }
                Kind::MatchRegex(string_var, _regex_def_id, capture_match_arms) => {
                    self.visit_var(string_var);
                    for (index, arm) in capture_match_arms.$iter().enumerate() {
                        self.visit_capture_match_arm(index, arm);
                    }
                }
            }
        }

        #[allow(unused_variables)]
        fn traverse_prop(
            &mut self,
            struct_var: arg!($ref Var),
            id: arg!($ref PropertyId),
            variants: arg!($ref [PropVariant<'l, L>]),
        ) {
            self.visit_var(struct_var);
            self.visit_property_id(id);
            for (index, variant) in variants.$iter().enumerate() {
                self.visit_prop_variant(index, variant);
            }
        }

        fn traverse_prop_variant(&mut self, variant: arg!($ref PropVariant<'l, L>)) {
            match variant {
                PropVariant::Singleton(attr) => {
                    self.visit_node(0, borrow!($ref attr.rel));
                    self.visit_node(1, borrow!($ref attr.val));
                }
                PropVariant::Seq(seq_variant) => {
                    self.visit_label(seq_variant.label.$label());
                    for (index, element) in seq_variant.elements.$iter().enumerate() {
                        self.visit_seq_prop_element(index, element);
                    }
                }
            }
        }

        #[allow(unused_variables)]
        fn traverse_seq_prop_element(&mut self, index: usize, element: arg!($ref SeqPropertyElement<'l, L>)) {
            self.visit_node(0, borrow!($ref element.attribute.rel));
            self.visit_node(1, borrow!($ref element.attribute.val));
        }

        fn traverse_prop_match_arm(&mut self, match_arm: arg!($ref PropMatchArm<'l, L>)) {
            match borrow!($ref match_arm.pattern) {
                PropPattern::Attr(rel, val) => {
                    self.visit_pattern_binding(0, rel);
                    self.visit_pattern_binding(1, val);
                }
                PropPattern::Seq(val, _) => {
                    self.visit_pattern_binding(0, val);
                }
                PropPattern::Absent => {}
            }
            for (index, child) in match_arm.nodes.$iter().enumerate() {
                self.visit_node(index, child);
            }
        }

        fn traverse_capture_match_arm(&mut self, match_arm: arg!($ref CaptureMatchArm<'l, L>)) {
            for group in match_arm.capture_groups.$iter() {
                self.visit_binder(group.binder.$var());
            }
            for (index, child) in match_arm.nodes.$iter().enumerate() {
                self.visit_node(index, child);
            }
        }

        fn traverse_pattern_binding(&mut self, binding: arg!($ref Binding<'l, L>)) {
            match binding {
                Binding::Binder(binder) => self.visit_binder(binder.$var()),
                Binding::Wildcard => {}
            }
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
    visitor_trait_methods!((&'p), kind, var, label, iter);
}

pub trait HirMutVisitor<'l, L: Lang + 'l> {
    visitor_trait_methods!((&mut), kind_mut, var_mut, label_mut, iter_mut);
}
