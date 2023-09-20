use ontol_runtime::value::PropertyId;

use crate::{
    arena::Arena, Attribute, Binding, CaptureMatchArm, Iter, Kind, Label, Lang, Node, Nodes,
    Optional, PropPattern, PropVariant, Var,
};

pub trait HirVisitor<'h, 'a: 'h, L: Lang + 'h> {
    fn arena(&self) -> &'h Arena<'a, L>;

    fn visit_node(&mut self, index: usize, node: Node) {
        self.visit_kind(index, self.arena().kind(node));
    }

    #[allow(unused_variables)]
    fn visit_kind(&mut self, index: usize, kind: &Kind<'a, L>) {
        self.traverse_kind(kind);
    }

    #[allow(unused_variables)]
    fn visit_prop(
        &mut self,
        optional: Optional,
        struct_var: Var,
        prop_id: PropertyId,
        variants: &[PropVariant<'a, L>],
    ) {
        self.traverse_prop(struct_var, prop_id, variants);
    }

    #[allow(unused_variables)]
    fn visit_prop_variant(&mut self, index: usize, variant: &PropVariant<'a, L>) {
        self.traverse_prop_variant(variant);
    }

    #[allow(unused_variables)]
    fn visit_seq_prop_element(&mut self, index: usize, element: &(Iter, Attribute<Node>)) {
        self.traverse_seq_prop_element(index, element);
    }

    #[allow(unused_variables)]
    fn visit_prop_match_arm(&mut self, index: usize, match_arm: &(PropPattern<'a, L>, Nodes)) {
        self.traverse_prop_match_arm(match_arm);
    }

    #[allow(unused_variables)]
    fn visit_capture_match_arm(&mut self, index: usize, match_arm: &CaptureMatchArm<'a, L>) {
        self.traverse_capture_match_arm(match_arm);
    }

    #[allow(unused_variables)]
    fn visit_pattern_binding(&mut self, index: usize, binding: &Binding<'a, L>) {
        self.traverse_pattern_binding(binding);
    }

    #[allow(unused_variables)]
    fn visit_property_id(&mut self, prop_id: PropertyId) {}

    #[allow(unused_variables)]
    fn visit_binder(&mut self, var: Var) {}

    #[allow(unused_variables)]
    fn visit_var(&mut self, var: Var) {}

    #[allow(unused_variables)]
    fn visit_label(&mut self, label: Label) {}

    fn traverse_kind(&mut self, kind: &Kind<'a, L>) {
        match kind {
            Kind::Var(var) => {
                self.visit_var(*var);
            }
            Kind::Unit | Kind::I64(_) | Kind::F64(_) | Kind::Text(_) | Kind::Const(_) => {}
            Kind::Call(_proc, params) => {
                for (index, arg) in params.iter().enumerate() {
                    self.visit_node(index, *arg);
                }
            }
            Kind::Map(arg) => {
                self.visit_node(0, *arg);
            }
            Kind::Let(binder, def, body) => {
                self.visit_binder(L::inner(binder).var);
                self.visit_node(0, *def);
                for (index, node) in body.iter().enumerate() {
                    self.visit_node(index + 1, *node);
                }
            }
            Kind::DeclSeq(label, spec) => {
                self.visit_label(*L::inner(label));
                self.visit_node(0, spec.rel);
                self.visit_node(1, spec.val);
            }
            Kind::Struct(binder, _flags, children) => {
                self.visit_binder(L::inner(binder).var);
                for (index, child) in children.iter().enumerate() {
                    self.visit_node(index, *child);
                }
            }
            Kind::Prop(optional, struct_var, prop_id, variants) => {
                self.visit_prop(*optional, *struct_var, *prop_id, variants);
            }
            Kind::MatchProp(struct_var, prop_id, arms) => {
                self.visit_var(*struct_var);
                self.visit_property_id(*prop_id);
                for (index, arm) in arms.iter().enumerate() {
                    self.visit_prop_match_arm(index, arm);
                }
            }
            Kind::Sequence(binder, children) => {
                self.visit_binder(L::inner(binder).var);
                for (index, child) in children.iter().enumerate() {
                    self.visit_node(index, *child);
                }
            }
            Kind::ForEach(seq_var, (rel, val), body) => {
                self.visit_var(*seq_var);
                self.traverse_pattern_binding(rel);
                self.traverse_pattern_binding(val);
                for (index, node) in body.iter().enumerate() {
                    self.visit_node(index, *node);
                }
            }
            Kind::SeqPush(seq_var, attr) => {
                self.visit_var(*seq_var);
                self.visit_node(0, attr.rel);
                self.visit_node(1, attr.val);
            }
            Kind::StringPush(to_var, node) => {
                self.visit_var(*to_var);
                self.visit_node(0, *node);
            }
            Kind::Regex(label, _, capture_groups_list) => {
                if let Some(label) = label {
                    self.visit_label(*L::inner(label));
                }

                for capture_groups in capture_groups_list.iter() {
                    for capture_group in capture_groups.iter() {
                        self.visit_binder(L::inner(&capture_group.binder).var);
                    }
                }
            }
            Kind::MatchRegex(_iter, string_var, _regex_def_id, capture_match_arms) => {
                self.visit_var(*string_var);
                for (index, arm) in capture_match_arms.iter().enumerate() {
                    self.visit_capture_match_arm(index, arm);
                }
            }
        }
    }

    #[allow(unused_variables)]
    fn traverse_prop(
        &mut self,
        struct_var: Var,
        prop_id: PropertyId,
        variants: &[PropVariant<'a, L>],
    ) {
        self.visit_var(struct_var);
        self.visit_property_id(prop_id);
        for (index, variant) in variants.iter().enumerate() {
            self.visit_prop_variant(index, variant);
        }
    }

    fn traverse_prop_variant(&mut self, variant: &PropVariant<'a, L>) {
        match variant {
            PropVariant::Singleton(attr) => {
                self.visit_node(0, attr.rel);
                self.visit_node(1, attr.val);
            }
            PropVariant::Seq(seq_variant) => {
                self.visit_label(*L::inner(&seq_variant.label));
                for (index, element) in seq_variant.elements.iter().enumerate() {
                    self.visit_seq_prop_element(index, element);
                }
            }
        }
    }

    #[allow(unused_variables)]
    fn traverse_seq_prop_element(&mut self, index: usize, element: &(Iter, Attribute<Node>)) {
        self.visit_node(0, element.1.rel);
        self.visit_node(1, element.1.val);
    }

    fn traverse_prop_match_arm(&mut self, (pattern, nodes): &(PropPattern<'a, L>, Nodes)) {
        match pattern {
            PropPattern::Attr(rel, val) => {
                self.visit_pattern_binding(0, rel);
                self.visit_pattern_binding(1, val);
            }
            PropPattern::Seq(val, _) => {
                self.visit_pattern_binding(0, val);
            }
            PropPattern::Absent => {}
        }
        for (index, node) in nodes.iter().enumerate() {
            self.visit_node(index, *node);
        }
    }

    fn traverse_capture_match_arm(&mut self, match_arm: &CaptureMatchArm<'a, L>) {
        for group in match_arm.capture_groups.iter() {
            self.visit_binder(L::inner(&group.binder).var);
        }
        for (index, node) in match_arm.nodes.iter().enumerate() {
            self.visit_node(index, *node);
        }
    }

    fn traverse_pattern_binding(&mut self, binding: &Binding<'a, L>) {
        match binding {
            Binding::Binder(binder) => self.visit_binder(L::inner(binder).var),
            Binding::Wildcard => {}
        }
    }
}
