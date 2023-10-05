use ontol_runtime::value::PropertyId;

use crate::{
    arena::{Arena, NodeRef},
    Attribute, Binding, CaptureMatchArm, Iter, Kind, Label, Lang, Node, Nodes, Optional,
    PropPattern, PropVariant, Var,
};

pub trait HirVisitor<'h, 'a: 'h, L: Lang + 'h> {
    #[allow(unused_variables)]
    fn visit_node(&mut self, index: usize, node_ref: NodeRef<'h, 'a, L>) {
        self.traverse_node(node_ref);
    }

    #[allow(unused_variables)]
    fn visit_prop(
        &mut self,
        optional: Optional,
        struct_var: Var,
        prop_id: PropertyId,
        variants: &[PropVariant<'a, L>],
        arena: &'h Arena<'a, L>,
    ) {
        self.traverse_prop(struct_var, prop_id, variants, arena);
    }

    #[allow(unused_variables)]
    fn visit_prop_variant(
        &mut self,
        index: usize,
        variant: &PropVariant<'a, L>,
        arena: &'h Arena<'a, L>,
    ) {
        self.traverse_prop_variant(variant, arena);
    }

    #[allow(unused_variables)]
    fn visit_seq_prop_element(
        &mut self,
        index: usize,
        element: &(Iter, Attribute<Node>),
        arena: &'h Arena<'a, L>,
    ) {
        self.traverse_seq_prop_element(index, element, arena);
    }

    #[allow(unused_variables)]
    fn visit_prop_match_arm(
        &mut self,
        index: usize,
        match_arm: &(PropPattern<'a, L>, Nodes),
        arena: &'h Arena<'a, L>,
    ) {
        self.traverse_prop_match_arm(match_arm, arena);
    }

    #[allow(unused_variables)]
    fn visit_capture_match_arm(
        &mut self,
        index: usize,
        match_arm: &CaptureMatchArm<'a, L>,
        arena: &'h Arena<'a, L>,
    ) {
        self.traverse_capture_match_arm(match_arm, arena);
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

    fn traverse_node(&mut self, node_ref: NodeRef<'h, 'a, L>) {
        let arena = node_ref.arena;
        match node_ref.kind() {
            Kind::Var(var) => {
                self.visit_var(*var);
            }
            Kind::Unit | Kind::I64(_) | Kind::F64(_) | Kind::Text(_) | Kind::Const(_) => {}
            Kind::Call(_proc, params) => {
                for (index, arg) in arena.refs(params).enumerate() {
                    self.visit_node(index, arg);
                }
            }
            Kind::Map(arg) => {
                self.visit_node(0, arena.node_ref(*arg));
            }
            Kind::Let(binder, def, body) => {
                self.visit_binder(L::as_hir(binder).var);
                self.visit_node(0, arena.node_ref(*def));
                for (index, node_ref) in arena.refs(body).enumerate() {
                    self.visit_node(index + 1, node_ref);
                }
            }
            Kind::DeclSeq(label, attr) => {
                self.visit_label(*L::as_hir(label));
                self.visit_node(0, arena.node_ref(attr.rel));
                self.visit_node(1, arena.node_ref(attr.val));
            }
            Kind::Struct(binder, _flags, children) => {
                self.visit_binder(L::as_hir(binder).var);
                for (index, child) in arena.refs(children).enumerate() {
                    self.visit_node(index, child);
                }
            }
            Kind::Prop(optional, struct_var, prop_id, variants) => {
                self.visit_prop(*optional, *struct_var, *prop_id, variants, arena);
            }
            Kind::MatchProp(struct_var, prop_id, arms) => {
                self.visit_var(*struct_var);
                self.visit_property_id(*prop_id);
                for (index, arm) in arms.iter().enumerate() {
                    self.visit_prop_match_arm(index, arm, arena);
                }
            }
            Kind::Sequence(binder, children) => {
                self.visit_binder(L::as_hir(binder).var);
                for (index, child) in arena.refs(children).enumerate() {
                    self.visit_node(index, child);
                }
            }
            Kind::ForEach(seq_var, (rel, val), body) => {
                self.visit_var(*seq_var);
                self.traverse_pattern_binding(rel);
                self.traverse_pattern_binding(val);
                for (index, node_ref) in arena.refs(body).enumerate() {
                    self.visit_node(index, node_ref);
                }
            }
            Kind::SeqPush(seq_var, attr) => {
                self.visit_var(*seq_var);
                self.visit_node(0, arena.node_ref(attr.rel));
                self.visit_node(1, arena.node_ref(attr.val));
            }
            Kind::StringPush(to_var, node) => {
                self.visit_var(*to_var);
                self.visit_node(0, arena.node_ref(*node));
            }
            Kind::Regex(label, _, capture_groups_list) => {
                if let Some(label) = label {
                    self.visit_label(*L::as_hir(label));
                }

                for capture_groups in capture_groups_list.iter() {
                    for capture_group in capture_groups.iter() {
                        self.visit_binder(L::as_hir(&capture_group.binder).var);
                    }
                }
            }
            Kind::MatchRegex(_iter, string_var, _regex_def_id, capture_match_arms) => {
                self.visit_var(*string_var);
                for (index, arm) in capture_match_arms.iter().enumerate() {
                    self.visit_capture_match_arm(index, arm, arena);
                }
            }
            Kind::PushCondClause(var, _) => {
                self.visit_var(*var);
            }
        }
    }

    #[allow(unused_variables)]
    fn traverse_prop(
        &mut self,
        struct_var: Var,
        prop_id: PropertyId,
        variants: &[PropVariant<'a, L>],
        arena: &'h Arena<'a, L>,
    ) {
        self.visit_var(struct_var);
        self.visit_property_id(prop_id);
        for (index, variant) in variants.iter().enumerate() {
            self.visit_prop_variant(index, variant, arena);
        }
    }

    fn traverse_prop_variant(&mut self, variant: &PropVariant<'a, L>, arena: &'h Arena<'a, L>) {
        match variant {
            PropVariant::Singleton(attr) => {
                self.visit_node(0, arena.node_ref(attr.rel));
                self.visit_node(1, arena.node_ref(attr.val));
            }
            PropVariant::Seq(seq_variant) => {
                self.visit_label(*L::as_hir(&seq_variant.label));
                for (index, element) in seq_variant.elements.iter().enumerate() {
                    self.visit_seq_prop_element(index, element, arena);
                }
            }
        }
    }

    #[allow(unused_variables)]
    fn traverse_seq_prop_element(
        &mut self,
        index: usize,
        element: &(Iter, Attribute<Node>),
        arena: &'h Arena<'a, L>,
    ) {
        self.visit_node(0, arena.node_ref(element.1.rel));
        self.visit_node(1, arena.node_ref(element.1.val));
    }

    fn traverse_prop_match_arm(
        &mut self,
        (pattern, nodes): &(PropPattern<'a, L>, Nodes),
        arena: &'h Arena<'a, L>,
    ) {
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
            self.visit_node(index, arena.node_ref(*node));
        }
    }

    fn traverse_capture_match_arm(
        &mut self,
        match_arm: &CaptureMatchArm<'a, L>,
        arena: &'h Arena<'a, L>,
    ) {
        for group in match_arm.capture_groups.iter() {
            self.visit_binder(L::as_hir(&group.binder).var);
        }
        for (index, node) in match_arm.nodes.iter().enumerate() {
            self.visit_node(index, arena.node_ref(*node));
        }
    }

    fn traverse_pattern_binding(&mut self, binding: &Binding<'a, L>) {
        match binding {
            Binding::Binder(binder) => self.visit_binder(L::as_hir(binder).var),
            Binding::Wildcard => {}
        }
    }
}
