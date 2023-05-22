use ontol_runtime::value::PropertyId;

use crate::{
    kind::{MatchArm, NodeKind, PatternBinding, PropPattern},
    Lang, Node, Variable,
};

pub trait OntosVisitor<'a, L: Lang + 'a> {
    #[allow(unused_variables)]
    fn visit_node(&mut self, index: usize, node: &mut L::Node<'a>) {
        self.visit_kind(index, node.kind_mut());
    }

    #[allow(unused_variables)]
    fn visit_kind(&mut self, index: usize, kind: &mut NodeKind<'a, L>) {
        self.traverse_kind(kind);
    }

    #[allow(unused_variables)]
    fn visit_match_arm(&mut self, index: usize, match_arm: &mut MatchArm<'a, L>) {
        self.traverse_match_arm(match_arm);
    }

    #[allow(unused_variables)]
    fn visit_pattern_binding(&mut self, index: usize, binding: &mut PatternBinding) {
        self.traverse_pattern_binding(binding);
    }

    #[allow(unused_variables)]
    fn visit_property_id(&mut self, id: &mut PropertyId) {}

    #[allow(unused_variables)]
    fn visit_binder(&mut self, variable: &mut Variable) {}

    #[allow(unused_variables)]
    fn visit_variable(&mut self, variable: &mut Variable) {}

    fn traverse_kind(&mut self, kind: &mut NodeKind<'a, L>) {
        match kind {
            NodeKind::VariableRef(var) => {
                self.visit_variable(var);
            }
            NodeKind::Int(_) => {}
            NodeKind::Unit => {}
            NodeKind::Call(_proc, args) => {
                for (index, arg) in args.iter_mut().enumerate() {
                    self.visit_node(index, arg);
                }
            }
            NodeKind::Map(arg) => {
                self.visit_node(0, arg);
            }
            NodeKind::Let(binder, def, body) => {
                self.visit_binder(&mut binder.0);
                self.visit_node(0, def);
                for (index, node) in body.iter_mut().enumerate() {
                    self.visit_node(index + 1, node);
                }
            }
            NodeKind::Seq(binder, children) => {
                self.visit_binder(&mut binder.0);
                for (index, child) in children.iter_mut().enumerate() {
                    self.visit_node(index, child);
                }
            }
            NodeKind::MapSeq(var, binder, children) => {
                self.visit_variable(var);
                self.visit_binder(&mut binder.0);
                for (index, child) in children.iter_mut().enumerate() {
                    self.visit_node(index, child);
                }
            }
            NodeKind::Struct(binder, children) => {
                self.visit_binder(&mut binder.0);
                for (index, child) in children.iter_mut().enumerate() {
                    self.visit_node(index, child);
                }
            }
            NodeKind::Prop(struct_var, id, variant) => {
                self.visit_variable(struct_var);
                self.visit_property_id(id);
                self.visit_node(0, &mut variant.rel);
                self.visit_node(1, &mut variant.val);
            }
            NodeKind::MatchProp(struct_var, id, arms) => {
                self.visit_variable(struct_var);
                self.visit_property_id(id);
                for (index, arm) in arms.iter_mut().enumerate() {
                    self.visit_match_arm(index, arm);
                }
            }
        }
    }

    fn traverse_match_arm(&mut self, match_arm: &mut MatchArm<'a, L>) {
        if let PropPattern::Present(rel, val) = &mut match_arm.pattern {
            self.visit_pattern_binding(0, rel);
            self.visit_pattern_binding(1, val);
        }
    }

    fn traverse_pattern_binding(&mut self, binding: &mut PatternBinding) {
        match binding {
            PatternBinding::Binder(var) => self.visit_binder(var),
            PatternBinding::Wildcard => {}
        }
    }
}
