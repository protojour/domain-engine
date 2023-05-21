use crate::{
    kind::{MatchArm, NodeKind, PatternBinding, PropPattern},
    Lang, Node, Variable,
};

pub trait OntosVisitor<L: Lang> {
    #[allow(unused_variables)]
    fn visit_kind(&mut self, index: usize, kind: &NodeKind<'_, L>) {
        self.traverse_kind(kind);
    }

    #[allow(unused_variables)]
    fn visit_match_arm(&mut self, index: usize, match_arm: &MatchArm<'_, L>) {
        self.traverse_match_arm(match_arm);
    }

    #[allow(unused_variables)]
    fn visit_pattern_binding(&mut self, index: usize, binding: &PatternBinding) {
        self.traverse_pattern_binding(binding);
    }

    #[allow(unused_variables)]
    fn visit_prop(&mut self, prop: &str) {}

    #[allow(unused_variables)]
    fn visit_binder(&mut self, variable: &Variable) {}

    #[allow(unused_variables)]
    fn visit_variable(&mut self, variable: &Variable) {}

    fn traverse_kind(&mut self, kind: &NodeKind<'_, L>) {
        match kind {
            NodeKind::VariableRef(var) => {
                self.visit_variable(var);
            }
            NodeKind::Int(_) => {}
            NodeKind::Unit => {}
            NodeKind::Call(_proc, args) => {
                for (index, arg) in args.iter().enumerate() {
                    self.visit_kind(index, arg.kind());
                }
            }
            NodeKind::Let(binder, def, body) => {
                self.visit_binder(&binder.0);
                self.visit_kind(0, def.kind());
                for (index, node) in body.iter().enumerate() {
                    self.visit_kind(index + 1, node.kind());
                }
            }
            NodeKind::Seq(binder, children) => {
                self.visit_binder(&binder.0);
                for (index, child) in children.iter().enumerate() {
                    self.visit_kind(index, child.kind());
                }
            }
            NodeKind::MapSeq(var, binder, children) => {
                self.visit_variable(var);
                self.visit_binder(&binder.0);
                for (index, child) in children.iter().enumerate() {
                    self.visit_kind(index, child.kind());
                }
            }
            NodeKind::Struct(binder, children) => {
                self.visit_binder(&binder.0);
                for (index, child) in children.iter().enumerate() {
                    self.visit_kind(index, child.kind());
                }
            }
            NodeKind::Prop(struct_var, prop, variant) => {
                self.visit_variable(struct_var);
                self.visit_prop(prop);
                self.visit_kind(0, variant.rel.kind());
                self.visit_kind(1, variant.val.kind());
            }
            NodeKind::MatchProp(struct_var, prop, arms) => {
                self.visit_variable(struct_var);
                self.visit_prop(prop);
                for (index, arm) in arms.iter().enumerate() {
                    self.visit_match_arm(index, arm);
                }
            }
        }
    }

    fn traverse_match_arm(&mut self, match_arm: &MatchArm<'_, L>) {
        if let PropPattern::Present(rel, val) = &match_arm.pattern {
            self.visit_pattern_binding(0, rel);
            self.visit_pattern_binding(1, val);
        }
    }

    fn traverse_pattern_binding(&mut self, binding: &PatternBinding) {
        match binding {
            PatternBinding::Binder(var) => self.visit_binder(var),
            PatternBinding::Wildcard => {}
        }
    }
}
