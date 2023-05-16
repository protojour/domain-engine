use crate::{
    kind::{NodeKind, PatternBinding, PropPattern, Variable},
    Lang, Node,
};

pub trait OntosVisitor<L: Lang> {
    fn visit_kind(&mut self, kind: &NodeKind<'_, L>) {
        match kind {
            NodeKind::VariableRef(var) => {
                self.visit_variable(var);
            }
            NodeKind::Int(_) => {}
            NodeKind::Unit => {}
            NodeKind::Call(_proc, args) => {
                for arg in args {
                    self.visit_kind(arg.kind());
                }
            }
            NodeKind::Struct(binder, children) => {
                self.visit_binder(&binder.0);
                for child in children {
                    self.visit_kind(child.kind());
                }
            }
            NodeKind::Prop(struct_var, prop, rel, val) => {
                self.visit_variable(struct_var);
                self.visit_prop(prop);
                self.visit_kind(rel.kind());
                self.visit_kind(val.kind());
            }
            NodeKind::Destruct(arg, children) => {
                self.visit_variable(arg);
                for child in children {
                    self.visit_kind(child.kind());
                }
            }
            NodeKind::MatchProp(struct_var, prop, arms) => {
                self.visit_variable(struct_var);
                self.visit_prop(prop);
                for arm in arms {
                    if let PropPattern::Present(rel, val) = &arm.pattern {
                        self.visit_pattern_binding(rel);
                        self.visit_pattern_binding(val);
                    }
                }
            }
        }
    }
    fn visit_pattern_binding(&mut self, binding: &PatternBinding) {
        match binding {
            PatternBinding::Binder(var) => self.visit_binder(var),
            PatternBinding::Wildcard => {}
        }
    }
    fn visit_prop(&mut self, prop: &str);
    fn visit_binder(&mut self, variable: &Variable);
    fn visit_variable(&mut self, variable: &Variable);
}
