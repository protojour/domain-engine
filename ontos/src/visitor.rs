use super::ast::{Hir2Ast, Hir2AstPatternBinding, Hir2AstPropPattern, Hir2AstVariable};

pub trait Hir2AstVisitor {
    fn visit_ast(&mut self, ast: &Hir2Ast) {
        match ast {
            Hir2Ast::VariableRef(var) => {
                self.visit_variable(var);
            }
            Hir2Ast::Int(_) => {}
            Hir2Ast::Unit => {}
            Hir2Ast::Call(_proc, args) => {
                for arg in args {
                    self.visit_ast(arg);
                }
            }
            Hir2Ast::Construct(children) => {
                for child in children {
                    self.visit_ast(child);
                }
            }
            Hir2Ast::ConstructProp(prop, rel, val) => {
                self.visit_prop(prop);
                self.visit_ast(rel);
                self.visit_ast(val);
            }
            Hir2Ast::Destruct(arg, children) => {
                self.visit_variable(arg);
                for child in children {
                    self.visit_ast(child);
                }
            }
            Hir2Ast::DestructProp(prop, arms) => {
                self.visit_prop(prop);
                for arm in arms {
                    if let Hir2AstPropPattern::Present(rel, val) = &arm.pattern {
                        self.visit_pattern_binding(rel);
                        self.visit_pattern_binding(val);
                    }
                }
            }
        }
    }
    fn visit_pattern_binding(&mut self, binding: &Hir2AstPatternBinding) {
        match binding {
            Hir2AstPatternBinding::Binder(var) => self.visit_binder(var),
            Hir2AstPatternBinding::Wildcard => {}
        }
    }
    fn visit_prop(&mut self, prop: &str);
    fn visit_variable(&mut self, variable: &Hir2AstVariable);
    fn visit_binder(&mut self, variable: &Hir2AstVariable);
}
