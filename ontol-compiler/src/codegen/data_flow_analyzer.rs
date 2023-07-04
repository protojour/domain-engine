//! High-level data flow of struct properties

use fnv::{FnvHashMap, FnvHashSet};
use ontol_hir::Node;
use ontol_runtime::{env::DataFlow, value::PropertyId};

use crate::typed_hir::{HirFunc, TypedHirNode};

pub struct DataFlowAnalyzer {}

impl DataFlowAnalyzer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn analyze(&mut self, hir_func: &HirFunc) -> Option<DataFlow> {
        match &hir_func.body.kind() {
            ontol_hir::Kind::Struct(_struct_binder, nodes) => {
                let mut struct_analyzer = StructAnalyzer {
                    input_var: hir_func.arg.var,
                    scope: Default::default(),
                };
                for node in nodes {
                    struct_analyzer.analyze_node(node);
                }

                None
            }
            _ => None,
        }
    }
}

struct StructAnalyzer {
    input_var: ontol_hir::Var,
    scope: FnvHashMap<ontol_hir::Var, FnvHashSet<PropertyId>>,
}

impl StructAnalyzer {
    #[allow(unused)]
    fn analyze_node(&mut self, node: &TypedHirNode) {
        match node.kind() {
            ontol_hir::Kind::Var(var) => {}
            ontol_hir::Kind::Unit => {}
            ontol_hir::Kind::Int(i64) => {}
            ontol_hir::Kind::String(string) => {}
            ontol_hir::Kind::Let(binder, definition, body) => {}
            ontol_hir::Kind::Call(proc, params) => {}
            ontol_hir::Kind::Map(node) => {}
            ontol_hir::Kind::Seq(label, attr) => {}
            ontol_hir::Kind::Struct(binder, body) => {}
            ontol_hir::Kind::Prop(optional, var, property_id, variants) => {}
            ontol_hir::Kind::MatchProp(struct_var, property_id, arms) => {
                let deps: FnvHashSet<PropertyId> = if *struct_var == self.input_var {
                    FnvHashSet::from_iter([*property_id])
                } else {
                    todo!()
                };

                for arm in arms {
                    match &arm.pattern {
                        ontol_hir::PropPattern::Attr(rel, val) => {
                            self.scope_binding(rel, Some(&deps));
                            self.scope_binding(val, Some(&deps));
                        }
                        ontol_hir::PropPattern::Seq(binding) => {
                            self.scope_binding(binding, Some(&deps));
                        }
                        ontol_hir::PropPattern::Absent => {}
                    }

                    for node in &arm.nodes {
                        self.analyze_node(node);
                    }

                    match &arm.pattern {
                        ontol_hir::PropPattern::Attr(rel, val) => {
                            self.scope_binding(rel, None);
                            self.scope_binding(val, None);
                        }
                        ontol_hir::PropPattern::Seq(binding) => {
                            self.scope_binding(binding, None);
                        }
                        ontol_hir::PropPattern::Absent => {}
                    }
                }
            }
            ontol_hir::Kind::Gen(var, iter_binder, nodes) => {}
            ontol_hir::Kind::Iter(var, iter_binder, nodes) => {}
            ontol_hir::Kind::Push(var, atribute) => {}
        }
    }

    fn scope_binding(
        &mut self,
        binding: &ontol_hir::Binding,
        deps: Option<&FnvHashSet<PropertyId>>,
    ) {
        if let ontol_hir::Binding::Binder(var) = binding {
            if let Some(deps) = deps {
                self.scope.insert(*var, deps.clone());
            } else {
                self.scope.remove(var);
            }
        }
    }
}
