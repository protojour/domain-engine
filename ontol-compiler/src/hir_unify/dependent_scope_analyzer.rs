use std::collections::HashMap;

use ontol_hir::GetKind;
use smallvec::SmallVec;
use tracing::debug;

use crate::{
    hir_unify::UnifierError,
    typed_hir::{TypedHir, TypedHirNode},
};

use super::{UnifierResult, VarSet};

pub type Path = SmallVec<[u16; 32]>;

#[derive(Debug)]
pub struct PropAnalysis {
    pub defined_var: Option<ontol_hir::Var>,
    pub dependencies: VarSet,
}

#[derive(Default)]
pub struct DepScopeAnalyzer {
    current_path: Path,
    stack: Vec<StackAtom>,
    prop_variables: VarSet,
    bound_variables: VarSet,
    prop_variant_deps: HashMap<Path, VarSet>,
}

enum StackAtom {
    Prop,
    Node,
}

impl DepScopeAnalyzer {
    pub fn prop_analysis(self) -> UnifierResult<HashMap<Path, PropAnalysis>> {
        let prop_variant_deps = self.prop_variant_deps;

        debug!("prop variant deps: {prop_variant_deps:?}");

        struct Pending {
            var_set: VarSet,
        }

        let mut pending_map: HashMap<Path, Pending> = HashMap::default();

        for (path, var_set) in prop_variant_deps {
            pending_map.insert(path, Pending { var_set });
        }

        let mut in_scope = VarSet::default();
        let mut output: HashMap<Path, PropAnalysis> = HashMap::default();

        while !pending_map.is_empty() {
            let current_pending_len = pending_map.len();
            let mut next_pending_map: HashMap<Path, Pending> = HashMap::default();

            for (path, pending) in pending_map {
                let mut difference = pending.var_set.0.difference(&in_scope.0);

                if let Some(next_unscoped) = difference.next() {
                    if difference.next().is_some() {
                        // more than one unscoped variable, put this one back into pending for later processing
                        next_pending_map.insert(path, pending);
                    } else {
                        let defined_var = ontol_hir::Var(next_unscoped.try_into().unwrap());
                        in_scope.0.insert(next_unscoped);
                        let mut dependencies = pending.var_set;
                        dependencies.0.remove(next_unscoped);
                        output.insert(
                            path,
                            PropAnalysis {
                                defined_var: Some(defined_var),
                                dependencies,
                            },
                        );
                    }
                } else {
                    output.insert(
                        path,
                        PropAnalysis {
                            defined_var: None,
                            dependencies: pending.var_set,
                        },
                    );
                }
            }

            if next_pending_map.len() == current_pending_len {
                // stagnation - no progress this round :(

                let mut unresolved_var_set = VarSet::default();
                for (_, pending) in next_pending_map {
                    for var in &pending.var_set {
                        if !in_scope.0.contains(var.0 as usize) {
                            unresolved_var_set.0.insert(var.0 as usize);
                        }
                    }
                }

                return Err(UnifierError::NonUniqueVariableDatapoints(
                    unresolved_var_set,
                ));
            }

            pending_map = next_pending_map;
        }

        Ok(output)
    }

    fn enter_child(&mut self, index: usize, func: impl FnOnce(&mut Self)) {
        self.current_path.push(index.try_into().unwrap());
        func(self);
        self.current_path.pop();
    }
}

impl<'s, 'm: 's> ontol_hir::visitor::HirVisitor<'s, 'm, TypedHir> for DepScopeAnalyzer {
    fn visit_node(&mut self, index: usize, node: &'s TypedHirNode<'m>) {
        self.enter_child(index, |zelf| {
            if let Some(StackAtom::Prop) = zelf.stack.last() {
                let mut prop_variables = VarSet::default();
                std::mem::swap(&mut zelf.prop_variables, &mut prop_variables);

                zelf.stack.push(StackAtom::Node);
                zelf.visit_kind(index, node.kind());
                zelf.stack.pop();

                if !zelf.prop_variables.0.is_empty() {
                    zelf.prop_variant_deps
                        .insert(zelf.current_path.clone(), zelf.prop_variables.clone());
                }

                std::mem::swap(&mut zelf.prop_variables, &mut prop_variables);
            } else {
                zelf.stack.push(StackAtom::Node);
                zelf.visit_kind(index, node.kind());
                zelf.stack.pop();
            }
        });
    }

    fn visit_prop_variant(
        &mut self,
        index: usize,
        variant: &'s ontol_hir::PropVariant<'m, TypedHir>,
    ) {
        self.stack.push(StackAtom::Prop);

        self.enter_child(index, |zelf| {
            let ontol_hir::PropVariant { dimension, .. } = variant;
            match dimension {
                ontol_hir::Dimension::Singular => {
                    zelf.traverse_prop_variant(variant);
                }
                ontol_hir::Dimension::Seq(_) => {}
            }
        });

        self.stack.pop();
    }

    fn visit_var(&mut self, var: &ontol_hir::Var) {
        if !self.bound_variables.0.contains(var.0 as usize) {
            self.prop_variables.0.insert(var.0 as usize);
        }
    }

    fn visit_binder(&mut self, var: &ontol_hir::Var) {
        self.bound_variables.0.insert(var.0 as usize);
    }

    fn visit_label(&mut self, _label: &ontol_hir::Label) {}
}
