use crate::typed_hir::{TypedBinder, TypedHir};

use super::{
    expr,
    flat_scope::{self, ScopeVar},
    VarSet,
};

#[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct ExprIdx(pub usize);

pub(super) struct Table<'m> {
    table: Vec<ScopeMap<'m>>,
    pub const_expr: Option<expr::Expr<'m>>,
}

#[derive(Clone, Copy, Debug)]
pub enum ExprSelector {
    Struct(ontol_hir::Var),
}

impl<'m> Table<'m> {
    pub fn new(flat_scope: flat_scope::FlatScope<'m>) -> Self {
        Self {
            table: flat_scope
                .scope_nodes
                .into_iter()
                .map(|scope_node| ScopeMap {
                    scope: scope_node,
                    assignments: vec![],
                })
                .collect(),
            const_expr: None,
        }
    }

    pub fn scope_map_mut(&mut self, index: usize) -> &mut ScopeMap<'m> {
        &mut self.table[index]
    }

    pub fn table_mut(&mut self) -> &mut [ScopeMap<'m>] {
        &mut self.table
    }

    pub fn find_assignment_slot(
        &mut self,
        free_vars: &VarSet,
        is_seq: bool,
    ) -> Option<AssignmentSlot<'_, 'm>> {
        let mut tmp_candidate: Option<usize> = None;
        let mut observed_variables = VarSet::default();

        // Note: Reverse search.
        for (index, scope_map) in self.table.iter().enumerate().rev() {
            let scope_pub_vars = &scope_map.scope.meta().pub_vars;

            // quick early check
            if scope_pub_vars.0.is_empty() {
                continue;
            }

            // only consider proper scope-introducing nodes
            match scope_map.scope.kind() {
                flat_scope::Kind::PropVariant(..) => {}
                flat_scope::Kind::SeqPropVariant(..) => {
                    if !is_seq {
                        continue;
                    }
                }
                _ => continue,
            }

            // Only consider candidates that introduces the scope
            if scope_pub_vars.0.intersection(&free_vars.0).next().is_none() {
                continue;
            }

            // If the candidate is only introducing variables that we've
            // already seen, this is not specific enough. The search is over..
            if !observed_variables.0.is_empty()
                && scope_pub_vars.0.is_superset(&observed_variables.0)
            {
                break;
            }

            observed_variables.union_with(scope_pub_vars);

            // Note that we choose the _last_ (in iteration order, really the _first_) candidate
            // that introduces at least one variable in free_vars.
            tmp_candidate = Some(index);
        }

        if let Some(index) = tmp_candidate {
            let scope_map = self.scope_map_mut(index);
            let extra_deps = VarSet(
                free_vars
                    .0
                    .difference(&scope_map.scope.1.pub_vars.0)
                    .collect(),
            );

            Some(AssignmentSlot {
                scope_map,
                lateral_deps: extra_deps,
            })
        } else {
            // instead of crashing, we should just add this at index 0 ("const scope")
            // If there's a real scoping error, this will show up as "unbound variable"
            // errors during codegen instead.
            None
            // AssignmentSlot {
            //     scope_map: self.scope_map_mut(0),
            //     lateral_deps: Default::default(),
            // }
        }
    }

    pub fn find_scope_map_by_scope_var(&self, scope_var: ScopeVar) -> Option<&ScopeMap<'m>> {
        self.table
            .iter()
            .find(|scope_map| scope_map.scope.meta().scope_var == scope_var)
    }

    pub fn find_var_index(&self, scope_var: ScopeVar) -> Option<usize> {
        self.table
            .iter()
            .enumerate()
            .find_map(|(index, assignment)| {
                if matches!(
                    assignment.scope.kind(),
                    flat_scope::Kind::Var
                        | flat_scope::Kind::SeqPropVariant(..)
                        | flat_scope::Kind::RegexCapture(_)
                ) {
                    if assignment.scope.meta().scope_var == scope_var {
                        Some(index)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
    }

    pub fn find_data_point(&self, var: ontol_hir::Var) -> Option<usize> {
        self.table
            .iter()
            .enumerate()
            .find_map(|(index, assignment)| {
                if matches!(assignment.scope.kind(), flat_scope::Kind::PropVariant(..)) {
                    if assignment.scope.meta().pub_vars.contains(var) {
                        Some(index)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
    }

    pub fn find_scope_var_child(&self, scope_var: ScopeVar) -> Option<&flat_scope::ScopeNode<'m>> {
        let indexes = self.dependees(Some(scope_var));
        if indexes.len() > 1 {
            todo!("Too many indexes here?");
        }

        indexes
            .into_iter()
            .next()
            .map(|index| &self.table[index].scope)
    }

    pub fn dependees(&self, scope_var: Option<ScopeVar>) -> Vec<usize> {
        if let Some(scope_var) = scope_var {
            self.table
                .iter()
                .enumerate()
                .filter_map(|(index, assignment)| {
                    if assignment.scope.meta().deps.contains(scope_var.0) {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            self.table
                .iter()
                .enumerate()
                .filter_map(|(index, assignment)| {
                    if assignment.scope.meta().deps.0.is_empty() {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect()
        }
    }

    pub fn scope_prop_variant_bindings(
        &self,
        variant_var: ScopeVar,
    ) -> ontol_hir::Attribute<Option<ScopeVar>> {
        let mut attribute = ontol_hir::Attribute {
            rel: None,
            val: None,
        };

        for index in self.dependees(Some(variant_var)) {
            let scope_map = &self.table[index];
            match scope_map.scope.kind() {
                flat_scope::Kind::PropRelParam => {
                    attribute.rel = Some(scope_map.scope.meta().scope_var);
                }
                flat_scope::Kind::PropValue => {
                    attribute.val = Some(scope_map.scope.meta().scope_var);
                }
                _ => {}
            }
        }

        attribute
    }

    pub fn rel_val_bindings(
        &self,
        scope_var: ScopeVar,
    ) -> (
        ontol_hir::Binding<'m, TypedHir>,
        ontol_hir::Binding<'m, TypedHir>,
    ) {
        let var_attribute = self.scope_prop_variant_bindings(scope_var);

        fn make_binding<'m>(
            scope_node: Option<&flat_scope::ScopeNode<'m>>,
        ) -> ontol_hir::Binding<'m, TypedHir> {
            match scope_node {
                Some(scope_node) => ontol_hir::Binding::Binder(TypedBinder {
                    var: scope_node.meta().scope_var.0,
                    meta: scope_node.meta().hir_meta,
                }),
                None => ontol_hir::Binding::Wildcard,
            }
        }

        let rel_binding = make_binding(
            var_attribute
                .rel
                .and_then(|var| self.find_scope_var_child(var)),
        );
        let val_binding = make_binding(
            var_attribute
                .val
                .and_then(|var| self.find_scope_var_child(var)),
        );

        (rel_binding, val_binding)
    }
}

pub(super) struct ScopeMap<'m> {
    pub scope: flat_scope::ScopeNode<'m>,
    pub assignments: Vec<Assignment<'m>>,
}

impl<'m> ScopeMap<'m> {
    pub fn take_single_assignment(&mut self) -> Option<Assignment<'m>> {
        let expressions = std::mem::take(&mut self.assignments);
        if expressions.len() > 1 {
            panic!("Multiple assignments");
        }
        expressions.into_iter().next()
    }

    pub fn take_assignments(&mut self) -> Vec<ScopedAssignment<'m>> {
        std::mem::take(&mut self.assignments)
            .into_iter()
            .map(|assignment| ScopedAssignment {
                scope_var: self.scope.meta().scope_var,
                expr: assignment.expr,
                lateral_deps: assignment.lateral_deps,
            })
            .collect()
    }

    pub fn select_assignments(&mut self, selector: ExprSelector) -> Vec<ScopedAssignment<'m>> {
        let (filtered, retained) =
            std::mem::take(&mut self.assignments)
                .into_iter()
                .partition(|assignment| match (assignment.expr.kind(), selector) {
                    (expr::Kind::Prop(prop), ExprSelector::Struct(struct_var)) => {
                        prop.struct_var == struct_var
                    }
                    _ => true,
                });
        self.assignments = retained;
        filtered
            .into_iter()
            .map(|assignment| ScopedAssignment {
                scope_var: self.scope.meta().scope_var,
                expr: assignment.expr,
                lateral_deps: assignment.lateral_deps,
            })
            .collect()
    }
}

#[derive(Debug)]
pub(super) struct Assignment<'m> {
    pub expr: expr::Expr<'m>,
    pub lateral_deps: VarSet,
}

#[derive(Debug)]
pub(super) struct ScopedAssignment<'m> {
    pub scope_var: ScopeVar,
    pub expr: expr::Expr<'m>,
    #[allow(unused)]
    pub lateral_deps: VarSet,
}

pub(super) struct AssignmentSlot<'a, 'm> {
    pub scope_map: &'a mut ScopeMap<'m>,
    pub lateral_deps: VarSet,
}
