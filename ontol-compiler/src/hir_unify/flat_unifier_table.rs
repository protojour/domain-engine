use std::fmt::Debug;

use tracing::debug;

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
    Struct(ontol_hir::Var, ScopeVar),
}

pub trait IsInScope {
    fn is_in_scope(&self, in_scope: &VarSet) -> bool;
    fn var_set(&self) -> VarSet;
}

#[derive(Clone, Default)]
pub struct ScopeFilter {
    /// The set of scope branches that apply.
    /// I.e. the allocated scope branch _needs_ these constraints
    pub req_constraints: VarSet,
}

impl ScopeFilter {
    pub fn constraint(&self, scope_var: ScopeVar) -> Self {
        Self {
            req_constraints: self.req_constraints.union_one(scope_var.0),
        }
    }
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
        optional: ontol_hir::Optional,
        filter: &mut ScopeFilter,
    ) -> Option<AssignmentSlot> {
        let mut candidate: Option<usize> = None;
        let mut observed_variables = VarSet::default();

        let mut lateral_deps = VarSet::default();

        if optional.0 {
            let filter_req_len = filter.req_constraints.0.len();

            // Note: Reverse search.
            for (index, scope_map) in self.table.iter().enumerate().rev() {
                let scope_meta = &scope_map.scope.meta();
                let scope_constraints_len = scope_meta.constraints.0.len();

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

                if scope_constraints_len > filter_req_len + 1 {
                    continue;
                }

                if !scope_meta.free_vars.0.is_superset(&free_vars.0) {
                    continue;
                }

                if candidate.is_some() && scope_constraints_len == filter_req_len {
                    break;
                }

                candidate = Some(index);
            }
        } else {
            // Note: Reverse search.
            for (index, scope_map) in self.table.iter().enumerate().rev() {
                let scope_defs = &scope_map.scope.meta().defs;

                // quick early check
                if scope_defs.0.is_empty() {
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
                if scope_defs.0.intersection(&free_vars.0).next().is_none() {
                    continue;
                }

                // If the candidate is only introducing variables that we've
                // already seen, this is not specific enough. The search is over..
                if !observed_variables.0.is_empty()
                    && scope_defs.0.is_superset(&observed_variables.0)
                {
                    debug!("    END at {:?}", scope_map.scope.meta().scope_var);
                    break;
                }

                observed_variables.union_with(scope_defs);

                debug!("    candidate: {:?}", scope_map.scope.meta().scope_var);

                // Note that we choose the _last_ (in iteration order, really the _first_) candidate
                // that introduces at least one variable in free_vars.
                candidate = Some(index);
            }

            lateral_deps = if let Some(index) = candidate {
                let scope_map = self.scope_map_mut(index);
                VarSet(free_vars.0.difference(&scope_map.scope.1.defs.0).collect())
            } else {
                VarSet::default()
            };
        }

        // Apply filter successively
        // if let Some(index) = candidate {
        //     let scope_map = self.scope_map_mut(index);
        //
        //     if scope_map.scope.meta().constraints.as_ref() != &filter.req_constraints {
        //         let mut deps = scope_map.scope.meta().deps.clone();
        //         while !deps.0.is_empty() {
        //             let mut next_deps = VarSet::default();
        //
        //             for dep in &deps {
        //                 let (index, scope_map) =
        //                     self.find_scope_map_by_scope_var(ScopeVar(dep)).unwrap();
        //                 if scope_map.scope.meta().constraints.as_ref() == &filter.req_constraints {
        //                     match scope_map.scope.kind() {
        //                         flat_scope::Kind::PropVariant(..) => {}
        //                         flat_scope::Kind::SeqPropVariant(..) => {
        //                             candidate = Some(index);
        //                             next_deps.0.clear();
        //                             break;
        //                         }
        //                         _ => {}
        //                     }
        //                 }
        //
        //                 next_deps.union_with(&scope_map.scope.meta().deps);
        //             }
        //
        //             deps = next_deps;
        //         }
        //     }
        // }

        // apply new filter
        if let Some(index) = candidate {
            let scope_map = self.scope_map_mut(index);
            let scope_var = scope_map.scope.meta().scope_var;
            match scope_map.scope.kind() {
                flat_scope::Kind::PropVariant(_, optional, ..) => {
                    if optional.0 {
                        filter.req_constraints.insert(scope_var.0);
                    }
                }
                flat_scope::Kind::SeqPropVariant(..) => {
                    filter.req_constraints.insert(scope_var.0);
                }
                _ => {}
            }
        }

        candidate.map(|index| AssignmentSlot {
            index,
            lateral_deps,
        })
    }

    pub fn find_scope_map_by_scope_var(
        &self,
        scope_var: ScopeVar,
    ) -> Option<(usize, &ScopeMap<'m>)> {
        self.table
            .iter()
            .enumerate()
            .find(|(_index, scope_map)| scope_map.scope.meta().scope_var == scope_var)
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
                    if assignment.scope.meta().defs.contains(var) {
                        Some(index)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
    }

    pub fn find_upstream_data_point(&self, scope_var: ScopeVar) -> Option<&ScopeMap<'m>> {
        let (_, mut scope_map) = self.find_scope_map_by_scope_var(scope_var)?;

        loop {
            let Some(dep) = scope_map.scope.meta().deps.iter().next() else {
                return None;
            };

            let (_, next_scope_map) = self.find_scope_map_by_scope_var(ScopeVar(dep))?;

            if let flat_scope::Kind::PropVariant(..) = next_scope_map.scope.kind() {
                return Some(next_scope_map);
            }

            scope_map = next_scope_map;
        }
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

    pub fn rel_val_bindings(&self, scope_var: ScopeVar) -> RelValBindings<'m> {
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

        let rel = make_binding(
            var_attribute
                .rel
                .and_then(|var| self.find_scope_var_child(var)),
        );
        let val = make_binding(
            var_attribute
                .val
                .and_then(|var| self.find_scope_var_child(var)),
        );

        RelValBindings { rel, val }
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
                    (expr::Kind::Prop(prop), ExprSelector::Struct(struct_var, _)) => {
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

pub(super) struct AssignmentSlot {
    pub index: usize,
    pub lateral_deps: VarSet,
}

pub(super) struct RelValBindings<'m> {
    pub rel: ontol_hir::Binding<'m, TypedHir>,
    pub val: ontol_hir::Binding<'m, TypedHir>,
}

impl<'m> Debug for RelValBindings<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({} {})", self.rel, self.val)
    }
}

impl<'m> IsInScope for ontol_hir::Binding<'m, TypedHir> {
    fn is_in_scope(&self, in_scope: &VarSet) -> bool {
        match self {
            ontol_hir::Binding::Binder(binder) => in_scope.contains(binder.var),
            ontol_hir::Binding::Wildcard => true,
        }
    }

    fn var_set(&self) -> VarSet {
        match self {
            ontol_hir::Binding::Binder(binder) => [binder.var].into(),
            ontol_hir::Binding::Wildcard => Default::default(),
        }
    }
}

impl<'m> IsInScope for RelValBindings<'m> {
    fn is_in_scope(&self, in_scope: &VarSet) -> bool {
        self.rel.is_in_scope(in_scope) && self.val.is_in_scope(in_scope)
    }

    fn var_set(&self) -> VarSet {
        let mut var_set = self.rel.var_set();
        var_set.union_with(&self.val.var_set());
        var_set
    }
}
