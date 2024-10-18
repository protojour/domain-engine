use ontol_hir::{
    import::arena_import, Binder, Binding, CaptureGroup, Kind, Node, Nodes, OverloadFunc, Pack,
};
use ontol_runtime::{
    var::{Var, VarSet},
    DefId, PropId,
};
use thin_vec::ThinVec;
use tracing::{debug, warn};

use crate::{
    hir_unify::{ssa_util::scan_all_vars_and_labels, UnifierError},
    typed_hir::{TypedHir, TypedHirData},
    CompileError, SourceSpan,
};

use super::{ssa_unifier::SsaUnifier, UnifierResult};

#[derive(Clone)]
pub enum Let<'m> {
    Prop(Pack<Binding<'m, TypedHir>>, (Var, PropId)),
    PropDefault(Pack<Binding<'m, TypedHir>>, (Var, PropId), ThinVec<Node>),
    Narrow(TypedHirData<'m, Var>),
    Regex(ThinVec<ThinVec<CaptureGroup<'m, TypedHir>>>, DefId, Var),
    RegexIter(
        TypedHirData<'m, Binder>,
        ThinVec<ThinVec<CaptureGroup<'m, TypedHir>>>,
        DefId,
        Var,
    ),
    Expr(TypedHirData<'m, Binder>, Node),
    /// Unsolved complex expression that does not yet expose a data point
    Complex(ComplexExpr<'m>),
}

#[derive(Clone)]
pub struct ComplexExpr<'m> {
    pub dependency: TypedHirData<'m, Binder>,
    pub produces: VarSet,
    pub scope_node: Node,
}

impl<'m> Let<'m> {
    pub fn defines(&self) -> VarSet {
        match self {
            Self::Prop(bind_pack, ..) | Self::PropDefault(bind_pack, ..) => match bind_pack {
                Pack::Unit(u) => Self::binding_defines(u),
                Pack::Tuple(t) => {
                    let mut set = VarSet::default();
                    for b in t {
                        set = set.union(&Self::binding_defines(b));
                    }
                    set
                }
            },
            Self::Regex(groups_list, ..) => {
                let mut var_set = VarSet::default();
                for groups in groups_list {
                    for group in groups {
                        var_set.insert(group.binder.hir().var);
                    }
                }
                var_set
            }
            Self::Narrow(_) => VarSet::default(),
            Self::RegexIter(binder, ..) => VarSet::from_iter([binder.hir().var]),
            Self::Expr(binder, _) => VarSet::from_iter([binder.hir().var]),
            Self::Complex(ComplexExpr { dependency, .. }) => {
                VarSet::from_iter([dependency.hir().var])
            }
        }
    }

    pub fn dependencies(&self) -> VarSet {
        match self {
            Self::Prop(_, (var, _)) | Self::PropDefault(_, (var, _), _) => {
                VarSet::from_iter([*var])
            }
            Self::Narrow(input) => VarSet::from_iter([input.0]),
            Self::Regex(.., var) => VarSet::from_iter([*var]),
            Self::RegexIter(.., var) => VarSet::from_iter([*var]),
            Self::Expr(..) => VarSet::default(),
            Self::Complex { .. } => VarSet::default(),
        }
    }

    fn binding_defines(binding: &Binding<'m, TypedHir>) -> VarSet {
        match binding {
            Binding::Binder(binder) => VarSet::from_iter([binder.hir().var]),
            Binding::Wildcard => VarSet::default(),
        }
    }
}

pub type SpannedLet<'m> = (Let<'m>, SourceSpan);

impl<'c, 'm> SsaUnifier<'c, 'm> {
    pub fn process_let_graph(
        &mut self,
        graph: Vec<SpannedLet<'m>>,
    ) -> UnifierResult<Vec<SpannedLet<'m>>> {
        let mut bounded_list = vec![];
        let mut complex_list = vec![];

        for (let_node, span) in graph {
            match let_node {
                Let::Complex(complex) => {
                    complex_list.push((complex, span));
                }
                node => {
                    bounded_list.push((node, span));
                }
            }
        }

        loop {
            let next_list = std::mem::take(&mut complex_list);
            let list_len = next_list.len();

            // a "fix-point" algorithm.
            // it's rerun until no progress can be made.
            for (complex, span) in next_list {
                let unique_produces = VarSet(
                    complex
                        .produces
                        .0
                        .difference(&self.scope_tracker.in_scope.0)
                        .collect(),
                );

                match unique_produces.0.len() {
                    0 => {}
                    1 => {
                        debug!(
                            dependency = ?complex.dependency,
                            produces = ?complex.produces,
                            ?unique_produces,
                            "single variable producer",
                        );

                        let produces_var = unique_produces.iter().next().unwrap();
                        let dependency = complex.dependency;

                        let mut out_binder = None;

                        let substitution =
                            self.mk_node(Kind::Var(dependency.hir().var), *dependency.meta());
                        let node = self.invert_call(
                            complex.scope_node,
                            produces_var,
                            substitution,
                            &mut out_binder,
                        )?;

                        if let Some(binder) = out_binder {
                            // Updating in_scope directly
                            self.scope_tracker.in_scope.insert(produces_var);
                            bounded_list.push((Let::Expr(binder, node), span));
                        } else {
                            warn!("Inversion did not write a binder");
                        }
                    }
                    _ => {
                        debug!(unique = ?unique_produces, "still ambiguous");
                        complex_list.push((complex, span));
                    }
                }
            }

            if complex_list.is_empty() {
                break;
            } else if complex_list.len() == list_len {
                debug!("not able to make progress");
                for (_, span) in complex_list {
                    CompileError::UnsolvableEquation.span(span).report(self);
                }
                return Err(UnifierError::Unsolvable);
            }
        }

        Ok(bounded_list)
    }

    // from: (* (* (* (* $a 60) 60) 24) 365)
    //   to: (/ (/ (/ (/ $b 365) 24) 60) 60)
    fn invert_call(
        &mut self,
        node: Node,
        respect_to: Var,
        substitution: Node,
        out_binder: &mut Option<TypedHirData<'m, Binder>>,
    ) -> UnifierResult<Node> {
        let node_ref = self.scope_arena.node_ref(node);
        debug!(node = %node_ref, "invert");
        match node_ref.kind() {
            ontol_hir::Kind::Var(var) => {
                assert_eq!(*var, respect_to);
                *out_binder = Some(TypedHirData(Binder { var: *var }, *node_ref.meta()));
                Ok(substitution)
            }
            ontol_hir::Kind::Call(func, params) => {
                let mut subst_params = vec![];

                for (index, param) in params.iter().enumerate() {
                    let param_ref = self.scope_arena.node_ref(*param);
                    let free_vars = scan_all_vars_and_labels(self.scope_arena, [*param]);
                    if free_vars.contains(respect_to) {
                        subst_params.push((index, param_ref));
                    }
                }

                if subst_params.len() > 1 {
                    for (_, param_ref) in subst_params {
                        CompileError::UnsupportedVariableDuplication
                            .span(param_ref.meta().span)
                            .report(self);
                    }
                    return Err(UnifierError::Unsolvable);
                }
                let (subst_param_index, subst_node_ref) = subst_params.into_iter().next().unwrap();
                let mut out_params = Nodes::default();

                for (index, param) in params.iter().enumerate() {
                    let param_ref = self.scope_arena.node_ref(*param);
                    if index == subst_param_index {
                        out_params.push(substitution);
                    } else {
                        out_params.push(arena_import(&mut self.out_arena, param_ref));
                    }
                }

                // FIXME: Properly check this
                let inverted_func = match func {
                    OverloadFunc::Add => OverloadFunc::Sub,
                    OverloadFunc::Sub => OverloadFunc::Add,
                    OverloadFunc::Mul => OverloadFunc::Div,
                    OverloadFunc::Div => OverloadFunc::Mul,
                    _ => panic!("Unsupported procedure; cannot invert {func:?}"),
                };

                let new_substitution = self.mk_node(
                    Kind::Call(inverted_func, out_params),
                    *subst_node_ref.meta(),
                );

                self.invert_call(
                    subst_node_ref.node(),
                    respect_to,
                    new_substitution,
                    out_binder,
                )
            }
            _other => Err(super::UnifierError::Unimplemented(
                "cannot invert non-function".to_string(),
            )),
        }
    }
}
