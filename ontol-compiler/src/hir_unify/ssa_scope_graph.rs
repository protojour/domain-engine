use ontol_hir::{Attribute, Binder, Binding, CaptureGroup, Kind, Node, Nodes};
use ontol_runtime::{
    smart_format,
    value::PropertyId,
    var::{Var, VarSet},
    vm::proc::BuiltinProc,
    DefId,
};
use thin_vec::ThinVec;
use tracing::{debug, warn};

use crate::{
    hir_unify::{ssa_util::scan_all_vars_and_labels, UnifierError},
    typed_hir::{arena_import, TypedHir, TypedHirData},
    SourceSpan,
};

use super::{ssa_unifier::SsaUnifier, UnifierResult};

#[derive(Clone)]
pub enum Let<'m> {
    Prop(Attribute<Binding<'m, TypedHir>>, (Var, PropertyId)),
    PropDefault(
        Attribute<Binding<'m, TypedHir>>,
        (Var, PropertyId),
        Attribute<Node>,
    ),
    Regex(ThinVec<ThinVec<CaptureGroup<'m, TypedHir>>>, DefId, Var),
    RegexIter(
        TypedHirData<'m, Binder>,
        ThinVec<ThinVec<CaptureGroup<'m, TypedHir>>>,
        DefId,
        Var,
    ),
    Expr(TypedHirData<'m, Binder>, Node),
    AmbiguousExpr(AmbiguousExpr<'m>),
}

#[derive(Clone)]
pub struct AmbiguousExpr<'m> {
    pub dependency: TypedHirData<'m, Binder>,
    pub produces: VarSet,
    pub scope_node: Node,
}

impl<'m> Let<'m> {
    pub fn defines(&self) -> VarSet {
        match self {
            Self::Prop(attr, _) | Self::PropDefault(attr, ..) => {
                Self::binding_defines(&attr.rel).union(&Self::binding_defines(&attr.val))
            }
            Self::Regex(groups_list, ..) => {
                let mut var_set = VarSet::default();
                for groups in groups_list {
                    for group in groups {
                        var_set.insert(group.binder.hir().var);
                    }
                }
                var_set
            }
            Self::RegexIter(binder, ..) => VarSet::from_iter([binder.hir().var]),
            Self::Expr(binder, _) => VarSet::from_iter([binder.hir().var]),
            Self::AmbiguousExpr(AmbiguousExpr { dependency, .. }) => {
                VarSet::from_iter([dependency.hir().var])
            }
        }
    }

    pub fn dependencies(&self) -> VarSet {
        match self {
            Self::Prop(_, (var, _)) | Self::PropDefault(_, (var, _), _) => {
                VarSet::from_iter([*var])
            }
            Self::Regex(.., var) => VarSet::from_iter([*var]),
            Self::RegexIter(.., var) => VarSet::from_iter([*var]),
            Self::Expr(..) => VarSet::default(),
            Self::AmbiguousExpr { .. } => VarSet::default(),
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
        let mut ambiguous_list = vec![];

        for (let_node, span) in graph {
            match let_node {
                Let::AmbiguousExpr(ambiguous) => {
                    ambiguous_list.push((ambiguous, span));
                }
                node => {
                    bounded_list.push((node, span));
                }
            }
        }

        loop {
            let next_list = std::mem::take(&mut ambiguous_list);
            let list_len = next_list.len();

            // a "fix-point" algorithm.
            // it's rerun until no progress can be made.
            for (ambiguous, span) in next_list {
                let unique_produces = VarSet(
                    ambiguous
                        .produces
                        .0
                        .difference(&self.scope_tracker.in_scope.0)
                        .collect(),
                );

                match unique_produces.0.len() {
                    0 => {}
                    1 => {
                        debug!(
                            "single variable producer. dependency={dependency:?} produces={produces:?} unique={unique_produces:?}",
                            dependency = ambiguous.dependency,
                            produces = ambiguous.produces
                        );

                        let produces_var = unique_produces.iter().next().unwrap();
                        let dependency = ambiguous.dependency;

                        let mut out_binder = None;

                        let substitution =
                            self.mk_node(Kind::Var(dependency.hir().var), *dependency.meta());
                        let node = self.invert_call(
                            ambiguous.scope_node,
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
                        debug!("still ambiguous: {unique_produces:?}");
                        ambiguous_list.push((ambiguous, span));
                    }
                }
            }

            if ambiguous_list.is_empty() {
                break;
            } else if ambiguous_list.len() == list_len {
                debug!("not able to make progress");
                break;
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
        debug!("invert {node_ref}");
        match node_ref.kind() {
            ontol_hir::Kind::Var(var) => {
                assert_eq!(*var, respect_to);
                *out_binder = Some(TypedHirData(Binder { var: *var }, *node_ref.meta()));
                Ok(substitution)
            }
            ontol_hir::Kind::Call(proc, params) => {
                let mut subst_params = vec![];

                for (index, param) in params.iter().enumerate() {
                    let param_ref = self.scope_arena.node_ref(*param);
                    let free_vars = scan_all_vars_and_labels(self.scope_arena, [*param]);
                    if free_vars.contains(respect_to) {
                        subst_params.push((index, param_ref));
                    }
                }

                if subst_params.len() > 1 {
                    return Err(UnifierError::Unimplemented(smart_format!(
                        "Ambiguous inversion"
                    )));
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
                let inverted_proc = match proc {
                    BuiltinProc::Add => BuiltinProc::Sub,
                    BuiltinProc::Sub => BuiltinProc::Add,
                    BuiltinProc::Mul => BuiltinProc::Div,
                    BuiltinProc::Div => BuiltinProc::Mul,
                    _ => panic!("Unsupported procedure; cannot invert {proc:?}"),
                };

                let new_substitution = self.mk_node(
                    Kind::Call(inverted_proc, out_params),
                    *subst_node_ref.meta(),
                );

                self.invert_call(
                    subst_node_ref.node(),
                    respect_to,
                    new_substitution,
                    out_binder,
                )
            }
            _other => Err(super::UnifierError::Unimplemented(smart_format!(
                "cannot invert non-function"
            ))),
        }
    }
}
