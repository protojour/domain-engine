use std::collections::HashMap;

use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams, OpCode, Procedure},
    DefId,
};
use smallvec::{smallvec, SmallVec};
use tracing::{debug, warn};

use crate::{
    codegen::{codegen_map_obj::codegen_map_obj_origin, typed_expr::TypedExprKind},
    compiler::Compiler,
    types::{Type, TypeRef},
    SourceSpan,
};

use super::{
    link::{link, LinkResult},
    typed_expr::{NodeId, SealedTypedExprTable, SyntaxVar, TypedExprTable},
    CodegenTask,
};

pub type SpannedOpCodes = SmallVec<[(OpCode, SourceSpan); 32]>;

#[derive(Default)]
pub(super) struct ProcTable {
    pub procs: HashMap<(DefId, DefId), UnlinkedProc>,
    pub translate_calls: Vec<TranslateCall>,
}

impl ProcTable {
    /// Allocate a temporary procedure id for a translate call.
    /// This will be resolved to final "physical" ID in the link phase.
    fn gen_translate_call(&mut self, from: DefId, to: DefId) -> OpCode {
        let id = self.translate_calls.len() as u32;
        self.translate_calls.push(TranslateCall {
            translation: (from, to),
        });
        OpCode::Call(Procedure {
            start: id,
            n_params: NParams(1),
        })
    }
}

pub(super) struct TranslateCall {
    pub translation: (DefId, DefId),
}

/// Procedure that has been generated but not linked.
/// i.e. OpCode::Call has incorrect parameters, and
/// we need to look up a table to resolve that in the link phase.
pub(super) struct UnlinkedProc {
    pub n_params: NParams,
    pub opcodes: SpannedOpCodes,
}

/// Perform all codegen tasks
pub fn execute_codegen_tasks(compiler: &mut Compiler) {
    let tasks = std::mem::take(&mut compiler.codegen_tasks.tasks);

    let mut proc_table = ProcTable::default();

    for task in tasks {
        match task {
            CodegenTask::Eq(mut eq_task) => {
                // a -> b
                codegen_translate_rewrite(
                    &mut proc_table,
                    &mut eq_task.typed_expr_table,
                    eq_task.node_a,
                    eq_task.node_b,
                    DebugDirection::Forward,
                );

                eq_task.typed_expr_table.reset();

                // b -> a
                codegen_translate_rewrite(
                    &mut proc_table,
                    &mut eq_task.typed_expr_table,
                    eq_task.node_b,
                    eq_task.node_a,
                    DebugDirection::Backward,
                );
            }
        }
    }

    let LinkResult { lib, translations } = link(compiler, &mut proc_table);

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_translations = translations;
}

enum DebugDirection {
    Forward,
    Backward,
}

fn codegen_translate_rewrite(
    proc_table: &mut ProcTable,
    table: &mut SealedTypedExprTable,
    origin_node: NodeId,
    dest_node: NodeId,
    direction: DebugDirection,
) -> bool {
    match table.inner.rewriter().rewrite_expr(origin_node) {
        Ok(_) => {
            let key_a = find_translation_key(&table.inner.expressions[origin_node].ty);
            let key_b = find_translation_key(&table.inner.expressions[dest_node].ty);
            match (key_a, key_b) {
                (Some(a), Some(b)) => {
                    let procedure = codegen_translate(
                        proc_table,
                        b,
                        &table.inner,
                        origin_node,
                        dest_node,
                        direction,
                    );

                    proc_table.procs.insert((a, b), procedure);
                    true
                }
                other => {
                    warn!("unable to save translation: key = {other:?}");
                    false
                }
            }
        }
        Err(error) => {
            panic!("TODO: could not rewrite: {error:?}");
        }
    }
}

fn find_translation_key(ty: &TypeRef) -> Option<DefId> {
    match ty {
        Type::Domain(def_id) => Some(*def_id),
        other => {
            warn!("unable to get translation key: {other:?}");
            None
        }
    }
}

fn codegen_translate<'m>(
    proc_table: &mut ProcTable,
    return_def_id: DefId,
    expr_table: &TypedExprTable<'m>,
    origin_node: NodeId,
    dest_node: NodeId,
    direction: DebugDirection,
) -> UnlinkedProc {
    let (_, origin_expr) = expr_table.get_expr(&expr_table.source_rewrites, origin_node);

    // for easier readability:
    match direction {
        DebugDirection::Forward => debug!(
            "codegen source: {} target: {}",
            expr_table.debug_tree(&expr_table.source_rewrites, origin_node),
            expr_table.debug_tree(&expr_table.target_rewrites, dest_node),
        ),
        DebugDirection::Backward => debug!(
            "codegen target: {} source: {}",
            expr_table.debug_tree(&expr_table.target_rewrites, dest_node),
            expr_table.debug_tree(&expr_table.source_rewrites, origin_node),
        ),
    }

    match &origin_expr.kind {
        TypedExprKind::ValueObj(_) => {
            codegen_value_obj_origin(proc_table, return_def_id, expr_table, dest_node)
        }
        TypedExprKind::MapObj(attributes) => {
            codegen_map_obj_origin(proc_table, expr_table, attributes, dest_node)
        }
        other => panic!("unable to generate translation: {other:?}"),
    }
}

fn codegen_value_obj_origin<'m>(
    proc_table: &mut ProcTable,
    return_def_id: DefId,
    expr_table: &TypedExprTable<'m>,
    dest_node: NodeId,
) -> UnlinkedProc {
    let (_, dest_expr) = expr_table.get_expr(&expr_table.target_rewrites, dest_node);

    struct ValueCodegen {
        input_local: Local,
        var_tracker: VarFlowTracker,
    }

    impl Codegen for ValueCodegen {
        fn codegen_variable(
            &mut self,
            var: SyntaxVar,
            opcodes: &mut SpannedOpCodes,
            span: &SourceSpan,
        ) {
            // There should only be one origin variable (but can flow into several slots)
            assert!(var.0 == 0);
            self.var_tracker.count_use(var);
            opcodes.push((OpCode::Clone(self.input_local), *span));
        }
    }

    let mut value_codegen = ValueCodegen {
        input_local: Local(0),
        var_tracker: Default::default(),
    };
    let mut opcodes = smallvec![];
    let span = dest_expr.span;

    match &dest_expr.kind {
        TypedExprKind::ValueObj(node_id) => {
            value_codegen.codegen_expr(proc_table, expr_table, *node_id, &mut opcodes);
            opcodes.push((OpCode::Return0, dest_expr.span));
        }
        TypedExprKind::MapObj(dest_attrs) => {
            opcodes.push((
                OpCode::CallBuiltin(BuiltinProc::NewMap, return_def_id),
                span,
            ));

            // the input value is not compound, so it will be consumed.
            // Therefore it must be top of the stack:
            opcodes.push((OpCode::Swap(Local(0), Local(1)), span));
            value_codegen.input_local = Local(1);

            for (relation_id, node) in dest_attrs {
                value_codegen.codegen_expr(proc_table, expr_table, *node, &mut opcodes);
                opcodes.push((OpCode::PutAttr(Local(0), *relation_id), span));
            }

            opcodes.push((OpCode::Return0, span));
        }
        kind => {
            todo!("target: {kind:?}");
        }
    }

    let opcodes = opcodes
        .into_iter()
        .filter(|op| {
            match op {
                (OpCode::Clone(local), _) if *local == value_codegen.input_local => {
                    if value_codegen.var_tracker.do_use(SyntaxVar(0)).use_count > 1 {
                        // Keep cloning until the last use of the variable,
                        // which must pop it off the stack. (i.e. keep the clone instruction)
                        true
                    } else {
                        // drop clone instruction. Stack should only contain the return value.
                        false
                    }
                }
                _ => true,
            }
        })
        .collect::<SpannedOpCodes>();

    debug!("{opcodes:#?}");

    UnlinkedProc {
        n_params: NParams(1),
        opcodes,
    }
}

pub(super) trait Codegen {
    fn codegen_expr<'m>(
        &mut self,
        proc_table: &mut ProcTable,
        expr_table: &TypedExprTable<'m>,
        expr_id: NodeId,
        opcodes: &mut SpannedOpCodes,
    ) {
        let (_, expr) = expr_table.get_expr(&expr_table.target_rewrites, expr_id);
        let span = expr.span;
        match &expr.kind {
            TypedExprKind::Call(proc, params) => {
                for param in params.iter() {
                    self.codegen_expr(proc_table, expr_table, *param, opcodes);
                }

                let return_def_id = expr.ty.get_single_def_id().unwrap();

                opcodes.push((OpCode::CallBuiltin(*proc, return_def_id), span));
            }
            TypedExprKind::Constant(k) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                opcodes.push((OpCode::Constant(*k, return_def_id), span));
            }
            TypedExprKind::Variable(var) => {
                self.codegen_variable(*var, opcodes, &span);
            }
            TypedExprKind::Translate(param_id, from_ty) => {
                self.codegen_expr(proc_table, expr_table, *param_id, opcodes);

                debug!("translate from {from_ty:?} to {:?}", expr.ty);
                let from = find_translation_key(from_ty).unwrap();
                let to = find_translation_key(&expr.ty).unwrap();
                opcodes.push((proc_table.gen_translate_call(from, to), span));
            }
            TypedExprKind::ValueObj(_) => {
                todo!()
            }
            TypedExprKind::MapObj(_) => {
                todo!()
            }
            TypedExprKind::Unit => {
                todo!()
            }
        }
    }

    fn codegen_variable(&mut self, var: SyntaxVar, opcodes: &mut SpannedOpCodes, span: &SourceSpan);
}

#[derive(Default)]
pub struct VarFlowTracker {
    // for determining whether to clone
    states: HashMap<SyntaxVar, VarFlowState>,
}

impl VarFlowTracker {
    /// count usages when building up the full state
    pub fn count_use(&mut self, var: SyntaxVar) {
        self.states.entry(var).or_default().use_count += 1;
    }

    /// actually use the variable. Returns previous state.
    pub fn do_use(&mut self, var: SyntaxVar) -> VarFlowState {
        let stored_state = self.states.entry(var).or_default();
        let clone = stored_state.clone();
        stored_state.use_count -= 1;
        stored_state.reused = true;
        clone
    }
}

#[derive(Clone, Default)]
pub struct VarFlowState {
    pub use_count: usize,
    pub reused: bool,
}
