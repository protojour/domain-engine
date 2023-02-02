use std::collections::HashMap;

use ontol_runtime::{
    proc::{BuiltinProc, Lib, Local, NParams, OpCode, Procedure},
    DefId,
};
use smallvec::{smallvec, SmallVec};
use tracing::{debug, warn};

use crate::{
    codegen::{codegen_map_obj::codegen_map_obj_origin, typed_expr::TypedExprKind},
    compiler::Compiler,
    types::{Type, TypeRef},
};

use super::{
    rewrite::rewrite,
    typed_expr::{NodeId, SealedTypedExprTable, SyntaxVar, TypedExprTable},
    CodegenTask,
};

/// A small part of a procedure, a "slice" of instructions
pub type OpCodes = SmallVec<[OpCode; 8]>;

/// Perform all codegen tasks
pub fn execute_codegen_tasks(compiler: &mut Compiler) {
    let mut tasks = vec![];
    tasks.append(&mut compiler.codegen_tasks.tasks);

    let mut lib = Lib::default();
    let mut translations = HashMap::default();

    // FIXME: Do we need do topological sort of tasks?
    // At least in the beginning there is no support for recursive eq
    // (since there are no "monads" (option or iterators, etc) yet)
    for task in tasks {
        match task {
            CodegenTask::Eq(mut eq_task) => {
                // a -> b
                codegen_translate_rewrite(
                    &mut lib,
                    &mut translations,
                    &mut eq_task.typed_expr_table,
                    eq_task.node_a,
                    eq_task.node_b,
                );

                eq_task.typed_expr_table.reset();

                // b -> a
                codegen_translate_rewrite(
                    &mut lib,
                    &mut translations,
                    &mut eq_task.typed_expr_table,
                    eq_task.node_b,
                    eq_task.node_a,
                );
            }
        }
    }

    compiler.codegen_tasks.result_lib = lib;
    compiler.codegen_tasks.result_translations = translations;
}

fn codegen_translate_rewrite(
    lib: &mut Lib,
    translations: &mut HashMap<(DefId, DefId), Procedure>,
    table: &mut SealedTypedExprTable,
    origin_node: NodeId,
    dest_node: NodeId,
) -> bool {
    match rewrite(&mut table.inner, origin_node) {
        Ok(()) => {
            let key_a = find_translation_key(&table.inner.get_expr_no_rewrite(origin_node).ty);
            let key_b = find_translation_key(&table.inner.get_expr_no_rewrite(dest_node).ty);
            match (key_a, key_b) {
                (Some(a), Some(b)) => {
                    let procedure = codegen_translate(lib, b, &table.inner, origin_node, dest_node);

                    translations.insert((a, b), procedure);
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
    lib: &mut Lib,
    return_def_id: DefId,
    table: &TypedExprTable<'m>,
    origin_node: NodeId,
    dest_node: NodeId,
) -> Procedure {
    let (_, origin_expr) = table.get_expr(&table.source_rewrites, origin_node);

    debug!(
        "codegen origin: {} dest: {}",
        table.debug_tree(&table.source_rewrites, origin_node),
        table.debug_tree(&table.target_rewrites, dest_node),
    );

    match &origin_expr.kind {
        TypedExprKind::ValueObj(_) => {
            codegen_value_obj_origin(lib, return_def_id, table, dest_node)
        }
        TypedExprKind::MapObj(attributes) => {
            codegen_map_obj_origin(lib, table, attributes, dest_node)
        }
        other => panic!("unable to generate translation: {other:?}"),
    }
}

fn codegen_value_obj_origin<'m>(
    lib: &mut Lib,
    return_def_id: DefId,
    table: &TypedExprTable<'m>,
    dest_node: NodeId,
) -> Procedure {
    let (_, dest_expr) = table.get_expr(&table.target_rewrites, dest_node);

    struct ValueCodegen {
        input_local: Local,
        var_tracker: VarFlowTracker,
    }

    impl Codegen for ValueCodegen {
        fn codegen_variable(&mut self, var: SyntaxVar, opcodes: &mut OpCodes) {
            // There should only be one origin variable (but can flow into several slots)
            assert!(var.0 == 0);
            self.var_tracker.count_use(var);
            opcodes.push(OpCode::Clone(self.input_local));
        }
    }

    let mut value_codegen = ValueCodegen {
        input_local: Local(0),
        var_tracker: Default::default(),
    };
    let mut opcodes = smallvec![];

    match &dest_expr.kind {
        TypedExprKind::ValueObj(node_id) => {
            value_codegen.codegen_expr(table, *node_id, &mut opcodes);
            opcodes.push(OpCode::Return0);
        }
        TypedExprKind::MapObj(dest_attrs) => {
            opcodes.push(OpCode::CallBuiltin(BuiltinProc::NewMap, return_def_id));

            // the input value is not compound, so it will be consumed.
            // Therefore it must be top of the stack:
            opcodes.push(OpCode::Swap(Local(0), Local(1)));
            value_codegen.input_local = Local(1);

            for (property_id, node) in dest_attrs {
                value_codegen.codegen_expr(table, *node, &mut opcodes);
                opcodes.push(OpCode::PutAttr(Local(0), *property_id));
            }

            opcodes.push(OpCode::Return0);
        }
        kind => {
            todo!("target: {kind:?}");
        }
    }

    let opcodes = opcodes
        .into_iter()
        .filter(|opcode| {
            match opcode {
                OpCode::Clone(local) if *local == value_codegen.input_local => {
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
        .collect::<OpCodes>();

    debug!("{opcodes:#?}");

    lib.add_procedure(NParams(1), opcodes)
}

pub trait Codegen {
    fn codegen_expr<'m>(
        &mut self,
        table: &TypedExprTable<'m>,
        expr_id: NodeId,
        opcodes: &mut OpCodes,
    ) {
        let (_, expr) = table.get_expr(&table.target_rewrites, expr_id);
        match &expr.kind {
            TypedExprKind::Call(proc, params) => {
                for param in params.iter() {
                    self.codegen_expr(table, *param, opcodes);
                }

                let return_def_id = expr.ty.get_single_def_id().unwrap();

                opcodes.push(OpCode::CallBuiltin(*proc, return_def_id));
            }
            TypedExprKind::Constant(k) => {
                let return_def_id = expr.ty.get_single_def_id().unwrap();
                opcodes.push(OpCode::Constant(*k, return_def_id));
            }
            TypedExprKind::Variable(var) => {
                self.codegen_variable(*var, opcodes);
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

    fn codegen_variable(&mut self, var: SyntaxVar, opcodes: &mut OpCodes);
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
