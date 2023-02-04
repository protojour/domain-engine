use std::collections::HashMap;

use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams, OpCode},
    RelationId,
};
use smallvec::{smallvec, SmallVec};
use tracing::debug;

use crate::{
    codegen::{
        codegen::{Codegen, OpCodes, VarFlowTracker},
        typed_expr::SyntaxVar,
    },
    SourceSpan,
};

use super::{
    codegen::{ProcTable, UnlinkedProc},
    typed_expr::{NodeId, TypedExprKind, TypedExprTable},
};

/// Generate code originating from a map obj destructuring
pub(super) fn codegen_map_obj_origin<'m>(
    proc_table: &mut ProcTable,
    expr_table: &TypedExprTable<'m>,
    origin_attrs: &HashMap<RelationId, NodeId>,
    dest_node: NodeId,
    eq_span: &SourceSpan,
) -> UnlinkedProc {
    let (_, dest_expr) = expr_table.get_expr(&expr_table.target_rewrites, dest_node);

    let mut origin_properties: Vec<_> = origin_attrs
        .iter()
        .map(|(prop_id, node_id)| {
            match &expr_table
                .get_expr(&expr_table.source_rewrites, *node_id)
                .1
                .kind
            {
                TypedExprKind::Variable(var) => (*prop_id, *var),
                _ => panic!("source property not a variable"),
            }
        })
        .collect();
    origin_properties.sort_by_key(|(_, var)| *var);

    if !origin_properties.is_empty() {
        // must start with SyntaxVar(0)
        assert!(origin_properties[0].1 == SyntaxVar(0));
    }

    #[derive(Default)]
    struct MapCodegen {
        var_tracker: VarFlowTracker,
    }

    impl Codegen for MapCodegen {
        fn codegen_variable(&mut self, var: SyntaxVar, opcodes: &mut OpCodes) {
            self.var_tracker.count_use(var);
            // Make Clone(Local(v + 2)) represent use of the syntax variable.
            // this will be rewritten later:
            opcodes.push(OpCode::Clone(Local(var.0 + 2)));
        }
    }

    let mut map_codegen = MapCodegen::default();
    let mut ops = smallvec![];

    match &dest_expr.kind {
        TypedExprKind::MapObj(dest_attrs) => {
            let return_def_id = dest_expr.ty.get_single_def_id().unwrap();

            // Local(1), this is the return value:
            ops.push(OpCode::CallBuiltin(BuiltinProc::NewMap, return_def_id));

            for (relation_id, node) in dest_attrs {
                map_codegen.codegen_expr(proc_table, expr_table, *node, &mut ops, eq_span);
                ops.push(OpCode::PutAttr(Local(1), *relation_id));
            }

            ops.push(OpCode::Return(Local(1)));
        }
        TypedExprKind::ValueObj(node_id) => {
            map_codegen.codegen_expr(proc_table, expr_table, *node_id, &mut ops, eq_span);
            ops.push(OpCode::Return(Local(1)));
        }
        kind => {
            todo!("target: {kind:?}");
        }
    }

    // post-process
    let mut var_stack_state = VarStackState::default();
    let opcodes: OpCodes = ops.into_iter().fold(smallvec![], |mut opcodes, opcode| {
        match opcode {
            OpCode::Clone(Local(pos)) if pos >= 2 => {
                let syntax_var = SyntaxVar(pos - 2);
                let state = map_codegen.var_tracker.do_use(syntax_var);
                let origin_property = origin_properties[syntax_var.0 as usize];

                match (state.use_count, state.reused) {
                    (1, false) => {
                        // no need to clone
                        opcodes.push(OpCode::TakeAttr(Local(0), origin_property.0));
                    }
                    (_, false) => {
                        // first use, must clone
                        opcodes.push(OpCode::TakeAttr(Local(0), origin_property.0));
                        var_stack_state.stack.push(syntax_var);
                        opcodes.push(OpCode::Clone(Local(
                            2 + var_stack_state.stack.len() as u32 - 1,
                        )));
                    }
                    (1, true) => {
                        // last use
                        if let Some(index) = var_stack_state.swap_to_top(syntax_var) {
                            // swap to top
                            opcodes.push(OpCode::Swap(
                                Local(2 + index),
                                Local(2 + var_stack_state.stack.len() as u32 - 1),
                            ));
                        }
                        var_stack_state.stack.pop();
                    }
                    (_, true) => {
                        // not first, and not last use
                        let index = var_stack_state.find(syntax_var);
                        opcodes.push(OpCode::Clone(Local(2 + index as u32)));
                    }
                }
            }
            _ => {
                opcodes.push(opcode);
            }
        }
        opcodes
    });

    debug!("{opcodes:#?}");

    UnlinkedProc {
        n_params: NParams(1),
        opcodes,
    }
}

#[derive(Default)]
struct VarStackState {
    stack: SmallVec<[SyntaxVar; 8]>,
}

impl VarStackState {
    fn swap_to_top(&mut self, var: SyntaxVar) -> Option<u32> {
        let top = *self.stack.last().unwrap();
        if top == var {
            None
        } else {
            let index = self.find(var);
            let last = self.stack.len() - 1;
            self.stack.swap(index, last);
            Some(index as u32)
        }
    }

    fn find(&self, var: SyntaxVar) -> usize {
        let (index, _) = self
            .stack
            .iter()
            .enumerate()
            .find(|(_, stack_var)| **stack_var == var)
            .unwrap();
        index
    }
}
