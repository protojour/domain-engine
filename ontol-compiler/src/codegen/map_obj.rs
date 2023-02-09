use indexmap::IndexMap;
use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams, OpCode},
    value::PropertyId,
};
use smallvec::{smallvec, SmallVec};
use tracing::debug;

use crate::{
    codegen::{translate::VarFlowTracker, Codegen, SpannedOpCodes},
    typed_expr::{ExprRef, SyntaxVar, TypedExprKind, TypedExprTable},
    SourceSpan,
};

use super::{ProcTable, UnlinkedProc};

/// Generate code originating from a map obj destructuring
pub(super) fn codegen_map_obj_origin(
    proc_table: &mut ProcTable,
    expr_table: &TypedExprTable,
    to: ExprRef,
    origin_attrs: &IndexMap<PropertyId, ExprRef>,
) -> UnlinkedProc {
    let (_, to_expr, span) = expr_table.resolve_expr(&expr_table.target_rewrites, to);

    let mut origin_properties: Vec<_> = origin_attrs
        .iter()
        .map(|(prop_id, expr_ref)| {
            match &expr_table
                .resolve_expr(&expr_table.source_rewrites, *expr_ref)
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
        fn codegen_variable(
            &mut self,
            var: SyntaxVar,
            opcodes: &mut SpannedOpCodes,
            span: &SourceSpan,
        ) {
            self.var_tracker.count_use(var);
            // Make Clone(Local(v + 2)) represent use of the syntax variable.
            // this will be rewritten later:
            opcodes.push((OpCode::Clone(Local(var.0 + 2)), *span));
        }
    }

    let mut map_codegen = MapCodegen::default();
    let mut ops = smallvec![];

    match &to_expr.kind {
        TypedExprKind::MapObjPattern(dest_attrs) => {
            let return_def_id = to_expr.ty.get_single_def_id().unwrap();

            // Local(1), this is the return value:
            ops.push((
                OpCode::CallBuiltin(BuiltinProc::NewMap, return_def_id),
                span,
            ));

            for (property_id, expr_ref) in dest_attrs {
                map_codegen.codegen_expr(proc_table, expr_table, *expr_ref, &mut ops);
                ops.push((OpCode::PutUnitAttr(Local(1), *property_id), span));
            }

            ops.push((OpCode::Return(Local(1)), span));
        }
        TypedExprKind::ValueObjPattern(expr_ref) => {
            map_codegen.codegen_expr(proc_table, expr_table, *expr_ref, &mut ops);
            ops.push((OpCode::Return(Local(1)), span));
        }
        kind => {
            todo!("to: {kind:?}");
        }
    }

    // post-process
    let mut var_stack_state = VarStackState::default();
    let opcodes: SpannedOpCodes = ops.into_iter().fold(smallvec![], |mut opcodes, opcode| {
        match opcode {
            (OpCode::Clone(Local(pos)), span) if pos >= 2 => {
                let syntax_var = SyntaxVar(pos - 2);
                let state = map_codegen.var_tracker.do_use(syntax_var);
                let origin_property = origin_properties[syntax_var.0 as usize];

                match (state.use_count, state.reused) {
                    (1, false) => {
                        // no need to clone
                        opcodes.push((OpCode::TakeAttrValue(Local(0), origin_property.0), span));
                    }
                    (_, false) => {
                        // first use, must clone
                        opcodes.push((OpCode::TakeAttrValue(Local(0), origin_property.0), span));
                        var_stack_state.stack.push(syntax_var);
                        opcodes.push((
                            OpCode::Clone(Local(2 + var_stack_state.stack.len() as u32 - 1)),
                            span,
                        ));
                    }
                    (1, true) => {
                        // last use
                        if let Some(index) = var_stack_state.swap_to_top(syntax_var) {
                            // swap to top
                            opcodes.push((
                                OpCode::Swap(
                                    Local(2 + index),
                                    Local(2 + var_stack_state.stack.len() as u32 - 1),
                                ),
                                span,
                            ));
                        }
                        var_stack_state.stack.pop();
                    }
                    (_, true) => {
                        // not first, and not last use
                        let index = var_stack_state.find(syntax_var);
                        opcodes.push((OpCode::Clone(Local(2 + index as u32)), span));
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
