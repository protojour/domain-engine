use std::collections::HashMap;

use ontol_runtime::{
    proc::{BuiltinProc, Lib, Local, NParams, OpCode, Procedure},
    PropertyId,
};
use smallvec::{smallvec, SmallVec};
use tracing::debug;

use crate::codegen::{
    codegen::{Codegen, OpCodes, VarFlowTracker},
    typed_expr::SyntaxVar,
};

use super::typed_expr::{NodeId, TypedExprKind, TypedExprTable};

/// Generate code originating from a map obj destructuring
pub fn codegen_map_obj_origin<'m>(
    lib: &mut Lib,
    table: &TypedExprTable<'m>,
    origin_attrs: &HashMap<PropertyId, NodeId>,
    dest_node: NodeId,
) -> Procedure {
    let (_, dest_expr) = table.get_expr(&table.target_rewrites, dest_node);

    let mut origin_properties: Vec<_> = origin_attrs
        .iter()
        .map(
            |(prop_id, node_id)| match &table.get_expr(&table.source_rewrites, *node_id).1.kind {
                TypedExprKind::Variable(var) => (*prop_id, *var),
                _ => panic!("source property not a variable"),
            },
        )
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
    let mut opcodes = smallvec![];

    match &dest_expr.kind {
        TypedExprKind::MapObj(dest_attrs) => {
            // Local(1), this is the return value:
            opcodes.push(OpCode::CallBuiltin(BuiltinProc::NewCompound));

            for (property_id, node) in dest_attrs {
                map_codegen.codegen_expr(table, *node, &mut opcodes);
                opcodes.push(OpCode::PutAttr(Local(1), *property_id));
            }

            opcodes.push(OpCode::Return(Local(1)));
        }
        TypedExprKind::ValueObj(node_id) => {
            map_codegen.codegen_expr(table, *node_id, &mut opcodes);
            opcodes.push(OpCode::Return(Local(1)));
        }
        kind => {
            todo!("target: {kind:?}");
        }
    }

    // post-process
    let mut var_stack_state = VarStackState::default();
    let opcodes: OpCodes = opcodes
        .into_iter()
        .fold(smallvec![], |mut opcodes, opcode| {
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

    lib.add_procedure(NParams(1), opcodes)
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
