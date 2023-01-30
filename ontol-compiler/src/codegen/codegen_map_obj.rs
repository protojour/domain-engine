use std::collections::HashMap;

use ontol_runtime::{
    vm::{BuiltinProc, EntryPoint, Local, NArgs, OpCode, Program},
    PropertyId,
};
use smallvec::{smallvec, SmallVec};

use crate::codegen::{
    codegen::{Codegen, Snippet},
    typed_expr::SyntaxVar,
};

use super::typed_expr::{NodeId, TypedExprKind, TypedExprTable};

/// Generate code originating from a map obj destructuring
pub fn codegen_map_obj_origin<'m>(
    program: &mut Program,
    table: &TypedExprTable<'m>,
    origin_attrs: &HashMap<PropertyId, NodeId>,
    dest_node: NodeId,
) -> EntryPoint {
    let mut origin_properties: Vec<_> = origin_attrs
        .iter()
        .map(
            |(prop_id, node_id)| match &table.get_expr_no_rewrite(*node_id).kind {
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

    let (_, dest_expr) = table.get_expr(&table.target_rewrites, dest_node);

    match &dest_expr.kind {
        TypedExprKind::MapObj(dest_attrs) => {
            codegen_map_to_map(program, table, origin_properties, dest_attrs)
        }
        kind => {
            todo!("target: {kind:?}");
        }
    }
}

/// Generate code for a map translating into another map
fn codegen_map_to_map<'m>(
    program: &mut Program,
    table: &TypedExprTable<'m>,
    origin_properties: Vec<(PropertyId, SyntaxVar)>,
    dest_attrs: &HashMap<PropertyId, NodeId>,
) -> EntryPoint {
    let mut opcodes = vec![];

    opcodes.push(OpCode::CallBuiltin(BuiltinProc::NewCompound));

    #[derive(Default)]
    struct PropertyFlowState {
        use_count: usize,
        loaded: bool,
    }

    struct MapCodegen {
        // for determining whether to clone
        var_states: HashMap<SyntaxVar, PropertyFlowState>,
    }

    impl Codegen for MapCodegen {
        fn codegen_variable(&mut self, var: SyntaxVar, snippet: &mut Snippet) {
            self.var_states.entry(var).or_default().use_count += 1;
            snippet.push(OpCode::Clone(Local(var.0 + 2)));
        }
    }

    let mut map_codegen = MapCodegen {
        var_states: Default::default(),
    };

    let dest_snippets = dest_attrs
        .iter()
        .map(|(property_id, node)| {
            let mut snippet = smallvec![];
            map_codegen.codegen_expr(table, *node, &mut snippet);
            snippet.push(OpCode::PutAttr(Local(1), *property_id));
            snippet
        })
        .collect::<Vec<_>>();

    let mut var_stack_state = VarStackState::default();

    for opcode in dest_snippets.into_iter().flat_map(|opcodes| opcodes) {
        match opcode {
            OpCode::Clone(Local(l)) if l >= 2 => {
                let syntax_var = SyntaxVar(l - 2);
                let state = map_codegen.var_states.entry(syntax_var).or_default();
                let origin_property = origin_properties[syntax_var.0 as usize];

                match (state.use_count, state.loaded) {
                    (1, false) => {
                        // no need to clone
                        state.loaded = true;
                        opcodes.push(OpCode::TakeAttr(Local(0), origin_property.0));
                    }
                    (_, false) => {
                        // first use, must clone
                        state.loaded = true;
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

                state.use_count -= 1;
            }
            _ => {
                opcodes.push(opcode);
            }
        }
    }

    opcodes.push(OpCode::Return(Local(1)));

    println!("{opcodes:#?}");

    program.add_procedure(NArgs(1), opcodes)
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
