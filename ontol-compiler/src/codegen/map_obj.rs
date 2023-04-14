use std::{cell::RefCell, rc::Rc};

use indexmap::IndexMap;
use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams, OpCode},
    value::PropertyId,
};
use smallvec::{smallvec, SmallVec};
use tracing::debug;

use crate::{
    codegen::{
        generator::{CodeGenerator, CodegenVariable},
        ir::{Ir, Terminator},
        proc_builder::Block,
        translate::VarFlowTracker,
    },
    typed_expr::{BindDepth, ExprRef, SyntaxVar, TypedExprKind},
    SourceSpan,
};

use super::{equation::TypedExprEquation, proc_builder::ProcBuilder, ProcTable};

/// Generate code originating from a map obj destructuring
pub(super) fn codegen_map_obj_origin(
    proc_table: &mut ProcTable,
    equation: &TypedExprEquation,
    to: ExprRef,
    origin_attrs: &IndexMap<PropertyId, ExprRef>,
) -> ProcBuilder {
    let (_, to_expr, span) = equation.resolve_expr(&equation.expansions, to);

    // always start at 0 (for now), there are no recursive map_objs (yet)
    let input_local = 0;

    // debug!("origin attrs: {origin_attrs:#?}");
    // debug!("reductions: {:?}", equation.reductions.debug_table());

    let mut origin_properties: Vec<_> = origin_attrs
        .iter()
        .map(|(prop_id, expr_ref)| {
            match &equation
                .resolve_expr(&equation.reductions, *expr_ref)
                .1
                .kind
            {
                TypedExprKind::Variable(var) => (*prop_id, *var),
                other => {
                    panic!(
                        "reduced property {prop_id:?} {expr_ref:?} not a variable, but {other:?}"
                    )
                }
            }
        })
        .collect();
    origin_properties.sort_by_key(|(_, var)| *var);

    if !origin_properties.is_empty() {
        // must start with SyntaxVar(0)
        assert!(origin_properties[0].1 == SyntaxVar(0, BindDepth(0)));
    }

    struct MapCodegen {
        origin_properties: Vec<(PropertyId, SyntaxVar)>,
        var_tracker: VarFlowTracker,
        origin_local: u16,
    }

    impl CodegenVariable for Rc<RefCell<MapCodegen>> {
        fn codegen_variable(
            &mut self,
            builder: &mut ProcBuilder,
            block: &mut Block,
            var: SyntaxVar,
            span: &SourceSpan,
        ) {
            let mut this = self.borrow_mut();
            this.var_tracker.count_use(var);

            let origin_local = this.origin_local;

            // To find the value local, we refer to the first local resulting from TakeAttr2,
            // after the definition of the return value
            let value_local = (origin_local + 2) + (var.0 * 2) + 1;

            builder.ir_push(1, Ir::Clone(Local(value_local)), *span, block);
        }
    }

    let map_codegen = Rc::new(RefCell::new(MapCodegen {
        origin_properties,
        var_tracker: Default::default(),
        origin_local: input_local,
    }));

    let mut builder = ProcBuilder::new(NParams(1));
    let mut block = builder.new_block(Terminator::Return(Local(1)), span);

    CodeGenerator::default().enter_bind_level(map_codegen.clone(), |generator| {
        let origin_local = map_codegen.borrow().origin_local;
        match &to_expr.kind {
            TypedExprKind::MapObjPattern(dest_attrs) => {
                let return_def_id = to_expr.ty.get_single_def_id().unwrap();

                // Local(1), this is the return value:
                builder.ir_push(
                    1,
                    Ir::CallBuiltin(BuiltinProc::NewMap, return_def_id),
                    span,
                    &mut block,
                );

                for (property_id, _) in &map_codegen.borrow().origin_properties {
                    builder.ir_push(
                        2,
                        Ir::TakeAttr2(Local(origin_local), *property_id),
                        span,
                        &mut block,
                    );
                }

                for (property_id, expr_ref) in dest_attrs {
                    generator.codegen_expr(
                        proc_table,
                        &mut builder,
                        &mut block,
                        equation,
                        *expr_ref,
                    );
                    builder.ir_pop(
                        1,
                        Ir::PutAttrValue(Local(1), *property_id),
                        span,
                        &mut block,
                    );
                }
            }
            TypedExprKind::ValueObjPattern(expr_ref) => {
                // FIXME: Code duplication (above)
                for (property_id, _) in &map_codegen.borrow().origin_properties {
                    builder.ir_push(
                        2,
                        Ir::TakeAttr2(Local(origin_local), *property_id),
                        span,
                        &mut block,
                    );
                }
                generator.codegen_expr(proc_table, &mut builder, &mut block, equation, *expr_ref);
            }
            kind => {
                todo!("to: {kind:?}");
            }
        }
    });

    let mut map_codegen_mut = map_codegen.borrow_mut();

    // post-process
    if false {
        let mut var_stack_state = VarStackState::default();
        block.opcodes = block
            .opcodes
            .into_iter()
            .fold(smallvec![], |mut opcodes, opcode| {
                match opcode {
                    (OpCode::Clone(Local(pos)), span) if pos >= 2 => {
                        let syntax_var = SyntaxVar(pos - 2_u16, BindDepth(0));
                        let state = map_codegen_mut.var_tracker.do_use(syntax_var);
                        let origin_properties = &map_codegen_mut.origin_properties;
                        let origin_property = origin_properties[syntax_var.0 as usize];

                        match (state.use_count, state.reused) {
                            (1, false) => {
                                // no need to clone
                                opcodes
                                    .push((OpCode::TakeAttr2(Local(0), origin_property.0), span));
                            }
                            (_, false) => {
                                // first use, must clone
                                opcodes
                                    .push((OpCode::TakeAttr2(Local(0), origin_property.0), span));
                                var_stack_state.stack.push(syntax_var);
                                opcodes.push((
                                    OpCode::Clone(Local(
                                        2 + var_stack_state.stack.len() as u16 - 1,
                                    )),
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
                                            Local(2 + var_stack_state.stack.len() as u16 - 1),
                                        ),
                                        span,
                                    ));
                                }
                                var_stack_state.stack.pop();
                            }
                            (_, true) => {
                                // not first, and not last use
                                let index = var_stack_state.find(syntax_var);
                                opcodes.push((OpCode::Clone(Local(2 + index as u16)), span));
                            }
                        }
                    }
                    _ => {
                        opcodes.push(opcode);
                    }
                }
                opcodes
            });
    }

    debug!("{:#?}", block.opcodes);

    builder.commit(block);

    builder
}

#[derive(Default)]
struct VarStackState {
    stack: SmallVec<[SyntaxVar; 8]>,
}

impl VarStackState {
    fn swap_to_top(&mut self, var: SyntaxVar) -> Option<u16> {
        let top = *self.stack.last().unwrap();
        if top == var {
            None
        } else {
            let index = self.find(var);
            let last = self.stack.len() - 1;
            self.stack.swap(index, last);
            Some(index as u16)
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
