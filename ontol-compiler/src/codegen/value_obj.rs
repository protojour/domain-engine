use std::{cell::RefCell, rc::Rc};

use ontol_runtime::{
    proc::{BuiltinProc, Local, NParams, OpCode},
    DefId,
};
use tracing::debug;

use crate::{
    codegen::{
        generator::{CodeGenerator, CodegenVariable},
        proc_builder::SpannedOpCodes,
        translate::VarFlowTracker,
        Block, ProcBuilder, Terminator,
    },
    typed_expr::{BindDepth, ExprRef, SyntaxVar, TypedExprKind},
    SourceSpan,
};

use super::{equation::TypedExprEquation, ProcTable};

pub(super) fn codegen_value_obj_origin(
    proc_table: &mut ProcTable,
    equation: &TypedExprEquation,
    to: ExprRef,
    to_def: DefId,
) -> ProcBuilder {
    let (_, to_expr, span) = equation.resolve_expr(&equation.expansions, to);

    struct ValueCodegen {
        input_local: Local,
        var_tracker: VarFlowTracker,
    }

    impl CodegenVariable for Rc<RefCell<ValueCodegen>> {
        fn codegen_variable(
            &mut self,
            builder: &mut ProcBuilder,
            block: &mut Block,
            var: SyntaxVar,
            span: &SourceSpan,
        ) {
            let mut this = self.borrow_mut();
            // There should only be one origin variable (but can flow into several slots)
            assert!(var.0 == 0);
            this.var_tracker.count_use(var);
            builder.push_stack_old(1, (OpCode::Clone(this.input_local), *span), block);
        }
    }

    let value_codegen = Rc::new(RefCell::new(ValueCodegen {
        input_local: Local(0),
        var_tracker: Default::default(),
    }));
    let mut builder = ProcBuilder::new(NParams(1));
    let mut block = builder.new_block(Terminator::Return(Local(0)), span);

    CodeGenerator::default().enter_bind_level(value_codegen.clone(), |generator| {
        match &to_expr.kind {
            TypedExprKind::ValueObjPattern(expr_ref) => {
                generator.codegen_expr(proc_table, &mut builder, &mut block, equation, *expr_ref);
            }
            TypedExprKind::MapObjPattern(dest_attrs) => {
                block
                    .opcodes
                    .push((OpCode::CallBuiltin(BuiltinProc::NewMap, to_def), span));

                // the input value is not a map, so it will be consumed.
                // Therefore it must be top of the stack:
                block.opcodes.push((OpCode::Swap(Local(0), Local(1)), span));
                value_codegen.borrow_mut().input_local = Local(1);

                for (property_id, node) in dest_attrs {
                    generator.codegen_expr(proc_table, &mut builder, &mut block, equation, *node);
                    block
                        .opcodes
                        .push((OpCode::PutUnitAttr(Local(0), *property_id), span));
                }
            }
            kind => {
                todo!("target: {kind:?}");
            }
        }
    });

    let mut value_codegen_mut = value_codegen.borrow_mut();

    block.opcodes = block
        .opcodes
        .into_iter()
        .filter(|op| {
            match op {
                (OpCode::Clone(local), _) if *local == value_codegen_mut.input_local => {
                    // Keep cloning until the last use of the variable,
                    // which must pop it off the stack. (i.e. keep the clone instruction).
                    // else: drop clone instruction. Stack should only contain the return value.
                    value_codegen_mut
                        .var_tracker
                        .do_use(SyntaxVar(0, BindDepth(0)))
                        .use_count
                        > 1
                }
                _ => true,
            }
        })
        .collect::<SpannedOpCodes>();

    debug!("{:#?}", block.opcodes);

    builder.commit(block);

    builder
}
